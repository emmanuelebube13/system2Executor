"""EXEC-012 tests — trade-close tracking: register-on-fill, broker-close sweep, close
emission (§6.1 flat FillEvent), idempotency across restarts, backfill tool, fail-open.

The §6.1 close-event shape is additionally validated against System 3's REAL production
validator (``ams.common.contracts``) when the S3 repo is present; that test skips
gracefully otherwise so the suite stays portable. S3 code is an oracle only — read-only.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from system2.broker.position_manager import ManagedTrade, PositionManager
from system2.common.queue_backend import LocalDurableBackend
from system2.execution.close_tracker import (
    VALID_EXIT_REASONS,
    CloseEmitter,
    CloseSweeper,
    build_close_event,
    build_managed_trade,
    exit_reason_from_trade_details,
    exit_reason_from_transaction,
    facts_from_close_response,
    facts_from_trade_details,
    facts_from_transaction,
    find_close_transaction,
    managed_trade_from_broker_trade,
    managed_trade_from_fill,
    resolve_close_from_transactions,
)
from system2.execution.fill_producer import FillProducer, FillResult, build_outbox
from system2.execution.lifecycle import EmergencyStop, ExecutionRuntime
from system2.execution.outbound_consumer import OutboundConsumer, SqliteProcessedStore
from system2.execution.pipeline import ApprovedOrder, ExecMode, ExecutionPipeline, RiskContext

T0 = datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)
WED = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)  # in-session
S3_TOPIC = "ams-inbound.ams"
OUT = "AMS_Outbound_Queue"
REPO_ROOT = Path(__file__).resolve().parents[4]
S3_SRC = Path(r"C:\Users\emman\OneDrive\Documents\Projects\working\scalablebrain\system3\ams\src")

# FillEvent.schema.json full field list — additionalProperties:false at S3, so a close
# event carrying ANY key outside this set would be dead-lettered by the consumer.
ALLOWED_EVENT_FIELDS = {
    "schema_version", "broker_order_id", "order_request_id", "status", "signal_id",
    "pair", "direction", "units", "fill_price", "fill_time", "event_time",
    "realized_pnl", "slippage_pips", "exit_reason",
}


# --------------------------------------------------------------------------- #
# Fakes (no network, no real broker)
# --------------------------------------------------------------------------- #
class FakeQueue:
    """Downstream queue stub recording (topic, body) pairs."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    def publish(self, topic: str, body: dict) -> None:
        self.published.append((topic, body))

    def events(self, topic: str = S3_TOPIC) -> list[dict]:
        return [b for t, b in self.published if t == topic]


class FakeInbound:
    """Entry-fill inbound queue (EXEC-005 path)."""

    def __init__(self) -> None:
        self.delivered: list[dict] = []

    def publish(self, topic: str, body: dict) -> None:
        self.delivered.append(body)


class FakeTransport:
    """OandaTransport stub: get_open_trades / get_trade / transaction stream, with counts."""

    def __init__(self, open_trades=None, trade_details=None, transactions=None,
                 last_txn_id=None, trade_error=None) -> None:
        self._open = list(open_trades or [])
        self._details = dict(trade_details or {})
        self._transactions = list(transactions or [])
        self._last_txn_id = last_txn_id
        self.trade_error = trade_error  # exception to raise from get_trade (e.g. NO_SUCH_TRADE)
        self.open_trades_calls = 0
        self.get_trade_calls = 0
        self.idrange_calls = 0
        self.fail_open_trades = False

    def get_open_trades(self):
        self.open_trades_calls += 1
        if self.fail_open_trades:
            raise ConnectionError("OANDA 503 service unavailable")
        return self._open

    def get_trade(self, trade_id):
        self.get_trade_calls += 1
        if self.trade_error is not None:
            raise self.trade_error
        return self._details.get(str(trade_id))

    def get_account_summary(self):
        return {"lastTransactionID": str(self._last_txn_id)} if self._last_txn_id else {}

    def get_transactions_idrange(self, from_id, to_id):
        self.idrange_calls += 1
        out = []
        for txn in self._transactions:
            try:
                tid = int(txn.get("id"))
            except (TypeError, ValueError):
                continue
            if from_id <= tid <= to_id:
                out.append(txn)
        return out


class ClosingAdapter:
    """Adapter stub whose close_trade returns a realistic OANDA TradeClose response."""

    def __init__(self, response: dict) -> None:
        self.response = response
        self.closed: list[str] = []
        self.transport = FakeTransport()

    def close_trade(self, trade_id, units="ALL"):
        self.closed.append(trade_id)
        return self.response

    def modify_stop(self, trade_id, new_stop, instrument=None):
        return {"ok": True}


class FlakyOutbox:
    """Delegates to a real outbox; ``fail=True`` makes publish raise (simulated disk error)."""

    def __init__(self, inner) -> None:
        self.inner = inner
        self.fail = False

    def publish(self, topic, body):
        if self.fail:
            raise sqlite3.OperationalError("disk I/O error")
        self.inner.publish(topic, body)

    def pull(self, *args, **kwargs):
        return self.inner.pull(*args, **kwargs)


# --------------------------------------------------------------------------- #
# Realistic broker payloads + builders
# --------------------------------------------------------------------------- #
def _closed_trade_details(**over) -> dict:
    """OANDA TradeDetails payload for a CLOSED trade (SL hit; nanosecond timestamps)."""
    d = {
        "id": "2518",
        "instrument": "EUR_USD",
        "price": "1.14200",
        "openTime": "2026-07-15T09:12:01.123456789Z",
        "initialUnits": "-307419",
        "currentUnits": "0",
        "state": "CLOSED",
        "realizedPL": "-545.4201",
        "averageClosePrice": "1.14266",
        "closeTime": "2026-07-15T12:45:36.329990644Z",
        "closingTransactionIDs": ["2559", "2560"],
        "stopLossOrder": {"id": "2520", "state": "FILLED", "price": "1.14266"},
        "takeProfitOrder": {"id": "2519", "state": "CANCELLED", "price": "1.13900"},
    }
    d.update(over)
    return d


def _trade_close_response(**over) -> dict:
    """OANDA TradeClose (PUT .../close) response — the S2-originated close-facts source."""
    d = {
        "orderFillTransaction": {
            "id": "2600",
            "pl": "12.5000",
            "price": "1.10500",
            "time": "2026-07-15T13:00:00.123456789Z",
            "units": "10000",
            "reason": "MARKET_ORDER_TRADE_CLOSE",
            "tradesClosed": [
                {"tradeID": "T1", "units": "10000", "price": "1.10500", "realizedPL": "12.5000"}
            ],
        },
        "relatedTransactionIDs": ["2599", "2600"],
    }
    d.update(over)
    return d


def _short_trade(**over) -> ManagedTrade:
    base = dict(
        broker_trade_id="2518", instrument="EUR_USD", side="SELL", entry_price=1.14200,
        initial_stop_price=1.14266, take_profit_price=1.13900, open_time=T0,
        granularity="H1",
        correlation_id="3cedddf6-0965-447b-a79c-8d89c57af610",
        order_request_id="0f7a9d7e-1111-2222-3333-444455556666",
    )
    base.update(over)
    return build_managed_trade(**base)


def _approved_order(**over) -> ApprovedOrder:
    base = dict(
        idempotency_key="k1", correlation_id="c1", instrument="EUR_USD", side="BUY",
        units=10000, granularity="H1", risk_context=RiskContext(atr=0.0010),
        ams_decision_id="ams-1",
    )
    base.update(over)
    return ApprovedOrder(**base)


def _fill(**over) -> FillResult:
    base = dict(
        realized_status="FILLED", filled_units=10000, broker_order_id="2517",
        broker_trade_id="2518", fill_price=1.10002,
        fill_time="2026-07-01T12:00:01.000000000Z",
        stop_loss_price=1.09900, take_profit_price=1.10300,
    )
    base.update(over)
    return FillResult(**base)


# --------------------------------------------------------------------------- #
# Rigs
# --------------------------------------------------------------------------- #
def _sweep_rig(tmp_path, *, trade=None, details=None, open_trades=(),
               outbox_name="close_outbox.db", ledger_name="close_sweep.db"):
    queue = FakeQueue()
    outbox = build_outbox(tmp_path / outbox_name)
    ledger = SqliteProcessedStore(tmp_path / ledger_name)
    emitter = CloseEmitter(queue=queue, outbox=outbox, topic=S3_TOPIC, ledger=ledger)
    posman = PositionManager(adapter=None, clock=lambda: T0)
    trade = trade or _short_trade()
    posman.register(trade)
    transport = FakeTransport(
        open_trades=open_trades,
        trade_details={trade.broker_trade_id: details or _closed_trade_details()},
    )
    clk = [T0]
    sweeper = CloseSweeper(position_manager=posman, transport=transport,
                           emitter=emitter, interval_sec=30.0, clock=lambda: clk[0])
    return dict(queue=queue, outbox=outbox, ledger=ledger, emitter=emitter, posman=posman,
                trade=trade, transport=transport, clk=clk, sweeper=sweeper)


def _order_msg(**over) -> dict:
    m = {
        "schema_version": "1", "message_id": "m-" + over.get("idempotency_key", "k1"),
        "idempotency_key": "k1", "correlation_id": "c1",
        "created_at": "2026-07-01T11:59:00Z", "instrument": "EUR_USD", "side": "BUY",
        "units": 10000, "granularity": "H1", "risk_context": {"atr": 0.0010},
        "ams_decision_id": "ams-1",
    }
    m.update(over)
    return m


def _consumer_rig(tmp_path):
    """Runtime-level rig mirroring build_from_secrets' EXEC-012 _emit_fill wiring."""
    out_q = LocalDurableBackend(tmp_path / "out.db")
    inbound = FakeInbound()
    producer = FillProducer(inbound, "ams-inbound", build_outbox(tmp_path / "fill_outbox.db"))
    posman = PositionManager(adapter=None, clock=lambda: WED)
    processed = SqliteProcessedStore(tmp_path / "proc.db")
    pipe = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=False,
                             processed_store=processed, clock=lambda: WED)
    trade_seq = iter(range(1, 100))

    def _submit(constructed):
        n = next(trade_seq)
        return FillResult(realized_status="FILLED", filled_units=constructed.units,
                          broker_order_id=f"o{n}", broker_trade_id=f"T{n}",
                          fill_price=constructed.entry_price,
                          stop_loss_price=constructed.stop_loss,
                          take_profit_price=constructed.take_profit,
                          fill_time="2026-07-01T12:00:01Z")

    def _emit_fill(order, constructed, fill):
        # mirror of lifecycle.build_from_secrets._emit_fill (EXEC-012 register-on-fill)
        producer.publish_fill(order, fill)
        managed = managed_trade_from_fill(order, constructed, fill)
        if managed is not None:
            posman.register(managed)

    consumer = OutboundConsumer(
        queue=out_q, subscription=OUT, pipeline=pipe,
        price_fn=lambda o: 1.10000,
        submit_fn=_submit,
        emit_fn=_emit_fill,
        open_instruments_fn=lambda: [t.instrument for t in posman.trades.values()
                                     if not t.closed],
        park_backoff_sec=0.0, clock=lambda: WED,
    )
    return dict(out_q=out_q, inbound=inbound, producer=producer, posman=posman,
                consumer=consumer)


# --------------------------------------------------------------------------- #
# 1) Register-on-fill
# --------------------------------------------------------------------------- #
def test_managed_trade_from_fill_carries_order_identity():
    mt = managed_trade_from_fill(_approved_order(), None, _fill())
    assert mt is not None
    assert mt.broker_trade_id == "2518"
    assert mt.order_request_id == "k1"      # = order idempotency_key = S3 order_request_id
    assert mt.correlation_id == "c1"        # = S3 signal_id (journal match key)
    assert mt.side == "BUY" and mt.instrument == "EUR_USD"
    assert mt.entry_price == pytest.approx(1.10002)
    assert mt.initial_stop_price == pytest.approx(1.09900)
    assert mt.take_profit_price == pytest.approx(1.10300)
    assert not mt.closed


def test_managed_trade_from_fill_none_when_nothing_to_manage():
    assert managed_trade_from_fill(
        _approved_order(), None,
        _fill(realized_status="REJECTED", broker_trade_id=None, filled_units=0)) is None
    assert managed_trade_from_fill(_approved_order(), None, _fill(broker_trade_id=None)) is None


def test_register_on_fill_lands_trade_in_position_manager(tmp_path):
    rig = _consumer_rig(tmp_path)
    rig["out_q"].publish(OUT, _order_msg())
    stats = rig["consumer"].poll_once()
    assert stats.get("executed") == 1
    assert len(rig["inbound"].delivered) == 1  # entry fill still emitted (EXEC-005 unchanged)
    trade = rig["posman"].trades["T1"]
    assert trade.order_request_id == "k1"
    assert trade.correlation_id == "c1"
    assert trade.side == "BUY"
    assert trade.entry_price == pytest.approx(1.10000)
    assert trade.initial_stop_price == pytest.approx(1.10000 - 0.0010)  # entry - 1*ATR
    assert not trade.closed


# --------------------------------------------------------------------------- #
# 6) Duplicate-instrument backup guard now sees session-opened trades
# --------------------------------------------------------------------------- #
def test_duplicate_instrument_guard_sees_session_opened_trades(tmp_path):
    """The 2026-07-15 hole: two concurrent EUR_USD orders. The second must be blocked."""
    rig = _consumer_rig(tmp_path)
    rig["out_q"].publish(OUT, _order_msg(idempotency_key="k1", correlation_id="c1"))
    rig["out_q"].publish(OUT, _order_msg(idempotency_key="k2", correlation_id="c2"))
    stats = rig["consumer"].poll_once()
    assert stats.get("executed") == 1
    assert stats.get("rejected_backup_guard") == 1
    assert len(rig["posman"].trades) == 1          # only the first trade registered
    assert rig["out_q"].pull(OUT) == []            # rejection durably handled (ack'd)


# --------------------------------------------------------------------------- #
# 2) Sweep detects a broker-side close and emits the §6.1 event
# --------------------------------------------------------------------------- #
def test_sweep_detects_broker_close_and_emits_contract_event(tmp_path):
    rig = _sweep_rig(tmp_path)
    res = rig["sweeper"].sweep()
    assert res == {"candidates": 1, "closed": 1}
    events = rig["queue"].events()
    assert len(events) == 1                        # exactly one close event on ams-inbound.ams
    e = events[0]
    assert set(e) <= ALLOWED_EVENT_FIELDS          # additionalProperties:false at S3
    assert e["schema_version"] == "1"
    assert e["status"] == "closed"
    assert e["broker_order_id"] == "2560"          # LAST closing transaction id...
    assert e["broker_order_id"] != "2517"          # ...NEVER the entry fill txn id (S3 dedup)
    assert e["order_request_id"] == "0f7a9d7e-1111-2222-3333-444455556666"
    assert e["signal_id"] == "3cedddf6-0965-447b-a79c-8d89c57af610"  # = correlation_id
    assert e["pair"] == "EUR_USD"
    assert e["direction"] == "short"
    assert e["units"] == -307419                   # signed: negative for a short
    assert isinstance(e["units"], int)
    assert e["fill_price"] == pytest.approx(1.14266)
    assert e["fill_time"] == "2026-07-15T12:45:36.329990644Z"
    assert e["realized_pnl"] == pytest.approx(-545.4201)
    assert e["exit_reason"] == "sl"                # stopLossOrder.state == FILLED
    assert rig["trade"].closed
    assert rig["ledger"].seen("2518")


# --------------------------------------------------------------------------- #
# 3) Idempotency — a second sweep emits nothing new
# --------------------------------------------------------------------------- #
def test_second_sweep_past_throttle_emits_nothing_new(tmp_path):
    rig = _sweep_rig(tmp_path)
    rig["sweeper"].sweep()
    assert len(rig["queue"].events()) == 1
    rig["clk"][0] = T0 + timedelta(seconds=31)     # past the 30s throttle
    res = rig["sweeper"].sweep()
    assert res == {"candidates": 0}                # trade marked closed -> no candidates
    assert len(rig["queue"].events()) == 1         # zero new events


# --------------------------------------------------------------------------- #
# 4) Restart between close and sweep + deterministic broker_order_id
# --------------------------------------------------------------------------- #
def test_restart_between_close_and_sweep_does_not_reemit(tmp_path):
    rig = _sweep_rig(tmp_path)
    rig["sweeper"].sweep()
    assert len(rig["queue"].events()) == 1

    # restart: NEW emitter/sweeper over the SAME ledger + outbox paths; the trade is
    # re-adopted fresh (closed=False), as startup_reconcile would after a restart.
    queue2 = FakeQueue()
    emitter2 = CloseEmitter(queue=queue2, outbox=build_outbox(tmp_path / "close_outbox.db"),
                            topic=S3_TOPIC,
                            ledger=SqliteProcessedStore(tmp_path / "close_sweep.db"))
    posman2 = PositionManager(adapter=None, clock=lambda: T0)
    posman2.register(_short_trade())
    sweeper2 = CloseSweeper(position_manager=posman2, transport=rig["transport"],
                            emitter=emitter2, interval_sec=30.0,
                            clock=lambda: T0 + timedelta(seconds=60))
    sweeper2.sweep()
    assert queue2.events() == []                   # ledgered close NOT re-emitted


def test_reemission_with_fresh_ledger_has_same_broker_order_id(tmp_path):
    """Lost ledger => re-emission happens, but with the SAME deterministic id (S3 dedups)."""
    rig = _sweep_rig(tmp_path)
    rig["sweeper"].sweep()
    first = rig["queue"].events()[0]

    rig2 = _sweep_rig(tmp_path, outbox_name="close_outbox2.db", ledger_name="close_sweep2.db")
    rig2["sweeper"].sweep()
    second = rig2["queue"].events()[0]
    assert second["broker_order_id"] == first["broker_order_id"] == "2560"


# --------------------------------------------------------------------------- #
# 5) Broker API error during sweep: fail-open, then recover
# --------------------------------------------------------------------------- #
def test_broker_error_during_sweep_fails_open_then_recovers(tmp_path):
    rig = _sweep_rig(tmp_path)
    rig["transport"].fail_open_trades = True
    res = rig["sweeper"].sweep()
    assert res == {"sweep_error": 1}               # error marker, no crash
    assert rig["queue"].events() == []
    assert not rig["trade"].closed

    rig["transport"].fail_open_trades = False
    rig["clk"][0] = T0 + timedelta(seconds=31)
    res2 = rig["sweeper"].sweep()
    assert res2 == {"candidates": 1, "closed": 1}  # healthy transport -> emits
    assert len(rig["queue"].events()) == 1


# --------------------------------------------------------------------------- #
# 9) Throttle: ONE get_open_trades call per interval
# --------------------------------------------------------------------------- #
def test_throttle_one_open_trades_call_inside_interval(tmp_path):
    trade = _short_trade()
    rig = _sweep_rig(tmp_path, trade=trade, open_trades=[{"id": trade.broker_trade_id}])
    rig["sweeper"].sweep()
    rig["clk"][0] = T0 + timedelta(seconds=10)     # inside the 30s interval
    assert rig["sweeper"].sweep() == {}
    assert rig["transport"].open_trades_calls == 1  # one call total for both sweeps
    rig["clk"][0] = T0 + timedelta(seconds=31)
    rig["sweeper"].sweep()
    assert rig["transport"].open_trades_calls == 2


# --------------------------------------------------------------------------- #
# 10) Fail-open emission
# --------------------------------------------------------------------------- #
def test_publish_failure_fails_open_and_retries_next_sweep(tmp_path):
    queue = FakeQueue()
    flaky = FlakyOutbox(build_outbox(tmp_path / "close_outbox.db"))
    ledger = SqliteProcessedStore(tmp_path / "close_sweep.db")
    emitter = CloseEmitter(queue=queue, outbox=flaky, topic=S3_TOPIC, ledger=ledger)
    posman = PositionManager(adapter=None, clock=lambda: T0)
    trade = _short_trade()
    posman.register(trade)
    transport = FakeTransport(trade_details={trade.broker_trade_id: _closed_trade_details()})
    clk = [T0]
    sweeper = CloseSweeper(position_manager=posman, transport=transport,
                           emitter=emitter, interval_sec=30.0, clock=lambda: clk[0])

    flaky.fail = True
    assert emitter.emit(trade, facts_from_trade_details(_closed_trade_details()), "sl") is False
    res = sweeper.sweep()
    assert res == {"candidates": 1, "closed": 0}
    assert not trade.closed                        # NOT marked closed -> retried next sweep
    assert not ledger.seen(trade.broker_trade_id)
    assert queue.events() == []

    flaky.fail = False
    clk[0] = T0 + timedelta(seconds=31)
    res2 = sweeper.sweep()
    assert res2 == {"candidates": 1, "closed": 1}
    assert trade.closed
    assert len(queue.events()) == 1


def test_engine_tick_survives_raising_sweeper():
    class BoomSweeper:
        def sweep(self):
            raise RuntimeError("boom")

    class _Lag:
        last_message_at = None

    class FakeConsumer:
        lag = _Lag()

        def poll_once(self):
            return {}

    class FakeMonitor:
        def evaluate(self, last_message_at, now):
            return "running"

    runtime = ExecutionRuntime(
        consumer=FakeConsumer(), monitor=FakeMonitor(),
        position_manager=PositionManager(adapter=None),
        producer=None, emergency_stop=EmergencyStop(),
        close_sweeper=BoomSweeper(),
    )
    res = runtime.tick()                           # must not propagate
    assert res["stopped"] is False


# --------------------------------------------------------------------------- #
# 7) S2-originated closes: time exit ("expiry") and flatten-on-stop ("flatten")
# --------------------------------------------------------------------------- #
def test_time_exit_close_emits_expiry_event(tmp_path):
    queue = FakeQueue()
    emitter = CloseEmitter(queue=queue, outbox=build_outbox(tmp_path / "close_outbox.db"),
                           topic=S3_TOPIC, ledger=SqliteProcessedStore(tmp_path / "ledger.db"))
    adapter = ClosingAdapter(_trade_close_response())
    mgr = PositionManager(adapter=adapter, clock=lambda: T0 + timedelta(seconds=3600),
                          emit_close_fn=emitter.emit_broker_close)
    trade = ManagedTrade(
        broker_trade_id="T1", instrument="EUR_USD", side="SELL", entry_price=1.10800,
        initial_stop_price=1.11000, take_profit_price=1.10200, open_time=T0,
        granularity="H1", max_duration_sec=3600.0, correlation_id="c-exp",
        order_request_id="k-exp")
    acts = mgr.on_tick(trade, 1.10500)             # 100% duration -> force close
    assert "time_exit_close" in acts
    assert adapter.closed == ["T1"]
    events = queue.events()
    assert len(events) == 1
    e = events[0]
    assert set(e) <= ALLOWED_EVENT_FIELDS
    assert e["status"] == "closed"
    assert e["exit_reason"] == "expiry"
    assert e["broker_order_id"] == "2600"          # TradeClose orderFillTransaction.id
    assert e["order_request_id"] == "k-exp"
    assert e["signal_id"] == "c-exp"
    assert e["realized_pnl"] == pytest.approx(12.5)
    assert e["fill_price"] == pytest.approx(1.10500)
    assert e["fill_time"] == "2026-07-15T13:00:00.123456789Z"
    assert e["units"] == -10000                    # signed by the trade's side (short)


def test_flatten_on_stop_emits_flatten_event(tmp_path):
    queue = FakeQueue()
    emitter = CloseEmitter(queue=queue, outbox=build_outbox(tmp_path / "close_outbox.db"),
                           topic=S3_TOPIC, ledger=SqliteProcessedStore(tmp_path / "ledger.db"))
    inbound = FakeInbound()
    producer = FillProducer(inbound, "ams-inbound", build_outbox(tmp_path / "fill_outbox.db"))
    adapter = ClosingAdapter(_trade_close_response())
    posman = PositionManager(adapter=adapter, clock=lambda: T0)
    posman.register(ManagedTrade(
        broker_trade_id="T1", instrument="EUR_USD", side="BUY", entry_price=1.10000,
        initial_stop_price=1.09900, take_profit_price=1.10300, open_time=T0,
        granularity="H1", max_duration_sec=0.0, correlation_id="c-flat",
        order_request_id="k-flat"))
    runtime = ExecutionRuntime(
        consumer=None, monitor=None, position_manager=posman, producer=producer,
        emergency_stop=EmergencyStop(), adapter=adapter,
        close_emitter=emitter, flatten_on_stop=True)
    res = runtime.shutdown("flatten-test")
    assert res["flattened"] == 1
    assert adapter.closed == ["T1"]
    events = queue.events()
    assert len(events) == 1
    assert events[0]["exit_reason"] == "flatten"
    assert events[0]["order_request_id"] == "k-flat"
    assert events[0]["signal_id"] == "c-flat"
    assert events[0]["units"] == 10000             # long -> positive
    # STOPPED control event still emitted after flatten
    assert any(d.get("event_type") == "STOPPED" for d in inbound.delivered)


# --------------------------------------------------------------------------- #
# Startup-reconcile identity recovery + exit-reason mapping
# --------------------------------------------------------------------------- #
def test_startup_reconcile_recovers_identity_from_outbox(tmp_path):
    outbox_path = tmp_path / "fill_outbox.db"
    producer = FillProducer(FakeInbound(), "ams-inbound", build_outbox(outbox_path))
    producer.publish_fill(
        _approved_order(idempotency_key="k9", correlation_id="c9", granularity="H4"),
        _fill(broker_trade_id="T9", broker_order_id="o9"))
    broker_trade = {
        "id": "T9", "instrument": "EUR_USD", "price": "1.10002",
        "initialUnits": "10000", "currentUnits": "10000",
        "openTime": "2026-07-01T12:00:01.000000000Z",
        "clientExtensions": {"id": "sb-k9"},
        "stopLossOrder": {"price": "1.09900", "state": "PENDING"},
        "takeProfitOrder": {"price": "1.10300", "state": "PENDING"},
    }
    mt = managed_trade_from_broker_trade(broker_trade, outbox_path=outbox_path)
    assert mt is not None
    assert mt.broker_trade_id == "T9"
    assert mt.order_request_id == "k9"             # from clientExtensions sb-<key>
    assert mt.correlation_id == "c9"               # recovered from the durable outbox
    assert mt.granularity == "H4"
    assert mt.side == "BUY"
    assert mt.entry_price == pytest.approx(1.10002)
    assert mt.initial_stop_price == pytest.approx(1.09900)


def test_exit_reason_mapping_from_dependent_orders():
    assert exit_reason_from_trade_details(_closed_trade_details()) == "sl"
    tp = _closed_trade_details(stopLossOrder={"state": "CANCELLED"},
                               takeProfitOrder={"state": "FILLED"})
    assert exit_reason_from_trade_details(tp) == "tp"
    manual = _closed_trade_details(stopLossOrder={"state": "CANCELLED"},
                                   takeProfitOrder={"state": "CANCELLED"})
    assert exit_reason_from_trade_details(manual) == "other"


# --------------------------------------------------------------------------- #
# §6.1 shape vs System 3's REAL production validator (oracle, read-only)
# --------------------------------------------------------------------------- #
def _s3_contracts():
    if not (S3_SRC / "ams" / "common" / "contracts.py").exists():
        return None
    if str(S3_SRC) not in sys.path:
        sys.path.insert(0, str(S3_SRC))
    try:
        from ams.common import contracts  # noqa: PLC0415 - oracle import, read-only
        return contracts
    except Exception:
        return None


def test_close_events_pass_s3_production_validator():
    contracts = _s3_contracts()
    if contracts is None:
        pytest.skip(f"S3 repo not available at {S3_SRC}")
    details = _closed_trade_details()
    sweep_event = build_close_event(_short_trade(), facts_from_trade_details(details),
                                    exit_reason_from_trade_details(details))
    long_trade = build_managed_trade(
        broker_trade_id="T1", instrument="EUR_USD", side="BUY", entry_price=1.10,
        initial_stop_price=1.099, take_profit_price=1.103, open_time=T0,
        granularity="H1", correlation_id="c1", order_request_id="k1")
    resp_event = build_close_event(
        long_trade, facts_from_close_response(_trade_close_response()), "expiry")
    for event in (sweep_event, resp_event):
        # S3's own dependency-free validator: raises ContractError on any violation
        contracts.validate_and_check_fresh("FillEvent", event)
        # structural routing at S3's consumer: flat broker_order_id, no nested payload
        assert "broker_order_id" in event and "payload" not in event


# --------------------------------------------------------------------------- #
# 8) Backfill tool
# --------------------------------------------------------------------------- #
class FakeSecrets:
    def __init__(self, values: dict) -> None:
        self.values = values

    def get(self, key, default=None):
        return self.values.get(key, default)

    def get_int(self, key, default=None):
        v = self.values.get(key)
        return int(v) if v is not None else default

    def get_bool(self, key, default=False):
        v = self.values.get(key)
        return default if v is None else str(v).lower() in ("1", "true", "yes")

    def require(self, key):
        if key not in self.values:
            raise KeyError(key)
        return self.values[key]


def _load_backfill_tool():
    path = REPO_ROOT / "tools" / "backfill_closes.py"
    spec = importlib.util.spec_from_file_location("backfill_closes_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _seed_fill_outbox(tmp_path) -> Path:
    """Write FILLED envelopes with the REAL FillProducer/outbox machinery."""
    outbox_path = tmp_path / "fill_outbox.db"
    producer = FillProducer(FakeQueue(), "ams-inbound", build_outbox(outbox_path))
    producer.publish_fill(_approved_order(idempotency_key="k1", correlation_id="c1"),
                          _fill(broker_trade_id="T1", broker_order_id="2517"))
    producer.publish_fill(
        _approved_order(idempotency_key="k2", correlation_id="c2",
                        instrument="GBP_USD", side="SELL"),
        _fill(broker_trade_id="T2", broker_order_id="2543", fill_price=1.28000,
              stop_loss_price=1.28500, take_profit_price=1.27000))
    return outbox_path


def _backfill_transport() -> FakeTransport:
    return FakeTransport(trade_details={
        "T1": _closed_trade_details(id="T1", instrument="EUR_USD", initialUnits="10000",
                                    closingTransactionIDs=["3001"],
                                    closeTime="2026-07-15T18:00:00.000000000Z",
                                    realizedPL="21.5", averageClosePrice="1.10250"),
        "T2": _closed_trade_details(id="T2", instrument="GBP_USD", initialUnits="-8000",
                                    closingTransactionIDs=["3002"],
                                    closeTime="2026-07-16T02:00:00.000000000Z",
                                    realizedPL="-13.75", averageClosePrice="1.28500"),
    })


def _patch_backfill_env(monkeypatch, secrets):
    import system2.broker.oanda_transport as transport_mod
    import system2.common.secrets as secrets_mod

    monkeypatch.setattr(secrets_mod, "get_secrets", lambda: secrets)
    monkeypatch.setattr(transport_mod, "build_transport", lambda s: _backfill_transport())


def test_backfill_emits_only_the_missing_close(tmp_path, monkeypatch):
    outbox_path = _seed_fill_outbox(tmp_path)
    queue_path = tmp_path / "queue.db"
    ledger_path = tmp_path / "close_sweep.db"
    # T2's close was already emitted in a previous run (in the ledger)
    pre = SqliteProcessedStore(ledger_path)
    pre.mark("T2")
    pre.close()
    _patch_backfill_env(monkeypatch, FakeSecrets({
        "QUEUE_PROVIDER": "local", "QUEUE_LOCAL_PATH": str(queue_path),
        "FILL_OUTBOX_PATH": str(outbox_path),
        "CLOSE_LEDGER_PATH": str(ledger_path),
        "S3_CLOSE_TOPIC": S3_TOPIC,
    }))

    tool = _load_backfill_tool()
    assert tool.main(["--since", "2026-07-01T00:00:00Z"]) == 0

    delivered = LocalDurableBackend(queue_path)
    msgs = delivered.pull(S3_TOPIC, max_messages=10)
    assert len(msgs) == 1                          # exactly the MISSING close, nothing else
    event = msgs[0].body
    assert set(event) <= ALLOWED_EVENT_FIELDS
    assert event["status"] == "closed"
    assert event["broker_order_id"] == "3001"      # T1's close txn
    assert event["order_request_id"] == "k1"
    assert event["signal_id"] == "c1"
    assert event["realized_pnl"] == pytest.approx(21.5)
    ledger = SqliteProcessedStore(ledger_path)
    assert ledger.seen("T1") and ledger.seen("T2")


def test_backfill_dry_run_emits_nothing_and_writes_no_ledger(tmp_path, monkeypatch, capsys):
    outbox_path = _seed_fill_outbox(tmp_path)
    queue_path = tmp_path / "queue.db"
    ledger_path = tmp_path / "close_sweep.db"
    _patch_backfill_env(monkeypatch, FakeSecrets({
        "QUEUE_PROVIDER": "local", "QUEUE_LOCAL_PATH": str(queue_path),
        "FILL_OUTBOX_PATH": str(outbox_path),
        "CLOSE_LEDGER_PATH": str(ledger_path),
        "S3_CLOSE_TOPIC": S3_TOPIC,
    }))

    tool = _load_backfill_tool()
    assert tool.main(["--since", "2026-07-01T00:00:00Z", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "dry-run: would emit 2" in out          # both closes reported...
    assert '"status": "closed"' in out             # ...as contract-shaped JSON
    assert not queue_path.exists()                 # nothing published anywhere
    assert not ledger_path.exists()                # no ledger writes


# --------------------------------------------------------------------------- #
# EXEC-013 — exit slippage on close events (signed, vs the FILLED SL/TP order)
# --------------------------------------------------------------------------- #
def test_exit_slippage_sl_close_usd_pair_signed():
    # SL placed at 1.14200, actually closed at 1.14266 -> (fill - expected)/0.0001 = +6.6
    details = _closed_trade_details(
        stopLossOrder={"id": "2520", "state": "FILLED", "price": "1.14200"})
    facts = facts_from_trade_details(details)
    assert facts.slippage_pips == pytest.approx(6.6)
    event = build_close_event(_short_trade(), facts, "sl")
    assert event["slippage_pips"] == pytest.approx(6.6)
    assert set(event) <= ALLOWED_EVENT_FIELDS


def test_exit_slippage_jpy_pip_size():
    details = _closed_trade_details(
        instrument="USD_JPY", averageClosePrice="148.62",
        stopLossOrder={"id": "9", "state": "FILLED", "price": "148.50"})
    facts = facts_from_trade_details(details)
    assert facts.slippage_pips == pytest.approx(12.0)  # pip size 0.01 for JPY quotes


def test_exit_slippage_tp_close_uses_tp_order():
    details = _closed_trade_details(
        stopLossOrder={"id": "1", "state": "CANCELLED", "price": "1.13000"},
        takeProfitOrder={"id": "2", "state": "FILLED", "price": "1.14300"})
    facts = facts_from_trade_details(details)
    assert facts.slippage_pips == pytest.approx((1.14266 - 1.14300) / 0.0001)


def test_exit_slippage_none_for_manual_close_and_event_omits_key():
    details = _closed_trade_details(
        stopLossOrder={"id": "1", "state": "CANCELLED", "price": "1.13000"},
        takeProfitOrder={"id": "2", "state": "CANCELLED", "price": "1.15000"})
    facts = facts_from_trade_details(details)
    assert facts.slippage_pips is None
    event = build_close_event(_short_trade(), facts, "manual")
    assert "slippage_pips" not in event
    # adapter close_trade responses (flatten/time exits) carry no expected price either
    resp_facts = facts_from_close_response(_trade_close_response())
    assert resp_facts.slippage_pips is None


def test_exit_slippage_malformed_price_fails_open():
    details = _closed_trade_details(
        stopLossOrder={"id": "1", "state": "FILLED", "price": "not-a-price"})
    assert facts_from_trade_details(details).slippage_pips is None


# --------------------------------------------------------------------------- #
# 9) Transaction-stream fallback — the 2026-07-21 NO_SUCH_TRADE incident
#
# OANDA 404'd /trades/2580 for a trade it had just opened and stopped out (absent from
# /trades in EVERY state), while the transaction stream carried the full close. Payloads
# below are the REAL ones pulled from account 101-002-38449021-001.
# --------------------------------------------------------------------------- #
class NoSuchTrade(Exception):
    """Mirrors the V20Error the transport raises for a 404 NO_SUCH_TRADE."""


NO_SUCH_TRADE_ERR = NoSuchTrade(
    '{"lastTransactionID":"2584","errorMessage":"The trade ID specified does not exist",'
    '"errorCode":"NO_SUCH_TRADE"}')


def _txn_2580_stream() -> list[dict]:
    """Transactions 2579-2584 exactly as OANDA returned them for the incident trade."""
    return [
        {"id": "2579", "type": "MARKET_ORDER", "instrument": "EUR_USD", "units": "116992",
         "reason": "CLIENT_ORDER", "time": "2026-07-21T15:41:24.086239000Z"},
        {"id": "2580", "type": "ORDER_FILL", "instrument": "EUR_USD", "units": "116992",
         "price": "1.14085", "reason": "MARKET_ORDER", "orderID": "2579", "pl": "0.0000",
         "accountBalance": "84234.7938", "time": "2026-07-21T15:41:24.086239000Z",
         "clientOrderID": "sb-2a3670bc-1122-4f58-bf9e-dd647f1b3815",
         "tradeOpened": {"tradeID": "2580", "units": "116992", "price": "1.14085"}},
        {"id": "2581", "type": "TAKE_PROFIT_ORDER", "price": "1.14445", "reason": "ON_FILL",
         "tradeID": "2580", "time": "2026-07-21T15:41:24.086239000Z"},
        {"id": "2582", "type": "STOP_LOSS_ORDER", "price": "1.13965", "reason": "ON_FILL",
         "tradeID": "2580", "time": "2026-07-21T15:41:24.086239000Z"},
        {"id": "2583", "type": "ORDER_FILL", "instrument": "EUR_USD", "units": "-116992",
         "price": "1.13964", "reason": "STOP_LOSS_ORDER", "orderID": "2582",
         "pl": "-201.7304", "accountBalance": "84033.0634",
         "time": "2026-07-21T20:57:50.123456789Z",
         "tradesClosed": [{"tradeID": "2580",
                           "clientTradeID": "sb-2a3670bc-1122-4f58-bf9e-dd647f1b3815",
                           "units": "-116992", "price": "1.13964",
                           "realizedPL": "-201.7304"}]},
        {"id": "2584", "type": "ORDER_CANCEL", "reason": "LINKED_TRADE_CLOSED",
         "orderID": "2581", "time": "2026-07-21T20:57:50.123456789Z"},
    ]


def _trade_2580() -> ManagedTrade:
    return build_managed_trade(
        broker_trade_id="2580", instrument="EUR_USD", side="BUY", entry_price=1.14085,
        initial_stop_price=1.13965, take_profit_price=1.14445,
        open_time="2026-07-21T15:41:24.086239000Z", granularity="H1",
        correlation_id="cfd5c544-e320-4d18-a1aa-0d62a55ee2c7",
        order_request_id="2a3670bc-1122-4f58-bf9e-dd647f1b3815")


def _txn_transport(**over) -> FakeTransport:
    base = dict(open_trades=[], trade_details={}, transactions=_txn_2580_stream(),
                last_txn_id=2584, trade_error=NO_SUCH_TRADE_ERR)
    base.update(over)
    return FakeTransport(**base)


def test_find_close_transaction_locates_the_closing_fill():
    located = find_close_transaction(_txn_transport(), "2580")
    assert located is not None
    txn, closed, index = located
    assert txn["id"] == "2583" and txn["reason"] == "STOP_LOSS_ORDER"
    assert closed["realizedPL"] == "-201.7304"
    assert "2582" in index  # the SL order txn is indexed for the slippage lookup


def test_facts_from_transaction_matches_the_broker_numbers():
    txn, closed, index = find_close_transaction(_txn_transport(), "2580")
    facts = facts_from_transaction(txn, closed, instrument="EUR_USD", txn_by_id=index)
    assert facts.close_txn_id == "2583"          # close txn id — the S3 dedup key
    assert facts.realized_pnl == pytest.approx(-201.7304)
    assert facts.close_price == pytest.approx(1.13964)
    assert facts.units == pytest.approx(116992)  # magnitude; sign comes from the trade
    assert facts.close_time == "2026-07-21T20:57:50.123456789Z"
    # EXEC-013: SL order 2582 was at 1.13965, filled 1.13964 -> -0.1 pip
    assert facts.slippage_pips == pytest.approx(-0.1)
    assert exit_reason_from_transaction(txn) == "sl"


def test_transaction_exit_reason_mapping():
    assert exit_reason_from_transaction({"reason": "TAKE_PROFIT_ORDER"}) == "tp"
    assert exit_reason_from_transaction({"reason": "STOP_LOSS_ORDER"}) == "sl"
    assert exit_reason_from_transaction({"reason": "TRAILING_STOP_LOSS_ORDER"}) == "sl"
    assert exit_reason_from_transaction({"reason": "MARKET_ORDER_TRADE_CLOSE"}) == "manual"
    assert exit_reason_from_transaction({"reason": "MARKET_ORDER_MARGIN_CLOSEOUT"}) == "flatten"
    assert exit_reason_from_transaction({"reason": "SOMETHING_NEW"}) == "other"
    for reason in ("STOP_LOSS_ORDER", "MARKET_ORDER_TRADE_CLOSE", "SOMETHING_NEW"):
        assert exit_reason_from_transaction({"reason": reason}) in VALID_EXIT_REASONS


def test_manual_close_via_transactions_has_no_slippage():
    """A market close carries no expected price, so slippage must stay absent."""
    stream = _txn_2580_stream()
    stream[4] = dict(stream[4], reason="MARKET_ORDER_TRADE_CLOSE", orderID="9999")
    txn, closed, index = find_close_transaction(_txn_transport(transactions=stream), "2580")
    facts = facts_from_transaction(txn, closed, instrument="EUR_USD", txn_by_id=index)
    assert facts.slippage_pips is None
    assert "slippage_pips" not in build_close_event(_trade_2580(), facts, "manual")


def test_sweep_recovers_close_when_get_trade_404s(tmp_path):
    """THE regression test: /trades 404s, the close still reaches System 3."""
    rig = _sweep_rig(tmp_path, trade=_trade_2580())
    rig["sweeper"].transport = _txn_transport()

    result = rig["sweeper"].sweep()

    assert result["closed"] == 1 and result["via_transactions"] == 1
    events = rig["queue"].events()
    assert len(events) == 1
    event = events[0]
    assert set(event) <= ALLOWED_EVENT_FIELDS          # S3 rejects unknown fields
    assert event["status"] == "closed"
    assert event["broker_order_id"] == "2583"          # the close txn id
    assert event["order_request_id"] == "2a3670bc-1122-4f58-bf9e-dd647f1b3815"
    assert event["signal_id"] == "cfd5c544-e320-4d18-a1aa-0d62a55ee2c7"
    assert event["exit_reason"] == "sl"
    assert event["realized_pnl"] == pytest.approx(-201.7304)
    assert event["units"] == 116992                    # long -> positive
    assert event["pair"] == "EUR_USD"
    assert rig["trade"].closed is True


def test_fallback_not_used_when_get_trade_works(tmp_path):
    """Cheap path stays the default: no transaction scan when /trades answers."""
    rig = _sweep_rig(tmp_path)
    rig["sweeper"].sweep()
    assert rig["transport"].idrange_calls == 0
    assert rig["queue"].events()[0]["broker_order_id"] == "2560"  # from closingTransactionIDs


def test_open_trade_never_triggers_a_transaction_scan(tmp_path):
    """A trade the broker still reports OPEN is open-list lag, not a missing close."""
    rig = _sweep_rig(tmp_path, details=_closed_trade_details(state="OPEN"))
    transport = _txn_transport(trade_details={"2518": _closed_trade_details(state="OPEN")},
                              trade_error=None)
    rig["sweeper"].transport = transport
    assert rig["sweeper"].sweep()["closed"] == 0
    assert transport.idrange_calls == 0
    assert rig["queue"].events() == []


def test_transaction_fallback_is_backed_off_per_trade(tmp_path):
    """An unresolvable trade must not rescan the stream every 30s forever."""
    rig = _sweep_rig(tmp_path, trade=_trade_2580())
    transport = _txn_transport(transactions=[])  # nothing to find -> stays unresolved
    rig["sweeper"].transport = transport
    clk = rig["clk"]

    rig["sweeper"].sweep()
    assert transport.idrange_calls == 1    # first miss scans the stream once
    first_get_trade = transport.get_trade_calls

    clk[0] = T0 + timedelta(seconds=31)    # past the sweep throttle, inside the fallback backoff
    rig["sweeper"].sweep()
    assert transport.get_trade_calls > first_get_trade  # the sweep itself still runs
    assert transport.idrange_calls == 1                 # ...but the stream is NOT rescanned

    clk[0] = T0 + timedelta(seconds=400)   # past the 300s fallback backoff
    rig["sweeper"].sweep()
    assert transport.idrange_calls == 2                 # one retry, not thirteen
    assert rig["queue"].events() == []      # still unresolved, still no phantom close


def test_fallback_and_trade_details_produce_the_same_dedup_key(tmp_path):
    """Both sources must yield the same close txn id, or a close could double-book."""
    details = _closed_trade_details(
        id="2580", instrument="EUR_USD", initialUnits="116992",
        closingTransactionIDs=["2583"], realizedPL="-201.7304",
        averageClosePrice="1.13964", closeTime="2026-07-21T20:57:50.123456789Z",
        stopLossOrder={"id": "2582", "state": "FILLED", "price": "1.13965"},
        takeProfitOrder={"id": "2581", "state": "CANCELLED", "price": "1.14445"})
    from_details = facts_from_trade_details(details)
    txn, closed, index = find_close_transaction(_txn_transport(), "2580")
    from_txn = facts_from_transaction(txn, closed, instrument="EUR_USD", txn_by_id=index)

    assert from_details.close_txn_id == from_txn.close_txn_id == "2583"
    assert from_details.realized_pnl == pytest.approx(from_txn.realized_pnl)
    assert from_details.close_price == pytest.approx(from_txn.close_price)
    assert from_details.slippage_pips == pytest.approx(from_txn.slippage_pips)


def test_backfill_recovers_the_close_when_get_trade_404s(tmp_path, monkeypatch, capsys):
    """The documented recovery path must work for the very failure it exists to repair."""
    import system2.broker.oanda_transport as transport_mod
    import system2.common.secrets as secrets_mod

    outbox_path = tmp_path / "fill_outbox.db"
    producer = FillProducer(FakeQueue(), "ams-inbound", build_outbox(outbox_path))
    producer.publish_fill(
        _approved_order(idempotency_key="2a3670bc-1122-4f58-bf9e-dd647f1b3815",
                        correlation_id="cfd5c544-e320-4d18-a1aa-0d62a55ee2c7",
                        instrument="EUR_USD", side="BUY", units=116992),
        _fill(broker_trade_id="2580", broker_order_id="2579", fill_price=1.14085,
              stop_loss_price=1.13965, take_profit_price=1.14445))

    queue_path = tmp_path / "queue.db"
    ledger_path = tmp_path / "close_sweep.db"
    secrets = FakeSecrets({
        "QUEUE_PROVIDER": "local", "QUEUE_LOCAL_PATH": str(queue_path),
        "FILL_OUTBOX_PATH": str(outbox_path), "CLOSE_LEDGER_PATH": str(ledger_path),
        "S3_CLOSE_TOPIC": S3_TOPIC,
    })
    monkeypatch.setattr(secrets_mod, "get_secrets", lambda: secrets)
    monkeypatch.setattr(transport_mod, "build_transport", lambda s: _txn_transport())

    tool = _load_backfill_tool()
    assert tool.main(["--since", "2026-07-21T00:00:00Z"]) == 0

    out = capsys.readouterr().out
    assert "[fallback]" in out and "transaction stream" in out
    assert "via=transactions" in out
    msgs = LocalDurableBackend(queue_path).pull(S3_TOPIC, max_messages=10)
    assert len(msgs) == 1
    event = msgs[0].body
    assert set(event) <= ALLOWED_EVENT_FIELDS
    assert event["broker_order_id"] == "2583"
    assert event["exit_reason"] == "sl"
    assert event["realized_pnl"] == pytest.approx(-201.7304)
    assert SqliteProcessedStore(ledger_path).seen("2580")   # rerunning is now a no-op


def test_transaction_path_close_event_passes_s3_production_validator():
    """The fallback's event crosses the same contract boundary as the normal path."""
    contracts = _s3_contracts()
    if contracts is None:
        pytest.skip(f"S3 repo not available at {S3_SRC}")
    txn, closed, index = find_close_transaction(_txn_transport(), "2580")
    facts = facts_from_transaction(txn, closed, instrument="EUR_USD", txn_by_id=index)
    event = build_close_event(_trade_2580(), facts, exit_reason_from_transaction(txn))
    contracts.validate_and_check_fresh("FillEvent", event)
    assert "broker_order_id" in event and "payload" not in event


def test_resolve_close_from_transactions_fails_open_on_broker_error():
    class Boom(FakeTransport):
        def get_transactions_idrange(self, from_id, to_id):
            raise ConnectionError("OANDA 503")

    assert resolve_close_from_transactions(
        Boom(last_txn_id=2584), "2580", instrument="EUR_USD") is None


def test_non_numeric_trade_id_skips_the_scan_cleanly():
    """Fake/other-broker ids have no transaction range — must not explode."""
    assert find_close_transaction(_txn_transport(), "T1") is None


def test_close_event_with_slippage_passes_s3_validator():
    contracts = _s3_contracts()
    if contracts is None:
        pytest.skip(f"S3 repo not available at {S3_SRC}")
    details = _closed_trade_details(
        stopLossOrder={"id": "2520", "state": "FILLED", "price": "1.14200"})
    event = build_close_event(_short_trade(), facts_from_trade_details(details), "sl")
    assert "slippage_pips" in event
    contracts.validate_and_check_fresh("FillEvent", event)
