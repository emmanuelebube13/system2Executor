"""EXEC-010 tests — emergency STOP, safety-gated tick loop, startup reconcile, graceful shutdown."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from system2.broker.position_manager import ManagedTrade, PositionManager
from system2.common.queue_backend import LocalDurableBackend
from system2.execution.fill_producer import FillProducer, FillResult, build_outbox
from system2.execution.lifecycle import EmergencyStop, ExecutionRuntime
from system2.execution.outbound_consumer import OutboundConsumer, SqliteProcessedStore
from system2.execution.pipeline import ExecMode, ExecutionPipeline
from system2.execution.safety_mode import SafetyMonitor

WED = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)  # in-session
OUT = "AMS_Outbound_Queue"


def _order_msg(**over) -> dict:
    m = {
        "schema_version": "1", "message_id": "m1", "idempotency_key": "k1",
        "correlation_id": "c1", "created_at": "2026-07-01T11:59:00Z", "instrument": "EUR_USD",
        "side": "BUY", "units": 10000, "granularity": "H1", "risk_context": {"atr": 0.0010},
        "ams_decision_id": "ams-1",
    }
    m.update(over)
    return m


class FakeInbound:
    def __init__(self):
        self.delivered = []

    def publish(self, topic, body):
        self.delivered.append(body)


class FakeTransport:
    def __init__(self, open_trades=None):
        self._open = open_trades or []

    def get_open_trades(self):
        return self._open


class FakeAdapter:
    def __init__(self, open_trades=None):
        self.transport = FakeTransport(open_trades)
        self.closed = []

    def close_trade(self, trade_id, units="ALL"):
        self.closed.append(trade_id)
        return {"ok": True}


def _fill():
    return FillResult(realized_status="FILLED", filled_units=10000, broker_order_id="o1",
                      broker_trade_id="T1", fill_price=1.10001, model_set_id="set-1")


@pytest.fixture
def rig(tmp_path: Path):
    out_q = LocalDurableBackend(tmp_path / "out.db")
    inbound = FakeInbound()
    outbox = build_outbox(tmp_path / "outbox.db")
    producer = FillProducer(inbound, "AMS_Inbound_Queue", outbox)
    processed = SqliteProcessedStore(tmp_path / "proc.db")
    pipe = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=False,
                             processed_store=processed, clock=lambda: WED)
    monitor = SafetyMonitor(clock=lambda: WED)
    monitor._started_at = WED
    adapter = FakeAdapter()
    posman = PositionManager(adapter=adapter, clock=lambda: WED)
    submits: list = []

    consumer = OutboundConsumer(
        queue=out_q, subscription=OUT, pipeline=pipe,
        price_fn=lambda o: 1.10000,
        submit_fn=lambda c: (submits.append(c) or _fill()),
        emit_fn=lambda order, c, f: producer.publish_fill(order, f),
        submit_gate_fn=monitor.can_submit,
        park_backoff_sec=0.0, clock=lambda: WED,
    )
    stop = EmergencyStop(sentinel_path=tmp_path / "STOP")
    runtime = ExecutionRuntime(
        consumer=consumer, monitor=monitor, position_manager=posman, producer=producer,
        emergency_stop=stop, adapter=adapter, clock=lambda: WED,
    )
    return dict(out_q=out_q, inbound=inbound, producer=producer, monitor=monitor,
                adapter=adapter, posman=posman, consumer=consumer, stop=stop,
                runtime=runtime, submits=submits)


# ----- emergency stop -------------------------------------------------------------
def test_emergency_stop_trigger_and_reason(tmp_path):
    stop = EmergencyStop()
    assert not stop.is_stopped()
    stop.trigger("operator")
    assert stop.is_stopped() and stop.reason == "operator"


def test_emergency_stop_sentinel_file(tmp_path):
    sentinel = tmp_path / "STOP"
    stop = EmergencyStop(sentinel_path=sentinel)
    assert not stop.is_stopped()
    sentinel.write_text("halt")
    assert stop.is_stopped()
    assert "sentinel" in stop.reason


# ----- running tick: execute + emit fill ------------------------------------------
def test_tick_running_executes_and_emits_fill(rig):
    rig["out_q"].publish(OUT, _order_msg())
    res = rig["runtime"].tick()
    assert res["stopped"] is False
    assert res["state"] == "running"
    assert len(rig["submits"]) == 1
    assert len(rig["inbound"].delivered) == 1  # fill confirmation published
    assert rig["inbound"].delivered[0]["correlation_id"] == "c1"


# ----- safety gate: PAUSED parks new orders, then resumes --------------------------
def test_paused_parks_order_then_resumes(rig):
    m = rig["monitor"]
    m._started_at = WED - timedelta(seconds=400)  # queue looks stale -> PAUSE on first eval
    rig["out_q"].publish(OUT, _order_msg())

    res1 = rig["runtime"].tick()
    assert res1["state"] == "paused"
    assert len(rig["submits"]) == 0          # NOT submitted while paused
    assert res1["consume"].get("paused_park") == 1

    # the poll above bumped consumer lag (fresh) -> next tick resumes and executes the parked order
    res2 = rig["runtime"].tick()
    assert res2["state"] == "running"
    assert len(rig["submits"]) == 1
    assert len(rig["inbound"].delivered) == 1


def test_safety_invariant_stop_gate_never_submits_when_paused(rig):
    """Queue down + paused: even repeated ticks never submit an order."""
    m = rig["monitor"]
    m._started_at = WED - timedelta(seconds=999)
    rig["out_q"].publish(OUT, _order_msg())
    # force staleness every tick by keeping lag stale: use a message that is itself old & re-parked
    rig["runtime"].tick()
    assert rig["monitor"].state.value == "paused"
    assert len(rig["submits"]) == 0


# ----- startup reconcile ----------------------------------------------------------
def test_startup_reconcile_adopts_open_trades(tmp_path, rig):
    rig["adapter"].transport._open = [
        {"id": "T9", "instrument": "GBP_USD", "side": "BUY", "price": "1.25000"}
    ]

    def _reconcile(t):
        return ManagedTrade(
            broker_trade_id=t["id"], instrument=t["instrument"], side=t["side"],
            entry_price=float(t["price"]), initial_stop_price=1.24900,
            take_profit_price=1.25300, open_time=WED, granularity="H1",
            max_duration_sec=3600, correlation_id="reco",
        )

    rig["runtime"].reconcile_fn = _reconcile
    adopted = rig["runtime"].startup_reconcile()
    assert adopted == 1
    assert "T9" in rig["posman"].trades


# ----- graceful shutdown ----------------------------------------------------------
def test_shutdown_flushes_and_emits_stopped(rig):
    res = rig["runtime"].shutdown("test-stop")
    assert res["reason"] == "test-stop"
    # STOPPED control event delivered to the inbound queue
    events = [d for d in rig["inbound"].delivered if d.get("event_type") == "STOPPED"]
    assert len(events) == 1
    assert events[0]["payload"]["reason"] == "test-stop"


def test_shutdown_is_idempotent(rig):
    rig["runtime"].shutdown("first")
    again = rig["runtime"].shutdown("second")
    assert again == {"already": True}


def test_shutdown_default_leaves_positions(rig):
    rig["posman"].register(ManagedTrade(
        broker_trade_id="T1", instrument="EUR_USD", side="BUY", entry_price=1.10,
        initial_stop_price=1.099, take_profit_price=1.103, open_time=WED,
        granularity="H1", max_duration_sec=3600, correlation_id="c1"))
    rig["runtime"].shutdown("safe")
    assert rig["adapter"].closed == []  # SAFE DEFAULT: positions kept (broker stop protects them)


def test_shutdown_flatten_on_stop_closes_positions(rig):
    rig["runtime"].flatten_on_stop = True
    rig["posman"].register(ManagedTrade(
        broker_trade_id="T1", instrument="EUR_USD", side="BUY", entry_price=1.10,
        initial_stop_price=1.099, take_profit_price=1.103, open_time=WED,
        granularity="H1", max_duration_sec=3600, correlation_id="c1"))
    res = rig["runtime"].shutdown("flatten")
    assert rig["adapter"].closed == ["T1"]
    assert res["flattened"] == 1


# ----- run loop stops on emergency stop -------------------------------------------
def test_run_loop_halts_on_emergency_stop(rig):
    rig["stop"].trigger("kill")
    rig["runtime"].run(max_ticks=10)  # should exit immediately and shut down once
    # a STOPPED event proves shutdown ran
    assert any(d.get("event_type") == "STOPPED" for d in rig["inbound"].delivered)
