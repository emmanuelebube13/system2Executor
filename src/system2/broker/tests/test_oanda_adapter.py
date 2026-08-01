"""EXEC-006 tests — pip/slippage/idempotency math, fill validation, stop confirmation, toggle."""

from __future__ import annotations

import pytest

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from system2.broker.oanda_adapter import (
    BrokerEnvironment,
    InstrumentSpec,
    LiveConfigError,
    MarketClosedError,
    OandaAdapter,
    ReconcileUnavailableError,
    TransientBrokerError,
    client_order_id,
    compute_slippage_pips,
    default_display_precision,
    format_price,
    pip_size,
    prior_submission_verdict,
    quantize_price,
    reference_price_from_broker_row,
    resolve_environment,
    transaction_client_order_id,
)
from system2.common.secrets import Secrets
from system2.execution.pipeline import ConstructedOrder, InvalidOrderError

PRACTICE = BrokerEnvironment("practice", "tok", "101-002-1234-001", "https://api-fxpractice.oanda.com")


def _order(**over) -> ConstructedOrder:
    base = dict(
        idempotency_key="k1", correlation_id="c1", instrument="EUR_USD", side="BUY",
        units=10000, entry_price=1.10000, stop_loss=1.09900, take_profit=1.10300,
        atr_value=0.0010, rr_ratio=3.0,
    )
    base.update(over)
    return ConstructedOrder(**base)


class FakeTransport:
    """Configurable OandaTransport double."""

    def __init__(self, *, fill=None, cancel=None, raise_on_create=None, open_trades=None, trade=None,
                 modify_raises=False, specs=None, prices=None, pricing_raises=False):
        self._fill = fill
        self._cancel = cancel
        self._raise_seq = list(raise_on_create or [])
        self._open_trades = open_trades or []
        self._trade = trade
        self._modify_raises = modify_raises
        self._specs = specs  # None -> no spec capability at all
        self._prices = prices
        self._pricing_raises = pricing_raises
        self.created = []
        self.modified = []
        self.modify_instruments = []
        self.closed = []
        self.spec_calls = 0

    def create_order(self, payload):
        self.created.append(payload)
        if self._raise_seq:
            exc = self._raise_seq.pop(0)
            if exc is not None:
                raise exc
        resp = {}
        if self._fill is not None:
            resp["orderFillTransaction"] = self._fill
        if self._cancel is not None:
            resp["orderCancelTransaction"] = self._cancel
        return resp

    def get_open_trades(self):
        return self._open_trades

    def get_trade(self, trade_id):
        return self._trade

    def modify_trade_stop(self, trade_id, stop_price, instrument=None):
        if self._modify_raises:
            from system2.broker.oanda_adapter import BrokerError
            raise BrokerError("cannot attach")
        self.modified.append((trade_id, stop_price))
        self.modify_instruments.append(instrument)
        return {"ok": True}

    def close_trade(self, trade_id, units="ALL"):
        self.closed.append((trade_id, units))
        return {"ok": True}


class FakePricingTransport(FakeTransport):
    """``FakeTransport`` + the optional read-only market-data half of the seam.

    Kept as a subclass on purpose: the base double deliberately lacks ``get_pricing`` /
    ``get_account_instruments`` so the adapter's feature-detection + fallback path is
    exercised by every other test in this module.
    """

    def get_pricing(self, instruments):
        self.spec_calls += 0
        if self._pricing_raises:
            raise TransientBrokerError("pricing unavailable")
        return list(self._prices or [])

    def get_account_instruments(self, instruments=None):
        self.spec_calls += 1
        return list(self._specs or [])


def _adapter(transport, **kw):
    return OandaAdapter(transport, environment=PRACTICE, slippage_tolerance_pips=2.0,
                        retry_max=3, backoff_sec=0.0, sleep_fn=lambda s: None, **kw)


# ----- pure math ------------------------------------------------------------------
def test_pip_size_majors_vs_jpy():
    assert pip_size("EUR_USD") == 0.0001
    assert pip_size("USD_JPY") == 0.01


def test_slippage_sign_and_magnitude():
    assert compute_slippage_pips(1.10000, 1.10002, "EUR_USD") == pytest.approx(0.2)
    assert compute_slippage_pips(1.10000, 1.09997, "EUR_USD") == pytest.approx(-0.3)
    assert compute_slippage_pips(156.00, 156.03, "USD_JPY") == pytest.approx(3.0)


def test_client_id_derivation():
    assert client_order_id("abc") == "sb-abc"


# ----- F-308: per-instrument price precision --------------------------------------
NOW = datetime(2026, 7, 31, 12, 0, 0, tzinfo=timezone.utc)

# The live practice ``/v3/accounts/{id}/instruments`` spec for the deployed allowlist,
# as captured by audit/harness/teamC_oanda_practice_readonly.py on 2026-07-29.
PRACTICE_SPECS = [
    {"name": "EUR_USD", "displayPrecision": 5, "pipLocation": -4},
    {"name": "GBP_USD", "displayPrecision": 5, "pipLocation": -4},
    {"name": "AUD_USD", "displayPrecision": 5, "pipLocation": -4},
    {"name": "USD_CAD", "displayPrecision": 5, "pipLocation": -4},
    {"name": "USD_JPY", "displayPrecision": 3, "pipLocation": -2},
]


def test_default_display_precision_matches_the_live_practice_spec():
    """JPY quotes carry 3 decimals, the rest 5 — the hardcoded .5f was wrong for USD_JPY."""
    for row in PRACTICE_SPECS:
        assert default_display_precision(row["name"]) == row["displayPrecision"]


def test_format_price_uses_instrument_precision_not_five_decimals():
    # the F-308 example: an ATR-derived JPY stop rounded to 6 decimals upstream
    assert format_price(153.427183, "USD_JPY") == "153.427"
    assert len(format_price(153.427183, "USD_JPY").split(".")[1]) == 3
    assert format_price(1.0994567, "EUR_USD") == "1.09946"
    assert format_price(1.0995, "EUR_USD") == "1.09950"  # trailing zeros preserved


def test_quantize_price_is_decimal_exact_not_binary_float():
    assert quantize_price(0.1 + 0.2, "EUR_USD") == Decimal("0.30000")
    assert isinstance(quantize_price(1.1, "EUR_USD"), Decimal)


def test_quantization_never_widens_a_protective_price():
    """``toward`` = entry, so the tick-snap can only tighten. Money must not drift outward."""
    # BUY: stop below entry -> rounds UP (toward entry) -> risk cannot grow
    assert quantize_price(153.427183, "USD_JPY", 3, toward=153.5) == Decimal("153.428")
    # BUY: target above entry -> rounds DOWN (toward entry) -> reward cannot be overstated
    assert quantize_price(153.918451, "USD_JPY", 3, toward=153.5) == Decimal("153.918")
    # SELL: stop above entry -> rounds DOWN
    assert quantize_price(153.427999, "USD_JPY", 3, toward=153.0) == Decimal("153.427")


def test_instrument_spec_from_broker_overrides_the_default():
    t = FakePricingTransport(specs=[{"name": "EUR_USD", "displayPrecision": 4, "pipLocation": -4}])
    a = _adapter(t)
    assert a.display_precision("EUR_USD") == 4
    assert a.instrument_spec("EUR_USD") == InstrumentSpec("EUR_USD", 4, -4)
    a.display_precision("EUR_USD")
    assert t.spec_calls == 1  # cached for the process


def test_display_precision_falls_back_when_the_transport_cannot_speak_specs():
    assert _adapter(FakeTransport()).display_precision("USD_JPY") == 3
    assert _adapter(FakeTransport()).display_precision("EUR_USD") == 5


def test_usdjpy_payload_prices_render_three_decimals():
    """The F-308 defect, at the payload boundary: ``"153.42718"`` had 2 illegal decimals."""
    t = FakePricingTransport(specs=PRACTICE_SPECS)
    order = _order(instrument="USD_JPY", entry_price=153.500000,
                   stop_loss=153.427183, take_profit=153.918451, atr_value=0.072817)
    payload = _adapter(t)._build_payload(order)["order"]
    assert payload["stopLossOnFill"]["price"] == "153.428"
    assert payload["takeProfitOnFill"]["price"] == "153.918"
    for leg in ("stopLossOnFill", "takeProfitOnFill"):
        assert len(payload[leg]["price"].split(".")[1]) == 3


def test_non_jpy_payload_prices_still_render_five_decimals():
    t = FakePricingTransport(specs=PRACTICE_SPECS)
    payload = _adapter(t)._build_payload(_order())["order"]
    assert payload["stopLossOnFill"]["price"] == "1.09900"
    assert payload["takeProfitOnFill"]["price"] == "1.10300"


def test_payload_refuses_a_non_positive_stop_even_if_one_reaches_the_adapter():
    """Defence in depth: ``build_order`` already refuses, but the wire is the last line."""
    bad = _order(entry_price=0.0016, stop_loss=0.0, take_profit=0.0064)
    with pytest.raises(InvalidOrderError):
        _adapter(FakeTransport())._build_payload(bad)


def test_modify_stop_routes_the_instrument_and_refuses_a_non_positive_price():
    t = FakeTransport()
    a = _adapter(t)
    a.modify_stop("T1", 153.427, "USD_JPY")
    assert t.modified == [("T1", 153.427)] and t.modify_instruments == ["USD_JPY"]
    with pytest.raises(InvalidOrderError):
        a.modify_stop("T1", 0.0, "USD_JPY")


# ----- F-306: a real market reference price ---------------------------------------
def _quote(instrument="EUR_USD", bid="1.09990", ask="1.10010", time=None, **over):
    row = {
        "instrument": instrument, "type": "PRICE", "status": "tradeable", "tradeable": True,
        "time": time if time is not None else NOW.isoformat().replace("+00:00", "Z"),
        "bids": [{"price": bid, "liquidity": 10000000}],
        "asks": [{"price": ask, "liquidity": 10000000}],
    }
    row.update(over)
    return row


def _price_adapter(t):
    return _adapter(t, clock=lambda: NOW, price_max_age_sec=60.0)


def test_reference_price_crosses_the_right_side_of_the_book():
    a = _price_adapter(FakePricingTransport(prices=[_quote()]))
    assert a.reference_price("EUR_USD", "BUY") == 1.10010   # pays the ask
    assert a.reference_price("EUR_USD", "SELL") == 1.09990  # hits the bid


def test_reference_price_parses_oanda_nanosecond_timestamps():
    a = _price_adapter(FakePricingTransport(prices=[_quote(time="2026-07-31T12:00:00.123456789Z")]))
    assert a.reference_price("EUR_USD", "BUY") == 1.10010


def test_stale_quote_yields_no_price():
    old = (NOW - timedelta(seconds=61)).isoformat().replace("+00:00", "Z")
    a = _price_adapter(FakePricingTransport(prices=[_quote(time=old)]))
    assert a.reference_price("EUR_USD", "BUY") is None


def test_halted_or_untradeable_instrument_yields_no_price():
    for row in (_quote(status="non-tradeable"), _quote(tradeable=False), _quote(bids=[])):
        assert _price_adapter(FakePricingTransport(prices=[row])).reference_price("EUR_USD", "BUY") is None


def test_pricing_failure_yields_no_price_rather_than_a_guess():
    assert _price_adapter(FakePricingTransport(pricing_raises=True)).reference_price("EUR_USD", "BUY") is None
    assert _price_adapter(FakePricingTransport(prices=[])).reference_price("EUR_USD", "BUY") is None
    # a transport with no pricing capability at all must also yield None, never a fallback
    assert _price_adapter(FakeTransport()).reference_price("EUR_USD", "BUY") is None


def test_price_fn_is_a_drop_in_for_the_consumer_and_never_returns_the_stop_or_the_atr():
    """``lifecycle._price_fn`` returned ``suggested_sl or atr``. This returns a market price."""
    from system2.execution.pipeline import ApprovedOrder, RiskContext

    order = ApprovedOrder(
        idempotency_key="k", correlation_id="c", instrument="EUR_USD", side="BUY",
        units=1000, granularity="H1",
        risk_context=RiskContext(atr=0.0016, suggested_sl=1.0950, suggested_tp=1.1150),
    )
    a = _price_adapter(FakePricingTransport(prices=[_quote()]))
    assert a.price_fn(order) == 1.10010
    assert _price_adapter(FakeTransport()).price_fn(order) is None


def test_reference_price_row_helper_rejects_garbage():
    assert reference_price_from_broker_row({}) is None
    assert reference_price_from_broker_row(_quote(bids=[{"price": "0"}])) is None
    assert reference_price_from_broker_row(_quote(asks=[{"price": "not-a-number"}])) is None


# ----- environment toggle ---------------------------------------------------------
def test_practice_is_default(monkeypatch):
    monkeypatch.setenv("OANDA_PRACTICE_API_KEY", "pk")
    monkeypatch.setenv("OANDA_PRACTICE_ACCOUNT_ID", "acct")
    monkeypatch.delenv("OANDA_ENV", raising=False)
    env = resolve_environment(Secrets())
    assert env.env == "practice" and not env.is_live


def test_live_refused_without_creds(monkeypatch):
    monkeypatch.setenv("OANDA_ENV", "live")
    monkeypatch.delenv("OANDA_LIVE_API_KEY", raising=False)
    monkeypatch.delenv("OANDA_LIVE_ACCOUNT_ID", raising=False)
    with pytest.raises(LiveConfigError):
        resolve_environment(Secrets())


def test_live_allowed_with_creds(monkeypatch):
    monkeypatch.setenv("OANDA_ENV", "live")
    monkeypatch.setenv("OANDA_LIVE_API_KEY", "lk")
    monkeypatch.setenv("OANDA_LIVE_ACCOUNT_ID", "live-acct")
    env = resolve_environment(Secrets())
    assert env.is_live and "…" in env.banner()


# ----- submit / fill validation ---------------------------------------------------
def _fill(**over):
    base = {"id": "t1", "orderID": "o1", "price": "1.10001", "units": "10000",
            "time": "2026-06-24T12:00:01Z", "tradeOpened": {"tradeID": "T1"}}
    base.update(over)
    return base


def _trade_with_stops():
    return {"id": "T1", "stopLossOrder": {"price": "1.09900"},
            "takeProfitOrder": {"price": "1.10300"}}


def test_submit_happy_path_filled(monkeypatch):
    t = FakeTransport(fill=_fill(), trade=_trade_with_stops())
    res = _adapter(t).submit(_order(), model_set_id="set-1")
    assert res.realized_status == "FILLED"
    assert res.broker_trade_id == "T1"
    assert res.fill_price == 1.10001
    assert res.slippage_pips == pytest.approx(0.1)
    assert res.stop_loss_price == 1.09900 and res.take_profit_price == 1.10300
    assert res.reject_reason is None
    assert res.model_set_id == "set-1"
    # units never re-sized: payload carries the AMS units verbatim
    assert t.created[0]["order"]["units"] == "10000"
    assert t.created[0]["order"]["clientExtensions"]["id"] == "sb-k1"


def test_partial_fill_reported(monkeypatch):
    t = FakeTransport(fill=_fill(units="4000"), trade=_trade_with_stops())
    res = _adapter(t).submit(_order(units=10000))
    assert res.realized_status == "PARTIAL"
    assert res.filled_units == 4000


def test_slippage_beyond_tolerance_flagged(monkeypatch):
    t = FakeTransport(fill=_fill(price="1.10030"), trade=_trade_with_stops())  # 2.9 pips
    res = _adapter(t).submit(_order())
    assert res.realized_status == "FILLED"  # already filled -> accept-and-flag
    assert "SLIPPAGE_EXCEEDED" in res.reject_reason


def test_missing_stop_attached(monkeypatch):
    trade_no_sl = {"id": "T1", "takeProfitOrder": {"price": "1.10300"}}
    t = FakeTransport(fill=_fill(), trade=trade_no_sl)
    res = _adapter(t).submit(_order())
    assert t.modified == [("T1", 1.09900)]  # attach attempted
    assert t.modify_instruments == ["EUR_USD"]  # F-308: instrument routed for precision
    assert res.stop_loss_price == 1.09900
    assert res.reject_reason is None


def test_missing_stop_unattachable_is_unsafe(monkeypatch):
    trade_no_sl = {"id": "T1", "takeProfitOrder": {"price": "1.10300"}}
    t = FakeTransport(fill=_fill(), trade=trade_no_sl, modify_raises=True)
    res = _adapter(t).submit(_order())
    assert "NO_STOP_UNSAFE" in res.reject_reason
    assert res.stop_loss_price is None


def test_market_closed_returns_cancelled(monkeypatch):
    t = FakeTransport(raise_on_create=[MarketClosedError("MARKET_HALTED")])
    res = _adapter(t).submit(_order())
    assert res.realized_status == "CANCELLED"
    assert res.reject_reason == "MARKET_CLOSED"


def test_order_not_filled_surfaces_reason(monkeypatch):
    t = FakeTransport(fill=None, cancel={"reason": "INSUFFICIENT_MARGIN"})
    res = _adapter(t).submit(_order())
    assert res.realized_status == "CANCELLED"
    assert res.reject_reason == "INSUFFICIENT_MARGIN"


# ----- idempotency / retry / reconcile --------------------------------------------
def _open_trade(**over):
    base = {"id": "T1", "price": "1.10001", "currentUnits": "10000",
            "openTime": "2026-06-24T12:00:01Z", "clientExtensions": {"id": "sb-k1"}}
    base.update(over)
    return base


class LostReplyTransport(FakeTransport):
    """The order reaches the broker and fills, but the reply is lost on the wire.

    The trade therefore becomes visible only *after* the ``create_order`` that raised — which
    is what makes this a test of the post-transient reconcile rather than of the pre-submit
    one (that one has its own tests below).
    """

    def get_open_trades(self):
        return self._open_trades if self.created else []


def test_transient_then_reconcile_no_duplicate(monkeypatch):
    """Transient error but the order actually filled -> reconcile from open trades, no resubmit."""
    t = LostReplyTransport(raise_on_create=[TransientBrokerError("timeout")],
                           open_trades=[_open_trade()])
    res = _adapter(t).submit(_order())
    assert res.realized_status == "FILLED"
    assert res.broker_trade_id == "T1"
    assert len(t.created) == 1  # NOT resubmitted


def test_transient_then_retry_succeeds(monkeypatch):
    """Transient error, nothing reconciled -> backoff + retry, second attempt fills."""
    t = FakeTransport(raise_on_create=[TransientBrokerError("timeout"), None],
                      fill=_fill(), trade=_trade_with_stops())
    res = _adapter(t).submit(_order())
    assert res.realized_status == "FILLED"
    assert len(t.created) == 2  # retried once


def test_transient_exhausts_retries_raises(monkeypatch):
    errs = [TransientBrokerError("timeout")] * 3
    t = FakeTransport(raise_on_create=errs)
    with pytest.raises(TransientBrokerError):
        _adapter(t).submit(_order())


# ----- F-303: reconcile BEFORE create_order, across deliveries ---------------------
# Transaction shapes below are copied from the live practice stream (read-only probe,
# 2026-08-01): the order transaction carries the client id as ``clientExtensions.id``, the
# outcome transaction as ``clientOrderID``.
def _market_order_txn(txn_id, cid, **over):
    row = {"id": str(txn_id), "type": "MARKET_ORDER", "instrument": "EUR_USD", "units": "10000",
           "reason": "CLIENT_ORDER", "time": "2026-06-24T12:00:01Z",
           "clientExtensions": {"id": cid}}
    row.update(over)
    return row


def _order_fill_txn(txn_id, cid, order_txn_id, **over):
    row = {"id": str(txn_id), "type": "ORDER_FILL", "orderID": str(order_txn_id),
           "clientOrderID": cid, "instrument": "EUR_USD", "units": "10000",
           "price": "1.10001", "reason": "MARKET_ORDER", "time": "2026-06-24T12:00:01Z",
           "tradeOpened": {"tradeID": str(txn_id)}}
    row.update(over)
    return row


def _order_cancel_txn(txn_id, cid, order_txn_id, reason="MARKET_HALTED"):
    return {"id": str(txn_id), "type": "ORDER_CANCEL", "orderID": str(order_txn_id),
            "clientOrderID": cid, "reason": reason, "time": "2026-06-24T12:00:01Z"}


class ReconcilingTransport(FakeTransport):
    """A broker double that remembers what it was sent, the way OANDA does.

    Every accepted ``create_order`` appends the MARKET_ORDER + ORDER_FILL pair to a
    transaction history that ``get_recent_transactions`` then serves — so a second submit of
    the same order can actually *see* the first one. ``get_open_trades`` stays empty on
    purpose: these tests must pass through the transaction path, which is the only one that
    covers a fill whose trade has since closed.
    """

    def __init__(self, *, transactions=None, raise_on_transactions=None, **kw):
        super().__init__(**kw)
        self.transactions = list(transactions or [])
        self._raise_on_transactions = raise_on_transactions
        self.transaction_calls = 0
        self.transaction_counts = []
        self._next_id = 1000

    def create_order(self, payload):
        resp = super().create_order(payload)
        cid = payload["order"]["clientExtensions"]["id"]
        order_id, outcome_id = self._next_id, self._next_id + 1
        self._next_id += 2
        self.transactions.append(_market_order_txn(order_id, cid))
        fill = resp.get("orderFillTransaction")
        if fill is None:
            self.transactions.append(_order_cancel_txn(outcome_id, cid, order_id))
            return resp
        # The transaction stream IS the source of the create_order reply, so it carries the
        # same ids — a reconciled fill must be indistinguishable from the original.
        self.transactions.append({**fill, "type": "ORDER_FILL", "clientOrderID": cid})
        return resp

    def get_recent_transactions(self, count=200):
        self.transaction_calls += 1
        self.transaction_counts.append(count)
        if self._raise_on_transactions is not None:
            raise self._raise_on_transactions
        return self.transactions[-int(count):]


def test_reconcile_runs_before_create_order_on_every_submit():
    """The happy path pays exactly one bounded reconcile lookup, and still submits."""
    t = ReconcilingTransport(fill=_fill(), trade=_trade_with_stops())
    res = _adapter(t).submit(_order())
    assert res.realized_status == "FILLED"
    assert t.transaction_calls == 1 and len(t.created) == 1
    assert t.transaction_counts == [200]  # bounded window, never a full-history scan


def test_redelivery_after_a_lost_idempotency_mark_does_not_submit_twice():
    """THE F-303 crash window, at the adapter: fill returned, marker never landed, redelivery.

    A fresh ``submit()`` of the same order — exactly what the consumer does when the process
    died between the broker fill and ``processed.mark()`` — must recognise the broker's own
    record of the first submission and return it instead of placing a second order.
    """
    t = ReconcilingTransport(fill=_fill(), trade=_trade_with_stops())
    first = _adapter(t).submit(_order())
    second = _adapter(t).submit(_order())  # a NEW adapter would behave identically: no state
    assert len(t.created) == 1, "the same approved order reached the broker twice"
    assert first.realized_status == second.realized_status == "FILLED"
    assert second.broker_trade_id == first.broker_trade_id


def test_reconcile_sees_a_fill_whose_trade_has_already_closed():
    """Constraint the open-trades check cannot meet: SL/TP already took the position out.

    ``get_open_trades`` is empty here. Only the transaction history still knows the order
    filled — and those are precisely the orders whose resubmission has already cost money.
    """
    closed_trade = {"id": "1001", "state": "CLOSED"}
    t = ReconcilingTransport(
        transactions=[_market_order_txn(1000, "sb-k1"), _order_fill_txn(1001, "sb-k1", 1000)],
        open_trades=[], trade=closed_trade, fill=_fill(),
    )
    res = _adapter(t).submit(_order())
    assert t.created == [], "resubmitted an order that had already filled and closed"
    assert res.realized_status == "FILLED" and res.broker_trade_id == "1001"
    assert res.reject_reason is None  # a closed trade needs no stop; not a risk alert


def test_a_prior_order_that_the_broker_cancelled_is_safe_to_submit():
    """Cancelled/rejected means no position exists — deferring here would lose real orders."""
    t = ReconcilingTransport(
        transactions=[_market_order_txn(1000, "sb-k1"),
                      _order_cancel_txn(1001, "sb-k1", 1000, reason="MARKET_HALTED")],
        fill=_fill(), trade=_trade_with_stops(),
    )
    res = _adapter(t).submit(_order())
    assert len(t.created) == 1 and res.realized_status == "FILLED"


def test_a_prior_order_with_no_outcome_yet_refuses_to_resubmit():
    """Ambiguous: the broker has the order but has not said what became of it. Do not double."""
    t = ReconcilingTransport(transactions=[_market_order_txn(1000, "sb-k1")],
                             fill=_fill(), trade=_trade_with_stops())
    with pytest.raises(ReconcileUnavailableError):
        _adapter(t).submit(_order())
    assert t.created == []


def test_a_failed_reconcile_refuses_to_submit_and_stays_retryable():
    """"Cannot verify" resolves toward "already submitted": defer, never risk a duplicate.

    The refusal is a ``TransientBrokerError`` subclass, so the consumer nacks and the queue
    redelivers — a reconcile outage postpones the order (and eventually dead-letters it
    visibly), it does not silently drop it.
    """
    t = ReconcilingTransport(raise_on_transactions=TransientBrokerError("502 from OANDA"),
                             fill=_fill(), trade=_trade_with_stops())
    with pytest.raises(ReconcileUnavailableError):
        _adapter(t).submit(_order())
    assert t.created == [], "submitted an order it could not prove was unsubmitted"
    assert issubclass(ReconcileUnavailableError, TransientBrokerError)


def test_an_unreadable_broker_after_an_ambiguous_send_is_not_retried_into_a_duplicate():
    t = ReconcilingTransport(raise_on_create=[TransientBrokerError("timeout")],
                             fill=_fill(), trade=_trade_with_stops())
    a = _adapter(t)
    a.submit(_order(idempotency_key="warmup"))          # history now readable
    t._raise_on_transactions = TransientBrokerError("502")  # broker goes dark mid-flight
    with pytest.raises(ReconcileUnavailableError):
        a.submit(_order())
    assert len(t.created) == 2  # the warmup, plus the one ambiguous attempt — never a retry


def test_a_transport_that_cannot_read_transactions_degrades_instead_of_halting():
    """A missing capability is a deployment fact, not a fact about this order.

    Failing closed on it would stop trading 100% of the time (the audit harness's own double
    is such a transport); it degrades to the open-trades check — today's behaviour — instead.
    """
    t = FakeTransport(fill=_fill(), trade=_trade_with_stops())
    assert not hasattr(t, "get_recent_transactions")
    assert _adapter(t).submit(_order()).realized_status == "FILLED"
    assert len(t.created) == 1


def test_the_deployed_transport_actually_has_the_reconcile_capability():
    """The guard on the paragraph above: production must never take the degraded path."""
    from system2.broker.oanda_adapter import OandaReconcileTransport
    from system2.broker.oanda_transport import OandaRestTransport

    assert isinstance(OandaRestTransport(secrets=None, environment=PRACTICE),
                      OandaReconcileTransport)


# ----- F-303: the verdict function, on live-observed transaction shapes ------------
def test_verdict_reads_both_fields_oanda_uses_for_a_client_order_id():
    assert transaction_client_order_id(_market_order_txn(1, "sb-k1")) == "sb-k1"
    assert transaction_client_order_id(_order_fill_txn(2, "sb-k1", 1)) == "sb-k1"
    assert transaction_client_order_id({"type": "DAILY_FINANCING", "id": "3"}) is None


def test_verdict_classifies_the_four_states():
    fill = _order_fill_txn(2, "sb-k1", 1)
    assert prior_submission_verdict([_market_order_txn(1, "sb-k1"), fill], "sb-k1") == ("filled", fill)
    assert prior_submission_verdict(
        [_market_order_txn(1, "sb-k1"), _order_cancel_txn(2, "sb-k1", 1)], "sb-k1"
    ) == ("unfilled", None)
    assert prior_submission_verdict([_market_order_txn(1, "sb-k1")], "sb-k1") == ("pending", None)
    assert prior_submission_verdict([], "sb-k1") == ("absent", None)


def test_verdict_ignores_other_orders_client_ids():
    other = [_market_order_txn(1, "sb-other"), _order_fill_txn(2, "sb-other", 1),
             {"id": "3", "type": "DAILY_FINANCING"},
             {"id": "4", "type": "STOP_LOSS_ORDER", "tradeID": "2"}]
    assert prior_submission_verdict(other, "sb-k1") == ("absent", None)


def test_verdict_treats_a_rejected_order_as_leaving_nothing_behind():
    rejected = [_market_order_txn(1, "sb-k1"),
                {"id": "2", "type": "MARKET_ORDER_REJECT", "clientOrderID": "sb-k1",
                 "rejectReason": "INSUFFICIENT_MARGIN"}]
    assert prior_submission_verdict(rejected, "sb-k1") == ("unfilled", None)


# ----- F-303: the crash window end-to-end through the real pipeline ----------------
WEDNESDAY = datetime(2026, 7, 29, 12, 0, 0, tzinfo=timezone.utc)  # in session


class NeverDurableProcessedStore:
    """A processed store whose ``mark()`` never survives — i.e. the process died writing it.

    This is the F-303 window made explicit: ``pipeline.process`` marks immediately after the
    fill (pipeline.py:481-482), so the only way a redelivery can still reach the broker is if
    that write never became durable. Then nothing upstream of the adapter can stop the second
    submission — which is why the adapter must.
    """

    def seen(self, key: str) -> bool:
        return False

    def mark(self, key: str) -> None:
        return None


def _approved_order(key="k1"):
    from system2.execution.pipeline import ApprovedOrder, RiskContext

    return ApprovedOrder(
        idempotency_key=key, correlation_id="c1", instrument="EUR_USD", side="BUY",
        units=10000, granularity="H1",
        risk_context=RiskContext(atr=0.0010, suggested_sl=1.09900, suggested_tp=1.10300),
    )


def test_pipeline_redelivery_with_a_lost_mark_still_reaches_the_broker_once():
    """End-to-end: crash between fill and durable mark, then redelivery. ONE broker order."""
    from system2.execution.pipeline import Decision, ExecMode, ExecutionPipeline

    t = ReconcilingTransport(fill=_fill(), trade=_trade_with_stops())
    adapter = _adapter(t)
    pipeline = ExecutionPipeline(
        mode=ExecMode.EXECUTION_ONLY, shadow=False,
        processed_store=NeverDurableProcessedStore(), clock=lambda: WEDNESDAY,
    )
    first = pipeline.process(_approved_order(), 1.10000, submit_fn=adapter.submit)
    second = pipeline.process(_approved_order(), 1.10000, submit_fn=adapter.submit)

    assert first["decision"] is Decision.EXECUTED
    assert second["decision"] is Decision.EXECUTED  # the pipeline still ran the full path...
    assert len(t.created) == 1, "a redelivered approved order reached the broker twice"
    assert second["fill"].broker_trade_id == first["fill"].broker_trade_id
