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
    TransientBrokerError,
    client_order_id,
    compute_slippage_pips,
    default_display_precision,
    format_price,
    pip_size,
    quantize_price,
    reference_price_from_broker_row,
    resolve_environment,
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
def test_transient_then_reconcile_no_duplicate(monkeypatch):
    """Transient error but the order actually filled -> reconcile from open trades, no resubmit."""
    open_trade = {"id": "T1", "price": "1.10001", "currentUnits": "10000",
                  "openTime": "2026-06-24T12:00:01Z",
                  "clientExtensions": {"id": "sb-k1"}}
    t = FakeTransport(raise_on_create=[TransientBrokerError("timeout")], open_trades=[open_trade])
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
