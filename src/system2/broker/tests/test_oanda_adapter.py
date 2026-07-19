"""EXEC-006 tests — pip/slippage/idempotency math, fill validation, stop confirmation, toggle."""

from __future__ import annotations

import pytest

from system2.broker.oanda_adapter import (
    BrokerEnvironment,
    LiveConfigError,
    MarketClosedError,
    OandaAdapter,
    TransientBrokerError,
    client_order_id,
    compute_slippage_pips,
    pip_size,
    resolve_environment,
)
from system2.common.secrets import Secrets
from system2.execution.pipeline import ConstructedOrder

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
                 modify_raises=False):
        self._fill = fill
        self._cancel = cancel
        self._raise_seq = list(raise_on_create or [])
        self._open_trades = open_trades or []
        self._trade = trade
        self._modify_raises = modify_raises
        self.created = []
        self.modified = []
        self.closed = []

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

    def modify_trade_stop(self, trade_id, stop_price):
        if self._modify_raises:
            from system2.broker.oanda_adapter import BrokerError
            raise BrokerError("cannot attach")
        self.modified.append((trade_id, stop_price))
        return {"ok": True}

    def close_trade(self, trade_id, units="ALL"):
        self.closed.append((trade_id, units))
        return {"ok": True}


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
