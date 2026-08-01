"""EXEC-007 tests — R math, breakeven-once/forward-only, trailing-tighten-only, time exits."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from system2.broker.position_manager import (
    ManagedTrade,
    PositionManager,
    is_tighter,
    r_multiple,
    trailing_stop,
)

T0 = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)


class FakeAdapter:
    def __init__(self):
        self.modified = []
        self.instruments = []
        self.closed = []

    def modify_stop(self, trade_id, new_stop, instrument=None):
        self.modified.append((trade_id, new_stop))
        self.instruments.append(instrument)
        return {"ok": True}

    def close_trade(self, trade_id, units="ALL"):
        self.closed.append(trade_id)
        return {"ok": True}


def _long(**over) -> ManagedTrade:
    base = dict(
        broker_trade_id="T1", instrument="EUR_USD", side="BUY", entry_price=1.10000,
        initial_stop_price=1.09900, take_profit_price=1.10300, open_time=T0,
        granularity="H1", max_duration_sec=3600, correlation_id="c1",
    )
    base.update(over)
    return ManagedTrade(**base)


def _short(**over) -> ManagedTrade:
    base = dict(
        broker_trade_id="T2", instrument="EUR_USD", side="SELL", entry_price=1.10000,
        initial_stop_price=1.10100, take_profit_price=1.09700, open_time=T0,
        granularity="H1", max_duration_sec=3600, correlation_id="c2",
    )
    base.update(over)
    return ManagedTrade(**base)


def _mgr(adapter, clock_holder, **kw):
    return PositionManager(adapter=adapter, clock=lambda: clock_holder[0], **kw)


# ----- pure rule engine -----------------------------------------------------------
def test_r_multiple_long_and_short():
    assert r_multiple(_long(), 1.10100) == pytest.approx(1.0)
    assert r_multiple(_long(), 1.10050) == pytest.approx(0.5)
    assert r_multiple(_short(), 1.09900) == pytest.approx(1.0)


def test_is_tighter_direction_aware():
    long = _long()
    assert is_tighter(long, 1.10050, 1.10000) is True   # long: higher = tighter
    assert is_tighter(long, 1.09990, 1.10000) is False
    short = _short()
    assert is_tighter(short, 1.09950, 1.10000) is True   # short: lower = tighter


def test_trailing_stop_favorable_only():
    long = _long()
    assert trailing_stop(long, 1.10200, 0.0005) == pytest.approx(1.10150)
    short = _short()
    assert trailing_stop(short, 1.09800, 0.0005) == pytest.approx(1.09850)


# ----- breakeven ------------------------------------------------------------------
def test_breakeven_moves_stop_to_entry_once():
    adapter = FakeAdapter()
    clk = [T0]
    mgr = _mgr(adapter, clk, breakeven_buffer_pips=1.0)
    trade = _long()
    acts = mgr.on_tick(trade, 1.10100)  # R == 1
    assert "breakeven" in acts
    assert adapter.modified == [("T1", pytest.approx(1.10010))]  # entry + 1 pip buffer
    assert trade.at_breakeven
    # a second tick must not move it again
    acts2 = mgr.on_tick(trade, 1.10120)
    assert "breakeven" not in acts2
    assert len(adapter.modified) == 1


def test_stop_moves_carry_the_instrument_for_price_precision():
    """F-308: without the instrument the adapter cannot render the price at the right
    precision, and a JPY stop-move (breakeven/trailing) is rejected by the broker."""
    adapter = FakeAdapter()
    mgr = _mgr(adapter, [T0], breakeven_buffer_pips=1.0)
    mgr.on_tick(_long(), 1.10100)
    assert adapter.instruments == ["EUR_USD"]


def test_breakeven_not_triggered_below_1r():
    adapter = FakeAdapter()
    clk = [T0]
    mgr = _mgr(adapter, clk)
    trade = _long()
    acts = mgr.on_tick(trade, 1.10050)  # R == 0.5
    assert acts == []
    assert adapter.modified == []


# ----- trailing -------------------------------------------------------------------
def test_trailing_only_tightens():
    adapter = FakeAdapter()
    clk = [T0]
    mgr = _mgr(adapter, clk, trail_distance=0.0005)
    trade = _long()
    trade.at_breakeven = True
    trade.current_stop = 1.10001
    mgr.on_tick(trade, 1.10200)  # new_stop 1.10150 -> tighter
    assert adapter.modified[-1] == ("T1", pytest.approx(1.10150))
    # price pulls back: new_stop lower -> must NOT loosen
    n = len(adapter.modified)
    mgr.on_tick(trade, 1.10120)  # new_stop 1.10070 < 1.10150
    assert len(adapter.modified) == n


# ----- time-based exits -----------------------------------------------------------
def test_time_actions_fire_at_fractions():
    adapter = FakeAdapter()
    clk = [T0]
    mgr = _mgr(adapter, clk)
    trade = _long()
    clk[0] = T0 + timedelta(seconds=1800)  # 50%
    assert "time_action_50pct" in mgr.on_tick(trade, 1.10000)
    clk[0] = T0 + timedelta(seconds=2700)  # 75%
    assert "time_action_75pct" in mgr.on_tick(trade, 1.10000)


def test_time_100pct_force_closes_and_emits():
    adapter = FakeAdapter()
    emitted = []
    clk = [T0 + timedelta(seconds=3600)]  # 100%
    mgr = _mgr(adapter, clk,
               emit_close_fn=lambda t, resp, reason: emitted.append(t.broker_trade_id))
    trade = _long()
    acts = mgr.on_tick(trade, 1.10000)
    assert "time_exit_close" in acts
    assert adapter.closed == ["T1"]
    assert emitted == ["T1"]
    assert trade.closed
    # a closed trade is not managed further
    assert mgr.on_tick(trade, 1.10500) == []


def test_evaluate_all_skips_missing_prices_and_closed():
    adapter = FakeAdapter()
    clk = [T0]
    mgr = _mgr(adapter, clk, breakeven_buffer_pips=1.0)
    a, b = _long(), _short()
    mgr.register(a)
    mgr.register(b)
    out = mgr.evaluate_all({"EUR_USD": 1.10100})  # both are EUR_USD
    assert "T1" in out and "T2" in out
    assert "breakeven" in out["T1"]  # long hit 1R


def test_disabled_manager_is_noop():
    adapter = FakeAdapter()
    clk = [T0 + timedelta(seconds=3600)]
    mgr = _mgr(adapter, clk, enabled=False)
    trade = _long()
    assert mgr.on_tick(trade, 1.10100) == []
    assert adapter.modified == [] and adapter.closed == []
