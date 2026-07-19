"""EXEC-008 tests — staleness PAUSE, session-awareness, hysteresis, audited BYPASS, invariant."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from system2.execution.safety_mode import (
    SafetyConfig,
    SafetyMonitor,
    SafetyState,
    conservative_position_size,
)

WED = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)  # in-session
SAT = datetime(2026, 6, 27, 12, 0, 0, tzinfo=timezone.utc)  # weekend


def _mon(clk, **cfg):
    holder = {"now": clk}
    config = SafetyConfig(**cfg)
    m = SafetyMonitor(config=config, clock=lambda: holder["now"])
    return m, holder


# ----- conservative sizing --------------------------------------------------------
def test_conservative_sizing_math():
    # equity 10000, risk 0.25% = 25; ATR stop 0.0010 -> 25000 units
    assert conservative_position_size(10000, 0.0010, risk_pct=0.25) == 25000
    assert conservative_position_size(0, 0.0010, risk_pct=0.25) == 0
    assert conservative_position_size(10000, 0.0, risk_pct=0.25) == 0


# ----- staleness boundary ---------------------------------------------------------
def test_fresh_queue_stays_running():
    m, h = _mon(WED, staleness_limit_sec=300)
    last = WED - timedelta(seconds=60)
    assert m.evaluate(last, now=WED) is SafetyState.RUNNING
    assert m.can_submit()


def test_stale_beyond_limit_pauses():
    m, h = _mon(WED, staleness_limit_sec=300)
    last = WED - timedelta(seconds=301)
    assert m.evaluate(last, now=WED) is SafetyState.PAUSED
    assert not m.can_submit()


def test_boundary_exactly_at_limit_not_paused():
    m, h = _mon(WED, staleness_limit_sec=300)
    last = WED - timedelta(seconds=300)  # exactly 5 min -> not > limit
    assert m.evaluate(last, now=WED) is SafetyState.RUNNING


def test_startup_grace_no_immediate_pause():
    # started_at == WED; no messages yet -> staleness 0, not paused
    m, h = _mon(WED, staleness_limit_sec=300)
    assert m.evaluate(None, now=WED) is SafetyState.RUNNING


# ----- session awareness ----------------------------------------------------------
def test_out_of_session_silence_does_not_pause():
    m, h = _mon(SAT, staleness_limit_sec=300)
    m._started_at = SAT - timedelta(hours=2)  # stale, but it's the weekend
    assert m.evaluate(None, now=SAT) is SafetyState.RUNNING


# ----- auto-resume + hysteresis ---------------------------------------------------
def test_auto_resume_with_hysteresis():
    m, h = _mon(WED, staleness_limit_sec=300, hysteresis_sec=30)
    assert m.evaluate(WED - timedelta(seconds=400), now=WED) is SafetyState.PAUSED
    # a message 280s old: staleness 280 which is < 300 but NOT < 270 -> stays paused (anti-flap)
    assert m.evaluate(WED - timedelta(seconds=280), now=WED) is SafetyState.PAUSED
    # a fresh message: staleness 10 < 270 -> resume
    assert m.evaluate(WED - timedelta(seconds=10), now=WED) is SafetyState.RUNNING


def test_heartbeat_keeps_fresh_without_orders():
    m, h = _mon(WED, staleness_limit_sec=300)
    m._started_at = WED - timedelta(seconds=1000)
    m.record_heartbeat(WED - timedelta(seconds=60))  # recent heartbeat
    assert m.evaluate(None, now=WED) is SafetyState.RUNNING


# ----- BYPASS ---------------------------------------------------------------------
def test_bypass_refused_when_disabled():
    m, h = _mon(WED, bypass_enable=False, bypass_confirm_token="tok")
    assert m.request_bypass("tok", "alice", "sys3 down") is False
    assert m.state is SafetyState.RUNNING


def test_bypass_refused_without_token():
    m, h = _mon(WED, bypass_enable=True, bypass_confirm_token="tok")
    assert m.request_bypass("wrong", "alice", "sys3 down") is False
    assert m.request_bypass("", "alice", "sys3 down") is False
    assert m.state is SafetyState.RUNNING


def test_bypass_enabled_with_token_audits():
    audits = []
    m, h = _mon(WED, bypass_enable=True, bypass_confirm_token="tok")
    m.audit_fn = audits.append
    assert m.request_bypass("tok", "alice", "sys3 down") is True
    assert m.state is SafetyState.BYPASS
    assert m.can_submit()  # bypass can submit (Layer-3 direct, conservative sizing)
    assert any(a["to_state"] == "bypass" and a.get("operator") == "alice" for a in audits)


def test_bypass_time_bounded_auto_revert():
    m, h = _mon(WED, bypass_enable=True, bypass_confirm_token="tok", bypass_max_duration_sec=600)
    m.request_bypass("tok", "alice", "sys3 down")
    h["now"] = WED + timedelta(seconds=601)
    # window expired AND the queue is fresh again -> reverts to RUNNING
    fresh = h["now"] - timedelta(seconds=10)
    assert m.evaluate(fresh, now=h["now"]) is SafetyState.RUNNING
    assert m.state is SafetyState.RUNNING


def test_bypass_holds_until_expiry_or_exit():
    m, h = _mon(WED, bypass_enable=True, bypass_confirm_token="tok", bypass_max_duration_sec=3600)
    m.request_bypass("tok", "alice", "sys3 down")
    h["now"] = WED + timedelta(seconds=1000)
    # even with a stale queue, BYPASS is intentional and holds
    assert m.evaluate(WED - timedelta(seconds=9999), now=h["now"]) is SafetyState.BYPASS
    m.exit_bypass("operator_disabled")
    assert m.state is SafetyState.RUNNING


# ----- safety invariant -----------------------------------------------------------
def test_invariant_queue_down_bypass_off_never_submits():
    m, h = _mon(WED, staleness_limit_sec=300, bypass_enable=False)
    m._started_at = WED - timedelta(hours=1)  # queue long dead
    m.evaluate(None, now=WED)
    assert m.state is SafetyState.PAUSED
    assert m.can_submit() is False


def test_transition_alerts_fire():
    alerts = []
    m, h = _mon(WED, staleness_limit_sec=300)
    m.alert_fn = lambda **kw: alerts.append(kw)
    m.evaluate(WED - timedelta(seconds=400), now=WED)  # -> PAUSED
    assert alerts and alerts[-1]["to_state"] == "paused"
