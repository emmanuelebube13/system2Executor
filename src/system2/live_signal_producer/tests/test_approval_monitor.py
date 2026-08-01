"""FIX_PLAN 2.1(d) / F-602 — the runtime gatekeeper approval-rate monitor.

The headline test is ``test_alarms_on_the_real_production_numbers``: a monitor that has
never been shown the failure it exists to catch is not verified, so it is fed the actual
2026-08-01 production figures (1,893 approved of 1,894 evaluations) and must alarm.
"""

from __future__ import annotations

import pytest

from system2.live_signal_producer.approval_monitor import (
    DEFAULT_TURNOVER_BAND,
    REMINDER_SEC,
    STATE_OK,
    STATE_OUT_OF_BAND,
    STATE_UNKNOWN,
    STATE_WARMING_UP,
    ApprovalRateMonitor,
    wilson_interval,
)

# ---- the real incident, from production telemetry on 2026-08-01 -------------------
LIVE_EVALUATIONS = 1894
LIVE_APPROVED = 1893
LIVE_RATE = LIVE_APPROVED / LIVE_EVALUATIONS          # 0.99947...
MODEL_OOS_APPROVAL_RATE = 0.3379                      # champion_manifest oos_uplift
LIVE_BAND = (0.05, 0.60)                              # champion_manifest turnover_band


def _verdicts(total: int, approved: int) -> list[bool]:
    """`total` verdicts of which `approved` are True, rejections spread through."""
    if approved >= total:
        return [True] * total
    step = total / (total - approved)
    rejects = {int(i * step) for i in range(total - approved)}
    return [i not in rejects for i in range(total)]


# ----- the proof ------------------------------------------------------------------
def test_alarms_on_the_real_production_numbers():
    """1,893 of 1,894 approved, against the manifest's own [5%, 60%] band."""
    monitor = ApprovalRateMonitor(band=LIVE_BAND)
    monitor.seed(_verdicts(LIVE_EVALUATIONS, LIVE_APPROVED))
    snap = monitor.snapshot()

    assert snap["state"] == STATE_OUT_OF_BAND
    assert snap["alarm"] is True
    assert snap["band"] == [0.05, 0.60]
    assert snap["lifetime_evaluations"] == LIVE_EVALUATIONS
    assert snap["lifetime_approval_rate"] == pytest.approx(LIVE_RATE, abs=1e-4)
    # The whole 99% interval is above the band's upper edge — not a boundary quibble.
    assert snap["ci99"][0] > 0.60
    assert "not gating" in snap["reason"]
    # And it says so in a form an operator can act on.
    assert "ABOVE" in snap["reason"]


def test_would_have_alarmed_within_the_first_fifty_evaluations():
    """The value of this monitor is *latency*: weeks of blindness becomes ~50 signals.

    At the 50-sample floor with everything approved, the 99% Wilson lower bound is
    already ~0.88 — far above the 0.60 band edge — so the alarm opens the moment the
    monitor is allowed to have an opinion at all.
    """
    monitor = ApprovalRateMonitor(band=LIVE_BAND, min_samples=50)
    for i in range(49):
        monitor.observe(True)
        assert monitor.snapshot()["state"] == STATE_WARMING_UP, i
        assert monitor.snapshot()["alarm"] is False
    monitor.observe(True)  # the 50th
    snap = monitor.snapshot()
    assert snap["evaluations"] == 50
    assert snap["alarm"] is True
    assert snap["ci99"][0] > 0.85


def test_the_models_own_oos_rate_does_not_alarm():
    """0.3379 is what the champion claims it does. That must read as healthy."""
    total = 1000
    monitor = ApprovalRateMonitor(band=LIVE_BAND)
    monitor.seed(_verdicts(total, round(total * MODEL_OOS_APPROVAL_RATE)))
    snap = monitor.snapshot()
    assert snap["state"] == STATE_OK
    assert snap["alarm"] is False
    assert snap["reason"] is None
    assert LIVE_BAND[0] < snap["approval_rate"] < LIVE_BAND[1]


# ----- the other edge -------------------------------------------------------------
def test_alarms_when_the_gate_refuses_almost_everything():
    """The band has two edges; a gate stuck closed is also a defect worth paging on."""
    monitor = ApprovalRateMonitor(band=LIVE_BAND)
    monitor.seed(_verdicts(400, 2))  # 0.5% approval
    snap = monitor.snapshot()
    assert snap["state"] == STATE_OUT_OF_BAND
    assert snap["ci99"][1] < 0.05
    assert "BELOW" in snap["reason"]
    assert "switched off" in snap["reason"]


# ----- window semantics -----------------------------------------------------------
def test_window_is_a_rolling_count_so_a_fix_clears_the_alarm():
    monitor = ApprovalRateMonitor(band=LIVE_BAND, window=100, min_samples=50)
    for _ in range(100):
        monitor.observe(True)
    assert monitor.snapshot()["alarm"] is True
    # Recalibrated model: approvals drop to ~1 in 3. The rolling window forgets.
    for i in range(100):
        monitor.observe(i % 3 == 0)
    snap = monitor.snapshot()
    assert snap["evaluations"] == 100           # window is bounded
    assert snap["alarm"] is False
    assert snap["approval_rate"] == pytest.approx(0.34, abs=0.02)
    # ...but history is not erased: all-time still records what happened.
    assert snap["lifetime_evaluations"] == 200
    assert snap["lifetime_approval_rate"] > 0.6


def test_small_samples_never_alarm():
    """A rate over 10 signals is noise. It must not be allowed to cry wolf."""
    monitor = ApprovalRateMonitor(band=LIVE_BAND, min_samples=50)
    assert monitor.snapshot()["state"] == STATE_UNKNOWN
    for _ in range(10):
        monitor.observe(True)
    snap = monitor.snapshot()
    assert snap["state"] == STATE_WARMING_UP
    assert snap["alarm"] is False
    assert snap["approval_rate"] == 1.0     # measured and published, just not judged


def test_seeding_from_the_ledger_survives_a_restart():
    """A restart must not reset the measurement below the alarm floor."""
    first = ApprovalRateMonitor(band=LIVE_BAND, min_samples=50)
    for _ in range(60):
        first.observe(True)
    assert first.snapshot()["alarm"] is True

    restarted = ApprovalRateMonitor(band=LIVE_BAND, min_samples=50)
    assert restarted.snapshot()["alarm"] is False       # cold
    assert restarted.seed([True] * 60) == 60
    snap = restarted.snapshot()
    assert snap["alarm"] is True
    assert snap["seeded_from_ledger"] is True


# ----- band plumbing --------------------------------------------------------------
def test_band_defaults_and_is_adopted_from_the_manifest():
    monitor = ApprovalRateMonitor()
    assert monitor.band == DEFAULT_TURNOVER_BAND
    monitor.set_band([0.10, 0.45])
    assert monitor.snapshot()["band"] == [0.10, 0.45]
    assert monitor.snapshot()["band_source"] == "manifest"


@pytest.mark.parametrize("bad", [None, [], [0.6, 0.05], [-1.0, 0.5], [0.2, 2.0],
                                 ["a", "b"], [0.3]])
def test_a_malformed_band_keeps_the_previous_one(bad):
    """Never let a bad manifest silently disable the monitor."""
    monitor = ApprovalRateMonitor(band=(0.05, 0.60))
    monitor.set_band(bad)
    assert monitor.band == (0.05, 0.60)


# ----- alerting -------------------------------------------------------------------
def test_alert_fires_once_per_episode_then_reminds_then_resolves():
    monitor = ApprovalRateMonitor(band=LIVE_BAND, min_samples=50)
    assert monitor.poll_alert(1000.0) is None            # healthy/unknown: silent
    monitor.seed([True] * 200)

    opened = monitor.poll_alert(1000.0)
    assert opened is not None and opened["event"] == "opened"
    assert monitor.poll_alert(1001.0) is None            # not once per bar
    assert monitor.poll_alert(1000.0 + REMINDER_SEC - 1) is None
    reminder = monitor.poll_alert(1000.0 + REMINDER_SEC)
    assert reminder is not None and reminder["event"] == "reminder"
    assert reminder["since_ts"] == 1000.0

    for i in range(200):                                  # recalibrated into the band
        monitor.observe(i % 3 == 0)
    resolved = monitor.poll_alert(2000.0)
    assert resolved is not None and resolved["event"] == "resolved"
    assert monitor.poll_alert(2001.0) is None


# ----- fail-loud vs fail-closed ---------------------------------------------------
def test_default_is_alarm_only_and_never_blocks():
    monitor = ApprovalRateMonitor(band=LIVE_BAND)
    monitor.seed([True] * 300)
    assert monitor.alarming is True
    assert monitor.enforcing is False
    assert monitor.should_block() is False       # fail-LOUD is the default posture


def test_enforcement_blocks_only_when_explicitly_armed():
    monitor = ApprovalRateMonitor(band=LIVE_BAND, enforcing=True)
    assert monitor.should_block() is False       # armed but healthy/unknown
    monitor.seed([True] * 300)
    assert monitor.should_block() is True
    assert monitor.snapshot()["enforcing"] is True


# ----- the statistic --------------------------------------------------------------
def test_wilson_does_not_degenerate_at_the_extremes():
    """Wald would report a zero-width interval at p=1, exactly where we live."""
    lo, hi = wilson_interval(200, 200)
    assert hi == pytest.approx(1.0)
    assert 0.9 < lo < 1.0                        # a real, usable lower bound
    lo0, hi0 = wilson_interval(0, 200)
    assert lo0 == pytest.approx(0.0) and 0.0 < hi0 < 0.1
    assert wilson_interval(0, 0) == (0.0, 1.0)   # no data => no opinion


def test_wilson_interval_brackets_the_point_estimate_and_narrows_with_n():
    wide = wilson_interval(30, 60)
    narrow = wilson_interval(3000, 6000)
    for lo, hi in (wide, narrow):
        assert lo < 0.5 < hi
    assert (narrow[1] - narrow[0]) < (wide[1] - wide[0])


def test_snapshot_is_json_shaped_and_never_raises():
    import json

    monitor = ApprovalRateMonitor()
    json.dumps(monitor.snapshot())               # /signal and /status serialise this
    monitor.seed([True, False, True])
    json.dumps(monitor.snapshot())
