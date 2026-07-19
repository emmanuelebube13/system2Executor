"""Tests for the regime scheduler (drives the detector) + the /regime telemetry payload.

Uses a fake detector so it runs anywhere — no OANDA creds, no model bundle, no symlinks.
"""

from __future__ import annotations

from datetime import datetime, timezone

from system2.artifact_sync.regime_scheduler import RegimeScheduler
from system2.telemetry.health import HealthReporter

T0 = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)


class FakeDetector:
    """Records detect() calls and serves a grid, mimicking LiveRegimeDetector's surface."""

    def __init__(self, raise_on=()):
        self.calls = []
        self.raise_on = set(raise_on)  # instruments that blow up mid-sweep

    def detect(self, instrument, granularity):
        if instrument in self.raise_on:
            raise RuntimeError("boom")
        self.calls.append((instrument, granularity))

    def snapshot_grid(self):
        return [
            {"instrument": i, "granularity": g, "smoothed_label": "Trending-Up",
             "smoothed_confidence": 0.8, "stale": False}
            for (i, g) in self.calls
        ]


def _sched(detector, **over):
    base = dict(detector=detector, instruments=["EUR_USD", "USD_JPY"],
                granularities=["H1", "H4"], refresh_sec=60)
    base.update(over)
    return RegimeScheduler(**base)


# ----- sweep drives every cell ------------------------------------------------
def test_sweep_covers_full_watchlist():
    det = FakeDetector()
    n = _sched(det).sweep_once()
    assert n == 4
    assert set(det.calls) == {("EUR_USD", "H1"), ("EUR_USD", "H4"),
                              ("USD_JPY", "H1"), ("USD_JPY", "H4")}


def test_sweep_is_fail_open_on_one_bad_pair():
    # EUR_USD raises on every granularity; the sweep must still finish USD_JPY.
    det = FakeDetector(raise_on={"EUR_USD"})
    n = _sched(det).sweep_once()
    assert n == 2
    assert ("USD_JPY", "H1") in det.calls and ("USD_JPY", "H4") in det.calls


def test_grid_accessor_reflects_detector():
    det = FakeDetector()
    sched = _sched(det)
    sched.sweep_once()
    grid = sched.grid()
    assert len(grid) == 4
    assert all(cell["smoothed_label"] == "Trending-Up" for cell in grid)


def test_grid_accessor_never_raises():
    class Broken:
        def snapshot_grid(self):
            raise RuntimeError("nope")
    assert _sched(Broken()).grid() == []


# ----- telemetry payload ------------------------------------------------------
def test_regime_payload_and_summary():
    grid = [
        {"instrument": "EUR_USD", "granularity": "H1", "smoothed_label": "Trending-Up", "stale": False},
        {"instrument": "EUR_USD", "granularity": "H4", "smoothed_label": "Ranging", "stale": False},
        {"instrument": "USD_JPY", "granularity": "H1", "smoothed_label": "Trending-Up", "stale": False},
        {"instrument": "USD_JPY", "granularity": "H4", "smoothed_label": "High-Vol", "stale": True},
    ]
    rep = HealthReporter(regime_grid_fn=lambda: grid, clock=lambda: T0)

    payload = rep.regime()
    assert payload["service"] == "system-2-execution-engine"
    assert len(payload["grid"]) == 4
    assert payload["as_of"].endswith("Z")

    # summary: stale cells excluded; dominant is the most common live label.
    summary = rep.status()["regime"]
    assert summary["cells"] == 4
    assert summary["dominant"] == "Trending-Up"
    assert summary["by_label"] == {"Trending-Up": 2, "Ranging": 1}  # High-Vol was stale


def test_regime_defaults_empty_when_unset():
    rep = HealthReporter(clock=lambda: T0)
    assert rep.regime()["grid"] == []
    assert rep.status()["regime"]["cells"] == 0
    assert rep.status()["regime"]["dominant"] is None
