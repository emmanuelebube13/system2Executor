"""EXEC-011 tests — producer sweep: publish, dedup, shadow, heartbeat, fail-open."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from system2.live_signal_producer.dedup import SignalDedupStore
from system2.live_signal_producer.producer import HEARTBEAT_EVENT_TYPE, LiveSignalProducer
from system2.live_signal_producer.tests.conftest import (
    make_candles,
    make_manifest,
    write_champion_set,
)

PROBS = [0.6, 0.1, 0.2, 0.1]


class FakeSecrets:
    def __init__(self, values: dict | None = None):
        self._v = values or {}

    def get(self, name, default=None):
        return self._v.get(name, default)

    def get_bool(self, name, default=False):
        val = self._v.get(name)
        if val is None:
            return default
        return str(val).strip().lower() in {"1", "true", "yes", "on"}

    def get_int(self, name, default):
        try:
            return int(self._v[name])
        except (KeyError, ValueError):
            return default


class FakeQueue:
    def __init__(self, fail: bool = False):
        self.published: list[tuple[str, dict]] = []
        self.fail = fail

    def publish(self, topic, body):
        if self.fail:
            raise ConnectionError("queue unavailable")
        self.published.append((topic, body))


class FakeCandleSource:
    def __init__(self, fail_for: set[str] | None = None):
        self.fail_for = fail_for or set()

    def fetch_candles(self, instrument, granularity, count):
        if instrument in self.fail_for:
            raise ConnectionError(f"candle fetch down for {instrument}")
        return make_candles(count)


class FakeDetector:
    def __init__(self, label="Trending-Up", stale=False):
        self.label = label
        self.stale = stale

    def detect(self, instrument, granularity):
        return SimpleNamespace(smoothed_label=self.label, raw_probs=list(PROBS),
                               stale=self.stale, note=None)


def _make_root(tmp_path: Path, thresholds: float = 0.0) -> Path:
    """Artifact root with a real ``active`` dir (Windows-safe) + state.json set id."""
    root = tmp_path / "model-cache"
    manifest = make_manifest(dynamic_thresholds={
        "Trending-Up": thresholds, "Trending-Down": thresholds,
        "Ranging": thresholds, "High-Vol": thresholds, "fallback": thresholds,
    })
    write_champion_set(root / "active", manifest=manifest)
    (root / "state.json").write_text(json.dumps({"active_model_set_id": "set-A"}), encoding="utf-8")
    return root


def _make_producer(tmp_path: Path, root: Path, queue=None, detector=None,
                   candles=None, instruments="EUR_USD", enabled=True) -> LiveSignalProducer:
    secrets = FakeSecrets({
        "SIGNAL_INSTRUMENTS": instruments,
        "SIGNAL_GRANULARITIES": "H1",
        "LIVE_SIGNAL_ENABLED": "true" if enabled else "false",
        "SIGNAL_LEDGER_PATH": str(tmp_path / "ledger.db"),
    })
    return LiveSignalProducer(
        artifact_root=root,
        queue=queue if queue is not None else FakeQueue(),
        regime_detector=detector or FakeDetector(),
        secrets=secrets,
        candle_source=candles or FakeCandleSource(),
        dedup=SignalDedupStore(tmp_path / "dedup.db"),
    )


# ----- publish path ---------------------------------------------------------------
def test_sweep_publishes_one_signal_per_mapped_strategy(tmp_path: Path):
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue)

    assert producer.sweep_once() == 2  # Trending-Up EUR_USD -> strategies 10, 12
    signals = [b for _, b in queue.published if "event_type" not in b]
    assert len(signals) == 2
    for topic, body in queue.published[:2]:
        assert topic == "scored-signals"
        # FLAT wire shape per S3's ScoredSignal.schema.json (no envelope)
        assert body["schema_version"] == "1"
        assert body["signal_id"]
        assert body["pair"] == "EUR_USD"
        assert body["regime"] == "Trending-Up"
        assert body["granularity"] == "H1"
        assert 0.0 <= body["model_score"] <= 1.0
        assert isinstance(body["strategy_id"], str)
    assert {b["strategy_id"] for _, b in queue.published[:2]} == {"10", "12"}


def test_same_bar_never_publishes_twice(tmp_path: Path):
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue)
    assert producer.sweep_once() == 2
    assert producer.sweep_once() == 0  # same closed bar -> dedup holds
    signals = [b for _, b in queue.published if "event_type" not in b]
    assert len(signals) == 2


def test_gatekeeper_rejection_publishes_nothing(tmp_path: Path):
    root = _make_root(tmp_path, thresholds=1.1)  # unreachable threshold
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue)
    assert producer.sweep_once() == 0
    assert [b for _, b in queue.published if "event_type" not in b] == []


def test_unqualified_regime_cell_produces_nothing(tmp_path: Path):
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue,
                              detector=FakeDetector(label="High-Vol"))  # mapped to []
    assert producer.sweep_once() == 0
    assert [b for _, b in queue.published if "event_type" not in b] == []


def test_stale_regime_skips_cell(tmp_path: Path):
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue,
                              detector=FakeDetector(stale=True))
    assert producer.sweep_once() == 0
    assert [b for _, b in queue.published if "event_type" not in b] == []


# ----- shadow mode ---------------------------------------------------------------
def test_shadow_mode_runs_pipeline_but_sends_nothing(tmp_path: Path):
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue, enabled=False)
    # the system2 root logger has propagate=False, so capture on the module logger itself
    records: list[logging.LogRecord] = []

    class _Collector(logging.Handler):
        def emit(self, record):
            records.append(record)

    collector = _Collector(level=logging.INFO)
    producer_log = logging.getLogger("system2.live_signal_producer.producer")
    producer_log.addHandler(collector)
    try:
        assert producer.sweep_once() == 0
    finally:
        producer_log.removeHandler(collector)

    assert queue.published == []  # nothing on the queue, not even a heartbeat
    shadow_lines = [r.getMessage() for r in records if "[SHADOW] would publish:" in r.getMessage()]
    assert len(shadow_lines) == 2
    assert "pair=EUR_USD" in shadow_lines[0]


# ----- heartbeat ------------------------------------------------------------------
def test_heartbeat_when_no_signals(tmp_path: Path):
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue,
                              detector=FakeDetector(label="High-Vol"))
    producer.sweep_once()
    heartbeats = [(t, b) for t, b in queue.published
                  if b.get("event_type") == HEARTBEAT_EVENT_TYPE]
    assert len(heartbeats) == 1
    topic, hb_body = heartbeats[0]
    assert topic == "scored-signals.heartbeat"  # own topic: S3's validator DLQs non-signals
    hb = hb_body["payload"]
    assert hb["pairs_watched"] == 1
    assert hb["last_signal_at"] is None
    assert hb["model_set_id"] == "set-A"


def test_no_heartbeat_right_after_a_signal(tmp_path: Path):
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue)
    producer.sweep_once()  # publishes signals -> outbound is fresh
    assert [b for _, b in queue.published if b.get("event_type") == HEARTBEAT_EVENT_TYPE] == []


def test_heartbeat_after_four_quiet_minutes(tmp_path: Path):
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue)
    producer.sweep_once()
    producer._last_outbound_at = datetime.now(timezone.utc) - timedelta(seconds=300)
    producer.sweep_once()  # same bar: no signals, but quiet > 240s -> heartbeat
    heartbeats = [b for _, b in queue.published if b.get("event_type") == HEARTBEAT_EVENT_TYPE]
    assert len(heartbeats) == 1
    assert heartbeats[0]["payload"]["last_signal_at"] is not None


# ----- fail-open -------------------------------------------------------------------
def test_one_failing_instrument_never_stops_the_sweep(tmp_path: Path):
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(
        tmp_path, root, queue=queue, instruments="EUR_USD,GBP_USD",
        candles=FakeCandleSource(fail_for={"EUR_USD"}),
    )
    assert producer.sweep_once() == 1  # GBP_USD Trending-Up -> strategy 10 still flows
    signals = [b for _, b in queue.published if "event_type" not in b]
    assert [b["pair"] for b in signals] == ["GBP_USD"]


def test_queue_outage_is_survived(tmp_path: Path):
    root = _make_root(tmp_path)
    producer = _make_producer(tmp_path, root, queue=FakeQueue(fail=True))
    assert producer.sweep_once() == 0  # logged, not raised


def test_missing_artifacts_idle_gracefully(tmp_path: Path):
    root = tmp_path / "empty-cache"
    root.mkdir()
    producer = _make_producer(tmp_path, root)
    assert producer.sweep_once() == 0


# ----- hot reload -------------------------------------------------------------------
def test_hot_reload_on_model_set_change(tmp_path: Path):
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue)
    producer.sweep_once()
    assert producer._model_set_id == "set-A"

    # Rollover: same active dir, new set id + a map that unmaps everything.
    (root / "active" / "regime_strategy_map.json").write_text(
        json.dumps({"EUR_USD": {"Trending-Up": []}}), encoding="utf-8")
    (root / "state.json").write_text(json.dumps({"active_model_set_id": "set-B"}), encoding="utf-8")

    before = len([b for _, b in queue.published if "event_type" not in b])
    producer.sweep_once()
    assert producer._model_set_id == "set-B"  # reloaded without restart
    assert producer.book.strategies_for("EUR_USD", "Trending-Up") == []
    after = len([b for _, b in queue.published if "event_type" not in b])
    assert after == before


def test_shutdown_stops_loop_and_closes_dedup(tmp_path: Path):
    root = _make_root(tmp_path)
    producer = _make_producer(tmp_path, root)
    producer.shutdown()
    assert producer._stop.is_set()
    with pytest.raises(Exception):
        producer.dedup.claim("k", "s", "t")  # connection closed


# ----- OBS-001 ledger integration -------------------------------------------------
def test_sweep_writes_persistent_ledger_rows(tmp_path: Path):
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue)
    produced = producer.sweep_once()
    assert produced >= 1
    agg = producer.ledger.daily_aggregates()
    assert agg is not None
    assert agg["total_evaluations"] >= produced
    assert agg["total_published"] == produced
    # measured approval rate is defined and within [0, 1]
    assert 0.0 <= agg["measured_approval_rate"] <= 1.0
    # a fresh producer over the same secrets sees the same history (restart survival)
    producer2 = _make_producer(tmp_path, root, queue=FakeQueue())
    agg2 = producer2.ledger.daily_aggregates()
    assert agg2["total_evaluations"] == agg["total_evaluations"]
    # /signal surface carries the ledger block
    snap = producer.telemetry_snapshot()
    assert snap["ledger"]["total_published"] == produced


# ----- FIX_PLAN 2.1(d): runtime approval-rate monitor wiring -----------------------
def test_sweep_feeds_the_approval_monitor_and_publishes_it_on_signal(tmp_path: Path):
    """Every gatekeeper verdict must reach the monitor, and the monitor must be visible
    on the same /signal payload ``bridge/ops_watchdog.py`` already polls."""
    root = _make_root(tmp_path)          # thresholds 0.0 => everything approves
    producer = _make_producer(tmp_path, root)
    produced = producer.sweep_once()
    assert produced >= 1

    snap = producer.telemetry_snapshot()["approval_monitor"]
    assert snap["evaluations"] == produced
    assert snap["approved"] == produced          # threshold 0.0: all approved
    assert snap["approval_rate"] == 1.0
    assert snap["state"] == "warming_up"         # far below the 50-sample floor
    assert snap["alarm"] is False                # ...so it does NOT cry wolf


def test_approval_monitor_alarms_once_it_has_enough_evidence(tmp_path: Path):
    """Drive the producer's own monitor past the floor with an all-approve stream."""
    root = _make_root(tmp_path)
    producer = _make_producer(tmp_path, root)
    for _ in range(120):
        producer.approval_monitor.observe(True)

    snap = producer.telemetry_snapshot()["approval_monitor"]
    assert snap["state"] == "out_of_band"
    assert snap["alarm"] is True
    assert snap["band"] == [0.05, 0.60]          # the default; this manifest omits it
    assert producer._poll_approval_alarm()["event"] == "opened"
    assert producer._poll_approval_alarm() is None   # throttled, not once per sweep


def test_band_is_adopted_from_the_champion_manifest(tmp_path: Path):
    """The band is System 1's, shipped in the manifest — never a second S2 opinion."""
    root = tmp_path / "banded-cache"
    manifest = make_manifest(turnover_band=[0.10, 0.42])
    write_champion_set(root / "active", manifest=manifest)
    (root / "state.json").write_text(json.dumps({"active_model_set_id": "set-A"}),
                                     encoding="utf-8")
    producer = _make_producer(tmp_path, root)
    producer.sweep_once()
    assert producer.gatekeeper.turnover_band == [0.10, 0.42]
    assert producer.approval_monitor.snapshot()["band"] == [0.10, 0.42]
    assert producer.approval_monitor.snapshot()["band_source"] == "manifest"


def test_monitor_is_seeded_from_the_ledger_across_a_restart(tmp_path: Path):
    root = _make_root(tmp_path)
    first = _make_producer(tmp_path, root)
    produced = first.sweep_once()
    assert produced >= 1

    restarted = _make_producer(tmp_path, root)      # same SIGNAL_LEDGER_PATH
    snap = restarted.approval_monitor.snapshot()
    assert snap["seeded_from_ledger"] is True
    assert snap["evaluations"] >= produced


def test_alarm_alone_does_not_stop_trading(tmp_path: Path):
    """Fail-LOUD is the default: a breached band alarms, it does not withhold signals."""
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue)
    for _ in range(200):
        producer.approval_monitor.observe(True)
    assert producer.approval_monitor.alarming is True
    assert producer.approval_monitor.enforcing is False

    assert producer.sweep_once() == 2                       # unchanged behaviour
    assert len([b for _, b in queue.published if "event_type" not in b]) == 2


def test_enforcement_when_armed_withholds_approved_signals(tmp_path: Path):
    """Fail-CLOSED is a config flip, not a code change: SIGNAL_APPROVAL_BAND_ENFORCE."""
    root = _make_root(tmp_path)
    queue = FakeQueue()
    producer = _make_producer(tmp_path, root, queue=queue)
    producer.approval_monitor.enforcing = True
    for _ in range(200):
        producer.approval_monitor.observe(True)

    assert producer.sweep_once() == 0
    assert [b for _, b in queue.published if "event_type" not in b] == []
    # the withheld evaluation is still recorded: approved by the gate, never published
    agg = producer.ledger.daily_aggregates()
    assert agg["total_approved"] == 2 and agg["total_published"] == 0


def test_enforcement_flag_is_read_from_secrets(tmp_path: Path):
    root = _make_root(tmp_path)
    secrets = FakeSecrets({
        "SIGNAL_INSTRUMENTS": "EUR_USD", "SIGNAL_GRANULARITIES": "H1",
        "SIGNAL_LEDGER_PATH": str(tmp_path / "ledger2.db"),
        "SIGNAL_APPROVAL_BAND_ENFORCE": "true",
        "SIGNAL_APPROVAL_WINDOW": "50", "SIGNAL_APPROVAL_MIN_SAMPLES": "10",
    })
    producer = LiveSignalProducer(
        artifact_root=root, queue=FakeQueue(), regime_detector=FakeDetector(),
        secrets=secrets, candle_source=FakeCandleSource(),
        dedup=SignalDedupStore(tmp_path / "dedup2.db"))
    snap = producer.approval_monitor.snapshot()
    assert snap["enforcing"] is True
    assert snap["window"] == 50 and snap["min_samples"] == 10


def test_shadow_mode_still_reports_the_approval_monitor(tmp_path: Path):
    """Shadow still SCORES, so the gate is still measurable — and FIX_PLAN 2.1 ships
    the recalibration in shadow. A drifted gate must not go invisible there."""
    root = _make_root(tmp_path)
    producer = _make_producer(tmp_path, root, enabled=False)
    producer.sweep_once()
    snap = producer.telemetry_snapshot()
    assert snap["running"] is False
    assert snap["approval_monitor"]["evaluations"] >= 1


def test_monitor_failure_never_breaks_the_health_surface(tmp_path: Path):
    root = _make_root(tmp_path)
    producer = _make_producer(tmp_path, root)

    class Exploding:
        def snapshot(self):
            raise RuntimeError("boom")

    producer.approval_monitor = Exploding()
    snap = producer.approval_monitor_snapshot()
    assert snap["alarm"] is False and "monitor unavailable" in snap["reason"]
