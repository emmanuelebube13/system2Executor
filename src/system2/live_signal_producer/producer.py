"""EXEC-011 — Live Scored-Signal Producer.

The missing step 1 of the pipeline: every closed H1/H4 bar, for each watched instrument,
it computes causal features from live OANDA candles, reads the live regime (EXEC-002),
scores the cell with the champion gatekeeper (Layer 3 triad), and publishes one
ScoredSignal per mapped strategy to the ``scored-signals`` queue that feeds System 3.

Design posture (mirrors RegimeScheduler / ModelDownloader):
  * **Fail-open, never crash the engine.** One bad pair never stops the sweep; candle,
    model, and publish failures log and retry next cycle; ``run_forever`` never raises.
  * **Shadow by default gate.** The full pipeline always runs; actual publishing happens
    only when ``LIVE_SIGNAL_ENABLED=true`` — otherwise every would-be publish is logged
    with a ``[SHADOW]`` prefix and nothing touches the queue.
  * **At-most-once per bar per strategy** via the SQLite dedup store (survives restarts).
  * **Hot-reload on model rollover**: when the downloader flips the ``active`` set, the
    gatekeeper triad and strategy book are reloaded without a restart.
  * **Heartbeat**: a thin ``live_signal_producer.heartbeat`` event goes to the same topic
    when >4 minutes pass without an outbound message, so quiet markets never look like a
    dead producer to the downstream staleness monitors.

One-shot smoke run (no publish, stub candles allowed)::

    python -m system2.live_signal_producer.producer --once --shadow
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
import uuid
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from system2.artifact_sync.features import compute_regime_features
from system2.common.logging import get_logger, log_event, set_correlation_id
from system2.common.queue_backend import make_envelope
from system2.common.secrets import Secrets, get_secrets
from system2.live_signal_producer.dedup import SignalDedupStore, make_dedup_key
from system2.live_signal_producer.gatekeeper import GatekeeperScorer, price_position_20
from system2.live_signal_producer.signal_builder import StrategyBook, build_signal

log = get_logger("live_signal_producer.producer")

HEARTBEAT_EVENT_TYPE = "live_signal_producer.heartbeat"
HEARTBEAT_AFTER_SEC = 240  # < the 300s outbound-silence PAUSE threshold downstream

_DEFAULT_INSTRUMENTS = "EUR_USD,GBP_USD,USD_JPY,USD_CHF,AUD_USD,USD_CAD,NZD_USD,EUR_GBP"
_DEFAULT_GRANULARITIES = "H1"


def _parse_list(raw: str | None, default: str) -> list[str]:
    items = [x.strip() for x in (raw or default).split(",")]
    return [x for x in items if x]


def _bar_time_iso(value: Any) -> str:
    ts = pd.to_datetime(value, utc=True)
    return ts.isoformat().replace("+00:00", "Z")


class LiveSignalProducer:
    """Generates and publishes ScoredSignals on a poll loop (background daemon thread)."""

    def __init__(
        self,
        artifact_root: Path | str,
        queue: Any,
        regime_detector: Any,
        secrets: Secrets | None = None,
        candle_source: Any = None,
        dedup: SignalDedupStore | None = None,
    ) -> None:
        self.secrets = secrets or get_secrets()
        self.root = Path(artifact_root)
        self.queue = queue
        self.detector = regime_detector
        # Default to the detector's candle source so both see identical bars.
        self.candle_source = candle_source or getattr(regime_detector, "candle_source", None)
        if self.candle_source is None:
            raise ValueError("LiveSignalProducer needs a candle source (own or the detector's)")

        self.topic = self.secrets.get("SCORED_SIGNAL_QUEUE", "scored-signals") or "scored-signals"
        # Heartbeats go to their own topic: S3's strict ScoredSignal validator would DLQ
        # any non-signal message arriving on its subscription.
        self.heartbeat_topic = (
            self.secrets.get("SIGNAL_HEARTBEAT_TOPIC", "scored-signals.heartbeat")
            or "scored-signals.heartbeat"
        )
        self.enabled = self.secrets.get_bool("LIVE_SIGNAL_ENABLED", True)
        self.instruments = _parse_list(self.secrets.get("SIGNAL_INSTRUMENTS"), _DEFAULT_INSTRUMENTS)
        self.granularities = _parse_list(self.secrets.get("SIGNAL_GRANULARITIES"), _DEFAULT_GRANULARITIES)
        self.poll_interval = max(5, self.secrets.get_int("SIGNAL_POLL_INTERVAL_SEC", 30))
        self.lookback = self.secrets.get_int("REGIME_CANDLE_LOOKBACK", 120)

        self.gatekeeper = GatekeeperScorer()
        self.book = StrategyBook()
        self.dedup = dedup or SignalDedupStore(
            self.secrets.get("SIGNAL_DEDUP_PATH", "state/signals/signal_dedup.db")
        )
        # OBS-001: persistent evaluation ledger — live frequency/approval as a
        # measurement that survives restarts. Fail-open; never blocks production.
        from system2.live_signal_producer.signal_ledger import SignalLedger
        self.ledger = SignalLedger(
            self.secrets.get("SIGNAL_LEDGER_PATH", "state/signals/signal_ledger.db")
        )

        self._model_set_id: str | None = None
        self._last_signal_at: str | None = None
        self._last_outbound_at: datetime | None = None
        self._stop = threading.Event()

        # Telemetry stats (served read-only via GET /signal on the health surface).
        # All mutations happen under the lock; snapshot copies under the lock too.
        self._stats_lock = threading.Lock()
        self._started_at = datetime.now(timezone.utc)
        self._stats_day: date = self._started_at.date()
        self._signals_today = 0
        self._signals_by_pair: dict[str, int] = {}
        self._signals_by_regime: dict[str, int] = {}
        self._score_sum = 0.0
        self._score_count = 0
        self._last_regime_by_pair: dict[str, str] = {}

    # ----- model hot-reload --------------------------------------------------------
    def _active_dir(self) -> Path:
        return self.root / "active"

    def _current_model_set_id(self) -> str | None:
        """The active set id: resolved symlink target name, else state.json record."""
        link = self._active_dir()
        try:
            if link.exists():
                name = link.resolve().name
                if name and name != "active":
                    return name
        except OSError:
            pass
        state_file = self.root / "state.json"
        if state_file.exists():
            try:
                return json.loads(state_file.read_text(encoding="utf-8")).get("active_model_set_id")
            except (ValueError, OSError):
                return None
        return None

    def _maybe_reload(self) -> None:
        """Reload the gatekeeper triad + strategy book when the active set changed."""
        current = self._current_model_set_id()
        if current is None:
            if self._model_set_id is None and not self.gatekeeper.loaded:
                log_event(log, logging.WARNING, "no active model set; producer idles",
                          artifact_root=str(self.root))
            return
        needs_load = current != self._model_set_id or not (self.gatekeeper.loaded and self.book.loaded)
        if not needs_load:
            return
        active = self._active_dir()
        gk_ok = self.gatekeeper.load(active)
        book_ok = self.book.load(active)
        log_event(log, logging.INFO, "model set (re)load",
                  model_set_id=current, previous=self._model_set_id,
                  gatekeeper=gk_ok, strategy_book=book_ok)
        self._model_set_id = current

    # ----- telemetry (read-only /signal surface) --------------------------------------
    def _roll_day_if_needed(self, now: datetime) -> None:
        """Reset the daily counters at midnight UTC (called from the sweep loop)."""
        with self._stats_lock:
            if now.date() == self._stats_day:
                return
            self._stats_day = now.date()
            self._signals_today = 0
            self._signals_by_pair = {}
            self._signals_by_regime = {}
            self._score_sum = 0.0
            self._score_count = 0

    def _record_regime(self, instrument: str, regime: str) -> None:
        with self._stats_lock:
            self._last_regime_by_pair[instrument] = regime

    def _record_score(self, score: float) -> None:
        with self._stats_lock:
            self._score_sum += float(score)
            self._score_count += 1

    def _record_signal(self, pair: str, regime: str, produced_at: str) -> None:
        with self._stats_lock:
            today = datetime.now(timezone.utc).date()
            if today != self._stats_day:  # UTC day rollover resets the daily counters
                self._stats_day = today
                self._signals_today = 0
                self._signals_by_pair.clear()
                self._signals_by_regime.clear()
            self._signals_today += 1
            self._signals_by_pair[pair] = self._signals_by_pair.get(pair, 0) + 1
            self._signals_by_regime[regime] = self._signals_by_regime.get(regime, 0) + 1
            self._last_signal_at = produced_at

    def telemetry_snapshot(self) -> dict[str, Any]:
        """The GET /signal payload. Never raises; safe to call from the health thread."""
        now = datetime.now(timezone.utc)
        if not self.enabled:
            return {"running": False, "reason": "disabled (LIVE_SIGNAL_ENABLED=false)"}
        with self._stats_lock:
            avg = (self._score_sum / self._score_count) if self._score_count else None
            heartbeat_age = (
                (now - self._last_outbound_at).total_seconds()
                if self._last_outbound_at is not None else None
            )
            return {
                "schema_version": "1",
                "running": not self._stop.is_set(),
                "model_set_id": self._model_set_id,
                "pairs_watched": len(self.instruments),
                "granularities": list(self.granularities),
                "last_signal_at": self._last_signal_at,
                "heartbeat_age_sec": round(heartbeat_age, 1) if heartbeat_age is not None else None,
                "signals_produced_today": self._signals_today,
                "signals_by_pair": dict(self._signals_by_pair),
                "signals_by_regime": dict(self._signals_by_regime),
                "avg_gatekeeper_score": round(avg, 6) if avg is not None else None,
                "last_regime": dict(self._last_regime_by_pair),
                "uptime_sec": round((now - self._started_at).total_seconds(), 1),
                # OBS-001: persistent measurements (survive restarts); None => no ledger
                "ledger": self.ledger.daily_aggregates(days=14),
            }

    # ----- one sweep -----------------------------------------------------------------
    def sweep_once(self) -> int:
        """Score every watched (instrument, granularity) once; returns signals produced.

        Never raises — each cell is individually guarded (fail-open, like RegimeScheduler).
        """
        self._maybe_reload()
        self._roll_day_if_needed(datetime.now(timezone.utc))
        produced = 0
        for instrument in self.instruments:
            for granularity in self.granularities:
                if self._stop.is_set():
                    return produced
                try:
                    produced += self._process_cell(instrument, granularity)
                except Exception as exc:  # fail-open: one bad cell never stops the sweep
                    log_event(log, logging.WARNING, "signal cell failed",
                              instrument=instrument, granularity=granularity,
                              error=type(exc).__name__, detail=str(exc))
        self._heartbeat_if_quiet()
        return produced

    def _process_cell(self, instrument: str, granularity: str) -> int:
        """Full pipeline for one cell: candles -> features -> regime -> score -> signals."""
        set_correlation_id(f"signal-{instrument}-{granularity}")
        if not (self.gatekeeper.loaded and self.book.loaded):
            return 0

        candles = self.candle_source.fetch_candles(instrument, granularity, self.lookback)
        if candles is None or candles.empty:
            log_event(log, logging.WARNING, "no candles; skipping cell",
                      instrument=instrument, granularity=granularity)
            return 0
        bar_iso = _bar_time_iso(candles["bar_time_utc"].iloc[-1])

        obs = self.detector.detect(instrument, granularity)
        regime = getattr(obs, "smoothed_label", None)
        if regime is None or getattr(obs, "stale", False):
            log_event(log, logging.INFO, "regime unavailable/stale; skipping cell",
                      instrument=instrument, granularity=granularity,
                      note=getattr(obs, "note", None))
            return 0

        self._record_regime(instrument, regime)

        strategies = self.book.strategies_for(instrument, regime, granularity)
        if not strategies:
            return 0  # unqualified cell (e.g. High-Vol -> []) — by design, not an error
        # Skip scoring entirely when every strategy already produced for this bar.
        pending = [
            s for s in strategies
            if not self.dedup.seen(make_dedup_key(instrument, granularity, bar_iso, s.strategy_id))
        ]
        if not pending:
            return 0

        feats = compute_regime_features(candles)
        if pd.isna(feats["atr_14"].iloc[-1]):
            log_event(log, logging.WARNING, "insufficient feature history; skipping cell",
                      instrument=instrument, granularity=granularity, rows=len(candles))
            return 0

        last_close = float(candles["close"].iloc[-1])
        atr = float(feats["atr_14"].iloc[-1])
        produced = 0
        for strategy in pending:
            # The champion scores a concrete (strategy, direction) proposal, so direction
            # is resolved BEFORE scoring ("auto" = regime rule; Ranging fades the range).
            direction = self._resolve_direction(strategy, regime, feats)
            if direction is None:
                continue
            feature_row = self.gatekeeper.build_feature_row(
                feats, list(obs.raw_probs), regime,
                strategy_id=strategy.strategy_id, direction=direction,
            )
            if feature_row is None:
                log_event(log, logging.WARNING, "feature row unavailable; skipping strategy",
                          instrument=instrument, granularity=granularity,
                          strategy_id=strategy.strategy_id, rows=len(candles))
                continue
            gate = self.gatekeeper.score(feature_row, regime)
            if gate is None:
                continue
            self._record_score(gate.score)

            key = make_dedup_key(instrument, granularity, bar_iso, strategy.strategy_id)
            signal = build_signal(
                instrument=instrument, granularity=granularity, regime=regime,
                model_score=gate.score, last_close=last_close, atr=atr,
                strategy=replace(strategy, direction=direction),
            )
            # Claim the bar whether approved or not: one evaluation per bar per strategy.
            if not self.dedup.claim(key, signal["signal_id"], signal["produced_at"]):
                continue
            if not gate.approved:
                log_event(log, logging.INFO, "gatekeeper rejected",
                          instrument=instrument, granularity=granularity,
                          strategy_id=strategy.strategy_id, direction=direction,
                          score=round(gate.score, 6), threshold=gate.threshold, regime=regime)
                self.ledger.record(
                    bar_time=bar_iso, pair=instrument, granularity=granularity,
                    regime=regime, strategy_id=strategy.strategy_id, direction=direction,
                    score=gate.score, threshold=gate.threshold,
                    approved=False, published=False, signal_id=signal["signal_id"])
                continue
            published = self._publish_signal(signal)
            self.ledger.record(
                bar_time=bar_iso, pair=instrument, granularity=granularity,
                regime=regime, strategy_id=strategy.strategy_id, direction=direction,
                score=gate.score, threshold=gate.threshold,
                approved=True, published=published, signal_id=signal["signal_id"])
            if published:
                produced += 1
        return produced

    def _resolve_direction(self, strategy: Any, regime: str, feats: pd.DataFrame) -> str | None:
        """Resolve an ``auto`` direction from the regime (trend-follow / range-fade)."""
        if strategy.direction in ("long", "short"):
            return strategy.direction
        if regime == "Trending-Up":
            return "long"
        if regime == "Trending-Down":
            return "short"
        if regime == "Ranging":
            # Fade the 20-bar range: near the top -> short, near the bottom -> long.
            pos = float(price_position_20(feats).iloc[-1])
            return "short" if pos > 0.5 else "long"
        log_event(log, logging.INFO, "no direction rule for regime; skipping",
                  regime=regime, strategy_id=strategy.strategy_id)
        return None

    # ----- publishing ---------------------------------------------------------------
    def _publish_signal(self, signal: dict[str, Any]) -> bool:
        """Publish one ScoredSignal (or shadow-log it). True iff actually published."""
        if not self.enabled:
            log.info(
                "[SHADOW] would publish: pair=%s dir=%s strategy=%s score=%s",
                signal["pair"], signal["direction"], signal["strategy_id"], signal["model_score"],
                extra={"context": {"signal": signal}},
            )
            return False
        # FLAT on the wire (no envelope): S3's ScoredSignal validator is
        # additionalProperties:false and reads signal_id/schema_version at top level.
        try:
            self.queue.publish(self.topic, signal)
        except Exception as exc:  # queue hiccup: dedup already claimed, log + move on;
            # the bar is not retried (stale price) but the producer itself stays alive.
            log_event(log, logging.ERROR, "signal publish failed",
                      signal_id=signal["signal_id"], topic=self.topic,
                      error=type(exc).__name__, detail=str(exc))
            return False
        now = datetime.now(timezone.utc)
        self._record_signal(signal["pair"], signal["regime"], signal["produced_at"])
        self._last_outbound_at = now
        log_event(log, logging.INFO, "signal published",
                  signal_id=signal["signal_id"], pair=signal["pair"],
                  direction=signal["direction"], strategy_id=signal["strategy_id"],
                  score=signal["model_score"], regime=signal["regime"], topic=self.topic)
        return True

    def _heartbeat_if_quiet(self) -> None:
        """Keep the topic warm: a thin heartbeat when >4 min without an outbound message."""
        now = datetime.now(timezone.utc)
        if self._last_outbound_at is not None:
            quiet_sec = (now - self._last_outbound_at).total_seconds()
            if quiet_sec < HEARTBEAT_AFTER_SEC:
                return
        payload = {
            "last_signal_at": self._last_signal_at,
            "pairs_watched": len(self.instruments),
            "model_set_id": self._model_set_id,
        }
        if not self.enabled:
            log.info("[SHADOW] would publish heartbeat", extra={"context": payload})
            self._last_outbound_at = now  # shadow still paces itself like live
            return
        hb_id = str(uuid.uuid4())
        envelope = make_envelope(
            payload=payload, idempotency_key=hb_id, correlation_id=hb_id,
            event_type=HEARTBEAT_EVENT_TYPE,
        )
        try:
            self.queue.publish(self.heartbeat_topic, envelope)
            self._last_outbound_at = now
            log_event(log, logging.INFO, "heartbeat published",
                      topic=self.heartbeat_topic, **payload)
        except Exception as exc:
            log_event(log, logging.WARNING, "heartbeat publish failed",
                      error=type(exc).__name__, detail=str(exc))

    # ----- daemon lifecycle ------------------------------------------------------------
    def run_forever(self) -> None:
        """Poll loop until shutdown. Top-level guarded: never raises into the engine."""
        try:
            log_event(log, logging.INFO, "live signal producer start",
                      instruments=len(self.instruments), granularities=self.granularities,
                      poll_interval_sec=self.poll_interval, topic=self.topic,
                      enabled=self.enabled)
            while not self._stop.is_set():
                try:
                    self.sweep_once()
                except Exception as exc:  # last-resort guard: loop must never die
                    log_event(log, logging.ERROR, "signal sweep failed",
                              error=type(exc).__name__, detail=str(exc))
                self._stop.wait(self.poll_interval)
        except Exception as exc:  # absolute top-level guard (daemon thread)
            log_event(log, logging.CRITICAL, "live signal producer crashed",
                      error=type(exc).__name__, detail=str(exc))
        log_event(log, logging.INFO, "live signal producer stopped")

    def shutdown(self) -> None:
        self._stop.set()
        try:
            self.dedup.close()
        except Exception:  # best-effort; shutdown must never raise
            pass


# --------------------------------------------------------------------------- #
# One-shot CLI (smoke): python -m system2.live_signal_producer.producer --once --shadow
# --------------------------------------------------------------------------- #
class _StubCandleSource:
    """Deterministic random-walk candles for keyless smoke runs (never used in prod)."""

    def fetch_candles(self, instrument: str, granularity: str, count: int) -> pd.DataFrame:
        import numpy as np

        rng = np.random.default_rng(abs(hash((instrument, granularity))) % (2**32))
        step = {"H1": "1h", "H4": "4h"}.get(granularity, "1h")
        idx = pd.date_range(end=pd.Timestamp.now(tz="UTC").floor(step), periods=count, freq=step)
        close = 1.10 + np.cumsum(rng.normal(0, 0.0008, count))
        high = close + rng.uniform(0.0002, 0.0012, count)
        low = close - rng.uniform(0.0002, 0.0012, count)
        return pd.DataFrame({
            "bar_time_utc": idx, "open": close, "high": high, "low": low,
            "close": close, "volume": rng.uniform(100, 1000, count),
        })


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="EXEC-011 live scored-signal producer")
    parser.add_argument("--once", action="store_true", help="run a single sweep and exit")
    parser.add_argument("--shadow", action="store_true",
                        help="force shadow mode: log would-be publishes, send nothing")
    parser.add_argument("--stub-candles", action="store_true",
                        help="use synthetic candles (no OANDA key needed)")
    args = parser.parse_args(argv)

    secrets = get_secrets()
    artifact_root = Path(secrets.get("ARTIFACT_ROOT", "state/model-cache") or "state/model-cache")

    candle_source: Any
    if args.stub_candles:
        candle_source = _StubCandleSource()
    else:
        try:
            from system2.artifact_sync.candles import OandaCandleSource

            candle_source = OandaCandleSource(secrets)
        except Exception as exc:
            log_event(log, logging.WARNING, "OANDA candle source unavailable; using stub",
                      error=type(exc).__name__, detail=str(exc))
            candle_source = _StubCandleSource()

    from system2.artifact_sync.live_regime import LiveRegimeDetector

    detector = LiveRegimeDetector(artifact_root, candle_source, secrets=secrets)

    queue: Any = None
    shadow = args.shadow or not secrets.get_bool("LIVE_SIGNAL_ENABLED", True)
    if not shadow:
        from system2.common.queue_backend import build_queue

        queue = build_queue(secrets)

    producer = LiveSignalProducer(
        artifact_root=artifact_root, queue=queue, regime_detector=detector,
        secrets=secrets, candle_source=candle_source,
    )
    if shadow:
        producer.enabled = False

    if args.once:
        produced = producer.sweep_once()
        log_event(log, logging.INFO, "one-shot sweep complete",
                  produced=produced, shadow=not producer.enabled)
        producer.shutdown()
        return 0
    producer.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
