"""Drives the live regime detector on a schedule (EXEC-002 / telemetry-A).

The detector (`LiveRegimeDetector.detect`) is pure-on-demand; nothing woke it in the
running service, so `_last_obs` was always empty. This scheduler is that missing driver:
a daemon thread that, every ``REGIME_REFRESH_SEC``, computes the regime for each
(instrument, granularity) in the configured watch-list and lets the detector cache it.

Design posture (matches the rest of System 2):
  * **Fail-open — never touches the trading path.** A candle-fetch or model error logs and
    is skipped; one bad pair never stops the sweep, and the scheduler never raises into the
    engine. If it can't be built at all (no OANDA key in dev), it simply isn't started.
  * **Read-only + private.** It only reads candles and writes to the detector's in-memory
    cache; the grid is served on the read-only health surface.
  * **Config-driven watch-list**, no hard-coded instruments.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from system2.common.logging import get_logger, log_event
from system2.common.secrets import Secrets, get_secrets

log = get_logger("artifact_sync.regime_scheduler")

# Sensible defaults; every one is overridable from config.
_DEFAULT_INSTRUMENTS = "EUR_USD,GBP_USD,USD_JPY,USD_CHF,AUD_USD,USD_CAD,NZD_USD,EUR_GBP"
_DEFAULT_GRANULARITIES = "H1,H4"  # the model's trained set; unsupported ones degrade to stale


def _parse_list(raw: str | None, default: str) -> list[str]:
    items = [x.strip() for x in (raw or default).split(",")]
    return [x for x in items if x]


class RegimeScheduler:
    """Periodically refreshes the regime grid for a watch-list of instrument x granularity."""

    def __init__(
        self,
        detector: Any,
        instruments: list[str],
        granularities: list[str],
        refresh_sec: int = 60,
    ) -> None:
        self.detector = detector
        self.instruments = instruments
        self.granularities = granularities
        self.refresh_sec = max(5, int(refresh_sec))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @classmethod
    def from_secrets(cls, detector: Any, secrets: Secrets | None = None) -> "RegimeScheduler":
        secrets = secrets or get_secrets()
        return cls(
            detector=detector,
            instruments=_parse_list(secrets.get("REGIME_INSTRUMENTS"), _DEFAULT_INSTRUMENTS),
            granularities=_parse_list(secrets.get("REGIME_GRANULARITIES"), _DEFAULT_GRANULARITIES),
            refresh_sec=secrets.get_int("REGIME_REFRESH_SEC", 60),
        )

    # ----- one sweep --------------------------------------------------------
    def sweep_once(self) -> int:
        """Refresh every pair x granularity once. Returns the count refreshed. Never raises."""
        n = 0
        for instrument in self.instruments:
            for granularity in self.granularities:
                if self._stop.is_set():
                    return n
                try:
                    self.detector.detect(instrument, granularity)
                    n += 1
                except Exception as exc:  # fail-open: one bad pair never stops the sweep
                    log_event(log, logging.WARNING, "regime sweep item failed",
                              instrument=instrument, granularity=granularity,
                              error=type(exc).__name__, detail=str(exc))
        return n

    # ----- daemon lifecycle -------------------------------------------------
    def _loop(self) -> None:
        log_event(log, logging.INFO, "regime scheduler start",
                  instruments=len(self.instruments), granularities=self.granularities,
                  refresh_sec=self.refresh_sec)
        while not self._stop.is_set():
            try:
                self.sweep_once()
            except Exception as exc:  # last-resort guard: loop must never die
                log_event(log, logging.ERROR, "regime sweep failed", error=str(exc))
            self._stop.wait(self.refresh_sec)
        log_event(log, logging.INFO, "regime scheduler stopped")

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="regime-scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def grid(self) -> list[dict[str, Any]]:
        """The current regime for every observed cell (read-only, safe)."""
        try:
            return self.detector.snapshot_grid()
        except Exception:
            return []
