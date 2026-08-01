"""OBS-001 — persistent scored-signal ledger.

The producer's in-memory /signal counters die with the process, which makes the
operator's central question — how often does this system actually signal, and at
what approval rate? — unanswerable. This ledger appends one row per CLAIMED
evaluation (the dedup store's one-per-bar-per-strategy unit) to a small SQLite
file, so live frequency becomes a measurement instead of an extrapolation.

Fail-open by construction: every method swallows and logs; a broken ledger can
never delay or block signal production. Rows are only ever appended (no updates,
no deletes); the file is size-bounded by reality (~a few hundred rows/week).
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time as _time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from system2.common.logging import get_logger, log_event

log = get_logger("live_signal_producer.signal_ledger")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS signal_evaluations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,             -- evaluation wall-clock (UTC ISO)
    bar_time     TEXT,                      -- the H1/H4 bar being evaluated
    pair         TEXT NOT NULL,
    granularity  TEXT NOT NULL,
    regime       TEXT,
    strategy_id  TEXT,
    direction    TEXT,
    score        REAL,
    threshold    REAL,
    approved     INTEGER NOT NULL,          -- gatekeeper verdict
    published    INTEGER NOT NULL,          -- actually reached the queue (0 in shadow)
    signal_id    TEXT
);
CREATE INDEX IF NOT EXISTS idx_signal_evaluations_ts ON signal_evaluations(ts);
"""


class SignalLedger:
    """Append-only evaluation ledger + read-side daily aggregates."""

    AGG_CACHE_SEC = 30.0  # /signal polls every few seconds; aggregates don't move that fast

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._agg_cache: tuple[float, dict[str, Any] | None] | None = None
        self._ok = False
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as conn:
                conn.executescript(_SCHEMA)
            self._ok = True
        except Exception as exc:  # producer must run fine without a ledger
            log_event(log, logging.ERROR, "signal ledger unavailable (producer unaffected)",
                      path=str(self.path), error=str(exc))

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=3)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    # ---- write side (called from the sweep thread) ---------------------------
    def record(
        self,
        *,
        bar_time: str | None,
        pair: str,
        granularity: str,
        regime: str | None,
        strategy_id: Any,
        direction: str | None,
        score: float | None,
        threshold: float | None,
        approved: bool,
        published: bool,
        signal_id: str | None,
    ) -> None:
        """Append one evaluation row. Never raises."""
        if not self._ok:
            return
        try:
            with self._lock, self._connect() as conn:
                conn.execute(
                    "INSERT INTO signal_evaluations(ts, bar_time, pair, granularity, regime,"
                    " strategy_id, direction, score, threshold, approved, published, signal_id)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                     bar_time, pair, granularity, regime,
                     str(strategy_id) if strategy_id is not None else None,
                     direction,
                     float(score) if score is not None else None,
                     float(threshold) if threshold is not None else None,
                     1 if approved else 0, 1 if published else 0, signal_id),
                )
        except Exception as exc:
            log_event(log, logging.WARNING, "signal ledger write failed (ignored)",
                      error=str(exc))

    # ---- read side (called from the health thread) ---------------------------
    def recent_verdicts(self, limit: int = 200) -> list[bool]:
        """The last ``limit`` gatekeeper verdicts, oldest-first.

        Seeds the runtime approval-rate monitor (FIX_PLAN 2.1(d)) at startup so the
        measurement survives a restart. Without it, a process that restarts often
        enough would never accumulate the monitor's minimum sample count and a drifted
        model could stay permanently unjudged — the same "nobody was looking" shape
        that let the 0.9995 live approval rate run for weeks. Never raises; an
        unusable ledger just means the monitor starts cold.
        """
        if not self._ok:
            return []
        try:
            with self._lock, self._connect() as conn:
                rows = conn.execute(
                    "SELECT approved FROM signal_evaluations ORDER BY id DESC LIMIT ?",
                    (max(0, int(limit)),),
                ).fetchall()
        except Exception as exc:
            log_event(log, logging.WARNING, "signal ledger verdict read failed", error=str(exc))
            return []
        return [bool(r[0]) for r in reversed(rows)]

    def daily_aggregates(self, days: int = 14) -> dict[str, Any] | None:
        """Per-UTC-day evaluation/approval/publish counts for the last N days.

        Returns None when the ledger is unusable — the /signal surface renders
        that as an honest "no ledger" rather than zeros.
        """
        if not self._ok:
            return None
        cached = self._agg_cache
        if cached is not None and _time.monotonic() - cached[0] < self.AGG_CACHE_SEC:
            return cached[1]
        try:
            with self._lock, self._connect() as conn:
                rows = conn.execute(
                    "SELECT substr(ts,1,10) AS day, COUNT(*),"
                    " SUM(approved), SUM(published)"
                    " FROM signal_evaluations"
                    " WHERE ts >= datetime('now', ?)"
                    " GROUP BY day ORDER BY day",
                    (f"-{int(days)} days",),
                ).fetchall()
                total = conn.execute(
                    "SELECT COUNT(*), SUM(approved), SUM(published),"
                    " MIN(ts) FROM signal_evaluations"
                ).fetchone()
        except Exception as exc:
            log_event(log, logging.WARNING, "signal ledger read failed", error=str(exc))
            return None
        n_all, approved_all, published_all, first_ts = (
            int(total[0] or 0), int(total[1] or 0), int(total[2] or 0), total[3])
        result = {
            "days": [
                {"day": d, "evaluations": int(n), "approved": int(a or 0),
                 "published": int(p or 0)}
                for d, n, a, p in rows
            ],
            "total_evaluations": n_all,
            "total_approved": approved_all,
            "total_published": published_all,
            "measured_approval_rate": round(approved_all / n_all, 4) if n_all else None,
            "since": first_ts,
        }
        self._agg_cache = (_time.monotonic(), result)
        return result
