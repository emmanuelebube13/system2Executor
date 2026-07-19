"""EXEC-011 — bar-based signal deduplication state.

A scored signal is uniquely identified by ``(instrument, granularity, bar_time_utc,
strategy_id)``: at most one signal is produced per strategy per closed bar, across
restarts. The state lives in a small SQLite table (WAL mode, same posture as the
queue/outbox stores) so a crash between "scored" and "published" redelivers nothing —
the bar is only claimed when the INSERT succeeds.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


def make_dedup_key(instrument: str, granularity: str, bar_time_iso: str, strategy_id: int | str) -> str:
    """Canonical dedup key: ``{instrument}:{granularity}:{bar_time_iso}:{strategy_id}``."""
    return f"{instrument}:{granularity}:{bar_time_iso}:{strategy_id}"


class SignalDedupStore:
    """SQLite-backed at-most-once-per-bar claim store for produced signals."""

    def __init__(self, db_path: Path | str) -> None:
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS signal_dedup (
                dedup_key TEXT PRIMARY KEY,
                signal_id TEXT NOT NULL,
                produced_at TEXT NOT NULL
            )"""
        )

    def claim(self, dedup_key: str, signal_id: str, produced_at: str) -> bool:
        """Atomically claim ``dedup_key``. True iff this bar+strategy was not seen before."""
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO signal_dedup(dedup_key, signal_id, produced_at) VALUES (?,?,?)",
            (dedup_key, signal_id, produced_at),
        )
        return cur.rowcount == 1

    def seen(self, dedup_key: str) -> bool:
        """Read-only check (no claim) — used to skip work before scoring an old bar."""
        row = self._conn.execute(
            "SELECT 1 FROM signal_dedup WHERE dedup_key=?", (dedup_key,)
        ).fetchone()
        return row is not None

    def close(self) -> None:
        self._conn.close()
