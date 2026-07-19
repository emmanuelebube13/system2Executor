"""EXEC-004 — consume approved, pre-sized orders from ``AMS_Outbound_Queue``.

In ``EXEC_MODE=execution_only`` Layer 4 sources work ONLY from the queue (never
``Fact_Signals``). The consumer is a session-scoped poller with manual ack:

  pull -> validate envelope -> (malformed -> DLQ+ack) -> (expired/seen -> ack-drop)
       -> (out-of-session -> nack/park) -> pipeline.process (last-line §7.2 gate)
       -> ack iff durably handled, else nack (redeliver).

An order is ack'd only after it is *durably handled* (submitted+recorded, or definitively
rejected) so a crash mid-processing redelivers rather than loses the order. Dedup is
persistent (``SqliteProcessedStore``) so a redelivered ``idempotency_key`` is recognised
across restarts and never produces a second broker order. Consumer lag (last message
timestamp, last poll time) is exposed for EXEC-008 staleness detection + Layer 5.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from system2.common.logging import get_logger, log_event
from system2.execution.pipeline import ApprovedOrder, Decision, ExecutionPipeline
from system2.execution.validation import is_expired, validate_envelope

log = get_logger("execution.outbound_consumer")

# Decisions that mean "we are done with this message" -> ack (do NOT redeliver).
_DURABLE_DECISIONS = frozenset(
    {
        Decision.EXECUTED,
        Decision.SHADOW,
        Decision.SKIPPED_DUPLICATE,
        Decision.REJECTED_BACKUP_GUARD,
        Decision.REJECTED_INVALID,
        Decision.REJECTED_VALIDATION,
        Decision.DEFERRED_LEGACY,
    }
)


class SqliteProcessedStore:
    """Persistent idempotency ledger (survives restarts). Drop-in for InMemoryProcessedStore."""

    def __init__(self, db_path: Path | str) -> None:
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS processed("
            "idempotency_key TEXT PRIMARY KEY, processed_at TEXT NOT NULL)"
        )

    def seen(self, key: str) -> bool:
        return (
            self._conn.execute(
                "SELECT 1 FROM processed WHERE idempotency_key=?", (key,)
            ).fetchone()
            is not None
        )

    def mark(self, key: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO processed(idempotency_key, processed_at) VALUES (?, ?)",
            (key, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")),
        )

    def close(self) -> None:
        self._conn.close()


@dataclass
class ConsumerLag:
    """Snapshot consumed by EXEC-008 staleness detection + Layer 5 telemetry."""

    last_poll_at: datetime | None = None
    last_message_at: datetime | None = None  # wall-clock receipt of the last message
    last_message_created_at: datetime | None = None  # producer ``created_at`` of last message
    messages_seen: int = 0

    def seconds_since_last_message(self, now: datetime) -> float | None:
        if self.last_message_at is None:
            return None
        return (now.astimezone(timezone.utc) - self.last_message_at).total_seconds()


@dataclass
class OutboundConsumer:
    """Polls ``AMS_Outbound_Queue`` and feeds the slim execution path."""

    queue: Any
    subscription: str
    pipeline: ExecutionPipeline
    price_fn: Callable[[ApprovedOrder], float]
    submit_fn: Callable[[Any], Any] | None = None
    persist_fn: Callable[[Any, Any], None] | None = None
    emit_fn: Callable[[ApprovedOrder, Any, Any], None] | None = None
    validate_fn: Callable[[Any], tuple[bool, str]] | None = None
    submit_gate_fn: Callable[[], bool] | None = None  # EXEC-008: park new orders when PAUSED
    open_instruments_fn: Callable[[], Iterable[str]] = field(default=lambda: ())
    max_age_sec: int | None = None
    prefetch: int = 4
    park_backoff_sec: float = 30.0
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))
    lag: ConsumerLag = field(default_factory=ConsumerLag)

    def poll_once(self) -> dict[str, int]:
        """Pull up to ``prefetch`` messages and handle each. Returns per-outcome counts.

        Never raises on a single bad message — the loop must stay alive (EXEC-004 §risk).
        """
        now = self.clock()
        self.lag.last_poll_at = now
        stats: dict[str, int] = {}
        try:
            batch = self.queue.pull(self.subscription, max_messages=self.prefetch)
        except Exception as exc:  # transport hiccup — log, report empty, retry next poll
            log_event(log, logging.ERROR, "queue pull failed", error=str(exc))
            return {"pull_error": 1}
        for msg in batch:
            outcome = self._handle(msg, now)
            stats[outcome] = stats.get(outcome, 0) + 1
        return stats

    def _handle(self, msg: Any, now: datetime) -> str:
        raw = msg.body
        # --- envelope validation: malformed -> dead-letter + ack (no poison loop) ---
        res = validate_envelope(raw)
        if not res.ok:
            log_event(log, logging.ERROR, "malformed message -> DLQ",
                      code=res.code, reason=res.reason)
            try:
                msg.to_dlq(f"{res.code}: {res.reason}")
            except Exception as exc:
                log_event(log, logging.ERROR, "DLQ routing failed; nack", error=str(exc))
                msg.nack(self.park_backoff_sec)
                return "dlq_error"
            return "dead_lettered"

        # well-formed -> update lag from this message
        self.lag.messages_seen += 1
        self.lag.last_message_at = now
        created = raw.get("created_at")
        if created:
            try:
                self.lag.last_message_created_at = datetime.fromisoformat(
                    created.replace("Z", "+00:00")
                ).astimezone(timezone.utc)
            except ValueError:
                pass

        # --- TTL: stale order -> ack-drop (never submit a stale order) ---
        if is_expired(raw, now, self.max_age_sec):
            log_event(log, logging.WARNING, "expired order dropped",
                      idempotency_key=raw.get("idempotency_key"))
            msg.ack()
            return "expired"

        order = ApprovedOrder.from_dict(raw)

        # --- dedup before doing any work (cheap, persistent) ---
        if self.pipeline.processed.seen(order.idempotency_key):
            log_event(log, logging.INFO, "duplicate idempotency_key -> ack/skip",
                      idempotency_key=order.idempotency_key)
            msg.ack()
            return "duplicate"

        # --- market-hours guard: park (nack w/ backoff), do not submit ---
        from system2.execution.pipeline import is_in_session

        if not is_in_session(now):
            log_event(log, logging.INFO, "out of session -> parked",
                      idempotency_key=order.idempotency_key)
            msg.nack(self.park_backoff_sec)
            return "parked"

        # --- safety-mode gate (EXEC-008 PAUSED): hold NEW orders on the queue, never submit ---
        if self.submit_gate_fn is not None and not self.submit_gate_fn():
            log_event(log, logging.INFO, "safety PAUSE -> new order parked (not submitted)",
                      idempotency_key=order.idempotency_key)
            msg.nack(self.park_backoff_sec)
            return "paused_park"

        # --- feed the slim execution path ---
        try:
            price = self.price_fn(order)
            emit = None
            if self.emit_fn is not None:
                emit = lambda c, f, _o=order: self.emit_fn(_o, c, f)  # noqa: E731
            result = self.pipeline.process(
                order,
                price,
                open_instruments=self.open_instruments_fn(),
                submit_fn=self.submit_fn,
                persist_fn=self.persist_fn,
                emit_fn=emit,
                validate_fn=self.validate_fn,
            )
        except Exception as exc:  # transient (broker/network) -> redeliver
            log_event(log, logging.ERROR, "execution failed; nack for redelivery",
                      idempotency_key=order.idempotency_key, error=str(exc))
            msg.nack(self.park_backoff_sec)
            return "exec_error"

        decision = result["decision"]
        if decision in _DURABLE_DECISIONS:
            msg.ack()
            return decision.value
        # e.g. REJECTED_OUT_OF_SESSION slipped through a clock edge -> park
        msg.nack(self.park_backoff_sec)
        return f"nack_{decision.value}"

    def run_forever(self, stop_predicate: Callable[[], bool], idle_sleep_sec: float = 1.0) -> None:
        """Long-running poll loop. ``stop_predicate`` is checked each iteration (e.g. STOP)."""
        log_event(log, logging.INFO, "outbound consumer started", subscription=self.subscription)
        while not stop_predicate():
            stats = self.poll_once()
            if not stats:
                time.sleep(idle_sleep_sec)
        log_event(log, logging.INFO, "outbound consumer stopped", subscription=self.subscription)
