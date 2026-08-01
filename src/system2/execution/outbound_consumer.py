"""EXEC-004 — consume approved, pre-sized orders from ``AMS_Outbound_Queue``.

In ``EXEC_MODE=execution_only`` Layer 4 sources work ONLY from the queue (never
``Fact_Signals``). The consumer is a session-scoped poller with manual ack:

  pull -> (S3 heartbeat -> credit freshness + ack) -> validate envelope
       -> (malformed -> DLQ+ack) -> (expired/seen -> ack-drop)
       -> (out-of-session -> nack/park) -> pipeline.process (last-line §7.2 gate)
       -> ack iff durably handled, else nack (redeliver).

Queue freshness (the EXEC-008 staleness input) is credited **only after** a message has
passed its TTL (F-305): an expired redelivery proves the queue is replaying, not that
System 3 is alive, and must never be able to un-PAUSE the engine.

An order is ack'd only after it is *durably handled* (submitted+recorded, or definitively
rejected) so a crash mid-processing redelivers rather than loses the order. Dedup is
persistent (``SqliteProcessedStore``) so a redelivered ``idempotency_key`` — and the source
``signal_id``, guarded too (F-206/F-303) — is recognised across restarts and never produces
a second broker order; the marker is written the instant the broker fills, before any hook
that could still fail. Consumer lag (last message
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
from system2.execution.pipeline import ApprovedOrder, Decision, ExecutionPipeline, guard_keys
from system2.execution.validation import is_expired, validate_envelope

log = get_logger("execution.outbound_consumer")

# System 3 -> System 2 keepalive (deployment-guide/05 §4). Published on the SAME outbound
# subscription as approved orders; carries freshness only and never becomes an order.
HEARTBEAT_EVENT_TYPE = "system3.heartbeat"

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
    heartbeat_fn: Callable[[datetime | None], bool] | None = None  # EXEC-008: S3 keepalive sink
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

    @staticmethod
    def _parse_ts(raw: dict[str, Any], *fields: str) -> datetime | None:
        """First parseable ISO timestamp among ``fields``, as UTC. None if there is none."""
        for field_name in fields:
            value = raw.get(field_name)
            if not value:
                continue
            try:
                return datetime.fromisoformat(
                    str(value).replace("Z", "+00:00")
                ).astimezone(timezone.utc)
            except (TypeError, ValueError):
                continue
        return None

    def _credit_freshness(self, raw: dict[str, Any], now: datetime) -> None:
        """Mark the queue as fresh. Only ever called once a message has passed its TTL."""
        self.lag.last_message_at = now
        created = self._parse_ts(raw, "created_at")
        if created is not None:
            self.lag.last_message_created_at = created

    def _handle_heartbeat(self, msg: Any, raw: dict[str, Any]) -> str:
        """Route a System-3 keepalive to the safety monitor, then ack it.

        The keepalive is credited from its producer ``created_at`` (never wall-clock
        receipt), and the monitor independently refuses one that is already older than the
        staleness limit — so a replayed heartbeat cannot resurrect a dark System 3 (F-305).
        Always ack'd: a heartbeat is worthless on redelivery, so it must never be requeued.

        Deliberately does NOT bump ``lag.messages_seen``: that counter means "approved
        orders have reached System 2", and a keepalive must never make a dead order path
        look alive on the telemetry surface.

        ``produced_at`` is accepted alongside ``created_at`` because System 3's envelope
        builder stamps the former (``ams/common/queue_backend.py:56``) while System 2's
        order envelope uses the latter.
        """
        sent_at = self._parse_ts(raw, "created_at", "produced_at")
        credited = False
        if sent_at is None:
            # Fail closed: an undated keepalive cannot be shown to be recent, so it buys
            # no freshness. Crediting `now` here would let any replay resume the engine.
            log_event(log, logging.WARNING, "heartbeat without a usable timestamp ignored",
                      source=(raw.get("payload") or {}).get("source"))
        elif self.heartbeat_fn is not None:
            try:
                credited = bool(self.heartbeat_fn(sent_at))
            except Exception as exc:  # a bad keepalive must never break the poll loop
                log_event(log, logging.ERROR, "heartbeat handling failed", error=str(exc),
                          source=(raw.get("payload") or {}).get("source"))
        log_event(log, logging.DEBUG if credited else logging.INFO,
                  "system3 heartbeat received", credited=credited,
                  sent_at=sent_at.isoformat().replace("+00:00", "Z") if sent_at else None)
        msg.ack()
        return "heartbeat" if credited else "heartbeat_stale"

    def _handle(self, msg: Any, now: datetime) -> str:
        raw = msg.body
        # --- System-3 keepalive (EXEC-008/F-305): freshness only, never an order ---
        # Routed before envelope validation because a heartbeat carries no order fields and
        # would otherwise be dead-lettered as malformed.
        if isinstance(raw, dict) and raw.get("event_type") == HEARTBEAT_EVENT_TYPE:
            return self._handle_heartbeat(msg, raw)

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

        self.lag.messages_seen += 1

        # --- TTL: stale order -> ack-drop (never submit a stale order) ---
        # NOTE (F-305): this check runs BEFORE any freshness is credited. An expired
        # redelivery is evidence of a replaying queue, not of a live System 3, so it must
        # not be able to un-PAUSE the engine.
        if is_expired(raw, now, self.max_age_sec):
            log_event(log, logging.WARNING, "expired order dropped",
                      idempotency_key=raw.get("idempotency_key"))
            msg.ack()
            return "expired"

        # well-formed AND within TTL -> this message is real evidence System 3 is alive
        self._credit_freshness(raw, now)

        order = ApprovedOrder.from_dict(raw)

        # --- dedup before doing any work (cheap, persistent) ---
        # Both the order identity and the source signal (F-206/F-303 defence in depth) —
        # see ``pipeline.guard_keys``.
        for key in guard_keys(order):
            if self.pipeline.processed.seen(key):
                log_event(log, logging.INFO, "already executed -> ack/skip",
                          idempotency_key=order.idempotency_key, dedup_key=key)
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
