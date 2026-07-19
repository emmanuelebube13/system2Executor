"""Pluggable QueueBackend (orders/fills/control transport).

Live backend (decision D-002): **Google Cloud Pub/Sub** (``QUEUE_PROVIDER=pubsub``), over
HTTPS, no VPN. Dev/test backend: ``LocalDurableBackend`` (``QUEUE_PROVIDER=local``,
SQLite-backed under ``state/queue/``). Same pull/ack/nack/DLQ interface; chosen by config.

Pull-based with manual ack so a consumer can ack only after an order is *durably handled*
(EXEC-004): a crash mid-processing redelivers rather than loses the order. A poison message
goes to a dead-letter topic after ``max_attempts`` (EXEC-004 §7.3). See
docs/STORAGE_AND_QUEUE_ABSTRACTION.md. The pubsub SDK is imported lazily.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

SCHEMA_VERSION = "1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def make_envelope(
    payload: dict[str, Any],
    idempotency_key: str,
    correlation_id: str,
    granularity: str | None = None,
    event_type: str | None = None,
) -> dict[str, Any]:
    """Build a standard message envelope (see abstraction doc)."""
    env = {
        "schema_version": SCHEMA_VERSION,
        "message_id": str(uuid.uuid4()),
        "idempotency_key": idempotency_key,
        "correlation_id": correlation_id,
        "created_at": utc_now_iso(),
        "payload": payload,
    }
    if granularity is not None:
        env["granularity"] = granularity
    if event_type is not None:
        env["event_type"] = event_type
    return env


@dataclass
class ReceivedMessage:
    ack_id: str
    topic: str
    body: dict[str, Any]
    attempts: int = 1
    _backend: Any = field(default=None, repr=False)

    def ack(self) -> None:
        self._backend.ack(self)

    def nack(self, backoff_sec: float = 5.0) -> None:
        self._backend.nack(self, backoff_sec)

    def to_dlq(self, reason: str) -> None:
        self._backend.to_dlq(self, reason)


@runtime_checkable
class QueueBackend(Protocol):
    def publish(self, topic: str, body: dict[str, Any]) -> None: ...
    def pull(self, subscription: str, max_messages: int = 1) -> list[ReceivedMessage]: ...
    def ack(self, msg: ReceivedMessage) -> None: ...
    def nack(self, msg: ReceivedMessage, backoff_sec: float = 5.0) -> None: ...
    def to_dlq(self, msg: ReceivedMessage, reason: str) -> None: ...


# --------------------------------------------------------------------------- #
# Local durable backend (SQLite) — dev/test, same interface
# --------------------------------------------------------------------------- #
class LocalDurableBackend:
    """File-backed durable queue. ``topic`` == ``subscription`` name. DLQ = ``<topic>.dlq``."""

    def __init__(self, db_path: Path | str, max_attempts: int = 5) -> None:
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_attempts = max_attempts
        self._conn = sqlite3.connect(str(self.path), isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS queue(
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL, body TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'ready',
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at REAL NOT NULL DEFAULT 0,
                ack_id TEXT)"""
        )

    def publish(self, topic: str, body: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO queue(topic, body, state, attempts, available_at) VALUES (?,?, 'ready', 0, 0)",
            (topic, json.dumps(body)),
        )

    def pull(self, subscription: str, max_messages: int = 1) -> list[ReceivedMessage]:
        now = time.time()
        cur = self._conn.execute(
            "SELECT seq, body, attempts FROM queue WHERE topic=? AND state='ready' AND available_at<=? "
            "ORDER BY seq LIMIT ?",
            (subscription, now, max_messages),
        )
        rows = cur.fetchall()
        out: list[ReceivedMessage] = []
        for seq, body, attempts in rows:
            ack_id = str(uuid.uuid4())
            self._conn.execute(
                "UPDATE queue SET state='inflight', ack_id=?, attempts=attempts+1 WHERE seq=?",
                (ack_id, seq),
            )
            out.append(
                ReceivedMessage(ack_id=ack_id, topic=subscription, body=json.loads(body),
                                attempts=attempts + 1, _backend=self)
            )
        return out

    def ack(self, msg: ReceivedMessage) -> None:
        self._conn.execute("UPDATE queue SET state='done' WHERE ack_id=?", (msg.ack_id,))

    def nack(self, msg: ReceivedMessage, backoff_sec: float = 5.0) -> None:
        row = self._conn.execute(
            "SELECT seq, attempts FROM queue WHERE ack_id=?", (msg.ack_id,)
        ).fetchone()
        if row is None:
            return
        seq, attempts = row
        if attempts >= self.max_attempts:
            self.to_dlq(msg, f"max_attempts {self.max_attempts} exceeded")
            return
        self._conn.execute(
            "UPDATE queue SET state='ready', ack_id=NULL, available_at=? WHERE seq=?",
            (time.time() + backoff_sec, seq),
        )

    def to_dlq(self, msg: ReceivedMessage, reason: str) -> None:
        wrapper = {
            "original": msg.body,
            "dlq_reason": reason,
            "dlq_timestamp": utc_now_iso(),
            "attempts": msg.attempts,
        }
        self._conn.execute(
            "INSERT INTO queue(topic, body, state, available_at) VALUES (?, ?, 'ready', 0)",
            (f"{msg.topic}.dlq", json.dumps(wrapper)),
        )
        self._conn.execute("UPDATE queue SET state='dead' WHERE ack_id=?", (msg.ack_id,))

    # test/inspection helpers
    def depth(self, topic: str, state: str = "ready") -> int:
        return self._conn.execute(
            "SELECT COUNT(*) FROM queue WHERE topic=? AND state=?", (topic, state)
        ).fetchone()[0]

    def close(self) -> None:
        self._conn.close()


# --------------------------------------------------------------------------- #
# Google Cloud Pub/Sub backend (live) — lazy SDK import
# --------------------------------------------------------------------------- #
class PubSubBackend:
    """Google Cloud Pub/Sub. ``topic`` = topic id for publish; ``subscription`` for pull."""

    def __init__(self, project_id: str, dlq_topic: str | None = None) -> None:
        from google.cloud import pubsub_v1  # lazy

        self._publisher = pubsub_v1.PublisherClient()
        self._subscriber = pubsub_v1.SubscriberClient()
        self._project = project_id
        self._dlq_topic = dlq_topic

    def _topic_path(self, topic: str) -> str:
        return self._publisher.topic_path(self._project, topic)

    def _sub_path(self, sub: str) -> str:
        return self._subscriber.subscription_path(self._project, sub)

    def publish(self, topic: str, body: dict[str, Any]) -> None:
        future = self._publisher.publish(self._topic_path(topic), json.dumps(body).encode("utf-8"))
        future.result(timeout=30)

    def pull(self, subscription: str, max_messages: int = 1) -> list[ReceivedMessage]:
        resp = self._subscriber.pull(
            subscription=self._sub_path(subscription), max_messages=max_messages, return_immediately=True
        )
        out: list[ReceivedMessage] = []
        for rm in resp.received_messages:
            body = json.loads(rm.message.data.decode("utf-8"))
            attempts = int(rm.message.attributes.get("delivery_attempt", 1)) if rm.message.attributes else 1
            out.append(ReceivedMessage(ack_id=rm.ack_id, topic=subscription, body=body,
                                       attempts=attempts, _backend=self))
        return out

    def ack(self, msg: ReceivedMessage) -> None:
        self._subscriber.acknowledge(subscription=self._sub_path(msg.topic), ack_ids=[msg.ack_id])

    def nack(self, msg: ReceivedMessage, backoff_sec: float = 5.0) -> None:
        # ack_deadline 0 => immediate redelivery; Pub/Sub dead-letter policy handles max attempts.
        self._subscriber.modify_ack_deadline(
            subscription=self._sub_path(msg.topic), ack_ids=[msg.ack_id], ack_deadline_seconds=0
        )

    def to_dlq(self, msg: ReceivedMessage, reason: str) -> None:
        if self._dlq_topic:
            wrapper = {"original": msg.body, "dlq_reason": reason, "dlq_timestamp": utc_now_iso()}
            self.publish(self._dlq_topic, wrapper)
        self.ack(msg)  # remove from the live subscription


def build_queue(secrets: Any | None = None) -> QueueBackend:
    """Factory. Reads ``QUEUE_PROVIDER`` (``pubsub`` default | ``local``)."""
    from system2.common.secrets import get_secrets

    secrets = secrets or get_secrets()
    provider = (secrets.get("QUEUE_PROVIDER", "pubsub") or "pubsub").lower()
    if provider == "local":
        path = secrets.get("QUEUE_LOCAL_PATH", "state/queue/queue.db")
        return LocalDurableBackend(path, max_attempts=secrets.get_int("QUEUE_MAX_DELIVERY_ATTEMPTS", 5))
    if provider == "pubsub":
        return PubSubBackend(
            project_id=secrets.require("PUBSUB_PROJECT_ID"),
            dlq_topic=secrets.get("QUEUE_DLQ_TOPIC"),
        )
    raise ValueError(f"Unknown QUEUE_PROVIDER '{provider}' (expected pubsub|local)")
