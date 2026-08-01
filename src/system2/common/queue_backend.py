"""Pluggable QueueBackend (orders/fills/control transport).

Live backend (decision D-002): **Google Cloud Pub/Sub** (``QUEUE_PROVIDER=pubsub``), over
HTTPS, no VPN. Dev/test backend: ``LocalDurableBackend`` (``QUEUE_PROVIDER=local``,
SQLite-backed under ``state/queue/``). Same pull/ack/nack/DLQ interface; chosen by config.

Pull-based with manual ack so a consumer can ack only after an order is *durably handled*
(EXEC-004): a crash mid-processing redelivers rather than loses the order. A poison message
goes to a dead-letter topic after ``max_attempts`` (EXEC-004 §7.3). See
docs/STORAGE_AND_QUEUE_ABSTRACTION.md. The pubsub SDK is imported lazily.

In-flight lease (F-307)
-----------------------
``LocalDurableBackend`` backs that redelivery promise with a **lease**: ``pull`` stamps the
row with an owner and an expiry, and every ``pull`` first reclaims the subscription's
orphaned rows. Without it a row pulled but never acked stayed ``inflight`` forever — a
crash lost the order silently, which is at-most-once, not the at-least-once the dedup
store and ack-after-durable-handling are built on.

The same three columns and the same semantics are implemented in
``system3/ams/src/ams/common/queue_backend.py`` and in ``bridge/s2s3_bridge.py``: in
local mode all three components share ONE queue.db, so a lease honoured by only one of
them would let two consumers believe they own the same row.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

SCHEMA_VERSION = "1"

# How long one consumer owns a pulled-but-unacked row (F-307). Far longer than a
# poll -> price -> submit -> persist cycle, so a *live* consumer is never raced; short
# enough that a wedged one cannot hold an order for a session.
DEFAULT_LEASE_SEC = 300.0

# Lease timestamps are written AND compared by SQLite itself, never by a Python clock.
# Three processes share this file (so, one host and one system clock), and several of
# them run on an injected/pinned clock; making the DB the single writer of "now" keeps
# the three from disagreeing about when a lease ended.
_DB_NOW = "CAST(strftime('%s','now') AS REAL)"


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

    def __init__(
        self,
        db_path: Path | str,
        max_attempts: int = 5,
        lease_sec: float = DEFAULT_LEASE_SEC,
        owner: str | None = None,
    ) -> None:
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_attempts = max_attempts
        self.lease_sec = float(lease_sec)
        # Owner identity is per *instance*: "who holds this row right now". A restarted
        # process (or a backend reopened after a crash) is a new owner, which is what
        # makes orphan recovery possible at all — see ``reclaim``.
        self.owner = owner or f"system2:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self._recovered: set[str] = set()
        self._conn = sqlite3.connect(str(self.path), isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS queue(
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL, body TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'ready',
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at REAL NOT NULL DEFAULT 0,
                ack_id TEXT,
                leased_until REAL NOT NULL DEFAULT 0,
                lease_owner TEXT)"""
        )
        self._add_lease_columns()

    def _add_lease_columns(self) -> None:
        """Add the lease columns to a queue.db created before F-307 was fixed.

        ``CREATE TABLE IF NOT EXISTS`` is a no-op on an existing table and the three
        components share one file, so whichever opened it first may have created it
        without these columns. Idempotent and race-safe: a peer that adds the column
        first just makes this ``ALTER`` fail with "duplicate column name".
        """
        cols = {row[1] for row in self._conn.execute("PRAGMA table_info(queue)")}
        for name, decl in (("leased_until", "REAL NOT NULL DEFAULT 0"), ("lease_owner", "TEXT")):
            if name in cols:
                continue
            try:
                self._conn.execute(f"ALTER TABLE queue ADD COLUMN {name} {decl}")
            except sqlite3.OperationalError:  # a peer process added it between the two calls
                pass

    def publish(self, topic: str, body: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO queue(topic, body, state, attempts, available_at) VALUES (?,?, 'ready', 0, 0)",
            (topic, json.dumps(body)),
        )

    def reclaim(self, subscription: str) -> int:
        """Return orphaned in-flight rows on ``subscription`` to ``ready`` (F-307).

        Called at the top of every ``pull``, so the reaper runs for the whole life of a
        long-running consumer and not only at startup. Two kinds of orphan:

        * **expired lease** — the holder crashed, hung, or dropped the message on an
          exception path. The lease is the only bound on how long one consumer may hold
          a message, and it is what makes pull/ack at-least-once instead of at-most-once.
        * **another instance's lease, on our first pull of this subscription** — i.e. we
          have just started. The deployed local-mode topology gives every subscription
          exactly one consumer process (deployment-guide/05 §1: bridge owns
          ``ams-outbound``/``ams-inbound``, S2 owns ``ams-outbound.executor``, S3 owns
          ``scored-signals.ams``/``ams-inbound.ams``), so a lease held by a *different
          instance* of the consumer we are is a dead predecessor. This runs once per
          subscription per instance — never on later polls — so two processes that do
          share a subscription cannot ping-pong a message between them; they degrade to
          at-least-once, which is the contract, rather than to a steal loop.

        ``attempts`` is never reset, so a message that is leased and orphaned over and
        over is dead-lettered at ``max_attempts`` exactly like a repeatedly nacked one:
        an expired lease can never become an unbounded redelivery loop. Returns the
        number of rows reclaimed or dead-lettered (0 on the common path).
        """
        first_pull = subscription not in self._recovered
        self._recovered.add(subscription)
        sql = ("SELECT seq, body, attempts FROM queue "
               f"WHERE topic=? AND state='inflight' AND (leased_until<={_DB_NOW}")
        params: list[Any] = [subscription]
        if first_pull:
            sql += " OR lease_owner IS NOT ?"          # IS NOT: NULL-safe (pre-fix rows)
            params.append(self.owner)
        rows = self._conn.execute(sql + ")", params).fetchall()
        for seq, body, attempts in rows:
            if attempts >= self.max_attempts:
                self._dead_letter_row(
                    seq, subscription, body, attempts,
                    f"lease expired; max_attempts {self.max_attempts} exceeded",
                )
                continue
            self._conn.execute(
                "UPDATE queue SET state='ready', ack_id=NULL, lease_owner=NULL, "
                "leased_until=0, available_at=0 WHERE seq=? AND state='inflight'",
                (seq,),
            )
        return len(rows)

    def _dead_letter_row(self, seq: int, topic: str, body: str, attempts: int,
                         reason: str) -> None:
        """DLQ a row the reaper must not redeliver again (``to_dlq``'s wrapper shape).

        Claims the row first and only writes the wrapper if the claim won, so two
        processes reaping the same row cannot both file it.
        """
        claimed = self._conn.execute(
            "UPDATE queue SET state='dead' WHERE seq=? AND state='inflight'", (seq,)
        ).rowcount
        if not claimed:
            return
        try:
            original: Any = json.loads(body)
        except ValueError:
            original = body                     # keep the raw text rather than lose it
        wrapper = {
            "original": original,
            "dlq_reason": reason,
            "dlq_timestamp": utc_now_iso(),
            "attempts": attempts,
        }
        self._conn.execute(
            "INSERT INTO queue(topic, body, state, available_at) VALUES (?, ?, 'ready', 0)",
            (f"{topic}.dlq", json.dumps(wrapper)),
        )

    def pull(self, subscription: str, max_messages: int = 1) -> list[ReceivedMessage]:
        self.reclaim(subscription)
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
            # ``AND state='ready'`` makes the claim atomic (autocommit => one statement,
            # one transaction): if a peer claimed this row between our SELECT and here,
            # rowcount is 0 and we simply do not deliver it.
            claimed = self._conn.execute(
                "UPDATE queue SET state='inflight', ack_id=?, attempts=attempts+1, "
                f"lease_owner=?, leased_until={_DB_NOW}+? WHERE seq=? AND state='ready'",
                (ack_id, self.owner, self.lease_sec, seq),
            ).rowcount
            if not claimed:
                continue
            out.append(
                ReceivedMessage(ack_id=ack_id, topic=subscription, body=json.loads(body),
                                attempts=attempts + 1, _backend=self)
            )
        return out

    def ack(self, msg: ReceivedMessage) -> None:
        # Unchanged and deliberately un-fenced: ``ack_id`` is regenerated on every pull,
        # so an ack from a consumer whose lease was already reclaimed matches no row and
        # is a harmless no-op. The common path stays one statement.
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
            "UPDATE queue SET state='ready', ack_id=NULL, lease_owner=NULL, leased_until=0, "
            "available_at=? WHERE seq=?",
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
        return LocalDurableBackend(
            path,
            max_attempts=secrets.get_int("QUEUE_MAX_DELIVERY_ATTEMPTS", 5),
            lease_sec=secrets.get_int("QUEUE_LEASE_SEC", int(DEFAULT_LEASE_SEC)),
        )
    if provider == "pubsub":
        return PubSubBackend(
            project_id=secrets.require("PUBSUB_PROJECT_ID"),
            dlq_topic=secrets.get("QUEUE_DLQ_TOPIC"),
        )
    raise ValueError(f"Unknown QUEUE_PROVIDER '{provider}' (expected pubsub|local)")
