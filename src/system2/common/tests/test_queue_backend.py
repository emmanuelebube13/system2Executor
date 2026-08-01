"""Tests for the LocalDurableBackend queue (publish/pull/ack/nack/DLQ + envelope).

The ``lease`` block at the bottom is the F-307 regression set: a message pulled but never
acked used to sit in ``inflight`` forever, so a crash silently lost an approved order.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from system2.common.queue_backend import (
    LocalDurableBackend,
    ReceivedMessage,
    make_envelope,
)


@pytest.fixture
def backend(tmp_path: Path) -> LocalDurableBackend:
    b = LocalDurableBackend(tmp_path / "queue.db", max_attempts=3)
    yield b
    b.close()


def test_make_envelope_shape():
    env = make_envelope({"a": 1}, idempotency_key="k", correlation_id="c",
                        granularity="H1", event_type="order")
    assert env["schema_version"] == "1"
    assert env["idempotency_key"] == "k" and env["correlation_id"] == "c"
    assert env["granularity"] == "H1" and env["event_type"] == "order"
    assert env["payload"] == {"a": 1}
    assert env["message_id"] and env["created_at"].endswith("Z")


def test_publish_pull_ack_roundtrip(backend: LocalDurableBackend):
    backend.publish("orders", {"x": 1})
    msgs = backend.pull("orders", max_messages=5)
    assert len(msgs) == 1
    assert isinstance(msgs[0], ReceivedMessage)
    assert msgs[0].body == {"x": 1}
    msgs[0].ack()
    assert backend.pull("orders") == []  # ack removes it from ready/inflight


def test_pull_marks_inflight_not_redelivered(backend: LocalDurableBackend):
    backend.publish("orders", {"x": 1})
    first = backend.pull("orders")
    assert len(first) == 1
    # a second pull before ack/nack must not redeliver the inflight message
    assert backend.pull("orders") == []


def test_nack_redelivers_after_backoff(backend: LocalDurableBackend):
    backend.publish("orders", {"x": 1})
    m = backend.pull("orders")[0]
    m.nack(backoff_sec=0.0)  # immediately available
    again = backend.pull("orders")
    assert len(again) == 1 and again[0].attempts == 2


def test_poison_message_goes_to_dlq_after_max_attempts(backend: LocalDurableBackend):
    backend.publish("orders", {"x": 1})
    # max_attempts=3: attempts reach 3 then nack routes to DLQ
    for _ in range(3):
        batch = backend.pull("orders")
        if not batch:
            break
        batch[0].nack(backoff_sec=0.0)
    assert backend.pull("orders") == []
    assert backend.depth("orders.dlq", "ready") == 1
    dlq = backend.pull("orders.dlq")
    assert dlq[0].body["original"] == {"x": 1}
    assert "max_attempts" in dlq[0].body["dlq_reason"]


def test_explicit_to_dlq(backend: LocalDurableBackend):
    backend.publish("orders", {"bad": True})
    m = backend.pull("orders")[0]
    m.to_dlq("malformed")
    assert backend.depth("orders.dlq", "ready") == 1
    assert backend.pull("orders") == []


def test_fifo_order_preserved(backend: LocalDurableBackend):
    for i in range(3):
        backend.publish("orders", {"i": i})
    msgs = backend.pull("orders", max_messages=3)
    assert [m.body["i"] for m in msgs] == [0, 1, 2]


# --------------------------------------------------------------------------- #
# In-flight lease / reaper (F-307)
# --------------------------------------------------------------------------- #
def test_crash_before_ack_redelivers_after_restart(tmp_path: Path):
    """The contract in the module docstring: "a crash mid-processing redelivers".

    Before the lease, ``q2`` saw nothing and the approved order was lost with no DLQ
    entry, no counter and no alert (audit/findings/F-307).
    """
    path = tmp_path / "queue.db"
    q1 = LocalDurableBackend(path, max_attempts=3)
    q1.publish("orders", {"x": 1})
    assert len(q1.pull("orders")) == 1
    q1.close()                                    # the process dies: no ack, no nack

    q2 = LocalDurableBackend(path, max_attempts=3)          # operator restarts it
    again = q2.pull("orders")
    assert [m.body for m in again] == [{"x": 1}]
    assert again[0].attempts == 2                 # the attempt counter survived the crash
    again[0].ack()
    assert q2.pull("orders") == []
    q2.close()


def test_expired_lease_is_reclaimed_by_a_long_running_consumer(tmp_path: Path):
    """The reaper must work mid-life, not only at startup: same instance, same process."""
    q = LocalDurableBackend(tmp_path / "queue.db", max_attempts=3, lease_sec=0.0)
    q.publish("orders", {"x": 1})
    q.pull("orders")                              # taken, then dropped on the floor
    assert q.depth("orders", "inflight") == 1
    again = q.pull("orders")                      # the same instance reclaims it
    assert len(again) == 1 and again[0].attempts == 2
    q.close()


def test_live_lease_is_not_reclaimed(backend: LocalDurableBackend):
    """A consumer that is merely slow keeps its message for the whole lease."""
    backend.publish("orders", {"x": 1})
    backend.pull("orders")
    assert backend.reclaim("orders") == 0
    assert backend.pull("orders") == []


def test_orphaned_message_is_dead_lettered_at_max_attempts(tmp_path: Path):
    """An expired lease must not become an infinite poison loop (max_attempts=3)."""
    q = LocalDurableBackend(tmp_path / "queue.db", max_attempts=3, lease_sec=0.0)
    q.publish("orders", {"x": 1})
    for _ in range(4):
        q.pull("orders")                          # pull, crash, pull, crash, ...
    assert q.pull("orders") == []                 # no longer redelivered
    assert q.depth("orders", "dead") == 1
    dlq = q.pull("orders.dlq")
    assert dlq[0].body["original"] == {"x": 1}
    assert "max_attempts" in dlq[0].body["dlq_reason"]
    assert dlq[0].body["attempts"] == 3
    q.close()


def test_ack_from_a_reclaimed_consumer_is_a_no_op(tmp_path: Path):
    """The stale owner coming back must not ack the copy someone else now owns.

    ``ack_id`` is regenerated on every pull, so it doubles as the fencing token.
    """
    path = tmp_path / "queue.db"
    q1 = LocalDurableBackend(path, max_attempts=3, lease_sec=0.0)
    q1.publish("orders", {"x": 1})
    stale = q1.pull("orders")[0]
    q2 = LocalDurableBackend(path, max_attempts=3)
    fresh = q2.pull("orders")[0]                  # reclaimed by the restarted consumer
    stale.ack()                                   # the zombie finally finishes
    assert q2.depth("orders", "inflight") == 1    # still owned by q2, not marked done
    fresh.ack()
    assert q2.depth("orders", "done") == 1
    q1.close()
    q2.close()


def test_recovers_a_queue_db_written_before_the_lease_existed(tmp_path: Path):
    """A pre-fix queue.db has no lease columns and may already hold stranded rows."""
    path = tmp_path / "queue.db"
    con = sqlite3.connect(str(path), isolation_level=None)
    con.execute(
        """CREATE TABLE queue(
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL, body TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'ready',
            attempts INTEGER NOT NULL DEFAULT 0,
            available_at REAL NOT NULL DEFAULT 0,
            ack_id TEXT)"""
    )
    con.execute("INSERT INTO queue(topic, body, state, attempts, ack_id) "
                "VALUES ('orders', '{\"stranded\": true}', 'inflight', 1, 'old-ack')")
    con.close()

    q = LocalDurableBackend(path, max_attempts=3)
    cols = {r[1] for r in q._conn.execute("PRAGMA table_info(queue)")}
    assert {"leased_until", "lease_owner"} <= cols
    msgs = q.pull("orders")
    assert [m.body for m in msgs] == [{"stranded": True}]
    q.close()
