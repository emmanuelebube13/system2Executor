"""Tests for the LocalDurableBackend queue (publish/pull/ack/nack/DLQ + envelope).

The ``lease`` block at the bottom is the F-307 regression set: a message pulled but never
acked used to sit in ``inflight`` forever, so a crash silently lost an approved order.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path, PurePosixPath

import pytest

from system2.common.queue_backend import (
    LocalDurableBackend,
    SharedQueueMissing,
    _assert_shared_queue,
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


# --------------------------------------------------------------------------- #
# Shared-queue assertion (P0)
#
# sqlite3.connect creates a database at any path it is given, so a wrong
# QUEUE_LOCAL_PATH used to yield a private, permanently empty queue that looked
# exactly like a quiet one. These pin the refusal to the specific shapes a wrong
# path takes.
# --------------------------------------------------------------------------- #

def test_assert_shared_queue_accepts_a_real_queue(tmp_path: Path) -> None:
    path = tmp_path / "queue.db"
    LocalDurableBackend(path).close()
    _assert_shared_queue(path)  # does not raise


def test_assert_shared_queue_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(SharedQueueMissing, match="does not exist"):
        _assert_shared_queue(tmp_path / "queue.db")


def test_assert_shared_queue_rejects_zero_byte_file(tmp_path: Path) -> None:
    """The fingerprint of a previous silent-create, not a shared queue."""
    path = tmp_path / "queue.db"
    path.touch()
    with pytest.raises(SharedQueueMissing, match="0 bytes"):
        _assert_shared_queue(path)


def test_assert_shared_queue_rejects_a_db_without_the_queue_table(tmp_path: Path) -> None:
    path = tmp_path / "queue.db"
    con = sqlite3.connect(str(path), isolation_level=None)
    con.execute("CREATE TABLE something_else(x INTEGER)")
    con.close()
    with pytest.raises(SharedQueueMissing, match="no 'queue' table"):
        _assert_shared_queue(path)


def test_require_existing_refuses_before_creating_the_file(tmp_path: Path) -> None:
    """The file must not exist afterwards — refusing after creating it is no refusal."""
    path = tmp_path / "nested" / "queue.db"
    with pytest.raises(SharedQueueMissing):
        LocalDurableBackend(path, require_existing=True)
    assert not path.exists()
    assert not path.parent.exists()


def test_require_existing_defaults_off_so_local_stores_still_self_create(tmp_path: Path) -> None:
    """The fill outbox and tests legitimately create their own db."""
    q = LocalDurableBackend(tmp_path / "outbox.db")
    q.close()


# --------------------------------------------------------------------------- #
# Identity, not shape (the trading-1 case)
#
# The first version of _assert_shared_queue checked five SHAPES. The bogus file
# on trading-1 was 4096 bytes of valid SQLite containing a `queue` table --
# because our own code created it -- so it passed all five and was still the
# wrong file. These pin the checks that actually distinguish identity.
# --------------------------------------------------------------------------- #

def test_a_private_queue_our_own_code_created_still_passes_every_shape_check(tmp_path):
    """Establishes the premise: shape checks alone cannot catch this."""
    bogus = tmp_path / "private" / "queue.db"
    LocalDurableBackend(bogus).close()
    assert bogus.exists() and bogus.stat().st_size > 0
    _assert_shared_queue(bogus)  # every shape check passes -- and it is the wrong file


def test_identity_rejects_a_lookalike_with_a_different_inode(tmp_path):
    shared = tmp_path / "shared" / "queue.db"
    bogus = tmp_path / "private" / "queue.db"
    LocalDurableBackend(shared).close()
    LocalDurableBackend(bogus).close()
    _assert_shared_queue(shared, expected=shared)          # same file: fine
    with pytest.raises(SharedQueueMissing, match="not the expected file"):
        _assert_shared_queue(bogus, expected=shared)       # lookalike: refused


def test_identity_accepts_a_different_route_to_the_same_file(tmp_path):
    """Same inode via a different path spelling must still pass."""
    shared = tmp_path / "shared" / "queue.db"
    LocalDurableBackend(shared).close()
    roundabout = tmp_path / "shared" / ".." / "shared" / "queue.db"
    _assert_shared_queue(roundabout, expected=shared)


def test_expected_path_that_does_not_exist_is_refused(tmp_path):
    shared = tmp_path / "queue.db"
    LocalDurableBackend(shared).close()
    with pytest.raises(SharedQueueMissing, match="EXPECTED_PATH cannot be read"):
        _assert_shared_queue(shared, expected=tmp_path / "nowhere.db")


def test_a_relative_queue_path_is_refused(tmp_path, monkeypatch):
    """The trading-1 defect itself.

    A Windows path on POSIX has no leading '/', so it is one relative filename that
    resolves against the working directory -- which is how the bogus file came to sit
    inside the application directory. Refused before it can exist.
    """
    monkeypatch.chdir(tmp_path)
    LocalDurableBackend(Path("state") / "queue.db").close()   # make it genuinely exist
    with pytest.raises(SharedQueueMissing, match="not absolute"):
        _assert_shared_queue(Path("state") / "queue.db")


@pytest.mark.skipif(os.name == "nt",
                    reason="POSIX-only defect: on Windows a C: path really is absolute")
def test_a_windows_path_on_posix_is_refused(tmp_path, monkeypatch):
    r"""The trading-1 file, reproduced end to end.

    C:\Users\...\queue.db has no leading '/', so sqlite3 creates one oddly-named file in
    the working directory: 4096 bytes of valid SQLite with a queue table. Real, and wrong.
    """
    monkeypatch.chdir(tmp_path)
    winpath = Path(r"C:\Users\emman\OneDrive\shared\queue\queue.db")
    LocalDurableBackend(winpath).close()          # exactly what sqlite3 did on trading-1
    assert winpath.exists() and winpath.stat().st_size > 0
    with pytest.raises(SharedQueueMissing):
        _assert_shared_queue(winpath)


def test_the_windows_path_is_relative_under_posix_semantics():
    """Platform-independent proof of the property the POSIX test above relies on.

    This one runs on the Windows dev box, so the discriminating fact is verified here
    before the fix ships to the Linux host where the full test can actually execute.
    """
    p = PurePosixPath(r"C:\Users\emman\OneDrive\shared\queue\queue.db")
    assert not p.is_absolute(), "if this ever becomes absolute the guard needs rethinking"
    assert len(p.parts) == 1, "it is ONE filename containing backslashes, not a path"
