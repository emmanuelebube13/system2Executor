"""Tests for the LocalDurableBackend queue (publish/pull/ack/nack/DLQ + envelope)."""

from __future__ import annotations

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
