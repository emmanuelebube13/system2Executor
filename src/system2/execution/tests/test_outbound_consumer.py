"""EXEC-004 consumer tests — dedup, TTL, DLQ, session park, ack-after-durable, crash safety."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from system2.common.queue_backend import LocalDurableBackend, make_envelope
from system2.execution.outbound_consumer import OutboundConsumer, SqliteProcessedStore
from system2.execution.pipeline import ExecMode, ExecutionPipeline

WED = datetime(2026, 6, 24, 12, tzinfo=timezone.utc)   # in-session
SAT = datetime(2026, 6, 27, 12, tzinfo=timezone.utc)   # weekend
SUB = "AMS_Outbound_Queue"
PRICES = {"EUR_USD": 1.10000}


def _order_msg(**over) -> dict:
    payload = {
        "schema_version": "1",
        "message_id": "m-" + over.get("idempotency_key", "k1"),
        "idempotency_key": "k1",
        "correlation_id": "c1",
        "created_at": "2026-06-24T11:59:00Z",
        "instrument": "EUR_USD",
        "side": "BUY",
        "units": 10000,
        "granularity": "H1",
        "risk_context": {"atr": 0.0010},
    }
    payload.update(over)
    return payload


@pytest.fixture
def queue(tmp_path: Path) -> LocalDurableBackend:
    q = LocalDurableBackend(tmp_path / "queue.db")
    yield q
    q.close()


def _consumer(queue, tmp_path, *, clock=lambda: WED, submits=None, shadow=False, persist_fn=None):
    store = SqliteProcessedStore(tmp_path / "processed.db")
    pipe = ExecutionPipeline(
        mode=ExecMode.EXECUTION_ONLY, shadow=shadow, processed_store=store, clock=clock,
    )
    submit_fn = None if shadow else (lambda c: (submits.append(c) if submits is not None else None) or {"id": "f1"})
    return OutboundConsumer(
        queue=queue, subscription=SUB, pipeline=pipe,
        price_fn=lambda o: PRICES[o.instrument],
        submit_fn=submit_fn, persist_fn=persist_fn, clock=clock,
    ), store


def test_happy_path_executes_and_acks(queue, tmp_path):
    submits: list = []
    consumer, _ = _consumer(queue, tmp_path, submits=submits)
    queue.publish(SUB, _order_msg())
    stats = consumer.poll_once()
    assert stats.get("executed") == 1
    assert len(submits) == 1
    assert queue.pull(SUB) == []  # ack'd, nothing left


def test_duplicate_idempotency_key_not_resubmitted(queue, tmp_path):
    submits: list = []
    consumer, _ = _consumer(queue, tmp_path, submits=submits)
    queue.publish(SUB, _order_msg())
    queue.publish(SUB, _order_msg(message_id="m2"))  # same idempotency_key k1
    consumer.poll_once()
    consumer.poll_once()
    assert len(submits) == 1  # second is a duplicate -> skipped


def test_malformed_message_dead_lettered(queue, tmp_path):
    consumer, _ = _consumer(queue, tmp_path)
    bad = _order_msg()
    del bad["instrument"]
    queue.publish(SUB, bad)
    stats = consumer.poll_once()
    assert stats.get("dead_lettered") == 1
    assert queue.depth(f"{SUB}.dlq", "ready") == 1
    assert queue.pull(SUB) == []  # ack'd, no poison loop


def test_expired_order_dropped(queue, tmp_path):
    submits: list = []
    consumer, _ = _consumer(queue, tmp_path, submits=submits)
    consumer.max_age_sec = 300
    queue.publish(SUB, _order_msg(expires_at="2026-06-24T11:00:00Z"))
    stats = consumer.poll_once()
    assert stats.get("expired") == 1
    assert len(submits) == 0
    assert queue.pull(SUB) == []  # ack-dropped


def test_out_of_session_parked_not_submitted(queue, tmp_path):
    submits: list = []
    consumer, _ = _consumer(queue, tmp_path, clock=lambda: SAT, submits=submits)
    consumer.park_backoff_sec = 0.0  # so we can observe it return to ready
    queue.publish(SUB, _order_msg())
    stats = consumer.poll_once()
    assert stats.get("parked") == 1
    assert len(submits) == 0
    assert len(queue.pull(SUB)) == 1  # redelivered (parked, not lost)


def test_crash_safety_redelivery_not_resubmitted(queue, tmp_path):
    """Mark persists; a redelivered key after 'crash' is recognised and not re-submitted."""
    submits: list = []
    consumer, store = _consumer(queue, tmp_path, submits=submits)
    queue.publish(SUB, _order_msg())
    consumer.poll_once()
    assert len(submits) == 1
    store.close()

    # simulate restart: brand-new consumer + store pointed at the same files; redeliver
    consumer2, _ = _consumer(queue, tmp_path, submits=submits)
    queue.publish(SUB, _order_msg(message_id="redeliver"))  # same idempotency_key k1
    consumer2.poll_once()
    assert len(submits) == 1  # NOT re-submitted across the restart


def test_persist_failure_after_fill_is_not_resubmitted_on_redelivery(queue, tmp_path):
    """F-303 end-to-end: one transient Fact_Live_Trades failure after a successful fill.

    persist_fn raises => the consumer nacks => the queue redelivers. Before the fix the
    redelivery found no marker and placed a SECOND live order for one approved order.
    """
    submits: list = []
    calls = {"n": 0}

    def flaky_persist(_c, _f):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated Fact_Live_Trades write failure")

    consumer, _ = _consumer(queue, tmp_path, submits=submits, persist_fn=flaky_persist)
    consumer.park_backoff_sec = 0.0  # redeliver on the next poll, no sleeping
    queue.publish(SUB, _order_msg())

    assert consumer.poll_once() == {"exec_error": 1}
    assert len(submits) == 1
    assert consumer.poll_once() == {"duplicate": 1}
    assert len(submits) == 1  # the broker saw the order ONCE


def test_signal_id_guard_blocks_a_remitted_order_id(queue, tmp_path):
    """Defence in depth (F-206): a second order id for a signal already executed is dropped."""
    submits: list = []
    consumer, _ = _consumer(queue, tmp_path, submits=submits)
    queue.publish(SUB, _order_msg(idempotency_key="ord-a", signal_id="sig-9"))
    assert consumer.poll_once() == {"executed": 1}

    queue.publish(SUB, _order_msg(idempotency_key="ord-b", message_id="m-ord-b",
                                  signal_id="sig-9"))
    assert consumer.poll_once() == {"duplicate": 1}
    assert len(submits) == 1


def test_lag_tracking_updates(queue, tmp_path):
    consumer, _ = _consumer(queue, tmp_path)
    queue.publish(SUB, _order_msg())
    consumer.poll_once()
    assert consumer.lag.messages_seen == 1
    assert consumer.lag.last_poll_at == WED
    assert consumer.lag.last_message_created_at is not None
    assert consumer.lag.seconds_since_last_message(WED) == 0.0


def test_last_line_validation_rejects(queue, tmp_path):
    """A validate_fn failure -> REJECTED_VALIDATION, durably handled (ack), no submit."""
    submits: list = []
    consumer, _ = _consumer(queue, tmp_path, submits=submits)
    consumer.validate_fn = lambda c: (False, "units over cap")
    queue.publish(SUB, _order_msg())
    stats = consumer.poll_once()
    assert stats.get("rejected_validation") == 1
    assert len(submits) == 0
    assert queue.pull(SUB) == []  # ack'd (definitive rejection, not redelivered)
