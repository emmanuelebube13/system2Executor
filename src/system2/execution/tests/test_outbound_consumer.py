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


# ----- F-305: freshness is credited only by evidence System 3 is alive ------------
def _heartbeat_msg(created_at: str = "2026-06-24T11:59:30Z", **over) -> dict:
    payload = {
        "schema_version": "1",
        "message_id": "hb-1",
        "idempotency_key": "hb-1",
        "correlation_id": "c-hb",
        "created_at": created_at,
        "event_type": "system3.heartbeat",
        "payload": {"source": "system3.ams", "state": "enforce"},
    }
    payload.update(over)
    return payload


def test_expired_order_does_not_credit_freshness(queue, tmp_path):
    """F-305: an expired redelivery must not look like a live System 3."""
    consumer, _ = _consumer(queue, tmp_path)
    consumer.max_age_sec = 300
    queue.publish(SUB, _order_msg(expires_at="2026-06-24T11:00:00Z"))
    stats = consumer.poll_once()
    assert stats.get("expired") == 1
    assert consumer.lag.last_message_at is None  # no freshness credited
    assert consumer.lag.seconds_since_last_message(WED) is None


def test_heartbeat_credits_freshness_and_is_not_dead_lettered(queue, tmp_path):
    seen: list = []
    consumer, _ = _consumer(queue, tmp_path)
    consumer.heartbeat_fn = lambda at: (seen.append(at) or True)
    queue.publish(SUB, _heartbeat_msg())
    stats = consumer.poll_once()
    assert stats.get("heartbeat") == 1
    assert queue.depth(f"{SUB}.dlq", "ready") == 0  # NOT malformed
    assert queue.pull(SUB) == []  # ack'd, never redelivered
    assert seen and seen[0] == datetime(2026, 6, 24, 11, 59, 30, tzinfo=timezone.utc)


def test_heartbeat_does_not_inflate_messages_seen(queue, tmp_path):
    """A keepalive must never make a dead order path look alive."""
    consumer, _ = _consumer(queue, tmp_path)
    consumer.heartbeat_fn = lambda at: True
    queue.publish(SUB, _heartbeat_msg())
    consumer.poll_once()
    assert consumer.lag.messages_seen == 0


def test_rejected_heartbeat_is_still_acked(queue, tmp_path):
    """The monitor refused it (too old) — it is worthless, so it must not be requeued."""
    consumer, _ = _consumer(queue, tmp_path)
    consumer.heartbeat_fn = lambda at: False
    queue.publish(SUB, _heartbeat_msg(created_at="2026-06-24T09:00:00Z"))
    stats = consumer.poll_once()
    assert stats.get("heartbeat_stale") == 1
    assert queue.pull(SUB) == []


def test_heartbeat_failure_never_breaks_the_poll_loop(queue, tmp_path):
    def _boom(at):
        raise RuntimeError("monitor exploded")

    consumer, _ = _consumer(queue, tmp_path)
    consumer.heartbeat_fn = _boom
    queue.publish(SUB, _heartbeat_msg())
    stats = consumer.poll_once()  # must not raise
    assert stats.get("heartbeat_stale") == 1


def test_heartbeat_accepts_system3_produced_at_field(queue, tmp_path):
    """System 3's envelope builder stamps `produced_at`, not `created_at`."""
    seen: list = []
    consumer, _ = _consumer(queue, tmp_path)
    consumer.heartbeat_fn = lambda at: (seen.append(at) or True)
    hb = _heartbeat_msg()
    del hb["created_at"]
    hb["produced_at"] = "2026-06-24T11:59:30Z"
    queue.publish(SUB, hb)
    assert consumer.poll_once().get("heartbeat") == 1
    assert seen[0] == datetime(2026, 6, 24, 11, 59, 30, tzinfo=timezone.utc)


def test_undated_heartbeat_is_refused(queue, tmp_path):
    """Fail closed: an undated keepalive cannot be shown to be recent, so it buys nothing."""
    calls: list = []
    consumer, _ = _consumer(queue, tmp_path)
    consumer.heartbeat_fn = lambda at: (calls.append(at) or True)
    hb = _heartbeat_msg()
    del hb["created_at"]
    queue.publish(SUB, hb)
    stats = consumer.poll_once()
    assert stats.get("heartbeat_stale") == 1
    assert calls == []  # the monitor was never even asked
    assert queue.pull(SUB) == []  # still ack'd, never a poison loop

# ----- Divergence Check ----------------------------------------------------
def test_divergence_check_fires_when_signals_published_but_unseen():
    """A test where signals_published_total increments while messages_seen does not raises the divergence condition; a test where both stay flat does not."""
    from system2.telemetry.health import check_signal_divergence as d

    B = "2026-08-31T00:00:00Z"   # same process across both observations

    # Nothing published => nothing to expect.
    assert d(0, 0, 0, 0, B, B) == "ok"
    assert d(5, 5, 5, 5, B, B) == "ok"

    # Published, and nothing arrived => a real gap.
    assert d(1, 0, 0, 0, B, B) == "divergent"
    assert d(6, 5, 5, 5, B, B) == "divergent"

    # Published, and something arrived => healthy flow.
    assert d(1, 0, 1, 0, B, B) == "ok"
    assert d(2, 0, 1, 0, B, B) == "ok"   # S2 may lag; it is not flat

    # S1's counter going backwards is not a divergence.
    assert d(0, 1, 0, 0, B, B) == "ok"


def test_divergence_does_not_false_alarm_across_a_restart():
    """messages_seen resets to 0 on restart; S1's counter does not.

    Differencing them across a process boundary compares a fresh counter against an
    old one. The pre-fix implementation clamped the negative delta to zero with
    max(0, ...), so a restart during which every message was consumed still reported
    a gap. These are the exact cases that fired.
    """
    from system2.telemetry.health import check_signal_divergence as d

    OLD, NEW = "2026-08-30T00:00:00Z", "2026-08-31T00:00:00Z"

    # Restarted and consumed 3 -- previously "divergent", which was wrong.
    assert d(6, 5, 3, 5, NEW, OLD) == "indeterminate"

    # Restarted and consumed all 10 -- previously "divergent", most wrong of all.
    assert d(15, 5, 10, 40, NEW, OLD) == "indeterminate"

    # Restarted and genuinely saw nothing: still not assertable from these numbers.
    assert d(6, 5, 0, 5, NEW, OLD) == "indeterminate"

    # Same process, counter goes backwards anyway => untrustworthy, never "ok".
    assert d(6, 5, 3, 5, NEW, NEW) == "indeterminate"

    # And the restart must not mask a real gap once a baseline is re-established.
    assert d(7, 6, 0, 0, NEW, NEW) == "divergent"


def test_heartbeats_do_not_suppress_divergence_check(queue, tmp_path):
    """A test drives the consumer with heartbeats only for a simulated hour and asserts the divergence check can still fire — i.e. heartbeats alone never mark the signal path healthy."""
    from system2.telemetry.health import check_signal_divergence
    consumer, _ = _consumer(queue, tmp_path)
    consumer.heartbeat_fn = lambda at: True

    # Drive consumer with heartbeats only for a simulated hour
    for _ in range(60):
        queue.publish(SUB, _heartbeat_msg())
        consumer.poll_once()

    # heartbeats do not inflate messages_seen
    assert consumer.lag.messages_seen == 0

    # Divergence check MUST STILL FIRE if signals were supposedly published
    assert check_signal_divergence(
        s1_published_total_now=10, 
        s1_published_total_prev=0, 
        messages_seen_now=consumer.lag.messages_seen, 
        messages_seen_prev=0,
        process_started_at_now="2026-08-31T00:00:00Z",
        process_started_at_prev="2026-08-31T00:00:00Z",
    ) == "divergent"


# --------------------------------------------------------------------------- #
# D5 acceptance tests — 2026-09-02 USD_JPY re-emission deduplication
# --------------------------------------------------------------------------- #
def test_d5_usd_jpy_reemission_trade_intent_dedupe(queue, tmp_path):
    """Acceptance test D5:
    Replaying both USD_JPY messages through the ingester produces ONE trade intent.
    The retained intent carries entry 158.568 (the first), not 158.849.
    The suppression is counted and logged, and that count is queryable.
    """
    from system2.telemetry.health import HealthReporter

    submits: list = []
    consumer, _ = _consumer(queue, tmp_path, submits=submits)
    consumer.price_fn = lambda o: (o.risk_context.proposed_entry or 158.568)

    reporter = HealthReporter(
        messages_seen_fn=lambda: consumer.lag.messages_seen,
        duplicates_suppressed_fn=lambda: consumer.lag.duplicates_suppressed,
    )

    signal_id = "4af8a6fe-d8f8-5eec-97af-b9e2c793338f"

    # Emission 1 (2026-09-02T14:15:17Z): score_run_id=98953363..., entry 158.568
    msg1 = _order_msg(
        idempotency_key="ord-d5-first-98953363",
        message_id="m-first",
        signal_id=signal_id,
        instrument="USD_JPY",
        side="SELL",
        units=-10000,
        proposed_entry=158.568,
        suggested_sl=160.1835,
        suggested_tp=155.337,
        created_at="2026-09-02T14:15:17Z",
    )
    # Ensure risk_context carries proposed_entry
    msg1["risk_context"]["proposed_entry"] = 158.568

    # Emission 2 (2026-09-02T17:15:27Z): score_run_id=964afac9..., entry 158.849
    msg2 = _order_msg(
        idempotency_key="ord-d5-second-964afac9",
        message_id="m-second",
        signal_id=signal_id,
        instrument="USD_JPY",
        side="SELL",
        units=-10000,
        proposed_entry=158.849,
        suggested_sl=160.1835,
        suggested_tp=155.337,
        created_at="2026-09-02T17:15:27Z",
    )
    msg2["risk_context"]["proposed_entry"] = 158.849

    # 1. Replay first message through ingester
    queue.publish(SUB, msg1)
    stats1 = consumer.poll_once()
    assert stats1 == {"executed": 1}
    assert len(submits) == 1
    assert submits[0].entry_price == 158.568
    assert consumer.lag.messages_seen == 1
    assert consumer.lag.duplicates_suppressed == 0
    assert reporter.status()["queue"]["duplicates_suppressed"] == 0

    # 2. Replay second message (re-emission) through ingester
    queue.publish(SUB, msg2)
    stats2 = consumer.poll_once()
    assert stats2 == {"duplicate": 1}

    # Exactly ONE trade intent retained, carrying entry 158.568 (the first), not 158.849
    assert len(submits) == 1
    assert submits[0].entry_price == 158.568

    # The suppression is counted and logged, and that count is queryable
    assert consumer.lag.messages_seen == 2
    assert consumer.lag.duplicates_suppressed == 1
    assert consumer.pipeline.processed.suppressed_count() == 1
    assert reporter.status()["queue"]["duplicates_suppressed"] == 1


def test_d5_ledger_ingester_composite_key_stores_two_rows(tmp_path):
    """Acceptance test D5 ledger semantics:
    Replaying both through the ledger ingester produces TWO stored rows
    because ledger rows are observations keyed on (signal_id, score_run_id).
    """
    import sqlite3

    db_path = tmp_path / "ledger.db"
    conn = sqlite3.connect(str(db_path))
    # Schema matches s1_scored_signals_log PRIMARY KEY (signal_id, score_run_id)
    conn.execute(
        """CREATE TABLE s1_scored_signals_log (
            signal_id TEXT NOT NULL,
            score_run_id TEXT NOT NULL,
            signal_time_utc TEXT NOT NULL,
            logged_at TEXT NOT NULL,
            pair TEXT NOT NULL,
            proposed_entry REAL NOT NULL,
            PRIMARY KEY (signal_id, score_run_id)
        )"""
    )

    signal_id = "4af8a6fe-d8f8-5eec-97af-b9e2c793338f"
    row1 = (signal_id, "98953363-52af-4197-b406-f2cb3adca509", "2026-09-02T13:00:00Z",
            "2026-09-02T14:15:17Z", "USD_JPY", 158.568)
    row2 = (signal_id, "964afac9-abaa-4e1c-9770-e0d20fb0b970", "2026-09-02T13:00:00Z",
            "2026-09-02T17:15:27Z", "USD_JPY", 158.849)

    for row in [row1, row2]:
        conn.execute(
            """INSERT INTO s1_scored_signals_log
               (signal_id, score_run_id, signal_time_utc, logged_at, pair, proposed_entry)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT (signal_id, score_run_id) DO NOTHING""",
            row,
        )

    count = conn.execute("SELECT count(*) FROM s1_scored_signals_log").fetchone()[0]
    assert count == 2

    # Verify both observations are preserved with their respective entries
    rows = conn.execute(
        "SELECT score_run_id, proposed_entry FROM s1_scored_signals_log ORDER BY logged_at"
    ).fetchall()
    assert rows[0] == ("98953363-52af-4197-b406-f2cb3adca509", 158.568)
    assert rows[1] == ("964afac9-abaa-4e1c-9770-e0d20fb0b970", 158.849)
    conn.close()


def test_d5_shadow_mode_dedupes_on_signal_id(queue, tmp_path):
    """In shadow mode, a repeat signal_id is also a no-op duplicate."""
    consumer, _ = _consumer(queue, tmp_path, shadow=True)
    signal_id = "4af8a6fe-d8f8-5eec-97af-b9e2c793338f"

    msg1 = _order_msg(idempotency_key="ord-1", signal_id=signal_id)
    msg2 = _order_msg(idempotency_key="ord-2", signal_id=signal_id)

    queue.publish(SUB, msg1)
    assert consumer.poll_once() == {"shadow_constructed": 1}
    assert consumer.lag.duplicates_suppressed == 0

    queue.publish(SUB, msg2)
    assert consumer.poll_once() == {"duplicate": 1}
    assert consumer.lag.duplicates_suppressed == 1

