"""OBS-001 tests — persistent scored-signal ledger: fail-open writes, correct
aggregates, restart survival, and producer integration via record()."""

from __future__ import annotations

import sqlite3

from system2.live_signal_producer.signal_ledger import SignalLedger


def _row(**over):
    base = dict(bar_time="2026-07-15T09:00:00Z", pair="EUR_USD", granularity="H1",
                regime="Ranging", strategy_id="10", direction="short",
                score=0.61, threshold=0.6, approved=True, published=True,
                signal_id="sig-1")
    base.update(over)
    return base


def test_record_and_aggregate(tmp_path):
    led = SignalLedger(tmp_path / "ledger.db")
    led.record(**_row())
    led.record(**_row(approved=False, published=False, signal_id="sig-2", score=0.4))
    led.record(**_row(published=False, signal_id="sig-3"))  # approved but shadow
    agg = led.daily_aggregates()
    assert agg["total_evaluations"] == 3
    assert agg["total_approved"] == 2
    assert agg["total_published"] == 1
    assert agg["measured_approval_rate"] == round(2 / 3, 4)
    assert len(agg["days"]) == 1
    day = agg["days"][0]
    assert (day["evaluations"], day["approved"], day["published"]) == (3, 2, 1)


def test_counts_survive_reopen_like_a_restart(tmp_path):
    path = tmp_path / "ledger.db"
    led1 = SignalLedger(path)
    led1.record(**_row())
    led1.record(**_row(signal_id="sig-2"))
    # new instance over the same file = process restart
    led2 = SignalLedger(path)
    agg = led2.daily_aggregates()
    assert agg["total_evaluations"] == 2
    assert agg["since"] is not None


def test_unwritable_path_fails_open(tmp_path):
    # a directory where the DB file should be -> init fails, methods become no-ops
    bad = tmp_path / "ledger.db"
    bad.mkdir()
    led = SignalLedger(bad)
    led.record(**_row())  # must not raise
    assert led.daily_aggregates() is None


def test_corrupt_db_read_fails_open(tmp_path):
    path = tmp_path / "ledger.db"
    led = SignalLedger(path)
    led.record(**_row())
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE signal_evaluations")
    led._agg_cache = None  # bypass the read cache
    assert led.daily_aggregates() is None  # logged, not raised


def test_aggregates_cache_serves_within_window(tmp_path):
    led = SignalLedger(tmp_path / "ledger.db")
    led.record(**_row())
    first = led.daily_aggregates()
    led.record(**_row(signal_id="sig-2"))
    # within the 30s cache window the stale-but-cheap copy is returned
    assert led.daily_aggregates() is first


# ----- FIX_PLAN 2.1(d): seeding the runtime approval-rate monitor ------------------
def test_recent_verdicts_returns_oldest_first_and_is_bounded(tmp_path):
    led = SignalLedger(tmp_path / "ledger.db")
    pattern = [True, False, True, True, False]
    for i, ok in enumerate(pattern):
        led.record(**{**_row(signal_id=f"sig-{i}"), "approved": ok})
    assert led.recent_verdicts(10) == pattern          # oldest -> newest
    assert led.recent_verdicts(2) == pattern[-2:]      # the LAST n, not the first
    assert led.recent_verdicts(0) == []


def test_recent_verdicts_on_an_unusable_ledger_is_empty_not_fatal(tmp_path):
    import sqlite3

    path = tmp_path / "ledger.db"
    led = SignalLedger(path)
    led.record(**_row())
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE signal_evaluations")
    assert led.recent_verdicts(50) == []               # logged, not raised
