"""EXEC-011 tests — bar-based signal dedup store."""

from __future__ import annotations

from pathlib import Path

from system2.live_signal_producer.dedup import SignalDedupStore, make_dedup_key


def test_make_dedup_key_shape():
    key = make_dedup_key("EUR_USD", "H1", "2026-07-13T12:00:00Z", 10)
    assert key == "EUR_USD:H1:2026-07-13T12:00:00Z:10"


def test_first_claim_wins_second_is_ignored(tmp_path: Path):
    store = SignalDedupStore(tmp_path / "dedup.db")
    key = make_dedup_key("EUR_USD", "H1", "2026-07-13T12:00:00Z", 10)
    assert store.claim(key, "sig-1", "2026-07-13T12:00:05Z") is True
    assert store.claim(key, "sig-2", "2026-07-13T12:00:35Z") is False
    store.close()


def test_distinct_strategy_and_bar_are_independent(tmp_path: Path):
    store = SignalDedupStore(tmp_path / "dedup.db")
    base = ("EUR_USD", "H1", "2026-07-13T12:00:00Z")
    assert store.claim(make_dedup_key(*base, 10), "s1", "t") is True
    assert store.claim(make_dedup_key(*base, 12), "s2", "t") is True  # other strategy
    assert store.claim(make_dedup_key("EUR_USD", "H1", "2026-07-13T13:00:00Z", 10), "s3", "t") is True
    assert store.claim(make_dedup_key("EUR_USD", "H4", "2026-07-13T12:00:00Z", 10), "s4", "t") is True
    store.close()


def test_seen_is_readonly(tmp_path: Path):
    store = SignalDedupStore(tmp_path / "dedup.db")
    key = make_dedup_key("GBP_USD", "H1", "2026-07-13T12:00:00Z", 10)
    assert store.seen(key) is False
    assert store.claim(key, "sig-1", "t") is True  # seen() did not claim
    assert store.seen(key) is True
    store.close()


def test_claims_survive_restart(tmp_path: Path):
    path = tmp_path / "dedup.db"
    store = SignalDedupStore(path)
    key = make_dedup_key("EUR_USD", "H1", "2026-07-13T12:00:00Z", 10)
    assert store.claim(key, "sig-1", "t") is True
    store.close()

    reopened = SignalDedupStore(path)  # a restart must not re-publish the same bar
    assert reopened.seen(key) is True
    assert reopened.claim(key, "sig-2", "t") is False
    reopened.close()
