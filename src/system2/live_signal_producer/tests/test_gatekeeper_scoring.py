"""EXEC-011 tests — champion gatekeeper loading, feature assembly, and scoring."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from system2.artifact_sync.features import compute_regime_features
from system2.live_signal_producer.gatekeeper import (
    GatekeeperScorer,
    get_threshold,
    price_position_20,
)
from system2.live_signal_producer.tests.conftest import (
    GATEKEEPER_FEATURES,
    make_candles,
    make_manifest,
    write_champion_set,
)

PROBS = [0.6, 0.1, 0.2, 0.1]  # semantic order: up, down, ranging, high-vol


# ----- threshold resolution --------------------------------------------------------
def test_threshold_prefers_regime_then_fallback_then_approval():
    manifest = make_manifest()
    assert get_threshold("Ranging", manifest) == 0.40
    assert get_threshold("Unknown-Regime", manifest) == 0.35        # fallback
    manifest["dynamic_thresholds"].pop("fallback")
    assert get_threshold("Unknown-Regime", manifest) == 0.20        # approval_threshold
    assert get_threshold(None, {"approval_threshold": 0.9}) == 0.9
    assert get_threshold(None, {}) == 0.20                          # hard default


# ----- price_position_20 -----------------------------------------------------------
def test_price_position_is_clamped_and_neutral_on_warmup():
    candles = make_candles(60)
    pos = price_position_20(candles)
    assert (pos.iloc[:19] == 0.5).all()          # warm-up -> neutral
    assert ((pos >= 0.0) & (pos <= 1.0)).all()   # clamped


def test_price_position_close_at_high_is_one():
    candles = make_candles(40).copy()
    candles.loc[candles.index[-1], "close"] = candles["high"].rolling(20).max().iloc[-1]
    assert price_position_20(candles).iloc[-1] == pytest.approx(1.0)


# ----- loading ----------------------------------------------------------------------
def test_load_and_score_roundtrip(champion_dir: Path):
    scorer = GatekeeperScorer()
    assert scorer.load(champion_dir) is True
    assert scorer.features == GATEKEEPER_FEATURES

    feats = compute_regime_features(make_candles(120))
    row = scorer.build_feature_row(feats, PROBS, "Trending-Up")
    assert row is not None
    assert set(row) == set(GATEKEEPER_FEATURES)
    assert row["regime_smoothed"] == "Trending-Up"
    assert row["prob_trending_up"] == pytest.approx(0.6)
    assert row["prob_high_vol"] == pytest.approx(0.1)

    gate = scorer.score(row, "Trending-Up")
    assert gate is not None
    assert 0.0 <= gate.score <= 1.0
    assert gate.threshold == 0.30
    assert gate.approved is (gate.score >= 0.30)


def test_load_failure_keeps_last_good(tmp_path: Path, champion_dir: Path):
    scorer = GatekeeperScorer()
    assert scorer.load(champion_dir) is True
    assert scorer.load(tmp_path / "does-not-exist") is False
    assert scorer.loaded is True  # still serving set-A
    feats = compute_regime_features(make_candles(120))
    assert scorer.score(scorer.build_feature_row(feats, PROBS, "Ranging"), "Ranging") is not None


def test_manifest_missing_mandatory_fields_refuses(tmp_path: Path):
    bad = make_manifest()
    bad.pop("approval_threshold")
    bad.pop("dynamic_thresholds")
    set_dir = write_champion_set(tmp_path / "bad-set", manifest=bad)
    scorer = GatekeeperScorer()
    assert scorer.load(set_dir) is False
    assert scorer.loaded is False


def test_manifest_empty_features_refuses(tmp_path: Path):
    set_dir = write_champion_set(tmp_path / "bad-set2", manifest=make_manifest(features=[]))
    assert GatekeeperScorer().load(set_dir) is False


# ----- feature assembly guards ------------------------------------------------------
def test_warmup_bars_refuse_to_score(champion_dir: Path):
    scorer = GatekeeperScorer()
    scorer.load(champion_dir)
    feats = compute_regime_features(make_candles(10))  # inside ATR/vol warm-up
    assert scorer.build_feature_row(feats, PROBS, "Ranging") is None


def test_unknown_manifest_feature_refuses(tmp_path: Path):
    manifest = make_manifest(features=GATEKEEPER_FEATURES + ["mystery_feature"])
    set_dir = write_champion_set(tmp_path / "mystery", manifest=manifest)
    scorer = GatekeeperScorer()
    assert scorer.load(set_dir) is True
    feats = compute_regime_features(make_candles(120))
    assert scorer.build_feature_row(feats, PROBS, "Ranging") is None


def test_score_before_load_returns_none():
    scorer = GatekeeperScorer()
    assert scorer.score({"x": 1}, "Ranging") is None
    assert scorer.build_feature_row(None, PROBS, "Ranging") is None


# ----- deterministic scoring --------------------------------------------------------
def test_scoring_is_deterministic(champion_dir: Path):
    scorer = GatekeeperScorer()
    scorer.load(champion_dir)
    feats = compute_regime_features(make_candles(120))
    row = scorer.build_feature_row(feats, PROBS, "Ranging")
    s1 = scorer.score(row, "Ranging")
    s2 = scorer.score(dict(row), "Ranging")
    assert s1.score == pytest.approx(s2.score)


def test_regime_changes_threshold_not_pipeline(champion_dir: Path):
    scorer = GatekeeperScorer()
    scorer.load(champion_dir)
    feats = compute_regime_features(make_candles(120))
    row = scorer.build_feature_row(feats, PROBS, "High-Vol")
    gate = scorer.score(row, "High-Vol")
    assert gate.threshold == 0.55
