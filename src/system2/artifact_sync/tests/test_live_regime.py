"""EXEC-002 tests — live regime detector (synthetic HMM bundle + fake candle source)."""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pytest
from hmmlearn.hmm import GaussianHMM
from sklearn.preprocessing import StandardScaler

from system2.artifact_sync import regime_mapping as M
from system2.artifact_sync.features import compute_regime_features
from system2.artifact_sync.live_regime import (
    LiveRegimeDetector,
    OnlineDebouncer,
    RegimeObservation,
)

FEATURE_NAMES = ["atr_14", "adx_14", "volatility_20", "returns_1", "trend_20"]
WEIGHTS = [1.0, 1.0, 1.0, 0.5, 3.0]


# ----- candle generation ----------------------------------------------------------
def _make_candles(n: int = 200, seed: int = 0, drift: float = 0.0, vol: float = 0.001) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rets = rng.normal(drift, vol, n)
    close = 1.10 * np.exp(np.cumsum(rets))
    high = close * (1 + rng.uniform(0, vol, n))
    low = close * (1 - rng.uniform(0, vol, n))
    times = pd.date_range("2026-01-01", periods=n, freq="h", tz="UTC")
    return pd.DataFrame({"bar_time_utc": times, "open": close, "high": high, "low": low,
                         "close": close, "volume": 1000.0})


class FakeCandleSource:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.calls = 0

    def fetch_candles(self, instrument, granularity, count):
        self.calls += 1
        return self.df.tail(count).reset_index(drop=True)


class FailingCandleSource:
    def fetch_candles(self, instrument, granularity, count):
        raise ConnectionError("oanda down")


# ----- bundle fixture -------------------------------------------------------------
def _train_bundle(granularity: str = "H1") -> dict:
    """Train a tiny GaussianHMM on synthetic features and package like MODEL-003."""
    df = _make_candles(400, seed=42, drift=0.0002, vol=0.002)
    feats = compute_regime_features(df).dropna(subset=FEATURE_NAMES)
    X = feats[FEATURE_NAMES].to_numpy(dtype="float64")
    scaler = StandardScaler().fit(X)
    weights = np.array(WEIGHTS)
    Xs = scaler.transform(X) * weights
    hmm = GaussianHMM(n_components=4, covariance_type="full", n_iter=50, random_state=42)
    hmm.fit(Xs)
    mapping = M.map_states_to_labels(hmm.means_, FEATURE_NAMES, "trend_20")
    return {
        "models": {granularity: {"model": hmm, "scaler": scaler, "mapping": mapping, "weights": WEIGHTS}},
        "feature_names": FEATURE_NAMES,
        "direction_feature": "trend_20",
        "trend_window": 20,
        "seed": 42,
        "model_version": "test",
        "semantic_order": M.SEMANTIC_ORDER,
    }


def _install_bundle(root: Path, set_id: str, bundle: dict, link_name: str = "active") -> None:
    set_dir = root / "sets" / set_id
    set_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, set_dir / "regime_hmm.pkl")
    link = root / link_name
    if link.exists() or link.is_symlink():
        link.unlink()
    import os
    os.symlink(os.path.relpath(set_dir, root), link)


@pytest.fixture
def detector(tmp_path):
    root = tmp_path / "artifacts"
    root.mkdir()
    _install_bundle(root, "set-A", _train_bundle("H1"))
    src = FakeCandleSource(_make_candles(200, seed=7, drift=0.0002, vol=0.002))
    det = LiveRegimeDetector(artifact_root=root, candle_source=src)
    return det, root, src


# ----- OnlineDebouncer (pure) -----------------------------------------------------
def test_debouncer_holds_single_flip():
    d = OnlineDebouncer(window=3, high_conf=0.99)
    assert d.update("Ranging", 0.5) == "Ranging"      # first adopts
    assert d.update("Trending-Up", 0.5) == "Ranging"  # single flip held
    assert d.update("Ranging", 0.5) == "Ranging"


def test_debouncer_switches_after_window():
    d = OnlineDebouncer(window=3, high_conf=0.99)
    d.update("Ranging", 0.5)
    assert d.update("High-Vol", 0.5) == "Ranging"   # 1
    assert d.update("High-Vol", 0.5) == "Ranging"   # 2
    assert d.update("High-Vol", 0.5) == "High-Vol"  # 3 -> switch


def test_debouncer_switches_on_high_confidence():
    d = OnlineDebouncer(window=5, high_conf=0.90)
    d.update("Ranging", 0.5)
    assert d.update("Trending-Up", 0.95) == "Trending-Up"  # immediate on high conf


# ----- detection ------------------------------------------------------------------
def test_detect_returns_valid_observation(detector):
    det, _root, _src = detector
    obs = det.detect("EUR_USD", "H1")
    assert isinstance(obs, RegimeObservation)
    assert obs.smoothed_label in M.SEMANTIC_ORDER
    assert obs.model_set_id == "set-A"
    assert abs(sum(obs.raw_probs) - 1.0) < 1e-6
    assert obs.stale is False


def test_detect_is_deterministic(detector):
    det, root, _src = detector
    # Two detectors, same bundle + candles -> identical raw probs.
    obs1 = det.detect("EUR_USD", "H1")
    src2 = FakeCandleSource(_make_candles(200, seed=7, drift=0.0002, vol=0.002))
    det2 = LiveRegimeDetector(artifact_root=root, candle_source=src2)
    obs2 = det2.detect("EUR_USD", "H1")
    assert obs1.raw_probs == pytest.approx(obs2.raw_probs)
    assert obs1.raw_state == obs2.raw_state


def test_unknown_granularity_is_stale(detector):
    det, _root, _src = detector
    obs = det.detect("EUR_USD", "H4")  # bundle only has H1
    assert obs.stale is True
    assert "granularity" in (obs.note or "")


def test_candle_fetch_failure_serves_last_good(tmp_path):
    root = tmp_path / "artifacts"
    root.mkdir()
    _install_bundle(root, "set-A", _train_bundle("H1"))
    good = FakeCandleSource(_make_candles(200, seed=7, drift=0.0002, vol=0.002))
    det = LiveRegimeDetector(artifact_root=root, candle_source=good)
    first = det.detect("EUR_USD", "H1")
    assert first.stale is False
    # Now swap to a failing source and re-detect -> last good, marked stale.
    det.candle_source = FailingCandleSource()
    second = det.detect("EUR_USD", "H1")
    assert second.stale is True
    assert second.smoothed_label == first.smoothed_label


def test_insufficient_history_holds(detector):
    det, _root, _src = detector
    det.candle_source = FakeCandleSource(_make_candles(10))  # < warmup
    obs = det.detect("EUR_USD", "H1")
    assert obs.stale is True
    assert "history" in (obs.note or "")


def test_corrupt_active_falls_back_to_last_good(tmp_path):
    root = tmp_path / "artifacts"
    root.mkdir()
    # last_good is a valid bundle; active is corrupt.
    _install_bundle(root, "set-A", _train_bundle("H1"), link_name="last_good")
    bad = root / "sets" / "set-B"
    bad.mkdir(parents=True)
    (bad / "regime_hmm.pkl").write_bytes(b"not a pickle")
    import os
    active = root / "active"
    os.symlink(os.path.relpath(bad, root), active)
    det = LiveRegimeDetector(
        artifact_root=root,
        candle_source=FakeCandleSource(_make_candles(200, seed=7, drift=0.0002, vol=0.002)),
    )
    obs = det.detect("EUR_USD", "H1")
    assert obs.smoothed_label in M.SEMANTIC_ORDER  # served from last_good
    assert det._bundle_source == "last_good"
