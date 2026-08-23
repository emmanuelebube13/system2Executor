"""EXEC-002 tests — live regime detector (synthetic HMM bundle + fake candle source)."""

from __future__ import annotations

import logging
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


def test_posterior_uses_the_sequence_not_just_the_last_bar(detector):
    """Regression: inference must score the window, never a length-1 sequence.

    hmmlearn collapses a single-bar sequence to normalize(startprob_ * emission),
    which discards transmat_ and lets a degenerate startprob_ pin the label forever.
    The 2026-07 production bundle had H1 startprob_ = [0.2, 0, 0.8, 0], so two of the
    four regimes were unreachable for every input — the classifier sat on one label
    for 832 straight observations. Guard both halves: the detector must feed >1 row,
    and its posterior must match the sequence result rather than the last-bar one.
    """
    det, _root, _src = detector
    det.load_bundle()
    candles = _src.df.tail(det.lookback).reset_index(drop=True)
    mat, _as_of = det._feature_matrix(candles)
    assert mat.shape[0] > 1, "feature matrix must carry the window, not one bar"

    mo = det._bundle["models"]["H1"]
    x = mo["scaler"].transform(mat) * np.asarray(mo["weights"], dtype="float64")
    seq_probs = np.asarray(mo["model"].predict_proba(x))[-1]
    single_probs = np.asarray(mo["model"].predict_proba(x[-1:].copy()))[0]

    got = det._predict_probs("H1", mat)
    assert got == pytest.approx(seq_probs)
    # A single-bar posterior can never put mass on a zero-startprob state; the
    # sequence posterior can. If the two ever coincide this test is not proving much.
    zero_start = [i for i, p in enumerate(mo["model"].startprob_) if p == 0.0]
    for i in zero_start:
        assert single_probs[i] == 0.0


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


# ----- P2: reload on bundle swap --------------------------------------------------
#
# load_bundle used to return early whenever a bundle was in memory, and nothing in
# production ever passed force=True. The downloader's atomic swap therefore replaced the
# files while a long-lived detector kept inferring from the set it loaded at startup --
# silently, because the swap itself worked. It also made a post-sync reference-vector
# replay meaningless: the replay would exercise the new bundle while production served
# the old one.

def test_detector_reloads_when_the_active_set_is_swapped(detector):
    det, root, _src = detector
    assert det.load_bundle()
    assert det._bundle_set_id == "set-A"
    first = det._bundle

    _install_bundle(root, "set-B", _train_bundle("H1"))     # atomic swap, as the downloader does

    assert det.load_bundle()
    assert det._bundle_set_id == "set-B", "detector kept serving the pre-swap bundle"
    assert det._bundle is not first, "bundle object was not replaced"


def test_detect_picks_up_a_swap_without_an_explicit_force(detector):
    """The production path calls detect() -> load_bundle() with no force argument."""
    det, root, _src = detector
    det.detect("EUR_USD", "H1")
    assert det._bundle_set_id == "set-A"

    _install_bundle(root, "set-B", _train_bundle("H1"))

    obs = det.detect("EUR_USD", "H1")
    assert det._bundle_set_id == "set-B"
    assert obs.model_set_id == "set-B", "observations were stamped with the stale set"


def test_no_reload_when_the_set_is_unchanged(detector):
    """The cache must still work — a reload per detect() would be a real cost."""
    det, _root, _src = detector
    assert det.load_bundle()
    first = det._bundle
    for _ in range(5):
        assert det.load_bundle()
    assert det._bundle is first, "bundle was reloaded despite no swap"


def test_serving_from_last_good_does_not_reload_every_call(detector, monkeypatch):
    """active unloadable -> we serve last_good, but must not retry the broken load forever.

    The loaded id (last_good's) and the on-disk id (active's) legitimately differ here, so
    comparing those two directly would re-attempt the corrupt joblib.load on every call.
    """
    det, root, _src = detector
    _install_bundle(root, "set-good", _train_bundle("H1"), link_name="last_good")
    (root / "sets" / "set-A" / "regime_hmm.pkl").write_bytes(b"not a joblib file")

    assert det.load_bundle()
    assert det._bundle_source == "last_good"
    assert det._bundle_set_id == "set-good"

    calls: list[str] = []
    real_load = joblib.load

    def counting_load(path, *a, **kw):
        calls.append(str(path))
        return real_load(path, *a, **kw)

    monkeypatch.setattr("system2.artifact_sync.live_regime.joblib.load", counting_load)
    for _ in range(5):
        assert det.load_bundle()
    assert calls == [], f"re-attempted the broken load {len(calls)} times"


# ----- feature contract (the 2026-08-23 atr_pct_14 incident) ----------------------
#
# System 1 bumped feature_set_version to 1.1.0 and the vector became
# ["atr_pct_14", ...]. System 2 produced atr_14. The mismatch surfaced as a KeyError
# once per inference call -- WARNING, swallowed -- so 100% of regime detections failed
# while the service reported itself healthy. It only became visible when the P2 cache
# fix stopped it serving a four-day-stale bundle whose contract happened to match.

def test_atr_pct_is_atr_over_close_and_dimensionless():
    """The normalisation is the point: comparable across a 0.7 AUD_USD and a 159 USD_JPY."""
    cheap = _make_candles(120, seed=3, vol=0.001)
    dear = cheap.copy()
    for col in ("open", "high", "low", "close"):
        dear[col] = dear[col] * 145.0          # same shape, ~145x the price

    fc = compute_regime_features(cheap)
    fd = compute_regime_features(dear)

    assert np.allclose(fc["atr_pct_14"].dropna().to_numpy(),
                       (fc["atr_14"] / fc["close"]).dropna().to_numpy(), rtol=1e-12)
    # absolute ATR scales with price; the normalised one does not
    assert fd["atr_14"].iloc[-1] == pytest.approx(fc["atr_14"].iloc[-1] * 145.0, rel=1e-9)
    assert fd["atr_pct_14"].iloc[-1] == pytest.approx(fc["atr_pct_14"].iloc[-1], rel=1e-9)
    # warm-up inherited from atr_14, not invented
    assert fc["atr_pct_14"].isna().sum() == fc["atr_14"].isna().sum()


def test_bundle_wanting_an_unproducible_feature_is_refused_at_load(detector):
    """Refuse at load, not once per inference.

    The failure being per-call is what made it survivable-looking: a WARNING every sweep,
    no CRITICAL, and a service that considered itself up while producing no regimes at all.
    """
    det, root, _src = detector
    bundle = _train_bundle("H1")
    bundle["feature_names"] = ["atr_pct_14", "adx_14", "no_such_feature_v9"]
    bundle["feature_set_version"] = "9.9.9"
    _install_bundle(root, "set-future", bundle)

    # The system2 root logger sets propagate=False and binds sys.stdout at configure
    # time, so neither caplog nor capsys sees it. Attach our own handler.
    records: list[logging.LogRecord] = []

    class _Collect(logging.Handler):
        def emit(self, record): records.append(record)

    lg = logging.getLogger("system2.artifact_sync.live_regime")
    h = _Collect()
    lg.addHandler(h)
    try:
        assert det.load_bundle() is False, "a bundle we cannot satisfy must not load"
    finally:
        lg.removeHandler(h)

    assert det._bundle is None
    # and it says WHICH feature, at CRITICAL -- the old failure was a per-call WARNING
    crit = [r for r in records if r.levelno >= logging.CRITICAL]
    assert crit, "an unsatisfiable feature contract must be CRITICAL, not a warning"
    assert any("no_such_feature_v9" in str(getattr(r, "context", "")) for r in crit)


def test_a_satisfiable_bundle_still_loads(detector):
    det, root, _src = detector
    bundle = _train_bundle("H1")
    bundle["feature_names"] = ["atr_pct_14", "adx_14", "volatility_20", "returns_1", "trend_20"]
    _install_bundle(root, "set-current", bundle)
    assert det.load_bundle() is True
    assert det._bundle_set_id == "set-current"
