"""Shared EXEC-011 test fixtures: a tiny real champion triad + strategy files on disk.

The gatekeeper is a genuinely fitted sklearn pipeline (StandardScaler+OneHotEncoder ->
LogisticRegression) so ``preprocessor.transform`` + ``predict_proba`` exercise the same
code paths as the production XGBoost/LightGBM artifacts, without heavyweight deps.
"""

from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pytest

GATEKEEPER_FEATURES = [
    "returns_1", "atr_14", "price_position_20", "volatility_20",
    "prob_trending_up", "prob_trending_down", "prob_ranging", "prob_high_vol",
    "regime_smoothed",
]
NUMERIC_FEATURES = GATEKEEPER_FEATURES[:-1]
REGIMES = ["Trending-Up", "Trending-Down", "Ranging", "High-Vol"]


def make_candles(n: int = 120, seed: int = 7) -> pd.DataFrame:
    """Deterministic random-walk OHLCV candles (ascending, complete bars)."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2026-07-01", periods=n, freq="1h", tz="UTC")
    close = 1.10 + np.cumsum(rng.normal(0, 0.0008, n))
    high = close + rng.uniform(0.0002, 0.0012, n)
    low = close - rng.uniform(0.0002, 0.0012, n)
    return pd.DataFrame({
        "bar_time_utc": idx, "open": close, "high": high, "low": low,
        "close": close, "volume": rng.uniform(100, 1000, n),
    })


def train_champion() -> tuple:
    """Fit a small (preprocessor, model) pair on synthetic gatekeeper rows."""
    from sklearn.compose import ColumnTransformer
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    rng = np.random.default_rng(42)
    n = 200
    X = pd.DataFrame({f: rng.normal(0, 1, n) for f in NUMERIC_FEATURES})
    X["regime_smoothed"] = rng.choice(REGIMES, n)
    # A learnable rule so scores are non-degenerate.
    y = (X["returns_1"] + X["prob_trending_up"] > 0).astype(int)

    preprocessor = ColumnTransformer([
        ("num", StandardScaler(), NUMERIC_FEATURES),
        ("cat", OneHotEncoder(handle_unknown="ignore"), ["regime_smoothed"]),
    ], remainder="drop")
    Xt = preprocessor.fit_transform(X)
    model = LogisticRegression(max_iter=200).fit(Xt, y)
    return preprocessor, model


def make_manifest(**overrides) -> dict:
    manifest = {
        "manifest_version": "1.0.0",
        "model_type": "sklearn-test",
        "features": list(GATEKEEPER_FEATURES),
        "categorical_features": ["regime_smoothed"],
        "approval_threshold": 0.20,
        "dynamic_thresholds": {
            "Trending-Up": 0.30, "Trending-Down": 0.30,
            "Ranging": 0.40, "High-Vol": 0.55, "fallback": 0.35,
        },
        "feature_set_version": "1.0.0",
        "regime_model_version": "hmm_test",
        "training_run_id": "test-run",
    }
    manifest.update(overrides)
    return manifest


STRATEGY_MAP = {
    "EUR_USD": {
        "Trending-Up": [10, 12], "Trending-Down": [11],
        "Ranging": [14], "High-Vol": [],
    },
    "GBP_USD": {"Trending-Up": [10], "Trending-Down": [], "Ranging": [], "High-Vol": []},
}

STRATEGY_WEIGHTS = {
    "10": {"name": "trend_follow_long", "direction": "long",
           "sl_atr_mult": 1.0, "tp_atr_mult": 3.0, "entry_offset_atr": 0.0},
    "11": {"name": "trend_follow_short", "direction": "short",
           "sl_atr_mult": 1.5, "tp_atr_mult": 2.5, "entry_offset_atr": 0.1},
    "12": {"name": "breakout_long", "direction": "long",
           "sl_atr_mult": 2.0, "tp_atr_mult": 4.0},
    "14": {"name": "range_fade_short", "direction": "short",
           "sl_atr_mult": 1.0, "tp_atr_mult": 1.5},
}


def write_champion_set(set_dir: Path, manifest: dict | None = None) -> Path:
    """Write the full artifact set (triad + strategy files) into ``set_dir``."""
    set_dir.mkdir(parents=True, exist_ok=True)
    preprocessor, model = train_champion()
    joblib.dump(model, set_dir / "champion_model.pkl")
    joblib.dump(preprocessor, set_dir / "champion_preprocessor.pkl")
    (set_dir / "champion_manifest.json").write_text(
        json.dumps(manifest or make_manifest()), encoding="utf-8")
    (set_dir / "regime_strategy_map.json").write_text(json.dumps(STRATEGY_MAP), encoding="utf-8")
    (set_dir / "strategy_weights.json").write_text(json.dumps(STRATEGY_WEIGHTS), encoding="utf-8")
    return set_dir


@pytest.fixture
def champion_dir(tmp_path: Path) -> Path:
    return write_champion_set(tmp_path / "set-A")
