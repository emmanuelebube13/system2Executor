"""Regime feature computation — self-contained copy of System 1's causal feature defs.

Copied (and trimmed to the regime path) from the monolith
``src/system1/features/definitions.py`` + the ATR/ADX in ``src/layer0/indicators.py``,
so System 2's live inference computes **byte-identical** features to MODEL-003 training
(avoids train/serve skew). Every feature at bar t depends only on bars <= t (no leakage).

Do NOT "improve" these formulae independently — they are a contract with the trained model.
"""

from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

ATR_PERIOD = 14
ADX_PERIOD = 14
VOLATILITY_WINDOW = 20

# The ordered vector MODEL-003 (HMM / K-Means) consumes (matches REGIME_FEATURE_COLUMNS).
REGIME_FEATURE_COLUMNS: List[str] = ["atr_14", "adx_14", "volatility_20", "returns_1"]


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Average True Range via causal EWM of true range."""
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return true_range.ewm(span=period, adjust=False).mean()


def adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Average Directional Index (0-100), causal."""
    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm < 0] = 0
    plus_dm[plus_dm <= minus_dm] = 0
    minus_dm[minus_dm <= plus_dm] = 0

    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr_val = true_range.ewm(span=period, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(span=period, adjust=False).mean() / atr_val
    minus_di = 100 * minus_dm.ewm(span=period, adjust=False).mean() / atr_val
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    return dx.ewm(span=period, adjust=False).mean()


def compute_regime_features(
    df: pd.DataFrame, direction_feature: str = "trend_20", trend_window: int = 20
) -> pd.DataFrame:
    """Compute the regime feature columns + the derived persistent-trend feature.

    ``df`` must be sorted ascending by time and contain ``high, low, close`` (``open``,
    ``volume`` optional). Warm-up rows are NaN exactly as in training so the caller can
    drop them. Adds: returns_1, atr_14, adx_14, volatility_20, and ``direction_feature``.
    """
    out = df.copy()
    close = out["close"].astype("float64")
    high = out["high"].astype("float64")
    low = out["low"].astype("float64")

    out["returns_1"] = np.log(close / close.shift(1))

    a = atr(high, low, close, ATR_PERIOD).astype("float64")
    a.iloc[: ATR_PERIOD - 1] = np.nan
    out["atr_14"] = a.to_numpy()

    dx = adx(high, low, close, ADX_PERIOD).astype("float64")
    dx.iloc[: 2 * ADX_PERIOD - 1] = np.nan
    out["adx_14"] = dx.to_numpy()

    out["volatility_20"] = out["returns_1"].rolling(
        VOLATILITY_WINDOW, min_periods=VOLATILITY_WINDOW
    ).std()

    # Derived point-in-time persistent trend (trailing mean of 1-bar log returns).
    out[direction_feature] = out["returns_1"].rolling(
        trend_window, min_periods=trend_window
    ).mean()

    return out
