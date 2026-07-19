"""EXEC-011 — champion gatekeeper loading, feature assembly, and scoring.

Loads the Layer 3 champion triad (``champion_model.pkl``, ``champion_preprocessor.pkl``,
``champion_manifest.json``) from the ``active`` model set and scores one feature row per
(instrument, granularity, bar). Feature definitions follow ``docs/skills/layer3-contract.md``
exactly — the numeric columns come from ``compute_regime_features`` (causal, byte-identical
to training) plus ``price_position_20`` computed here from rolling 20-bar extremes; the
regime probability columns come from the live detector's semantically-ordered posteriors.

Fail-open posture: a reload failure keeps serving the last good gatekeeper (logged), and a
manifest missing its mandatory fields refuses to score (per layer3-contract §Mandatory Fields).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from system2.artifact_sync.regime_mapping import PROB_COLUMNS
from system2.common.logging import get_logger, log_event

log = get_logger("live_signal_producer.gatekeeper")

MODEL_FILE = "champion_model.pkl"
PREPROCESSOR_FILE = "champion_preprocessor.pkl"
MANIFEST_FILE = "champion_manifest.json"

PRICE_POSITION_WINDOW = 20
DEFAULT_APPROVAL_THRESHOLD = 0.20

# Manifest fields that must be present before we are allowed to score (layer3-contract).
_MANDATORY_MANIFEST_FIELDS = ("features",)


@dataclass(frozen=True)
class GateScore:
    """One gatekeeper evaluation: the model score against the regime's threshold."""

    score: float
    threshold: float
    approved: bool
    regime: str


def get_threshold(regime: str | None, manifest: dict[str, Any]) -> float:
    """Resolve the approval threshold for ``regime`` per layer3-contract.

    Precedence: ``dynamic_thresholds[regime]`` -> ``dynamic_thresholds["fallback"]``
    -> ``manifest["approval_threshold"]`` -> 0.20.
    """
    thresholds = manifest.get("dynamic_thresholds") or {}
    if regime is not None and regime in thresholds:
        return float(thresholds[regime])
    return float(
        thresholds.get("fallback", manifest.get("approval_threshold", DEFAULT_APPROVAL_THRESHOLD))
    )


def price_position_20(candles: pd.DataFrame, window: int = PRICE_POSITION_WINDOW) -> pd.Series:
    """Causal position of the close inside the rolling ``window``-bar range.

    ``(close - low_20) / (high_20 - low_20)`` clamped to [0, 1]; NaN (warm-up or a
    degenerate flat range) fills to the neutral 0.5, matching training.
    """
    high_w = candles["high"].rolling(window, min_periods=window).max()
    low_w = candles["low"].rolling(window, min_periods=window).min()
    span = high_w - low_w
    pos = (candles["close"] - low_w) / span.where(span != 0)
    return pos.clip(0.0, 1.0).fillna(0.5)


class GatekeeperScorer:
    """Loads champion artifacts from a model-set directory and scores feature rows."""

    def __init__(self) -> None:
        self._model: Any = None
        self._preprocessor: Any = None
        self._manifest: dict[str, Any] | None = None
        self._loaded_from: Path | None = None

    @property
    def loaded(self) -> bool:
        return self._model is not None and self._manifest is not None

    @property
    def manifest(self) -> dict[str, Any] | None:
        return self._manifest

    @property
    def features(self) -> list[str]:
        return list((self._manifest or {}).get("features", []))

    # ----- loading / hot-reload ---------------------------------------------------
    def load(self, set_dir: Path) -> bool:
        """(Re)load the champion triad from ``set_dir``.

        Returns True on success. On any failure the previously loaded gatekeeper (if
        any) keeps serving — never raises.
        """
        set_dir = Path(set_dir)
        try:
            manifest_path = set_dir / MANIFEST_FILE
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            missing = [f for f in _MANDATORY_MANIFEST_FIELDS if not manifest.get(f)]
            if not manifest.get("approval_threshold") and not manifest.get("dynamic_thresholds"):
                missing.append("approval_threshold|dynamic_thresholds")
            if missing:
                raise ValueError(f"manifest missing mandatory fields: {missing}")
            model = joblib.load(set_dir / MODEL_FILE)
            preprocessor = joblib.load(set_dir / PREPROCESSOR_FILE)
        except Exception as exc:
            log_event(log, logging.ERROR, "gatekeeper load failed; keeping last good",
                      path=str(set_dir), error=type(exc).__name__, detail=str(exc),
                      had_previous=self.loaded)
            return False
        self._model, self._preprocessor, self._manifest = model, preprocessor, manifest
        self._loaded_from = set_dir
        log_event(log, logging.INFO, "gatekeeper loaded",
                  path=str(set_dir), features=len(manifest["features"]),
                  model_type=manifest.get("model_type"))
        return True

    # ----- feature assembly ---------------------------------------------------------
    def build_feature_row(
        self,
        candle_features: pd.DataFrame,
        regime_probs: list[float],
        regime_smoothed: str,
        strategy_id: int | str | None = None,
        direction: str | None = None,
    ) -> dict[str, Any] | None:
        """Assemble the gatekeeper feature dict for the LAST bar of ``candle_features``.

        Speaks BOTH manifest dialects: the deployed champion (``system1/gatekeeper/
        train.py``: atr_value/adx_value, prob_causal_*, volatility_regime,
        trending_strength, adx_over_atr, regime_causal, strategy_id, entry_signal_type)
        and the layer3-contract spec names (returns_1, atr_14, price_position_20, ...).
        Derived formulas are copied verbatim from training:

            volatility_regime = (prob_causal_high_vol > 0.3) as 0/1
            trending_strength = prob_up + prob_down
            adx_over_atr      = adx / atr  (0 when atr <= 1e-8)

        ``candle_features`` is the output of ``compute_regime_features`` over raw candles;
        ``regime_probs`` is the detector's semantically-ordered posterior. Returns None
        while inside the indicator warm-up, or when a manifest feature needs a
        strategy/direction that wasn't supplied.
        """
        if not self.loaded:
            return None
        last = candle_features.iloc[-1]
        probs = [float(p) for p in regime_probs] + [0.0] * (4 - len(regime_probs))
        p_up, p_down, _p_rng, p_hv = probs[0], probs[1], probs[2], probs[3]
        pos = None  # computed lazily; only some manifests use it

        def numeric(col: str) -> float | None:
            if col not in candle_features.columns or pd.isna(last[col]):
                return None
            return float(last[col])

        row: dict[str, Any] = {}
        for feature in self.features:
            value: Any = None
            if feature in ("regime_smoothed", "regime_causal"):
                value = regime_smoothed
            elif feature == "strategy_id":
                value = str(strategy_id) if strategy_id is not None else None
            elif feature == "entry_signal_type":
                value = direction
            elif feature in PROB_COLUMNS:
                value = probs[PROB_COLUMNS.index(feature)]
            elif feature.startswith("prob_causal_"):
                short = "prob_" + feature[len("prob_causal_"):]
                value = probs[PROB_COLUMNS.index(short)] if short in PROB_COLUMNS else None
            elif feature == "atr_value":
                value = numeric("atr_14")
            elif feature == "adx_value":
                value = numeric("adx_14")
            elif feature == "volatility_regime":
                value = 1.0 if p_hv > 0.3 else 0.0
            elif feature == "trending_strength":
                value = p_up + p_down
            elif feature == "adx_over_atr":
                atr_v, adx_v = numeric("atr_14"), numeric("adx_14")
                if atr_v is not None and adx_v is not None:
                    value = adx_v / atr_v if atr_v > 1e-8 else 0.0
            elif feature == "price_position_20":
                if pos is None:
                    pos = price_position_20(candle_features)
                value = float(pos.iloc[-1])
            elif feature in candle_features.columns:
                value = numeric(feature)
            else:
                log_event(log, logging.WARNING, "unknown gatekeeper feature; refusing to score",
                          feature=feature)
                return None
            if value is None:
                return None  # warm-up NaN or missing strategy/direction context
            row[feature] = value
        return row

    # ----- scoring -----------------------------------------------------------------
    def score(self, feature_row: dict[str, Any], regime: str) -> GateScore | None:
        """Score one feature row; None when no gatekeeper is loaded or inference fails."""
        if not self.loaded:
            return None
        try:
            X_df = pd.DataFrame([feature_row], columns=self.features)
            X = self._preprocessor.transform(X_df)
            score = float(np.asarray(self._model.predict_proba(X))[0, 1])
        except Exception as exc:
            log_event(log, logging.ERROR, "gatekeeper inference failed",
                      error=type(exc).__name__, detail=str(exc))
            return None
        threshold = get_threshold(regime, self._manifest or {})
        return GateScore(score=score, threshold=threshold,
                         approved=score >= threshold, regime=regime)
