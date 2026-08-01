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
import math
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


def _known_categories(preprocessor: Any) -> dict[str, frozenset[str]]:
    """Read the categorical vocabulary the model was actually fitted on (F-103).

    The champion preprocessor is a ``ColumnTransformer`` whose ``cat`` step is a
    ``OneHotEncoder(handle_unknown='ignore')`` over ``regime_causal / strategy_id /
    entry_signal_type``. ``handle_unknown='ignore'`` is the whole defect: a value the
    model has never seen silently encodes as an all-zero block and still gets a number
    back. Measured on the live champion, ``strategy_id="999"`` scores **0.4802**, which
    clears the deployed High-Vol threshold of 0.45 — an unknown strategy is APPROVED.

    So we ask the fitted encoder what it actually knows and refuse anything else.
    Derived from ``categories_`` rather than a hardcoded list precisely so a retrain
    that legitimately adds strategy 11 starts accepting strategy 11 the moment its
    artifacts land — the guard tracks the shipped model, it does not second-guess it.

    Returns ``{}`` when no such vocabulary is discoverable, which degrades to the old
    (unguarded) behaviour rather than refusing everything.
    """
    known: dict[str, frozenset[str]] = {}
    try:
        transformers = getattr(preprocessor, "transformers_", None) or []
        for _name, trans, cols in transformers:
            cats = getattr(trans, "categories_", None)
            if cats is None or isinstance(cols, str):
                continue
            for col, values in zip(list(cols), list(cats)):
                known[str(col)] = frozenset(str(v) for v in list(values))
    except Exception as exc:  # never let introspection break loading
        log_event(log, logging.WARNING, "could not read model categories; "
                  "unknown-category refusal is DISABLED for this model set",
                  error=type(exc).__name__, detail=str(exc))
        return {}
    return known


def _nonfinite_fields(row: dict[str, Any]) -> list[str]:
    """Names of numeric fields in ``row`` that are NaN or +/-inf (F-103).

    ``build_feature_row`` only ever tested ``value is None``, and ``float('nan') is not
    None`` — so a row carrying NaN regime posteriors was scored (measured 0.4391)
    because XGBoost consumes NaN natively as a "missing" branch direction. That is a
    model answering a question it was never asked. NaN also propagates through the
    derived columns invisibly: ``trending_strength = p_up + p_down`` inherits it, and
    ``volatility_regime = 1.0 if p_hv > 0.3 else 0.0`` maps it to a confident 0.0
    because ``nan > 0.3`` is False.

    Note this is NOT covered by the message-contract validators (System 3's validator
    and System 2's ``validate_envelope``): those police the ScoredSignal on the wire,
    which is produced *downstream* of this scoring call. The model's inputs come from
    the live regime detector and OANDA candles and cross no envelope at all.
    """
    bad = []
    for key, value in row.items():
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float, np.floating, np.integer)):
            if not math.isfinite(float(value)):
                bad.append(key)
    return sorted(bad)


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
        self._known_categories: dict[str, frozenset[str]] = {}

    @property
    def loaded(self) -> bool:
        return self._model is not None and self._manifest is not None

    @property
    def manifest(self) -> dict[str, Any] | None:
        return self._manifest

    @property
    def features(self) -> list[str]:
        return list((self._manifest or {}).get("features", []))

    @property
    def known_categories(self) -> dict[str, frozenset[str]]:
        """The categorical vocabulary the loaded model was fitted on (F-103)."""
        return dict(self._known_categories)

    @property
    def turnover_band(self) -> list[float] | None:
        """The ``[min, max]`` approval-rate band System 1 trained under, if declared.

        The live champion manifest ships ``"turnover_band": [0.05, 0.6]`` next to
        ``"oos_approval_rate": 0.3379``. It was enforced at training time and never at
        runtime (F-602 / FIX_PLAN 2.1(d)); exposing it here lets the runtime monitor
        judge the live rate against the model's OWN declared band instead of a second,
        divergent number kept in System 2.
        """
        band = (self._manifest or {}).get("turnover_band")
        if isinstance(band, (list, tuple)) and len(band) >= 2:
            try:
                return [float(band[0]), float(band[1])]
            except (TypeError, ValueError):
                return None
        return None

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
        self._known_categories = _known_categories(preprocessor)
        log_event(log, logging.INFO, "gatekeeper loaded",
                  path=str(set_dir), features=len(manifest["features"]),
                  model_type=manifest.get("model_type"),
                  guarded_categories=sorted(self._known_categories),
                  turnover_band=self.turnover_band)
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
        if self.refusal_reason(row) is not None:
            return None  # logged by refusal_reason; caller treats None as "do not score"
        return row

    # ----- input validation (F-103) -------------------------------------------------
    def refusal_reason(self, feature_row: dict[str, Any]) -> str | None:
        """Why this row must NOT be scored, or None if it is safe to score.

        The default-safe contract for this system is "missing / stale / error => reject".
        Two boundary cases used to violate it *at the model interface* (F-103), and both
        are silent by nature — they produce a plausible number rather than an error:

          1. a value outside the model's fitted categorical vocabulary (unknown
             ``strategy_id`` etc.), which ``handle_unknown='ignore'`` turns into an
             all-zero block and a generic ~0.48 score that clears the 0.45 High-Vol
             threshold;
          2. a non-finite numeric (NaN/inf), which XGBoost happily routes down its
             missing-value branch.

        A model asked to score something it has never seen must say "I cannot", not
        return a number that happens to clear a threshold. Both now refuse, loudly.
        """
        if not isinstance(feature_row, dict):
            return "feature row is not a mapping"
        bad_numeric = _nonfinite_fields(feature_row)
        if bad_numeric:
            log_event(log, logging.WARNING,
                      "non-finite feature value; REFUSING to score (F-103)",
                      fields=bad_numeric,
                      values={k: repr(feature_row[k]) for k in bad_numeric})
            return f"non-finite feature(s): {bad_numeric}"
        for column, allowed in self._known_categories.items():
            if column not in feature_row:
                continue
            value = feature_row[column]
            if value is None or str(value) not in allowed:
                log_event(log, logging.WARNING,
                          "value outside the model's fitted categories; "
                          "REFUSING to score (F-103)",
                          column=column, value=repr(value),
                          known=sorted(allowed)[:24])
                return f"unknown category for {column}: {value!r}"
        return None

    # ----- scoring -----------------------------------------------------------------
    def score(self, feature_row: dict[str, Any], regime: str) -> GateScore | None:
        """Score one feature row; None when no gatekeeper is loaded, the row is refused
        (F-103: unknown category or non-finite value), or inference fails.

        The check is repeated here rather than trusted from ``build_feature_row``
        because ``score`` is a public entry point: this is the last line before the
        model, so it is where the refusal has to be unconditional.
        """
        if not self.loaded:
            return None
        if self.refusal_reason(feature_row) is not None:
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
