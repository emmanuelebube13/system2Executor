"""EXEC-002 — Live Regime Detector (Artifact-Sync).

Runs the HMM from the EXEC-001 ``active`` model set on live OANDA candles to publish the
current regime label + per-state probabilities, with **causal persistence smoothing**
(no single-candle flips). Falls back to ``last_good`` if the active HMM fails to load.
Read-only / contextual: it must NEVER alter the risk or size of an AMS-approved order
(that authority is System 3's — see docs/SYSTEM_BOUNDARY.md).

Inference parity with MODEL-003 training: features from features.compute_regime_features,
then ``scaler.transform(X) * weights`` -> ``model.predict_proba`` -> deterministic
state->label mapping (regime_mapping). The scaler/weights/mapping all come from the
serialized bundle, never re-derived here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import joblib
import numpy as np
import pandas as pd

from system2.artifact_sync import regime_mapping as M
from system2.artifact_sync.features import compute_regime_features
from system2.common.logging import get_logger, log_event, set_correlation_id
from system2.common.secrets import Secrets, get_secrets

log = get_logger("artifact_sync.live_regime")

# Bundle filename candidates inside a model set (first that exists wins).
_ARTIFACT_CANDIDATES = ("regime_hmm.pkl", "hmm_model.joblib")


@runtime_checkable
class CandleSource(Protocol):
    """Supplies recent OHLCV candles for an instrument/granularity (ascending by time)."""

    def fetch_candles(self, instrument: str, granularity: str, count: int) -> pd.DataFrame:
        """Return a DataFrame with columns: high, low, close, bar_time_utc (>=count rows)."""
        ...


@dataclass
class RegimeObservation:
    granularity: str
    as_of: str | None
    raw_state: int | None
    raw_probs: list[float]
    smoothed_label: str | None
    smoothed_confidence: float
    persistence_window: int
    model_set_id: str | None
    source: str = "hmm-live"
    stale: bool = False
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "granularity": self.granularity,
            "as_of": self.as_of,
            "raw_state": self.raw_state,
            "raw_probs": self.raw_probs,
            "smoothed_label": self.smoothed_label,
            "smoothed_confidence": self.smoothed_confidence,
            "persistence_window": self.persistence_window,
            "model_set_id": self.model_set_id,
            "source": self.source,
            "stale": self.stale,
            "note": self.note,
        }


@dataclass
class OnlineDebouncer:
    """Causal, stateful persistence smoothing (one per instrument+granularity).

    Switches the published label only when a new label has held for ``window``
    consecutive predictions OR its probability >= ``high_conf``; otherwise holds.
    """

    window: int
    high_conf: float
    published: str | None = None
    _candidate: str | None = field(default=None, repr=False)
    _count: int = field(default=0, repr=False)

    def update(self, raw_label: str, raw_prob: float) -> str:
        if self.published is None:  # first observation adopts immediately
            self.published = raw_label
            self._candidate, self._count = None, 0
            return self.published
        if raw_label == self.published:
            self._candidate, self._count = None, 0
            return self.published
        # A label different from the published one.
        if raw_label == self._candidate:
            self._count += 1
        else:
            self._candidate, self._count = raw_label, 1
        if self._count >= self.window or raw_prob >= self.high_conf:
            self.published = raw_label
            self._candidate, self._count = None, 0
        return self.published


class LiveRegimeDetector:
    """Loads the regime bundle from the active set and predicts the live regime."""

    def __init__(
        self,
        artifact_root: Path,
        candle_source: CandleSource,
        secrets: Secrets | None = None,
    ) -> None:
        self.secrets = secrets or get_secrets()
        self.root = Path(artifact_root)
        self.candle_source = candle_source
        self.lookback = self.secrets.get_int("REGIME_CANDLE_LOOKBACK", 120)
        self.window = self.secrets.get_int("PERSISTENCE_WINDOW", 3)
        self.high_conf = float(self.secrets.get("REGIME_HIGH_CONF", "0.90") or 0.90)
        self._bundle: dict[str, Any] | None = None
        self._bundle_source: str | None = None  # "active" | "last_good"
        self._debouncers: dict[tuple[str, str], OnlineDebouncer] = {}
        self._last_obs: dict[tuple[str, str], RegimeObservation] = {}

    # ----- bundle loading --------------------------------------------------------
    def _find_artifact(self, set_link: Path) -> Path | None:
        if not set_link.exists():
            return None
        for name in _ARTIFACT_CANDIDATES:
            cand = set_link / name
            if cand.exists():
                return cand
        return None

    def _model_set_id(self, link: Path) -> str | None:
        try:
            return link.resolve().name
        except OSError:
            return None

    def load_bundle(self, force: bool = False) -> bool:
        """Load the regime bundle from ``active``, falling back to ``last_good``.

        Returns True if a bundle is loaded. Raises nothing — failure is logged + alerted.
        """
        if self._bundle is not None and not force:
            return True
        for source, link in (("active", self.root / "active"), ("last_good", self.root / "last_good")):
            path = self._find_artifact(link)
            if path is None:
                continue
            try:
                bundle = joblib.load(path)
                # minimal contract check
                if "models" not in bundle or "feature_names" not in bundle:
                    raise ValueError("bundle missing 'models'/'feature_names'")
                self._bundle = bundle
                self._bundle_source = source
                self._bundle_set_id = self._model_set_id(link)
                if source == "last_good":
                    log_event(log, logging.WARNING,
                              "active regime bundle unusable; using last_good",
                              model_set_id=self._bundle_set_id)
                else:
                    log_event(log, logging.INFO, "loaded regime bundle",
                              model_set_id=self._bundle_set_id)
                return True
            except Exception as exc:  # corrupt/unloadable
                log_event(log, logging.CRITICAL, "regime bundle failed to load",
                          source=source, path=str(path), error=type(exc).__name__, detail=str(exc))
        log_event(log, logging.CRITICAL, "no usable regime bundle (active or last_good)")
        return False

    # ----- prediction ------------------------------------------------------------
    def _feature_vector(self, candles: pd.DataFrame) -> tuple[np.ndarray, str] | None:
        """Build the last-bar feature vector per the bundle's feature contract."""
        b = self._bundle
        feature_names: list[str] = b["feature_names"]
        direction = b.get("direction_feature", "trend_20")
        trend_window = int(b.get("trend_window", 20))
        feats = compute_regime_features(candles, direction_feature=direction, trend_window=trend_window)
        feats = feats.dropna(subset=feature_names)
        if feats.empty:
            return None
        last = feats.iloc[-1]
        as_of = str(pd.to_datetime(last["bar_time_utc"], utc=True)) if "bar_time_utc" in feats else None
        vec = last[feature_names].to_numpy(dtype="float64").reshape(1, -1)
        return vec, (as_of or "")

    def _predict_probs(self, granularity: str, vec: np.ndarray) -> np.ndarray:
        """Apply scaler+weights and return raw-state posteriors for one sample."""
        model_obj = self._bundle["models"][granularity]
        scaler = model_obj["scaler"]
        weights = np.asarray(model_obj["weights"], dtype="float64")
        x = scaler.transform(vec) * weights
        model = model_obj["model"]
        if hasattr(model, "predict_proba"):
            return np.asarray(model.predict_proba(x))[0]
        # K-Means fallback model: one-hot the assigned cluster.
        state = int(model.predict(x)[0])
        n = int(getattr(model, "n_clusters", len(model_obj["mapping"])))
        onehot = np.zeros(n)
        onehot[state] = 1.0
        return onehot

    def detect(self, instrument: str, granularity: str) -> RegimeObservation:
        """Compute and return the smoothed live regime for one instrument+granularity.

        Never raises: on any failure it returns the last good observation marked stale,
        or an empty stale observation, and logs/alerts.
        """
        key = (instrument, granularity)
        set_correlation_id(f"regime-{instrument}-{granularity}")
        if not self.load_bundle():
            return self._stale(key, granularity, "no model bundle")
        if granularity not in self._bundle["models"]:
            return self._stale(key, granularity, f"no model for granularity {granularity}")

        try:
            candles = self.candle_source.fetch_candles(instrument, granularity, self.lookback)
        except Exception as exc:
            log_event(log, logging.WARNING, "candle fetch failed; serving last good regime",
                      instrument=instrument, granularity=granularity,
                      error=type(exc).__name__, detail=str(exc))
            return self._stale(key, granularity, f"candle fetch error: {type(exc).__name__}")

        fv = self._feature_vector(candles)
        if fv is None:
            log_event(log, logging.WARNING, "insufficient candle history; holding last regime",
                      instrument=instrument, granularity=granularity, rows=len(candles))
            return self._stale(key, granularity, "insufficient history")

        vec, as_of = fv
        probs_state = self._predict_probs(granularity, vec)
        mapping = {int(k): v for k, v in self._bundle["models"][granularity]["mapping"].items()}
        raw_state = int(np.argmax(probs_state))
        raw_label = mapping[raw_state]
        raw_prob = float(probs_state[raw_state])
        ordered = M.order_probabilities(probs_state.reshape(1, -1), mapping)[0].tolist()

        deb = self._debouncers.setdefault(key, OnlineDebouncer(self.window, self.high_conf))
        smoothed = deb.update(raw_label, raw_prob)
        smoothed_conf = float(ordered[M.SEMANTIC_ORDER.index(smoothed)]) if smoothed in M.SEMANTIC_ORDER else raw_prob

        obs = RegimeObservation(
            granularity=granularity,
            as_of=as_of or None,
            raw_state=raw_state,
            raw_probs=[float(p) for p in ordered],
            smoothed_label=smoothed,
            smoothed_confidence=round(smoothed_conf, 6),
            persistence_window=self.window,
            model_set_id=getattr(self, "_bundle_set_id", None),
            stale=False,
        )
        self._last_obs[key] = obs
        log_event(log, logging.INFO, "regime", instrument=instrument, granularity=granularity,
                  raw=raw_label, smoothed=smoothed, conf=obs.smoothed_confidence,
                  model_set_id=obs.model_set_id)
        return obs

    def snapshot_grid(self) -> list[dict[str, Any]]:
        """Read-only view of the current regime for every observed (instrument, granularity).

        Pure accessor over ``_last_obs`` — never triggers inference, never raises. Used by the
        telemetry surface so the dashboard can render the pair x timeframe regime matrix.
        """
        out: list[dict[str, Any]] = []
        for (instrument, granularity), obs in self._last_obs.items():
            row = obs.to_dict()
            row["instrument"] = instrument
            out.append(row)
        out.sort(key=lambda r: (r["instrument"], r["granularity"]))
        return out

    def _stale(self, key: tuple[str, str], granularity: str, note: str) -> RegimeObservation:
        prev = self._last_obs.get(key)
        if prev is not None:
            return RegimeObservation(
                granularity=prev.granularity, as_of=prev.as_of, raw_state=prev.raw_state,
                raw_probs=prev.raw_probs, smoothed_label=prev.smoothed_label,
                smoothed_confidence=prev.smoothed_confidence, persistence_window=prev.persistence_window,
                model_set_id=prev.model_set_id, stale=True, note=note,
            )
        return RegimeObservation(
            granularity=granularity, as_of=None, raw_state=None, raw_probs=[],
            smoothed_label=None, smoothed_confidence=0.0, persistence_window=self.window,
            model_set_id=getattr(self, "_bundle_set_id", None), stale=True, note=note,
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
