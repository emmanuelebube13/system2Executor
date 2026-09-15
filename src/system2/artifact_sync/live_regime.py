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
from system2.artifact_sync.features import compute_regime_features, produced_feature_names
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
        self._bundle_set_id: str | None = None   # the set the in-memory bundle came from
        self._seen_set_id: str | None = None     # the set last observed on disk (P2)
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

    def _on_disk_set_id(self) -> str | None:
        """The model set ``active`` points at right now, or None if there is no active link.

        Read on every load so a swap is noticed. Falls back to ``last_good`` only when
        ``active`` is absent, mirroring the source walk below.
        """
        for link in (self.root / "active", self.root / "last_good"):
            if link.exists():
                return self._model_set_id(link)
        return None

    def load_bundle(self, force: bool = False) -> bool:
        """Load the regime bundle from ``active``, falling back to ``last_good``.

        Returns True if a bundle is loaded. Raises nothing — failure is logged + alerted.

        P2: the cache is keyed on the model set id, not merely on "a bundle is loaded".
        It used to return early whenever ``self._bundle`` was set, and no production caller
        ever passed ``force=True`` — so the downloader's atomic swap replaced the files on
        disk and the long-lived detector kept inferring from the set it loaded at startup,
        for as long as the process ran. That is how live regime labels came to be stamped
        with a model set older than the active one, with nothing logged: the swap worked,
        and the only consumer never looked again.

        It also makes P4 meaningful rather than merely passing. A reference-vector replay
        run after a sync would exercise the freshly downloaded bundle while production kept
        serving the stale in-memory one, so the gate would prove a property of a bundle that
        was not the one inferring. Reloading on swap is what ties the replay to what runs.
        """
        on_disk = self._on_disk_set_id()
        if self._bundle is not None and not force:
            # Compare against the id last *observed on disk*, not the id actually loaded.
            # They differ when `active` is unloadable and we fell back to `last_good`: then
            # the loaded id is last_good's while disk reads active's, and comparing the two
            # would re-attempt the broken load on every detect() call. A new swap changes
            # the observed id and does trigger the reload.
            if on_disk is None or on_disk == self._seen_set_id:
                return True
            log_event(log, logging.INFO, "model set changed on disk; reloading regime bundle",
                      loaded=self._bundle_set_id, on_disk=on_disk)
        self._seen_set_id = on_disk
        for source, link in (("active", self.root / "active"), ("last_good", self.root / "last_good")):
            path = self._find_artifact(link)
            if path is None:
                continue
            try:
                bundle = joblib.load(path)
                # minimal contract check
                if "models" not in bundle or "feature_names" not in bundle:
                    raise ValueError("bundle missing 'models'/'feature_names'")
                # Feature-contract check, at LOAD time. The bundle is self-describing, so
                # ask it what it wants and refuse if we cannot produce it. Without this the
                # mismatch surfaces as a KeyError once per inference call, logged WARNING
                # and swallowed: on 2026-08-23 that meant 100% of regime detections failing
                # while the service reported itself healthy. A contract we cannot satisfy is
                # a reason to refuse the bundle, not to keep asking.
                wanted = list(bundle["feature_names"])
                producible = produced_feature_names(bundle.get("direction_feature", "trend_20"))
                missing = [f for f in wanted if f not in producible]
                if missing:
                    raise ValueError(
                        f"bundle wants features this build cannot compute: {missing}; "
                        f"wanted={wanted} producible={sorted(producible)} "
                        f"feature_set_version={bundle.get('feature_set_version')!r}"
                    )
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
    def _feature_matrix(self, candles: pd.DataFrame) -> tuple[np.ndarray, str] | None:
        """Build the feature matrix (oldest->newest) per the bundle's feature contract.

        The whole warm-up-trimmed window is returned, not just the last bar: the HMM's
        posterior for the current bar is only meaningful when the forward pass has the
        preceding sequence to run over (see :meth:`_predict_probs`).
        """
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
        mat = feats[feature_names].to_numpy(dtype="float64")
        return mat, (as_of or "")

    def _weights_for(self, model_obj: dict[str, Any]) -> np.ndarray:
        """Post-standardisation weights, taken from the bundle's NAMED contract.

        The bundle carries the same weights twice: top-level ``feature_weights`` keyed by
        feature name, and per-granularity ``weights`` as a bare positional list. They agree
        today. Two copies that agree until they don't is precisely the shape of the
        ``atr_pct_14`` incident, so prefer the named one -- it cannot silently transpose if
        ``feature_names`` is reordered -- and refuse when the two disagree rather than
        picking one and hoping.
        """
        b = self._bundle
        names: list[str] = list(b["feature_names"])
        positional = model_obj.get("weights")
        named = b.get("feature_weights")

        if isinstance(named, dict):
            missing = [n for n in names if n not in named]
            if missing:
                raise ValueError(f"feature_weights has no entry for {missing}")
            w = np.asarray([float(named[n]) for n in names], dtype="float64")
            if positional is not None:
                pos = np.asarray(positional, dtype="float64")
                if pos.shape != w.shape or not np.allclose(pos, w, rtol=0, atol=0):
                    raise ValueError(
                        "bundle weights disagree: feature_weights (by name) gives "
                        f"{w.tolist()} for {names}, positional 'weights' gives {pos.tolist()}"
                    )
            return w

        if positional is None:
            raise ValueError("bundle has neither 'feature_weights' nor per-model 'weights'")
        pos = np.asarray(positional, dtype="float64")
        if pos.shape[0] != len(names):
            raise ValueError(
                f"positional weights length {pos.shape[0]} != {len(names)} feature_names"
            )
        return pos

    def _predict_probs(self, granularity: str, mat: np.ndarray) -> np.ndarray:
        """Raw-state posteriors for the LAST bar, conditioned on the whole window.

        hmmlearn scores a *sequence*. Passing a single bar collapses the posterior to
        ``normalize(startprob_ * emission)``: the transition matrix is never consulted
        and a degenerate trained ``startprob_`` becomes a permanent prior — any state
        with ``startprob_ == 0`` can then never be emitted, at any time, for any input.
        Feeding the window lets the forward recursion do its job; ``startprob_`` only
        anchors the oldest bar, whose influence decays.

        Still causal: we take the final row, whose backward term is 1, so it depends
        on bars <= t only — no lookahead leaks into a live trading decision.
        """
        model_obj = self._bundle["models"][granularity]
        scaler = model_obj["scaler"]
        weights = self._weights_for(model_obj)
        x = scaler.transform(mat) * weights
        model = model_obj["model"]
        if hasattr(model, "predict_proba"):
            return np.asarray(model.predict_proba(x))[-1]
        # K-Means fallback model: no temporal structure, so classify the last bar alone.
        state = int(model.predict(x[-1:])[0])
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

        fv = self._feature_matrix(candles)
        if fv is None:
            log_event(log, logging.WARNING, "insufficient candle history; holding last regime",
                      instrument=instrument, granularity=granularity, rows=len(candles))
            return self._stale(key, granularity, "insufficient history")

        mat, as_of = fv
        probs_state = self._predict_probs(granularity, mat)
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
