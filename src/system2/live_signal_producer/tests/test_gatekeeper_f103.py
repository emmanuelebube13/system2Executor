"""F-103 — the gatekeeper must REFUSE, not score, garbage.

Two holes, both silent by nature because they return a plausible number instead of an
error:

  1. an **unknown strategy_id** goes through ``OneHotEncoder(handle_unknown='ignore')``
     and comes back with a *passing* score. Measured on the live champion,
     ``strategy_id="999"`` -> **0.4802**, above the deployed High-Vol threshold of 0.45:
     a strategy the model has never heard of would have been APPROVED;
  2. a **NaN** feature row is scored (measured 0.4391) because XGBoost consumes NaN
     natively as a missing-value branch.

The fixture here deliberately mirrors the LIVE champion's preprocessor layout —
``ColumnTransformer([('num', StandardScaler(), ...), ('cat',
OneHotEncoder(handle_unknown='ignore'), ['regime_causal','strategy_id',
'entry_signal_type'])])`` with ``strategy_id`` categories ``'1'..'10'`` — so these tests
exercise the same shape the defect was measured on, without needing the GCS artifacts.
"""

from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pytest

from system2.live_signal_producer.gatekeeper import (
    GatekeeperScorer,
    _known_categories,
    _nonfinite_fields,
)

# The live champion's feature list, verbatim from champion_manifest.json.
LIVE_FEATURES = [
    "atr_value", "adx_value",
    "prob_causal_trending_up", "prob_causal_trending_down",
    "prob_causal_ranging", "prob_causal_high_vol",
    "volatility_regime", "trending_strength", "adx_over_atr",
    "regime_causal", "strategy_id", "entry_signal_type",
]
NUMERIC = LIVE_FEATURES[:9]
CATEGORICAL = ["regime_causal", "strategy_id", "entry_signal_type"]
KNOWN_STRATEGIES = [str(i) for i in range(1, 11)]     # '1'..'10', as the live model
REGIMES = ["High-Vol", "Ranging", "Trending-Down", "Trending-Up"]

BASE_ROW = {
    "atr_value": 0.0012, "adx_value": 25.0,
    "prob_causal_trending_up": 0.25, "prob_causal_trending_down": 0.25,
    "prob_causal_ranging": 0.25, "prob_causal_high_vol": 0.25,
    "volatility_regime": 0.0, "trending_strength": 0.5,
    "adx_over_atr": 25.0 / 0.0012,
    "regime_causal": "Ranging", "strategy_id": "1", "entry_signal_type": "long",
}


@pytest.fixture
def live_shaped_champion(tmp_path: Path) -> Path:
    """A fitted triad with the live champion's exact preprocessor structure."""
    from sklearn.compose import ColumnTransformer
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    rng = np.random.default_rng(11)
    n = 400
    X = pd.DataFrame({c: rng.normal(0, 1, n) for c in NUMERIC})
    X["regime_causal"] = rng.choice(REGIMES, n)
    X["strategy_id"] = rng.choice(KNOWN_STRATEGIES, n)
    X["entry_signal_type"] = rng.choice(["long", "short"], n)
    y = (X["adx_value"] + rng.normal(0, 0.5, n) > 0).astype(int)

    prep = ColumnTransformer([
        ("num", StandardScaler(), NUMERIC),
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL),
    ], remainder="drop")
    model = LogisticRegression(max_iter=500).fit(prep.fit_transform(X[LIVE_FEATURES]), y)

    set_dir = tmp_path / "live-shaped"
    set_dir.mkdir()
    joblib.dump(model, set_dir / "champion_model.pkl")
    joblib.dump(prep, set_dir / "champion_preprocessor.pkl")
    (set_dir / "champion_manifest.json").write_text(json.dumps({
        "model_type": "sklearn-live-shaped",
        "features": LIVE_FEATURES,
        "dynamic_thresholds": {"High-Vol": 0.45, "Ranging": 0.6,
                               "Trending-Down": 0.6, "Trending-Up": 0.5,
                               "fallback": 0.6},
        "turnover_band": [0.05, 0.6],
    }), encoding="utf-8")
    return set_dir


@pytest.fixture
def scorer(live_shaped_champion: Path) -> GatekeeperScorer:
    s = GatekeeperScorer()
    assert s.load(live_shaped_champion) is True
    return s


# ----- the guard reads the model, not a hardcoded list ----------------------------
def test_known_categories_are_read_from_the_fitted_encoder(scorer: GatekeeperScorer):
    known = scorer.known_categories
    assert known["strategy_id"] == frozenset(KNOWN_STRATEGIES)
    assert known["regime_causal"] == frozenset(REGIMES)
    assert known["entry_signal_type"] == frozenset({"long", "short"})
    assert "atr_value" not in known          # numeric columns are not gated this way


def test_a_retrain_that_adds_a_strategy_is_accepted_without_a_code_change(
    tmp_path: Path, live_shaped_champion: Path,
):
    """The guard must track the shipped model, not second-guess System 1.

    Refusing strategy 11 forever would be the F-103 fix causing its own outage; the
    finding's own risk note says to gate on the model's declared categories.
    """
    from sklearn.compose import ColumnTransformer
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    rng = np.random.default_rng(12)
    n = 400
    strategies = KNOWN_STRATEGIES + ["11"]
    X = pd.DataFrame({c: rng.normal(0, 1, n) for c in NUMERIC})
    X["regime_causal"] = rng.choice(REGIMES, n)
    X["strategy_id"] = rng.choice(strategies, n)
    X["entry_signal_type"] = rng.choice(["long", "short"], n)
    prep = ColumnTransformer([
        ("num", StandardScaler(), NUMERIC),
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL),
    ], remainder="drop")
    model = LogisticRegression(max_iter=500).fit(
        prep.fit_transform(X[LIVE_FEATURES]), (X["adx_value"] > 0).astype(int))
    set_dir = tmp_path / "retrained"
    set_dir.mkdir()
    joblib.dump(model, set_dir / "champion_model.pkl")
    joblib.dump(prep, set_dir / "champion_preprocessor.pkl")
    (set_dir / "champion_manifest.json").write_text(json.dumps(
        {"features": LIVE_FEATURES, "approval_threshold": 0.5}), encoding="utf-8")

    s = GatekeeperScorer()
    s.load(live_shaped_champion)
    assert s.score({**BASE_ROW, "strategy_id": "11"}, "Ranging") is None   # not yet known
    assert s.load(set_dir) is True
    assert s.score({**BASE_ROW, "strategy_id": "11"}, "Ranging") is not None  # now known


# ----- hole 1: unknown strategy_id ------------------------------------------------
def test_unknown_strategy_id_is_refused_not_scored(scorer: GatekeeperScorer):
    """The measured defect: strategy_id='999' scored 0.4802 and cleared High-Vol 0.45."""
    assert scorer.score(dict(BASE_ROW), "High-Vol") is not None      # known id still works
    assert scorer.score({**BASE_ROW, "strategy_id": "999"}, "High-Vol") is None
    reason = scorer.refusal_reason({**BASE_ROW, "strategy_id": "999"})
    assert reason is not None and "strategy_id" in reason


@pytest.mark.parametrize("bad_id", ["999", "07", "", "10 ", "STRAT_10", None, "0"])
def test_id_typing_mismatches_refuse(scorer: GatekeeperScorer, bad_id):
    """`"07"` vs `"7"` is exactly the renumbering/typing hazard F-103 calls out."""
    assert scorer.score({**BASE_ROW, "strategy_id": bad_id}, "High-Vol") is None


def test_unknown_regime_and_direction_also_refuse(scorer: GatekeeperScorer):
    """Same silent all-zero path, same answer — refuse."""
    assert scorer.score({**BASE_ROW, "regime_causal": "Melt-Up"}, "Ranging") is None
    assert scorer.score({**BASE_ROW, "entry_signal_type": "flat"}, "Ranging") is None


def test_refusal_is_logged_so_it_is_not_silent(scorer: GatekeeperScorer):
    """F-103's Impact note: both holes were silent — no log, no distinct telemetry.

    ``system2``'s root logger sets ``propagate = False`` (common/logging.py:101), so
    ``caplog`` never sees these records — attach a handler, as test_safety_mode.py does.
    """
    import logging as _logging

    records: list[_logging.LogRecord] = []

    class _Capture(_logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = _logging.getLogger("system2.live_signal_producer.gatekeeper")
    handler = _Capture(level=_logging.WARNING)
    logger.addHandler(handler)
    try:
        assert scorer.score({**BASE_ROW, "strategy_id": "999"}, "High-Vol") is None
        assert scorer.score({**BASE_ROW, "atr_value": float("nan")}, "High-Vol") is None
    finally:
        logger.removeHandler(handler)

    messages = [r.getMessage() for r in records]
    assert any("outside the model's fitted categories" in m for m in messages)
    assert any("non-finite feature value" in m for m in messages)
    assert all("REFUSING to score" in m for m in messages)


# ----- hole 2: non-finite values --------------------------------------------------
@pytest.mark.parametrize("column", NUMERIC)
def test_nan_in_any_numeric_feature_is_refused(scorer: GatekeeperScorer, column):
    assert scorer.score({**BASE_ROW, column: float("nan")}, "Ranging") is None


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"),
                                   np.float64("nan"), np.float32("inf")])
def test_every_non_finite_flavour_is_refused(scorer: GatekeeperScorer, value):
    """inf matters as much as NaN: it survives `dropna` upstream and is what turns
    into NaN inside the model's own arithmetic."""
    assert scorer.score({**BASE_ROW, "trending_strength": value}, "Ranging") is None


def test_nan_propagation_through_derived_features_is_caught(scorer: GatekeeperScorer):
    """The exact F-103 row: NaN regime posteriors, with the derived columns showing
    how invisible it was — `trending_strength` inherits the NaN, while
    `volatility_regime` maps it to a confident 0.0 because `nan > 0.3` is False."""
    p_up = p_down = p_hv = float("nan")
    row = {
        **BASE_ROW,
        "prob_causal_trending_up": p_up,
        "prob_causal_trending_down": p_down,
        "prob_causal_high_vol": p_hv,
        "trending_strength": p_up + p_down,          # NaN, silently
        "volatility_regime": 1.0 if p_hv > 0.3 else 0.0,   # 0.0, confidently wrong
    }
    assert row["volatility_regime"] == 0.0           # the silent part, reproduced
    assert scorer.score(row, "Ranging") is None      # ...and now refused anyway


def test_nonfinite_fields_reports_every_offender():
    fields = _nonfinite_fields({
        "a": 1.0, "b": float("nan"), "c": float("inf"), "d": "text",
        "e": True, "f": -3, "g": None,
    })
    assert fields == ["b", "c"]        # strings/bools/None are not this guard's job


def test_booleans_are_not_mistaken_for_numbers():
    assert _nonfinite_fields({"flag": True, "other": False}) == []


# ----- degradation --------------------------------------------------------------
def test_unreadable_preprocessor_degrades_instead_of_refusing_everything():
    """No discoverable vocabulary => the category guard is off, not stuck closed.

    A guard that refuses everything when it cannot introspect the model would take the
    system down on any preprocessor shape we did not anticipate.
    """
    class Opaque:
        transformers_ = None

    assert _known_categories(Opaque()) == {}
    assert _known_categories(object()) == {}


def test_a_good_row_still_scores_and_still_respects_the_threshold(scorer: GatekeeperScorer):
    gate = scorer.score(dict(BASE_ROW), "High-Vol")
    assert gate is not None
    assert 0.0 <= gate.score <= 1.0
    assert gate.threshold == 0.45
    assert gate.approved is (gate.score >= 0.45)


def test_turnover_band_is_exposed_from_the_manifest(scorer: GatekeeperScorer):
    """FIX_PLAN 2.1(d) reads the band from the shipped manifest, not from S2."""
    assert scorer.turnover_band == [0.05, 0.6]


def test_turnover_band_is_none_when_the_manifest_omits_it(tmp_path: Path):
    from system2.live_signal_producer.tests.conftest import write_champion_set

    s = GatekeeperScorer()
    assert s.load(write_champion_set(tmp_path / "no-band")) is True
    assert s.turnover_band is None
