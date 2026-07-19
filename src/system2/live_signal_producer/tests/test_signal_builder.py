"""EXEC-011 tests — strategy map parsing and ScoredSignal construction."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from system2.live_signal_producer.signal_builder import (
    StrategyBook,
    StrategyConfig,
    build_signal,
)

# S3's contracts/v1/ScoredSignal.schema.json required set (additionalProperties: false).
REQUIRED_FIELDS = {
    "schema_version", "signal_id", "produced_at", "pair", "direction", "strategy_id",
    "regime", "model_score", "granularity", "proposed_entry", "proposed_sl",
    "proposed_tp", "atr",
}


# ----- StrategyBook -------------------------------------------------------------------
def test_book_loads_and_maps_regime_cells(champion_dir: Path):
    book = StrategyBook()
    assert book.load(champion_dir) is True
    ids = [s.strategy_id for s in book.strategies_for("EUR_USD", "Trending-Up")]
    assert ids == [10, 12]
    assert book.strategies_for("EUR_USD", "High-Vol") == []  # explicitly unqualified
    assert book.strategies_for("USD_JPY", "Ranging") == []   # unmapped instrument
    assert book.strategies_for("EUR_USD", None) == []


def test_book_skips_strategy_without_weights(tmp_path: Path, champion_dir: Path):
    # Map references strategy 99 which has no weights entry — skipped, not fatal.
    strat_map = {"EUR_USD": {"Ranging": [14, 99]}}
    (champion_dir / "regime_strategy_map.json").write_text(json.dumps(strat_map), encoding="utf-8")
    book = StrategyBook()
    assert book.load(champion_dir) is True
    ids = [s.strategy_id for s in book.strategies_for("EUR_USD", "Ranging")]
    assert ids == [14]


def test_book_keeps_last_good_on_corrupt_reload(champion_dir: Path):
    book = StrategyBook()
    assert book.load(champion_dir) is True
    (champion_dir / "strategy_weights.json").write_text("{not json", encoding="utf-8")
    assert book.load(champion_dir) is False
    # previous book still serves
    assert [s.strategy_id for s in book.strategies_for("EUR_USD", "Ranging")] == [14]


def test_config_rejects_bad_direction():
    with pytest.raises(ValueError):
        StrategyConfig.from_dict(10, {"direction": "sideways", "sl_atr_mult": 1, "tp_atr_mult": 2})


# ----- build_signal ----------------------------------------------------------------------
def _long_cfg(**over) -> StrategyConfig:
    base = dict(strategy_id=10, name="t", direction="long",
                sl_atr_mult=1.0, tp_atr_mult=3.0, entry_offset_atr=0.0)
    base.update(over)
    return StrategyConfig(**base)


def test_long_signal_geometry_matches_contract_example():
    sig = build_signal("EUR_USD", "H1", "Ranging", 0.63,
                       last_close=1.0842, atr=0.0021, strategy=_long_cfg(sl_atr_mult=1.5238095, tp_atr_mult=3.047619))
    assert sig["direction"] == "long"
    assert sig["proposed_entry"] == pytest.approx(1.0842)
    assert sig["proposed_sl"] < sig["proposed_entry"] < sig["proposed_tp"]  # protective stop below


def test_short_signal_mirrors_geometry():
    cfg = StrategyConfig(strategy_id=11, name="s", direction="short",
                         sl_atr_mult=1.5, tp_atr_mult=2.5, entry_offset_atr=0.1)
    sig = build_signal("EUR_USD", "H4", "Trending-Down", 0.5,
                       last_close=1.1000, atr=0.0020, strategy=cfg)
    assert sig["proposed_entry"] == pytest.approx(1.1000 - 0.0020 * 0.1)
    assert sig["proposed_sl"] == pytest.approx(1.1000 + 0.0020 * 1.5)   # stop above a short
    assert sig["proposed_tp"] == pytest.approx(1.1000 - 0.0020 * 2.5)   # target below
    assert sig["proposed_tp"] < 1.1000 < sig["proposed_sl"]


def test_signal_has_all_contract_fields():
    sig = build_signal("GBP_USD", "H1", "Trending-Up", 0.42,
                       last_close=1.2500, atr=0.0015, strategy=_long_cfg())
    assert set(sig) == REQUIRED_FIELDS
    assert sig["schema_version"] == "1"
    assert sig["pair"] == "GBP_USD"
    assert sig["strategy_id"] == "10"  # STRING per S3's deployed schema
    assert sig["granularity"] == "H1"
    assert sig["model_score"] == pytest.approx(0.42)
    assert sig["atr"] == pytest.approx(0.0015)
    assert sig["produced_at"].endswith("Z")
    assert len(sig["signal_id"]) == 36  # uuid4


def test_signal_ids_are_unique_per_call():
    a = build_signal("EUR_USD", "H1", "Ranging", 0.5, 1.1, 0.001, _long_cfg())
    b = build_signal("EUR_USD", "H1", "Ranging", 0.5, 1.1, 0.001, _long_cfg())
    assert a["signal_id"] != b["signal_id"]


# ----- serializer v1.0.0 format (what System 1 actually publishes) --------------------
SERIALIZER_MAP = {
    "schema_version": "1.0.0",
    "regimes": {
        "Trending-Up": [
            {"strategy_id": 10, "variant": "Range_Stochastic_Divergence@H1", "rank": 1},
        ],
        "Trending-Down": [
            {"strategy_id": 10, "variant": "Range_Stochastic_Divergence@H1", "rank": 1},
        ],
        "Ranging": [
            {"strategy_id": 10, "variant": "Range_Stochastic_Divergence@H1", "rank": 1},
            {"strategy_id": 10, "variant": "Range_Stochastic_Divergence@H4", "rank": 2},
        ],
    },
    "empty_regimes": ["High-Vol"],
}
SERIALIZER_WEIGHTS = {
    "schema_version": "1.0.0",
    "weights": {
        "Trending-Up": {"Range_Stochastic_Divergence@H1": 1.0},
        "Trending-Down": {"Range_Stochastic_Divergence@H1": 1.0},
        "Ranging": {
            "Range_Stochastic_Divergence@H1": 0.99999992,
            "Range_Stochastic_Divergence@H4": 8e-08,  # unqualified tail
        },
    },
}


def _serializer_dir(tmp_path: Path) -> Path:
    d = tmp_path / "serializer-set"
    d.mkdir()
    (d / "regime_strategy_map.json").write_text(json.dumps(SERIALIZER_MAP), encoding="utf-8")
    (d / "strategy_weights.json").write_text(json.dumps(SERIALIZER_WEIGHTS), encoding="utf-8")
    return d


def test_serializer_format_detected_and_scoped_by_granularity(tmp_path: Path):
    book = StrategyBook()
    assert book.load(_serializer_dir(tmp_path)) is True
    assert book.format == "serializer"
    # regime-keyed map is global across instruments
    for instrument in ("EUR_USD", "USD_JPY"):
        cfgs = book.strategies_for(instrument, "Trending-Up", "H1")
        assert [c.strategy_id for c in cfgs] == [10]
        assert cfgs[0].direction == "auto"
        assert cfgs[0].granularity == "H1"
    # variant scoped to H1 must not fire on an H4 cell
    assert book.strategies_for("EUR_USD", "Trending-Up", "H4") == []
    assert book.strategies_for("EUR_USD", "High-Vol", "H1") == []


def test_serializer_negligible_weight_variant_is_unqualified(tmp_path: Path):
    book = StrategyBook()
    book.load(_serializer_dir(tmp_path))
    # Ranging@H4 has weight 8e-08 -> filtered out; H4 cell gets nothing
    assert book.strategies_for("EUR_USD", "Ranging", "H4") == []
    assert [c.strategy_id for c in book.strategies_for("EUR_USD", "Ranging", "H1")] == [10]


def test_build_signal_refuses_unresolved_auto_direction():
    cfg = StrategyConfig(strategy_id=10, name="x", direction="auto")
    with pytest.raises(ValueError):
        build_signal("EUR_USD", "H1", "Ranging", 0.5, 1.1, 0.001, cfg)
