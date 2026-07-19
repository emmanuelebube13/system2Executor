"""EXEC-011 — strategy map parsing and ScoredSignal construction.

Reads ``regime_strategy_map.json`` and ``strategy_weights.json`` from the active model
set and turns one scored (instrument, regime, granularity, bar) cell into ScoredSignal
payloads.

Two on-disk formats are supported (auto-detected):

* **Serializer v1.0.0 (what System 1 actually publishes)** — regime-keyed, global
  across instruments::

      regime_strategy_map.json: {"regimes": {"Ranging": [{"strategy_id": 10,
          "variant": "Range_Stochastic_Divergence@H1", "rank": 1, ...}], ...}}
      strategy_weights.json:    {"weights": {"Ranging": {"Range_...@H1": 0.999, ...}}}

  The variant's ``@<granularity>`` suffix scopes it to that timeframe; a variant whose
  weight is ~0 is unqualified. This format carries no direction or SL/TP config, so
  direction is ``auto`` (resolved by the producer from regime + price position) and the
  SL/TP geometry uses the system-wide 1×ATR / 3×ATR convention — which the S2/S3 bridge
  inverts exactly (``atr = |tp - sl| / 4``).

* **EXEC-011 spec format** — instrument-keyed map + per-strategy static config
  (direction, ``sl_atr_mult``, ``tp_atr_mult``, ``entry_offset_atr``).

The wire shape follows System 3's DEPLOYED validator (``ams/contracts/v1/
ScoredSignal.schema.json``), which is stricter than the guide's example: the message is
FLAT (``additionalProperties: false`` — no envelope), ``schema_version: "1"`` is a
top-level const, and ``strategy_id`` is a STRING. ``signal_id`` is S3's dedup key.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from system2.common.logging import get_logger, log_event

log = get_logger("live_signal_producer.signal_builder")

STRATEGY_MAP_FILE = "regime_strategy_map.json"
STRATEGY_WEIGHTS_FILE = "strategy_weights.json"

# System-wide protective-stop convention (send_test_signal.py, bridge atr=|tp-sl|/4).
DEFAULT_SL_ATR_MULT = 1.0
DEFAULT_TP_ATR_MULT = 3.0

# A variant whose portfolio weight is below this is unqualified (e.g. the 8e-08
# Ranging@H4 tail in the 2026-07-01 bundle).
MIN_VARIANT_WEIGHT = 1e-3

_PRICE_DECIMALS = 6  # enough for JPY pips and 5-decimal majors alike


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class StrategyConfig:
    """Per-strategy signal parameters (static config or derived from the variant)."""

    strategy_id: int
    name: str
    direction: str  # "long" | "short" | "auto" (producer resolves from regime/price)
    sl_atr_mult: float = DEFAULT_SL_ATR_MULT
    tp_atr_mult: float = DEFAULT_TP_ATR_MULT
    entry_offset_atr: float = 0.0
    granularity: str | None = None  # variant timeframe scope; None = any

    @classmethod
    def from_dict(cls, strategy_id: int | str, d: dict[str, Any]) -> "StrategyConfig":
        direction = str(d["direction"]).lower()
        if direction not in ("long", "short", "auto"):
            raise ValueError(f"strategy {strategy_id}: invalid direction '{direction}'")
        return cls(
            strategy_id=int(strategy_id),
            name=str(d.get("name", f"strategy_{strategy_id}")),
            direction=direction,
            sl_atr_mult=float(d["sl_atr_mult"]),
            tp_atr_mult=float(d["tp_atr_mult"]),
            entry_offset_atr=float(d.get("entry_offset_atr", 0.0)),
        )


def _split_variant(variant: str) -> tuple[str, str | None]:
    """``"Range_Stochastic_Divergence@H1"`` -> ``("Range_Stochastic_Divergence", "H1")``."""
    name, sep, gran = variant.partition("@")
    return name, (gran or None) if sep else None


class StrategyBook:
    """The active set's regime->strategy map + configs (hot-reloadable, dual-format)."""

    def __init__(self) -> None:
        # spec format: instrument -> regime -> [strategy ids] (+ id -> config)
        self._by_instrument: dict[str, dict[str, list[int]]] = {}
        self._spec_weights: dict[int, StrategyConfig] = {}
        # serializer v1.0.0 format: regime -> [StrategyConfig] (weight-filtered, ranked)
        self._by_regime: dict[str, list[StrategyConfig]] = {}
        self._format: str | None = None  # "spec" | "serializer"
        self._loaded_from: Path | None = None

    @property
    def loaded(self) -> bool:
        return self._format is not None

    @property
    def format(self) -> str | None:
        return self._format

    # ----- loading -----------------------------------------------------------------
    def load(self, set_dir: Path) -> bool:
        """(Re)load both strategy files from ``set_dir``. Keeps last good on failure."""
        set_dir = Path(set_dir)
        try:
            raw_map = json.loads((set_dir / STRATEGY_MAP_FILE).read_text(encoding="utf-8"))
            raw_weights = json.loads((set_dir / STRATEGY_WEIGHTS_FILE).read_text(encoding="utf-8"))
            if "regimes" in raw_map:
                by_regime = self._parse_serializer(raw_map, raw_weights)
                fmt = "serializer"
                by_instrument: dict[str, dict[str, list[int]]] = {}
                spec_weights: dict[int, StrategyConfig] = {}
            else:
                by_instrument, spec_weights = self._parse_spec(raw_map, raw_weights)
                fmt = "spec"
                by_regime = {}
        except Exception as exc:
            log_event(log, logging.ERROR, "strategy book load failed; keeping last good",
                      path=str(set_dir), error=type(exc).__name__, detail=str(exc),
                      had_previous=self.loaded)
            return False
        self._by_instrument, self._spec_weights = by_instrument, spec_weights
        self._by_regime, self._format = by_regime, fmt
        self._loaded_from = set_dir
        log_event(log, logging.INFO, "strategy book loaded",
                  path=str(set_dir), format=fmt,
                  regimes=len(by_regime) or sum(len(v) for v in by_instrument.values()))
        return True

    @staticmethod
    def _parse_serializer(
        raw_map: dict[str, Any], raw_weights: dict[str, Any]
    ) -> dict[str, list[StrategyConfig]]:
        """Parse System 1's serializer v1.0.0 shape (regime-keyed, variant-scoped)."""
        weight_of: dict[str, dict[str, float]] = {
            str(regime): {str(v): float(w) for v, w in variants.items()}
            for regime, variants in (raw_weights.get("weights") or {}).items()
        }
        out: dict[str, list[StrategyConfig]] = {}
        for regime, entries in (raw_map.get("regimes") or {}).items():
            configs: list[StrategyConfig] = []
            for entry in sorted(entries, key=lambda e: e.get("rank", 0)):
                variant = str(entry["variant"])
                name, gran = _split_variant(variant)
                weight = weight_of.get(regime, {}).get(variant, 1.0)
                if weight < MIN_VARIANT_WEIGHT:
                    continue  # unqualified tail allocation
                configs.append(StrategyConfig(
                    strategy_id=int(entry["strategy_id"]), name=name,
                    direction="auto", granularity=gran,
                ))
            out[str(regime)] = configs
        return out

    @staticmethod
    def _parse_spec(
        raw_map: dict[str, Any], raw_weights: dict[str, Any]
    ) -> tuple[dict[str, dict[str, list[int]]], dict[int, StrategyConfig]]:
        """Parse the EXEC-011 spec shape (instrument-keyed map + static configs)."""
        by_instrument = {
            str(instrument): {str(regime): [int(s) for s in ids]
                              for regime, ids in regimes.items()}
            for instrument, regimes in raw_map.items()
        }
        spec_weights = {
            int(sid): StrategyConfig.from_dict(sid, cfg) for sid, cfg in raw_weights.items()
        }
        return by_instrument, spec_weights

    # ----- lookup ------------------------------------------------------------------
    def strategies_for(
        self, instrument: str, regime: str | None, granularity: str | None = None
    ) -> list[StrategyConfig]:
        """Configs for the qualified (instrument, regime, granularity) cell; [] if none.

        In serializer format the map is global across instruments and each variant is
        scoped to its ``@granularity``; one config per strategy_id (best rank wins).
        """
        if regime is None or not self.loaded:
            return []
        if self._format == "serializer":
            out: list[StrategyConfig] = []
            seen: set[int] = set()
            for cfg in self._by_regime.get(regime, []):
                if granularity is not None and cfg.granularity not in (None, granularity):
                    continue
                if cfg.strategy_id in seen:
                    continue
                seen.add(cfg.strategy_id)
                out.append(cfg)
            return out
        ids = self._by_instrument.get(instrument, {}).get(regime, [])
        out = []
        for sid in ids:
            cfg = self._spec_weights.get(sid)
            if cfg is None:
                log_event(log, logging.WARNING, "strategy id has no weights entry; skipping",
                          instrument=instrument, regime=regime, strategy_id=sid)
                continue
            if granularity is not None and cfg.granularity not in (None, granularity):
                continue
            out.append(cfg)
        return out


def build_signal(
    instrument: str,
    granularity: str,
    regime: str,
    model_score: float,
    last_close: float,
    atr: float,
    strategy: StrategyConfig,
    signal_id: str | None = None,
    produced_at: str | None = None,
) -> dict[str, Any]:
    """Construct one ScoredSignal payload (S3 contracts/v1) for one strategy.

    ``strategy.direction`` must already be resolved to long/short (never "auto" here).
    """
    if strategy.direction not in ("long", "short"):
        raise ValueError(f"unresolved direction '{strategy.direction}' for strategy "
                         f"{strategy.strategy_id} — resolve 'auto' before building")
    sign = 1.0 if strategy.direction == "long" else -1.0
    entry = last_close + atr * strategy.entry_offset_atr * sign
    sl = last_close - atr * strategy.sl_atr_mult * sign
    tp = last_close + atr * strategy.tp_atr_mult * sign
    return {
        "schema_version": "1",
        "signal_id": signal_id or str(uuid.uuid4()),
        "produced_at": produced_at or _utc_now_iso(),
        "pair": instrument,
        "direction": strategy.direction,
        "strategy_id": str(strategy.strategy_id),
        "regime": regime,
        "model_score": round(float(model_score), 6),
        "granularity": granularity,
        "proposed_entry": round(entry, _PRICE_DECIMALS),
        "proposed_sl": round(sl, _PRICE_DECIMALS),
        "proposed_tp": round(tp, _PRICE_DECIMALS),
        "atr": round(float(atr), _PRICE_DECIMALS),
    }
