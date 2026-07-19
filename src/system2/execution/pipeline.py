"""EXEC-003 — Layer 4 slimmed to execution-only (Execution-Core).

The monolith ``src/layer4_executor/live_pipeline.py`` did BOTH position-level execution
AND account-level risk. In the three-system topology, account-level risk (Quarter-Kelly
sizing, daily/weekly limits, drawdown/consecutive-loss breakers, the authoritative ML
approval gate) belongs to **System 3**. This slim pipeline keeps ONLY execution concerns:

  * ATR-based stops/targets — the preserved determinism contract: SL = entry ∓ 1.0*ATR,
    TP = entry ± 3.0*ATR, RR 3.0 (identical math to the monolith ``compute_atr_risk_parameters``).
  * order construction; ``units`` is **taken as given** from the AMS-approved order — NEVER re-sized.
  * a lightweight **backup** correlation/exposure guard (fail-safe backstop only — NOT the
    authoritative risk gate; that's System 3).
  * fill validation / slippage / persistence / fill emission are delegated to injectable
    hooks (wired by EXEC-006/005); this module owns the deterministic decision + mode gating.

Feature flag ``EXEC_MODE`` (legacy | execution_only) keeps the legacy path warm for rollback;
``shadow`` runs execution_only WITHOUT submitting so its constructed orders can be compared
to the legacy formula (dual-run) before cutover. See docs/SYSTEM_BOUNDARY.md.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Iterable

from system2.common.logging import get_logger, log_event, set_correlation_id
from system2.common.secrets import Secrets, get_secrets

log = get_logger("execution.pipeline")

# Preserved execution-determinism contract (verbatim from the monolith).
DEFAULT_RR_RATIO = 3.0
DEFAULT_ATR_MULTIPLIER_SL = 1.0
DEFAULT_ATR_MULTIPLIER_TP = 3.0


class ExecMode(str, Enum):
    LEGACY = "legacy"
    EXECUTION_ONLY = "execution_only"


class Decision(str, Enum):
    EXECUTED = "executed"
    SHADOW = "shadow_constructed"
    SKIPPED_DUPLICATE = "skipped_duplicate"
    REJECTED_BACKUP_GUARD = "rejected_backup_guard"
    REJECTED_OUT_OF_SESSION = "rejected_out_of_session"
    REJECTED_INVALID = "rejected_invalid"
    REJECTED_VALIDATION = "rejected_validation"
    DEFERRED_LEGACY = "deferred_legacy"


# --------------------------------------------------------------------------- #
# Input contract (produced by System 3, consumed by EXEC-004)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RiskContext:
    atr: float
    suggested_sl: float | None = None
    suggested_tp: float | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RiskContext":
        return cls(
            atr=float(d["atr"]),
            suggested_sl=(float(d["suggested_sl"]) if d.get("suggested_sl") is not None else None),
            suggested_tp=(float(d["suggested_tp"]) if d.get("suggested_tp") is not None else None),
        )


@dataclass(frozen=True)
class ApprovedOrder:
    """Already-sized, risk-approved order. ``units`` is authoritative — never re-sized here."""

    idempotency_key: str
    correlation_id: str
    instrument: str
    side: str  # "BUY" | "SELL"
    units: float  # signed, already sized by AMS
    granularity: str  # "H1" | "H4"
    risk_context: RiskContext
    signal_id: int | None = None
    strategy_id: int | None = None
    ams_decision_id: str | None = None
    created_at: str | None = None
    schema_version: str = "1"
    message_id: str | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ApprovedOrder":
        return cls(
            idempotency_key=str(d["idempotency_key"]),
            correlation_id=str(d["correlation_id"]),
            instrument=str(d["instrument"]),
            side=str(d["side"]).upper(),
            units=float(d["units"]),
            granularity=str(d["granularity"]),
            risk_context=RiskContext.from_dict(d["risk_context"]),
            signal_id=d.get("signal_id"),
            strategy_id=d.get("strategy_id"),
            ams_decision_id=d.get("ams_decision_id"),
            created_at=d.get("created_at"),
            schema_version=str(d.get("schema_version", "1")),
            message_id=d.get("message_id"),
        )

    @property
    def direction(self) -> int:
        return 1 if self.side == "BUY" else -1


@dataclass(frozen=True)
class RiskParameters:
    entry_price: float
    stop_loss: float
    take_profit: float
    atr_value: float
    units: float
    rr_ratio: float = DEFAULT_RR_RATIO


@dataclass(frozen=True)
class ConstructedOrder:
    """Deterministic execution artifact — the unit compared in golden-file / dual-run tests."""

    idempotency_key: str
    correlation_id: str
    instrument: str
    side: str
    units: float
    entry_price: float
    stop_loss: float
    take_profit: float
    atr_value: float
    rr_ratio: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "idempotency_key": self.idempotency_key,
            "correlation_id": self.correlation_id,
            "instrument": self.instrument,
            "side": self.side,
            "units": self.units,
            "entry_price": self.entry_price,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "atr_value": self.atr_value,
            "rr_ratio": self.rr_ratio,
        }


class InvalidOrderError(Exception):
    """Order cannot be constructed (bad ATR, wrong-side SL/TP)."""


# --------------------------------------------------------------------------- #
# ATR stop/target math — single source of truth (preserved contract)
# --------------------------------------------------------------------------- #
def _atr_stops(
    direction: int, entry_price: float, atr: float,
    sl_mult: float = DEFAULT_ATR_MULTIPLIER_SL, tp_mult: float = DEFAULT_ATR_MULTIPLIER_TP,
) -> tuple[float, float]:
    """Return (stop_loss, take_profit). Raises InvalidOrderError on bad inputs/side."""
    if not atr or atr <= 0:
        raise InvalidOrderError(f"invalid ATR value: {atr}")
    sl_distance = atr * sl_mult
    tp_distance = atr * tp_mult
    if direction == 1:  # Buy
        stop_loss = entry_price - sl_distance
        take_profit = entry_price + tp_distance
        if stop_loss >= entry_price or take_profit <= entry_price:
            raise InvalidOrderError(f"BUY SL/TP wrong side: entry={entry_price} SL={stop_loss} TP={take_profit}")
    else:  # Sell
        stop_loss = entry_price + sl_distance
        take_profit = entry_price - tp_distance
        if stop_loss <= entry_price or take_profit >= entry_price:
            raise InvalidOrderError(f"SELL SL/TP wrong side: entry={entry_price} SL={stop_loss} TP={take_profit}")
    return stop_loss, take_profit


def legacy_atr_risk_parameters(
    direction: int, entry_price: float, atr: float,
    rr_ratio: float = DEFAULT_RR_RATIO,
    atr_multiplier_sl: float = DEFAULT_ATR_MULTIPLIER_SL,
    atr_multiplier_tp: float = DEFAULT_ATR_MULTIPLIER_TP,
) -> tuple[float, float]:
    """Faithful reproduction of the monolith ``compute_atr_risk_parameters`` SL/TP math.

    Stands in for "the legacy monolith on Computer 1" so the dual-run shadow test can
    assert the slim path constructs equivalent orders before cutover.
    """
    sl_distance = atr * atr_multiplier_sl
    tp_distance = atr * atr_multiplier_tp
    if direction == 1:
        return entry_price - sl_distance, entry_price + tp_distance
    return entry_price + sl_distance, entry_price - tp_distance


def is_in_session(dt: datetime) -> bool:
    """Trading session guard: Sun 22:00 UTC -> Fri 20:00 UTC (weekend gap rejected)."""
    dt = dt.astimezone(timezone.utc)
    wd = dt.weekday()  # Mon=0 .. Sun=6
    if wd == 5:  # Saturday — always closed
        return False
    if wd == 6:  # Sunday — open from 22:00
        return dt.hour >= 22
    if wd == 4:  # Friday — closed from 20:00
        return dt.hour < 20
    return True  # Mon-Thu fully open


# --------------------------------------------------------------------------- #
# Backup correlation/exposure guard (fail-safe backstop — NOT the risk engine)
# --------------------------------------------------------------------------- #
@dataclass
class BackupCorrelationGuard:
    """Conservative hard backstop. Catches a System-3 fault; never re-decides risk.

    Rejects only past *loose* hard limits set well above normal AMS sizing:
      * total open positions >= ``max_open_positions``;
      * a second open position in the same instrument (duplicate stacking).
    """

    max_open_positions: int = 8

    def evaluate(self, order: ApprovedOrder, open_instruments: Iterable[str]) -> tuple[bool, str]:
        open_list = list(open_instruments)
        if len(open_list) >= self.max_open_positions:
            return False, f"backup_guard: open positions {len(open_list)} >= cap {self.max_open_positions}"
        if order.instrument in open_list:
            return False, f"backup_guard: already an open position in {order.instrument}"
        return True, ""


# --------------------------------------------------------------------------- #
# Processed-key store (idempotency) — in-memory default; EXEC-004 persists to state/
# --------------------------------------------------------------------------- #
class InMemoryProcessedStore:
    def __init__(self) -> None:
        self._seen: set[str] = set()

    def seen(self, key: str) -> bool:
        return key in self._seen

    def mark(self, key: str) -> None:
        self._seen.add(key)


# --------------------------------------------------------------------------- #
# The slim pipeline
# --------------------------------------------------------------------------- #
class ExecutionPipeline:
    """Execution-only Layer 4. Deterministic order construction + mode/flag gating."""

    def __init__(
        self,
        mode: ExecMode | None = None,
        shadow: bool | None = None,
        processed_store: Any | None = None,
        backup_guard: BackupCorrelationGuard | None = None,
        secrets: Secrets | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.secrets = secrets or get_secrets()
        self.mode = mode or ExecMode((self.secrets.get("EXEC_MODE", "execution_only") or "execution_only"))
        self.shadow = self.secrets.get_bool("EXEC_SHADOW", False) if shadow is None else shadow
        self.processed = processed_store or InMemoryProcessedStore()
        self.backup_guard = backup_guard or BackupCorrelationGuard(
            max_open_positions=self.secrets.get_int("MAX_OPEN_POSITIONS", 8)
        )
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def build_order(self, order: ApprovedOrder, entry_price: float) -> ConstructedOrder:
        """Pure, deterministic construction (ATR stops + AMS units). Raises on invalid.

        ``units`` is taken from the order verbatim — never modified.
        """
        rc = order.risk_context
        if rc.suggested_sl is not None and rc.suggested_tp is not None:
            stop_loss, take_profit = rc.suggested_sl, rc.suggested_tp
        else:
            stop_loss, take_profit = _atr_stops(order.direction, entry_price, rc.atr)
        return ConstructedOrder(
            idempotency_key=order.idempotency_key,
            correlation_id=order.correlation_id,
            instrument=order.instrument,
            side=order.side,
            units=order.units,  # NEVER re-sized
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            atr_value=rc.atr,
            rr_ratio=DEFAULT_RR_RATIO,
        )

    def process(
        self,
        order: ApprovedOrder,
        entry_price: float,
        open_instruments: Iterable[str] = (),
        submit_fn: Callable[[ConstructedOrder], Any] | None = None,
        persist_fn: Callable[[ConstructedOrder, Any], None] | None = None,
        emit_fn: Callable[[ConstructedOrder, Any], None] | None = None,
        validate_fn: Callable[[ConstructedOrder], tuple[bool, str]] | None = None,
    ) -> dict[str, Any]:
        """Run the execution_only path for one approved order.

        submit/persist/emit are injectable (wired by EXEC-006/005). ``validate_fn`` is the
        EXEC-004 last-line (§7.2) sanity gate, applied to the *constructed* order after
        build and before the backup guard / submit; it returns ``(passed, reason)`` and a
        failure short-circuits to ``REJECTED_VALIDATION`` (durably handled — ack, no retry).
        Returns a result dict with the decision, the constructed order (if any), and the
        fill (if submitted).
        """
        set_correlation_id(order.correlation_id)

        if self.mode is ExecMode.LEGACY:
            log_event(log, logging.INFO, "EXEC_MODE=legacy; deferring to legacy path",
                      idempotency_key=order.idempotency_key)
            return {"decision": Decision.DEFERRED_LEGACY, "order": None, "fill": None}

        if self.processed.seen(order.idempotency_key):
            log_event(log, logging.INFO, "duplicate idempotency_key; skipping",
                      idempotency_key=order.idempotency_key)
            return {"decision": Decision.SKIPPED_DUPLICATE, "order": None, "fill": None}

        if not is_in_session(self._clock()):
            log_event(log, logging.WARNING, "order outside trading session; rejected",
                      idempotency_key=order.idempotency_key, instrument=order.instrument)
            return {"decision": Decision.REJECTED_OUT_OF_SESSION, "order": None, "fill": None}

        try:
            constructed = self.build_order(order, entry_price)
        except InvalidOrderError as exc:
            log_event(log, logging.ERROR, "invalid order; rejected",
                      idempotency_key=order.idempotency_key, detail=str(exc))
            return {"decision": Decision.REJECTED_INVALID, "order": None, "fill": None, "reason": str(exc)}

        if validate_fn is not None:
            ok, reason = validate_fn(constructed)
            if not ok:
                log_event(log, logging.WARNING, "last-line validation rejected order (§7.2)",
                          idempotency_key=order.idempotency_key, reason=reason)
                return {"decision": Decision.REJECTED_VALIDATION, "order": constructed,
                        "fill": None, "reason": reason}

        passed, reason = self.backup_guard.evaluate(order, open_instruments)
        if not passed:
            log_event(log, logging.WARNING, "backup guard rejected order (System-3 fault backstop)",
                      idempotency_key=order.idempotency_key, reason=reason)
            return {"decision": Decision.REJECTED_BACKUP_GUARD, "order": constructed, "fill": None, "reason": reason}

        if self.shadow:
            log_event(log, logging.INFO, "shadow mode: constructed order, NOT submitted",
                      idempotency_key=order.idempotency_key)
            return {"decision": Decision.SHADOW, "order": constructed, "fill": None}

        if submit_fn is None:
            raise ValueError("submit_fn required in non-shadow execution_only mode (wired by EXEC-006)")
        fill = submit_fn(constructed)
        if persist_fn is not None:
            persist_fn(constructed, fill)
        if emit_fn is not None:
            emit_fn(constructed, fill)
        self.processed.mark(order.idempotency_key)
        log_event(log, logging.INFO, "order executed",
                  idempotency_key=order.idempotency_key, instrument=order.instrument,
                  units=order.units, sl=constructed.stop_loss, tp=constructed.take_profit)
        return {"decision": Decision.EXECUTED, "order": constructed, "fill": fill}
