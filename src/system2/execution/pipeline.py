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
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from enum import Enum
from typing import Any, Callable, Iterable

from system2.common.logging import get_logger, log_event, set_correlation_id
from system2.common.secrets import MissingSecretError, Secrets, get_secrets

log = get_logger("execution.pipeline")

# Preserved execution-determinism contract (verbatim from the monolith).
DEFAULT_RR_RATIO = 3.0
DEFAULT_ATR_MULTIPLIER_SL = 1.0
DEFAULT_ATR_MULTIPLIER_TP = 3.0

# --------------------------------------------------------------------------- #
# EXEC_SHADOW — one source of truth (F-309 / OD-5)
# --------------------------------------------------------------------------- #
# `EXEC_SHADOW` decides whether an approved order is actually submitted to the broker
# or merely simulated. It had TWO readers that disagreed on the default: this module
# resolved an absent flag to False (submit for real) while `lifecycle.build_from_secrets`
# resolved it to True (simulate). So the intent could be read from two places and give
# two answers, and nothing exposed the RESOLVED value at all — the deployed reality was
# unreadable without shell access (F-309). Both callers now resolve through
# `resolve_shadow`, and the production path passes `require_explicit=True`.
#
# The default is True because that is the safe direction: if this is ever reached, it
# simulates rather than sends. `exec_mode` and `EXEC_SHADOW` are ORTHOGONAL — a PAUSED
# engine is neither shadow nor live, and reading one from the other is a category error.
SHADOW_DEFAULT = True


def resolve_shadow(secrets: Secrets, *, require_explicit: bool = False) -> bool:
    """The effective ``EXEC_SHADOW``, resolved in exactly one place.

    ``require_explicit=True`` refuses to guess: a flag that decides whether orders reach
    a real broker should not be settled by whichever import happened to win, so the
    production path treats an absent ``EXEC_SHADOW`` as a missing secret and fails closed
    at startup rather than silently adopting a posture.
    """
    if require_explicit and not str(secrets.get("EXEC_SHADOW", "") or "").strip():
        raise MissingSecretError(
            "EXEC_SHADOW is not set. It decides whether approved orders are submitted to "
            "the broker (false) or only simulated (true); it is too consequential to "
            "default. Set it explicitly in .env.system2."
        )
    return secrets.get_bool("EXEC_SHADOW", SHADOW_DEFAULT)


class ExecMode(str, Enum):
    LEGACY = "legacy"
    EXECUTION_ONLY = "execution_only"


class Decision(str, Enum):
    EXECUTED = "executed"
    SHADOW = "shadow_constructed"
    SKIPPED_DUPLICATE = "skipped_duplicate"
    # A rehearsal that reached the last step and deliberately stopped. Distinct from SHADOW
    # (an engine-wide posture) because this is one order opting out, and distinct from every
    # REJECTED_* because nothing was wrong with it — it passed every gate.
    DRILL_NOT_SUBMITTED = "drill_not_submitted"
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
    # The entry System 1 designed the bracket around. ``suggested_sl``/``suggested_tp`` are
    # ABSOLUTE prices anchored to THIS entry, not to the price System 2 actually fills at, so
    # without it the bracket cannot be re-anchored and its distances are meaningless. Optional
    # because System 3 and the bridge only started forwarding it in this change — an order
    # that predates that keeps the old verbatim behaviour rather than being rejected.
    proposed_entry: float | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RiskContext":
        return cls(
            atr=float(d["atr"]),
            suggested_sl=(float(d["suggested_sl"]) if d.get("suggested_sl") is not None else None),
            suggested_tp=(float(d["suggested_tp"]) if d.get("suggested_tp") is not None else None),
            proposed_entry=(
                float(d["proposed_entry"]) if d.get("proposed_entry") is not None else None
            ),
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
    # A rehearsal: construct and check this order exactly like a real one, then STOP before the
    # broker. Defaults False so an order that predates the field — or one from a producer that
    # never sets it — behaves exactly as it does today. Absent is NOT ambiguous here precisely
    # because System 1 stamps it on every message; if that ever changes, the safe reading of a
    # missing flag is "real order", which is what this default gives.
    drill: bool = False

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
            drill=bool(d.get("drill", False)),
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


class NoMarketPriceError(InvalidOrderError):
    """No usable market price for the expected-entry reference — refuse to construct.

    F-306: the reference price used to be ``suggested_sl or atr`` (lifecycle ``_price_fn``),
    i.e. the STOP price, or — when System 3 sent no suggestion — a raw ATR (~0.0016) used as
    if it were a price, which constructed a BUY with ``SL = 0.0016 - 0.0016 = 0.0``. A missing
    price is a **reject**, never a guess: an order with no real market reference cannot be
    sized-checked, cannot be slippage-checked, and can carry a structurally invalid stop.
    """


# --------------------------------------------------------------------------- #
# Price sanity — fail-closed gate in front of all order construction (F-306)
# --------------------------------------------------------------------------- #
def require_market_price(instrument: str, entry_price: Any) -> float:
    """Return ``entry_price`` as a usable market price, or raise ``NoMarketPriceError``.

    Fail-closed by design: ``None``, NaN/Inf and non-positive values are all *absence of a
    price*, and the only safe response to that is to refuse to build the order.
    """
    if entry_price is None:
        raise NoMarketPriceError(
            f"{instrument}: no market price available for the expected-entry reference; "
            "refusing to construct an order (fail-closed)"
        )
    try:
        price = float(entry_price)
    except (TypeError, ValueError) as exc:
        raise NoMarketPriceError(f"{instrument}: entry price {entry_price!r} is not a number") from exc
    if not math.isfinite(price):
        raise NoMarketPriceError(f"{instrument}: entry price {price!r} is not finite")
    if price <= 0:
        raise NoMarketPriceError(f"{instrument}: entry price {price!r} is not positive")
    return price


def assert_protective_prices(
    instrument: str, direction: int, entry_price: float, stop_loss: Any, take_profit: Any
) -> tuple[float, float]:
    """Last-line invariant on the constructed SL/TP. Raises ``InvalidOrderError``.

    Two rules, both absolute:
      1. **A stop-loss or take-profit is never <= 0.** A zero/negative protective price is
         either a broker reject or — worse, if the broker takes it — an unprotected position.
         This is the F-306 headline: ATR-as-entry produced ``SL = 0.0`` and it reached order
         construction because the ``suggested_sl``/``suggested_tp`` branch validated nothing.
      2. **SL and TP are on the correct side of the entry price.** ``_atr_stops`` already
         enforced this for the ATR branch; System-3-supplied prices were previously taken
         verbatim, so a stale/mismatched suggestion could invert the trade's risk.
    """
    for name, value in (("stop_loss", stop_loss), ("take_profit", take_profit)):
        if value is None:
            raise InvalidOrderError(f"{instrument}: {name} is missing")
        value = float(value)
        if not math.isfinite(value) or value <= 0:
            raise InvalidOrderError(
                f"{instrument}: {name}={value} is not a positive price "
                f"(entry={entry_price}); refusing to construct an order"
            )
    stop_loss, take_profit = float(stop_loss), float(take_profit)
    if direction == 1:
        if stop_loss >= entry_price or take_profit <= entry_price:
            raise InvalidOrderError(
                f"{instrument}: BUY SL/TP wrong side: entry={entry_price} "
                f"SL={stop_loss} TP={take_profit}"
            )
    elif stop_loss <= entry_price or take_profit >= entry_price:
        raise InvalidOrderError(
            f"{instrument}: SELL SL/TP wrong side: entry={entry_price} "
            f"SL={stop_loss} TP={take_profit}"
        )
    return stop_loss, take_profit


class StaleSetupError(InvalidOrderError):
    """The market has moved so far from the signal's entry that the setup no longer exists."""


# How far the market may drift from ``proposed_entry`` before the setup is considered gone,
# expressed as a multiple of the signal's own stop distance. Re-anchoring keeps the trade's
# geometry intact but it cannot make a stale premise fresh: past this the entry level the
# strategy was reasoning about is simply not where price is any more.
DEFAULT_ENTRY_DRIFT_SL_MULT = 2.0


def reanchor_bracket(
    instrument: str,
    direction: int,
    price: float,
    proposed_entry: float,
    suggested_sl: float,
    suggested_tp: float,
    *,
    drift_sl_mult: float = DEFAULT_ENTRY_DRIFT_SL_MULT,
) -> tuple[float, float]:
    """Re-express System 1's bracket around the entry System 2 is actually filling at.

    ``suggested_sl``/``suggested_tp`` are absolute prices anchored to ``proposed_entry`` — a
    *setup level*, not a spot quote (the same 1.36778 was proposed for GBP_USD on both 08-24
    and 08-26). System 2 fills at market, and taking those absolute levels verbatim against a
    different entry silently changes the trade:

      * observed 2026-08-24, both fills: intended stop 151.9 / 163.1 pips, actual stop
        103.0 / 112.8 pips — ~32% tighter than the distance System 3 sized the position on
        (``sizing.py``: "the true risk-per-unit is |proposed_entry - proposed_sl|");
      * observed 2026-08-26, GBP_USD: drift 73 pips exceeded the 64-pip stop, so the stop
        landed on the far side of entry and ``assert_protective_prices`` rejected the order.

    Preserving the *distances* rather than the *levels* fixes both: the stop stays the width
    System 3 sized against, and a wrong-side bracket becomes arithmetically impossible.
    """
    sl_distance = abs(proposed_entry - suggested_sl)
    tp_distance = abs(suggested_tp - proposed_entry)
    if sl_distance <= 0 or tp_distance <= 0:
        raise InvalidOrderError(
            f"{instrument}: degenerate bracket from System 3 — entry={proposed_entry} "
            f"SL={suggested_sl} TP={suggested_tp}; refusing to construct an order"
        )

    drift = abs(price - proposed_entry)
    if drift > drift_sl_mult * sl_distance:
        raise StaleSetupError(
            f"{instrument}: entry drifted {drift:.5f} from proposed {proposed_entry} "
            f"(> {drift_sl_mult}x the {sl_distance:.5f} stop distance); setup is stale, "
            "refusing to construct an order"
        )

    if direction == 1:
        return price - sl_distance, price + tp_distance
    return price + sl_distance, price - tp_distance


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


FX_TZ = "America/New_York"
FX_SESSION_LOCAL_HOUR = 17          # the FX week: Sun 17:00 ET -> Fri 17:00 ET


def _fx_session_edge(now: datetime, weekday: int, tz_name: str, local_hour: int) -> datetime:
    """``weekday`` (Mon=0) at ``local_hour`` in ``tz_name``, as UTC, within ``now``'s week."""
    local_now = now.astimezone(ZoneInfo(tz_name))
    day = (local_now + timedelta(days=weekday - local_now.weekday())).date()
    return datetime(day.year, day.month, day.day, local_hour,
                    tzinfo=ZoneInfo(tz_name)).astimezone(timezone.utc)


def is_in_session(dt: datetime, tz_name: str = FX_TZ,
                  local_hour: int = FX_SESSION_LOCAL_HOUR) -> bool:
    """Trading session guard: the real FX week, Sun 17:00 ET -> Fri 17:00 ET.

    Was hardcoded as Sun 22:00 -> Fri 20:00 UTC. Two problems with fixed UTC hours:

      * They are only right for half the year. 17:00 ET is 21:00 UTC under EDT and 22:00 UTC
        under EST, so a fixed pair is an hour out for roughly five months at a time.
      * They did not agree with System 3's layer I (Fri 18:00 -> Sun 22:00) or with each other,
        so three different components each held a different opinion about when the market was
        open, and the tightest one silently won.

    Resolving through the exchange's timezone makes the boundary track the actual session and
    gives both systems one definition to share.
    """
    dt = dt.astimezone(timezone.utc)
    close = _fx_session_edge(dt, 4, tz_name, local_hour)   # Friday close
    open_ = _fx_session_edge(dt, 6, tz_name, local_hour)   # Sunday open
    if open_ < close:
        open_ += timedelta(days=7)
    return not (close <= dt < open_)


# --------------------------------------------------------------------------- #
# Backup correlation/exposure guard (fail-safe backstop — NOT the risk engine)
# --------------------------------------------------------------------------- #
@dataclass
class BackupCorrelationGuard:
    """Conservative hard backstop. Catches a System-3 fault; never re-decides risk.

    Rejects only past one *loose* hard limit set well above normal AMS sizing:
      * total open positions >= ``max_open_positions``.

    **Same-instrument re-entry is NOT blocked here.** It used to be: a second position in
    an instrument already held was rejected outright. That is a per-pair exposure decision,
    and per-pair exposure belongs to System 3 — this guard's own contract is "catches a
    System-3 fault; never re-decides risk". In practice the block was load-bearing in the
    wrong direction: three positions opened 2026-08-24 never closed, and because EUR_USD,
    GBP_USD and USD_CAD carry nearly all of the signal flow, every subsequent approved order
    on those pairs was refused here for four days while System 3 had approved each one.

    The overall ``max_open_positions`` ceiling still bounds total exposure, so a runaway
    System 3 cannot open unbounded positions. Note that System 3 currently enforces no
    per-pair stacking limit of its own; if one is wanted, that is where it belongs.
    """

    max_open_positions: int = 8

    def evaluate(self, order: ApprovedOrder, open_instruments: Iterable[str]) -> tuple[bool, str]:
        open_list = list(open_instruments)
        if len(open_list) >= self.max_open_positions:
            return False, f"backup_guard: open positions {len(open_list)} >= cap {self.max_open_positions}"
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


def guard_keys(order: "ApprovedOrder") -> list[str]:
    """Every key an approved order must be deduped on, most specific first.

    ``idempotency_key`` is the order identity (System 3's ``order_request_id``, relayed by the
    bridge). ``signal_id`` is added as **defence in depth** (F-206/F-303): System 3 is supposed
    to derive one stable order id per signal, but if it ever re-mints one, the order id stops
    deduping while the signal still does — so a Guardian fault cannot double a live position
    on System 2's watch. One approved signal ⇒ at most one broker order, by construction.
    """
    keys = [order.idempotency_key]
    if order.signal_id is not None and str(order.signal_id) != "":
        keys.append(f"signal:{order.signal_id}")
    return keys


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
        self.shadow = resolve_shadow(self.secrets) if shadow is None else shadow
        self.processed = processed_store or InMemoryProcessedStore()
        self.backup_guard = backup_guard or BackupCorrelationGuard(
            max_open_positions=self.secrets.get_int("MAX_OPEN_POSITIONS", 8)
        )
        self.entry_drift_sl_mult = float(
            self.secrets.get_int("ENTRY_DRIFT_MAX_SL_MULT", int(DEFAULT_ENTRY_DRIFT_SL_MULT))
        )
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def build_order(self, order: ApprovedOrder, entry_price: float | None) -> ConstructedOrder:
        """Pure, deterministic construction (ATR stops + AMS units). Raises on invalid.

        ``units`` is taken from the order verbatim — never modified.

        ``entry_price`` must be a **real market price** for ``order.instrument``. F-306:
        it used to be the stop price (or, absent a suggestion, a raw ATR), which made every
        fill look like it slipped by the SL distance and let an order be built with
        ``SL = 0.0``. ``entry_price=None`` means "the price source had nothing" and is a
        hard reject — see ``require_market_price``; never substitute a guess.
        """
        rc = order.risk_context
        price = require_market_price(order.instrument, entry_price)
        if rc.suggested_sl is not None and rc.suggested_tp is not None:
            if rc.proposed_entry is not None:
                # Preserve the bracket's GEOMETRY at the entry actually used, and refuse
                # outright once the market has left the setup behind — see reanchor_bracket.
                stop_loss, take_profit = reanchor_bracket(
                    order.instrument, order.direction, price,
                    rc.proposed_entry, rc.suggested_sl, rc.suggested_tp,
                    drift_sl_mult=self.entry_drift_sl_mult,
                )
            else:
                # Pre-change order with no entry anchor: unchanged verbatim behaviour.
                stop_loss, take_profit = rc.suggested_sl, rc.suggested_tp
        else:
            stop_loss, take_profit = _atr_stops(order.direction, price, rc.atr)
        stop_loss, take_profit = assert_protective_prices(
            order.instrument, order.direction, price, stop_loss, take_profit
        )
        entry_price = price
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
        entry_price: float | None,
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

        ``entry_price`` may be ``None`` when the market-price source had nothing to give;
        that is a ``REJECTED_INVALID`` (durably acked, never submitted), not a fallback.
        """
        set_correlation_id(order.correlation_id)

        if self.mode is ExecMode.LEGACY:
            log_event(log, logging.INFO, "EXEC_MODE=legacy; deferring to legacy path",
                      idempotency_key=order.idempotency_key)
            return {"decision": Decision.DEFERRED_LEGACY, "order": None, "fill": None}

        for key in guard_keys(order):
            if self.processed.seen(key):
                log_event(log, logging.INFO, "already executed; skipping",
                          idempotency_key=order.idempotency_key, dedup_key=key)
                return {"decision": Decision.SKIPPED_DUPLICATE, "order": None, "fill": None}

        if not is_in_session(self._clock()):
            log_event(log, logging.WARNING, "order outside trading session; rejected",
                      idempotency_key=order.idempotency_key, instrument=order.instrument)
            return {"decision": Decision.REJECTED_OUT_OF_SESSION, "order": None, "fill": None}

        try:
            constructed = self.build_order(order, entry_price)
        except NoMarketPriceError as exc:
            # F-306 fail-closed branch: no price ⇒ no order. Loud, because a price source that
            # is silently down means the engine is dropping approved orders on the floor.
            log_event(log, logging.ERROR, "no market price; order NOT constructed (fail-closed)",
                      idempotency_key=order.idempotency_key, instrument=order.instrument,
                      detail=str(exc))
            return {"decision": Decision.REJECTED_INVALID, "order": None, "fill": None, "reason": str(exc)}
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

        # A drill stops HERE — after construction, §7.2 validation and the backup guard, and
        # immediately before the only irreversible step. Placing it last is the whole point:
        # the rehearsal exercises every check a real order faces, so a drill that reaches this
        # line has proven the path rather than bypassed it.
        #
        # It deliberately does NOT mark the idempotency key: a drill is a test, and testing one
        # must not consume the identity of a real order that could legitimately follow.
        if order.drill:
            log_event(log, logging.INFO,
                      "DRILL: order constructed and fully validated, NOT submitted to broker",
                      idempotency_key=order.idempotency_key, instrument=constructed.instrument,
                      side=constructed.side, units=constructed.units,
                      entry_price=constructed.entry_price, sl=constructed.stop_loss,
                      tp=constructed.take_profit, correlation_id=order.correlation_id)
            return {"decision": Decision.DRILL_NOT_SUBMITTED, "order": constructed, "fill": None}

        if self.shadow:
            log_event(log, logging.INFO, "shadow mode: constructed order, NOT submitted",
                      idempotency_key=order.idempotency_key)
            return {"decision": Decision.SHADOW, "order": constructed, "fill": None}

        if submit_fn is None:
            raise ValueError("submit_fn required in non-shadow execution_only mode (wired by EXEC-006)")
        fill = submit_fn(constructed)
        # F-303: the broker now holds this order, so the idempotency marker must land BEFORE
        # anything that can still fail. ``mark()`` used to come AFTER persist/emit, and a
        # persist failure (Fact_Live_Trades write) nacks the message => redelivery => a SECOND
        # broker order for the same approved order. The store autocommits, so nothing the
        # consumer does afterwards can roll the marker back.
        for key in guard_keys(order):
            self.processed.mark(key)
        try:
            if persist_fn is not None:
                persist_fn(constructed, fill)
            if emit_fn is not None:
                emit_fn(constructed, fill)
        except Exception:  # noqa: BLE001 — re-raised; this only makes the state explicit
            # The order IS filled and IS marked: it will never be resubmitted. Say so loudly —
            # the local trade record / fill relay is now behind the broker and needs reconciling.
            log_event(log, logging.ERROR,
                      "order FILLED but a post-fill hook failed; not resubmittable — reconcile",
                      idempotency_key=order.idempotency_key, instrument=order.instrument,
                      units=order.units)
            raise
        log_event(log, logging.INFO, "order executed",
                  idempotency_key=order.idempotency_key, instrument=order.instrument,
                  units=order.units, sl=constructed.stop_loss, tp=constructed.take_profit)
        return {"decision": Decision.EXECUTED, "order": constructed, "fill": fill}
