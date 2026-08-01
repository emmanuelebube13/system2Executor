"""EXEC-004 message validation + the last-line (§7.2) pre-submit sanity gate.

Two layers, deliberately separate because they have different failure handling:

1. **Envelope/message validation** (``validate_envelope`` + ``is_expired``) — structural
   checks on a raw ``AMS_Outbound_Queue`` message *before* it becomes an order:
   schema_version, required fields, side/granularity vocab, ``units != 0``, well-formed
   ``risk_context``. A *malformed* message is poison → dead-letter (EXEC-004 §7.3). An
   *expired* message is well-formed but stale → ack-and-drop (never submit a stale order).

2. **Last-line order validation** (``OrderValidator``) — the §7.2 belt-and-braces gate on
   the *constructed* broker order, the final check before it leaves the box even though
   System 3 already sized and approved it: a stop-loss must be present, per-pair units and
   total notional caps, a leverage ceiling, a tradeable-instrument allowlist, and the
   trading-session window. This is a fail-closed backstop against a System-3 fault or a
   corrupted message — it never *re-sizes* or *re-decides*, it only refuses to submit
   something that violates a hard safety limit. See docs/SYSTEM_BOUNDARY.md.
"""

from __future__ import annotations

import math

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from system2.execution.pipeline import ConstructedOrder, is_in_session

# Message vocab (the agreed ApprovedOrder envelope — EXEC-004 §7.1).
SUPPORTED_SCHEMA_VERSIONS = {"1"}
VALID_GRANULARITIES = {"H1", "H4"}
VALID_SIDES = {"BUY", "SELL"}
REQUIRED_ORDER_FIELDS = (
    "schema_version",
    "message_id",
    "idempotency_key",
    "correlation_id",
    "instrument",
    "side",
    "units",
    "granularity",
    "risk_context",
)


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of a validation step. ``code`` lets callers route (DLQ vs drop vs reject)."""

    ok: bool
    code: str = ""
    reason: str = ""

    @classmethod
    def passed(cls) -> "ValidationResult":
        return cls(ok=True)

    @classmethod
    def failed(cls, code: str, reason: str) -> "ValidationResult":
        return cls(ok=False, code=code, reason=reason)


# --------------------------------------------------------------------------- #
# Layer 1 — envelope / message validation (malformed -> dead-letter)
# --------------------------------------------------------------------------- #
def validate_envelope(raw: Any) -> ValidationResult:
    """Structural validity of a raw ``AMS_Outbound_Queue`` message. Failures -> DLQ."""
    if not isinstance(raw, dict):
        return ValidationResult.failed("not_a_dict", f"message is {type(raw).__name__}, expected object")

    sv = str(raw.get("schema_version", ""))
    if sv not in SUPPORTED_SCHEMA_VERSIONS:
        return ValidationResult.failed("schema_version", f"unsupported schema_version {sv!r}")

    missing = [f for f in REQUIRED_ORDER_FIELDS if raw.get(f) is None]
    if missing:
        return ValidationResult.failed("missing_field", f"missing required field(s): {', '.join(missing)}")

    side = str(raw.get("side", "")).upper()
    if side not in VALID_SIDES:
        return ValidationResult.failed("side", f"invalid side {raw.get('side')!r}")

    gran = str(raw.get("granularity", ""))
    if gran not in VALID_GRANULARITIES:
        return ValidationResult.failed("granularity", f"invalid granularity {gran!r}")

    try:
        units = float(raw["units"])
    except (TypeError, ValueError):
        return ValidationResult.failed("units", f"units not numeric: {raw.get('units')!r}")
    if not math.isfinite(units):
        return ValidationResult.failed("units", f"units is not a finite number: {units}")
    if units == 0:
        return ValidationResult.failed("zero_units", "units == 0")

    rc = raw.get("risk_context")
    if not isinstance(rc, dict):
        return ValidationResult.failed("risk_context", "risk_context is not an object")
    try:
        atr = float(rc.get("atr"))
    except (TypeError, ValueError):
        return ValidationResult.failed("risk_context", f"risk_context.atr not numeric: {rc.get('atr')!r}")
    # NaN/Inf must be rejected BEFORE the bound check: every comparison against NaN is False,
    # so `atr <= 0` passes NaN, and +Inf passes it too. The full-loop chaos matrix
    # (audit/loop/test_f2_chaos_matrix.py) showed a non-finite ATR entering at THIS boundary
    # was caught nowhere and reached the broker as a real order. System 3 rejects non-finite
    # values at its own contract validator (F-203), but System 2 is the process that talks to
    # the broker and must not depend on an upstream check it cannot see.
    if not math.isfinite(atr):
        return ValidationResult.failed("risk_context", f"risk_context.atr is not a finite number: {atr}")
    if atr <= 0:
        return ValidationResult.failed("risk_context", f"risk_context.atr must be > 0, got {atr}")

    return ValidationResult.passed()


def _parse_iso(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (TypeError, ValueError, AttributeError):
        return None


def is_expired(raw: dict[str, Any], now: datetime, max_age_sec: int | None = None) -> bool:
    """True if the order is past its TTL. ``expires_at`` wins; else ``created_at`` age.

    A message with neither a parseable ``expires_at`` nor a ``created_at`` is treated as
    *not* expired (it will still be subject to the envelope/last-line gates).
    """
    now = now.astimezone(timezone.utc)
    exp = _parse_iso(raw["expires_at"]) if raw.get("expires_at") else None
    if exp is not None:
        return now >= exp.astimezone(timezone.utc)
    if max_age_sec is not None and raw.get("created_at"):
        created = _parse_iso(raw["created_at"])
        if created is not None:
            age = (now - created.astimezone(timezone.utc)).total_seconds()
            return age > max_age_sec
    return False


# --------------------------------------------------------------------------- #
# Layer 2 — last-line (§7.2) pre-submit sanity gate on the constructed order
# --------------------------------------------------------------------------- #
@dataclass
class OrderValidator:
    """Final fail-closed sanity gate before a constructed order is submitted.

    Limits are *loose hard ceilings* set above normal AMS sizing: they exist to catch a
    fault (a runaway units value, a missing stop, an untradeable symbol), not to re-decide
    risk. ``tradeable_instruments=None`` means "allow any" (no allowlist configured).
    """

    require_stop_loss: bool = True
    max_units_per_pair: float = 1_000_000.0
    max_total_notional: float = 5_000_000.0
    max_leverage: float = 30.0
    tradeable_instruments: frozenset[str] | None = None
    clock: Callable[[], datetime] | None = None

    def _now(self) -> datetime:
        return (self.clock or (lambda: datetime.now(timezone.utc)))()

    def validate(
        self,
        order: ConstructedOrder,
        *,
        account_equity: float | None = None,
        open_notional: float = 0.0,
    ) -> ValidationResult:
        if self.require_stop_loss and (order.stop_loss is None or order.stop_loss == 0):
            return ValidationResult.failed("no_stop_loss", "stop-loss required but missing")

        if (
            self.tradeable_instruments is not None
            and order.instrument not in self.tradeable_instruments
        ):
            return ValidationResult.failed("untradeable", f"{order.instrument} not in tradeable allowlist")

        units = abs(order.units)
        if units > self.max_units_per_pair:
            return ValidationResult.failed(
                "max_units_per_pair", f"|units| {units} > cap {self.max_units_per_pair}"
            )

        notional = units * order.entry_price
        if notional + open_notional > self.max_total_notional:
            return ValidationResult.failed(
                "max_total_notional",
                f"notional {notional + open_notional:.2f} > cap {self.max_total_notional}",
            )

        if account_equity and account_equity > 0:
            leverage = (notional + open_notional) / account_equity
            if leverage > self.max_leverage:
                return ValidationResult.failed(
                    "max_leverage", f"leverage {leverage:.2f}x > cap {self.max_leverage}x"
                )

        if not is_in_session(self._now()):
            return ValidationResult.failed("out_of_session", "outside trading session window")

        return ValidationResult.passed()

    @classmethod
    def from_secrets(cls, secrets: Any, clock: Callable[[], datetime] | None = None) -> "OrderValidator":
        allow = secrets.get("TRADEABLE_INSTRUMENTS")
        instruments = (
            frozenset(s.strip() for s in allow.split(",") if s.strip()) if allow else None
        )
        return cls(
            require_stop_loss=secrets.get_bool("REQUIRE_STOP_LOSS", True),
            max_units_per_pair=float(secrets.get_int("MAX_UNITS_PER_PAIR", 1_000_000)),
            max_total_notional=float(secrets.get_int("MAX_TOTAL_NOTIONAL", 5_000_000)),
            max_leverage=float(secrets.get_int("MAX_LEVERAGE", 30)),
            tradeable_instruments=instruments,
            clock=clock,
        )
