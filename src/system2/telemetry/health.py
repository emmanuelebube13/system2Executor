"""EXEC-009 — System 2's own read-only health / telemetry surface (Layer 5, the Hand's).

Scope note (SYSTEM_BOUNDARY.md): System 2 exposes **its own** structured health/heartbeat and
*mirrors* System-3 breaker state read-only — it does NOT own or serve System 3's account store.
The EXEC-009 spec's account endpoints (equity-curve, decisions, circuit-breakers,
strategy-performance, daily-summary) read AMS-009 views on System 3 and are that system's own
Layer 5; serving them here would require a Computer-1/System-3 DB dependency, which the portable
Computer-2 service must not have (D-001). So this module surfaces what the Hand actually knows:
execution mode (RUNNING/PAUSED/BYPASS, EXEC-008), queue lag/staleness (EXEC-004), the open
positions it manages (EXEC-007), fill-outbox depth (EXEC-005), the active model set (EXEC-001),
and the broker environment (EXEC-006) — all read-only, never leaking secrets.

``HealthReporter`` is pure (returns dicts from injected providers) so it is unit-tested without
an HTTP server; ``server.py`` mounts it behind FastAPI (lazy import).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

SCHEMA_VERSION = "1"


def _utc_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class HealthReporter:
    """Aggregates System-2 live state into read-only status payloads.

    Every field comes from an injected provider so the reporter stays decoupled from the
    consumer/monitor/adapter and is trivially testable. All providers are optional; a missing
    one degrades that field to ``None``/``unknown`` rather than failing the whole endpoint.
    """

    safety_state_fn: Callable[[], str] | None = None
    staleness_fn: Callable[[], float | None] | None = None
    message_staleness_fn: Callable[[], float | None] | None = None
    heartbeat_staleness_fn: Callable[[], float | None] | None = None
    staleness_limit_fn: Callable[[], float | None] | None = None
    last_message_at_fn: Callable[[], datetime | None] | None = None
    messages_seen_fn: Callable[[], int] | None = None
    duplicates_suppressed_fn: Callable[[], int | None] | None = None
    open_positions_fn: Callable[[], list[dict[str, Any]]] | None = None
    outbox_depth_fn: Callable[[], int] | None = None
    model_set_id_fn: Callable[[], str | None] | None = None
    broker_env_fn: Callable[[], str] | None = None
    account_summary_fn: Callable[[], dict[str, Any]] | None = None
    regime_grid_fn: Callable[[], list[dict[str, Any]]] | None = None
    # FIX_PLAN 2.1(d) / F-602 — the live gatekeeper approval rate against the band the
    # champion manifest declares. Exposed here for the same reason as
    # `queue.staleness_limit_sec` (F-304): the EFFECTIVE safety posture this process is
    # running with has to be auditable from the outside, without shell access to the
    # box. The live gate drifted to a 0.9995 approval rate for weeks because the only
    # published number was a mean score, which has no band to be judged against.
    gatekeeper_approval_fn: Callable[[], dict[str, Any] | None] | None = None
    # F-309 / OD-5 — the EFFECTIVE `EXEC_SHADOW` this process resolved at startup: True =
    # orders are constructed and validated but NOT submitted to the broker, False = they
    # are sent for real. Exposed for exactly the reason `queue.staleness_limit_sec` is
    # (F-304): the deployed safety posture must be auditable from outside the box. It was
    # on no HTTP surface at all, while its two code defaults disagreed — so the deployed
    # reality was unreadable from anywhere. `None` means the provider is unwired and the
    # payload renders "unknown", never a reassuring guess.
    #
    # NOTE: `exec_mode` and `exec_shadow` are ORTHOGONAL. A PAUSED engine is neither
    # shadow nor live; inferring one from the other is a category error.
    shadow_fn: Callable[[], bool | None] | None = None
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))
    _started_at: datetime | None = None

    def __post_init__(self) -> None:
        if self._started_at is None:
            self._started_at = self.clock()

    # ----- small safe accessors (never raise; degrade instead) --------------
    @staticmethod
    def _safe(fn: Callable[[], Any] | None, default: Any = None) -> Any:
        if fn is None:
            return default
        try:
            return fn()
        except Exception:
            return default

    def _uptime_sec(self) -> float:
        assert self._started_at is not None
        return round((self.clock().astimezone(timezone.utc) - self._started_at).total_seconds(), 1)

    def _exec_shadow(self) -> bool | str:
        """The resolved EXEC_SHADOW, or the string "unknown" if nothing is wired.

        Deliberately NOT defaulted to True or False: a wrong boolean here would read as a
        confident answer about whether orders reach a real broker. "unknown" is the honest
        rendering of an unwired provider.
        """
        value = self._safe(self.shadow_fn)
        return "unknown" if value is None else bool(value)

    # ----- payloads ---------------------------------------------------------
    def health(self) -> dict[str, Any]:
        """Liveness probe — cheap, always 200 when the process is up."""
        return {
            "status": "ok",
            "schema_version": SCHEMA_VERSION,
            "uptime_sec": self._uptime_sec(),
            "as_of": _utc_iso(self.clock()),
        }

    def account_state(self) -> dict[str, Any]:
        """System-2 slice of ``/api/account/state`` — the parts the Hand actually owns.

        Balance/equity/margin are System-3-owned and intentionally omitted (not a Computer-2
        concern); the operator gets execution mode, queue staleness, and open-position count.
        """
        positions = self._safe(self.open_positions_fn, []) or []
        summary = self._safe(self.account_summary_fn, {}) or {}
        return {
            "schema_version": SCHEMA_VERSION,
            "exec_mode": (self._safe(self.safety_state_fn, "unknown") or "unknown").upper(),
            "exec_shadow": self._exec_shadow(),
            "queue_staleness_sec": self._safe(self.staleness_fn),
            "queue_staleness_limit_sec": self._safe(self.staleness_limit_fn),
            "open_positions": len(positions),
            "broker_env": self._safe(self.broker_env_fn, "unknown"),
            "model_set_id": self._safe(self.model_set_id_fn),
            "balance": float(summary.get("balance", 0)),
            "equity": float(summary.get("NAV", 0)),
            "margin_used": float(summary.get("marginUsed", 0)),
            "margin_available": float(summary.get("marginAvailable", 0)),
            "account_summary": summary,
            "as_of": _utc_iso(self.clock()),
        }

    def status(self) -> dict[str, Any]:
        """Full System-2 status snapshot for the operator dashboard."""
        return {
            "schema_version": SCHEMA_VERSION,
            "service": "system-2-execution-engine",
            "uptime_sec": self._uptime_sec(),
            "exec_mode": (self._safe(self.safety_state_fn, "unknown") or "unknown").upper(),
            # The effective EXEC_SHADOW (F-309). Orthogonal to exec_mode above.
            "exec_shadow": self._exec_shadow(),
            "queue": {
                "staleness_sec": self._safe(self.staleness_fn),
                "heartbeat_staleness_sec": self._safe(self.heartbeat_staleness_fn),
                "message_staleness_sec": self._safe(self.message_staleness_fn),
                # The EFFECTIVE limit this process is running with. Exposed so the deployed
                # safety posture can be audited without shell access to the box (F-304).
                "staleness_limit_sec": self._safe(self.staleness_limit_fn),
                "last_message_at": _utc_iso(self._safe(self.last_message_at_fn)),
                "messages_seen": self._safe(self.messages_seen_fn, 0),
                "duplicates_suppressed": self._safe(self.duplicates_suppressed_fn, 0),
                "process_started_at": _utc_iso(self._started_at),
            },
            "outbox_depth": self._safe(self.outbox_depth_fn, 0),
            "open_positions": self._safe(self.open_positions_fn, []) or [],
            "model_set_id": self._safe(self.model_set_id_fn),
            "broker_env": self._safe(self.broker_env_fn, "unknown"),
            "account_summary": self._safe(self.account_summary_fn, {}),
            "regime": self._regime_summary(),
            "gatekeeper": self._gatekeeper_summary(),
            "as_of": _utc_iso(self.clock()),
        }

    # ----- gatekeeper approval-rate band (FIX_PLAN 2.1(d)) -------------------
    def _gatekeeper_summary(self) -> dict[str, Any]:
        """Compact roll-up of the runtime approval-rate monitor for the status tile.

        Degrades to ``state: "unavailable"`` when no provider is wired — an honest
        "not measured" rather than a reassuring absence. The full detail (window,
        counts, confidence interval) stays on GET /signal, which is also the payload
        ``bridge/ops_watchdog.py`` already polls.
        """
        snap = self._safe(self.gatekeeper_approval_fn)
        if not isinstance(snap, dict):
            return {"state": "unavailable", "alarm": False,
                    "reason": "no approval-rate provider wired"}
        return {
            "state": snap.get("state", "unknown"),
            "alarm": bool(snap.get("alarm", False)),
            "reason": snap.get("reason"),
            "approval_rate": snap.get("approval_rate"),
            "band": snap.get("band"),
            "evaluations": snap.get("evaluations"),
            "window": snap.get("window"),
            "enforcing": snap.get("enforcing"),
        }

    # ----- regime (EXEC-002): live market regime per instrument x granularity -----
    def _regime_summary(self) -> dict[str, Any]:
        """Compact roll-up for the overview tile: cell count + the dominant live label."""
        grid = self._safe(self.regime_grid_fn, []) or []
        counts: dict[str, int] = {}
        for cell in grid:
            if cell.get("stale"):
                continue
            label = cell.get("smoothed_label")
            if label:
                counts[label] = counts.get(label, 0) + 1
        dominant = max(counts, key=counts.get) if counts else None
        return {"cells": len(grid), "dominant": dominant, "by_label": counts}

    def regime(self) -> dict[str, Any]:
        """Full regime grid — the pair x timeframe matrix the dashboard renders."""
        return {
            "schema_version": SCHEMA_VERSION,
            "service": "system-2-execution-engine",
            "grid": self._safe(self.regime_grid_fn, []) or [],
            "as_of": _utc_iso(self.clock()),
        }

def check_signal_divergence(
    s1_published_total_now: int,
    s1_published_total_prev: int,
    messages_seen_now: int,
    messages_seen_prev: int,
    process_started_at_now: str | None = None,
    process_started_at_prev: str | None = None,
) -> str:
    """Did System 1 publish signals that System 2 never saw?

    Returns one of:
      ``"divergent"``     -- S1 published, S2 saw nothing. A real gap.
      ``"ok"``            -- either nothing was published, or what was published arrived.
      ``"indeterminate"`` -- the two observations cannot be compared.

    Silence is only a fault when something was supposed to arrive, so this
    cross-references System 1's published counter. Both numbers already reach the
    same bucket, so no new transport is needed.

    THE RESTART TRAP, and why this returns three states rather than a bool.
    ``messages_seen`` lives on an in-memory ``ConsumerLag`` and resets to 0 on every
    process start, while S1's ``signals_published_total`` is cumulative and persists.
    Differencing them naively across a restart is not a comparison of like with like:

        prev s1=5  now=6   |  prev s2=40  now=10   (S2 restarted, consumed all 10)
        s2_delta = max(0, 10 - 40) = 0             -> reads as "saw nothing"

    That fires an alarm at the exact moment flow is healthy. Clamping the delta at
    zero hides the restart instead of revealing it. When the process boundary moved,
    the honest answer is that we cannot tell -- and "cannot tell" must not be folded
    into "fine", which is the same rule the rest of this system follows for absent
    values. Persisting ``messages_seen`` across restarts would remove the ambiguity
    and is the better long-term fix; until then, say so rather than guess.
    """
    if process_started_at_now != process_started_at_prev:
        # The process restarted between observations: messages_seen_prev belongs to a
        # counter that no longer exists. Re-baseline on the next observation.
        return "indeterminate"

    if messages_seen_now < messages_seen_prev:
        # Counter went backwards without the start time changing -- it cannot be
        # trusted either way. Never silently clamp this to zero.
        return "indeterminate"

    s1_delta = s1_published_total_now - s1_published_total_prev
    if s1_delta <= 0:
        return "ok"

    return "divergent" if (messages_seen_now - messages_seen_prev) == 0 else "ok"
