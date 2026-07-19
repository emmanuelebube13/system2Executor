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
    last_message_at_fn: Callable[[], datetime | None] | None = None
    messages_seen_fn: Callable[[], int] | None = None
    open_positions_fn: Callable[[], list[dict[str, Any]]] | None = None
    outbox_depth_fn: Callable[[], int] | None = None
    model_set_id_fn: Callable[[], str | None] | None = None
    broker_env_fn: Callable[[], str] | None = None
    account_summary_fn: Callable[[], dict[str, Any]] | None = None
    regime_grid_fn: Callable[[], list[dict[str, Any]]] | None = None
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
            "queue_staleness_sec": self._safe(self.staleness_fn),
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
            "queue": {
                "staleness_sec": self._safe(self.staleness_fn),
                "last_message_at": _utc_iso(self._safe(self.last_message_at_fn)),
                "messages_seen": self._safe(self.messages_seen_fn, 0),
            },
            "outbox_depth": self._safe(self.outbox_depth_fn, 0),
            "open_positions": self._safe(self.open_positions_fn, []) or [],
            "model_set_id": self._safe(self.model_set_id_fn),
            "broker_env": self._safe(self.broker_env_fn, "unknown"),
            "account_summary": self._safe(self.account_summary_fn, {}),
            "regime": self._regime_summary(),
            "as_of": _utc_iso(self.clock()),
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
