"""EXEC-008 — Layer-4 safety mode (staleness PAUSE) + audited emergency BYPASS.

In ``execution_only`` the engine trades ONLY on approved orders from ``AMS_Outbound_Queue``.
If System 3 or the queue goes dark, the safe posture is to stop opening **new** risk while
still managing **existing** risk (EXEC-007 keeps running). This module is the state machine
that enforces that:

  RUNNING  — consume the queue, execute approved orders (normal).
  PAUSED   — queue stale > ``staleness_limit_sec`` while in-session. No new orders. Alert.
             Auto-resumes to RUNNING when fresh orders/heartbeats return.
  BYPASS   — operator-only manual override (System 3 down but a human must act): read Layer 3
             directly with *conservative hard-coded sizing*. Opt-in, confirmed, time-bounded,
             loudly audited. Never the default, never auto-enabled.

Freshness is ``now − max(last order/heartbeat, monitor start)`` — startup has grace, and a
System-3 heartbeat distinguishes silence-by-design from an outage. Session-aware: weekend/
out-of-session silence is expected and never false-PAUSEs. Hysteresis prevents flapping on
borderline lag. The core safety invariant: **queue down + BYPASS off ⇒ ``can_submit()`` is
False**, so the engine provably never trades without risk approval.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

from system2.common.logging import get_logger, log_event
from system2.execution.pipeline import is_in_session

log = get_logger("execution.safety_mode")


class SafetyState(str, Enum):
    RUNNING = "running"
    PAUSED = "paused"
    BYPASS = "bypass"


@dataclass
class SafetyConfig:
    staleness_limit_sec: float = 300.0
    hysteresis_sec: float = 30.0  # must be this far under the limit to auto-resume (anti-flap)
    bypass_enable: bool = False
    bypass_confirm_token: str = ""
    bypass_risk_pct: float = 0.25  # conservative fixed risk %, independent of Kelly
    bypass_max_positions: int = 3
    bypass_max_duration_sec: float = 3600.0

    @classmethod
    def from_secrets(cls, secrets: Any) -> "SafetyConfig":
        return cls(
            staleness_limit_sec=float(secrets.get_int("STALENESS_LIMIT_SEC", 300)),
            hysteresis_sec=float(secrets.get_int("STALENESS_HYSTERESIS_SEC", 30)),
            bypass_enable=secrets.get_bool("EXEC_BYPASS_ENABLE", False),
            bypass_confirm_token=secrets.get("BYPASS_CONFIRM_TOKEN", "") or "",
            bypass_risk_pct=float(secrets.get("BYPASS_RISK_PCT", "0.25") or 0.25),
            bypass_max_positions=secrets.get_int("BYPASS_MAX_POSITIONS", 3),
            bypass_max_duration_sec=float(secrets.get_int("BYPASS_MAX_DURATION_SEC", 3600)),
        )


def conservative_position_size(
    account_equity: float, atr: float, *, risk_pct: float, sl_atr_mult: float = 1.0
) -> int:
    """BYPASS sizing: fixed small ``risk_pct`` of equity over the ATR stop distance.

    Intentionally independent of any Kelly logic (System 3's job). Returns floored units;
    0 if inputs are non-positive (fail-safe: size nothing rather than guess).
    """
    if account_equity <= 0 or atr <= 0 or risk_pct <= 0:
        return 0
    risk_amount = account_equity * (risk_pct / 100.0)
    stop_distance = atr * sl_atr_mult
    if stop_distance <= 0:
        return 0
    return int(risk_amount / stop_distance)


@dataclass
class SafetyMonitor:
    """Evaluates staleness and holds the current safety state + BYPASS session."""

    config: SafetyConfig = field(default_factory=SafetyConfig)
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))
    session_fn: Callable[[datetime], bool] = is_in_session
    alert_fn: Callable[..., None] | None = None
    audit_fn: Callable[[dict[str, Any]], None] | None = None

    state: SafetyState = SafetyState.RUNNING
    _last_heartbeat_at: datetime | None = None
    _started_at: datetime | None = None
    _bypass_started_at: datetime | None = None
    _bypass_operator: str | None = None

    def __post_init__(self) -> None:
        if self._started_at is None:
            self._started_at = self.clock()

    # ----- freshness inputs -------------------------------------------------
    def record_heartbeat(self, at: datetime | None = None) -> None:
        """System 3 heartbeat / any well-formed message bumps freshness (silence-by-design)."""
        self._last_heartbeat_at = (at or self.clock()).astimezone(timezone.utc)

    def _fresh_at(self, last_message_at: datetime | None) -> datetime:
        # Real signal = the most recent order/heartbeat. Only when NOTHING has arrived yet do
        # we fall back to the monitor start time (startup grace, which decays naturally).
        marks: list[datetime] = []
        if last_message_at is not None:
            marks.append(last_message_at.astimezone(timezone.utc))
        if self._last_heartbeat_at is not None:
            marks.append(self._last_heartbeat_at)
        if marks:
            return max(marks)
        assert self._started_at is not None
        return self._started_at

    def staleness_seconds(self, now: datetime, last_message_at: datetime | None) -> float:
        return (now.astimezone(timezone.utc) - self._fresh_at(last_message_at)).total_seconds()

    # ----- state evaluation -------------------------------------------------
    def evaluate(self, last_message_at: datetime | None, now: datetime | None = None) -> SafetyState:
        """Recompute the safety state from queue freshness. Call before each submit decision."""
        now = (now or self.clock()).astimezone(timezone.utc)

        # BYPASS is manual + time-bounded: auto-revert on expiry, else stays BYPASS.
        if self.state is SafetyState.BYPASS:
            assert self._bypass_started_at is not None
            if (now - self._bypass_started_at).total_seconds() >= self.config.bypass_max_duration_sec:
                self._exit_bypass("bypass_window_expired", now)
            else:
                return self.state

        # Out-of-session silence is expected — never false-PAUSE on weekends.
        if not self.session_fn(now):
            if self.state is SafetyState.PAUSED:
                self._transition(SafetyState.RUNNING, "out_of_session_no_false_pause", now)
            return self.state

        staleness = self.staleness_seconds(now, last_message_at)
        limit = self.config.staleness_limit_sec
        if self.state is SafetyState.RUNNING and staleness > limit:
            self._transition(SafetyState.PAUSED, "queue_stale", now, staleness_sec=round(staleness, 1))
        elif self.state is SafetyState.PAUSED and staleness < (limit - self.config.hysteresis_sec):
            self._transition(SafetyState.RUNNING, "queue_fresh_resume", now, staleness_sec=round(staleness, 1))
        return self.state

    def can_submit(self) -> bool:
        """New orders may be submitted only in RUNNING or BYPASS — never PAUSED."""
        return self.state in (SafetyState.RUNNING, SafetyState.BYPASS)

    # ----- BYPASS control ---------------------------------------------------
    def request_bypass(self, token: str, operator: str, reason: str) -> bool:
        """Enable BYPASS. Requires the enable flag AND a matching non-empty confirm token."""
        if not self.config.bypass_enable:
            log_event(log, logging.WARNING, "BYPASS refused: not enabled", operator=operator)
            return False
        expected = self.config.bypass_confirm_token
        if not expected or token != expected:
            log_event(log, logging.WARNING, "BYPASS refused: bad/absent confirm token", operator=operator)
            return False
        now = self.clock().astimezone(timezone.utc)
        self._bypass_started_at = now
        self._bypass_operator = operator
        self._transition(SafetyState.BYPASS, f"bypass_enabled:{reason}", now, operator=operator, source="BYPASS")
        return True

    def exit_bypass(self, reason: str = "operator_disabled") -> None:
        if self.state is SafetyState.BYPASS:
            self._exit_bypass(reason, self.clock().astimezone(timezone.utc))

    def _exit_bypass(self, reason: str, now: datetime) -> None:
        operator = self._bypass_operator
        self._bypass_started_at = None
        self._bypass_operator = None
        self._transition(SafetyState.RUNNING, f"bypass_exit:{reason}", now, operator=operator, source="BYPASS")

    # ----- transition plumbing (alert + audit) ------------------------------
    def _transition(self, new: SafetyState, reason: str, now: datetime, **ctx: Any) -> None:
        old = self.state
        if old is new and not reason.startswith("bypass"):
            return
        self.state = new
        log_event(log, logging.WARNING, "safety state transition",
                  from_state=old.value, to_state=new.value, reason=reason, **ctx)
        if self.alert_fn is not None:
            self.alert_fn(event="safety_transition", from_state=old.value, to_state=new.value,
                          reason=reason, **ctx)
        if self.audit_fn is not None:
            self.audit_fn({
                "event": "safety_transition",
                "from_state": old.value,
                "to_state": new.value,
                "reason": reason,
                "ts_utc": now.isoformat().replace("+00:00", "Z"),
                **ctx,
            })
