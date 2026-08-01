"""FIX_PLAN 2.1(d) — RUNTIME gatekeeper approval-rate monitor (F-602).

Why this file exists
--------------------
System 1 trains the gatekeeper under a turnover band — the champion manifest itself
ships ``"turnover_band": [0.05, 0.6]`` alongside ``"oos_approval_rate": 0.3379`` — but
that band was only ever *enforced at training time*. Nothing on the live side ever
measured what the deployed model actually approves. It drifted to a measured
**1893/1894 = 0.9995** approval rate and ran that way for weeks with no alert: an ML
gate that gates nothing, and the most likely locus of the 14-trade / 1-win loss.

This module is the missing half: the same band, evaluated continuously against the
live verdict stream, published on the telemetry surface, and alarmed on.

Design decisions (each one is a real trade-off; the reasoning is recorded here because
a monitor whose thresholds are unexplained is a monitor nobody will trust enough to act
on):

**Window — a rolling count of the last N *evaluations*, not a wall-clock window.**
Evaluation cadence is wildly non-uniform: 8 pairs x H1 x the strategies the regime
qualifies gives ~dozens/day when markets are open, exactly zero over a weekend, and
zero while the account sits CIRCUIT_BROKEN. A "last 24h" rate therefore has an
undefined denominator half the time and would either divide by zero or report a
fabricated 0%. Approval rate is a *per-evaluation* property, so counting evaluations
keeps the statistical power of the window constant no matter what the market is doing.
The cost is honest and accepted: in a quiet market N evaluations can span days, so
detection latency is measured in signals rather than in hours — which is the correct
unit, because a gate that has not been asked anything has not yet misbehaved.
All-time figures ride along in the snapshot so a recent window can never *hide* a
long-standing drift.

**N = 200, with no alarm below 50.** A rate over all of history smears a fresh
regression into months of good behaviour; a rate over 10 signals is coin-flip noise
that would cry wolf and get muted. 200 sits between: it is a couple of days of live
signalling, and the 99% Wilson half-width at the 0.60 band edge is ~0.09, so the alarm
needs an observed rate above ~0.69 before it fires — comfortably clear of the edge,
never a boundary quibble.

**Alarm on the confidence interval, not the point estimate.** We fire only when the
*entire* Wilson 99% interval sits outside the declared band. Small-sample noise cannot
trip it; a genuine 0.9995 trips it almost immediately (at the 50-sample floor with
every sample approved the 99% lower bound is already ~0.88, far above 0.60). Applied to
the real incident: this monitor would have alarmed within the first ~50 live
evaluations — hours to a day after the model went live, instead of never.

**Fail-LOUD by default, fail-closed only when explicitly armed.** See
``ApprovalRateMonitor.enforcing`` and the note in ``producer.py``. An out-of-band
approval *rate* is a statement about a population of past decisions, not about the
decision in front of us, and the lower edge of the band means the gate is too *strict*
— "you are trading too little" cannot sanely be answered with "then trade nothing". A
second, unreviewable kill switch inside the Hand would also duplicate and could fight
the risk authority that already owns stopping trade (System 3's state machine and the
EmergencyStop sentinel). And fail-closing would not have prevented this incident
anyway: the defect survived for weeks because nothing was *looking*, not because
nothing could stop it. So the default is alarm-only, and
``SIGNAL_APPROVAL_BAND_ENFORCE=true`` promotes it to blocking once the alert has earned
its track record — a config flip, not a code change.

Pure and dependency-free (stdlib + the logging helper) so it is unit-testable without a
producer, a queue, or a model.
"""

from __future__ import annotations

import logging
import math
import threading
from collections import deque
from typing import Any, Iterable

from system2.common.logging import get_logger, log_event

log = get_logger("live_signal_producer.approval_monitor")

# The band System 1 trains under. Only a fallback: the live band is read from the
# champion manifest's ``turnover_band`` so this file never becomes a second, divergent
# opinion about what the band is. Thresholds/bands are manifest content (System 1's).
DEFAULT_TURNOVER_BAND: tuple[float, float] = (0.05, 0.60)

DEFAULT_WINDOW = 200        # evaluations in the rolling window
DEFAULT_MIN_SAMPLES = 50    # no verdict below this — see module docstring
REMINDER_SEC = 6 * 3600     # re-alert a persistent episode this often (ops_watchdog shape)

# Wilson interval z for a two-sided 99% CI. 99 rather than 95 because this alarm is
# meant to be believed: we would rather be slow than noisy.
Z_99 = 2.5758293035489004

STATE_UNKNOWN = "unknown"
STATE_WARMING_UP = "warming_up"
STATE_OK = "ok"
STATE_OUT_OF_BAND = "out_of_band"


def wilson_interval(approved: int, total: int, z: float = Z_99) -> tuple[float, float]:
    """Two-sided Wilson score interval for a binomial rate.

    Chosen over the normal (Wald) approximation because Wald degenerates to a
    zero-width interval exactly at p=0 and p=1 — which is precisely where this monitor
    lives (the live rate is 0.9995). Wald would have reported [1.0, 1.0] and made the
    "is it outside the band" question meaningless.
    """
    if total <= 0:
        return (0.0, 1.0)
    p = approved / total
    denom = 1.0 + z * z / total
    center = (p + z * z / (2 * total)) / denom
    half = (z / denom) * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total))
    return (max(0.0, center - half), min(1.0, center + half))


class ApprovalRateMonitor:
    """Rolling approval-rate measurement against the manifest's turnover band.

    Thread-safe: the sweep thread calls :meth:`observe`, the health thread calls
    :meth:`snapshot`. Never raises — a monitor that can break the thing it monitors is
    worse than no monitor.
    """

    def __init__(
        self,
        band: tuple[float, float] | list[float] | None = None,
        window: int = DEFAULT_WINDOW,
        min_samples: int = DEFAULT_MIN_SAMPLES,
        enforcing: bool = False,
    ) -> None:
        self._lock = threading.Lock()
        self._window = max(1, int(window))
        self._min_samples = max(1, int(min_samples))
        self._recent: deque[bool] = deque(maxlen=self._window)
        self._lifetime_total = 0
        self._lifetime_approved = 0
        self._seeded = False
        self.enforcing = bool(enforcing)
        self._band = DEFAULT_TURNOVER_BAND
        self._band_source = "default"
        # Episode bookkeeping so a standing alarm logs once + a reminder, never per bar.
        self._episode_since: float | None = None
        self._last_alert_ts: float | None = None
        if band is not None:
            self.set_band(band, source="explicit")

    # ----- configuration ------------------------------------------------------
    def set_band(self, band: Iterable[float] | None, source: str = "manifest") -> None:
        """Adopt the band declared by the champion manifest (``turnover_band``).

        Called on every model (re)load so a retrain that ships a different band is
        honoured without a redeploy. A malformed band is ignored (keeps the previous
        one) rather than silently disabling the monitor.
        """
        try:
            lo, hi = (float(x) for x in list(band or [])[:2])
        except (TypeError, ValueError):
            return
        if not (0.0 <= lo < hi <= 1.0):
            log_event(log, logging.WARNING, "ignoring implausible turnover band",
                      band=[lo, hi], source=source, keeping=list(self._band))
            return
        with self._lock:
            changed = (lo, hi) != self._band
            self._band = (lo, hi)
            self._band_source = source
        if changed:
            log_event(log, logging.INFO, "approval-rate band adopted",
                      band=[lo, hi], source=source)

    @property
    def band(self) -> tuple[float, float]:
        return self._band

    # ----- measurement --------------------------------------------------------
    def seed(self, verdicts: Iterable[bool] | None) -> int:
        """Prime the window from the persistent ledger at startup.

        Without this a restart resets the measurement to zero, and "restarts often
        enough" would be a way for a drifted model to stay permanently below the
        min-sample floor and never be judged. Returns how many verdicts were adopted.
        """
        rows = [bool(v) for v in (verdicts or [])]
        if not rows:
            return 0
        with self._lock:
            self._recent.extend(rows[-self._window:])
            self._lifetime_total += len(rows)
            self._lifetime_approved += sum(rows)
            self._seeded = True
        return len(rows)

    def observe(self, approved: bool) -> None:
        """Record one gatekeeper verdict (approved / rejected). Never raises."""
        with self._lock:
            self._recent.append(bool(approved))
            self._lifetime_total += 1
            self._lifetime_approved += 1 if approved else 0

    # ----- verdict ------------------------------------------------------------
    def _evaluate_locked(self) -> dict[str, Any]:
        n = len(self._recent)
        k = sum(self._recent)
        lo_band, hi_band = self._band
        rate = (k / n) if n else None
        ci_lo, ci_hi = wilson_interval(k, n)
        if n == 0:
            state, reason = STATE_UNKNOWN, "no evaluations observed yet"
        elif n < self._min_samples:
            state = STATE_WARMING_UP
            reason = f"{n}/{self._min_samples} evaluations — below the alarm floor"
        elif ci_lo > hi_band:
            state = STATE_OUT_OF_BAND
            reason = (
                f"approval rate {rate:.4f} over the last {n} evaluations "
                f"(99% CI [{ci_lo:.4f}, {ci_hi:.4f}]) is ABOVE the declared turnover "
                f"band [{lo_band:g}, {hi_band:g}] — the gate is approving almost "
                f"everything it is asked about and is not gating."
            )
        elif ci_hi < lo_band:
            state = STATE_OUT_OF_BAND
            reason = (
                f"approval rate {rate:.4f} over the last {n} evaluations "
                f"(99% CI [{ci_lo:.4f}, {ci_hi:.4f}]) is BELOW the declared turnover "
                f"band [{lo_band:g}, {hi_band:g}] — the gate is refusing almost "
                f"everything and the strategy is effectively switched off."
            )
        else:
            state, reason = STATE_OK, None
        return {
            "schema_version": "1",
            "state": state,
            "alarm": state == STATE_OUT_OF_BAND,
            "reason": reason,
            "band": [lo_band, hi_band],
            "band_source": self._band_source,
            "window": self._window,
            "min_samples": self._min_samples,
            "evaluations": n,
            "approved": k,
            "approval_rate": round(rate, 6) if rate is not None else None,
            "ci99": [round(ci_lo, 6), round(ci_hi, 6)],
            "lifetime_evaluations": self._lifetime_total,
            "lifetime_approval_rate": (
                round(self._lifetime_approved / self._lifetime_total, 6)
                if self._lifetime_total else None
            ),
            "seeded_from_ledger": self._seeded,
            "enforcing": self.enforcing,
        }

    def snapshot(self) -> dict[str, Any]:
        """The read-only payload published on ``/signal`` and ``/status``."""
        with self._lock:
            return self._evaluate_locked()

    @property
    def alarming(self) -> bool:
        with self._lock:
            return self._evaluate_locked()["alarm"]

    def should_block(self) -> bool:
        """True only when the band is breached AND enforcement was explicitly armed."""
        return self.enforcing and self.alarming

    # ----- alerting -----------------------------------------------------------
    def poll_alert(self, now_ts: float) -> dict[str, Any] | None:
        """Episode-throttled alert edge, mirroring ``bridge/ops_watchdog.py`` semantics.

        Returns the snapshot exactly once when the alarm opens, again every
        ``REMINDER_SEC`` while it persists, and once (``resolved``) when it clears —
        never once per evaluation. An alert that fires on every bar is an alert that
        gets muted, and a muted alert is how this defect survived in the first place.
        """
        snap = self.snapshot()
        if snap["alarm"]:
            if self._episode_since is None:
                self._episode_since = now_ts
                self._last_alert_ts = now_ts
                return {**snap, "event": "opened", "since_ts": now_ts}
            if now_ts - (self._last_alert_ts or 0) >= REMINDER_SEC:
                self._last_alert_ts = now_ts
                return {**snap, "event": "reminder", "since_ts": self._episode_since}
            return None
        if self._episode_since is not None:
            since = self._episode_since
            self._episode_since = None
            self._last_alert_ts = None
            return {**snap, "event": "resolved", "since_ts": since}
        return None
