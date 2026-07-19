# EXEC-008 — Safety Mode & Emergency BYPASS

**Task ID:** EXEC-008
**System:** System 2 — Execution Engine
**Priority:** P0-Critical
**Estimated Effort:** 2d
**Prerequisites:** EXEC-004
**External Dependencies:**
- **Message queue (FND-002):** `AMS_Outbound_Queue` last-message timestamp / consumer lag is the staleness signal.
- **DB / ODBC (FND-004):** BYPASS mode reads Layer 3 directly (`Fact_Signals` + model artifacts) when activated.
- **Observability (FND-005):** PAUSE and BYPASS transitions must alert the operator.
- **Secrets (FND-003).**

## Objective
Implement Layer 4 safety mode (pause if `AMS_Outbound_Queue` stale > 5 min) and an audited emergency BYPASS mode (read Layer 3 directly with conservative hardcoded sizing) as the manual override.

## Current State
- Layer 4 in `execution_only` (EXEC-003/004) depends entirely on `AMS_Outbound_Queue` for approved orders. If System 3 or the queue is down, Layer 4 would either idle (no orders) or — worse, if mis-built — trade without risk approval.
- There is no formal staleness detector and no controlled fallback path. The reorg mandates: **never trade without risk approval**, with a manual emergency override.

## Target State
- **Safety mode (automatic):** Layer 4 continuously checks the freshness of `AMS_Outbound_Queue` (last approved-order timestamp / consumer lag). If stale **> 5 min** during the session, Layer 4 transitions to **PAUSED**: it stops submitting new orders (open-position management EXEC-007 continues — managing existing risk is always safe), logs the transition, and alerts. It auto-resumes when fresh approved orders flow again.
- **Emergency BYPASS (manual, audited):** an explicit operator-enabled mode where Layer 4 reads Layer 3 directly (`Fact_Signals` + champion/legacy model artifacts) and trades with **conservative hardcoded sizing** (small fixed risk, well below normal Kelly output), used only when System 3 is down but the operator must act. BYPASS is opt-in, time-bounded, loudly logged/alerted, and never the default.

## Technical Specification

**State machine (text):**
```
states: RUNNING -> PAUSED -> RUNNING ; (manual) -> BYPASS -> RUNNING
RUNNING:  consume AMS_Outbound_Queue, execute approved orders (normal).
PAUSED:   queue stale > STALENESS_LIMIT_SEC. No new orders. EXEC-007 continues. Alert. Poll for freshness.
BYPASS:   operator-enabled only. Read Layer 3 directly, conservative fixed sizing. Audited. Time-bounded.
```

**Staleness detection (text):** `staleness = now − max(last_approved_order.created_at, last_successful_poll_with_data)`; if `staleness > STALENESS_LIMIT_SEC` (default 300) while in-session ⇒ PAUSE. Distinguish "queue healthy but legitimately no orders" from "queue/System 3 down": an empty-but-reachable queue with recent heartbeats is **not** a fault (System 3 may publish a periodic heartbeat/empty marker on `AMS_Outbound_Queue` per FND-002/AMS-008 so EXEC-008 can tell silence-by-design from outage). Without a heartbeat, treat prolonged silence conservatively as PAUSE.

**BYPASS activation (text):**
- Requires an explicit flag/command, e.g. `EXEC_BYPASS_ENABLE=true` plus a confirmation token; refuses to auto-enable.
- Conservative sizing: `BYPASS_RISK_PCT` hardcoded small (e.g., 0.25–0.5% per trade), independent of any Kelly logic; a max concurrent positions cap; still applies ATR stops (EXEC-003) and the basic correlation backup guard.
- **Audit:** every BYPASS entry/exit and every BYPASS-originated trade is written to `Fact_Execution_Log` with `source=BYPASS`, the operator id/reason, and start/end timestamps; an alert fires on entry and on exit.
- Time-bounded: `BYPASS_MAX_DURATION_SEC` auto-reverts to RUNNING/PAUSED when System 3 returns or the window expires.

**Pseudo-code (clarifying):**
```
if bypass_enabled and confirmed:
    state = BYPASS
    orders = read_layer3_signals_directly()     # Fact_Signals + model artifacts
    size = conservative_fixed(BYPASS_RISK_PCT)
    execute(orders, size); audit("BYPASS")
elif in_session and staleness > STALENESS_LIMIT_SEC:
    state = PAUSED; alert("queue_stale"); manage_open_positions_only()
else:
    state = RUNNING; consume_and_execute(AMS_Outbound_Queue)
```

**Env vars:** `STALENESS_LIMIT_SEC` (default 300), `EXEC_BYPASS_ENABLE` (default false), `BYPASS_CONFIRM_TOKEN`, `BYPASS_RISK_PCT` (default 0.25), `BYPASS_MAX_POSITIONS`, `BYPASS_MAX_DURATION_SEC`.

**Data flow:** EXEC-004 lag metric → EXEC-008 staleness check → state machine → (RUNNING: normal) / (PAUSED: hold) / (BYPASS: direct Layer 3, conservative). All transitions → `Fact_Execution_Log` + alerts (FND-005) + Layer 5 surface.

## Testing & Validation
- **Unit:** staleness boundary at exactly 5 min; heartbeat distinguishes silence-by-design from outage; BYPASS refuses to enable without the confirm token; conservative sizing math.
- **Integration:** stall `AMS_Outbound_Queue` > 5 min → Layer 4 PAUSES, places no orders, EXEC-007 keeps managing opens; resume flow → auto-RUNNING.
- **BYPASS drill:** enable BYPASS with token → trades originate from Layer 3 with conservative fixed size; audit rows + alerts present; time-bound auto-revert works; disabling returns to normal.
- **Edge cases:** **queue staleness** is the core trigger; **weekend gap** — out-of-session silence is expected and must not falsely PAUSE/alert (session-aware); **partial fills/slippage** in BYPASS still validated via EXEC-006 and reported via EXEC-005; clock skew/NTP handled (staleness uses UTC); **flapping** — hysteresis so it does not oscillate RUNNING/PAUSED on borderline lag.
- **Safety invariant test:** with the queue down and BYPASS off, Layer 4 never submits a new order.

## Rollback Plan
- Safety mode is fail-safe by construction; if its logic is buggy, the safest fallback is to leave Layer 4 PAUSED (no orders) until fixed.
- BYPASS defaults off and is removable by clearing the flag; it has no effect unless explicitly enabled.
- If EXEC-008 must be disabled entirely, the system should be left in `EXEC_MODE=legacy` (EXEC-003) or stopped — never run `execution_only` without staleness protection.

## Acceptance Criteria
- [ ] When `AMS_Outbound_Queue` is stale > 5 min during the session, Layer 4 enters PAUSED and submits no new orders while continuing open-position management.
- [ ] PAUSED auto-resumes to RUNNING when fresh approved orders (or heartbeats) return; in-session vs out-of-session silence is distinguished (no false PAUSE on weekends).
- [ ] Emergency BYPASS only activates with an explicit, audited, confirmed flag, uses conservative hardcoded sizing, is time-bounded, and logs every BYPASS trade to `Fact_Execution_Log`.
- [ ] With the queue down and BYPASS off, Layer 4 provably never trades without risk approval.
- [ ] All state transitions alert the operator (FND-005) and are visible in Layer 5.

## Notes & Risks
- Risk: BYPASS becomes a habit and undermines the risk discipline System 3 enforces. Mitigated by time-bounding, loud auditing, conservative sizing, and requiring explicit confirmation each time.
- Risk: false PAUSE during legitimately quiet markets. Mitigated by System 3 heartbeats + session-awareness + hysteresis.
- Open-position management (EXEC-007) intentionally continues during PAUSE — pausing **new** risk while protecting **existing** risk is the safe posture.
