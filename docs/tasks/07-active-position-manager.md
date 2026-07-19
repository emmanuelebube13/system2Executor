# EXEC-007 — Active Position Manager

**Task ID:** EXEC-007
**System:** System 2 — Execution Engine
**Priority:** P1-High
**Estimated Effort:** 3d
**Prerequisites:** EXEC-006
**External Dependencies:**
- **OANDA pricing stream:** `GET /v3/accounts/{id}/pricing/stream` for live bid/ask used to evaluate breakeven/trailing/time triggers.
- **OANDA REST:** `GET /v3/accounts/{id}/openTrades` + `/openPositions` (current open trades and their stops), `PUT .../trades/{id}/orders` to modify stop-loss, and trade close endpoints for exits.
- **Secrets (FND-003) + encrypted transport (FND-008).**

## Objective
Build active position management — pricing-stream monitoring, breakeven at 1R, trailing stops, and time-based exits (50%/75%/100% of max duration) per the trade-management rules.

## Current State
- Layer 4 today places a trade with a fixed ATR stop/target and then logs it; there is **no** in-life management of open positions (no breakeven move, no trailing stop, no time-based exit). Layer 6 only reconciles outcomes post-close.
- EXEC-006 establishes a hardened adapter with stop/TP confirmation and openTrades access that this task builds on.

## Target State
- A position-management loop (active during the session) that monitors open trades via the pricing stream and applies trade-management rules:
  - **Breakeven at 1R:** once unrealized profit reaches 1R (1× the initial risk distance), move the stop-loss to entry (breakeven), removing downside on the position.
  - **Trailing stop:** beyond breakeven, trail the stop by a defined rule (e.g., ATR-based or R-multiple step) so profit is protected as price advances; the stop only ever moves in the favorable direction.
  - **Time-based exits:** at **50% / 75% / 100% of max duration** for the trade's granularity, apply staged management (e.g., tighten/partial-reduce at 50/75%, force-close at 100%) so trades do not linger past their thesis horizon.
- All stop modifications go through the EXEC-006 adapter (idempotent, confirmed); any close emits a fill confirmation via EXEC-005.

## Technical Specification

**Inputs per managed trade (text):** `broker_trade_id`, `instrument`, `side`, `entry_price`, `initial_stop_price` (⇒ `risk_distance = |entry − initial_stop|` = 1R), `take_profit_price`, `open_time`, `granularity`, `max_duration` (per granularity), `correlation_id`.

**Rule engine (text):**
- **1R breakeven:** `R = |current_price − entry| / risk_distance` in the favorable direction; when `R >= 1.0` and stop is still below entry (long) / above entry (short), move stop to entry (± a small buffer for spread). One-way only.
- **Trailing:** once at breakeven, recompute a trailing stop each tick/candle as `best_price ∓ trail_distance` (ATR- or R-based, `TRAIL_DISTANCE`); only tighten, never loosen.
- **Time-based:** `elapsed = now − open_time`; thresholds at `0.5 / 0.75 / 1.0 * max_duration`. At 50%/75% apply the configured tightening/partial action; at 100% force-close the remaining position.
- All modifications are idempotent (no-op if the stop is already at/ tighter than target) to avoid churn against the broker.

**Max duration defaults (text):** parameterized per granularity (e.g., H1 trades measured in hours, H4 in multi-hour/day horizons) — exact values configurable; the 50/75/100% staging is relative to that max.

**Pseudo-code (clarifying):**
```
on price tick / candle close for each open trade t:
    R = favorable_move(t, price) / t.risk_distance
    if R >= 1.0 and not t.at_breakeven: set_stop(t, t.entry ± buffer)   # EXEC-006
    if t.at_breakeven: new_stop = trail(price, TRAIL_DISTANCE)
                       if tighter(new_stop, t.stop): set_stop(t, new_stop)
    elapsed_frac = (now - t.open_time) / t.max_duration
    if elapsed_frac >= 1.0: close(t)                # emits EXEC-005 fill
    elif elapsed_frac >= 0.75: apply_75pct_action(t)
    elif elapsed_frac >= 0.50: apply_50pct_action(t)
```

**Env vars:** `POSMAN_ENABLED`, `BREAKEVEN_R` (default 1.0), `BREAKEVEN_BUFFER_PIPS`, `TRAIL_MODE` (atr|r), `TRAIL_DISTANCE`, `MAX_DURATION_H1`, `MAX_DURATION_H4`, `TIME_EXIT_FRACTIONS` (default "0.5,0.75,1.0").

**Data flow:** pricing stream → rule engine (per trade) → stop modifications/closes via EXEC-006 → fill/close confirmations via EXEC-005 → `Fact_Live_Trades` `Updated_At` refreshed. Reconnect the pricing stream on drop with backoff; on prolonged stream loss, fall back to periodic REST polling of openTrades.

## Testing & Validation
- **Unit:** R computation per side; breakeven moves stop to entry exactly once and only forward; trailing only tightens; time-exit thresholds fire at the right fractions for H1 vs H4.
- **Live-sim (practice):** open a trade, drive price to 1R → stop moves to breakeven; continue favorable → stop trails; hold to 100% duration → force-close emits an EXEC-005 confirmation.
- **Edge cases:** **gap / weekend** — price gaps through the stop: rely on broker stop execution, reconcile actual fill + slippage via EXEC-006 (do not assume stop price == fill price); **pricing stream drop** → reconnect/poll fallback, never leave a position unmanaged silently; **partial fill** parent trade still managed on `filled_units`; **slippage** on stop/close measured and reported; **idempotent modify** — duplicate ticks do not spam the broker.
- **Safety:** a trade discovered without a stop (EXEC-006 confirmation failed) is escalated/alerted, not managed blindly.

## Rollback Plan
- `POSMAN_ENABLED=false` disables active management entirely; trades then rest on their original ATR stop/TP (today's behavior) — degraded but safe, since every trade still has a broker-side stop from EXEC-006.
- Stop modifications are reversible only in the favorable direction by design; disabling the manager simply stops further moves. No destructive action.

## Acceptance Criteria
- [ ] At 1R unrealized profit, the stop is moved to breakeven exactly once and never loosened.
- [ ] Beyond breakeven, the stop trails in the favorable direction only, per the configured trail rule.
- [ ] Time-based actions fire at 50%/75%/100% of max duration (per granularity), with 100% forcing a close that emits an EXEC-005 confirmation.
- [ ] Pricing-stream loss is handled by reconnect/REST-poll fallback so no open position is left unmanaged silently.
- [ ] Stop modifications are idempotent and routed through the EXEC-006 adapter with confirmation.

## Notes & Risks
- Risk: over-trading the broker with stop modifications (rate limits/cost). Mitigated by idempotent no-op checks and tick throttling.
- Risk: a gap-through stop fills worse than the stop price — handled by trusting broker fills and recording real slippage, not the intended stop.
- Trade-management parameters (trail distance, max durations) should ideally be governed by System 1/3 policy; EXEC-007 implements the mechanism with configurable parameters.
