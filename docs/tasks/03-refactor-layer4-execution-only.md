# EXEC-003 — Refactor Layer 4 to Execution-Only

**Task ID:** EXEC-003
**System:** System 2 — Execution Engine
**Priority:** P0-Critical
**Estimated Effort:** 4d
**Prerequisites:** FND-002
**External Dependencies:**
- **Message queue (FND-002):** the slimmed Layer 4 will receive work from `AMS_Outbound_Queue` and emit fills to `AMS_Inbound_Queue`. The contract must be defined even though the consumer/producer wiring lands in EXEC-004/005.
- **DB / ODBC (FND-004):** continued writes to `Fact_Live_Trades` and `Fact_Execution_Log`.
- **OANDA practice API (FND-003):** unchanged broker path via Layer 7.

## Objective
Refactor `live_pipeline.py` to execution-only responsibilities (ATR stops/targets, order construction/submission, fill validation, slippage), removing account-level risk/sizing logic now owned by System 3 (keep a basic correlation guard as backup), behind a feature flag for safe cutover.

## Current State
- `src/layer4_executor/live_pipeline.py` (1400+ lines) is a **monolith** running 8 stages: (1) load signals from `Fact_Signals`, (2) load regime from `Fact_Market_Regime_V2`, (3) load Layer 3 model artifact, (4) compute ATR risk params (**1.0x ATR SL, 3.0x ATR TP, RR 3.0**), (5) **portfolio correlation gate (max 0.85 correlation, max 25% exposure)**, (6) **ML gatekeeper threshold** (`LAYER3_APPROVAL_THRESHOLD=0.20`), (7) execute via Layer 7, (8) log to `Fact_Live_Trades` / `Fact_Execution_Log`.
- Key classes: `ExecutionPipeline`, `TradeDecision`, `SignalContext`, `RegimeContext`, `ModelArtifact`, `RiskParameters`, `CorrelationResult`, `ExecutionResult`.
- Runtime behaviors to preserve: ODBC auto-detect (18/17/env), `Fact_Signals.Is_Active` schema-awareness, escaped `[Close]`, rotating logs (10MB/14), exit 0 on no eligible signals.
- It does **both** position-level execution **and** account-level risk — the reorg requires splitting these.

## Target State
- Layer 4 retains **only execution-level** concerns:
  - **ATR-based stops/targets** (keep 1.0x SL / 3.0x TP / RR 3.0 contract).
  - **Order construction & submission** (via Layer 7), **fill validation**, **slippage measurement**, and **open-position management** (the detailed rules live in EXEC-007).
  - A **basic correlation guard as a backup** safety net (lightweight; not the authoritative risk gate).
- Account-level risk **removed** from Layer 4 and owned by System 3: position sizing / Quarter-Kelly, daily/weekly limits, drawdown circuit breakers, consecutive-loss halts, and the authoritative ML approval gate. Layer 4 trusts that an order arriving on `AMS_Outbound_Queue` is already sized and approved.
- The refactor sits **behind a feature flag** (`EXEC_MODE`) so the legacy monolith path remains runnable for dual-run and instant rollback. Execution determinism contract preserved.

## Technical Specification

**Feature flag / modes (env `EXEC_MODE`):**
```
EXEC_MODE = legacy           # current monolith: signals -> risk -> ML -> execute (unchanged path)
EXEC_MODE = execution_only   # new slim path: consume approved order -> ATR stops/targets -> submit -> validate fill
```
EXEC-004 (consumer) and EXEC-005 (producer) plug into `execution_only`. During cutover both can run in **dual-run/shadow**: `execution_only` computes the order but does not submit, and its decisions are compared to `legacy` (golden-file determinism check) before the flag is flipped to live submission.

**Responsibility split (text):**
- **Keep in Layer 4:** `RiskParameters` (ATR SL/TP), order construction, `ExecutionResult`, fill validation, slippage, open-position management, a **basic** `CorrelationResult` backup guard.
- **Move to System 3 (delete/disable in Layer 4):** Quarter-Kelly sizing, exposure cap as the authoritative gate, ML approval threshold as the authoritative gate, daily/weekly/drawdown/consecutive-loss logic.
- **Backup correlation guard:** retains a conservative correlation check (e.g., reject if an approved order would push correlation/exposure past a hard backstop) — logged, fail-safe, never a substitute for AMS's risk engine. It exists only to catch a System 3 fault.

**Approved-order input contract (consumed by EXEC-004, defined here so the refactor targets it):**
```
ApprovedOrder {
  schema_version, message_id, idempotency_key, correlation_id,
  created_at (UTC), granularity ("H1"|"H4"),
  instrument, side ("BUY"|"SELL"),
  units (signed, already sized by AMS),
  signal_id, strategy_id,
  risk_context { atr, suggested_sl?, suggested_tp? },   # Layer 4 computes ATR stops if not supplied
  ams_decision_id
}
```
Layer 4 computes ATR SL/TP deterministically (1.0x/3.0x) from candle/ATR inputs unless AMS supplies explicit prices; `units` is **taken as given** (never re-sized).

**Preserve:** ODBC auto-detect, `Is_Active` schema-awareness, escaped `[Close]`, rotating logs, exit-0-on-empty, SQLAlchemy parameter binding, H1/H4 granularity.

**Pseudo-code (execution_only path, clarifying):**
```
order = next_approved_order()            # EXEC-004 supplies; here: contract only
if duplicate(order.idempotency_key): skip
risk = compute_atr_stops(order)          # 1.0x SL / 3.0x TP
if not basic_correlation_backup(order): reject_and_log("backup_guard")
result = layer7.submit(order, risk)      # EXEC-006 adapter
validate_fill_and_slippage(result)       # 2-pip tolerance (EXEC-006)
persist(Fact_Live_Trades, Fact_Execution_Log)
emit_fill_confirmation(result)           # EXEC-005
```

## Testing & Validation
- **Golden-file determinism:** for a fixed set of approved orders + ATR inputs + model set, `execution_only` produces identical entry/SL/TP/units across runs (execution-determinism contract).
- **Dual-run shadow:** run `legacy` and `execution_only` side by side on the same inputs (with submission disabled in shadow) and assert the constructed orders match before cutover.
- **Unit:** ATR SL/TP math (1.0x/3.0x/RR 3.0); backup correlation guard triggers only past the hard backstop; `units` from AMS is never modified.
- **Regression:** ODBC auto-detect, `Is_Active`-absent path, `[Close]` escaping, exit-0-on-empty, rotating logs all still pass.
- **Edge cases:** approved order with missing ATR inputs → fetch candles / compute, or reject with a clear reason; duplicate `idempotency_key` → no second order; **partial fills / slippage / queue staleness** are exercised in EXEC-004/005/006/008 but the slim path must surface them, not swallow them; **weekend gap** → orders outside session are rejected/parked.

## Rollback Plan
- Flip `EXEC_MODE=legacy` to restore the full monolith path immediately; the legacy code remains intact during the migration window.
- The cron entry-point (`shell/cron_layer4_pipeline.sh`) continues to invoke Layer 4; in `legacy` it behaves exactly as today.
- No schema or queue changes are forced by this task alone (those land in EXEC-004/005/006), so reverting the flag fully restores prior behavior.

## Acceptance Criteria
- [ ] In `execution_only`, Layer 4 contains no account-level sizing/limits/drawdown/consecutive-loss logic and never re-sizes AMS `units`.
- [ ] ATR stop/target math (1.0x/3.0x/RR 3.0) and execution determinism are preserved and covered by golden-file tests.
- [ ] A basic correlation guard remains as a logged, fail-safe backup only.
- [ ] `EXEC_MODE=legacy` restores the original monolith behavior with no code deletion of the legacy path during migration.
- [ ] Dual-run shadow shows `execution_only` and `legacy` construct equivalent orders before the flag is flipped to live submission.

## Notes & Risks
- Risk: silently dropping the authoritative ML/risk gate while AMS is not yet wired would let unfiltered signals through. Mitigation: `execution_only` only acts on `AMS_Outbound_Queue` (EXEC-004); until AMS produces orders, it has nothing to execute, and EXEC-008's staleness pause is the backstop.
- This is the longest pole and the highest-blast-radius change; keep changes additive and the legacy path warm until EXEC-004/005/006/008 are all green.
