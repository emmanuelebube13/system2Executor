# EXEC-006 — Broker Adapter Hardening

**Task ID:** EXEC-006
**System:** System 2 — Execution Engine
**Priority:** P1-High
**Estimated Effort:** 3d
**Prerequisites:** EXEC-003
**External Dependencies:**
- **OANDA v20 REST API:** order placement (`POST /v3/accounts/{id}/orders`), `GET /v3/accounts/{id}/openTrades` + `/openPositions`, `GET /v3/accounts/{id}/transactions`, `GET /v3/accounts/{id}/summary`. Authoritative source for fills, stops/TP confirmation, and idempotency checks.
- **Secrets (FND-003):** OANDA practice **and** live keys, stored separately; the live key is only reachable when the live toggle is set.
- **DB / ODBC (FND-004):** the `Fact_Live_Trades` schema additions documented here.

## Objective
Harden Layer 7 OANDA adapter (`src/layer7/oanda_executor.py`): robust order construction, fill validation within 2-pip slippage tolerance, stop/take-profit confirmation, retries/idempotency, practice→live toggle. Also document the schema additions `Fact_Live_Trades` needs (Broker_Order_ID, Fill_Price, Fill_Time, Slippage_Pips, etc.).

## Current State
- `src/layer7/oanda_executor.py` (643 lines): OANDA order placement with Quarter-Kelly sizing (2% cap). It is **designed but not actively live-trading**. Sizing (Kelly) now belongs to System 3 — the adapter should receive already-sized `units` from the approved order (EXEC-003/004) and not re-size.
- `Fact_Live_Trades` includes `Order_ID` but **lacks** `Broker_Order_ID`, `Fill_Price`, `Fill_Time`, `Slippage_Pips`, `Model_Threshold`, `Regime_Label`, `Correlation_Score`, `Correlation_Passed`, `Updated_At` (per CLAUDE.md / docs/reference/DATABASE_MIGRATION.md).
- No formal idempotency, slippage tolerance, or practice→live guard exists.

## Target State
- A hardened adapter that:
  - **Constructs orders robustly** from the approved order: instrument, signed `units` (as given), order type, and SL/TP from EXEC-003's ATR computation, with input validation and clear error taxonomy.
  - **Idempotent submission:** sets OANDA `clientExtensions.id` (client request id) derived from the order's `idempotency_key`; before submitting, checks open trades/positions and recent transactions to avoid duplicates.
  - **Confirms stops/TP:** after fill, verifies the stop-loss and take-profit orders actually exist on the trade (re-reads via openTrades/transactions); if missing, attaches or aborts per policy.
  - **Validates fills within a 2-pip slippage tolerance:** computes `slippage_pips` = (fill_price − expected_price) in pips; within tolerance ⇒ accept; beyond ⇒ flag/reject per policy and report status to EXEC-005.
  - **Retries with backoff + idempotency** on transient errors (network/5xx/rate limit); never blindly resubmits a possibly-filled order (re-check before retry).
  - **Practice→live toggle:** a single explicit, audited switch; refuses live unless both the live env flag and live creds are present; startup banner states the active environment.
- The required `Fact_Live_Trades` schema additions are **documented** (DDL described, paired with docs/reference/DATABASE_MIGRATION.md) so EXEC-005 can persist full fill metadata.

## Technical Specification

**Order construction inputs (from approved order, EXEC-003):** `instrument`, `side`, `units` (signed, AMS-sized — not modified), `sl_price`, `tp_price`, `idempotency_key`, `correlation_id`.

**Idempotency:** OANDA `order.clientExtensions.id = "sb-" + idempotency_key` (and a matching `tradeClientExtensions`). On submit, if OANDA reports the client id already used, treat as success and reconcile from transactions rather than resubmitting.

**Slippage (text):** pip size per instrument (e.g., 0.0001 majors, 0.01 JPY pairs). `slippage_pips = signed(fill_price − expected_price) / pip_size`. Tolerance = `SLIPPAGE_TOLERANCE_PIPS` (default 2). Beyond tolerance → status `REJECTED`/flagged with reason (policy: cancel-if-not-yet-filled, or accept-and-flag if already filled), reported via EXEC-005.

**Stop/TP confirmation (text):** after the fill transaction, fetch openTrades for the trade id; assert `stopLossOrder` and `takeProfitOrder` present at expected prices; if absent, attempt to attach; if still absent, mark unsafe and alert (a position without a stop is a risk event).

**Practice→live toggle (env):**
```
OANDA_ENV = practice | live           # default practice
OANDA_URL = https://api-fxpractice.oanda.com | https://api-fxtrade.oanda.com
# live requires BOTH OANDA_ENV=live AND a live key+account present (FND-003); else refuse to start in live
```

**Documented `Fact_Live_Trades` schema additions (DDL described, not applied here — pair with docs/reference/DATABASE_MIGRATION.md):**
```
ALTER TABLE Fact_Live_Trades ADD
  Broker_Order_ID   NVARCHAR(64)  NULL,   -- OANDA order/transaction id
  Broker_Trade_ID   NVARCHAR(64)  NULL,
  Fill_Price        DECIMAL(18,6) NULL,
  Fill_Time         DATETIME2     NULL,   -- UTC
  Requested_Price   DECIMAL(18,6) NULL,
  Slippage_Pips     DECIMAL(9,2)  NULL,   -- signed
  Realized_Status   NVARCHAR(16)  NULL,   -- FILLED/PARTIAL/REJECTED/...
  Filled_Units      INT           NULL,
  Stop_Loss_Price   DECIMAL(18,6) NULL,
  Take_Profit_Price DECIMAL(18,6) NULL,
  Model_Threshold   DECIMAL(9,4)  NULL,
  Model_Set_ID      NVARCHAR(64)  NULL,   -- EXEC-001 active set
  Regime_Label      NVARCHAR(32)  NULL,
  Correlation_Score DECIMAL(9,4)  NULL,
  Correlation_Passed BIT          NULL,
  Updated_At        DATETIME2     NULL;   -- UTC
```
(Schema-aware writes: detect column presence at runtime per existing convention; escape reserved words.)

**Retry/backoff (pseudo-code, clarifying):**
```
for attempt in 1..MAX:
    try: resp = oanda.submit(order_with_client_id)
    except transient: reconcile = check_transactions(client_id)
                      if reconcile.filled: return reconcile  # don't resubmit
                      sleep(backoff(attempt)); continue
    return validate(resp)   # slippage + stop/TP confirmation
```

**Env vars:** `OANDA_ENV`, `OANDA_URL`, `OANDA_API_KEY`/`OANDA_API_KEY_LIVE` (FND-003), `OANDA_ACCOUNT_ID_DEMO`/`_LIVE`, `SLIPPAGE_TOLERANCE_PIPS` (2), `BROKER_RETRY_MAX`, `BROKER_RETRY_BACKOFF_SEC`.

## Testing & Validation
- **Unit:** pip-size per instrument (majors vs JPY); slippage computation sign/magnitude; tolerance accept/reject boundary at exactly 2 pips; client-id idempotency mapping.
- **Practice integration:** submit, confirm fill, confirm SL/TP attached, compute slippage, reconcile against the transactions endpoint.
- **Idempotency:** resubmit the same `idempotency_key` (and simulate a transient error after a successful fill) → no duplicate order; adapter reconciles from transactions.
- **Edge cases:** **partial fill** → `PARTIAL`, `filled_units` reported; **slippage > 2 pips** → policy path exercised; **stop/TP missing after fill** → attach-or-alert; **rate limit / 5xx** → backoff without duplication; **weekend gap / market closed** → OANDA rejects, adapter surfaces a clear closed-market status; **live toggle** refuses to start in live without live creds.
- **Negative:** invalid instrument, zero units, SL on wrong side of price → rejected with clear taxonomy.

## Rollback Plan
- The hardened adapter is additive; keep `OANDA_ENV=practice` and the existing call path during rollout. If issues arise, revert Layer 4 to `EXEC_MODE=legacy` (EXEC-003) which uses the prior adapter behavior.
- The schema additions are nullable/additive — adding them does not break existing writes, and they can be left unused if the new write path is disabled. Reverting is a non-destructive column drop (with confirmation per project rules).
- Never enable `OANDA_ENV=live` until practice validation is complete; the toggle is the rollback boundary for environment.

## Acceptance Criteria
- [ ] Orders are constructed from AMS-sized `units` (never re-sized), submitted idempotently via a client request id derived from `idempotency_key`, with no duplicate orders on retry.
- [ ] Fills are validated within a 2-pip slippage tolerance and slippage is computed and reported; stop-loss and take-profit are confirmed present after fill (or the trade is flagged unsafe + alerted).
- [ ] The practice→live toggle is a single explicit switch that refuses live unless live creds and the live flag are both set, with a startup banner stating the environment.
- [ ] The `Fact_Live_Trades` schema additions are documented (DDL + docs/reference/DATABASE_MIGRATION.md) and writes are schema-aware.

## Notes & Risks
- Risk: resubmitting an order that already filled (network ambiguity). Mitigated by client-id idempotency + transactions reconciliation before any retry.
- Risk: a position left without a stop. Treated as a safety event — confirm-or-alert, never silently proceed.
- Quarter-Kelly sizing is intentionally removed from the adapter's responsibility (now System 3); the adapter trusts `units`.
