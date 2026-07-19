# EXEC-005 — Fill Confirmation Producer

**Task ID:** EXEC-005
**System:** System 2 — Execution Engine
**Priority:** P0-Critical
**Estimated Effort:** 2d
**Prerequisites:** FND-002, EXEC-003
**External Dependencies:**
- **Message queue (FND-002):** `AMS_Inbound_Queue` — Computer 2 needs produce rights and the connection/credentials.
- **OANDA transactions API:** `GET /v3/accounts/{id}/transactions` (and order/fill responses) for authoritative fill price, time, and realized status used to build the confirmation.
- **Secrets (FND-003) + encrypted transport (FND-008).**

## Objective
After each execution, publish fill confirmations (broker order id, fill price, fill time, slippage, realized status) to `AMS_Inbound_Queue` for the System 3 post-trade processor.

## Current State
- Today Layer 4 logs results only to local DB tables (`Fact_Live_Trades`, `Fact_Execution_Log`). There is no cross-host feedback to a risk/account engine — because account-level risk currently lives inside Layer 4.
- In the reorg, System 3 owns post-trade processing (P&L, drawdown, circuit breakers, consecutive-loss tracking) and needs fills delivered to it.

## Target State
- After every execution attempt, Layer 4 publishes a **fill confirmation** message to `AMS_Inbound_Queue` carrying the broker order id, fill price, fill time, computed slippage, realized status, and the `correlation_id` from the originating approved order.
- Publication is **at-least-once with idempotency**: the message carries the same `correlation_id`/`idempotency_key` so System 3 can dedup; a publish failure retries without creating divergent records.
- The same data is also persisted locally to `Fact_Live_Trades` (using the EXEC-006 schema additions) so the DB and the queue agree.

## Technical Specification

**`AMS_Inbound_Queue` fill-confirmation message (text):**
```
{
  schema_version: int,
  message_id: string,
  idempotency_key: string,        # echoes the originating order's key
  correlation_id: string,         # echoes the approved order's correlation_id
  created_at: ISO-8601 UTC,
  granularity: "H1" | "H4",
  ams_decision_id: string,
  instrument: string,
  side: "BUY" | "SELL",
  requested_units: int,
  filled_units: int,              # may be < requested for partial fills
  realized_status: "FILLED" | "PARTIAL" | "REJECTED" | "CANCELLED" | "EXPIRED",
  broker_order_id: string,        # OANDA order/transaction id
  broker_trade_id?: string,
  requested_price?: float,
  fill_price?: float,
  fill_time?: ISO-8601 UTC,
  slippage_pips?: float,          # signed; computed vs requested/expected price (EXEC-006)
  stop_loss_price?: float,
  take_profit_price?: float,
  reject_reason?: string,
  model_set_id: string            # which artifact set was active (EXEC-001)
}
```

**Producer behavior (text):**
- Built immediately after the Layer 7 submit/validate step (EXEC-006) returns, using OANDA's order/fill/transaction response as the source of truth (cross-check via the transactions endpoint when needed).
- **Partial fills:** `realized_status=PARTIAL`, `filled_units < requested_units`; include both so System 3 reconciles exposure correctly.
- **Rejections/cancellations:** still publish (with `realized_status` + `reject_reason`) so System 3 can release reserved risk and update its state.
- **Slippage:** `slippage_pips` computed by EXEC-006 (vs requested/expected price), echoed here.
- **Reliability:** persist-then-publish (write `Fact_Live_Trades` first, then publish); on publish failure, retry with backoff; an outbox/local-queue pattern ensures no fill is lost if `AMS_Inbound_Queue` is briefly unavailable.

**Pseudo-code (clarifying):**
```
result = layer7.submit_and_validate(order, risk)   # EXEC-006
record = build_fill_record(order, result)          # includes slippage, status, ids
persist(Fact_Live_Trades, Fact_Execution_Log, record)
msg = build_inbound_message(record, order.correlation_id)
publish_with_retry(AMS_Inbound_Queue, msg)         # outbox-backed; idempotent on correlation_id
```

**Env vars:** `INBOUND_QUEUE_NAME`, `QUEUE_ENDPOINT`, `QUEUE_CREDS` (FND-003), `FILL_PUBLISH_RETRY_MAX`, `FILL_PUBLISH_TIMEOUT_SEC`.

**Latency budget:** fill detected → `AMS_Inbound_Queue` publish < ~5 s (cross-cutting SLO, FND-005).

## Testing & Validation
- **Unit:** message built correctly for FILLED, PARTIAL, REJECTED, CANCELLED, EXPIRED; slippage echoed; `correlation_id`/`idempotency_key` preserved from the order.
- **Integration:** execute a practice order → confirmation lands on `AMS_Inbound_Queue` and matches the OANDA transaction record and the `Fact_Live_Trades` row.
- **Reliability / edge:** `AMS_Inbound_Queue` down at publish time → outbox retries and the fill is delivered once the queue returns (no loss, no divergence); duplicate publish (same `correlation_id`) is dedup-safe for System 3.
- **Edge cases:** **partial fill** reports `filled_units` and `PARTIAL`; **slippage** beyond 2 pips flagged per EXEC-006 and reflected in status/reason; **queue staleness** on the inbound side does not block execution (outbox absorbs it); **weekend gap** — no fills generated, nothing to publish.
- **Consistency:** DB row and queue message never disagree (persist-then-publish ordering verified).

## Rollback Plan
- Feature-flag the producer: if disabled, Layer 4 still persists to `Fact_Live_Trades`/`Fact_Execution_Log` as today, and System 3 can fall back to reading the DB directly for post-trade processing (degraded but functional).
- The outbox is local; clearing it after a confirmed deliver is safe. No broker side effects from this task.

## Acceptance Criteria
- [ ] Every execution attempt (fill, partial, reject, cancel, expire) produces a fill-confirmation message on `AMS_Inbound_Queue` with broker order id, fill price/time, slippage, and realized status.
- [ ] The message echoes the originating `correlation_id`/`idempotency_key` so System 3 can dedup; duplicate publishes are harmless.
- [ ] Persist-then-publish guarantees `Fact_Live_Trades` and the queue message agree; a transient queue outage loses no fill (outbox retry).
- [ ] Partial fills are reported with both `requested_units` and `filled_units`.

## Notes & Risks
- Risk: a fill that is recorded in the DB but never delivered to System 3 would desync risk state. Mitigated by the outbox + retry and the DB-fallback path.
- The `model_set_id` echo lets System 3 attribute realized outcomes to the exact model set (closes the loop with EXEC-001).
