# EXEC-004 — Outbound Queue Consumer

**Task ID:** EXEC-004
**System:** System 2 — Execution Engine
**Priority:** P0-Critical
**Estimated Effort:** 2d
**Prerequisites:** FND-002, EXEC-003, AMS-008
**External Dependencies:**
- **Message queue (FND-002):** `AMS_Outbound_Queue` — Computer 2 needs consume rights and the broker connection/credentials.
- **System 3 producer (AMS-008):** publishes approved, pre-sized orders in the agreed envelope. EXEC-004 cannot be validated end-to-end without it.
- **Secrets (FND-003):** queue credentials. **Encryption in transit (FND-008):** consume over the private network/TLS.

## Objective
Make Layer 4 consume approved, pre-sized orders from `AMS_Outbound_Queue` instead of pulling signals from Layer 3 directly.

## Current State
- Layer 4 (`live_pipeline.py`) currently loads signals directly from `Fact_Signals` (stage 1) and applies its own risk + ML gate before executing. There is no queue consumer.
- EXEC-003 introduces the `execution_only` mode and the `ApprovedOrder` input contract but does not yet wire the transport.

## Target State
- In `EXEC_MODE=execution_only`, Layer 4 sources work **only** from `AMS_Outbound_Queue`: a consumer pulls approved orders, deduplicates by `idempotency_key`, and feeds them into the execution path (ATR stops → Layer 7 submit → fill validation → EXEC-005 fill publish).
- The consumer tracks the **last-message timestamp / consumer lag** so EXEC-008 can detect staleness (> 5 min → PAUSE).
- Acknowledgement semantics ensure an order is only ack'd after it is durably handled (submitted + recorded, or definitively rejected), so a crash mid-processing redelivers rather than loses the order.

## Technical Specification

**`AMS_Outbound_Queue` message (ApprovedOrder envelope, text):**
```
{
  schema_version: int,
  message_id: string,            # unique per publish
  idempotency_key: string,       # stable per intended trade (dedup key)
  correlation_id: string,        # links to the eventual fill on AMS_Inbound_Queue
  created_at: ISO-8601 UTC,
  granularity: "H1" | "H4",
  ams_decision_id: string,
  instrument: string,            # e.g. "EUR_USD"
  side: "BUY" | "SELL",
  units: int (signed, AMS-sized),
  signal_id: int,
  strategy_id: int,
  risk_context: { atr: float, suggested_sl?: float, suggested_tp?: float },
  expires_at?: ISO-8601 UTC      # order TTL; expired orders are dropped, not executed
}
```

**Consumer behavior (text):**
- Long-running poller (during session) with manual ack; prefetch small (e.g., 1–4) to bound in-flight orders.
- **Dedup:** persist processed `idempotency_key`s (local state store); a redelivered key is ack'd and skipped (no second order).
- **TTL:** if `expires_at` has passed (or `created_at` older than a max age), drop + log (do not submit a stale order).
- **Market-hours guard:** outside Sun 22:00–Fri 20:00 UTC, do not submit; nack/park per FND-002 semantics and log.
- **Lag tracking:** record `created_at` of the last consumed message and the last successful poll time; expose to EXEC-008 + Layer 5.
- **Validation:** reject malformed messages (schema_version mismatch, missing required fields, units == 0) to a dead-letter / log, ack to avoid poison-message loops.

**Ack lifecycle (pseudo-code, clarifying):**
```
msg = consume(AMS_Outbound_Queue)
if not valid(msg): deadletter(msg); ack; continue
if expired(msg) or seen(msg.idempotency_key): ack; continue
if not in_session(now): nack/park; continue
record_seen(msg.idempotency_key)
result = execution_path(msg)        # EXEC-003 slim path + EXEC-006 submit
if result.durably_handled: ack
else: nack  # redeliver
```

**Env vars:** `OUTBOUND_QUEUE_NAME`, `QUEUE_ENDPOINT`, `QUEUE_CREDS` (FND-003), `CONSUMER_PREFETCH`, `ORDER_MAX_AGE_SEC`, `EXEC_MODE`.

**Data flow:** AMS-008 → `AMS_Outbound_Queue` → EXEC-004 consumer → EXEC-003 execution path → EXEC-005 fill to `AMS_Inbound_Queue`. The `correlation_id` threads the whole way through.

## Testing & Validation
- **Unit:** dedup skips a repeated `idempotency_key`; TTL/expiry drops old orders; malformed message → dead-letter + ack; units==0 rejected.
- **Integration (with AMS-008 or a stub producer):** an approved order published is consumed, executed (practice), and ack'd exactly once; matching fill appears on `AMS_Inbound_Queue`.
- **Crash safety:** kill the consumer after submit but before ack → on restart the redelivered message is recognized as already-processed via `idempotency_key` and not re-submitted.
- **Edge cases:** **queue staleness** (no messages > 5 min) surfaces the lag metric for EXEC-008; **partial fill** order still ack'd and reported (reconciliation in EXEC-005/006); **slippage** breach handled per EXEC-006 policy; **weekend gap** — messages arriving outside session are parked, not executed; **backpressure** — prefetch bounds in-flight orders so a burst cannot overrun the broker.
- **Determinism:** consuming the same approved order yields the same constructed broker order (with EXEC-003).

## Rollback Plan
- Flip `EXEC_MODE=legacy` (EXEC-003) to revert Layer 4 to direct `Fact_Signals` reads; the consumer is inert in legacy mode.
- If the queue is unhealthy but trading must continue, EXEC-008 BYPASS provides an audited manual override; otherwise EXEC-008 staleness pause keeps the system safe (no orders).
- Consumer offsets / processed keys are local state; clearing them is safe because OANDA idempotency (EXEC-006) is the second line of defense against duplicates.

## Acceptance Criteria
- [ ] In `execution_only`, Layer 4 sources orders solely from `AMS_Outbound_Queue` and no longer reads `Fact_Signals` for execution.
- [ ] Redelivery / duplicate `idempotency_key` never produces a second broker order.
- [ ] Expired, malformed, or out-of-session orders are dropped/parked with clear logs, never blindly submitted.
- [ ] Consumer lag / last-message timestamp is exposed for EXEC-008 staleness detection and Layer 5.
- [ ] An order is ack'd only after it is durably handled (submitted+recorded or definitively rejected).

## Notes & Risks
- Risk: poison message loops — mitigated by dead-lettering invalid messages and acking them rather than infinite nack.
- Risk: ordering across instruments — the queue need not guarantee global ordering; per-instrument idempotency + open-order checks (EXEC-006) prevent double exposure.
- The cron entry-point can remain for `legacy`; in `execution_only` Layer 4 becomes a long-running session-scoped poller.
