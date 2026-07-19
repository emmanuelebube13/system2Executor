# System 2 — Execution Engine ("The Hand")

> Part of the **Scalable Brain — Migration & Implementation Architecture Plan**.
> Audience: Staff PM + Principal Architect. Author: Principal Software Architect.
> Host: **Computer 2**. Operating profile: **active during market hours (Sun 22:00 – Fri 20:00 UTC)**.

## Purpose

System 2 is **The Hand** of the Scalable Brain: it turns risk-approved, pre-sized orders into real broker fills and manages open positions until they close. It comprises the current **Layer 4** (execution orchestrator), **Layer 7** (OANDA broker adapter), and **Layer 5** (read-only telemetry API + React dashboard).

In the new three-system topology, System 2 no longer makes account-level risk decisions. **System 3 (AMS, "The Guardian")** becomes middleware between Layer 3 and Layer 4: it consumes signals, applies sizing (Quarter-Kelly), daily/weekly limits, drawdown circuit breakers and consecutive-loss halts, then publishes **approved, pre-sized orders** onto `AMS_Outbound_Queue`. System 2 **polls that queue**, executes deterministically, and **pushes fill confirmations** back onto `AMS_Inbound_Queue` for System 3's post-trade processor.

System 2 also gains two model-consumption responsibilities previously implicit in a single-host deployment: it must **download and validate model artifacts** published by System 1 to object storage, and run a **live regime detector** (HMM inference on live candles) so strategy/signal selection reflects the current market state on Computer 2.

## Scope

In scope (this folder, EXEC-001..009):

- Model artifact downloader/validator on Computer 2 (poll `latest.json` + SHA256 + atomic swap).
- Live regime detector (HMM predict + persistence smoothing) on live candles.
- Slimming Layer 4 to **execution-only** responsibilities behind a feature flag.
- Queue consumer (`AMS_Outbound_Queue`) and fill-confirmation producer (`AMS_Inbound_Queue`).
- Hardening the OANDA adapter (idempotency, slippage tolerance, stop/TP confirmation, practice→live toggle) and documenting required `Fact_Live_Trades` schema additions.
- Active position management (breakeven, trailing stops, time-based exits).
- Safety mode (pause on stale queue) + audited emergency BYPASS.
- Layer 5 read endpoints over AMS account telemetry + dashboard views.

Out of scope (owned elsewhere):

- Position sizing / Quarter-Kelly, daily/weekly limits, drawdown circuit breakers, consecutive-loss halts → **System 3 (AMS)**.
- Model training and artifact publication → **System 1 (MODEL-007 publishes; MODEL-003 trains the regime model)**.
- Object storage, message queue, secrets, networking, observability provisioning → **Foundational (FND-001..010)**.

## Goals

1. Decouple execution from risk: Layer 4 trades **only** what System 3 has approved, never autonomously sizing risk in steady state.
2. Make execution **deterministic and idempotent**: the same approved order produces exactly one broker order, with stops/targets confirmed and slippage measured.
3. Make Computer 2 **self-sufficient for inference**: it pulls and verifies its own model artifacts and computes the live regime locally.
4. **Fail safe**: if risk approval is stale (`AMS_Outbound_Queue` > 5 min old), pause rather than trade blind; provide an audited manual BYPASS for emergencies only.
5. Provide **operator visibility** into AMS account state through Layer 5 without duplicating decision logic.

## Success Criteria

- [ ] A change to `latest.json` in object storage is detected within ~15 min, downloaded, SHA256-verified, and atomically swapped with zero partial-state reads by Layer 4 / the regime detector.
- [ ] An order placed on `AMS_Outbound_Queue` by System 3 results in exactly one OANDA practice order with stop-loss and take-profit confirmed, and a matching fill confirmation appears on `AMS_Inbound_Queue` within the latency budget.
- [ ] Replaying the same outbound message (same `idempotency_key`) never produces a second broker order.
- [ ] When `AMS_Outbound_Queue` is artificially stalled > 5 min, Layer 4 enters PAUSED and places no orders; BYPASS can only be enabled with an explicit, audited flag.
- [ ] Slippage is measured per fill and orders exceeding the 2-pip tolerance are flagged/rejected per policy; partial fills are reconciled correctly.
- [ ] Layer 5 exposes `/api/account/state`, `/equity-curve`, `/decisions`, `/circuit-breakers`, `/strategy-performance`, `/daily-summary` reading AMS state, with dashboard views.
- [ ] The legacy monolithic Layer 4 path can be re-enabled by toggling a feature flag (safe cutover preserved throughout).

## Task Index

| ID | Task | Priority | Effort | Prerequisites |
|----|------|----------|--------|---------------|
| EXEC-001 | Model downloader & validator | P0-Critical | 3d | FND-001, MODEL-007 |
| EXEC-002 | Live regime detector | P1-High | 2d | EXEC-001, MODEL-003 |
| EXEC-003 | Refactor Layer 4 to execution-only | P0-Critical | 4d | FND-002 |
| EXEC-004 | Outbound queue consumer | P0-Critical | 2d | FND-002, EXEC-003, AMS-008 |
| EXEC-005 | Fill confirmation producer | P0-Critical | 2d | FND-002, EXEC-003 |
| EXEC-006 | Broker adapter hardening | P1-High | 3d | EXEC-003 |
| EXEC-007 | Active position manager | P1-High | 3d | EXEC-006 |
| EXEC-008 | Safety mode & emergency BYPASS | P0-Critical | 2d | EXEC-004 |
| EXEC-009 | Layer 5 AMS telemetry endpoints | P2-Medium | 3d | AMS-001, AMS-009 |

### Recommended execution order

1. **Model self-sufficiency (parallelizable with the refactor):** EXEC-001 then EXEC-002. These depend on Foundational object storage (FND-001) and System 1 artifacts (MODEL-007, MODEL-003) but not on the queue.
2. **Execution-only refactor (longest pole):** EXEC-003 behind a feature flag. Start as soon as FND-002 lands the queue contract. Keep the legacy monolith path runnable for dual-run.
3. **Queue wiring:** EXEC-004 (consume approved orders) and EXEC-005 (publish fills) once EXEC-003 lands; EXEC-004 also needs the System 3 producer (AMS-008).
4. **Broker depth:** EXEC-006 (adapter hardening) then EXEC-007 (active position management).
5. **Safety:** EXEC-008 immediately after EXEC-004 — do not cut over to queue-driven trading without the staleness pause and BYPASS in place.
6. **Telemetry:** EXEC-009 last; it depends on the System 3 schema/state endpoints (AMS-001, AMS-009).

## Cross-system dependencies (summary)

- **Foundational:** FND-001 (object storage) gates EXEC-001; FND-002 (message queue + IPC) gates EXEC-003/004/005; FND-003 (secrets) supplies OANDA + queue + storage credentials; FND-005 (observability) defines SLOs/alerts referenced throughout.
- **System 1:** MODEL-007 (model serializer/publisher to object storage) gates EXEC-001; MODEL-003 (regime model training) gates EXEC-002.
- **System 3:** AMS-008 (outbound order producer) gates EXEC-004; AMS-001 (schema) and AMS-009 (account-state read API/views) gate EXEC-009.

## Cross-cutting standards applied across all EXEC tasks

- **Queue contracts:** every message carries `schema_version`, `message_id`, `idempotency_key`, `granularity` (H1/H4), `created_at` (UTC), and a `correlation_id` that ties an order to its fill.
- **Determinism:** given the same approved order + model artifact set + ATR inputs, the constructed broker order (entry/SL/TP/units) must be identical (preserves the Layer 4 execution-determinism contract).
- **Idempotency:** OANDA `clientExtensions.id` / client request id derived from `idempotency_key`; replays are no-ops.
- **Encryption in transit:** queue and object-storage traffic over the FND-008 private network with TLS; OANDA over HTTPS only.
- **Secrets:** never in `.env`/git — sourced from FND-003 (OANDA practice + live keys, queue/storage creds).
- **Granularity:** H1/H4 preserved end-to-end; no recompute of upstream Layer 1/2 outputs in Layer 4.
- **Latency/slippage budgets:** outbound-poll-to-order-submit target < ~2 s on H1; fill-to-`AMS_Inbound_Queue` publish < ~5 s; slippage tolerance 2 pips (EXEC-006).

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Layer 4 trades without risk approval (queue down / refactor bug) | Medium | Critical | EXEC-008 staleness pause (> 5 min → PAUSED); BYPASS is opt-in, audited, conservative-sizing only; feature flag keeps legacy path; dual-run before cutover. |
| Duplicate broker orders from queue redelivery or retries | Medium | High | Idempotency key on every outbound message → OANDA client request id; EXEC-006 idempotent submission + pre-submit open-order check. |
| Stale or corrupt model artifact swapped into live inference | Low | High | EXEC-001 SHA256 verify + atomic swap + keep last-known-good; refuse to swap on checksum mismatch; EXEC-002 falls back to last good regime. |
| Slippage / partial fills mis-recorded, corrupting post-trade risk math | Medium | High | EXEC-006 2-pip tolerance + fill validation; EXEC-005 reports realized status + partial-fill reconciliation to AMS; schema additions (Fill_Price, Slippage_Pips, ...). |
| Weekend/holiday gap or market-closed window produces bad orders | Medium | High | Market-hours guard (Sun 22:00–Fri 20:00 UTC); reject/queue-park orders outside session; gap-aware ATR/stop checks in EXEC-007. |
| Practice→live toggle flipped accidentally | Low | Critical | Single explicit env flag (`OANDA_ENV`) + startup banner + secrets separation (FND-003); refuse live unless live creds + flag both set; audit log. |
| Refactor regresses execution determinism contract | Medium | High | Golden-file determinism tests in EXEC-003; dual-run compares legacy vs slim decisions before flag flip. |
| Computer 2 offline during market hours | Low | High | Monitoring/SLO + alert (FND-005); System 3 holds approved orders on the queue (backpressure) until consumer returns; staleness handling on both ends. |
| Layer 5 telemetry leaks account data publicly | Low | High | VPN-only ingress (FND-008); read-only endpoints; auth on dashboard; no secrets in responses. |

## Operating profile notes

- System 2 is **only active during the trading session** (Sun 22:00 – Fri 20:00 UTC). Model download (EXEC-001) and regime detection (EXEC-002) may run slightly ahead of session open to warm caches.
- Layer 4 is currently cron-driven hourly (`shell/cron_layer4_pipeline.sh`); the queue-consumer model (EXEC-004) shifts it toward a long-running poller during the session while retaining the cron entry-point for compatibility/fallback.
