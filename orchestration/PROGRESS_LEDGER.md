# PROGRESS LEDGER (single source of truth — read the event-log tail first)

**Updated:** 2026-07-01T13:00:00Z · **Phase:** Build + DevOps + polish COMPLETE · **Mode:** single-threaded

## NEXT ACTION
**All 10 EXEC tasks + DevOps + optional polish done (152/152 tests green).** Transfer-ready:
`python -m system2` (fail-closed exit 2), health server, pruned `requirements.txt`, `RUNBOOK.md`,
`docs/SCHEMA_ADDITIONS.md` + `migrations/{sqlite,postgres}/` + `common/db.py` (migrate CLI),
`Fact_Live_Trades` persist-then-publish now wired (`execution/trade_recorder.py`), `Dockerfile` +
`.dockerignore` + `deploy/system2.service`. **Remaining is human/ops, not code:** (1) provision creds in
`config/.env.system2`; (2) `python -m system2.common.db migrate`; (3) **practice integration drill**;
(4) log the **D-004 cutover decision** to flip EXEC-003 out of SHADOW.
⚠ EXEC-003 live submission stays in **shadow** (`EXEC_SHADOW` default true) until D-004 is logged.

## Decisions locked
- **D-001 connectivity:** Local datastore on Computer 2 (default local PostgreSQL; SQLite for dev).
- **D-002 queue backend:** Google Cloud Pub/Sub (local durable for dev/test).
- **D-003 Phase-0 sign-off:** ✅ APPROVED 2026-06-29 (AG-EXEC-000 green).

## Task board
| Task | Status | Owner | Gate |
|---|---|---|---|
| PHASE-0 | ✅ done | Architect | AG-EXEC-000 green |
| EXEC-001 model downloader | ✅ done (11/11 tests) | Artifact-Sync | AG-EXEC-001 green |
| EXEC-002 live regime | ✅ done (9/9 tests) | Artifact-Sync | AG-EXEC-002 green |
| EXEC-003 slim Layer 4 | ✅ done (14/14 tests, shadow) | Execution-Core | AG-EXEC-003 green |
| EXEC-004 outbound consumer + validation + DLQ | ✅ done (9 consumer + 21 validation + 7 queue tests) | Execution-Core | AG-EXEC-004 green |
| EXEC-005 fill producer | ✅ done (11/11 tests) | Execution-Core | AG-EXEC-005 green |
| EXEC-006 broker hardening + reconcile | ✅ done (17/17 tests) | Broker | AG-EXEC-006 green |
| EXEC-007 active position mgr | ✅ done (11/11 tests) | Broker | AG-EXEC-007 green |
| EXEC-008 staleness PAUSE + BYPASS | ✅ done (15/15 tests) | Execution-Core | AG-EXEC-008 green |
| EXEC-009 telemetry + health | ✅ done (10/10 tests) | Telemetry | AG-EXEC-009 green |
| EXEC-010 emergency STOP + lifecycle | ✅ done (11/11 tests) | Execution-Core | AG-EXEC-010 green |

## Event-log tail
1. Session start; build target empty, all sources/specs/skills present.
2. Read README + dependencies spec.
3. HUMAN DECISIONS: D-001 (local datastore), D-002 (Pub/Sub).
4. Folder skeleton created; specs + 8 skills copied into `docs/`.
5. Phase-0 artifacts written (architecture, boundary, abstraction, manifest, config, scaffolding).
6. **STOP** — awaiting human Phase-0 sign-off (D-003) before any EXEC build.
7. D-003 approved → EXEC-001/002 (Artifact-Sync cluster) built & green; AG-EXEC-001/002 green.
8. EXEC-003 slim execution-only pipeline built (ATR determinism preserved, golden+dual-run); shadow hold.
9. `common/queue_backend.py` written: QueueBackend Protocol, LocalDurableBackend (SQLite WAL + DLQ), PubSubBackend (lazy), make_envelope, build_queue factory.
10. EXEC-004 built: `execution/validation.py` (envelope + TTL + last-line §7.2 OrderValidator), `execution/outbound_consumer.py` (SqliteProcessedStore persistent dedup, DLQ on poison, session park, ack-after-durable, lag tracking). `validate_fn` + `Decision.REJECTED_VALIDATION` wired additively into `pipeline.process`.
11. EXEC-005 built: `execution/fill_producer.py` (FillResult contract, persist-then-publish via durable outbox, retry-forever flush, control-event emitter).
12. Full suite **82/82 green** (was 34). Gates AG-EXEC-004/005 green: zero monolith imports (decoupling), no hardcoded secrets.
13. EXEC-006 built: `broker/oanda_adapter.py` (pip/slippage/client-id math, idempotent submit w/ reconcile-before-retry, 2-pip slippage flag, stop/TP confirm-or-unsafe-alert, practice/live fail-closed toggle) over an `OandaTransport` seam; `broker/oanda_transport.py` (live REST, lazy oandapyV20, transient/market-closed mapping).
14. EXEC-007 built: `broker/position_manager.py` (pure rule engine — R-multiple, breakeven-once/forward-only w/ float-eps guard, trailing-tighten-only, 50/75/100% time exits; `PositionManager` routes idempotent stop moves/closes through the adapter, emits EXEC-005 on force-close).
15. Full suite **108/108 green**. Gates AG-EXEC-006/007 green: zero monolith imports, no hardcoded secrets, SDK-free import (lazy), live fail-closed verified.
16. EXEC-008 built: `execution/safety_mode.py` (SafetyMonitor state machine RUNNING↔PAUSED, manual audited BYPASS; freshness = last order/heartbeat else startup grace; session-aware no-false-pause; hysteresis anti-flap; `can_submit()` gate; `conservative_position_size`; alert/audit hooks). **Safety invariant proven:** queue down + BYPASS off ⇒ `can_submit()` False. NOTE: BYPASS "read Layer-3 directly" order-source is monolith-only (violates D-001 no-Computer-1-dep) so it is intentionally NOT implemented here — the state machine, submit-gate, conservative sizing, and audit are.
17. Full suite **123/123 green**. AG-EXEC-008 green: no monolith imports, no hardcoded secrets, invariant test asserts no-submit-without-approval.
18. EXEC-009 built: `telemetry/health.py` (`HealthReporter`, pure, provider-injected, degrades-not-crashes) + `telemetry/server.py` (FastAPI mount, lazy import, `/health` `/status` `/api/account/state`, private-network bind). SCOPED to System-2's OWN state (exec_mode, queue lag, open positions, outbox depth, model_set_id, broker env) — the AMS account-store endpoints (equity-curve/decisions/breakers/strategy-perf/daily-summary) are System-3's own Layer 5 and are intentionally NOT served here (would breach D-001 + SYSTEM_BOUNDARY). No secrets in responses (asserted by test).
19. Full suite **133/133 green**. AG-EXEC-009 green: no monolith imports, no hardcoded secrets, web stack (fastapi/uvicorn) lazy-imported.
20. EXEC-010 built: `execution/lifecycle.py` — `EmergencyStop` (signal/trigger/filesystem-sentinel, always reachable) + `ExecutionRuntime` (tick loop: evaluate safety → consume gated by `submit_gate_fn=can_submit` → manage positions always; startup reconcile adopts broker open trades; graceful shutdown flushes outbox, emits STOPPED, leaves positions with broker stops by default / flatten opt-in; idempotent) + `build_from_secrets` composition root wiring submit_fn=OandaAdapter.submit, emit_fn=FillProducer.publish_fill, validate_fn=OrderValidator.validate, HealthReporter bound to live objects (EXEC_SHADOW defaults true). Added additive `submit_gate_fn` to `OutboundConsumer` (PAUSE parks new orders, never submits).
21. Full suite **144/144 green**. AG-EXEC-010 green + full-service sweep: zero monolith imports service-wide, no hardcoded secrets, all optional deps (oandapyV20/fastapi/uvicorn/pubsub) lazy, `build_from_secrets` fail-closes on missing creds. **BUILD DAG COMPLETE — 10/10 EXEC tasks done.** EXEC-003 remains SHADOW pending logged human cutover.
22. DevOps/packaging: `requirements.txt` + `requirements-dev.txt` (pruned to real imports, pinned to dev venv), `src/system2/__main__.py` entrypoint (`python -m system2`: build_from_secrets → health thread → run loop; fail-closed exit 2, verified), `docs/SCHEMA_ADDITIONS.md` + `migrations/{sqlite,postgres}/001_fact_live_trades_additions.sql` (EXEC-006 columns), `src/system2/common/db.py` (D-001 local datastore: DbConfig sqlite|postgres fail-closed, idempotent migration runner + `python -m system2.common.db migrate` CLI). Logged **D-004** (shadow→live cutover) as ⏳ PENDING mandatory human decision in DECISIONS_LOG.md.
23. Full suite **148/148 green** (+4 db tests). Entrypoint + migrate CLI smoke-tested. **System 2 is transfer-ready** (code + packaging); live trading gated on D-004.
24. Optional polish: `execution/trade_recorder.py` (schema-aware, parameterized, upsert `Fact_Live_Trades` writer wired as pipeline `persist_fn` in `build_from_secrets` → persist-then-publish; log-and-continue on DB error so a fill is never blocked); `Dockerfile` + `.dockerignore` (non-root, practice/SHADOW default, secrets injected at runtime); `deploy/system2.service` (systemd: ExecStartPre migrate, SIGTERM graceful stop w/ 60s, fail-closed restart limits, FS hardening). Full suite **152/152 green**; `build_from_secrets` still fail-closes (persist wiring runs after the adapter's OANDA secret check). **All engineering complete — only the human D-004 cutover + ops provisioning remain.**
