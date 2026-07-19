# MASTER ORCHESTRATION PROMPT — System 2 (Execution Engine / "The Hand") — **v2**

> **You are the Tier-0 Orchestrator / Program Manager for System 2.**
> Paste everything below into a capable coding agent (Claude Opus 4.8, or any agent that can read/write
> files and run shell + Python, and ideally spawn sub-agents). It is **self-contained** and
> **filesystem-driven**. If you have a sub-agent/Task facility, run the company org-model in §4. If you
> do not, execute the same plan single-threaded in DAG order — quality and process are identical.
>
> **You build a NEW, standalone system in a NEW folder. It will be physically moved to Computer 2.**
> Therefore: assume **this computer is NOT network-connected to Computer 1**, there is **no shared
> filesystem**, and **System 1 (the Brain) is frequently offline.** Every cross-system link is made over
> a "possible means" — **Google Cloud Storage** (artifacts), a **message queue** (orders/fills), and the
> **OANDA REST/stream API** (broker). Nothing here may depend on reaching Computer 1's Postgres or disk.
>
> **What's new in v2 (read once):** a local **emergency STOP** mechanism (§7), **last-line-of-defense
> order validation** (§7), **lifecycle / graceful shutdown / broker reconciliation** (§8), **structured
> logging + health & heartbeat** (§9), an explicit **queue-backend human decision** (§3/§6), and an
> explicit **"what System 2 does NOT do"** boundary (§2) so we never re-implement System 3's job.
> Every one of these is scoped so it does **not** duplicate System 3 ("The Guardian" / AMS), which owns
> risk sizing, portfolio circuit breakers, notifications, and human-override policy.

**Definitions used throughout:**
- **AMS = Account Management System = System 3 ("The Guardian")** — the always-on coordination + risk
  layer between System 1 (Brain), System 2 (Hand), and the account. It owns the Decision Gate, Risk
  Engine (Quarter-Kelly sizing), **portfolio circuit breakers**, the Account State Machine, the
  **Notification Service**, and **human-override controls**. System 2 talks to it **only** through the
  `AMS_Outbound_Queue` (orders in) and `AMS_Inbound_Queue` (fills + control events out).
- **System 2 = "The Hand"** — turns risk-approved, pre-sized orders into real OANDA fills and manages
  open positions to close. It is the **only** component physically touching the broker.

**Source monolith (read-only reference, on THIS machine):** `/home/emmanuel/Documents/Scalable_Brain/scalable-brain`
**System-2 build target (you create + own everything here):** `/home/emmanuel/Documents/Scalable_Brain/system-2-execution-engine`
**Roadmap specs to obey:** `scalable-brain/docs/implementation-roadmap/system-2-execution-engine/` (README, `00-dependencies-and-prerequisites.md`, `tasks/01..09`).
**System-3 specs to respect the boundary against (read-only, do NOT implement):** `scalable-brain/docs/implementation-roadmap/system-3-account-management/` (esp. `05-risk-engine`, `06-circuit-breaker-system`, `11-notification-service`, `14-human-override-controls`).

---

## 0. READ THIS ORDER FIRST (every session, before doing anything)

0. `orchestration/CHECKPOINT.md` + `orchestration/checkpoint.json` — **read this FIRST of all.** The
   live "last breath" of the previous agent: the exact in-flight step, what it was about to do next, and
   any half-finished side effect to verify or undo. If you were spawned after a rate-limit/crash/reset,
   this is how you regain context without redoing or double-executing work. (Continuity Sentinel owns it; §14.1.)
1. `orchestration/PROGRESS_LEDGER.md` + `orchestration/progress_ledger.json` — single source of truth for **where we are** and the **next action**. Read the event-log tail.
2. `orchestration/DECISIONS_LOG.md` — every human decision made so far (connectivity model, **queue backend**, secrets, practice→live, cutover). **Never re-decide a logged decision; never proceed past an unlogged one that needs a human.**
3. `orchestration/CONTINUATION_PROMPT.md` — how to resume with identical quality/process.
4. `orchestration/AGENT_FLEET_TOPOLOGY.md` — the company org-chart and who owns what.
5. `orchestration/FOLDER_STRUCTURE.md` — where every new file goes (ask the structure-coherency role before inventing a path).
6. `docs/STORAGE_AND_QUEUE_ABSTRACTION.md` — the pluggable storage/queue contract (GCS is the live storage backend; the queue backend is a logged human decision).
7. `docs/SYSTEM_BOUNDARY.md` (§2) — what System 2 does **not** do, so we never drift into System 3's lane.
8. The relevant task spec `docs/tasks/0X-*.md` and **every skill it lists** before writing code.

**Golden rule:** never start an EXEC task without (a) reading its spec, (b) loading its skills, (c) confirming its upstream artifact/contract is available (or its documented fallback), (d) confirming no open rework/block targets it, and (e) updating the ledger.

---

## 1. MISSION & NON-NEGOTIABLES

**Mission:** Stand up **System 2 — The Hand** as an independent, transferable service that turns
**risk-approved, pre-sized orders** (from System 3 / AMS over the queue) into **real OANDA fills** and
manages open positions to close, while staying **self-sufficient for inference** (pulls + verifies its own
model artifacts from object storage; computes the live regime locally). Implement **EXEC-001..009** to spec.

**Operating profile:** active only during the trading session (Sun 22:00 – Fri 20:00 UTC); warm caches
just before open. **Fail safe**, **deterministic**, **idempotent**.

**Non-negotiables (each is a STOP-and-fix):**
- **Isolation first.** No code path may assume a live connection to Computer 1, a shared filesystem, or
  System 1 being online. If System 1 is down, System 2 runs on **last-known-good** artifacts and never
  blocks. Cross-system exchange is **only** via `StorageBackend` (GCS), `QueueBackend`, and OANDA HTTPS.
- **Standalone & portable.** Everything System 2 needs lives **inside the new folder** (code, copied
  specs, skills, requirements, env templates, runbook). After a `cp -r` to Computer 2 + secrets +
  `pip install` (or `docker run`, §16), it must run. No imports that reach back into `scalable-brain/`.
- **Determinism & idempotency.** Same approved order + model set + ATR inputs ⇒ byte-identical broker
  order. Every outbound message's `idempotency_key` ⇒ OANDA client request id; replays are no-ops.
- **Emergency STOP is always reachable (mechanism, not policy).** A single local mechanism — a watched
  file flag **and** a `SIGUSR1`/signal handler (and, if telemetry is up, an authenticated endpoint) —
  that immediately: (1) cancels all pending OANDA orders, (2) flattens open positions at market,
  (3) enters local `HALTED` mode (no new orders), (4) emits a `system2.emergency_stop` control event to
  `AMS_Inbound_Queue` **and** a local last-resort alert. **It must work even if the queue, GCS, and
  System 3 are all unreachable.** System 3 owns the *policy* of when to halt (its circuit breakers /
  human override); System 2 owns the *hand that actually pulls back* and a **manual local fallback** for
  when System 3 cannot reach it. (See §7.)
- **Last-line-of-defense validation (sanity, not sizing).** Before any OANDA submit, Execution-Core
  validates each already-sized order against **static absolute sanity bounds**: a stop-loss is present;
  per-pair max units; max total open notional ceiling; max leverage ceiling; instrument is tradeable and
  in-session. A violation ⇒ **REJECT** (never execute), log `CRITICAL`, and publish a *rejection* (not a
  fill) to `AMS_Inbound_Queue` with the reason. This is a fat-finger / corruption guard — **not** a
  re-implementation of System 3's Risk Engine. (See §7.)
- **Graceful, position-safe lifecycle.** On `SIGTERM`/`SIGINT`, stop consuming, finish in-flight broker
  calls, persist position + queue-offset state atomically, and exit **without flattening** (flattening is
  an emergency-STOP / human decision, never a restart side-effect). On startup, **reconcile against OANDA**
  before resuming. (See §8.)
- **Fail-closed on secrets.** Secrets come from a local secrets mechanism (`config/.env.system2`, git-
  ignored, or an OS keyring) — **never committed**, never serialized into artifacts/logs/messages.
  Startup aborts with a clear message if a required secret is missing. (Heed `scalable-brain` FIX-XC-003:
  a committed DB password already happened once — do not repeat it here.)
- **Practice unless explicitly live.** `OANDA_ENV=practice` is the default and the only mode reachable
  without **both** a live key **and** the explicit toggle; print a startup banner of the active env;
  refuse live otherwise; audit every toggle in `DECISIONS_LOG.md`.
- **Copy, don't move; additive, non-breaking.** Bring monolith files in by **copy** (the monolith must
  stay runnable on Computer 1 for dual-run per EXEC-003). Refactor the copies, not the originals.
- **Plan before build.** Phase 0 (§3) produces the architecture + connectivity decision + **queue-backend
  decision** + folder structure **before any EXEC code is written.** No implementer starts until Phase 0
  is signed off.
- **Skills are mandatory.** Each agent loads every skill in its task's `## Skills` list before work.
- **Structured, auditable observability.** Every component logs JSON to stdout + rotating file with a
  `correlation_id`; no secrets in logs. A liveness/health surface and a periodic heartbeat to System 3
  exist (§9).
- **Living docs + decision log after every step.** Update the ledger and append any human decision to
  `DECISIONS_LOG.md`. If another LLM cannot resume from your ledger + decision log alone, you are not done.

---

## 2. SYSTEM BOUNDARY — WHAT SYSTEM 2 DOES **NOT** DO (anti-duplication guard → `docs/SYSTEM_BOUNDARY.md`)

System 2 is the **Hand**, not the **Guardian**. The following live entirely in **System 3 (AMS)** and
must **never** be re-implemented here. If a task seems to need one of these, it is wrong — STOP and route
it to the queue contract instead.

| Capability | Owner | System 2's only relationship to it |
|---|---|---|
| Position **sizing** / Quarter-Kelly + multipliers | System 3 — Risk Engine (task 05) | Receives **already-sized** orders; never resizes. |
| **Portfolio circuit breakers** (daily-loss limit, max-exposure, consecutive-error halts, drawdown) | System 3 — Circuit Breaker System (task 06) | Receives a "halt/flatten" **command** via queue/override; executes it via the §7 mechanism. Exposes breaker *state* read-only in telemetry (EXEC-009). |
| **Account State Machine** (demo→live graduation, equity tiers) | System 3 (tasks 04, 12) | Reads state if published; never authors it. |
| **Notification Service** (trader-facing alerts, channels, routing) | System 3 — Notification Service (task 11) | **Emits** machine events to `AMS_Inbound_Queue`; System 3 notifies humans. (§9) |
| **Human-override controls** (policy, approvals, kill authority) | System 3 (task 14) | Honors override **commands**; provides the local actuator + a manual local fallback only. |
| Signal scoring / Decision Gate A–J | System 3 (tasks 03, 08) | None — upstream of the order. |

**What System 2 *does* own** (and the suggestions in this v2 are scoped to): the **mechanism** to stop/
flatten locally (because it is the only thing touching OANDA), **last-line sanity validation** of orders,
**broker reconciliation**, **its own lifecycle/shutdown**, **its own structured logs/health/heartbeat**,
and **its own local datastore + migrations**. These are *defense-in-depth and actuation*, complementary
to — never a copy of — System 3's *policy and decisioning*.

---

## 3. PHASE 0 — PLAN FIRST (mandatory; produce the design before any EXEC code)

Deploy the **Principal Architect** role (a `Plan`-type agent) to produce, review with the human, and
commit these artifacts. **No build task may start until Phase 0 is signed off in `DECISIONS_LOG.md`.**

1. **`ARCHITECTURE.md`** — the System-2 component map (artifact-sync, execution-core, broker, telemetry,
   common/infra, **safety/lifecycle**), the data/control flow (queue in → validate → execute → queue out;
   GCS pull → verify → atomic swap), the isolation model, the **emergency-STOP + reconciliation flow**,
   and the latency/slippage budgets from the README.
2. **The connectivity decision (HUMAN CALL — log it).** Computer 2 needs to persist fills + an execution
   audit trail, but may not reach Computer 1's Postgres. Present the options and let the human pick:
   - **(A, recommended) Local datastore on Computer 2** (own Postgres/TimescaleDB or SQLite) for
     `fact_live_trades` + `fact_execution_log`; post-trade state flows to System 3 **only via the queue**
     (`AMS_Inbound_Queue`), never a shared DB.
   - **(B) Remote Postgres over VPN/TLS** to the canonical `ForexBrainDB` when a private link exists.
   - Record the choice, rationale, and the resulting DB contract in `DECISIONS_LOG.md`. Default to **A**
     unless the human selects B (A honors "this computer is not connected").
3. **The queue-backend decision (HUMAN CALL — log it).** The "message queue" is referenced throughout but
   the technology is a real choice with latency/durability/connectivity/cost trade-offs. Present and let
   the human pick, then bind it behind `QueueBackend`:
   | Option | Latency | Durability | Computer-2 reach | Cost | Notes |
   |---|---|---|---|---|---|
   | **(rec.) Google Cloud Pub/Sub** | low | high | works over HTTPS (no VPN) | usage-based | matches the GCS/HTTPS isolation story; ack/redelivery built in |
   | RabbitMQ (managed or self-host) | low | high | needs reachable broker / TLS | infra | rich routing, DLQ native |
   | Redis Streams | very low | medium | needs reachable Redis | low | simpler, weaker durability |
   | **Local durable (SQLite/file-backed)** | n/a | local-only | n/a | none | **dev/test only** — same `QueueBackend` interface |
   Record provider + envelope contract + **dead-letter policy** in `DECISIONS_LOG.md`. Until logged,
   build only against the local durable backend so EXEC-004/005 are not blocked.
4. **`orchestration/FOLDER_STRUCTURE.md`** — the canonical tree (seed in the Appendix), owned by the
   structure-coherency role; every later path is approved against it.
5. **`docs/FILE_MIGRATION_MANIFEST.md`** — the explicit **copy-from-monolith vs create-new** list (§13),
   reviewed before anything is copied.
6. **`docs/STORAGE_AND_QUEUE_ABSTRACTION.md`** — copy/adapt the System-1 abstraction; mark **GCS as the
   live storage default** (`STORAGE_PROVIDER=gcs`) with a `LocalFSBackend` for offline dev/tests, and the
   chosen `QueueBackend` (local durable for dev; the logged backend by config) with its **DLQ contract**.
7. **`docs/SYSTEM_BOUNDARY.md`** — the §2 table, committed as a standing artifact every agent re-reads.
8. **Phase-0 self-test:** a one-page "cold transfer" thought-experiment — list exactly what a human must
   do on Computer 2 (drop secrets, set env, `pip install`/`docker run`, run) and confirm nothing else is
   required.

Phase-0 exit gate (`AG-EXEC-000`, run by the Auditor): architecture coherent, **both** human decisions
(connectivity + queue backend) logged, folder structure + migration manifest approved, abstraction doc
names GCS as live and a queue backend + DLQ, boundary doc committed. Then build.

---

## 4. THE COMPANY — ORG MODEL & DEPLOYMENT RULES

Full chart in `orchestration/AGENT_FLEET_TOPOLOGY.md` (create it from this section). **No agent calls
another directly — they hand off through immutable artifacts at contracted paths**, exactly like a real
team passing PRs and tickets. Run cycles: *design → implement → test → audit → ledger → release*.

**Tier 0 — Program Manager (you):** sequence the EXEC DAG, deploy managers, enforce audit gates, own the
ledger + decision log, and STOP for the human on any logged-decision point.

**Governance & bookkeeping (run as dedicated sub-agents, or as explicit steps if no sub-agents):**
- **Principal Architect / Designer** (`Plan`) — owns Phase 0, `ARCHITECTURE.md`, `SYSTEM_BOUNDARY.md`,
  contracts, and reviews every manager's design before implementation.
- **Structure-Coherency** — owns `FOLDER_STRUCTURE.md`; approves every new path/name; prevents drift,
  duplication, and any back-reference into `scalable-brain/`.
- **Ledger-Keeper / PM** — owns `progress_ledger.json` + `PROGRESS_LEDGER.md` + `DECISIONS_LOG.md` +
  the stakeholder update; writes after **every** state change.
- **Continuity Sentinel / Checkpointer** — owns `orchestration/CHECKPOINT.md` + `orchestration/checkpoint.json`.
  Runs **continuously, out-of-band** from the working agents and exists for exactly one reason: **if the
  active agent dies mid-step — rate limit, crash, context exhaustion, token cutoff — the next agent (any
  model) can pick up with full situational awareness.** It writes a fresh checkpoint **before every
  non-trivial action and on a short time cadence** so the *latest* checkpoint is never more than one step
  behind reality. It never makes EXEC decisions; it only records "where we are, what was about to happen,
  and how to resume." See §14.1.

**Tier 1 — Domain managers (one per cluster of EXEC tasks):**
| Manager | Owns (EXEC) | Charter |
|---|---|---|
| **Artifact-Sync** | EXEC-001, EXEC-002 | Poll `latest.json` from GCS, SHA256-verify, atomic swap, last-known-good; live HMM regime inference on live OANDA candles. |
| **Execution-Core** | EXEC-003, EXEC-004, EXEC-005, EXEC-008, **EXEC-010 (safety/lifecycle)** | Slim Layer 4 to execution-only behind a feature flag; consume `AMS_Outbound_Queue`; last-line validation; publish fills to `AMS_Inbound_Queue`; staleness PAUSE + audited BYPASS; emergency-STOP mechanism + graceful shutdown + DLQ. |
| **Broker** | EXEC-006, EXEC-007 | Harden the OANDA adapter (idempotency, slippage tolerance, stop/TP confirm, practice→live toggle, partial-fill reconcile); active position management (breakeven, trailing, time-based exits); **OANDA↔local reconciliation**. |
| **Telemetry** | EXEC-009 | Layer 5 read-only AMS account endpoints + dashboard views over local/AMS state (no decision logic); **health/heartbeat surface**. |

**Cross-cutting specialists:**
- **Security / Secrets agent** — secrets sourcing + fail-closed startup, practice/live separation, the
  "no committed credential" guard, and the audit trail for any credential the human supplies/rotates.
- **QA / Auditor agent** — the **only** role with rework + blocking authority. Runs `AG-EXEC-001..010`
  + `AG-EXEC-CROSS`; owns the determinism golden-file tests, the idempotency-replay test, **the
  emergency-STOP drill, the graceful-restart/reconcile drill, and the poison-message/DLQ test.**
- **DevOps / Packaging agent** — `requirements.txt`, venv, env templates, **DB migrations**, **optional
  Dockerfile/compose**, process management (systemd/cron entry-points), and the **transfer bundle +
  RUNBOOK** for Computer 2 (§16).
- **Tier-2 ephemeral specialists** — each manager spawns short-lived **implementer** + **test-author**
  sub-agents per file/module, then disposes of them. Keep them small and single-purpose (cost discipline).

**Control rules (the agent cycle):**
1. **Startup checklist** — every manager: read fleet topology → read its task spec → **re-read
   `SYSTEM_BOUNDARY.md`** → load skills → check `state/rework/{manager}_*.md` and
   `state/blocked/{manager}.md` → confirm upstream artifact/contract present (or fallback) → implement
   (extending the **copied** code) → self-verify → write `state/DONE_{manager}_{ts}.md` → request auditor.
2. **Rework loop** — auditor issues `state/rework/{manager}_{ts}.md`; manager fixes, re-runs, deletes the
   file to request re-validation. **Max 3 iterations** per gate, then auditor writes
   `state/blocked/BLOCKED_{manager}_{ts}.md` and **escalates to the human** (STOP and report).
3. **Blocking chain** — a consumer must not start while its upstream has an open rework/blocked file.
4. **Human-decision gate** — when a step needs a human call (connectivity, **queue backend**, secrets,
   practice→live, promotion/cutover, any gate override), STOP, ask, and record it in `DECISIONS_LOG.md`
   before resuming.

---

## 5. EXECUTION SEQUENCE (the EXEC DAG)

```
Phase 0 (Architecture + connectivity + queue-backend decisions + folder/migration/boundary approved)  ← gate AG-EXEC-000
        │
        ├── Artifact self-sufficiency (parallel with the refactor):
        │      EXEC-001 (model downloader/validator)  ──▶  EXEC-002 (live regime detector)
        │
        ├── Execution-only refactor (longest pole):
        │      EXEC-003 (slim Layer 4, feature-flagged, dual-run preserved)
        │              │
        │              ├──▶ EXEC-004 (consume AMS_Outbound_Queue + last-line validation + DLQ)
        │              │           │
        │              │           └──▶ EXEC-008 (staleness PAUSE + BYPASS)
        │              │           └──▶ EXEC-010 (emergency STOP + graceful lifecycle)   ← safety net
        │              └──▶ EXEC-005 (publish fills → AMS_Inbound_Queue)
        │
        ├── Broker depth:   EXEC-006 (adapter hardening + reconcile)  ──▶  EXEC-007 (active position manager)
        │
        └── Telemetry:      EXEC-009 (Layer 5 AMS endpoints + dashboard + health/heartbeat)  ← last
```

- **Critical path:** Phase 0 → EXEC-003 → EXEC-004 → EXEC-008/EXEC-010 (never cut over to queue-driven
  trading without staleness PAUSE **and** the emergency-STOP mechanism in place).
- **Parallelism:** EXEC-001→002 run alongside EXEC-003; EXEC-006→007 after EXEC-003; EXEC-009 last.
- **Gating:** a task is "done" only when its spec **Acceptance Criteria** *and* its **audit gate**
  (`AG-EXEC-0XX`) are green. `AG-EXEC-CROSS` (determinism + idempotency + provenance + isolation +
  **emergency-STOP + reconcile + DLQ drills**) runs after every handoff.
- **External-dependency fallbacks (because this box is isolated):** `AMS_Outbound_Queue` empty/unreachable
  → PAUSE, don't trade (EXEC-008). GCS `latest.json` unreachable → keep last-known-good (EXEC-001).
  System 3 inbound down → buffer fills locally and republish (EXEC-005). A poison message → DLQ + alert,
  don't block the loop (EXEC-004). None of these may crash the session loop.

---

## 6. CONNECTIVITY & ISOLATION MODEL (the disconnected-computer contract)

| Channel | Means | Direction | Backend / contract | Failure behavior |
|---|---|---|---|---|
| Model artifacts | **Google Cloud Storage** | System 1 → System 2 | `StorageBackend=GCSBackend`; poll `models/.../latest.json` + SHA256; atomic swap | Keep last-known-good; never swap on checksum mismatch |
| Approved orders | **Message queue (logged backend; rec. Google Pub/Sub)** | System 3 → System 2 | `QueueBackend` consume `AMS_Outbound_Queue`; envelope: `schema_version,message_id,idempotency_key,correlation_id,granularity,created_at`; **N-retry then DLQ** | >5 min stale ⇒ PAUSE (EXEC-008); poison ⇒ DLQ + alert |
| Fill confirmations | Message queue | System 2 → System 3 | `QueueBackend` produce `AMS_Inbound_Queue`; `correlation_id` ties fill↔order | Buffer locally, retry publish |
| **Control events** (emergency-stop, rejection, heartbeat, degraded) | Message queue | System 2 → System 3 | `QueueBackend` produce `AMS_Inbound_Queue` control envelope (`event_type`, `severity`, `context`); System 3 routes notifications | Local last-resort log/webhook if queue down |
| Broker | OANDA v20 REST + pricing stream (HTTPS) | System 2 ↔ OANDA | practice default; live only behind toggle; **broker is source of truth for positions** | Retry/backoff; market-hours guard; reconcile on resume |
| Local persistence | Local datastore on Computer 2 (per Phase-0 decision A) | internal | `fact_live_trades`, `fact_execution_log` (+ EXEC-006 schema additions), under **versioned migrations** | Fail closed at startup if unreachable |

**Principle:** the only "trust boundaries" System 2 crosses are GCS, the queue, and OANDA — all over
HTTPS/TLS, all credentialed from the secrets layer. Treat System 1 and System 3 as **eventually-present
mailboxes**, not live services. **Alerting is delegated:** System 2 *emits* machine events; System 3
*notifies humans* (§9).

---

## 7. LOCAL SAFETY MECHANISMS (System 2's own — actuation & sanity, not policy)

> These exist because System 2 is the **only** component touching OANDA and must stay safe when System 3,
> the queue, and GCS are all unreachable. They are **mechanism and defense-in-depth**, not a copy of
> System 3's Risk Engine / circuit breakers (§2). Implemented under **EXEC-010** + EXEC-004; lives in
> `src/system2/execution/safety.py` and `src/system2/execution/validation.py`.

**7.1 Emergency STOP (the kill switch — mechanism + manual fallback).**
- **Triggers (any one):** (a) presence of `state/control/EMERGENCY_STOP` file flag; (b) `SIGUSR1`;
  (c) an authenticated `POST /control/emergency-stop` on the telemetry app *if it is running*; (d) a
  `halt`/`flatten` **command** received from System 3 over the queue/override channel.
- **Action sequence (idempotent, ordered):** cancel all pending OANDA orders → flatten open positions at
  market → set local mode `HALTED` (refuse all new orders) → persist `state/control/halted.json` →
  emit `system2.emergency_stop` control event to `AMS_Inbound_Queue` → write the local last-resort alert.
- **Robustness:** must complete using only OANDA HTTPS + local disk; queue/GCS/System-3 outages must not
  prevent the flatten. Re-entrancy safe (a second trigger is a no-op). Clearing `HALTED` is a **logged
  human decision** (`DECISIONS_LOG.md`), never automatic.
- **Boundary:** System 3 decides *when* to halt (its circuit breakers/human override); System 2 guarantees
  the halt *executes* and provides the standalone local trigger for when System 3 can't reach it.

**7.2 Last-line-of-defense order validation (sanity bounds, not sizing).**
- Runs in the outbound consumer (EXEC-004) **after** decode, **before** broker submit, on the
  already-sized order. Checks static absolute bounds from `config/.env.system2` (e.g. `MAX_UNITS_PER_PAIR`,
  `MAX_TOTAL_NOTIONAL`, `MAX_LEVERAGE`, `REQUIRE_STOP_LOSS=true`, tradeable-instrument + in-session).
- On violation: **REJECT** (never submit), log `CRITICAL`, publish a `system2.order_rejected` event
  (with `correlation_id` + reason) to `AMS_Inbound_Queue` — a **rejection, not a fill** — and continue.
- These ceilings are deliberately *loose* fat-finger guards set well above normal System-3 sizing; they
  catch corruption/100x errors, they do **not** re-decide risk.

**7.3 Poison-message / dead-letter handling.**
- A message that fails decode/validation/processing is retried up to N times (backoff), then moved to the
  configured **DLQ** (or `state/dlq/` for the local backend) with the failure context, and a
  `system2.message_dead_lettered` event is emitted. The consumer **never** infinite-loops or crashes the
  session on a bad message.

---

## 8. LIFECYCLE, GRACEFUL SHUTDOWN & BROKER RECONCILIATION (System 2's own — EXEC-010)

**8.1 Graceful shutdown.** On `SIGTERM`/`SIGINT` during an active session:
1. Stop consuming new messages from `AMS_Outbound_Queue` (no new orders).
2. Let in-flight OANDA requests complete (bounded grace window, e.g. 30s).
3. Atomically persist current position state + queue offset/ack state to
   `state/graceful_shutdown_{ts}.json` (+ flush buffered fills, §EXEC-005).
4. **Do NOT flatten positions** — flattening is an emergency-STOP / human decision, never a restart side
   effect. Open positions remain protected by their broker-side SL/TP.
5. Exit cleanly (zero exit code).

**8.2 Startup reconciliation (always, not just after graceful exit).**
1. If `state/graceful_shutdown_*.json` exists, load it as the *expected* state and then archive it.
2. **Pull authoritative truth from OANDA** (open positions, pending orders, recent fills). The **broker is
   source of truth** — stops/TPs/manual closes can fire out-of-band while System 2 was down.
3. Reconcile local `fact_live_trades`/position cache against OANDA; record any divergence to
   `fact_execution_log`, emit a `system2.reconciliation` event, and adopt OANDA's reality.
4. Resume the consumer from the saved offset in **PAUSED-until-queue-fresh** mode (§EXEC-008).

**8.3 Periodic reconciliation loop (runtime, not just startup).** On a fixed cadence during the session,
re-pull OANDA positions and reconcile against local belief; a mismatch beyond tolerance ⇒ log `WARNING`,
emit a `system2.reconciliation` event, adopt broker truth, and (if material) request human review. This is
how out-of-band SL/TP fills become known fills.

---

## 9. OBSERVABILITY — STRUCTURED LOGGING, HEALTH & HEARTBEAT (System 2's own)

**9.1 Logging standard** (`src/system2/common/logging.py`, used everywhere):
- JSON-structured logs to **stdout + rotating file** (`logs/system2.log`).
- Every entry carries: `timestamp` (UTC), `level`, `correlation_id`, `component`, `message`, `context`(dict).
- Levels: `DEBUG` (dev only), `INFO` (normal ops), `WARNING` (degradation/staleness/reconcile drift),
  `ERROR` (failed operation), `CRITICAL` (emergency-STOP / rejection / auth failure).
- Retention: 30 days local; forward to long-term storage only if configured.
- **No secrets, PII, or full API responses** — mask keys, truncate prices/payloads.

**9.2 Health / liveness** (on the EXEC-009 telemetry app): `GET /healthz` (process up + event-loop alive)
and `GET /readyz` (secrets loaded, GCS reachable, queue reachable, OANDA auth OK, local DB reachable,
not `HALTED`). Used by systemd/the runbook to detect a wedged process.

**9.3 Heartbeat to System 3 (emit, don't notify).** On a fixed cadence System 2 publishes a
`system2.heartbeat` control event to `AMS_Inbound_Queue` (`mode`: TRADING/PAUSED/HALTED, last queue
message age, open-position count, last reconcile time). **System 3's Notification Service decides what to
tell the human** — System 2 does not own alert routing.

**9.4 Local last-resort alert only.** For the one case System 3 cannot cover — *the queue itself is
unreachable* — System 2 may fire a single, lightweight local alert (a `CRITICAL` log line and an optional
single webhook URL from `config/.env.system2`). If that channel is down, log locally and continue. This is
a backstop, **not** a parallel notification stack (that is System 3, task 11).

---

## 10. SECRETS, CREDENTIALS & HUMAN-DECISION LOGGING

- **Secrets the agents must wire (sourced, never committed):** OANDA practice key + account id, OANDA
  live key + account id (separate, toggle-gated), GCS service-account JSON / read creds for the model
  bucket, **queue credentials (for the logged backend)**, the local DB password (per Phase-0 decision),
  and the optional local-alert webhook URL. Provide `config/.env.system2.template` with **names only**
  (no values) and add `config/.env.system2` to `.gitignore`. Startup validates presence and **fails
  closed** on any missing required secret.
- **Do not echo or persist secret values** into logs, ledgers, artifacts, or messages. The Security agent
  scans every outgoing bundle/message for credential-shaped strings before release.
- **`DECISIONS_LOG.md` (append-only) — log every human decision** with: `timestamp (UTC) · decision-id ·
  question · options considered · chosen option · rationale · decided-by · affected EXEC tasks`. Mandatory
  log points: the Phase-0 **connectivity** choice; the Phase-0 **queue-backend** choice; any credential the
  human supplies or rotates; any practice→live toggle; promotion of a new model artifact set into live
  inference; the legacy→slim Layer-4 cutover; **any clearing of `HALTED` mode**; and any auditor-gate
  override. When you reach one of these, **STOP and ask the human, then record their answer before
  proceeding.**

---

## 11. BACKGROUND EXECUTION & COST DISCIPLINE

Run long/continuous jobs **detached** and poll their state via the ledger; never block an agent on them.
Record a `background_jobs[]` entry (handle, command, log path, start, expected duration, poll cadence).
Jobs that belong in the background:
- The **session loop** itself (the long-running queue consumer / position manager / heartbeat + periodic
  reconcile during market hours).
- **EXEC-001** GCS polling loop (~15 min cadence) and large artifact downloads.
- **EXEC-002** HMM warm-up / batch regime inference over recent candles.
- Any **dual-run** comparison harness (legacy monolith vs slim Layer 4) for EXEC-003.

Cost discipline: extend the **copied** code rather than rewrite; keep sub-agents short-lived and
single-purpose; cache the verified artifact set + last-known-good; don't re-download an artifact whose
SHA256 already matches.

---

## 12. PER-TASK PLAYBOOK

For **every** task: ① read the spec under `docs/tasks/`; ② **re-read `SYSTEM_BOUNDARY.md`**; ③ load its
skills; ④ confirm upstream contract/artifact (or fallback) present and no open rework/block; ⑤ build by
**extending the copied code**, additive + feature-flagged; ⑥ run the spec's self-verification gates;
⑦ write `DONE_…`; ⑧ request the auditor `AG-EXEC-0XX`; ⑨ on pass, update ledger + decision log and release
downstream; on fail, run the rework loop. Outputs land at the exact contracted paths.

| Task | Owner | Skills to load | Copy-from monolith (refactor in place) | Key new outputs | Gate |
|---|---|---|---|---|---|
| **EXEC-001** Model downloader/validator | Artifact-Sync | object-storage-protocol, postgres-patterns | (storage abstraction from System-1 orchestration) | `src/system2/artifact_sync/downloader.py`, `state/model-cache/{version}/`, `state/last_known_good/`, atomic-swap + SHA256 verify | AG-EXEC-001 |
| **EXEC-002** Live regime detector | Artifact-Sync | hmm-semantic-mapping, point-in-time-leakage, postgres-patterns | `src/system1/regime/hmm_regime.py` (inference path), `src/layer1_regime/Fact_market_regime_v2.py` | `src/system2/artifact_sync/live_regime.py` (HMM predict on live candles + persistence smoothing, last-good fallback) | AG-EXEC-002 |
| **EXEC-003** Layer 4 → execution-only | Execution-Core | layer3-contract, point-in-time-leakage, postgres-patterns | `src/layer4_executor/live_pipeline.py`, `src/common/db.py` | `src/system2/execution/pipeline.py` (slim, feature-flagged), golden-file determinism tests, dual-run harness | AG-EXEC-003 |
| **EXEC-004** Outbound queue consumer + last-line validation + DLQ | Execution-Core | queue-decoupling, layer3-contract, postgres-patterns | (queue abstraction) | `src/system2/execution/outbound_consumer.py`, `validation.py` (§7.2), processed-`idempotency_key` store + DLQ under `state/` | AG-EXEC-004 |
| **EXEC-005** Fill confirmation producer | Execution-Core | queue-decoupling, postgres-patterns | (queue abstraction) | `src/system2/execution/fill_producer.py`, local buffer+retry, `correlation_id` mapping, control-event emitter | AG-EXEC-005 |
| **EXEC-006** Broker adapter hardening + reconcile | Broker | oanda-ingestion, postgres-patterns | `src/layer7/oanda_executor.py`, `src/layer6_auditor/trade_auditor.py` | `src/system2/broker/oanda_adapter.py` (idempotent submit, 2-pip slippage, stop/TP confirm, partial-fill reconcile, practice→live toggle, **OANDA↔local reconcile** §8), `docs/SCHEMA_ADDITIONS.md` | AG-EXEC-006 |
| **EXEC-007** Active position manager | Broker | oanda-ingestion, financial-metrics | `src/layer7/oanda_executor.py`, `src/layer6_auditor/trade_auditor.py` | `src/system2/broker/position_manager.py` (breakeven, trailing, time-based exits, gap-aware) | AG-EXEC-007 |
| **EXEC-008** Safety mode & BYPASS | Execution-Core | queue-decoupling, layer3-contract | — | `src/system2/execution/safety.py` (>5 min stale ⇒ PAUSE; audited opt-in BYPASS, conservative sizing only) | AG-EXEC-008 |
| **EXEC-009** Layer 5 AMS telemetry + health | Telemetry | postgres-patterns, layer3-contract | `src/layer5/` (FastAPI + React) | `src/system2/telemetry/` endpoints (`/api/account/state`, `/equity-curve`, `/decisions`, `/circuit-breakers` (read-only mirror), `/strategy-performance`, `/daily-summary`, `/healthz`, `/readyz`, `/control/emergency-stop`) + dashboard views | AG-EXEC-009 |
| **EXEC-010** Emergency STOP + graceful lifecycle + reconcile glue *(new)* | Execution-Core | queue-decoupling, layer3-contract, oanda-ingestion | (new) | `src/system2/execution/safety.py` (emergency STOP §7.1), `lifecycle.py` (SIGTERM/startup/periodic reconcile §8), DLQ wiring | AG-EXEC-010 |

> Skills live in `scalable-brain/docs/implementation-roadmap/system-1-model-building/tasks/skills/`
> (object-storage-protocol, queue-decoupling, postgres-patterns, oanda-ingestion, layer3-contract,
> point-in-time-leakage, hmm-semantic-mapping, financial-metrics). **Copy the ones referenced above into
> `system-2-execution-engine/docs/skills/`** so the system is self-contained on Computer 2.

---

## 13. FILE MIGRATION MANIFEST (copy / create — produce `docs/FILE_MIGRATION_MANIFEST.md` in Phase 0)

**Copy from the monolith (then refactor the copy; never edit the original):**
- `src/layer4_executor/live_pipeline.py` → execution core (slim to execution-only).
- `src/layer7/oanda_executor.py` → broker adapter + position manager base.
- `src/layer5/` (FastAPI backend + React/Vite frontend) → telemetry.
- `src/common/db.py` → `src/system2/common/db.py` (re-point to the Phase-0 datastore).
- `src/system1/regime/hmm_regime.py` + `src/layer1_regime/Fact_market_regime_v2.py` → live-regime inference.
- `src/layer6_auditor/trade_auditor.py` → reconciliation logic for the position manager + §8 reconcile.
- The **EXEC task specs + dependencies + README** → `system-2-execution-engine/docs/`.
- The **referenced skills** → `system-2-execution-engine/docs/skills/`.
- A pruned **`requirements.txt`** (only what System 2 runs: `oandapyV20`, `sqlalchemy`, **`alembic`**, DB
  driver, `pandas`, `numpy`, `ta`, `scikit-learn`, `hmmlearn`, `joblib`, `fastapi`, `uvicorn`, GCS SDK,
  queue client). Check before adding anything new.

**Create new (no monolith equivalent):**
- `MASTER_ORCHESTRATION_PROMPT.md` (this file), `ARCHITECTURE.md`, `RUNBOOK.md`.
- `orchestration/` (ledger json+md, `DECISIONS_LOG.md`, `FOLDER_STRUCTURE.md`,
  `AGENT_FLEET_TOPOLOGY.md`, `CONTINUATION_PROMPT.md`, **`CHECKPOINT.md` + `checkpoint.json`** (§14.1)).
- `docs/SYSTEM_BOUNDARY.md` (§2).
- `src/system2/common/storage_backend.py` + `queue_backend.py` + `secrets.py` + **`logging.py`**.
- `src/system2/execution/safety.py`, `validation.py`, `lifecycle.py` (§7–§8).
- `src/system2/artifact_sync/`, `execution/`, `broker/`, `telemetry/` packages + their `tests/`.
- `migrations/` (Alembic) for the local datastore.
- `config/.env.system2.template`, `.gitignore`, `docs/SCHEMA_ADDITIONS.md`,
  `docs/FILE_MIGRATION_MANIFEST.md`, `docs/STORAGE_AND_QUEUE_ABSTRACTION.md`.
- *(Optional, recommended)* `Dockerfile`, `docker-compose.yml` (§16).

**Do NOT bring across, and do NOT build (System 3 owns it — §2):** position sizing / Risk Engine,
portfolio circuit breakers, the Account State Machine, the Notification Service, human-override policy,
signal scoring / Decision Gate, or any back-import into `scalable-brain/`.

---

## 14. RESUMABILITY PROTOCOL (so any LLM can take over)

After every meaningful step: ① append an **event** to `progress_ledger.json` and update the task
snapshot; ② set the task's `next_action` to the precise next step; ③ append any human decision to
`DECISIONS_LOG.md`; ④ keep all artifacts at contracted paths so provenance walks back to the OANDA call /
the queue message. On a fresh start: read the ledger tail + `DECISIONS_LOG.md` + `CONTINUATION_PROMPT.md`,
re-verify the last green gate, **run the startup reconciliation (§8.2)**, then execute `next_action`. If
the ledger is ambiguous, re-run the last audit gate to establish ground truth — never "randomly continue."

### 14.1 CONTINUITY CHECKPOINTING (survive rate-limits, crashes, context cutoffs)

The ledger records *what is done*; the **checkpoint** records *what was happening the instant the agent
stopped*. A rate-limit or token cutoff can hit **mid-step** — between deciding to submit an order and
confirming it, or between writing a file and updating the ledger. The **Continuity Sentinel** keeps a tiny,
always-current record so the **next agent (any model, after the limit resets) resumes in seconds, not by
re-deriving the world.**

**When the checkpoint is written (whichever comes first):**
- **Before** every non-trivial or side-effecting action (file write, broker call, queue publish, gate
  request, human-decision STOP), recording the *intent* — "about to do X; if you see this without a
  matching completion event, verify X did/didn't happen."
- **After** that action completes, flipping the intent to a result.
- On a **short wall-clock cadence** (e.g. every 60–90s) during any long-running step, so even a silent
  death leaves a checkpoint ≤ ~1 step stale.
- The write is **atomic** (write temp → `fsync` → rename) so a checkpoint is never half-written, and it is
  **append-aware**: keep the last N checkpoints in `checkpoint.json` history; `CHECKPOINT.md` always shows
  the single newest one for a human to read at a glance.

**What every checkpoint must contain (keep it small — this is a breadcrumb, not a log):**
```json
{
  "ts_utc": "2026-06-29T14:03:22Z",
  "agent": "Execution-Core",            // which role was active
  "phase_or_task": "EXEC-004",          // where in the DAG
  "step": "submitting first OANDA practice order for correlation_id=...",
  "status": "in_flight | completed | blocked | awaiting_human",
  "intent": "exactly what this step is trying to achieve + the precise next_action",
  "side_effects_in_flight": ["OANDA submit idempotency_key=abc123 (UNCONFIRMED)"],
  "verify_on_resume": ["query OANDA for client order abc123 before re-submitting (idempotency makes re-submit safe)"],
  "files_touched": ["src/system2/execution/outbound_consumer.py (written, ledger NOT yet updated)"],
  "open_gates_or_blocks": ["AG-EXEC-004 not yet requested"],
  "do_not_repeat": ["do NOT re-run migrations; already at head"],
  "resume_pointer": "ledger event-id 184; CONTINUATION_PROMPT.md §EXEC-004"
}
```

**Resume contract (first thing any new/resumed agent does):**
1. Read `CHECKPOINT.md`/`checkpoint.json` **before** the ledger. If `status` is `in_flight`, an action may
   have partially happened — **do not blindly redo it.**
2. Run every item in `verify_on_resume` to establish ground truth (idempotency + §8.2 broker reconciliation
   make this safe: re-submitting the same `idempotency_key` is a no-op; the broker is source of truth).
3. Reconcile the checkpoint against the ledger's last event. If the checkpoint shows a side effect the
   ledger never confirmed, finish or record it, then continue from `resume_pointer`.
4. Only then proceed with `next_action`. Honor `do_not_repeat` to avoid double-execution.

**Operating rules:** the Sentinel never makes EXEC decisions and never blocks the working agent (best-effort,
fire-and-forget, local-disk only — must keep working even with queue/GCS/System-3 down). It writes **no
secrets** (same masking as §9.1). The checkpoint **complements** the ledger and `DECISIONS_LOG.md`; on any
conflict, the **broker + ledger are authoritative**, the checkpoint is the breadcrumb that tells you *where
to look first*. If run single-threaded (no sub-agents), the active agent performs this checkpoint write
inline at the same trigger points — the protocol is identical.

---

## 15. DEFINITION OF DONE (System 2)

- **Phase 0 signed off** (architecture + logged **connectivity** decision + logged **queue-backend**
  decision + approved folder/migration + committed `SYSTEM_BOUNDARY.md`).
- All **EXEC-001..010 acceptance criteria** green; all **AG-EXEC-001..010 + AG-EXEC-CROSS** passed.
- A change to `latest.json` in **GCS** is detected, downloaded, SHA256-verified, atomically swapped with
  zero partial reads; corrupt/checksum-mismatch artifacts are refused and last-known-good is retained.
- An order on `AMS_Outbound_Queue` ⇒ passes last-line validation ⇒ **exactly one** OANDA practice order
  with SL/TP confirmed; a matching fill appears on `AMS_Inbound_Queue` within budget; **replaying the same
  `idempotency_key` is a no-op**; a bad message lands in the **DLQ**, not an infinite loop.
- Stalling `AMS_Outbound_Queue` > 5 min ⇒ Layer 4 **PAUSED**, no orders; **BYPASS** is opt-in + audited.
- **Emergency-STOP drill passes:** triggering the file flag / `SIGUSR1` cancels pending orders + flattens
  positions + enters `HALTED` + emits the control event — proven to work **with the queue and GCS down**.
- **Graceful-restart drill passes:** `SIGTERM` persists state without flattening; on restart the system
  reconciles against OANDA, adopts broker truth for any out-of-band SL/TP fill, and resumes PAUSED.
- Slippage measured per fill; > 2-pip flagged/rejected per policy; partial fills reconciled.
- Layer 5 exposes the AMS read endpoints + dashboard + `/healthz`/`/readyz`; heartbeat events reach
  `AMS_Inbound_Queue`; **no decision logic / no notification routing** duplicated from System 3.
- **DB migrations:** schema changes apply via numbered, idempotent migrations run at startup before the
  app accepts work.
- **Rollback proven:** a human can toggle the feature flag to re-enable the legacy monolith path within
  5 minutes with zero position disruption; steps documented in `RUNBOOK.md` §"Emergency Rollback."
- **Cold-transfer proven:** a clean `cp -r` (or built container image) + secrets + `pip install`/`docker
  run` runs on a fresh box; no import or path reaches back into `scalable-brain/`. Storage=GCS,
  queue=configured backend — both by config, not code.
- `progress_ledger.json` shows every task `done`; `DECISIONS_LOG.md` captures every human call.
- **Continuity proven:** killing the active agent mid-step leaves a ≤ ~1-step-stale `CHECKPOINT.md`; a
  fresh agent reads it first, runs `verify_on_resume`, and continues without redoing or double-executing
  any side effect (§14.1).

---

## 16. PACKAGING & DEVOPS — TRANSFER TO COMPUTER 2 (DevOps agent, final step)

Produce a **`RUNBOOK.md`** and a transfer bundle so the human can move the folder out and run it.

**16.1 Transfer artifact (pick per Phase-0, default A):**
- **(A) Folder bundle (default):** the whole `system-2-execution-engine/` (code + `docs/` +
  `orchestration/` + `config/` templates + `migrations/`), **excluding** `config/.env.system2`, `state/`,
  caches, and any secret.
- **(B, recommended add-on) Container image:** a `Dockerfile` (`python:3.12-slim`) + `docker-compose.yml`
  (LocalFSBackend + local-durable queue for dev) so the **image is the deterministic transfer artifact**.
  Secrets are mounted via `--env-file`/Docker secrets, **never baked in**.

**16.2 On Computer 2:** create the Python 3.12 venv + `pip install -r requirements.txt` (or `docker run`),
drop the real `config/.env.system2` (OANDA, GCS, queue, DB, optional webhook), set `STORAGE_PROVIDER=gcs`
+ bucket and the `QueueBackend` provider, verify clock/NTP (UTC), **run migrations to latest**, then run
the startup self-check (secrets present → GCS reachable → queue reachable → OANDA practice auth → local DB
reachable → migrations current) **fail-closed** on any miss.

**16.3 First run:** warm artifact cache (EXEC-001) + regime (EXEC-002) before session open; start the
session loop in **PAUSED-until-queue-fresh** mode; **run the startup reconciliation (§8.2)**; confirm a
practice round-trip (order→validate→fill→inbound) and an **emergency-STOP drill** before any practice→live
decision (which is its own logged human gate).

**16.4 Emergency Rollback (document exact steps in `RUNBOOK.md`):** toggle the EXEC-003 feature flag back
to the legacy monolith path; confirm open positions are untouched (broker-side SL/TP intact); verify the
slim path stops consuming; record the action in `DECISIONS_LOG.md`. Target: < 5 minutes, zero position
disruption.

---

## Appendix — recommended folder map (structure-coherency owns the final version)

```
system-2-execution-engine/
├── MASTER_ORCHESTRATION_PROMPT.md      # this file (the seed)
├── ARCHITECTURE.md                     # Phase 0
├── RUNBOOK.md                          # DevOps / transfer guide (+ Emergency Rollback)
├── requirements.txt                    # pruned (incl. alembic)
├── Dockerfile / docker-compose.yml     # optional, recommended (§16)
├── .gitignore                          # ignores config/.env.system2, state/, caches
├── config/
│   └── .env.system2.template           # names only, no secret values
├── migrations/                         # Alembic numbered, idempotent migrations
├── orchestration/
│   ├── progress_ledger.json / .md
│   ├── DECISIONS_LOG.md                # append-only human decisions
│   ├── CHECKPOINT.md / checkpoint.json # live "last breath" for rate-limit/crash resume (§14.1)
│   ├── FOLDER_STRUCTURE.md
│   ├── AGENT_FLEET_TOPOLOGY.md
│   └── CONTINUATION_PROMPT.md
├── docs/
│   ├── README.md                       # copied EXEC overview
│   ├── 00-dependencies-and-prerequisites.md
│   ├── tasks/01..09-*.md               # copied EXEC specs
│   ├── skills/*.md                     # copied referenced skills
│   ├── SYSTEM_BOUNDARY.md              # what System 2 does NOT do (§2)
│   ├── STORAGE_AND_QUEUE_ABSTRACTION.md
│   ├── FILE_MIGRATION_MANIFEST.md
│   └── SCHEMA_ADDITIONS.md
├── logs/                               # git-ignored: rotating system2.log (JSON)
├── src/system2/
│   ├── common/        (db.py, storage_backend.py, queue_backend.py, secrets.py, logging.py)
│   ├── artifact_sync/ (downloader.py, live_regime.py, tests/)
│   ├── execution/     (pipeline.py, outbound_consumer.py, fill_producer.py, safety.py,
│   │                   validation.py, lifecycle.py, tests/)
│   ├── broker/        (oanda_adapter.py, position_manager.py, tests/)
│   └── telemetry/     (api/, frontend/, tests/)
├── tests/                              # cross-module: determinism + idempotency + emergency-stop +
│                                       #   graceful-restart/reconcile + poison-message/DLQ drills
└── state/                             # git-ignored: model-cache/, last_known_good/, queue/, offsets/,
                                       #   dlq/, control/ (EMERGENCY_STOP flag, halted.json), graceful_shutdown_*.json
```

— END OF MASTER ORCHESTRATION PROMPT (v2) —
```
