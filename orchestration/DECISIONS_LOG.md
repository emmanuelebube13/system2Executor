# DECISIONS LOG (append-only)

> Every human decision. Format: `decision-id · timestamp(UTC) · question · options · chosen ·
> rationale · decided-by · affected EXEC tasks`. **Never re-decide a logged decision; never proceed
> past an unlogged decision that needs a human.**

---

### D-001 — Phase-0 connectivity / persistence model
- **Timestamp (UTC):** 2026-06-29
- **Question:** How does System 2 (Computer 2) persist fills + execution audit trail, given it may
  not reach Computer 1's Postgres?
- **Options considered:** (A) Local datastore on Computer 2; (B) Remote Postgres over VPN/TLS to
  canonical `ForexBrainDB`.
- **Chosen:** **(A) Local datastore on Computer 2.**
- **Resulting contract:** Local store holds `fact_live_trades` + `fact_execution_log` (+ EXEC-006
  schema additions). Default engine **local PostgreSQL** (reuses `common/db.py`, SQLAlchemy 2.0,
  `INSERT … ON CONFLICT`); **SQLite** via same `db.py` URL for offline dev/tests. Post-trade state
  reaches System 3 **only** via `AMS_Inbound_Queue` — never a shared DB. Alembic migrations at
  startup. (Postgres-vs-SQLite is an implementation choice with this default, not a further human gate.)
- **Rationale:** Honors "this computer is not network-connected to Computer 1"; no live cross-computer
  DB dependency.
- **Decided-by:** Emmanuel (human)
- **Affected:** EXEC-003, EXEC-005, EXEC-006, EXEC-009; `common/db.py`, `migrations/`.

---

### D-002 — Phase-0 queue backend
- **Timestamp (UTC):** 2026-06-29
- **Question:** Which message-queue backend binds behind `QueueBackend` for `AMS_Outbound_Queue` /
  `AMS_Inbound_Queue`?
- **Options considered:** Google Cloud Pub/Sub; RabbitMQ; Redis Streams; Local durable (dev/test).
- **Chosen:** **Google Cloud Pub/Sub** (`QUEUE_PROVIDER=pubsub`).
- **Resulting contract:** Envelope per `docs/STORAGE_AND_QUEUE_ABSTRACTION.md`; **DLQ** = Pub/Sub
  dead-letter topic (`QUEUE_DLQ_TOPIC`), `QUEUE_MAX_DELIVERY_ATTEMPTS=5`. `LocalDurableBackend`
  (`QUEUE_PROVIDER=local`, `state/queue/`) is the dev/test backend behind the same interface — build
  EXEC-004/005 against `local` until Pub/Sub creds are provisioned.
- **Rationale:** Works over HTTPS (no VPN) — matches the GCS/HTTPS isolation story; built-in
  ack/redelivery/DLQ; low latency, high durability.
- **Decided-by:** Emmanuel (human)
- **Affected:** EXEC-004, EXEC-005, EXEC-008, EXEC-010; `common/queue_backend.py`.

---

### D-003 — Phase-0 sign-off
- **Timestamp (UTC):** 2026-06-29
- **Status:** ✅ APPROVED — build may proceed.
- **Question:** Are the Phase-0 artifacts (ARCHITECTURE.md, SYSTEM_BOUNDARY.md,
  STORAGE_AND_QUEUE_ABSTRACTION.md, FILE_MIGRATION_MANIFEST.md, FOLDER_STRUCTURE.md, this log)
  approved to proceed to build, gate AG-EXEC-000?
- **Chosen:** Approved as-is; AG-EXEC-000 green (7/7 criteria).
- **Decided-by:** Emmanuel (human)
- **Affected:** unblocks EXEC-001..010.

---

### D-004 — EXEC-003 shadow→live cutover  ⏳ PENDING (mandatory before any live order)
- **Status:** ⏳ NOT YET REQUESTED — build DAG complete (EXEC-001..010, 144/144 tests) but the slim
  execution path runs in **SHADOW** (`EXEC_SHADOW` defaults true in `build_from_secrets`; `OANDA_ENV`
  defaults `practice`). No real order is submitted until this decision is logged APPROVED.
- **Preconditions to request:** (1) DevOps/packaging done; (2) a **practice integration drill** passes
  (real practice creds: order consumed → OANDA fill → fill on `AMS_Inbound_Queue` → dual-run vs legacy
  matches → PAUSE/STOP drills exercised); (3) human reviews the drill evidence.
- **Question (when raised):** Flip EXEC-003 out of shadow (`EXEC_SHADOW=false`) and/or `OANDA_ENV=live`?
- **Decided-by:** Emmanuel (human) — REQUIRED. Do not self-approve.
- **Affected:** EXEC-003 shadow flag, EXEC-006 practice→live toggle, EXEC-010 runtime.

<!-- Future mandatory log points: any credential supplied/rotated; any practice→live toggle (see D-004);
     model artifact set promoted into live inference; legacy→slim Layer-4 cutover; any clearing of HALTED;
     any auditor-gate override. STOP and ask the human, then record here, before proceeding. -->

---

### D-005 — 2026-W31 remediation: the circuit breaker stays CLOSED
- **Timestamp (UTC):** 2026-07-31
- **Status:** ✅ DECIDED — breaker remains engaged; no reset.
- **Question:** The account has been `CIRCUIT_BROKEN` since the 14-trade / −3,693 CAD run, and
  System 3 is rejecting 100% of decisions at Layer A. Should the breaker be reset so execution-path
  fixes can be verified end to end?
- **Chosen:** **No.** The breaker stays closed for the duration of the remediation campaign. It is
  not to be revisited until BOTH (a) FIX_PLAN Group 1 has landed and been independently verified,
  and (b) System 1's §9 question — why all realised trades lost — has been answered with the
  per-trade live-vs-backtest gap analysis.
- **Rationale:** an unlocked account combined with the still-live duplicate-order paths
  (F-206 + F-303, both P0, both reproduced on 2026-07-31) is the one combination that can actually
  lose money. Verification of execution-path fixes will be done against harnesses and shadow
  payloads, not against a live account. Per the S1 handoff §0: correct sizing of a
  negative-edge strategy loses money *faster*; the jam is currently the only thing preventing
  further loss.
- **Note on scope:** the account is OANDA **practice** (`101-002-38449021-001`, mode `demo`,
  stage `paper`), so the recorded losses are not real capital. The decision stands regardless —
  the defects would be real on a live account, and D-004 (shadow→live cutover) is still PENDING.
- **Decided-by:** Emmanuel (human) — via the 2026-07-31 remediation plan approval.
- **Recorded-by:** ORCHESTRATOR-v2
- **Affected:** blocks any `/reset_breaker`; gates FIX_PLAN Group 1 verification strategy;
  interacts with D-004 and with owner decisions OD-3/OD-4 in `audit/state/orchestrator-state.json`.

---

### D-006 — Security hardening is deferred until the account holds real money
- **Timestamp (UTC):** 2026-08-15
- **Status:** ✅ DECIDED — accept the current exposure; revisit on a named trigger.
- **Question:** EXEC-4 surfaced two security items needing an owner decision: the Cloud Run
  dashboard is invokable by `allUsers` (S2-17), and an exported `system1-rw@` key with
  `objectAdmin` on the artifact bucket sits on the trading VM (F-601b / S2-26). Fix now, or defer?
- **Chosen:** **Defer both. Do nothing for now.** No IAM change, no key rotation, no ingress
  change. Engineering effort goes to the money path instead.
- **Rationale (owner's, recorded as given):** the broker account is OANDA **practice**, not real
  capital; access to the environment is limited to the owner; and the system is still being built
  toward its core purpose — *make money and conserve money*. Security work that does not move that
  purpose forward is not the best use of the next block of effort. The residual risk was
  quantified before deciding, not assumed: an unauthenticated caller reaches the static dashboard
  bundle and **nothing else** (all five `/api/*` endpoints return 401, verified 2026-08-15), and
  the remote-code-execution path is gone.
- **REVISIT TRIGGER (owner-named, binding):** **before** the account is switched from practice to
  real money. This is the same gate as D-004 (shadow→live cutover). Treat D-006 as a blocking
  prerequisite of D-004: the cutover checklist must not be signed off while D-006 is still
  "deferred". Also revisit if the environment ever gains a second human user, or if the dashboard
  is shared outside the owner.
- **What is knowingly accepted:**
  1. The dashboard stays permanently reachable from the internet, so any *future* regression that
     re-registers a write endpoint is immediately public. This has happened once (the chat
     endpoint stayed live eight days after being reported fixed) — the exposure is what turned a
     code mistake into a public one.
  2. A credential that can **delete** the model-artifact store sits on the VM at mode `0666`. The
     realistic failure here is not an attacker but an accident or a bug exercising delete rights;
     the cost would be a rebuild/republish from System 1, not a capital loss.
  3. F-601 (`trading-vm@` → `objectViewer`) is consequently **also parked**, since executing it
     alone would report a privilege reduction that does not exist (see F-601b).
- **Decided-by:** Emmanuel (human) — 2026-08-15, in response to the EXEC-4 report.
- **Recorded-by:** EXEC-4
- **Affected:** parks S2-17, S2-18/F-601, S2-26/F-601b. Does **not** affect D-005 (breaker stays
  closed). Blocks sign-off of D-004 until reopened.
