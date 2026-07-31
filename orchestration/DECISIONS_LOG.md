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
