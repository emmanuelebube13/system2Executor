# System 2 — Dependencies & Prerequisites

> Everything that must exist, be provisioned, or be accessible **before** any EXEC-XXX task can start. This is the "you cannot begin until these are true" checklist for Computer 2.

System 2 runs on **Computer 2** during market hours (Sun 22:00 – Fri 20:00 UTC). It consumes artifacts from System 1, orders from System 3, and talks to OANDA. The dependencies below are grouped by category; each entry says **what** is needed and **why**.

---

## 1. Object storage (artifact exchange) — gates EXEC-001/002

**What:** Read access from Computer 2 to the shared object store provisioned in **FND-001**, including:
- A bucket/prefix where System 1 publishes models (`MODEL-007`), e.g. `models/champion/` and `models/regime/`.
- A pointer manifest `latest.json` describing the current active artifact set + per-file SHA256 checksums.
- Scoped, **read-only**, least-privilege credentials for Computer 2 (no write/delete to the model prefix).

**Why:** EXEC-001 polls `latest.json` every ~15 min, downloads changed artifacts, verifies checksums, and atomically swaps the active set. EXEC-002 loads the regime (HMM) model from that set. Without read access and a stable manifest contract, Computer 2 cannot become self-sufficient for inference and would have to share a filesystem (explicitly disallowed by the reorg).

**Prerequisite tasks:** FND-001 (provision storage), MODEL-007 (serializer/publisher writes `latest.json` + checksums), MODEL-003 (regime model present in the artifact set).

---

## 2. Message queue + IPC (order/fill transport) — gates EXEC-003/004/005/008

**What:** Access from Computer 2 to the message broker provisioned in **FND-002**, with two logical queues/topics:
- `AMS_Outbound_Queue` — System 3 → System 2: approved, pre-sized orders. Computer 2 needs **consume** rights.
- `AMS_Inbound_Queue` — System 2 → System 3: fill confirmations. Computer 2 needs **produce** rights.
- The canonical message envelope/contract (schema_version, message_id, idempotency_key, correlation_id, granularity, created_at) defined in FND-002 and aligned with AMS-008.

**Why:** EXEC-004 replaces direct Layer 3 reads with consuming approved orders from `AMS_Outbound_Queue`; EXEC-005 publishes fills to `AMS_Inbound_Queue`; EXEC-008 needs the queue's last-message timestamp / consumer lag to detect staleness (> 5 min → PAUSE). No queue ⇒ no safe risk-approved execution path.

**Prerequisite tasks:** FND-002 (queue + IPC + contract), AMS-008 (System 3 produces the outbound orders).

---

## 3. OANDA broker access (practice + live) — gates EXEC-003/006/007

**What:**
- **OANDA practice** API key + practice account id (already present: `OANDA_ACCOUNT_ID_DEMO=101-002-38449021-001`, `OANDA_URL=https://api-fxpractice.oanda.com`). Used for all development, dual-run, and live-sim validation.
- **OANDA live** API key + live account id — provisioned and stored separately, **only** to be activated by the explicit practice→live toggle (EXEC-006). Live trading uses `https://api-fxtrade.oanda.com`.
- Network egress (HTTPS) to OANDA REST + the **pricing stream** endpoint.
- v20 endpoints in use: `/v3/instruments/{instrument}/candles` (live candles for regime/ATR), `/v3/accounts/{id}/summary` (account summary), `/v3/accounts/{id}/openPositions` + `/openTrades` (open-position management), `/v3/accounts/{id}/transactions` (fill/transaction reconciliation), and the `/v3/accounts/{id}/pricing/stream` pricing stream (EXEC-007 monitoring).

**Why:** Layer 7 constructs and submits orders, confirms stops/TP, validates fills, and streams prices for active position management. The practice/live separation prevents accidental real-money trading.

**Prerequisite tasks:** FND-003 (store OANDA practice + live keys as secrets; never in `.env`/git).

---

## 4. Database / ODBC access — gates EXEC-003/006/009

**What:**
- Reachability from Computer 2 to the canonical datastore (`ForexBrainDB` strategy ratified in FND-004), with a working ODBC driver (18/17, the auto-detect Layer 4 already performs).
- Write access to `Fact_Live_Trades` and `Fact_Execution_Log` (Layer 4 outputs).
- The **schema additions** documented in EXEC-006 applied to `Fact_Live_Trades` (Broker_Order_ID, Fill_Price, Fill_Time, Slippage_Pips, Model_Threshold, Regime_Label, Correlation_Score, Correlation_Passed, Updated_At) — these do **not** exist today.
- Read access for Layer 5 to whatever AMS state store/views System 3 exposes (AMS-001/AMS-009).

**Why:** Execution must persist fills and an execution audit trail; Layer 5 must read AMS account telemetry. The new fill metadata cannot be recorded until the schema is extended.

**Prerequisite tasks:** FND-004 (datastore decision), AMS-001 (AMS schema), EXEC-006 (documents the `Fact_Live_Trades` additions).

---

## 5. Dashboard hosting (Layer 5) — gates EXEC-009

**What:**
- A host/process for the FastAPI telemetry backend (currently `src/layer5/run.py`, port 8001) and the React/Vite frontend build, reachable on the **FND-008 private network only** (no public ingress), with auth.
- Read connectivity to AMS account-state endpoints/views (AMS-009).

**Why:** EXEC-009 adds AMS account-telemetry read endpoints and dashboard views. The dashboard must be reachable by the operator but not exposed publicly (it surfaces account state).

**Prerequisite tasks:** FND-008 (networking/security), AMS-001/AMS-009 (state + read API).

---

## 6. Secrets, networking, observability (cross-cutting) — gates all EXEC tasks

**What & why:**
- **FND-003 (secrets):** OANDA practice + live keys, queue credentials, object-storage read keys, DB credentials — all sourced from the secrets layer, never committed. System 2 startup must fail closed if a required secret is missing.
- **FND-008 (networking):** Computer 2 reaches the queue, object store, DB, and AMS state over the encrypted private network (VPN + TLS); OANDA over HTTPS.
- **FND-005 (observability/SLOs):** logging/metrics/alerting so System 2's latency budgets (poll-to-submit < ~2 s, fill-publish < ~5 s), queue-staleness alarms, slippage-breach alerts, and "Computer 2 offline during session" alerts are monitored.

---

## 7. Local runtime prerequisites on Computer 2

- Python 3.12 venv with `oandapyV20`, `pyodbc`, `sqlalchemy`, `pandas`, `numpy`, `ta`, `scikit-learn`, plus the regime model's runtime (e.g. `hmmlearn` / `joblib`) — check `requirements.txt` before adding anything new.
- A local **artifact cache directory** for the atomically-swapped model set (EXEC-001) and a **last-known-good** copy.
- A local **state directory** for consumer offsets / processed `idempotency_key`s (idempotency + restart safety).
- Rotating log handlers (10 MB / 14 backups) per existing convention.
- Correct system clock / NTP (UTC) — staleness detection (EXEC-008) and market-hours guards depend on accurate time.

---

## Dependency ordering summary

| Need | Provided by | Blocks |
|------|-------------|--------|
| Object storage read + `latest.json` | FND-001, MODEL-007 | EXEC-001, EXEC-002 |
| Regime model in artifact set | MODEL-003 | EXEC-002 |
| Queue consume/produce + contract | FND-002, AMS-008 | EXEC-003, EXEC-004, EXEC-005, EXEC-008 |
| OANDA practice + live + pricing stream | FND-003 | EXEC-003, EXEC-006, EXEC-007 |
| DB/ODBC + schema additions | FND-004, AMS-001, EXEC-006 | EXEC-003, EXEC-006, EXEC-009 |
| Dashboard host + AMS read API | FND-008, AMS-001, AMS-009 | EXEC-009 |
| Secrets / network / observability | FND-003, FND-008, FND-005 | all EXEC tasks |
