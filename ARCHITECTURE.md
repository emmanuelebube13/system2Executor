# ARCHITECTURE — System 2, "The Hand" (Execution Engine)

> Phase-0 design artifact. Produced before any EXEC code. Reviewed with the human; both Phase-0
> human decisions are logged in `orchestration/DECISIONS_LOG.md` (D-001 connectivity, D-002 queue).
> Companion docs: `docs/SYSTEM_BOUNDARY.md`, `docs/STORAGE_AND_QUEUE_ABSTRACTION.md`,
> `docs/FILE_MIGRATION_MANIFEST.md`.

## 1. Mission

Turn **risk-approved, pre-sized orders** (from System 3 over `AMS_Outbound_Queue`) into **real
OANDA fills**, manage open positions to close, and stay **self-sufficient for inference** (pull +
verify own model artifacts from GCS; compute the live regime locally). Fail safe, deterministic,
idempotent. Standalone and transferable to Computer 2.

## 2. Component map

```
                          ┌──────────────────────────── System 2 (Computer 2) ────────────────────────────┐
                          │                                                                                │
  GCS (models/latest.json)│   ┌─────────────┐        ┌──────────────────────────────────────────┐         │
  ───────────────────────────▶│ artifact_   │ atomic │              execution                    │         │
        (SHA256 verify)   │   │ sync        │  swap   │  pipeline (slim L4, execution-only)       │         │
                          │   │ downloader  ├────────▶│  outbound_consumer ─ validation(§7.2)     │         │
                          │   │ live_regime │        │   │            │                           │         │
                          │   └─────────────┘        │   ▼            ▼                           │         │
 AMS_Outbound_Queue       │                          │ fill_producer  safety(§7.1) / lifecycle(§8)│         │
  (Pub/Sub) ───────────────────────────────────────▶│   │            │  emergency STOP, reconcile│         │
        orders in         │                          │   │            └──────────┬────────────────┘         │
                          │                          │   │                       │                          │
 AMS_Inbound_Queue        │                          │   ▼                       ▼                          │
  (Pub/Sub) ◀────────────────── fills + control ─────┤ broker: oanda_adapter ─ position_manager ──▶ OANDA   │
        fills/events out  │                          └───────────────┬──────────────────────────┘  (HTTPS) │
                          │                                           ▼                                      │
                          │   common: db.py · storage_backend · queue_backend · secrets · logging          │
                          │   telemetry: FastAPI read endpoints + dashboard + /healthz /readyz /control     │
                          │   local datastore (Postgres/SQLite): fact_live_trades, fact_execution_log       │
                          └────────────────────────────────────────────────────────────────────────────────┘
```

**Packages** (`src/system2/`): `common/`, `artifact_sync/`, `execution/`, `broker/`,
`telemetry/`, plus safety/lifecycle inside `execution/`.

## 3. Data & control flow

**Order path (steady state):** `AMS_Outbound_Queue` → decode envelope → **idempotency check**
(processed-key store) → **last-line sanity validation** (§7.2; stop-loss present, per-pair max
units, max notional, max leverage, tradeable + in-session) → `oanda_adapter` idempotent submit
(SL/TP confirmed, ≤2-pip slippage) → persist to local `fact_live_trades`/`fact_execution_log` →
publish fill to `AMS_Inbound_Queue` (`correlation_id` ties fill↔order). Poison message ⇒ DLQ.

**Artifact path:** poll GCS `latest.json` (~15 min) → download changed files → SHA256 verify →
atomic swap into `state/model-cache/{version}/` → update `state/last_known_good/`. Mismatch or
GCS down ⇒ retain last-known-good.

**Regime path:** `live_regime` loads the HMM from the verified artifact set, predicts on live
OANDA candles with persistence smoothing; last-good fallback if inference fails.

## 4. Isolation model (the disconnected-computer contract)

Only three trust boundaries, all HTTPS/TLS, all credentialed from the secrets layer: **GCS**,
**Pub/Sub**, **OANDA**. No path assumes Computer 1's Postgres, a shared FS, or System 1/3 online.
System 1 & 3 are **eventually-present mailboxes**. Persistence is **local** to Computer 2
(decision D-001). See `docs/STORAGE_AND_QUEUE_ABSTRACTION.md` for per-channel failure behavior.

## 5. Emergency STOP + reconciliation flow (System 2's own — §7/§8)

**Emergency STOP** (any of: `state/control/EMERGENCY_STOP` flag, `SIGUSR1`, authenticated
`POST /control/emergency-stop`, or a queue `halt`/`flatten` command): cancel all pending OANDA
orders → flatten open positions at market → set local `HALTED` (refuse new orders) → persist
`state/control/halted.json` → emit `system2.emergency_stop` → local last-resort alert. **Works
with queue/GCS/System-3 all down** (OANDA HTTPS + local disk only). Re-entrant. Clearing `HALTED`
is a logged human decision.

**Graceful shutdown** (`SIGTERM`/`SIGINT`): stop consuming → let in-flight broker calls finish
(≤30s) → atomically persist position + queue offset → flush buffered fills → **do NOT flatten** →
exit 0. **Startup/periodic reconciliation:** OANDA is **source of truth** — pull open positions /
pending orders / recent fills, reconcile local belief, record divergence, emit
`system2.reconciliation`, adopt broker reality; resume **PAUSED-until-queue-fresh**.

## 6. Budgets (from README)

- Outbound poll → order submit: **< ~2 s** (H1).
- Fill → `AMS_Inbound_Queue` publish: **< ~5 s**.
- Slippage tolerance: **2 pips** (flag/reject beyond).
- Artifact change detection: within **~15 min**.

## 7. Determinism & idempotency

Same approved order + model set + ATR inputs ⇒ byte-identical broker order (entry/SL/TP/units).
Golden-file tests (EXEC-003). `idempotency_key` ⇒ OANDA client request id; replays no-op
(processed-key store under `state/offsets/`). Verified by AG-EXEC-CROSS.

## 8. Persistence (decision D-001 — Local datastore on Computer 2)

Local datastore on Computer 2 holds `fact_live_trades` + `fact_execution_log` (+ EXEC-006 schema
additions). Default engine **local PostgreSQL** for fidelity with the reused `common/db.py`
(SQLAlchemy 2.0, `INSERT … ON CONFLICT`, TimescaleDB-compatible); **SQLite** supported via the
same `db.py` URL for offline dev/tests. Schema changes apply via **Alembic** migrations run at
startup before work is accepted. Post-trade state reaches System 3 **only** via `AMS_Inbound_Queue`
— never a shared DB.

## 9. Observability (§9)

JSON logs → stdout + rotating `logs/system2.log`, every line carrying `correlation_id`; no
secrets/PII. `GET /healthz` (process+loop alive), `GET /readyz` (secrets, GCS, queue, OANDA auth,
local DB, not HALTED). Periodic `system2.heartbeat` to `AMS_Inbound_Queue` (mode, queue age, open
positions, last reconcile). Local last-resort alert only when the **queue itself** is down.

## 10. Build sequence

Phase 0 (this) → EXEC-001→002 (artifact/regime, parallel) ‖ EXEC-003 (slim L4) → EXEC-004
(consumer+validation+DLQ) → EXEC-008 (staleness PAUSE) + EXEC-010 (emergency STOP + lifecycle) →
EXEC-005 (fills) ‖ EXEC-006→007 (broker depth) → EXEC-009 (telemetry+health). Critical path:
**Phase 0 → 003 → 004 → 008/010** — never cut over to queue-driven trading without staleness PAUSE
*and* the emergency-STOP mechanism in place.

## 11. Cold-transfer thought-experiment (Phase-0 self-test)

On a fresh Computer 2, a human must only: (1) `cp -r system-2-execution-engine/` (or `docker run`
the built image); (2) drop real `config/.env.system2` (OANDA practice [+ live, toggle-gated], GCS
read SA JSON, Pub/Sub creds, local DB password, optional webhook); (3) set `STORAGE_PROVIDER=gcs`
+ bucket and `QUEUE_PROVIDER=pubsub`; (4) Python 3.12 venv + `pip install -r requirements.txt`;
(5) verify clock/NTP (UTC); (6) `alembic upgrade head`; (7) run startup self-check (fail-closed).
**Nothing else** — no import or path reaches back into `scalable-brain/`. ✅ design satisfies this.
