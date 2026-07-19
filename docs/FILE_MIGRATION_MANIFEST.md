# File Migration Manifest

> **Copy, don't move.** The monolith at `scalable-brain/` must stay runnable on Computer 1 for
> dual-run (EXEC-003). We `cp` files in and refactor the **copies**; we never edit the originals
> and never import back into `scalable-brain/`. Reviewed in Phase 0 before any copy is executed.

Source root: `/home/emmanuel/Documents/Scalable_Brain/scalable-brain`
Target root: `/home/emmanuel/Documents/Scalable_Brain/system-2-execution-engine`

## A. Copy-from-monolith (then refactor the copy)

| Source (monolith) | Target | Refactor intent | Used by |
|---|---|---|---|
| `src/layer4_executor/live_pipeline.py` | `src/system2/execution/pipeline.py` | Slim to **execution-only**, feature-flagged; drop sizing/risk decisioning | EXEC-003 |
| `src/layer7/oanda_executor.py` | `src/system2/broker/oanda_adapter.py` + `position_manager.py` base | Idempotent submit, slippage tol, stop/TP confirm, practice→live toggle, reconcile | EXEC-006/007 |
| `src/layer5/` (FastAPI + React/Vite) | `src/system2/telemetry/` | Read-only AMS endpoints + dashboard; add `/healthz` `/readyz` `/control/emergency-stop` | EXEC-009 |
| `src/common/db.py` | `src/system2/common/db.py` | Re-point to **local datastore** (decision D-001); keep SQLAlchemy 2.0 + `INSERT … ON CONFLICT` | EXEC-003/006/009 |
| `src/system1/regime/hmm_regime.py` | `src/system2/artifact_sync/live_regime.py` (inference path) | HMM predict on live candles + persistence smoothing + last-good fallback | EXEC-002 |
| `src/layer1_regime/Fact_market_regime_v2.py` | (reference for live_regime.py) | Regime label mapping / feature derivation, inference-only | EXEC-002 |
| `src/layer6_auditor/trade_auditor.py` | reconciliation logic in `position_manager.py` + lifecycle | OANDA↔local reconcile (§8) | EXEC-006/007/010 |
| System-2 EXEC specs + README + deps | `docs/` | self-containment | ✅ DONE |
| Referenced skills (8) | `docs/skills/` | self-containment | ✅ DONE |
| Pruned `requirements.txt` | `requirements.txt` | only what System 2 runs (see below) | DevOps |

**Pruned `requirements.txt` target set:** `oandapyV20`, `sqlalchemy`, `alembic`, DB driver
(`psycopg2-binary` for local Postgres / stdlib `sqlite3` for dev), `pandas`, `numpy`, `ta`,
`scikit-learn`, `hmmlearn`, `joblib`, `fastapi`, `uvicorn`, `google-cloud-storage`,
`google-cloud-pubsub`. Check before adding anything new.

## B. Create-new (no monolith equivalent)

- `MASTER_ORCHESTRATION_PROMPT*.md` (✅ present), `ARCHITECTURE.md`, `RUNBOOK.md`.
- `orchestration/`: ledger json+md, `DECISIONS_LOG.md`, `FOLDER_STRUCTURE.md`,
  `AGENT_FLEET_TOPOLOGY.md`, `CONTINUATION_PROMPT.md`, `CHECKPOINT.md` + `checkpoint.json`.
- `docs/SYSTEM_BOUNDARY.md` (✅), `STORAGE_AND_QUEUE_ABSTRACTION.md` (✅), this manifest (✅),
  `SCHEMA_ADDITIONS.md`.
- `src/system2/common/`: `storage_backend.py`, `queue_backend.py`, `secrets.py`, `logging.py`.
- `src/system2/execution/`: `safety.py`, `validation.py`, `lifecycle.py`,
  `outbound_consumer.py`, `fill_producer.py`.
- `src/system2/artifact_sync/`: `downloader.py`.
- `migrations/` (Alembic) for the local datastore.
- `config/.env.system2.template`, `.gitignore`.
- *(Optional)* `Dockerfile`, `docker-compose.yml`.

## C. Do NOT bring across / do NOT build (System 3 owns — see SYSTEM_BOUNDARY.md)

Position sizing / Risk Engine · portfolio circuit breakers · Account State Machine ·
Notification Service · human-override policy · signal scoring / Decision Gate · any back-import
into `scalable-brain/`.
