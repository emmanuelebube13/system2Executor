# CHECKPOINT — live "last breath" (read this FIRST on any resume)

**ts_utc:** 2026-07-01T13:00:00Z
**agent:** Tier-0 Orchestrator / DevOps
**phase_or_task:** BUILD (10/10) + DevOps + optional polish COMPLETE → transfer-ready
**status:** `completed` — full suite 152/152; live trading gated on human decision D-004

**step:** DevOps/packaging done on top of the complete EXEC-001..010 build:
- `requirements.txt` / `requirements-dev.txt` — pruned to actual imports, pinned to the dev venv;
  every heavy dep (oandapyV20, google-cloud-{pubsub,storage}, fastapi, uvicorn) is lazy-imported.
- `src/system2/__main__.py` — `python -m system2`: `build_from_secrets()` → start health server on a
  daemon thread → `runtime.run()`; fail-closes with exit code **2** on a missing secret (verified).
- `docs/SCHEMA_ADDITIONS.md` + `migrations/{sqlite,postgres}/001_fact_live_trades_additions.sql` — the
  EXEC-006 `Fact_Live_Trades` columns (map 1:1 to `FillResult`).
- `src/system2/common/db.py` — D-001 local datastore: `DbConfig` (sqlite dev / postgres prod, fail-closed
  on missing DSN), lazy psycopg, idempotent migration runner tracked in `schema_migrations`, CLI
  `python -m system2.common.db migrate` (smoke-tested).
- `RUNBOOK.md` — start/stop, STOP sentinel, health endpoints, PAUSE behaviour, BYPASS, D-004 cutover.
- Logged **D-004** (shadow→live cutover) as ⏳ PENDING in DECISIONS_LOG.md.

**intent (remaining — human/ops, NOT code):**
1. Provision `config/.env.system2` from the template (OANDA practice creds, queue/storage config).
2. `python -m system2.common.db migrate` on the target datastore.
3. Run `python -m system2` in practice+SHADOW; execute the **practice integration drill** (order →
   OANDA fill → fill on AMS_Inbound_Queue → dual-run matches → PAUSE/STOP drills).
4. **Log D-004 APPROVED** (human), then set `EXEC_SHADOW=false` (+ `OANDA_ENV=live` for real money).
Optional polish is now DONE: `execution/trade_recorder.py` wired as `persist_fn` (persist-then-publish,
log-and-continue on DB error), `Dockerfile` + `.dockerignore`, `deploy/system2.service` (systemd).

**verify_on_resume:**
1. `cd system-2-execution-engine && PYTHONPATH=src SYSTEM2_ENV_FILE=/nonexistent.env /home/emmanuel/Documents/Scalable_Brain/.venv/bin/python -m pytest src -q` → expect 152 passed.
2. `python -m system2` with no creds → exit code 2 (fail-closed); `python -m system2.common.db migrate` (sqlite) applies 001.
3. Read DECISIONS_LOG.md D-004 — if still PENDING, do NOT go live; that is a human decision.

**side_effects_in_flight:** none.

**do_not_repeat:** don't re-decide D-001/D-002/D-003; EXEC-001..010 + packaging complete & tested; keep all
optional deps lazy; adapter never re-sizes units; telemetry serves only System-2 state; never edit
`scalable-brain/`. Do NOT flip EXEC-003 out of SHADOW / enable live until D-004 is LOGGED APPROVED by the
human after a passing practice drill. Do NOT build a Computer-1-DB BYPASS order source (violates D-001).

**open_gates_or_blocks:** live trading blocked on D-004 (human, mandatory). All engineering work for a
transfer-ready practice service is complete.

**resume_pointer:** ledger event-id 23 · next: ops (provision creds → migrate → practice drill → log D-004).
