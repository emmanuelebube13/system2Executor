# RUNBOOK — System 2, the Execution Engine ("The Hand")

Operational guide for running, pausing, and stopping System 2 on Computer 2. System 2 turns
risk-approved, pre-sized orders (from System 3 over the queue) into OANDA fills and manages
positions to close. It **never** sizes risk or approves trades — that is System 3.

> **Safety default:** the engine starts in `execution_only` + **SHADOW** (`EXEC_SHADOW=true`)
> and `OANDA_ENV=practice`. It constructs and validates real orders but submits **nothing** to
> the broker until the logged human cutover decision (DECISIONS_LOG.md **D-004**).

---

## 1. Prerequisites

- Python 3.12, a virtualenv with `pip install -r requirements.txt` (dev: `requirements-dev.txt`).
- `config/.env.system2` (git-ignored) filled from `config/.env.system2.template`. Startup is
  **fail-closed**: a missing required secret aborts with a clear message (exit code 2).
- Local datastore migrated: `python -m system2.common.db migrate`.
- Cross-system links reachable as configured: GCS (artifacts), Pub/Sub (orders/fills), OANDA HTTPS.
  For dev, set `STORAGE_PROVIDER=local` and `QUEUE_PROVIDER=local` to need none of the cloud SDKs.

## 2. Start / stop

```bash
# start (foreground; health server on 127.0.0.1:HEALTH_PORT)
python -m system2

# graceful stop — ANY of:
#   - Ctrl-C / SIGTERM (systemd stop)                → graceful shutdown
#   - touch the emergency-STOP sentinel:
touch state/control/STOP
```

On stop the engine finishes the current tick, **flushes the fill outbox** (no fill is lost),
emits a `STOPPED` control event, and **leaves open positions in place** — each already has a
broker-side stop (EXEC-006), so exiting never orphans risk. To force-flatten on stop instead,
set `EXEC_STOP_FLATTEN=true` (deliberate; not the default).

On start it **reconciles** the broker's open trades into the position manager, so a restart
never leaves a live position unmanaged.

## 3. Health / observability

Read-only, private-network only (`HEALTH_HOST` default `127.0.0.1`):

- `GET /health` — liveness + uptime.
- `GET /status` — full System-2 snapshot (exec mode, queue lag, open positions, outbox depth, model set, broker env).
- `GET /api/account/state` — exec mode + queue staleness + open-position count.

No secrets are ever included in responses. Account balance/equity/breakers are **System 3's**
surface, not this one.

## 4. Safety mode (automatic) — EXEC-008

If `AMS_Outbound_Queue` goes stale **> `STALENESS_LIMIT_SEC`** (default 300) while in-session,
the engine transitions **RUNNING → PAUSED**: it stops submitting **new** orders (they park on
the queue, not lost) while **open-position management keeps running**. It auto-resumes to
RUNNING when fresh approved orders / heartbeats return. Out-of-session (weekend) silence never
false-PAUSEs. Hysteresis prevents flapping. **Invariant:** queue down + BYPASS off ⇒ the engine
never submits.

## 5. Emergency BYPASS (manual, audited) — EXEC-008

Only for "System 3 is down but I must act." Off by default; each activation is loud + audited.

```bash
# in config/.env.system2 (or env):
EXEC_BYPASS_ENABLE=true
BYPASS_CONFIRM_TOKEN=<the agreed token>
# then request it in-process (operator tooling) with token + operator id + reason.
```

Uses conservative fixed sizing (`BYPASS_RISK_PCT`, default 0.25%, Kelly-independent), a max
positions cap, and is **time-bounded** (`BYPASS_MAX_DURATION_SEC`) — it auto-reverts. Every
BYPASS entry/exit is audited.

> **Boundary:** on Computer 2 there is no Computer-1 DB (D-001), so a BYPASS Layer-3-direct
> order *source* is not implemented here; if ever needed it must arrive via the queue/GCS with a
> logged decision. The state machine, submit-gate, conservative sizing, and audit are in place.

## 6. Practice → live cutover (D-004 — human decision, mandatory)

1. Complete the DevOps/packaging checklist and `python -m system2` runs clean in practice+SHADOW.
2. Run the **practice integration drill**: publish an approved order → confirm OANDA fill →
   confirm the fill lands on `AMS_Inbound_Queue` → confirm dual-run matches the legacy formula →
   exercise PAUSE and emergency STOP.
3. Record the review + decision in `orchestration/DECISIONS_LOG.md` (D-004).
4. Only then set `EXEC_SHADOW=false` (and `OANDA_ENV=live` + live creds when going to real money).

Never self-approve the cutover. Reverting: set `EXEC_SHADOW=true` or `EXEC_MODE=legacy`, or STOP.

## 7. Common issues

| Symptom | Likely cause | Action |
|---|---|---|
| Exit code 2 on start | missing required secret | fill it in `config/.env.system2`; message names the key |
| Stuck in PAUSED | queue stale / System 3 down | check System 3 + queue; PAUSED is the safe state |
| Fills not reaching System 3 | inbound queue down | outbox retries automatically; check `/status.outbox_depth` |
| Position without a stop | broker stop attach failed | EXEC-006 flags `NO_STOP_UNSAFE` + alerts — investigate immediately |
| No orders ever execute | still in SHADOW | expected until D-004; confirm `/status.exec_mode` and `EXEC_SHADOW` |
