# System 2 — Execution Engine ("The Hand")

The order-execution service of a three-system autonomous FX trading stack. System 1
(the Brain) trains and qualifies strategies offline; **System 2 (this repo)** produces
live scored signals and executes approved orders against the broker; System 3 (the
Guardian) is the independent risk middleware that approves, sizes, and audits every
trade. The systems communicate only through validated, versioned message contracts —
System 2 never sizes its own trades and never bypasses the Guardian.

## What this service does

- **Live signal production** — evaluates every closed H1/H4 bar per watched pair:
  candles → regime detection (HMM, trained by System 1) → strategy book lookup →
  gatekeeper model scoring → deduplicated `ScoredSignal` to the queue.
- **Order execution** — consumes Guardian-approved orders, constructs and submits them
  to OANDA (practice or live) with broker-side stop-loss/take-profit, validates fills,
  and measures entry slippage against the requested price.
- **Trade-close tracking** — registers every session-opened trade with the position
  manager and sweeps the broker for SL/TP/manual closes, emitting idempotent close
  events back to the Guardian so the ledger always matches the broker (EXEC-012).
- **Model hot-reload** — picks up new champion model sets published by System 1 via
  GCS artifact sync, with SHA-verified downloads and last-good rollback.
- **Telemetry** — a read-only health surface (`/status`, `/health`, `/regime`,
  `/signal`) feeding the operator dashboard, including a persistent scored-signal
  ledger (evaluations, approval rate, publish counts) that survives restarts.

## Design principles

- **Fail-open observability, fail-closed execution.** Telemetry, ledgers, and
  recorders may never block or delay the trading path; order submission refuses to
  run without validated config and contracts.
- **Idempotency everywhere.** Orders carry idempotency keys; fills and closes are
  deduplicated on broker transaction ids; redeliveries are no-ops.
- **Durable outbox.** Every event to the Guardian is persisted before publish —
  at-least-once delivery, never lost on crash.
- **Shadow-first.** `EXEC_SHADOW=true` runs the full pipeline without sending orders;
  live trading is a deliberate, double-gated flip.

## Layout

```
src/system2/
  artifact_sync/         # GCS model-set downloader, live regime detector + scheduler
  broker/                # OANDA adapter (REST), position manager, slippage validation
  execution/             # pipeline, outbound consumer, fill producer, close tracker,
                         # lifecycle wiring, safety monitor, trade recorder
  live_signal_producer/  # bar sweep, gatekeeper scoring, dedup, signal ledger
  telemetry/             # health/status HTTP surface
  common/                # config/secrets, structured logging, queue backend, db
migrations/              # additive SQL migrations (sqlite + postgres)
tools/                   # operational tools (e.g. backfill_closes.py)
config/                  # .env.system2.template (copy to .env.system2 — never committed)
deploy/                  # deployment assets
```

## Running

```bash
python -m venv venv && venv/Scripts/pip install -r requirements.txt   # Windows
cp config/.env.system2.template config/.env.system2                   # then fill in
python -m system2                                                     # start the engine
```

Tests (no network, no broker — everything faked):

```bash
python -m pytest src -q
```

The suite must stay green before any deploy; contract-shape tests validate outbound
events against System 3's actual production validator when the sibling repo is present.

## Operational docs

| Doc | Contents |
|---|---|
| `ARCHITECTURE.md` | component map and message flow |
| `RUNBOOK.md` | start/stop, health checks, common failures |
| `TRANSFER_VIA_GCS.md` | moving state between machines |
| `orchestration/` | build/review records for major work packages |

## Safety notice

This software places real orders when configured with live credentials. It is built
for a single supervised account behind an independent risk layer (System 3), staged
rollout (paper → micro → small → full), and hard circuit breakers. Nothing here is
financial advice; run it only against a practice account unless you have verified
every gate yourself.
