# AGENT FLEET TOPOLOGY (the company org-chart)

> No agent calls another directly — they hand off through **immutable artifacts at contracted
> paths** (like a team passing PRs/tickets). Cycle: design → implement → test → audit → ledger →
> release. If run single-threaded (no sub-agents), the same roles are executed as explicit steps in
> DAG order — quality/process identical.

## Tier 0 — Program Manager (orchestrator)
Sequences the EXEC DAG, deploys managers, enforces audit gates, owns ledger + decision log, STOPs
for the human at every logged-decision point.

## Governance & bookkeeping
| Role | Owns | Charter |
|---|---|---|
| Principal Architect (`Plan`) | `ARCHITECTURE.md`, `SYSTEM_BOUNDARY.md`, contracts | Phase 0; reviews every design before impl. |
| Structure-Coherency | `FOLDER_STRUCTURE.md` | Approves every new path; prevents drift / back-refs. |
| Ledger-Keeper / PM | ledger json+md, `DECISIONS_LOG.md` | Writes after every state change. |
| Continuity Sentinel | `CHECKPOINT.md` + `checkpoint.json` | Out-of-band "last breath" so any model resumes mid-step (§14.1). Never makes EXEC decisions. |

## Tier 1 — Domain managers
| Manager | Owns (EXEC) | Charter |
|---|---|---|
| **Artifact-Sync** | 001, 002 | GCS poll + SHA256 verify + atomic swap + last-known-good; live HMM regime. |
| **Execution-Core** | 003, 004, 005, 008, **010** | Slim L4; consume `AMS_Outbound_Queue`; last-line validation; publish fills; staleness PAUSE + BYPASS; emergency STOP + graceful lifecycle + DLQ. |
| **Broker** | 006, 007 | OANDA adapter hardening (idempotency, slippage, stop/TP confirm, practice→live, partial-fill) + reconcile; active position mgmt. |
| **Telemetry** | 009 | Read-only AMS endpoints + dashboard + health/heartbeat. No decision logic. |

## Cross-cutting specialists
- **Security / Secrets** — secrets sourcing + fail-closed startup; practice/live separation; no-committed-credential guard; credential audit trail.
- **QA / Auditor** — the **only** role with rework + blocking authority. Runs `AG-EXEC-000..010` + `AG-EXEC-CROSS`; owns determinism golden-file, idempotency-replay, emergency-STOP, graceful-restart/reconcile, poison-message/DLQ drills.
- **DevOps / Packaging** — `requirements.txt`, venv, env templates, DB migrations, optional Docker, process mgmt, transfer bundle + `RUNBOOK.md`.
- **Tier-2 ephemeral** — short-lived implementer + test-author sub-agents per file/module; disposed after use (cost discipline).

## Control rules
1. **Startup checklist** (every manager): read this file → read task spec → re-read `SYSTEM_BOUNDARY.md` → load skills → check `state/rework/{mgr}_*` + `state/blocked/{mgr}` → confirm upstream artifact/contract (or fallback) → implement (extend the **copied** code) → self-verify → write `state/DONE_{mgr}_{ts}.md` → request auditor.
2. **Rework loop**: auditor writes `state/rework/{mgr}_{ts}.md`; manager fixes, re-runs, deletes file. **Max 3 iterations**, then `state/blocked/BLOCKED_{mgr}_{ts}.md` + escalate to human (STOP).
3. **Blocking chain**: a consumer must not start while upstream has an open rework/blocked file.
4. **Human-decision gate**: connectivity, queue backend, secrets, practice→live, promotion/cutover, clearing HALTED, any gate override ⇒ STOP, ask, log in `DECISIONS_LOG.md` before resuming.

## Execution mode for this program
Running **single-threaded** (Tier-0 orchestrator performs each role's work inline in DAG order),
with checkpoint writes inline at the §14.1 trigger points. Sub-agents may be spawned later only if
the human asks.
