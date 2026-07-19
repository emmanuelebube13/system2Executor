# EXEC-009 — Layer 5 AMS Telemetry Endpoints

**Task ID:** EXEC-009
**System:** System 2 — Execution Engine
**Priority:** P2-Medium
**Estimated Effort:** 3d
**Prerequisites:** AMS-001, AMS-009
**External Dependencies:**
- **System 3 schema (AMS-001) + account-state read API/views (AMS-009):** the source of account state, equity curve, decisions, circuit breakers, strategy performance, and daily summary. Layer 5 reads these; it does not compute them.
- **DB / ODBC (FND-004):** read connectivity to the AMS state store/views.
- **Networking (FND-008):** dashboard reachable only on the private network with auth; no public ingress.

## Objective
Add Layer 5 FastAPI read endpoints (`/api/account/state`, `/equity-curve`, `/decisions`, `/circuit-breakers`, `/strategy-performance`, `/daily-summary`) and dashboard views over AMS state.

## Current State
- Layer 5 is a read-only FastAPI telemetry backend (`src/layer5/run.py`, port 8001; `src/layer5/api/main.py` with 7 route modules: `kpi.py`, `trades.py`, `risk.py`, `regimes.py`, `model.py`, `strategies.py`, `assets.py`) plus a React/Vite dashboard. Config in `src/layer5/api/config.py`, DI in `dependencies.py`, DB access via `services/db_client.py` + `query_builder.py`.
- These endpoints surface the **current 8-layer** tables; there is **no** view onto System 3 (AMS) account-level state, because AMS does not yet exist. The reorg moves account risk to System 3, so the operator now needs AMS state surfaced here.

## Target State
- A new Layer 5 route module (e.g. `routes/account.py`) registered in `main.py`, exposing read endpoints over AMS state (read-only; **no decision logic duplicated** — consistent with Layer 5's charter):
  - `GET /api/account/state` — current account snapshot (balance, equity, open exposure, margin, active mode RUNNING/PAUSED/BYPASS from EXEC-008).
  - `GET /api/account/equity-curve` — equity/balance over time for charting.
  - `GET /api/account/decisions` — recent AMS decisions (approved/rejected, reasons), joinable to executions via `correlation_id`/`ams_decision_id`.
  - `GET /api/account/circuit-breakers` — current/historical circuit-breaker + halt state (daily/weekly limits, drawdown, consecutive-loss) owned by System 3.
  - `GET /api/account/strategy-performance` — per-strategy realized performance.
  - `GET /api/account/daily-summary` — per-day P&L, trade counts, win rate, exposure.
- Corresponding **dashboard views** (React/Vite) consuming these endpoints via `services/api.ts`, with charts (Recharts) for equity curve and daily summary.

## Technical Specification

**Endpoint contracts (text; shapes mirror AMS-009 read API):**
```
GET /api/account/state ->
  { account_id, balance, equity, unrealized_pl, open_exposure_pct, margin_used,
    open_positions: int, exec_mode: "RUNNING"|"PAUSED"|"BYPASS", queue_staleness_sec, as_of }

GET /api/account/equity-curve?from&to&granularity ->
  { points: [ { ts, balance, equity, drawdown_pct } ] }

GET /api/account/decisions?limit&from&to ->
  { decisions: [ { ams_decision_id, ts, instrument, strategy_id, granularity,
                   verdict: "APPROVED"|"REJECTED", reason, sized_units?, correlation_id } ] }

GET /api/account/circuit-breakers ->
  { breakers: [ { name, status: "OK"|"WARN"|"TRIPPED", value, threshold, tripped_at?, scope } ] }

GET /api/account/strategy-performance?from&to ->
  { strategies: [ { strategy_id, name, trades, win_rate, expectancy, realized_pl, max_dd } ] }

GET /api/account/daily-summary?from&to ->
  { days: [ { date, realized_pl, trades, wins, losses, win_rate, avg_R, max_exposure_pct } ] }
```

**Implementation notes (text):**
- Reuse existing patterns: `db_client.py` + `query_builder.py` with **parameterized** queries; escape reserved words; schema-aware column handling for any optional AMS columns; FastAPI DI via `dependencies.py`; env-driven config in `config.py` for the AMS read connection.
- Endpoints are **strictly read-only** and must not expose secrets; redact any credential-bearing fields.
- Pagination/time-window params on list/series endpoints; sensible defaults and caps to avoid heavy queries.
- Caching: short TTL cache for hot endpoints (`/state`) is acceptable; `as_of` timestamps make staleness visible.

**Data flow:** AMS-009 views/API (System 3 state) → Layer 5 `routes/account.py` (parameterized read) → JSON → React dashboard views/charts. Joins to executions (`Fact_Live_Trades`) via `correlation_id`/`ams_decision_id` let the dashboard show decision→fill lineage.

## Testing & Validation
- **Unit:** each endpoint returns the documented shape; parameter validation (date ranges, limits); reserved-word escaping; empty-result handling returns 200 with empty arrays (not errors).
- **Integration:** against AMS-009 (or a seeded fixture), endpoints return data consistent with the AMS state store; decisions join correctly to `Fact_Live_Trades` fills.
- **Frontend:** dashboard views render equity curve, circuit-breaker status, daily summary; loading/empty/error states handled.
- **Edge cases:** AMS state store unreachable → endpoints return a clear 5xx/degraded payload (not a crash) and the dashboard shows a degraded banner; **weekend gap** — empty trading days render as zero rows, not gaps that break charts; **queue staleness / PAUSED / BYPASS** mode (EXEC-008) is reflected in `/state.exec_mode`.
- **Security:** no secrets in responses; endpoints only reachable on the private network/auth (FND-008).

## Rollback Plan
- Purely additive: the new route module and dashboard views can be feature-flagged or simply not registered. Disabling them leaves the existing 7 Layer 5 route modules and dashboard untouched.
- Read-only with no writes anywhere — rollback has zero data risk.

## Acceptance Criteria
- [ ] All six endpoints (`/state`, `/equity-curve`, `/decisions`, `/circuit-breakers`, `/strategy-performance`, `/daily-summary`) are implemented as read-only routes registered in `main.py`, using parameterized queries.
- [ ] The endpoints read AMS state (AMS-009) and never duplicate System 3's decision/risk logic.
- [ ] Dashboard views render equity curve, circuit-breaker status, decisions, and daily summary with proper loading/empty/error states.
- [ ] `/api/account/state` reflects the live execution mode (RUNNING/PAUSED/BYPASS) and queue staleness from EXEC-008.
- [ ] No secrets are exposed and the dashboard is reachable only on the private network with auth.

## Notes & Risks
- Risk: divergence between what Layer 5 shows and System 3's true state if Layer 5 recomputes anything. Mitigated by reading AMS-009 views directly and surfacing `as_of`.
- This is P2 and depends on System 3 existing; sequence it after the P0 execution path (EXEC-003/004/005/008) is live so the dashboard reflects real flow.
- Dashboard hosting is provisioned per the dependencies doc (FND-008, private-network only).
