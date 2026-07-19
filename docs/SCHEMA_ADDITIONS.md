# SCHEMA_ADDITIONS.md — `Fact_Live_Trades` additions (EXEC-006)

System 2 records every execution attempt locally (persist-then-publish, EXEC-005) in
`Fact_Live_Trades`. The monolith's table lacks the broker/fill columns the reorg needs, so
this documents the additive, **nullable** columns and provides idempotent migrations.

**Datastore (D-001):** the store is **local to Computer 2** — default **local PostgreSQL** in
production, **SQLite** for dev/test. Post-trade state reaches System 3 only via the queue
(EXEC-005), never a shared DB. These columns are the local record that the queue message is
built from; the two must agree (persist-then-publish ordering, EXEC-005).

## Columns (map 1:1 to `FillResult` / the inbound fill-confirmation message)

| Column | Type (PG / SQLite) | Source (`FillResult`) |
|---|---|---|
| `Broker_Order_ID` | `TEXT` / `TEXT` | `broker_order_id` |
| `Broker_Trade_ID` | `TEXT` / `TEXT` | `broker_trade_id` |
| `Fill_Price` | `NUMERIC(18,6)` / `REAL` | `fill_price` |
| `Fill_Time` | `TIMESTAMPTZ` / `TEXT` (ISO-8601 UTC) | `fill_time` |
| `Requested_Price` | `NUMERIC(18,6)` / `REAL` | `requested_price` |
| `Slippage_Pips` | `NUMERIC(9,2)` / `REAL` | `slippage_pips` (signed) |
| `Realized_Status` | `TEXT` / `TEXT` | `realized_status` (FILLED/PARTIAL/REJECTED/CANCELLED/EXPIRED) |
| `Filled_Units` | `INTEGER` / `INTEGER` | `filled_units` |
| `Stop_Loss_Price` | `NUMERIC(18,6)` / `REAL` | `stop_loss_price` |
| `Take_Profit_Price` | `NUMERIC(18,6)` / `REAL` | `take_profit_price` |
| `Model_Threshold` | `NUMERIC(9,4)` / `REAL` | (from approved order, optional) |
| `Model_Set_ID` | `TEXT` / `TEXT` | `model_set_id` (EXEC-001 active set) |
| `Regime_Label` | `TEXT` / `TEXT` | (EXEC-002, optional) |
| `Correlation_Score` | `NUMERIC(9,4)` / `REAL` | (backup guard, optional) |
| `Correlation_Passed` | `BOOLEAN` / `INTEGER` | (backup guard, optional) |
| `Reject_Reason` | `TEXT` / `TEXT` | `reject_reason` |
| `Updated_At` | `TIMESTAMPTZ` / `TEXT` (ISO-8601 UTC) | write time |

## Applying

```bash
# dev (SQLite)
DB_PROVIDER=sqlite DB_PATH=state/db/system2.db \
  python -m system2.common.db migrate
# prod (local PostgreSQL)
DB_PROVIDER=postgres DB_DSN='postgresql://user:pass@localhost/system2' \
  python -m system2.common.db migrate
```

Migrations live in `migrations/<dialect>/` and are applied in filename order; each is tracked
in `schema_migrations` so re-running is a no-op (idempotent). Writes are **schema-aware**
(detect column presence) and **parameterized** — never string-formatted — per project rules.

## Rollback

All additions are nullable/additive: adding them does not break existing writes, and the new
write path can be disabled leaving them unused. Reverting is a non-destructive column drop
(with confirmation per project rules).
