# PostgreSQL Patterns

**Skill ID:** `postgres-patterns`
**File:** `docs/implementation-roadmap/system-1-model-building/tasks/skills/postgres-patterns.md`
**Applies To:** Every agent that reads from or writes to the database.

---

## Canonical Connection

```python
from src.common.db import get_engine, get_session

engine = get_engine()          # Reads .env: DB_SERVER, DB_USER, DB_PASS, DB_NAME, DB_PORT
session = get_session()        # SQLAlchemy session with autocommit=False, autoflush=False
```

**NEVER** build a connection string or call `create_engine()` inline. Always use `src/common/db.py`.

---

## Database Facts

| Property | Value |
|----------|-------|
| Engine | PostgreSQL 16 + TimescaleDB 2.26.3 |
| Host | `localhost:5432` (host system cluster, NOT Docker) |
| Database | `ForexBrainDB` |
| Role | `sa` |
| Driver | `psycopg2` (via `postgresql+psycopg2://`) |

---

## Column Case Rules

| Case | Examples | How to Reference |
|------|----------|-----------------|
| Mixed-case (legacy) | `"Open"`, `"Close"` | **Double-quote**: `'"Open"'`, `'"Close"'` |
| Reserved word | `"timestamp"` | **Double-quote**: `'"timestamp"'` |
| Lowercase (modern) | `asset_id`, `bar_time_utc`, `granularity` | No quoting: `'asset_id'` |

**Rule of thumb:** `"Open"`, `"Close"`, `"timestamp"` are the only double-quoted columns. Everything else is lowercase.

In SQLAlchemy `text()`:
```python
from sqlalchemy import text
conn.execute(text('SELECT "Close" FROM fact_market_prices WHERE bar_time_utc = :ts'), {"ts": ts})
```

---

## Idempotent Writes (The Uptable Pattern)

All layers use `INSERT ... ON CONFLICT` for idempotent writes. Never use raw `INSERT` without a conflict clause.

```python
from sqlalchemy import text

upsert_sql = text("""
    INSERT INTO fact_market_prices (
        asset_id, granularity, bar_time_utc, "Open", "High", "Low", "Close", volume, complete, source, ingest_run_id, ingested_at_utc
    ) VALUES (
        :asset_id, :granularity, :bar_time_utc, :open, :high, :low, :close, :volume, :complete, :source, :ingest_run_id, :ingested_at_utc
    )
    ON CONFLICT (asset_id, granularity, bar_time_utc)
    DO UPDATE SET
        "Open" = EXCLUDED."Open",
        "High" = EXCLUDED."High",
        "Low"  = EXCLUDED."Low",
        "Close" = EXCLUDED."Close",
        volume = EXCLUDED.volume,
        complete = EXCLUDED.complete,
        ingest_run_id = EXCLUDED.ingest_run_id,
        ingested_at_utc = EXCLUDED.ingested_at_utc
""")
```

**Key points:**
- Natural key: `(asset_id, granularity, bar_time_utc)`.
- `DO UPDATE SET` = updates if the bar already exists (e.g., incomplete bar now complete).
- `EXCLUDED.*` references the incoming row's values.
- Always use `:named` parameters (SQLAlchemy style), never `%s` and never f-strings.

---

## Schema-Aware Code Pattern

Some columns may or may not exist due to schema drift. Always check dynamically:

```python
def column_exists(conn, table: str, column: str) -> bool:
    result = conn.execute(text("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = :table AND column_name = :column
        )
    """), {"table": table, "column": column})
    return result.scalar()

has_is_active = column_exists(conn, "fact_signals", "is_active")
```

**Tables known to have drifted:** `fact_signals` (may lack `is_active`), `fact_live_trades` (narrow, lacks many columns), `fact_market_regime_v2` (lacks lineage JSON columns), `dim_strategy` (lacks `strategy_key`), `dim_strategy_asset_mapping` (lacks `priority`).

---

## TimescaleDB Hypertable Facts

- `fact_market_prices` is a hypertable (4.67M+ rows, 248 chunks).
- Compression policies are active.
- Queries on `bar_time_utc` are automatically optimized.
- Standard SQL works — TimescaleDB is transparent for most operations.

---

## Reserved Words (Always Double-Quote)

- `"Open"`, `"Close"`, `"High"`, `"Low"` — mixed-case legacy
- `"timestamp"` — SQL reserved word

These appear in: `fact_market_prices`, `fact_market_regime_v2`, `fact_signals`.

---

## Parameter Binding (Never String Interpolation)

```python
# CORRECT
conn.execute(text("SELECT * FROM fact_signals WHERE asset_id = :asset_id"), {"asset_id": 1})

# WRONG — SQL injection risk
conn.execute(text(f"SELECT * FROM fact_signals WHERE asset_id = {asset_id}"))
```

---

## Querying Regime Data

```python
# Regime at or before a given time (point-in-time, no look-ahead)
sql = text("""
    SELECT regime_smoothed, prob_trending_up, prob_trending_down, prob_ranging, prob_high_vol
    FROM fact_market_regime_v2
    WHERE asset_id = :asset_id
      AND granularity = :granularity
      AND bar_time_utc <= :signal_time
    ORDER BY bar_time_utc DESC
    LIMIT 1
""")
```

---

## Transaction Management

Use SQLAlchemy session or explicit connection transactions:

```python
with engine.begin() as conn:
    conn.execute(upsert_sql, params)  # Auto-commit on success, rollback on exception
```

Never leave transactions open. Use context managers.

---

## Alias Outputs for Caller Expectations

If upstream callers expect mixed-case aliases:
```python
conn.execute(text('SELECT asset_id AS "Asset_ID", bar_time_utc AS "Bar_Time_UTC" FROM fact_market_prices'))
```
