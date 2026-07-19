"""Local datastore for System 2 (D-001): the store lives ON Computer 2, never shared.

Default **local PostgreSQL** in production, **SQLite** for dev/test — chosen by config
(``DB_PROVIDER``), same DB-API surface. Post-trade state reaches System 3 only via the queue
(EXEC-005); this store is System 2's own local record (e.g. ``Fact_Live_Trades``, EXEC-006).

Provides a fail-closed connection factory and an idempotent migration runner (tracks applied
files in ``schema_migrations``). psycopg is imported lazily so SQLite-only dev needs nothing
extra. CLI:  ``python -m system2.common.db migrate [--dir migrations]``.
"""

from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from system2.common.logging import get_logger, log_event
from system2.common.secrets import Secrets, get_secrets

log = get_logger("common.db")


@dataclass(frozen=True)
class DbConfig:
    provider: str  # "sqlite" | "postgres"
    sqlite_path: str = "state/db/system2.db"
    dsn: str | None = None

    @classmethod
    def from_secrets(cls, secrets: Secrets | None = None) -> "DbConfig":
        secrets = secrets or get_secrets()
        provider = (secrets.get("DB_PROVIDER", "sqlite") or "sqlite").lower()
        if provider == "postgres":
            return cls(provider, dsn=secrets.require("DB_DSN"))  # fail-closed: no DSN, no start
        if provider == "sqlite":
            return cls(provider, sqlite_path=secrets.get("DB_PATH", "state/db/system2.db"))
        raise ValueError(f"Unknown DB_PROVIDER '{provider}' (expected sqlite|postgres)")


def connect(config: DbConfig | None = None, secrets: Secrets | None = None) -> Any:
    """Return a DB-API connection for the configured provider."""
    config = config or DbConfig.from_secrets(secrets)
    if config.provider == "sqlite":
        path = Path(config.sqlite_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path))
        conn.execute("PRAGMA journal_mode=WAL")
        return conn
    if config.provider == "postgres":
        import psycopg  # lazy — only needed in production

        return psycopg.connect(config.dsn)
    raise ValueError(f"Unknown DB_PROVIDER '{config.provider}'")


def _placeholder(provider: str) -> str:
    return "?" if provider == "sqlite" else "%s"


def _run_script(conn: Any, provider: str, sql: str) -> None:
    if provider == "sqlite":
        conn.executescript(sql)
    else:
        with conn.cursor() as cur:
            cur.execute(sql)


def apply_migrations(
    conn: Any, provider: str, migrations_root: str | Path = "migrations"
) -> list[str]:
    """Apply un-applied ``migrations/<provider>/*.sql`` in filename order. Idempotent."""
    ph = _placeholder(provider)
    _run_script(
        conn, provider,
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(name TEXT PRIMARY KEY, applied_at TEXT NOT NULL)",
    )
    conn.commit()

    cur = conn.cursor()
    cur.execute("SELECT name FROM schema_migrations")
    applied = {row[0] for row in cur.fetchall()}

    mig_dir = Path(migrations_root) / provider
    files = sorted(mig_dir.glob("*.sql")) if mig_dir.exists() else []
    newly: list[str] = []
    for f in files:
        if f.name in applied:
            continue
        log_event(log, 20, "applying migration", migration=f.name, provider=provider)
        _run_script(conn, provider, f.read_text(encoding="utf-8"))
        cur.execute(
            f"INSERT INTO schema_migrations(name, applied_at) VALUES ({ph}, {ph})",
            (f.name, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")),
        )
        conn.commit()
        newly.append(f.name)
    log_event(log, 20, "migrations complete", provider=provider,
              applied_now=len(newly), total=len(applied) + len(newly))
    return newly


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="system2.common.db")
    sub = parser.add_subparsers(dest="cmd", required=True)
    mig = sub.add_parser("migrate", help="apply pending migrations")
    mig.add_argument("--dir", default="migrations")
    args = parser.parse_args(argv)

    if args.cmd == "migrate":
        config = DbConfig.from_secrets()
        conn = connect(config)
        try:
            newly = apply_migrations(conn, config.provider, args.dir)
        finally:
            conn.close()
        print(f"applied {len(newly)} migration(s): {', '.join(newly) or '(none)'}")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
