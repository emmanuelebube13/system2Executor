"""Tests for the local datastore (D-001) — SQLite connect + idempotent migration runner."""

from __future__ import annotations

from pathlib import Path

import pytest

from system2.common.db import DbConfig, apply_migrations, connect

# repo migrations/ dir (this file: src/system2/common/tests/test_db.py -> up 4 to repo root)
REPO_MIGRATIONS = Path(__file__).resolve().parents[4] / "migrations"


def test_sqlite_connect_creates_file(tmp_path):
    cfg = DbConfig(provider="sqlite", sqlite_path=str(tmp_path / "db" / "system2.db"))
    conn = connect(cfg)
    try:
        conn.execute("CREATE TABLE t(x)")
        assert (tmp_path / "db" / "system2.db").exists()
    finally:
        conn.close()


def test_migrations_apply_then_noop(tmp_path):
    cfg = DbConfig(provider="sqlite", sqlite_path=str(tmp_path / "system2.db"))
    conn = connect(cfg)
    try:
        first = apply_migrations(conn, "sqlite", REPO_MIGRATIONS)
        assert "001_fact_live_trades_additions.sql" in first
        # the new columns exist
        cols = {r[1] for r in conn.execute("PRAGMA table_info(Fact_Live_Trades)").fetchall()}
        for expected in ("Broker_Order_ID", "Fill_Price", "Slippage_Pips", "Model_Set_ID", "Updated_At"):
            assert expected in cols
        # re-running is a no-op (idempotent)
        second = apply_migrations(conn, "sqlite", REPO_MIGRATIONS)
        assert second == []
    finally:
        conn.close()


def test_postgres_config_failcloses_without_dsn(monkeypatch, tmp_path):
    from system2.common.secrets import MissingSecretError, Secrets

    monkeypatch.setenv("DB_PROVIDER", "postgres")
    monkeypatch.delenv("DB_DSN", raising=False)
    # `Secrets` merges os.environ OVER a dotenv file, so deleting the env var is
    # not enough — on a developer box config/.env.system2 supplies DB_DSN and the
    # fail-closed path was never reached. Point it at a file that does not exist.
    with pytest.raises(MissingSecretError):
        DbConfig.from_secrets(Secrets(env_file=tmp_path / "absent.env"))


def test_unknown_provider_rejected(monkeypatch, tmp_path):
    from system2.common.secrets import Secrets

    monkeypatch.setenv("DB_PROVIDER", "oracle")
    with pytest.raises(ValueError):
        DbConfig.from_secrets(Secrets(env_file=tmp_path / "absent.env"))
