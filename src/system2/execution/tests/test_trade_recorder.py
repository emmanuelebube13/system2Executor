"""EXEC-005/006 — TradeRecorder writes Fact_Live_Trades (schema-aware, parameterized, upsert)."""

from __future__ import annotations

from pathlib import Path

from system2.common.db import DbConfig, apply_migrations, connect
from system2.execution.fill_producer import FillResult
from system2.execution.pipeline import ConstructedOrder
from system2.execution.trade_recorder import TradeRecorder

REPO_MIGRATIONS = Path(__file__).resolve().parents[4] / "migrations"


def _constructed(**over) -> ConstructedOrder:
    base = dict(
        idempotency_key="k1", correlation_id="c1", instrument="EUR_USD", side="BUY",
        units=10000, entry_price=1.10000, stop_loss=1.09900, take_profit=1.10300,
        atr_value=0.0010, rr_ratio=3.0,
    )
    base.update(over)
    return ConstructedOrder(**base)


def _fill(**over) -> FillResult:
    base = dict(realized_status="FILLED", filled_units=10000, broker_order_id="o1",
                broker_trade_id="T1", fill_price=1.10002, slippage_pips=0.2, model_set_id="set-1")
    base.update(over)
    return FillResult(**base)


def _recorder(tmp_path):
    cfg = DbConfig(provider="sqlite", sqlite_path=str(tmp_path / "system2.db"))
    conn = connect(cfg)
    apply_migrations(conn, "sqlite", REPO_MIGRATIONS)
    return TradeRecorder(conn, "sqlite"), conn


def test_record_inserts_row(tmp_path):
    rec, conn = _recorder(tmp_path)
    rec.record(_constructed(), _fill())
    row = conn.execute(
        "SELECT Order_ID, Correlation_ID, Broker_Trade_ID, Fill_Price, Slippage_Pips, "
        "Realized_Status, Model_Set_ID FROM Fact_Live_Trades WHERE Order_ID='k1'"
    ).fetchone()
    assert row == ("k1", "c1", "T1", 1.10002, 0.2, "FILLED", "set-1")


def test_record_upsert_updates_not_duplicates(tmp_path):
    rec, conn = _recorder(tmp_path)
    rec.record(_constructed(), _fill(realized_status="PARTIAL", filled_units=4000))
    rec.record(_constructed(), _fill(realized_status="FILLED", filled_units=10000))
    rows = conn.execute("SELECT Realized_Status, Filled_Units FROM Fact_Live_Trades WHERE Order_ID='k1'").fetchall()
    assert rows == [("FILLED", 10000)]  # one row, updated


def test_record_falls_back_to_constructed_stops(tmp_path):
    rec, conn = _recorder(tmp_path)
    rec.record(_constructed(), _fill(stop_loss_price=None, take_profit_price=None))
    sl, tp = conn.execute(
        "SELECT Stop_Loss_Price, Take_Profit_Price FROM Fact_Live_Trades WHERE Order_ID='k1'"
    ).fetchone()
    assert sl == 1.09900 and tp == 1.10300


def test_record_never_raises_on_bad_conn(tmp_path):
    rec, conn = _recorder(tmp_path)
    conn.close()  # subsequent writes will error internally but must be swallowed
    rec.record(_constructed(), _fill())  # no exception
