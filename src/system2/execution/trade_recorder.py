"""EXEC-005/006 — local ``Fact_Live_Trades`` writer (the "persist" in persist-then-publish).

Wired as the pipeline's ``persist_fn``: it runs BEFORE the fill is published to
``AMS_Inbound_Queue`` (EXEC-005), so the local DB row and the queue message agree. Writes are
**schema-aware** (only columns that exist are written — matched case-insensitively so it works
whether the datastore folds identifiers, e.g. PostgreSQL, or not, e.g. SQLite) and always
**parameterized**. Keyed by ``Order_ID`` = ``idempotency_key`` with an upsert, so a redelivery
updates rather than duplicates.

A DB failure here is logged and swallowed — never block delivering a fill to System 3 (the
durable outbox still guarantees the queue message). The row can be reconciled later.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from system2.common.logging import get_logger, log_event

log = get_logger("execution.trade_recorder")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class TradeRecorder:
    """Schema-aware, parameterized upsert into ``Fact_Live_Trades``."""

    def __init__(self, conn: Any, provider: str, table: str = "Fact_Live_Trades") -> None:
        self.conn = conn
        self.provider = provider
        self.table = table
        self._ph = "?" if provider == "sqlite" else "%s"
        # map lower(col) -> actual column name as stored, for case-insensitive matching
        self._cols: dict[str, str] = {c.lower(): c for c in self._existing_columns()}

    def _existing_columns(self) -> list[str]:
        cur = self.conn.cursor()
        if self.provider == "sqlite":
            cur.execute(f"PRAGMA table_info({self.table})")
            return [r[1] for r in cur.fetchall()]
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE lower(table_name)=lower(%s)",
            (self.table,),
        )
        return [r[0] for r in cur.fetchall()]

    def record(self, constructed: Any, fill: Any) -> None:
        """Persist one execution outcome. Never raises (log-and-continue on DB error)."""
        candidate: dict[str, Any] = {
            "Order_ID": constructed.idempotency_key,
            "Correlation_ID": constructed.correlation_id,
            "Instrument": constructed.instrument,
            "Side": constructed.side,
            "Units": int(constructed.units),
            "Requested_Price": constructed.entry_price,
            "Broker_Order_ID": fill.broker_order_id,
            "Broker_Trade_ID": fill.broker_trade_id,
            "Fill_Price": fill.fill_price,
            "Fill_Time": fill.fill_time,
            "Slippage_Pips": fill.slippage_pips,
            "Realized_Status": fill.realized_status,
            "Filled_Units": int(fill.filled_units) if fill.filled_units is not None else None,
            "Stop_Loss_Price": fill.stop_loss_price if fill.stop_loss_price is not None else constructed.stop_loss,
            "Take_Profit_Price": fill.take_profit_price if fill.take_profit_price is not None else constructed.take_profit,
            "Model_Set_ID": fill.model_set_id,
            "Reject_Reason": fill.reject_reason,
            "Updated_At": _utc_now_iso(),
        }
        # keep only columns that actually exist, mapped to their stored names
        row = {self._cols[k.lower()]: v for k, v in candidate.items() if k.lower() in self._cols}
        if "Order_ID".lower() not in self._cols or not row:
            log_event(log, logging.WARNING, "Fact_Live_Trades not writable (no matching columns); skipping")
            return

        key_col = self._cols["order_id"]
        cols = list(row.keys())
        placeholders = ", ".join(self._ph for _ in cols)
        col_list = ", ".join(cols)
        updates = ", ".join(f"{c}={self._ph}" for c in cols if c != key_col)
        sql = (
            f"INSERT INTO {self.table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT ({key_col}) DO UPDATE SET {updates}"
        )
        params = list(row.values()) + [row[c] for c in cols if c != key_col]
        try:
            self.conn.execute(sql, params)
            self.conn.commit()
        except Exception as exc:  # persist failure must not block fill delivery
            log_event(log, logging.ERROR, "Fact_Live_Trades write failed (fill still delivered)",
                      order_id=constructed.idempotency_key, error=str(exc))
