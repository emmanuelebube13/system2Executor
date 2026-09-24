"""EXEC-012 — trade-close tracking for session-opened trades.

Closes the loop the 2026-07-15 incident exposed: trades submitted during the session were
never registered with the ``PositionManager`` and broker-side closes (SL/TP) were never
detected, so System 3's journal kept phantom open positions for ~20 hours. This module
provides:

  * **One ManagedTrade builder** used BOTH when a fill is confirmed (register-on-fill)
    and when ``startup_reconcile`` adopts broker-open trades after a restart, so the
    manager watches every session trade and order identity survives to close time.
  * **CloseSweeper** — rides the engine tick (throttled): ONE ``get_open_trades`` call
    per sweep; any registered, non-closed trade missing from the broker's open list is
    fetched with ``get_trade`` and its close is emitted. When ``get_trade`` cannot answer
    (see below) the sweeper falls back to the **transaction stream**, which is
    authoritative.
  * **CloseEmitter** — builds the flat FillEvent (``status="closed"``) System 3's
    post-trade processor understands and publishes it durably (persist-then-publish via
    the never-pruned fill outbox, reusing ``FillProducer``'s flush machinery) straight to
    the S3-side topic (default ``ams-inbound.ams``). The bridge cannot translate closes;
    its own SnapshotRelay sets the precedent of publishing contract-valid flat events
    onto that topic, and S2 shares the same physical local queue DB.

Why the transaction fallback exists (2026-07-21 incident): OANDA answered
``GET /v3/accounts/{a}/trades/2580`` with **404 NO_SUCH_TRADE** for a trade it had just
opened and stopped out — the trade was absent from ``/trades`` in *every* state
(``?ids=``, ``state=ALL``, ``state=CLOSED``) while the transaction stream carried the
full close (``ORDER_FILL`` reason=STOP_LOSS_ORDER with ``tradesClosed[]``). The old
single-endpoint dependency retried forever, so the close never reached System 3: the
journal row kept a phantom open position, ``strategy_performance`` never recomputed and
the daily summary under-reported P&L. ``resolve_close_from_transactions`` scans
``transactions/idrange`` from the trade id (OANDA trade ids ARE the opening ORDER_FILL
transaction id) up to ``lastTransactionID`` and reconstructs the same ``CloseFacts``.

Idempotency: the close event's ``broker_order_id`` is the OANDA close *transaction* id
(deterministic — S3 dedups on it and additionally guards already-completed journal rows),
plus a small persisted ledger keyed by broker trade id avoids re-emission noise across
restarts. Both close-facts sources yield the SAME close transaction id, so a close found
either way dedups against the other. Everything here is fail-open: errors are logged and
retried on the next sweep — close tracking never blocks new trading or crashes the tick.
"""

from __future__ import annotations

import logging
import json
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from system2.broker.oanda_adapter import compute_slippage_pips
from system2.broker.position_manager import ManagedTrade
from system2.common.logging import get_logger, log_event
from system2.common.queue_backend import utc_now_iso
from system2.execution.fill_producer import FillProducer, FillResult
from system2.execution.pipeline import ApprovedOrder, ConstructedOrder

log = get_logger("execution.close_tracker")

# S3 FillEvent.schema.json exit_reason enum — anything else would DLQ the event.
VALID_EXIT_REASONS = frozenset({"tp", "sl", "manual", "flatten", "expiry", "other"})

_PAIR_RE = re.compile(r"^[A-Z]{3}_[A-Z]{3}$")
_CLIENT_ID_PREFIX = "sb-"  # OandaAdapter's clientExtensions.id = "sb-" + idempotency_key


def _parse_time(value: Any) -> datetime:
    """Best-effort RFC3339 → aware UTC datetime (OANDA nanosecond precision parses fine)."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if value:
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- #
# ONE ManagedTrade builder (register-on-fill AND startup reconcile use this)
# --------------------------------------------------------------------------- #
def build_managed_trade(
    *,
    broker_trade_id: str,
    instrument: str,
    side: str,
    entry_price: float,
    initial_stop_price: float | None,
    take_profit_price: float | None,
    open_time: Any,
    granularity: str | None,
    correlation_id: str | None,
    order_request_id: str | None,
) -> ManagedTrade:
    """Normalize broker/fill facts into a ManagedTrade (the single construction point)."""
    return ManagedTrade(
        broker_trade_id=str(broker_trade_id),
        instrument=str(instrument),
        side=str(side).upper(),
        entry_price=float(entry_price),
        initial_stop_price=float(initial_stop_price or 0.0),
        take_profit_price=float(take_profit_price or 0.0),
        open_time=_parse_time(open_time),
        granularity=granularity or "H1",
        # No duration policy is configured anywhere today (time exits stay disabled);
        # broker-side SL/TP plus the close sweep own the exit path for now.
        max_duration_sec=48.0 * 3600.0,
        correlation_id=correlation_id or "",
        order_request_id=order_request_id,
    )


def managed_trade_from_fill(
    order: ApprovedOrder, constructed: ConstructedOrder | None, fill: FillResult
) -> ManagedTrade | None:
    """Register-on-fill mapping. Returns None when there is nothing to manage (no trade)."""
    if fill.realized_status not in ("FILLED", "PARTIAL") or not fill.broker_trade_id:
        return None
    return build_managed_trade(
        broker_trade_id=fill.broker_trade_id,
        instrument=order.instrument,
        side=order.side,
        entry_price=(fill.fill_price
                     if fill.fill_price is not None
                     else (constructed.entry_price if constructed is not None else 0.0)),
        initial_stop_price=(fill.stop_loss_price
                            if fill.stop_loss_price is not None
                            else (constructed.stop_loss if constructed is not None else None)),
        take_profit_price=(fill.take_profit_price
                           if fill.take_profit_price is not None
                           else (constructed.take_profit if constructed is not None else None)),
        open_time=fill.fill_time,
        granularity=order.granularity,
        correlation_id=order.correlation_id,
        order_request_id=order.idempotency_key,
    )


def managed_trade_from_broker_trade(
    trade: dict[str, Any], *, outbox_path: str | Path | None = None
) -> ManagedTrade | None:
    """Startup-reconcile mapping: adopt a broker open-trade dict.

    Order identity is recovered from the trade's client id (``sb-<idempotency_key>``);
    the signal id (``correlation_id``) and granularity are recovered — best-effort — from
    the durable fill outbox, which keeps every emitted fill envelope forever.
    """
    trade_id = trade.get("id")
    if not trade_id:
        return None
    client_id = (trade.get("clientExtensions") or {}).get("id") or ""
    order_request_id = (client_id[len(_CLIENT_ID_PREFIX):]
                        if client_id.startswith(_CLIENT_ID_PREFIX) else None)
    envelope = (find_fill_envelope(outbox_path, order_request_id)
                if (outbox_path and order_request_id) else None)
    units = float(trade.get("initialUnits") or trade.get("currentUnits") or 0.0)
    return build_managed_trade(
        broker_trade_id=str(trade_id),
        instrument=str(trade.get("instrument", "")),
        side="SELL" if units < 0 else "BUY",
        entry_price=float(trade.get("price") or 0.0),
        initial_stop_price=_order_price(trade.get("stopLossOrder")),
        take_profit_price=_order_price(trade.get("takeProfitOrder")),
        open_time=trade.get("openTime"),
        granularity=(envelope or {}).get("granularity"),
        correlation_id=(envelope or {}).get("correlation_id"),
        order_request_id=order_request_id,
    )


def _order_price(dependent_order: dict[str, Any] | None) -> float | None:
    price = (dependent_order or {}).get("price")
    return float(price) if price else None


# --------------------------------------------------------------------------- #
# Durable fill-outbox scan (restart identity recovery + the backfill tool)
# --------------------------------------------------------------------------- #
def iter_fill_envelopes(outbox_path: str | Path) -> Iterator[dict[str, Any]]:
    """Yield every fill-confirmation envelope ever persisted to the outbox (newest first).

    The outbox never prunes (rows only flip to state='done'), so it is the durable record
    of every session-opened trade. Read-only; yields nothing on any error (fail-open).
    """
    path = Path(outbox_path)
    if not path.exists():
        return
    try:
        conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        log_event(log, logging.WARNING, "fill outbox not readable", error=str(exc))
        return
    try:
        rows = conn.execute(
            "SELECT body FROM queue WHERE topic='fill_outbox' ORDER BY seq DESC"
        ).fetchall()
    except sqlite3.Error as exc:
        log_event(log, logging.WARNING, "fill outbox scan failed", error=str(exc))
        return
    finally:
        conn.close()
    for (body,) in rows:
        try:
            envelope = json.loads(body)
        except (TypeError, ValueError):
            continue
        if envelope.get("event_type") == "fill_confirmation":
            yield envelope


def find_fill_envelope(
    outbox_path: str | Path | None, order_request_id: str
) -> dict[str, Any] | None:
    """Find the fill-confirmation envelope for one order (idempotency_key match)."""
    if not outbox_path:
        return None
    for envelope in iter_fill_envelopes(outbox_path):
        if envelope.get("idempotency_key") == order_request_id:
            return envelope
    return None


# --------------------------------------------------------------------------- #
# Broker close facts (both sources normalize to CloseFacts)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CloseFacts:
    """What the broker says about a completed close (either response shape)."""

    close_txn_id: str  # OANDA close transaction id — the close event's broker_order_id
    realized_pnl: float
    close_price: float | None
    close_time: str | None  # broker RFC3339, passed through as-is
    units: float | None  # magnitude of the closed position (sign derived from the trade)
    # EXEC-013: signed exit slippage (fill − expected)/pip vs the FILLED SL/TP order's
    # price — the broker's ACTUAL dependent order, correct even after stop modifications.
    # None when no dependent order triggered (manual/flatten market closes have no
    # expected price). S3 writes it onto the journal row at close time.
    slippage_pips: float | None = None


def facts_from_close_response(resp: dict[str, Any] | None) -> CloseFacts | None:
    """Close facts from an ``adapter.close_trade`` (TradeClose) response."""
    txn = (resp or {}).get("orderFillTransaction") or {}
    if not txn.get("id"):
        return None
    closed = (txn.get("tradesClosed") or [{}])[0]
    realized = closed.get("realizedPL", txn.get("pl"))
    units = closed.get("units", txn.get("units"))
    return CloseFacts(
        close_txn_id=str(txn["id"]),
        realized_pnl=float(realized or 0.0),
        close_price=float(txn["price"]) if txn.get("price") else None,
        close_time=txn.get("time"),
        units=abs(float(units)) if units else None,
    )


def facts_from_trade_details(trade: dict[str, Any]) -> CloseFacts | None:
    """Close facts from a ``transport.get_trade`` (TradeDetails) response on a CLOSED trade."""
    closing_ids = trade.get("closingTransactionIDs") or []
    if not closing_ids:
        return None
    units = trade.get("initialUnits")
    close_price = float(trade["averageClosePrice"]) if trade.get("averageClosePrice") else None
    return CloseFacts(
        close_txn_id=str(closing_ids[-1]),
        realized_pnl=float(trade.get("realizedPL") or 0.0),
        close_price=close_price,
        close_time=trade.get("closeTime"),
        units=abs(float(units)) if units else None,
        slippage_pips=_exit_slippage_pips(trade, close_price),
    )


def _exit_slippage_pips(trade: dict[str, Any], close_price: float | None) -> float | None:
    """EXEC-013: signed pips between the triggered SL/TP order's price and the actual close.

    Uses the FILLED dependent order from TradeDetails (the broker's live order, so stop
    modifications are already reflected). Never raises; None when not computable.
    """
    if close_price is None:
        return None
    try:
        expected = None
        for key in ("stopLossOrder", "takeProfitOrder"):
            dep = trade.get(key) or {}
            if dep.get("state") == "FILLED" and dep.get("price"):
                expected = float(dep["price"])
                break
        instrument = str(trade.get("instrument") or "")
        if expected is None or expected <= 0 or not instrument:
            return None
        return compute_slippage_pips(expected, close_price, instrument)
    except (TypeError, ValueError):
        return None


def exit_reason_from_trade_details(trade: dict[str, Any]) -> str:
    """sl/tp when the linked dependent order FILLED (i.e. it triggered the close), else other."""
    if (trade.get("stopLossOrder") or {}).get("state") == "FILLED":
        return "sl"
    if (trade.get("takeProfitOrder") or {}).get("state") == "FILLED":
        return "tp"
    return "other"


# --------------------------------------------------------------------------- #
# Transaction-stream fallback (authoritative when /trades/{id} cannot answer)
# --------------------------------------------------------------------------- #
TXN_PAGE_SIZE = 500  # OANDA idrange caps at 1000 per call; stay well under
TXN_MAX_SCAN = 5000  # bound the work so a stuck trade can never sweep the whole history

# OANDA ORDER_FILL.reason → S3 exit_reason (VALID_EXIT_REASONS).
_TXN_REASON_TO_EXIT = {
    "STOP_LOSS_ORDER": "sl",
    "TRAILING_STOP_LOSS_ORDER": "sl",
    "GUARANTEED_STOP_LOSS_ORDER": "sl",
    "TAKE_PROFIT_ORDER": "tp",
    "MARKET_ORDER_TRADE_CLOSE": "manual",
    "MARKET_ORDER": "manual",
    "CLIENT_ORDER": "manual",
    "MARKET_ORDER_POSITION_CLOSEOUT": "flatten",
    "MARKET_ORDER_MARGIN_CLOSEOUT": "flatten",
    "MARGIN_CLOSEOUT": "flatten",
}
# Fill reasons that imply a dependent order carried an EXPECTED price (EXEC-013 slippage).
_DEPENDENT_FILL_REASONS = frozenset(
    {"STOP_LOSS_ORDER", "TRAILING_STOP_LOSS_ORDER",
     "GUARANTEED_STOP_LOSS_ORDER", "TAKE_PROFIT_ORDER"})


def exit_reason_from_transaction(txn: dict[str, Any]) -> str:
    """Map a closing ORDER_FILL's ``reason`` to the S3 exit_reason enum."""
    return _TXN_REASON_TO_EXIT.get(str(txn.get("reason") or "").upper(), "other")


def fetch_transaction_range(transport: Any, from_id: int, to_id: int) -> list[dict[str, Any]]:
    """One ``transactions/idrange`` page (inclusive).

    Prefers a first-class ``transport.get_transactions_idrange(from, to)`` when the
    transport grows one; otherwise drives oandapyV20 through the transport's own request
    plumbing so this fix needs no change outside close tracking. (Follow-up: promoting
    this to a real method on ``OandaRestTransport`` would drop the private access.)
    """
    getter = getattr(transport, "get_transactions_idrange", None)
    if callable(getter):
        return list(getter(from_id, to_id) or [])
    from oandapyV20.endpoints.transactions import TransactionIDRange  # lazy, like the transport

    req = TransactionIDRange(accountID=transport.env.account_id,
                             params={"from": str(from_id), "to": str(to_id)})
    resp = transport._request(req)
    return list((resp or {}).get("transactions") or [])


def _last_transaction_id(transport: Any) -> int | None:
    try:
        summary = transport.get_account_summary() or {}
        raw = summary.get("lastTransactionID")
        return int(raw) if raw is not None else None
    except Exception:
        return None


def iter_transactions(transport: Any, from_id: int, to_id: int) -> Iterator[dict[str, Any]]:
    """Yield transactions in [from_id, to_id] ascending, paged and bounded."""
    scanned = 0
    lo = from_id
    while lo <= to_id and scanned < TXN_MAX_SCAN:
        hi = min(lo + TXN_PAGE_SIZE - 1, to_id)
        batch = fetch_transaction_range(transport, lo, hi)
        if not batch:
            break
        for txn in batch:
            yield txn
        scanned += len(batch)
        highest = hi
        for txn in batch:  # advance past what the broker actually returned
            try:
                highest = max(highest, int(txn.get("id")))
            except (TypeError, ValueError):
                continue
        lo = highest + 1


def find_close_transaction(
    transport: Any, trade_id: str, *, to_id: int | None = None
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]] | None:
    """Locate the ORDER_FILL that closed ``trade_id`` in the transaction stream.

    Returns ``(fill_txn, tradesClosed_entry, txn_by_id)``; the index is returned so the
    caller can resolve the triggering SL/TP order's EXPECTED price for slippage without
    another API call (that order transaction always precedes the fill in the same range).
    Scans from the trade id itself — an OANDA trade id IS its opening ORDER_FILL
    transaction id, so the close can never precede it. Returns None when not found.
    """
    try:
        from_id = int(str(trade_id))
    except (TypeError, ValueError):
        return None  # non-numeric id (tests/other brokers) — no transaction range to scan
    last_id = to_id if to_id is not None else _last_transaction_id(transport)
    if last_id is None or last_id < from_id:
        return None
    txn_by_id: dict[str, dict[str, Any]] = {}
    found: tuple[dict[str, Any], dict[str, Any]] | None = None
    tid = str(trade_id)
    for txn in iter_transactions(transport, from_id, last_id):
        txn_by_id[str(txn.get("id"))] = txn
        if txn.get("type") != "ORDER_FILL":
            continue
        for closed in txn.get("tradesClosed") or []:
            if str(closed.get("tradeID")) == tid:
                found = (txn, closed)  # keep the LAST (a partial close may appear first)
    if found is None:
        return None
    return found[0], found[1], txn_by_id


def _exit_slippage_from_transaction(
    txn: dict[str, Any], close_price: float | None, instrument: str,
    txn_by_id: dict[str, dict[str, Any]] | None,
) -> float | None:
    """EXEC-013 on the transaction path: expected price = the triggering order's price.

    ``txn['orderID']`` points at the dependent order that fired, and its own transaction
    (STOP_LOSS_ORDER / TAKE_PROFIT_ORDER) carries the price actually in force — so stop
    modifications are reflected, exactly like the FILLED-dependent-order rule on the
    TradeDetails path. None when the close was not dependent-order driven.
    """
    if close_price is None or not instrument:
        return None
    if str(txn.get("reason") or "").upper() not in _DEPENDENT_FILL_REASONS:
        return None  # manual/flatten market closes have no expected price
    order_txn = (txn_by_id or {}).get(str(txn.get("orderID") or "")) or {}
    try:
        raw = order_txn.get("price")
        expected = float(raw) if raw else None
        if expected is None or expected <= 0:
            return None
        return compute_slippage_pips(expected, close_price, instrument)
    except (TypeError, ValueError):
        return None


def facts_from_transaction(
    txn: dict[str, Any], closed: dict[str, Any], *,
    instrument: str | None = None,
    txn_by_id: dict[str, dict[str, Any]] | None = None,
) -> CloseFacts | None:
    """Close facts from a closing ORDER_FILL transaction + its ``tradesClosed`` entry."""
    if not txn.get("id"):
        return None
    realized = closed.get("realizedPL", txn.get("pl"))
    units = closed.get("units", txn.get("units"))
    raw_price = closed.get("price") or txn.get("price")
    close_price = float(raw_price) if raw_price else None
    inst = str(instrument or txn.get("instrument") or "")
    return CloseFacts(
        close_txn_id=str(txn["id"]),
        realized_pnl=float(realized or 0.0),
        close_price=close_price,
        close_time=txn.get("time"),
        units=abs(float(units)) if units else None,
        slippage_pips=_exit_slippage_from_transaction(txn, close_price, inst, txn_by_id),
    )


def resolve_close_from_transactions(
    transport: Any, trade_id: str, *, instrument: str | None = None
) -> tuple[CloseFacts, str] | None:
    """(facts, exit_reason) for a vanished trade, or None. Never raises."""
    try:
        located = find_close_transaction(transport, trade_id)
    except Exception as exc:
        log_event(log, logging.WARNING, "transaction scan failed; retry later",
                  trade_id=str(trade_id), error=str(exc))
        return None
    if located is None:
        return None
    txn, closed, txn_by_id = located
    facts = facts_from_transaction(txn, closed, instrument=instrument, txn_by_id=txn_by_id)
    if facts is None:
        return None
    return facts, exit_reason_from_transaction(txn)


# --------------------------------------------------------------------------- #
# Flat close FillEvent (the exact shape S3's production validator accepts)
# --------------------------------------------------------------------------- #
def build_close_event(
    trade: ManagedTrade, facts: CloseFacts, exit_reason: str, event_time: str | None = None
) -> dict[str, Any]:
    """Build the flat ``status="closed"`` FillEvent for ``ams-inbound.ams``.

    Contract (S3 FillEvent.schema.json, additionalProperties:false): broker_order_id is
    the OANDA CLOSE transaction id (per-event dedup key — must differ from the entry's),
    order_request_id links back to the ApprovedOrder, signal_id is S3's journal match key,
    realized_pnl is required in practice (S3 books a 0-PnL close without it).
    """
    event: dict[str, Any] = {
        "schema_version": "1",
        "broker_order_id": str(facts.close_txn_id),
        "order_request_id": trade.order_request_id or f"s2-unknown:{trade.broker_trade_id}",
        "status": "closed",
        # ALWAYS carry a signal_id: without one S3 falls back to pair-matching and would
        # complete the OLDEST open journal row on the pair — potentially the wrong trade.
        # An unknown signal_id makes S3 skip safely with no mutation.
        "signal_id": trade.correlation_id or f"s2-unknown:{trade.broker_trade_id}",
        "direction": "long" if trade.direction == 1 else "short",
        "event_time": event_time or utc_now_iso(),
        "realized_pnl": float(facts.realized_pnl),
        "exit_reason": exit_reason if exit_reason in VALID_EXIT_REASONS else "other",
    }
    if _PAIR_RE.match(trade.instrument or ""):
        event["pair"] = trade.instrument
    if facts.units:
        event["units"] = int(round(facts.units)) * trade.direction  # negative for short
    if facts.close_price and facts.close_price > 0:
        event["fill_price"] = facts.close_price
    if facts.close_time:
        event["fill_time"] = facts.close_time
    if facts.slippage_pips is not None:  # EXEC-013: lands on the journal row at close
        event["slippage_pips"] = float(facts.slippage_pips)
    return event


# --------------------------------------------------------------------------- #
# Emission: persist-then-publish to the S3-side topic (durable, idempotent)
# --------------------------------------------------------------------------- #
class CloseEmitter:
    """ONE close-emission path for every close source (time exit, flatten, broker sweep).

    Rides the existing outbox machinery: the flat event is persisted to a dedicated
    outbox topic first, then drained to the configured S3-side topic by ``FillProducer``'s
    flush loop (ack only after successful publish — at-least-once, never lost). A
    persisted ledger keyed by broker trade id makes one broker close = one close event
    across restarts; any residual re-emission is harmless (S3 dedups on the deterministic
    close transaction id). Never raises.
    """

    def __init__(
        self,
        queue: Any,
        outbox: Any,
        topic: str,
        ledger: Any,  # SqliteProcessedStore-shaped: seen(key) / mark(key)
        *,
        outbox_topic: str = "close_outbox",
    ) -> None:
        self.topic = topic
        self.ledger = ledger
        self.outbox = outbox
        self.outbox_topic = outbox_topic
        self._publisher = FillProducer(queue, topic, outbox, outbox_topic=outbox_topic)

    def emit(self, trade: ManagedTrade, facts: CloseFacts, exit_reason: str) -> bool:
        """Durably emit one close. True iff the close is (now or already) recorded."""
        try:
            if self.ledger.seen(trade.broker_trade_id):
                return True  # already emitted (possibly a prior run) — idempotent no-op
            event = build_close_event(trade, facts, exit_reason)
            self.outbox.publish(self.outbox_topic, event)  # durable FIRST
            self.ledger.mark(trade.broker_trade_id)
            log_event(log, logging.INFO, "close event persisted",
                      trade_id=trade.broker_trade_id, close_txn_id=facts.close_txn_id,
                      exit_reason=event["exit_reason"], realized_pnl=event["realized_pnl"],
                      correlation_id=trade.correlation_id)
        except Exception as exc:  # fail-open: log, retry next sweep — never block trading
            log_event(log, logging.ERROR, "close event emission failed (will retry)",
                      trade_id=trade.broker_trade_id, error=str(exc),
                      correlation_id=trade.correlation_id)
            return False
        self.flush()
        return True

    def emit_broker_close(
        self, trade: ManagedTrade, resp: dict[str, Any] | None, exit_reason: str
    ) -> bool:
        """Emit from a captured ``adapter.close_trade`` response (time exit / flatten)."""
        try:
            facts = facts_from_close_response(resp)
        except Exception as exc:  # malformed broker response — never crash the close path
            log_event(log, logging.ERROR, "close response unusable; close not emitted",
                      trade_id=trade.broker_trade_id, error=str(exc))
            return False
        if facts is None:
            log_event(log, logging.WARNING, "close response carried no fill txn; close not emitted",
                      trade_id=trade.broker_trade_id)
            return False
        return self.emit(trade, facts, exit_reason)

    def flush(self) -> int:
        """Drain any parked close events to the S3 topic. Never raises."""
        try:
            return self._publisher.flush()
        except Exception as exc:
            log_event(log, logging.WARNING, "close outbox flush failed; will retry", error=str(exc))
            return 0


# --------------------------------------------------------------------------- #
# Broker-close sweep (detects SL/TP/manual closes of registered trades)
# --------------------------------------------------------------------------- #
@dataclass
class CloseSweeper:
    """Throttled broker sweep riding the engine tick. Entirely fail-open."""

    position_manager: Any  # PositionManager (trades: dict[str, ManagedTrade])
    transport: Any  # OandaTransport (get_open_trades / get_trade / get_account_summary)
    emitter: CloseEmitter
    interval_sec: float = 30.0
    # A trade whose /trades lookup is broken would otherwise trigger a full transaction
    # scan every sweep, forever. Back the fallback off to its own slower cadence.
    txn_fallback_interval_sec: float = 300.0
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))
    _last_sweep_at: datetime | None = field(default=None, repr=False)
    _txn_fallback_at: dict[str, datetime] = field(default_factory=dict, repr=False)

    def _fallback_due(self, trade_id: str, now: datetime) -> bool:
        last = self._txn_fallback_at.get(trade_id)
        if last is not None and (now - last).total_seconds() < self.txn_fallback_interval_sec:
            return False
        self._txn_fallback_at[trade_id] = now
        return True

    def sweep(self) -> dict[str, int]:
        """One throttled pass: ONE get_open_trades call, get_trade only for vanished ids."""
        now = self.clock()
        if (self._last_sweep_at is not None
                and (now - self._last_sweep_at).total_seconds() < self.interval_sec):
            return {}
        self._last_sweep_at = now
        self.emitter.flush()  # deliver any close still parked from a failed publish
        candidates = [t for t in self.position_manager.trades.values() if not t.closed]
        if not candidates:
            return {"candidates": 0}
        try:
            open_ids = {str(t.get("id")) for t in self.transport.get_open_trades()}
        except Exception as exc:  # broker/API error — skip this sweep entirely
            log_event(log, logging.WARNING, "close sweep skipped (open-trades fetch failed)",
                      error=str(exc))
            return {"sweep_error": 1}
        emitted = 0
        fallbacks = 0
        for trade in candidates:
            if trade.broker_trade_id in open_ids:
                continue
            details = None
            fetch_error: str | None = None
            try:
                details = self.transport.get_trade(trade.broker_trade_id)
            except Exception as exc:
                fetch_error = str(exc)  # e.g. 404 NO_SUCH_TRADE — the transaction path may know

            resolved: tuple[CloseFacts, str] | None = None
            source = "trade_details"
            if details and details.get("state") == "CLOSED":
                facts = facts_from_trade_details(details)
                if facts is not None:
                    resolved = (facts, exit_reason_from_trade_details(details))
            elif details:
                continue  # broker still reports it open — open-list lag, re-check next sweep

            if resolved is None:
                # /trades could not answer (404 / missing / CLOSED-but-no-close-ids).
                # The transaction stream is authoritative; scan it on its own cadence.
                if not self._fallback_due(trade.broker_trade_id, now):
                    continue
                log_event(log, logging.WARNING,
                          "trade lookup unusable; trying transaction fallback",
                          trade_id=trade.broker_trade_id, error=fetch_error,
                          state=(details or {}).get("state"))
                resolved = resolve_close_from_transactions(
                    self.transport, trade.broker_trade_id, instrument=trade.instrument)
                source = "transactions"
                if resolved is None:
                    log_event(log, logging.WARNING,
                              "close not found in trade details or transactions; retry later",
                              trade_id=trade.broker_trade_id)
                    continue
                fallbacks += 1

            facts, exit_reason = resolved
            if self.emitter.emit(trade, facts, exit_reason):
                trade.closed = True  # only after the close is durably recorded
                emitted += 1
                log_event(log, logging.INFO, "broker-side close detected and emitted",
                          trade_id=trade.broker_trade_id, source=source,
                          exit_reason=exit_reason,
                          correlation_id=trade.correlation_id)
        result = {"candidates": len(candidates), "closed": emitted}
        if fallbacks:
            result["via_transactions"] = fallbacks
        return result
