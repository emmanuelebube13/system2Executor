"""EXEC-012 — backfill broker-side closes that never reached System 3.

Replays broker CLOSED trades since a timestamp through the SAME emission path (same
event shape, same outbox, same idempotency ledger) as the live close tracker, so a
future gap is repaired by rerunning this instead of hand-SQL (the 2026-07-16 incident).

Source of trade ids: the durable fill outbox (``state/queue/fill_outbox.db``), which
keeps every FILLED confirmation envelope forever — exactly the population of
session-opened trades System 3 knows entries for.

Each candidate is checked with ``transport.get_trade`` first. That endpoint is NOT
reliable for closed trades: on 2026-07-21 OANDA returned 404 ``NO_SUCH_TRADE`` for a
trade it had opened and stopped out the same day (absent from ``/trades`` in every
state), which is exactly the gap this tool exists to repair — so it would have repaired
nothing. When ``get_trade`` cannot answer, the tool falls back to the **transaction
stream** (``transactions/idrange``), which is authoritative and carries the close as an
``ORDER_FILL`` with ``tradesClosed[]``.

Emission is idempotent end-to-end: the ledger skips already-emitted trades and System 3
dedups on the deterministic close transaction id (identical from either source), so
rerunning is safe.

Run from the repo root (config/.env.system2 is resolved relative to the cwd):

    venv314\\Scripts\\python.exe tools\\backfill_closes.py --since 2026-07-15T00:00:00Z
    venv314\\Scripts\\python.exe tools\\backfill_closes.py --since 2026-07-15T00:00:00Z --dry-run

``--dry-run`` prints the would-be close events without publishing or ledger writes.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from system2.execution.close_tracker import (  # noqa: E402
    CloseEmitter,
    build_close_event,
    build_managed_trade,
    exit_reason_from_trade_details,
    facts_from_trade_details,
    iter_fill_envelopes,
    resolve_close_from_transactions,
)


def _parse_since(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO-8601 timestamp {value!r}: {exc}")
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _trade_from_envelope(envelope: dict) -> object | None:
    """Rebuild the trade's identity from a FILLED fill-confirmation envelope."""
    payload = envelope.get("payload") or {}
    if payload.get("realized_status") not in ("FILLED", "PARTIAL"):
        return None
    broker_trade_id = payload.get("broker_trade_id")
    if not broker_trade_id:
        return None
    return build_managed_trade(
        broker_trade_id=broker_trade_id,
        instrument=payload.get("instrument", ""),
        side=payload.get("side", "BUY"),
        entry_price=payload.get("fill_price") or 0.0,
        initial_stop_price=payload.get("stop_loss_price"),
        take_profit_price=payload.get("take_profit_price"),
        open_time=payload.get("fill_time"),
        granularity=envelope.get("granularity"),
        correlation_id=envelope.get("correlation_id"),
        order_request_id=envelope.get("idempotency_key"),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--since", required=True, type=_parse_since,
                        help="ISO-8601 timestamp; only closes at/after this are replayed")
    parser.add_argument("--dry-run", action="store_true",
                        help="print would-be close events; no publish, no ledger writes")
    args = parser.parse_args(argv)

    from system2.broker.oanda_transport import build_transport
    from system2.common.secrets import get_secrets

    secrets = get_secrets()
    transport = build_transport(secrets)
    outbox_path = secrets.get("FILL_OUTBOX_PATH", "state/queue/fill_outbox.db")

    emitter = None
    if not args.dry_run:
        from system2.common.queue_backend import build_queue
        from system2.execution.fill_producer import build_outbox
        from system2.execution.outbound_consumer import SqliteProcessedStore

        emitter = CloseEmitter(
            queue=build_queue(secrets),
            outbox=build_outbox(outbox_path),
            topic=secrets.get("S3_CLOSE_TOPIC", "ams-inbound.ams"),
            ledger=SqliteProcessedStore(
                secrets.get("CLOSE_LEDGER_PATH", "state/offsets/close_sweep.db")),
        )

    seen_ids: set[str] = set()
    scanned = emitted = skipped = 0
    for envelope in iter_fill_envelopes(outbox_path):
        trade = _trade_from_envelope(envelope)
        if trade is None or trade.broker_trade_id in seen_ids:
            continue
        seen_ids.add(trade.broker_trade_id)
        scanned += 1
        details = None
        fetch_error = None
        try:
            details = transport.get_trade(trade.broker_trade_id)
        except Exception as exc:  # 404 NO_SUCH_TRADE etc — the transaction path may still know
            fetch_error = exc

        facts = exit_reason = None
        source = "trade_details"
        if details and details.get("state") == "CLOSED":
            facts = facts_from_trade_details(details)
            if facts is None:
                print(f"[skip] trade {trade.broker_trade_id}: CLOSED but no closing "
                      f"transaction ids")
                skipped += 1
                continue
            exit_reason = exit_reason_from_trade_details(details)
        elif details:
            continue  # broker still reports it open — nothing to backfill
        else:
            resolved = resolve_close_from_transactions(
                transport, trade.broker_trade_id, instrument=trade.instrument)
            if resolved is None:
                why = f"broker fetch failed: {fetch_error}" if fetch_error else "trade not found"
                print(f"[skip] trade {trade.broker_trade_id}: {why}; "
                      f"no close in transaction stream either")
                skipped += 1
                continue
            facts, exit_reason = resolved
            source = "transactions"
            print(f"[fallback] trade {trade.broker_trade_id}: /trades unusable "
                  f"({fetch_error or 'not found'}); close recovered from transaction stream")

        try:
            closed_at = datetime.fromisoformat(str(facts.close_time).replace("Z", "+00:00"))
        except ValueError:
            closed_at = None
        if closed_at is None or closed_at < args.since:
            continue
        if args.dry_run:
            print(json.dumps(build_close_event(trade, facts, exit_reason), indent=2))
            emitted += 1
        elif emitter.ledger.seen(trade.broker_trade_id):
            print(f"[already-ledgered] trade {trade.broker_trade_id}: close previously "
                  f"emitted; skipping")
            skipped += 1
        elif emitter.emit(trade, facts, exit_reason):
            print(f"[emitted] trade {trade.broker_trade_id} close_txn {facts.close_txn_id} "
                  f"reason={exit_reason} pnl={facts.realized_pnl} via={source}")
            emitted += 1
        else:
            print(f"[failed] trade {trade.broker_trade_id}: emission failed (see logs)")
            skipped += 1

    mode = "dry-run: would emit" if args.dry_run else "emitted"
    print(f"done: scanned {scanned} session trades, {mode} {emitted} close event(s), "
          f"{skipped} skipped/failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
