"""EXEC-005 — publish fill confirmations to ``AMS_Inbound_Queue`` for System 3.

After every execution attempt (fill, partial, reject, cancel, expire) System 2 tells
System 3 what actually happened at the broker — so System 3 can release reserved risk,
update drawdown/consecutive-loss state, and attribute the realised outcome to the exact
model set (``model_set_id``, closing the loop with EXEC-001).

Reliability is **persist-then-publish** via a local durable **outbox**: the confirmation is
written to the outbox first, then published; on publish failure it stays in the outbox and
is retried (``flush``) until ``AMS_Inbound_Queue`` is reachable again — no fill is ever
lost if the queue is briefly unavailable. The message echoes the originating
``correlation_id``/``idempotency_key`` so a duplicate publish is harmless (System 3 dedups).
``emit_control`` rides the same durable path for lifecycle/control events (EXEC-010).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

from system2.common.logging import get_logger, log_event
from system2.common.queue_backend import make_envelope
from system2.execution.pipeline import ApprovedOrder

log = get_logger("execution.fill_producer")

VALID_REALIZED_STATUS = {"FILLED", "PARTIAL", "REJECTED", "CANCELLED", "EXPIRED"}

# The outbox must retry *forever* while the downstream queue is unreachable — a transient
# outage must never escalate a fill into a dead-letter and lose it. So the outbox-backing
# durable queue is configured with an effectively unbounded delivery-attempt ceiling.
_OUTBOX_MAX_ATTEMPTS = 10**9


def build_outbox(db_path: Any) -> Any:
    """Return a local durable backend suitable as a fill outbox (retry-forever semantics)."""
    from system2.common.queue_backend import LocalDurableBackend

    return LocalDurableBackend(db_path, max_attempts=_OUTBOX_MAX_ATTEMPTS)


@dataclass(frozen=True)
class FillResult:
    """Authoritative broker outcome (built by EXEC-006 from the OANDA fill/txn response)."""

    realized_status: str  # FILLED | PARTIAL | REJECTED | CANCELLED | EXPIRED
    filled_units: float = 0.0
    broker_order_id: str | None = None
    broker_trade_id: str | None = None
    requested_price: float | None = None
    fill_price: float | None = None
    fill_time: str | None = None  # ISO-8601 UTC
    slippage_pips: float | None = None  # signed, computed by EXEC-006
    stop_loss_price: float | None = None
    take_profit_price: float | None = None
    reject_reason: str | None = None
    model_set_id: str | None = None


class FillProducer:
    """Builds and durably publishes fill confirmations + control events."""

    def __init__(
        self,
        queue: Any,
        inbound_topic: str,
        outbox: Any,
        *,
        outbox_topic: str = "fill_outbox",
        retry_max: int = 5,
        clock: Callable[[], Any] | None = None,
    ) -> None:
        self.queue = queue
        self.inbound_topic = inbound_topic
        self.outbox = outbox
        self.outbox_topic = outbox_topic
        self.retry_max = retry_max

    def build_message(self, order: ApprovedOrder, fill: FillResult) -> dict[str, Any]:
        """Construct the ``AMS_Inbound_Queue`` fill-confirmation envelope (EXEC-005 §7.1)."""
        if fill.realized_status not in VALID_REALIZED_STATUS:
            raise ValueError(f"invalid realized_status {fill.realized_status!r}")
        payload: dict[str, Any] = {
            "ams_decision_id": order.ams_decision_id,
            "instrument": order.instrument,
            "side": order.side,
            "requested_units": order.units,
            "filled_units": fill.filled_units,
            "realized_status": fill.realized_status,
            "broker_order_id": fill.broker_order_id,
            "broker_trade_id": fill.broker_trade_id,
            "requested_price": fill.requested_price,
            "fill_price": fill.fill_price,
            "fill_time": fill.fill_time,
            "slippage_pips": fill.slippage_pips,
            "stop_loss_price": fill.stop_loss_price,
            "take_profit_price": fill.take_profit_price,
            "reject_reason": fill.reject_reason,
            "model_set_id": fill.model_set_id,
        }
        return make_envelope(
            payload=payload,
            idempotency_key=order.idempotency_key,
            correlation_id=order.correlation_id,
            granularity=order.granularity,
            event_type="fill_confirmation",
        )

    def publish_fill(self, order: ApprovedOrder, fill: FillResult) -> dict[str, Any]:
        """Persist the confirmation to the outbox, then attempt delivery. Returns the message."""
        msg = self.build_message(order, fill)
        self.outbox.publish(self.outbox_topic, msg)  # durable FIRST (persist-then-publish)
        self.flush()
        return msg

    def emit_control(
        self, event_type: str, payload: dict[str, Any], correlation_id: str = ""
    ) -> dict[str, Any]:
        """Durably publish a lifecycle/control event (PAUSED/RESUMED/STOPPED, etc.)."""
        msg = make_envelope(
            payload=payload,
            idempotency_key=f"control:{event_type}:{payload.get('reason', '')}",
            correlation_id=correlation_id,
            event_type=event_type,
        )
        self.outbox.publish(self.outbox_topic, msg)
        self.flush()
        return msg

    def flush(self) -> int:
        """Drain the outbox to ``AMS_Inbound_Queue``. Returns the count delivered this call.

        Each message is ack'd only after a successful publish; a publish failure nacks it
        so it is retried on the next ``flush`` (no loss, at-least-once + idempotent).
        """
        delivered = 0
        while True:
            batch = self.outbox.pull(self.outbox_topic, max_messages=10)
            if not batch:
                break
            progressed = False
            for m in batch:
                try:
                    self.queue.publish(self.inbound_topic, m.body)
                    m.ack()
                    delivered += 1
                    progressed = True
                except Exception as exc:
                    log_event(log, logging.WARNING, "fill publish failed; will retry",
                              error=str(exc),
                              correlation_id=m.body.get("correlation_id"))
                    m.nack(backoff_sec=0.0)  # available again immediately for next flush call
            if not progressed:
                break  # queue still down — stop looping, retry on the next flush
        return delivered
