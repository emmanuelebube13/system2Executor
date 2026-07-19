"""EXEC-005 producer tests — message build, persist-then-publish, outbox reliability."""

from __future__ import annotations

from pathlib import Path

import pytest

from system2.execution.fill_producer import FillProducer, FillResult, build_outbox
from system2.execution.pipeline import ApprovedOrder, RiskContext

INBOUND = "AMS_Inbound_Queue"


def _order(**over) -> ApprovedOrder:
    base = dict(
        idempotency_key="k1", correlation_id="c1", instrument="EUR_USD", side="BUY",
        units=10000, granularity="H1", risk_context=RiskContext(atr=0.0010),
        ams_decision_id="ams-9",
    )
    base.update(over)
    return ApprovedOrder(**base)


def _fill(**over) -> FillResult:
    base = dict(
        realized_status="FILLED", filled_units=10000, broker_order_id="o1",
        broker_trade_id="t1", requested_price=1.10000, fill_price=1.10002,
        fill_time="2026-06-24T12:00:01Z", slippage_pips=0.2,
        stop_loss_price=1.099, take_profit_price=1.103, model_set_id="set-2026-06-30",
    )
    base.update(over)
    return FillResult(**base)


class FakeQueue:
    """Inbound queue stub; ``down=True`` makes publish raise (simulated outage)."""

    def __init__(self) -> None:
        self.delivered: list[dict] = []
        self.down = False

    def publish(self, topic: str, body: dict) -> None:
        if self.down:
            raise ConnectionError("AMS_Inbound_Queue unavailable")
        self.delivered.append(body)


@pytest.fixture
def queue() -> FakeQueue:
    return FakeQueue()


@pytest.fixture
def producer(queue: FakeQueue, tmp_path: Path) -> FillProducer:
    return FillProducer(queue, INBOUND, build_outbox(tmp_path / "outbox.db"))


# ----- message construction -------------------------------------------------------
def test_message_echoes_order_keys(producer: FillProducer):
    msg = producer.build_message(_order(), _fill())
    assert msg["idempotency_key"] == "k1"
    assert msg["correlation_id"] == "c1"
    assert msg["granularity"] == "H1"
    assert msg["event_type"] == "fill_confirmation"
    p = msg["payload"]
    assert p["realized_status"] == "FILLED"
    assert p["filled_units"] == 10000 and p["requested_units"] == 10000
    assert p["broker_order_id"] == "o1" and p["model_set_id"] == "set-2026-06-30"
    assert p["slippage_pips"] == 0.2


@pytest.mark.parametrize("status", ["FILLED", "PARTIAL", "REJECTED", "CANCELLED", "EXPIRED"])
def test_all_realized_statuses_build(producer: FillProducer, status: str):
    msg = producer.build_message(_order(), _fill(realized_status=status))
    assert msg["payload"]["realized_status"] == status


def test_partial_fill_reports_both_unit_counts(producer: FillProducer):
    msg = producer.build_message(_order(units=10000), _fill(realized_status="PARTIAL", filled_units=4000))
    assert msg["payload"]["requested_units"] == 10000
    assert msg["payload"]["filled_units"] == 4000


def test_reject_includes_reason(producer: FillProducer):
    msg = producer.build_message(
        _order(), _fill(realized_status="REJECTED", filled_units=0, reject_reason="MARKET_HALTED")
    )
    assert msg["payload"]["reject_reason"] == "MARKET_HALTED"


def test_invalid_status_raises(producer: FillProducer):
    with pytest.raises(ValueError):
        producer.build_message(_order(), _fill(realized_status="WAT"))


# ----- persist-then-publish + outbox reliability ----------------------------------
def test_publish_fill_delivers(producer: FillProducer, queue: FakeQueue):
    producer.publish_fill(_order(), _fill())
    assert len(queue.delivered) == 1
    assert queue.delivered[0]["correlation_id"] == "c1"


def test_outage_then_recovery_no_loss(producer: FillProducer, queue: FakeQueue):
    queue.down = True
    producer.publish_fill(_order(), _fill())  # publish fails -> stays in outbox
    assert queue.delivered == []
    # queue recovers; flush drains the outbox exactly once
    queue.down = False
    delivered = producer.flush()
    assert delivered == 1
    assert len(queue.delivered) == 1
    # nothing left to flush
    assert producer.flush() == 0


def test_multiple_fills_buffered_during_outage(producer: FillProducer, queue: FakeQueue):
    queue.down = True
    producer.publish_fill(_order(idempotency_key="k1", correlation_id="c1"), _fill())
    producer.publish_fill(_order(idempotency_key="k2", correlation_id="c2"), _fill())
    assert queue.delivered == []
    queue.down = False
    assert producer.flush() == 2
    assert {m["correlation_id"] for m in queue.delivered} == {"c1", "c2"}


def test_emit_control_durably_published(producer: FillProducer, queue: FakeQueue):
    producer.emit_control("PAUSED", {"reason": "queue_stale"}, correlation_id="ctl-1")
    assert len(queue.delivered) == 1
    assert queue.delivered[0]["event_type"] == "PAUSED"
    assert queue.delivered[0]["payload"]["reason"] == "queue_stale"
