# Queue Decoupling Patterns

**Skill ID:** `queue-decoupling`
**File:** `docs/implementation-roadmap/system-1-model-building/tasks/skills/queue-decoupling.md`
**Applies To:** `queue-nlp-agent` (MODEL-008, MODEL-010).

---

## Configuration

From `.env`:
```
QUEUE_URL=amqp://localhost:5672                    # RabbitMQ
# or
QUEUE_URL=redis://localhost:6379                   # Redis

SCORED_SIGNAL_QUEUE=scored_signal_queue
MAX_QUEUE_SIZE=10000
DLQ_NAME=scored_signal_dlq
BACKPRESSURE_TIMEOUT_MS=5000
BACKPRESSURE_MAX_RETRIES=3
```

---

## Message Contract (Scored_Signal_Queue)

```json
{
  "message_id": "<idempotency_key>",
  "signal_id": "uuid",
  "instrument": "EUR_USD",
  "granularity": "H1",
  "signal_time_utc": "2026-06-23T14:00:00Z",
  "direction": "long|short",
  "model_score": 0.83,
  "approved": true,
  "threshold_applied": 0.72,
  "regime": "Trending-Up",
  "regime_probs": {
    "trending_up": 0.72,
    "trending_down": 0.10,
    "ranging": 0.08,
    "high_vol": 0.10
  },
  "bundle_version": "2026-06-23T00:00:00Z",
  "produced_at_utc": "2026-06-23T14:00:05Z"
}
```

**Required fields:** All above fields must be present. Missing any = invalid message → DLQ.
**Schema version:** Add `"schema_version": "1.0.0"` to the message contract.

---

## Idempotency Key

```python
def build_message_id(signal_id: str, score_run_id: str) -> str:
    """
    Deterministic idempotency key.
    Same (signal_id, score_run_id) always produces the same message_id.
    """
    return f"{signal_id}:{score_run_id}"

# Consumer deduplication: use message_id as a dedupe key.
# If consumer has already processed this message_id, it's a no-op.
```

The consumer (System 3) is responsible for deduplication. The producer sends with `message_id`; the consumer maintains a set of processed IDs.

---

## RabbitMQ Publisher Pattern

```python
import pika
import json
import time
import random

def publish_signals(signals: list[dict], score_run_id: str):
    """Publish scored signals with backpressure."""

    connection = pika.BlockingConnection(pika.URLParameters(os.environ["QUEUE_URL"]))
    channel = connection.channel()

    # Declare queue with max length (backpressure)
    max_size = int(os.environ.get("MAX_QUEUE_SIZE", 10000))
    channel.queue_declare(
        queue=os.environ["SCORED_SIGNAL_QUEUE"],
        durable=True,
        arguments={"x-max-length": max_size, "x-overflow": "reject-publish"}
    )
    # Declare DLQ
    dlq_name = os.environ.get("DLQ_NAME", "scored_signal_dlq")
    channel.queue_declare(queue=dlq_name, durable=True)

    # Enable publisher confirms
    channel.confirm_delivery()

    published = 0
    dlq_count = 0
    backpressure_events = 0

    for signal in signals:
        message = build_message(signal, score_run_id)
        body = json.dumps(message)

        # Check current queue depth
        queue_state = channel.queue_declare(queue=os.environ["SCORED_SIGNAL_QUEUE"], passive=True)
        current_depth = queue_state.method.message_count

        if current_depth >= max_size * 0.95:
            # Backpressure: block with timeout
            backpressure_events += 1
            timeout_ms = int(os.environ.get("BACKPRESSURE_TIMEOUT_MS", 5000))
            max_retries = int(os.environ.get("BACKPRESSURE_MAX_RETRIES", 3))

            for retry in range(max_retries):
                time.sleep(timeout_ms / 1000 * (retry + 1))  # Linear backoff
                queue_state = channel.queue_declare(queue=os.environ["SCORED_SIGNAL_QUEUE"], passive=True)
                if queue_state.method.message_count < max_size:
                    break
            else:
                # Still full → DLQ
                route_to_dlq(channel, dlq_name, body, "QUEUE_FULL")
                dlq_count += 1
                continue

        try:
            channel.basic_publish(
                exchange="",
                routing_key=os.environ["SCORED_SIGNAL_QUEUE"],
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,           # Persistent
                    message_id=message["message_id"],
                    content_type="application/json",
                ),
            )
            published += 1
        except pika.exceptions.NackError:
            route_to_dlq(channel, dlq_name, body, "PUBLISH_NACK")
            dlq_count += 1

    connection.close()
    return {"published": published, "dlq": dlq_count, "backpressure": backpressure_events}


def route_to_dlq(channel, dlq_name, body, reason):
    dlq_message = {
        "original_message": json.loads(body),
        "dlq_reason": reason,
        "dlq_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    channel.basic_publish(
        exchange="",
        routing_key=dlq_name,
        body=json.dumps(dlq_message),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )


def build_message(signal, score_run_id):
    return {
        "message_id": build_message_id(signal["signal_id"], score_run_id),
        "signal_id": signal["signal_id"],
        "instrument": signal["instrument"],
        "granularity": signal["granularity"],
        "signal_time_utc": signal["signal_time_utc"],
        "direction": signal["direction"],
        "model_score": signal["model_score"],
        "approved": signal["model_score"] >= signal["threshold_applied"],
        "threshold_applied": signal["threshold_applied"],
        "regime": signal["regime"],
        "regime_probs": signal["regime_probs"],
        "bundle_version": signal["bundle_version"],
        "produced_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
```

---

## Redis Pub/Sub Alternative

```python
import redis

r = redis.Redis.from_url(os.environ["QUEUE_URL"])

def publish_signal_redis(signal, score_run_id):
    message = json.dumps(build_message(signal, score_run_id))

    # Check list length (queue depth)
    queue_name = os.environ["SCORED_SIGNAL_QUEUE"]
    max_size = int(os.environ.get("MAX_QUEUE_SIZE", 10000))
    current_len = r.llen(queue_name)

    if current_len >= max_size * 0.95:
        timeout_ms = int(os.environ.get("BACKPRESSURE_TIMEOUT_MS", 5000))
        time.sleep(timeout_ms / 1000)
        current_len = r.llen(queue_name)
        if current_len >= max_size:
            # DLQ
            dlq_message = {"original": message, "reason": "QUEUE_FULL"}
            r.lpush(os.environ.get("DLQ_NAME", "scored_signal_dlq"), json.dumps(dlq_message))
            return "dlq"

    r.lpush(queue_name, message)
    return "published"
```

---

## Backpressure Rules

1. **Never drop a valid scored signal silently.**
2. When queue depth ≥ 95% of `MAX_QUEUE_SIZE`:
   - Block the producer and retry with backoff.
   - If still full after max retries → route to DLQ (not silently drop).
3. Log every backpressure event with a metric.
4. Alert on DLQ growth rate (e.g., > 10 messages/minute).

---

## Delivery Semantics

- **Producer:** at-least-once (publisher confirms/acks).
- **Consumer:** deduplicates on `message_id` → effectively exactly-once.
- Message persistence: `delivery_mode=2` (survives broker restart).

---

## Decoupling Check (Static Analysis)

Before declaring MODEL-008 complete, run:

```bash
# Must return ZERO results — no Layer 4 import in System 1 scoring path
rg 'from src.layer4_executor|import.*layer4_executor' src/layer3_ml/ src/
```

This is a hard gate. The scoring path must have zero knowledge of the execution layer.

---

## In-Process Fallback Flag (Transition Only)

```python
# .env
USE_QUEUE_PRODUCER=true  # false = use in-process Layer 3→4 path

# In scoring code:
if os.environ.get("USE_QUEUE_PRODUCER", "false").lower() == "true":
    publish_to_queue(signals)   # MODEL-008 path
else:
    pass  # Legacy in-process path (Layer 4 loads champion and scores itself)
```

**This flag should be removed once System 3 consumption is proven stable.** Long-term, the in-process path must not exist — it defeats the System 1 / System 2 / System 3 separation.

---

## Observability Metrics

After each publish batch, log:
```python
logger.info(json.dumps({
    "event": "queue_publish",
    "published_count": published,
    "dlq_count": dlq_count,
    "backpressure_events": backpressure,
    "queue_depth": current_depth,
    "bundle_version": bundle_version,
}))
```

Metric thresholds for alerting:
- `dlq_count > 0` → WARNING
- `dlq_count > 50` (per run) → CRITICAL
- `backpressure_events > 10` (per run) → WARNING (consumer lag)
- `queue_depth > MAX_QUEUE_SIZE * 0.8` for > 5 minutes → WARNING
