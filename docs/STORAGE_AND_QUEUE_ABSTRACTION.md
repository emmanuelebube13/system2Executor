# Storage & Queue Abstraction

> The pluggable contracts that keep System 2 isolated from Computer 1. All cross-system exchange
> goes through exactly three trust boundaries — **GCS** (artifacts), the **QueueBackend** (orders/
> fills/control), and **OANDA HTTPS** (broker). Nothing reaches Computer 1's Postgres or disk.

## 1. StorageBackend (artifact exchange — System 1 → System 2)

**Live backend:** `GCSBackend` (`STORAGE_PROVIDER=gcs`). Dev/test backend: `LocalFSBackend`
(`STORAGE_PROVIDER=localfs`). Same interface; chosen by config, never by code.

```
class StorageBackend(Protocol):
    def get_text(self, key: str) -> str: ...            # read latest.json manifest
    def download(self, key: str, dest: Path) -> Path: ... # stream artifact to disk
    def exists(self, key: str) -> bool: ...
    def list(self, prefix: str) -> list[str]: ...
```

- **Read-only, least-privilege** creds for Computer 2 (no write/delete to the model prefix).
- Manifest contract: `models/.../latest.json` lists the active artifact set + per-file SHA256.
- EXEC-001 protocol: poll `latest.json` (~15 min) → download changed files → **verify SHA256** →
  **atomic swap** (write to temp dir, fsync, rename) → keep **last-known-good**. Never swap on
  checksum mismatch. GCS unreachable ⇒ keep last-known-good, never block.

## 2. QueueBackend (order/fill/control transport)

**Live backend (LOGGED DECISION D-002):** **Google Cloud Pub/Sub** (`QUEUE_PROVIDER=pubsub`),
over HTTPS, no VPN required — matches the GCS isolation story; ack/redelivery/DLQ built in.
Dev/test backend: `LocalDurableBackend` (`QUEUE_PROVIDER=local`, SQLite/file-backed under
`state/queue/`). Same interface; build against `local` until Pub/Sub creds are provisioned.

```
class QueueBackend(Protocol):
    def consume(self, topic: str, handler: Callable[[Message], Ack]) -> None: ...
    def publish(self, topic: str, message: Message) -> None: ...
    def nack_to_dlq(self, msg: Message, reason: str) -> None: ...
```

### Logical topics

| Topic | Direction | Rights on Computer 2 |
|---|---|---|
| `AMS_Outbound_Queue` | System 3 → System 2 (approved, pre-sized orders) | **consume** |
| `AMS_Inbound_Queue` | System 2 → System 3 (fills + control events) | **produce** |

### Message envelope (every message)

```json
{
  "schema_version": "1",
  "message_id": "uuid",
  "idempotency_key": "stable-key-derived-by-system3",
  "correlation_id": "ties order ↔ fill",
  "granularity": "H1 | H4",
  "created_at": "UTC ISO-8601",
  "payload": { ... }
}
```

`idempotency_key` ⇒ OANDA `clientExtensions.id` / client request id. Replays are **no-ops**.

### Control-event envelope (System 2 → System 3 on `AMS_Inbound_Queue`)

```json
{ "event_type": "system2.emergency_stop | system2.order_rejected | system2.heartbeat |
                 system2.reconciliation | system2.message_dead_lettered | system2.degraded",
  "severity": "INFO | WARNING | CRITICAL",
  "correlation_id": "...",
  "context": { ... } }
```

### Dead-letter policy (DLQ)

A message that fails decode/validation/processing is retried up to **N** times (exponential
backoff). After N it is moved to the **DLQ** — a Pub/Sub dead-letter topic in production, or
`state/dlq/` for `local` — with full failure context, and a `system2.message_dead_lettered`
control event is emitted. The consumer **never** infinite-loops or crashes the session loop on a
poison message.

## 3. Failure behavior (the disconnected-computer contract)

| Channel | Failure | System 2 behavior |
|---|---|---|
| GCS `latest.json` | unreachable / checksum mismatch | keep last-known-good; never swap |
| `AMS_Outbound_Queue` | empty / unreachable / > 5 min stale | **PAUSE** (EXEC-008); do not trade |
| `AMS_Inbound_Queue` | unreachable | buffer fills locally, retry publish (EXEC-005) |
| Outbound message | poison | DLQ + control event; loop continues |
| OANDA | error | retry/backoff; market-hours guard; reconcile on resume |

Treat System 1 and System 3 as **eventually-present mailboxes**, not live services.
