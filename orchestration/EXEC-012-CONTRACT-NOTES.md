# EXEC-012 — Contract Recon Notes (Agent A)

Date: 2026-07-18. All paths absolute; line numbers verified against files as read today.
Path abbreviations used below:
- **S2** = `C:\Users\emman\OneDrive\Documents\Projects\working\scalablebrain\system2\system-2-execution-engine`
- **S3** = `C:\Users\emman\OneDrive\Documents\Projects\working\scalablebrain\system3\ams`
- **BRIDGE** = `C:\Users\emman\OneDrive\Documents\Projects\working\scalablebrain\bridge\s2s3_bridge.py`

---

## 1. S3 close wire shape

### 1.1 Routing (`classify_inbound`)
`S3\src\ams\service\consumers.py:35-41` — the inbound queue (`ams-inbound.ams`) carries fills AND account snapshots; classification is **structural on top-level flat fields**:
- `"broker_order_id" in body` → `"FillEvent"` (consumers.py:37-38). **The field must be flat/top-level** — an envelope with a nested `payload` does not classify.
- `"open_positions"` or `"as_of"` present → `"AccountSnapshot"` (consumers.py:39-40).
- Neither → `ContractError` → DLQ (consumers.py:41, 98-103).

After classification the body is schema-validated + freshness-checked (`validate_and_check_fresh`, consumers.py:97; `S3\src\ams\common\contracts.py:183-185`). **FillEvent is never-stale** — `FRESHNESS_SECONDS["FillEvent"] = None` (contracts.py:32-38, specifically :35), so `check_fresh` is a no-op for closes (contracts.py:168-170). No `produced_at` needed.

### 1.2 Dedup (fills dedup on `broker_order_id`)
- `idempotency_key()` for FillEvent returns `body.get("broker_order_id")` (consumers.py:48-49).
- Checked against `processed_messages` before handling (consumers.py:105-111, table access :77-90) and recorded after the handler commits (consumers.py:112-115).
- **Consequence for the builder**: the CLOSE event's `broker_order_id` MUST be different from the entry event's `broker_order_id`, or the consumer silently skips it as a duplicate (consumers.py:108-111). This matches the processor's design: "The event's own `broker_order_id` is per-event (different on the filled vs the closed event)" (`S3\src\ams\posttrade\processor.py:23-25`).
- Evidence from the live DB (`S3\state\db\ams.db`, read-only query 2026-07-18): `processed_messages` rows for queue `ams-inbound.ams` are keyed `'2517'`, `'2521'`, `'2525'`, `'2543'`, `'2547'`, `'2551'` (OANDA fill-transaction ids from entry fills) plus synthetic `s2:{uuid}:CANCELLED` keys (bridge fallback, BRIDGE:176).

### 1.3 Per-trade identity is `order_request_id`
- processor.py:22-25 (docstring, the spec statement) — `order_request_id` "links back to the ApprovedOrder and is constant across the trade's life"; used as the open book's `broker_trade_id` (processor.py:227 in `_handle_filled`, `broker_trade_id=order_request_id`).
- On close it is required (`body["order_request_id"]`, processor.py:327) and drives `close_position(conn, provider, order_request_id)` (processor.py:353 and :402).
- Schema: `order_request_id` required, "links back to ApprovedOrder" (`S3\contracts\v1\FillEvent.schema.json:14`, required list :8-10).

### 1.4 OPEN vs CLOSE discrimination
Dispatch is purely on the FillEvent `status` field (`handle_inbound`, processor.py:169-182):
- `status == "filled"` → `_handle_filled` (processor.py:171-172, :211-285): upserts open-book position keyed `order_request_id` and inserts the `trade_journal` entry row keyed UNIQUE `broker_order_id` (processor.py:244-271).
- `status == "closed"` → `_handle_closed` (processor.py:174-175, :325-433).
- `status == "rejected"` → log only (processor.py:176-177, :439-444).
- Any other status → warn no-op (processor.py:178-181). Schema restricts `status` to `["filled", "rejected", "closed"]` (FillEvent.schema.json:16).

### 1.5 Fields driving the close mutation (`_handle_closed`, processor.py:325-433)
| body field | read at | used as |
|---|---|---|
| `order_request_id` | :327 (**required**, KeyError if absent) | open-book removal key (:353, :402), logging |
| `signal_id` | :328 | **primary journal-row match key** (:344, `_find_entry_row` :291-322) |
| `realized_pnl` | :329 | pnl applied to balance/equity/daily/weekly/streak/peak/drawdown (:361-372, :384-390). **Defaults to `Decimal("0")` if absent** (`_dec(body.get("realized_pnl"))`, :78-82) — omitting it silently records a 0-PnL close. |
| `fill_price` | :330 | `trade_journal.exit_price` (:376-383) |
| `fill_time` (fallback `event_time`, fallback `now`) | :331 | `trade_journal.exit_time` + equity-curve ts (:376-395) |
| `slippage_pips` | :332 | `trade_journal.slippage_pips` (:376-383) |
| `exit_reason` | :333 | `trade_journal.exit_reason` (:376-383) |
| `event_time` | :334 | breaker-evaluation clock (`event_dt`, :423-427) |
| `pair` | :337-342 | fallback match key when `signal_id` absent (recovered from open book if omitted) |

**CRITICAL matching detail**: `_find_entry_row` (processor.py:291-322) does **NOT** match on `order_request_id` or `broker_order_id` — it finds the journal row by `signal_id` (preferred) or `pair` (fallback), requiring `exit_time IS NULL` (:309-313). So the close event **must carry the same `signal_id` the entry carried** (post-bridge, `signal_id` comes from the S2 envelope's `correlation_id` — see §2). If S2 emits a close without a matching signal_id and without pair-matchable open row, the close is skipped.

### 1.6 Guards that reject/skip a close
- **Already closed** (redelivery): `_find_entry_row` returns `("closed", None)` when rows exist but all have `exit_time` set → full no-op (processor.py:345-349; design note :29-33).
- **Unknown trade** (entry never seen): returns `("none", None)` → removes any stray open-book row but makes **no** account/journal mutation (processor.py:350-356).
- **Malformed** (fails schema: e.g. `status` not in enum, `exit_reason` not in enum, extra top-level field): DLQ at the consumer (consumers.py:98-103), never reaches the processor.

### 1.7 FillEvent.schema.json — full field list (`S3\contracts\v1\FillEvent.schema.json`)
- `additionalProperties: false` (line 7) — **any extra top-level field DLQs the message**.
- `required`: `schema_version, broker_order_id, order_request_id, status, event_time` (lines 8-10).
- `schema_version`: `const "1"` (line 12) — must be the exact string `"1"`.
- Optional: `signal_id` (string), `pair` (pattern `^[A-Z]{3}_[A-Z]{3}$`), `direction` (`long|short`), `units` (**integer**, signed), `fill_price` (number, `exclusiveMinimum: 0`), `fill_time` (date-time), `realized_pnl` (number, "present on close"), `slippage_pips` (number), `exit_reason` (**enum**: `tp, sl, manual, flatten, expiry, other`) (lines 15-25).
- S3 validates with its own dependency-free interpreter supporting exactly these keywords (contracts.py:8-11); nanosecond timestamps ARE accepted (proven in §6c).

---

## 2. Bridge translation (BRIDGE = bridge\s2s3_bridge.py)

Topology (BRIDGE:15-16, :48-52): S2 publishes envelopes on topic `ams-inbound` (`TOPIC_S2_EVENTS`, :50); bridge pulls them (:329), runs `translate_fill` (:163-207), and publishes the flat FillEvent to `ams-inbound.ams` (`TOPIC_S3_EVENTS`, :51, :333). Anything `translate_fill` returns `None` for is **parked on `bridge.unrouted`** (:337-343) — it never reaches S3.

### 2.1 `translate_fill` gate conditions (all must hold)
1. `env["event_type"] == "fill_confirmation"` (:166).
2. `env["payload"]` is a dict and `env["idempotency_key"]` truthy (:167-170).
3. `payload["realized_status"]` is a key of `_STATUS_MAP` (:171-174). `_STATUS_MAP` (:59-65) = `{FILLED→filled, PARTIAL→filled, REJECTED→rejected, CANCELLED→rejected, EXPIRED→rejected}` — **there is NO mapping to `"closed"`**.

### 2.2 Field mapping (what survives)
| S2 envelope/payload | → S3 FillEvent | cite |
|---|---|---|
| `payload.broker_order_id` (else synthetic `s2:{idempotency_key}:{realized_status}`) | `broker_order_id` | :176-179 |
| `idempotency_key` | `order_request_id` | :180 |
| `_STATUS_MAP[payload.realized_status]` | `status` | :171-181 |
| `created_at` (else now) | `event_time` | :182 |
| `correlation_id` | `signal_id` | :184-186 |
| `payload.instrument` (only if `XXX_YYY` shape) | `pair` | :187-189 |
| `payload.side` BUY/SELL | `direction` long/short | :190-191 |
| `payload.filled_units` (rounded to int; **forced negative when side==SELL**) | `units` | :192-197 |
| `payload.fill_price` (only if > 0) | `fill_price` | :198-200 |
| `payload.fill_time` | `fill_time` | :201-203 |
| `payload.slippage_pips` | `slippage_pips` | :204-206 |

**Dropped entirely** (never read by `translate_fill`): `realized_pnl`, `exit_reason`, plus `ams_decision_id`, `requested_units`, `broker_trade_id`, `requested_price`, `stop_loss_price`, `take_profit_price`, `reject_reason`, `model_set_id`. Grep evidence: no occurrence of `realized_pnl` or `exit_reason` anywhere in s2s3_bridge.py (searched during recon; also proven at runtime in §6e).

### 2.3 Verdict: a CLOSE **cannot** round-trip today's bridge unchanged
Proven programmatically (§6d): `translate_fill(<fill_confirmation envelope with realized_status="CLOSED">)` returns `None` → the close would be parked on `bridge.unrouted`, never reaching S3. Even if the status mapped, `realized_pnl`/`exit_reason` would be dropped (§6e), and S3 would record a 0-PnL close (§1.5).

Two viable routes for the builder (decision, with facts):
- **Route A — extend the bridge** (2 edits in BRIDGE): add `"CLOSED": "closed"` to `_STATUS_MAP` (:59-65) and copy `payload.realized_pnl` (float) + `payload.exit_reason` (only if in the schema enum) in `translate_fill` (:163-207). S2 then publishes the same envelope shape it already publishes (§3) with `realized_status="CLOSED"` — but note S2's `FillProducer.build_message` currently **raises** on `"CLOSED"` (`VALID_REALIZED_STATUS`, S2\src\system2\execution\fill_producer.py:28, raise :82-83), so S2's set needs `"CLOSED"` added too.
- **Route B — publish the flat FillEvent directly to `ams-inbound.ams`**, bypassing translation. Precedent exists in the bridge itself: `SnapshotRelay` publishes contract-valid AccountSnapshots straight onto `TOPIC_S3_EVENTS` (BRIDGE:293). S2 and the bridge share the same physical queue DB: S2 `config\.env.system2:34-38` sets `QUEUE_PROVIDER=local`, `QUEUE_LOCAL_PATH=...\scalablebrain\shared\queue\queue.db`; S2's `LocalDurableBackend` table schema (S2\src\system2\common\queue_backend.py:94-101) is identical to the bridge's `SharedQueue` (BRIDGE:86-93) — the bridge docstring says so explicitly (BRIDGE:78, "schema-compatible"). Caveat: this hard-codes the S3-side topic name in S2 and diverges from the pubsub-later story (.env.system2:32).

Either way, **what must arrive at S3** is exactly the candidate JSON in §6.

---

## 3. Today's ENTRY fill shape on the wire

### 3.1 Envelope construction
`FillProducer.build_message` (S2\src\system2\execution\fill_producer.py:80-108) builds:
```
payload = {
  ams_decision_id, instrument, side, requested_units, filled_units,
  realized_status, broker_order_id, broker_trade_id, requested_price,
  fill_price, fill_time, slippage_pips, stop_loss_price,
  take_profit_price, reject_reason, model_set_id
}                                        # fill_producer.py:84-101
```
wrapped by `make_envelope(payload, idempotency_key=order.idempotency_key, correlation_id=order.correlation_id, granularity=order.granularity, event_type="fill_confirmation")` (fill_producer.py:102-108), producing (S2\src\system2\common\queue_backend.py:31-51):
```
{ "schema_version": "1", "message_id": <uuid4>, "idempotency_key": ...,
  "correlation_id": ..., "created_at": <utc now iso>, "payload": {...},
  "granularity": ..., "event_type": "fill_confirmation" }
```
`realized_status` must be in `{FILLED, PARTIAL, REJECTED, CANCELLED, EXPIRED}` or `build_message` raises (fill_producer.py:28, :82-83). **No CLOSED member exists** — grep `CLOSED` across `S2\src\system2`: only `MARKET_CLOSED` hits (oanda_adapter.py:205, oanda_transport.py:22,52,65); grep `realized_pnl|exit_reason` across `S2\src\system2`: **zero matches**.

### 3.2 Publish path
`publish_fill` (fill_producer.py:110-115): persist-then-publish — writes the envelope to the durable outbox topic `"fill_outbox"` first (:113), then `flush()` (:114). `flush` (:131-156) drains the outbox to `self.inbound_topic` = `INBOUND_QUEUE_NAME` = `ams-inbound` (config\.env.system2:36; wired at lifecycle.py:265-266), acking each outbox row only after successful publish (:144-147).

Identity mapping for the trade's life: `order.idempotency_key` = S3's `order_request_id` and `order.correlation_id` = S3's `signal_id` — set by the bridge when the order came IN (`translate_order`, BRIDGE:146-147) and defined on `ApprovedOrder` (S2\src\system2\execution\pipeline.py:75-91).

### 3.3 DB evidence (optional, gathered)
Read-only query of `S3\state\db\ams.db` (2026-07-18): `trade_journal` rows id 1-6 have `broker_order_id` `2517/2521/2525/2543/2547/2551` (OANDA **order-fill transaction ids**; the corresponding OANDA trade ids are 2518/2522/2526/… = fill-txn id + 1), `signal_id` = the signal UUID, entry fields populated, and — decisive — every `exit_reason` reads `stop_loss[backfilled-from-broker-2026-07-16]` with `slippage_pips: None`: **all closes to date were hand-backfilled directly into the DB; no close event has ever arrived on the wire.**

---

## 4. `emit_close_fn` wiring + S2 close path

### 4.1 Production wiring: **None**
`build_from_secrets` constructs `PositionManager(adapter=adapter)` — `emit_close_fn` is not passed (S2\src\system2\execution\lifecycle.py:287), so it takes its dataclass default `None` (S2\src\system2\broker\position_manager.py:112). The only call site is guarded `if self.emit_close_fn is not None` (position_manager.py:190-191), so **in production it is dead code**.

### 4.2 What `PositionManager._close` does (position_manager.py:185-191)
1. `self.adapter.close_trade(trade.broker_trade_id)` (:186) → OANDA TradeClose. **The response — which contains the realized PL, close price, and time — is discarded** (return value unused).
2. Sets `trade.closed = True` (:187) — in-memory only, nothing persisted.
3. Logs (:188-189), then calls `emit_close_fn(trade)` if wired (:190-191).

Data in hand at that moment: only the `ManagedTrade` fields (position_manager.py:39-53): `broker_trade_id, instrument, side, entry_price, initial_stop_price, take_profit_price, open_time, granularity, max_duration_sec, correlation_id, current_stop, at_breakeven, time_actions_done, closed`. **No realized PL, no close price, no close time, no order_request_id** (`ManagedTrade` has no idempotency_key field — `correlation_id` is the signal_id). The close facts are in the discarded `adapter.close_trade` response (§5).

### 4.3 Explicit statements for the builder
- **S2-originated closes (time exits at 100% duration, position_manager.py:137-140) never reach S3 today** — `emit_close_fn` is None in production.
- The **flatten-on-stop** path also closes at the broker with no emission of any kind: lifecycle.py:205-215 calls `adapter.close_trade(tid)` directly, bypassing `PositionManager._close` entirely.
- **Broker-originated closes (SL/TP hit) are not even detected**: nothing in S2 polls for a tracked trade disappearing from the broker's open list. `startup_reconcile` (lifecycle.py:128-149) only *adopts* open trades — and is itself a production no-op because `reconcile_fn` is never passed to `ExecutionRuntime` (constructor call lifecycle.py:369-374 omits it; guard :132-133 returns 0). Grep `realized_pnl|exit_reason|CLOSED` in `S2\src\system2`: no close-tracking code exists anywhere.
- **Therefore EXEC-012 must route BOTH paths — S2-originated closes (time exit, flatten) AND broker-originated closes (SL/TP, detected by a sweep) — through ONE emission function** that builds the §6 event and rides the existing durable outbox (`FillProducer` publish path, §3.2).

---

## 5. Broker close facts (OANDA v20)

### 5.1 What exists in S2 today
- `adapter.close_trade(trade_id, units="ALL")` (S2\src\system2\broker\oanda_adapter.py:373-374) → `transport.close_trade` (S2\src\system2\broker\oanda_transport.py:87-91), wrapping `oandapyV20.endpoints.trades.TradeClose` (PUT `/v3/accounts/{accountID}/trades/{tradeID}/close`). **Its return value (currently discarded at position_manager.py:186) contains the close facts**: OANDA's response carries `orderFillTransaction` with `pl` (realized PL of the fill), `price`, `time`, `id` (the close transaction id — the natural new `broker_order_id`), and `tradesClosed: [{tradeID, units, price, realizedPL, ...}]`. For S2-originated closes, **no extra request is needed** — capture this response.
- `transport.get_trade(trade_id)` (oanda_transport.py:75-79), wrapping `oandapyV20.endpoints.trades.TradeDetails` (GET `/v3/accounts/{accountID}/trades/{tradeID}`). Works for **closed** trades too: OANDA returns the trade with `state: "CLOSED"`, `realizedPL`, `averageClosePrice`, `closeTime`, and `closingTransactionIDs` — i.e. **a closed-trade details fetch already exists in the transport**; the sweep can use it as-is for SL/TP closes. Exit-reason inference: the trade's linked `stopLossOrder`/`takeProfitOrder` (already read by the adapter at oanda_adapter.py:335-338) have `state: "FILLED"` on the one that triggered → map to schema enum `sl`/`tp`; unknown → `other`.
- `transport.get_open_trades()` (oanda_transport.py:69-73), wrapping `OpenTrades` — the sweep's detection primitive: any tracked trade absent from this list has closed at the broker. (Same call the adapter uses to reconcile after transient errors, oanda_adapter.py:222-243.)
- `transport.get_account_summary()` (oanda_transport.py:93-97) — not needed for closes but shows the same pattern.

### 5.2 What does NOT exist
**No transaction-history fetch exists.** Grep `TransactionList|TransactionIDRange|transactions` in `S2\src\system2`: the only hit is a docstring word (oanda_adapter.py:8). If ever needed (e.g. to attribute partial closes), it would wrap `oandapyV20.endpoints.transactions.TransactionsSinceID` (GET `/v3/accounts/{accountID}/transactions/sinceid`) following the existing `_request` pattern with its error taxonomy (oanda_transport.py:43-56). **For EXEC-012 it is not required**: `close_trade`'s response + `get_trade` on vanished trades cover both paths.

Field availability summary for a closed trade: `realizedPL` ✓ (TradeClose `orderFillTransaction.pl` / per-trade `tradesClosed[].realizedPL`; TradeDetails `trade.realizedPL`), `closeTime` ✓ (TradeDetails `trade.closeTime`; TradeClose `orderFillTransaction.time`), close price ✓ (TradeClose `orderFillTransaction.price`; TradeDetails `trade.averageClosePrice`), close transaction id ✓ (`orderFillTransaction.id` / `trade.closingTransactionIDs[-1]`).

---

## 6. Schema proof (Gate 1) — VALIDATION PASSED

### 6.1 Candidate CLOSE event (post-bridge = what S3 must receive on `ams-inbound.ams`)
```json
{
  "schema_version": "1",
  "broker_order_id": "2560",
  "order_request_id": "0f7a9d7e-1111-2222-3333-444455556666",
  "signal_id": "3cedddf6-0965-447b-a79c-8d89c57af610",
  "status": "closed",
  "pair": "EUR_USD",
  "direction": "short",
  "units": -307419,
  "fill_price": 1.14266,
  "fill_time": "2026-07-15T12:45:36.329990Z",
  "event_time": "2026-07-15T12:45:40.000000Z",
  "realized_pnl": -545.4201,
  "slippage_pips": 0.0,
  "exit_reason": "sl"
}
```
Semantics: `broker_order_id` = OANDA **close** transaction id (unique per event — required for consumer dedup, §1.2); `order_request_id` = the order's `idempotency_key` (per-trade identity, §1.3); `signal_id` = the order's `correlation_id` (**the journal-row match key**, §1.5); `fill_price` = exit price; `realized_pnl` mandatory in practice (defaults to 0 in S3 if omitted, §1.5); `exit_reason` must be one of `tp|sl|manual|flatten|expiry|other`.

### 6.2 Validation command + actual output
Script: `...\scratchpad\validate_close_event.py` (throwaway; imports S3's own production validator and the bridge module read-only). Command:
```
& "S2\venv314\Scripts\python.exe" "...\scratchpad\validate_close_event.py"
```
Actual output (2026-07-18):
```
=== (a) S3's own production validator (ams.common.contracts) ===
PASS: validate_and_check_fresh('FillEvent', candidate_close) raised nothing

=== (a2) classify_inbound routes it to FillEvent? ===
classified as: FillEvent | dedup key: 2560

=== (b) jsonschema package validation ===
jsonschema not installed in this venv (S3 validator above is the production gatekeeper)

=== (c) OANDA nanosecond timestamp probe (closeTime has 9 fractional digits) ===
PASS: nanosecond fill_time/event_time accepted by S3 validator

=== (d) bridge translate_fill probe: CLOSED envelope round-trip ===
_STATUS_MAP: {'FILLED': 'filled', 'PARTIAL': 'filled', 'REJECTED': 'rejected', 'CANCELLED': 'rejected', 'EXPIRED': 'rejected'}
translate_fill(CLOSED envelope) -> None

=== (e) what a FILLED envelope translates to (fields dropped?) ===
translate_fill(FILLED envelope) -> { ... 12 fields ... }
realized_pnl survives bridge? False
exit_reason survives bridge? False

=== (f) PRE-bridge S2 envelope vs FillEvent schema (does schema apply there?) ===
FAIL (expected — schema applies only post-bridge/flat): FillEvent failed validation: : missing
required field 'broker_order_id'; ... missing 'order_request_id'/'status'/'event_time'; unexpected...
```
Notes:
- (a) is the authoritative check — S3 validates with `ams.common.contracts.validate` (dependency-free interpreter, contracts.py:8-11), not the `jsonschema` package; `jsonschema` is not installed in S2's venv314, and the S3 repo has no venv of its own that was needed.
- (c) OANDA's RFC3339 nanosecond timestamps (e.g. `...36.329990644Z`) pass S3's `format: date-time` check and `_parse_dt` under venv314's Python — no truncation needed at this layer (S3's runtime showed the same tolerance: journal `exit_time` values with 9 fractional digits parse fine).
- (d)/(e) prove §2.3: today's bridge cannot carry a close, and even a mapped status would lose `realized_pnl`/`exit_reason`.
- (f) the FillEvent schema does **NOT** apply to the pre-bridge S2 envelope layer (nested `payload`, no flat required fields — fails as expected). The pre-bridge S2 envelope has no JSON-schema validator anywhere on its path (bridge validates nothing; S3 validates only the post-bridge flat event), so §3.1's envelope + the Route-A field additions (`realized_status: "CLOSED"`, `payload.realized_pnl`, `payload.exit_reason`, `payload.broker_order_id` = close txn id) is the pre-bridge contract.

---

## 7. Idempotency / durability across S2 restarts

Facts, then a recommendation.

1. **Fill outbox is durable and never prunes.** `FillProducer` persists every envelope to a `LocalDurableBackend` at `state/queue/fill_outbox.db` (build: fill_producer.py:36-40; path: lifecycle.py:264 + config\.env.system2:39) with retry-forever semantics (`max_attempts=10**9`, fill_producer.py:33). `ack` only marks rows `state='done'` (queue_backend.py:130-131); **there is no DELETE anywhere in queue_backend.py** — every envelope ever emitted (entries today; closes tomorrow) remains queryable in the outbox DB forever. It is written BEFORE publish (persist-then-publish, fill_producer.py:113-114), so it can never miss an emitted event.
2. **S3 is already idempotent against re-emission.** Consumer dedup on `broker_order_id` (consumers.py:48-49, :108-111) plus the journal-completion guard (`exit_time IS NULL`, processor.py:29-33, :345-349) mean a re-emitted close with the **same deterministic `broker_order_id`** (the OANDA close transaction id) is a guaranteed no-op at S3. Correctness therefore does NOT depend on S2 never re-emitting — only on the close `broker_order_id` being deterministic (from the broker, not a fresh uuid).
3. **Existing precedent for persisted sweep state**: `SqliteProcessedStore` (S2\src\system2\execution\outbound_consumer.py:47-75) — a tiny WAL sqlite `processed(idempotency_key PRIMARY KEY, processed_at)` ledger under `state/offsets/processed.db` (lifecycle.py:282).
4. **Not suitable as source of truth**: `Fact_Live_Trades` via `TradeRecorder` — best-effort, failures swallowed by design (trade_recorder.py:10-11, :92-94), and may be disabled entirely (lifecycle.py:271-280).

**Recommendation**: two layers, both cheap.
- **Correctness layer (mandatory, free)**: derive the close event's `broker_order_id` from the OANDA close transaction id (`orderFillTransaction.id` from `close_trade`, or `closingTransactionIDs[-1]` from `get_trade`). S3's dedup + completion guard then make any restart-induced re-emission harmless end-to-end (fact 2).
- **State layer (recommended)**: a `SqliteProcessedStore`-pattern ledger for the sweep (e.g. `state/offsets/close_sweep.db`, key = OANDA `broker_trade_id`) marking `close_emitted` — written in the same function that enqueues the close to the outbox. On restart the sweep rebuilds "session-opened trades still needing close-tracking" from the outbox DB (durable record of every FILLED confirmation incl. `payload.broker_trade_id`, fact 1) minus the ledger's already-closed set. Prefer the dedicated ledger over scanning the outbox for closes because outbox rows are JSON blobs keyed by topic, not by trade — but the outbox remains the durable *fallback* source (a lost ledger only causes harmless re-emission, per the correctness layer).

---

## Answers summary for the builder

- **Exact close-event field list (what S3 must receive, flat, on `ams-inbound.ams`)** — validated PASS against S3's production validator (§6):
  `schema_version:"1"` (const), `broker_order_id:<OANDA close txn id — MUST differ from entry's>`, `order_request_id:<order.idempotency_key>`, `signal_id:<order.correlation_id — REQUIRED in practice: it is the journal match key>`, `status:"closed"`, `pair`, `direction:long|short`, `units:<signed int>`, `fill_price:<exit price, >0>`, `fill_time:<closeTime ISO>`, `event_time:<emit time ISO>` (required), `realized_pnl:<number — REQUIRED in practice, else S3 books a 0-PnL close>`, `slippage_pips` (optional), `exit_reason:<tp|sl|manual|flatten|expiry|other>`. No other fields (`additionalProperties: false`).
- **Bridge today cannot carry it**: `_STATUS_MAP` has no `closed` mapping (BRIDGE:59-65; probe → `None`) and `translate_fill` drops `realized_pnl`/`exit_reason` (BRIDGE:163-207). Route A: add `"CLOSED": "closed"` + copy the two fields in the bridge AND add `"CLOSED"` to `VALID_REALIZED_STATUS` (fill_producer.py:28); S2 keeps publishing its normal envelope with `realized_status:"CLOSED"`, `payload.broker_order_id=<close txn id>`, `payload.realized_pnl`, `payload.exit_reason`. Route B: publish the flat event straight to `ams-inbound.ams` (SnapshotRelay precedent BRIDGE:293; shared queue.db per .env.system2:34-38). Route A preserves topology; Route B avoids touching S2's producer whitelist. Pick one; both end at the §6 JSON.
- **One emission function**: build it once (order identity + ManagedTrade + broker close facts → outbox via `FillProducer`), and call it from BOTH (a) `PositionManager.emit_close_fn` — currently `None` in production (lifecycle.py:287) — plus the flatten-on-stop loop (lifecycle.py:205-215), and (b) the new broker-close sweep (detect via `transport.get_open_trades` diff, fetch facts via `transport.get_trade` — both already exist, oanda_transport.py:69-79). Capture `close_trade`'s response in `PositionManager._close` (position_manager.py:186 currently discards it) — it already contains pl/price/time/txn-id.
- **Close facts sources**: S2-originated → TradeClose response (`orderFillTransaction.{id,pl,price,time}`, `tradesClosed[].realizedPL`); broker-originated (SL/TP) → `get_trade` on a vanished trade (`realizedPL`, `averageClosePrice`, `closeTime`, `closingTransactionIDs`; reason from which of `stopLossOrder`/`takeProfitOrder` is FILLED). No transaction-history endpoint exists or is needed.
- **Idempotency mechanism**: deterministic `broker_order_id` = OANDA close transaction id (S3 dedups on it, consumers.py:48-49, + journal guard processor.py:345-349 ⇒ re-emission is always safe) + a `SqliteProcessedStore`-style sweep ledger keyed `broker_trade_id` under `state/offsets/` to avoid re-emission noise; the never-pruned fill outbox (`state/queue/fill_outbox.db`, ack='done' only, queue_backend.py:130-131) is the durable fallback for rebuilding session-opened-trade state after restart.
- **Also fix while in there**: `reconcile_fn` is unwired (lifecycle.py:369-374) so `startup_reconcile` is a production no-op — after a restart the position manager tracks nothing, which the sweep's restart-rebuild must account for.
