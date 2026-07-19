# EXEC-012 — Review / Verification Report (Agent D)

Date: 2026-07-18. Reviewer: Agent D (fresh eyes — authored no feature or test code).
Verification scripts (throwaway) live in the session scratchpad
(`...\scratchpad\xsys_proof.py`, `junit_full.xml`); the only repo write by D is this file.

## VERDICT: **APPROVE** (with 2 medium, 2 low non-blocking findings + nits below)

All four hard rules verified upheld with evidence. Full suite matches baseline
(217 passed / 3 known Windows-env failures, 19 new tests). The cross-system proof —
a real S2-built close event driven through S2's actual sweep/emitter into a real
in-process S3 `Service` over a **shared physical queue DB** — passed all 22 checks,
including journal completion, open-book removal, single PnL application, redelivery
no-op, unknown-trade skip, and never-stale clock skew.

---

## 1. Hard rules — evidence

### Rule 1: ZERO changes under `system3\` and `bridge\` — **UPHELD**
The S2 tree is untracked in the enclosing git repo, so this was verified by mtime scan
(session work began ~14:30 local 2026-07-18):

```
find system3 bridge -newermt "2026-07-18 14:00" -type f
```
Result — the ONLY files touched after 14:00 are:
- `__pycache__/*.pyc` under `system3\ams\src\ams\{common,service}` and `bridge\`
  (15:20) — byte-compile artifacts of the documented **read-only imports** by Agent A's
  validation probe and Agent C's oracle test. No `.py`, schema, or config source files.
- `system3\ams\state\db\ams.db-shm` / `ams.db-wal` (15:18) — SQLite WAL sidecars from
  A's documented **read-only** DB queries. The main `ams.db` mtime is pre-session.
  **Content verified unchanged** via read-only query (2026-07-18): `trade_journal` = 6
  rows, all `exit_reason = 'stop_loss[backfilled-from-broker-2026-07-16]'`,
  `ams_open_positions` = 0, `processed_messages` keys for `ams-inbound.ams` =
  `2517/2521/2525/2543/2547/2551` + 4 `s2:{uuid}:CANCELLED` — byte-for-byte what
  A's CONTRACT-NOTES §1.2/§3.3 recorded. No new rows, no new files.

S2 files modified this session (mtime scan of `src tools config orchestration`):
`close_tracker.py` (new), `lifecycle.py`, `position_manager.py`,
`test_close_tracker.py` (new), `test_position_manager.py`, `tools/backfill_closes.py`
(new), `config/.env.system2` (+template), `orchestration/EXEC-012-CONTRACT-NOTES.md`.
`test_position_manager.py` was NOT in B/C's declared lists — inspected: it is only the
consequential arity update for the new `emit_close_fn(trade, resp, reason)` signature
(line 139). Legitimate; flagged as a nit (undeclared).

### Rule 2: OutboundConsumer / FillProducer entry path unmodified — **UPHELD**
- `src\system2\execution\outbound_consumer.py`: matches every pre-EXEC-012 ground-truth
  anchor — `emit_fn: Callable[[ApprovedOrder, Any, Any], None]` at :103, invoked at
  :194-195, `SqliteProcessedStore` at :47-75. No close-related code in the file.
- `src\system2\execution\fill_producer.py`: `VALID_REALIZED_STATUS` (:28) still exactly
  `{FILLED, PARTIAL, REJECTED, CANCELLED, EXPIRED}` (no CLOSED); `build_message`
  (:80-108) payload field list and `make_envelope` call identical to A's §3.1;
  `publish_fill` (:110-115) unchanged persist-then-publish. The close path reuses
  `FillProducer` only as a flush engine over a SEPARATE outbox topic (`close_outbox`)
  and never calls `build_message`. Register-on-fill lives in the `lifecycle.py`
  `_emit_fill` lambda (:333-343), with `producer.publish_fill(order, fill)` first and
  UNTOUCHED — the entry wire shape is exactly the pre-EXEC-012 shape.
- Cross-system proof independently confirmed the entry path: the S3 journal row was
  opened by a `status="filled"` event keyed `broker_order_id=2517`, and the close event
  carried a different id (`2560`) as A's dedup analysis requires.

### Rule 3: Fail-open — **UPHELD** (every new external call path read)
- **Tick hook** (`lifecycle.py:127-132`): `close_sweeper.sweep()` wrapped in
  try/except; proven by test `test_engine_tick_survives_raising_sweeper` (a sweeper
  whose `sweep()` raises does not break `tick()`).
- **Sweep** (`close_tracker.py:389-429`): `emitter.flush()` never raises (internal
  try); `get_open_trades` failure → log + skip sweep (`{"sweep_error": 1}`, proven by
  `test_broker_error_during_sweep_fails_open_then_recovers`); per-trade `get_trade`
  failure → log + continue; non-CLOSED/lagging state → re-check next sweep; missing
  close facts → log + retry. `trade.closed` is set ONLY after durable emission, so a
  failed emit is retried next sweep (proven by
  `test_publish_failure_fails_open_and_retries_next_sweep`).
- **Emitter** (`close_tracker.py:330-372`): whole emit body inside try/except → returns
  False; `flush()` wraps `FillProducer.flush` (which itself acks-only-after-publish and
  nacks on failure). `emit_broker_close` guards the response parse too. Never raises.
- **Register-on-fill** (`lifecycle.py:337-343`): registration inside try/except AFTER
  the untouched `publish_fill` — a registration failure degrades close tracking only,
  never the fill path.
- **Flatten path** (`lifecycle.py:226-231`): emission wrapped in its own try/except so
  it can never block shutdown; per-trade close failures continue the loop.
- **Backfill** (`tools/backfill_closes.py:109-114, 124-139`): per-trade broker fetch
  failure → print + skip; emission failure → print + count as skipped; exit 0.

### Rule 4: Idempotency — **UPHELD**
Three independent layers, each verified:
1. **Deterministic `broker_order_id`** = OANDA close transaction id
   (`closingTransactionIDs[-1]` from TradeDetails; `orderFillTransaction.id` from
   TradeClose) — `test_reemission_with_fresh_ledger_has_same_broker_order_id` and
   cross-system check: lost-ledger re-emission produced 2 wire events **both keyed
   `2560`**, and S3 applied PnL exactly once.
2. **Persisted ledger** keyed broker_trade_id (`SqliteProcessedStore` pattern,
   `state/offsets/close_sweep.db`): one broker close = one wire event across restarts
   (`test_restart_between_close_and_sweep_does_not_reemit`; cross-system check:
   two emitters sharing the ledger produced exactly ONE wire event for txn 8860).
3. **S3-side dedup + journal-completion guard**: proven in-process (below, item 3).

---

## 2. Full suite — MATCHES BASELINE

```
.\venv314\Scripts\python.exe -m pytest -q --junitxml=...\junit_full.xml
junit: {'tests': '220', 'failures': '3', 'errors': '0', 'skipped': '0', 'time': '13.008'}
```
- **217 passed / 3 failed** = baseline 198 passed + 19 new EXEC-012 tests, 3 failures
  unchanged and exactly the known Windows-env set:
  `test_downloader.py::test_new_set_moves_last_good` and `::test_rollback_to_last_good`
  (symlink `PermissionError: [WinError 5]` — Developer Mode) and
  `test_db.py::test_postgres_config_failcloses_without_dsn` (env-file artifact).
- `skipped: 0` — the S3-oracle contract test (`test_close_events_pass_s3_production_validator`)
  RAN (S3 repo present), it did not skip.
- Matches Agent C's reported 217/3 exactly.

## 3. Cross-system proof — **PASSED (22/22 checks)**

Script: scratchpad `xsys_proof.py` (harness.py pattern: real `ams.service.main.Service`,
fresh migrated temp DB + temp local queue + pinned clock; S3 imported read-only, real
`state\db\ams.db` never opened for write). Key design point verified first:
**S2's `LocalDurableBackend` and S3's local queue backend have byte-identical `queue`
table schemas**, so S2's real emitter wrote into the SAME physical `queue.db` the S3
service consumed from — Route B exercised literally, with NO bridge involvement.

Sequence and results (all PASS):
1. `SUB_AMS_INBOUND == "ams-inbound.ams" == S3_CLOSE_TOPIC` config value.
2. ENTRY flat FillEvent (`status="filled"`, `broker_order_id=2517`,
   `order_request_id=0f7a9d7e-…`, `signal_id=3cedddf6-…`, EUR_USD short −307419)
   through the real S3 consumer → journal row opened (`exit_time NULL`), open-book row
   keyed by `order_request_id`.
3. S2's ACTUAL code path end-to-end: `managed_trade_from_fill` (production
   register-on-fill mapping) → `PositionManager.register` → real `CloseSweeper.sweep()`
   with a stub transport returning a realistic OANDA CLOSED TradeDetails payload
   (nanosecond `closeTime`, `closingTransactionIDs=["2559","2560"]`, SL FILLED) → real
   `CloseEmitter` → shared queue DB. Result `{'candidates': 1, 'closed': 1}`.
4. **Wire event == `build_close_event` output exactly** (modulo the emit-time
   `event_time` stamp): flat, 12 fields, `broker_order_id="2560"`, `status="closed"`,
   `realized_pnl=-545.4201`, `exit_reason="sl"`, `units=-307419` (signed int),
   `fill_time` nanosecond passthrough. No translation happened or was needed.
5. `classify_inbound` → `FillEvent`; S3's production `validate_and_check_fresh` accepts.
6. S3 consumed it: journal row completed (`exit_time=2026-07-15T12:45:36.329990Z`,
   `exit_price=1.14266`, `exit_reason='sl'`, `realized_pnl=-545.4201`), open-positions
   row REMOVED, account mutated exactly once (balance 100000 → 99454.5799,
   consecutive_losses 0 → 1), `processed_messages` contains `'2560'`.
7. **Second delivery no-op**: simulated lost-ledger restart re-emitted the same event
   (2 wire copies of `2560` total in the queue DB); S3 skipped the duplicate — account,
   journal unchanged.
8. **Unknown-trade close** (entry never reached S3; legacy trade with
   `order_request_id=None` → event carries `s2-unknown:9999`): passes S3 schema, S3
   logs "close for unknown trade — skipped", NO account/journal mutation, and the
   service keeps processing subsequent fills (not wedged).
9. **Clock skew**: a close with future-dated `fill_time`/`event_time` validates —
   FillEvent is never-stale (`FRESHNESS_SECONDS["FillEvent"] = None`), confirmed A §1.1.
10. **Two S2 instances, shared ledger + shared queue DB**: both swept the same closed
    trade; exactly ONE wire event reached the queue (ledger suppressed the second —
    `SqliteProcessedStore` WAL cross-connection visibility separately verified), S3
    booked the PnL exactly once. No DB corruption (WAL, autocommit, INSERT OR IGNORE).

## 4. Adversarial pass — findings

### MEDIUM-1: closes during S2 downtime are never auto-emitted after restart
`startup_reconcile` adopts only trades still OPEN at the broker. A trade whose SL/TP
fired while S2 was down is absent from `get_open_trades`, is never registered, and the
sweep never sees it — its close reaches S3 only when an operator runs
`tools\backfill_closes.py`. A's §7 recommendation (rebuild "session-opened trades still
needing close-tracking" from the fill outbox minus the ledger) was NOT implemented.
The primary incident class (S2 running, broker-side close) IS covered; this is the
restart window only, and the backfill tool is the designed repair. **Recommendation
(non-blocking)**: run the backfill (or an equivalent outbox−ledger sweep seed) at
engine startup, or add it as a mandatory post-restart ops step in the runbook.

### MEDIUM-2: pair-fallback can mis-close the wrong journal row for identity-less trades
`build_close_event` omits `signal_id` when `correlation_id` is empty (e.g. a manual/
legacy broker trade adopted at startup with no `sb-` client id and no outbox record).
S3's `_find_entry_row` (processor.py:291-322) then matches by `pair` and completes the
OLDEST open journal row on that pair — which could be a different, attributed trade
(PnL and exit facts land on the wrong row). Narrow window: requires an adopted
identity-less trade closing while a same-pair attributed journal row is open — but note
the planned human-verification step (manual practice trade) plus an engine restart is
exactly this shape. Proven-safe alternative already exists in the same code path: when
`signal_id` IS present-but-unknown S3 safely skips (proof item 8). **Recommendation
(non-blocking, 1-line)**: always emit `signal_id`, falling back to
`s2-unknown:<broker_trade_id>` when `correlation_id` is empty, so identity-less closes
take the safe signal-match→"none"→skip path instead of pair fallback.

### LOW-1: flatten-on-stop emission failure loses the close until backfill
In `shutdown()` flatten, `trade.closed=True` is set before emission; if the durable
`outbox.publish` itself fails (disk error) during shutdown, the close is not parked
anywhere and the process exits. Repairable by backfill; requires disk failure at the
exact shutdown moment. Acceptable.

### LOW-2: partial broker closes emit one event at final close
A trade partially closed at the broker keeps `state=OPEN` (still in the open list), so
no event is emitted until the final close; the single event then carries
`units=initialUnits` and `realizedPL` = lifetime total from TradeDetails. Correct
totals, single application (S3 ignores `units` on close); interim S3 exposure is
slightly overstated between partial and final close. Documented-behavior note, not a bug.

Also examined, no issue found:
- **PARTIAL entry fills** register for tracking (`managed_trade_from_fill` accepts
  PARTIAL; proven) — consistent with the bridge mapping PARTIAL→filled for the entry.
- **Close between register and first sweep**: first sweep detects and emits (this is
  literally the main proof flow).
- **Open-list lag** (trade missing from open list but `get_trade` not CLOSED yet):
  `sweep` re-checks next sweep (close_tracker.py:416-417), no event, no crash.
- **Sweep vs shutdown race**: the runtime is single-threaded — `run()`'s tick loop
  exits before `shutdown()` runs; sweep and flatten cannot interleave. Flatten marks
  the ledger, and a closed-at-broker trade is not re-adopted on restart, so no
  double-emit path exists.
- **Shared outbox instance** (fill producer and close emitter share one
  `LocalDurableBackend`): separate topics (`fill_outbox` vs `close_outbox`);
  `iter_fill_envelopes` filters `topic='fill_outbox'` AND
  `event_type='fill_confirmation'` — no cross-contamination.

## 5. Nits (non-blocking)

- `tools\backfill_closes.py` prints `[emitted]` (and counts `emitted`) for a trade the
  ledger already recorded — `CloseEmitter.emit` returns True for the idempotent no-op.
  Wire truth is correct (the test asserts queue contents); the label is misleading.
- `lifecycle.py:324` reads the sweep interval via `get_int` — fractional
  `CLOSE_SWEEP_INTERVAL_SEC` values are unsupported (cosmetic; default 30 is fine).
- `src\system2\broker\tests\test_position_manager.py` was modified (necessary
  `emit_close_fn` arity update) but did not appear in B's or C's declared file lists.
- Read-only imports of S3/bridge modules generated `__pycache__/*.pyc` files under
  `system3\` and `bridge\` — unavoidable CPython import artifact, no source changes;
  harmless, but could be avoided in future recon with `PYTHONDONTWRITEBYTECODE=1`.
- `build_managed_trade` hard-codes `max_duration_sec=0.0` (time exits disabled for
  session trades) — matches today's production reality (no duration policy configured)
  and is commented as such, but is a behavior decision worth the orchestrator's eyes.

## 6. Gate-3 statement

Suite green vs baseline; cross-system proof passed end-to-end against the real S3
service; hard rules 1-4 verified with evidence. **APPROVE for merge.** Recommend
tracking MEDIUM-1 (startup backfill) and MEDIUM-2 (always-emit `signal_id`) as
fast-follow items before the Sunday 20:00 UTC window's first restart-with-open-positions
or manual-trade scenario.
