# ADR-001 Phase 2 — build state

Checklist from the System 2 build brief §5. Tick only what is verified. Never re-run a
ticked step. See §0 of the brief for the resume protocol.

**Host:** `LAPTOP-T57QJODI` (Windows 11). **THIS IS NOT THE TRADING SYSTEM 2.**
`handoff/adr001/HOST-RULING-trading-1.md` (owner's decision) rules that the `trading-1` VM
in `europe-west1-b` is System 2, and that this workstation should be stood down as
non-trading with `EXEC_SHADOW=true` permanently. Work done here is development work that
**ports** to `trading-1`; it is not a deployment.

**Documents received.** All four "Read first" docs, plus the signal schema, are in
`gs://scalable-brain-artifacts/handoff/adr001/`. P1 is unblocked.

---

## P0 — Own blockers ✅ COMPLETE

- [x] **`QUEUE_LOCAL_PATH`** — *adapted, not applied as written.* The brief's fix (repoint at
      `/opt/scalablebrain/shared/queue/queue.db`) is for a Linux host. On this host the
      configured value is already correct: it resolves to
      `…\scalablebrain\shared\queue\queue.db`, a 3.2 MB SQLite file with 9,325 rows shared
      with System 3 and the bridge. **Left unchanged** — repointing it here would have
      disconnected a working queue.
- [x] Deleted the bogus **0-byte `queue.db`** at the workspace root (dated 2026-08-16). It
      was untracked, git-ignored, and empty — the residue of an earlier silent-create.
- [x] **Startup assertion** — `_assert_shared_queue` in `src/system2/common/queue_backend.py`.
      Runs *before* `sqlite3.connect`, so the empty file is never created. Refuses unless the
      path exists, is a non-empty file, and contains a `queue` table. Wired in `build_queue`
      only (the fill outbox and tests legitimately self-create). Escape hatch for a genuinely
      new deployment: `QUEUE_LOCAL_ALLOW_CREATE=true`.
      Verified refusing: missing path, 0-byte file, non-SQLite file, SQLite without a `queue`
      table, and a POSIX path on this host. 6 regression tests added.
- [x] **Purged `LIVE_SIGNAL_ENABLED` and every `SIGNAL_*`** from `config/.env.system2`:
      `SCORED_SIGNAL_QUEUE`, `SIGNAL_HEARTBEAT_TOPIC`, `LIVE_SIGNAL_ENABLED` (**was `true`**),
      `SIGNAL_GRANULARITIES`, `SIGNAL_INSTRUMENTS`, `SIGNAL_POLL_INTERVAL_SEC`,
      `SIGNAL_DEDUP_PATH`. Nothing in `src/` had read them since the producer was deleted in
      `b3b0abc`, so this was inert today — but Phase 2 reintroduces code that reads that name,
      and it would have deployed into an already-true flag. Purged rather than set false: an
      absent name fails loudly, a stale `true` publishes silently.
- [x] **`EXEC_SHADOW=true`** left in place, deliberately. Comes off at P6.
- [~] **Confirm a message crosses from System 3 to System 2** — *partial, see Blockers.*
      A publish→pull→ack round-trip through the shared file via `build_queue` succeeds, which
      proves System 2 is attached to the shared queue and not a private one. It does **not**
      prove System 3 currently publishes: the last real message on
      `ams-outbound.executor` is dated **2026-07-15**, five weeks stale. Needs System 3
      running to close.

## P1 — Sync and verify ⏸ UNBLOCKED, NOT STARTED

Spec is `BUNDLE-CONSUMER-GUIDE.md` §2–§4 and `SIGNING.md`.

- [x] Poll the **bucket-root** manifest — already correct: `MANIFEST_KEY=latest.json`.
- [ ] Verify the signature before the checksums — **algorithm independently confirmed
      working** (see *Verified against the live bundle*), not yet wired into the downloader.
- [ ] Verify every artifact's SHA256; one mismatch refuses the whole set.
- [ ] Refuse unless `status == "published"`.
- [ ] Keep `last_good`; fall back on refusal, never to local signals.

## P2 — Stale in-memory bundle ✅ COMPLETE (commit 9ee33ca)

- [x] `LiveRegimeDetector.load_bundle` cached forever and no production caller passed
      `force=True`. Confirmed, then fixed.
- [x] The cache is now keyed on the model set id, compared against the id last *observed on
      disk* — not the id loaded. Those differ legitimately when `active` is unloadable and we
      serve `last_good`, and comparing them would re-attempt the broken load on every call.
- [x] 4 regression tests. Verified they fail against the old behaviour, with observations
      stamped by the pre-swap set — the exact reported symptom.
- Approved in `P0-reply.md` §4, which also notes this is a **precondition for P4 being
  meaningful**, not merely a staleness fix.

## P3 — Install the code bundle ⏸ NOT STARTED
## P4 — Determinism gate ⏸ NOT STARTED
## P5 — Inference and emit ⏸ NOT STARTED (schema blocker CLOSED)
## P6 — Cutover ⏸ NOT STARTED

---

## Verified against the live bundle (2026-08-22, read-only)

Independent confirmation of the brief's §3 claims, done before any code was written:

- **Manifest signature VALID** — `gs://scalable-brain-artifacts/latest.json` +
  `latest.json.sig` against the published `system1_manifest_signing_key.pub`. RSA-PSS,
  MGF1/SHA256, MAX_LENGTH salt, over `json.dumps(manifest, sort_keys=True)`.
  256-byte signature, 2026-byte payload.
- `status: "published"`, `code_dirty: false`, `code_commit bad55cea200baf79541e6e5eeda14d35927ac61b`,
  `model_set_id 2026-08-21T16-29-15Z-372f6956_gk-d614163c`, 8 artifacts.
- **`code_bundle.zip` SHA256 matches** its manifest entry
  (`823397ab81fe085647b42b045937b2ac7d1076a551af052a3ab0f19a12fac2c1`). 133 entries;
  `DETERMINISM.md`, `reference_vector.json`, `candle_fingerprint.json` and `requirements.txt`
  all present, plus `src/layer0/strategies/` and `src/layer0/data_access/indicators.py`.

System 1's side of the handoff is sound. The blockers below are all on the consuming side.

## Blockers

1. ~~The four "Read first" documents do not exist.~~ **RESOLVED** — delivered to
   `gs://scalable-brain-artifacts/handoff/adr001/`. `SIGNING.md` matches, exactly, the
   verification already done independently against the live manifest.

2. ~~Schema v2 is not published.~~ **RESOLVED, by withdrawal.** `P0-reply.md` §2: v2 is
   withdrawn and System 1 now conforms to the **v1** contract verbatim
   (`signal-message-contract.json` is byte-equivalent in shape to
   `contracts/v1/ScoredSignal.schema.json`: `schema_version` const `"1"`,
   `additionalProperties: false`, same 18 properties). Ignore P5's `producer` /
   `model_set_id` / `reference_vector_ok` field list until v2 is agreed by all three systems
   as one coordinated release. System 1's emitter had in fact been live and sending
   `schema_version: "2.0.0"`; fixed on their side.
   **Carry-over, ours to raise:** System 3's `granularity` enum is
   `M15, M30, H1, H4, D, D1` — no `W1` — but System 1 processes W1 bars, so W1 signals now
   fail validation on System 1's side rather than dead-lettering on ours. If W1 should be
   tradeable, the enum needs it.

3. **System 3 → System 2 liveness unproven on THIS host.** Last real crossing 2026-07-15.
   `HOST-RULING` §5 reports a real drill signal `S1-DRILL-20260822T234641Z` published today
   with a real price and ATR, which may already sit in `scored-signals.ams` **on
   `trading-1`** — finding it there proves links 1–2 with no new work. Not checkable here.
   `ams-inbound.ams`: 530 `dead` + 530 `.dlq` — `P0-reply.md` §5 suggests checking whether
   they share one rejection reason (one fault repeated, not 530 faults).
   The unconsumed `scored-signals.ams` message dated 2026-08-17 is **a leaked test fixture**
   (`entry: 1.05` for USD_JPY at ~159). System 1 says discard it; they purged their copy.

4. **NEW — the trading host.** Everything from P3 onward, and all of P6, belongs on
   `trading-1`, which has not been touched. See *What is owed to `trading-1`*.

## What is owed to `trading-1` (none of it done)

Per `HOST-RULING-trading-1.md` §3 and §6, in this order. The VM is up
(`europe-west1-b`, `e2-medium`, RUNNING) and reachable via `gcloud compute`, but nothing
here has been run against it.

1. Find `S1-DRILL-20260822T234641Z` in the local `scored-signals.ams` — proves links 1–2 for free.
2. **Port** the P0 assertion (`bfa081d`) and the `SIGNAL_*` purge — *before* step 3, so a wrong
   path fails loudly instead of silently creating another empty database.
3. **Then** repoint `QUEUE_LOCAL_PATH` to `/opt/scalablebrain/shared/queue/queue.db` and delete
   the backslash-named file. On that host the path really is broken: `/proc/<pid>/fd` showed
   System 2 holding a 4 KB empty DB literally named `C:\Users\...` while System 3 held the
   live 92 MB file.
4. Verify both processes hold the **same fd** — the check that actually proves the link.
5. Start System 2's processes; let the drill reach the broker call under shadow mode.
6. `EXEC_SHADOW=false` against **practice** — owner's framing is demo-only, no live account.
   An owner decision, not to be taken on an agent's initiative.

Also stand this Windows box down explicitly as non-trading, `EXEC_SHADOW=true` permanent.

## Note for whoever resumes

The working tree carried **uncommitted F-107 work** (withdrawal handling in `downloader.py`
and `live_regime.py`, `done_at` retention column, `MODEL_VERIFY_STRICT` removal) when P0
started. It is **not** mine and was left untouched and uncommitted. Decide whether it lands
before P1.

## Log

- 2026-08-22 — P0 started. Host/spec mismatches found and confirmed with owner before any
  change: this is the right machine, the brief's Linux details are stale, docs to follow.
- 2026-08-22 — Bundle verification (signature + checksum + contents) run read-only. Clean.
- 2026-08-22 — Documents located at `handoff/adr001/` and read. Two rulings landed: P0
  accepted (the refusal to repoint the queue path explicitly endorsed as better than the
  brief), and the trading host is `trading-1`, not this box — so some P0 work was done on a
  machine that is not in the trading path. P5's schema blocker closed by System 1
  withdrawing v2 and conforming to v1.
- 2026-08-22 — P2 done and committed (9ee33ca). Full suite green: 319 passed, 1 skipped.
- 2026-08-22 — P0 complete except the System 3 liveness half of item 3. Full suite green:
  315 passed, 1 skipped.
  Surprise: the first edit of `config/.env.system2` used Python's locale codec and mangled
  the file's `—`/`§` characters, which broke `secrets.py`'s strict UTF-8 read. Restored from
  backup and redone with explicit `encoding="utf-8"`; new content is ASCII-only. Anything
  writing that file must pass the encoding explicitly.
