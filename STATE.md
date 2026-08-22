# ADR-001 Phase 2 — build state

Checklist from the System 2 build brief §5. Tick only what is verified. Never re-run a
ticked step. See §0 of the brief for the resume protocol.

**Host:** `LAPTOP-T57QJODI` (Windows 11). The brief describes Computer 2 as a Linux host;
that detail is stale — confirmed with the owner 2026-08-22. P0 was adapted accordingly and
the deviations are recorded below.

**Currently blocked at P1** pending four documents (see *Blockers*).

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

## P1 — Sync and verify ⏸ BLOCKED (docs)

- [x] Poll the **bucket-root** manifest — already correct: `MANIFEST_KEY=latest.json`.
- [ ] Verify the signature before the checksums — **algorithm independently confirmed
      working** (see *Verified against the live bundle*), not yet wired into the downloader.
- [ ] Verify every artifact's SHA256; one mismatch refuses the whole set.
- [ ] Refuse unless `status == "published"`.
- [ ] Keep `last_good`; fall back on refusal, never to local signals.

## P2 — Stale in-memory bundle ⏸ NOT STARTED

- [ ] `LiveRegimeDetector.load_bundle` caches forever
      (`src/system2/artifact_sync/live_regime.py`: `if self._bundle is not None and not force`).
      **Confirmed present.** No production caller passes `force=True`.
- [ ] Force a detector reload on every bundle swap.

## P3 — Install the code bundle ⏸ NOT STARTED
## P4 — Determinism gate ⏸ NOT STARTED
## P5 — Inference and emit ⏸ BLOCKED (schema v2 unpublished)
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

1. **The four "Read first" documents do not exist** — `docs/BUNDLE-CONSUMER-GUIDE.md`,
   `docs/design/ADR-001-where-inference-runs.md`, `src/serializer/SIGNING.md`,
   `src/serializer/DETERMINISM.md`. Absent from this repo, from the parent workspace, and
   from the bucket (`handoff/` and `contracts/` hold only schemas and scoring scripts).
   P1–P5 are written as "implement per guide §2–§7". Owner is supplying them.

2. **Schema v2 is not published.** The bucket has only `contracts/v1/ScoredSignal.schema.json`.
   It already carries the field names the brief credits to v2 (`pair`, `proposed_entry`,
   `proposed_sl`, `proposed_tp`, `atr`) but has **no** `producer`, `model_set_id` or
   `reference_vector_ok` — and it is `additionalProperties: false`. P5's emit step, written
   literally, would be dead-lettered by the only published contract. `scored-signals.ams`
   already shows 8 `dead` + 8 in `.dlq`.

3. **System 3 → System 2 liveness unproven** (P0, above). Last real crossing 2026-07-15.
   Also worth a look before P6: `ams-inbound.ams` carries 530 `dead` + 530 in `.dlq`, and
   `scored-signals.ams` has one unconsumed `ready` message dated 2026-08-17.

## Note for whoever resumes

The working tree carried **uncommitted F-107 work** (withdrawal handling in `downloader.py`
and `live_regime.py`, `done_at` retention column, `MODEL_VERIFY_STRICT` removal) when P0
started. It is **not** mine and was left untouched and uncommitted. Decide whether it lands
before P1.

## Log

- 2026-08-22 — P0 started. Host/spec mismatches found and confirmed with owner before any
  change: this is the right machine, the brief's Linux details are stale, docs to follow.
- 2026-08-22 — Bundle verification (signature + checksum + contents) run read-only. Clean.
- 2026-08-22 — P0 complete except the System 3 liveness half of item 3. Full suite green:
  315 passed, 1 skipped.
  Surprise: the first edit of `config/.env.system2` used Python's locale codec and mangled
  the file's `—`/`§` characters, which broke `secrets.py`'s strict UTF-8 read. Restored from
  backup and redone with explicit `encoding="utf-8"`; new content is ASCII-only. Anything
  writing that file must pass the encoding explicitly.
