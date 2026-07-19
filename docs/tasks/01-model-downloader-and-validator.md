# EXEC-001 — Model Downloader & Validator

**Task ID:** EXEC-001
**System:** System 2 — Execution Engine
**Priority:** P0-Critical
**Estimated Effort:** 3d
**Prerequisites:** FND-001, MODEL-007
**External Dependencies:**
- **Object storage (FND-001):** read-only, least-privilege access from Computer 2 to the model prefix — the source of truth for artifacts. Computer 2 must never write/delete here.
- **System 1 publisher (MODEL-007):** writes `latest.json` (pointer manifest) + per-file SHA256 checksums in the agreed contract. Without it there is nothing to poll.
- **Secrets (FND-003):** storage read key sourced from the secrets layer, never `.env`/git.
- **Encrypted transport (FND-008) + TLS:** download traffic stays on the private network / HTTPS.

## Objective
Build a model downloader/validator on Computer 2 that polls object storage `latest.json` every ~15 min, downloads changed artifacts, verifies SHA256 checksums, and atomically swaps the active model set.

## Current State
- Today the system is single-host: Layer 4 (`src/layer4_executor/live_pipeline.py`) loads artifacts directly from the local `models/` directory (`champion_model.pkl`, `champion_preprocessor.pkl`, `champion_manifest.json`, with legacy fallback `best_ml_gatekeeper_sklearn.pkl` / `best_ml_gatekeeper_preprocessor.pkl`).
- There is **no** cross-host artifact exchange: Computer 2 has no mechanism to fetch models produced by System 1.
- CLAUDE.md notes the champion contract "may not be materialized," and the project already uses SHA256 hashing for artifact integrity (Layer 3) — this task extends that integrity guarantee across the network boundary.

## Target State
- A standalone downloader process/service on Computer 2 (its own module, not embedded in `live_pipeline.py`) that:
  - Polls `latest.json` on a ~15-min cadence (configurable), comparing a content version/etag to the locally cached one.
  - On change, downloads only the artifacts whose checksums differ, into a **staging** directory.
  - Verifies each file's SHA256 against the manifest; **refuses to activate** if any checksum mismatches.
  - **Atomically swaps** the active set (symlink/directory rename) so Layer 4 and the regime detector (EXEC-002) never read a half-written set.
  - Retains the previous set as **last-known-good** for rollback and for EXEC-002 fallback.
- Layer 4 and EXEC-002 read the model set through a stable local path (the active symlink), decoupled from object storage.

## Technical Specification

**`latest.json` manifest contract (text):**
```
{
  schema_version: int,
  published_at: ISO-8601 UTC,
  model_set_id: string (immutable id of this published set),
  artifacts: [
    { name: "champion_model.pkl",        path: "models/champion/<id>/...", sha256: hex, bytes: int },
    { name: "champion_preprocessor.pkl", path: "...", sha256: hex, bytes: int },
    { name: "champion_manifest.json",    path: "...", sha256: hex, bytes: int },
    { name: "regime_hmm.pkl",            path: "models/regime/<id>/...", sha256: hex, bytes: int }
  ]
}
```

**Local layout:**
```
<artifact_root>/
  active        -> sets/<model_set_id>      (atomic symlink; what Layer 4/EXEC-002 read)
  last_good     -> sets/<prev_model_set_id>
  staging/<model_set_id>/...                (download target, promoted on full verification)
  sets/<model_set_id>/...
  state.json    (active model_set_id, last_poll_at, last_etag)
```

**Env vars:** `MODEL_STORE_ENDPOINT`, `MODEL_STORE_BUCKET`, `MODEL_STORE_PREFIX`, `MODEL_STORE_READ_KEY` (via FND-003), `MODEL_POLL_INTERVAL_SEC` (default 900), `ARTIFACT_ROOT`, `MODEL_VERIFY_STRICT` (default true).

**Data flow (text):** poll `latest.json` → compare `model_set_id`/etag to `state.json` → if unchanged, sleep; if changed, download each artifact to `staging/<id>` → compute SHA256 → compare to manifest → on full match, fsync, then rename `active` to point at `sets/<id>` and update `state.json` → keep prior as `last_good`. On any mismatch/partial download: discard staging, **do not swap**, emit alert (FND-005), keep serving current active set.

**Pseudo-code (clarifying only):**
```
loop every MODEL_POLL_INTERVAL_SEC:
    manifest = fetch(latest.json)
    if manifest.model_set_id == state.active_id: continue
    for a in manifest.artifacts: download(a -> staging/id/a.name)
    if all sha256(staging file) == a.sha256:
        atomically point active -> sets/id ; last_good -> old active
        state.active_id = id
    else:
        discard staging ; alert("checksum_mismatch") ; keep active
```

## Testing & Validation
- **Unit:** checksum verification passes on good files, fails on a flipped byte; manifest parse/version handling; "no change" short-circuit on identical `model_set_id`.
- **Integration:** publish a new set to a test bucket → downloader detects within one poll, verifies, swaps; `active` points to new set; `last_good` to old.
- **Atomicity / edge:** kill the process mid-download → on restart, staging is incomplete and is discarded, `active` unchanged (no half-set served). Concurrent read by Layer 4 during swap never sees a partial set.
- **Failure modes:** corrupt artifact (checksum mismatch) → no swap + alert; object store unreachable → keep active set, retry with backoff, alert if stale beyond N polls; truncated `latest.json` → ignored, alert.
- **Edge cases (cross-cutting):** weekend gap — downloader may run pre-session to warm the cache so the session opens on a verified set; staleness — a model that has not refreshed in > X hours raises a warning but does not stop trading (last-good is valid).

## Rollback Plan
- The swap is a single symlink flip; revert by repointing `active` to `last_good` (one operation).
- If the downloader misbehaves, disable it and let Layer 4 read the last verified local set directly; the system keeps trading on the last-known-good model.
- No DB or broker side effects, so rollback is filesystem-only and instantaneous.

## Acceptance Criteria
- [ ] A change to `latest.json` is detected within one poll interval and the new set is downloaded, SHA256-verified, and atomically activated.
- [ ] A checksum mismatch (or partial download) never activates a set; the prior active set keeps serving and an alert fires.
- [ ] `active` and `last_good` always point at complete, verified sets; a kill mid-download leaves no half-set readable.
- [ ] Layer 4 and EXEC-002 read only through the stable `active` path and require no object-storage access themselves.
- [ ] Rollback to `last_good` is a single, reversible operation.

## Notes & Risks
- Determinism: the active `model_set_id` should be logged with every Layer 4 decision and EXEC-005 fill so a trade can be tied to the exact model set used.
- Risk: a poison artifact passing checksum (i.e., a valid-but-bad model) is **not** caught here — that is System 1's promotion gate's responsibility (MODEL-007). EXEC-001 only guarantees integrity, not quality.
- Keep the downloader independent of `live_pipeline.py` so the refactor (EXEC-003) and this task can proceed in parallel.
