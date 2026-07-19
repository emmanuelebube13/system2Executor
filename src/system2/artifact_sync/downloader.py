"""EXEC-001 — Model downloader & validator (Artifact-Sync).

Polls object storage ``latest.json`` (~15 min), downloads changed artifacts to a staging
dir, verifies each file's SHA256 against the manifest, and **atomically swaps** the active
model set (symlink flip). Retains the previous set as ``last_good``. Never activates a
checksum-mismatched or partially-downloaded set; on storage outage it keeps serving the
current active set. Independent of the execution pipeline so EXEC-003 can proceed in parallel.

Local layout (per EXEC-001 spec)::

    <artifact_root>/
      active     -> sets/<model_set_id>   (atomic symlink — what Layer 4 / EXEC-002 read)
      last_good  -> sets/<prev_id>
      staging/<model_set_id>/...
      sets/<model_set_id>/...
      state.json
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from system2.common.logging import get_logger, log_event, set_correlation_id
from system2.common.secrets import Secrets, get_secrets
from system2.common.storage_backend import StorageBackend, build_storage, sha256_file

log = get_logger("artifact_sync.downloader")

MANIFEST_KEY = "latest.json"


class ChecksumMismatch(Exception):
    """A downloaded artifact failed SHA256 verification — the set must not be activated."""


@dataclass(frozen=True)
class Artifact:
    name: str
    path: str  # key in object storage
    sha256: str
    bytes: int = 0

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Artifact":
        return cls(name=d["name"], path=d["path"], sha256=d["sha256"], bytes=int(d.get("bytes", 0)))


@dataclass(frozen=True)
class Manifest:
    schema_version: int
    model_set_id: str
    published_at: str
    artifacts: tuple[Artifact, ...]

    @classmethod
    def parse(cls, text: str) -> "Manifest":
        d = json.loads(text)
        artifacts = tuple(Artifact.from_dict(a) for a in d["artifacts"])
        if not artifacts:
            raise ValueError("manifest has no artifacts")
        return cls(
            schema_version=int(d["schema_version"]),
            model_set_id=str(d["model_set_id"]),
            published_at=str(d.get("published_at", "")),
            artifacts=artifacts,
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class ModelDownloader:
    """Poll/verify/atomic-swap the active model set on Computer 2."""

    def __init__(
        self,
        artifact_root: Path,
        storage: StorageBackend | None = None,
        secrets: Secrets | None = None,
        manifest_key: str = MANIFEST_KEY,
        strict: bool = True,
    ) -> None:
        self.secrets = secrets or get_secrets()
        self.storage = storage or build_storage(self.secrets)
        self.root = Path(artifact_root)
        self.manifest_key = manifest_key
        self.strict = strict
        self.sets_dir = self.root / "sets"
        self.staging_dir = self.root / "staging"
        self.active_link = self.root / "active"
        self.last_good_link = self.root / "last_good"
        self.state_file = self.root / "state.json"
        for d in (self.sets_dir, self.staging_dir):
            d.mkdir(parents=True, exist_ok=True)

    # ----- state -----------------------------------------------------------------
    def _read_state(self) -> dict[str, Any]:
        if self.state_file.exists():
            try:
                return json.loads(self.state_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                log_event(log, logging.WARNING, "state.json unreadable; treating as empty")
        return {}

    def _write_state(self, state: dict[str, Any]) -> None:
        tmp = self.state_file.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.state_file)

    @property
    def active_model_set_id(self) -> str | None:
        return self._read_state().get("active_model_set_id")

    # ----- core protocol ---------------------------------------------------------
    def fetch_manifest(self) -> Manifest:
        raw_text = self.storage.get_text(self.manifest_key)
        d = json.loads(raw_text)
        
        # If the json is a pointer (like gatekeeper's latest.json), it has a 'path' but no 'artifacts'
        if "artifacts" not in d and "path" in d:
            # Fetch the actual manifest from the path
            manifest_path = os.path.join(d["path"], "champion_manifest.json").replace("\\", "/")
            raw_text = self.storage.get_text(manifest_path)
            
        return Manifest.parse(raw_text)

    def _download_and_verify(self, manifest: Manifest) -> Path:
        """Download all artifacts into a fresh staging dir and verify every SHA256.

        Raises ChecksumMismatch on any mismatch. Returns the verified staging path.
        """
        stage = self.staging_dir / manifest.model_set_id
        if stage.exists():
            shutil.rmtree(stage)  # discard any prior partial download
        stage.mkdir(parents=True)

        for art in manifest.artifacts:
            dest = stage / art.name
            self.storage.download(art.path, dest)
            actual = sha256_file(dest)
            if actual != art.sha256:
                raise ChecksumMismatch(
                    f"{art.name}: expected {art.sha256[:12]}…, got {actual[:12]}…"
                )
        return stage

    def _promote(self, manifest: Manifest, stage: Path) -> None:
        """Move verified staging -> sets/<id>, fsync, then atomically flip symlinks."""
        final = self.sets_dir / manifest.model_set_id
        if final.exists():
            shutil.rmtree(final)
        shutil.move(str(stage), str(final))
        self._fsync_dir(final)

        prev_active = self.active_model_set_id
        # last_good points at whatever was active before this swap.
        if prev_active and prev_active != manifest.model_set_id:
            self._atomic_symlink(self.last_good_link, self.sets_dir / prev_active)
        self._atomic_symlink(self.active_link, final)

        self._write_state(
            {
                "active_model_set_id": manifest.model_set_id,
                "previous_model_set_id": prev_active,
                "published_at": manifest.published_at,
                "last_poll_at": _utc_now(),
                "last_swapped_at": _utc_now(),
            }
        )

    @staticmethod
    def _atomic_symlink(link: Path, target: Path) -> None:
        """Create/replace ``link`` -> ``target`` atomically (temp symlink + os.replace)."""
        tmp = link.with_name(link.name + ".tmp")
        if tmp.exists() or tmp.is_symlink():
            tmp.unlink()
        # Store a relative target so the artifact_root stays relocatable (cold transfer).
        os.symlink(os.path.relpath(target, link.parent), tmp)
        os.replace(tmp, link)

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(str(path), os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass  # best-effort durability; not all FSes support dir fsync

    # ----- public entry points ---------------------------------------------------
    def poll_once(self) -> bool:
        """One poll cycle. Returns True iff a new set was activated.

        Never raises on storage outage or checksum mismatch — keeps serving the current
        active set and records the condition. Re-raises only on programmer errors.
        """
        set_correlation_id(f"artifact-poll-{_utc_now()}")
        try:
            manifest = self.fetch_manifest()
        except Exception as exc:  # storage unreachable / truncated manifest
            state = self._read_state()
            state["last_poll_at"] = _utc_now()
            state["last_poll_error"] = type(exc).__name__
            self._write_state(state)
            log_event(
                log, logging.WARNING, "manifest fetch failed; keeping active set",
                error=type(exc).__name__, detail=str(exc), active=self.active_model_set_id,
            )
            return False

        if manifest.model_set_id == self.active_model_set_id:
            state = self._read_state()
            state["last_poll_at"] = _utc_now()
            self._write_state(state)
            log_event(log, logging.DEBUG, "no change", model_set_id=manifest.model_set_id)
            return False

        log_event(
            log, logging.INFO, "new model set detected",
            new=manifest.model_set_id, current=self.active_model_set_id,
            artifacts=len(manifest.artifacts),
        )
        try:
            stage = self._download_and_verify(manifest)
        except ChecksumMismatch as exc:
            log_event(
                log, logging.CRITICAL, "checksum mismatch — refusing to activate set",
                model_set_id=manifest.model_set_id, detail=str(exc),
            )
            shutil.rmtree(self.staging_dir / manifest.model_set_id, ignore_errors=True)
            return False
        except Exception as exc:  # partial download / network drop mid-stream
            log_event(
                log, logging.WARNING, "download failed — discarding staging, keeping active",
                model_set_id=manifest.model_set_id, error=type(exc).__name__, detail=str(exc),
            )
            shutil.rmtree(self.staging_dir / manifest.model_set_id, ignore_errors=True)
            return False

        self._promote(manifest, stage)
        log_event(
            log, logging.INFO, "activated new model set",
            model_set_id=manifest.model_set_id, last_good=self._read_state().get("previous_model_set_id"),
        )
        return True

    def rollback_to_last_good(self) -> bool:
        """Repoint ``active`` -> ``last_good`` (single reversible op). Returns success."""
        if not self.last_good_link.exists():
            log_event(log, logging.WARNING, "rollback requested but no last_good exists")
            return False
        target = self.last_good_link.resolve()
        self._atomic_symlink(self.active_link, target)
        state = self._read_state()
        state["active_model_set_id"] = target.name
        state["last_rolled_back_at"] = _utc_now()
        self._write_state(state)
        log_event(log, logging.WARNING, "rolled back to last_good", model_set_id=target.name)
        return True

    def run_forever(self, interval_sec: int | None = None) -> None:
        """Background poll loop (run detached per §11). Backs off on repeated failures."""
        interval = interval_sec or self.secrets.get_int("MODEL_POLL_INTERVAL_SEC", 900)
        log_event(log, logging.INFO, "downloader loop start", interval_sec=interval)
        while True:
            try:
                self.poll_once()
            except Exception as exc:  # last-resort guard: loop must never die
                log_event(log, logging.ERROR, "unexpected poll error", error=type(exc).__name__, detail=str(exc))
            time.sleep(interval)


def main() -> None:
    secrets = get_secrets()
    root = Path(secrets.require("ARTIFACT_ROOT"))
    downloader = ModelDownloader(
        artifact_root=root,
        manifest_key=secrets.get("MANIFEST_KEY", MANIFEST_KEY),
        strict=secrets.get_bool("MODEL_VERIFY_STRICT", True),
    )
    downloader.run_forever()


if __name__ == "__main__":
    main()
