"""EXEC-001 tests — downloader/validator (uses LocalFSBackend as a fake bucket)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from system2.common.storage_backend import LocalFSBackend
from system2.artifact_sync.downloader import (
    ChecksumMismatch,
    Manifest,
    ModelDownloader,
)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _publish_set(bucket: Path, model_set_id: str, files: dict[str, bytes]) -> None:
    """Write artifact files + latest.json into the fake bucket, manifest last (atomic order)."""
    prefix = f"models/{model_set_id}"
    artifacts = []
    for name, data in files.items():
        key = f"{prefix}/{name}"
        p = bucket / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        artifacts.append({"name": name, "path": key, "sha256": _sha(data), "bytes": len(data)})
    manifest = {
        "schema_version": 1,
        "published_at": "2026-06-30T00:00:00Z",
        "model_set_id": model_set_id,
        "artifacts": artifacts,
    }
    (bucket / "latest.json").write_text(json.dumps(manifest), encoding="utf-8")


@pytest.fixture
def env(tmp_path: Path):
    bucket = tmp_path / "bucket"
    bucket.mkdir()
    root = tmp_path / "artifacts"
    storage = LocalFSBackend(bucket)
    dl = ModelDownloader(artifact_root=root, storage=storage)
    return bucket, root, storage, dl


# ----- manifest parsing -----------------------------------------------------------
def test_manifest_parse_and_empty_rejected():
    txt = json.dumps({
        "schema_version": 1, "published_at": "t", "model_set_id": "m1",
        "artifacts": [{"name": "a.pkl", "path": "models/m1/a.pkl", "sha256": "x", "bytes": 1}],
    })
    m = Manifest.parse(txt)
    assert m.model_set_id == "m1" and len(m.artifacts) == 1
    with pytest.raises(ValueError):
        Manifest.parse(json.dumps({"schema_version": 1, "model_set_id": "m", "artifacts": []}))


# ----- happy path swap ------------------------------------------------------------
def test_detects_verifies_and_swaps(env):
    bucket, root, _storage, dl = env
    _publish_set(bucket, "set-A", {"champion_model.pkl": b"AAA", "regime_hmm.pkl": b"BBB"})
    assert dl.poll_once() is True
    assert dl.active_model_set_id == "set-A"
    assert (dl.active_link / "champion_model.pkl").read_bytes() == b"AAA"


def test_no_change_short_circuits(env):
    bucket, _root, _storage, dl = env
    _publish_set(bucket, "set-A", {"m.pkl": b"AAA"})
    assert dl.poll_once() is True
    assert dl.poll_once() is False  # same model_set_id -> no swap


def test_new_set_moves_last_good(env):
    bucket, _root, _storage, dl = env
    _publish_set(bucket, "set-A", {"m.pkl": b"AAA"})
    dl.poll_once()
    _publish_set(bucket, "set-B", {"m.pkl": b"CCC"})
    assert dl.poll_once() is True
    assert dl.active_model_set_id == "set-B"
    assert (dl.active_link / "m.pkl").read_bytes() == b"CCC"
    assert dl.last_good_link.resolve().name == "set-A"
    assert (dl.last_good_link / "m.pkl").read_bytes() == b"AAA"


# ----- checksum / corruption ------------------------------------------------------
def test_checksum_mismatch_never_activates(env):
    bucket, _root, _storage, dl = env
    _publish_set(bucket, "set-A", {"m.pkl": b"AAA"})
    # Corrupt the artifact AFTER the manifest recorded the good hash.
    (bucket / "models/set-A/m.pkl").write_bytes(b"TAMPERED")
    assert dl.poll_once() is False           # refused
    assert dl.active_model_set_id is None     # nothing activated
    assert not dl.active_link.exists()


def test_corrupt_set_keeps_prior_active(env):
    bucket, _root, _storage, dl = env
    _publish_set(bucket, "set-A", {"m.pkl": b"AAA"})
    dl.poll_once()
    _publish_set(bucket, "set-B", {"m.pkl": b"CCC"})
    (bucket / "models/set-B/m.pkl").write_bytes(b"corrupt")
    assert dl.poll_once() is False
    assert dl.active_model_set_id == "set-A"   # prior set still serving
    assert (dl.active_link / "m.pkl").read_bytes() == b"AAA"


def test_download_and_verify_raises_on_flipped_byte(env):
    bucket, _root, storage, dl = env
    _publish_set(bucket, "set-A", {"m.pkl": b"AAA"})
    m = dl.fetch_manifest()
    (bucket / "models/set-A/m.pkl").write_bytes(b"AAB")  # flip
    with pytest.raises(ChecksumMismatch):
        dl._download_and_verify(m)


# ----- storage outage / truncated manifest ---------------------------------------
def test_storage_unreachable_keeps_active(env, tmp_path):
    bucket, _root, _storage, dl = env
    _publish_set(bucket, "set-A", {"m.pkl": b"AAA"})
    dl.poll_once()
    (bucket / "latest.json").unlink()  # simulate manifest gone
    assert dl.poll_once() is False
    assert dl.active_model_set_id == "set-A"


def test_truncated_manifest_ignored(env):
    bucket, _root, _storage, dl = env
    _publish_set(bucket, "set-A", {"m.pkl": b"AAA"})
    dl.poll_once()
    (bucket / "latest.json").write_text("{ this is not json", encoding="utf-8")
    assert dl.poll_once() is False
    assert dl.active_model_set_id == "set-A"


# ----- atomicity: leftover staging from a killed run is discarded ------------------
def test_partial_staging_discarded_on_next_poll(env):
    bucket, _root, _storage, dl = env
    _publish_set(bucket, "set-A", {"m.pkl": b"AAA"})
    # Simulate a half-written staging dir from a previously killed process.
    stale = dl.staging_dir / "set-A"
    stale.mkdir(parents=True)
    (stale / "garbage.partial").write_bytes(b"xx")
    assert dl.poll_once() is True
    assert dl.active_model_set_id == "set-A"
    assert not (dl.active_link / "garbage.partial").exists()


# ----- rollback -------------------------------------------------------------------
def test_rollback_to_last_good(env):
    bucket, _root, _storage, dl = env
    _publish_set(bucket, "set-A", {"m.pkl": b"AAA"})
    dl.poll_once()
    _publish_set(bucket, "set-B", {"m.pkl": b"CCC"})
    dl.poll_once()
    assert dl.active_model_set_id == "set-B"
    assert dl.rollback_to_last_good() is True
    assert dl.active_model_set_id == "set-A"
    assert (dl.active_link / "m.pkl").read_bytes() == b"AAA"
