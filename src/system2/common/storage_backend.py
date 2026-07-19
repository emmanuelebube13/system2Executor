"""Pluggable StorageBackend (artifact exchange, System 1 -> System 2).

Live backend: ``GCSBackend`` (``STORAGE_PROVIDER=gcs``). Dev/test: ``LocalFSBackend``
(``STORAGE_PROVIDER=localfs``). Same interface; chosen by config, never by code. Computer 2
has **read-only** access to the model prefix. See docs/STORAGE_AND_QUEUE_ABSTRACTION.md.

The GCS SDK is imported lazily so dev/tests run with no cloud dependency.
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Protocol, runtime_checkable

from system2.common.secrets import Secrets, get_secrets

_CHUNK = 1024 * 1024


def sha256_file(path: Path) -> str:
    """Streaming SHA256 of a local file."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


@runtime_checkable
class StorageBackend(Protocol):
    """Read-only artifact access. Implementations must not expose write/delete to System 2."""

    def get_text(self, key: str) -> str:
        """Return the text contents of ``key`` (e.g. ``latest.json``)."""
        ...

    def download(self, key: str, dest: Path) -> Path:
        """Stream the object at ``key`` to local ``dest`` (parents created). Returns dest."""
        ...

    def exists(self, key: str) -> bool:
        ...

    def list(self, prefix: str) -> list[str]:
        ...


class LocalFSBackend:
    """Filesystem-backed backend for offline dev/tests. ``root`` simulates the bucket."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _resolve(self, key: str) -> Path:
        return self.root / key

    def get_text(self, key: str) -> str:
        return self._resolve(key).read_text(encoding="utf-8")

    def download(self, key: str, dest: Path) -> Path:
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self._resolve(key), dest)
        return dest

    def exists(self, key: str) -> bool:
        return self._resolve(key).exists()

    def list(self, prefix: str) -> list[str]:
        base = self._resolve(prefix)
        if not base.exists():
            return []
        if base.is_file():
            return [prefix]
        return [
            str(p.relative_to(self.root)) for p in sorted(base.rglob("*")) if p.is_file()
        ]


class GCSBackend:
    """Google Cloud Storage backend (live). Read-only use from Computer 2."""

    def __init__(self, bucket: str, project: str | None = None) -> None:
        from google.cloud import storage  # lazy import; only needed in production

        self._client = storage.Client(project=project) if project else storage.Client()
        self._bucket = self._client.bucket(bucket)

    def get_text(self, key: str) -> str:
        return self._bucket.blob(key).download_as_text()

    def download(self, key: str, dest: Path) -> Path:
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        self._bucket.blob(key).download_to_filename(str(dest))
        return dest

    def exists(self, key: str) -> bool:
        return self._bucket.blob(key).exists()

    def list(self, prefix: str) -> list[str]:
        return [b.name for b in self._client.list_blobs(self._bucket, prefix=prefix)]


def build_storage(secrets: Secrets | None = None) -> StorageBackend:
    """Factory. Reads ``STORAGE_PROVIDER`` (``gcs`` default | ``localfs``)."""
    secrets = secrets or get_secrets()
    provider = (secrets.get("STORAGE_PROVIDER", "gcs") or "gcs").lower()
    if provider == "localfs":
        root = secrets.require("STORAGE_LOCAL_ROOT")
        return LocalFSBackend(Path(root))
    if provider == "gcs":
        bucket = secrets.require("GCS_BUCKET")
        return GCSBackend(bucket, project=secrets.get("PUBSUB_PROJECT_ID"))
    raise ValueError(f"Unknown STORAGE_PROVIDER '{provider}' (expected gcs|localfs)")
