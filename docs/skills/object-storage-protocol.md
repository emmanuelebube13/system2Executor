# Object Storage Protocol

**Skill ID:** `object-storage-protocol`
**File:** `docs/implementation-roadmap/system-1-model-building/tasks/skills/object-storage-protocol.md`
**Applies To:** `serializer-infra-agent` (MODEL-007, MODEL-009).

---

## Provider-agnostic FIRST (read before the S3 examples)

**No object store is provisioned yet.** Code against the pluggable **`StorageBackend`** interface
(`src/common/storage/`), not a vendor SDK. The default is a **local-filesystem backend**; a
**Google Cloud Storage** adapter attaches later by config only. Full spec:
`orchestration/STORAGE_AND_QUEUE_ABSTRACTION.md`.

```python
from src.common.storage import build_storage   # reads STORAGE_PROVIDER (local|gcs)

storage = build_storage()
storage.put_object(f"model-artifacts/{bundle_version}/hmm_model.joblib", local_path, encrypt=True)
assert storage.sha256(key) == local_sha256          # round-trip verify BEFORE flipping the pointer
storage.atomic_pointer_update("latest.json", latest_payload)   # write-temp + atomic rename
```

The **semantics below are identical across backends** — immutable versions, upload-everything-then-flip-
pointer ordering, SHA256 round-trip, encryption-flag, lifecycle/retention, and the pre-upload secrets
scan. Only the adapter changes:

| Concern | `local` (default now) | `gcs` (attach later) | S3/MinIO (alt adapter) |
|---------|-----------------------|----------------------|------------------------|
| Object store | files under `STORAGE_LOCAL_ROOT` | GCS bucket (`google-cloud-storage`) | bucket via `boto3`/`minio` |
| Atomic pointer | temp file + `os.replace()` | overwrite w/ generation precondition | upload `latest.json` last |
| Encryption at rest | N/A → `head().encrypted=False` (audit: *skipped-with-note*) | default Google-managed (CMEK optional) → `encrypted=True` | SSE-S3/KMS → `encrypted=True` |
| TLS in transit | local (n/a) | default | `use_ssl=True`, reject non-`https://` |

> The boto3/MinIO code in the sections that follow is **one concrete adapter** behind
> `StorageBackend` — keep it for the S3 case, but task code calls `build_storage()`, never `boto3`
> directly. The GCS adapter implements the same interface; MODEL-007's publish ordering is unchanged.

---

## Configuration

From `.env`:
```
STORAGE_ENDPOINT=https://s3.amazonaws.com          # or MinIO: http://localhost:9000
STORAGE_ACCESS_KEY=...
STORAGE_SECRET_KEY=...
STORAGE_BUCKET=scalable-brain-model-artifacts
STORAGE_USE_TLS=true
STORAGE_REGION=us-east-1
```

---

## Client Initialization

```python
import boto3
import os

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["STORAGE_ENDPOINT"],
    aws_access_key_id=os.environ["STORAGE_ACCESS_KEY"],
    aws_secret_access_key=os.environ["STORAGE_SECRET_KEY"],
    use_ssl=os.environ.get("STORAGE_USE_TLS", "true").lower() == "true",
    region_name=os.environ.get("STORAGE_REGION", "us-east-1"),
)
```

**MinIO alternative** (drop-in S3-compatible):
```python
from minio import Minio

client = Minio(
    endpoint=os.environ["STORAGE_ENDPOINT"].replace("https://", "").replace("http://", ""),
    access_key=os.environ["STORAGE_ACCESS_KEY"],
    secret_key=os.environ["STORAGE_SECRET_KEY"],
    secure=os.environ.get("STORAGE_USE_TLS", "true").lower() == "true",
)
```

---

## Bucket Layout

```
{STORAGE_BUCKET}/
├── latest.json                                    # Mutable pointer
├── model-artifacts/
│   ├── 2026-06-23T00-00-00Z/                      # Immutable version
│   │   ├── hmm_model.joblib
│   │   ├── strategy_weights.json
│   │   ├── regime_strategy_map.json
│   │   ├── model_metadata.json
│   │   └── checksums.sha256
│   ├── 2026-06-16T00-00-00Z/                      # Previous version (retained)
│   │   └── ...
│   └── 2026-06-09T00-00-00Z/                      # Previous version (retained)
│       └── ...
```

Lifecycle policy: retain last N versions (default N=5). Older versions auto-deleted.

---

## Atomic `latest.json` Update (THE CRITICAL PATTERN)

**Order matters.** Upload all artifacts first, verify checksums in storage, then (and only then) update `latest.json`.

```python
import hashlib
import json
from datetime import datetime, timezone

def publish_bundle(artifacts: dict, metadata: dict):
    """
    artifacts: {"filename": local_filepath, ...}
    metadata: full model_metadata.json as dict
    bundle_version: ISO8601 timestamp (UTC)
    """
    bundle_version = metadata["bundle_version"]
    prefix = f"model-artifacts/{bundle_version}/"

    # Step 1: Compute local SHA256
    checksums = {}
    for filename, filepath in artifacts.items():
        checksums[filename] = sha256_file(filepath)
    metadata["artifacts"] = {k: {"sha256": checksums[k], "bytes": os.path.getsize(artifacts[k])} for k in artifacts}

    # Step 2: Write checksums.sha256 locally
    checksum_content = "\n".join(f"{h}  {f}" for f, h in checksums.items())
    checksum_path = "/tmp/checksums.sha256"
    with open(checksum_path, "w") as f:
        f.write(checksum_content)

    # Step 3: Write model_metadata.json locally
    metadata_path = "/tmp/model_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    # Step 4: Upload all files to timestamped prefix
    for filename, filepath in artifacts.items():
        upload_with_encryption(s3, bucket, f"{prefix}{filename}", filepath)
    upload_with_encryption(s3, bucket, f"{prefix}checksums.sha256", checksum_path)
    upload_with_encryption(s3, bucket, f"{prefix}model_metadata.json", metadata_path)

    # Step 5: Verify SHA256 in storage (round-trip)
    for filename in list(artifacts.keys()) + ["checksums.sha256", "model_metadata.json"]:
        key = f"{prefix}{filename}"
        stored_hash = sha256_s3_object(s3, bucket, key)
        local_hash = checksums.get(filename) or sha256_file(metadata_path if filename == "model_metadata.json" else checksum_path)
        if stored_hash != local_hash:
            # Delete corrupt artifacts, abort
            delete_prefix(s3, bucket, prefix)
            raise ValueError(f"Checksum mismatch for {key}: local={local_hash}, stored={stored_hash}")

    # Step 6: Only NOW update latest.json atomically
    latest = {
        "bundle_version": bundle_version,
        "path": prefix,
        "metadata_sha256": sha256_file(metadata_path),
        "promoted_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    latest_path = "/tmp/latest.json"
    with open(latest_path, "w") as f:
        json.dump(latest, f, indent=2)
    upload_with_encryption(s3, bucket, "latest.json", latest_path)

    print(f"Bundle {bundle_version} published and latest.json updated.")
```

**If any step fails before step 6:** `latest.json` still points to the previous version. The partial upload at `prefix` can be cleaned up or left for garbage collection.

---

## SHA256 Helpers

```python
def sha256_file(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def sha256_s3_object(s3, bucket, key):
    """Download and compute SHA256 (or use ETag if multipart upload not used)."""
    response = s3.get_object(Bucket=bucket, Key=key)
    h = hashlib.sha256()
    for chunk in response["Body"].iter_chunks(8192):
        h.update(chunk)
    return h.hexdigest()
```

---

## Encryption Enforcement

```python
def upload_with_encryption(s3, bucket, key, local_path):
    s3.upload_file(
        local_path, bucket, key,
        ExtraArgs={
            "ServerSideEncryption": "AES256",  # SSE-S3
            # or "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "..." for KMS
        }
    )

    # Verify encryption flag on the object
    response = s3.head_object(Bucket=bucket, Key=key)
    assert "ServerSideEncryption" in response, f"Object {key} is NOT encrypted at rest!"
```

TLS: enforced by `use_ssl=True` (checked at client init). Reject any endpoint URL not starting with `https://`.

---

## `latest.json` Contract

```json
{
  "bundle_version": "2026-06-23T00:00:00Z",
  "path": "model-artifacts/2026-06-23T00-00-00Z/",
  "metadata_sha256": "abc123def456...",
  "promoted_at_utc": "2026-06-23T00:05:00Z"
}
```

Computer 2 protocol:
1. GET `latest.json` from bucket root.
2. Read `path` and `metadata_sha256`.
3. GET all files at `path`.
4. Verify every artifact's SHA256 against `checksums.sha256` in the bundle.
5. Verify `model_metadata.json` SHA256 against `metadata_sha256` in `latest.json`.
6. Only if all checksums match: load `hmm_model.joblib`, parse `strategy_weights.json`, parse `regime_strategy_map.json`.

---

## Rollback

To roll back, update `latest.json` to point to any retained prior version:

```python
def rollback(target_bundle_version: str):
    target_prefix = f"model-artifacts/{target_bundle_version}/"

    # Verify the target bundle exists and is complete
    assert object_exists(s3, bucket, f"{target_prefix}hmm_model.joblib")
    assert object_exists(s3, bucket, f"{target_prefix}checksums.sha256")

    # Download and verify checksums
    stored_json = download_json(s3, bucket, f"{target_prefix}model_metadata.json")
    for filename, info in stored_json["artifacts"].items():
        key = f"{target_prefix}{filename}"
        stored_hash = sha256_s3_object(s3, bucket, key)
        assert stored_hash == info["sha256"], f"Checksum mismatch for {filename}"

    # Update latest.json
    latest = {
        "bundle_version": target_bundle_version,
        "path": target_prefix,
        "metadata_sha256": stored_json["artifacts"].get("model_metadata.json", {}).get("sha256", ""),
        "promoted_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    with open("/tmp/latest.json", "w") as f:
        json.dump(latest, f, indent=2)
    s3.upload_file("/tmp/latest.json", bucket, "latest.json")
```

Rollback is instant because versions are immutable — just repoint the pointer.

---

## Bucket Lifecycle Policy

S3 example (or MinIO equivalent):
```xml
<LifecycleConfiguration>
  <Rule>
    <ID>RetainLast5Versions</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix>model-artifacts/</Prefix>
    </Filter>
    <Expiration>
      <Days>90</Days>  <!-- Keep for 90 days, or use version count -->
    </Expiration>
  </Rule>
</LifecycleConfiguration>
```

---

## Secrets Scan (Pre-Upload)

Before uploading any file, scan for credential patterns:
```python
import re

SECRET_PATTERNS = [
    r'sk-[A-Za-z0-9]{20,}',            # OpenAI-style API key
    r'AKIA[0-9A-Z]{16}',                 # AWS Access Key
    r'[A-Za-z0-9+/]{40,}={0,2}',        # Base64-like tokens
    r'Bearer\s+[A-Za-z0-9\-_\.]+',      # Bearer tokens
    r'password\s*[:=]\s*["\']?\S+',     # Password assignments
]

def scan_for_secrets(filepath):
    with open(filepath) as f:
        content = f.read()
    for pattern in SECRET_PATTERNS:
        if re.search(pattern, content):
            raise ValueError(f"Potential secret found in {filepath} matching pattern: {pattern}")
```

**No secrets ever in artifacts.** Credentials are loaded from `.env` at runtime, never serialized.
