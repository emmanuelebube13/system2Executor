# Transfer System 2 via Google Cloud Storage

Send the whole `system-2-execution-engine/` folder to a GCS bucket, then pull it down on the
new computer. Uses `gcloud storage` (install: Google Cloud SDK).

---

## 0. One-time: authenticate

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

## 1. Create a bucket (skip if you already have one)

```bash
gcloud storage buckets create gs://YOUR-TRANSFER-BUCKET --location=us-central1
```

## 2. Upload the whole folder

Run from the **parent** of `system-2-execution-engine/`:

```bash
gcloud storage rsync --recursive \
  --exclude='\.venv/.*|.*/__pycache__/.*|\.git/.*|state/.*|logs/.*|.*\.pyc|config/\.env\.system2$' \
  system-2-execution-engine \
  gs://YOUR-TRANSFER-BUCKET/system-2-execution-engine
```

The `--exclude` skips things that should NOT travel:

- `config/.env.system2` — your real secrets (never upload). The template is kept.
- `.venv/`, `__pycache__/`, `*.pyc`, `.git/` — regenerated on the new machine.
- `state/`, `logs/` — local runtime data (queue db, model cache, offsets).

> Want a literal, no-exclusions "as is" copy instead? Use:
> `gcloud storage cp --recursive system-2-execution-engine gs://YOUR-TRANSFER-BUCKET/`
> — but then **delete `config/.env.system2` from the bucket afterwards** so secrets aren't left in storage.

## 3. Verify it landed

```bash
gcloud storage ls --recursive gs://YOUR-TRANSFER-BUCKET/system-2-execution-engine | head
```

## 4. Download on the new computer

```bash
gcloud auth login
gcloud storage cp --recursive \
  gs://YOUR-TRANSFER-BUCKET/system-2-execution-engine .
```

## 5. Finish setup on the new computer

The folder is now in place but not runnable yet. Follow the full guide
(`System2_Setup_On_New_Computer.docx`) — in short:

```bash
cd system-2-execution-engine
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config/.env.system2.template config/.env.system2   # then fill it in
PYTHONPATH=src python -m system2.common.db migrate
PYTHONPATH=src python -m system2                       # starts in practice + SHADOW
```

## 6. Clean up the transfer bucket (optional)

```bash
gcloud storage rm --recursive gs://YOUR-TRANSFER-BUCKET/system-2-execution-engine
```
