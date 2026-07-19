# System 2 — Execution Engine ("The Hand"). Portable image for Computer 2.
# Practice + SHADOW by default; live requires the D-004 cutover (env at runtime, never baked in).
FROM python:3.12-slim AS base

# Non-root, no bytecode noise, unbuffered logs for JSON stdout.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# Dependencies first (layer cache). requirements.txt is pruned + pinned.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Source + migrations + docs (no secrets — config/.env.system2 is mounted/injected at runtime).
COPY src/ ./src/
COPY migrations/ ./migrations/
COPY RUNBOOK.md ./

# Writable state (model cache, queue/outbox, offsets, control sentinel, local db).
RUN useradd --create-home --uid 10001 hand \
    && mkdir -p state/model-cache state/queue state/offsets state/control state/db logs \
    && chown -R hand:hand /app
USER hand

# Health surface (EXEC-009) — bind private; publish only on the trusted network.
EXPOSE 8002

# Fail-closed: aborts (exit 2) if a required secret is missing. Apply migrations then run:
#   docker run --env-file config/.env.system2 <img> \
#     sh -c "python -m system2.common.db migrate && python -m system2"
CMD ["python", "-m", "system2"]
