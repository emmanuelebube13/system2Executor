"""Structured JSON logging for System 2 (§9.1 of the master orchestration prompt).

Every log entry is a single JSON object on stdout AND a rotating file
(``logs/system2.log``), carrying: ``timestamp`` (UTC), ``level``, ``correlation_id``,
``component``, ``message`` and a ``context`` dict. No secrets, PII, or full API
responses — keys are masked and long payloads truncated.

Usage::

    from system2.common.logging import get_logger, set_correlation_id
    log = get_logger("artifact_sync.downloader")
    set_correlation_id("poll-2026-06-30T22:00:00Z")
    log.info("manifest fetched", extra={"context": {"model_set_id": "abc"}})
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

# correlation id is request/loop-scoped; defaults to "-" when unset.
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="-")

# Patterns that must never appear verbatim in a log line. Values are masked.
_SECRET_PATTERNS = [
    re.compile(r"(?i)(api[_-]?key|secret|password|token|authorization|bearer)\s*[:=]\s*\S+"),
    re.compile(r"sk-[A-Za-z0-9]{20,}"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
]

_LOG_DIR = Path(os.environ.get("SYSTEM2_LOG_DIR", "logs"))
_LOG_FILE = _LOG_DIR / "system2.log"
_MAX_CONTEXT_CHARS = 4000
_configured = False


def set_correlation_id(correlation_id: str) -> None:
    """Bind a correlation id for all subsequent log lines on this context."""
    _correlation_id.set(correlation_id or "-")


def get_correlation_id() -> str:
    return _correlation_id.get()


def _mask_keyed(match: re.Match) -> str:
    """Keep the key name, mask the value: ``api_key=xyz`` -> ``api_key=***MASKED***``."""
    sep = "=" if "=" in match.group(0) else ":"
    key = match.group(0).split(sep, 1)[0]
    return f"{key}{sep}***MASKED***"


def _mask(text: str) -> str:
    # First pattern is key=value; keep the key. Remaining patterns are raw tokens.
    text = _SECRET_PATTERNS[0].sub(_mask_keyed, text)
    for pat in _SECRET_PATTERNS[1:]:
        text = pat.sub("***MASKED***", text)
    return text


class JsonFormatter(logging.Formatter):
    """Render a LogRecord as a one-line JSON object with masked secrets."""

    def format(self, record: logging.LogRecord) -> str:
        context = getattr(record, "context", {}) or {}
        try:
            context_str = json.dumps(context, default=str, sort_keys=True)
        except (TypeError, ValueError):
            context_str = json.dumps({"_unserializable": str(context)})
        if len(context_str) > _MAX_CONTEXT_CHARS:
            context_str = context_str[:_MAX_CONTEXT_CHARS] + "...<truncated>"

        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "correlation_id": _correlation_id.get(),
            "component": record.name,
            "message": _mask(record.getMessage()),
            "context": json.loads(context_str),
        }
        if record.exc_info:
            payload["exception"] = _mask(self.formatException(record.exc_info))
        return _mask(json.dumps(payload, default=str))


def _configure_root() -> None:
    global _configured
    if _configured:
        return
    level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    root = logging.getLogger("system2")
    root.setLevel(level)
    root.propagate = False

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(JsonFormatter())
    root.addHandler(stream)

    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            _LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=14, encoding="utf-8"
        )
        file_handler.setFormatter(JsonFormatter())
        root.addHandler(file_handler)
    except OSError:
        # Disk/permission issue must never crash the process; stdout still works.
        root.warning("could not open rotating log file; stdout-only logging")

    _configured = True


def get_logger(component: str) -> logging.Logger:
    """Return a configured child logger under the ``system2`` namespace."""
    _configure_root()
    return logging.getLogger(f"system2.{component}")


def log_event(logger: logging.Logger, level: int, message: str, **context: Any) -> None:
    """Convenience: emit ``message`` with a structured ``context`` dict."""
    logger.log(level, message, extra={"context": context})
