"""Fail-closed secrets/config loader for System 2 (§10 of the master orchestration prompt).

Secrets come from a local, git-ignored env file (``config/.env.system2``) or the real
process environment — **never committed**. Startup aborts with a clear message if a
required secret is missing. Values are never serialized into logs/artifacts/messages.

Precedence: real ``os.environ`` overrides the env file (so containers/CI can inject).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

_DEFAULT_ENV_FILE = Path(
    os.environ.get("SYSTEM2_ENV_FILE", "config/.env.system2")
)


class MissingSecretError(RuntimeError):
    """Raised when a required secret/config value is absent — startup must fail closed."""


def _parse_env_file(path: Path) -> dict[str, str]:
    """Minimal dotenv parser (no external dependency). Ignores blanks and ``#`` comments."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key:
            values[key] = val
    return values


class Secrets:
    """Lazily merged view over the env file + process environment (env wins)."""

    def __init__(self, env_file: Path | None = None) -> None:
        self._file_values = _parse_env_file(env_file or _DEFAULT_ENV_FILE)

    def get(self, name: str, default: str | None = None) -> str | None:
        if name in os.environ:
            return os.environ[name]
        return self._file_values.get(name, default)

    def require(self, name: str) -> str:
        """Return the value or raise ``MissingSecretError`` (fail closed)."""
        val = self.get(name)
        if val is None or val == "":
            raise MissingSecretError(
                f"Required secret/config '{name}' is missing. "
                f"Set it in {_DEFAULT_ENV_FILE} (git-ignored) or the environment. "
                f"System 2 refuses to start without it."
            )
        return val

    def require_many(self, names: Iterable[str]) -> dict[str, str]:
        """Validate a batch; reports ALL missing names at once for a clean startup error."""
        missing: list[str] = []
        out: dict[str, str] = {}
        for name in names:
            val = self.get(name)
            if val is None or val == "":
                missing.append(name)
            else:
                out[name] = val
        if missing:
            raise MissingSecretError(
                "Required secrets/config missing (fail-closed startup): "
                + ", ".join(sorted(missing))
            )
        return out

    def get_bool(self, name: str, default: bool = False) -> bool:
        val = self.get(name)
        if val is None:
            return default
        return val.strip().lower() in {"1", "true", "yes", "on"}

    def get_int(self, name: str, default: int) -> int:
        val = self.get(name)
        if val is None or val == "":
            return default
        try:
            return int(val)
        except ValueError:
            return default


_singleton: Secrets | None = None


def get_secrets() -> Secrets:
    """Process-wide singleton view of secrets/config."""
    global _singleton
    if _singleton is None:
        _singleton = Secrets()
    return _singleton
