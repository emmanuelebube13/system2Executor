"""System 2 — Execution Engine service entrypoint.

    python -m system2                 # run the engine (practice, SHADOW by default)

Composition happens in ``execution.lifecycle.build_from_secrets`` (fail-closed on missing
secrets). This module just: starts the read-only health server (EXEC-009) on a background
thread, installs the emergency-STOP signal handlers, and runs the tick loop until STOP. It
is deliberately thin — no business logic lives here.

Safety posture on startup: ``EXEC_MODE`` defaults to ``execution_only`` and ``EXEC_SHADOW``
defaults to true, so the engine constructs and validates real orders but submits NOTHING to
the broker until the logged human cutover decision (see orchestration/DECISIONS_LOG.md D-004).
"""

from __future__ import annotations

import logging
import sys
import threading

from system2.common.logging import get_logger, log_event
from system2.common.secrets import MissingSecretError, get_secrets

log = get_logger("main")


def _start_health_server(runtime, secrets) -> None:
    """Serve the health app on a daemon thread so a crash there never blocks trading."""
    if runtime.reporter is None or not secrets.get_bool("HEALTH_ENABLED", True):
        return
    try:
        from system2.telemetry.server import build_health_app, run_server

        app = build_health_app(runtime.reporter)
        t = threading.Thread(
            target=run_server, args=(app,), kwargs={"secrets": secrets},
            name="health-server", daemon=True,
        )
        t.start()
        log_event(log, logging.INFO, "health server thread started")
    except Exception as exc:  # health is best-effort; never let it stop the engine
        log_event(log, logging.ERROR, "health server failed to start", error=str(exc))


def main(argv: list[str] | None = None) -> int:
    secrets = get_secrets()
    try:
        from system2.execution.lifecycle import build_from_secrets

        runtime = build_from_secrets(secrets)
    except MissingSecretError as exc:
        # Fail-closed: refuse to start without required config, with a clear message.
        log_event(log, logging.CRITICAL, "startup aborted: missing required secret", detail=str(exc))
        return 2

    log_event(log, logging.INFO, "System 2 execution engine starting",
              exec_mode=runtime.consumer.pipeline.mode.value,
              shadow=runtime.consumer.pipeline.shadow,
              broker_env=runtime.adapter.env.env if runtime.adapter else "unknown")
    _start_health_server(runtime, secrets)
    runtime.run()  # blocks until emergency STOP; performs graceful, position-safe shutdown
    log_event(log, logging.INFO, "System 2 execution engine exited",
              stop_reason=runtime.emergency_stop.reason)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
