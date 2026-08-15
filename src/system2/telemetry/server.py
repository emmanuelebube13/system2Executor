"""EXEC-009 — FastAPI mount for the System-2 health/telemetry surface (lazy import).

Read-only. Binds to the private network only (default ``127.0.0.1``; no public ingress per
FND-008). Serves the ``HealthReporter`` payloads — no writes, no secrets. FastAPI/uvicorn are
imported lazily so importing this module (and running the rest of the engine/tests) never
requires the web stack to be installed.
"""

from __future__ import annotations

from typing import Any

from system2.telemetry.health import HealthReporter


def build_health_app(reporter: HealthReporter) -> Any:
    """Return a FastAPI app exposing the read-only System-2 telemetry endpoints."""
    from fastapi import FastAPI  # lazy

    app = FastAPI(title="System 2 — Execution Engine (health)", docs_url=None, redoc_url=None)

    @app.get("/health")
    def health() -> dict[str, Any]:  # liveness
        return reporter.health()

    @app.get("/status")
    def status() -> dict[str, Any]:  # full snapshot
        return reporter.status()

    @app.get("/api/account/state")
    def account_state() -> dict[str, Any]:  # System-2 slice (exec_mode + staleness + positions)
        return reporter.account_state()

    @app.get("/regime")
    def regime() -> dict[str, Any]:  # live regime grid (pair x timeframe)
        return reporter.regime()

    @app.get("/signal")
    def signal() -> dict[str, Any]:
        """EXEC-011's producer stats. The producer is gone; the route is not.

        Deleting the route would make the dashboard's fetch fail, and a failed
        fetch reads as "unreachable" — a transient fault an operator waits out.
        This is not a fault and there is nothing to wait for: System 2 no longer
        originates signals at all (S1-NOTICE-2026-08-15 §4.3). `removed` says so
        in a field a consumer can branch on, rather than only in prose.
        """
        return {
            "running": False,
            "removed": True,
            "reason": "signal production removed 2026-08-15 — System 2 is "
                      "execution-only and originates no signals; entry logic is "
                      "System 1's (S1-REPLY-2026-08-02b §2)",
        }

    return app


def run_server(app: Any, host: str | None = None, port: int | None = None, secrets: Any | None = None) -> None:
    """Serve the app with uvicorn (lazy). Host/port from config; private-network default."""
    import uvicorn  # lazy

    if secrets is None:
        from system2.common.secrets import get_secrets

        secrets = get_secrets()
    host = host or secrets.get("HEALTH_HOST", "127.0.0.1") or "127.0.0.1"
    port = port or secrets.get_int("HEALTH_PORT", 8002)
    uvicorn.run(app, host=host, port=port, log_level="info")
