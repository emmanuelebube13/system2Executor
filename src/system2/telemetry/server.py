"""EXEC-009 — FastAPI mount for the System-2 health/telemetry surface (lazy import).

Read-only. Binds to the private network only (default ``127.0.0.1``; no public ingress per
FND-008). Serves the ``HealthReporter`` payloads — no writes, no secrets. FastAPI/uvicorn are
imported lazily so importing this module (and running the rest of the engine/tests) never
requires the web stack to be installed.
"""

from __future__ import annotations

from typing import Any

from system2.telemetry.health import HealthReporter


def build_health_app(reporter: HealthReporter, signal_producer: Any = None) -> Any:
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
    def signal() -> dict[str, Any]:  # live scored-signal producer stats (EXEC-011)
        if signal_producer is None:
            return {"running": False, "reason": "signal producer not loaded"}
        try:
            return signal_producer.telemetry_snapshot()
        except Exception as exc:  # telemetry must never 500 the health surface
            return {"running": False, "reason": f"{type(exc).__name__}: {exc}"}

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
