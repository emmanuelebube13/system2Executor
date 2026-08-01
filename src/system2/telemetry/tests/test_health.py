"""EXEC-009 tests — HealthReporter payloads (pure) + FastAPI surface (read-only, no secrets)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from system2.telemetry.health import HealthReporter

T0 = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)


def _reporter(**over):
    base = dict(
        safety_state_fn=lambda: "running",
        staleness_fn=lambda: 12.0,
        last_message_at_fn=lambda: datetime(2026, 7, 1, 11, 59, 48, tzinfo=timezone.utc),
        messages_seen_fn=lambda: 7,
        open_positions_fn=lambda: [{"trade_id": "T1"}, {"trade_id": "T2"}],
        outbox_depth_fn=lambda: 0,
        model_set_id_fn=lambda: "set-2026-06-30",
        broker_env_fn=lambda: "practice",
        clock=lambda: T0,
    )
    base.update(over)
    return HealthReporter(**base)


# ----- pure payloads --------------------------------------------------------------
def test_health_liveness():
    h = _reporter().health()
    assert h["status"] == "ok"
    assert h["uptime_sec"] == 0.0
    assert h["as_of"].endswith("Z")


def test_account_state_shape():
    s = _reporter().account_state()
    assert s["exec_mode"] == "RUNNING"
    assert s["queue_staleness_sec"] == 12.0
    assert s["open_positions"] == 2
    assert s["broker_env"] == "practice"
    assert s["model_set_id"] == "set-2026-06-30"


def test_status_full_snapshot():
    s = _reporter().status()
    assert s["service"] == "system-2-execution-engine"
    assert s["exec_mode"] == "RUNNING"
    assert s["queue"]["messages_seen"] == 7
    assert s["queue"]["last_message_at"].endswith("Z")
    assert s["outbox_depth"] == 0
    assert len(s["open_positions"]) == 2


def test_exec_mode_reflects_paused():
    s = _reporter(safety_state_fn=lambda: "paused").account_state()
    assert s["exec_mode"] == "PAUSED"


def test_missing_providers_degrade_not_crash():
    r = HealthReporter(clock=lambda: T0)  # no providers wired
    s = r.status()
    assert s["exec_mode"] == "UNKNOWN"
    assert s["queue"]["staleness_sec"] is None
    assert s["open_positions"] == []
    assert s["outbox_depth"] == 0
    # and the account slice still returns 200-able data
    assert r.account_state()["open_positions"] == 0


def test_provider_exception_is_swallowed():
    def boom():
        raise RuntimeError("db down")

    r = _reporter(staleness_fn=boom, open_positions_fn=boom)
    s = r.status()
    assert s["queue"]["staleness_sec"] is None
    assert s["open_positions"] == []


# ----- FastAPI surface ------------------------------------------------------------
@pytest.fixture
def client():
    from starlette.testclient import TestClient

    from system2.telemetry.server import build_health_app

    return TestClient(build_health_app(_reporter()))


def test_http_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_http_status_endpoint(client):
    resp = client.get("/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["exec_mode"] == "RUNNING"
    assert body["open_positions"][0]["trade_id"] == "T1"


def test_http_regime_endpoint():
    from starlette.testclient import TestClient

    from system2.telemetry.server import build_health_app

    grid = [{"instrument": "EUR_USD", "granularity": "H1",
             "smoothed_label": "Trending-Up", "smoothed_confidence": 0.82, "stale": False}]
    rep = _reporter(regime_grid_fn=lambda: grid)
    client = TestClient(build_health_app(rep))
    resp = client.get("/regime")
    assert resp.status_code == 200
    body = resp.json()
    assert body["grid"][0]["instrument"] == "EUR_USD"
    assert body["grid"][0]["smoothed_label"] == "Trending-Up"


def test_http_account_state_endpoint(client):
    resp = client.get("/api/account/state")
    assert resp.status_code == 200
    assert resp.json()["queue_staleness_sec"] == 12.0


def test_http_responses_carry_no_secrets(client):
    """Belt-and-braces: telemetry must never leak credential-bearing fields."""
    blob = (client.get("/status").text + client.get("/api/account/state").text).lower()
    for banned in ("api_key", "password", "secret", "token", "access_token"):
        assert banned not in blob


# ----- F-304: the effective staleness limit must be auditable remotely -------------
def test_status_exposes_the_effective_staleness_limit():
    """The deployed limit is the safety posture; it must be visible without shell access."""
    r = _reporter(staleness_limit_fn=lambda: 300.0)
    assert r.status()["queue"]["staleness_limit_sec"] == 300.0
    assert r.account_state()["queue_staleness_limit_sec"] == 300.0


def test_staleness_limit_degrades_to_none_when_unwired():
    s = _reporter().status()
    assert s["queue"]["staleness_limit_sec"] is None
