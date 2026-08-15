"""F-309 / OD-5 — ``EXEC_SHADOW`` resolves in one place, and production refuses to guess.

The defect these tests witness: ``EXEC_SHADOW`` decides whether an approved order is
actually submitted to the broker or merely simulated, and it had **two** readers that
disagreed on the default — ``execution.pipeline`` resolved an absent flag to ``False``
(submit for real) while ``execution.lifecycle`` resolved it to ``True`` (simulate). The
intent could be read from two places and give two answers, and the *resolved* value was on
no HTTP surface at all, so the deployed reality was unreadable without shell access.

So there are three separate properties here, and they fail independently:
  1. one resolver, one default — the two callers cannot drift apart again;
  2. the production path REFUSES to default at all;
  3. the resolved value is observable on the health surface.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from system2.common.secrets import MissingSecretError, Secrets
from system2.execution.pipeline import (
    SHADOW_DEFAULT,
    ExecMode,
    ExecutionPipeline,
    resolve_shadow,
)
from system2.telemetry.health import HealthReporter


def _secrets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, body: str = "") -> Secrets:
    """A Secrets view over a controlled env file, with the process env neutralised.

    ``Secrets.get`` consults ``os.environ`` FIRST, so a stray EXEC_SHADOW in the test
    runner's environment would silently decide these assertions.
    """
    monkeypatch.delenv("EXEC_SHADOW", raising=False)
    env = tmp_path / ".env.test"
    env.write_text(body, encoding="utf-8")
    return Secrets(env_file=env)


# ----- 1. one resolver, one default ----------------------------------------------
def test_absent_flag_resolves_to_shadow_the_safe_direction(tmp_path, monkeypatch):
    """An unset flag simulates rather than sends. This is the whole point of the default."""
    assert SHADOW_DEFAULT is True
    assert resolve_shadow(_secrets(tmp_path, monkeypatch)) is True


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("true", True), ("1", True), ("yes", True), ("on", True),
     ("false", False), ("0", False), ("no", False), ("off", False)],
)
def test_explicit_flag_is_honoured_both_ways(tmp_path, monkeypatch, raw, expected):
    assert resolve_shadow(_secrets(tmp_path, monkeypatch, f"EXEC_SHADOW={raw}\n")) is expected


def test_pipeline_and_production_path_agree_when_the_flag_is_absent(tmp_path, monkeypatch):
    """The regression itself: the two readers used to disagree on an absent flag.

    ``ExecutionPipeline`` resolved False, ``lifecycle`` resolved True. Whichever import
    won decided whether orders reached a real broker. They must now be the same value.
    """
    secrets = _secrets(tmp_path, monkeypatch)
    pipeline_view = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, secrets=secrets).shadow
    assert pipeline_view is resolve_shadow(secrets)
    assert pipeline_view is True, "an absent EXEC_SHADOW must never resolve to 'send for real'"


def test_explicit_constructor_argument_still_wins(tmp_path, monkeypatch):
    """Harnesses pass `shadow=` directly; the env must not override an explicit choice."""
    secrets = _secrets(tmp_path, monkeypatch, "EXEC_SHADOW=true\n")
    assert ExecutionPipeline(shadow=False, secrets=secrets).shadow is False


# ----- 2. the production path refuses to default ----------------------------------
def test_production_path_refuses_to_default(tmp_path, monkeypatch):
    """A flag that decides whether money moves must not be settled by a default."""
    with pytest.raises(MissingSecretError, match="EXEC_SHADOW"):
        resolve_shadow(_secrets(tmp_path, monkeypatch), require_explicit=True)


@pytest.mark.parametrize("blank", ["", "EXEC_SHADOW=\n", "EXEC_SHADOW=   \n"])
def test_production_path_treats_blank_as_absent(tmp_path, monkeypatch, blank):
    """`EXEC_SHADOW=` is not an answer — an empty value would silently mean 'live'."""
    with pytest.raises(MissingSecretError):
        resolve_shadow(_secrets(tmp_path, monkeypatch, blank), require_explicit=True)


def test_production_path_accepts_an_explicit_value(tmp_path, monkeypatch):
    secrets = _secrets(tmp_path, monkeypatch, "EXEC_SHADOW=false\n")
    assert resolve_shadow(secrets, require_explicit=True) is False


# ----- 3. the resolved value is observable ----------------------------------------
@pytest.mark.parametrize("value", [True, False])
def test_health_surface_reports_the_resolved_value(value):
    """OD-1 was closed by putting the effective limit on the health payload; same here."""
    reporter = HealthReporter(shadow_fn=lambda: value)
    assert reporter.status()["exec_shadow"] is value
    assert reporter.account_state()["exec_shadow"] is value


def test_unwired_provider_reports_unknown_not_a_guess():
    """A boolean here would read as a confident claim about whether orders are real."""
    reporter = HealthReporter()
    assert reporter.status()["exec_shadow"] == "unknown"
    assert reporter.account_state()["exec_shadow"] == "unknown"


def test_a_raising_provider_degrades_to_unknown():
    def boom() -> bool:
        raise RuntimeError("pipeline gone")

    assert HealthReporter(shadow_fn=boom).status()["exec_shadow"] == "unknown"


def test_exec_mode_and_exec_shadow_are_orthogonal():
    """"PAUSED, therefore neither shadow nor live" is a category error made once already."""
    reporter = HealthReporter(safety_state_fn=lambda: "paused", shadow_fn=lambda: False)
    status = reporter.status()
    assert status["exec_mode"] == "PAUSED"
    assert status["exec_shadow"] is False, "a PAUSED engine is still configured live"
