"""EXEC-003 tests — slim execution-only pipeline (determinism, dual-run, guards)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from system2.execution.pipeline import (
    ApprovedOrder,
    BackupCorrelationGuard,
    ConstructedOrder,
    Decision,
    ExecMode,
    ExecutionPipeline,
    InMemoryProcessedStore,
    InvalidOrderError,
    RiskContext,
    _atr_stops,
    is_in_session,
    legacy_atr_risk_parameters,
)

GOLDEN = Path(__file__).parent / "golden" / "exec003_orders.json"


def _orders() -> list[ApprovedOrder]:
    """Fixed, representative approved orders (the golden-file inputs)."""
    return [
        ApprovedOrder(
            idempotency_key="k-buy-eurusd", correlation_id="c1", instrument="EUR_USD",
            side="BUY", units=10000, granularity="H1", risk_context=RiskContext(atr=0.0010),
        ),
        ApprovedOrder(
            idempotency_key="k-sell-gbpusd", correlation_id="c2", instrument="GBP_USD",
            side="SELL", units=-5000, granularity="H4", risk_context=RiskContext(atr=0.0020),
        ),
        ApprovedOrder(
            idempotency_key="k-buy-usdjpy", correlation_id="c3", instrument="USD_JPY",
            side="BUY", units=3000, granularity="H1",
            risk_context=RiskContext(atr=0.05, suggested_sl=156.00, suggested_tp=156.90),
        ),
    ]


def _build_all(pipe: ExecutionPipeline) -> list[dict]:
    prices = {"EUR_USD": 1.10000, "GBP_USD": 1.25000, "USD_JPY": 156.30}
    return [pipe.build_order(o, prices[o.instrument]).to_dict() for o in _orders()]


# ----- ATR math contract ----------------------------------------------------------
def test_atr_stops_buy_and_sell():
    sl, tp = _atr_stops(1, 1.10, 0.0010)   # 1.0x SL, 3.0x TP
    assert sl == pytest.approx(1.099) and tp == pytest.approx(1.103)
    sl, tp = _atr_stops(-1, 1.25, 0.0020)
    assert sl == pytest.approx(1.252) and tp == pytest.approx(1.244)


def test_atr_invalid_atr_raises():
    with pytest.raises(InvalidOrderError):
        _atr_stops(1, 1.10, 0.0)


# ----- golden-file determinism ----------------------------------------------------
def test_golden_file_determinism():
    pipe = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=True)
    out = _build_all(pipe)
    assert GOLDEN.exists(), "golden file missing — regenerate with scripts"
    expected = json.loads(GOLDEN.read_text())
    assert out == expected  # exact match pins the execution-determinism contract


def test_build_is_repeatable():
    p1, p2 = ExecutionPipeline(shadow=True), ExecutionPipeline(shadow=True)
    assert _build_all(p1) == _build_all(p2)


# ----- dual-run shadow: slim path matches the legacy formula -----------------------
def test_dual_run_matches_legacy():
    pipe = ExecutionPipeline(shadow=True)
    prices = {"EUR_USD": 1.10000, "GBP_USD": 1.25000}
    for o in _orders()[:2]:  # the two ATR-derived (no suggested SL/TP) orders
        c = pipe.build_order(o, prices[o.instrument])
        leg_sl, leg_tp = legacy_atr_risk_parameters(o.direction, prices[o.instrument], o.risk_context.atr)
        assert c.stop_loss == pytest.approx(leg_sl)
        assert c.take_profit == pytest.approx(leg_tp)


# ----- units are never re-sized ---------------------------------------------------
def test_units_never_modified():
    pipe = ExecutionPipeline(shadow=True)
    o = _orders()[0]
    c = pipe.build_order(o, 1.10000)
    assert c.units == o.units == 10000


# ----- suggested SL/TP honored ----------------------------------------------------
def test_suggested_sl_tp_used_when_supplied():
    pipe = ExecutionPipeline(shadow=True)
    o = _orders()[2]
    c = pipe.build_order(o, 156.30)
    assert c.stop_loss == 156.00 and c.take_profit == 156.90


# ----- session guard --------------------------------------------------------------
def test_session_guard():
    sat = datetime(2026, 6, 27, 12, tzinfo=timezone.utc)   # Saturday
    sun_early = datetime(2026, 6, 28, 10, tzinfo=timezone.utc)  # Sunday pre-open
    sun_open = datetime(2026, 6, 28, 22, tzinfo=timezone.utc)   # Sunday 22:00
    fri_late = datetime(2026, 6, 26, 21, tzinfo=timezone.utc)   # Friday after close
    wed = datetime(2026, 6, 24, 12, tzinfo=timezone.utc)
    assert not is_in_session(sat)
    assert not is_in_session(sun_early)
    assert is_in_session(sun_open)
    assert not is_in_session(fri_late)
    assert is_in_session(wed)


def test_process_rejects_out_of_session():
    pipe = ExecutionPipeline(shadow=True, clock=lambda: datetime(2026, 6, 27, 12, tzinfo=timezone.utc))
    res = pipe.process(_orders()[0], 1.10000)
    assert res["decision"] is Decision.REJECTED_OUT_OF_SESSION


# ----- idempotency / dedup --------------------------------------------------------
def test_duplicate_idempotency_key_skipped():
    store = InMemoryProcessedStore()
    submits = []
    pipe = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=False, processed_store=store,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    res1 = pipe.process(_orders()[0], 1.10000, submit_fn=lambda c: submits.append(c) or {"id": "fill1"})
    assert res1["decision"] is Decision.EXECUTED
    res2 = pipe.process(_orders()[0], 1.10000, submit_fn=lambda c: submits.append(c) or {"id": "fill2"})
    assert res2["decision"] is Decision.SKIPPED_DUPLICATE
    assert len(submits) == 1  # exactly one broker submit


# ----- backup correlation guard ---------------------------------------------------
def test_backup_guard_rejects_past_backstop():
    guard = BackupCorrelationGuard(max_open_positions=2)
    pipe = ExecutionPipeline(shadow=True, backup_guard=guard,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    res = pipe.process(_orders()[0], 1.10000, open_instruments=["AUD_USD", "NZD_USD"])
    assert res["decision"] is Decision.REJECTED_BACKUP_GUARD


def test_backup_guard_rejects_duplicate_instrument():
    pipe = ExecutionPipeline(shadow=True,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    res = pipe.process(_orders()[0], 1.10000, open_instruments=["EUR_USD"])
    assert res["decision"] is Decision.REJECTED_BACKUP_GUARD


# ----- mode gating ----------------------------------------------------------------
def test_legacy_mode_defers():
    pipe = ExecutionPipeline(mode=ExecMode.LEGACY)
    res = pipe.process(_orders()[0], 1.10000)
    assert res["decision"] is Decision.DEFERRED_LEGACY


# ----- full execution path with injected hooks ------------------------------------
def test_execution_path_invokes_hooks():
    events = {"submit": 0, "persist": 0, "emit": 0}
    pipe = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=False,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    res = pipe.process(
        _orders()[0], 1.10000,
        submit_fn=lambda c: events.__setitem__("submit", events["submit"] + 1) or {"fill_price": 1.10001},
        persist_fn=lambda c, f: events.__setitem__("persist", events["persist"] + 1),
        emit_fn=lambda c, f: events.__setitem__("emit", events["emit"] + 1),
    )
    assert res["decision"] is Decision.EXECUTED
    assert events == {"submit": 1, "persist": 1, "emit": 1}
    assert isinstance(res["order"], ConstructedOrder)
