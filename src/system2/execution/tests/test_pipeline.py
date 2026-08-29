"""EXEC-003 tests — slim execution-only pipeline (determinism, dual-run, guards)."""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime, timedelta, timezone
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
    NoMarketPriceError,
    RiskContext,
    StaleSetupError,
    _atr_stops,
    reanchor_bracket,
    assert_protective_prices,
    is_in_session,
    legacy_atr_risk_parameters,
    require_market_price,
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


# ----- F-306: the expected-entry reference must be a real price --------------------
def _no_suggestion_order(instrument="EUR_USD", side="BUY", atr=0.0016, units=1000) -> ApprovedOrder:
    return ApprovedOrder(
        idempotency_key=f"k-{instrument}-{side}", correlation_id="c-306", instrument=instrument,
        side=side, units=units, granularity="H1", risk_context=RiskContext(atr=atr),
    )


def test_NO_ORDER_IS_EVER_BUILT_WITH_A_NON_POSITIVE_STOP_LOSS():
    """FIX_PLAN 1.6 headline invariant. This is the whole point of the fix.

    F-306 evidence (audit/harness/teamC_idempotency.py Scenario C, 2026-07-31):
        no-suggestion order: entry_ref=0.0016  SL=0.0  TP=0.0064  (ATR=0.0016)
    The ATR was used as the entry price, so ``SL = entry - 1.0*ATR = 0.0``. On a real
    broker that is a rejected order or an unprotected position. Sweep the plausible
    entry-reference values an upstream fault can produce and assert that EVERY order the
    pipeline is willing to construct carries ``stop_loss > 0`` and ``take_profit > 0``.
    """
    pipe = ExecutionPipeline(shadow=True)
    built = rejected = 0
    for instrument, atr, real_price in (
        ("EUR_USD", 0.0016, 1.10000), ("GBP_USD", 0.0020, 1.25000),
        ("USD_JPY", 0.0500, 156.30), ("USD_CAD", 0.0012, 1.37000),
    ):
        for side in ("BUY", "SELL"):
            order = _no_suggestion_order(instrument, side, atr,
                                         units=1000 if side == "BUY" else -1000)
            candidate_refs = [
                real_price,       # the only correct value: a real market price
                atr,              # THE BUG: raw ATR passed off as a price
                order.risk_context.atr * 1.0,
                0.0, -1.0, None, float("nan"), float("inf"),
                atr / 2.0,        # any price below 1 ATR ⇒ SL would be <= 0
            ]
            for ref in candidate_refs:
                try:
                    c = pipe.build_order(order, ref)
                except InvalidOrderError:
                    rejected += 1
                    continue
                built += 1
                assert c.stop_loss > 0, f"{instrument} {side} ref={ref} built SL={c.stop_loss}"
                assert c.take_profit > 0, f"{instrument} {side} ref={ref} built TP={c.take_profit}"
    assert built == 8, "exactly the 8 real-price cases should construct"
    assert rejected == 8 * 8


def test_atr_as_entry_price_is_refused_verbatim_harness_case():
    """The exact Scenario C input that printed ``SL=0.0``: EUR_USD BUY, ATR 0.0016, ref 0.0016."""
    pipe = ExecutionPipeline(shadow=True)
    order = _no_suggestion_order()
    with pytest.raises(InvalidOrderError) as exc:
        pipe.build_order(order, order.risk_context.atr)
    assert "stop_loss" in str(exc.value) or "not a positive price" in str(exc.value)


def test_missing_price_is_a_reject_not_a_fallback():
    pipe = ExecutionPipeline(shadow=True)
    with pytest.raises(NoMarketPriceError):
        pipe.build_order(_no_suggestion_order(), None)


@pytest.mark.parametrize("bad", [None, 0.0, -1.10, float("nan"), float("inf"), "not-a-price"])
def test_require_market_price_rejects_every_non_price(bad):
    with pytest.raises(NoMarketPriceError):
        require_market_price("EUR_USD", bad)


def test_process_with_no_price_rejects_and_never_submits():
    """No price ⇒ REJECTED_INVALID (durably acked by the consumer), and no broker call."""
    submits = []
    pipe = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=False,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    res = pipe.process(_no_suggestion_order(), None, submit_fn=lambda c: submits.append(c))
    assert res["decision"] is Decision.REJECTED_INVALID
    assert res["order"] is None and submits == []


def test_suggested_stop_of_zero_is_refused():
    """The System-3-supplied branch used to be taken verbatim, with no validation at all."""
    pipe = ExecutionPipeline(shadow=True)
    order = ApprovedOrder(
        idempotency_key="k-zero-sl", correlation_id="c", instrument="EUR_USD", side="BUY",
        units=1000, granularity="H1",
        risk_context=RiskContext(atr=0.0016, suggested_sl=0.0, suggested_tp=1.1050),
    )
    with pytest.raises(InvalidOrderError):
        pipe.build_order(order, 1.10000)


def test_suggested_stop_on_the_wrong_side_is_refused():
    pipe = ExecutionPipeline(shadow=True)
    order = ApprovedOrder(
        idempotency_key="k-wrong-side", correlation_id="c", instrument="EUR_USD", side="BUY",
        units=1000, granularity="H1",
        # a BUY whose stop sits ABOVE the market: risk is inverted, broker would reject
        risk_context=RiskContext(atr=0.0016, suggested_sl=1.1050, suggested_tp=1.1100),
    )
    with pytest.raises(InvalidOrderError):
        pipe.build_order(order, 1.10000)


def test_stop_price_as_entry_reference_is_refused():
    """F-306's production ``_price_fn``: ``suggested_sl`` used as the entry price.

    With entry == SL the order is degenerate (zero risk distance, 50 pips of phantom
    slippage on every fill). It must not construct.
    """
    pipe = ExecutionPipeline(shadow=True)
    order = ApprovedOrder(
        idempotency_key="k-sl-as-entry", correlation_id="c", instrument="EUR_USD", side="BUY",
        units=1000, granularity="H1",
        risk_context=RiskContext(atr=0.0016, suggested_sl=1.0950, suggested_tp=1.1150),
    )
    with pytest.raises(InvalidOrderError):
        pipe.build_order(order, order.risk_context.suggested_sl)
    # ...and it constructs normally once a REAL market price is supplied
    c = pipe.build_order(order, 1.10000)
    assert c.entry_price == 1.10000 and c.stop_loss == 1.0950


def test_assert_protective_prices_returns_floats_on_the_happy_path():
    assert assert_protective_prices("EUR_USD", 1, 1.10, 1.099, 1.103) == (1.099, 1.103)
    assert assert_protective_prices("EUR_USD", -1, 1.10, 1.101, 1.097) == (1.101, 1.097)


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


def test_mark_lands_before_persist_so_a_post_fill_failure_cannot_resubmit():
    """F-303: ``mark()`` used to run AFTER persist/emit.

    A persist failure (Fact_Live_Trades write) after a successful broker fill propagates to the
    consumer, which nacks for redelivery — and the redelivery found ``seen()`` still False and
    submitted a SECOND live order for one approved order. The marker must outlive anything that
    can fail after the fill.
    """
    store = InMemoryProcessedStore()
    submits = []
    pipe = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=False, processed_store=store,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    order = _orders()[0]

    def boom(_c, _f):
        raise RuntimeError("simulated Fact_Live_Trades write failure")

    with pytest.raises(RuntimeError):
        pipe.process(order, 1.10000,
                     submit_fn=lambda c: submits.append(c) or {"id": "fill1"}, persist_fn=boom)
    assert store.seen(order.idempotency_key)  # marked despite the failure that followed

    res = pipe.process(order, 1.10000,
                       submit_fn=lambda c: submits.append(c) or {"id": "fill2"})
    assert res["decision"] is Decision.SKIPPED_DUPLICATE
    assert len(submits) == 1  # the broker saw the order ONCE


def test_signal_id_guard_blocks_a_second_order_for_the_same_signal():
    """Defence in depth (F-206): one approved signal => at most one broker order, even if
    System 3 regresses and re-mints ``order_request_id`` for a signal it already published."""
    store = InMemoryProcessedStore()
    submits = []
    pipe = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=False, processed_store=store,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    base = _orders()[0]
    first = dataclasses.replace(base, idempotency_key="ord-a", signal_id="sig-9")
    reminted = dataclasses.replace(base, idempotency_key="ord-b", signal_id="sig-9")

    assert pipe.process(first, 1.10000,
                        submit_fn=lambda c: submits.append(c))["decision"] is Decision.EXECUTED
    res = pipe.process(reminted, 1.10000, submit_fn=lambda c: submits.append(c))
    assert res["decision"] is Decision.SKIPPED_DUPLICATE
    assert len(submits) == 1  # a different order id no longer buys a second position


# ----- backup correlation guard ---------------------------------------------------
def test_backup_guard_rejects_past_backstop():
    guard = BackupCorrelationGuard(max_open_positions=2)
    pipe = ExecutionPipeline(shadow=True, backup_guard=guard,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    res = pipe.process(_orders()[0], 1.10000, open_instruments=["AUD_USD", "NZD_USD"])
    assert res["decision"] is Decision.REJECTED_BACKUP_GUARD


def test_backup_guard_allows_re_entry_into_held_instrument():
    """Same-pair re-entry is System 3's call, not this backstop's.

    The old behaviour rejected outright, which blocked every approved order on EUR_USD,
    GBP_USD and USD_CAD for four days behind three positions opened 2026-08-24.
    """
    pipe = ExecutionPipeline(shadow=True,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    res = pipe.process(_orders()[0], 1.10000, open_instruments=["EUR_USD"])
    assert res["decision"] is not Decision.REJECTED_BACKUP_GUARD


def test_backup_guard_still_caps_total_positions_with_duplicates():
    """Re-entry is allowed, but the overall ceiling still bounds it."""
    guard = BackupCorrelationGuard(max_open_positions=2)
    pipe = ExecutionPipeline(shadow=True, backup_guard=guard,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    res = pipe.process(_orders()[0], 1.10000, open_instruments=["EUR_USD", "EUR_USD"])
    assert res["decision"] is Decision.REJECTED_BACKUP_GUARD


# ----- bracket re-anchoring (entry drift) ------------------------------------------
def test_reanchor_preserves_stop_distance_at_the_real_entry():
    """The 2026-08-24 EUR_USD fill: intended 151.9 pips, actually got 103.0."""
    sl, tp = reanchor_bracket("EUR_USD", 1, 1.16647, 1.17136, 1.156165, 1.186555)
    assert round(1.16647 - sl, 6) == round(1.17136 - 1.156165, 6)   # stop distance preserved
    assert round(tp - 1.16647, 6) == round(1.186555 - 1.17136, 6)   # target distance preserved
    assert sl < 1.16647 < tp


def test_reanchor_rescues_the_order_that_was_rejected_wrong_side():
    """The 2026-08-26 GBP_USD reject: drift 73 pips vs a 64-pip stop."""
    sl, tp = reanchor_bracket("GBP_USD", 1, 1.36051, 1.36778, 1.361345, 1.374215)
    assert sl < 1.36051 < tp
    assert round(1.36051 - sl, 6) == round(1.36778 - 1.361345, 6)


def test_reanchor_reverses_geometry_for_a_short():
    sl, tp = reanchor_bracket("GBP_USD", -1, 1.36051, 1.36778, 1.374215, 1.361345)
    assert tp < 1.36051 < sl


def test_reanchor_rejects_a_stale_setup_past_the_drift_limit():
    with pytest.raises(StaleSetupError):
        reanchor_bracket("GBP_USD", 1, 1.34000, 1.36778, 1.361345, 1.374215,
                         drift_sl_mult=2.0)


def test_reanchor_rejects_a_degenerate_bracket():
    with pytest.raises(InvalidOrderError):
        reanchor_bracket("EUR_USD", 1, 1.10, 1.10, 1.10, 1.10)


def test_build_order_without_entry_anchor_keeps_verbatim_behaviour():
    """Orders minted before this change carry no proposed_entry — do not reject them."""
    pipe = ExecutionPipeline(shadow=True,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    rc = RiskContext(atr=0.001, suggested_sl=1.0950, suggested_tp=1.1150)
    order = dataclasses.replace(_orders()[0], risk_context=rc)
    assert order.risk_context.proposed_entry is None
    built = pipe.build_order(order, 1.10000)
    assert built.stop_loss == 1.0950
    assert built.take_profit == 1.1150


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


# ----- FX session boundaries (DST-aware) --------------------------------------------
def test_session_tracks_dst_not_a_fixed_utc_hour():
    """17:00 ET is 21:00 UTC in summer and 22:00 UTC in winter.

    The old fixed pair (Sun 22:00 -> Fri 20:00 UTC) closed the book an hour early every
    summer Friday and refused the first hour of every summer Sunday session.
    """
    from system2.execution.pipeline import is_in_session

    # --- summer (EDT, close 21:00 UTC) ---
    assert is_in_session(datetime(2026, 8, 28, 20, 59, tzinfo=timezone.utc))      # open
    assert not is_in_session(datetime(2026, 8, 28, 21, 1, tzinfo=timezone.utc))   # closed
    assert not is_in_session(datetime(2026, 8, 30, 20, 59, tzinfo=timezone.utc))  # pre-open
    assert is_in_session(datetime(2026, 8, 30, 21, 1, tzinfo=timezone.utc))       # reopened

    # --- winter (EST, close 22:00 UTC) ---
    assert is_in_session(datetime(2026, 12, 4, 21, 59, tzinfo=timezone.utc))      # still open
    assert not is_in_session(datetime(2026, 12, 4, 22, 1, tzinfo=timezone.utc))   # closed
    assert not is_in_session(datetime(2026, 12, 6, 21, 59, tzinfo=timezone.utc))  # pre-open
    assert is_in_session(datetime(2026, 12, 6, 22, 1, tzinfo=timezone.utc))       # reopened


def test_session_closed_all_saturday_and_open_midweek():
    from system2.execution.pipeline import is_in_session

    for h in (0, 6, 12, 18, 23):
        assert not is_in_session(datetime(2026, 8, 29, h, tzinfo=timezone.utc)), f"Sat {h}:00"
    for d in (24, 25, 26, 27):   # Mon-Thu
        for h in (0, 12, 23):
            assert is_in_session(datetime(2026, 8, d, h, tzinfo=timezone.utc)), f"{d} {h}:00"


def test_session_open_hours_match_the_real_fx_week():
    """120 of 168 hours — the market is shut 48h, and we should be available for the other 120."""
    from system2.execution.pipeline import is_in_session

    start = datetime(2026, 8, 24, tzinfo=timezone.utc)  # Monday
    assert sum(1 for i in range(168)
               if is_in_session(start + timedelta(hours=i))) == 120


# ----- drill: rehearse the whole path, stop before the broker -----------------------
def _drill_order(**over) -> ApprovedOrder:
    base = dict(
        idempotency_key="k-drill-1", correlation_id="c-drill", instrument="EUR_USD",
        side="BUY", units=1000, granularity="H1",
        risk_context=RiskContext(atr=0.0010), drill=True,
    )
    base.update(over)
    return ApprovedOrder(**base)


def test_drill_is_never_submitted():
    """The headline invariant: a drill must not reach the broker."""
    submits = []
    pipe = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=False,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    res = pipe.process(_drill_order(), 1.10000, submit_fn=lambda c: submits.append(c))
    assert res["decision"] is Decision.DRILL_NOT_SUBMITTED
    assert submits == [], "a drill reached the broker"
    assert res["order"] is not None, "the constructed order should still be returned"
    assert res["fill"] is None


def test_drill_still_passes_through_every_gate():
    """A drill that fails a real check must fail it, not sail through as a rehearsal."""
    pipe = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=False,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    # no market price -> rejected at construction, NOT reported as a clean drill
    res = pipe.process(_drill_order(), None, submit_fn=lambda c: None)
    assert res["decision"] is Decision.REJECTED_INVALID

    # last-line validation rejects -> that wins over the drill short-circuit
    res = pipe.process(_drill_order(idempotency_key="k-drill-2"), 1.10000,
                       submit_fn=lambda c: None,
                       validate_fn=lambda c: (False, "notional over cap"))
    assert res["decision"] is Decision.REJECTED_VALIDATION

    # backup guard rejects -> also wins
    guard = BackupCorrelationGuard(max_open_positions=1)
    pipe2 = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=False, backup_guard=guard,
                              clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    res = pipe2.process(_drill_order(idempotency_key="k-drill-3"), 1.10000,
                        submit_fn=lambda c: None, open_instruments=["GBP_USD"])
    assert res["decision"] is Decision.REJECTED_BACKUP_GUARD


def test_drill_does_not_consume_the_idempotency_key():
    """A rehearsal must not burn the identity of the real order that may follow."""
    submits = []
    pipe = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=False,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    pipe.process(_drill_order(), 1.10000, submit_fn=lambda c: submits.append(c))
    real = dataclasses.replace(_drill_order(), drill=False)
    res = pipe.process(real, 1.10000, submit_fn=lambda c: submits.append(c))
    assert res["decision"] is Decision.EXECUTED
    assert len(submits) == 1, "the real order after a drill must still execute"


def test_absent_drill_flag_means_real_order():
    """Back-compat + fail-safe: no flag is a REAL order, never a silent rehearsal."""
    submits = []
    pipe = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY, shadow=False,
                             clock=lambda: datetime(2026, 6, 24, 12, tzinfo=timezone.utc))
    order = ApprovedOrder.from_dict({
        "idempotency_key": "k-real", "correlation_id": "c-real", "instrument": "EUR_USD",
        "side": "BUY", "units": 1000, "granularity": "H1", "risk_context": {"atr": 0.0010},
    })
    assert order.drill is False
    res = pipe.process(order, 1.10000, submit_fn=lambda c: submits.append(c))
    assert res["decision"] is Decision.EXECUTED and len(submits) == 1


def test_drill_flag_survives_from_dict():
    o = ApprovedOrder.from_dict({
        "idempotency_key": "k", "correlation_id": "c", "instrument": "EUR_USD", "side": "BUY",
        "units": 1000, "granularity": "H1", "risk_context": {"atr": 0.001}, "drill": True,
    })
    assert o.drill is True
