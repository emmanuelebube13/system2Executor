"""EXEC-004 validation tests — envelope checks, TTL/expiry, last-line §7.2 gate."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from system2.execution.pipeline import ConstructedOrder
from system2.execution.validation import (
    OrderValidator,
    is_expired,
    validate_envelope,
)

NOW = datetime(2026, 6, 24, 12, tzinfo=timezone.utc)  # a Wednesday, in-session


def _msg(**over) -> dict:
    base = {
        "schema_version": "1",
        "message_id": "m1",
        "idempotency_key": "k1",
        "correlation_id": "c1",
        "created_at": "2026-06-24T11:59:00Z",
        "instrument": "EUR_USD",
        "side": "BUY",
        "units": 10000,
        "granularity": "H1",
        "risk_context": {"atr": 0.0010},
    }
    base.update(over)
    return base


def _order(**over) -> ConstructedOrder:
    base = dict(
        idempotency_key="k1", correlation_id="c1", instrument="EUR_USD", side="BUY",
        units=10000, entry_price=1.10, stop_loss=1.099, take_profit=1.103,
        atr_value=0.0010, rr_ratio=3.0,
    )
    base.update(over)
    return ConstructedOrder(**base)


# ----- envelope validation --------------------------------------------------------
def test_valid_envelope_passes():
    assert validate_envelope(_msg()).ok


def test_non_dict_rejected():
    assert validate_envelope("nope").code == "not_a_dict"


def test_bad_schema_version_rejected():
    assert validate_envelope(_msg(schema_version="9")).code == "schema_version"


def test_missing_field_rejected():
    m = _msg()
    del m["instrument"]
    res = validate_envelope(m)
    assert res.code == "missing_field" and "instrument" in res.reason


def test_invalid_side_rejected():
    assert validate_envelope(_msg(side="HOLD")).code == "side"


def test_invalid_granularity_rejected():
    assert validate_envelope(_msg(granularity="M5")).code == "granularity"


def test_zero_units_rejected():
    assert validate_envelope(_msg(units=0)).code == "zero_units"


def test_non_numeric_units_rejected():
    assert validate_envelope(_msg(units="lots")).code == "units"


def test_bad_risk_context_rejected():
    assert validate_envelope(_msg(risk_context={"atr": 0})).code == "risk_context"
    assert validate_envelope(_msg(risk_context="x")).code == "risk_context"


# ----- TTL / expiry ---------------------------------------------------------------
def test_expires_at_in_past_is_expired():
    m = _msg(expires_at="2026-06-24T11:00:00Z")
    assert is_expired(m, NOW)


def test_expires_at_in_future_not_expired():
    m = _msg(expires_at="2026-06-24T13:00:00Z")
    assert not is_expired(m, NOW)


def test_created_at_age_beyond_max_age_is_expired():
    old = (NOW - timedelta(seconds=600)).isoformat().replace("+00:00", "Z")
    assert is_expired(_msg(created_at=old), NOW, max_age_sec=300)
    assert not is_expired(_msg(created_at=old), NOW, max_age_sec=1200)


def test_no_ttl_info_not_expired():
    m = _msg()
    m.pop("created_at")
    assert not is_expired(m, NOW)


# ----- last-line §7.2 order validation --------------------------------------------
def test_order_validator_passes_clean_order():
    v = OrderValidator(clock=lambda: NOW)
    assert v.validate(_order()).ok


def test_missing_stop_loss_rejected():
    v = OrderValidator(clock=lambda: NOW)
    assert v.validate(_order(stop_loss=0)).code == "no_stop_loss"


def test_units_over_cap_rejected():
    v = OrderValidator(max_units_per_pair=5000, clock=lambda: NOW)
    assert v.validate(_order(units=10000)).code == "max_units_per_pair"


def test_total_notional_cap_rejected():
    v = OrderValidator(max_total_notional=1000, clock=lambda: NOW)
    assert v.validate(_order()).code == "max_total_notional"


def test_leverage_cap_rejected():
    v = OrderValidator(max_leverage=2.0, clock=lambda: NOW)
    # notional = 10000 * 1.10 = 11000; equity 1000 -> 11x leverage
    assert v.validate(_order(), account_equity=1000).code == "max_leverage"


def test_untradeable_instrument_rejected():
    v = OrderValidator(tradeable_instruments=frozenset({"GBP_USD"}), clock=lambda: NOW)
    assert v.validate(_order(instrument="EUR_USD")).code == "untradeable"


def test_out_of_session_rejected():
    sat = datetime(2026, 6, 27, 12, tzinfo=timezone.utc)
    v = OrderValidator(clock=lambda: sat)
    assert v.validate(_order()).code == "out_of_session"
