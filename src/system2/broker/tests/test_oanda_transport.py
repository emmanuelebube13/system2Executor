"""EXEC-006 transport tests — request shaping only (no network; ``_request`` is stubbed).

Guards F-308 at the last layer before the wire: ``modify_trade_stop`` used to render every
stop price as ``f"{price:.5f}"``, which is an illegal price for USD_JPY (displayPrecision=3)
— including on the "attach a missing stop or mark the position UNSAFE" repair path.
"""

from __future__ import annotations

import pytest

from system2.broker.oanda_adapter import BrokerEnvironment
from system2.broker.oanda_transport import OandaRestTransport

PRACTICE = BrokerEnvironment("practice", "tok", "101-002-1234-001", "https://api-fxpractice.oanda.com")


class _Capture:
    """Stands in for ``OandaRestTransport._request``: records the endpoint, sends nothing."""

    def __init__(self, response: dict | None = None) -> None:
        self.requests: list = []
        self.response = response or {}

    def __call__(self, req):
        self.requests.append(req)
        return self.response


@pytest.fixture()
def transport() -> OandaRestTransport:
    return OandaRestTransport(secrets=None, environment=PRACTICE)


def _stub(transport, response=None) -> _Capture:
    cap = _Capture(response)
    transport._request = cap  # type: ignore[method-assign]
    return cap


def test_modify_trade_stop_renders_jpy_at_three_decimals(transport):
    cap = _stub(transport)
    transport.modify_trade_stop("T1", 153.427183, "USD_JPY")
    assert cap.requests[0].data == {"stopLoss": {"price": "153.427", "timeInForce": "GTC"}}


def test_modify_trade_stop_renders_non_jpy_at_five_decimals(transport):
    cap = _stub(transport)
    transport.modify_trade_stop("T1", 1.0994567, "EUR_USD")
    assert cap.requests[0].data == {"stopLoss": {"price": "1.09946", "timeInForce": "GTC"}}


def test_modify_trade_stop_looks_the_instrument_up_when_the_caller_omits_it(transport):
    cap = _stub(transport, {"trade": {"id": "T1", "instrument": "USD_JPY"}})
    transport.modify_trade_stop("T1", 153.427183)
    assert cap.requests[-1].data == {"stopLoss": {"price": "153.427", "timeInForce": "GTC"}}


def test_pricing_and_instrument_endpoints_are_read_only_and_shaped_right(transport):
    cap = _stub(transport, {"prices": [{"instrument": "EUR_USD"}],
                            "instruments": [{"name": "EUR_USD"}]})
    assert transport.get_pricing(["EUR_USD", "USD_JPY"]) == [{"instrument": "EUR_USD"}]
    assert cap.requests[0].params == {"instruments": "EUR_USD,USD_JPY"}
    assert cap.requests[0].method == "GET"

    assert transport.get_account_instruments(["USD_JPY"]) == [{"name": "EUR_USD"}]
    assert cap.requests[1].params == {"instruments": "USD_JPY"}
    assert cap.requests[1].method == "GET"
