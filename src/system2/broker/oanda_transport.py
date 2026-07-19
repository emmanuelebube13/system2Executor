"""EXEC-006 — live OANDA v20 REST transport (the network glue behind OandaTransport).

Isolated from ``OandaAdapter`` so the adapter's decision logic is unit-tested without a
broker. Maps oandapyV20 responses/errors to the adapter's transport contract + error
taxonomy (transient vs market-closed). The SDK is imported lazily so dev/tests run without
it, and rate-limit/5xx are surfaced as ``TransientBrokerError`` for the adapter to
reconcile-and-retry.
"""

from __future__ import annotations

from typing import Any

from system2.broker.oanda_adapter import (
    BrokerEnvironment,
    MarketClosedError,
    TransientBrokerError,
    resolve_environment,
)
from system2.common.secrets import Secrets, get_secrets

_MARKET_CLOSED_TOKENS = ("MARKET_HALTED", "MARKET_CLOSED", "TRADING_HALTED")


class OandaRestTransport:
    """Concrete ``OandaTransport`` over oandapyV20 (lazy import)."""

    def __init__(self, secrets: Secrets | None = None, environment: BrokerEnvironment | None = None) -> None:
        self.secrets = secrets or get_secrets()
        self.env = environment or resolve_environment(self.secrets)
        self._api = None

    def _client(self):
        if self._api is None:
            from oandapyV20 import API  # lazy

            self._api = API(
                access_token=self.env.token,
                environment="live" if self.env.is_live else "practice",
            )
        return self._api

    def _request(self, req) -> dict[str, Any]:
        from oandapyV20.exceptions import V20Error  # lazy

        try:
            self._client().request(req)
            return req.response
        except V20Error as exc:
            code = getattr(exc, "code", None) or 0
            msg = str(exc)
            if any(tok in msg.upper() for tok in _MARKET_CLOSED_TOKENS):
                raise MarketClosedError(msg) from exc
            if code == 429 or 500 <= code < 600:
                raise TransientBrokerError(f"transient OANDA error {code}: {msg}") from exc
            raise

    # ----- OandaTransport protocol -----------------------------------------
    def create_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        from oandapyV20.endpoints.orders import OrderCreate

        resp = self._request(OrderCreate(accountID=self.env.account_id, data=payload))
        # OANDA reports a market-closed rejection inside the response, not as an exception.
        cancel = resp.get("orderCancelTransaction") or {}
        if cancel.get("reason") in _MARKET_CLOSED_TOKENS:
            raise MarketClosedError(cancel.get("reason"))
        return resp

    def get_open_trades(self) -> list[dict[str, Any]]:
        from oandapyV20.endpoints.trades import OpenTrades

        resp = self._request(OpenTrades(accountID=self.env.account_id))
        return resp.get("trades", [])

    def get_trade(self, trade_id: str) -> dict[str, Any] | None:
        from oandapyV20.endpoints.trades import TradeDetails

        resp = self._request(TradeDetails(accountID=self.env.account_id, tradeID=trade_id))
        return resp.get("trade")

    def modify_trade_stop(self, trade_id: str, stop_price: float) -> dict[str, Any]:
        from oandapyV20.endpoints.trades import TradeCRCDO

        data = {"stopLoss": {"price": f"{stop_price:.5f}", "timeInForce": "GTC"}}
        return self._request(TradeCRCDO(accountID=self.env.account_id, tradeID=trade_id, data=data))

    def close_trade(self, trade_id: str, units: str | int = "ALL") -> dict[str, Any]:
        from oandapyV20.endpoints.trades import TradeClose

        data = {"units": str(units)}
        return self._request(TradeClose(accountID=self.env.account_id, tradeID=trade_id, data=data))

    def get_account_summary(self) -> dict[str, Any]:
        from oandapyV20.endpoints.accounts import AccountSummary

        resp = self._request(AccountSummary(accountID=self.env.account_id))
        return resp.get("account", {})


def build_transport(secrets: Secrets | None = None) -> OandaRestTransport:
    return OandaRestTransport(secrets=secrets)
