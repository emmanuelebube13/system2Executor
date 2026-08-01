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
    format_price,
    resolve_environment,
)
from system2.common.secrets import Secrets, get_secrets

_MARKET_CLOSED_TOKENS = ("MARKET_HALTED", "MARKET_CLOSED", "TRADING_HALTED")

# OANDA rejects a ``transactions/idrange`` page wider than 1000 ids.
MAX_TRANSACTION_ID_RANGE = 1000


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

    def modify_trade_stop(
        self, trade_id: str, stop_price: float, instrument: str | None = None
    ) -> dict[str, Any]:
        from oandapyV20.endpoints.trades import TradeCRCDO

        # F-308: was ``f"{stop_price:.5f}"`` for every instrument. USD_JPY allows 3 decimals,
        # so every JPY stop-move (including the "attach a missing stop or mark the position
        # UNSAFE" repair path) was sent with an illegal price.
        price = format_price(stop_price, instrument or self._instrument_of(trade_id) or "")
        data = {"stopLoss": {"price": price, "timeInForce": "GTC"}}
        return self._request(TradeCRCDO(accountID=self.env.account_id, tradeID=trade_id, data=data))

    def _instrument_of(self, trade_id: str) -> str | None:
        """Last-resort instrument lookup so a caller that omitted it still gets the right grid."""
        try:
            trade = self.get_trade(trade_id) or {}
        except Exception:  # noqa: BLE001 — best effort; falls back to the default precision
            return None
        return trade.get("instrument")

    def get_pricing(self, instruments: list[str]) -> list[dict[str, Any]]:
        """Current tradeable quotes (F-306 expected-entry reference). Read-only."""
        from oandapyV20.endpoints.pricing import PricingInfo

        resp = self._request(
            PricingInfo(accountID=self.env.account_id, params={"instruments": ",".join(instruments)})
        )
        return resp.get("prices", [])

    def get_account_instruments(self, instruments: list[str] | None = None) -> list[dict[str, Any]]:
        """Instrument specs (``displayPrecision``/``pipLocation``) for F-308 formatting. Read-only."""
        from oandapyV20.endpoints.accounts import AccountInstruments

        params = {"instruments": ",".join(instruments)} if instruments else None
        resp = self._request(AccountInstruments(accountID=self.env.account_id, params=params))
        return resp.get("instruments", [])

    # ----- transaction history (F-303 pre-submit reconcile) -----------------
    def get_transactions_idrange(self, from_id: int, to_id: int) -> list[dict[str, Any]]:
        """One inclusive ``transactions/idrange`` page. Read-only.

        Promoted to a first-class transport method so the adapter's pre-submit reconcile has
        a bounded transaction reader, and so ``close_tracker.fetch_transaction_range`` — which
        already prefers this exact name (close_tracker.py:348) — stops reaching through
        ``transport._request``.
        """
        from oandapyV20.endpoints.transactions import TransactionIDRange

        resp = self._request(
            TransactionIDRange(
                accountID=self.env.account_id,
                params={"from": str(int(from_id)), "to": str(int(to_id))},
            )
        )
        return resp.get("transactions", [])

    def get_recent_transactions(self, count: int = 200) -> list[dict[str, Any]]:
        """The last ``count`` account transactions — bounded, never a full-history scan.

        Two read-only GETs: the account summary for the authoritative ``lastTransactionID``
        head, then one id-range page ending at it. The head is re-read on every call on
        purpose — a watermark cached in this process would miss an order placed *after* it by
        another consumer, or by this consumer before the crash, which is precisely the case
        F-303's pre-submit reconcile exists to catch.

        Raises rather than returning ``[]`` when the lookup fails: an empty list is read by
        the adapter as "verified — no such transaction", and a failed lookup must never be
        mistaken for that.
        """
        head = self._last_transaction_id()
        window = max(1, min(int(count), MAX_TRANSACTION_ID_RANGE))
        return self.get_transactions_idrange(max(1, head - window + 1), head)

    def _last_transaction_id(self) -> int:
        """The account's current ``lastTransactionID`` (present at both levels of the reply)."""
        from oandapyV20.endpoints.accounts import AccountSummary

        resp = self._request(AccountSummary(accountID=self.env.account_id))
        raw = resp.get("lastTransactionID") or (resp.get("account") or {}).get("lastTransactionID")
        try:
            return int(raw)
        except (TypeError, ValueError) as exc:
            raise TransientBrokerError(
                "account summary carried no lastTransactionID; cannot bound a reconcile scan"
            ) from exc

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
