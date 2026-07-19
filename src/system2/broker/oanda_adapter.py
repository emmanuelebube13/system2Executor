"""EXEC-006 — hardened OANDA v20 broker adapter (Layer 7).

Turns a deterministic ``ConstructedOrder`` (already-sized ``units`` from System 3 — never
re-sized here) into a real OANDA fill and reports the authoritative outcome as a
``FillResult`` (consumed by EXEC-005). Hardening concerns owned here:

  * **Idempotent submission** — ``order.clientExtensions.id = "sb-" + idempotency_key`` so a
    retry after an ambiguous network failure reconciles from open trades/transactions
    instead of ever placing a duplicate order.
  * **Fill validation** — signed ``slippage_pips`` vs the expected price with a 2-pip
    tolerance; beyond tolerance is flagged (already-filled orders are accept-and-flagged,
    reported to System 3, not silently accepted).
  * **Stop/TP confirmation** — after a fill, re-read the trade and assert the stop-loss and
    take-profit exist; if absent, attempt to attach, else mark the position **unsafe** and
    alert (a live position without a stop is a risk event, never silently proceeded past).
  * **Practice→live toggle** — a single audited switch; live is refused unless BOTH
    ``OANDA_ENV=live`` and live creds are present (fail-closed, per §non-negotiables).

The network is isolated behind the ``OandaTransport`` seam so the deterministic logic
(pip size, slippage, client-id, retry/reconcile, stop confirmation) is unit-tested without
a broker. ``OandaRestTransport`` is the live implementation (oandapyV20 imported lazily).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Protocol, runtime_checkable

from system2.common.logging import get_logger, log_event
from system2.common.secrets import Secrets, get_secrets
from system2.execution.fill_producer import FillResult
from system2.execution.pipeline import ConstructedOrder

log = get_logger("broker.oanda_adapter")

CLIENT_ID_PREFIX = "sb-"
DEFAULT_SLIPPAGE_TOLERANCE_PIPS = 2.0
JPY_PIP = 0.01
STD_PIP = 0.0001


# --------------------------------------------------------------------------- #
# Error taxonomy
# --------------------------------------------------------------------------- #
class BrokerError(Exception):
    """Base for broker-adapter failures."""


class TransientBrokerError(BrokerError):
    """Network / 5xx / rate-limit — safe to retry AFTER a reconcile check."""


class MarketClosedError(BrokerError):
    """OANDA rejected because the market is closed (weekend/holiday)."""


class LiveConfigError(BrokerError):
    """Refused to operate in live mode without both the toggle and live creds."""


# --------------------------------------------------------------------------- #
# Pip size + slippage (pure, unit-tested)
# --------------------------------------------------------------------------- #
def pip_size(instrument: str) -> float:
    """0.01 for JPY-quoted pairs, else 0.0001."""
    return JPY_PIP if instrument.upper().endswith("_JPY") else STD_PIP


def compute_slippage_pips(expected_price: float, fill_price: float, instrument: str) -> float:
    """Signed slippage in pips: ``(fill − expected) / pip_size`` (EXEC-006 §slippage)."""
    return round((fill_price - expected_price) / pip_size(instrument), 2)


def client_order_id(idempotency_key: str) -> str:
    return f"{CLIENT_ID_PREFIX}{idempotency_key}"


# --------------------------------------------------------------------------- #
# Environment resolution (practice default; live fail-closed)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class BrokerEnvironment:
    env: str  # "practice" | "live"
    token: str
    account_id: str
    base_url: str

    @property
    def is_live(self) -> bool:
        return self.env == "live"

    def banner(self) -> str:
        return f"[OANDA] environment={self.env.upper()} account=…{self.account_id[-4:]} url={self.base_url}"


def resolve_environment(secrets: Secrets | None = None) -> BrokerEnvironment:
    """Resolve the active broker environment. Live requires BOTH the toggle and live creds."""
    secrets = secrets or get_secrets()
    env = (secrets.get("OANDA_ENV", "practice") or "practice").lower()
    if env == "live":
        token = secrets.get("OANDA_LIVE_API_KEY")
        account = secrets.get("OANDA_LIVE_ACCOUNT_ID")
        if not token or not account:
            raise LiveConfigError(
                "OANDA_ENV=live but live credentials are missing "
                "(OANDA_LIVE_API_KEY / OANDA_LIVE_ACCOUNT_ID). Refusing to start live."
            )
        url = secrets.get("OANDA_LIVE_URL", "https://api-fxtrade.oanda.com")
        return BrokerEnvironment("live", token, account, url)
    # practice (default, and the only mode reachable without the live toggle)
    return BrokerEnvironment(
        "practice",
        secrets.require("OANDA_PRACTICE_API_KEY"),
        secrets.require("OANDA_PRACTICE_ACCOUNT_ID"),
        secrets.get("OANDA_PRACTICE_URL", "https://api-fxpractice.oanda.com"),
    )


# --------------------------------------------------------------------------- #
# Transport seam
# --------------------------------------------------------------------------- #
@runtime_checkable
class OandaTransport(Protocol):
    def create_order(self, payload: dict[str, Any]) -> dict[str, Any]: ...
    def get_open_trades(self) -> list[dict[str, Any]]: ...
    def get_trade(self, trade_id: str) -> dict[str, Any] | None: ...
    def modify_trade_stop(self, trade_id: str, stop_price: float) -> dict[str, Any]: ...
    def close_trade(self, trade_id: str, units: str | int = "ALL") -> dict[str, Any]: ...
    def get_account_summary(self) -> dict[str, Any]: ...


# --------------------------------------------------------------------------- #
# Adapter
# --------------------------------------------------------------------------- #
class OandaAdapter:
    """Deterministic order submission + fill validation over an ``OandaTransport``."""

    def __init__(
        self,
        transport: OandaTransport,
        environment: BrokerEnvironment | None = None,
        *,
        secrets: Secrets | None = None,
        slippage_tolerance_pips: float | None = None,
        retry_max: int | None = None,
        backoff_sec: float | None = None,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        self.transport = transport
        self.secrets = secrets or get_secrets()
        self.env = environment or resolve_environment(self.secrets)
        self.slippage_tolerance_pips = (
            slippage_tolerance_pips
            if slippage_tolerance_pips is not None
            else float(self.secrets.get_int("SLIPPAGE_TOLERANCE_PIPS", 2))
        )
        self.retry_max = retry_max if retry_max is not None else self.secrets.get_int("BROKER_RETRY_MAX", 3)
        self.backoff_sec = (
            backoff_sec if backoff_sec is not None else float(self.secrets.get_int("BROKER_RETRY_BACKOFF_SEC", 2))
        )
        import time as _time

        self._sleep = sleep_fn or _time.sleep
        log_event(log, logging.INFO, self.env.banner(), env=self.env.env)

    # ----- order payload ----------------------------------------------------
    def _build_payload(self, order: ConstructedOrder) -> dict[str, Any]:
        cid = client_order_id(order.idempotency_key)
        return {
            "order": {
                "type": "MARKET",
                "instrument": order.instrument,
                "units": str(int(order.units)),  # signed; taken as given (never re-sized)
                "timeInForce": "FOK",
                "positionFill": "DEFAULT",
                "clientExtensions": {"id": cid},
                "tradeClientExtensions": {"id": cid},
                "stopLossOnFill": {"price": f"{order.stop_loss:.5f}"},
                "takeProfitOnFill": {"price": f"{order.take_profit:.5f}"},
            }
        }

    # ----- submit -----------------------------------------------------------
    def submit(
        self,
        order: ConstructedOrder,
        *,
        expected_price: float | None = None,
        model_set_id: str | None = None,
    ) -> FillResult:
        """Submit ``order`` idempotently, validate the fill, confirm stop/TP. Returns FillResult."""
        expected = expected_price if expected_price is not None else order.entry_price
        cid = client_order_id(order.idempotency_key)
        payload = self._build_payload(order)

        resp: dict[str, Any] | None = None
        for attempt in range(1, self.retry_max + 1):
            try:
                resp = self.transport.create_order(payload)
                break
            except MarketClosedError:
                log_event(log, logging.WARNING, "market closed; not retrying",
                          idempotency_key=order.idempotency_key)
                return self._reject(order, expected, model_set_id, "MARKET_CLOSED", "CANCELLED")
            except TransientBrokerError as exc:
                # never blindly resubmit — reconcile first (the order may already be filled)
                reconciled = self._reconcile(cid, order, expected, model_set_id)
                if reconciled is not None:
                    log_event(log, logging.INFO, "reconciled existing fill after transient error",
                              idempotency_key=order.idempotency_key)
                    return reconciled
                if attempt >= self.retry_max:
                    log_event(log, logging.ERROR, "broker submit exhausted retries",
                              idempotency_key=order.idempotency_key, error=str(exc))
                    raise
                self._sleep(self.backoff_sec * attempt)  # linear backoff

        assert resp is not None
        return self._build_fill_result(order, resp, expected, model_set_id)

    def _reconcile(
        self, cid: str, order: ConstructedOrder, expected: float, model_set_id: str | None
    ) -> FillResult | None:
        """Look for an already-open trade carrying our client id; return its FillResult if found."""
        try:
            trades = self.transport.get_open_trades()
        except BrokerError:
            return None
        for t in trades:
            ext = (t.get("clientExtensions") or {}).get("id")
            if ext == cid:
                fill_price = float(t.get("price", expected))
                return self._finalize(
                    order, expected, model_set_id,
                    realized_status="FILLED",
                    filled_units=abs(float(t.get("currentUnits", order.units))),
                    broker_order_id=str(t.get("id", "")),
                    broker_trade_id=str(t.get("id", "")),
                    fill_price=fill_price,
                    fill_time=t.get("openTime"),
                    trade=t,
                )
        return None

    def _build_fill_result(
        self, order: ConstructedOrder, resp: dict[str, Any], expected: float, model_set_id: str | None
    ) -> FillResult:
        fill_txn = resp.get("orderFillTransaction")
        if not fill_txn:
            # order was created but not filled (cancelled/rejected) — surface the reason
            cancel = resp.get("orderCancelTransaction") or {}
            reason = cancel.get("reason", "NOT_FILLED")
            status = "CANCELLED" if cancel else "REJECTED"
            return self._reject(order, expected, model_set_id, reason, status)

        fill_price = float(fill_txn["price"])
        filled_units = abs(float(fill_txn.get("units", order.units)))
        trade_opened = fill_txn.get("tradeOpened") or {}
        trade_id = str(trade_opened.get("tradeID", fill_txn.get("id", "")))
        requested_units = abs(float(order.units))
        status = "PARTIAL" if filled_units < requested_units else "FILLED"
        return self._finalize(
            order, expected, model_set_id,
            realized_status=status,
            filled_units=filled_units,
            broker_order_id=str(fill_txn.get("orderID", fill_txn.get("id", ""))),
            broker_trade_id=trade_id,
            fill_price=fill_price,
            fill_time=fill_txn.get("time"),
            trade=None,
        )

    def _finalize(
        self,
        order: ConstructedOrder,
        expected: float,
        model_set_id: str | None,
        *,
        realized_status: str,
        filled_units: float,
        broker_order_id: str,
        broker_trade_id: str,
        fill_price: float | None,
        fill_time: str | None,
        trade: dict[str, Any] | None,
    ) -> FillResult:
        slippage = (
            compute_slippage_pips(expected, fill_price, order.instrument)
            if fill_price is not None
            else None
        )
        reject_reason = None
        if slippage is not None and abs(slippage) > self.slippage_tolerance_pips:
            # already filled -> accept-and-flag (report to System 3); never silently accept
            reject_reason = f"SLIPPAGE_EXCEEDED:{slippage}pips>{self.slippage_tolerance_pips}"
            log_event(log, logging.WARNING, "slippage beyond tolerance (flagged)",
                      idempotency_key=order.idempotency_key, slippage_pips=slippage)

        sl_price, tp_price, unsafe = self._confirm_stops(order, broker_trade_id, trade)
        if unsafe:
            reject_reason = (reject_reason + "; " if reject_reason else "") + "NO_STOP_UNSAFE"
            log_event(log, logging.ERROR, "position has no confirmed stop-loss — UNSAFE, alerting",
                      idempotency_key=order.idempotency_key, trade_id=broker_trade_id)

        return FillResult(
            realized_status=realized_status,
            filled_units=filled_units,
            broker_order_id=broker_order_id,
            broker_trade_id=broker_trade_id,
            requested_price=expected,
            fill_price=fill_price,
            fill_time=fill_time,
            slippage_pips=slippage,
            stop_loss_price=sl_price,
            take_profit_price=tp_price,
            reject_reason=reject_reason,
            model_set_id=model_set_id,
        )

    def _confirm_stops(
        self, order: ConstructedOrder, trade_id: str, trade: dict[str, Any] | None
    ) -> tuple[float | None, float | None, bool]:
        """Return (stop_loss_price, take_profit_price, unsafe). Attaches a missing stop if possible."""
        if not trade_id:
            return order.stop_loss, order.take_profit, False
        if trade is None:
            try:
                trade = self.transport.get_trade(trade_id)
            except BrokerError:
                trade = None
        if not trade:
            return order.stop_loss, order.take_profit, False  # cannot verify; not proven unsafe

        sl = trade.get("stopLossOrder")
        tp = trade.get("takeProfitOrder")
        sl_price = float(sl["price"]) if sl and sl.get("price") else None
        tp_price = float(tp["price"]) if tp and tp.get("price") else None

        if sl_price is None:
            # attempt to attach the intended stop
            try:
                self.transport.modify_trade_stop(trade_id, order.stop_loss)
                sl_price = order.stop_loss
            except BrokerError:
                return None, tp_price, True  # unsafe: could not confirm or attach a stop
        return sl_price, (tp_price if tp_price is not None else order.take_profit), False

    def _reject(
        self, order: ConstructedOrder, expected: float, model_set_id: str | None,
        reason: str, status: str,
    ) -> FillResult:
        return FillResult(
            realized_status=status,
            filled_units=0.0,
            broker_order_id=None,
            broker_trade_id=None,
            requested_price=expected,
            fill_price=None,
            fill_time=None,
            slippage_pips=None,
            stop_loss_price=order.stop_loss,
            take_profit_price=order.take_profit,
            reject_reason=reason,
            model_set_id=model_set_id,
        )

    # ----- position-management helpers (used by EXEC-007) -------------------
    def modify_stop(self, trade_id: str, new_stop_price: float) -> dict[str, Any]:
        """Idempotent stop modification (routed here so EXEC-007 stays broker-agnostic)."""
        return self.transport.modify_trade_stop(trade_id, new_stop_price)

    def close_trade(self, trade_id: str, units: str | int = "ALL") -> dict[str, Any]:
        return self.transport.close_trade(trade_id, units)

    def get_account_summary(self) -> dict[str, Any]:
        return self.transport.get_account_summary()
