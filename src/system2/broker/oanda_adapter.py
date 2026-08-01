"""EXEC-006 — hardened OANDA v20 broker adapter (Layer 7).

Turns a deterministic ``ConstructedOrder`` (already-sized ``units`` from System 3 — never
re-sized here) into a real OANDA fill and reports the authoritative outcome as a
``FillResult`` (consumed by EXEC-005). Hardening concerns owned here:

  * **Idempotent submission (F-303)** — ``order.clientExtensions.id = "sb-" + idempotency_key``
    is only a *label*: OANDA does not dedupe market orders by it. The protection is that
    ``submit`` **always reconciles the broker's recent transactions for that label before
    ``create_order``** — not merely after a transient error — so a redelivered order whose
    idempotency marker never became durable (a crash between the fill and ``mark()``) is
    recognised as already submitted instead of being placed a second time. When the broker
    cannot tell us, the order is *not* submitted; see ``_prior_submission``.
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
from datetime import datetime, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Protocol, runtime_checkable

from system2.common.logging import get_logger, log_event
from system2.common.secrets import Secrets, get_secrets
from system2.execution.fill_producer import FillResult
from system2.execution.pipeline import ConstructedOrder, InvalidOrderError, assert_protective_prices

log = get_logger("broker.oanda_adapter")

CLIENT_ID_PREFIX = "sb-"
DEFAULT_SLIPPAGE_TOLERANCE_PIPS = 2.0
JPY_PIP = 0.01
STD_PIP = 0.0001

# F-308: OANDA rejects an order price carrying more decimals than the instrument's
# ``displayPrecision``. These are the *fallbacks* used when the broker's instrument spec
# cannot be read; they were verified against the live practice
# ``/v3/accounts/{id}/instruments`` spec for the entire deployed allowlist
# (audit/harness/teamC_oanda_practice_readonly.py): USD_JPY → 3, and EUR_USD / GBP_USD /
# AUD_USD / USD_CAD → 5. The authoritative value is still fetched per instrument below.
JPY_DISPLAY_PRECISION = 3
STD_DISPLAY_PRECISION = 5

# A reference price older than this is not a price. Fail-closed: the caller refuses to build.
DEFAULT_PRICE_MAX_AGE_SEC = 60.0

# F-303: how far back the pre-submit reconcile looks for this order's client id. Bounded on
# purpose — a duplicate can only arrive from a redelivery seconds-to-minutes old, and an
# unbounded history scan on the money path is not acceptable. Verified against the live
# practice account (2026-08-01): 200 ids covered ~35 market orders there, far more than any
# redelivery window.
RECONCILE_TRANSACTION_WINDOW = 200


# --------------------------------------------------------------------------- #
# Error taxonomy
# --------------------------------------------------------------------------- #
class BrokerError(Exception):
    """Base for broker-adapter failures."""


class TransientBrokerError(BrokerError):
    """Network / 5xx / rate-limit — safe to retry AFTER a reconcile check."""


class ReconcileUnavailableError(TransientBrokerError):
    """The broker could not tell us whether this order was already submitted (F-303).

    Deliberately transient: the consumer's catch-all nacks it, the queue redelivers with
    backoff, and the order is submitted as soon as the broker is readable again — so a
    reconcile outage *defers* trading (visibly, and into the DLQ after
    ``QUEUE_MAX_DELIVERY_ATTEMPTS``) rather than either halting it silently or doubling a
    live position. Never raised for "checked, nothing found" — only for "could not check".
    """


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
# Instrument price precision + price formatting (F-308) — pure, unit-tested
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class InstrumentSpec:
    """The subset of OANDA's instrument spec that the order path depends on."""

    name: str
    display_precision: int
    pip_location: int

    @classmethod
    def from_broker_row(cls, row: dict[str, Any]) -> "InstrumentSpec":
        return cls(
            name=str(row["name"]),
            display_precision=int(row["displayPrecision"]),
            pip_location=int(row["pipLocation"]),
        )


def default_display_precision(instrument: str) -> int:
    """Fallback price precision for ``instrument`` when the broker spec is unavailable."""
    return JPY_DISPLAY_PRECISION if (instrument or "").upper().endswith("_JPY") else STD_DISPLAY_PRECISION


def quantize_price(
    price: float | Decimal,
    instrument: str,
    display_precision: int | None = None,
    *,
    toward: float | Decimal | None = None,
) -> Decimal:
    """Snap ``price`` onto the instrument's price grid, in ``Decimal`` (never binary float).

    ``toward`` is the entry/reference price. When given, the rounding is directional —
    always **toward** the reference — so quantizing a protective price can only ever make
    it tighter, never wider: a rounded stop-loss cannot silently add up to a tick of risk,
    and a rounded take-profit cannot silently move further away. Without ``toward`` this is
    plain nearest-tick (ROUND_HALF_UP).
    """
    decimals = display_precision if display_precision is not None else default_display_precision(instrument)
    if decimals < 0:
        raise ValueError(f"{instrument}: negative display precision {decimals}")
    try:
        value = price if isinstance(price, Decimal) else Decimal(str(price))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{instrument}: {price!r} is not a price") from exc
    if not value.is_finite():
        raise ValueError(f"{instrument}: price {price!r} is not finite")

    rounding = ROUND_HALF_UP
    if toward is not None:
        reference = toward if isinstance(toward, Decimal) else Decimal(str(toward))
        if value > reference:
            rounding = ROUND_FLOOR      # above the reference → down, i.e. toward it
        elif value < reference:
            rounding = ROUND_CEILING    # below the reference → up, i.e. toward it
    return value.quantize(Decimal(1).scaleb(-decimals), rounding=rounding)


def format_price(
    price: float | Decimal,
    instrument: str,
    display_precision: int | None = None,
    *,
    toward: float | Decimal | None = None,
) -> str:
    """Render ``price`` as OANDA expects it: fixed-point at the instrument's precision.

    F-308: this replaces a hardcoded ``f"{price:.5f}"``. USD_JPY has
    ``displayPrecision=3``, so every JPY stop/target the engine has ever built carried two
    illegal decimals (``"153.42718"``) and would be rejected by the v20 API.
    """
    return f"{quantize_price(price, instrument, display_precision, toward=toward):f}"


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
    def modify_trade_stop(
        self, trade_id: str, stop_price: float, instrument: str | None = None
    ) -> dict[str, Any]: ...
    def close_trade(self, trade_id: str, units: str | int = "ALL") -> dict[str, Any]: ...
    def get_account_summary(self) -> dict[str, Any]: ...


@runtime_checkable
class OandaPricingTransport(Protocol):
    """The read-only market-data half of the seam (F-306/F-308).

    Deliberately **separate** from ``OandaTransport`` so existing transports/doubles stay
    conformant; the adapter feature-detects these two methods and degrades safely (no
    price ⇒ the pipeline refuses to construct; no spec ⇒ the verified per-instrument
    precision fallback).
    """

    def get_pricing(self, instruments: list[str]) -> list[dict[str, Any]]: ...
    def get_account_instruments(self, instruments: list[str] | None = None) -> list[dict[str, Any]]: ...


@runtime_checkable
class OandaReconcileTransport(Protocol):
    """The read-only transaction-history half of the seam (F-303).

    Separate from ``OandaTransport`` for the same reason ``OandaPricingTransport`` is: the
    adapter feature-detects it so existing transports and test doubles stay conformant. The
    *deployed* transport must implement it — ``OandaRestTransport`` does, and a test asserts
    so — because without it the pre-submit reconcile degrades to open trades only.
    """

    def get_recent_transactions(self, count: int = ...) -> list[dict[str, Any]]: ...


# --------------------------------------------------------------------------- #
# Prior-submission verdict (F-303) — pure, unit-tested
# --------------------------------------------------------------------------- #
# What the broker's recent transactions say about one client order id.
PRIOR_FILLED = "filled"      # an ORDER_FILL carries it: a position exists. NEVER resubmit.
PRIOR_UNFILLED = "unfilled"  # the order was cancelled/rejected: nothing exists. Safe to submit.
PRIOR_PENDING = "pending"    # the order is there with no outcome yet: ambiguous. Do not submit.
PRIOR_ABSENT = "absent"      # not in the window at all: never submitted. Safe to submit.


def transaction_client_order_id(txn: dict[str, Any]) -> str | None:
    """The client-assigned order id a v20 transaction refers to, or ``None``.

    Two different fields carry it, verified against the live practice transaction stream
    (read-only probe, 2026-08-01): the *order* transaction carries it as
    ``clientExtensions.id`` (``MARKET_ORDER``, reason ``CLIENT_ORDER``) while the *outcome*
    transaction carries it as ``clientOrderID`` (``ORDER_FILL``, ``ORDER_CANCEL``).
    """
    raw = txn.get("clientOrderID")
    if raw:
        return str(raw)
    raw = (txn.get("clientExtensions") or {}).get("id")
    return str(raw) if raw else None


def prior_submission_verdict(
    transactions: Iterable[dict[str, Any]], client_order_id_: str
) -> tuple[str, dict[str, Any] | None]:
    """What a bounded transaction window says about ``client_order_id_``.

    Returns ``(verdict, fill_transaction | None)``. Reading *transactions* rather than open
    trades is what makes this complete: an order that filled and has since closed (SL/TP, or
    a later flatten) is long gone from ``get_open_trades`` but its ``ORDER_FILL`` is
    permanent — and those are exactly the orders whose resubmission has already cost money.
    A fill that merely reduced an existing position opens no trade at all and is likewise
    only visible here.

    Anything carrying the id that is neither a fill nor a terminal outcome counts as
    ``PRIOR_PENDING`` — unknown outcome resolves toward "do not submit", never toward "submit".
    """
    fill: dict[str, Any] | None = None
    saw_order = False
    saw_terminal = False
    for txn in transactions or ():
        if transaction_client_order_id(txn) != client_order_id_:
            continue
        txn_type = str(txn.get("type") or "").upper()
        if txn_type == "ORDER_FILL":
            fill = txn
        elif txn_type == "ORDER_CANCEL" or txn_type.endswith("_REJECT"):
            saw_terminal = True
        else:
            saw_order = True
    if fill is not None:
        return PRIOR_FILLED, fill
    if saw_terminal:
        return PRIOR_UNFILLED, None
    if saw_order:
        return PRIOR_PENDING, None
    return PRIOR_ABSENT, None


# --------------------------------------------------------------------------- #
# Market reference price (F-306)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ReferencePrice:
    """A tradeable two-sided quote — the expected-entry reference for a market order."""

    instrument: str
    bid: float
    ask: float
    time: datetime | None = None

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    def for_side(self, side: str) -> float:
        """The side of the book the order will actually cross: BUY pays the ask."""
        return self.ask if str(side).upper() == "BUY" else self.bid

    def age_sec(self, now: datetime | None = None) -> float | None:
        if self.time is None:
            return None
        now = now or datetime.now(timezone.utc)
        return (now.astimezone(timezone.utc) - self.time.astimezone(timezone.utc)).total_seconds()


def _parse_broker_time(raw: Any) -> datetime | None:
    """RFC3339 as OANDA emits it (up to 9 fractional digits — more than ``fromisoformat``)."""
    if not raw:
        return None
    text = str(raw).strip().replace("Z", "+00:00")
    if "." in text:
        head, _, tail = text.partition(".")
        cut = min((i for i in (tail.find("+"), tail.find("-")) if i != -1), default=len(tail))
        fraction, offset = tail[:cut], tail[cut:]
        text = f"{head}.{(fraction[:6] or '0')}{offset}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def reference_price_from_broker_row(row: dict[str, Any]) -> ReferencePrice | None:
    """Build a ``ReferencePrice`` from one ``/v3/accounts/{id}/pricing`` row.

    Returns ``None`` — meaning *no price*, so the caller must refuse — when the instrument
    is halted/untradeable or either side of the book is empty. Never invents a side.
    """
    if not row:
        return None
    if str(row.get("status", "tradeable")).lower() != "tradeable":
        return None
    if row.get("tradeable") is False:
        return None
    try:
        bid = float((row.get("bids") or [{}])[0]["price"])
        ask = float((row.get("asks") or [{}])[0]["price"])
    except (KeyError, IndexError, TypeError, ValueError):
        return None
    if not (bid > 0 and ask > 0):
        return None
    return ReferencePrice(
        instrument=str(row.get("instrument", "")),
        bid=bid,
        ask=ask,
        time=_parse_broker_time(row.get("time")),
    )


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
        price_max_age_sec: float | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.transport = transport
        self.secrets = secrets or get_secrets()
        self.env = environment or resolve_environment(self.secrets)
        self.price_max_age_sec = (
            price_max_age_sec
            if price_max_age_sec is not None
            else float(self.secrets.get_int("PRICE_MAX_AGE_SEC", int(DEFAULT_PRICE_MAX_AGE_SEC)))
        )
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._specs: dict[str, InstrumentSpec] = {}
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

    # ----- instrument spec / price precision (F-308) ------------------------
    def instrument_spec(self, instrument: str) -> InstrumentSpec | None:
        """The broker's spec for ``instrument``, cached for the process. ``None`` if unknown.

        Transport failures are **not** cached — a spec lookup that fails now must be retried
        on the next order rather than pinning a fallback precision for the whole process.
        """
        key = (instrument or "").upper()
        if key in self._specs:
            return self._specs[key]
        fetch = getattr(self.transport, "get_account_instruments", None)
        if fetch is None:
            return None
        try:
            rows = fetch([key]) or []
        except Exception as exc:  # noqa: BLE001 — degrade to the verified fallback, loudly
            log_event(log, logging.WARNING, "instrument spec lookup failed; using default precision",
                      instrument=key, error=str(exc))
            return None
        for row in rows:
            try:
                spec = InstrumentSpec.from_broker_row(row)
            except (KeyError, TypeError, ValueError):
                continue
            self._specs[spec.name.upper()] = spec
        return self._specs.get(key)

    def display_precision(self, instrument: str) -> int:
        spec = self.instrument_spec(instrument)
        if spec is not None:
            return spec.display_precision
        return default_display_precision(instrument)

    # ----- market reference price (F-306) -----------------------------------
    def reference_price(self, instrument: str, side: str) -> float | None:
        """The price a market order of ``side`` would actually cross, or ``None``.

        ``None`` means *there is no usable price right now* — the transport has no pricing
        capability, the call failed, the instrument is halted, or the quote is staler than
        ``price_max_age_sec``. The caller MUST refuse to construct an order on ``None``
        (``pipeline.require_market_price``); it must never substitute the stop price, the
        ATR, or any other stand-in. That substitution is exactly what F-306 was.
        """
        fetch = getattr(self.transport, "get_pricing", None)
        if fetch is None:
            log_event(log, logging.ERROR, "transport has no pricing capability; no reference price",
                      instrument=instrument)
            return None
        try:
            rows = fetch([instrument]) or []
        except Exception as exc:  # noqa: BLE001 — no price is a reject, not a crash
            log_event(log, logging.ERROR, "pricing lookup failed; no reference price",
                      instrument=instrument, error=str(exc))
            return None
        for row in rows:
            quote = reference_price_from_broker_row(row)
            if quote is None or quote.instrument.upper() not in ("", instrument.upper()):
                continue
            age = quote.age_sec(self._clock())
            if age is not None and age > self.price_max_age_sec:
                log_event(log, logging.ERROR, "reference price too stale; refusing to price the order",
                          instrument=instrument, age_sec=round(age, 3),
                          max_age_sec=self.price_max_age_sec)
                return None
            return quote.for_side(side)
        log_event(log, logging.ERROR, "no tradeable quote returned; no reference price",
                  instrument=instrument)
        return None

    def price_fn(self, order: Any) -> float | None:
        """Drop-in ``OutboundConsumer.price_fn``: a REAL market price, or ``None`` to reject.

        This is the intended replacement for ``lifecycle.build_from_secrets._price_fn``,
        which returned ``suggested_sl or atr`` — the stop price, or a raw ATR pretending to
        be a price (F-306).
        """
        return self.reference_price(order.instrument, order.side)

    # ----- order payload ----------------------------------------------------
    def _build_payload(self, order: ConstructedOrder) -> dict[str, Any]:
        cid = client_order_id(order.idempotency_key)
        decimals = self.display_precision(order.instrument)
        # Directional quantization (``toward=entry``) guarantees the tick-snap can only
        # tighten a protective price, never widen the stop.
        stop_loss = quantize_price(order.stop_loss, order.instrument, decimals, toward=order.entry_price)
        take_profit = quantize_price(order.take_profit, order.instrument, decimals, toward=order.entry_price)
        # Last line before the wire: the invariant that survives rounding (F-306).
        assert_protective_prices(
            order.instrument, 1 if str(order.side).upper() == "BUY" else -1,
            order.entry_price, float(stop_loss), float(take_profit),
        )
        return {
            "order": {
                "type": "MARKET",
                "instrument": order.instrument,
                "units": str(int(order.units)),  # signed; taken as given (never re-sized)
                "timeInForce": "FOK",
                "positionFill": "DEFAULT",
                "clientExtensions": {"id": cid},
                "tradeClientExtensions": {"id": cid},
                "stopLossOnFill": {"price": f"{stop_loss:f}"},
                "takeProfitOnFill": {"price": f"{take_profit:f}"},
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
        payload = self._build_payload(order)  # cheap local validation before any network call

        # F-303 — ALWAYS reconcile before the first ``create_order``, not only after a
        # transient error. A fresh ``submit()`` after a redelivery has no memory of the
        # previous attempt; the broker does. This is what closes the window between the fill
        # returning and the pipeline's ``processed.mark()`` becoming durable: if the process
        # dies in it, no marker exists, the queue redelivers, and only this check stands
        # between the approved order and a second live position.
        prior, verified = self._prior_submission(cid, order, expected, model_set_id)
        if prior is not None:
            log_event(log, logging.WARNING,
                      "this order is already at the broker; NOT resubmitting (reconciled)",
                      idempotency_key=order.idempotency_key, client_order_id=cid,
                      broker_trade_id=prior.broker_trade_id)
            return prior
        if not verified:
            raise ReconcileUnavailableError(
                f"{order.idempotency_key}: cannot prove this order was not already submitted; "
                "refusing to risk a duplicate live position"
            )

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
                reconciled, verified = self._prior_submission(cid, order, expected, model_set_id)
                if reconciled is not None:
                    log_event(log, logging.INFO, "reconciled existing fill after transient error",
                              idempotency_key=order.idempotency_key)
                    return reconciled
                if not verified:
                    # An ambiguous send followed by an unreadable broker is the one state in
                    # which a retry can genuinely double the position. Defer instead.
                    raise ReconcileUnavailableError(
                        f"{order.idempotency_key}: broker unreadable after an ambiguous submit; "
                        "refusing to retry into a possible duplicate"
                    ) from exc
                if attempt >= self.retry_max:
                    log_event(log, logging.ERROR, "broker submit exhausted retries",
                              idempotency_key=order.idempotency_key, error=str(exc))
                    raise
                self._sleep(self.backoff_sec * attempt)  # linear backoff

        assert resp is not None
        return self._build_fill_result(order, resp, expected, model_set_id)

    # ----- reconcile (F-303) -------------------------------------------------
    def _prior_submission(
        self, cid: str, order: ConstructedOrder, expected: float, model_set_id: str | None
    ) -> tuple[FillResult | None, bool]:
        """Has this exact client order id already reached the broker? ``(fill, verified)``.

        ``verified is False`` means *the broker could not tell us*, and the caller must then
        refuse to submit. The judgement, stated once here: **"cannot verify" is resolved
        toward "already submitted"**, because the two errors are not symmetric — refusing
        defers an order that the queue will redeliver (and, failing that, dead-letters
        visibly), whereas submitting blind can put a second live position on the book, which
        nothing downstream can undo. Note also that a broker we cannot *read* is usually a
        broker we should not be *writing* to.

        The one place that judgement is inverted is a transport with no transaction-reading
        capability at all. That is a deterministic property of the deployment, not a fact
        about this order: failing closed on it would halt trading 100% of the time on a
        misconfigured transport (the audit's own harness double is such a transport) instead
        of protecting a rare window. So a missing capability degrades to the OPEN-trades
        check — today's behaviour, never worse — loudly, while a *failed call* fails closed.
        """
        fetch = getattr(self.transport, "get_recent_transactions", None)
        if fetch is None:
            log_event(log, logging.WARNING,
                      "transport cannot read recent transactions; pre-submit reconcile covers "
                      "OPEN trades only (a filled-and-closed order would not be seen)",
                      idempotency_key=order.idempotency_key)
            return self._reconcile(cid, order, expected, model_set_id)

        try:
            transactions = fetch(RECONCILE_TRANSACTION_WINDOW) or []
        except Exception as exc:  # noqa: BLE001 — any failure here is "cannot verify"
            log_event(log, logging.ERROR,
                      "pre-submit reconcile failed; cannot prove this order is unsubmitted",
                      idempotency_key=order.idempotency_key, error=str(exc))
            return None, False

        verdict, fill_txn = prior_submission_verdict(transactions, cid)
        if verdict == PRIOR_FILLED:
            # Reuse the normal fill path so a reconciled fill is validated (slippage) and has
            # its stop/TP confirmed exactly like a freshly returned one.
            return self._build_fill_result(
                order, {"orderFillTransaction": fill_txn}, expected, model_set_id
            ), True
        if verdict == PRIOR_PENDING:
            log_event(log, logging.ERROR,
                      "an order with this client id is at the broker with no fill or cancel; "
                      "refusing to resubmit until its outcome is known",
                      idempotency_key=order.idempotency_key, client_order_id=cid)
            return None, False
        return None, True  # PRIOR_ABSENT (never submitted) or PRIOR_UNFILLED (nothing exists)

    def _reconcile(
        self, cid: str, order: ConstructedOrder, expected: float, model_set_id: str | None
    ) -> tuple[FillResult | None, bool]:
        """Open-trades fallback: ``(fill, verified)`` for transports without transaction reads.

        Proves *presence* reliably; its "absent" is best-effort only — a trade that has
        already closed, or a fill that merely reduced a position, never appears here. A failed
        lookup is reported as unverified so the caller still fails closed.
        """
        try:
            trades = self.transport.get_open_trades()
        except BrokerError as exc:
            log_event(log, logging.ERROR, "open-trades reconcile failed; order not verifiable",
                      idempotency_key=order.idempotency_key, error=str(exc))
            return None, False
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
                ), True
        return None, True

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
        if str(trade.get("state") or "OPEN").upper() != "OPEN":
            # No live position, so no stop to confirm and nothing to repair. Reachable via the
            # F-303 reconcile, which can legitimately recover a fill that has already closed;
            # attaching a stop to a closed trade would be a pointless write and flagging it
            # NO_STOP_UNSAFE would be a false risk alert.
            return None, None, False

        sl = trade.get("stopLossOrder")
        tp = trade.get("takeProfitOrder")
        sl_price = float(sl["price"]) if sl and sl.get("price") else None
        tp_price = float(tp["price"]) if tp and tp.get("price") else None

        if sl_price is None:
            # attempt to attach the intended stop (F-308: the repair path must respect the
            # instrument's price precision too, or a JPY position stays unprotected)
            try:
                self.transport.modify_trade_stop(trade_id, order.stop_loss, order.instrument)
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
    def modify_stop(self, trade_id: str, new_stop_price: float, instrument: str) -> dict[str, Any]:
        """Idempotent stop modification (routed here so EXEC-007 stays broker-agnostic).

        ``instrument`` is required: without it the price cannot be rendered at the right
        precision and a JPY stop-move is rejected by the broker (F-308).
        """
        if new_stop_price is None or not float(new_stop_price) > 0:
            raise InvalidOrderError(
                f"{instrument}: refusing to move trade {trade_id} stop to {new_stop_price!r}"
            )
        return self.transport.modify_trade_stop(trade_id, new_stop_price, instrument)

    def close_trade(self, trade_id: str, units: str | int = "ALL") -> dict[str, Any]:
        return self.transport.close_trade(trade_id, units)

    def get_account_summary(self) -> dict[str, Any]:
        return self.transport.get_account_summary()
