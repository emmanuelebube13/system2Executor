"""EXEC-007 — active position management (breakeven, trailing, time-based exits).

Once a trade is live, this manages it to close per the trade-management rules:

  * **Breakeven at 1R** — when unrealized profit reaches 1× the initial risk distance, move
    the stop to entry (± buffer), exactly once, one-way only.
  * **Trailing stop** — beyond breakeven, trail the stop in the favorable direction only
    (ATR- or R-based step); never loosen.
  * **Time-based exits** — at 50% / 75% / 100% of the trade's max duration apply staged
    action; 100% force-closes the remaining position (emitting an EXEC-005 confirmation).

The rule engine is pure and unit-tested; all broker mutations route through the EXEC-006
``OandaAdapter`` (idempotent, confirmed). On pricing-stream loss the caller falls back to
REST polling of open trades — a position is never left unmanaged silently. Stop moves are
idempotent no-ops when the stop is already at/tighter than target, to avoid churning the
broker (rate limits/cost).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from system2.common.logging import get_logger, log_event

log = get_logger("broker.position_manager")

# Guard the 1R breakeven threshold against float noise (a price exactly 1R away computes to
# ~0.99999R in IEEE-754). Without this, a trade sitting precisely at 1R would miss breakeven.
_R_EPS = 1e-9


# --------------------------------------------------------------------------- #
# Managed trade + pure rule engine
# --------------------------------------------------------------------------- #
@dataclass
class ManagedTrade:
    broker_trade_id: str
    instrument: str
    side: str  # "BUY" | "SELL"
    entry_price: float
    initial_stop_price: float
    take_profit_price: float
    open_time: datetime
    granularity: str  # "H1" | "H4"
    max_duration_sec: float
    correlation_id: str
    current_stop: float = field(default=0.0)
    at_breakeven: bool = False
    time_actions_done: frozenset[str] = field(default_factory=frozenset)
    closed: bool = False
    # EXEC-012: the originating order's idempotency_key (= S3's order_request_id), needed
    # to identify the trade in the close FillEvent. None for legacy/unattributable trades.
    order_request_id: str | None = None

    def __post_init__(self) -> None:
        if not self.current_stop:
            self.current_stop = self.initial_stop_price

    @property
    def direction(self) -> int:
        return 1 if self.side.upper() == "BUY" else -1

    @property
    def risk_distance(self) -> float:
        return abs(self.entry_price - self.initial_stop_price)


def favorable_move(trade: ManagedTrade, price: float) -> float:
    """Signed favorable price move from entry (positive = in profit)."""
    return (price - trade.entry_price) * trade.direction


def r_multiple(trade: ManagedTrade, price: float) -> float:
    """Unrealized profit in R (risk-distance multiples). 0 if risk_distance degenerate."""
    if trade.risk_distance <= 0:
        return 0.0
    return favorable_move(trade, price) / trade.risk_distance


def breakeven_stop(trade: ManagedTrade, buffer_pips: float, pip: float) -> float:
    """Entry ± a small favorable buffer (covers spread) so breakeven isn't stopped by noise."""
    return trade.entry_price + trade.direction * buffer_pips * pip


def trailing_stop(trade: ManagedTrade, best_price: float, trail_distance: float) -> float:
    """Stop trailing ``trail_distance`` behind the best favorable price."""
    return best_price - trade.direction * trail_distance


def is_tighter(trade: ManagedTrade, new_stop: float, current_stop: float) -> bool:
    """A tighter stop is closer to price in the favorable direction (protects more profit)."""
    if trade.direction == 1:  # long: higher stop is tighter
        return new_stop > current_stop
    return new_stop < current_stop  # short: lower stop is tighter


def time_fraction(trade: ManagedTrade, now: datetime) -> float:
    if trade.max_duration_sec <= 0:
        return 0.0
    elapsed = (now.astimezone(timezone.utc) - trade.open_time.astimezone(timezone.utc)).total_seconds()
    return elapsed / trade.max_duration_sec


# --------------------------------------------------------------------------- #
# Manager
# --------------------------------------------------------------------------- #
@dataclass
class PositionManager:
    """Applies the rule engine to open trades via the EXEC-006 adapter (idempotently)."""

    adapter: Any  # OandaAdapter (modify_stop / close_trade)
    # EXEC-012: (trade, close_trade response, exit_reason) — the response carries the
    # authoritative close facts (orderFillTransaction.{id,pl,price,time}).
    emit_close_fn: Callable[[ManagedTrade, dict[str, Any] | None, str], None] | None = None
    breakeven_r: float = 1.0
    breakeven_buffer_pips: float = 1.0
    trail_distance: float = 0.0  # in price units; 0 disables trailing
    time_exit_fractions: tuple[float, ...] = (0.5, 0.75, 1.0)
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))
    enabled: bool = True
    trades: dict[str, ManagedTrade] = field(default_factory=dict)

    def register(self, trade: ManagedTrade) -> None:
        self.trades[trade.broker_trade_id] = trade

    def _pip(self, instrument: str) -> float:
        from system2.broker.oanda_adapter import pip_size

        return pip_size(instrument)

    def on_tick(self, trade: ManagedTrade, price: float) -> list[str]:
        """Apply all rules to one trade at ``price``. Returns the list of actions taken."""
        actions: list[str] = []
        if not self.enabled or trade.closed:
            return actions

        # --- time-based exits first (100% force-close short-circuits) ---
        frac = time_fraction(trade, self.clock())
        if frac >= 1.0 and "t100" not in trade.time_actions_done:
            self._close(trade)
            trade.time_actions_done = trade.time_actions_done | {"t100"}
            actions.append("time_exit_close")
            return actions
        for f, tag in ((0.75, "t75"), (0.5, "t50")):
            if f in self.time_exit_fractions and frac >= f and tag not in trade.time_actions_done:
                trade.time_actions_done = trade.time_actions_done | {tag}
                actions.append(f"time_action_{int(f * 100)}pct")
                break

        # --- breakeven at 1R (once, forward only) ---
        if not trade.at_breakeven and r_multiple(trade, price) >= self.breakeven_r - _R_EPS:
            be = breakeven_stop(trade, self.breakeven_buffer_pips, self._pip(trade.instrument))
            if is_tighter(trade, be, trade.current_stop):
                self._set_stop(trade, be)
                actions.append("breakeven")
            trade.at_breakeven = True

        # --- trailing beyond breakeven (tighten only) ---
        if trade.at_breakeven and self.trail_distance > 0 and not trade.closed:
            new_stop = trailing_stop(trade, price, self.trail_distance)
            if is_tighter(trade, new_stop, trade.current_stop):
                self._set_stop(trade, new_stop)
                actions.append("trail")

        return actions

    def evaluate_all(self, prices: dict[str, float]) -> dict[str, list[str]]:
        """Apply rules to every registered open trade for which a price is available."""
        out: dict[str, list[str]] = {}
        for tid, trade in list(self.trades.items()):
            if trade.closed:
                continue
            price = prices.get(trade.instrument)
            if price is None:
                continue
            out[tid] = self.on_tick(trade, price)
        return out

    # ----- broker mutations (idempotent, via EXEC-006) ----------------------
    def _set_stop(self, trade: ManagedTrade, new_stop: float) -> None:
        # ``instrument`` is required by the adapter so the price is rendered at the
        # instrument's displayPrecision — a JPY stop-move is rejected otherwise (F-308).
        self.adapter.modify_stop(trade.broker_trade_id, new_stop, trade.instrument)
        trade.current_stop = new_stop
        log_event(log, logging.INFO, "stop modified",
                  trade_id=trade.broker_trade_id, new_stop=new_stop,
                  correlation_id=trade.correlation_id)

    def _close(self, trade: ManagedTrade, exit_reason: str = "expiry") -> None:
        resp = self.adapter.close_trade(trade.broker_trade_id)  # response = close facts (EXEC-012)
        trade.closed = True
        log_event(log, logging.INFO, "trade force-closed (max duration reached)",
                  trade_id=trade.broker_trade_id, correlation_id=trade.correlation_id)
        if self.emit_close_fn is not None:
            self.emit_close_fn(trade, resp, exit_reason)
