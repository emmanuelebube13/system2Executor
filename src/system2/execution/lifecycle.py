"""EXEC-010 — emergency STOP, graceful position-safe lifecycle, and the runtime assembly root.

This is the final glue: it wires the deterministic pipeline (EXEC-003) to the queue consumer
(EXEC-004), fill producer (EXEC-005), broker adapter (EXEC-006), position manager (EXEC-007),
safety monitor (EXEC-008), and health surface (EXEC-009) into one running service, and gives
the operator the two things they must always have:

  * **Emergency STOP — always reachable.** A process signal (SIGTERM/SIGINT), an in-process
    ``trigger()``, OR a filesystem sentinel (``state/control/STOP``) any of which halts new
    trading immediately. Reachable even if the queue/network is down (touch the file).
  * **Graceful, position-safe lifecycle.** On stop the runtime finishes the current tick,
    flushes the fill outbox (never drop a fill), emits a STOPPED control event, and — by
    default — LEAVES open positions in place because every one already carries a broker-side
    stop (EXEC-006 confirmed), so exiting never orphans risk. Flattening on stop is an explicit
    opt-in. On start it reconciles the broker's open trades into the position manager so a
    restart never leaves a live position unmanaged.

The safety gate is honoured here: each tick evaluates staleness (EXEC-008) and the consumer's
``submit_gate_fn`` is ``SafetyMonitor.can_submit`` — when PAUSED, new orders are parked on the
queue (not lost, not submitted) while position management keeps protecting existing risk.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from system2.common.logging import get_logger, log_event

log = get_logger("execution.lifecycle")


# --------------------------------------------------------------------------- #
# Emergency STOP — always reachable
# --------------------------------------------------------------------------- #
class EmergencyStop:
    """A latch that is set by a signal, an in-process call, or a filesystem sentinel."""

    def __init__(self, sentinel_path: str | Path | None = None) -> None:
        self._event = threading.Event()
        self._reason: str | None = None
        self.sentinel_path = Path(sentinel_path) if sentinel_path else None

    def trigger(self, reason: str = "manual") -> None:
        if not self._event.is_set():
            self._reason = reason
            self._event.set()
            log_event(log, logging.CRITICAL, "EMERGENCY STOP triggered", reason=reason)

    def is_stopped(self) -> bool:
        if self._event.is_set():
            return True
        if self.sentinel_path is not None and self.sentinel_path.exists():
            self.trigger(f"sentinel:{self.sentinel_path}")
            return True
        return False

    @property
    def reason(self) -> str | None:
        return self._reason

    def install_signal_handlers(self) -> None:
        """Bind SIGTERM/SIGINT to STOP (best-effort; no-op off the main thread)."""
        def _handler(signum, _frame):  # pragma: no cover - exercised via integration only
            self.trigger(f"signal:{signal.Signals(signum).name}")

        try:
            signal.signal(signal.SIGTERM, _handler)
            signal.signal(signal.SIGINT, _handler)
        except (ValueError, RuntimeError):
            # not on the main thread (e.g. under a test runner) — sentinel/trigger still work
            log_event(log, logging.WARNING, "signal handlers not installed (non-main thread)")


# --------------------------------------------------------------------------- #
# Runtime assembly root
# --------------------------------------------------------------------------- #
@dataclass
class ExecutionRuntime:
    """Owns the tick loop, the safety gate, startup reconcile, and graceful shutdown.

    Components are injected (built by ``build_from_secrets`` in production) so the loop is
    unit-tested with fakes.
    """

    consumer: Any                 # OutboundConsumer (submit_gate_fn should be monitor.can_submit)
    monitor: Any                  # SafetyMonitor
    position_manager: Any         # PositionManager
    producer: Any                 # FillProducer (outbox flush + control events)
    emergency_stop: EmergencyStop
    reporter: Any = None          # HealthReporter (optional)
    adapter: Any = None           # OandaAdapter (for startup reconcile + flatten-on-stop)
    regime_scheduler: Any = None  # RegimeScheduler (optional; fail-open, never blocks trading)
    close_sweeper: Any = None     # CloseSweeper (EXEC-012; optional, fail-open, self-throttled)
    close_emitter: Any = None     # CloseEmitter (EXEC-012; close events for flatten-on-stop)
    price_source_fn: Callable[[], dict[str, float]] | None = None
    reconcile_fn: Callable[[dict[str, Any]], Any] | None = None  # broker trade -> ManagedTrade
    flatten_on_stop: bool = False
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))
    _shutdown_done: bool = False

    # ----- one control tick -------------------------------------------------
    def tick(self) -> dict[str, Any]:
        """One control iteration: evaluate safety → consume (gated) → manage open positions."""
        if self.emergency_stop.is_stopped():
            return {"stopped": True, "reason": self.emergency_stop.reason}

        now = self.clock()
        # 1) safety state from queue freshness (one-tick lag is fine)
        state = self.monitor.evaluate(self.consumer.lag.last_message_at, now)
        # 2) consume + execute; the consumer's submit_gate_fn parks NEW orders while PAUSED
        consume_stats = self.consumer.poll_once()
        # 3) manage EXISTING positions always — protecting live risk is safe even when paused
        managed: dict[str, Any] = {}
        if self.price_source_fn is not None:
            prices = self.price_source_fn() or {}
            if prices:
                managed = self.position_manager.evaluate_all(prices)
        # 4) broker-close sweep (EXEC-012) — throttled internally; NEVER blocks the tick
        if self.close_sweeper is not None:
            try:
                self.close_sweeper.sweep()
            except Exception as exc:  # fail-open: log, retry next tick
                log_event(log, logging.ERROR, "close sweep failed (will retry next tick)",
                          error=str(exc))
        return {"stopped": False, "state": getattr(state, "value", state),
                "consume": consume_stats, "managed": managed}

    # ----- startup reconcile ------------------------------------------------
    def startup_reconcile(self) -> int:
        """Adopt the broker's currently-open trades so a restart never orphans management."""
        if self.adapter is None or self.reconcile_fn is None:
            return 0
        try:
            trades = self.adapter.transport.get_open_trades()
        except Exception as exc:
            log_event(log, logging.ERROR, "startup reconcile failed to read open trades", error=str(exc))
            return 0
        adopted = 0
        for t in trades:
            try:
                managed = self.reconcile_fn(t)
            except Exception as exc:
                log_event(log, logging.WARNING, "skipping unreconcilable open trade",
                          trade=str(t.get("id")), error=str(exc))
                continue
            if managed is not None:
                self.position_manager.register(managed)
                adopted += 1
        log_event(log, logging.INFO, "startup reconcile complete", adopted=adopted)
        return adopted

    # ----- run loop ---------------------------------------------------------
    def run(self, max_ticks: int | None = None, idle_sleep_sec: float = 1.0) -> None:
        """Long-running loop until emergency STOP (or ``max_ticks`` for tests)."""
        self.emergency_stop.install_signal_handlers()
        self.startup_reconcile()
        if self.regime_scheduler is not None:
            try:
                self.regime_scheduler.start()
            except Exception as exc:  # regime is best-effort; never block trading start
                log_event(log, logging.ERROR, "regime scheduler failed to start", error=str(exc))
        log_event(log, logging.INFO, "execution runtime started")
        n = 0
        while not self.emergency_stop.is_stopped():
            if max_ticks is not None and n >= max_ticks:
                break
            res = self.tick()
            n += 1
            if not res.get("consume"):
                time.sleep(idle_sleep_sec)
        self.shutdown(self.emergency_stop.reason or "loop_exit")

    # ----- graceful, position-safe shutdown ---------------------------------
    def shutdown(self, reason: str = "shutdown") -> dict[str, Any]:
        """Flush fills, optionally flatten, emit STOPPED. Idempotent (safe to call once)."""
        if self._shutdown_done:
            return {"already": True}
        self._shutdown_done = True
        log_event(log, logging.WARNING, "graceful shutdown starting", reason=reason)

        if self.regime_scheduler is not None:
            try:
                self.regime_scheduler.stop()
            except Exception:  # best-effort; shutdown must not be blocked by telemetry
                pass

        flushed = 0
        try:
            flushed = self.producer.flush()  # never drop a recorded fill
        except Exception as exc:
            log_event(log, logging.ERROR, "outbox flush during shutdown failed", error=str(exc))

        flattened = 0
        if self.flatten_on_stop and self.adapter is not None:
            for tid, trade in list(getattr(self.position_manager, "trades", {}).items()):
                if getattr(trade, "closed", False):
                    continue
                try:
                    resp = self.adapter.close_trade(tid)
                    trade.closed = True
                    flattened += 1
                except Exception as exc:
                    log_event(log, logging.ERROR, "flatten-on-stop failed for trade",
                              trade_id=tid, error=str(exc))
                    continue
                if self.close_emitter is not None:  # EXEC-012: report the close to System 3
                    try:
                        self.close_emitter.emit_broker_close(trade, resp, "flatten")
                    except Exception as exc:  # never let emission block shutdown
                        log_event(log, logging.ERROR, "flatten close emission failed",
                                  trade_id=tid, error=str(exc))
        else:
            # SAFE DEFAULT: leave positions — each already carries a broker-side stop (EXEC-006).
            open_count = sum(
                1 for t in getattr(self.position_manager, "trades", {}).values()
                if not getattr(t, "closed", False)
            )
            if open_count:
                log_event(log, logging.INFO,
                          "leaving open positions with broker-side stops (no orphaned risk)",
                          open_positions=open_count)

        try:
            self.producer.emit_control("STOPPED", {"reason": reason, "flattened": flattened})
        except Exception as exc:
            log_event(log, logging.ERROR, "STOPPED control event emit failed", error=str(exc))

        log_event(log, logging.WARNING, "graceful shutdown complete",
                  reason=reason, fills_flushed=flushed, flattened=flattened)
        return {"reason": reason, "fills_flushed": flushed, "flattened": flattened}


# --------------------------------------------------------------------------- #
# Production assembly (wires the real components from config; needs live secrets)
# --------------------------------------------------------------------------- #
def build_from_secrets(secrets: Any | None = None) -> ExecutionRuntime:
    """Construct a fully-wired runtime from config. Fail-closed on missing secrets.

    Not exercised by unit tests (requires OANDA creds) — the loop logic is tested via injected
    fakes. This is the composition root used by the service entrypoint.
    """
    from system2.broker.oanda_adapter import OandaAdapter
    from system2.broker.oanda_transport import build_transport
    from system2.broker.position_manager import PositionManager
    from system2.common.queue_backend import build_queue
    from system2.common.secrets import get_secrets
    from system2.execution.close_tracker import (
        CloseEmitter,
        CloseSweeper,
        managed_trade_from_broker_trade,
        managed_trade_from_fill,
    )
    from system2.execution.fill_producer import FillProducer, build_outbox
    from system2.execution.outbound_consumer import OutboundConsumer, SqliteProcessedStore
    from system2.execution.pipeline import ExecMode, ExecutionPipeline
    from system2.execution.safety_mode import SafetyConfig, SafetyMonitor
    from system2.execution.validation import OrderValidator
    from system2.telemetry.health import HealthReporter

    secrets = secrets or get_secrets()

    queue = build_queue(secrets)
    transport = build_transport(secrets)
    adapter = OandaAdapter(transport, secrets=secrets)

    outbox = build_outbox(secrets.get("FILL_OUTBOX_PATH", "state/queue/fill_outbox.db"))
    producer = FillProducer(queue, secrets.require("INBOUND_QUEUE_NAME"), outbox,
                            retry_max=secrets.get_int("FILL_PUBLISH_RETRY_MAX", 5))

    # Optional local Fact_Live_Trades recorder (persist-then-publish). Best-effort: if the
    # datastore is unavailable we still trade + deliver fills via the outbox.
    persist_fn = None
    if secrets.get_bool("DB_RECORD_ENABLED", True):
        try:
            from system2.common.db import DbConfig, connect
            from system2.execution.trade_recorder import TradeRecorder

            db_cfg = DbConfig.from_secrets(secrets)
            recorder = TradeRecorder(connect(db_cfg), db_cfg.provider)
            persist_fn = recorder.record
        except Exception as exc:  # never let DB setup block startup of the trading path
            log_event(log, logging.ERROR, "trade recorder disabled (datastore setup failed)", error=str(exc))

    processed = SqliteProcessedStore(secrets.get("PROCESSED_STORE_PATH", "state/offsets/processed.db"))
    pipeline = ExecutionPipeline(mode=ExecMode.EXECUTION_ONLY,
                                 shadow=secrets.get_bool("EXEC_SHADOW", True),  # SHADOW until cutover
                                 processed_store=processed, secrets=secrets)
    monitor = SafetyMonitor(config=SafetyConfig.from_secrets(secrets))

    # Trade-close tracking (EXEC-012): ONE emission path for time-exit / flatten / broker
    # (SL/TP) closes. The flat close FillEvent is published DIRECTLY to the S3-side topic —
    # the bridge cannot translate closes (SnapshotRelay precedent; shared local queue DB).
    close_ledger = SqliteProcessedStore(
        secrets.get("CLOSE_LEDGER_PATH", "state/offsets/close_sweep.db"))
    close_emitter = CloseEmitter(
        queue=queue, outbox=outbox,
        topic=secrets.get("S3_CLOSE_TOPIC", "ams-inbound.ams"),
        ledger=close_ledger,
    )
    position_manager = PositionManager(adapter=adapter,
                                       emit_close_fn=close_emitter.emit_broker_close)
    close_sweeper = CloseSweeper(
        position_manager=position_manager, transport=transport, emitter=close_emitter,
        interval_sec=float(secrets.get_int("CLOSE_SWEEP_INTERVAL_SEC", 30)),
    )
    validator = OrderValidator.from_secrets(secrets)

    # F-306: the entry reference now comes from the BROKER's book (adapter.price_fn), not from
    # the order. What stood here returned `suggested_sl or atr` — the STOP price, or a raw ATR
    # (a distance) masquerading as a price. Since the bridge always populates suggested_sl,
    # production's "entry price" was literally the stop price, so entry == SL; and on the
    # fallback path SL = atr - 1.0*atr = 0.0, which is the SL=0.0 the audit harness reproduced.
    # adapter.price_fn returns None when there is no usable quote, and the pipeline REFUSES
    # rather than substituting a stand-in. Do not reintroduce a fallback here.

    def _emit_fill(order: Any, constructed: Any, fill: Any) -> None:
        # publish the entry fill FIRST (unchanged EXEC-005 path), then register the trade
        # with the position manager so its close is tracked (EXEC-012 register-on-fill).
        producer.publish_fill(order, fill)
        try:
            managed = managed_trade_from_fill(order, constructed, fill)
            if managed is not None:
                position_manager.register(managed)
        except Exception as exc:  # fail-open: close tracking must never block the fill path
            log_event(log, logging.ERROR, "register-on-fill failed (close tracking degraded)",
                      idempotency_key=order.idempotency_key, error=str(exc))

    consumer = OutboundConsumer(
        queue=queue,
        subscription=secrets.require("OUTBOUND_QUEUE_NAME"),
        pipeline=pipeline,
        price_fn=adapter.price_fn,
        submit_fn=adapter.submit,
        persist_fn=persist_fn,
        emit_fn=_emit_fill,
        validate_fn=lambda c: (lambda r: (r.ok, r.reason))(validator.validate(c)),
        submit_gate_fn=monitor.can_submit,
        heartbeat_fn=monitor.record_heartbeat,  # EXEC-008: S3 keepalive -> freshness (F-305)
        open_instruments_fn=lambda: [t.instrument for t in position_manager.trades.values()
                                     if not t.closed],
        max_age_sec=secrets.get_int("ORDER_MAX_AGE_SEC", 300),
    )

    # Live regime detector + scheduler (EXEC-002). Best-effort and fail-open: if the model
    # bundle or OANDA candle credentials are absent (e.g. dev), the engine trades normally
    # and the regime grid is simply empty — it never blocks or crashes the trading path.
    regime_scheduler = None
    if secrets.get_bool("REGIME_ENABLED", True):
        try:
            from system2.artifact_sync.candles import OandaCandleSource
            from system2.artifact_sync.live_regime import LiveRegimeDetector
            from system2.artifact_sync.regime_scheduler import RegimeScheduler

            artifact_root = Path(secrets.require("ARTIFACT_ROOT"))
            detector = LiveRegimeDetector(artifact_root, OandaCandleSource(secrets), secrets=secrets)
            regime_scheduler = RegimeScheduler.from_secrets(detector, secrets)
        except Exception as exc:  # never let regime setup block startup of the trading path
            log_event(log, logging.ERROR, "regime detector disabled (setup failed)", error=str(exc))

    # EXEC-011's live scored-signal producer used to be built here. It was DELETED on
    # 2026-08-15 (S1-NOTICE-2026-08-15 §4.3), not disabled: it fabricated an order's
    # direction from the regime label — Trending-Down ⇒ short, every polling cycle,
    # for every instrument — with no entry condition of any kind behind it. Correcting
    # its arithmetic would have produced correctly-signed orders for trades that have
    # no setup, which is worse, because it would look right.
    #
    # System 1 owns entry logic (S1-REPLY-2026-08-02b §2). System 2 is execution-only:
    # it acts on scored signals it receives and originates none. Do not reintroduce a
    # local signal source here; when there is something to transport, the schema
    # conversation happens first.

    # Active model set id for the health surface: the downloader's state.json is the
    # authority (works even when regime/signal components are disabled).
    def _active_model_set_id() -> str | None:
        try:
            import json as _json

            state_file = Path(secrets.require("ARTIFACT_ROOT")) / "state.json"
            return _json.loads(state_file.read_text(encoding="utf-8")).get("active_model_set_id")
        except Exception:
            return None

    # The gatekeeper's runtime approval-rate band (F-602, FIX_PLAN 2.1(d)) used to read
    # through the producer's approval monitor. With the producer deleted nothing in this
    # process evaluates signals, so there is no approval rate to report and the hook is
    # left unwired — the reporter renders that as an explicit "unavailable", which is the
    # truth, rather than a false all-clear. MODEL-006 rewires it when the gatekeeper is
    # rebuilt (S1-NOTICE-2026-08-15 §4.4: honest strategies first, gatekeeper second).

    reporter = HealthReporter(
        safety_state_fn=lambda: monitor.state.value,
        model_set_id_fn=_active_model_set_id,
        staleness_fn=lambda: monitor.staleness_seconds(monitor.clock(), consumer.lag.last_message_at),
        staleness_limit_fn=lambda: monitor.config.staleness_limit_sec,
        last_message_at_fn=lambda: consumer.lag.last_message_at,
        messages_seen_fn=lambda: consumer.lag.messages_seen,
        open_positions_fn=lambda: [{"trade_id": t.broker_trade_id, "instrument": t.instrument}
                                   for t in position_manager.trades.values() if not t.closed],
        outbox_depth_fn=lambda: outbox.depth("fill_outbox"),
        broker_env_fn=lambda: adapter.env.env,
        account_summary_fn=adapter.get_account_summary,
        regime_grid_fn=(regime_scheduler.grid if regime_scheduler is not None else None),
    )

    # EXEC-012: startup reconcile builds ManagedTrades via the SAME builder as
    # register-on-fill; signal/order identity is recovered from the durable fill outbox.
    fill_outbox_path = secrets.get("FILL_OUTBOX_PATH", "state/queue/fill_outbox.db")

    def _reconcile(t: dict[str, Any]) -> Any:
        return managed_trade_from_broker_trade(t, outbox_path=fill_outbox_path)

    stop = EmergencyStop(sentinel_path=secrets.get("STOP_SENTINEL_PATH", "state/control/STOP"))
    return ExecutionRuntime(
        consumer=consumer, monitor=monitor, position_manager=position_manager,
        producer=producer, emergency_stop=stop, reporter=reporter, adapter=adapter,
        regime_scheduler=regime_scheduler,
        close_sweeper=close_sweeper, close_emitter=close_emitter,
        reconcile_fn=_reconcile,
        flatten_on_stop=secrets.get_bool("EXEC_STOP_FLATTEN", False),
    )
