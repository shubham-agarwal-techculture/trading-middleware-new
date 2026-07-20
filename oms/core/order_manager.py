"""
OrderManager — the central OMS engine.

Architecture
------------
┌──────────────────────────────────────────────────────────────┐
│  Strategy Process A  ──PUSH──►  ZMQ PULL (port 5555)        │
│  Strategy Process B  ──PUSH──►                               │
│                                      ▼                       │
│                              Signal Receiver                 │
│                              (validate & enqueue)            │
│                                      ▼                       │
│                           asyncio.Queue (order_queue)        │
│                                      ▼                       │
│                          Order Workers (N concurrent)        │
│                          broker.place_order() / cancel / mod │
│                                      ▼                       │
│                    Broker REST API (XTS Interactive)         │
│                                                              │
│  Broker Socket.IO  ──inject_broker_event()──►                │
│                                      ▼                       │
│                            State Machine Update              │
│                            Position Tracker update           │
│                            File Storage persist              │
│                                      ▼                       │
│                         ZMQ PUB (port 5556)                  │
│                               │                              │
│        ┌──────────────────────┴──────────────────────┐      │
│  Strategy A SUB (topic=A)            Strategy B SUB (topic=B)│
└──────────────────────────────────────────────────────────────┘

ZMQ message protocol
--------------------
Incoming (PUSH → PULL): JSON string, fields defined by msg_type.
Outgoing (PUB): ``"{strategy_id} {json_payload}"`` — topic is strategy_id.

Msg types received:  PLACE_ORDER, CANCEL_ORDER, MODIFY_ORDER,
                     SQUAREOFF, CANCEL_ALL
Msg types published: ORDER_ACK, ORDER_OPEN, ORDER_PARTIAL,
                     ORDER_FILLED, ORDER_CANCELLED, ORDER_REJECTED,
                     ORDER_MODIFIED, ORDER_EXPIRED, ORDER_ERROR,
                     CANCEL_ACK, MODIFY_ACK, SQUAREOFF_ACK
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from oms.core.broker_events import BrokerEventProcessor
from oms.core.dispatcher import SignalDispatcher
from oms.core.order_book_sync import OrderBookSync
from oms.core.transport import ZmqTransport
from oms.core.worker import OrderWorker
from oms.models.order import Order, OrderStatus, TERMINAL_STATES, ACTIVE_STATES
from oms.models.response import OrderResponse, ResponseType
from oms.utils.logger import get_logger, get_strategy_logger
from oms.utils.timeutil import now_iso

log = get_logger(__name__)


class OrderManager:
    """
    Runs as a long-lived asyncio task set.  Call ``start()`` to begin
    processing, ``stop()`` for a graceful shutdown.
    """

    def __init__(self, config, broker, file_store, position_tracker) -> None:
        """
        Parameters
        ----------
        config          : OMSConfig
        broker          : AbstractBrokerAdapter
        file_store      : FileStore
        position_tracker: PositionTracker
        """
        self._cfg = config
        self._tz = getattr(config, "timezone", "Asia/Kolkata")
        self._broker = broker
        self._store = file_store
        self._positions = position_tracker

        # In-memory order book: oms_order_id → Order
        self._orders: Dict[str, Order] = {}
        # Reverse index: order_unique_identifier → oms_order_id
        self._uid_index: Dict[str, str] = {}
        # Reverse index: broker_order_id → oms_order_id
        self._broker_index: Dict[str, str] = {}

        # Processing queue
        self._order_queue: asyncio.Queue = asyncio.Queue(
            maxsize=self._cfg.max_queue_size
        )

        # Transport (ZMQ PULL ingress + PUB egress)
        self._transport = ZmqTransport(self._cfg.pull_address, self._cfg.pub_address)

        # Command-pattern dispatcher: msg_type -> handler
        self._dispatcher = SignalDispatcher(on_unknown=self._handle_unknown)
        self._dispatcher.register("PLACE_ORDER", self._handle_place_order)
        self._dispatcher.register("CANCEL_ORDER", self._handle_cancel_order)
        self._dispatcher.register("MODIFY_ORDER", self._handle_modify_order)
        self._dispatcher.register("SQUAREOFF", self._handle_squareoff)
        self._dispatcher.register("CANCEL_ALL", self._handle_cancel_all)

        # Broker-event fill inference (shared, side-effect-free)
        self._event_processor = BrokerEventProcessor()

        # Statistics
        self._stats: Dict[str, Any] = {}
        self._stats_lock = asyncio.Lock()

        # Shutdown flag
        self._running = False
        self._tasks: list = []

        # Serialize MODIFY per order (multiple workers + burst signals otherwise
        # race and XTS returns 400 on stale/concurrent replace requests).
        self._modify_locks: Dict[str, asyncio.Lock] = {}

        # Per-strategy loggers cache (strategy_id → logging.Logger)
        self._strategy_loggers: dict = {}

    def _stamp(self, resp: OrderResponse) -> OrderResponse:
        """Attach OMS publish timestamp to responses built outside _build_response."""
        resp.timestamp = now_iso(self._tz)
        return resp

    def _slog(self, strategy_id: str) -> "logging.Logger":
        """Return the dedicated file logger for *strategy_id*, creating it on first use."""
        import logging as _logging
        if strategy_id not in self._strategy_loggers:
            log_dir = getattr(getattr(self._cfg, "logging", None), "log_dir", "./logs")
            self._strategy_loggers[strategy_id] = get_strategy_logger(strategy_id, log_dir)
        return self._strategy_loggers[strategy_id]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Bind sockets, restore state, launch all background tasks."""
        # Restore previous state
        await self._restore_state()

        # Bind ZMQ sockets
        self._transport.bind()

        # Brief pause so subscribers can connect
        await asyncio.sleep(0.1)

        self._running = True
        log.info(
            "OMS started",
            pull=self._cfg.pull_address,
            pub=self._cfg.pub_address,
            workers=self._cfg.order_workers,
        )

        # Order-book reconciliation safety net (parser injected → broker-agnostic)
        order_book_sync = OrderBookSync(
            manager=self,
            broker=self._broker,
            event_parser=self._broker,
            idle_interval=self._cfg.order_sync_interval,
            active_interval=self._cfg.active_order_sync_interval,
        )

        # Launch background tasks
        self._tasks = [
            asyncio.create_task(self._receive_loop(), name="oms-receive"),
            asyncio.create_task(order_book_sync.run(), name="oms-sync"),
        ]
        for i in range(self._cfg.order_workers):
            worker = OrderWorker(self, i)
            self._tasks.append(
                asyncio.create_task(worker.run(), name=f"oms-worker-{i}")
            )

        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def stop(self) -> None:
        """Signal all tasks to stop and wait for clean shutdown."""
        log.info("OMS shutdown initiated")
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

        self._transport.close()

        await self._broker.close()
        await self._persist_state()
        log.info("OMS shutdown complete")

    # ------------------------------------------------------------------
    # State restoration
    # ------------------------------------------------------------------

    async def _restore_state(self) -> None:
        saved = await self._store.load_orders_state()
        for oms_id, data in saved.items():
            try:
                # Only restore non-terminal orders that might still be live
                status = OrderStatus(data.get("status", "ERROR"))
                if status in TERMINAL_STATES:
                    continue
                order = self._dict_to_order(data)
                self._orders[oms_id] = order
                if order.order_unique_identifier:
                    self._uid_index[order.order_unique_identifier] = oms_id
                if order.broker_order_id:
                    self._broker_index[order.broker_order_id] = oms_id
            except Exception as exc:
                log.warning("Could not restore order", oms_order_id=oms_id, error=str(exc))
        log.info("Orders restored", count=len(self._orders))

    async def _persist_state(self) -> None:
        snapshot = {oid: o.to_dict() for oid, o in self._orders.items()}
        await self._store.save_orders_state(snapshot)

    # ------------------------------------------------------------------
    # Receive loop (ZMQ PULL)
    # ------------------------------------------------------------------

    async def _receive_loop(self) -> None:
        log.info("Signal receiver started")
        while self._running:
            try:
                signal = await self._transport.recv_signal(timeout_ms=1000)
                if signal is None:
                    continue
                await self._dispatch_signal(signal)
            except asyncio.CancelledError:
                break
            except json.JSONDecodeError as exc:
                log.warning("Malformed signal received", error=str(exc))
            except Exception as exc:
                log.error("Receive loop error", error=str(exc), exc_info=True)

    async def _dispatch_signal(self, signal: Dict[str, Any]) -> None:
        """Route a signal through the command dispatcher."""
        await self._dispatcher.dispatch(signal)

    async def _handle_unknown(self, signal: Dict[str, Any]) -> None:
        msg_type = signal.get("msg_type", "")
        strategy_id = signal.get("strategy_id", "UNKNOWN")
        signal_id = signal.get("signal_id", "")
        log.warning("Unknown msg_type", msg_type=msg_type, strategy=strategy_id)
        await self._publish_error(
            strategy_id=strategy_id,
            signal_id=signal_id,
            oms_order_id="",
            error_code="UNKNOWN_MSG_TYPE",
            error_message=f"Unsupported msg_type: {msg_type}",
        )

    # ------------------------------------------------------------------
    # Signal handlers
    # ------------------------------------------------------------------

    async def _handle_place_order(self, signal: Dict[str, Any]) -> None:
        strategy_id = signal.get("strategy_id", "")
        signal_id = signal.get("signal_id", "")

        # Validate required fields
        required = [
            "exchange_segment", "exchange_instrument_id",
            "product_type", "order_type", "order_side",
            "time_in_force", "order_quantity",
        ]
        for field_name in required:
            if field_name not in signal:
                await self._publish_error(
                    strategy_id=strategy_id,
                    signal_id=signal_id,
                    oms_order_id="",
                    error_code="VALIDATION_ERROR",
                    error_message=f"Missing required field: {field_name}",
                )
                return

        oms_id = Order.generate_id()
        uid = Order.generate_id()   # sent as orderUniqueIdentifier to broker

        order = Order(
            oms_order_id=oms_id,
            strategy_id=strategy_id,
            signal_id=signal_id,
            exchange_segment=signal["exchange_segment"],
            exchange_instrument_id=int(signal["exchange_instrument_id"]),
            instrument_name=signal.get("instrument_name", ""),
            product_type=signal["product_type"],
            order_type=signal["order_type"],
            order_side=signal["order_side"],
            time_in_force=signal["time_in_force"],
            order_quantity=float(signal["order_quantity"]),
            limit_price=float(signal.get("limit_price", 0.0)),
            stop_price=float(signal.get("stop_price", 0.0)),
            disclosed_quantity=int(signal.get("disclosed_quantity", 0)),
            order_unique_identifier=uid,
            pending_quantity=float(signal["order_quantity"]),
            status=OrderStatus.QUEUED,
            tags=signal.get("tags", {}),
        )

        self._orders[oms_id] = order
        self._uid_index[uid] = oms_id

        await self._store.append_order_log(order.to_dict(), "QUEUED")
        await self._update_stats_on_new(order)

        log.info(
            "Order queued",
            oms_order_id=oms_id,
            strategy=strategy_id,
            instrument=order.instrument_name,
            side=order.order_side,
            qty=order.order_quantity,
        )
        self._slog(strategy_id).info(
            "[QUEUED]   oms_id=%s | signal_id=%s | %s | side=%s | qty=%d | type=%s",
            oms_id, order.signal_id, order.instrument_name,
            order.order_side, order.order_quantity, order.order_type,
        )

        await self._order_queue.put(("PLACE", order))

    async def _handle_cancel_order(self, signal: Dict[str, Any]) -> None:
        strategy_id = signal.get("strategy_id", "")
        signal_id = signal.get("signal_id", "")
        oms_id = signal.get("oms_order_id", "")

        order = self._orders.get(oms_id)
        if not order:
            await self._publish_error(
                strategy_id=strategy_id,
                signal_id=signal_id,
                oms_order_id=oms_id,
                error_code="ORDER_NOT_FOUND",
                error_message=f"Order {oms_id} not found in OMS",
            )
            return

        if order.is_terminal:
            await self._publish_error(
                strategy_id=strategy_id,
                signal_id=signal_id,
                oms_order_id=oms_id,
                error_code="ORDER_ALREADY_TERMINAL",
                error_message=f"Order {oms_id} is already in terminal state {order.status}",
            )
            return

        order.cancel_reason = signal.get("reason", "Strategy requested cancel")
        await self._order_queue.put(("CANCEL", order))

        log.info("Cancel queued", oms_order_id=oms_id, strategy=strategy_id)

    async def _handle_modify_order(self, signal: Dict[str, Any]) -> None:
        strategy_id = signal.get("strategy_id", "")
        signal_id = signal.get("signal_id", "")
        oms_id = signal.get("oms_order_id", "")

        order = self._orders.get(oms_id)
        if not order and oms_id:
            # Strategies often hold signal_id until ORDER_ACK remaps to real oms_id.
            # Accept that reference here so modify can be safely gated in OMS.
            for existing in self._orders.values():
                if existing.signal_id == oms_id:
                    order = existing
                    break
        if not order:
            await self._publish_error(
                strategy_id=strategy_id,
                signal_id=signal_id,
                oms_order_id=oms_id,
                error_code="ORDER_NOT_FOUND",
                error_message=f"Order {oms_id} not found",
            )
            return

        if order.is_terminal:
            await self._publish_error(
                strategy_id=strategy_id,
                signal_id=signal_id,
                oms_order_id=oms_id,
                error_code="ORDER_ALREADY_TERMINAL",
                error_message=f"Cannot modify terminal order {oms_id}",
            )
            return

        # Latest params win; only one queue item per in-flight modify batch.
        order.tags["_modify"] = {
            "new_order_quantity": signal.get("new_order_quantity"),
            "new_limit_price": signal.get("new_limit_price"),
            "new_stop_price": signal.get("new_stop_price"),
            "new_order_type": signal.get("new_order_type"),
            "signal_id": signal_id,
        }
        if order.tags.get("_modify_queued"):
            log.info(
                "Modify coalesced",
                oms_order_id=oms_id,
                strategy=strategy_id,
            )
            return
        order.tags["_modify_queued"] = True
        await self._order_queue.put(("MODIFY", order))
        log.info("Modify queued", oms_order_id=oms_id, strategy=strategy_id)

    async def _handle_squareoff(self, signal: Dict[str, Any]) -> None:
        strategy_id = signal.get("strategy_id", "")
        signal_id = signal.get("signal_id", "")
        try:
            await self._broker.squareoff_position(
                exchange_segment=signal["exchange_segment"],
                exchange_instrument_id=int(signal["exchange_instrument_id"]),
                product_type=signal["product_type"],
            )
            resp = OrderResponse(
                msg_type=ResponseType.SQUAREOFF_ACK,
                strategy_id=strategy_id,
                oms_order_id="",
                signal_id=signal_id,
                status="SQUAREOFF_SENT",
                exchange_segment=signal.get("exchange_segment", ""),
                exchange_instrument_id=int(signal.get("exchange_instrument_id", 0)),
                message="Squareoff request sent to broker",
            )
            await self._publish_response(self._stamp(resp))
        except Exception as exc:
            await self._publish_error(
                strategy_id=strategy_id,
                signal_id=signal_id,
                oms_order_id="",
                error_code="SQUAREOFF_FAILED",
                error_message=str(exc),
            )

    async def _handle_cancel_all(self, signal: Dict[str, Any]) -> None:
        strategy_id = signal.get("strategy_id", "")
        signal_id = signal.get("signal_id", "")
        try:
            await self._broker.cancel_all_orders(
                exchange_segment=signal.get("exchange_segment"),
                exchange_instrument_id=signal.get("exchange_instrument_id"),
            )
            resp = OrderResponse(
                msg_type=ResponseType.CANCEL_ACK,
                strategy_id=strategy_id,
                oms_order_id="",
                signal_id=signal_id,
                status="CANCEL_ALL_SENT",
                message="Cancel-all request sent to broker",
            )
            await self._publish_response(self._stamp(resp))
        except Exception as exc:
            await self._publish_error(
                strategy_id=strategy_id,
                signal_id=signal_id,
                oms_order_id="",
                error_code="CANCEL_ALL_FAILED",
                error_message=str(exc),
            )

    # ------------------------------------------------------------------
    # Order execution (invoked by OrderWorker pool)
    # ------------------------------------------------------------------

    async def _execute_place(self, order: Order, worker_id: int) -> None:
        attempt = 0
        while attempt <= self._cfg.retry_attempts:
            try:
                order.status = OrderStatus.SENT
                order.sent_at = datetime.utcnow()
                order.updated_at = datetime.utcnow()
                await self._store.append_order_log(order.to_dict(), "SENT")

                result = await self._broker.place_order(
                    exchange_segment=order.exchange_segment,
                    exchange_instrument_id=order.exchange_instrument_id,
                    product_type=order.product_type,
                    order_type=order.order_type,
                    order_side=order.order_side,
                    time_in_force=order.time_in_force,
                    disclosed_quantity=order.disclosed_quantity,
                    order_quantity=order.order_quantity,
                    limit_price=order.limit_price,
                    stop_price=order.stop_price,
                    order_unique_identifier=order.order_unique_identifier,
                    instrument_name=order.instrument_name or "",
                )

                broker_order_id = result.get("broker_order_id", "")
                order.broker_order_id = broker_order_id
                order.status = OrderStatus.PENDING
                order.updated_at = datetime.utcnow()
                self._broker_index[broker_order_id] = order.oms_order_id

                await self._store.append_order_log(order.to_dict(), "PENDING")
                await self._persist_state()

                # Acknowledge to strategy
                resp = OrderResponse(
                    msg_type=ResponseType.ORDER_ACK,
                    strategy_id=order.strategy_id,
                    oms_order_id=order.oms_order_id,
                    signal_id=order.signal_id,
                    status=OrderStatus.PENDING,
                    exchange_segment=order.exchange_segment,
                    exchange_instrument_id=order.exchange_instrument_id,
                    instrument_name=order.instrument_name,
                    order_side=order.order_side,
                    order_type=order.order_type,
                    order_quantity=order.order_quantity,
                    broker_order_id=broker_order_id,
                    pending_quantity=order.order_quantity,
                    tags=order.tags,
                    message="Order placed successfully — awaiting exchange confirmation",
                )
                await self._publish_response(self._stamp(resp))

                log.info(
                    "Order placed successfully",
                    worker=worker_id,
                    oms_order_id=order.oms_order_id,
                    broker_order_id=broker_order_id,
                    strategy=order.strategy_id,
                    instrument=order.instrument_name,
                )
                self._slog(order.strategy_id).info(
                    "[ACK]      oms_id=%s | broker_id=%s | %s | side=%s | qty=%d",
                    order.oms_order_id, broker_order_id,
                    order.instrument_name, order.order_side, order.order_quantity,
                )
                return

            except Exception as exc:
                attempt += 1
                if attempt > self._cfg.retry_attempts:
                    order.status = OrderStatus.ERROR
                    order.error_message = str(exc)
                    order.updated_at = datetime.utcnow()
                    await self._store.append_order_log(order.to_dict(), "ERROR")
                    await self._persist_state()
                    await self._publish_error(
                        strategy_id=order.strategy_id,
                        signal_id=order.signal_id,
                        oms_order_id=order.oms_order_id,
                        error_code="BROKER_ERROR",
                        error_message=str(exc),
                    )
                    log.error(
                        "Order placement failed after retries",
                        oms_order_id=order.oms_order_id,
                        strategy=order.strategy_id,
                        error=str(exc),
                    )
                    self._slog(order.strategy_id).error(
                        "[ERROR]    oms_id=%s | %s | BROKER_ERROR | %s",
                        order.oms_order_id, order.instrument_name, str(exc),
                    )
                else:
                    delay = self._cfg.retry_delay_ms / 1000.0
                    log.warning(
                        "Order placement failed, retrying",
                        attempt=attempt,
                        delay=delay,
                        error=str(exc),
                    )
                    await asyncio.sleep(delay)

    async def _execute_cancel(self, order: Order, worker_id: int) -> None:
        if not order.broker_order_id:
            await self._publish_error(
                strategy_id=order.strategy_id,
                signal_id=order.signal_id,
                oms_order_id=order.oms_order_id,
                error_code="NO_BROKER_ORDER_ID",
                error_message="Cannot cancel: no broker order ID assigned yet",
            )
            return

        try:
            await self._broker.cancel_order(order.broker_order_id)
            resp = OrderResponse(
                msg_type=ResponseType.CANCEL_ACK,
                strategy_id=order.strategy_id,
                oms_order_id=order.oms_order_id,
                signal_id=order.signal_id,
                status=order.status,
                broker_order_id=order.broker_order_id,
                instrument_name=order.instrument_name,
                message="Cancel request sent to broker",
                tags=order.tags,
            )
            await self._publish_response(self._stamp(resp))
            log.info(
                "Cancel request sent",
                oms_order_id=order.oms_order_id,
                broker_order_id=order.broker_order_id,
            )
        except Exception as exc:
            await self._publish_error(
                strategy_id=order.strategy_id,
                signal_id=order.signal_id,
                oms_order_id=order.oms_order_id,
                error_code="CANCEL_FAILED",
                error_message=str(exc),
            )

    def _modify_lock(self, oms_order_id: str) -> asyncio.Lock:
        lock = self._modify_locks.get(oms_order_id)
        if lock is None:
            lock = asyncio.Lock()
            self._modify_locks[oms_order_id] = lock
        return lock

    async def _execute_modify(self, order: Order, worker_id: int) -> None:
        async with self._modify_lock(order.oms_order_id):
            # Bound how long we will wait for the order to reach the exchange
            # open-order list before attempting a broker modify.
            open_deadline = datetime.utcnow() + timedelta(
                seconds=getattr(self._cfg, "modify_open_wait_secs", 2.0)
            )
            try:
                while True:
                    if order.is_terminal:
                        order.tags.pop("_modify", None)
                        log.debug(
                            "Modify skipped — order terminal",
                            oms_order_id=order.oms_order_id,
                            status=order.status,
                            worker=worker_id,
                        )
                        return

                    if not order.broker_order_id:
                        # Hard gate: never send broker modify before ACK assigns broker_order_id.
                        # Keep latest coalesced modify request and wait for ACK/open events.
                        await asyncio.sleep(0.05)
                        continue

                    # Second gate: XTS rejects a modify (HTTP 400 "not found in
                    # OpenOrder List") while the order is still PENDING/PendingNew —
                    # it has an AppOrderID but has not yet reached the exchange.
                    # Wait for it to go OPEN/PARTIAL_FILL (or terminal) before sending.
                    cur_status = (
                        OrderStatus(order.status)
                        if isinstance(order.status, str)
                        else order.status
                    )
                    if cur_status == OrderStatus.PENDING:
                        if datetime.utcnow() < open_deadline:
                            await asyncio.sleep(0.05)
                            continue
                        # Deadline passed (no socket update yet) — fall through and
                        # attempt the modify; failure is handled non-fatally below.

                    mod = order.tags.pop("_modify", None)
                    if not mod:
                        return

                    signal_id = mod.get("signal_id") or order.signal_id
                    qty = (
                        mod["new_order_quantity"]
                        if mod.get("new_order_quantity") is not None
                        else order.order_quantity
                    )
                    if order.filled_quantity > 0 and qty < order.filled_quantity:
                        qty = order.filled_quantity + max(order.pending_quantity, 0)

                    try:
                        await self._broker.modify_order(
                            broker_order_id=order.broker_order_id,
                            product_type=mod.get("new_product_type") or order.product_type,
                            order_type=mod.get("new_order_type") or order.order_type,
                            order_quantity=qty,
                            disclosed_quantity=(
                                mod["new_disclosed_quantity"]
                                if mod.get("new_disclosed_quantity") is not None
                                else order.disclosed_quantity
                            ),
                            limit_price=(
                                mod["new_limit_price"]
                                if mod.get("new_limit_price") is not None
                                else order.limit_price
                            ),
                            stop_price=(
                                mod["new_stop_price"]
                                if mod.get("new_stop_price") is not None
                                else order.stop_price
                            ),
                            time_in_force=mod.get("new_time_in_force") or order.time_in_force,
                            order_unique_identifier=order.order_unique_identifier,
                        )
                        if mod.get("new_order_quantity") is not None:
                            order.order_quantity = int(mod["new_order_quantity"])
                        if mod.get("new_limit_price") is not None:
                            order.limit_price = float(mod["new_limit_price"])
                        if mod.get("new_stop_price") is not None:
                            order.stop_price = float(mod["new_stop_price"])
                        if mod.get("new_order_type"):
                            order.order_type = mod["new_order_type"]
                        order.updated_at = datetime.utcnow()

                        resp = OrderResponse(
                            msg_type=ResponseType.MODIFY_ACK,
                            strategy_id=order.strategy_id,
                            oms_order_id=order.oms_order_id,
                            signal_id=signal_id,
                            status=order.status,
                            broker_order_id=order.broker_order_id,
                            instrument_name=order.instrument_name,
                            message="Modify request sent to broker",
                            tags=order.tags,
                        )
                        await self._publish_response(self._stamp(resp))
                    except Exception as exc:
                        # A failed MODIFY is NOT an order failure: the underlying
                        # order is still live (or already filled) and keeps being
                        # tracked via socket/order-book events. Never publish
                        # ORDER_ERROR here — strategies treat that as the order
                        # dying and would abandon a position that actually filled.
                        if order.is_terminal:
                            log.info(
                                "Modify rejected by broker but order already terminal",
                                oms_order_id=order.oms_order_id,
                                status=order.status,
                                error=str(exc),
                            )
                            return
                        resp = OrderResponse(
                            msg_type=ResponseType.MODIFY_REJECTED,
                            strategy_id=order.strategy_id,
                            oms_order_id=order.oms_order_id,
                            signal_id=signal_id,
                            status=order.status,
                            broker_order_id=order.broker_order_id,
                            instrument_name=order.instrument_name,
                            order_side=order.order_side,
                            order_type=order.order_type,
                            order_quantity=order.order_quantity,
                            filled_quantity=order.filled_quantity,
                            pending_quantity=order.pending_quantity,
                            error_code="MODIFY_FAILED",
                            error_message=str(exc),
                            message="Modify rejected by broker — order is unchanged and still live",
                            tags=order.tags,
                        )
                        await self._publish_response(self._stamp(resp))
                        log.warning(
                            "Modify rejected by broker — order left unchanged",
                            oms_order_id=order.oms_order_id,
                            broker_order_id=order.broker_order_id,
                            strategy=order.strategy_id,
                            status=order.status,
                            error=str(exc),
                        )
                        self._slog(order.strategy_id).warning(
                            "[MODIFY_REJ] oms_id=%s | %s | order unchanged | %s",
                            order.oms_order_id, order.instrument_name, str(exc),
                        )
                        return

                    if "_modify" not in order.tags:
                        return
            finally:
                order.tags.pop("_modify_queued", None)
                if (
                    "_modify" in order.tags
                    and not order.is_terminal
                    and not order.tags.get("_modify_queued")
                ):
                    order.tags["_modify_queued"] = True
                    await self._order_queue.put(("MODIFY", order))

    # ------------------------------------------------------------------
    # Broker event injection (called from Socket.IO or polling callbacks)
    # ------------------------------------------------------------------

    async def inject_broker_event(self, parsed_event: Dict[str, Any]) -> None:
        """
        Process a normalised broker order event.

        Call this from your XTS Interactive Socket.IO 'order' event
        callback after passing the raw event through
        ``XTSBrokerAdapter.parse_order_event()``.

        Parameters
        ----------
        parsed_event : dict returned by ``XTSBrokerAdapter.parse_order_event()``
        """
        uid = parsed_event.get("order_unique_identifier", "")
        broker_id = parsed_event.get("broker_order_id", "")

        # Lookup by uid first (most reliable), then by broker_order_id
        oms_id = self._uid_index.get(uid) or self._broker_index.get(broker_id)
        if not oms_id:
            log.debug(
                "Broker event for unknown order — possibly from another session",
                uid=uid,
                broker_id=broker_id,
            )
            return

        order = self._orders.get(oms_id)
        if not order or order.is_terminal:
            return

        prev_status = order.status

        # Compute the concrete fill update (fill inference lives in one place).
        update = self._event_processor.compute(order, parsed_event)
        if update is None:
            return

        new_status = update.new_status
        new_filled = update.filled_quantity
        new_pending = update.pending_quantity
        new_avg = update.avg_fill_price
        new_last_price = update.last_fill_price
        last_qty = update.last_fill_quantity

        exchange_ts = update.exchange_transact_time
        last_upd = update.last_update_time
        if exchange_ts:
            order.exchange_transact_time = exchange_ts
        if last_upd:
            order.last_update_time = last_upd

        # Update order fields
        order.status = new_status
        order.filled_quantity = new_filled
        order.pending_quantity = new_pending
        order.avg_fill_price = new_avg
        order.last_fill_price = new_last_price
        order.last_fill_quantity = last_qty
        order.reject_reason = parsed_event.get("reject_reason", "")
        order.updated_at = datetime.utcnow()
        if new_status == OrderStatus.FILLED:
            order.filled_at = self._parse_iso_datetime(exchange_ts) or datetime.utcnow()
        if broker_id and not order.broker_order_id:
            order.broker_order_id = broker_id
            self._broker_index[broker_id] = oms_id

        event_name = new_status.value

        # Publish to strategy immediately — persistence runs after
        resp = self._build_response(order, new_status)
        await self._publish_response(resp)

        asyncio.create_task(
            self._persist_broker_event(order, event_name, new_status, last_qty, new_last_price),
            name=f"persist-{oms_id}-{event_name}",
        )

        log.info(
            "Order state changed",
            oms_order_id=oms_id,
            strategy=order.strategy_id,
            instrument=order.instrument_name,
            prev_status=prev_status,
            new_status=new_status,
            filled_qty=order.filled_quantity,
            avg_price=order.avg_fill_price,
            reject_reason=order.reject_reason or None,
            error_message=order.error_message or None,
        )
        if new_status == OrderStatus.REJECTED:
            log.warning(
                "Order rejected by broker",
                oms_order_id=oms_id,
                strategy=order.strategy_id,
                instrument=order.instrument_name,
                reason=order.reject_reason,
            )
        elif new_status == OrderStatus.ERROR:
            log.error(
                "Order error",
                oms_order_id=oms_id,
                strategy=order.strategy_id,
                instrument=order.instrument_name,
                reason=order.error_message,
            )
        # Per-strategy file log
        _slog = self._slog(order.strategy_id)
        if new_status == OrderStatus.OPEN:
            _slog.info(
                "[OPEN]     oms_id=%s | broker_id=%s | %s",
                oms_id, order.broker_order_id, order.instrument_name,
            )
        elif new_status == OrderStatus.PARTIAL_FILL:
            _slog.info(
                "[PARTIAL]  oms_id=%s | %s | filled=%d/%d | avg=%.2f",
                oms_id, order.instrument_name,
                order.filled_quantity, order.order_quantity, order.avg_fill_price,
            )
        elif new_status == OrderStatus.FILLED:
            _slog.info(
                "[FILLED]   oms_id=%s | broker_id=%s | %s | side=%s | qty=%d | avg=%.2f",
                oms_id, order.broker_order_id, order.instrument_name,
                order.order_side, order.filled_quantity, order.avg_fill_price,
            )
        elif new_status == OrderStatus.CANCELLED:
            _slog.info(
                "[CANCELLED] oms_id=%s | %s",
                oms_id, order.instrument_name,
            )
        elif new_status == OrderStatus.REJECTED:
            _slog.warning(
                "[REJECTED]  oms_id=%s | %s | reason=%s",
                oms_id, order.instrument_name, order.reject_reason,
            )
        elif new_status == OrderStatus.EXPIRED:
            _slog.info(
                "[EXPIRED]   oms_id=%s | %s",
                oms_id, order.instrument_name,
            )
        elif new_status == OrderStatus.ERROR:
            _slog.error(
                "[ERROR]     oms_id=%s | %s | %s",
                oms_id, order.instrument_name, order.error_message,
            )

    async def _persist_broker_event(
        self,
        order: Order,
        event_name: str,
        new_status: OrderStatus,
        last_qty: int,
        last_price: float,
    ) -> None:
        """Background persistence — must not delay strategy responses."""
        oms_id = order.oms_order_id
        try:
            await self._store.append_order_log(order.to_dict(), event_name)
            await self._persist_state()

            if new_status in (OrderStatus.PARTIAL_FILL, OrderStatus.FILLED) and last_qty > 0:
                await self._store.append_trade(
                    oms_order_id=oms_id,
                    broker_order_id=order.broker_order_id,
                    strategy_id=order.strategy_id,
                    exchange_segment=order.exchange_segment,
                    exchange_instrument_id=order.exchange_instrument_id,
                    instrument_name=order.instrument_name,
                    order_side=order.order_side,
                    product_type=order.product_type,
                    fill_quantity=last_qty,
                    fill_price=last_price,
                    filled_quantity_total=order.filled_quantity,
                    avg_fill_price=order.avg_fill_price,
                    pending_quantity=order.pending_quantity,
                )
                await self._positions.on_fill(
                    exchange_segment=order.exchange_segment,
                    exchange_instrument_id=order.exchange_instrument_id,
                    instrument_name=order.instrument_name,
                    product_type=order.product_type,
                    order_side=order.order_side,
                    fill_quantity=last_qty,
                    fill_price=last_price,
                    strategy_id=order.strategy_id,
                )
                await self._update_stats_on_fill(order, last_qty, last_price)
        except Exception as exc:
            log.error(
                "Failed to persist broker event",
                oms_order_id=oms_id,
                event=event_name,
                error=str(exc),
                exc_info=True,
            )

    def _build_response(self, order: Order, status: OrderStatus) -> OrderResponse:
        status_to_msgtype = {
            OrderStatus.PENDING: ResponseType.ORDER_ACK,
            OrderStatus.OPEN: ResponseType.ORDER_OPEN,
            OrderStatus.PARTIAL_FILL: ResponseType.ORDER_PARTIAL,
            OrderStatus.FILLED: ResponseType.ORDER_FILLED,
            OrderStatus.CANCELLED: ResponseType.ORDER_CANCELLED,
            OrderStatus.REJECTED: ResponseType.ORDER_REJECTED,
            OrderStatus.EXPIRED: ResponseType.ORDER_EXPIRED,
            OrderStatus.ERROR: ResponseType.ORDER_ERROR,
        }
        msg_type = status_to_msgtype.get(status, ResponseType.ORDER_ACK)

        messages = {
            OrderStatus.OPEN: "Order accepted by exchange",
            OrderStatus.PARTIAL_FILL: (
                f"Partial fill: {order.filled_quantity}/{order.order_quantity} "
                f"@ {order.last_fill_price}"
            ),
            OrderStatus.FILLED: (
                f"Order fully filled: {order.order_quantity} @ avg {order.avg_fill_price}"
            ),
            OrderStatus.CANCELLED: "Order cancelled",
            OrderStatus.REJECTED: f"Order rejected: {order.reject_reason}",
            OrderStatus.EXPIRED: "Order expired",
            OrderStatus.ERROR: f"Order error: {order.error_message}",
        }

        filled_at_str = ""
        if order.filled_at:
            filled_at_str = (
                order.filled_at.isoformat()
                if hasattr(order.filled_at, "isoformat")
                else str(order.filled_at)
            )

        return OrderResponse(
            msg_type=msg_type,
            strategy_id=order.strategy_id,
            oms_order_id=order.oms_order_id,
            signal_id=order.signal_id,
            status=status.value if isinstance(status, OrderStatus) else status,
            exchange_segment=order.exchange_segment,
            exchange_instrument_id=order.exchange_instrument_id,
            instrument_name=order.instrument_name,
            order_side=order.order_side,
            order_type=order.order_type,
            order_quantity=order.order_quantity,
            broker_order_id=order.broker_order_id,
            filled_quantity=order.filled_quantity,
            pending_quantity=order.pending_quantity,
            avg_fill_price=order.avg_fill_price,
            last_fill_price=order.last_fill_price,
            last_fill_quantity=order.last_fill_quantity,
            reject_reason=order.reject_reason,
            message=messages.get(status, ""),
            timestamp=now_iso(self._tz),
            exchange_timestamp=order.exchange_transact_time,
            filled_at=filled_at_str,
            updated_at=(
                order.updated_at.isoformat()
                if order.updated_at and hasattr(order.updated_at, "isoformat")
                else now_iso(self._tz)
            ),
            tags=order.tags,
        )

    # ------------------------------------------------------------------
    # ZMQ publish
    # ------------------------------------------------------------------

    async def _publish_response(self, resp: OrderResponse) -> None:
        try:
            await self._transport.publish(resp.strategy_id, resp.to_dict())
            msg_type = resp.msg_type
            if isinstance(msg_type, ResponseType):
                msg_type = msg_type.value
            msg_type = str(msg_type)
            reason = (
                resp.error_message
                or resp.reject_reason
                or resp.message
                or resp.error_code
                or ""
            )
            if msg_type == ResponseType.ORDER_ERROR.value:
                log.error(
                    "Order failed",
                    strategy=resp.strategy_id,
                    msg_type=msg_type,
                    oms_order_id=resp.oms_order_id,
                    signal_id=resp.signal_id,
                    instrument=resp.instrument_name,
                    error_code=resp.error_code,
                    reason=reason,
                )
            elif msg_type in (
                ResponseType.ORDER_REJECTED.value,
                ResponseType.ORDER_EXPIRED.value,
                ResponseType.ORDER_CANCELLED.value,
            ):
                log.warning(
                    "Order failed",
                    strategy=resp.strategy_id,
                    msg_type=msg_type,
                    oms_order_id=resp.oms_order_id,
                    signal_id=resp.signal_id,
                    instrument=resp.instrument_name,
                    reason=reason,
                )
            else:
                log.debug(
                    "Response published",
                    strategy=resp.strategy_id,
                    msg_type=msg_type,
                    oms_order_id=resp.oms_order_id,
                    status=resp.status,
                )
        except Exception as exc:
            log.error("Failed to publish response", error=str(exc))

    async def _publish_error(
        self,
        strategy_id: str,
        signal_id: str,
        oms_order_id: str,
        error_code: str,
        error_message: str,
    ) -> None:
        resp = OrderResponse(
            msg_type=ResponseType.ORDER_ERROR,
            strategy_id=strategy_id,
            oms_order_id=oms_order_id,
            signal_id=signal_id,
            status=OrderStatus.ERROR,
            error_code=error_code,
            error_message=error_message,
            message=error_message,
        )
        await self._publish_response(self._stamp(resp))
        log.error(
            "Order error",
            strategy=strategy_id,
            oms_order_id=oms_order_id,
            error_code=error_code,
            error_message=error_message,
        )
        self._slog(strategy_id).error(
            "[OMS_ERROR] oms_id=%s | code=%s | %s",
            oms_order_id, error_code, error_message,
        )

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    async def _update_stats_on_new(self, order: Order) -> None:
        async with self._stats_lock:
            self._stats.setdefault("date", datetime.utcnow().strftime("%Y-%m-%d"))
            self._stats.setdefault("total_orders", 0)
            self._stats.setdefault("strategies", {})
            self._stats["total_orders"] += 1
            strat = self._stats["strategies"].setdefault(order.strategy_id, {"orders": 0, "fills": 0})
            strat["orders"] += 1
        await self._store.save_statistics(self._stats)

    async def _update_stats_on_fill(self, order: Order, fill_qty: int, fill_price: float) -> None:
        async with self._stats_lock:
            self._stats.setdefault("total_fills", 0)
            self._stats["total_fills"] += 1
            notional = fill_qty * fill_price
            if order.order_side.upper() == "BUY":
                self._stats["total_buy_value"] = self._stats.get("total_buy_value", 0.0) + notional
            else:
                self._stats["total_sell_value"] = self._stats.get("total_sell_value", 0.0) + notional
            strat = self._stats["strategies"].setdefault(order.strategy_id, {"orders": 0, "fills": 0})
            strat["fills"] += 1
        await self._store.save_statistics(self._stats)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_iso_datetime(value: str) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None

    @staticmethod
    def _dict_to_order(data: Dict[str, Any]) -> Order:
        """Reconstruct an Order from a saved dict (delegates to Order.from_dict)."""
        return Order.from_dict(data)

    def get_order(self, oms_order_id: str) -> Optional[Order]:
        return self._orders.get(oms_order_id)

    def get_all_orders(self) -> Dict[str, Order]:
        return dict(self._orders)
