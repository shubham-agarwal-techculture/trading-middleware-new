"""
Periodic order-book reconciliation.

Socket.IO fill/status events are the primary source of truth, but they can be
missed (disconnects, restarts). This background task polls the broker order book
and re-injects any state changes it finds for still-active orders, acting as a
safety net. It polls faster while orders are active and slower when idle.

The broker's event parser is injected (see :class:`oms.broker.base.BrokerEventParser`)
so this module has no direct dependency on any specific broker implementation.
"""

from __future__ import annotations

import asyncio

from oms.utils.logger import get_logger

log = get_logger(__name__)


class OrderBookSync:
    """Reconciles OMS order state against the broker order book on an interval."""

    def __init__(
        self,
        manager,
        broker,
        event_parser,
        idle_interval: float,
        active_interval: float,
    ) -> None:
        self._m = manager
        self._broker = broker
        self._parser = event_parser
        self._idle_interval = idle_interval
        self._active_interval = active_interval

    async def run(self) -> None:
        """Loop: sleep (interval depends on activity), then reconcile."""
        manager = self._m
        log.info(
            "Order sync loop started",
            idle_interval=self._idle_interval,
            active_interval=self._active_interval,
        )
        while manager._running:
            try:
                has_active = any(o.is_active for o in manager._orders.values())
                interval = self._active_interval if has_active else self._idle_interval
                await asyncio.sleep(interval)
                await self._sync_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("Order sync error", error=str(exc))

    async def _sync_once(self) -> None:
        """Pull the broker order book and re-inject any missed state changes."""
        manager = self._m
        if not any(o.is_active for o in manager._orders.values()):
            return

        try:
            data = await self._broker.get_order_book()
            broker_orders = data.get("result", []) or []
            if not isinstance(broker_orders, list):
                broker_orders = [broker_orders]

            for broker_order in broker_orders:
                uid = str(broker_order.get("OrderUniqueIdentifier", ""))
                oms_id = manager._uid_index.get(uid)
                if not oms_id:
                    continue
                order = manager._orders.get(oms_id)
                if not order or order.is_terminal:
                    continue

                parsed = self._parser.parse_order_event({"Appended": broker_order})
                if parsed:
                    await manager.inject_broker_event(parsed)

            log.debug("Order book sync completed", checked=len(broker_orders))
        except Exception as exc:
            log.warning("Failed to sync order book", error=str(exc))
