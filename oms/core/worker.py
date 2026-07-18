"""
Order worker — the consumer side of the OMS producer/consumer pipeline.

Signal handlers enqueue ``(operation, order)`` tuples onto the shared order
queue; a pool of :class:`OrderWorker` instances drains that queue concurrently
and invokes the matching executor on the OrderManager. Extracting the loop from
the manager keeps concurrency concerns separate from order logic.
"""

from __future__ import annotations

import asyncio

from oms.utils.logger import get_logger

log = get_logger(__name__)


class OrderWorker:
    """Runs one concurrent consumer of the OMS order queue."""

    def __init__(self, manager, worker_id: int) -> None:
        self._m = manager
        self._id = worker_id

    async def run(self) -> None:
        """Consume queued operations until the manager stops running."""
        manager = self._m
        log.info("Order worker started", worker_id=self._id)
        while manager._running:
            try:
                op, order = await asyncio.wait_for(
                    manager._order_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            try:
                if op == "PLACE":
                    await manager._execute_place(order, self._id)
                elif op == "CANCEL":
                    await manager._execute_cancel(order, self._id)
                elif op == "MODIFY":
                    await manager._execute_modify(order, self._id)
            except Exception as exc:
                log.error(
                    "Worker unhandled error",
                    worker_id=self._id,
                    op=op,
                    oms_order_id=order.oms_order_id,
                    error=str(exc),
                    exc_info=True,
                )
            finally:
                manager._order_queue.task_done()
