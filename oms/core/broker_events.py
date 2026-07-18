"""
Broker event processing — turning a normalized broker order event into the
concrete field changes to apply to an :class:`~oms.models.order.Order`.

The broker adapter's ``parse_order_event`` produces a raw normalized dict; this
module contains the *fill-inference* rules (e.g. deriving a fill quantity when
the broker sends a terminal status with empty quantity fields). Centralizing it
here removes the duplication that previously lived both in the XTS adapter and
inline in the OrderManager.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from oms.models.order import Order, OrderStatus


@dataclass
class FillUpdate:
    """Computed order-state change derived from a broker event."""

    new_status: OrderStatus
    filled_quantity: int
    pending_quantity: int
    avg_fill_price: float
    last_fill_price: float
    last_fill_quantity: int
    exchange_transact_time: str
    last_update_time: str


class BrokerEventProcessor:
    """Computes fill updates from parsed broker events (no side effects)."""

    @staticmethod
    def compute(order: Order, event: Dict[str, Any]) -> Optional[FillUpdate]:
        """Return the :class:`FillUpdate` for *event*, or ``None`` if it is a no-op.

        A ``None`` result means the event carries no new information relative to
        the order's current state and should be ignored.
        """
        new_status = OrderStatus(event["oms_status"])
        new_filled = int(event.get("filled_quantity", order.filled_quantity))
        new_pending = int(event.get("pending_quantity", order.pending_quantity))
        new_avg = float(event.get("avg_fill_price", order.avg_fill_price))
        new_last_price = float(event.get("last_fill_price", order.last_fill_price))
        last_qty = int(event.get("last_fill_quantity", 0))

        has_new_fill_data = (
            new_filled > order.filled_quantity
            or (new_avg > 0 and order.avg_fill_price <= 0)
            or (new_last_price > 0 and order.last_fill_price <= 0)
        )
        if (
            new_status == order.status
            and new_filled == order.filled_quantity
            and new_pending == order.pending_quantity
            and abs(new_avg - order.avg_fill_price) < 1e-9
            and not has_new_fill_data
        ):
            return None

        # Infer missing fill qty/price (common in order-book poll before fields populate)
        order_qty = int(event.get("order_quantity") or order.order_quantity or 0)
        if new_status == OrderStatus.FILLED and new_filled <= 0 and order_qty > 0 and new_pending <= 0:
            new_filled = order_qty
        if new_avg <= 0 and new_last_price > 0:
            new_avg = new_last_price
        elif new_avg <= 0 and float(event.get("order_price", 0)) > 0 and new_filled > 0:
            new_avg = float(event["order_price"])
        elif new_avg <= 0 and order.limit_price > 0 and new_filled > 0:
            new_avg = order.limit_price
        if last_qty == 0 and new_filled > order.filled_quantity:
            last_qty = new_filled - order.filled_quantity
        if last_qty > 0 and new_last_price <= 0:
            new_last_price = new_avg

        return FillUpdate(
            new_status=new_status,
            filled_quantity=new_filled,
            pending_quantity=new_pending,
            avg_fill_price=new_avg,
            last_fill_price=new_last_price,
            last_fill_quantity=last_qty,
            exchange_transact_time=str(event.get("exchange_transact_time", "") or ""),
            last_update_time=str(event.get("last_update_time", "") or ""),
        )
