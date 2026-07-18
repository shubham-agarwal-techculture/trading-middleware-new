"""
Storage backend protocol (Repository / Strategy).

:class:`~oms.storage.file_store.FileStore` is the default filesystem
implementation. Tests or alternate backends can implement this protocol
without changing :class:`~oms.core.order_manager.OrderManager`.
"""

from __future__ import annotations

from typing import Any, Dict, Protocol, runtime_checkable


@runtime_checkable
class StorageBackend(Protocol):
    """Async persistence contract used by the OMS."""

    async def save_orders_state(self, orders: Dict[str, Any]) -> None: ...

    async def load_orders_state(self) -> Dict[str, Any]: ...

    async def append_order_log(self, order_dict: Dict[str, Any], event: str) -> None: ...

    async def append_trade(
        self,
        oms_order_id: str,
        broker_order_id: str,
        strategy_id: str,
        exchange_segment: str,
        exchange_instrument_id: int,
        instrument_name: str,
        order_side: str,
        product_type: str,
        fill_quantity: int,
        fill_price: float,
        filled_quantity_total: int,
        avg_fill_price: float,
        pending_quantity: int,
    ) -> None: ...

    async def save_positions(self, positions: Dict[str, Any]) -> None: ...

    async def load_positions(self) -> Dict[str, Any]: ...

    async def save_statistics(self, stats: Dict[str, Any]) -> None: ...

    async def load_statistics(self) -> Dict[str, Any]: ...
