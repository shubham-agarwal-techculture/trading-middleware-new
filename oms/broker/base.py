"""Abstract broker adapter interface.

Defines the broker-agnostic contract the OMS depends on. Concrete adapters
(XTS today, others later) implement :class:`AbstractBrokerAdapter`, and any
object that can turn a raw broker event into a normalized dict satisfies the
:class:`BrokerEventParser` protocol used by the order-book sync.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Protocol, runtime_checkable


class BrokerError(Exception):
    """Raised when the broker API returns an error."""

    def __init__(self, message: str, code: str = "", description: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.description = description


@runtime_checkable
class BrokerEventParser(Protocol):
    """Anything that can normalize a raw broker order event into a dict.

    Implemented by broker adapters (e.g. ``XTSBrokerAdapter.parse_order_event``)
    and injected into the order-book sync so it stays broker-agnostic.
    """

    def parse_order_event(self, event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ...


class AbstractBrokerAdapter(ABC):
    """
    Broker-agnostic interface used by OrderManager.

    All methods are async.  Concrete implementations (XTS, Zerodha, etc.)
    sub-class this and translate to their respective REST/socket APIs.
    """

    @abstractmethod
    async def login(self) -> Dict[str, Any]:
        """Authenticate and store session token. Must be called before other methods."""

    @abstractmethod
    async def place_order(
        self,
        exchange_segment: str,
        exchange_instrument_id: int,
        product_type: str,
        order_type: str,
        order_side: str,
        time_in_force: str,
        disclosed_quantity: int,
        order_quantity: int,
        limit_price: float,
        stop_price: float,
        order_unique_identifier: str,
    ) -> Dict[str, Any]:
        """
        Place a new order.

        Returns dict with at least ``broker_order_id`` key.
        """

    @abstractmethod
    async def modify_order(
        self,
        broker_order_id: str,
        product_type: str,
        order_type: str,
        order_quantity: int,
        disclosed_quantity: int,
        limit_price: float,
        stop_price: float,
        time_in_force: str,
        order_unique_identifier: str,
    ) -> Dict[str, Any]:
        """Modify an existing open order."""

    @abstractmethod
    async def cancel_order(self, broker_order_id: str) -> Dict[str, Any]:
        """Cancel an existing open order."""

    @abstractmethod
    async def cancel_all_orders(
        self,
        exchange_segment: Optional[str] = None,
        exchange_instrument_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Cancel all open orders, optionally scoped to one instrument."""

    @abstractmethod
    async def squareoff_position(
        self,
        exchange_segment: str,
        exchange_instrument_id: int,
        product_type: str,
    ) -> Dict[str, Any]:
        """Square off (flatten) a position by sending an offsetting order."""

    @abstractmethod
    async def get_order_book(self) -> Dict[str, Any]:
        """Fetch the full order book from the broker."""

    @abstractmethod
    async def get_positions(self) -> Dict[str, Any]:
        """Fetch current positions."""

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources (close HTTP client, etc.)."""
