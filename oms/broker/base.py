"""Abstract broker adapter interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BrokerError(Exception):
    """Raised when the broker API returns an error."""

    def __init__(self, message: str, code: str = "", description: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.description = description


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
    async def get_order_book(self) -> Dict[str, Any]:
        """Fetch the full order book from the broker."""

    @abstractmethod
    async def get_positions(self) -> Dict[str, Any]:
        """Fetch current positions."""

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources (close HTTP client, etc.)."""
