"""
Shared pytest fixtures for the trading-middleware test suite.

The most important helper here is :class:`FakeBroker`, an in-memory
implementation of :class:`oms.broker.base.AbstractBrokerAdapter`. It lets the
OMS characterization tests exercise the full order lifecycle (place / cancel /
modify / squareoff / fill) without any network, ZeroMQ, or real broker calls.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

# Make the repository root importable regardless of where pytest is invoked.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from oms.broker.base import AbstractBrokerAdapter  # noqa: E402
from oms.config import OMSConfig, StorageConfig  # noqa: E402
from oms.core.order_manager import OrderManager  # noqa: E402
from oms.core.position_tracker import PositionTracker  # noqa: E402
from oms.storage.file_store import FileStore  # noqa: E402


class FakeBroker(AbstractBrokerAdapter):
    """Record-and-replay broker used by OMS tests.

    Every call is recorded so tests can assert on what the OrderManager sent,
    and ``place_order`` hands back a deterministic broker order id.
    """

    def __init__(self) -> None:
        self.placed: List[Dict[str, Any]] = []
        self.modified: List[Dict[str, Any]] = []
        self.cancelled: List[str] = []
        self.squared_off: List[Dict[str, Any]] = []
        self.cancel_all_calls: List[Dict[str, Any]] = []
        self.order_book: Dict[str, Any] = {"result": []}
        self.login_called = False
        self.closed = False
        self._counter = 0
        # Set to an Exception instance to make the next place_order raise.
        self.place_error: Optional[Exception] = None

    async def login(self) -> Dict[str, Any]:
        self.login_called = True
        return {"ok": True}

    async def place_order(self, **kwargs: Any) -> Dict[str, Any]:
        if self.place_error is not None:
            raise self.place_error
        self.placed.append(kwargs)
        self._counter += 1
        return {"broker_order_id": f"BRK{self._counter}"}

    async def modify_order(self, **kwargs: Any) -> Dict[str, Any]:
        self.modified.append(kwargs)
        return {"ok": True}

    async def cancel_order(self, broker_order_id: str) -> Dict[str, Any]:
        self.cancelled.append(broker_order_id)
        return {"ok": True}

    async def get_order_book(self) -> Dict[str, Any]:
        return self.order_book

    async def get_positions(self) -> Dict[str, Any]:
        return {"result": []}

    async def squareoff_position(self, **kwargs: Any) -> Dict[str, Any]:
        self.squared_off.append(kwargs)
        return {"ok": True}

    async def cancel_all_orders(self, **kwargs: Any) -> Dict[str, Any]:
        self.cancel_all_calls.append(kwargs)
        return {"ok": True}

    async def close(self) -> None:
        self.closed = True


class OMSHarness:
    """Bundle of an OrderManager plus its collaborators and captured output."""

    def __init__(self, order_manager, broker, positions, responses) -> None:
        self.om: OrderManager = order_manager
        self.broker: FakeBroker = broker
        self.positions: PositionTracker = positions
        self.responses: List[Dict[str, Any]] = responses

    def responses_of_type(self, msg_type: str) -> List[Dict[str, Any]]:
        return [r for r in self.responses if r.get("msg_type") == msg_type]


@pytest.fixture
def make_oms(tmp_path):
    """Factory returning an :class:`OMSHarness` with ZMQ publishing stubbed out."""

    def _make(**cfg_overrides: Any) -> OMSHarness:
        defaults = dict(order_workers=1, retry_attempts=0, modify_open_wait_secs=0.0)
        defaults.update(cfg_overrides)
        cfg = OMSConfig(**defaults)
        storage = StorageConfig(data_dir=str(tmp_path / "data"))
        file_store = FileStore(storage, timezone=cfg.timezone)
        positions = PositionTracker(file_store)
        broker = FakeBroker()
        order_manager = OrderManager(
            config=cfg,
            broker=broker,
            file_store=file_store,
            position_tracker=positions,
        )

        captured: List[Dict[str, Any]] = []

        async def _capture(resp) -> None:
            captured.append(resp.to_dict())

        # Replace the real ZMQ publish with an in-memory capture.
        order_manager._publish_response = _capture  # type: ignore[assignment]

        return OMSHarness(order_manager, broker, positions, captured)

    return _make


@pytest.fixture
def place_signal():
    """A minimal, valid PLACE_ORDER signal used across OMS tests."""
    return {
        "msg_type": "PLACE_ORDER",
        "strategy_id": "TEST_STRAT",
        "signal_id": "sig-1",
        "exchange_segment": "NSEFO",
        "exchange_instrument_id": 41723,
        "instrument_name": "NIFTY26JUN27000CE",
        "product_type": "MIS",
        "order_type": "LIMIT",
        "order_side": "BUY",
        "time_in_force": "DAY",
        "order_quantity": 50,
        "limit_price": 100.0,
    }
