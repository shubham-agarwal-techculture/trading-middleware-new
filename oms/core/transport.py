"""
ZeroMQ transport for the OMS.

Encapsulates the two sockets the OMS owns:

* a ``PULL`` socket that receives order signals from strategies, and
* a ``PUB`` socket that publishes responses back, topic-prefixed by
  ``strategy_id`` so each strategy only receives its own updates.

Keeping all socket handling here lets :class:`~oms.core.order_manager.OrderManager`
stay focused on order lifecycle orchestration.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

import zmq
import zmq.asyncio

from oms.utils.logger import get_logger

log = get_logger(__name__)


class ZmqTransport:
    """Owns the OMS PULL (ingress) and PUB (egress) ZeroMQ sockets."""

    def __init__(self, pull_address: str, pub_address: str) -> None:
        self._pull_address = pull_address
        self._pub_address = pub_address
        self._ctx = zmq.asyncio.Context.instance()
        self._pull: Optional[zmq.asyncio.Socket] = None
        self._pub: Optional[zmq.asyncio.Socket] = None

    def bind(self) -> None:
        """Bind both sockets. Call once during startup."""
        self._pull = self._ctx.socket(zmq.PULL)
        self._pull.bind(self._pull_address)
        self._pub = self._ctx.socket(zmq.PUB)
        self._pub.bind(self._pub_address)

    async def recv_signal(self, timeout_ms: int = 1000) -> Optional[Dict[str, Any]]:
        """Return the next decoded signal, or ``None`` if none arrived in time.

        Polling first (rather than blocking on ``recv``) keeps the receive loop
        cancellable on shutdown without dropping a consumed message.
        """
        assert self._pull is not None, "ZmqTransport.bind() must be called first"
        events = await self._pull.poll(timeout=timeout_ms)
        if not events:
            return None
        raw = await self._pull.recv_string()
        return json.loads(raw)

    async def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        """Publish *payload* on the PUB socket, prefixed with *topic*."""
        assert self._pub is not None, "ZmqTransport.bind() must be called first"
        message = f"{topic} {json.dumps(payload)}"
        await self._pub.send_string(message)

    def close(self) -> None:
        """Close both sockets immediately (no linger)."""
        if self._pull is not None:
            self._pull.close(linger=0)
        if self._pub is not None:
            self._pub.close(linger=0)
