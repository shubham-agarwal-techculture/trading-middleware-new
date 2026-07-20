"""
OMSClient — strategy-side client library.

Each strategy script creates one OMSClient instance.  It handles:
  • Sending order signals to the OMS via ZMQ PUSH
  • Receiving responses from the OMS via ZMQ SUB (filtered by strategy_id)
  • Optional asyncio callbacks or Future-based await for responses

Quick-start
-----------

    import asyncio
    from clients.oms_client import OMSClient

    async def main():
        client = OMSClient(strategy_id="MOMENTUM_001")
        await client.connect()

        # Register a response handler
        @client.on_response
        async def handle(resp):
            print(resp["msg_type"], resp["status"])

        # Place an order — returns signal_id immediately (oms_order_id arrives in ORDER_ACK)
        signal_id = await client.place_order(
            exchange_segment="NSEFO",
            exchange_instrument_id=35003,
            instrument_name="NIFTY25MAY19500CE",
            product_type="MIS",
            order_type="LIMIT",
            order_side="BUY",
            time_in_force="DAY",
            order_quantity=50,
            limit_price=250.0,
            tags={"trade_id": "T001"},
        )
        print("Placed signal:", signal_id)
        ack = await client.wait_for_ack(signal_id, timeout=10)
        oms_id = ack["oms_order_id"] if ack else None

        # OR — await a specific status (blocking until timeout)
        response = await client.wait_for_terminal(oms_id, timeout=30) if oms_id else None
        print("Final status:", response["status"])

        await client.disconnect()

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Callable, Coroutine, Dict, Optional

import zmq
import zmq.asyncio

from oms.utils.runtime import use_selector_event_loop_policy
from oms.utils.timeutil import now_iso as get_ist_now

use_selector_event_loop_policy()


class OMSClient:
    """
    Async client used by strategy scripts to communicate with the OMS.

    Parameters
    ----------
    strategy_id   : Unique identifier for this strategy.  Responses are
                    filtered by this ID on the ZMQ PUB/SUB channel.
    pull_address  : OMS PULL socket address (must match OMS config).
    sub_address   : OMS PUB socket address (must match OMS config).
    """

    def __init__(
        self,
        strategy_id: str,
        push_address: str = "tcp://127.0.0.1:5555",
        sub_address: str = "tcp://127.0.0.1:5556",
    ) -> None:
        self.strategy_id = strategy_id
        self._push_address = push_address
        self._sub_address = sub_address

        self._zmq_ctx = zmq.asyncio.Context.instance()
        self._push_socket: Optional[zmq.asyncio.Socket] = None
        self._sub_socket: Optional[zmq.asyncio.Socket] = None

        # Registered async callback for all incoming responses
        self._response_callback: Optional[Callable] = None

        # Pending futures keyed by oms_order_id for await-style usage
        self._pending_futures: Dict[str, asyncio.Future] = {}

        self._receiver_task: Optional[asyncio.Task] = None
        self._running = False

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open ZMQ sockets and start the background receiver."""
        self._push_socket = self._zmq_ctx.socket(zmq.PUSH)
        self._push_socket.connect(self._push_address)

        self._sub_socket = self._zmq_ctx.socket(zmq.SUB)
        self._sub_socket.connect(self._sub_address)
        # Subscribe only to this strategy's responses.
        # Append a space so ZMQ prefix-matching hits "STRATEGY_ID {json}"
        # and NOT strategies whose IDs START WITH the same prefix
        # (e.g. "EMA_CROSS_9_21_NIFTY" must not receive
        #  "EMA_CROSS_9_21_NIFTY_SWING_EXIT ..." messages).
        self._sub_socket.setsockopt_string(zmq.SUBSCRIBE, self.strategy_id + " ")

        # Brief pause to let ZMQ establish connections
        await asyncio.sleep(0.05)

        self._running = True
        self._receiver_task = asyncio.create_task(
            self._receive_loop(), name=f"oms-client-{self.strategy_id}"
        )

    async def disconnect(self) -> None:
        """Close sockets and cancel receiver task."""
        self._running = False
        if self._receiver_task:
            self._receiver_task.cancel()
            try:
                await self._receiver_task
            except asyncio.CancelledError:
                pass
        if self._push_socket:
            self._push_socket.close(linger=0)
        if self._sub_socket:
            self._sub_socket.close(linger=0)

    # ------------------------------------------------------------------
    # Response handling
    # ------------------------------------------------------------------

    def on_response(self, func: Callable[..., Coroutine]) -> Callable:
        """
        Decorator to register an async callback for all incoming responses.

        Example::

            @client.on_response
            async def handler(resp: dict):
                print(resp["status"], resp["oms_order_id"])
        """
        self._response_callback = func
        return func

    async def _receive_loop(self) -> None:
        while self._running:
            try:
                # poll() does not consume the message, so it is safe to cancel
                # on timeout.  Only call recv_string() when a message is ready.
                events = await self._sub_socket.poll(timeout=50)  # ms — low latency for fills
                if not events:
                    continue
                raw = await self._sub_socket.recv_string()
                # Format: "{strategy_id} {json_payload}"
                _, _, payload = raw.partition(" ")
                if not payload:
                    continue
                response = json.loads(payload)
                await self._dispatch_response(response)
            except asyncio.CancelledError:
                break
            except json.JSONDecodeError:
                continue
            except Exception:
                continue

    async def _dispatch_response(self, response: Dict[str, Any]) -> None:
        oms_id = response.get("oms_order_id", "")
        msg_type = response.get("msg_type", "")
        status = response.get("status", "")

        # Resolve any waiting futures for this order
        if oms_id and oms_id in self._pending_futures:
            terminal_statuses = {"FILLED", "CANCELLED", "REJECTED", "EXPIRED", "ERROR"}
            if status in terminal_statuses or msg_type in (
                "ORDER_FILLED", "ORDER_CANCELLED", "ORDER_REJECTED",
                "ORDER_EXPIRED", "ORDER_ERROR",
            ):
                fut = self._pending_futures.pop(oms_id, None)
                if fut and not fut.done():
                    fut.set_result(response)

        # Fire the registered callback
        if self._response_callback:
            try:
                await self._response_callback(response)
            except Exception as exc:
                # Don't crash the receiver on callback errors
                pass

    # ------------------------------------------------------------------
    # Order operations
    # ------------------------------------------------------------------

    async def place_order(
        self,
        exchange_segment: str,
        exchange_instrument_id: int,
        instrument_name: str,
        product_type: str,
        order_type: str,
        order_side: str,
        time_in_force: str,
        order_quantity: int,
        limit_price: float = 0.0,
        stop_price: float = 0.0,
        disclosed_quantity: int = 0,
        tags: Optional[Dict[str, Any]] = None,
        signal_id: Optional[str] = None,
    ) -> str:
        """
        Send a PLACE_ORDER signal to the OMS.

        Returns the ``oms_order_id`` immediately (fire-and-forget).
        Use ``wait_for_terminal()`` if you need to await the final status.
        """
        sid = signal_id or uuid.uuid4().hex
        signal = {
            "msg_type": "PLACE_ORDER",
            "strategy_id": self.strategy_id,
            "signal_id": sid,
            "timestamp": get_ist_now(),
            "exchange_segment": exchange_segment,
            "exchange_instrument_id": exchange_instrument_id,
            "instrument_name": instrument_name,
            "product_type": product_type,
            "order_type": order_type,
            "order_side": order_side,
            "time_in_force": time_in_force,
            "order_quantity": order_quantity,
            "limit_price": limit_price,
            "stop_price": stop_price,
            "disclosed_quantity": disclosed_quantity,
            "tags": tags or {},
        }
        await self._send(signal)

        # We don't know the oms_order_id until the ACK comes back.
        # Return the signal_id so callers can match on the first ACK.
        return sid

    async def wait_for_ack(
        self,
        signal_id: str,
        timeout: float = 10.0,
    ) -> Optional[Dict[str, Any]]:
        """
        Wait for the ORDER_ACK that carries the oms_order_id.

        Returns the full response dict, or None on timeout.
        """
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()

        # Temporarily hook into the callback to catch the ack
        original_cb = self._response_callback

        async def _watch(resp: Dict[str, Any]) -> None:
            if resp.get("signal_id") != signal_id or fut.done():
                if original_cb:
                    await original_cb(resp)
                return
            msg_type = resp.get("msg_type", "")
            if msg_type in ("ORDER_ACK", "ORDER_ERROR", "ORDER_REJECTED"):
                fut.set_result(resp)
            if original_cb:
                await original_cb(resp)

        self._response_callback = _watch
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self._response_callback = original_cb

    async def wait_for_open(
        self,
        oms_order_id: str,
        timeout: float = 30.0,
    ) -> Optional[Dict[str, Any]]:
        """
        Wait for ORDER_OPEN (exchange-acknowledged).

        Also returns immediately if the order reaches a terminal state first
        (REJECTED, CANCELLED, FILLED, etc.) so callers are not left waiting
        the full timeout.  Check ``msg_type == 'ORDER_OPEN'`` before modifying.
        """
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        original_cb = self._response_callback
        terminal_msg_types = {
            "ORDER_FILLED", "ORDER_CANCELLED", "ORDER_REJECTED",
            "ORDER_EXPIRED", "ORDER_ERROR",
        }

        async def _watch(resp: Dict[str, Any]) -> None:
            if resp.get("oms_order_id") != oms_order_id or fut.done():
                if original_cb:
                    await original_cb(resp)
                return
            msg_type = resp.get("msg_type", "")
            if msg_type == "ORDER_OPEN" or msg_type in terminal_msg_types:
                fut.set_result(resp)
            if original_cb:
                await original_cb(resp)

        self._response_callback = _watch
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self._response_callback = original_cb

    async def wait_for_terminal(
        self,
        oms_order_id: str,
        timeout: float = 60.0,
    ) -> Optional[Dict[str, Any]]:
        """
        Block until this order reaches a terminal state or timeout.

        Returns the final response dict, or None on timeout.
        """
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_futures[oms_order_id] = fut
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_futures.pop(oms_order_id, None)
            return None

    async def cancel_order(
        self,
        oms_order_id: str,
        reason: str = "",
        signal_id: Optional[str] = None,
    ) -> None:
        """Send a CANCEL_ORDER signal to the OMS."""
        signal = {
            "msg_type": "CANCEL_ORDER",
            "strategy_id": self.strategy_id,
            "signal_id": signal_id or uuid.uuid4().hex,
            "timestamp": get_ist_now(),
            "oms_order_id": oms_order_id,
            "reason": reason,
        }
        await self._send(signal)

    async def modify_order(
        self,
        oms_order_id: str,
        new_order_quantity: Optional[int] = None,
        new_limit_price: Optional[float] = None,
        new_stop_price: Optional[float] = None,
        new_order_type: Optional[str] = None,
        new_product_type: Optional[str] = None,
        new_disclosed_quantity: Optional[int] = None,
        new_time_in_force: Optional[str] = None,
        signal_id: Optional[str] = None,
    ) -> None:
        """Send a MODIFY_ORDER signal to the OMS."""
        signal = {
            "msg_type": "MODIFY_ORDER",
            "strategy_id": self.strategy_id,
            "signal_id": signal_id or uuid.uuid4().hex,
            "timestamp": get_ist_now(),
            "oms_order_id": oms_order_id,
            "new_order_quantity": new_order_quantity,
            "new_limit_price": new_limit_price,
            "new_stop_price": new_stop_price,
            "new_order_type": new_order_type,
            "new_product_type": new_product_type,
            "new_disclosed_quantity": new_disclosed_quantity,
            "new_time_in_force": new_time_in_force,
        }
        await self._send(signal)

    async def squareoff(
        self,
        exchange_segment: str,
        exchange_instrument_id: int,
        product_type: str,
        signal_id: Optional[str] = None,
    ) -> None:
        """Send a SQUAREOFF signal for a specific instrument/product."""
        signal = {
            "msg_type": "SQUAREOFF",
            "strategy_id": self.strategy_id,
            "signal_id": signal_id or uuid.uuid4().hex,
            "timestamp": get_ist_now(),
            "exchange_segment": exchange_segment,
            "exchange_instrument_id": exchange_instrument_id,
            "product_type": product_type,
        }
        await self._send(signal)

    async def cancel_all(
        self,
        exchange_segment: Optional[str] = None,
        exchange_instrument_id: Optional[int] = None,
        signal_id: Optional[str] = None,
    ) -> None:
        """Send a CANCEL_ALL signal optionally scoped to an instrument."""
        signal = {
            "msg_type": "CANCEL_ALL",
            "strategy_id": self.strategy_id,
            "signal_id": signal_id or uuid.uuid4().hex,
            "timestamp": get_ist_now(),
            "exchange_segment": exchange_segment,
            "exchange_instrument_id": exchange_instrument_id,
        }
        await self._send(signal)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _send(self, signal: Dict[str, Any]) -> None:
        payload = json.dumps(signal)
        await self._push_socket.send_string(payload)
