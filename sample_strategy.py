"""
sample_strategy.py — Reference implementation for a strategy using OMSClient.

This script demonstrates:
  1. Connecting to the OMS
  2. Receiving market data (stubbed here — replace with your pub/sub feed)
  3. Generating an order signal and placing it
  4. Handling all response types: ACK, OPEN, PARTIAL_FILL, FILLED,
     CANCELLED, REJECTED, ERROR
  5. Modifying and cancelling orders
  6. Graceful shutdown

Run OMS first:  python oms_server.py
Then run this:  python sample_strategy.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from strategy_client import OMSClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("MOMENTUM_001")

# ---------------------------------------------------------------------------
# Strategy configuration
# ---------------------------------------------------------------------------
STRATEGY_ID = "MOMENTUM_001"

# Instrument: NIFTY 26 MAY 23400 CE (replace with real exchange_instrument_id)
INSTRUMENT = {
    "exchange_segment": "NSEFO",
    "exchange_instrument_id": 41723,
    "instrument_name": "NIFTY2651223400CE",
}

# NSEFO NIFTY options lot size — qty must be a multiple of this (not 65).
# Verify via XTS instrument master / contract file for your symbol.
LOT_SIZE = 75

OMS_PUSH = "tcp://127.0.0.1:5555"
OMS_SUB = "tcp://127.0.0.1:5556"


# ---------------------------------------------------------------------------
# Response handler
# ---------------------------------------------------------------------------

class StrategyResponseHandler:
    """Tracks open orders and handles OMS responses."""

    def __init__(self, strategy_id: str) -> None:
        self.strategy_id = strategy_id
        # signal_id / oms_order_id → latest response
        self._order_book: Dict[str, Dict[str, Any]] = {}

    async def handle(self, response: Dict[str, Any]) -> None:
        """Called on every response published by the OMS for this strategy."""
        msg_type = response.get("msg_type", "")
        oms_id = response.get("oms_order_id", "N/A")
        status = response.get("status", "")
        instrument = response.get("instrument_name", "")

        self._order_book[oms_id] = response

        if msg_type == "ORDER_ACK":
            log.info(
                "[ACK] Order accepted by broker | %s | oms_id=%s | broker_id=%s",
                instrument, oms_id, response.get("broker_order_id"),
            )

        elif msg_type == "ORDER_OPEN":
            log.info(
                "[OPEN] Order live on exchange | %s | oms_id=%s | qty=%d",
                instrument, oms_id, response.get("order_quantity", 0),
            )

        elif msg_type == "ORDER_PARTIAL":
            log.info(
                "[PARTIAL] %s | oms_id=%s | filled=%d | pending=%d | avg=%.2f",
                instrument, oms_id,
                response.get("filled_quantity", 0),
                response.get("pending_quantity", 0),
                response.get("avg_fill_price", 0.0),
            )

        elif msg_type == "ORDER_FILLED":
            log.info(
                "[FILLED] %s | oms_id=%s | qty=%d | avg_price=%.2f",
                instrument, oms_id,
                response.get("filled_quantity", 0),
                response.get("avg_fill_price", 0.0),
            )
            # Example: trigger exit logic here
            # await self.place_exit_order(response)

        elif msg_type == "ORDER_CANCELLED":
            log.info(
                "[CANCELLED] %s | oms_id=%s",
                instrument, oms_id,
            )

        elif msg_type == "ORDER_REJECTED":
            log.warning(
                "[REJECTED] %s | oms_id=%s | reason=%s",
                instrument, oms_id, response.get("reject_reason"),
            )

        elif msg_type == "ORDER_EXPIRED":
            log.info("[EXPIRED] %s | oms_id=%s", instrument, oms_id)

        elif msg_type == "ORDER_MODIFIED":
            log.info("[MODIFIED] %s | oms_id=%s", instrument, oms_id)

        elif msg_type == "ORDER_ERROR":
            log.error(
                "[ERROR] %s | oms_id=%s | code=%s | msg=%s",
                instrument, oms_id,
                response.get("error_code"), response.get("error_message"),
            )

        elif msg_type in ("CANCEL_ACK", "MODIFY_ACK", "SQUAREOFF_ACK"):
            log.info("[%s] oms_id=%s", msg_type, oms_id)

        else:
            log.debug("[%s] %s", msg_type, response)


# ---------------------------------------------------------------------------
# Strategy logic
# ---------------------------------------------------------------------------

async def run_strategy() -> None:
    handler = StrategyResponseHandler(STRATEGY_ID)

    client = OMSClient(
        strategy_id=STRATEGY_ID,
        push_address=OMS_PUSH,
        sub_address=OMS_SUB,
    )
    await client.connect()

    # Register the response handler
    @client.on_response
    async def on_response(resp: Dict[str, Any]) -> None:
        await handler.handle(resp)

    log.info("Strategy connected to OMS | strategy_id=%s", STRATEGY_ID)

    # -----------------------------------------------------------------------
    # EXAMPLE 1: Place a LIMIT BUY order and await ACK
    # -----------------------------------------------------------------------
    log.info("Sending BUY order signal ...")
    signal_id = await client.place_order(
        exchange_segment=INSTRUMENT["exchange_segment"],
        exchange_instrument_id=INSTRUMENT["exchange_instrument_id"],
        instrument_name=INSTRUMENT["instrument_name"],
        product_type="MIS",
        order_type="LIMIT",
        order_side="BUY",
        time_in_force="DAY",
        order_quantity=LOT_SIZE,
        limit_price=0.5,
        tags={"trade_id": "T001", "signal_reason": "momentum_breakout"},
    )
    log.info("Signal sent | signal_id=%s", signal_id)

    print(f"signal_id: {signal_id}")

    # Wait for the initial ACK (which carries the oms_order_id)
    ack = await client.wait_for_ack(signal_id, timeout=10.0)
    if ack is None:
        log.warning("No ACK received within timeout — OMS may not be running")
        await client.disconnect()
        return

    oms_order_id = ack.get("oms_order_id")
    log.info("Order acknowledged | oms_order_id=%s", oms_order_id)

    # -----------------------------------------------------------------------
    # EXAMPLE 2: Await terminal state (blocks until filled/cancelled/rejected)
    # -----------------------------------------------------------------------
    log.info("Waiting for terminal state (max 30s) ...")
    final = await client.wait_for_terminal(oms_order_id, timeout=30.0)
    if final:
        log.info(
            "Order terminal | status=%s | filled=%d | avg_price=%.2f",
            final.get("status"),
            final.get("filled_quantity", 0),
            final.get("avg_fill_price", 0.0),
        )
    else:
        log.warning("Order did not reach terminal state within timeout")
        # Cancel order1 since it did not fill — leave no dangling open orders
        log.info("Cancelling unfilled order1 | oms_id=%s", oms_order_id)
        await client.cancel_order(oms_order_id, reason="Did not fill within timeout")

    # -----------------------------------------------------------------------
    # EXAMPLE 3: Place an order, wait for OPEN, then modify it
    # -----------------------------------------------------------------------
    PLACE2_PRICE = 0.25
    MODIFY2_PRICE = 0.30
    log.info("Placing order to modify ...")
    sig2 = await client.place_order(
        exchange_segment=INSTRUMENT["exchange_segment"],
        exchange_instrument_id=INSTRUMENT["exchange_instrument_id"],
        instrument_name=INSTRUMENT["instrument_name"],
        product_type="MIS",
        order_type="LIMIT",
        order_side="BUY",
        time_in_force="DAY",
        order_quantity=LOT_SIZE,
        limit_price=PLACE2_PRICE,
        tags={"trade_id": "T002"},
    )
    ack2 = await client.wait_for_ack(sig2, timeout=10.0)
    if ack2:
        oid2 = ack2.get("oms_order_id")
        # Wait for the exchange to acknowledge the order (OPEN) before modifying.
        # XTS rejects modify requests on orders that are still PENDING.
        log.info("Waiting for order2 to go OPEN before modifying | oms_id=%s", oid2)
        open_resp = await client.wait_for_open(oid2, timeout=30.0)
        if open_resp is None:
            log.warning(
                "Order2 did not reach OPEN within timeout — skipping modify/cancel | oms_id=%s",
                oid2,
            )
        elif open_resp.get("msg_type") != "ORDER_OPEN":
            log.warning(
                "Order2 ended before OPEN | status=%s | reason=%s — skipping modify/cancel | oms_id=%s",
                open_resp.get("status"),
                open_resp.get("reject_reason") or open_resp.get("message"),
                oid2,
            )
        else:
            log.info(
                "Modifying order price: %.2f → %.2f | oms_id=%s",
                PLACE2_PRICE, MODIFY2_PRICE, oid2,
            )
            await client.modify_order(oid2, new_limit_price=MODIFY2_PRICE)
            await asyncio.sleep(2)

            # -------------------------------------------------------------------
            # EXAMPLE 4: Cancel the modified order (only while still live)
            # -------------------------------------------------------------------
            latest = handler._order_book.get(oid2, {})
            if latest.get("status") in ("OPEN", "PARTIAL_FILL", "PENDING"):
                log.info("Cancelling order | oms_id=%s", oid2)
                await client.cancel_order(oid2, reason="Signal reversed")
            else:
                log.info(
                    "Skip cancel — order already terminal | status=%s | oms_id=%s",
                    latest.get("status"), oid2,
                )

    # -----------------------------------------------------------------------
    # EXAMPLE 5: Squareoff an entire position
    # -----------------------------------------------------------------------
    # await client.squareoff(
    #     exchange_segment="NSEFO",
    #     exchange_instrument_id=35003,
    #     product_type="MIS",
    # )

    # Keep running to receive any late responses
    log.info("Strategy in monitoring mode — press Ctrl+C to stop")
    try:
        while True:
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        pass

    await client.disconnect()
    log.info("Strategy disconnected")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        asyncio.run(run_strategy())
    except KeyboardInterrupt:
        log.info("Strategy stopped by user")
