"""
Standalone Nifty Signal Bridge

This script parses master_data/NSEFO.csv to resolve NIFTY ATM option contracts
and routes trade signals to the OMS via strategy_client.OMSClient.

Usage:
    python nifty_signal_bridge.py --port 5002
"""

import asyncio
import csv
import json
import logging
import sys
import threading
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from strategy_client import OMSClient
from nifty_atm_ltp import get_atm_data

# Configuration
STRATEGY_ID = "NIFTY_SIGNAL_BRIDGE"
OMS_PUSH = "tcp://192.168.1.26:5555"
OMS_SUB = "tcp://192.168.1.26:5556"
CSV_PATH = Path("master_data/NSEFO.csv")
DEFAULT_PORT = 5002

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("NIFTY_BRIDGE")

# Global variables
loop = None
client = None
http_port = DEFAULT_PORT
atm_data = None


async def handle_signal(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Process a signal and route to OMS."""
    global atm_data
    try:
        action = signal.get("action", "").upper()
        position = signal.get("position", "").lower()
        quantity = signal.get("quantity")
        option_type = signal.get("optionType", "CE").upper()  # Default to CE

        if not action or not position:
            return {
                "status": "error",
                "message": "Missing required fields: action, position",
            }

        # Get live ATM data
        if atm_data is None:
            atm_data = await get_atm_data()
            print(atm_data["atm_strike"])
            log.info("Fetched live ATM data: strike=%d", atm_data["atm_strike"])

        if option_type == "CE":
            contract_data = atm_data["ce_contract"]
            limit_price = atm_data["ce_ltp"]
        elif option_type == "PE":
            contract_data = atm_data["pe_contract"]
            limit_price = atm_data["pe_ltp"]
        else:
            return {
                "status": "error",
                "message": "Invalid optionType, must be CE or PE",
            }

        contract = {
            "exchange_segment": contract_data["ExchangeSegment"],
            "exchange_instrument_id": int(contract_data["ExchangeInstrumentID"]),
            "instrument_name": contract_data["Description"],
            "lot_size": int(contract_data["LotSize"]),
            "strike": atm_data["atm_strike"],
            "option_type": option_type,
        }

        # Determine quantity
        if quantity is not None:
            try:
                quantity = int(quantity)
            except ValueError:
                quantity = contract["lot_size"]
        else:
            quantity = contract["lot_size"]

        # Get product type from signal or default to MIS
        product_type = (
            signal.get("productType") or signal.get("product_type") or "MIS"
        ).upper()

        # Get order type from signal or default to LIMIT
        order_type = (
            signal.get("orderType") or signal.get("order_type") or "LIMIT"
        ).upper()

        # Override limit price from signal if provided
        if signal.get("limitPrice") or signal.get("limit_price"):
            limit_price = float(signal.get("limitPrice") or signal.get("limit_price"))
        stop_price = float(signal.get("stopPrice") or signal.get("stop_price") or 0.0)

        log.info(
            "Received signal: Action=%s, Position=%s, Qty=%d, Instrument=%s",
            action,
            position,
            quantity,
            contract["instrument_name"],
        )

        if position == "flat":
            # Square off position
            log.info("Processing square-off for %s...", contract["instrument_name"])
            sig_id = uuid.uuid4().hex
            await client.squareoff(
                exchange_segment=contract["exchange_segment"],
                exchange_instrument_id=contract["exchange_instrument_id"],
                product_type=product_type,
                signal_id=sig_id,
            )
            log.info("Squareoff command sent | signal_id=%s", sig_id)
            return {
                "status": "submitted",
                "msg_type": "SQUAREOFF",
                "signal_id": sig_id,
                "timestamp": datetime.utcnow().isoformat(),
                "instrument": contract["instrument_name"],
            }
        else:
            # Place order
            log.info(
                "Processing order placement for %s...", contract["instrument_name"]
            )
            sig_id = uuid.uuid4().hex
            signal_id = await client.place_order(
                exchange_segment=contract["exchange_segment"],
                exchange_instrument_id=contract["exchange_instrument_id"],
                instrument_name=contract["instrument_name"],
                product_type=product_type,
                order_type=order_type,
                order_side=action,
                time_in_force="DAY",
                order_quantity=quantity,
                limit_price=limit_price,
                stop_price=stop_price,
                tags=signal.get("tags") or {},
                signal_id=sig_id,
            )
            log.info("Order signal sent | signal_id=%s", signal_id)

            # Wait for ORDER_ACK
            log.info("Waiting for ORDER_ACK from OMS (timeout 10s)...")
            ack = await client.wait_for_ack(signal_id, timeout=10.0)

            if ack:
                oms_order_id = ack.get("oms_order_id")
                log.info("Order acknowledged by OMS | oms_order_id=%s", oms_order_id)
                return {
                    "status": "acknowledged",
                    "oms_order_id": oms_order_id,
                    "signal_id": signal_id,
                    "instrument": contract["instrument_name"],
                    "response": ack,
                }
            else:
                log.warning("Timeout waiting for ORDER_ACK for signal_id=%s", signal_id)
                return {
                    "status": "timeout",
                    "message": "Order sent but no ACK received within 10 seconds",
                    "signal_id": signal_id,
                    "instrument": contract["instrument_name"],
                }

    except Exception as e:
        log.exception("Error handling signal:")
        return {"status": "error", "message": str(e)}


class BridgeHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/signal":
            try:
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                signal = json.loads(post_data.decode("utf-8"))

                # Dispatch to async handler
                future = asyncio.run_coroutine_threadsafe(handle_signal(signal), loop)

                # Wait for result (timeout 15s)
                result = future.result(timeout=15.0)

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(result).encode("utf-8"))

            except Exception as e:
                log.exception("HTTP Handler error:")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
                )
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")

    def log_message(self, format, *args):
        # Suppress default HTTP logging
        log.debug(format % args)


def run_http_server():
    """Run HTTP server in a separate thread."""
    server_address = ("", http_port)
    httpd = HTTPServer(server_address, BridgeHTTPRequestHandler)
    log.info("HTTP Bridge Server listening on port %d...", http_port)
    try:
        httpd.serve_forever()
    except Exception as e:
        log.error("HTTP Server error: %s", e)
    finally:
        httpd.server_close()


async def main():
    """Main entry point."""
    global loop, client, http_port, atm_data

    # Parse command line args
    import argparse

    parser = argparse.ArgumentParser(description="Nifty Signal Bridge")
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"HTTP port (default: {DEFAULT_PORT})",
    )
    args = parser.parse_args()

    http_port = args.port

    log.info("Starting Nifty Signal Bridge...")
    log.info("Configuration: port=%d", http_port)

    # Pre-fetch ATM data on startup
    log.info("Fetching initial ATM data...")
    atm_data = await get_atm_data()
    log.info("Initial ATM data loaded: strike=%d", atm_data["atm_strike"])

    # Get event loop
    loop = asyncio.get_running_loop()

    # Initialize OMS Client
    client = OMSClient(
        strategy_id=STRATEGY_ID,
        push_address=OMS_PUSH,
        sub_address=OMS_SUB,
    )

    log.info("Connecting to OMS at push=%s sub=%s...", OMS_PUSH, OMS_SUB)
    await client.connect()

    # Register response handler
    @client.on_response
    async def on_response(resp: Dict[str, Any]) -> None:
        msg_type = resp.get("msg_type", "")
        oms_id = resp.get("oms_order_id", "N/A")
        status = resp.get("status", "")
        log.info("[OMS Update] type=%s, oms_id=%s, status=%s", msg_type, oms_id, status)

    log.info("OMS Client connected. Starting HTTP thread...")

    # Start HTTP server in daemon thread
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()

    log.info("Nifty Signal Bridge is fully operational. Press Ctrl+C to terminate.")
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        log.info("Shutting down...")
        await client.disconnect()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bridge stopped by user.")
