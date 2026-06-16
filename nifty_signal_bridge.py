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
import re
from datetime import datetime
from typing import Optional

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from strategy_client import OMSClient
from nifty_atm_ltp import (
    ContractLoader,
    XTSMarketDataClient,
    get_atm_data,
    provide_xts_client,
)

# Configuration
STRATEGY_ID = "NIFTY_SIGNAL_BRIDGE"
OMS_PUSH = "tcp://192.168.1.26:5555"
OMS_SUB = "tcp://192.168.1.26:5556"
CSV_PATH = Path("master_data/NSEFO.csv")
DEFAULT_PORT = 5002
POSITIONS_FILE = Path("positions.json")

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
pending_orders = {}  # Track pending orders by signal_id
alerts = []  # Track alerts/signals


def load_positions():
    """Load positions from JSON file."""
    if not POSITIONS_FILE.exists():
        return {}

    try:
        with open(POSITIONS_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        log.error("Error loading positions: %s", e)
        return {}


def save_positions(positions):
    """Save positions to JSON file."""
    try:
        with open(POSITIONS_FILE, "w") as f:
            json.dump(positions, f, indent=4)
    except Exception as e:
        log.error("Error saving positions: %s", e)


def add_alert(alert_data):
    """Add a new alert to the alerts list."""
    global alerts
    alert = {
        "id": uuid.uuid4().hex,
        "timestamp": datetime.utcnow().isoformat(),
        **alert_data
    }
    alerts.insert(0, alert)  # Add to beginning for newest first
    # Keep only last 100 alerts
    if len(alerts) > 100:
        alerts = alerts[:100]


def tv_to_xts_description(tv_ticker: str) -> Optional[str]:
    """
    Convert TradingView option ticker format to XTS master Description format.

    Example:
        RELIANCE260630C1230  -> RELIANCE26JUN1230CE
        NIFTY260625P25000    -> NIFTY26JUN25000PE
    """

    tv_ticker = tv_ticker.strip().upper()

    match = re.match(r"^([A-Z]+)(\d{6})([CP])(\d+(?:\.\d+)?)$", tv_ticker)

    if not match:
        return None

    underlying = match.group(1)
    expiry_str = match.group(2)
    option_flag = match.group(3)
    strike = match.group(4)

    expiry = datetime.strptime(expiry_str, "%y%m%d")

    expiry_part = expiry.strftime("%y%b").upper()

    option_type = "CE" if option_flag == "C" else "PE"

    if strike.endswith(".0"):
        strike = str(int(float(strike)))

    return f"{underlying}{expiry_part}{strike}{option_type}"


async def resolve_contract_by_ticker(
    ticker: str,
) -> Optional[dict]:

    if not ticker:
        return None

    loader = ContractLoader(CSV_PATH)

    xts_description = tv_to_xts_description(ticker)

    if not xts_description:
        return None

    xts_description = xts_description.upper()

    for contract in loader.contracts:
        description = contract.get("Description", "").strip().upper()

        if description == xts_description:
            return contract

    return None


async def get_ltp_for_contract(contract_data: Dict[str, Any]) -> Optional[float]:
    """Fetch the live LTP for a specific contract using the XTS marketdata client."""
    xts_client = provide_xts_client()  # Use helper function to create client
    try:
        await xts_client.connect()
        return await xts_client.get_ltp(
            int(contract_data["ExchangeInstrumentID"]),
            contract_data.get("ExchangeSegment", "NSEFO"),
        )
    finally:
        await xts_client.disconnect()


async def process_order_status(signal_id: str, contract: Dict[str, Any], quantity: int):
    """Background task to monitor order status and update position book."""
    global pending_orders

    try:
        log.info("Monitoring order status for signal_id: %s", signal_id)

        # Wait for ORDER_ACK with status tracking
        timeout = 30.0
        start_time = datetime.utcnow()
        order_filled = False
        instrument_key = str(contract["exchange_instrument_id"])

        while (datetime.utcnow() - start_time).total_seconds() < timeout:
            ack = await client.wait_for_ack(signal_id, timeout=2.0)
            if ack:
                status = ack.get("status", "")
                log.info("Order status update for %s: %s", signal_id, status)

                if status in ["FILLED", "COMPLETE"]:
                    order_filled = True
                    # Save position only after order is FILLED
                    positions = load_positions()
                    positions[instrument_key] = {
                        "side": "BUY",
                        "qty": quantity,
                        "instrument": contract["instrument_name"],
                        "exchange_instrument_id": contract["exchange_instrument_id"],
                        "opened_at": datetime.utcnow().isoformat(),
                        "signal_id": signal_id,
                        "oms_order_id": ack.get("oms_order_id"),
                    }
                    save_positions(positions)
                    log.info("Position saved for %s", contract["instrument_name"])

                    # Update pending order status
                    if signal_id in pending_orders:
                        pending_orders[signal_id]["status"] = "filled"
                        pending_orders[signal_id]["response"] = ack
                    break

                elif status in ["REJECTED", "CANCELLED", "EXPIRED"]:
                    log.warning(
                        "Order failed with status: %s for signal_id: %s",
                        status,
                        signal_id,
                    )
                    if signal_id in pending_orders:
                        pending_orders[signal_id]["status"] = "failed"
                        pending_orders[signal_id]["response"] = ack
                    break
            else:
                # No ACK received yet, continue waiting
                await asyncio.sleep(0.5)

        if not order_filled and signal_id in pending_orders:
            pending_orders[signal_id]["status"] = "timeout"

    except Exception as e:
        log.exception("Error processing order status for %s: %s", signal_id, e)
        if signal_id in pending_orders:
            pending_orders[signal_id]["status"] = "error"
            pending_orders[signal_id]["error"] = str(e)


async def handle_signal(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Process a signal and route to OMS."""
    global atm_data, pending_orders
    try:
        action = signal.get("action", "").upper()
        position = signal.get("position", "").lower()
        quantity = signal.get("quantity")
        option_type = signal.get("optionType", "CE").upper()
        ticker = signal.get("ticker") or signal.get("symbol")

        if not action or not position:
            return {
                "status": "error",
                "message": "Missing required fields: action, position",
            }

        contract_data = None
        limit_price = None

        if ticker:
            contract_data = await resolve_contract_by_ticker(ticker)
            if not contract_data:
                return {
                    "status": "error",
                    "message": f"Contract not found for ticker: {ticker}",
                }

            log.info("Resolved ticker contract: %s", contract_data.get("Description"))

            fetched_ltp = await get_ltp_for_contract(contract_data)
            if fetched_ltp is None:
                return {
                    "status": "error",
                    "message": f"Could not fetch LTP for ticker contract: {ticker}",
                }

            if signal.get("limitPrice") or signal.get("limit_price"):
                limit_price = float(
                    signal.get("limitPrice") or signal.get("limit_price")
                )
            else:
                limit_price = fetched_ltp

            option_type = (
                "CE"
                if contract_data.get("OptionType") == "3"
                else "PE"
                if contract_data.get("OptionType") == "4"
                else option_type
            )

            contract = {
                "exchange_segment": contract_data["ExchangeSegment"],
                "exchange_instrument_id": int(contract_data["ExchangeInstrumentID"]),
                "instrument_name": contract_data["Description"],
                "lot_size": int(contract_data["LotSize"]),
                "strike": int(float(contract_data.get("StrikePrice", 0))),
                "option_type": option_type,
            }
        else:
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

        # Load position book
        instrument_key = str(contract["exchange_instrument_id"])
        positions = load_positions()
        current_position = positions.get(instrument_key)

        log.info(
            "Received signal: Action=%s, Position=%s, Qty=%d, Instrument=%s",
            action,
            position,
            quantity,
            contract["instrument_name"],
        )

        # Handle SELL signals - close existing BUY position
        if action == "SELL":
            if not current_position:
                return {
                    "status": "ignored",
                    "message": f"No open position found for {contract['instrument_name']}",
                }

            log.info(
                "SELL received. Closing existing BUY position for %s",
                contract["instrument_name"],
            )

            sig_id = uuid.uuid4().hex

            await client.squareoff(
                exchange_segment=contract["exchange_segment"],
                exchange_instrument_id=contract["exchange_instrument_id"],
                product_type=product_type,
                signal_id=sig_id,
            )

            # Wait for SQUAREOFF_ACK
            log.info("Waiting for SQUAREOFF_ACK from OMS (timeout 10s)...")
            ack = await client.wait_for_ack(sig_id, timeout=10.0)

            if ack and ack.get("status") in ["FILLED", "COMPLETE"]:
                # Remove position only after successful squareoff
                positions.pop(instrument_key, None)
                save_positions(positions)
                log.info(
                    "Position removed after successful squareoff: %s",
                    contract["instrument_name"],
                )

                return {
                    "status": "submitted",
                    "msg_type": "SQUAREOFF",
                    "signal_id": sig_id,
                    "instrument": contract["instrument_name"],
                    "timestamp": datetime.utcnow().isoformat(),
                    "response": ack,
                }
            else:
                log.warning(
                    "Squareoff failed or not confirmed for signal_id=%s", sig_id
                )
                return {
                    "status": "squareoff_failed",
                    "message": "Squareoff sent but not confirmed",
                    "signal_id": sig_id,
                    "instrument": contract["instrument_name"],
                }

        # Handle FLAT position - close existing BUY position
        if position == "flat":
            if not current_position:
                return {
                    "status": "ignored",
                    "message": f"No open position found for {contract['instrument_name']}",
                }

            log.info("Processing square-off for %s...", contract["instrument_name"])
            sig_id = uuid.uuid4().hex
            await client.squareoff(
                exchange_segment=contract["exchange_segment"],
                exchange_instrument_id=contract["exchange_instrument_id"],
                product_type=product_type,
                signal_id=sig_id,
            )

            # Wait for SQUAREOFF_ACK
            log.info("Waiting for SQUAREOFF_ACK from OMS (timeout 10s)...")
            ack = await client.wait_for_ack(sig_id, timeout=10.0)

            if ack and ack.get("status") in ["FILLED", "COMPLETE"]:
                # Remove position only after successful squareoff
                positions.pop(instrument_key, None)
                save_positions(positions)
                log.info(
                    "Position removed after successful squareoff: %s",
                    contract["instrument_name"],
                )

                return {
                    "status": "submitted",
                    "msg_type": "SQUAREOFF",
                    "signal_id": sig_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "instrument": contract["instrument_name"],
                    "response": ack,
                }
            else:
                log.warning(
                    "Squareoff failed or not confirmed for signal_id=%s", sig_id
                )
                return {
                    "status": "squareoff_failed",
                    "message": "Squareoff sent but not confirmed",
                    "signal_id": sig_id,
                    "instrument": contract["instrument_name"],
                }

        # Handle BUY signals
        if action == "BUY":
            # Check if position already exists
            if current_position:
                return {
                    "status": "ignored",
                    "message": f"Position already exists for {contract['instrument_name']}",
                }

            log.info(
                "Processing order placement for %s...", contract["instrument_name"]
            )
            sig_id = uuid.uuid4().hex

            # Store pending order
            pending_orders[sig_id] = {
                "status": "pending",
                "instrument": contract["instrument_name"],
                "timestamp": datetime.utcnow().isoformat(),
            }

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

            # Start background task to monitor order status
            asyncio.create_task(process_order_status(sig_id, contract, quantity))

            # Return immediately with pending status
            return {
                "status": "pending",
                "signal_id": sig_id,
                "message": "Order submitted, waiting for fill confirmation",
                "instrument": contract["instrument_name"],
                "quantity": quantity,
                "limit_price": limit_price,
            }

        return {"status": "error", "message": f"Unsupported action: {action}"}

    except Exception as e:
        log.exception("Error handling signal:")
        return {"status": "error", "message": str(e)}


class BridgeHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        global pending_orders

        if self.path == "/signal":
            try:
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                signal = json.loads(post_data.decode("utf-8"))

                # Add alert for incoming signal
                add_alert({
                    "type": "SIGNAL",
                    "message": f"Received {signal.get('action', 'UNKNOWN')} signal",
                    "data": signal
                })

                # Dispatch to async handler with shorter timeout
                future = asyncio.run_coroutine_threadsafe(handle_signal(signal), loop)

                # Wait for result (timeout 5s - just for initial submission)
                result = future.result(timeout=5.0)

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(result).encode("utf-8"))

            except TimeoutError:
                log.warning("Signal processing timeout - order may be pending")
                self.send_response(202)  # Accepted
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps(
                        {
                            "status": "processing",
                            "message": "Order submitted, processing in background",
                        }
                    ).encode("utf-8")
                )
            except Exception as e:
                log.exception("HTTP Handler error:")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
                )

        elif self.path == "/status":
            # Endpoint to check order status
            try:
                query = self.path.split("?")[1] if "?" in self.path else ""
                params = dict(p.split("=") for p in query.split("&")) if query else {}
                signal_id = params.get("signal_id")

                if not signal_id:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps({"error": "Missing signal_id"}).encode()
                    )
                    return

                status = pending_orders.get(signal_id, {"status": "not_found"})

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(status).encode("utf-8"))

            except Exception as e:
                log.exception("Status endpoint error:")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
                )

        elif self.path == "/positions":
            # Endpoint to check current positions
            try:
                positions = load_positions()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(positions).encode("utf-8"))
            except Exception as e:
                log.exception("Positions endpoint error:")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
                )

        elif self.path == "/alerts":
            # Endpoint to get alerts
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(alerts).encode("utf-8"))
            except Exception as e:
                log.exception("Alerts endpoint error:")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
                )

        elif self.path == "/squareoff":
            # Endpoint for manual square-off
            try:
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                squareoff_data = json.loads(post_data.decode("utf-8"))
                
                instrument_key = squareoff_data.get("instrument_key")
                if not instrument_key:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps({"status": "error", "message": "Missing instrument_key"}).encode()
                    )
                    return
                
                positions = load_positions()
                if instrument_key not in positions:
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps({"status": "error", "message": "Position not found"}).encode()
                    )
                    return
                
                position = positions[instrument_key]
                sig_id = uuid.uuid4().hex
                
                future = asyncio.run_coroutine_threadsafe(
                    client.squareoff(
                        exchange_segment=position.get("exchange_segment", "NSEFO"),
                        exchange_instrument_id=int(instrument_key),
                        product_type="MIS",
                        signal_id=sig_id,
                    ),
                    loop
                )
                future.result(timeout=5.0)
                
                # Wait for SQUAREOFF_ACK
                ack_future = asyncio.run_coroutine_threadsafe(
                    client.wait_for_ack(sig_id, timeout=10.0),
                    loop
                )
                ack = ack_future.result(timeout=10.0)
                
                if ack and ack.get("status") in ["FILLED", "COMPLETE"]:
                    positions.pop(instrument_key, None)
                    save_positions(positions)
                    add_alert({
                        "type": "SQUAREOFF",
                        "message": f"Manually squared off {position['instrument']}",
                        "instrument": position['instrument']
                    })
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(json.dumps({"status": "success", "message": "Position squared off"}).encode("utf-8"))
                else:
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(json.dumps({"status": "error", "message": "Squareoff failed"}).encode("utf-8"))
            except Exception as e:
                log.exception("Squareoff endpoint error:")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
                )
        else:
            self.send_response(404)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b"Not Found")

    def log_message(self, format, *args):
        # Suppress default HTTP logging
        log.debug(format % args)

    def do_OPTIONS(self):
        # Handle CORS preflight requests
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        # Handle GET requests
        if self.path == "/status" or self.path == "/positions" or self.path == "/alerts":
            self.do_POST()  # Reuse POST handler for GET
        else:
            self.send_response(404)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b"Not Found")


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
        signal_id = resp.get("signal_id", "")

        log.info(
            "[OMS Update] type=%s, oms_id=%s, status=%s, signal_id=%s",
            msg_type,
            oms_id,
            status,
            signal_id,
        )

        # Update pending order status
        if signal_id and signal_id in pending_orders:
            pending_orders[signal_id]["last_update"] = {
                "msg_type": msg_type,
                "status": status,
                "oms_order_id": oms_id,
                "timestamp": datetime.utcnow().isoformat(),
            }

    log.info("OMS Client connected. Starting HTTP thread...")

    # Start HTTP server in daemon thread
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()

    log.info("Nifty Signal Bridge is fully operational. Press Ctrl+C to terminate.")
    log.info("Endpoints:")
    log.info("  POST /signal - Submit trade signal")
    log.info("  GET  /status?signal_id=xxx - Check order status")
    log.info("  GET  /positions - View current positions")

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
