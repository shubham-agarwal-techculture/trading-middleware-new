"""
Standalone Nifty Signal Bridge

This script parses master_data/NSEFO.csv to resolve NIFTY 25000 CE contracts
and routes trade signals to the OMS via strategy_client.OMSClient.

Expiry Selection Modes:
- nearest: The closest active/upcoming contract (default)
- furthest: The chronologically furthest expiration date

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

# Configuration
STRATEGY_ID = "NIFTY_SIGNAL_BRIDGE"
OMS_PUSH = "tcp://192.168.1.26:5555"
OMS_SUB = "tcp://192.168.1.26:5556"
CSV_PATH = Path("master_data/NSEFO.csv")
DEFAULT_PORT = 5002
DUMMY_PRICE = 1.0

# Expiry selection mode: "nearest" or "furthest"
_expiry_mode = "nearest"

def get_expiry_mode() -> str:
    return _expiry_mode

def set_expiry_mode(mode: str) -> None:
    global _expiry_mode
    _expiry_mode = mode

# Target contract parameters
TARGET_NAME = "NIFTY"
TARGET_STRIKE = 25000
TARGET_OPTION_TYPE = "3"  # CE (Call Option)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("NIFTY_BRIDGE")


class ContractResolver:
    """Resolves NIFTY 25000 CE contracts from CSV data."""

    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.contract_index: Dict[Tuple[str, int, str], list] = {}
        self.resolved_contract: Optional[Dict[str, Any]] = None
        self._load_csv()

    def _load_csv(self) -> None:
        """Load and index the CSV file."""
        log.info("Loading CSV file: %s", self.csv_path)
        
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

        with open(self.csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                try:
                    name = row["Name"]
                    strike = int(float(row["StrikePrice"]))
                    option_type = row["OptionType"]
                    
                    # Index by (Name, StrikePrice, OptionType)
                    key = (name, strike, option_type)
                    if key not in self.contract_index:
                        self.contract_index[key] = []
                    self.contract_index[key].append(row)
                except (ValueError, KeyError) as e:
                    continue

        log.info("CSV loaded. Indexed %d contract groups.", len(self.contract_index))

    def resolve_contract(self) -> Dict[str, Any]:
        """
        Resolve the NIFTY 25000 CE contract based on EXPIRY_MODE.
        
        Returns:
            Dict with contract details including ExchangeInstrumentID, Description, LotSize, etc.
        """
        key = (TARGET_NAME, TARGET_STRIKE, TARGET_OPTION_TYPE)
        
        if key not in self.contract_index:
            raise ValueError(
                f"No contracts found for {TARGET_NAME} {TARGET_STRIKE} CE "
                f"(OptionType={TARGET_OPTION_TYPE})"
            )

        contracts = self.contract_index[key]
        log.info("Found %d contracts for NIFTY 25000 CE", len(contracts))

        # Filter contracts with expiry >= today
        today = datetime.now().date()
        valid_contracts = []
        
        for contract in contracts:
            try:
                expiry_str = contract["ContractExpiration"]
                expiry_date = datetime.fromisoformat(expiry_str).date()
                if expiry_date >= today:
                    valid_contracts.append((contract, expiry_date))
            except (ValueError, KeyError):
                continue

        if not valid_contracts:
            raise ValueError("No valid contracts with expiry >= today")

        log.info("Found %d valid contracts (expiry >= today)", len(valid_contracts))

        # Select based on expiry mode
        mode = get_expiry_mode()
        if mode == "nearest":
            # Sort by expiry date ascending and pick the first
            valid_contracts.sort(key=lambda x: x[1])
            selected_contract, selected_expiry = valid_contracts[0]
        elif mode == "furthest":
            # Sort by expiry date descending and pick the first
            valid_contracts.sort(key=lambda x: x[1], reverse=True)
            selected_contract, selected_expiry = valid_contracts[0]
        else:
            raise ValueError(f"Invalid EXPIRY_MODE: {mode}")

        self.resolved_contract = {
            "exchange_segment": selected_contract["ExchangeSegment"],
            "exchange_instrument_id": int(selected_contract["ExchangeInstrumentID"]),
            "instrument_name": selected_contract["Description"],
            "lot_size": int(selected_contract["LotSize"]),
            "expiry": selected_expiry.isoformat(),
            "strike": TARGET_STRIKE,
            "option_type": "CE",
        }

        log.info(
            "Resolved contract (mode=%s): %s | Expiry: %s | InstrumentID: %d | LotSize: %d",
            mode,
            self.resolved_contract["instrument_name"],
            self.resolved_contract["expiry"],
            self.resolved_contract["exchange_instrument_id"],
            self.resolved_contract["lot_size"],
        )

        return self.resolved_contract


# Global variables
loop = None
client = None
resolver = None
http_port = DEFAULT_PORT


async def handle_signal(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Process a signal and route to OMS."""
    try:
        action = signal.get("action", "").upper()
        position = signal.get("position", "").lower()
        quantity = signal.get("quantity")

        if not action or not position:
            return {
                "status": "error",
                "message": "Missing required fields: action, position"
            }

        # Get resolved contract details
        contract = resolver.resolve_contract()
        
        # Determine quantity
        if quantity is not None:
            try:
                quantity = int(quantity)
            except ValueError:
                quantity = contract["lot_size"]
        else:
            quantity = contract["lot_size"]

        # Get product type from signal or default to MIS
        product_type = (signal.get("productType") or signal.get("product_type") or "MIS").upper()
        
        # Get order type from signal or default to LIMIT
        order_type = (signal.get("orderType") or signal.get("order_type") or "LIMIT").upper()
        
        # Get limit price from signal or use dummy price
        limit_price = float(signal.get("limitPrice") or signal.get("limit_price") or DUMMY_PRICE)
        stop_price = float(signal.get("stopPrice") or signal.get("stop_price") or 0.0)

        log.info(
            "Received signal: Action=%s, Position=%s, Qty=%d, Instrument=%s",
            action, position, quantity, contract["instrument_name"]
        )

        if position == "flat":
            # Square off position
            log.info("Processing square-off for %s...", contract["instrument_name"])
            sig_id = uuid.uuid4().hex
            await client.squareoff(
                exchange_segment=contract["exchange_segment"],
                exchange_instrument_id=contract["exchange_instrument_id"],
                product_type=product_type,
                signal_id=sig_id
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
            log.info("Processing order placement for %s...", contract["instrument_name"])
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
                signal_id=sig_id
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
        return {
            "status": "error",
            "message": str(e)
        }


class BridgeHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/signal":
            try:
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                signal = json.loads(post_data.decode("utf-8"))

                # Dispatch to async handler
                future = asyncio.run_coroutine_threadsafe(
                    handle_signal(signal), loop
                )

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
                self.wfile.write(json.dumps({
                    "status": "error",
                    "message": str(e)
                }).encode("utf-8"))
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
    global loop, client, resolver, http_port

    # Parse command line args
    import argparse
    parser = argparse.ArgumentParser(description="Nifty Signal Bridge")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help=f"HTTP port (default: {DEFAULT_PORT})")
    parser.add_argument("--expiry-mode", type=str, default=get_expiry_mode(),
                        choices=["nearest", "furthest"],
                        help="Expiry selection mode (default: nearest)")
    args = parser.parse_args()

    http_port = args.port
    set_expiry_mode(args.expiry_mode)

    log.info("Starting Nifty Signal Bridge...")
    log.info("Configuration: port=%d, expiry_mode=%s", http_port, get_expiry_mode())

    # Initialize contract resolver
    resolver = ContractResolver(CSV_PATH)
    resolver.resolve_contract()  # Pre-resolve to validate

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
