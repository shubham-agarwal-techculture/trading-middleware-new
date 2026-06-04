import asyncio
import json
import logging
import sys
import threading
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from strategy_client import OMSClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("OMS_BRIDGE")

# Global variables
loop = None
client = None
STRATEGY_ID = "WEBHOOK_BRIDGE"
OMS_PUSH = "tcp://192.168.1.26:5555"
OMS_SUB = "tcp://192.168.1.26:5556"
HTTP_PORT = 5002

# Default instrument mapping (similar to sample_strategy.py)
DEFAULT_INSTRUMENT = {
    "exchange_segment": "NSEFO",
    "exchange_instrument_id": 41723,
    "instrument_name": "NIFTY2651223400CE",
}

async def handle_signal(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Processes a signal in the main event loop and interacts with the OMS."""
    try:
        action = signal.get("action", "").upper()
        quantity = signal.get("quantity")
        position = signal.get("position", "").lower()
        
        # if not action or quantity is None or not position:
        #     return {
        #         "status": "error",
        #         "message": "Missing required fields: action, quantity, position"
        #     }

        if not action or not position:
            return {
                "status": "error",
                "message": "Missing required fields: action, position"
            }

        
            
        # quantity = int(quantity)
        quantity = int(quantity) | 65 

        # Resolve segment, instrument ID, and name
        exchange_segment = signal.get("exchange_segment") or DEFAULT_INSTRUMENT["exchange_segment"]
        exchange_instrument_id = signal.get("exchange_instrument_id")
        if exchange_instrument_id is not None:
            exchange_instrument_id = int(exchange_instrument_id)
        else:
            exchange_instrument_id = DEFAULT_INSTRUMENT["exchange_instrument_id"]
            
        instrument_name = signal.get("instrument_name") or DEFAULT_INSTRUMENT["instrument_name"]
        
        product_type = (signal.get("productType") or signal.get("product_type") or "MIS").upper()
        order_type = (signal.get("orderType") or signal.get("order_type") or "LIMIT").upper()
        limit_price = float(signal.get("limitPrice") or signal.get("limit_price") or 0.0)
        stop_price = float(signal.get("stopPrice") or signal.get("stop_price") or 0.0)

        log.info(
            "Received signal from webhook: Action=%s, Qty=%d, Position=%s, Symbol=%s",
            action, quantity, position, instrument_name
        )

        print("quantity", quantity)
        print("action", action)
        print("position", position)
        print("instrument_name", instrument_name)
        print("product_type", product_type)
        print("order_type", order_type)
        print("limit_price", limit_price)
        print("stop_price", stop_price)
        print("exchange_instrument_id", exchange_instrument_id)
        print("exchange_segment", exchange_segment)
        print("order_side", action)

        if position == "flat":
            log.info("Processing flat position (square-off) for %s ...", instrument_name)
            sig_id = uuid.uuid4().hex
            await client.squareoff(
                exchange_segment=exchange_segment,
                exchange_instrument_id=exchange_instrument_id,
                product_type=product_type,
                signal_id=sig_id
            )
            log.info("Squareoff command sent | signal_id=%s", sig_id)
            return {
                "status": "submitted",
                "msg_type": "SQUAREOFF",
                "signal_id": sig_id,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        else:
            log.info("Processing order placement for %s ...", instrument_name)
            sig_id = uuid.uuid4().hex
            signal_id = await client.place_order(
                exchange_segment=exchange_segment,
                exchange_instrument_id=exchange_instrument_id,
                instrument_name=instrument_name,
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
            
            # Wait for ORDER_ACK to get the oms_order_id
            log.info("Waiting for ACK from OMS server (timeout 10s)...")
            ack = await client.wait_for_ack(signal_id, timeout=10.0)
            
            if ack:
                oms_order_id = ack.get("oms_order_id")
                log.info("Order acknowledged by OMS | oms_order_id=%s", oms_order_id)
                return {
                    "status": "acknowledged",
                    "oms_order_id": oms_order_id,
                    "signal_id": signal_id,
                    "response": ack
                }
            else:
                log.warning("Timeout waiting for ORDER_ACK for signal_id=%s", signal_id)
                return {
                    "status": "timeout",
                    "message": "Order sent but no ACK received within 10 seconds",
                    "signal_id": signal_id
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
                
                # Dispatch the async handler to the main event loop
                future = asyncio.run_coroutine_threadsafe(
                    handle_signal(signal), loop
                )
                
                # Wait for the result from the async event loop (timeout 15s)
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
        # Suppress default HTTP server logging to keep output clean
        log.debug(format % args)

def run_http_server():
    server_address = ("", HTTP_PORT)
    httpd = HTTPServer(server_address, BridgeHTTPRequestHandler)
    log.info("HTTP Bridge Server listening on port %d...", HTTP_PORT)
    try:
        httpd.serve_forever()
    except Exception as e:
        log.error("HTTP Server error: %s", e)
    finally:
        httpd.server_close()

async def main():
    global loop, client
    loop = asyncio.get_running_loop()
    
    # Initialize the OMS Client
    client = OMSClient(
        strategy_id=STRATEGY_ID,
        push_address=OMS_PUSH,
        sub_address=OMS_SUB,
    )
    
    log.info("Connecting to OMS at push=%s sub=%s...", OMS_PUSH, OMS_SUB)
    await client.connect()
    
    # Register response handler to log updates in python console
    @client.on_response
    async def on_response(resp: Dict[str, Any]) -> None:
        msg_type = resp.get("msg_type", "")
        oms_id = resp.get("oms_order_id", "N/A")
        status = resp.get("status", "")
        log.info("[OMS Update] type=%s, oms_id=%s, status=%s", msg_type, oms_id, status)

    log.info("OMS Client connected. Starting HTTP thread...")
    
    # Start HTTP server in a daemon thread
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    
    log.info("Webhook OMS Bridge is fully operational. Press Ctrl+C to terminate.")
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
