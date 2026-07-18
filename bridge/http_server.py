"""
HTTP API for the signal bridge (dashboard + webhook ingress).

Routes:
  POST /signal      — trade signal
  GET  /status      — pending order status
  GET  /positions   — open positions (with live LTP)
  GET  /alerts      — recent alerts
  GET  /history     — closed positions
  POST /squareoff   — manual square-off
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from bridge import state
from bridge.market_data import enrich_positions_for_display
from bridge.positions import (
    add_alert,
    load_alerts,
    load_history,
    load_positions,
    save_positions,
)
from bridge.signal_service import handle_signal

log = logging.getLogger("NIFTY_BRIDGE")


def _send_json(handler: BaseHTTPRequestHandler, status: int, body: Any) -> None:
    """Write a JSON response with CORS headers."""
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.end_headers()
    handler.wfile.write(json.dumps(body).encode("utf-8"))


class BridgeHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        path = urlparse(self.path).path

        if path == "/signal":
            self._handle_signal()
        elif path == "/status":
            self._handle_status()
        elif path == "/positions":
            self._handle_positions()
        elif path == "/alerts":
            self._handle_alerts()
        elif path == "/history":
            self._handle_history()
        elif path == "/squareoff":
            self._handle_squareoff()
        else:
            self.send_response(404)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b"Not Found")

    def _handle_signal(self) -> None:
        try:
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)
            signal = json.loads(post_data.decode("utf-8"))

            add_alert(
                {
                    "type": "SIGNAL",
                    "message": f"Received {signal.get('action', 'UNKNOWN')} signal",
                    "data": signal,
                }
            )

            future = asyncio.run_coroutine_threadsafe(
                handle_signal(signal), state.loop
            )
            result = future.result(timeout=5.0)
            _send_json(self, 200, result)
        except TimeoutError:
            log.warning("Signal processing timeout - order may be pending")
            _send_json(
                self,
                202,
                {
                    "status": "processing",
                    "message": "Order submitted, processing in background",
                },
            )
        except Exception as e:
            log.exception("HTTP Handler error:")
            _send_json(self, 500, {"status": "error", "message": str(e)})

    def _handle_status(self) -> None:
        try:
            query = parse_qs(urlparse(self.path).query)
            signal_id = (query.get("signal_id") or [None])[0]

            if not signal_id:
                _send_json(self, 400, {"error": "Missing signal_id"})
                return

            status = state.pending_orders.get(signal_id, {"status": "not_found"})
            _send_json(self, 200, status)
        except Exception as e:
            log.exception("Status endpoint error:")
            _send_json(self, 500, {"status": "error", "message": str(e)})

    def _handle_positions(self) -> None:
        try:
            positions = asyncio.run(enrich_positions_for_display(load_positions()))
            _send_json(self, 200, positions)
        except Exception as e:
            log.exception("Positions endpoint error:")
            _send_json(self, 500, {"status": "error", "message": str(e)})

    def _handle_alerts(self) -> None:
        try:
            _send_json(self, 200, load_alerts())
        except Exception as e:
            log.exception("Alerts endpoint error:")
            _send_json(self, 500, {"status": "error", "message": str(e)})

    def _handle_history(self) -> None:
        try:
            _send_json(self, 200, load_history())
        except Exception as e:
            log.exception("History endpoint error:")
            _send_json(self, 500, {"status": "error", "message": str(e)})

    def _handle_squareoff(self) -> None:
        try:
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)
            squareoff_data = json.loads(post_data.decode("utf-8"))

            instrument_key = squareoff_data.get("instrument_key")
            if not instrument_key:
                _send_json(
                    self, 400, {"status": "error", "message": "Missing instrument_key"}
                )
                return

            positions = load_positions()
            if instrument_key not in positions:
                _send_json(
                    self, 404, {"status": "error", "message": "Position not found"}
                )
                return

            position = positions[instrument_key]
            sig_id = uuid.uuid4().hex
            pos_side = position.get("side", "BUY")
            reverse_side = "SELL" if pos_side.upper() == "BUY" else "BUY"
            pos_qty = position.get("qty", 0)

            positions[instrument_key]["squareoff_signal_id"] = sig_id
            save_positions(positions)

            future = asyncio.run_coroutine_threadsafe(
                state.client.place_order(
                    exchange_segment=position.get("exchange_segment", "NSEFO"),
                    exchange_instrument_id=int(instrument_key),
                    instrument_name=position.get("instrument"),
                    product_type="MIS",
                    order_type="MARKET",
                    order_side=reverse_side,
                    time_in_force="DAY",
                    order_quantity=pos_qty,
                    limit_price=0.0,
                    signal_id=sig_id,
                ),
                state.loop,
            )
            future.result(timeout=5.0)

            ack_future = asyncio.run_coroutine_threadsafe(
                state.client.wait_for_ack(sig_id, timeout=10.0), state.loop
            )
            ack = ack_future.result(timeout=10.0)

            if ack:
                add_alert(
                    {
                        "type": "SQUAREOFF",
                        "message": (
                            f"Manual square off order submitted for "
                            f"{position['instrument']}"
                        ),
                        "instrument": position["instrument"],
                    }
                )
                _send_json(
                    self,
                    200,
                    {"status": "success", "message": "Square off order submitted"},
                )
            else:
                _send_json(
                    self,
                    500,
                    {
                        "status": "error",
                        "message": "Failed to submit square off order",
                    },
                )
        except Exception as e:
            log.exception("Squareoff endpoint error:")
            _send_json(self, 500, {"status": "error", "message": str(e)})

    def log_message(self, format, *args):
        log.debug(format % args)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/status", "/positions", "/alerts", "/history"):
            self.do_POST()
        else:
            self.send_response(404)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b"Not Found")


def run_http_server():
    """Run HTTP server in a separate thread."""
    server_address = ("", state.http_port)
    httpd = HTTPServer(server_address, BridgeHTTPRequestHandler)
    log.info("HTTP Bridge Server listening on port %d...", state.http_port)
    try:
        httpd.serve_forever()
    except Exception as e:
        log.error("HTTP Server error: %s", e)
    finally:
        httpd.server_close()
