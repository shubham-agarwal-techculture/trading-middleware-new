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
import errno
import json
import logging
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Optional
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

# Written when the bridge HTTP port is chosen so the Node webhook/UI can follow.
RUNTIME_PORT_FILE = Path(__file__).resolve().parent.parent / ".bridge_http_port"
PORT_RETRY_COUNT = 20


def _is_addr_in_use(exc: OSError) -> bool:
    if getattr(exc, "winerror", None) == 10048:  # WSAEADDRINUSE
        return True
    return exc.errno in (
        errno.EADDRINUSE,
        getattr(errno, "WSAEADDRINUSE", 10048),
    )


def _write_runtime_port(port: int) -> None:
    try:
        RUNTIME_PORT_FILE.write_text(str(port), encoding="utf-8")
    except OSError as e:
        log.warning("Could not write runtime port file %s: %s", RUNTIME_PORT_FILE, e)


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

            future = asyncio.run_coroutine_threadsafe(
                handle_signal(signal), state.loop
            )
            try:
                result = future.result(timeout=5.0)
            except TimeoutError:
                log.warning("Signal processing timeout - order may be pending")
                result = {
                    "status": "processing",
                    "message": "Order submitted, processing in background",
                }
                self._record_signal_alert(signal, result)
                _send_json(self, 202, result)
                return

            self._record_signal_alert(signal, result if isinstance(result, dict) else {})
            _send_json(self, 200, result)
        except Exception as e:
            log.exception("HTTP Handler error:")
            _send_json(self, 500, {"status": "error", "message": str(e)})

    def _record_signal_alert(self, signal: dict, result: dict) -> None:
        """Persist a received-signal alert with full order / result details."""
        action = (
            signal.get("action")
            or result.get("order_side")
            or "UNKNOWN"
        )
        status = (result.get("status") or "").upper()
        instrument = (
            result.get("instrument")
            or signal.get("symbol")
            or signal.get("ticker")
            or signal.get("instrument_name")
            or "N/A"
        )
        qty = result.get("quantity", signal.get("quantity"))
        order_type = (
            result.get("order_type")
            or signal.get("orderType")
            or signal.get("order_type")
        )
        product_type = (
            result.get("product_type")
            or signal.get("productType")
            or signal.get("product_type")
        )
        limit_price = (
            result.get("limit_price")
            if result.get("limit_price") is not None
            else signal.get("limitPrice") or signal.get("limit_price")
        )

        parts = [f"Received {str(action).upper()} signal"]
        if instrument and instrument != "N/A":
            parts.append(f"for {instrument}")
        if qty is not None:
            parts.append(f"qty={qty}")
        if status:
            parts.append(f"[{status}]")

        order = {
            "action": str(action).upper() if action else None,
            "position": signal.get("position") or result.get("position"),
            "symbol": signal.get("symbol") or signal.get("ticker"),
            "instrument": result.get("instrument") or signal.get("instrument_name"),
            "exchange_segment": result.get("exchange_segment")
            or signal.get("exchange_segment")
            or signal.get("exchangeSegment"),
            "exchange_instrument_id": result.get("exchange_instrument_id")
            or signal.get("exchange_instrument_id")
            or signal.get("exchangeInstrumentID"),
            "quantity": qty,
            "order_type": order_type,
            "product_type": product_type,
            "limit_price": limit_price,
            "stop_price": result.get("stop_price")
            or signal.get("stopPrice")
            or signal.get("stop_price"),
            "option_type": result.get("option_type") or signal.get("optionType"),
            "strike": result.get("strike"),
            "order_side": result.get("order_side") or action,
            "signal_id": result.get("signal_id"),
            "status": result.get("status"),
            "result_message": result.get("message"),
            "msg_type": result.get("msg_type"),
        }
        # Drop empty keys for cleaner UI / storage
        order = {k: v for k, v in order.items() if v is not None and v != ""}

        add_alert(
            {
                "type": "SIGNAL",
                "message": " ".join(parts),
                "data": signal,
                "order": order,
                "result": {
                    k: v
                    for k, v in result.items()
                    if k != "response" and v is not None
                },
            }
        )

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
                            f"Manual square off submitted for {position.get('instrument')} "
                            f"qty={pos_qty} side={reverse_side}"
                        ),
                        "order": {
                            "instrument": position.get("instrument"),
                            "exchange_segment": position.get("exchange_segment", "NSEFO"),
                            "exchange_instrument_id": instrument_key,
                            "quantity": pos_qty,
                            "order_side": reverse_side,
                            "order_type": "MARKET",
                            "product_type": "MIS",
                            "signal_id": sig_id,
                            "status": "submitted",
                            "position_side": pos_side,
                        },
                        "data": {
                            "instrument_key": instrument_key,
                            "position": position,
                        },
                    }
                )
                _send_json(
                    self,
                    200,
                    {"status": "success", "message": "Square off order submitted"},
                )
            else:
                add_alert(
                    {
                        "type": "SQUAREOFF",
                        "message": (
                            f"Manual square off failed for {position.get('instrument')}"
                        ),
                        "order": {
                            "instrument": position.get("instrument"),
                            "exchange_instrument_id": instrument_key,
                            "quantity": pos_qty,
                            "order_side": reverse_side,
                            "signal_id": sig_id,
                            "status": "failed",
                        },
                    }
                )
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


def _bind_http_server(
    preferred_port: int, max_tries: int = PORT_RETRY_COUNT
) -> HTTPServer:
    """Bind to preferred_port, or the next free port if it is already in use."""
    last_err: Optional[OSError] = None
    for offset in range(max_tries):
        port = preferred_port + offset
        try:
            httpd = HTTPServer(("", port), BridgeHTTPRequestHandler)
            state.http_port = port
            _write_runtime_port(port)
            if port != preferred_port:
                log.warning(
                    "Port %d is in use; auto-selected port %d instead",
                    preferred_port,
                    port,
                )
            return httpd
        except OSError as e:
            if _is_addr_in_use(e):
                log.warning("Port %d in use, trying %d...", port, port + 1)
                last_err = e
                continue
            raise
    raise OSError(
        f"Could not bind HTTP bridge on any port in "
        f"{preferred_port}-{preferred_port + max_tries - 1}"
    ) from last_err


def run_http_server():
    """Run HTTP server in a separate thread."""
    preferred = state.http_port
    try:
        httpd = _bind_http_server(preferred)
    except OSError as e:
        log.error("HTTP Server failed to bind: %s", e)
        return

    log.info("HTTP Bridge Server listening on port %d...", state.http_port)
    try:
        httpd.serve_forever()
    except Exception as e:
        log.error("HTTP Server error: %s", e)
    finally:
        httpd.server_close()
