"""
XTS Interactive Socket.IO feed — real-time order/trade events.

Runs the blocking python-socketio client in a daemon thread and forwards
events into the asyncio OrderManager via the main event loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from typing import Any, Callable, Dict, Optional

from oms.broker.xts_adapter import XTSBrokerAdapter
import logging

from oms.utils.logger import get_logger

log = get_logger(__name__)
xts_log = logging.getLogger("xts")

# Engine.IO logs "packet queue is empty, aborting" at ERROR on transport drop,
# even when socketio logger=False. That is a normal close path, not an OMS bug.
for _logger_name in ("engineio.client", "engineio", "socketio.client", "socketio"):
    logging.getLogger(_logger_name).setLevel(logging.CRITICAL)


class XTSInteractiveSocket:
    """
    Connect to XTS Interactive Socket.IO and push order/trade updates to OMS.

    Connection URL (from XTS SDK):
      {base_url}/?token={token}&userID={userID}&apiType=INTERACTIVE
    Socket path: /interactive/socket.io
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        user_id: str,
        on_order_event: Callable[[Dict[str, Any]], None],
        on_trade_event: Optional[Callable[[Dict[str, Any]], None]] = None,
        verify_ssl: bool = True,
        reconnect: bool = True,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._user_id = str(user_id)
        self._on_order_event = on_order_event
        self._on_trade_event = on_trade_event
        self._verify_ssl = verify_ssl
        self._reconnect = reconnect
        self._thread: Optional[threading.Thread] = None
        self._running = False

    @property
    def connection_url(self) -> str:
        return (
            f"{self._base_url}/?token={self._token}"
            f"&userID={self._user_id}&apiType=INTERACTIVE"
        )

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run,
            name="xts-interactive-socket",
            daemon=True,
        )
        self._thread.start()
        log.info("XTS interactive socket thread started")

    def stop(self) -> None:
        self._running = False
        log.info("XTS interactive socket stop requested")

    def _handle_order(self, data: Any) -> None:
        xts_log.info("SOCKET order | data=%s", _safe_json(data))
        parsed = XTSBrokerAdapter.parse_order_event(data)
        if parsed:
            self._on_order_event(parsed)
        else:
            xts_log.warning("SOCKET order unparsed | data=%s", _safe_json(data))

    def _handle_trade(self, data: Any) -> None:
        xts_log.info("SOCKET trade | data=%s", _safe_json(data))
        if not self._on_trade_event:
            return
        parsed = XTSBrokerAdapter.parse_order_event(data)
        if parsed:
            self._on_trade_event(parsed)
        else:
            xts_log.warning("SOCKET trade unparsed | data=%s", _safe_json(data))

    def _build_client(self):
        import socketio

        sio = socketio.Client(
            reconnection=self._reconnect,
            reconnection_attempts=0,
            reconnection_delay=2,
            reconnection_delay_max=30,
            logger=False,
            engineio_logger=False,
            ssl_verify=self._verify_ssl,
        )

        @sio.event
        def connect():
            xts_log.info("SOCKET connect | user_id=%s", self._user_id)
            log.info("XTS interactive socket connected")

        @sio.event
        def disconnect():
            xts_log.info("SOCKET disconnect | user_id=%s", self._user_id)
            log.info(
                "XTS interactive socket disconnected (broker idle timeout or "
                "transport drop — auto-reconnect if enabled)"
            )

        @sio.on("joined")
        def on_joined(data):
            xts_log.info("SOCKET joined | data=%s", _safe_json(data))

        @sio.on("error")
        def on_error(data):
            xts_log.warning("SOCKET error | data=%s", _safe_json(data))
            log.warning("XTS socket error", data=str(data)[:500])

        @sio.on("order")
        def on_order(data):
            self._handle_order(data)

        @sio.on("trade")
        def on_trade(data):
            self._handle_trade(data)

        return sio

    def _run(self) -> None:
        try:
            import socketio  # noqa: F401
        except ImportError as exc:
            log.error(
                "python-socketio not installed — install requirements.txt",
                error=str(exc),
            )
            return

        sio = self._build_client()
        while self._running:
            try:
                if not sio.connected:
                    # Match official XTS SDK: websocket only (no polling upgrade).
                    sio.connect(
                        self.connection_url,
                        transports=["websocket"],
                        socketio_path="/interactive/socket.io",
                    )
                sio.wait()
            except Exception as exc:
                xts_log.error("SOCKET connection failed | error=%s", exc)
                log.error("XTS socket connection error", error=str(exc))
                if not self._reconnect or not self._running:
                    break
                threading.Event().wait(3.0)
            else:
                # wait() returned without exception — connection ended
                if not self._reconnect or not self._running:
                    break
                log.debug("XTS socket session ended, reconnecting ...")
                threading.Event().wait(2.0)

        try:
            if sio.connected:
                sio.disconnect()
        except Exception:
            pass


def _safe_json(data: Any, limit: int = 4000) -> str:
    try:
        text = json.dumps(data, default=str)
    except Exception:
        text = str(data)
    if len(text) > limit:
        return text[:limit] + "...(truncated)"
    return text


def attach_xts_socket(
    broker: XTSBrokerAdapter,
    order_manager: Any,
    *,
    verify_ssl: bool = True,
    reconnect: bool = True,
) -> Optional[XTSInteractiveSocket]:
    """
    Start the interactive socket feed if broker login succeeded.

    Returns the socket instance, or None if token/user_id are missing.
    """
    if not broker.token or not broker.user_id:
        log.warning("XTS socket not started — missing token or user_id after login")
        return None

    loop = asyncio.get_running_loop()

    def _schedule(coro) -> None:
        asyncio.run_coroutine_threadsafe(coro, loop)

    def on_order_event(parsed: Dict[str, Any]) -> None:
        _schedule(order_manager.inject_broker_event(parsed))

    def on_trade_event(parsed: Dict[str, Any]) -> None:
        _schedule(order_manager.inject_broker_event(parsed))

    feed = XTSInteractiveSocket(
        base_url=broker.base_url,
        token=broker.token,
        user_id=broker.user_id,
        on_order_event=on_order_event,
        on_trade_event=on_trade_event,
        verify_ssl=verify_ssl,
        reconnect=reconnect,
    )
    feed.start()
    return feed
