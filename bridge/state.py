"""Mutable process-level state shared by the bridge HTTP and signal layers."""

from __future__ import annotations

import threading
from typing import Any, Dict, Optional

DEFAULT_PORT = 5002
STRATEGY_ID = "NIFTY_SIGNAL_BRIDGE"
OMS_PUSH = "tcp://127.0.0.1:5555"
OMS_SUB = "tcp://127.0.0.1:5556"

loop = None
client = None
http_port = DEFAULT_PORT
atm_data: Optional[Dict[str, Any]] = None
pending_orders: Dict[str, Any] = {}
cleanup_stop_event = threading.Event()
