"""
Position / history / alert persistence for the signal bridge.

File schemas (``positions.json``, ``history.json``, ``alerts.json``) are
unchanged — this module is a Repository around those JSON files.
"""

from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from oms.utils.timeutil import now_iso

from bridge import state

log = logging.getLogger("NIFTY_BRIDGE")

POSITIONS_FILE = Path("positions.json")
HISTORY_FILE = Path("history.json")
ALERTS_FILE = Path("alerts.json")
MAX_ALERTS = 100


def get_ist_now() -> str:
    return now_iso("Asia/Kolkata")


def load_positions():
    if not POSITIONS_FILE.exists():
        return {}
    try:
        with open(POSITIONS_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        log.error("Error loading positions: %s", e)
        return {}


def save_positions(positions):
    try:
        with open(POSITIONS_FILE, "w") as f:
            json.dump(positions, f, indent=4)
    except Exception as e:
        log.error("Error saving positions: %s", e)


def get_position_display_values(
    position: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return display values for a position based on its current status."""
    if not position:
        return {"kind": "market", "ltp": None, "underlying": None}

    status = str(position.get("status", "")).upper()
    if status in {"FILLED", "COMPLETE"}:
        entry_price = (
            position.get("entry_price")
            or position.get("avg_price")
            or position.get("fill_price")
            or position.get("price")
            or position.get("limit_price")
        )
        current_ltp = position.get("current_ltp") or position.get("ltp")
        try:
            qty = float(position.get("qty", 1) or 1)
            entry_price = float(entry_price) if entry_price is not None else None
            current_ltp = float(current_ltp) if current_ltp is not None else None
        except (TypeError, ValueError):
            entry_price = None
            current_ltp = None

        if entry_price is None or current_ltp is None:
            return {
                "kind": "pnl",
                "value": None,
                "entry_price": entry_price,
                "current_ltp": current_ltp,
            }

        side = str(position.get("side", "")).upper()
        if side == "SELL":
            pnl_value = (entry_price - current_ltp) * qty
        else:
            pnl_value = (current_ltp - entry_price) * qty

        return {
            "kind": "pnl",
            "value": pnl_value,
            "entry_price": entry_price,
            "current_ltp": current_ltp,
        }

    return {
        "kind": "market",
        "ltp": position.get("current_ltp") or position.get("ltp"),
        "underlying": position.get("underlying_price")
        or position.get("underlying_ltp")
        or position.get("underlying"),
    }


def load_history():
    if not HISTORY_FILE.exists():
        return []
    try:
        with open(HISTORY_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        log.error("Error loading history: %s", e)
        return []


def save_history(history):
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=4)
    except Exception as e:
        log.error("Error saving history: %s", e)


def append_to_history(position, status):
    history = load_history()
    pos_copy = position.copy()
    pos_copy["final_status"] = status
    pos_copy["closed_at"] = get_ist_now()
    history.insert(0, pos_copy)
    if len(history) > 1000:
        history = history[:1000]
    save_history(history)


def load_alerts():
    if not ALERTS_FILE.exists():
        return []
    try:
        with open(ALERTS_FILE, "r") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception as e:
        log.error("Error loading alerts: %s", e)
        return []


def save_alerts(alerts):
    try:
        with open(ALERTS_FILE, "w") as f:
            json.dump(alerts, f, indent=4)
    except Exception as e:
        log.error("Error saving alerts: %s", e)


def add_alert(alert_data):
    alerts = load_alerts()
    alert = {
        "id": uuid.uuid4().hex,
        "timestamp": get_ist_now(),
        **alert_data,
    }
    alerts.insert(0, alert)
    if len(alerts) > MAX_ALERTS:
        alerts = alerts[:MAX_ALERTS]
    save_alerts(alerts)
    return alert


def periodic_cleanup():
    """Move terminal-status positions into history every 5 seconds."""
    log.info("Starting periodic cleanup thread (runs every 5 seconds)")
    while not state.cleanup_stop_event.is_set():
        try:
            positions = load_positions()
            updated = False
            terminal_statuses = ["REJECTED", "CANCELLED", "EXPIRED", "ERROR"]

            for key, pos in list(positions.items()):
                status = pos.get("status", "").upper()
                if status in terminal_statuses:
                    log.info(
                        "Moving position %s to history (terminal status: %s)",
                        pos.get("instrument"),
                        status,
                    )
                    append_to_history(pos, status)
                    positions.pop(key, None)
                    updated = True

            if updated:
                save_positions(positions)
        except Exception as e:
            log.error("Error in periodic cleanup: %s", e)

        state.cleanup_stop_event.wait(timeout=5.0)
