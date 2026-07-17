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
from datetime import datetime, timezone, timedelta
from typing import Optional
from calendar import monthcalendar

IST = timezone(timedelta(hours=5, minutes=30))


def get_ist_now():
    return datetime.now(IST).isoformat()


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
# OMS_PUSH = "tcp://192.168.1.26:5555"
# OMS_SUB = "tcp://192.168.1.26:5556"
OMS_PUSH = "tcp://127.0.0.1:5555"
OMS_SUB = "tcp://127.0.0.1:5556"
MASTER_DIR = Path("master_data")
# Order matters: preferred segment when the same instrument exists on more than one exchange
MASTER_SEGMENTS = ["NSEFO", "BSEFO", "MCXFO", "NSECM", "BSECM"]
CSV_PATH = MASTER_DIR / "NSEFO.csv"  # kept for NIFTY ATM flow (nifty_atm_ltp)
DEFAULT_PORT = 5002

# TradingView option roots that differ from exchange master names
TV_SYMBOL_ALIASES = {
    "BSX": "SENSEX",
    "BKX": "BANKEX",
}
POSITIONS_FILE = Path("positions.json")
HISTORY_FILE = Path("history.json")
ALERTS_FILE = Path("alerts.json")
MAX_ALERTS = 100

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
cleanup_stop_event = threading.Event()


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


async def hydrate_position_market_data(position: Dict[str, Any]) -> None:
    """Populate live market values for a single position from the exchange."""
    exchange_instrument_id = position.get("exchange_instrument_id")
    if not exchange_instrument_id:
        return

    try:
        contract_data = {
            "ExchangeInstrumentID": int(exchange_instrument_id),
            "ExchangeSegment": position.get("exchange_segment", "NSEFO"),
        }
        live_ltp = await get_ltp_for_contract(contract_data)
        if live_ltp is not None:
            position["current_ltp"] = live_ltp
            position["underlying_price"] = position.get("underlying_price", live_ltp)
            position["last_market_update_at"] = get_ist_now()
    except Exception as exc:
        log.warning(
            "Could not hydrate live market data for %s: %s",
            position.get("instrument"),
            exc,
        )


async def enrich_positions_for_display(
    positions: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Attach live market data and display values to each position entry."""
    if positions is None:
        positions = load_positions()

    for position in positions.values():
        await hydrate_position_market_data(position)
        position["display_values"] = get_position_display_values(position)

    return positions


def load_history():
    """Load history from JSON file."""
    if not HISTORY_FILE.exists():
        return []
    try:
        with open(HISTORY_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        log.error("Error loading history: %s", e)
        return []


def save_history(history):
    """Save history to JSON file."""
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=4)
    except Exception as e:
        log.error("Error saving history: %s", e)


def append_to_history(position, status):
    """Append a completed/failed position to history.json"""
    history = load_history()
    pos_copy = position.copy()
    pos_copy["final_status"] = status
    pos_copy["closed_at"] = get_ist_now()
    history.insert(0, pos_copy)
    if len(history) > 1000:
        history = history[:1000]
    save_history(history)


def load_alerts():
    """Load alerts from JSON file (persists across bridge restarts)."""
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
    """Save alerts to JSON file."""
    try:
        with open(ALERTS_FILE, "w") as f:
            json.dump(alerts, f, indent=4)
    except Exception as e:
        log.error("Error saving alerts: %s", e)


def add_alert(alert_data):
    """Add a new alert and persist it to alerts.json."""
    alerts = load_alerts()
    alert = {
        "id": uuid.uuid4().hex,
        "timestamp": get_ist_now(),
        **alert_data,
    }
    alerts.insert(0, alert)  # Newest first
    if len(alerts) > MAX_ALERTS:
        alerts = alerts[:MAX_ALERTS]
    save_alerts(alerts)
    return alert


def periodic_cleanup():
    """Periodically scan positions.json for terminal statuses and move to history"""
    log.info("Starting periodic cleanup thread (runs every 5 seconds)")
    while not cleanup_stop_event.is_set():
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

        # Wait for 5 seconds or until stop event is set
        cleanup_stop_event.wait(timeout=5.0)


def is_monthly_expiry(expiry):
    """
    NIFTY monthly expiry = last Tuesday of month
    """
    cal = monthcalendar(expiry.year, expiry.month)

    # Tuesday column = index 1
    last_tuesday = max(week[1] for week in cal if week[1] != 0)

    return (
        expiry.weekday() == 1  # Tuesday
        and expiry.day == last_tuesday
    )


def tv_to_xts_description(tv_ticker: str) -> Optional[str]:
    """
    TradingView:
        NIFTY260625C27000

    Monthly XTS:
        NIFTY26JUN27000CE

    Weekly XTS:
        NIFTY2661827000CE
    """

    tv_ticker = tv_ticker.strip().upper()

    match = re.match(
        r"^([A-Z]+)(\d{6})([CP])(\d+(?:\.\d+)?)$",
        tv_ticker,
    )

    if not match:
        return None

    underlying = match.group(1)
    expiry_str = match.group(2)
    option_flag = match.group(3)
    strike = match.group(4)

    try:
        expiry = datetime.strptime(expiry_str, "%y%m%d")
    except ValueError:
        return None

    option_type = "CE" if option_flag == "C" else "PE"

    if strike.endswith(".0"):
        strike = str(int(float(strike)))

    if is_monthly_expiry(expiry):
        # Example: NIFTY26JUN27000CE
        expiry_part = expiry.strftime("%y%b").upper()
    else:
        # Example: NIFTY2661827000CE
        expiry_part = f"{expiry.year % 100}{expiry.month}{expiry.day:02d}"

    return f"{underlying}{expiry_part}{strike}{option_type}"


# def is_monthly_expiry(expiry: datetime) -> bool:
#     """
#     Returns True if expiry is the last Thursday of the month.
#     """
#     cal = monthcalendar(expiry.year, expiry.month)

#     thursdays = [week[3] for week in cal if week[3] != 0]

#     return expiry.day == thursdays[-1]


# def tv_to_xts_description(tv_ticker: str) -> Optional[str]:
#     """
#     Convert TradingView option ticker format to XTS master Description format.

#     Examples:

#     Monthly expiry:
#         NIFTY260625C25000
#         -> NIFTY26JUN25000CE

#     Weekly expiry:
#         NIFTY260623C24650
#         -> NIFTY2662324650CE
#     """

#     tv_ticker = tv_ticker.strip().upper()

#     match = re.match(
#         r"^([A-Z]+)(\d{6})([CP])(\d+(?:\.\d+)?)$",
#         tv_ticker,
#     )

#     if not match:
#         return None

#     underlying = match.group(1)
#     expiry_str = match.group(2)
#     option_flag = match.group(3)
#     strike = match.group(4)

#     try:
#         expiry = datetime.strptime(expiry_str, "%y%m%d")
#     except ValueError:
#         return None

#     option_type = "CE" if option_flag == "C" else "PE"

#     if strike.endswith(".0"):
#         strike = str(int(float(strike)))

#     # Monthly expiry → NIFTY26JUN25000CE
#     if is_monthly_expiry(expiry):
#         expiry_part = expiry.strftime("%y%b").upper()

#     # Weekly expiry → NIFTY2662324650CE
#     else:
#         expiry_part = f"{expiry.year % 100}{expiry.month}{expiry.day:02d}"

#     return f"{underlying}{expiry_part}{strike}{option_type}"


# def tv_to_xts_description(tv_ticker: str) -> Optional[str]:
#     """
#     Convert TradingView option ticker format to XTS master Description format.

#     Example:
#         RELIANCE260630C1230  -> RELIANCE26JUN1230CE
#         NIFTY260625P25000    -> NIFTY26JUN25000PE
#     """

#     tv_ticker = tv_ticker.strip().upper()

#     match = re.match(r"^([A-Z]+)(\d{6})([CP])(\d+(?:\.\d+)?)$", tv_ticker)

#     if not match:
#         return None

#     underlying = match.group(1)
#     expiry_str = match.group(2)
#     option_flag = match.group(3)
#     strike = match.group(4)

#     expiry = datetime.strptime(expiry_str, "%y%m%d")

#     expiry_part = expiry.strftime("%y%b").upper()

#     option_type = "CE" if option_flag == "C" else "PE"

#     if strike.endswith(".0"):
#         strike = str(int(float(strike)))

#     return f"{underlying}{expiry_part}{strike}{option_type}"


# Cached master loaders, one per exchange segment (CSV parsing is expensive)
_master_loaders: Dict[str, Optional[ContractLoader]] = {}


def get_master_loader(segment: str) -> Optional[ContractLoader]:
    """Return a cached ContractLoader for the given segment, or None if no CSV."""
    seg = segment.strip().upper()
    if seg not in _master_loaders:
        path = MASTER_DIR / f"{seg}.csv"
        if not path.exists():
            log.warning("Master data file not found for segment %s: %s", seg, path)
            _master_loaders[seg] = None
        else:
            _master_loaders[seg] = ContractLoader(path)
    return _master_loaders[seg]


def parse_tv_option_ticker(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Parse a TradingView option ticker like NIFTY260625C27000 or BSX260723C81100
    into its components. Returns None if it does not look like an option ticker.
    """
    match = re.match(
        r"^([A-Z][A-Z0-9&\-]*?)(\d{6})([CP])(\d+(?:\.\d+)?)$",
        ticker.strip().upper(),
    )
    if not match:
        return None

    try:
        expiry = datetime.strptime(match.group(2), "%y%m%d").date()
    except ValueError:
        return None

    name = match.group(1)
    return {
        "name": TV_SYMBOL_ALIASES.get(name, name),
        "expiry": expiry,
        "option_type_csv": "3" if match.group(3) == "C" else "4",
        "strike": float(match.group(4)),
    }


def _segments_to_search(segment_hint: Optional[str]) -> list:
    if segment_hint:
        seg = segment_hint.strip().upper()
        if seg:
            return [seg]
    return list(MASTER_SEGMENTS)


async def resolve_contract_by_ticker(
    ticker: str,
    segment_hint: Optional[str] = None,
) -> Optional[dict]:
    """
    Resolve a ticker/symbol to a master contract row, searching NSE, BSE and MCX.

    Supports:
      - TradingView option tickers (NIFTY260625C27000, BSX260723C81100, ...)
        matched by underlying + expiry + strike + option type
      - Exact Description match (NIFTY26JUN27000CE, SENSEX2672373400PE,
        CRUDEOILM17AUG20265350CE, RELIANCE-EQ, ...)
      - Plain equity symbols (RELIANCE) resolved from cash-market masters
    """
    if not ticker:
        return None

    ticker = ticker.strip().upper()
    segments = _segments_to_search(segment_hint)

    parsed = parse_tv_option_ticker(ticker)
    if parsed:
        for seg in segments:
            loader = get_master_loader(seg)
            if not loader:
                continue
            for contract in loader.contracts:
                if contract.get("Name", "").strip().upper() != parsed["name"]:
                    continue
                if contract.get("OptionType", "").strip() != parsed["option_type_csv"]:
                    continue
                try:
                    if float(contract.get("StrikePrice") or 0) != parsed["strike"]:
                        continue
                    expiry_date = datetime.fromisoformat(
                        contract["ContractExpiration"]
                    ).date()
                except (ValueError, KeyError, TypeError):
                    continue
                if expiry_date == parsed["expiry"]:
                    return contract

        # Legacy fallback: NSE-style description built from the TV ticker
        xts_description = tv_to_xts_description(ticker)
        if xts_description:
            xts_description = xts_description.upper()
            for seg in segments:
                loader = get_master_loader(seg)
                if not loader:
                    continue
                for contract in loader.contracts:
                    if (
                        contract.get("Description", "").strip().upper()
                        == xts_description
                    ):
                        return contract
        return None

    # Exact Description / NameWithSeries match across all segments
    for seg in segments:
        loader = get_master_loader(seg)
        if not loader:
            continue
        for contract in loader.contracts:
            if (
                contract.get("Description", "").strip().upper() == ticker
                or contract.get("NameWithSeries", "").strip().upper() == ticker
            ):
                return contract

    # Plain equity symbol: match by Name in cash-market segments
    for seg in segments:
        if not seg.endswith("CM"):
            continue
        loader = get_master_loader(seg)
        if not loader:
            continue
        for contract in loader.contracts:
            if contract.get("Name", "").strip().upper() == ticker:
                return contract

    return None


def find_contract_by_instrument_id(
    segment: str, exchange_instrument_id: int
) -> Optional[dict]:
    """Look up a master contract row by exchange segment and instrument ID."""
    loader = get_master_loader(segment)
    if not loader:
        return None
    target = str(exchange_instrument_id)
    for contract in loader.contracts:
        if contract.get("ExchangeInstrumentID", "").strip() == target:
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

        instrument_key = str(contract["exchange_instrument_id"])

        # Wait for ORDER_ACK with status tracking
        ack = await client.wait_for_ack(signal_id, timeout=30.0)

        if ack:
            status = ack.get("status", "").upper()
            log.info("Order ack received for %s: %s", signal_id, status)

            if status not in ["REJECTED", "CANCELLED", "EXPIRED", "ERROR"]:
                # Save position once acknowledged
                positions = load_positions()
                positions[instrument_key] = {
                    "side": "BUY",
                    "qty": quantity,
                    "instrument": contract["instrument_name"],
                    "exchange_instrument_id": contract["exchange_instrument_id"],
                    "exchange_segment": contract.get("exchange_segment", "NSEFO"),
                    "opened_at": get_ist_now(),
                    "signal_id": signal_id,
                    "oms_order_id": ack.get("oms_order_id"),
                    "status": status,
                    "entry_price": pending_orders.get(signal_id, {}).get("limit_price"),
                }
                save_positions(positions)
                log.info("Position saved for %s", contract["instrument_name"])

                # Update pending order status
                if signal_id in pending_orders:
                    pending_orders[signal_id]["status"] = "acknowledged"
                    pending_orders[signal_id]["response"] = ack

            else:
                log.warning(
                    "Order failed at ack with status: %s for signal_id: %s",
                    status,
                    signal_id,
                )
                if signal_id in pending_orders:
                    pending_orders[signal_id]["status"] = "failed"
                    pending_orders[signal_id]["response"] = ack
        else:
            log.warning("Timeout waiting for ORDER_ACK for signal_id: %s", signal_id)
            if signal_id in pending_orders:
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
        explicit_segment = signal.get("exchange_segment") or signal.get(
            "exchangeSegment"
        )
        explicit_instrument_id = signal.get("exchange_instrument_id") or signal.get(
            "exchangeInstrumentID"
        )

        if not action or not position:
            return {
                "status": "error",
                "message": "Missing required fields: action, position",
            }

        def _safe_int(value, default=0):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return default

        def _contract_from_master(contract_data: Dict[str, Any]) -> Dict[str, Any]:
            csv_opt = str(contract_data.get("OptionType", "")).strip()
            opt = "CE" if csv_opt == "3" else "PE" if csv_opt == "4" else option_type
            return {
                "exchange_segment": contract_data["ExchangeSegment"],
                "exchange_instrument_id": int(contract_data["ExchangeInstrumentID"]),
                "instrument_name": contract_data["Description"],
                "lot_size": max(_safe_int(contract_data.get("LotSize"), 1), 1),
                "strike": _safe_int(contract_data.get("StrikePrice")),
                "option_type": opt,
            }

        contract_data = None
        limit_price = None

        if explicit_segment and explicit_instrument_id:
            # Manual/explicit signal: segment + instrument ID given directly
            explicit_segment = str(explicit_segment).strip().upper()
            explicit_instrument_id = int(explicit_instrument_id)

            master_row = find_contract_by_instrument_id(
                explicit_segment, explicit_instrument_id
            )
            if master_row:
                contract = _contract_from_master(master_row)
            else:
                # Fall back to signal-supplied fields when the master lacks the row
                contract = {
                    "exchange_segment": explicit_segment,
                    "exchange_instrument_id": explicit_instrument_id,
                    "instrument_name": signal.get("instrument_name")
                    or signal.get("instrumentName")
                    or str(explicit_instrument_id),
                    "lot_size": max(_safe_int(signal.get("lot_size"), 1), 1),
                    "strike": 0,
                    "option_type": option_type,
                }

            log.info(
                "Explicit instrument signal: %s @ %s (id=%d)",
                contract["instrument_name"],
                contract["exchange_segment"],
                contract["exchange_instrument_id"],
            )

            if signal.get("limitPrice") or signal.get("limit_price"):
                limit_price = float(
                    signal.get("limitPrice") or signal.get("limit_price")
                )
            else:
                limit_price = await get_ltp_for_contract(
                    {
                        "ExchangeInstrumentID": contract["exchange_instrument_id"],
                        "ExchangeSegment": contract["exchange_segment"],
                    }
                )
                if limit_price is None:
                    return {
                        "status": "error",
                        "message": (
                            "Could not fetch LTP for instrument "
                            f"{contract['exchange_instrument_id']} on {contract['exchange_segment']}"
                        ),
                    }
        elif ticker:
            contract_data = await resolve_contract_by_ticker(
                ticker, segment_hint=explicit_segment
            )
            if not contract_data:
                return {
                    "status": "error",
                    "message": f"Contract not found for ticker: {ticker}",
                }

            log.info(
                "Resolved ticker contract: %s @ %s",
                contract_data.get("Description"),
                contract_data.get("ExchangeSegment"),
            )

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

            contract = _contract_from_master(contract_data)
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

        # Determine if the position is currently active
        is_valid_position = False
        if current_position:
            status = current_position.get("status", "")
            is_valid_position = status not in [
                "REJECTED",
                "CANCELLED",
                "EXPIRED",
                "ERROR",
            ]

        log.info(
            "Received signal: Action=%s, Position=%s, Qty=%d, Instrument=%s",
            action,
            position,
            quantity,
            contract["instrument_name"],
        )

        # Handle SELL signals - close existing BUY position
        if action == "SELL":
            if not is_valid_position:
                return {
                    "status": "ignored",
                    "message": f"No valid open position found for {contract['instrument_name']}",
                }

            log.info(
                "SELL received. Closing existing BUY position for %s",
                contract["instrument_name"],
            )

            sig_id = uuid.uuid4().hex

            # Determine reverse side and quantity
            pos_side = current_position.get("side", "BUY")
            reverse_side = "SELL" if pos_side.upper() == "BUY" else "BUY"
            pos_qty = current_position.get("qty", 0)

            # Link the square-off signal to the position so background tracker can pop it on fill
            positions[instrument_key]["squareoff_signal_id"] = sig_id
            save_positions(positions)

            await client.place_order(
                exchange_segment=contract["exchange_segment"],
                exchange_instrument_id=contract["exchange_instrument_id"],
                instrument_name=contract["instrument_name"],
                product_type=product_type,
                order_type="MARKET",
                order_side=reverse_side,
                time_in_force="DAY",
                order_quantity=pos_qty,
                limit_price=0.0,
                signal_id=sig_id,
            )

            # Wait for ORDER_ACK
            log.info("Waiting for ORDER_ACK from OMS (timeout 10s)...")
            ack = await client.wait_for_ack(sig_id, timeout=10.0)

            if ack:
                log.info(
                    "Square-off order submitted for: %s",
                    contract["instrument_name"],
                )

                return {
                    "status": "submitted",
                    "msg_type": "SQUAREOFF",
                    "signal_id": sig_id,
                    "instrument": contract["instrument_name"],
                    "timestamp": get_ist_now(),
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
            if not is_valid_position:
                return {
                    "status": "ignored",
                    "message": f"No valid open position found for {contract['instrument_name']}",
                }

            log.info("Processing square-off for %s...", contract["instrument_name"])
            sig_id = uuid.uuid4().hex

            # Determine reverse side and quantity
            pos_side = current_position.get("side", "BUY")
            reverse_side = "SELL" if pos_side.upper() == "BUY" else "BUY"
            pos_qty = current_position.get("qty", 0)

            # Link the square-off signal to the position so background tracker can pop it on fill
            positions[instrument_key]["squareoff_signal_id"] = sig_id
            save_positions(positions)

            await client.place_order(
                exchange_segment=contract["exchange_segment"],
                exchange_instrument_id=contract["exchange_instrument_id"],
                instrument_name=contract["instrument_name"],
                product_type=product_type,
                order_type="MARKET",
                order_side=reverse_side,
                time_in_force="DAY",
                order_quantity=pos_qty,
                limit_price=0.0,
                signal_id=sig_id,
            )

            # Wait for ORDER_ACK
            log.info("Waiting for ORDER_ACK from OMS (timeout 10s)...")
            ack = await client.wait_for_ack(sig_id, timeout=10.0)

            if ack:
                log.info(
                    "Square-off order submitted for: %s",
                    contract["instrument_name"],
                )

                return {
                    "status": "submitted",
                    "msg_type": "SQUAREOFF",
                    "signal_id": sig_id,
                    "timestamp": get_ist_now(),
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
            if is_valid_position:
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
                "timestamp": get_ist_now(),
                "limit_price": limit_price,
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
                add_alert(
                    {
                        "type": "SIGNAL",
                        "message": f"Received {signal.get('action', 'UNKNOWN')} signal",
                        "data": signal,
                    }
                )

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
                self.send_header("Access-Control-Allow-Origin", "*")
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
                self.send_header("Access-Control-Allow-Origin", "*")
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
                positions = asyncio.run(enrich_positions_for_display(load_positions()))
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
            # Endpoint to get alerts (loaded from disk so they survive restarts)
            try:
                alerts = load_alerts()
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
        elif self.path == "/history":
            # Endpoint to get history
            try:
                history = load_history()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(history).encode("utf-8"))
            except Exception as e:
                log.exception("History endpoint error:")
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
                        json.dumps(
                            {"status": "error", "message": "Missing instrument_key"}
                        ).encode()
                    )
                    return

                positions = load_positions()
                if instrument_key not in positions:
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps(
                            {"status": "error", "message": "Position not found"}
                        ).encode()
                    )
                    return

                position = positions[instrument_key]
                sig_id = uuid.uuid4().hex

                # Determine reverse side and quantity
                pos_side = position.get("side", "BUY")
                reverse_side = "SELL" if pos_side.upper() == "BUY" else "BUY"
                pos_qty = position.get("qty", 0)

                # Link the square-off signal to the position
                positions[instrument_key]["squareoff_signal_id"] = sig_id
                save_positions(positions)

                future = asyncio.run_coroutine_threadsafe(
                    client.place_order(
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
                    loop,
                )
                future.result(timeout=5.0)

                # Wait for ORDER_ACK
                ack_future = asyncio.run_coroutine_threadsafe(
                    client.wait_for_ack(sig_id, timeout=10.0), loop
                )
                ack = ack_future.result(timeout=10.0)

                if ack:
                    add_alert(
                        {
                            "type": "SQUAREOFF",
                            "message": f"Manual square off order submitted for {position['instrument']}",
                            "instrument": position["instrument"],
                        }
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps(
                            {
                                "status": "success",
                                "message": "Square off order submitted",
                            }
                        ).encode("utf-8")
                    )
                else:
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps(
                            {
                                "status": "error",
                                "message": "Failed to submit square off order",
                            }
                        ).encode("utf-8")
                    )
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
        if (
            self.path == "/status"
            or self.path == "/positions"
            or self.path == "/alerts"
            or self.path == "/history"
        ):
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

        if not status and msg_type.startswith("ORDER_"):
            status = msg_type.replace("ORDER_", "")

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
                "timestamp": get_ist_now(),
            }

        # Update positions
        try:
            positions = load_positions()
            updated = False
            str_oms_id = str(oms_id) if oms_id != "N/A" else "N/A"
            str_signal_id = str(signal_id) if signal_id else ""

            for key, pos in list(positions.items()):
                pos_oms_id = str(pos.get("oms_order_id", ""))
                pos_sig_id = str(pos.get("signal_id", ""))
                pos_sq_sig_id = str(pos.get("squareoff_signal_id", ""))

                # Check if this update belongs to the original opening order
                if (str_oms_id != "N/A" and pos_oms_id == str_oms_id) or (
                    str_signal_id and pos_sig_id == str_signal_id
                ):
                    # Only update if there is a valid status string
                    if status:
                        pos["status"] = status
                        updated = True

                # Check if this update belongs to the reverse square-off order
                elif str_signal_id and pos_sq_sig_id == str_signal_id:
                    if status in ["FILLED", "COMPLETE"]:
                        append_to_history(pos, status)
                        positions.pop(key, None)
                        updated = True
                        log.info(
                            "Position %s removed due to successful square-off fill.",
                            pos.get("instrument"),
                        )
                    elif status in ["REJECTED", "CANCELLED", "ERROR", "EXPIRED"]:
                        pos.pop("squareoff_signal_id", None)
                        updated = True
                        log.warning(
                            "Square-off order failed/cancelled for %s. Unlinked squareoff_signal_id.",
                            pos.get("instrument"),
                        )

            if updated:
                save_positions(positions)
        except Exception as e:
            log.error("Failed to update position status in on_response: %s", e)

    log.info("OMS Client connected. Starting HTTP thread...")

    # Start HTTP server in daemon thread
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()

    # Start periodic cleanup thread
    cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
    cleanup_thread.start()

    log.info("Nifty Signal Bridge is fully operational. Press Ctrl+C to terminate.")
    log.info("Endpoints:")
    log.info("  POST /signal - Submit trade signal")
    log.info("  GET  /status?signal_id=xxx - Check order status")
    log.info("  GET  /positions - View current positions")
    log.info("  GET  /alerts - View recent alerts")
    log.info("  GET  /history - View position history")
    log.info("  POST /squareoff - Manually square off a position")

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        log.info("Shutting down...")
        cleanup_stop_event.set()
        await client.disconnect()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bridge stopped by user.")
