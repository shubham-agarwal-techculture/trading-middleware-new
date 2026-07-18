"""
Contract resolution — TradingView tickers → XTS master rows.

Resolution modes (tried in order for option tickers):
  1. Underlying + expiry + strike + option type match
  2. Legacy NSE-style description built via ``tv_to_xts_description``
  3. Exact Description / NameWithSeries match
  4. Cash-market Name match for plain equity symbols
"""

from __future__ import annotations

import logging
import re
from calendar import monthcalendar
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from market_data.contracts import ContractLoader

log = logging.getLogger("NIFTY_BRIDGE")

MASTER_DIR = Path("master_data")
MASTER_SEGMENTS = ["NSEFO", "BSEFO", "MCXFO", "NSECM", "BSECM"]

TV_SYMBOL_ALIASES = {
    "BSX": "SENSEX",
    "BKX": "BANKEX",
}

_master_loaders: Dict[str, Optional[ContractLoader]] = {}


def is_monthly_expiry(expiry) -> bool:
    """Return True if *expiry* is the last Tuesday of its month (NIFTY monthly)."""
    cal = monthcalendar(expiry.year, expiry.month)
    last_tuesday = max(week[1] for week in cal if week[1] != 0)
    return expiry.weekday() == 1 and expiry.day == last_tuesday


def tv_to_xts_description(tv_ticker: str) -> Optional[str]:
    """Convert a TradingView option ticker to an XTS Description string."""
    tv_ticker = tv_ticker.strip().upper()
    match = re.match(r"^([A-Z]+)(\d{6})([CP])(\d+(?:\.\d+)?)$", tv_ticker)
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
        expiry_part = expiry.strftime("%y%b").upper()
    else:
        expiry_part = f"{expiry.year % 100}{expiry.month}{expiry.day:02d}"

    return f"{underlying}{expiry_part}{strike}{option_type}"


def get_master_loader(segment: str) -> Optional[ContractLoader]:
    """Return a cached ContractLoader for *segment*, or None if no CSV exists."""
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
    """Parse a TradingView option ticker into name/expiry/type/strike components."""
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
    """Resolve a ticker/symbol to a master contract row across exchange CSVs."""
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
