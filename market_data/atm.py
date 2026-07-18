"""NIFTY ATM option discovery and LTP fetch."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

from market_data.contracts import STRIKE_INTERVAL, ContractLoader
from market_data.xts_client import XTSMarketDataClient
from oms.utils.env import env

log = logging.getLogger("NIFTY_ATM_LTP")

CSV_PATH = Path("master_data/NSEFO.csv")
XTS_API_KEY = env("XTS_MD_API_KEY")
XTS_API_SECRET = env("XTS_MD_API_SECRET")


async def get_atm_data() -> Dict[str, Any]:
    """Get ATM option data for NIFTY (CE + PE LTP and contracts)."""
    if not XTS_API_KEY or not XTS_API_SECRET:
        raise ValueError("XTS_API_KEY and XTS_API_SECRET must be set")

    loader = ContractLoader(CSV_PATH)
    futures_contract = loader.get_nearest_expiry_futures()
    if not futures_contract:
        raise Exception("Could not find nearest expiry futures contract")

    client = XTSMarketDataClient(XTS_API_KEY, XTS_API_SECRET)
    await client.connect()

    futures_id = int(futures_contract["ExchangeInstrumentID"])
    futures_ltp = await client.get_ltp(futures_id, futures_contract["ExchangeSegment"])
    if futures_ltp is None:
        await client.disconnect()
        raise Exception("Could not get futures LTP")

    ce_contract = loader.get_atm_options(futures_ltp, option_type="CE")
    if not ce_contract:
        await client.disconnect()
        raise Exception("Could not find ATM CE contract")
    ce_id = int(ce_contract["ExchangeInstrumentID"])
    ce_ltp = await client.get_ltp(ce_id, ce_contract["ExchangeSegment"])
    if ce_ltp is None:
        await client.disconnect()
        raise Exception("Could not get ATM CE LTP")

    pe_contract = loader.get_atm_options(futures_ltp, option_type="PE")
    if not pe_contract:
        await client.disconnect()
        raise Exception("Could not find ATM PE contract")
    pe_id = int(pe_contract["ExchangeInstrumentID"])
    pe_ltp = await client.get_ltp(pe_id, pe_contract["ExchangeSegment"])
    if pe_ltp is None:
        await client.disconnect()
        raise Exception("Could not get ATM PE LTP")

    atm_strike = round(futures_ltp / STRIKE_INTERVAL) * STRIKE_INTERVAL
    await client.disconnect()

    return {
        "ce_instrument_id": ce_id,
        "ce_ltp": ce_ltp,
        "pe_instrument_id": pe_id,
        "pe_ltp": pe_ltp,
        "atm_strike": atm_strike,
        "ce_contract": ce_contract,
        "pe_contract": pe_contract,
    }


def provide_xts_client() -> XTSMarketDataClient:
    """Create an unconnected XTSMarketDataClient from env credentials."""
    return XTSMarketDataClient(XTS_API_KEY, XTS_API_SECRET)
