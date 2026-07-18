"""Live LTP fetch and position market-data hydration for the bridge dashboard."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from market_data import provide_xts_client

from bridge.positions import get_ist_now, get_position_display_values, load_positions

log = logging.getLogger("NIFTY_BRIDGE")


async def get_ltp_for_contract(contract_data: Dict[str, Any]) -> Optional[float]:
    """Fetch the live LTP for a specific contract using the XTS marketdata client."""
    xts_client = provide_xts_client()
    try:
        await xts_client.connect()
        return await xts_client.get_ltp(
            int(contract_data["ExchangeInstrumentID"]),
            contract_data.get("ExchangeSegment", "NSEFO"),
        )
    finally:
        await xts_client.disconnect()


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
