"""Compatibility shim — use ``market_data`` package."""

from market_data import (  # noqa: F401
    ContractLoader,
    XTSMarketDataClient,
    get_atm_data,
    provide_xts_client,
    segment_to_code,
)

if __name__ == "__main__":
    import asyncio
    from market_data.atm import get_atm_data as _get

    async def _main():
        data = await _get()
        print("ATM Strike:", data["atm_strike"])
        print("CE LTP:", data["ce_ltp"])
        print("PE LTP:", data["pe_ltp"])

    asyncio.run(_main())
