"""Compatibility shim — use ``run_bridge`` or ``bridge`` package directly."""

from run_bridge import main  # noqa: F401
from bridge import (  # noqa: F401
    MASTER_SEGMENTS,
    _master_loaders,
    find_contract_by_instrument_id,
    get_position_display_values,
    is_monthly_expiry,
    parse_tv_option_ticker,
    resolve_contract_by_ticker,
    tv_to_xts_description,
)

if __name__ == "__main__":
    import asyncio
    from run_bridge import main as _main

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
