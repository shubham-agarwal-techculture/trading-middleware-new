"""
Signal Bridge launcher.

Usage:
    python run_bridge.py --port 5002
"""

from __future__ import annotations

import asyncio
import logging
import threading

from oms.utils.runtime import use_selector_event_loop_policy

use_selector_event_loop_policy()

from bridge import state  # noqa: E402
from bridge.http_server import run_http_server  # noqa: E402
from bridge.positions import periodic_cleanup  # noqa: E402
from bridge.signal_service import on_oms_response  # noqa: E402
from clients.oms_client import OMSClient  # noqa: E402
from market_data import get_atm_data  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("NIFTY_BRIDGE")


async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Signal Bridge")
    parser.add_argument(
        "--port",
        type=int,
        default=state.DEFAULT_PORT,
        help=f"HTTP port (default: {state.DEFAULT_PORT})",
    )
    args = parser.parse_args()
    state.http_port = args.port

    log.info("Starting Signal Bridge...")
    log.info("Configuration: port=%d", state.http_port)

    log.info("Fetching initial ATM data...")
    state.atm_data = await get_atm_data()
    log.info("Initial ATM data loaded: strike=%d", state.atm_data["atm_strike"])

    state.loop = asyncio.get_running_loop()
    state.client = OMSClient(
        strategy_id=state.STRATEGY_ID,
        push_address=state.OMS_PUSH,
        sub_address=state.OMS_SUB,
    )

    log.info(
        "Connecting to OMS at push=%s sub=%s...", state.OMS_PUSH, state.OMS_SUB
    )
    await state.client.connect()
    state.client.on_response(on_oms_response)

    log.info("OMS Client connected. Starting HTTP thread...")
    threading.Thread(target=run_http_server, daemon=True).start()
    threading.Thread(target=periodic_cleanup, daemon=True).start()

    log.info("Signal Bridge is fully operational. Press Ctrl+C to terminate.")
    log.info("  POST /signal | GET /status | GET /positions | GET /alerts | GET /history | POST /squareoff")

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        log.info("Shutting down...")
        state.cleanup_stop_event.set()
        await state.client.disconnect()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bridge stopped by user.")
