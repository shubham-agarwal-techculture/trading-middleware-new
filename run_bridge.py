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
    try:
        state.atm_data = await get_atm_data()
        log.info("Initial ATM data loaded: strike=%d", state.atm_data["atm_strike"])
    except Exception as e:
        state.atm_data = None
        log.warning(
            "Initial ATM fetch failed (%s); bridge will start anyway and retry on demand",
            e,
        )

    state.loop = asyncio.get_running_loop()
    state.client = OMSClient(
        strategy_id=state.STRATEGY_ID,
        push_address=state.OMS_PUSH,
        sub_address=state.OMS_SUB,
    )

    log.info(
        "Connecting to OMS at push=%s sub=%s...", state.OMS_PUSH, state.OMS_SUB
    )
    try:
        await state.client.connect()
    except Exception as e:
        log.error("Failed to connect to OMS: %s", e)
        raise
    state.client.on_response(on_oms_response)

    log.info("OMS Client connected. Starting HTTP thread...")
    threading.Thread(target=run_http_server, daemon=True).start()
    threading.Thread(target=periodic_cleanup, daemon=True).start()

    log.info("Signal Bridge is fully operational. Press Ctrl+C to terminate.")
    if state.atm_data is None:
        log.warning(
            "Running without ATM data; ATM CE/PE signals will retry market data on each request."
        )
    log.info("  POST /signal | GET /status | GET /positions | GET /alerts | GET /history | POST /squareoff")

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        log.info("Shutting down...")
        state.cleanup_stop_event.set()
        if state.client is not None:
            try:
                await state.client.disconnect()
            except Exception as e:
                log.warning("Error during OMS disconnect: %s", e)
        log.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bridge stopped by user.")
    except Exception:
        log.exception("Bridge exited due to an unhandled error")
        raise
