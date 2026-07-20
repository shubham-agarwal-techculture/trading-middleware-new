"""
OMS Server — entry point.

Run this script to start the Order Management System:

    python run_oms.py
    python run_oms.py --config path/to/config.yaml

The server will:
  1. Load configuration
  2. Set up logging
  3. Log in to the broker
  4. Restore any active orders from disk
  5. Bind ZMQ PULL (order signals) and PUB (responses) sockets
  6. Start order workers and background sync
  7. Handle SIGINT / SIGTERM for clean shutdown

Broker event integration
------------------------
XTS Interactive sends order status updates via Socket.IO events.
Hook your existing socket listener to call:

    await order_manager.inject_broker_event(
        XTSBrokerAdapter.parse_order_event(raw_event)
    )

This is the bridge between your market-data pub/sub system and the OMS.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys

from oms.utils.runtime import use_selector_event_loop_policy

# zmq's asyncio sockets need the Selector loop on Windows (see runtime helper).
use_selector_event_loop_policy()

from oms.broker.factory import create_broker
from oms.broker.xts_socket import attach_xts_socket
from oms.config import load_config
from oms.core.order_manager import OrderManager
from oms.core.position_tracker import PositionTracker
from oms.storage.file_store import FileStore
from oms.utils.logger import setup_logging, get_logger, get_xts_logger, get_exchange1_logger


async def main(config_path: str = "config.yaml") -> None:
    cfg = load_config(config_path)

    oms_log_path, session_dt = setup_logging(
        level=cfg.logging.level,
        log_dir=cfg.logging.log_dir,
        log_file=cfg.logging.log_file,
        rotation_size_mb=cfg.logging.rotation_size_mb,
        backup_count=cfg.logging.backup_count,
        timezone=cfg.oms.timezone,
    )
    _, xts_log_path = get_xts_logger(
        cfg.logging.log_dir,
        cfg.logging.xts_log_file,
        session_datetime=session_dt,
    )
    _, ex1_log_path = get_exchange1_logger(
        cfg.logging.log_dir,
        cfg.logging.exchange1_log_file,
        session_datetime=session_dt,
    )
    log = get_logger("run_oms")

    log.info("=== Order Management System Starting ===", version="1.0.0")
    log.info(
        "Session log files",
        oms_log=str(oms_log_path),
        xts_log=str(xts_log_path),
        exchange1_log=str(ex1_log_path),
        session_datetime=session_dt,
    )
    log.info("Configuration loaded", config_path=config_path)

    # --- Storage ---
    file_store = FileStore(cfg.storage, timezone=cfg.oms.timezone)

    # --- Position tracker ---
    position_tracker = PositionTracker(file_store)
    await position_tracker.load()

    # --- Broker (XTS + optional Exchange1 crypto via router) ---
    broker = create_broker(cfg.broker, cfg.crypto_broker)
    if cfg.crypto_broker and getattr(cfg.crypto_broker, "enabled", True):
        log.info(
            "Broker routing enabled",
            primary=cfg.broker.type,
            crypto=getattr(cfg.crypto_broker, "type", "exchange1"),
            crypto_segments=["CRYPTO", "EXCHANGE1", "SPOT"],
        )
    else:
        log.warning(
            "No crypto_broker in config — CRYPTO segment orders will be sent to XTS and fail. "
            "Add crypto_broker + EXCHANGE1_* env vars (see .env.example)."
        )
    try:
        await broker.login()
    except Exception as exc:
        log.error("Broker login failed — OMS will start but cannot place orders", error=str(exc))
        log.warning("Continuing in degraded mode (broker not connected)")

    # --- Order Manager ---
    order_manager = OrderManager(
        config=cfg.oms,
        broker=broker,
        file_store=file_store,
        position_tracker=position_tracker,
    )

    # --- XTS Interactive Socket.IO (real-time order/trade events) ---
    xts_socket = None
    xts_adapter = getattr(broker, "xts", broker)
    if cfg.broker.socket_enabled and getattr(xts_adapter, "token", None):
        try:
            xts_socket = attach_xts_socket(
                xts_adapter,
                order_manager,
                verify_ssl=cfg.broker.verify_ssl,
                reconnect=cfg.broker.socket_reconnect,
            )
            log.info("XTS interactive socket feed started")
        except Exception as exc:
            log.error("Failed to start XTS socket feed", error=str(exc))
    else:
        log.warning(
            "XTS socket feed disabled or broker not logged in — "
            "fill updates rely on order-book polling only"
        )

    # --- Graceful shutdown on SIGINT / SIGTERM ---
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal(sig: signal.Signals) -> None:
        log.info("Shutdown signal received", signal=sig.name)
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except NotImplementedError:
            # Windows does not support add_signal_handler for SIGTERM
            pass

    log.info("Starting OMS engine ...")

    # Run OMS as a task so we can also wait for shutdown event
    oms_task = asyncio.create_task(order_manager.start(), name="oms-main")

    # On Windows, keyboard interrupt handling via signal is limited;
    # we also catch CancelledError directly.
    try:
        await shutdown_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        log.info("Stopping OMS ...")
        if xts_socket is not None:
            xts_socket.stop()
        await order_manager.stop()
        oms_task.cancel()
        try:
            await oms_task
        except (asyncio.CancelledError, Exception):
            pass
        log.info("OMS stopped cleanly.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Order Management System Server")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(main(args.config))
    except KeyboardInterrupt:
        print("\nOMS interrupted by user.")
        sys.exit(0)
