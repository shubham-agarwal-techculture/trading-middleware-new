"""
Persistent file storage for the OMS.

Files written:
  data/orders_log_YYYYMMDD.csv   — append-only order event log (audit trail)
  data/orders_state.json         — current snapshot of all active + recent orders
  data/trades_YYYYMMDD.csv       — append-only fill/trade log
  data/positions.json            — current open positions snapshot
  data/statistics_YYYYMMDD.json  — daily aggregated statistics
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from oms.utils.logger import get_logger
from oms.utils.timeutil import now_iso

log = get_logger(__name__)

# CSV column definitions
_ORDER_LOG_FIELDS = [
    "ts", "event", "oms_order_id", "strategy_id", "signal_id",
    "exchange_segment", "exchange_instrument_id", "instrument_name",
    "product_type", "order_type", "order_side", "time_in_force",
    "order_quantity", "limit_price", "stop_price", "disclosed_quantity",
    "status", "broker_order_id", "order_unique_identifier",
    "filled_quantity", "pending_quantity", "avg_fill_price",
    "last_fill_price", "last_fill_quantity",
    "reject_reason", "cancel_reason", "error_message", "tags",
    "exchange_transact_time", "last_update_time",
]

_TRADE_LOG_FIELDS = [
    "ts", "oms_order_id", "broker_order_id", "strategy_id",
    "exchange_segment", "exchange_instrument_id", "instrument_name",
    "order_side", "product_type",
    "fill_quantity", "fill_price", "filled_quantity_total",
    "avg_fill_price", "pending_quantity",
]


class FileStore:
    """Thread-safe (asyncio.Lock) CSV/JSON storage layer."""

    def __init__(self, config, timezone: str = "Asia/Kolkata") -> None:
        """
        config: StorageConfig from oms.config
        """
        self._cfg = config
        self._timezone = timezone
        self._data_dir = Path(config.data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)

        self._orders_lock = asyncio.Lock()
        self._trades_lock = asyncio.Lock()
        self._positions_lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _dated(self, template: str) -> Path:
        date_str = datetime.utcnow().strftime("%Y%m%d")
        return self._data_dir / template.format(date=date_str)

    def _ensure_csv_header(self, path: Path, fields: List[str]) -> None:
        if not path.exists() or path.stat().st_size == 0:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()

    # ------------------------------------------------------------------
    # Orders state (JSON snapshot)
    # ------------------------------------------------------------------

    async def save_orders_state(self, orders: Dict[str, Any]) -> None:
        """Overwrite the orders_state.json with the current in-memory dict."""
        path = self._data_dir / self._cfg.orders_state_file
        async with self._orders_lock:
            try:
                tmp = str(path) + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(orders, f, indent=2, default=str)
                os.replace(tmp, path)
            except Exception as exc:
                log.error("Failed to save orders state", error=str(exc))

    async def load_orders_state(self) -> Dict[str, Any]:
        """Load orders_state.json on startup for recovery."""
        path = self._data_dir / self._cfg.orders_state_file
        if not path.exists():
            return {}
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            log.error("Failed to load orders state", error=str(exc))
            return {}

    # ------------------------------------------------------------------
    # Order event log (CSV append)
    # ------------------------------------------------------------------

    async def append_order_log(self, order_dict: Dict[str, Any], event: str) -> None:
        """Append a single order-state-change row to the daily order log CSV."""
        path = self._dated(self._cfg.orders_log_file)
        async with self._orders_lock:
            try:
                self._ensure_csv_header(path, _ORDER_LOG_FIELDS)
                row = {k: order_dict.get(k, "") for k in _ORDER_LOG_FIELDS}
                row["ts"] = now_iso(self._timezone)
                row["event"] = event
                with open(path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=_ORDER_LOG_FIELDS)
                    writer.writerow(row)
            except Exception as exc:
                log.error("Failed to append order log", event=event, error=str(exc))

    # ------------------------------------------------------------------
    # Trades / fills log (CSV append)
    # ------------------------------------------------------------------

    async def append_trade(
        self,
        oms_order_id: str,
        broker_order_id: str,
        strategy_id: str,
        exchange_segment: str,
        exchange_instrument_id: int,
        instrument_name: str,
        order_side: str,
        product_type: str,
        fill_quantity: int,
        fill_price: float,
        filled_quantity_total: int,
        avg_fill_price: float,
        pending_quantity: int,
    ) -> None:
        path = self._dated(self._cfg.trades_file)
        async with self._trades_lock:
            try:
                self._ensure_csv_header(path, _TRADE_LOG_FIELDS)
                row = {
                    "ts": now_iso(self._timezone),
                    "oms_order_id": oms_order_id,
                    "broker_order_id": broker_order_id,
                    "strategy_id": strategy_id,
                    "exchange_segment": exchange_segment,
                    "exchange_instrument_id": exchange_instrument_id,
                    "instrument_name": instrument_name,
                    "order_side": order_side,
                    "product_type": product_type,
                    "fill_quantity": fill_quantity,
                    "fill_price": fill_price,
                    "filled_quantity_total": filled_quantity_total,
                    "avg_fill_price": avg_fill_price,
                    "pending_quantity": pending_quantity,
                }
                with open(path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=_TRADE_LOG_FIELDS)
                    writer.writerow(row)
            except Exception as exc:
                log.error("Failed to append trade", oms_order_id=oms_order_id, error=str(exc))

    # ------------------------------------------------------------------
    # Positions (JSON snapshot)
    # ------------------------------------------------------------------

    async def save_positions(self, positions: Dict[str, Any]) -> None:
        path = self._data_dir / self._cfg.positions_file
        async with self._positions_lock:
            try:
                tmp = str(path) + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(positions, f, indent=2, default=str)
                os.replace(tmp, path)
            except Exception as exc:
                log.error("Failed to save positions", error=str(exc))

    async def load_positions(self) -> Dict[str, Any]:
        path = self._data_dir / self._cfg.positions_file
        if not path.exists():
            return {}
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            log.error("Failed to load positions", error=str(exc))
            return {}

    # ------------------------------------------------------------------
    # Statistics (JSON snapshot, per-day)
    # ------------------------------------------------------------------

    async def save_statistics(self, stats: Dict[str, Any]) -> None:
        path = self._dated(self._cfg.statistics_file)
        async with self._stats_lock:
            try:
                tmp = str(path) + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(stats, f, indent=2, default=str)
                os.replace(tmp, path)
            except Exception as exc:
                log.error("Failed to save statistics", error=str(exc))

    async def load_statistics(self) -> Dict[str, Any]:
        path = self._dated(self._cfg.statistics_file)
        if not path.exists():
            return {}
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            log.error("Failed to load statistics", error=str(exc))
            return {}
