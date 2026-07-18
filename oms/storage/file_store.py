"""
Persistent file storage for the OMS (default :class:`StorageBackend`).

Files written:
  data/orders_log_YYYYMMDD.csv   — append-only order event log (audit trail)
  data/orders_state.json         — current snapshot of all active + recent orders
  data/trades_YYYYMMDD.csv       — append-only fill/trade log
  data/positions.json            — current open positions snapshot
  data/statistics_YYYYMMDD.json  — daily aggregated statistics

Blocking disk I/O runs in a worker thread via ``asyncio.to_thread`` so the
event loop is never stalled by CSV/JSON writes.
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from oms.utils.logger import get_logger
from oms.utils.timeutil import now_iso

log = get_logger(__name__)

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


def _write_json_atomic(path: Path, data: Any) -> None:
    tmp = str(path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    os.replace(tmp, path)


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _ensure_csv_header(path: Path, fields: List[str]) -> None:
    if not path.exists() or path.stat().st_size == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fields).writeheader()


def _append_csv_row(path: Path, fields: List[str], row: Dict[str, Any]) -> None:
    _ensure_csv_header(path, fields)
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fields).writerow(row)


class FileStore:
    """Async-safe CSV/JSON storage layer implementing :class:`StorageBackend`."""

    def __init__(self, config, timezone: str = "Asia/Kolkata") -> None:
        self._cfg = config
        self._timezone = timezone
        self._data_dir = Path(config.data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)

        self._orders_lock = asyncio.Lock()
        self._trades_lock = asyncio.Lock()
        self._positions_lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()

    def _dated(self, template: str) -> Path:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        return self._data_dir / template.format(date=date_str)

    async def save_orders_state(self, orders: Dict[str, Any]) -> None:
        path = self._data_dir / self._cfg.orders_state_file
        async with self._orders_lock:
            try:
                await asyncio.to_thread(_write_json_atomic, path, orders)
            except Exception as exc:
                log.error("Failed to save orders state", error=str(exc))

    async def load_orders_state(self) -> Dict[str, Any]:
        path = self._data_dir / self._cfg.orders_state_file
        try:
            return await asyncio.to_thread(_read_json, path)
        except Exception as exc:
            log.error("Failed to load orders state", error=str(exc))
            return {}

    async def append_order_log(self, order_dict: Dict[str, Any], event: str) -> None:
        path = self._dated(self._cfg.orders_log_file)
        row = {k: order_dict.get(k, "") for k in _ORDER_LOG_FIELDS}
        row["ts"] = now_iso(self._timezone)
        row["event"] = event
        async with self._orders_lock:
            try:
                await asyncio.to_thread(_append_csv_row, path, _ORDER_LOG_FIELDS, row)
            except Exception as exc:
                log.error("Failed to append order log", event=event, error=str(exc))

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
        async with self._trades_lock:
            try:
                await asyncio.to_thread(_append_csv_row, path, _TRADE_LOG_FIELDS, row)
            except Exception as exc:
                log.error("Failed to append trade", oms_order_id=oms_order_id, error=str(exc))

    async def save_positions(self, positions: Dict[str, Any]) -> None:
        path = self._data_dir / self._cfg.positions_file
        async with self._positions_lock:
            try:
                await asyncio.to_thread(_write_json_atomic, path, positions)
            except Exception as exc:
                log.error("Failed to save positions", error=str(exc))

    async def load_positions(self) -> Dict[str, Any]:
        path = self._data_dir / self._cfg.positions_file
        try:
            return await asyncio.to_thread(_read_json, path)
        except Exception as exc:
            log.error("Failed to load positions", error=str(exc))
            return {}

    async def save_statistics(self, stats: Dict[str, Any]) -> None:
        path = self._dated(self._cfg.statistics_file)
        async with self._stats_lock:
            try:
                await asyncio.to_thread(_write_json_atomic, path, stats)
            except Exception as exc:
                log.error("Failed to save statistics", error=str(exc))

    async def load_statistics(self) -> Dict[str, Any]:
        path = self._dated(self._cfg.statistics_file)
        try:
            return await asyncio.to_thread(_read_json, path)
        except Exception as exc:
            log.error("Failed to load statistics", error=str(exc))
            return {}
