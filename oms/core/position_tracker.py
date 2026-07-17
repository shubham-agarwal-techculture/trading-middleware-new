"""
Position and P&L tracker.

Keeps a live in-memory snapshot of open positions keyed by
``"{exchange_segment}_{exchange_instrument_id}_{product_type}"``.
On each fill event the tracker updates quantities, average prices,
and realised P&L, then persists to positions.json.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, Optional

from oms.utils.logger import get_logger

log = get_logger(__name__)


class PositionTracker:
    """
    Thread-safe (asyncio.Lock) in-memory position book.

    Position key format: ``NSEFO_35003_MIS``

    Each position record::

        {
            "key": "NSEFO_35003_MIS",
            "exchange_segment": "NSEFO",
            "exchange_instrument_id": 35003,
            "instrument_name": "NIFTY25MAY19500CE",
            "product_type": "MIS",
            "net_quantity": 50,          # positive = long, negative = short
            "buy_quantity": 50,
            "sell_quantity": 0,
            "buy_avg_price": 251.5,
            "sell_avg_price": 0.0,
            "avg_price": 251.5,          # avg price of the net position
            "realised_pnl": 0.0,
            "last_updated": "2026-05-07T10:30:01",
            "strategies": {
                "MOMENTUM_001": {"net_quantity": 50, "buy_qty": 50, "sell_qty": 0}
            }
        }
    """

    def __init__(self, file_store) -> None:
        self._store = file_store
        self._positions: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def load(self) -> None:
        """Restore positions from disk on startup."""
        data = await self._store.load_positions()
        async with self._lock:
            self._positions = data
        log.info("Positions loaded from disk", count=len(self._positions))

    @staticmethod
    def _make_key(exchange_segment: str, exchange_instrument_id: int, product_type: str) -> str:
        return f"{exchange_segment}_{exchange_instrument_id}_{product_type}"

    async def on_fill(
        self,
        exchange_segment: str,
        exchange_instrument_id: int,
        instrument_name: str,
        product_type: str,
        order_side: str,       # "BUY" or "SELL"
        fill_quantity: int,
        fill_price: float,
        strategy_id: str,
    ) -> None:
        """Update position on a fill (full or partial)."""
        key = self._make_key(exchange_segment, exchange_instrument_id, product_type)

        async with self._lock:
            pos = self._positions.setdefault(key, {
                "key": key,
                "exchange_segment": exchange_segment,
                "exchange_instrument_id": exchange_instrument_id,
                "instrument_name": instrument_name,
                "product_type": product_type,
                "net_quantity": 0,
                "buy_quantity": 0,
                "sell_quantity": 0,
                "buy_avg_price": 0.0,
                "sell_avg_price": 0.0,
                "avg_price": 0.0,
                "realised_pnl": 0.0,
                "last_updated": "",
                "strategies": {},
            })

            is_buy = order_side.upper() == "BUY"

            if is_buy:
                # Update buy side
                prev_buy_qty = pos["buy_quantity"]
                prev_buy_avg = pos["buy_avg_price"]
                new_buy_qty = prev_buy_qty + fill_quantity
                pos["buy_avg_price"] = (
                    (prev_buy_avg * prev_buy_qty + fill_price * fill_quantity) / new_buy_qty
                    if new_buy_qty else 0.0
                )
                pos["buy_quantity"] = new_buy_qty

                # Realised P&L when closing a short
                short_qty = max(0, -pos["net_quantity"])
                closing_qty = min(fill_quantity, short_qty)
                if closing_qty > 0 and pos["sell_avg_price"] > 0:
                    pos["realised_pnl"] += closing_qty * (pos["sell_avg_price"] - fill_price)

                pos["net_quantity"] += fill_quantity
            else:
                # Update sell side
                prev_sell_qty = pos["sell_quantity"]
                prev_sell_avg = pos["sell_avg_price"]
                new_sell_qty = prev_sell_qty + fill_quantity
                pos["sell_avg_price"] = (
                    (prev_sell_avg * prev_sell_qty + fill_price * fill_quantity) / new_sell_qty
                    if new_sell_qty else 0.0
                )
                pos["sell_quantity"] = new_sell_qty

                # Realised P&L when closing a long
                long_qty = max(0, pos["net_quantity"])
                closing_qty = min(fill_quantity, long_qty)
                if closing_qty > 0 and pos["buy_avg_price"] > 0:
                    pos["realised_pnl"] += closing_qty * (fill_price - pos["buy_avg_price"])

                pos["net_quantity"] -= fill_quantity

            # Recalculate net average price
            net = pos["net_quantity"]
            if net > 0:
                pos["avg_price"] = pos["buy_avg_price"]
            elif net < 0:
                pos["avg_price"] = pos["sell_avg_price"]
            else:
                pos["avg_price"] = 0.0

            # Per-strategy breakdown
            strat = pos["strategies"].setdefault(strategy_id, {"net_quantity": 0, "buy_qty": 0, "sell_qty": 0})
            if is_buy:
                strat["buy_qty"] += fill_quantity
                strat["net_quantity"] += fill_quantity
            else:
                strat["sell_qty"] += fill_quantity
                strat["net_quantity"] -= fill_quantity

            pos["last_updated"] = datetime.utcnow().isoformat()

        # Persist asynchronously
        await self._persist()

        log.info(
            "Position updated",
            key=key,
            strategy=strategy_id,
            side=order_side,
            fill_qty=fill_quantity,
            fill_price=fill_price,
            net_qty=self._positions[key]["net_quantity"],
        )

    async def get_position(
        self,
        exchange_segment: str,
        exchange_instrument_id: int,
        product_type: str,
    ) -> Optional[Dict[str, Any]]:
        key = self._make_key(exchange_segment, exchange_instrument_id, product_type)
        async with self._lock:
            return dict(self._positions.get(key, {}))

    async def get_all_positions(self) -> Dict[str, Any]:
        async with self._lock:
            return dict(self._positions)

    async def _persist(self) -> None:
        async with self._lock:
            snapshot = dict(self._positions)
        await self._store.save_positions(snapshot)
