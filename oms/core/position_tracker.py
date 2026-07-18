"""
Position and P&L tracker.

Keeps a live in-memory snapshot of open positions keyed by
``"{exchange_segment}_{exchange_instrument_id}_{product_type}"``.
On each fill event the tracker updates quantities, average prices,
and realised P&L, then persists to positions.json.

Fill math lives on :class:`Position.apply_fill` so the rules are unit-testable
without the async tracker wrapper.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from oms.utils.logger import get_logger

log = get_logger(__name__)


@dataclass
class Position:
    """Single instrument/product position with fill-update logic."""

    key: str
    exchange_segment: str
    exchange_instrument_id: int
    instrument_name: str
    product_type: str
    net_quantity: int = 0
    buy_quantity: int = 0
    sell_quantity: int = 0
    buy_avg_price: float = 0.0
    sell_avg_price: float = 0.0
    avg_price: float = 0.0
    realised_pnl: float = 0.0
    last_updated: str = ""
    strategies: Dict[str, Dict[str, int]] = field(default_factory=dict)

    @classmethod
    def new(
        cls,
        exchange_segment: str,
        exchange_instrument_id: int,
        instrument_name: str,
        product_type: str,
    ) -> "Position":
        key = f"{exchange_segment}_{exchange_instrument_id}_{product_type}"
        return cls(
            key=key,
            exchange_segment=exchange_segment,
            exchange_instrument_id=exchange_instrument_id,
            instrument_name=instrument_name,
            product_type=product_type,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Position":
        return cls(
            key=data["key"],
            exchange_segment=data["exchange_segment"],
            exchange_instrument_id=int(data["exchange_instrument_id"]),
            instrument_name=data.get("instrument_name", ""),
            product_type=data["product_type"],
            net_quantity=int(data.get("net_quantity", 0)),
            buy_quantity=int(data.get("buy_quantity", 0)),
            sell_quantity=int(data.get("sell_quantity", 0)),
            buy_avg_price=float(data.get("buy_avg_price", 0.0)),
            sell_avg_price=float(data.get("sell_avg_price", 0.0)),
            avg_price=float(data.get("avg_price", 0.0)),
            realised_pnl=float(data.get("realised_pnl", 0.0)),
            last_updated=data.get("last_updated", ""),
            strategies=dict(data.get("strategies") or {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "exchange_segment": self.exchange_segment,
            "exchange_instrument_id": self.exchange_instrument_id,
            "instrument_name": self.instrument_name,
            "product_type": self.product_type,
            "net_quantity": self.net_quantity,
            "buy_quantity": self.buy_quantity,
            "sell_quantity": self.sell_quantity,
            "buy_avg_price": self.buy_avg_price,
            "sell_avg_price": self.sell_avg_price,
            "avg_price": self.avg_price,
            "realised_pnl": self.realised_pnl,
            "last_updated": self.last_updated,
            "strategies": self.strategies,
        }

    def apply_fill(
        self,
        order_side: str,
        fill_quantity: int,
        fill_price: float,
        strategy_id: str,
    ) -> None:
        """Mutate this position for one fill (full or partial)."""
        is_buy = order_side.upper() == "BUY"

        if is_buy:
            prev_buy_qty = self.buy_quantity
            prev_buy_avg = self.buy_avg_price
            new_buy_qty = prev_buy_qty + fill_quantity
            self.buy_avg_price = (
                (prev_buy_avg * prev_buy_qty + fill_price * fill_quantity) / new_buy_qty
                if new_buy_qty
                else 0.0
            )
            self.buy_quantity = new_buy_qty

            short_qty = max(0, -self.net_quantity)
            closing_qty = min(fill_quantity, short_qty)
            if closing_qty > 0 and self.sell_avg_price > 0:
                self.realised_pnl += closing_qty * (self.sell_avg_price - fill_price)

            self.net_quantity += fill_quantity
        else:
            prev_sell_qty = self.sell_quantity
            prev_sell_avg = self.sell_avg_price
            new_sell_qty = prev_sell_qty + fill_quantity
            self.sell_avg_price = (
                (prev_sell_avg * prev_sell_qty + fill_price * fill_quantity) / new_sell_qty
                if new_sell_qty
                else 0.0
            )
            self.sell_quantity = new_sell_qty

            long_qty = max(0, self.net_quantity)
            closing_qty = min(fill_quantity, long_qty)
            if closing_qty > 0 and self.buy_avg_price > 0:
                self.realised_pnl += closing_qty * (fill_price - self.buy_avg_price)

            self.net_quantity -= fill_quantity

        if self.net_quantity > 0:
            self.avg_price = self.buy_avg_price
        elif self.net_quantity < 0:
            self.avg_price = self.sell_avg_price
        else:
            self.avg_price = 0.0

        strat = self.strategies.setdefault(
            strategy_id, {"net_quantity": 0, "buy_qty": 0, "sell_qty": 0}
        )
        if is_buy:
            strat["buy_qty"] += fill_quantity
            strat["net_quantity"] += fill_quantity
        else:
            strat["sell_qty"] += fill_quantity
            strat["net_quantity"] -= fill_quantity

        self.last_updated = datetime.now(timezone.utc).isoformat()


class PositionTracker:
    """Thread-safe (asyncio.Lock) in-memory position book."""

    def __init__(self, file_store) -> None:
        self._store = file_store
        self._positions: Dict[str, Position] = {}
        self._lock = asyncio.Lock()

    async def load(self) -> None:
        """Restore positions from disk on startup."""
        data = await self._store.load_positions()
        async with self._lock:
            self._positions = {
                key: Position.from_dict(rec) for key, rec in data.items()
            }
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
        order_side: str,
        fill_quantity: int,
        fill_price: float,
        strategy_id: str,
    ) -> None:
        """Update position on a fill (full or partial)."""
        key = self._make_key(exchange_segment, exchange_instrument_id, product_type)

        async with self._lock:
            pos = self._positions.get(key)
            if pos is None:
                pos = Position.new(
                    exchange_segment, exchange_instrument_id, instrument_name, product_type
                )
                self._positions[key] = pos
            else:
                pos.instrument_name = instrument_name or pos.instrument_name

            pos.apply_fill(order_side, fill_quantity, fill_price, strategy_id)
            net_qty = pos.net_quantity

        await self._persist()

        log.info(
            "Position updated",
            key=key,
            strategy=strategy_id,
            side=order_side,
            fill_qty=fill_quantity,
            fill_price=fill_price,
            net_qty=net_qty,
        )

    async def get_position(
        self,
        exchange_segment: str,
        exchange_instrument_id: int,
        product_type: str,
    ) -> Optional[Dict[str, Any]]:
        key = self._make_key(exchange_segment, exchange_instrument_id, product_type)
        async with self._lock:
            pos = self._positions.get(key)
            return pos.to_dict() if pos else {}

    async def get_all_positions(self) -> Dict[str, Any]:
        async with self._lock:
            return {k: p.to_dict() for k, p in self._positions.items()}

    async def _persist(self) -> None:
        async with self._lock:
            snapshot = {k: p.to_dict() for k, p in self._positions.items()}
        await self._store.save_positions(snapshot)
