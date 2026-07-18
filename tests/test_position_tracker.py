"""
Characterization tests for :class:`oms.core.position_tracker.PositionTracker`.

Pins the net-quantity, average-price and realised-P&L math so the Phase 2
`Position` dataclass refactor stays behavior-preserving.
"""

from __future__ import annotations

import asyncio

from oms.config import StorageConfig
from oms.core.position_tracker import PositionTracker
from oms.storage.file_store import FileStore


def _tracker(tmp_path) -> PositionTracker:
    store = FileStore(StorageConfig(data_dir=str(tmp_path / "data")))
    return PositionTracker(store)


def test_single_buy_sets_long_position(tmp_path):
    pt = _tracker(tmp_path)

    async def scenario():
        await pt.on_fill("NSEFO", 1, "OPT", "MIS", "BUY", 50, 100.0, "S1")
        return await pt.get_position("NSEFO", 1, "MIS")

    pos = asyncio.run(scenario())
    assert pos["net_quantity"] == 50
    assert pos["buy_avg_price"] == 100.0
    assert pos["avg_price"] == 100.0
    assert pos["realised_pnl"] == 0.0


def test_averaged_buys(tmp_path):
    pt = _tracker(tmp_path)

    async def scenario():
        await pt.on_fill("NSEFO", 1, "OPT", "MIS", "BUY", 50, 100.0, "S1")
        await pt.on_fill("NSEFO", 1, "OPT", "MIS", "BUY", 50, 120.0, "S1")
        return await pt.get_position("NSEFO", 1, "MIS")

    pos = asyncio.run(scenario())
    assert pos["net_quantity"] == 100
    assert pos["buy_avg_price"] == 110.0


def test_buy_then_sell_realises_pnl_and_flattens(tmp_path):
    pt = _tracker(tmp_path)

    async def scenario():
        await pt.on_fill("NSEFO", 1, "OPT", "MIS", "BUY", 50, 100.0, "S1")
        await pt.on_fill("NSEFO", 1, "OPT", "MIS", "SELL", 50, 130.0, "S1")
        return await pt.get_position("NSEFO", 1, "MIS")

    pos = asyncio.run(scenario())
    assert pos["net_quantity"] == 0
    assert pos["avg_price"] == 0.0
    # (130 - 100) * 50 = 1500
    assert pos["realised_pnl"] == 1500.0


def test_per_strategy_breakdown(tmp_path):
    pt = _tracker(tmp_path)

    async def scenario():
        await pt.on_fill("NSEFO", 1, "OPT", "MIS", "BUY", 30, 100.0, "A")
        await pt.on_fill("NSEFO", 1, "OPT", "MIS", "BUY", 20, 100.0, "B")
        return await pt.get_position("NSEFO", 1, "MIS")

    pos = asyncio.run(scenario())
    assert pos["strategies"]["A"]["net_quantity"] == 30
    assert pos["strategies"]["B"]["net_quantity"] == 20
