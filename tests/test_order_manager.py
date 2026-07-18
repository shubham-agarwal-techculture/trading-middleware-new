"""
Characterization tests for :class:`oms.core.order_manager.OrderManager`.

These pin the OMS's externally observable behavior (published responses,
broker calls, order state, position updates) so the Phase 2 refactor can be
verified as behavior-preserving. Tests drive the handlers/workers directly and
stub ZMQ, so no sockets are bound.
"""

from __future__ import annotations

import asyncio

from oms.models.order import OrderStatus


async def _place(harness, signal):
    """Run a signal through validation, the queue, and the place worker."""
    await harness.om._handle_place_order(signal)
    op, order = await harness.om._order_queue.get()
    assert op == "PLACE"
    await harness.om._execute_place(order, worker_id=0)
    return order


def test_place_order_acknowledges_and_calls_broker(make_oms, place_signal):
    harness = make_oms()

    order = asyncio.run(_place(harness, place_signal))

    assert len(harness.broker.placed) == 1
    assert order.status == OrderStatus.PENDING
    assert order.broker_order_id == "BRK1"

    acks = harness.responses_of_type("ORDER_ACK")
    assert len(acks) == 1
    assert acks[0]["status"] == "PENDING"
    assert acks[0]["broker_order_id"] == "BRK1"
    assert acks[0]["strategy_id"] == "TEST_STRAT"


def test_place_order_missing_field_publishes_validation_error(make_oms, place_signal):
    harness = make_oms()
    del place_signal["order_quantity"]

    asyncio.run(harness.om._handle_place_order(place_signal))

    errors = harness.responses_of_type("ORDER_ERROR")
    assert len(errors) == 1
    assert errors[0]["error_code"] == "VALIDATION_ERROR"
    assert harness.broker.placed == []


def test_place_order_broker_failure_publishes_broker_error(make_oms, place_signal):
    harness = make_oms()
    harness.broker.place_error = RuntimeError("broker down")

    order = asyncio.run(_place(harness, place_signal))

    assert order.status == OrderStatus.ERROR
    errors = harness.responses_of_type("ORDER_ERROR")
    assert errors and errors[-1]["error_code"] == "BROKER_ERROR"


def test_fill_event_publishes_filled_and_updates_position(make_oms, place_signal):
    harness = make_oms()

    async def scenario():
        order = await _place(harness, place_signal)
        await harness.om.inject_broker_event(
            {
                "order_unique_identifier": order.order_unique_identifier,
                "broker_order_id": order.broker_order_id,
                "oms_status": "FILLED",
                "filled_quantity": 50,
                "pending_quantity": 0,
                "avg_fill_price": 100.0,
                "last_fill_price": 100.0,
                "last_fill_quantity": 50,
                "order_quantity": 50,
            }
        )
        # Let the fire-and-forget persistence/position task complete.
        await asyncio.sleep(0.05)
        pos = await harness.positions.get_position("NSEFO", 41723, "MIS")
        return order, pos

    order, pos = asyncio.run(scenario())

    assert order.status == OrderStatus.FILLED
    assert order.filled_quantity == 50
    filled = harness.responses_of_type("ORDER_FILLED")
    assert filled and filled[-1]["filled_quantity"] == 50
    assert pos["net_quantity"] == 50
    assert pos["buy_avg_price"] == 100.0


def test_partial_then_full_fill(make_oms, place_signal):
    harness = make_oms()

    async def scenario():
        order = await _place(harness, place_signal)
        await harness.om.inject_broker_event(
            {
                "order_unique_identifier": order.order_unique_identifier,
                "broker_order_id": order.broker_order_id,
                "oms_status": "PARTIAL_FILL",
                "filled_quantity": 20,
                "pending_quantity": 30,
                "avg_fill_price": 100.0,
                "last_fill_price": 100.0,
                "last_fill_quantity": 20,
                "order_quantity": 50,
            }
        )
        await harness.om.inject_broker_event(
            {
                "order_unique_identifier": order.order_unique_identifier,
                "broker_order_id": order.broker_order_id,
                "oms_status": "FILLED",
                "filled_quantity": 50,
                "pending_quantity": 0,
                "avg_fill_price": 100.0,
                "last_fill_price": 100.0,
                "last_fill_quantity": 30,
                "order_quantity": 50,
            }
        )
        await asyncio.sleep(0.05)
        return order

    order = asyncio.run(scenario())
    assert order.filled_quantity == 50
    assert harness.responses_of_type("ORDER_PARTIAL")
    assert harness.responses_of_type("ORDER_FILLED")


def test_cancel_order_flow(make_oms, place_signal):
    harness = make_oms()

    async def scenario():
        order = await _place(harness, place_signal)
        await harness.om._handle_cancel_order(
            {"strategy_id": "TEST_STRAT", "signal_id": "c1", "oms_order_id": order.oms_order_id}
        )
        op, o = await harness.om._order_queue.get()
        assert op == "CANCEL"
        await harness.om._execute_cancel(o, worker_id=0)
        return order

    order = asyncio.run(scenario())
    assert harness.broker.cancelled == [order.broker_order_id]
    assert harness.responses_of_type("CANCEL_ACK")


def test_cancel_unknown_order_errors(make_oms):
    harness = make_oms()
    asyncio.run(
        harness.om._handle_cancel_order(
            {"strategy_id": "S", "signal_id": "x", "oms_order_id": "does-not-exist"}
        )
    )
    errors = harness.responses_of_type("ORDER_ERROR")
    assert errors and errors[-1]["error_code"] == "ORDER_NOT_FOUND"


def test_modify_order_flow(make_oms, place_signal):
    harness = make_oms()

    async def scenario():
        order = await _place(harness, place_signal)
        await harness.om._handle_modify_order(
            {
                "strategy_id": "TEST_STRAT",
                "signal_id": "m1",
                "oms_order_id": order.oms_order_id,
                "new_limit_price": 105.0,
                "new_order_quantity": 75,
            }
        )
        op, o = await harness.om._order_queue.get()
        assert op == "MODIFY"
        await harness.om._execute_modify(o, worker_id=0)
        return order

    order = asyncio.run(scenario())
    assert len(harness.broker.modified) == 1
    assert order.limit_price == 105.0
    assert order.order_quantity == 75
    assert harness.responses_of_type("MODIFY_ACK")


def test_squareoff_flow(make_oms):
    harness = make_oms()
    asyncio.run(
        harness.om._handle_squareoff(
            {
                "strategy_id": "TEST_STRAT",
                "signal_id": "sq1",
                "exchange_segment": "NSEFO",
                "exchange_instrument_id": 41723,
                "product_type": "MIS",
            }
        )
    )
    assert len(harness.broker.squared_off) == 1
    acks = harness.responses_of_type("SQUAREOFF_ACK")
    assert acks and acks[-1]["status"] == "SQUAREOFF_SENT"


def test_cancel_all_flow(make_oms):
    harness = make_oms()
    asyncio.run(
        harness.om._handle_cancel_all(
            {"strategy_id": "TEST_STRAT", "signal_id": "ca1"}
        )
    )
    assert len(harness.broker.cancel_all_calls) == 1
    acks = harness.responses_of_type("CANCEL_ACK")
    assert acks and acks[-1]["status"] == "CANCEL_ALL_SENT"


def test_unknown_msg_type_errors(make_oms):
    harness = make_oms()
    asyncio.run(harness.om._dispatch_signal({"msg_type": "NONSENSE", "strategy_id": "S"}))
    errors = harness.responses_of_type("ORDER_ERROR")
    assert errors and errors[-1]["error_code"] == "UNKNOWN_MSG_TYPE"
