"""
Signal orchestration — BUY / SELL / FLAT commands routed to the OMS.

``handle_signal`` is the Command entry point used by the HTTP layer.
OMS fill updates are applied via ``on_oms_response``.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Dict

from market_data import get_atm_data

from bridge import state
from bridge.asset_class import (
    CRYPTO_SEGMENT,
    classify_signal,
    crypto_instrument_id,
    normalize_crypto_symbol,
)
from bridge.market_data import get_ltp_for_contract
from bridge.positions import (
    append_to_history,
    get_ist_now,
    load_positions,
    save_positions,
)
from bridge.resolution import find_contract_by_instrument_id, resolve_contract_by_ticker

log = logging.getLogger("NIFTY_BRIDGE")

_FAILURE_MSG_TYPES = frozenset({
    "ORDER_ERROR",
    "ORDER_REJECTED",
    "ORDER_EXPIRED",
    "ORDER_CANCELLED",
})
_FAILURE_STATUSES = frozenset({"REJECTED", "ERROR", "EXPIRED", "CANCELLED"})


def _order_failure_reason(resp: Dict[str, Any]) -> str:
    """Best available human-readable failure reason from an OMS response."""
    for key in ("error_message", "reject_reason", "message"):
        val = resp.get(key)
        if val:
            return str(val)
    code = resp.get("error_code")
    if code:
        return str(code)
    status = resp.get("status")
    if status:
        return str(status)
    return "unknown"


def _is_order_failure(resp: Dict[str, Any]) -> bool:
    msg_type = str(resp.get("msg_type", "")).upper()
    status = str(resp.get("status", "")).upper()
    if msg_type in _FAILURE_MSG_TYPES:
        return True
    if isinstance(resp.get("status"), str) and status in _FAILURE_STATUSES:
        return True
    return False


def _error_response(message: str, **extra: Any) -> Dict[str, Any]:
    log.warning("Signal rejected: %s", message)
    return {"status": "error", "message": message, **extra}


def _mark_pending_failure(signal_id: str, resp: Dict[str, Any]) -> None:
    if signal_id not in state.pending_orders:
        return
    reason = _order_failure_reason(resp)
    state.pending_orders[signal_id]["status"] = "failed"
    state.pending_orders[signal_id]["failure_reason"] = reason
    state.pending_orders[signal_id]["error_code"] = resp.get("error_code")
    state.pending_orders[signal_id]["response"] = resp


async def process_order_status(signal_id: str, contract: Dict[str, Any], quantity: int):
    """Background task to monitor order status and update position book."""
    try:
        log.info("Monitoring order status for signal_id: %s", signal_id)

        instrument_key = str(contract["exchange_instrument_id"])
        ack = await state.client.wait_for_ack(signal_id, timeout=30.0)

        if ack:
            msg_type = str(ack.get("msg_type", "")).upper()
            status = str(ack.get("status", "")).upper()
            log.info("Order ack received for %s: msg_type=%s status=%s", signal_id, msg_type, status)

            if msg_type == "ORDER_ERROR" or status == "ERROR" or _is_order_failure(ack):
                reason = _order_failure_reason(ack)
                log.error(
                    "Order failed | signal_id=%s instrument=%s reason=%s",
                    signal_id,
                    contract.get("instrument_name"),
                    reason,
                )
                _mark_pending_failure(signal_id, ack)
            elif status not in _FAILURE_STATUSES:
                positions = load_positions()
                positions[instrument_key] = {
                    "side": "BUY",
                    "qty": quantity,
                    "instrument": contract["instrument_name"],
                    "exchange_instrument_id": contract["exchange_instrument_id"],
                    "exchange_segment": contract.get("exchange_segment", "NSEFO"),
                    "asset_class": contract.get("asset_class", "india"),
                    "opened_at": get_ist_now(),
                    "signal_id": signal_id,
                    "oms_order_id": ack.get("oms_order_id"),
                    "status": status,
                    "entry_price": state.pending_orders.get(signal_id, {}).get(
                        "limit_price"
                    ),
                }
                save_positions(positions)
                log.info("Position saved for %s", contract["instrument_name"])

                if signal_id in state.pending_orders:
                    state.pending_orders[signal_id]["status"] = "acknowledged"
                    state.pending_orders[signal_id]["response"] = ack
            else:
                reason = _order_failure_reason(ack)
                log.warning(
                    "Order failed at ack | signal_id=%s status=%s reason=%s",
                    signal_id,
                    status,
                    reason,
                )
                _mark_pending_failure(signal_id, ack)
        else:
            log.warning("Timeout waiting for ORDER_ACK for signal_id: %s", signal_id)
            if signal_id in state.pending_orders:
                state.pending_orders[signal_id]["status"] = "timeout"

    except Exception as e:
        log.exception("Error processing order status for %s: %s", signal_id, e)
        if signal_id in state.pending_orders:
            state.pending_orders[signal_id]["status"] = "error"
            state.pending_orders[signal_id]["error"] = str(e)


async def handle_signal(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Process a signal and route to OMS."""
    try:
        action = signal.get("action", "").upper()
        position = signal.get("position", "").lower()
        quantity = signal.get("quantity")
        option_type = signal.get("optionType", "CE").upper()
        ticker = signal.get("ticker") or signal.get("symbol")
        explicit_segment = signal.get("exchange_segment") or signal.get(
            "exchangeSegment"
        )
        explicit_instrument_id = signal.get("exchange_instrument_id") or signal.get(
            "exchangeInstrumentID"
        )

        if not action or not position:
            return _error_response("Missing required fields: action, position")

        # Unchanged payload: infer crypto vs India and route accordingly.
        if classify_signal(signal) == "crypto":
            return await _handle_crypto_signal(signal, action, position, quantity)

        def _safe_int(value, default=0):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return default

        def _contract_from_master(contract_data: Dict[str, Any]) -> Dict[str, Any]:
            csv_opt = str(contract_data.get("OptionType", "")).strip()
            opt = "CE" if csv_opt == "3" else "PE" if csv_opt == "4" else option_type
            return {
                "exchange_segment": contract_data["ExchangeSegment"],
                "exchange_instrument_id": int(contract_data["ExchangeInstrumentID"]),
                "instrument_name": contract_data["Description"],
                "lot_size": max(_safe_int(contract_data.get("LotSize"), 1), 1),
                "strike": _safe_int(contract_data.get("StrikePrice")),
                "option_type": opt,
            }

        contract_data = None
        limit_price = None

        if explicit_segment and explicit_instrument_id:
            explicit_segment = str(explicit_segment).strip().upper()
            explicit_instrument_id = int(explicit_instrument_id)

            master_row = find_contract_by_instrument_id(
                explicit_segment, explicit_instrument_id
            )
            if master_row:
                contract = _contract_from_master(master_row)
            else:
                contract = {
                    "exchange_segment": explicit_segment,
                    "exchange_instrument_id": explicit_instrument_id,
                    "instrument_name": signal.get("instrument_name")
                    or signal.get("instrumentName")
                    or str(explicit_instrument_id),
                    "lot_size": max(_safe_int(signal.get("lot_size"), 1), 1),
                    "strike": 0,
                    "option_type": option_type,
                }

            log.info(
                "Explicit instrument signal: %s @ %s (id=%d)",
                contract["instrument_name"],
                contract["exchange_segment"],
                contract["exchange_instrument_id"],
            )

            if signal.get("limitPrice") or signal.get("limit_price"):
                limit_price = float(
                    signal.get("limitPrice") or signal.get("limit_price")
                )
            else:
                limit_price = await get_ltp_for_contract(
                    {
                        "ExchangeInstrumentID": contract["exchange_instrument_id"],
                        "ExchangeSegment": contract["exchange_segment"],
                    }
                )
                if limit_price is None:
                    return _error_response(
                        "Could not fetch LTP for instrument "
                        f"{contract['exchange_instrument_id']} on {contract['exchange_segment']}"
                    )
        elif ticker:
            contract_data = await resolve_contract_by_ticker(
                ticker, segment_hint=explicit_segment
            )
            if not contract_data:
                return _error_response(f"Contract not found for ticker: {ticker}")

            log.info(
                "Resolved ticker contract: %s @ %s",
                contract_data.get("Description"),
                contract_data.get("ExchangeSegment"),
            )

            fetched_ltp = await get_ltp_for_contract(contract_data)
            if fetched_ltp is None:
                return _error_response(f"Could not fetch LTP for ticker contract: {ticker}")

            if signal.get("limitPrice") or signal.get("limit_price"):
                limit_price = float(
                    signal.get("limitPrice") or signal.get("limit_price")
                )
            else:
                limit_price = fetched_ltp

            contract = _contract_from_master(contract_data)
        else:
            if state.atm_data is None:
                try:
                    state.atm_data = await get_atm_data()
                    log.info(
                        "Fetched live ATM data: strike=%d",
                        state.atm_data["atm_strike"],
                    )
                except Exception as e:
                    log.exception("ATM data fetch failed")
                    return _error_response(f"Could not fetch ATM data: {e}")

            if option_type == "CE":
                contract_data = state.atm_data["ce_contract"]
                limit_price = state.atm_data["ce_ltp"]
            elif option_type == "PE":
                contract_data = state.atm_data["pe_contract"]
                limit_price = state.atm_data["pe_ltp"]
            else:
                return _error_response("Invalid optionType, must be CE or PE")

            contract = {
                "exchange_segment": contract_data["ExchangeSegment"],
                "exchange_instrument_id": int(contract_data["ExchangeInstrumentID"]),
                "instrument_name": contract_data["Description"],
                "lot_size": int(contract_data["LotSize"]),
                "strike": state.atm_data["atm_strike"],
                "option_type": option_type,
            }

        if quantity is not None:
            try:
                quantity = int(quantity)
            except ValueError:
                quantity = contract["lot_size"]
        else:
            quantity = contract["lot_size"]

        product_type = (
            signal.get("productType") or signal.get("product_type") or "MIS"
        ).upper()
        order_type = (
            signal.get("orderType") or signal.get("order_type") or "LIMIT"
        ).upper()

        if signal.get("limitPrice") or signal.get("limit_price"):
            limit_price = float(signal.get("limitPrice") or signal.get("limit_price"))
        stop_price = float(signal.get("stopPrice") or signal.get("stop_price") or 0.0)

        instrument_key = str(contract["exchange_instrument_id"])
        positions = load_positions()
        current_position = positions.get(instrument_key)

        is_valid_position = False
        if current_position:
            status = current_position.get("status", "")
            is_valid_position = status not in [
                "REJECTED",
                "CANCELLED",
                "EXPIRED",
                "ERROR",
            ]

        log.info(
            "Received signal: Action=%s, Position=%s, Qty=%d, Instrument=%s",
            action,
            position,
            quantity,
            contract["instrument_name"],
        )

        if action == "SELL":
            return await _squareoff_existing(
                contract, current_position, is_valid_position, product_type, instrument_key
            )

        if position == "flat":
            return await _squareoff_existing(
                contract, current_position, is_valid_position, product_type, instrument_key
            )

        if action == "BUY":
            if is_valid_position:
                return {
                    "status": "ignored",
                    "message": f"Position already exists for {contract['instrument_name']}",
                }

            log.info(
                "Processing order placement for %s...", contract["instrument_name"]
            )
            sig_id = uuid.uuid4().hex

            state.pending_orders[sig_id] = {
                "status": "pending",
                "instrument": contract["instrument_name"],
                "timestamp": get_ist_now(),
                "limit_price": limit_price,
            }

            signal_id = await state.client.place_order(
                exchange_segment=contract["exchange_segment"],
                exchange_instrument_id=contract["exchange_instrument_id"],
                instrument_name=contract["instrument_name"],
                product_type=product_type,
                order_type=order_type,
                order_side=action,
                time_in_force="DAY",
                order_quantity=quantity,
                limit_price=limit_price,
                stop_price=stop_price,
                tags=signal.get("tags") or {},
                signal_id=sig_id,
            )
            log.info("Order signal sent | signal_id=%s", signal_id)

            asyncio.create_task(process_order_status(sig_id, contract, quantity))

            return {
                "status": "pending",
                "signal_id": sig_id,
                "message": "Order submitted, waiting for fill confirmation",
                "instrument": contract["instrument_name"],
                "exchange_segment": contract["exchange_segment"],
                "exchange_instrument_id": contract["exchange_instrument_id"],
                "quantity": quantity,
                "limit_price": limit_price,
                "order_type": order_type,
                "product_type": product_type,
                "order_side": action,
                "position": position,
                "option_type": contract.get("option_type"),
                "strike": contract.get("strike"),
            }

        return _error_response(f"Unsupported action: {action}")

    except Exception as e:
        log.exception("Error handling signal:")
        return _error_response(str(e))


async def _handle_crypto_signal(
    signal: Dict[str, Any],
    action: str,
    position: str,
    quantity: Any,
) -> Dict[str, Any]:
    """Route crypto symbols to Exchange1 via OMS segment ``CRYPTO``."""
    raw_symbol = signal.get("ticker") or signal.get("symbol") or ""
    if not raw_symbol:
        return _error_response("Crypto signal requires symbol/ticker (e.g. BTCUSDT)")

    symbol = normalize_crypto_symbol(str(raw_symbol))
    instrument_id = crypto_instrument_id(symbol)

    try:
        qty = float(quantity) if quantity is not None else 0.0
    except (TypeError, ValueError):
        qty = 0.0
    if qty <= 0:
        return _error_response("Crypto quantity must be a positive number")

    product_type = (
        signal.get("productType") or signal.get("product_type") or "SPOT"
    ).upper()
    order_type = (
        signal.get("orderType") or signal.get("order_type") or "LIMIT"
    ).upper()
    limit_price = float(
        signal.get("limitPrice") or signal.get("limit_price") or 0.0
    )
    stop_price = float(signal.get("stopPrice") or signal.get("stop_price") or 0.0)

    if order_type == "LIMIT" and limit_price <= 0:
        return _error_response("Crypto LIMIT orders require limitPrice")

    contract = {
        "exchange_segment": CRYPTO_SEGMENT,
        "exchange_instrument_id": instrument_id,
        "instrument_name": symbol,
        "lot_size": qty,
        "strike": 0,
        "option_type": "",
        "asset_class": "crypto",
    }
    instrument_key = str(instrument_id)
    positions = load_positions()
    current_position = positions.get(instrument_key)
    is_valid_position = False
    if current_position:
        status = current_position.get("status", "")
        is_valid_position = status not in [
            "REJECTED",
            "CANCELLED",
            "EXPIRED",
            "ERROR",
        ]

    log.info(
        "Crypto signal: Action=%s Position=%s Qty=%s Symbol=%s → Exchange1",
        action,
        position,
        qty,
        symbol,
    )

    if action == "SELL" or position == "flat":
        return await _squareoff_existing(
            contract, current_position, is_valid_position, product_type, instrument_key
        )

    if action != "BUY":
        return {"status": "error", "message": f"Unsupported crypto action: {action}"}

    if is_valid_position:
        return {
            "status": "ignored",
            "message": f"Position already exists for {symbol}",
        }

    sig_id = uuid.uuid4().hex
    state.pending_orders[sig_id] = {
        "status": "pending",
        "instrument": symbol,
        "timestamp": get_ist_now(),
        "limit_price": limit_price,
        "asset_class": "crypto",
    }

    signal_id = await state.client.place_order(
        exchange_segment=CRYPTO_SEGMENT,
        exchange_instrument_id=instrument_id,
        instrument_name=symbol,
        product_type=product_type if product_type not in ("MIS", "NRML", "CNC") else "SPOT",
        order_type=order_type,
        order_side=action,
        time_in_force="DAY",
        order_quantity=qty,
        limit_price=limit_price,
        stop_price=stop_price,
        tags={"asset_class": "crypto", "symbol": symbol},
        signal_id=sig_id,
    )
    log.info("Crypto order signal sent | signal_id=%s symbol=%s", signal_id, symbol)
    asyncio.create_task(process_order_status(sig_id, contract, qty))

    return {
        "status": "pending",
        "signal_id": sig_id,
        "message": "Crypto order submitted to Exchange1 via OMS",
        "instrument": symbol,
        "exchange_segment": CRYPTO_SEGMENT,
        "exchange_instrument_id": instrument_id,
        "quantity": qty,
        "limit_price": limit_price,
        "order_type": order_type,
        "product_type": product_type,
        "order_side": action,
        "position": position,
        "asset_class": "crypto",
    }


async def _squareoff_existing(
    contract, current_position, is_valid_position, product_type, instrument_key
) -> Dict[str, Any]:
    if not is_valid_position:
        return {
            "status": "ignored",
            "message": f"No valid open position found for {contract['instrument_name']}",
        }

    log.info(
        "Closing existing position for %s",
        contract["instrument_name"],
    )

    sig_id = uuid.uuid4().hex
    pos_side = current_position.get("side", "BUY")
    reverse_side = "SELL" if pos_side.upper() == "BUY" else "BUY"
    pos_qty = current_position.get("qty", 0)

    positions = load_positions()
    positions[instrument_key]["squareoff_signal_id"] = sig_id
    save_positions(positions)

    await state.client.place_order(
        exchange_segment=contract["exchange_segment"],
        exchange_instrument_id=contract["exchange_instrument_id"],
        instrument_name=contract["instrument_name"],
        product_type=product_type,
        order_type="MARKET",
        order_side=reverse_side,
        time_in_force="DAY",
        order_quantity=pos_qty,
        limit_price=0.0,
        signal_id=sig_id,
    )

    log.info("Waiting for ORDER_ACK from OMS (timeout 10s)...")
    ack = await state.client.wait_for_ack(sig_id, timeout=10.0)

    if ack:
        if _is_order_failure(ack):
            reason = _order_failure_reason(ack)
            log.error(
                "Square-off failed | signal_id=%s instrument=%s reason=%s",
                sig_id,
                contract["instrument_name"],
                reason,
            )
            return {
                "status": "squareoff_failed",
                "message": reason,
                "signal_id": sig_id,
                "instrument": contract["instrument_name"],
                "exchange_segment": contract["exchange_segment"],
                "exchange_instrument_id": contract["exchange_instrument_id"],
                "quantity": pos_qty,
                "order_side": reverse_side,
                "order_type": "MARKET",
                "product_type": product_type,
                "failure_reason": reason,
            }

        log.info("Square-off order submitted for: %s", contract["instrument_name"])
        return {
            "status": "submitted",
            "msg_type": "SQUAREOFF",
            "signal_id": sig_id,
            "instrument": contract["instrument_name"],
            "exchange_segment": contract["exchange_segment"],
            "exchange_instrument_id": contract["exchange_instrument_id"],
            "quantity": pos_qty,
            "order_side": reverse_side,
            "order_type": "MARKET",
            "product_type": product_type,
            "timestamp": get_ist_now(),
            "response": ack,
        }

    log.warning("Squareoff failed or not confirmed for signal_id=%s", sig_id)
    return {
        "status": "squareoff_failed",
        "message": "Squareoff sent but not confirmed (timeout waiting for OMS)",
        "signal_id": sig_id,
        "instrument": contract["instrument_name"],
        "exchange_segment": contract.get("exchange_segment"),
        "exchange_instrument_id": contract.get("exchange_instrument_id"),
        "quantity": pos_qty,
        "order_side": reverse_side,
        "order_type": "MARKET",
        "product_type": product_type,
    }


async def on_oms_response(resp: Dict[str, Any]) -> None:
    """Handle OMS PUB responses — update pending orders and position book."""
    msg_type = resp.get("msg_type", "")
    oms_id = resp.get("oms_order_id", "N/A")
    status = resp.get("status", "")
    signal_id = resp.get("signal_id", "")

    if not status and msg_type.startswith("ORDER_"):
        status = msg_type.replace("ORDER_", "")

    if _is_order_failure(resp):
        reason = _order_failure_reason(resp)
        log.error(
            "Order failed | signal_id=%s oms_id=%s msg_type=%s reason=%s",
            signal_id,
            oms_id,
            msg_type,
            reason,
        )
        if signal_id:
            _mark_pending_failure(signal_id, resp)
    else:
        log.info(
            "[OMS Update] type=%s, oms_id=%s, status=%s, signal_id=%s",
            msg_type,
            oms_id,
            status,
            signal_id,
        )

    if signal_id and signal_id in state.pending_orders:
        state.pending_orders[signal_id]["last_update"] = {
            "msg_type": msg_type,
            "status": status,
            "oms_order_id": oms_id,
            "timestamp": get_ist_now(),
        }
        if _is_order_failure(resp):
            state.pending_orders[signal_id]["last_update"]["failure_reason"] = (
                _order_failure_reason(resp)
            )

    try:
        positions = load_positions()
        updated = False
        str_oms_id = str(oms_id) if oms_id != "N/A" else "N/A"
        str_signal_id = str(signal_id) if signal_id else ""

        for key, pos in list(positions.items()):
            pos_oms_id = str(pos.get("oms_order_id", ""))
            pos_sig_id = str(pos.get("signal_id", ""))
            pos_sq_sig_id = str(pos.get("squareoff_signal_id", ""))

            if (str_oms_id != "N/A" and pos_oms_id == str_oms_id) or (
                str_signal_id and pos_sig_id == str_signal_id
            ):
                if status:
                    pos["status"] = status
                    updated = True
            elif str_signal_id and pos_sq_sig_id == str_signal_id:
                if status in ["FILLED", "COMPLETE"]:
                    append_to_history(pos, status)
                    positions.pop(key, None)
                    updated = True
                    log.info(
                        "Position %s removed due to successful square-off fill.",
                        pos.get("instrument"),
                    )
                elif status in ["REJECTED", "CANCELLED", "ERROR", "EXPIRED"]:
                    pos.pop("squareoff_signal_id", None)
                    updated = True
                    reason = _order_failure_reason(resp)
                    log.warning(
                        "Square-off order failed for %s | status=%s reason=%s",
                        pos.get("instrument"),
                        status,
                        reason,
                    )

        if updated:
            save_positions(positions)
    except Exception as e:
        log.error("Failed to update position status in on_oms_response: %s", e)
