"""OMS response models — messages published back to strategies."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class ResponseType(str, Enum):
    ORDER_ACK = "ORDER_ACK"               # Initial acknowledgment after placement
    ORDER_OPEN = "ORDER_OPEN"             # Exchange accepted the order
    ORDER_PARTIAL = "ORDER_PARTIAL"       # Partial fill received
    ORDER_FILLED = "ORDER_FILLED"         # Order fully executed
    ORDER_CANCELLED = "ORDER_CANCELLED"   # Order cancelled
    ORDER_REJECTED = "ORDER_REJECTED"     # Order rejected by broker/exchange
    ORDER_MODIFIED = "ORDER_MODIFIED"     # Order successfully modified
    ORDER_EXPIRED = "ORDER_EXPIRED"       # Order expired
    ORDER_ERROR = "ORDER_ERROR"           # Internal OMS/broker error
    CANCEL_ACK = "CANCEL_ACK"             # Cancel request sent to broker
    MODIFY_ACK = "MODIFY_ACK"             # Modify request sent to broker
    MODIFY_REJECTED = "MODIFY_REJECTED"   # Modify failed — order itself is UNCHANGED/still live
    SQUAREOFF_ACK = "SQUAREOFF_ACK"       # Squareoff request processed


@dataclass
class OrderResponse:
    """Structured response published back to the originating strategy."""

    # Routing
    msg_type: str
    strategy_id: str
    oms_order_id: str
    signal_id: str

    # Status
    status: str

    # Instrument
    exchange_segment: str = ""
    exchange_instrument_id: int = 0
    instrument_name: str = ""

    # Order info
    order_side: str = ""
    order_type: str = ""
    order_quantity: int = 0

    # Broker reference
    broker_order_id: str = ""

    # Fill details
    filled_quantity: int = 0
    pending_quantity: int = 0
    avg_fill_price: float = 0.0
    last_fill_price: float = 0.0
    last_fill_quantity: int = 0

    # Rejection / error details
    reject_reason: str = ""
    error_code: str = ""
    error_message: str = ""

    # Human-readable message
    message: str = ""

    # Timestamps (OMS publish time + exchange fill time when available)
    timestamp: str = ""
    exchange_timestamp: str = ""
    filled_at: str = ""
    updated_at: str = ""

    # Optional strategy metadata echoed back
    tags: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "msg_type": self.msg_type,
            "strategy_id": self.strategy_id,
            "oms_order_id": self.oms_order_id,
            "signal_id": self.signal_id,
            "status": self.status,
            "exchange_segment": self.exchange_segment,
            "exchange_instrument_id": self.exchange_instrument_id,
            "instrument_name": self.instrument_name,
            "order_side": self.order_side,
            "order_type": self.order_type,
            "order_quantity": self.order_quantity,
            "broker_order_id": self.broker_order_id,
            "filled_quantity": self.filled_quantity,
            "pending_quantity": self.pending_quantity,
            "avg_fill_price": self.avg_fill_price,
            "last_fill_price": self.last_fill_price,
            "last_fill_quantity": self.last_fill_quantity,
            "reject_reason": self.reject_reason,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "message": self.message,
            "timestamp": self.timestamp,
            "exchange_timestamp": self.exchange_timestamp,
            "filled_at": self.filled_at,
            "updated_at": self.updated_at,
            "tags": self.tags,
        }
