"""Order data models and enumerations."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Set


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class OrderStatus(str, Enum):
    NEW = "NEW"                       # Received from strategy, not yet queued
    QUEUED = "QUEUED"                 # Placed in internal processing queue
    SENT = "SENT"                     # API call made to broker
    PENDING = "PENDING"               # Broker acknowledged, awaiting exchange
    OPEN = "OPEN"                     # Exchange accepted, awaiting fill
    PARTIAL_FILL = "PARTIAL_FILL"     # Partially executed
    FILLED = "FILLED"                 # Fully executed
    CANCELLED = "CANCELLED"           # Cancelled by strategy or OMS
    REJECTED = "REJECTED"             # Rejected by broker or exchange
    EXPIRED = "EXPIRED"               # DAY order expired at end-of-day
    ERROR = "ERROR"                   # Internal OMS error


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"       # Stop-Loss Limit order
    SL_M = "SL-M"   # Stop-Loss Market order


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class ProductType(str, Enum):
    MIS = "MIS"      # Margin Intraday Square-off
    NRML = "NRML"    # Normal carry-forward
    CNC = "CNC"      # Cash and Carry (delivery equity)
    MTF = "MTF"      # Margin Trade Funding


class TimeInForce(str, Enum):
    DAY = "DAY"      # Valid for the trading day
    IOC = "IOC"      # Immediate or Cancel
    GTC = "GTC"      # Good Till Cancelled
    GTD = "GTD"      # Good Till Date


class ExchangeSegment(str, Enum):
    NSECM = "NSECM"    # NSE Cash Market
    NSEFO = "NSEFO"    # NSE Futures & Options
    BSECM = "BSECM"    # BSE Cash Market
    BSEFO = "BSEFO"    # BSE Futures & Options
    MCXFO = "MCXFO"    # MCX Futures
    NSECDS = "NSECDS"  # NSE Currency Derivatives


# State groups for quick checks
TERMINAL_STATES: Set[OrderStatus] = {
    OrderStatus.FILLED,
    OrderStatus.CANCELLED,
    OrderStatus.REJECTED,
    OrderStatus.EXPIRED,
    OrderStatus.ERROR,
}

ACTIVE_STATES: Set[OrderStatus] = {
    OrderStatus.SENT,
    OrderStatus.PENDING,
    OrderStatus.OPEN,
    OrderStatus.PARTIAL_FILL,
}

# Map XTS broker order status strings to OMS OrderStatus
XTS_STATUS_MAP: Dict[str, OrderStatus] = {
    "New": OrderStatus.OPEN,
    "PendingNew": OrderStatus.PENDING,
    "PartiallyFilled": OrderStatus.PARTIAL_FILL,
    "Filled": OrderStatus.FILLED,
    "Cancelled": OrderStatus.CANCELLED,
    "Rejected": OrderStatus.REJECTED,
    "Expired": OrderStatus.EXPIRED,
    "PendingCancel": OrderStatus.OPEN,      # Cancel requested, still live
    "PendingReplace": OrderStatus.OPEN,     # Modify requested, still live
    "Replaced": OrderStatus.OPEN,           # Modified successfully
}


# ---------------------------------------------------------------------------
# Order dataclass
# ---------------------------------------------------------------------------

@dataclass
class Order:
    """Full lifecycle representation of a single order in the OMS."""

    # --- Identity ---
    oms_order_id: str           # Internal unique ID (set at creation)
    strategy_id: str            # Source strategy identifier
    signal_id: str              # Strategy's own reference ID for this signal

    # --- Instrument ---
    exchange_segment: str
    exchange_instrument_id: int
    instrument_name: str        # Human-readable name (e.g. NIFTY25MAY19500CE)

    # --- Order parameters ---
    product_type: str
    order_type: str
    order_side: str
    time_in_force: str
    order_quantity: int
    limit_price: float
    stop_price: float
    disclosed_quantity: int

    # --- Lifecycle state ---
    status: OrderStatus = OrderStatus.NEW
    broker_order_id: str = ""            # AppOrderID returned by broker
    order_unique_identifier: str = ""    # Sent to broker; equals oms_order_id[:18]

    # --- Fill tracking ---
    filled_quantity: int = 0
    pending_quantity: int = 0
    avg_fill_price: float = 0.0
    last_fill_price: float = 0.0
    last_fill_quantity: int = 0

    # --- Timestamps ---
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    sent_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    exchange_transact_time: str = ""   # ISO, from XTS ExchangeTransactTime
    last_update_time: str = ""         # ISO, from XTS LastUpdateDateTime

    # --- Messages ---
    reject_reason: str = ""
    cancel_reason: str = ""
    error_message: str = ""

    # --- Arbitrary strategy metadata ---
    tags: Dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------

    @classmethod
    def generate_id(cls) -> str:
        """Generate an 18-char order unique identifier (compatible with XTS)."""
        return uuid.uuid4().hex[:18]

    @property
    def is_terminal(self) -> bool:
        st = OrderStatus(self.status) if isinstance(self.status, str) else self.status
        return st in TERMINAL_STATES

    @property
    def is_active(self) -> bool:
        st = OrderStatus(self.status) if isinstance(self.status, str) else self.status
        return st in ACTIVE_STATES

    def to_dict(self) -> Dict[str, Any]:
        return {
            "oms_order_id": self.oms_order_id,
            "strategy_id": self.strategy_id,
            "signal_id": self.signal_id,
            "exchange_segment": self.exchange_segment,
            "exchange_instrument_id": self.exchange_instrument_id,
            "instrument_name": self.instrument_name,
            "product_type": self.product_type,
            "order_type": self.order_type,
            "order_side": self.order_side,
            "time_in_force": self.time_in_force,
            "order_quantity": self.order_quantity,
            "limit_price": self.limit_price,
            "stop_price": self.stop_price,
            "disclosed_quantity": self.disclosed_quantity,
            "status": self.status.value if isinstance(self.status, OrderStatus) else self.status,
            "broker_order_id": self.broker_order_id,
            "order_unique_identifier": self.order_unique_identifier,
            "filled_quantity": self.filled_quantity,
            "pending_quantity": self.pending_quantity,
            "avg_fill_price": self.avg_fill_price,
            "last_fill_price": self.last_fill_price,
            "last_fill_quantity": self.last_fill_quantity,
            "created_at": self.created_at.isoformat() if self.created_at else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at else "",
            "sent_at": self.sent_at.isoformat() if self.sent_at else "",
            "filled_at": self.filled_at.isoformat() if self.filled_at else "",
            "exchange_transact_time": self.exchange_transact_time,
            "last_update_time": self.last_update_time,
            "reject_reason": self.reject_reason,
            "cancel_reason": self.cancel_reason,
            "error_message": self.error_message,
            "tags": json.dumps(self.tags),
        }
