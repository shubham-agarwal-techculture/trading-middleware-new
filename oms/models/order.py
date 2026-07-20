"""Order data models and enumerations.

The string enums below (:class:`OrderStatus`, :class:`OrderSide`, ...) are the
canonical vocabulary for order fields. The :class:`Order` dataclass stores those
fields as their string *values* (so CSV/JSON serialization is stable and matches
the broker wire format) and exposes typed ``*_enum`` accessors for callers that
prefer working with the enum type.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
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
    order_quantity: float
    limit_price: float
    stop_price: float
    disclosed_quantity: int

    # --- Lifecycle state ---
    status: OrderStatus = OrderStatus.NEW
    broker_order_id: str = ""            # AppOrderID returned by broker
    order_unique_identifier: str = ""    # Sent to broker; equals oms_order_id[:18]

    # --- Fill tracking ---
    filled_quantity: float = 0
    pending_quantity: float = 0
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

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Order":
        """Reconstruct an :class:`Order` from a persisted dict (inverse of to_dict)."""
        tags = data.get("tags", {})
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except Exception:
                tags = {}

        def _dt(value: str) -> Optional[datetime]:
            if not value:
                return None
            try:
                return datetime.fromisoformat(value)
            except Exception:
                return None

        return cls(
            oms_order_id=data["oms_order_id"],
            strategy_id=data["strategy_id"],
            signal_id=data.get("signal_id", ""),
            exchange_segment=data["exchange_segment"],
            exchange_instrument_id=int(data["exchange_instrument_id"]),
            instrument_name=data.get("instrument_name", ""),
            product_type=data["product_type"],
            order_type=data["order_type"],
            order_side=data["order_side"],
            time_in_force=data["time_in_force"],
            order_quantity=float(data["order_quantity"]),
            limit_price=float(data.get("limit_price", 0.0)),
            stop_price=float(data.get("stop_price", 0.0)),
            disclosed_quantity=int(data.get("disclosed_quantity", 0)),
            status=OrderStatus(data.get("status", "ERROR")),
            broker_order_id=data.get("broker_order_id", ""),
            order_unique_identifier=data.get("order_unique_identifier", ""),
            filled_quantity=float(data.get("filled_quantity", 0)),
            pending_quantity=float(data.get("pending_quantity", 0)),
            avg_fill_price=float(data.get("avg_fill_price", 0.0)),
            last_fill_price=float(data.get("last_fill_price", 0.0)),
            last_fill_quantity=int(data.get("last_fill_quantity", 0)),
            created_at=_dt(data.get("created_at", "")),
            updated_at=_dt(data.get("updated_at", "")),
            sent_at=_dt(data.get("sent_at", "")),
            filled_at=_dt(data.get("filled_at", "")),
            exchange_transact_time=data.get("exchange_transact_time", ""),
            last_update_time=data.get("last_update_time", ""),
            reject_reason=data.get("reject_reason", ""),
            cancel_reason=data.get("cancel_reason", ""),
            error_message=data.get("error_message", ""),
            tags=tags,
        )

    @property
    def is_terminal(self) -> bool:
        st = OrderStatus(self.status) if isinstance(self.status, str) else self.status
        return st in TERMINAL_STATES

    @property
    def is_active(self) -> bool:
        st = OrderStatus(self.status) if isinstance(self.status, str) else self.status
        return st in ACTIVE_STATES

    # --- Typed enum accessors (fields are stored as their string values) ---

    @staticmethod
    def _as_enum(enum_cls, value):
        """Return *value* as a member of *enum_cls*, or None if it doesn't map."""
        try:
            return enum_cls(value)
        except ValueError:
            return None

    @property
    def side(self) -> Optional[OrderSide]:
        return self._as_enum(OrderSide, self.order_side)

    @property
    def type(self) -> Optional[OrderType]:
        return self._as_enum(OrderType, self.order_type)

    @property
    def product(self) -> Optional[ProductType]:
        return self._as_enum(ProductType, self.product_type)

    @property
    def tif(self) -> Optional[TimeInForce]:
        return self._as_enum(TimeInForce, self.time_in_force)

    @property
    def segment(self) -> Optional[ExchangeSegment]:
        return self._as_enum(ExchangeSegment, self.exchange_segment)

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
