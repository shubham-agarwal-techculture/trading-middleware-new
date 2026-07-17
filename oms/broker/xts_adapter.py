"""
XTS Interactive API broker adapter.

Wraps the XTS REST endpoints for order placement, modification,
cancellation, and portfolio queries.  Broker event updates (order status
changes from the exchange) arrive via XTS's Socket.IO interactive feed;
the OMS calls ``inject_broker_event()`` from that websocket callback.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Optional

import httpx

from oms.broker.base import AbstractBrokerAdapter, BrokerError
from oms.utils.logger import get_logger
from oms.utils.rate_limiter import RateLimiter
from oms.utils.timeutil import parse_xts_datetime

log = get_logger(__name__)
xts_log = logging.getLogger("xts")


# ---------------------------------------------------------------------------
# XTS order status → OMS status string mapping
# ---------------------------------------------------------------------------
XTS_STATUS_MAP: Dict[str, str] = {
    "New": "OPEN",
    "PendingNew": "PENDING",
    "PartiallyFilled": "PARTIAL_FILL",
    "Filled": "FILLED",
    "Cancelled": "CANCELLED",
    "Rejected": "REJECTED",
    "Expired": "EXPIRED",
    "PendingCancel": "OPEN",
    "PendingReplace": "OPEN",
    "Replaced": "OPEN",
}


def _parse_numeric(val: Any) -> Optional[float]:
    """Parse int/float/str numeric values; returns None if not parseable."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    text = str(val).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_int_field(appended: Dict[str, Any], *keys: str) -> int:
    for key in keys:
        n = _parse_numeric(appended.get(key))
        if n is not None:
            return int(n)
    return 0


def _parse_float_field(appended: Dict[str, Any], *keys: str) -> float:
    for key in keys:
        n = _parse_numeric(appended.get(key))
        if n is not None and n > 0:
            return n
    return 0.0


def _coerce_event_data(event_data: Any) -> Optional[Dict[str, Any]]:
    """
    Normalise XTS socket payloads to a dict.

    XTS may send JSON strings, single-element lists, or nested ``Appended`` strings.
    """
    if event_data is None:
        return None

    if isinstance(event_data, str):
        text = event_data.strip()
        if not text:
            return None
        try:
            event_data = json.loads(text)
        except json.JSONDecodeError:
            return None

    if isinstance(event_data, list):
        if not event_data:
            return None
        return _coerce_event_data(event_data[0])

    if not isinstance(event_data, dict):
        return None

    appended = event_data.get("Appended")
    if isinstance(appended, str):
        try:
            event_data = {**event_data, "Appended": json.loads(appended)}
        except json.JSONDecodeError:
            pass

    return event_data


def _first_timestamp(appended: Dict[str, Any], timezone: str = "Asia/Kolkata") -> str:
    for key in (
        "ExchangeTransactTime",
        "exchangeTransactTime",
        "LastUpdateDateTime",
        "lastUpdateDateTime",
        "LastExecutionTransactTime",
        "OrderGeneratedDateTime",
    ):
        raw = appended.get(key)
        if raw:
            parsed = parse_xts_datetime(str(raw), timezone)
            if parsed:
                return parsed
    return ""


class XTSBrokerAdapter(AbstractBrokerAdapter):
    """
    Async adapter for XTS Interactive REST API.

    Rate limits:
      - Order mutations (place/modify/cancel): 10 per second
      - Query calls (order book, positions): 1 per second
    """

    def __init__(self, config) -> None:
        """config: BrokerConfig from oms.config"""
        self._cfg = config
        self._base_url = config.url.rstrip("/")
        self._token: Optional[str] = None
        self._user_id: Optional[str] = None
        self._is_investor_client: bool = True
        self._client: Optional[httpx.AsyncClient] = None
        self._order_limiter = RateLimiter(10)   # 10 order ops/sec
        self._query_limiter = RateLimiter(1)    # 1 query/sec

    @property
    def token(self) -> Optional[str]:
        return self._token

    @property
    def user_id(self) -> Optional[str]:
        return self._user_id

    @property
    def base_url(self) -> str:
        return self._base_url

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                verify=self._cfg.verify_ssl,
            )
        return self._client

    def _headers(self) -> Dict[str, str]:
        # Official XTS SDK uses capital-A "Authorization"; some gateways
        # reject lowercase "authorization" with e-session-0005.
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = self._token
        return headers

    def _handle_response(self, resp: httpx.Response) -> Dict[str, Any]:
        body_preview = resp.text[:8000]
        log.debug(
            "XTS API response",
            status_code=resp.status_code,
            url=str(resp.url),
            body=body_preview[:500],
        )
        xts_log.info(
            "REST response | %s %s | status=%s | body=%s",
            resp.request.method if resp.request else "?",
            str(resp.url),
            resp.status_code,
            body_preview,
        )

        # Parse body first so XTS error descriptions (e.g. e-session-0005)
        # are not swallowed by httpx's generic "400 Bad Request".
        data: Any = None
        try:
            data = resp.json()
        except Exception:
            data = None

        if isinstance(data, dict) and data.get("type") == "error":
            xts_log.error(
                "REST error | %s | code=%s | %s",
                str(resp.url),
                data.get("code", ""),
                data.get("description", ""),
            )
            raise BrokerError(
                data.get("description", "XTS API error"),
                code=data.get("code", ""),
                description=data.get("description", ""),
            )

        if resp.is_error:
            raise BrokerError(
                f"HTTP {resp.status_code}: {body_preview[:500] or resp.reason_phrase}",
                code=str(resp.status_code),
                description=body_preview[:500],
            )

        if not isinstance(data, dict):
            raise BrokerError(
                f"Unexpected XTS response: {body_preview[:500]}",
                code="BAD_RESPONSE",
                description=body_preview[:500],
            )
        return data

    def _log_request(self, method: str, url: str, payload: Any = None) -> None:
        try:
            payload_str = json.dumps(payload, default=str) if payload is not None else ""
        except Exception:
            payload_str = str(payload)
        auth = "present" if self._token else "MISSING"
        xts_log.info(
            "REST request | %s %s | auth=%s | payload=%s",
            method, url, auth, payload_str,
        )

    def _inject_client_id(self, payload: Dict) -> Dict:
        """Add clientID to payload when operating in dealer/CTCL mode."""
        if not self._is_investor_client and self._cfg.client_id:
            return {**payload, "clientID": self._cfg.client_id}
        return payload

    # ------------------------------------------------------------------
    # AbstractBrokerAdapter implementation
    # ------------------------------------------------------------------

    async def login(self) -> Dict[str, Any]:
        client = await self._get_client()
        payload = {
            "appKey": self._cfg.app_key,
            "secretKey": self._cfg.secret_key,
            "source": self._cfg.source,
        }
        url = f"{self._base_url}/interactive/user/session"
        self._log_request("POST", url, payload)
        resp = await client.post(url, json=payload)
        data = self._handle_response(resp)
        result = data.get("result", data)
        self._token = result.get("token")
        self._user_id = result.get("userID")
        self._is_investor_client = result.get("isInvestorClient", True)
        log.info(
            "XTS login successful",
            user_id=self._user_id,
            is_investor_client=self._is_investor_client,
        )
        return result

    async def place_order(
        self,
        exchange_segment: str,
        exchange_instrument_id: int,
        product_type: str,
        order_type: str,
        order_side: str,
        time_in_force: str,
        disclosed_quantity: int,
        order_quantity: int,
        limit_price: float = 0.0,
        stop_price: float = 0.0,
        order_unique_identifier: str = "",
    ) -> Dict[str, Any]:
        await self._order_limiter.acquire()
        client = await self._get_client()
        payload = self._inject_client_id({
            "exchangeSegment": exchange_segment,
            "exchangeInstrumentID": exchange_instrument_id,
            "productType": product_type,
            "orderType": order_type,
            "orderSide": order_side,
            "timeInForce": time_in_force,
            "disclosedQuantity": disclosed_quantity,
            "orderQuantity": order_quantity,
            "limitPrice": limit_price,
            "stopPrice": stop_price,
            "orderUniqueIdentifier": order_unique_identifier,
            "apiOrderSource": "ALGO",
        })
        log.info(
            "Placing order",
            exchange_segment=exchange_segment,
            instrument_id=exchange_instrument_id,
            side=order_side,
            qty=order_quantity,
            price=limit_price,
        )
        url = f"{self._base_url}/interactive/orders"
        self._log_request("POST", url, payload)
        resp = await client.post(url, json=payload, headers=self._headers())
        data = self._handle_response(resp)
        result = data.get("result", data)
        broker_order_id = str(result.get("AppOrderID", ""))
        log.info("Order placed", broker_order_id=broker_order_id)
        return {"broker_order_id": broker_order_id, "raw": result}

    async def modify_order(
        self,
        broker_order_id: str,
        product_type: str,
        order_type: str,
        order_quantity: int,
        disclosed_quantity: int,
        limit_price: float,
        stop_price: float,
        time_in_force: str,
        order_unique_identifier: str,
    ) -> Dict[str, Any]:
        await self._order_limiter.acquire()
        client = await self._get_client()
        payload: Dict[str, Any] = {
            "appOrderID": int(broker_order_id),
            "modifiedProductType": product_type,
            "modifiedOrderType": order_type,
            "modifiedOrderQuantity": order_quantity,
            "modifiedDisclosedQuantity": disclosed_quantity,
            "modifiedLimitPrice": limit_price,
            "modifiedStopPrice": stop_price,
            "modifiedTimeInForce": time_in_force,
            "orderUniqueIdentifier": order_unique_identifier,
        }
        payload = self._inject_client_id(payload)
        resp = await client.put(
            f"{self._base_url}/interactive/orders",
            json=payload,
            headers=self._headers(),
        )
        data = self._handle_response(resp)
        log.info("Order modified", broker_order_id=broker_order_id)
        return {"raw": data.get("result", data)}

    async def cancel_order(self, broker_order_id: str) -> Dict[str, Any]:
        await self._order_limiter.acquire()
        client = await self._get_client()
        params = self._inject_client_id({"appOrderID": broker_order_id})
        resp = await client.delete(
            f"{self._base_url}/interactive/orders",
            params=params,
            headers=self._headers(),
        )
        data = self._handle_response(resp)
        log.info("Order cancelled", broker_order_id=broker_order_id)
        return {"raw": data.get("result", data)}

    async def cancel_all_orders(
        self,
        exchange_segment: Optional[str] = None,
        exchange_instrument_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        await self._order_limiter.acquire()
        client = await self._get_client()
        payload: Dict[str, Any] = {}
        if exchange_segment:
            payload["exchangeSegment"] = exchange_segment
        if exchange_instrument_id:
            payload["exchangeInstrumentID"] = exchange_instrument_id
        payload = self._inject_client_id(payload)
        resp = await client.post(
            f"{self._base_url}/interactive/orders/cancelall",
            json=payload,
            headers=self._headers(),
        )
        return self._handle_response(resp)

    async def squareoff_position(
        self,
        exchange_segment: str,
        exchange_instrument_id: int,
        product_type: str,
        squareoff_mode: str = "DayWise",
        squareoff_qty_value: int = 0,
    ) -> Dict[str, Any]:
        await self._order_limiter.acquire()
        client = await self._get_client()
        payload = self._inject_client_id({
            "exchangeSegment": exchange_segment,
            "exchangeInstrumentID": exchange_instrument_id,
            "productType": product_type,
            "squareoffMode": squareoff_mode,
            "positionSquareOffQuantityType": "ExactQty",
            "squareOffQtyValue": squareoff_qty_value,
            "blockOrderSending": True,
            "cancelOrders": True,
        })
        resp = await client.put(
            f"{self._base_url}/interactive/portfolio/squareoff",
            json=payload,
            headers=self._headers(),
        )
        return self._handle_response(resp)

    async def get_order_book(self) -> Dict[str, Any]:
        await self._query_limiter.acquire()
        client = await self._get_client()
        if not self._is_investor_client:
            endpoint = f"{self._base_url}/interactive/orders/dealerorderbook"
            params = self._inject_client_id({})
        else:
            endpoint = f"{self._base_url}/interactive/orders"
            params = {}
        resp = await client.get(endpoint, params=params, headers=self._headers())
        return self._handle_response(resp)

    async def get_positions(self) -> Dict[str, Any]:
        await self._query_limiter.acquire()
        client = await self._get_client()
        if not self._is_investor_client:
            endpoint = f"{self._base_url}/interactive/portfolio/dealerpositions"
            params = self._inject_client_id({"dayOrNet": "NetWise"})
        else:
            endpoint = f"{self._base_url}/interactive/portfolio/positions"
            params = {"dayOrNet": "NetWise"}
        resp = await client.get(endpoint, params=params, headers=self._headers())
        return self._handle_response(resp)

    async def get_trade_book(self) -> Dict[str, Any]:
        await self._query_limiter.acquire()
        client = await self._get_client()
        if not self._is_investor_client:
            endpoint = f"{self._base_url}/interactive/orders/dealertradebook"
            params = self._inject_client_id({})
        else:
            endpoint = f"{self._base_url}/interactive/orders/trades"
            params = {}
        resp = await client.get(endpoint, params=params, headers=self._headers())
        return self._handle_response(resp)

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        log.info("XTS adapter closed")

    # ------------------------------------------------------------------
    # Broker event parsing (called from Socket.IO callback)
    # ------------------------------------------------------------------

    @staticmethod
    def parse_order_event(event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse an XTS interactive 'order' socket event into a normalised dict.

        Expected input::

            {
                "Appended": {
                    "AppOrderID": 1000001,
                    "OrderUniqueIdentifier": "<oms_order_id>",
                    "OrderStatus": "Filled",
                    "FilledQuantity": 50,
                    "RemainingQuantity": 0,
                    "AverageTradedPrice": 251.5,
                    "LastTradedPrice": 251.5,
                    "LastTradedQuantity": 50,
                    "CancelRejectReason": "",
                    ...
                }
            }

        Returns ``None`` if the event cannot be parsed.
        """
        try:
            event_data = _coerce_event_data(event_data)
            if not event_data:
                return None

            appended = event_data.get("Appended") or event_data
            if isinstance(appended, str):
                try:
                    appended = json.loads(appended)
                except json.JSONDecodeError:
                    return None
            if not isinstance(appended, dict):
                return None

            xts_status = str(
                appended.get("OrderStatus") or appended.get("orderStatus") or ""
            )
            oms_status = XTS_STATUS_MAP.get(xts_status, "")
            if not oms_status:
                return None

            order_qty = _parse_int_field(appended, "OrderQuantity", "orderQuantity")
            filled_qty = _parse_int_field(
                appended, "CumulativeQuantity", "FilledQuantity", "filledQuantity"
            )
            pending_qty = _parse_int_field(
                appended, "LeavesQuantity", "RemainingQuantity", "remainingQuantity"
            )
            order_price = _parse_float_field(
                appended, "OrderPrice", "orderPrice", "LimitPrice", "limitPrice"
            )
            avg_fill = _parse_float_field(
                appended,
                "OrderAverageTradedPrice",
                "AverageTradedPrice",
                "averageTradedPrice",
                "VWAP",
            )
            last_fill_price = _parse_float_field(
                appended,
                "LastTradedPrice",
                "lastTradedPrice",
                "OrderAverageTradedPrice",
                "AverageTradedPrice",
            )
            last_fill_qty = _parse_int_field(
                appended, "LastTradedQuantity", "LastTradedQty", "lastFillQuantity"
            )

            # Filled order with zero cumulative qty (stale order-book row)
            if xts_status == "Filled" and filled_qty <= 0 and order_qty > 0:
                if pending_qty <= 0:
                    filled_qty = order_qty
                elif pending_qty < order_qty:
                    filled_qty = order_qty - pending_qty

            if last_fill_qty <= 0 and filled_qty > 0:
                if xts_status == "Filled":
                    last_fill_qty = filled_qty
                elif xts_status == "PartiallyFilled" and order_qty > pending_qty:
                    last_fill_qty = order_qty - pending_qty

            if avg_fill <= 0 and last_fill_price > 0:
                avg_fill = last_fill_price
            if avg_fill <= 0 and order_price > 0 and filled_qty > 0:
                avg_fill = order_price
            if last_fill_price <= 0 and avg_fill > 0:
                last_fill_price = avg_fill

            exchange_ts = _first_timestamp(appended)

            parsed = {
                "broker_order_id": str(
                    appended.get("AppOrderID") or appended.get("appOrderID") or ""
                ),
                "order_unique_identifier": str(
                    appended.get("OrderUniqueIdentifier")
                    or appended.get("orderUniqueIdentifier")
                    or ""
                ),
                "oms_status": oms_status,
                "xts_status": xts_status,
                "filled_quantity": filled_qty,
                "pending_quantity": pending_qty,
                "avg_fill_price": avg_fill,
                "last_fill_price": last_fill_price,
                "last_fill_quantity": last_fill_qty,
                "order_price": order_price,
                "reject_reason": str(
                    appended.get("CancelRejectReason")
                    or appended.get("cancelRejectReason")
                    or ""
                ),
                "exchange_segment": str(
                    appended.get("ExchangeSegment") or appended.get("exchangeSegment") or ""
                ),
                "exchange_instrument_id": _parse_int_field(
                    appended, "ExchangeInstrumentID", "exchangeInstrumentID"
                ),
                "order_side": str(
                    appended.get("OrderSide") or appended.get("orderSide") or ""
                ),
                "order_quantity": order_qty,
                "exchange_transact_time": exchange_ts,
                "last_update_time": parse_xts_datetime(
                    str(
                        appended.get("LastUpdateDateTime")
                        or appended.get("lastUpdateDateTime")
                        or ""
                    )
                )
                or "",
            }

            xts_log.info(
                "PARSED event | broker_id=%s | uid=%s | xts_status=%s | "
                "filled=%d | pending=%d | avg=%.4f | last_px=%.4f | last_qty=%d | "
                "exchange_ts=%s",
                parsed["broker_order_id"],
                parsed["order_unique_identifier"],
                xts_status,
                filled_qty,
                pending_qty,
                avg_fill,
                last_fill_price,
                last_fill_qty,
                exchange_ts or "n/a",
            )
            return parsed
        except Exception as exc:
            log.warning("Failed to parse XTS order event", error=str(exc))
            xts_log.warning("PARSE failed | error=%s | raw=%s", exc, str(event_data)[:2000])
            return None
