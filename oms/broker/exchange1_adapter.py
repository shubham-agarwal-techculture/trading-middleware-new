"""
eXchange1 spot REST adapter (crypto).

Auth headers (from eXchange1 OpenAPI docs):
  X-CH-APIKEY, X-CH-TS, X-CH-SIGN
  SIGN = HMAC_SHA256(secret, timestamp + METHOD + requestPath + body)

Place:  POST /sapi/v2/order
Cancel: POST /sapi/v2/cancel
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from typing import Any, Dict, Optional

import httpx

from oms.broker.base import AbstractBrokerAdapter, BrokerError
from oms.utils.logger import get_logger

log = get_logger(__name__)
ex1_log = logging.getLogger("exchange1")

DEFAULT_BASE_URL = "https://openapi.exchange1.com"


def _normalize_symbol(symbol: str) -> str:
    s = str(symbol or "").strip().upper().replace(" ", "")
    s = s.replace("_", "/").replace("-", "/")
    if "/" in s:
        base, quote = s.split("/", 1)
        return f"{base}/{quote}"
    for quote in ("USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "BNB", "EUR", "INR"):
        if s.endswith(quote) and len(s) > len(quote):
            return f"{s[: -len(quote)]}/{quote}"
    return s


class Exchange1BrokerAdapter(AbstractBrokerAdapter):
    """Spot trading adapter for eXchange1 OpenAPI."""

    def __init__(self, config) -> None:
        self._cfg = config
        self._base_url = (getattr(config, "url", "") or DEFAULT_BASE_URL).rstrip("/")
        self._api_key = getattr(config, "app_key", "") or ""
        self._api_secret = getattr(config, "secret_key", "") or ""
        self._verify_ssl = bool(getattr(config, "verify_ssl", True))
        self._client: Optional[httpx.AsyncClient] = None
        self._logged_in = False
        # broker_order_id → symbol (needed for cancel)
        self._order_symbols: Dict[str, str] = {}
        self.token: Optional[str] = None  # not used; keeps router parity with XTS

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=30.0, verify=self._verify_ssl
            )
        return self._client

    def _sign(self, timestamp: str, method: str, path: str, body: str) -> str:
        payload = f"{timestamp}{method.upper()}{path}{body}"
        return hmac.new(
            self._api_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _headers(self, method: str, path: str, body: str = "") -> Dict[str, str]:
        ts = str(int(time.time() * 1000))
        return {
            "Content-Type": "application/json",
            "X-CH-APIKEY": self._api_key,
            "X-CH-TS": ts,
            "X-CH-SIGN": self._sign(ts, method, path, body),
        }

    async def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        client = await self._get_client()
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False) if payload else ""
        url = f"{self._base_url}{path}"
        headers = self._headers(method, path, body)
        # Never log raw API key / signature.
        safe_headers = {
            k: ("***" if k.upper() in ("X-CH-APIKEY", "X-CH-SIGN") else v)
            for k, v in headers.items()
        }
        ex1_log.info(
            "REST request | %s %s | headers=%s | body=%s",
            method.upper(),
            path,
            safe_headers,
            body[:4000] if body else "",
        )
        log.info("Exchange1 %s %s", method, path)
        try:
            resp = await client.request(
                method,
                url,
                content=body.encode("utf-8") if body and method.upper() != "GET" else None,
                headers=headers,
            )
        except Exception as exc:
            ex1_log.error("REST transport error | %s %s | error=%s", method, path, exc)
            raise BrokerError(f"Exchange1 request failed: {exc}") from exc

        body_preview = (resp.text or "")[:8000]
        ex1_log.info(
            "REST response | %s %s | status=%s | body=%s",
            method.upper(),
            path,
            resp.status_code,
            body_preview,
        )
        try:
            data = resp.json()
        except Exception:
            data = {"raw": resp.text}

        if resp.status_code >= 400:
            ex1_log.error(
                "REST HTTP error | %s %s | status=%s | body=%s",
                method.upper(),
                path,
                resp.status_code,
                body_preview,
            )
            raise BrokerError(
                f"Exchange1 HTTP {resp.status_code}: {data}",
                code=str(resp.status_code),
                description=str(data),
            )
        # Some Exchange1 responses use code != 0 for errors
        code = data.get("code") if isinstance(data, dict) else None
        if code not in (None, 0, "0"):
            ex1_log.error(
                "REST API error | %s %s | code=%s | body=%s",
                method.upper(),
                path,
                code,
                body_preview,
            )
            raise BrokerError(
                f"Exchange1 error: {data}",
                code=str(code),
                description=str(data.get("msg") or data),
            )
        return data if isinstance(data, dict) else {"result": data}

    async def login(self) -> Dict[str, Any]:
        if not self._api_key or not self._api_secret:
            raise BrokerError(
                "Exchange1 API key/secret not configured "
                "(crypto_broker.app_key / secret_key)"
            )
        # Spot ping (public) — validates connectivity; auth is per-request.
        client = await self._get_client()
        try:
            ping_url = f"{self._base_url}/sapi/v1/ping"
            ex1_log.info("REST request | GET /sapi/v1/ping | connectivity check")
            resp = await client.get(ping_url)
            ex1_log.info(
                "REST response | GET /sapi/v1/ping | status=%s | body=%s",
                resp.status_code,
                (resp.text or "")[:2000],
            )
            self._logged_in = resp.status_code < 500
        except Exception as exc:
            ex1_log.error("Connectivity check failed | error=%s", exc)
            raise BrokerError(f"Exchange1 connectivity check failed: {exc}") from exc
        log.info("Exchange1 adapter ready", base_url=self._base_url)
        ex1_log.info("Adapter ready | base_url=%s", self._base_url)
        return {"ok": True, "base_url": self._base_url}

    async def place_order(
        self,
        exchange_segment: str,
        exchange_instrument_id: int,
        product_type: str,
        order_type: str,
        order_side: str,
        time_in_force: str,
        disclosed_quantity: int,
        order_quantity: float | int,
        limit_price: float = 0.0,
        stop_price: float = 0.0,
        order_unique_identifier: str = "",
        instrument_name: str = "",
    ) -> Dict[str, Any]:
        if not self._logged_in:
            await self.login()

        symbol = _normalize_symbol(instrument_name or str(exchange_instrument_id))
        otype = (order_type or "LIMIT").upper()
        if otype not in ("LIMIT", "MARKET", "IOC", "FOK", "POST_ONLY"):
            otype = "LIMIT"
        side = (order_side or "BUY").upper()

        payload: Dict[str, Any] = {
            "symbol": symbol,
            "volume": float(order_quantity),
            "side": side,
            "type": otype,
        }
        if otype == "LIMIT" or otype in ("FOK", "IOC", "POST_ONLY"):
            payload["price"] = float(limit_price)
        if order_unique_identifier:
            payload["newClientOrderId"] = str(order_unique_identifier)[:32]

        data = await self._request("POST", "/sapi/v2/order", payload)
        result = data.get("data") or data.get("result") or data
        broker_order_id = str(
            result.get("orderIdString")
            or result.get("orderId")
            or result.get("order_id")
            or ""
        )
        if broker_order_id:
            self._order_symbols[broker_order_id] = symbol
        log.info(
            "Exchange1 order placed",
            broker_order_id=broker_order_id,
            symbol=symbol,
            side=side,
            qty=order_quantity,
        )
        ex1_log.info(
            "ORDER placed | broker_id=%s | symbol=%s | side=%s | qty=%s | type=%s | price=%s",
            broker_order_id,
            symbol,
            side,
            order_quantity,
            otype,
            limit_price,
        )
        return {"broker_order_id": broker_order_id, "raw": result, "symbol": symbol}

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
        instrument_name: str = "",
    ) -> Dict[str, Any]:
        # Spot modify is cancel+replace on many venues; not exposed as a single call here.
        raise BrokerError(
            "Exchange1 spot modify is not supported; cancel and place a new order"
        )

    async def cancel_order(self, broker_order_id: str) -> Dict[str, Any]:
        symbol = self._order_symbols.get(str(broker_order_id), "")
        if not symbol:
            raise BrokerError(
                f"Cannot cancel Exchange1 order {broker_order_id}: symbol unknown"
            )
        payload = {"orderId": str(broker_order_id), "symbol": symbol}
        data = await self._request("POST", "/sapi/v2/cancel", payload)
        return {"raw": data.get("data") or data}

    async def cancel_all_orders(
        self,
        exchange_segment: Optional[str] = None,
        exchange_instrument_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        # Best-effort: cancel each tracked open id; full open-order list if available.
        book = await self.get_order_book()
        results = []
        for row in book.get("result") or []:
            oid = str(row.get("orderId") or row.get("orderIdString") or "")
            sym = row.get("symbol") or row.get("symbolName") or ""
            if not oid:
                continue
            if sym:
                self._order_symbols[oid] = _normalize_symbol(str(sym))
            try:
                results.append(await self.cancel_order(oid))
            except Exception as exc:
                results.append({"error": str(exc), "order_id": oid})
        return {"raw": results}

    async def squareoff_position(
        self,
        exchange_segment: str,
        exchange_instrument_id: int,
        product_type: str,
        instrument_name: str = "",
    ) -> Dict[str, Any]:
        # Flatten by market-selling the symbol; quantity must come from positions API.
        raise BrokerError(
            "Exchange1 squareoff via OMS uses a reverse MARKET order from the bridge"
        )

    async def get_order_book(self) -> Dict[str, Any]:
        try:
            data = await self._request("GET", "/sapi/v2/openOrders")
            rows = data.get("data") or data.get("result") or data.get("list") or []
            if isinstance(rows, dict):
                rows = rows.get("list") or rows.get("orders") or []
            return {"result": rows if isinstance(rows, list) else []}
        except BrokerError:
            return {"result": []}

    async def get_positions(self) -> Dict[str, Any]:
        try:
            data = await self._request("GET", "/sapi/v1/account")
            return {"result": data.get("data") or data.get("result") or data}
        except BrokerError:
            return {"result": []}

    def parse_order_event(self, event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a sparse Exchange1 status payload if pushed later."""
        if not event_data:
            return None
        oid = str(
            event_data.get("orderIdString")
            or event_data.get("orderId")
            or event_data.get("broker_order_id")
            or ""
        )
        if not oid:
            return None
        status = str(event_data.get("status", "")).upper()
        status_map = {
            "0": "PENDING",
            "NEW": "PENDING",
            "PARTIALLY_FILLED": "PARTIAL",
            "FILLED": "FILLED",
            "CANCELED": "CANCELLED",
            "CANCELLED": "CANCELLED",
            "REJECTED": "REJECTED",
        }
        return {
            "broker_order_id": oid,
            "status": status_map.get(status, status or "PENDING"),
            "filled_quantity": float(
                event_data.get("executedQty") or event_data.get("filled_quantity") or 0
            ),
            "average_price": float(
                event_data.get("price") or event_data.get("average_price") or 0
            ),
            "raw": event_data,
        }

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        self._client = None
        self._logged_in = False
