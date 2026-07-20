"""Route OMS broker calls to XTS (India) or Exchange1 (crypto) by segment."""

from __future__ import annotations

from typing import Any, Dict, Optional, Set

from oms.broker.base import AbstractBrokerAdapter, BrokerError
from oms.utils.logger import get_logger

log = get_logger(__name__)

DEFAULT_CRYPTO_SEGMENTS = frozenset({"CRYPTO", "EXCHANGE1", "SPOT", "CRYPTO_SPOT"})


class BrokerRouter(AbstractBrokerAdapter):
    """Delegates to ``primary`` (XTS) or ``crypto`` (Exchange1) by exchange_segment."""

    def __init__(
        self,
        primary: AbstractBrokerAdapter,
        crypto: AbstractBrokerAdapter,
        crypto_segments: Optional[Set[str]] = None,
    ) -> None:
        self._primary = primary
        self._crypto = crypto
        self._crypto_segments = {
            s.upper() for s in (crypto_segments or DEFAULT_CRYPTO_SEGMENTS)
        }

    @property
    def xts(self) -> AbstractBrokerAdapter:
        return self._primary

    @property
    def exchange1(self) -> AbstractBrokerAdapter:
        return self._crypto

    @property
    def token(self) -> Optional[str]:
        return getattr(self._primary, "token", None)

    def _is_crypto(self, **kwargs: Any) -> bool:
        seg = str(kwargs.get("exchange_segment") or "").upper()
        return seg in self._crypto_segments

    def _pick(self, **kwargs: Any) -> AbstractBrokerAdapter:
        if self._is_crypto(**kwargs):
            return self._crypto
        return self._primary

    async def login(self) -> Dict[str, Any]:
        primary_result = await self._primary.login()
        crypto_result: Dict[str, Any] = {"ok": False}
        try:
            crypto_result = await self._crypto.login()
        except Exception as exc:
            log.warning(
                "Crypto broker (Exchange1) login failed — India routing still available",
                error=str(exc),
            )
        return {"primary": primary_result, "crypto": crypto_result}

    async def place_order(self, **kwargs: Any) -> Dict[str, Any]:
        broker = self._pick(**kwargs)
        if broker is self._crypto and not getattr(self._crypto, "_logged_in", True):
            try:
                await self._crypto.login()
            except Exception as exc:
                raise BrokerError(f"Crypto broker unavailable: {exc}") from exc
        return await broker.place_order(**kwargs)

    async def modify_order(self, **kwargs: Any) -> Dict[str, Any]:
        # Prefer primary unless caller tagged crypto via instrument_name segment in kwargs
        # Modify path usually has broker_order_id only — try primary first is wrong for crypto.
        # OrderManager doesn't pass segment on modify; try crypto then primary is unsafe.
        # Keep primary for modify; crypto adapter raises unsupported anyway.
        return await self._primary.modify_order(**kwargs)

    async def cancel_order(self, broker_order_id: str) -> Dict[str, Any]:
        # Try crypto first if it knows the id, else primary.
        crypto_syms = getattr(self._crypto, "_order_symbols", {})
        if str(broker_order_id) in crypto_syms:
            return await self._crypto.cancel_order(broker_order_id)
        try:
            return await self._primary.cancel_order(broker_order_id)
        except Exception:
            return await self._crypto.cancel_order(broker_order_id)

    async def cancel_all_orders(
        self,
        exchange_segment: Optional[str] = None,
        exchange_instrument_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        if exchange_segment and str(exchange_segment).upper() in self._crypto_segments:
            return await self._crypto.cancel_all_orders(
                exchange_segment=exchange_segment,
                exchange_instrument_id=exchange_instrument_id,
            )
        primary = await self._primary.cancel_all_orders(
            exchange_segment=exchange_segment,
            exchange_instrument_id=exchange_instrument_id,
        )
        crypto = await self._crypto.cancel_all_orders()
        return {"primary": primary, "crypto": crypto}

    async def squareoff_position(self, **kwargs: Any) -> Dict[str, Any]:
        return await self._pick(**kwargs).squareoff_position(**kwargs)

    async def get_order_book(self) -> Dict[str, Any]:
        primary = await self._primary.get_order_book()
        crypto = await self._crypto.get_order_book()
        rows = []
        for block in (primary, crypto):
            part = block.get("result") if isinstance(block, dict) else None
            if isinstance(part, list):
                rows.extend(part)
        return {"result": rows, "primary": primary, "crypto": crypto}

    async def get_positions(self) -> Dict[str, Any]:
        return {
            "primary": await self._primary.get_positions(),
            "crypto": await self._crypto.get_positions(),
        }

    def parse_order_event(self, event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # XTS events come through the XTS adapter; prefer primary parser.
        parser = getattr(self._primary, "parse_order_event", None)
        if callable(parser):
            return parser(event_data)
        crypto_parser = getattr(self._crypto, "parse_order_event", None)
        if callable(crypto_parser):
            return crypto_parser(event_data)
        return None

    async def close(self) -> None:
        await self._primary.close()
        await self._crypto.close()
