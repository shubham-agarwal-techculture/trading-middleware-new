"""
Signal dispatcher — a small Command-pattern registry.

Incoming ZMQ signals carry a ``msg_type`` (``PLACE_ORDER``, ``CANCEL_ORDER``,
``MODIFY_ORDER``, ``SQUAREOFF``, ``CANCEL_ALL``). Rather than a growing
if/elif chain, handlers register themselves by ``msg_type`` and the dispatcher
routes each signal to the matching handler, falling back to an
``unknown`` handler otherwise.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, Optional

from oms.utils.logger import get_logger

log = get_logger(__name__)

Handler = Callable[[Dict[str, Any]], Awaitable[None]]


class SignalDispatcher:
    """Maps ``msg_type`` strings to async handler coroutines."""

    def __init__(self, on_unknown: Optional[Handler] = None) -> None:
        self._handlers: Dict[str, Handler] = {}
        self._on_unknown = on_unknown

    def register(self, msg_type: str, handler: Handler) -> None:
        """Register *handler* for the given *msg_type*."""
        self._handlers[msg_type] = handler

    async def dispatch(self, signal: Dict[str, Any]) -> None:
        """Route *signal* to its registered handler (or the unknown handler)."""
        msg_type = signal.get("msg_type", "")
        strategy_id = signal.get("strategy_id", "UNKNOWN")
        signal_id = signal.get("signal_id", "")

        log.info(
            "Signal received",
            msg_type=msg_type,
            strategy=strategy_id,
            signal_id=signal_id,
        )

        handler = self._handlers.get(msg_type)
        if handler is not None:
            await handler(signal)
        elif self._on_unknown is not None:
            await self._on_unknown(signal)
        else:
            log.warning("Unknown msg_type", msg_type=msg_type, strategy=strategy_id)
