"""
Broker factory — construct a broker adapter from configuration.

Keeps ``run_oms.py`` decoupled from any concrete adapter: it asks for a
broker by ``config.type`` and the factory returns the matching
:class:`~oms.broker.base.AbstractBrokerAdapter`. Adding a new broker means
registering it here, with no changes to the server or order manager.

When ``crypto_broker`` is provided and enabled, returns a :class:`BrokerRouter`
that sends CRYPTO segment orders to Exchange1 and everything else to XTS.
"""

from __future__ import annotations

from typing import Any, Optional

from oms.broker.base import AbstractBrokerAdapter


def _build_single(config) -> AbstractBrokerAdapter:
    broker_type = (getattr(config, "type", "") or "xts").lower()

    if broker_type == "xts":
        from oms.broker.xts_adapter import XTSBrokerAdapter

        return XTSBrokerAdapter(config)

    if broker_type in ("exchange1", "ex1", "crypto"):
        from oms.broker.exchange1_adapter import Exchange1BrokerAdapter

        return Exchange1BrokerAdapter(config)

    raise ValueError(
        f"Unsupported broker type: {getattr(config, 'type', None)!r}. "
        "Supported types: 'xts', 'exchange1'."
    )


def create_broker(
    config,
    crypto_config: Optional[Any] = None,
) -> AbstractBrokerAdapter:
    """Instantiate the broker adapter named by ``config.type``.

    Parameters
    ----------
    config :
        Primary broker configuration (usually XTS).
    crypto_config :
        Optional Exchange1 / crypto broker config. When present and
        ``enabled`` is true, wraps primary + crypto in a :class:`BrokerRouter`.
    """
    primary = _build_single(config)

    if crypto_config is None:
        return primary

    enabled = getattr(crypto_config, "enabled", True)
    if not enabled:
        return primary

    crypto_type = (getattr(crypto_config, "type", "") or "exchange1").lower()
    if crypto_type in ("", "none", "disabled"):
        return primary

    # Ensure type defaults to exchange1 for the crypto section
    if not getattr(crypto_config, "type", None):
        try:
            crypto_config.type = "exchange1"
        except Exception:
            pass

    crypto = _build_single(crypto_config)
    from oms.broker.router import BrokerRouter

    return BrokerRouter(primary, crypto)
