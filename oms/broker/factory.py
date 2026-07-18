"""
Broker factory — construct a broker adapter from configuration.

Keeps ``run_oms.py`` decoupled from any concrete adapter: it asks for a
broker by ``config.type`` and the factory returns the matching
:class:`~oms.broker.base.AbstractBrokerAdapter`. Adding a new broker means
registering it here, with no changes to the server or order manager.
"""

from __future__ import annotations

from oms.broker.base import AbstractBrokerAdapter


def create_broker(config) -> AbstractBrokerAdapter:
    """Instantiate the broker adapter named by ``config.type``.

    Parameters
    ----------
    config : BrokerConfig
        Broker configuration; ``config.type`` selects the adapter.

    Raises
    ------
    ValueError
        If ``config.type`` is not a supported broker.
    """
    broker_type = (getattr(config, "type", "") or "xts").lower()

    if broker_type == "xts":
        # Imported lazily so unrelated brokers' optional deps aren't required.
        from oms.broker.xts_adapter import XTSBrokerAdapter

        return XTSBrokerAdapter(config)

    raise ValueError(
        f"Unsupported broker type: {config.type!r}. Supported types: 'xts'."
    )
