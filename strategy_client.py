"""Compatibility shim — use ``clients.oms_client.OMSClient``."""

from clients.oms_client import OMSClient  # noqa: F401

__all__ = ["OMSClient"]
