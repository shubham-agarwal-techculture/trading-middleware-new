"""
Signal bridge package — HTTP ingress, contract resolution, OMS routing.

Public helpers used by tests and the thin launcher are re-exported here.
"""

from bridge.positions import get_position_display_values
from bridge.resolution import (
    MASTER_SEGMENTS,
    find_contract_by_instrument_id,
    is_monthly_expiry,
    parse_tv_option_ticker,
    resolve_contract_by_ticker,
    tv_to_xts_description,
)

# Exposed for characterization tests that monkeypatch the loader cache.
from bridge import resolution as _resolution

_master_loaders = _resolution._master_loaders

__all__ = [
    "MASTER_SEGMENTS",
    "_master_loaders",
    "find_contract_by_instrument_id",
    "get_position_display_values",
    "is_monthly_expiry",
    "parse_tv_option_ticker",
    "resolve_contract_by_ticker",
    "tv_to_xts_description",
]
