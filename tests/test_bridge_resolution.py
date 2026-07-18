"""
Characterization tests for the signal bridge's contract-resolution logic.

Master data is injected through a fake loader so the tests stay fast and do not
depend on the multi-megabyte CSV files under ``master_data/``. These pin the
behavior that the Phase 3 ``bridge/resolution.py`` extraction must preserve.
"""

from __future__ import annotations

import asyncio

import pytest

import bridge


class _FakeLoader:
    """Stand-in for ``ContractLoader`` exposing a ``.contracts`` list."""

    def __init__(self, contracts):
        self.contracts = contracts


def _contract(**overrides):
    base = {
        "ExchangeSegment": "NSEFO",
        "ExchangeInstrumentID": "41723",
        "Name": "NIFTY",
        "Description": "NIFTY26JUN27000CE",
        "NameWithSeries": "NIFTY-OPTIDX",
        "OptionType": "3",  # 3 = CE, 4 = PE
        "StrikePrice": "27000",
        "ContractExpiration": "2026-06-30T14:30:00",
        "LotSize": "75",
    }
    base.update(overrides)
    return base


@pytest.fixture
def only_nsefo(monkeypatch):
    """Populate the loader cache: NSEFO has one contract, everything else empty."""
    contracts = [_contract()]
    for seg in bridge.MASTER_SEGMENTS:
        monkeypatch.setitem(
            bridge._master_loaders,
            seg,
            _FakeLoader(contracts) if seg == "NSEFO" else None,
        )
    return contracts


def test_parse_tv_option_ticker_call():
    parsed = bridge.parse_tv_option_ticker("NIFTY260630C27000")
    assert parsed["name"] == "NIFTY"
    assert parsed["option_type_csv"] == "3"
    assert parsed["strike"] == 27000.0


def test_parse_tv_option_ticker_put_and_alias():
    parsed = bridge.parse_tv_option_ticker("BSX260723P81100")
    assert parsed["name"] == "SENSEX"  # TV alias mapping
    assert parsed["option_type_csv"] == "4"


def test_parse_tv_option_ticker_rejects_non_option():
    assert bridge.parse_tv_option_ticker("RELIANCE-EQ") is None


def test_resolve_by_exact_description(only_nsefo):
    result = asyncio.run(bridge.resolve_contract_by_ticker("NIFTY26JUN27000CE"))
    assert result is not None
    assert result["ExchangeInstrumentID"] == "41723"


def test_resolve_by_tv_ticker(only_nsefo):
    result = asyncio.run(bridge.resolve_contract_by_ticker("NIFTY260630C27000"))
    assert result is not None
    assert result["Description"] == "NIFTY26JUN27000CE"


def test_resolve_unknown_returns_none(only_nsefo):
    assert asyncio.run(bridge.resolve_contract_by_ticker("DOESNOTEXIST99")) is None


def test_find_contract_by_instrument_id(only_nsefo):
    result = bridge.find_contract_by_instrument_id("NSEFO", 41723)
    assert result is not None
    assert result["Description"] == "NIFTY26JUN27000CE"


def test_find_contract_by_instrument_id_missing(only_nsefo):
    assert bridge.find_contract_by_instrument_id("NSEFO", 99999) is None
