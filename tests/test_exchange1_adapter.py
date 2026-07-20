"""Tests for Exchange1 symbol normalization and API paths."""

from oms.broker.exchange1_adapter import _normalize_symbol


def test_normalize_symbol_compact():
    assert _normalize_symbol("BTCUSDT") == "BTCUSDT"
    assert _normalize_symbol("btc/usdt") == "BTCUSDT"
    assert _normalize_symbol("BTC-USDT") == "BTCUSDT"
    assert _normalize_symbol("BTC_USDT") == "BTCUSDT"
    assert _normalize_symbol("ETH/USDT") == "ETHUSDT"
