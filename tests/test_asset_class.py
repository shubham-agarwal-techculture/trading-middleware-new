"""Tests for crypto vs India signal classification (unchanged payload)."""

from __future__ import annotations

from bridge.asset_class import (
    CRYPTO_SEGMENT,
    classify_signal,
    classify_symbol,
    crypto_instrument_id,
    looks_like_crypto_symbol,
    looks_like_tv_option,
    normalize_crypto_symbol,
)


def test_normalize_crypto_symbol_variants():
    assert normalize_crypto_symbol("btcusdt") == "BTC/USDT"
    assert normalize_crypto_symbol("ETH-USDT") == "ETH/USDT"
    assert normalize_crypto_symbol("SOL/USDC") == "SOL/USDC"


def test_crypto_instrument_id_stable():
    a = crypto_instrument_id("BTCUSDT")
    b = crypto_instrument_id("BTC/USDT")
    assert a == b
    assert a > 0


def test_looks_like_patterns():
    assert looks_like_tv_option("NIFTY260721C25150")
    assert looks_like_tv_option("GOLDM260729C149000")
    assert not looks_like_tv_option("BTCUSDT")
    assert looks_like_crypto_symbol("BTCUSDT")
    assert looks_like_crypto_symbol("eth/usdt")
    assert not looks_like_crypto_symbol("RELIANCE")


def test_classify_signal_crypto_by_symbol():
    assert classify_signal({"symbol": "BTCUSDT", "action": "BUY", "position": "long"}) == "crypto"
    assert classify_signal({"ticker": "ETH-USDT", "action": "BUY", "position": "long"}) == "crypto"


def test_classify_signal_india_option_and_segment():
    assert (
        classify_signal({"symbol": "NIFTY260721C25150", "action": "BUY", "position": "long"})
        == "india"
    )
    assert (
        classify_signal(
            {
                "symbol": "SOMETHING",
                "exchange_segment": "NSEFO",
                "exchange_instrument_id": 123,
            }
        )
        == "india"
    )


def test_classify_signal_explicit_crypto_segment():
    assert (
        classify_signal(
            {"symbol": "FOO", "exchange_segment": CRYPTO_SEGMENT, "action": "BUY"}
        )
        == "crypto"
    )


def test_classify_symbol_helper():
    assert classify_symbol("BTCUSDT") == "crypto"
    assert classify_symbol("NIFTY260721C25150") == "india"
