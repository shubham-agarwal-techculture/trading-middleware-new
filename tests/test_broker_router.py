"""Tests for BrokerRouter segment routing."""

from __future__ import annotations

import pytest

from oms.broker.router import BrokerRouter
from tests.conftest import FakeBroker


@pytest.fixture
def router():
    return BrokerRouter(FakeBroker(), FakeBroker())


def test_pick_india_segment(router):
    chosen = router._pick(exchange_segment="NSEFO", exchange_instrument_id=1)
    assert chosen is router.xts


def test_pick_crypto_segment(router):
    chosen = router._pick(exchange_segment="CRYPTO", exchange_instrument_id=1)
    assert chosen is router.exchange1


def test_pick_does_not_raise_when_exchange_segment_in_kwargs(router):
    """Regression: passing exchange_segment in kwargs must not duplicate the arg."""
    chosen = router._pick(
        exchange_segment="NSEFO",
        exchange_instrument_id=123,
        product_type="MIS",
    )
    assert chosen is router.xts
