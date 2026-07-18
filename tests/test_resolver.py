from datetime import datetime

import pytest

from bridge import tv_to_xts_description, is_monthly_expiry


@pytest.mark.parametrize(
    "tv_ticker,expected",
    [
        ("NIFTY260630C27000", "NIFTY26JUN27000CE"),  # monthly (last Tuesday)
        ("NIFTY260623C25400", "NIFTY2662325400CE"),  # weekly
    ],
)
def test_valid_tickers(tv_ticker, expected):
    assert tv_to_xts_description(tv_ticker) == expected


@pytest.mark.parametrize(
    "tv_ticker",
    [
        "",
        "NIFTY",
        "NIFTY26063027000",
        "NIFTY260630X27000",
        "NIFTY26JUN27000CE",
        "NIFTY260630C",
        "260630C27000",
        "NIFTY260630C27A00",
        "NIFTY-260630C27000",
    ],
)
def test_invalid_tickers(tv_ticker):
    assert tv_to_xts_description(tv_ticker) is None


def test_invalid_calendar_date():
    assert tv_to_xts_description("NIFTY260631C27000") is None


def test_last_tuesday_is_monthly():
    assert is_monthly_expiry(datetime.strptime("260630", "%y%m%d"))


def test_non_last_tuesday_is_weekly():
    assert not is_monthly_expiry(datetime.strptime("260623", "%y%m%d"))
