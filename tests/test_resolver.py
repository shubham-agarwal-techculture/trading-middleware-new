import pytest
from datetime import datetime

# Replace with your actual import
from nifty_signal_bridge import tv_to_xts_description, is_monthly_expiry


@pytest.mark.parametrize(
    "tv_ticker,expected",
    [
        # -------------------------
        # Monthly expiries (last Tuesday)
        # -------------------------
        # (
        #     "NIFTY260630C27000",
        #     "NIFTY26JUN27000CE",
        # ),
        # (
        #     "NIFTY260630P27000",
        #     "NIFTY26JUN27000PE",
        # ),
        # # -------------------------
        # # Weekly expiries
        # # -------------------------
        # (
        #     "NIFTY260623C27000",
        #     "NIFTY2662327000CE",
        # ),
        # (
        #     "NIFTY260623P27000",
        #     "NIFTY2662327000PE",
        # ),
        # (
        #     "NIFTY260707C26400",
        #     "NIFTY2670726400CE",
        # ),
        # (
        #     "NIFTY260707P26400",
        #     "NIFTY2670726400PE",
        # ),
        # # -------------------------
        # # Decimal strikes
        # # -------------------------
        # (
        #     "NIFTY260630C27000.0",
        #     "NIFTY26JUN27000CE",
        # ),
        # (
        #     "NIFTY260623P27000.0",
        #     "NIFTY2662327000PE",
        # ),
        # # -------------------------
        # # Case normalization
        # # -------------------------
        # (
        #     "nifty260630c27000",
        #     "NIFTY26JUN27000CE",
        # ),
        # # -------------------------
        # # Whitespace trimming
        # # -------------------------
        # (
        #     "  NIFTY260630C27000  ",
        #     "NIFTY26JUN27000CE",
        # ),
        # Call options
        # ("RELIANCE260630C1230", "RELIANCE26JUN1230CE"),
        # ("NIFTY260625C25000", "NIFTY26JUN25000CE"),
        # ("BANKNIFTY260730C55000", "BANKNIFTY26JUL55000CE"),
        # # Put options
        # ("RELIANCE260630P1230", "RELIANCE26JUN1230PE"),
        # ("NIFTY260625P25000", "NIFTY26JUN25000PE"),
        # ("BANKNIFTY260730P55000", "BANKNIFTY26JUL55000PE"),
        # # Decimal strike
        # ("RELIANCE260630C1230.0", "RELIANCE26JUN1230CE"),
        # ("RELIANCE260630P1230.0", "RELIANCE26JUN1230PE"),
        # # Decimal strike that should be preserved
        # ("RELIANCE260630C1230.5", "RELIANCE26JUN1230.5CE"),
        # # Lowercase input
        # ("reliance260630c1230", "RELIANCE26JUN1230CE"),
        # # Leading/trailing spaces
        # ("  NIFTY260625P25000  ", "NIFTY26JUN25000PE"),
        #
        ("NIFTY260630C27000", "NIFTY26JUN27000CE"),
        ("NIFTY260623C25400", "NIFTY2662325400CE"),
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
