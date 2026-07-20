"""Infer equity/F&O vs crypto from an unchanged signal payload.

No new client fields are required. Classification uses existing
``symbol`` / ``ticker`` / ``exchange_segment`` / instrument id hints and
known Indian master / TradingView option shapes.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, Literal, Optional

AssetClass = Literal["crypto", "india"]

CRYPTO_SEGMENT = "CRYPTO"
INDIA_SEGMENTS = frozenset(
    {"NSECM", "NSEFO", "NSECDS", "NSECO", "BSECM", "BSEFO", "BSECDS", "NCDEX", "MCXFO"}
)
CRYPTO_SEGMENTS = frozenset({"CRYPTO", "EXCHANGE1", "SPOT", "CRYPTO_SPOT"})

# TradingView-style option tickers used by the India bridge path.
_TV_OPTION_RE = re.compile(
    r"^[A-Z][A-Z0-9&\-]*?\d{6}[CP]\d+(?:\.\d+)?$",
    re.IGNORECASE,
)

# Common crypto pair forms: BTCUSDT, BTC/USDT, BTC-USDT, ETHUSD, SOL_USDC
_CRYPTO_PAIR_RE = re.compile(
    r"^[A-Z0-9]{2,15}[/_\-]?("
    r"USDT|USDC|USD|BUSD|BTC|ETH|BNB|EUR|INR"
    r")$",
    re.IGNORECASE,
)

_CRYPTO_BASES = frozenset(
    {
        "BTC",
        "ETH",
        "SOL",
        "XRP",
        "BNB",
        "DOGE",
        "ADA",
        "AVAX",
        "DOT",
        "MATIC",
        "POL",
        "LINK",
        "LTC",
        "TRX",
        "SHIB",
        "ATOM",
        "NEAR",
        "UNI",
        "APT",
        "ARB",
        "OP",
        "SUI",
        "PEPE",
        "WIF",
        "TON",
        "FIL",
        "ICP",
        "AAVE",
        "MKR",
        "CRV",
    }
)


def _ticker(signal: Dict[str, Any]) -> str:
    raw = signal.get("ticker") or signal.get("symbol") or signal.get("instrument_name") or ""
    return str(raw).strip()


def _segment(signal: Dict[str, Any]) -> str:
    raw = (
        signal.get("exchange_segment")
        or signal.get("exchangeSegment")
        or ""
    )
    return str(raw).strip().upper()


def normalize_crypto_symbol(symbol: str) -> str:
    """Normalize to Exchange1 display form ``BASE/QUOTE`` (e.g. BTC/USDT)."""
    s = str(symbol or "").strip().upper().replace(" ", "")
    s = s.replace("_", "/").replace("-", "/")
    if "/" in s:
        base, quote = s.split("/", 1)
        return f"{base}/{quote}"
    for quote in ("USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "BNB", "EUR", "INR"):
        if s.endswith(quote) and len(s) > len(quote):
            return f"{s[: -len(quote)]}/{quote}"
    return s


def crypto_compact_symbol(symbol: str) -> str:
    """``BTC/USDT`` → ``BTCUSDT`` for catalogs / hashing."""
    return normalize_crypto_symbol(symbol).replace("/", "")


def crypto_instrument_id(symbol: str) -> int:
    """Stable positive int id derived from the crypto symbol (fits OMS int field)."""
    digest = hashlib.sha1(crypto_compact_symbol(symbol).encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 2_000_000_000 + 1


def looks_like_tv_option(ticker: str) -> bool:
    return bool(ticker) and bool(_TV_OPTION_RE.match(ticker.strip().upper()))


def looks_like_crypto_symbol(ticker: str) -> bool:
    if not ticker:
        return False
    compact = crypto_compact_symbol(ticker).upper()
    if _CRYPTO_PAIR_RE.match(compact):
        return True
    for quote in ("USDT", "USDC", "BUSD", "USD"):
        if compact.endswith(quote) and compact[: -len(quote)] in _CRYPTO_BASES:
            return True
    if compact in _CRYPTO_BASES:
        return True
    return False


def classify_signal(signal: Dict[str, Any]) -> AssetClass:
    """Return ``crypto`` or ``india`` for routing. Payload schema is unchanged."""
    seg = _segment(signal)
    if seg in CRYPTO_SEGMENTS:
        return "crypto"
    if seg in INDIA_SEGMENTS:
        return "india"

    ticker = _ticker(signal)
    if not ticker:
        # ATM / NIFTY default path
        return "india"

    if looks_like_tv_option(ticker):
        return "india"

    if looks_like_crypto_symbol(ticker):
        return "crypto"

    # Explicit numeric India instrument without segment still treated as india
    iid = signal.get("exchange_instrument_id") or signal.get("exchangeInstrumentID")
    if iid is not None and str(iid).strip().isdigit():
        return "india"

    return "india"


def classify_symbol(symbol: str, segment: Optional[str] = None) -> AssetClass:
    """Convenience wrapper for UI / tests."""
    return classify_signal(
        {
            "symbol": symbol,
            "exchange_segment": segment or "",
        }
    )
