"""Timestamp helpers — XTS exchange format and configurable timezone."""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo


_XTS_DT_FORMATS = (
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M:%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
)


def now_iso(timezone: str = "Asia/Kolkata") -> str:
    """Current time as ISO string in the given timezone."""
    try:
        tz = ZoneInfo(timezone)
    except Exception:
        tz = ZoneInfo("UTC")
    return datetime.now(tz).isoformat()


def parse_xts_datetime(value: str, timezone: str = "Asia/Kolkata") -> Optional[str]:
    """
    Parse XTS datetime strings (e.g. ``14-05-2021 11:17:30``) to ISO format.

    XTS sends exchange-local times without timezone; we attach *timezone*
    (default IST for NSE) for consistent strategy consumption.
    """
    if not value or not str(value).strip():
        return None
    text = str(value).strip()
    for fmt in _XTS_DT_FORMATS:
        try:
            dt = datetime.strptime(text, fmt)
            try:
                dt = dt.replace(tzinfo=ZoneInfo(timezone))
            except Exception:
                pass
            return dt.isoformat()
        except ValueError:
            continue
    return None
