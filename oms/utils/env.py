"""
Environment variable loading shared across all entry points.

Secrets (broker API keys, client ids) live in a local ``.env`` file that is
never committed. This module loads that file once per process and exposes small
typed accessors. See ``.env.example`` for the full list of variables.
"""

from __future__ import annotations

import os
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dotenv is a declared dependency
    def load_dotenv(*_args, **_kwargs):  # type: ignore
        return False


_loaded = False


def load_env() -> None:
    """Load the nearest ``.env`` file into ``os.environ`` (idempotent)."""
    global _loaded
    if not _loaded:
        load_dotenv()
        _loaded = True


def env(name: str, default: str = "") -> str:
    """Return the environment variable *name*, or *default* if unset/empty."""
    load_env()
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def require_env(name: str) -> str:
    """Return the environment variable *name* or raise if it is missing."""
    load_env()
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Required environment variable {name!r} is not set. "
            f"Copy .env.example to .env and fill it in."
        )
    return value
