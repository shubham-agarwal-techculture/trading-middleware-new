"""
Process/runtime helpers shared by every entry point.

Currently this centralizes the Windows event-loop workaround: on Windows,
Python defaults to the Proactor event loop, which does not implement the
``add_reader`` API that pyzmq's asyncio sockets require. All entry points call
:func:`use_selector_event_loop_policy` at startup to switch to the Selector
loop.
"""

from __future__ import annotations

import asyncio
import sys


def use_selector_event_loop_policy() -> None:
    """Force the Selector event-loop policy on Windows (no-op elsewhere)."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
