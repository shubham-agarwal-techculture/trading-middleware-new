"""Async token-bucket rate limiter for broker API calls."""

from __future__ import annotations

import asyncio
import time


class RateLimiter:
    """
    Async rate limiter that enforces a maximum number of calls per second.

    Usage::

        limiter = RateLimiter(max_rate=10)  # 10 calls/sec
        await limiter.acquire()
        # make API call
    """

    def __init__(self, max_rate: float) -> None:
        if max_rate <= 0:
            raise ValueError("max_rate must be positive")
        self.max_rate = max_rate
        self._min_interval = 1.0 / max_rate
        self._last_call: float = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._min_interval - (now - self._last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()
