"""Fixed-window rate limiter.

The numbers reported in `meta.api.rate_limit` are the numbers actually enforced
here — an advertised limit that isn't applied is its own kind of silent lie.
"""
from __future__ import annotations

import time
from collections import defaultdict


class RateLimiter:
    def __init__(self, limit: int, window_seconds: int) -> None:
        self.limit = limit
        self.window = window_seconds
        self._buckets: dict[str, tuple[int, float]] = defaultdict(lambda: (0, 0.0))

    def check(self, key: str) -> tuple[bool, int, int]:
        """Return (allowed, remaining, retry_after_seconds)."""
        now = time.monotonic()
        count, window_start = self._buckets[key]

        if now - window_start >= self.window:
            count, window_start = 0, now

        if count >= self.limit:
            self._buckets[key] = (count, window_start)
            return False, 0, max(1, int(self.window - (now - window_start)))

        self._buckets[key] = (count + 1, window_start)
        return True, self.limit - count - 1, 0

    def reset(self) -> None:
        self._buckets.clear()
