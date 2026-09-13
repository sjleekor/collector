"""Token-bucket pacing for sources that publish an explicit request quota.

This is deliberately *not* :class:`~collector.kr.util.pipeline.HumanThrottle`.
That one sleeps a random human-looking interval and takes long rests, because
it exists to keep KRX MDC scraping from looking like a bot.  A published quota
is a different problem: KIS allows 20 requests/second per account and says so,
so the correct behaviour is to run right up to the line and never past it —
randomised delays would only make a fixed budget slower and less predictable.

Using the scraping throttle here would also hide the quota.  A token bucket
makes the limit a number in one place that the run can audit afterwards.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

logger = logging.getLogger(__name__)


class TokenBucket:
    """Allow ``rate_per_second`` requests per second with a small burst.

    ``acquire`` blocks until a token is available.  ``rate_per_second <= 0``
    disables pacing entirely, which is what tests use.
    """

    def __init__(
        self,
        rate_per_second: float,
        *,
        burst: int = 1,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._rate = max(0.0, float(rate_per_second))
        self._capacity = max(1.0, float(burst))
        self._sleep_fn = sleep_fn
        self._monotonic_fn = monotonic_fn
        self._tokens = self._capacity
        self._updated_at = monotonic_fn()
        self.waits = 0
        self.total_wait_seconds = 0.0

    @property
    def enabled(self) -> bool:
        return self._rate > 0

    def acquire(self, tokens: float = 1.0) -> None:
        """Block until ``tokens`` are available, then consume them."""
        if not self.enabled:
            return

        needed = max(0.0, tokens)
        while True:
            self._refill()
            if self._tokens >= needed:
                self._tokens -= needed
                return
            wait_seconds = (needed - self._tokens) / self._rate
            self.waits += 1
            self.total_wait_seconds += wait_seconds
            self._sleep_fn(wait_seconds)

    def _refill(self) -> None:
        now = self._monotonic_fn()
        elapsed = max(0.0, now - self._updated_at)
        self._updated_at = now
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
