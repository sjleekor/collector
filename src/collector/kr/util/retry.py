"""Retry / exponential-backoff helper (stdlib only).

Provides a generic retry decorator that can wrap any callable.  Uses
``time.sleep`` for delays — no third-party dependency required.

Usage::

    @retry(max_attempts=3, base_delay=1.0, backoff_factor=2.0)
    def flaky_call():
        ...

Note:
    This helper is intentionally NOT applied to any network call yet.
    It will be wired in when adapters are implemented.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

_F = TypeVar("_F", bound=Callable[..., Any])


def retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[_F], _F]:
    """Decorator: retry *func* with exponential backoff on failure.

    Args:
        max_attempts: Total number of attempts (including the first).
        base_delay: Initial delay in seconds before the first retry.
        backoff_factor: Multiplier applied to the delay after each retry.
        exceptions: Tuple of exception types that trigger a retry.

    Returns:
        Decorated function with retry behaviour.
    """

    def decorator(func: _F) -> _F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = base_delay
            last_exc: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        logger.error(
                            "All %d attempts failed for %s: %s",
                            max_attempts,
                            func.__qualname__,
                            exc,
                        )
                        raise
                    logger.warning(
                        "Attempt %d/%d for %s failed (%s), retrying in %.1fs…",
                        attempt,
                        max_attempts,
                        func.__qualname__,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                    delay *= backoff_factor
            # Should be unreachable, but satisfies type checker.
            raise last_exc  # type: ignore[misc]

        return wrapper  # type: ignore[return-value]

    return decorator
