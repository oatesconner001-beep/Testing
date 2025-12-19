"""Shared backoff helpers for rate-limited operations."""
from __future__ import annotations

import asyncio
import random
from typing import Awaitable, Callable, TypeVar


class RateLimitError(RuntimeError):
    """Raised when an upstream service signals rate limiting."""


T = TypeVar("T")


def _next_delay(base_delay: float, max_delay: float, attempt: int, jitter: float) -> float:
    delay = min(max_delay, base_delay * (2**attempt))
    if jitter:
        delay *= 1 + random.uniform(-jitter, jitter)
    return max(0.0, delay)


async def run_with_backoff(
    operation: Callable[[], Awaitable[T]],
    *,
    max_retries: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 10.0,
    jitter: float = 0.1,
) -> T:
    """Run an async operation with exponential backoff on RateLimitError."""
    for attempt in range(max_retries + 1):
        try:
            return await operation()
        except RateLimitError:
            if attempt >= max_retries:
                raise
            delay = _next_delay(base_delay, max_delay, attempt, jitter)
            await asyncio.sleep(delay)

    raise RuntimeError("Backoff retry loop exhausted unexpectedly.")
