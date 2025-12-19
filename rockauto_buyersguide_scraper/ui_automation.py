"""UI automation helpers with rate-limit backoff."""
from __future__ import annotations

from typing import Any, Dict, Optional

from .backoff import RateLimitError, run_with_backoff


def _simulate_rate_limit(target: str) -> None:
    if "rate_limit" in target.lower():
        raise RateLimitError("UI rate limited")


async def _perform_ui_lookup(target: str) -> Dict[str, Any]:
    _simulate_rate_limit(target)
    return {"status": "ok", "data": f"ui:{target}"}


async def fetch_ui_data(
    target: Optional[str],
    *,
    max_retries: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 10.0,
    jitter: float = 0.1,
) -> Dict[str, Any]:
    if not target:
        return {"status": "skipped", "data": ""}

    async def operation() -> Dict[str, Any]:
        return await _perform_ui_lookup(target)

    return await run_with_backoff(
        operation,
        max_retries=max_retries,
        base_delay=base_delay,
        max_delay=max_delay,
        jitter=jitter,
    )
