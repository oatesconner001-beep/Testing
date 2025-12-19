from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, TypeVar

from .logger import Logger

PartT = TypeVar("PartT")


@dataclass(frozen=True)
class PartOutcome:
    success: bool
    retries: int = 0
    cache_hit: bool = False
    failure_reason: Optional[str] = None


@dataclass
class RunSummary:
    total: int = 0
    successes: int = 0
    failures: int = 0
    retries: int = 0
    cache_hits: int = 0


class PartRunner:
    def __init__(self, logger: Logger) -> None:
        self.logger = logger

    def run(
        self,
        parts: Iterable[PartT],
        processor: Callable[[PartT], PartOutcome],
    ) -> RunSummary:
        summary = RunSummary()
        for part in parts:
            summary.total += 1
            part_label = str(part)
            self.logger.info("part.start", part=part_label)
            start_time = time.monotonic()
            try:
                outcome = processor(part)
            except Exception as exc:  # pragma: no cover - defensive logging
                duration_ms = (time.monotonic() - start_time) * 1000
                summary.failures += 1
                self.logger.error(
                    "part.failure",
                    part=part_label,
                    duration_ms=round(duration_ms, 3),
                    error=str(exc),
                )
                continue

            duration_ms = (time.monotonic() - start_time) * 1000
            summary.retries += outcome.retries
            summary.cache_hits += int(outcome.cache_hit)

            if outcome.success:
                summary.successes += 1
                self.logger.info(
                    "part.success",
                    part=part_label,
                    duration_ms=round(duration_ms, 3),
                    retries=outcome.retries,
                    cache_hit=outcome.cache_hit,
                )
            else:
                summary.failures += 1
                self.logger.error(
                    "part.failure",
                    part=part_label,
                    duration_ms=round(duration_ms, 3),
                    retries=outcome.retries,
                    cache_hit=outcome.cache_hit,
                    reason=outcome.failure_reason,
                )

            if outcome.retries:
                self.logger.warning(
                    "part.retries",
                    part=part_label,
                    retries=outcome.retries,
                )

        self.logger.info(
            "run.summary",
            total=summary.total,
            successes=summary.successes,
            failures=summary.failures,
            retries=summary.retries,
            cache_hits=summary.cache_hits,
        )
        return summary
