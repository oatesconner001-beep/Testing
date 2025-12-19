"""Task orchestration for RockAuto buyers guide scraping."""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from .csv_io import append_rows, ensure_output_schema, read_csv_in_batches
from .http_client import fetch_http_data
from .ui_automation import fetch_ui_data


EXTRA_FIELDS = [
    "http_status",
    "http_data",
    "ui_status",
    "ui_data",
]
URL_CACHE_SENTINEL = "__url__"
HTTP_CACHE_KIND = "http"
UI_CACHE_KIND = "ui"

CacheKey = Tuple[str, str, str]
CacheValue = Dict[str, str]


@dataclass
class Checkpoint:
    input_csv: str
    output_csv: str
    last_row_index: int
    updated_at: float


class CheckpointManager:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self.latest_path = self.directory / "latest.json"

    def load(self) -> Optional[Checkpoint]:
        if not self.latest_path.exists():
            return None
        with self.latest_path.open(encoding="utf-8") as handle:
            data = json.load(handle)
        return Checkpoint(
            input_csv=data["input_csv"],
            output_csv=data["output_csv"],
            last_row_index=data["last_row_index"],
            updated_at=data["updated_at"],
        )

    def save(self, checkpoint: Checkpoint) -> None:
        payload = {
            "input_csv": checkpoint.input_csv,
            "output_csv": checkpoint.output_csv,
            "last_row_index": checkpoint.last_row_index,
            "updated_at": checkpoint.updated_at,
        }
        checkpoint_path = self.directory / f"checkpoint_{checkpoint.last_row_index}.json"
        with checkpoint_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        with self.latest_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)


def _extract_http_target(row: Dict[str, str]) -> Optional[str]:
    return row.get("http_url") or row.get("url") or None


def _extract_ui_target(row: Dict[str, str]) -> Optional[str]:
    return row.get("ui_query") or row.get("query") or None


def _cache_key(row: Dict[str, str], cache_kind: str, target: Optional[str]) -> CacheKey:
    if target:
        return (URL_CACHE_SENTINEL, URL_CACHE_SENTINEL, f"{cache_kind}:{target}")
    part_number = row.get("part_number") or ""
    part_type = row.get("part_type") or ""
    return (part_number, part_type, cache_kind)


def _cache_lookup(cache: Dict[CacheKey, CacheValue], key: CacheKey) -> Optional[CacheValue]:
    return cache.get(key)


def _cache_store(cache: Dict[CacheKey, CacheValue], key: CacheKey, value: CacheValue) -> None:
    cache[key] = value


async def _bounded_fetch_http(
    row: Dict[str, str],
    semaphore: asyncio.Semaphore,
    cache: Dict[CacheKey, CacheValue],
) -> Dict[str, str]:
    target = _extract_http_target(row)
    cache_key = _cache_key(row, HTTP_CACHE_KIND, target)
    cached = _cache_lookup(cache, cache_key)
    if cached is not None:
        return cached

    async with semaphore:
        result = await fetch_http_data(target)
    payload = {
        "http_status": str(result.get("status", "")),
        "http_data": str(result.get("data", "")),
    }
    _cache_store(cache, cache_key, payload)
    return payload


async def _bounded_fetch_ui(
    row: Dict[str, str],
    semaphore: asyncio.Semaphore,
    cache: Dict[CacheKey, CacheValue],
) -> Dict[str, str]:
    target = _extract_ui_target(row)
    cache_key = _cache_key(row, UI_CACHE_KIND, target)
    cached = _cache_lookup(cache, cache_key)
    if cached is not None:
        return cached

    async with semaphore:
        result = await fetch_ui_data(target)
    payload = {
        "ui_status": str(result.get("status", "")),
        "ui_data": str(result.get("data", "")),
    }
    _cache_store(cache, cache_key, payload)
    return payload


async def _process_batch(
    rows: List[Dict[str, str]],
    *,
    max_concurrency: int,
) -> List[Dict[str, str]]:
    http_semaphore = asyncio.Semaphore(max_concurrency)
    ui_semaphore = asyncio.Semaphore(max_concurrency)
    cache: Dict[CacheKey, CacheValue] = {}

    http_tasks = [
        asyncio.create_task(_bounded_fetch_http(row, http_semaphore, cache)) for row in rows
    ]
    ui_tasks = [asyncio.create_task(_bounded_fetch_ui(row, ui_semaphore, cache)) for row in rows]

    http_results = await asyncio.gather(*http_tasks)
    ui_results = await asyncio.gather(*ui_tasks)

    combined: List[Dict[str, str]] = []
    for row, http_result, ui_result in zip(rows, http_results, ui_results):
        combined_row = {**row, **http_result, **ui_result}
        combined.append(combined_row)
    return combined


def run(
    *,
    input_csv: Path,
    output_csv: Path,
    batch_size: int,
    max_concurrency: int,
    checkpoint_dir: Path,
    resume: bool,
) -> None:
    checkpoint_manager = CheckpointManager(checkpoint_dir)
    skip_rows = 0
    if resume:
        checkpoint = checkpoint_manager.load()
        if checkpoint and checkpoint.input_csv == str(input_csv):
            skip_rows = checkpoint.last_row_index + 1

    batches = read_csv_in_batches(input_csv, batch_size=batch_size, skip_rows=skip_rows)
    output_fieldnames: Optional[List[str]] = None
    processed_index = skip_rows - 1

    for batch in batches:
        if output_fieldnames is None:
            output_fieldnames = ensure_output_schema(
                output_csv, list(batch[0].keys()), EXTRA_FIELDS
            )

        results = asyncio.run(_process_batch(batch, max_concurrency=max_concurrency))
        append_rows(output_csv, output_fieldnames, results)
        processed_index += len(batch)
        checkpoint_manager.save(
            Checkpoint(
                input_csv=str(input_csv),
                output_csv=str(output_csv),
                last_row_index=processed_index,
                updated_at=time.time(),
            )
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_csv", type=Path, help="Path to input CSV")
    parser.add_argument("output_csv", type=Path, help="Path to output CSV")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--max-concurrency", type=int, default=10)
    parser.add_argument("--checkpoint-dir", type=Path, default=Path("output/checkpoints"))
    parser.add_argument("--resume", action="store_true")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    run(
        input_csv=args.input_csv,
        output_csv=args.output_csv,
        batch_size=args.batch_size,
        max_concurrency=args.max_concurrency,
        checkpoint_dir=args.checkpoint_dir,
        resume=args.resume,
    )


if __name__ == "__main__":
    main()
