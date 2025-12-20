"""Task orchestration for RockAuto buyers guide scraping."""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .csv_io import append_rows, ensure_output_schema, read_csv_in_batches
from .http_client import fetch_http_data
from .ui_automation import fetch_ui_data
from src.cache import CacheStore


EXTRA_FIELDS = [
    "http_status",
    "http_data",
    "http_cache_hit",
    "ui_status",
    "ui_data",
    "ui_cache_hit",
]

DEFAULT_CACHE_TTL_SECONDS = 60 * 60 * 24
URL_CACHE_SENTINEL = "__url__"


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


def _extract_part_number(row: Dict[str, str]) -> str:
    for key in ("part_number", "input_part_number", "skp_number", "interchange_number"):
        value = row.get(key)
        if value:
            return value
    return ""


def _extract_part_type(row: Dict[str, str]) -> str:
    return row.get("part_type", "")


def _cache_lookup(
    cache: Optional[CacheStore],
    *,
    part_number: str,
    part_type: str,
    cache_kind: str,
) -> Optional[str]:
    if cache is None:
        return None
    if cache_kind.startswith(("http:", "ui:")):
        part_number = URL_CACHE_SENTINEL
        part_type = URL_CACHE_SENTINEL
    cached = cache.get(part_number or cache_kind, part_type or "unknown", cache_kind)
    if cached is None:
        return None
    return cached.value


def _cache_store(
    cache: Optional[CacheStore],
    *,
    part_number: str,
    part_type: str,
    cache_kind: str,
    value: str,
) -> None:
    if cache is None:
        return
    if cache_kind.startswith(("http:", "ui:")):
        part_number = URL_CACHE_SENTINEL
        part_type = URL_CACHE_SENTINEL
    cache.set(part_number or cache_kind, part_type or "unknown", cache_kind, value)


async def _bounded_fetch_http(
    row: Dict[str, str],
    semaphore: asyncio.Semaphore,
    cache: Optional[CacheStore],
) -> Dict[str, str]:
    target = _extract_http_target(row)
    part_number = _extract_part_number(row)
    part_type = _extract_part_type(row)

    cached_value = _cache_lookup(
        cache,
        part_number=part_number,
        part_type=part_type,
        cache_kind=f"http:{target}" if target else "http:missing",
    )
    if cached_value is not None:
        return {
            "http_status": "cached",
            "http_data": cached_value,
            "http_cache_hit": "true",
        }

    async with semaphore:
        result = await fetch_http_data(target)
    _cache_store(
        cache,
        part_number=part_number,
        part_type=part_type,
        cache_kind=f"http:{target}" if target else "http:missing",
        value=str(result.get("data", "")),
    )
    return {
        "http_status": str(result.get("status", "")),
        "http_data": str(result.get("data", "")),
        "http_cache_hit": "false",
    }


async def _bounded_fetch_ui(
    row: Dict[str, str],
    semaphore: asyncio.Semaphore,
    cache: Optional[CacheStore],
) -> Dict[str, str]:
    target = _extract_ui_target(row)
    part_number = _extract_part_number(row)
    part_type = _extract_part_type(row)

    cached_value = _cache_lookup(
        cache,
        part_number=part_number,
        part_type=part_type,
        cache_kind=f"ui:{target}" if target else "ui:missing",
    )
    if cached_value is not None:
        return {
            "ui_status": "cached",
            "ui_data": cached_value,
            "ui_cache_hit": "true",
        }

    async with semaphore:
        result = await fetch_ui_data(target)
    _cache_store(
        cache,
        part_number=part_number,
        part_type=part_type,
        cache_kind=f"ui:{target}" if target else "ui:missing",
        value=str(result.get("data", "")),
    )
    return {
        "ui_status": str(result.get("status", "")),
        "ui_data": str(result.get("data", "")),
        "ui_cache_hit": "false",
    }


async def _process_batch(
    rows: List[Dict[str, str]],
    *,
    max_concurrency: int,
    cache: Optional[CacheStore],
) -> List[Dict[str, str]]:
    http_semaphore = asyncio.Semaphore(max_concurrency)
    ui_semaphore = asyncio.Semaphore(max_concurrency)

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
    cache_dir: Optional[Path],
    cache_ttl: int,
    cache_clear: bool,
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
    cache = CacheStore(cache_dir, ttl_seconds=cache_ttl) if cache_dir else None
    if cache and cache_clear:
        cache.clear()

    for batch in batches:
        if output_fieldnames is None:
            output_fieldnames = ensure_output_schema(
                output_csv, list(batch[0].keys()), EXTRA_FIELDS
            )

        results = asyncio.run(
            _process_batch(batch, max_concurrency=max_concurrency, cache=cache)
        )
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
    parser.add_argument(
        "--cache-dir",
        default=".cache",
        help="Directory used to store cache files.",
    )
    parser.add_argument(
        "--cache-ttl",
        type=int,
        default=DEFAULT_CACHE_TTL_SECONDS,
        help="Cache TTL in seconds before entries are refreshed.",
    )
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--cache-dir", type=Path, default=Path("output/cache"))
    parser.add_argument("--cache-ttl", type=int, default=60 * 60 * 24)
    parser.add_argument("--cache-clear", action="store_true")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    cache = CacheStore(Path(args.cache_dir), ttl_seconds=args.cache_ttl)
    cache.prune_expired()
    run(
        input_csv=args.input_csv,
        output_csv=args.output_csv,
        batch_size=args.batch_size,
        max_concurrency=args.max_concurrency,
        checkpoint_dir=args.checkpoint_dir,
        resume=args.resume,
        cache_dir=args.cache_dir,
        cache_ttl=args.cache_ttl,
        cache_clear=args.cache_clear,
    )


if __name__ == "__main__":
    main()
