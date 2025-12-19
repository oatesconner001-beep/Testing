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
    "ui_status",
    "ui_data",
]

DEFAULT_CACHE_TTL_SECONDS = 60 * 60 * 24


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


async def _bounded_fetch_http(row: Dict[str, str], semaphore: asyncio.Semaphore) -> Dict[str, str]:
    async with semaphore:
        result = await fetch_http_data(_extract_http_target(row))
    return {
        "http_status": str(result.get("status", "")),
        "http_data": str(result.get("data", "")),
    }


async def _bounded_fetch_ui(row: Dict[str, str], semaphore: asyncio.Semaphore) -> Dict[str, str]:
    async with semaphore:
        result = await fetch_ui_data(_extract_ui_target(row))
    return {
        "ui_status": str(result.get("status", "")),
        "ui_data": str(result.get("data", "")),
    }


async def _process_batch(
    rows: List[Dict[str, str]],
    *,
    max_concurrency: int,
) -> List[Dict[str, str]]:
    http_semaphore = asyncio.Semaphore(max_concurrency)
    ui_semaphore = asyncio.Semaphore(max_concurrency)

    http_tasks = [asyncio.create_task(_bounded_fetch_http(row, http_semaphore)) for row in rows]
    ui_tasks = [asyncio.create_task(_bounded_fetch_ui(row, ui_semaphore)) for row in rows]

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
    )


if __name__ == "__main__":
    main()
