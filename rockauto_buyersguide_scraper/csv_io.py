"""CSV read/write helpers."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Sequence


def read_csv_in_batches(
    input_path: Path,
    *,
    batch_size: int,
    skip_rows: int = 0,
) -> Iterator[List[Dict[str, str]]]:
    with input_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        batch: List[Dict[str, str]] = []
        for index, row in enumerate(reader):
            if index < skip_rows:
                continue
            batch.append(row)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def ensure_output_schema(
    output_path: Path,
    input_fieldnames: Sequence[str],
    extra_fields: Sequence[str],
) -> List[str]:
    fieldnames = list(input_fieldnames) + list(extra_fields)
    if output_path.exists():
        with output_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            existing_header = next(reader, [])
        if existing_header:
            return existing_header
    return fieldnames


def append_rows(
    output_path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[Dict[str, str]],
) -> None:
    is_new_file = not output_path.exists()
    with output_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if is_new_file:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
