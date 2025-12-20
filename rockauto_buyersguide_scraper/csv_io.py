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
            merged_header = list(existing_header)
            merged_header.extend(
                name for name in fieldnames if name not in existing_header
            )
            return merged_header
    return fieldnames


def append_rows(
    output_path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[Dict[str, str]],
) -> None:
    resolved_fieldnames = list(fieldnames)
    is_new_file = not output_path.exists()
    if not is_new_file:
        with output_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            existing_header = reader.fieldnames or []
            if existing_header != resolved_fieldnames:
                existing_rows = list(reader)
                temp_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
                with temp_path.open("w", newline="", encoding="utf-8") as temp_handle:
                    writer = csv.DictWriter(temp_handle, fieldnames=resolved_fieldnames)
                    writer.writeheader()
                    for row in existing_rows:
                        writer.writerow(row)
                temp_path.replace(output_path)
    with output_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=resolved_fieldnames)
        if is_new_file:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
