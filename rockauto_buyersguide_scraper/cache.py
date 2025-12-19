from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class CacheEntry:
    value: str
    fetched_at: float


class CacheStore:
    def __init__(self, cache_dir: Path, ttl_seconds: int) -> None:
        self.cache_dir = cache_dir
        self.ttl_seconds = ttl_seconds
        self.db_path = self.cache_dir / "cache.sqlite3"
        self._ensure_db()

    def _ensure_db(self) -> None:
        os.makedirs(self.cache_dir, exist_ok=True)
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_entries (
                    part_number TEXT NOT NULL,
                    part_type TEXT NOT NULL,
                    cache_kind TEXT NOT NULL,
                    value TEXT NOT NULL,
                    fetched_at REAL NOT NULL,
                    PRIMARY KEY (part_number, part_type, cache_kind)
                )
                """
            )

    def get(self, part_number: str, part_type: str, cache_kind: str) -> Optional[CacheEntry]:
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT value, fetched_at
                FROM cache_entries
                WHERE part_number = ? AND part_type = ? AND cache_kind = ?
                """,
                (part_number, part_type, cache_kind),
            ).fetchone()
        if row is None:
            return None
        value, fetched_at = row
        if self._is_expired(fetched_at):
            self.delete(part_number, part_type, cache_kind)
            return None
        return CacheEntry(value=value, fetched_at=fetched_at)

    def set(self, part_number: str, part_type: str, cache_kind: str, value: str) -> None:
        fetched_at = time.time()
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO cache_entries (part_number, part_type, cache_kind, value, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(part_number, part_type, cache_kind)
                DO UPDATE SET value = excluded.value, fetched_at = excluded.fetched_at
                """,
                (part_number, part_type, cache_kind, value, fetched_at),
            )

    def delete(self, part_number: str, part_type: str, cache_kind: str) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                DELETE FROM cache_entries
                WHERE part_number = ? AND part_type = ? AND cache_kind = ?
                """,
                (part_number, part_type, cache_kind),
            )

    def clear(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("DELETE FROM cache_entries")

    def prune_expired(self) -> int:
        cutoff = time.time() - self.ttl_seconds
        with sqlite3.connect(self.db_path) as connection:
            cursor = connection.execute(
                """
                DELETE FROM cache_entries
                WHERE fetched_at < ?
                """,
                (cutoff,),
            )
            return cursor.rowcount

    def _is_expired(self, fetched_at: float) -> bool:
        return time.time() - fetched_at > self.ttl_seconds


def serialize_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def deserialize_json(payload: str) -> Any:
    return json.loads(payload)
