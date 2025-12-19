from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict


@dataclass
class Logger:
    stream: Any = sys.stdout
    default_fields: Dict[str, Any] = field(default_factory=dict)

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def log(self, level: str, message: str, **fields: Any) -> None:
        payload = {
            "timestamp": self._timestamp(),
            "level": level.upper(),
            "message": message,
            **self.default_fields,
            **fields,
        }
        json.dump(payload, self.stream, ensure_ascii=False)
        self.stream.write("\n")
        self.stream.flush()

    def info(self, message: str, **fields: Any) -> None:
        self.log("info", message, **fields)

    def warning(self, message: str, **fields: Any) -> None:
        self.log("warning", message, **fields)

    def error(self, message: str, **fields: Any) -> None:
        self.log("error", message, **fields)
