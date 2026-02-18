"""
query_history_store.py

Append-only JSONL storage for query history.

Each record captures the NL question, generated spec and SQL, and
execution metadata, enabling re-run, search, and browsing of past queries.
"""

from __future__ import annotations

import dataclasses
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# QueryRecord
# ---------------------------------------------------------------------------


@dataclass
class QueryRecord:
    """A single successfully-generated query."""

    history_id: str
    timestamp: str          # ISO format
    request_id: str         # Correlation ID from /query
    user_question: str
    spec: Dict[str, Any]
    sql: str
    platform: str           # spec["platform"]
    metrics: List[str]      # spec["metrics"]
    dimensions: List[str]   # spec["dimensions"]
    grain: str              # spec["grain"]
    row_count: Optional[int] = None
    parser_used: Optional[str] = None  # spec.get("notes", {}).get("parser")

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "QueryRecord":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# QueryHistoryStore
# ---------------------------------------------------------------------------


class QueryHistoryStore:
    """Thread-safe, append-only JSONL store for query history records."""

    def __init__(self, path: Path):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def append(self, record: QueryRecord) -> None:
        """Append a single record as one JSON line."""
        line = json.dumps(record.to_dict(), default=str) + "\n"
        with self._lock:
            with open(self._path, "a", encoding="utf-8") as f:
                f.write(line)

    def load_all(self) -> List[QueryRecord]:
        """Read every record from the JSONL file, newest-last."""
        if not self._path.exists():
            return []
        records: List[QueryRecord] = []
        with open(self._path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(QueryRecord.from_dict(json.loads(line)))
                except Exception:
                    continue  # Skip malformed lines
        return records

    def load_recent(self, days: int = 30) -> List[QueryRecord]:
        """Return records from the last N days."""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        return [r for r in self.load_all() if r.timestamp >= cutoff]

    def delete(self, history_id: str) -> bool:
        """Remove a record by history_id. Rewrites the file. Returns True if found."""
        with self._lock:
            records = self.load_all()
            new_records = [r for r in records if r.history_id != history_id]
            if len(new_records) == len(records):
                return False
            with open(self._path, "w", encoding="utf-8") as f:
                for r in new_records:
                    f.write(json.dumps(r.to_dict(), default=str) + "\n")
        return True

    def load_by_platform(self, platform: str) -> List[QueryRecord]:
        return [r for r in self.load_all() if r.platform == platform]

    def count(self) -> int:
        """Total number of stored records."""
        if not self._path.exists():
            return 0
        n = 0
        with open(self._path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    n += 1
        return n
