"""
favorites_store.py

JSON-backed store for bookmarked queries with upvote counts.

Uses a single JSON file (list of FavoriteRecord dicts) rather than JSONL
because upvoting and editing require in-place mutation of existing records.
Thread-safe via a file-level lock around all read-modify-write operations.
"""

from __future__ import annotations

import dataclasses
import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# FavoriteRecord
# ---------------------------------------------------------------------------


@dataclass
class FavoriteRecord:
    """A bookmarked query with metadata and vote count."""

    favorite_id: str
    history_id: str         # Links to QueryRecord (may be "" for direct saves)
    created_at: str         # ISO timestamp
    user_question: str
    spec: Dict[str, Any]
    sql: str
    platform: str
    metrics: List[str]
    dimensions: List[str]
    grain: str
    name: str = ""
    description: str = ""
    tags: List[str] = field(default_factory=list)
    votes: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "FavoriteRecord":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# FavoritesStore
# ---------------------------------------------------------------------------


class FavoritesStore:
    """Thread-safe JSON store for favorite/bookmarked queries."""

    def __init__(self, path: Path):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read(self) -> List[Dict[str, Any]]:
        """Load raw list from JSON file; returns [] if missing/empty."""
        if not self._path.exists():
            return []
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
        except (json.JSONDecodeError, IOError):
            return []

    def _write(self, data: List[Dict[str, Any]]) -> None:
        """Atomically overwrite the JSON file."""
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def append(self, record: FavoriteRecord) -> None:
        """Add a new favorite."""
        with self._lock:
            data = self._read()
            data.append(record.to_dict())
            self._write(data)

    def load_all(self) -> List[FavoriteRecord]:
        """Return all favorites, newest-first by created_at."""
        with self._lock:
            data = self._read()
        records = []
        for d in data:
            try:
                records.append(FavoriteRecord.from_dict(d))
            except Exception:
                continue
        return records

    def upvote(self, favorite_id: str) -> bool:
        """Increment vote count for a favorite. Returns True if found."""
        with self._lock:
            data = self._read()
            for item in data:
                if item.get("favorite_id") == favorite_id:
                    item["votes"] = item.get("votes", 0) + 1
                    self._write(data)
                    return True
        return False

    def update(
        self,
        favorite_id: str,
        name: str,
        description: str,
        tags: List[str],
    ) -> bool:
        """Update editable metadata for a favorite. Returns True if found."""
        with self._lock:
            data = self._read()
            for item in data:
                if item.get("favorite_id") == favorite_id:
                    item["name"] = name
                    item["description"] = description
                    item["tags"] = tags
                    self._write(data)
                    return True
        return False

    def delete(self, favorite_id: str) -> bool:
        """Remove a favorite by ID. Returns True if found and removed."""
        with self._lock:
            data = self._read()
            new_data = [d for d in data if d.get("favorite_id") != favorite_id]
            if len(new_data) == len(data):
                return False
            self._write(new_data)
        return True

    def contains(self, history_id: str) -> bool:
        """Check whether a history entry is already bookmarked."""
        return any(r.history_id == history_id for r in self.load_all())

    def count(self) -> int:
        return len(self.load_all())
