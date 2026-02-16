"""
feedback_store.py

Append-only JSONL storage for user corrections.

Each correction captures what the parser produced (original_spec)
vs what the user says it should have been (corrected_spec),
enabling pattern analysis and system improvement.
"""

from __future__ import annotations

import dataclasses
import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# Correction types (string constants, not enum — stays flexible)
# ---------------------------------------------------------------------------

METRIC_MISMATCH = "metric_mismatch"
DIMENSION_WRONG = "dimension_wrong"
PLATFORM_WRONG = "platform_wrong"
DATE_FILTER_WRONG = "date_filter_wrong"
FILTER_WRONG = "filter_wrong"
OTHER = "other"

VALID_TYPES = {
    METRIC_MISMATCH,
    DIMENSION_WRONG,
    PLATFORM_WRONG,
    DATE_FILTER_WRONG,
    FILTER_WRONG,
    OTHER,
}


# ---------------------------------------------------------------------------
# CorrectionRecord
# ---------------------------------------------------------------------------


@dataclass
class CorrectionRecord:
    """A single user correction."""

    feedback_id: str
    timestamp: str
    request_id: str
    original_question: str
    original_spec: Dict[str, Any]
    corrected_spec: Dict[str, Any]
    correction_type: str
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CorrectionRecord":
        return cls(**d)


# ---------------------------------------------------------------------------
# FeedbackStore
# ---------------------------------------------------------------------------


class FeedbackStore:
    """Thread-safe, append-only JSONL store for correction records."""

    def __init__(self, path: Path):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def append(self, record: CorrectionRecord) -> None:
        """Append a single record as one JSON line."""
        line = json.dumps(record.to_dict(), default=str) + "\n"
        with self._lock:
            with open(self._path, "a", encoding="utf-8") as f:
                f.write(line)

    def load_all(self) -> List[CorrectionRecord]:
        """Read every record from the JSONL file."""
        if not self._path.exists():
            return []
        records: List[CorrectionRecord] = []
        with open(self._path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                records.append(CorrectionRecord.from_dict(json.loads(line)))
        return records

    def load_by_type(self, correction_type: str) -> List[CorrectionRecord]:
        """Return only records matching the given correction type."""
        return [r for r in self.load_all() if r.correction_type == correction_type]

    def count(self) -> int:
        """Total number of stored corrections."""
        if not self._path.exists():
            return 0
        n = 0
        with open(self._path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    n += 1
        return n
