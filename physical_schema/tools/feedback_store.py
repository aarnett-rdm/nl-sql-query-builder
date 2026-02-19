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
import os
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

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

# How long a lock file is considered valid before being treated as stale (e.g. app crash)
LOCK_TIMEOUT_SECS = 120


class FeedbackLockedError(Exception):
    """Raised when another process holds the feedback file lock."""
    def __init__(self, age_secs: int):
        self.age_secs = age_secs
        remaining = max(0, LOCK_TIMEOUT_SECS - age_secs)
        super().__init__(
            f"Another user is currently submitting feedback. "
            f"Please wait about {remaining} seconds and try again."
        )


def get_feedback_path() -> Path:
    """Resolve the feedback JSONL path from NL_SQL_FEEDBACK_PATH env var or default."""
    env_path = os.environ.get("NL_SQL_FEEDBACK_PATH", "").strip()
    if env_path:
        return Path(env_path)
    return Path(__file__).resolve().parents[1] / "feedback" / "corrections.jsonl"


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

    @property
    def _lock_path(self) -> Path:
        return self._path.with_suffix(".lock")

    def is_locked(self) -> Tuple[bool, int]:
        """Check if another process holds the cross-machine file lock.

        Returns (is_locked, age_in_seconds).
        Locks older than LOCK_TIMEOUT_SECS are treated as stale and ignored.
        """
        if not self._lock_path.exists():
            return False, 0
        try:
            data = json.loads(self._lock_path.read_text(encoding="utf-8"))
            locked_at = datetime.fromisoformat(data["locked_at"])
            age = int((datetime.now() - locked_at).total_seconds())
            if age > LOCK_TIMEOUT_SECS:
                return False, age  # stale — treat as unlocked
            return True, age
        except Exception:
            return False, 0

    def _acquire_file_lock(self) -> None:
        self._lock_path.write_text(
            json.dumps({"locked_at": datetime.now().isoformat()}),
            encoding="utf-8",
        )

    def _release_file_lock(self) -> None:
        try:
            self._lock_path.unlink(missing_ok=True)
        except Exception:
            pass

    def append(self, record: CorrectionRecord) -> None:
        """Append a single record as one JSON line.

        Raises FeedbackLockedError if another process currently holds the lock.
        """
        locked, age = self.is_locked()
        if locked:
            raise FeedbackLockedError(age)

        line = json.dumps(record.to_dict(), default=str) + "\n"
        with self._lock:
            self._acquire_file_lock()
            try:
                with open(self._path, "a", encoding="utf-8") as f:
                    f.write(line)
            finally:
                self._release_file_lock()

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
