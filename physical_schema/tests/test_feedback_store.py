"""
Tests for feedback_store.py — JSONL storage for user corrections.

All offline, uses tmp_path fixture.
"""

import concurrent.futures
import json

import pytest

from tools.feedback_store import (
    DIMENSION_WRONG,
    METRIC_MISMATCH,
    PLATFORM_WRONG,
    VALID_TYPES,
    CorrectionRecord,
    FeedbackStore,
)


def _make_record(
    correction_type: str = METRIC_MISMATCH,
    feedback_id: str = "fb-1",
    question: str = "show spend yesterday",
    **overrides,
) -> CorrectionRecord:
    defaults = dict(
        feedback_id=feedback_id,
        timestamp="2026-02-11T10:00:00",
        request_id="req-1",
        original_question=question,
        original_spec={"metrics": ["spend"], "platform": None, "dimensions": [], "filters": {"date": {}, "where": []}, "grain": None},
        corrected_spec={"metrics": ["cost"], "platform": None, "dimensions": [], "filters": {"date": {}, "where": []}, "grain": None},
        correction_type=correction_type,
        notes="",
    )
    defaults.update(overrides)
    return CorrectionRecord(**defaults)


class TestCorrectionRecord:
    def test_to_dict_roundtrip(self):
        r = _make_record()
        d = r.to_dict()
        r2 = CorrectionRecord.from_dict(d)
        assert r2.feedback_id == r.feedback_id
        assert r2.original_question == r.original_question
        assert r2.correction_type == r.correction_type

    def test_to_dict_contains_all_fields(self):
        r = _make_record()
        d = r.to_dict()
        assert set(d.keys()) == {
            "feedback_id", "timestamp", "request_id",
            "original_question", "original_spec", "corrected_spec",
            "correction_type", "notes",
        }


class TestFeedbackStore:
    def test_append_and_load(self, tmp_path):
        store = FeedbackStore(tmp_path / "corrections.jsonl")
        r = _make_record()
        store.append(r)
        records = store.load_all()
        assert len(records) == 1
        assert records[0].feedback_id == "fb-1"
        assert records[0].original_question == "show spend yesterday"

    def test_append_multiple(self, tmp_path):
        store = FeedbackStore(tmp_path / "corrections.jsonl")
        for i in range(3):
            store.append(_make_record(feedback_id=f"fb-{i}"))
        assert len(store.load_all()) == 3

    def test_load_by_type(self, tmp_path):
        store = FeedbackStore(tmp_path / "corrections.jsonl")
        store.append(_make_record(correction_type=METRIC_MISMATCH, feedback_id="fb-1"))
        store.append(_make_record(correction_type=DIMENSION_WRONG, feedback_id="fb-2"))
        store.append(_make_record(correction_type=METRIC_MISMATCH, feedback_id="fb-3"))

        metric_records = store.load_by_type(METRIC_MISMATCH)
        assert len(metric_records) == 2
        assert all(r.correction_type == METRIC_MISMATCH for r in metric_records)

        dim_records = store.load_by_type(DIMENSION_WRONG)
        assert len(dim_records) == 1

    def test_empty_file(self, tmp_path):
        store = FeedbackStore(tmp_path / "nonexistent.jsonl")
        assert store.load_all() == []
        assert store.count() == 0

    def test_count(self, tmp_path):
        store = FeedbackStore(tmp_path / "corrections.jsonl")
        assert store.count() == 0
        store.append(_make_record(feedback_id="fb-1"))
        assert store.count() == 1
        store.append(_make_record(feedback_id="fb-2"))
        assert store.count() == 2

    def test_thread_safety(self, tmp_path):
        store = FeedbackStore(tmp_path / "corrections.jsonl")
        n = 20

        def write_one(i):
            store.append(_make_record(feedback_id=f"fb-{i}"))

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(write_one, range(n)))

        records = store.load_all()
        assert len(records) == n
        ids = {r.feedback_id for r in records}
        assert len(ids) == n  # no duplicates or corruption

    def test_creates_parent_dirs(self, tmp_path):
        store = FeedbackStore(tmp_path / "nested" / "deep" / "corrections.jsonl")
        store.append(_make_record())
        assert store.count() == 1

    def test_jsonl_format(self, tmp_path):
        """Each line is valid JSON."""
        store = FeedbackStore(tmp_path / "corrections.jsonl")
        store.append(_make_record(feedback_id="fb-1"))
        store.append(_make_record(feedback_id="fb-2"))

        lines = store.path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            data = json.loads(line)
            assert "feedback_id" in data


class TestValidTypes:
    def test_all_types_present(self):
        assert "metric_mismatch" in VALID_TYPES
        assert "dimension_wrong" in VALID_TYPES
        assert "platform_wrong" in VALID_TYPES
        assert "date_filter_wrong" in VALID_TYPES
        assert "filter_wrong" in VALID_TYPES
        assert "other" in VALID_TYPES
        assert len(VALID_TYPES) == 6
