"""
Tests for feedback_analyzer.py — pattern analysis and markdown generation.

All offline, no LLM needed.
"""

import pytest

from tools.feedback_store import (
    CorrectionRecord,
    DATE_FILTER_WRONG,
    DIMENSION_WRONG,
    METRIC_MISMATCH,
    OTHER,
    PLATFORM_WRONG,
)
from tools.feedback_analyzer import (
    find_date_filter_gaps,
    find_dimension_patterns,
    find_few_shot_candidates,
    find_metric_gaps,
    find_platform_gaps,
    generate_recommendations,
)


def _make_record(
    correction_type: str = METRIC_MISMATCH,
    feedback_id: str = "fb-1",
    question: str = "show spend yesterday",
    original_spec: dict = None,
    corrected_spec: dict = None,
    **overrides,
) -> CorrectionRecord:
    if original_spec is None:
        original_spec = {
            "metrics": ["spend"], "platform": None, "dimensions": [],
            "filters": {"date": {"yesterday": True}, "where": []}, "grain": None,
        }
    if corrected_spec is None:
        corrected_spec = {
            "metrics": ["cost"], "platform": None, "dimensions": [],
            "filters": {"date": {"yesterday": True}, "where": []}, "grain": None,
        }
    defaults = dict(
        feedback_id=feedback_id,
        timestamp="2026-02-11T10:00:00",
        request_id="req-1",
        original_question=question,
        original_spec=original_spec,
        corrected_spec=corrected_spec,
        correction_type=correction_type,
        notes="",
    )
    defaults.update(overrides)
    return CorrectionRecord(**defaults)


# ---------------------------------------------------------------------------
# find_metric_gaps
# ---------------------------------------------------------------------------

class TestFindMetricGaps:
    def test_detects_synonym_gap(self):
        records = [_make_record(correction_type=METRIC_MISMATCH)]
        gaps = find_metric_gaps(records)
        assert len(gaps) == 1
        assert gaps[0]["original"] == "spend"
        assert gaps[0]["corrected"] == "cost"
        assert gaps[0]["count"] == 1
        assert gaps[0]["action"] == "add synonym"

    def test_groups_by_pair(self):
        records = [
            _make_record(correction_type=METRIC_MISMATCH, feedback_id="fb-1"),
            _make_record(correction_type=METRIC_MISMATCH, feedback_id="fb-2"),
            _make_record(correction_type=METRIC_MISMATCH, feedback_id="fb-3"),
        ]
        gaps = find_metric_gaps(records)
        assert len(gaps) == 1
        assert gaps[0]["count"] == 3

    def test_ignores_non_metric_types(self):
        records = [
            _make_record(correction_type=DIMENSION_WRONG),
            _make_record(correction_type=PLATFORM_WRONG),
        ]
        gaps = find_metric_gaps(records)
        assert gaps == []

    def test_multiple_pairs(self):
        r1 = _make_record(
            correction_type=METRIC_MISMATCH,
            feedback_id="fb-1",
            original_spec={"metrics": ["spend"], "platform": None, "dimensions": [], "filters": {"date": {}, "where": []}, "grain": None},
            corrected_spec={"metrics": ["cost"], "platform": None, "dimensions": [], "filters": {"date": {}, "where": []}, "grain": None},
        )
        r2 = _make_record(
            correction_type=METRIC_MISMATCH,
            feedback_id="fb-2",
            original_spec={"metrics": ["orders"], "platform": None, "dimensions": [], "filters": {"date": {}, "where": []}, "grain": None},
            corrected_spec={"metrics": ["exchange orders"], "platform": None, "dimensions": [], "filters": {"date": {}, "where": []}, "grain": None},
        )
        gaps = find_metric_gaps([r1, r2])
        assert len(gaps) == 2
        originals = {g["original"] for g in gaps}
        assert "spend" in originals
        assert "orders" in originals


# ---------------------------------------------------------------------------
# find_dimension_patterns
# ---------------------------------------------------------------------------

class TestFindDimensionPatterns:
    def test_detects_table_preference(self):
        records = [
            _make_record(
                correction_type=DIMENSION_WRONG,
                feedback_id="fb-1",
                original_spec={"metrics": ["clicks"], "platform": None, "dimensions": ["AccountName"], "filters": {"date": {}, "where": []}, "grain": None},
                corrected_spec={"metrics": ["clicks"], "platform": None, "dimensions": ["Campaign.AccountName"], "filters": {"date": {}, "where": []}, "grain": None},
            ),
            _make_record(
                correction_type=DIMENSION_WRONG,
                feedback_id="fb-2",
                original_spec={"metrics": ["cost"], "platform": None, "dimensions": ["AccountName"], "filters": {"date": {}, "where": []}, "grain": None},
                corrected_spec={"metrics": ["cost"], "platform": None, "dimensions": ["Campaign.AccountName"], "filters": {"date": {}, "where": []}, "grain": None},
            ),
        ]
        patterns = find_dimension_patterns(records)
        assert len(patterns) == 1
        assert patterns[0]["column"] == "AccountName"
        assert patterns[0]["preferred_table"] == "Campaign"
        assert patterns[0]["count"] == 2

    def test_ignores_non_dimension_types(self):
        records = [_make_record(correction_type=METRIC_MISMATCH)]
        assert find_dimension_patterns(records) == []


# ---------------------------------------------------------------------------
# find_date_filter_gaps
# ---------------------------------------------------------------------------

class TestFindDateFilterGaps:
    def test_detects_misclassification(self):
        records = [
            _make_record(
                correction_type=DATE_FILTER_WRONG,
                feedback_id="fb-1",
                question="Show clicks this month",
                original_spec={"metrics": ["clicks"], "platform": None, "dimensions": [], "filters": {"date": {"last_n_days": 30}, "where": []}, "grain": None},
                corrected_spec={"metrics": ["clicks"], "platform": None, "dimensions": [], "filters": {"date": {"date_from": "2026-02-01", "date_to": "2026-02-11"}, "where": []}, "grain": None},
            ),
        ]
        gaps = find_date_filter_gaps(records)
        assert len(gaps) == 1
        assert gaps[0]["parsed_as"] == "last_n_days"
        assert gaps[0]["should_be"] == "date_range"
        assert gaps[0]["count"] == 1

    def test_ignores_non_date_types(self):
        records = [_make_record(correction_type=METRIC_MISMATCH)]
        assert find_date_filter_gaps(records) == []


# ---------------------------------------------------------------------------
# find_platform_gaps
# ---------------------------------------------------------------------------

class TestFindPlatformGaps:
    def test_detects_missing_alias(self):
        records = [
            _make_record(
                correction_type=PLATFORM_WRONG,
                feedback_id="fb-1",
                question="Show bing ads clicks",
                original_spec={"metrics": ["clicks"], "platform": None, "dimensions": [], "filters": {"date": {}, "where": []}, "grain": None},
                corrected_spec={"metrics": ["clicks"], "platform": "microsoft_ads", "dimensions": [], "filters": {"date": {}, "where": []}, "grain": None},
            ),
        ]
        gaps = find_platform_gaps(records)
        assert len(gaps) == 1
        assert gaps[0]["parsed_as"] is None
        assert gaps[0]["should_be"] == "microsoft_ads"
        assert gaps[0]["count"] == 1

    def test_ignores_non_platform_types(self):
        records = [_make_record(correction_type=METRIC_MISMATCH)]
        assert find_platform_gaps(records) == []


# ---------------------------------------------------------------------------
# find_few_shot_candidates
# ---------------------------------------------------------------------------

class TestFindFewShotCandidates:
    def test_low_score_triggers_candidate(self):
        """A correction where everything is different should be flagged."""
        records = [
            _make_record(
                correction_type=OTHER,
                feedback_id="fb-1",
                original_spec={
                    "metrics": [], "platform": None, "dimensions": [],
                    "filters": {"date": {}, "where": []}, "grain": None,
                },
                corrected_spec={
                    "metrics": ["clicks", "cost"], "platform": "google_ads",
                    "dimensions": ["CampaignName"],
                    "filters": {"date": {"last_n_days": 7}, "where": []}, "grain": None,
                },
            ),
        ]
        candidates = find_few_shot_candidates(records)
        assert len(candidates) == 1
        assert candidates[0]["original_score"] < 0.5

    def test_high_score_not_flagged(self):
        """A correction where only notes differ should not be flagged."""
        spec = {
            "metrics": ["clicks"], "platform": "google_ads",
            "dimensions": ["CampaignName"],
            "filters": {"date": {"last_n_days": 7}, "where": []}, "grain": None,
        }
        records = [
            _make_record(
                correction_type=OTHER,
                feedback_id="fb-1",
                original_spec=spec,
                corrected_spec=spec,
            ),
        ]
        candidates = find_few_shot_candidates(records)
        assert candidates == []


# ---------------------------------------------------------------------------
# generate_recommendations
# ---------------------------------------------------------------------------

class TestGenerateRecommendations:
    def test_empty_records(self):
        md = generate_recommendations([])
        assert "No corrections recorded" in md

    def test_produces_markdown(self):
        records = [
            _make_record(correction_type=METRIC_MISMATCH, feedback_id="fb-1"),
            _make_record(correction_type=METRIC_MISMATCH, feedback_id="fb-2"),
        ]
        md = generate_recommendations(records)
        assert md.startswith("# Feedback Recommendations")
        assert "## Summary" in md
        assert "## Metric Synonym Gaps" in md
        assert "`spend`" in md
        assert "`cost`" in md

    def test_respects_min_count(self):
        records = [
            _make_record(correction_type=METRIC_MISMATCH, feedback_id="fb-1"),
        ]
        # With min_count=1, should include the gap
        md1 = generate_recommendations(records, min_count=1)
        assert "## Metric Synonym Gaps" in md1

        # With min_count=5, should NOT include the gap (only 1 occurrence)
        md5 = generate_recommendations(records, min_count=5)
        assert "## Metric Synonym Gaps" not in md5

    def test_summary_counts(self):
        records = [
            _make_record(correction_type=METRIC_MISMATCH, feedback_id="fb-1"),
            _make_record(correction_type=PLATFORM_WRONG, feedback_id="fb-2",
                         original_spec={"metrics": ["clicks"], "platform": None, "dimensions": [], "filters": {"date": {}, "where": []}, "grain": None},
                         corrected_spec={"metrics": ["clicks"], "platform": "google_ads", "dimensions": [], "filters": {"date": {}, "where": []}, "grain": None}),
        ]
        md = generate_recommendations(records)
        assert "**Total corrections:** 2" in md
        assert "`metric_mismatch`: 1" in md
        assert "`platform_wrong`: 1" in md
