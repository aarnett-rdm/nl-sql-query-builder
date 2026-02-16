"""
Tests for eval_harness.py scoring functions.

All offline — no LLM needed.
"""

import pytest

from tools.eval_harness import (
    _classify_date_filter,
    _score_set,
    _score_exact,
    score_entry,
)


# ---------------------------------------------------------------------------
# _classify_date_filter
# ---------------------------------------------------------------------------

class TestClassifyDateFilter:
    def test_empty_dict(self):
        assert _classify_date_filter({}) == "none"

    def test_none_input(self):
        assert _classify_date_filter(None) == "none"

    def test_yesterday(self):
        assert _classify_date_filter({"yesterday": True}) == "yesterday"

    def test_last_n_days(self):
        assert _classify_date_filter({"last_n_days": 7}) == "last_n_days"

    def test_mtd(self):
        assert _classify_date_filter({"mtd": True}) == "mtd"

    def test_date_range_both(self):
        assert _classify_date_filter({"date_from": "2025-01-01", "date_to": "2025-03-31"}) == "date_range"

    def test_date_from_only(self):
        assert _classify_date_filter({"date_from": "2025-01-01"}) == "date_range"


# ---------------------------------------------------------------------------
# _score_set
# ---------------------------------------------------------------------------

class TestScoreSet:
    def test_perfect_match(self):
        r = _score_set(["clicks", "cost"], ["clicks", "cost"])
        assert r["precision"] == 1.0
        assert r["recall"] == 1.0
        assert r["f1"] == 1.0

    def test_superset_actual(self):
        r = _score_set(["clicks"], ["clicks", "cost"])
        assert r["recall"] == 1.0
        assert r["precision"] == 0.5

    def test_subset_actual(self):
        r = _score_set(["clicks", "cost"], ["clicks"])
        assert r["precision"] == 1.0
        assert r["recall"] == 0.5

    def test_no_overlap(self):
        r = _score_set(["clicks"], ["impressions"])
        assert r["f1"] == 0.0

    def test_both_empty(self):
        r = _score_set([], [])
        assert r["f1"] == 1.0

    def test_expected_empty_actual_not(self):
        r = _score_set([], ["clicks"])
        assert r["recall"] == 1.0
        assert r["precision"] == 0.0

    def test_case_insensitive(self):
        r = _score_set(["Clicks"], ["clicks"])
        assert r["f1"] == 1.0


# ---------------------------------------------------------------------------
# _score_exact
# ---------------------------------------------------------------------------

class TestScoreExact:
    def test_match(self):
        assert _score_exact("google_ads", "google_ads") == 1.0

    def test_mismatch(self):
        assert _score_exact("google_ads", "microsoft_ads") == 0.0

    def test_none_expected_skips(self):
        assert _score_exact(None, "anything") == 1.0

    def test_both_none(self):
        assert _score_exact(None, None) == 1.0

    def test_expected_set_actual_none(self):
        assert _score_exact("google_ads", None) == 0.0


# ---------------------------------------------------------------------------
# score_entry
# ---------------------------------------------------------------------------

class TestScoreEntry:
    def test_perfect_entry(self):
        expected = {
            "metrics": ["clicks"],
            "platform": "google_ads",
            "dimensions": ["CampaignName"],
            "grain": None,
            "date_filter_type": "last_n_days",
        }
        actual_spec = {
            "metrics": ["clicks"],
            "platform": "google_ads",
            "dimensions": ["CampaignName"],
            "grain": "campaign_calendar",
            "filters": {"date": {"last_n_days": 7}, "where": []},
        }
        scores = score_entry(expected, actual_spec)
        assert scores["overall"] == 1.0
        assert scores["metrics"]["f1"] == 1.0
        assert scores["platform"] == 1.0

    def test_wrong_platform(self):
        expected = {
            "metrics": ["clicks"],
            "platform": "google_ads",
            "dimensions": [],
            "grain": None,
            "date_filter_type": None,
        }
        actual_spec = {
            "metrics": ["clicks"],
            "platform": "microsoft_ads",
            "dimensions": [],
            "filters": {"date": {}, "where": []},
        }
        scores = score_entry(expected, actual_spec)
        assert scores["platform"] == 0.0
        assert scores["overall"] < 1.0
        # metrics + dimensions + grain + date all correct = 0.40 + 0.15 + 0.10 + 0.15 = 0.80
        assert scores["overall"] == pytest.approx(0.80, abs=0.01)

    def test_missing_metric(self):
        expected = {
            "metrics": ["clicks", "cost"],
            "platform": None,
            "dimensions": [],
            "grain": None,
            "date_filter_type": None,
        }
        actual_spec = {
            "metrics": ["clicks"],
            "platform": None,
            "dimensions": [],
            "filters": {"date": {}, "where": []},
        }
        scores = score_entry(expected, actual_spec)
        assert scores["metrics"]["recall"] == 0.5
        assert scores["metrics"]["precision"] == 1.0
        assert scores["overall"] < 1.0

    def test_wrong_date_filter_type(self):
        expected = {
            "metrics": ["cost"],
            "platform": None,
            "dimensions": [],
            "grain": None,
            "date_filter_type": "yesterday",
        }
        actual_spec = {
            "metrics": ["cost"],
            "platform": None,
            "dimensions": [],
            "filters": {"date": {"last_n_days": 7}, "where": []},
        }
        scores = score_entry(expected, actual_spec)
        assert scores["date_filter"] == 0.0
        # metrics(1.0*0.40) + platform(1.0*0.20) + dims(1.0*0.15) + date(0.0*0.15) + grain(1.0*0.10) = 0.85
        assert scores["overall"] == pytest.approx(0.85, abs=0.01)

    def test_empty_spec(self):
        expected = {
            "metrics": ["clicks"],
            "platform": "google_ads",
            "dimensions": ["CampaignName"],
            "grain": None,
            "date_filter_type": "last_n_days",
        }
        actual_spec = {
            "metrics": [],
            "platform": None,
            "dimensions": [],
            "filters": {"date": {}, "where": []},
        }
        scores = score_entry(expected, actual_spec)
        assert scores["metrics"]["f1"] == 0.0
        assert scores["platform"] == 0.0
        assert scores["dimensions"]["f1"] == 0.0
        assert scores["overall"] == pytest.approx(0.10, abs=0.01)  # only grain (None skip) scores
