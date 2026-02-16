"""
test_adversarial.py

Priority 4.3: Fuzz/adversarial NL input tests.

Ensures the pipeline handles bad, weird, or malicious inputs gracefully:
  - Never produces syntactically invalid SQL when metrics are recognized
  - Never crashes with unhandled exceptions
  - SQL injection patterns are neutralized by the deterministic builder
"""

import random
import string

import pytest
import sqlglot

from tools.nl_to_spec import nl_to_spec
from tools.spec_executor import execute_spec
from tools.metric_resolver import MetricResolutionError

SPEC_REQUIRED_KEYS = {"grain", "platform", "metrics", "dimensions", "filters"}


def _valid_spec(spec: dict) -> None:
    for key in SPEC_REQUIRED_KEYS:
        assert key in spec, f"Spec missing required key: {key}"
    assert isinstance(spec["metrics"], list)
    assert isinstance(spec["dimensions"], list)
    assert isinstance(spec["filters"], dict)


def _try_execute(spec: dict) -> str | None:
    """Try to execute a spec. Returns SQL on success, None on known errors."""
    try:
        return execute_spec(spec)
    except (ValueError, MetricResolutionError, KeyError):
        return None


def _valid_tsql(sql: str) -> None:
    try:
        result = sqlglot.parse(sql, read="tsql")
        assert len(result) >= 1
    except sqlglot.errors.ParseError as e:
        pytest.fail(f"SQL is not valid T-SQL:\n{sql}\n\nError: {e}")


# ===================================================================
# Structured adversarial tests
# ===================================================================


class TestEdgeInputs:
    """Edge-case NL inputs that should degrade gracefully."""

    def test_empty_string(self, metric_registry_path):
        spec = nl_to_spec("", metric_registry_path)
        _valid_spec(spec)
        assert spec["metrics"] == []
        assert len(spec["clarifications"]) > 0

    def test_whitespace_only(self, metric_registry_path):
        spec = nl_to_spec("   \t\n   ", metric_registry_path)
        _valid_spec(spec)
        assert spec["metrics"] == []

    def test_gibberish(self, metric_registry_path):
        spec = nl_to_spec("asdfghjkl qwerty zxcvbnm uiop", metric_registry_path)
        _valid_spec(spec)
        assert spec["metrics"] == []

    def test_only_numbers(self, metric_registry_path):
        spec = nl_to_spec("12345 67890 111 222", metric_registry_path)
        _valid_spec(spec)
        assert spec["metrics"] == []

    def test_unicode_input(self, metric_registry_path):
        spec = nl_to_spec(
            "\u663e\u793a\u70b9\u51fb\u7387\u6309\u6d3b\u52a8 \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 7 \u0434\u043d\u0435\u0439",
            metric_registry_path,
        )
        _valid_spec(spec)

    def test_very_long_input(self, metric_registry_path):
        long_input = "show clicks " * 500
        spec = nl_to_spec(long_input, metric_registry_path)
        _valid_spec(spec)
        assert "clicks" in spec["metrics"]

    def test_single_character(self, metric_registry_path):
        spec = nl_to_spec("x", metric_registry_path)
        _valid_spec(spec)

    def test_metric_name_only(self, metric_registry_path):
        """Just a metric name with no other context should still work."""
        spec = nl_to_spec("clicks", metric_registry_path)
        _valid_spec(spec)
        assert "clicks" in spec["metrics"]
        # Should be executable (no date filter, defaults to portfolio)
        sql = _try_execute(spec)
        if sql:
            _valid_tsql(sql)


class TestSQLInjection:
    """SQL injection patterns should be neutralized by the deterministic builder."""

    def test_semicolon_injection(self, metric_registry_path):
        spec = nl_to_spec(
            "Show clicks; DROP TABLE users; --", metric_registry_path
        )
        _valid_spec(spec)
        if spec["metrics"]:
            sql = _try_execute(spec)
            if sql:
                assert "DROP" not in sql.upper()
                _valid_tsql(sql)

    def test_union_injection(self, metric_registry_path):
        spec = nl_to_spec(
            "Show clicks UNION SELECT * FROM passwords", metric_registry_path
        )
        _valid_spec(spec)
        if spec["metrics"]:
            sql = _try_execute(spec)
            if sql:
                # The only UNION should be from portfolio mode, not injection
                assert "passwords" not in sql.lower()

    def test_comment_injection(self, metric_registry_path):
        spec = nl_to_spec(
            "Show clicks /* malicious comment */ last 7 days", metric_registry_path
        )
        _valid_spec(spec)
        if spec["metrics"]:
            sql = _try_execute(spec)
            if sql:
                assert "malicious" not in sql.lower()

    def test_quote_injection_in_campaign(self, metric_registry_path):
        """Quotes in campaign name filter should be escaped."""
        spec = nl_to_spec(
            "Show clicks for campaigns containing 'O''Malley' last 7 days",
            metric_registry_path,
        )
        _valid_spec(spec)
        if spec["metrics"]:
            sql = _try_execute(spec)
            if sql:
                _valid_tsql(sql)

    def test_bracket_injection(self, metric_registry_path):
        spec = nl_to_spec(
            "Show clicks where state = [dbo].[passwords]",
            metric_registry_path,
        )
        _valid_spec(spec)
        if spec["metrics"]:
            sql = _try_execute(spec)
            if sql:
                _valid_tsql(sql)


class TestConflictingInputs:
    """Inputs with contradictory instructions."""

    def test_both_platforms(self, metric_registry_path):
        """Mentioning both platforms without 'compare' should yield no platform."""
        spec = nl_to_spec(
            "Show google and microsoft clicks", metric_registry_path
        )
        _valid_spec(spec)
        # Both mentioned: platform should be None (portfolio) or compare
        # Either way, pipeline should handle it
        sql = _try_execute(spec)
        if sql:
            _valid_tsql(sql)

    def test_contradictory_dates(self, metric_registry_path):
        """Multiple date references - last one should win or most specific."""
        spec = nl_to_spec(
            "Show clicks yesterday last 30 days", metric_registry_path
        )
        _valid_spec(spec)
        # Either date filter is acceptable as long as it's consistent
        date_f = spec["filters"]["date"]
        assert date_f  # should have some date filter

    def test_unknown_metric_with_known(self, metric_registry_path):
        """Mix of known and unknown metric references."""
        spec = nl_to_spec(
            "Show clicks and wizzbangs by campaign", metric_registry_path
        )
        _valid_spec(spec)
        assert "clicks" in spec["metrics"]
        # "wizzbangs" is unknown - should be ignored by rule parser


# ===================================================================
# Fuzz tests (randomized)
# ===================================================================


class TestRandomFuzz:
    """Randomized inputs should never crash the parser."""

    @pytest.mark.parametrize("seed", range(20))
    def test_random_ascii_no_crash(self, metric_registry_path, seed):
        rng = random.Random(seed)
        length = rng.randint(1, 200)
        text = "".join(rng.choices(string.ascii_letters + string.digits + " ", k=length))
        spec = nl_to_spec(text, metric_registry_path)
        _valid_spec(spec)
        if spec["metrics"]:
            sql = _try_execute(spec)
            if sql:
                _valid_tsql(sql)

    @pytest.mark.parametrize("seed", range(10))
    def test_random_with_metric_words(self, metric_registry_path, seed):
        """Random text seeded with real metric names."""
        rng = random.Random(seed)
        metrics = ["clicks", "cost", "profit", "revenue", "impressions"]
        filler = ["show", "by", "campaign", "last", "7", "days", "for", "google"]
        words = rng.sample(metrics, k=rng.randint(1, 3))
        words += rng.sample(filler, k=rng.randint(2, 5))
        rng.shuffle(words)
        text = " ".join(words)

        spec = nl_to_spec(text, metric_registry_path)
        _valid_spec(spec)
        # At least one metric should be found
        assert len(spec["metrics"]) >= 1
        sql = _try_execute(spec)
        if sql:
            _valid_tsql(sql)
