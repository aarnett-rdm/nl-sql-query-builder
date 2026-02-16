"""
test_multi_fact.py

Tests for multi-fact-table CTE+JOIN support.
Covers: MetricResolver.partition_metrics(), _build_multi_fact_cte(), and end-to-end integration.
"""

import json
from pathlib import Path

import pytest
import sqlglot

from tools.metric_resolver import MetricRegistry, MetricResolver, MetricResolutionError
from tools.spec_executor import execute_spec, normalize_spec

# ===================================================================
# Fixtures
# ===================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = PROJECT_ROOT / "current" / "metric_registry.json"
SCHEMA_PATH = PROJECT_ROOT / "current" / "physical_schema.json"


@pytest.fixture
def registry():
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        return MetricRegistry(json.load(f))


@pytest.fixture
def resolver(registry):
    return MetricResolver(registry)


def _make_spec(metrics, platform="google_ads", dimensions=None, date_filter=None, campaign=None):
    """Helper to build a minimal spec for execute_spec()."""
    spec = {
        "grain": "campaign_calendar",
        "platform": platform,
        "metrics": metrics,
        "dimensions": dimensions or [],
        "filters": {
            "date": date_filter or {"last_n_days": 7},
            "where": [],
        },
    }
    if campaign:
        spec["filters"]["campaign"] = campaign
    return spec


def assert_valid_tsql(sql):
    assert sql and sql.strip(), "SQL is empty"
    try:
        result = sqlglot.parse(sql, read="tsql")
        assert len(result) >= 1, "sqlglot parsed zero statements"
        assert any(r is not None for r in result), "All parsed statements are None"
    except sqlglot.errors.ParseError as e:
        pytest.fail(f"SQL is not valid T-SQL:\n{sql}\n\nError: {e}")


# ===================================================================
# TestPartitionMetrics
# ===================================================================

class TestPartitionMetrics:

    def test_single_table_returns_one_partition(self, resolver):
        """clicks + cost share GoogleAdsCampaignPerformanceMetric -> 1 partition."""
        result = resolver.partition_metrics(["clicks", "cost"], "campaign_calendar", "google_ads")
        assert len(result) == 1
        assert len(result[0][1]) == 2

    def test_multi_table_returns_multiple_partitions(self, resolver):
        """cost + exchange revenue -> 2 partitions (different fact tables)."""
        result = resolver.partition_metrics(
            ["cost", "exchange revenue"], "campaign_calendar", "google_ads"
        )
        assert len(result) == 2
        # Each partition should have exactly 1 metric
        metric_sets = [set(p[1]) for p in result]
        assert {"cost"} in metric_sets
        assert {"exchange revenue"} in metric_sets

    def test_derived_stays_with_bases(self, resolver):
        """profit is a base metric on GoogleAdsCampaignPerformanceMetric;
        cost is also there. Both should be in the same partition."""
        result = resolver.partition_metrics(
            ["profit", "cost", "exchange revenue"], "campaign_calendar", "google_ads"
        )
        assert len(result) == 2
        # profit and cost should be in the same partition
        for table, metrics in result:
            if "profit" in metrics:
                assert "cost" in metrics
                break
        else:
            pytest.fail("profit not found in any partition")

    def test_all_same_table(self, resolver):
        """Multiple ads metrics all on the same fact table -> 1 partition."""
        result = resolver.partition_metrics(
            ["clicks", "cost", "impressions", "conversions", "profit"],
            "campaign_calendar", "google_ads"
        )
        assert len(result) == 1

    def test_unknown_metric_raises(self, resolver):
        """Invalid metric name -> MetricResolutionError."""
        with pytest.raises(MetricResolutionError):
            resolver.partition_metrics(["not_a_metric"], "campaign_calendar", "google_ads")

    def test_partition_preserves_original_names(self, resolver):
        """Metric names in output should match input strings."""
        result = resolver.partition_metrics(
            ["cost", "exchange revenue"], "campaign_calendar", "google_ads"
        )
        all_names = []
        for _, names in result:
            all_names.extend(names)
        assert sorted(all_names) == sorted(["cost", "exchange revenue"])

    def test_grain_validation_still_applies(self, resolver):
        """Unsupported grain should raise even during partitioning."""
        with pytest.raises(MetricResolutionError, match="not supported at grain"):
            resolver.partition_metrics(["clicks"], "nonexistent_grain", "google_ads")

    def test_single_metric_single_partition(self, resolver):
        """Edge case: 1 metric -> 1 partition."""
        result = resolver.partition_metrics(["clicks"], "campaign_calendar", "google_ads")
        assert len(result) == 1
        assert result[0][1] == ["clicks"]


# ===================================================================
# TestBuildMultiFactCte
# ===================================================================

class TestBuildMultiFactCte:

    def test_two_fact_tables_produces_join(self):
        """Output should contain FULL OUTER JOIN and COALESCE."""
        spec = _make_spec(
            ["cost", "exchange revenue"],
            dimensions=["CampaignName"],
        )
        sql = execute_spec(spec)
        sql_upper = sql.upper()
        assert "FULL OUTER JOIN" in sql_upper
        assert "COALESCE" in sql_upper
        assert_valid_tsql(sql)

    def test_dimensions_in_coalesce(self):
        """Each dimension should appear in a COALESCE expression."""
        spec = _make_spec(
            ["cost", "exchange revenue"],
            dimensions=["CampaignName"],
        )
        sql = execute_spec(spec)
        assert "COALESCE(" in sql
        assert "[CampaignName]" in sql

    def test_metrics_from_correct_cte(self):
        """cost from one CTE, exchange revenue from another."""
        spec = _make_spec(
            ["cost", "exchange revenue"],
            dimensions=["CampaignName"],
        )
        sql = execute_spec(spec)
        # Both metrics should appear in final SELECT
        assert "[cost]" in sql
        assert "[exchange revenue]" in sql
        assert_valid_tsql(sql)

    def test_no_dimensions_uses_cross_join(self):
        """No dimensions -> CROSS JOIN instead of FULL OUTER JOIN."""
        spec = _make_spec(
            ["cost", "exchange revenue"],
            dimensions=[],
        )
        sql = execute_spec(spec)
        sql_upper = sql.upper()
        assert "CROSS JOIN" in sql_upper
        assert "FULL OUTER JOIN" not in sql_upper
        assert_valid_tsql(sql)

    def test_date_filters_applied_to_all_ctes(self):
        """Both CTEs should have date filter WHERE clauses."""
        spec = _make_spec(
            ["cost", "exchange revenue"],
            dimensions=["CampaignName"],
            date_filter={"date_from": "2026-01-01", "date_to": "2026-01-31"},
        )
        sql = execute_spec(spec)
        # The date should appear in both CTE definitions
        assert sql.count("2026-01-01") >= 2
        assert sql.count("2026-01-31") >= 2
        assert_valid_tsql(sql)

    def test_campaign_filter_applied_to_all_ctes(self):
        """Campaign name filters should be in both CTEs."""
        spec = _make_spec(
            ["cost", "exchange revenue"],
            dimensions=["CampaignName"],
            campaign={"terms": ["spring training"], "mode": "any"},
        )
        sql = execute_spec(spec)
        sql_lower = sql.lower()
        # "spring training" should appear in both CTE WHERE clauses
        assert sql_lower.count("spring training") >= 2
        assert_valid_tsql(sql)

    def test_sqlglot_validates_output(self):
        """Generated SQL should parse as valid T-SQL."""
        spec = _make_spec(
            ["clicks", "cost", "exchange revenue"],
            dimensions=["CampaignName"],
        )
        sql = execute_spec(spec)
        assert_valid_tsql(sql)

    def test_single_partition_bypasses_multi_fact(self):
        """1 partition (single fact table) -> no multi-fact join."""
        spec = _make_spec(
            ["clicks", "cost"],
            dimensions=["CampaignName"],
        )
        sql = execute_spec(spec)
        sql_upper = sql.upper()
        assert "FULL OUTER JOIN" not in sql_upper
        assert "MF_0" not in sql_upper
        assert_valid_tsql(sql)


# ===================================================================
# TestEndToEnd
# ===================================================================

class TestEndToEnd:

    def test_spec_executor_multi_fact(self):
        """execute_spec() with cost + exchange revenue returns valid SQL."""
        spec = _make_spec(
            ["cost", "exchange revenue"],
            dimensions=["CampaignName"],
        )
        sql = execute_spec(spec)
        assert sql.strip()
        assert_valid_tsql(sql)
        # Should reference both fact tables
        sql_lower = sql.lower()
        assert "googleadscampaignperformancemetric" in sql_lower
        assert "closepeerexchangemetric" in sql_lower

    def test_spec_executor_single_fact_unchanged(self):
        """Existing single-fact queries produce identical-shaped output."""
        spec = _make_spec(
            ["clicks", "cost", "impressions"],
            dimensions=["CampaignName"],
        )
        sql = execute_spec(spec)
        sql_upper = sql.upper()
        # No multi-fact join needed
        assert "FULL OUTER JOIN" not in sql_upper
        assert sql_upper.strip().startswith("SELECT")
        assert_valid_tsql(sql)

    def test_portfolio_mode_multi_fact(self):
        """platform=None + multi-fact -> UNION of multi-fact CTEs."""
        spec = _make_spec(
            ["cost", "exchange revenue"],
            platform=None,
            dimensions=["CampaignName"],
        )
        sql = execute_spec(spec)
        assert sql.strip()
        # Should produce valid SQL with both platforms handled
        assert_valid_tsql(sql)

    def test_three_metrics_two_fact_tables(self):
        """clicks + cost from ads, exchange revenue from exchange -> 2 derived tables."""
        spec = _make_spec(
            ["clicks", "cost", "exchange revenue"],
            dimensions=["CampaignName"],
            date_filter={"date_from": "2026-01-01", "date_to": "2026-01-31"},
        )
        sql = execute_spec(spec)
        sql_upper = sql.upper()
        assert "MF_0" in sql_upper
        assert "MF_1" in sql_upper
        assert "FULL OUTER JOIN" in sql_upper
        assert_valid_tsql(sql)
