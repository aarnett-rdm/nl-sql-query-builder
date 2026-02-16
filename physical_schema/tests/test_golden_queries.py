"""
test_golden_queries.py

Priority 4.1 + 4.2: End-to-end golden query integration tests.

Tests the full NL -> Spec -> SQL pipeline for real department queries.
Each test validates:
  - Spec structure (required keys, correct types)
  - Correct metric/platform/date extraction
  - SQL is parseable T-SQL (via sqlglot)
  - SQL references expected tables and keywords
"""

import re
from datetime import date, timedelta

import pytest
import sqlglot

from tools.nl_to_spec import nl_to_spec
from tools.spec_executor import execute_spec


# ===================================================================
# Validation helpers (Priority 4.2)
# ===================================================================

SPEC_REQUIRED_KEYS = {"grain", "platform", "metrics", "dimensions", "filters"}


def assert_valid_spec(spec: dict) -> None:
    """Validate spec has required keys and correct types."""
    for key in SPEC_REQUIRED_KEYS:
        assert key in spec, f"Spec missing required key: {key}"
    assert isinstance(spec["metrics"], list), "metrics must be a list"
    assert isinstance(spec["dimensions"], list), "dimensions must be a list"
    assert isinstance(spec["filters"], dict), "filters must be a dict"
    assert "date" in spec["filters"], "filters must contain 'date'"


def assert_valid_tsql(sql: str) -> None:
    """Parse SQL with sqlglot's TSQL dialect. Raises on syntax errors."""
    assert sql and sql.strip(), "SQL is empty"
    try:
        result = sqlglot.parse(sql, read="tsql")
        assert len(result) >= 1, "sqlglot parsed zero statements"
        # Ensure at least one statement is not None (valid parse)
        assert any(r is not None for r in result), "All parsed statements are None"
    except sqlglot.errors.ParseError as e:
        pytest.fail(f"SQL is not valid T-SQL:\n{sql}\n\nError: {e}")


def assert_sql_references_table(sql: str, table_fragment: str) -> None:
    """Check that generated SQL references a table (by partial name match)."""
    assert table_fragment.lower() in sql.lower(), (
        f"Expected SQL to reference '{table_fragment}'.\nSQL:\n{sql}"
    )


def assert_sql_has_select_from(sql: str) -> None:
    """Basic structural check: SQL has SELECT and FROM."""
    upper = sql.upper()
    assert "SELECT" in upper, f"SQL missing SELECT:\n{sql}"
    assert "FROM" in upper, f"SQL missing FROM:\n{sql}"


def assert_sql_has_where(sql: str) -> None:
    """Check SQL has a WHERE clause."""
    assert "WHERE" in sql.upper(), f"SQL missing WHERE:\n{sql}"


def assert_sql_has_group_by(sql: str) -> None:
    """Check SQL has a GROUP BY clause."""
    assert "GROUP BY" in sql.upper(), f"SQL missing GROUP BY:\n{sql}"


def assert_sql_has_date_predicate(sql: str) -> None:
    """Check SQL has a date filter (PST_Date reference)."""
    assert "PST_Date" in sql or "pst_date" in sql.lower(), (
        f"SQL missing date predicate:\n{sql}"
    )


def run_golden(nl: str, registry_path) -> tuple:
    """Run full pipeline: NL -> Spec -> SQL. Returns (spec, sql)."""
    spec = nl_to_spec(nl, registry_path)
    assert_valid_spec(spec)
    sql = execute_spec(spec)
    assert_sql_has_select_from(sql)
    assert_valid_tsql(sql)
    return spec, sql


# ===================================================================
# 4.1 Golden query tests
# ===================================================================


class TestBasicMetrics:
    """Basic metric queries without platform or dimension."""

    def test_total_spend_yesterday(self, metric_registry_path):
        spec, sql = run_golden(
            "What was our total spend yesterday?", metric_registry_path
        )
        assert "cost" in spec["metrics"]
        assert spec["filters"]["date"].get("yesterday") is True
        # Portfolio mode (no platform): UNION ALL of both platforms
        assert "UNION ALL" in sql.upper()

    def test_profit_last_7_days(self, metric_registry_path):
        spec, sql = run_golden("show profit last 7 days", metric_registry_path)
        assert "profit" in spec["metrics"]
        assert spec["filters"]["date"].get("last_n_days") == 7
        assert_sql_has_date_predicate(sql)


class TestByCampaign:
    """Campaign-level queries with dimensions."""

    def test_multi_metric_campaign_filter(self, metric_registry_path):
        spec, sql = run_golden(
            "Show clicks, cost, profit for campaigns containing 'MLB' last 30 days",
            metric_registry_path,
        )
        assert "clicks" in spec["metrics"]
        assert "cost" in spec["metrics"]
        assert "profit" in spec["metrics"]
        assert spec["filters"]["date"].get("last_n_days") == 30
        # Campaign filter extracted
        campaign = spec["filters"].get("campaign", {})
        assert campaign.get("terms") and "MLB" in campaign["terms"]
        # SQL should have LIKE for campaign name filter
        assert "LIKE" in sql.upper()

    def test_impressions_by_campaign_this_month(self, metric_registry_path):
        spec, sql = run_golden(
            "Show impressions by campaign this month",
            metric_registry_path,
        )
        assert "impressions" in spec["metrics"]
        assert "CampaignName" in spec["dimensions"]
        # "this month" -> date_from = first of month, date_to = today
        date_filter = spec["filters"]["date"]
        today = date.today()
        assert date_filter.get("date_from") == today.replace(day=1).isoformat()
        assert date_filter.get("date_to") == today.isoformat()
        # Should have GROUP BY for dimension
        assert_sql_has_group_by(sql)


class TestPlatformSpecific:
    """Platform-scoped queries (single platform, no UNION ALL)."""

    def test_google_impressions_by_campaign(self, metric_registry_path):
        spec, sql = run_golden(
            "Google ads impressions by campaign this month",
            metric_registry_path,
        )
        assert spec["platform"] == "google_ads"
        assert "impressions" in spec["metrics"]
        assert "CampaignName" in spec["dimensions"]
        # Single platform: no UNION ALL
        assert "UNION ALL" not in sql.upper()
        # Should reference Google-specific tables
        assert_sql_references_table(sql, "GoogleAds")

    def test_by_account_needs_account_table(self, metric_registry_path):
        """AccountName dimension auto-joins the Account table at campaign_calendar grain."""
        spec, sql = run_golden(
            "Google ads impressions by account this month", metric_registry_path
        )
        assert "AccountName" in spec["dimensions"]
        assert spec["platform"] == "google_ads"
        # Account table should be joined in
        assert_sql_references_table(sql, "GoogleAdsAccount")
        assert "AccountName" in sql
        assert_sql_has_group_by(sql)

    def test_microsoft_cost_by_campaign_yesterday(self, metric_registry_path):
        spec, sql = run_golden(
            "Microsoft ads cost by campaign yesterday",
            metric_registry_path,
        )
        assert spec["platform"] == "microsoft_ads"
        assert "cost" in spec["metrics"]
        assert "CampaignName" in spec["dimensions"]
        assert spec["filters"]["date"].get("yesterday") is True
        assert_sql_references_table(sql, "MicrosoftAds")


class TestDerivedMetrics:
    """Derived/calculated metrics (CTR, conversion rate, CPC, ROI)."""

    def test_ctr_by_campaign_microsoft(self, metric_registry_path):
        spec, sql = run_golden(
            "What is our CTR by campaign for microsoft ads?",
            metric_registry_path,
        )
        assert "click through rate" in spec["metrics"]
        assert spec["platform"] == "microsoft_ads"
        assert "CampaignName" in spec["dimensions"]
        # Derived metric uses safe divide: NULLIF
        assert "NULLIF" in sql.upper()

    def test_conversion_rate_by_campaign(self, metric_registry_path):
        spec, sql = run_golden(
            "Show conversion rate by campaign last 30 days",
            metric_registry_path,
        )
        assert "conversion rate" in spec["metrics"]
        assert spec["filters"]["date"].get("last_n_days") == 30
        # Derived: should include NULLIF for safe divide
        assert "NULLIF" in sql.upper()

    def test_cpc_google_last_7_days(self, metric_registry_path):
        spec, sql = run_golden(
            "Show cost per click for google ads last 7 days",
            metric_registry_path,
        )
        assert "cost per click" in spec["metrics"]
        assert spec["platform"] == "google_ads"
        assert "NULLIF" in sql.upper()


class TestWhereFilters:
    """Generic WHERE filter queries."""

    def test_cost_where_state_texas(self, metric_registry_path):
        spec, sql = run_golden(
            "Show cost where state = Texas",
            metric_registry_path,
        )
        assert "cost" in spec["metrics"]
        where = spec["filters"]["where"]
        state_filters = [f for f in where if f["field"] == "State"]
        assert len(state_filters) >= 1
        assert_sql_has_where(sql)

    def test_clicks_where_status_active(self, metric_registry_path):
        spec, sql = run_golden(
            "Show clicks where status is active",
            metric_registry_path,
        )
        assert "clicks" in spec["metrics"]
        where = spec["filters"]["where"]
        status_filters = [f for f in where if f["field"] == "CampaignStatus"]
        assert len(status_filters) >= 1


class TestCampaignNameFilter:
    """Campaign name free-text filtering."""

    def test_revenue_campaigns_containing_super_bowl(self, metric_registry_path):
        spec, sql = run_golden(
            "Show revenue for campaigns containing 'super bowl'",
            metric_registry_path,
        )
        assert "revenue" in spec["metrics"]
        campaign = spec["filters"].get("campaign", {})
        assert campaign.get("terms") and "super bowl" in campaign["terms"]
        # SQL should have LIKE for campaign name
        assert "LIKE" in sql.upper()

    def test_profit_campaigns_containing_spring_training(self, metric_registry_path):
        spec, sql = run_golden(
            "Show profit for campaigns containing 'spring training' last 30 days",
            metric_registry_path,
        )
        assert "profit" in spec["metrics"]
        campaign = spec["filters"].get("campaign", {})
        assert campaign.get("terms")
        assert spec["filters"]["date"].get("last_n_days") == 30


class TestComparisons:
    """Cross-platform and period-over-period comparisons."""

    def test_cross_platform_clicks(self, metric_registry_path):
        spec, sql = run_golden(
            "Compare Google vs Microsoft clicks by campaign last 30 days",
            metric_registry_path,
        )
        assert spec["compare"] is not None
        assert spec["compare"]["type"] == "cross_platform"
        # SQL should reference both platforms
        assert_sql_references_table(sql, "GoogleAds")
        assert_sql_references_table(sql, "MicrosoftAds")
        # Cross-platform uses FULL OUTER JOIN
        assert "FULL OUTER JOIN" in sql.upper()

    def test_period_over_period_cost(self, metric_registry_path):
        spec, sql = run_golden(
            "Compare cost last 7 days vs prior 7 days",
            metric_registry_path,
        )
        assert spec["compare"] is not None
        assert spec["compare"]["type"] == "period_over_period"
        assert spec["compare"]["metric"] == "cost"
        # POP SQL calculates delta
        assert "delta" in sql.lower()


class TestDateRanges:
    """Various date filter patterns."""

    def test_profit_q1_2025(self, metric_registry_path):
        spec, sql = run_golden("Show profit Q1 2025", metric_registry_path)
        assert "profit" in spec["metrics"]
        date_filter = spec["filters"]["date"]
        assert date_filter.get("date_from") == "2025-01-01"
        assert date_filter.get("date_to") == "2025-03-31"
        assert_sql_has_date_predicate(sql)

    def test_clicks_ytd(self, metric_registry_path):
        spec, sql = run_golden("Show clicks YTD", metric_registry_path)
        assert "clicks" in spec["metrics"]
        date_filter = spec["filters"]["date"]
        today = date.today()
        assert date_filter.get("date_from") == date(today.year, 1, 1).isoformat()

    def test_impressions_last_month(self, metric_registry_path):
        spec, sql = run_golden("Show impressions last month", metric_registry_path)
        assert "impressions" in spec["metrics"]
        date_filter = spec["filters"]["date"]
        today = date.today()
        first_this = today.replace(day=1)
        last_prev = first_this - timedelta(days=1)
        first_prev = last_prev.replace(day=1)
        assert date_filter.get("date_from") == first_prev.isoformat()
        assert date_filter.get("date_to") == last_prev.isoformat()

    def test_cost_last_quarter(self, metric_registry_path):
        spec, sql = run_golden("Show cost last quarter", metric_registry_path)
        assert "cost" in spec["metrics"]
        date_filter = spec["filters"]["date"]
        assert date_filter.get("date_from") is not None
        assert date_filter.get("date_to") is not None


class TestExchangeMetrics:
    """Exchange/benchmark metrics."""

    def test_exchange_revenue_last_30_days(self, metric_registry_path):
        spec, sql = run_golden(
            "Exchange revenue last 30 days",
            metric_registry_path,
        )
        assert "exchange revenue" in spec["metrics"]
        assert spec["filters"]["date"].get("last_n_days") == 30
        # Should reference the exchange metric table
        assert_sql_references_table(sql, "ClosePeerExchangeMetric")


class TestMultiMetric:
    """Multi-metric queries with multiple columns in SELECT."""

    def test_four_metrics_google_by_campaign(self, metric_registry_path):
        spec, sql = run_golden(
            "Show clicks, impressions, cost, and revenue by campaign for google ads last 7 days",
            metric_registry_path,
        )
        assert spec["platform"] == "google_ads"
        for m in ["clicks", "impressions", "cost", "revenue"]:
            assert m in spec["metrics"], f"Missing metric: {m}"
        assert "CampaignName" in spec["dimensions"]
        assert spec["filters"]["date"].get("last_n_days") == 7
        # Single platform: no UNION ALL
        assert "UNION ALL" not in sql.upper()
        assert_sql_has_group_by(sql)

    def test_clicks_and_cost_by_campaign_microsoft(self, metric_registry_path):
        spec, sql = run_golden(
            "Show clicks and cost by campaign for microsoft ads yesterday",
            metric_registry_path,
        )
        assert spec["platform"] == "microsoft_ads"
        assert "clicks" in spec["metrics"]
        assert "cost" in spec["metrics"]
        assert spec["filters"]["date"].get("yesterday") is True


class TestCampaignIdFilter:
    """Campaign ID numeric filters."""

    def test_cost_for_campaign_ids(self, metric_registry_path):
        spec, sql = run_golden(
            "Show cost for campaign IDs 101, 102, 103 last 30 days",
            metric_registry_path,
        )
        assert "cost" in spec["metrics"]
        ids = spec["filters"].get("campaign_ids", [])
        assert 101 in ids
        assert 102 in ids
        assert 103 in ids
        assert "IN (101, 102, 103)" in sql.replace("\n", " ")


# ===================================================================
# 4.2 SQL Syntax Validation (structural tests)
# ===================================================================


class TestSQLStructure:
    """Validate SQL structural properties across different query shapes."""

    def test_portfolio_union_is_valid_tsql(self, metric_registry_path):
        """Portfolio (no platform) produces valid UNION ALL + reaggregate SQL."""
        _, sql = run_golden("Show cost last 7 days", metric_registry_path)
        assert "UNION ALL" in sql.upper()
        # Should have a reaggregation wrapper with SUM
        assert "SUM" in sql.upper()

    def test_single_platform_no_union(self, metric_registry_path):
        """Single platform produces a clean single SELECT."""
        _, sql = run_golden(
            "Google ads clicks yesterday", metric_registry_path
        )
        assert "UNION ALL" not in sql.upper()

    def test_comparison_sql_structure(self, metric_registry_path):
        """Cross-platform comparison produces valid FULL OUTER JOIN SQL."""
        _, sql = run_golden(
            "Compare Google vs Microsoft cost by campaign last 30 days",
            metric_registry_path,
        )
        assert "COALESCE" in sql.upper()
        assert "NULLIF" in sql.upper()

    def test_period_over_period_sql_structure(self, metric_registry_path):
        """Period-over-period produces current/prior subqueries."""
        _, sql = run_golden(
            "Compare clicks last 7 days vs prior 7 days",
            metric_registry_path,
        )
        # Should have at least 2 subqueries (current + prior)
        select_count = sql.upper().count("SELECT")
        assert select_count >= 2, f"Expected >=2 SELECTs, got {select_count}"

    def test_derived_metric_includes_base_columns(self, metric_registry_path):
        """Derived metrics (CTR) include base metric columns in SELECT."""
        _, sql = run_golden(
            "Show CTR for google ads last 7 days",
            metric_registry_path,
        )
        # CTR = clicks / impressions: both base columns should appear
        assert "Clicks" in sql
        assert "Impressions" in sql

    def test_left_joins_used(self, metric_registry_path):
        """All dimension joins use LEFT JOIN (never INNER JOIN for safety)."""
        _, sql = run_golden(
            "Google ads impressions by campaign last 7 days",
            metric_registry_path,
        )
        assert "LEFT JOIN" in sql.upper()
        assert "INNER JOIN" not in sql.upper()
