"""
Ending Events Report

Shows events ending soon or recently ended with campaign performance metrics.
Helps identify campaigns that need adjustment as events approach.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

# Ensure tools/ is importable
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from tools.common import tsql_qualified_table  # noqa: E402
from tools.fabric_conn import FabricConnection  # noqa: E402
from tools.metric_resolver import MetricRegistry, MetricResolver  # noqa: E402
from tools.spec_executor import normalize_spec  # noqa: E402
from ui.shared import (  # noqa: E402
    build_totals_row,
    build_excel_bytes,
    format_results,
    init_fabric_state,
    render_fabric_sidebar,
    sanitize_filename,
)
from ui.viz_utils import create_chart  # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG_DIR = _PROJECT_ROOT / "current"
METRIC_REGISTRY = CONFIG_DIR / "metric_registry.json"

# Default metrics based on user requirements
DEFAULT_METRICS = [
    "clicks",
    "cost",
    "revenue",
    "conversions",
    "profit",
    "roi",
    "conversion rate",
    "exchange revenue",
]

# Campaign status options
CAMPAIGN_STATUSES = ["All Campaigns", "Enabled", "Paused", "Removed"]
DEFAULT_CAMPAIGN_STATUS = "Enabled"

# Chart configuration
MAX_CHART_CAMPAIGNS = 20  # Maximum number of campaigns to show in chart

# ---------------------------------------------------------------------------
# Session State
# ---------------------------------------------------------------------------

init_fabric_state()

if "event_results" not in st.session_state:
    st.session_state.event_results = None
if "event_sql" not in st.session_state:
    st.session_state.event_sql = None
if "show_event_chart" not in st.session_state:
    st.session_state.show_event_chart = True
if "sync_performance_dates" not in st.session_state:
    st.session_state.sync_performance_dates = False

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def escape_sql_string(value: str) -> str:
    """
    Escape a string value for safe use in T-SQL queries.

    Doubles single quotes to prevent SQL injection.
    """
    if value is None:
        return ""
    return str(value).replace("'", "''")


def load_metric_names() -> list[str]:
    """Load available metric names from the registry."""
    registry = MetricRegistry.from_path(METRIC_REGISTRY)
    return sorted(registry.metrics.keys())


def _load_distinct_values(table: str, column: str, error_context: str) -> list[str]:
    """
    Generic helper to load distinct values from a table column.

    Args:
        table: Fully qualified table name (e.g., "GoTicketsCoreEntity.Category")
        column: Column name to select
        error_context: Context string for error messages

    Returns:
        Sorted list of distinct values
    """
    if not st.session_state.get("fabric_connected", False):
        return []

    formatted_table = tsql_qualified_table(table)
    sql = f"SELECT DISTINCT {column} FROM {formatted_table} ORDER BY {column}"

    try:
        fc: FabricConnection = st.session_state.fabric_conn
        df = fc.execute(sql, row_limit=0)  # 0 = no limit (None would use default 10000)
        return df[column].tolist()
    except Exception as e:
        st.error(f"Could not load {error_context}: {e}")
        return []


def load_account_names(platform: str) -> list[str]:
    """
    Load distinct account names for the specified platform.

    Args:
        platform: Platform filter (e.g., "google_ads", "microsoft_ads")

    Returns:
        Sorted list of account names
    """
    if platform == "google_ads":
        table = "GoTicketsCoreEntity.GoogleAdsAccount"
    elif platform == "microsoft_ads":
        table = "GoTicketsCoreEntity.MicrosoftAdsAccount"
    else:
        return []

    return _load_distinct_values(table, "AccountName", f"account names for {platform}")


def load_category_names() -> list[str]:
    """
    Load distinct category names from the Category table.

    Returns:
        Sorted list of category names
    """
    table = "GoTicketsCoreEntity.Category"
    return _load_distinct_values(table, "CategoryName", "category names")


def build_event_spec(
    platform: str | None,
    account: str,
    category: str,
    metrics: list[str],
    event_date_from: date,
    event_date_to: date,
    campaign_status: str,
    campaign_contains: str = "",
    perf_date_from: date | None = None,
    perf_date_to: date | None = None,
) -> dict:
    """
    Build a spec for campaign-level reporting filtered by event dates.

    Uses campaign_calendar grain (since most metrics only support this grain)
    and filters on EventDateTimeLocal to show campaigns for events in the date range.

    Args:
        event_date_from/to: Filter which events to include (Event.EventDateTimeLocal)
        perf_date_from/to: Filter performance data period (PST_Date in fact table)
    """
    where_filters = []

    # Event date range (required) - determines WHICH campaigns to show
    # Use table.column format - system now prefers CoreEntity over Bronze
    where_filters.append(
        {
            "field": "Event.EventDateTimeLocal",
            "op": ">=",
            "value": event_date_from.isoformat(),
        }
    )
    where_filters.append(
        {
            "field": "Event.EventDateTimeLocal",
            "op": "<=",
            "value": event_date_to.isoformat(),
        }
    )

    # Account filter
    if account != "All Accounts":
        where_filters.append({"field": "AccountName", "value": account})

    # Category filter
    if category != "All Categories":
        where_filters.append({"field": "CategoryName", "value": category})

    # Campaign status filter
    if campaign_status != "All Campaigns":
        where_filters.append({"field": "CampaignStatus", "value": campaign_status})

    # Performance date range (optional) - determines WHAT data to aggregate
    date_filter = {}
    if perf_date_from and perf_date_to:
        date_filter = {
            "date_from": perf_date_from.isoformat(),
            "date_to": perf_date_to.isoformat(),
        }

    # Campaign contains filter (OR logic)
    filters_dict = {"date": date_filter, "where": where_filters}
    if campaign_contains:
        campaign_terms = [t.strip() for t in campaign_contains.split(",") if t.strip()]
        filters_dict["campaign"] = {"terms": campaign_terms, "mode": "any"}

    return {
        "metrics": metrics,
        "platform": platform,
        "grain": "campaign_calendar",
        "dimensions": ["CampaignName"],
        "filters": filters_dict,
    }


def build_custom_event_query(
    platform: str,
    metrics: list[str],
    event_date_from: date,
    event_date_to: date,
    perf_date_from: date,
    perf_date_to: date,
    account: str = "All Accounts",
    category: str = "All Categories",
    campaign_status: str = "All Campaigns",
    campaign_contains: str = "",
) -> str:
    """
    Build a custom SQL query for ending events using CTEs.

    This handles:
    - Event date filtering via CTE
    - Category filtering via Event → Performer → Category join chain
    - Multiple metric sources (campaign performance + exchange metrics)
    - Proper aggregation to avoid fan-out
    """
    # Determine platform-specific tables
    if platform == "google_ads":
        perf_table = tsql_qualified_table("GoTicketsPerformanceMetric.GoogleAdsCampaignPerformanceMetric")
        campaign_table = tsql_qualified_table("GoTicketsCoreEntity.GoogleAdsCampaign")
        account_table = tsql_qualified_table("GoTicketsCoreEntity.GoogleAdsAccount")
        event_map_table = tsql_qualified_table("GoTicketsEntityMap.GoogleAdsCampaignEventMap")
    else:  # microsoft_ads
        perf_table = tsql_qualified_table("GoTicketsPerformanceMetric.MicrosoftAdsCampaignPerformanceMetric")
        campaign_table = tsql_qualified_table("GoTicketsCoreEntity.MicrosoftAdsCampaign")
        account_table = tsql_qualified_table("GoTicketsCoreEntity.MicrosoftAdsAccount")
        event_map_table = tsql_qualified_table("GoTicketsEntityMap.MicrosoftAdsCampaignEventMap")

    event_table = tsql_qualified_table("GoTicketsCoreEntity.Event")
    calendar_table = tsql_qualified_table("Utility.DimCalendar")
    performer_table = tsql_qualified_table("GoTicketsCoreEntity.Performer")
    category_table = tsql_qualified_table("GoTicketsCoreEntity.Category")
    exchange_metric_table = tsql_qualified_table("GoTicketsExchangeMetric.ClosePeerExchangeMetric")

    # Load metric registry to separate metrics by source table
    registry = MetricRegistry.from_path(METRIC_REGISTRY)
    resolver = MetricResolver(registry)

    # Separate metrics by source
    perf_metrics = []
    exchange_metrics = []
    expected_table = "GoogleAdsCampaignPerformanceMetric" if platform == "google_ads" else "MicrosoftAdsCampaignPerformanceMetric"

    for metric_name in metrics:
        metric = registry.metrics.get(metric_name)
        if not metric:
            continue

        # Get base_columns, skip if not present (derived metrics may not have this)
        base_columns = metric.get("base_columns", [])
        if not base_columns:
            # Derived metrics - add to perf_metrics and let resolver handle it
            perf_metrics.append(metric_name)
            continue

        base_tables = {col["table"] for col in base_columns}

        # Check if this is an exchange metric
        if "ClosePeerExchangeMetric" in base_tables:
            exchange_metrics.append(metric_name)
        elif not base_tables or expected_table in base_tables:
            perf_metrics.append(metric_name)

    # Get SQL expressions for performance metrics
    perf_metric_selects = []
    seen_aliases = set()  # Track to avoid duplicates

    for metric_name in perf_metrics:
        resolved = resolver.resolve_metrics([metric_name], "campaign_calendar", platform, fact_alias="fact")
        if resolved and len(resolved[1]) > 0:
            for metric_sql in resolved[1]:  # Handle all resolved metrics (derived metrics may expand)
                # Use canonical_key as unique identifier to avoid duplicates
                key = metric_sql.canonical_key.lower()
                if key not in seen_aliases:
                    seen_aliases.add(key)
                    perf_metric_selects.append(f"  {metric_sql.select_sql}")

    # Build WHERE predicates for ending_campaigns CTE (includes category filter)
    event_where_predicates = []

    if category != "All Categories":
        event_where_predicates.append(f"cat.[CategoryName] = '{escape_sql_string(category)}'")

    event_where_clause = f"\n    AND {' AND '.join(event_where_predicates)}" if event_where_predicates else ""

    # Build WHERE predicates for main query
    main_where_predicates = []

    if account != "All Accounts":
        main_where_predicates.append(f"acc.[AccountName] = '{escape_sql_string(account)}'")

    if campaign_status != "All Campaigns":
        main_where_predicates.append(f"c.[CampaignStatus] = '{escape_sql_string(campaign_status)}'")

    if campaign_contains:
        campaign_terms = [t.strip() for t in campaign_contains.split(",") if t.strip()]
        term_preds = [f"c.[CampaignName] LIKE '%{escape_sql_string(term)}%'" for term in campaign_terms]
        main_where_predicates.append(f"({' OR '.join(term_preds)})")

    main_where_clause = f"\n  AND {' AND '.join(main_where_predicates)}" if main_where_predicates else ""

    # Build exchange revenue CTE if needed
    exchange_cte = ""
    exchange_join = ""
    exchange_selects = []

    if exchange_metrics:
        # Aggregate exchange metrics by campaign
        exchange_metric_aggs = []
        for metric_name in exchange_metrics:
            if metric_name == "exchange revenue":
                exchange_metric_aggs.append("SUM(ex.[ExchangeRevenue]) AS [exchange revenue]")
            elif metric_name == "exchange orders":
                exchange_metric_aggs.append("SUM(ex.[ExchangeOrders]) AS [exchange orders]")

        if exchange_metric_aggs:
            exchange_cte = f""",
exchange_metrics AS (
  -- Aggregate exchange metrics by campaign to avoid fan-out
  -- Only include exchange revenue from events in the ending date range
  SELECT
    cem.[CampaignId],
    {',\n    '.join(exchange_metric_aggs)}
  FROM {event_map_table} cem
  JOIN {event_table} e ON cem.[EventId] = e.[EventId]
  JOIN {exchange_metric_table} ex ON e.[EventId] = ex.[EventId]
  JOIN {calendar_table} cal ON ex.[CalendarId] = cal.[CalendarId]
  WHERE e.[EventDateTimeLocal] >= '{event_date_from.isoformat()}'
    AND e.[EventDateTimeLocal] <= '{event_date_to.isoformat()}'
    AND cal.[PST_Date] >= '{perf_date_from.isoformat()}'
    AND cal.[PST_Date] <= '{perf_date_to.isoformat()}'
  GROUP BY cem.[CampaignId]
)"""
            exchange_join = "\nLEFT JOIN exchange_metrics exm ON c.[CampaignId] = exm.[CampaignId]"
            # Exchange metrics are already aggregated in the CTE, use MAX to avoid fan-out
            # (the same value appears in multiple rows when joined to daily performance data)
            # Only add exchange metrics that aren't already in performance metrics
            for m in exchange_metrics:
                if m.lower() not in seen_aliases:
                    seen_aliases.add(m.lower())
                    exchange_selects.append(f"  MAX(exm.[{m}]) AS [{m}]")

    # Combine all metric selects
    all_selects = perf_metric_selects + exchange_selects
    if not all_selects:
        all_selects = ["  COUNT(*) AS [row_count]"]  # Fallback if no metrics

    # Build the query
    sql = f"""
-- Ending Events Query: Campaigns with events in {event_date_from} to {event_date_to}
-- Performance period: {perf_date_from} to {perf_date_to}

WITH ending_campaigns AS (
  -- Find campaigns that have at least one event in the target date range
  -- Join to Performer → Category for category filtering
  SELECT DISTINCT cem.[CampaignId]
  FROM {event_map_table} cem
  JOIN {event_table} e ON cem.[EventId] = e.[EventId]
  JOIN {performer_table} p ON e.[PrimaryPerformerId] = p.[PerformerId]
  JOIN {category_table} cat ON p.[PrimaryCategoryId] = cat.[CategoryId]
  WHERE e.[EventDateTimeLocal] >= '{event_date_from.isoformat()}'
    AND e.[EventDateTimeLocal] <= '{event_date_to.isoformat()}'{event_where_clause}
){exchange_cte}
SELECT
  c.[CampaignName],
{',\n'.join(all_selects)}
FROM {perf_table} fact
JOIN {calendar_table} cal ON fact.[CalendarId] = cal.[CalendarId]
JOIN {campaign_table} c ON fact.[CampaignId] = c.[CampaignId]
JOIN ending_campaigns ec ON ec.[CampaignId] = c.[CampaignId]
JOIN {account_table} acc ON fact.[AccountId] = acc.[AccountId]{exchange_join}
WHERE cal.[PST_Date] >= '{perf_date_from.isoformat()}'
  AND cal.[PST_Date] <= '{perf_date_to.isoformat()}'{main_where_clause}
GROUP BY c.[CampaignName]
ORDER BY c.[CampaignName]
"""

    return sql


def execute_event_query(spec: dict) -> tuple[pd.DataFrame, str]:
    """
    Execute event query using custom CTE-based SQL.

    This avoids the complexity of modifying the general query builder
    and provides a clean, maintainable solution for the ending events use case.
    """
    spec = normalize_spec(spec)

    # Extract parameters from spec
    platform = spec.get("platform")
    if not platform:
        raise ValueError("Platform must be specified for event date filtering")

    metrics = spec.get("metrics", [])

    # Extract event date range from where filters
    event_date_from = None
    event_date_to = None
    other_filters = {}

    for f in spec.get("filters", {}).get("where", []):
        field = f.get("field", "")
        if "Event.EventDateTimeLocal" in field:
            op = f.get("op")
            value = f.get("value")
            if op == ">=":
                event_date_from = date.fromisoformat(value)
            elif op == "<=":
                event_date_to = date.fromisoformat(value)
        elif field == "AccountName":
            other_filters["account"] = f.get("value")
        elif field == "CategoryName":
            other_filters["category"] = f.get("value")
        elif field == "CampaignStatus":
            other_filters["campaign_status"] = f.get("value")

    if not event_date_from or not event_date_to:
        raise ValueError("Event date range must have both from and to dates")

    # Extract performance date range
    date_filter = spec.get("filters", {}).get("date", {})
    date_from_str = date_filter.get("date_from")
    date_to_str = date_filter.get("date_to")

    if not date_from_str or not date_to_str:
        raise ValueError("Performance date range must have both from and to dates")

    perf_date_from = date.fromisoformat(date_from_str)
    perf_date_to = date.fromisoformat(date_to_str)

    # Extract campaign contains filter
    campaign_filter = spec.get("filters", {}).get("campaign", {})
    campaign_terms = campaign_filter.get("terms", [])
    campaign_contains = ", ".join(campaign_terms) if campaign_terms else ""

    # Build custom SQL
    sql = build_custom_event_query(
        platform=platform,
        metrics=metrics,
        event_date_from=event_date_from,
        event_date_to=event_date_to,
        perf_date_from=perf_date_from,
        perf_date_to=perf_date_to,
        account=other_filters.get("account", "All Accounts"),
        category=other_filters.get("category", "All Categories"),
        campaign_status=other_filters.get("campaign_status", "All Campaigns"),
        campaign_contains=campaign_contains,
    )

    fc: FabricConnection = st.session_state.fabric_conn
    df = fc.execute(sql)

    return df, sql


# ---------------------------------------------------------------------------
# Main Application
# ---------------------------------------------------------------------------


def main():
    st.set_page_config(
        page_title="Ending Events",
        page_icon="🎭",
        layout="wide",
    )

    # Sidebar
    with st.sidebar:
        st.title("Ending Events")
        st.divider()
        render_fabric_sidebar()
        st.divider()
        if st.button("Clear results", use_container_width=True):
            st.session_state.event_results = None
            st.session_state.event_sql = None
            st.rerun()

    # Main content
    st.title("🎭 Campaigns for Events Ending Soon / Recently Ended")
    st.markdown(
        "View campaigns running for events in a specific date range, "
        "with flexible performance period selection. **Event dates** determine which campaigns "
        "to include; **performance dates** determine what metrics to aggregate."
    )

    if not st.session_state.fabric_connected:
        st.warning("Connect to Fabric in the sidebar to view event data.")
        return

    # Event Date Range selector
    st.subheader("📅 Event Date Range")
    st.markdown("*Determines which campaigns to include based on event dates*")
    col1, col2 = st.columns(2)
    with col1:
        days_before = st.number_input(
            "Days before today",
            min_value=0,
            max_value=365,
            value=30,
            help="Include events that ended this many days ago",
        )
    with col2:
        days_after = st.number_input(
            "Days after today",
            min_value=0,
            max_value=365,
            value=60,
            help="Include events happening this many days from now",
        )

    today = date.today()
    event_date_from = today - timedelta(days=days_before)
    event_date_to = today + timedelta(days=days_after)

    st.info(f"📍 Including campaigns for events from **{event_date_from}** to **{event_date_to}**")

    # Performance Date Range selector
    st.subheader("📊 Performance Data Period")
    st.markdown("*Determines which date range of performance metrics to aggregate*")

    sync_dates = st.checkbox(
        "Use same dates as event range",
        value=st.session_state.sync_performance_dates,
        key="sync_perf_checkbox",
        help="When checked, performance period will match event date range",
    )
    st.session_state.sync_performance_dates = sync_dates

    if sync_dates:
        perf_date_from = event_date_from
        perf_date_to = event_date_to
        st.info(f"📊 Performance data period: **{perf_date_from}** to **{perf_date_to}** (synced with event dates)")
    else:
        col1, col2 = st.columns(2)
        with col1:
            perf_date_from = st.date_input(
                "Performance data from",
                value=today - timedelta(days=30),
                help="Start date for performance metrics aggregation",
            )
        with col2:
            perf_date_to = st.date_input(
                "Performance data to",
                value=today,
                help="End date for performance metrics aggregation",
            )
        st.info(f"📊 Aggregating performance metrics from **{perf_date_from}** to **{perf_date_to}**")

    # Filters
    st.subheader("Filters")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        platform = st.selectbox(
            "Platform",
            options=[None, "google_ads", "microsoft_ads"],
            format_func=lambda x: "Both" if x is None else ("Google Ads" if x == "google_ads" else "Microsoft Ads"),
        )

    # Load account names for selected platform
    account_options = []
    if platform:
        with st.spinner("Loading accounts..."):
            account_options = load_account_names(platform)

    with col2:
        account = st.selectbox(
            "Account",
            options=["All Accounts"] + account_options,
        )

    # Category filter
    with col3:
        with st.spinner("Loading categories..."):
            category_options = load_category_names()
        category = st.selectbox(
            "Category",
            options=["All Categories"] + category_options,
        )

    # Campaign status filter
    with col4:
        campaign_status = st.selectbox(
            "Campaign Status",
            options=CAMPAIGN_STATUSES,
            index=CAMPAIGN_STATUSES.index(DEFAULT_CAMPAIGN_STATUS),
        )

    # Campaign contains filter
    campaign_contains = st.text_input(
        "Campaign contains (comma-separated for OR logic)",
        help='E.g., "mlb, spring training" matches campaigns containing either term',
    )

    # Metric selector
    st.subheader("Metrics")
    available_metrics = load_metric_names()
    selected_metrics = st.multiselect(
        "Select metrics to display",
        options=available_metrics,
        default=[m for m in DEFAULT_METRICS if m in available_metrics],
    )

    if not selected_metrics:
        st.warning("Please select at least one metric.")
        return

    # Execute button
    if st.button("Load Event Data", type="primary", use_container_width=True):
        # Validate platform selection
        if not platform:
            st.error("Please select a specific platform (Google Ads or Microsoft Ads). 'Both' is not supported for event filtering.")
            return

        with st.spinner("Querying event data..."):
            try:
                spec = build_event_spec(
                    platform=platform,
                    account=account,
                    category=category,
                    metrics=selected_metrics,
                    event_date_from=event_date_from,
                    event_date_to=event_date_to,
                    campaign_status=campaign_status,
                    campaign_contains=campaign_contains,
                    perf_date_from=perf_date_from,
                    perf_date_to=perf_date_to,
                )

                df, sql = execute_event_query(spec)

                st.session_state.event_results = df
                st.session_state.event_sql = sql
                st.success(
                    f"✅ Loaded {len(df)} campaign-event records | "
                    f"Events: {event_date_from} to {event_date_to} | "
                    f"Performance: {perf_date_from} to {perf_date_to}"
                )

            except Exception as e:
                st.error(f"Query failed: {e}")
                import traceback

                st.code(traceback.format_exc())
                return

    # Display results
    if st.session_state.event_results is not None:
        df = st.session_state.event_results

        st.subheader("Campaign Performance")
        st.dataframe(format_results(df), use_container_width=True)

        # Totals row
        totals = build_totals_row(df)
        if totals is not None:
            st.markdown("**Totals**")
            st.dataframe(format_results(totals), use_container_width=True)

        # Export buttons
        st.subheader("Export")
        col1, col2 = st.columns(2)

        _today = date.today().strftime("%Y%m%d")
        _stem = sanitize_filename(f"ending_events_{_today}")

        with col1:
            csv = df.to_csv(index=False)
            st.download_button(
                "⬇ Download CSV",
                data=csv,
                file_name=f"{_stem}.csv",
                mime="text/csv",
                use_container_width=True,
            )

        with col2:
            sheets = {"Events": df}
            if totals is not None:
                sheets["Totals"] = totals
            excel_bytes = build_excel_bytes(sheets)
            st.download_button(
                "⬇ Download Excel",
                data=excel_bytes,
                file_name=f"{_stem}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

        # Chart
        if len(df) > 0 and len(selected_metrics) > 0:
            st.subheader("Visualization")

            # Chart controls
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                chart_metric = st.selectbox("Metric to chart", options=selected_metrics)
            with col2:
                chart_type = st.selectbox(
                    "Chart type",
                    options=["bar", "line", "area"],
                )
            with col3:
                if st.button("📊 Hide Chart" if st.session_state.show_event_chart else "📊 Show Chart"):
                    st.session_state.show_event_chart = not st.session_state.show_event_chart
                    st.rerun()

            if st.session_state.show_event_chart:
                try:
                    # Prepare data for chart
                    chart_df = df.copy()

                    # Sort by metric value for better visualization
                    chart_df = chart_df.sort_values(chart_metric, ascending=False).head(MAX_CHART_CAMPAIGNS)

                    config = {
                        "x": "CampaignName",
                        "y": chart_metric,
                        "title": f"Top {MAX_CHART_CAMPAIGNS} Campaigns by {chart_metric}",
                    }

                    fig = create_chart(chart_df, chart_type, config)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.warning(f"Could not create chart: {e}")

        # Show SQL
        with st.expander("View SQL Query"):
            st.code(st.session_state.event_sql, language="sql")


if __name__ == "__main__":
    main()
