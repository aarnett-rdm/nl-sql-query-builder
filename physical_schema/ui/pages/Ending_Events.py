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
from tools.metric_resolver import MetricRegistry  # noqa: E402
from tools.spec_executor import execute_spec, normalize_spec  # noqa: E402
from ui.shared import (  # noqa: E402
    build_totals_row,
    build_excel_bytes,
    format_results,
    init_fabric_state,
    render_fabric_sidebar,
    sanitize_filename,
)
from ui.viz_utils import create_chart, detect_visualization_opportunity  # noqa: E402

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

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def load_metric_names() -> list[str]:
    """Load available metric names from the registry."""
    registry = MetricRegistry.from_path(METRIC_REGISTRY)
    return sorted(registry.metrics.keys())


def load_account_names(platform: str) -> list[str]:
    """
    Load distinct account names for the specified platform.

    Args:
        platform: Platform filter (e.g., "google_ads", "microsoft_ads")

    Returns:
        Sorted list of account names
    """
    if not st.session_state.get("fabric_connected", False):
        return []

    if platform == "google_ads":
        table = "GoTicketsCoreEntity.GoogleAdsAccount"
    elif platform == "microsoft_ads":
        table = "GoTicketsCoreEntity.MicrosoftAdsAccount"
    else:
        return []

    formatted_table = tsql_qualified_table(table)
    sql = f"SELECT DISTINCT TOP 10000 AccountName FROM {formatted_table} ORDER BY AccountName"

    try:
        fc: FabricConnection = st.session_state.fabric_conn
        df = fc.execute(sql, row_limit=None)
        return df["AccountName"].tolist()
    except Exception as e:
        st.error(f"Could not load account names for {platform}: {e}")
        return []


def load_category_names() -> list[str]:
    """
    Load distinct category names from the Category table.

    Returns:
        Sorted list of category names
    """
    if not st.session_state.get("fabric_connected", False):
        return []

    table = "GoTicketsCoreEntity.Category"
    formatted_table = tsql_qualified_table(table)
    sql = f"SELECT DISTINCT TOP 10000 CategoryName FROM {formatted_table} ORDER BY CategoryName"

    try:
        fc: FabricConnection = st.session_state.fabric_conn
        df = fc.execute(sql, row_limit=None)
        return df["CategoryName"].tolist()
    except Exception as e:
        st.error(f"Could not load category names: {e}")
        return []


def build_event_spec(
    platform: str | None,
    account: str,
    category: str,
    metrics: list[str],
    event_date_from: date,
    event_date_to: date,
    campaign_status: str,
    campaign_contains: str = "",
) -> dict:
    """
    Build a spec for campaign-level reporting filtered by event dates.

    Uses campaign_calendar grain (since most metrics only support this grain)
    and filters on EventDateTimeLocal to show campaigns for events in the date range.
    """
    where_filters = []

    # Event date range (required)
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

    # Campaign contains filter (OR logic)
    filters_dict = {"date": {}, "where": where_filters}
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


def execute_event_query(spec: dict) -> tuple[pd.DataFrame, str]:
    """Execute event query and return results with SQL."""
    spec = normalize_spec(spec)
    sql = execute_spec(spec)

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
        "View campaigns running for events in a specific date range. "
        "Shows campaign performance metrics, filtered to only include campaigns with events in the selected date range. "
        "Useful for identifying which campaigns need optimization as events approach."
    )

    if not st.session_state.fabric_connected:
        st.warning("Connect to Fabric in the sidebar to view event data.")
        return

    # Date range selector
    st.subheader("Event Date Range")
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

    st.info(f"Showing events from **{event_date_from}** to **{event_date_to}**")

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
                )

                df, sql = execute_event_query(spec)

                st.session_state.event_results = df
                st.session_state.event_sql = sql
                st.success(f"Loaded {len(df)} campaign-event records")

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
                    chart_df = chart_df.sort_values(chart_metric, ascending=False).head(20)

                    config = {
                        "x": "CampaignName",
                        "y": chart_metric,
                        "title": f"Top 20 Campaigns by {chart_metric}",
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
