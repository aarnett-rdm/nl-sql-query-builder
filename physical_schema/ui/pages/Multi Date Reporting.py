"""
Multi-Date Comparison Matrix

Allows comparing aggregate metrics across multiple date ranges side-by-side.
Replicates the key feature from the legacy Performer Profitability tool.
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

from tools.config import AppConfig  # noqa: E402
from tools.fabric_conn import FabricConnection  # noqa: E402
from tools.metric_resolver import MetricRegistry  # noqa: E402
from tools.spec_executor import execute_spec, normalize_spec  # noqa: E402
from ui.shared import format_results, init_fabric_state, render_fabric_sidebar, sanitize_filename, build_excel_bytes  # noqa: E402
from ui.viz_utils import create_chart  # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG_DIR = _PROJECT_ROOT / "current"
PHYSICAL_SCHEMA = CONFIG_DIR / "physical_schema.json"
METRIC_REGISTRY = CONFIG_DIR / "metric_registry.json"

# Platform options (only Google and Microsoft needed)
# Using internal platform keys that match metric registry
PLATFORMS = ["google_ads", "microsoft_ads"]
PLATFORM_LABELS = {"google_ads": "Google", "microsoft_ads": "Microsoft"}

# ---------------------------------------------------------------------------
# Session State
# ---------------------------------------------------------------------------

init_fabric_state()

if "comparison_results" not in st.session_state:
    st.session_state.comparison_results = None
if "comparison_queries" not in st.session_state:
    st.session_state.comparison_queries = []
if "campaign_details" not in st.session_state:
    st.session_state.campaign_details = None
if "detail_filters" not in st.session_state:
    st.session_state.detail_filters = []
if "metric_order" not in st.session_state:
    st.session_state.metric_order = []
if "show_summary_chart" not in st.session_state:
    st.session_state.show_summary_chart = True
if "show_campaign_chart" not in st.session_state:
    st.session_state.show_campaign_chart = True

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def load_metric_names() -> list[str]:
    """Load available metric names from the registry."""
    registry = MetricRegistry.from_path(METRIC_REGISTRY)
    return sorted(registry.metrics.keys())


def build_spec_for_range(
    platform: str,
    accounts: list[str],
    metrics: list[str],
    date_from: date,
    date_to: date,
    campaign_contains: str | None = None,
) -> dict:
    """
    Build a spec dict for a single date range.

    Args:
        platform: Platform filter (e.g., "google", "microsoft")
        accounts: List of account names to filter
        metrics: List of metric names to aggregate
        date_from: Start date (inclusive)
        date_to: End date (inclusive)
        campaign_contains: Optional campaign name filter (LIKE %value%)

    Returns:
        Spec dict ready for execute_spec()
    """
    # Build filters in the correct format for spec_executor
    where_filters = []

    # Add account filter if specified
    if accounts:
        where_filters.append({
            "field": "AccountName",
            "value": accounts[0]  # Single account
        })

    # Add campaign filter if specified
    if campaign_contains:
        where_filters.append({
            "field": "CampaignName",
            "op": "contains",
            "value": campaign_contains,
            "case_insensitive": True
        })

    spec = {
        "metrics": metrics,
        "platform": platform,
        "grain": "campaign_calendar",
        "dimensions": [],  # No dimensions = aggregate only
        "filters": {
            "date": {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
            },
            "where": where_filters
        },
    }

    return spec


def execute_comparison(
    platform: str,
    accounts: list[str],
    metrics: list[str],
    date_ranges: list[tuple[str, date, date]],
    campaign_contains: str | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Execute queries for all date ranges and return a summary matrix.

    Args:
        platform: Platform filter
        accounts: List of account names
        metrics: List of metric names
        date_ranges: List of (label, start_date, end_date) tuples
        campaign_contains: Optional campaign name filter

    Returns:
        Tuple of (DataFrame with date range labels as index and metrics as columns, list of SQL queries)
    """
    rows = []
    queries = []
    rate_keywords = ["rate", "percentage", "percent", "share", "roi"]

    for label, date_from, date_to in date_ranges:
        # Build spec for this date range
        spec = build_spec_for_range(platform, accounts, metrics, date_from, date_to, campaign_contains)

        # Normalize spec (adds default paths)
        spec = normalize_spec(spec)

        # Generate SQL
        sql = execute_spec(spec)
        queries.append(f"-- {label}\n{sql}")

        # Execute against Fabric
        fc: FabricConnection = st.session_state.fabric_conn
        df = fc.execute(sql)

        # Aggregate metrics (sum all rows)
        row = {"Date Range": label}
        for metric in metrics:
            # Find the metric column (case-insensitive match)
            metric_col = next((c for c in df.columns if c.lower() == metric.lower()), None)
            if metric_col and pd.api.types.is_numeric_dtype(df[metric_col]):
                row[metric] = df[metric_col].sum()
            else:
                row[metric] = 0

        rows.append(row)

    # Convert to DataFrame
    result_df = pd.DataFrame(rows).set_index("Date Range")

    # Recalculate rate/percentage metrics from base metrics
    for metric in metrics:
        metric_lower = metric.lower()

        # Skip if not a rate metric
        if not any(kw in metric_lower for kw in rate_keywords):
            continue

        # Conversion Rate = conversions / clicks
        if "conversion rate" in metric_lower or "conv rate" in metric_lower:
            if "conversions" in result_df.columns and "clicks" in result_df.columns:
                result_df[metric] = result_df["conversions"] / result_df["clicks"]
                result_df[metric] = result_df[metric].fillna(0)

        # Click Through Rate (CTR) = clicks / impressions
        elif "ctr" in metric_lower or "click through rate" in metric_lower:
            if "clicks" in result_df.columns and "impressions" in result_df.columns:
                result_df[metric] = result_df["clicks"] / result_df["impressions"]
                result_df[metric] = result_df[metric].fillna(0)

        # Cost Per Click = cost / clicks
        elif "cpc" in metric_lower or "cost per click" in metric_lower:
            if "cost" in result_df.columns and "clicks" in result_df.columns:
                result_df[metric] = result_df["cost"] / result_df["clicks"]
                result_df[metric] = result_df[metric].fillna(0)

        # ROI = (profit / cost) * 100
        elif "roi" in metric_lower:
            if "profit" in result_df.columns and "cost" in result_df.columns:
                result_df[metric] = (result_df["profit"] / result_df["cost"]) * 100
                result_df[metric] = result_df[metric].fillna(0)

        # Revenue Per Click = revenue / clicks
        elif "revenue per click" in metric_lower or "rpcl" in metric_lower:
            if "revenue" in result_df.columns and "clicks" in result_df.columns:
                result_df[metric] = result_df["revenue"] / result_df["clicks"]
                result_df[metric] = result_df[metric].fillna(0)

        # Revenue Per Conversion = revenue / conversions
        elif "revenue per conversion" in metric_lower or "rpc" in metric_lower:
            if "revenue" in result_df.columns and "conversions" in result_df.columns:
                result_df[metric] = result_df["revenue"] / result_df["conversions"]
                result_df[metric] = result_df[metric].fillna(0)

    return result_df, queries


def execute_campaign_details(
    platform: str,
    accounts: list[str],
    metrics: list[str],
    date_ranges: list[tuple[str, date, date]],
    campaign_contains: str | None = None,
) -> pd.DataFrame:
    """
    Execute queries for all date ranges with campaign-level detail.

    Args:
        platform: Platform filter
        accounts: List of account names
        metrics: List of metric names
        date_ranges: List of (label, start_date, end_date) tuples
        campaign_contains: Optional campaign name filter

    Returns:
        DataFrame with columns: Performer Name, Date Range, and all metrics
    """
    all_rows = []

    for label, date_from, date_to in date_ranges:
        # Build spec with CampaignName dimension
        spec = build_spec_for_range(platform, accounts, metrics, date_from, date_to, campaign_contains)
        spec["dimensions"] = ["CampaignName"]  # Add campaign dimension

        # Normalize spec (adds default paths)
        spec = normalize_spec(spec)

        # Generate SQL
        sql = execute_spec(spec)

        # Execute against Fabric
        fc: FabricConnection = st.session_state.fabric_conn
        df = fc.execute(sql)

        # Add Date Range column
        df.insert(0, "Date Range", label)

        # Rename CampaignName to Campaign Name
        if "CampaignName" in df.columns:
            df.rename(columns={"CampaignName": "Campaign Name"}, inplace=True)

        all_rows.append(df)

    # Combine all ranges
    if all_rows:
        combined = pd.concat(all_rows, ignore_index=True)

        # Recalculate derived metrics (rates/percentages) from base metrics
        # This ensures correct values even if DB has stale/incorrect calculated columns

        # Conversion Rate = conversions / clicks
        if "conversion rate" in combined.columns and "conversions" in combined.columns and "clicks" in combined.columns:
            combined["conversion rate"] = combined["conversions"] / combined["clicks"]
            combined["conversion rate"] = combined["conversion rate"].fillna(0)

        # Click Through Rate (CTR) = clicks / impressions
        if "click through rate" in combined.columns and "clicks" in combined.columns and "impressions" in combined.columns:
            combined["click through rate"] = combined["clicks"] / combined["impressions"]
            combined["click through rate"] = combined["click through rate"].fillna(0)

        # CTR might also be named as "ctr"
        if "ctr" in combined.columns and "clicks" in combined.columns and "impressions" in combined.columns:
            combined["ctr"] = combined["clicks"] / combined["impressions"]
            combined["ctr"] = combined["ctr"].fillna(0)

        # Cost Per Click = cost / clicks
        if "cost per click" in combined.columns and "cost" in combined.columns and "clicks" in combined.columns:
            combined["cost per click"] = combined["cost"] / combined["clicks"]
            combined["cost per click"] = combined["cost per click"].fillna(0)

        # CPC might also be named as such
        if "cpc" in combined.columns and "cost" in combined.columns and "clicks" in combined.columns:
            combined["cpc"] = combined["cost"] / combined["clicks"]
            combined["cpc"] = combined["cpc"].fillna(0)

        # ROI = (profit / cost) * 100
        if "roi" in combined.columns and "profit" in combined.columns and "cost" in combined.columns:
            combined["roi"] = (combined["profit"] / combined["cost"]) * 100
            combined["roi"] = combined["roi"].fillna(0)

        # Revenue Per Click = revenue / clicks
        if "revenue per click" in combined.columns and "revenue" in combined.columns and "clicks" in combined.columns:
            combined["revenue per click"] = combined["revenue"] / combined["clicks"]
            combined["revenue per click"] = combined["revenue per click"].fillna(0)

        # Revenue Per Conversion = revenue / conversions
        if "revenue per conversion" in combined.columns and "revenue" in combined.columns and "conversions" in combined.columns:
            combined["revenue per conversion"] = combined["revenue"] / combined["conversions"]
            combined["revenue per conversion"] = combined["revenue per conversion"].fillna(0)

        # Sort by Campaign Name, then Date Range
        combined = combined.sort_values(["Campaign Name", "Date Range"])

        return combined

    return pd.DataFrame()


def apply_filters(df: pd.DataFrame, filters: list[dict]) -> pd.DataFrame:
    """
    Apply filter conditions to a DataFrame.

    Args:
        df: DataFrame to filter
        filters: List of filter dicts with keys: column, operator, value, logic (and/or)

    Returns:
        Filtered DataFrame
    """
    if not filters or df.empty:
        return df

    mask = pd.Series([True] * len(df))

    for i, f in enumerate(filters):
        col = f.get("column")
        op = f.get("operator")
        value = f.get("value")
        logic = f.get("logic", "and")  # Default to AND

        if not col or col not in df.columns or value is None or value == "":
            continue

        # Create condition based on operator
        try:
            if pd.api.types.is_numeric_dtype(df[col]):
                # Numeric comparisons
                value = float(value)
                if op == "=":
                    condition = df[col] == value
                elif op == ">":
                    condition = df[col] > value
                elif op == "<":
                    condition = df[col] < value
                elif op == ">=":
                    condition = df[col] >= value
                elif op == "<=":
                    condition = df[col] <= value
                elif op == "!=":
                    condition = df[col] != value
                else:
                    continue
            else:
                # String comparisons
                if op == "contains":
                    condition = df[col].astype(str).str.contains(str(value), case=False, na=False)
                elif op == "equals":
                    condition = df[col].astype(str).str.lower() == str(value).lower()
                elif op == "starts_with":
                    condition = df[col].astype(str).str.startswith(str(value), na=False)
                elif op == "ends_with":
                    condition = df[col].astype(str).str.endswith(str(value), na=False)
                else:
                    continue

            # Combine with previous conditions using AND/OR logic
            if i == 0:
                mask = condition
            else:
                if logic == "or":
                    mask = mask | condition
                else:  # and
                    mask = mask & condition

        except (ValueError, TypeError):
            # Skip invalid filters
            continue

    return df[mask]


def create_summary_matrix(campaign_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create an aggregate summary matrix from campaign-level details.

    Args:
        campaign_df: DataFrame with Campaign Name, Date Range, and metric columns

    Returns:
        DataFrame with Date Range as index and aggregated metrics as columns
    """
    if campaign_df.empty:
        return pd.DataFrame()

    # Get metric columns (exclude Campaign Name and Date Range)
    metric_cols = [col for col in campaign_df.columns if col not in ["Campaign Name", "Date Range"]]

    # Identify rate/percentage columns that shouldn't be summed
    rate_keywords = ["rate", "percentage", "percent", "share", "roi"]
    rate_cols = [col for col in metric_cols if any(kw in col.lower() for kw in rate_keywords)]
    sum_cols = [col for col in metric_cols if col not in rate_cols]

    # Group by Date Range and sum non-rate metrics
    summary = campaign_df.groupby("Date Range")[sum_cols].sum()

    # Recalculate rate metrics from base metrics
    for rate_col in rate_cols:
        rate_lower = rate_col.lower()

        # Conversion Rate = conversions / clicks
        if "conversion rate" in rate_lower or "conv rate" in rate_lower:
            if "conversions" in summary.columns and "clicks" in summary.columns:
                summary[rate_col] = summary["conversions"] / summary["clicks"]
                summary[rate_col] = summary[rate_col].fillna(0)

        # Click Through Rate (CTR) = clicks / impressions
        elif "ctr" in rate_lower or "click through rate" in rate_lower:
            if "clicks" in summary.columns and "impressions" in summary.columns:
                summary[rate_col] = summary["clicks"] / summary["impressions"]
                summary[rate_col] = summary[rate_col].fillna(0)

        # ROI = (profit / cost) * 100
        elif "roi" in rate_lower:
            if "profit" in summary.columns and "cost" in summary.columns:
                summary[rate_col] = (summary["profit"] / summary["cost"]) * 100
                summary[rate_col] = summary[rate_col].fillna(0)

        else:
            # For other rate columns, use weighted average (may not be accurate for all cases)
            summary[rate_col] = campaign_df.groupby("Date Range")[rate_col].mean()

    return summary


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------


def main():
    st.set_page_config(
        page_title="Multi Date Reporting",
        page_icon="📊",
        layout="wide",
    )

    # Sidebar
    with st.sidebar:
        st.title("Multi-Date Comparison")
        st.divider()
        render_fabric_sidebar()
        st.divider()
        if st.button("Clear results", use_container_width=True):
            st.session_state.comparison_results = None
            st.session_state.comparison_queries = []
            st.session_state.campaign_details = None
            st.session_state.detail_filters = []
            st.session_state.metric_order = []
            st.rerun()

    # Main content
    st.title("Multi-Date Comparison Matrix")
    st.markdown(
        "Compare aggregate metrics across multiple date ranges. "
        "Select platform, accounts, metrics, and date ranges below."
    )

    # Check Fabric connection
    if not st.session_state.fabric_connected:
        st.warning("Connect to Fabric in the sidebar to run comparisons.")
        return

    # Load available metrics
    try:
        available_metrics = load_metric_names()
    except Exception as e:
        st.error(f"Failed to load metrics: {e}")
        return

    # Metric selection and reordering (outside form)
    with st.expander("⚙️ Select & Reorder Metrics", expanded=True):
        # Default to common metrics that exist in registry
        default_metrics = [m for m in ["impressions", "clicks", "cost", "revenue", "profit"]
                         if m in available_metrics]

        # Initialize metric order if empty
        if not st.session_state.metric_order:
            st.session_state.metric_order = default_metrics.copy()

        selected_metrics = st.multiselect(
            "Select Metrics",
            options=available_metrics,
            default=st.session_state.metric_order if st.session_state.metric_order else default_metrics,
            help="Choose which metrics to compare across date ranges",
            key="metric_selector"
        )

        # Update metric order when selection changes
        # Add newly selected metrics to the end
        for m in selected_metrics:
            if m not in st.session_state.metric_order:
                st.session_state.metric_order.append(m)
        # Remove deselected metrics
        st.session_state.metric_order = [m for m in st.session_state.metric_order if m in selected_metrics]

        # Always show the reorder section to prevent UI shifting
        st.markdown("**Reorder Metrics** (drag to change column order)")

        if selected_metrics and len(selected_metrics) > 1:
            # Show reorder UI only if 2+ metrics selected
            for idx, metric in enumerate(st.session_state.metric_order):
                cols = st.columns([4, 1, 1])

                with cols[0]:
                    st.text(metric)

                with cols[1]:
                    if idx > 0:  # Not first item
                        if st.button("↑", key=f"up_{metric}", help="Move up"):
                            # Swap with previous
                            st.session_state.metric_order[idx], st.session_state.metric_order[idx-1] = \
                                st.session_state.metric_order[idx-1], st.session_state.metric_order[idx]
                            st.rerun()

                with cols[2]:
                    if idx < len(st.session_state.metric_order) - 1:  # Not last item
                        if st.button("↓", key=f"down_{metric}", help="Move down"):
                            # Swap with next
                            st.session_state.metric_order[idx], st.session_state.metric_order[idx+1] = \
                                st.session_state.metric_order[idx+1], st.session_state.metric_order[idx]
                            st.rerun()
        elif selected_metrics and len(selected_metrics) == 1:
            st.caption("Select 2 or more metrics to enable reordering")
        else:
            st.caption("No metrics selected")

        # Use the ordered metrics for the comparison
        selected_metrics = st.session_state.metric_order.copy()

    # Input form (collapsible)
    with st.expander("⚙️ Filters & Date Ranges", expanded=True):
        with st.form("comparison_form"):
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Filters")

                platform = st.selectbox(
                    "Platform",
                    options=PLATFORMS,
                    format_func=lambda x: PLATFORM_LABELS.get(x, x),
                    index=0,
                    help="Select Google or Microsoft platform"
                )

                accounts = st.text_input(
                    "Account Name",
                    placeholder="e.g., Go-Performer-Sports",
                    help="Enter account name (leave blank for all accounts)",
                )
                account_list = [accounts.strip()] if accounts.strip() else []

                campaign_contains = st.text_input(
                    "Campaign Contains",
                    placeholder="e.g., Brand, Performance",
                    help="Filter campaigns by name (leave blank for all campaigns)",
                )

            with col2:
                st.subheader("Selected Metrics")
                if selected_metrics:
                    st.write(", ".join(selected_metrics))
                else:
                    st.info("No metrics selected. Please select metrics above.")

            st.subheader("Date Ranges")
            st.markdown("Define 2-5 date ranges to compare. Each range will appear as a row in the matrix.")

            # Date range inputs (3 by default, expandable to 5)
            num_ranges = st.slider("Number of date ranges", min_value=2, max_value=5, value=3)

            date_ranges = []
            today = date.today()

            for i in range(num_ranges):
                st.markdown(f"**Range {i + 1}**")
                col_label, col_start, col_end = st.columns([2, 1, 1])

                with col_label:
                    label = st.text_input(
                        "Label",
                        value=f"Range {i + 1}",
                        key=f"label_{i}",
                        label_visibility="collapsed",
                    )

                with col_start:
                    # Default: 7-day periods going backward
                    default_start = today - timedelta(days=(i + 1) * 7)
                    start = st.date_input(
                        "Start",
                        value=default_start,
                        key=f"start_{i}",
                        label_visibility="collapsed",
                    )

                with col_end:
                    default_end = today - timedelta(days=i * 7 + 1)
                    end = st.date_input(
                        "End",
                        value=default_end,
                        key=f"end_{i}",
                        label_visibility="collapsed",
                    )

                date_ranges.append((label, start, end))

            submitted = st.form_submit_button("Run Comparison", type="primary", use_container_width=True)

    # Execute comparison on submit
    if submitted:
        if not selected_metrics:
            st.error("Please select at least one metric.")
            return

        # Validate date ranges
        for label, start, end in date_ranges:
            if start > end:
                st.error(f"Invalid date range '{label}': start date must be before or equal to end date.")
                return

        with st.spinner("Running comparison queries..."):
            try:
                campaign_filter = campaign_contains.strip() if campaign_contains else None

                # Execute aggregate comparison
                matrix, queries = execute_comparison(
                    platform, account_list, selected_metrics, date_ranges, campaign_filter
                )
                st.session_state.comparison_results = matrix
                st.session_state.comparison_queries = queries

                # Execute campaign-level details
                campaign_details = execute_campaign_details(
                    platform, account_list, selected_metrics, date_ranges, campaign_filter
                )
                st.session_state.campaign_details = campaign_details

                st.success(f"Comparison complete! Analyzed {len(date_ranges)} date ranges.")
            except RuntimeError as e:
                # Connection lost
                st.session_state.fabric_connected = False
                st.error(f"Connection lost: {e}")
            except Exception as e:
                st.error(f"Comparison failed: {e}")
                st.exception(e)

    # Display results
    if st.session_state.comparison_results is not None:
        st.divider()

        # Show SQL queries
        if st.session_state.comparison_queries:
            with st.expander("View Generated SQL Queries", expanded=False):
                combined_sql = "\n\n".join(st.session_state.comparison_queries)
                st.text_area(
                    "SQL",
                    value=combined_sql,
                    height=min(400, max(200, combined_sql.count("\n") * 20)),
                    label_visibility="collapsed",
                )

        st.subheader("Comparison Matrix")

        matrix = st.session_state.comparison_results

        # Toggle button for chart/table view
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.session_state.show_summary_chart:
                if st.button("📊 Hide Chart", key="hide_summary_chart", use_container_width=True):
                    st.session_state.show_summary_chart = False
                    st.rerun()
            else:
                if st.button("📊 Show Chart", key="show_summary_chart_btn", use_container_width=True):
                    st.session_state.show_summary_chart = True
                    st.rerun()

        # Display chart if enabled
        if st.session_state.show_summary_chart:
            try:
                # Create a grouped bar chart comparing metrics across date ranges
                # Prepare data: date ranges on x-axis, metrics as separate bars
                chart_config = {
                    "x_col": matrix.index.name or "Date Range",
                    "y_cols": list(matrix.columns)
                }

                # Reset index to make Date Range a column
                chart_df = matrix.reset_index()

                fig = create_chart(chart_df, "grouped_bar", chart_config)
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Chart generation failed: {e}")

        # Display formatted matrix
        st.dataframe(
            format_results(matrix),
            use_container_width=True,
            height=min(600, (len(matrix) + 1) * 35 + 38),
        )

        st.caption(f"Showing {len(matrix)} date ranges × {len(matrix.columns)} metrics")

        # Download buttons — summary matrix
        _today = date.today().strftime("%Y%m%d")
        _dl_col1, _dl_col2, _ = st.columns([1, 1, 4])
        with _dl_col1:
            st.download_button(
                "⬇ CSV",
                data=matrix.reset_index().to_csv(index=False),
                file_name=f"mdr_summary_{_today}.csv",
                mime="text/csv",
                key="mdr_summary_csv",
                use_container_width=True,
            )
        with _dl_col2:
            st.download_button(
                "⬇ Excel",
                data=build_excel_bytes({"Summary Matrix": matrix}),
                file_name=f"mdr_comparison_{_today}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="mdr_summary_xlsx",
                use_container_width=True,
            )

        # Campaign-level details
        if st.session_state.campaign_details is not None and not st.session_state.campaign_details.empty:
            st.divider()
            st.subheader("Campaign-Level Details")

            campaign_df = st.session_state.campaign_details

            # Chart toggle and visualization
            col1, col2, col3 = st.columns([1, 2, 2])
            with col1:
                if st.session_state.show_campaign_chart:
                    if st.button("📊 Hide Chart", key="hide_campaign_chart", use_container_width=True):
                        st.session_state.show_campaign_chart = False
                        st.rerun()
                else:
                    if st.button("📊 Show Chart", key="show_campaign_chart_btn", use_container_width=True):
                        st.session_state.show_campaign_chart = True
                        st.rerun()

            with col2:
                # Metric selector for chart
                chart_metric = st.selectbox(
                    "Chart Metric",
                    options=[col for col in campaign_df.columns if col not in ["Campaign Name", "Date Range"]],
                    index=0,
                    key="campaign_chart_metric",
                    help="Select which metric to visualize"
                )

            with col3:
                # Top N selector
                top_n = st.slider(
                    "Top Campaigns",
                    min_value=5,
                    max_value=20,
                    value=10,
                    step=5,
                    key="campaign_chart_top_n",
                    help="Number of top campaigns to display"
                )

            # Display chart if enabled
            if st.session_state.show_campaign_chart and chart_metric:
                try:
                    # Get top N campaigns by selected metric (sum across all date ranges)
                    top_campaigns = (
                        campaign_df.groupby("Campaign Name")[chart_metric]
                        .sum()
                        .nlargest(top_n)
                        .reset_index()
                    )

                    chart_config = {
                        "x_col": chart_metric,
                        "y_col": "Campaign Name",
                        "sort_by": chart_metric,
                        "limit": top_n
                    }

                    fig = create_chart(top_campaigns, "horizontal_bar", chart_config)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Chart generation failed: {e}")

            # Filter UI
            with st.expander("🔍 Filter Campaign Details", expanded=False):
                col1, col2 = st.columns([3, 1])

                with col1:
                    st.markdown("**Add filters to narrow down campaign results**")

                with col2:
                    if st.button("Clear All Filters", use_container_width=True):
                        st.session_state.detail_filters = []
                        st.rerun()

                # Get available columns
                available_cols = list(campaign_df.columns)

                # Display existing filters and add new filter button
                for idx, filter_config in enumerate(st.session_state.detail_filters):
                    st.divider()
                    cols = st.columns([2, 2, 3, 1, 1])

                    with cols[0]:
                        # Logic selector (AND/OR) - skip for first filter
                        if idx == 0:
                            st.markdown("**Filter 1**")
                            filter_config["logic"] = "and"
                        else:
                            logic = st.selectbox(
                                "Logic",
                                options=["and", "or"],
                                index=0 if filter_config.get("logic", "and") == "and" else 1,
                                key=f"logic_{idx}",
                                help="Combine with previous filter using AND or OR"
                            )
                            filter_config["logic"] = logic

                    with cols[1]:
                        # Column selector
                        current_col = filter_config.get("column", available_cols[0])
                        col_idx = available_cols.index(current_col) if current_col in available_cols else 0
                        column = st.selectbox(
                            "Column",
                            options=available_cols,
                            index=col_idx,
                            key=f"col_{idx}"
                        )
                        filter_config["column"] = column

                    with cols[2]:
                        # Operator selector (depends on column type)
                        is_numeric = pd.api.types.is_numeric_dtype(campaign_df[column])

                        if is_numeric:
                            operators = ["=", ">", "<", ">=", "<=", "!="]
                            default_op = "="
                        else:
                            operators = ["contains", "equals", "starts_with", "ends_with"]
                            default_op = "contains"

                        current_op = filter_config.get("operator", default_op)
                        op_idx = operators.index(current_op) if current_op in operators else 0
                        operator = st.selectbox(
                            "Operator",
                            options=operators,
                            index=op_idx,
                            key=f"op_{idx}"
                        )
                        filter_config["operator"] = operator

                    with cols[3]:
                        # Value input
                        current_value = filter_config.get("value", "")
                        value = st.text_input(
                            "Value",
                            value=str(current_value),
                            key=f"val_{idx}",
                            label_visibility="collapsed",
                            placeholder="Enter value..."
                        )
                        filter_config["value"] = value

                    with cols[4]:
                        # Remove filter button
                        if st.button("❌", key=f"remove_{idx}", help="Remove this filter"):
                            st.session_state.detail_filters.pop(idx)
                            st.rerun()

                # Add new filter button
                if st.button("➕ Add Filter", use_container_width=True):
                    st.session_state.detail_filters.append({
                        "column": available_cols[0],
                        "operator": "contains",
                        "value": "",
                        "logic": "and"
                    })
                    st.rerun()

            # Apply filters
            filtered_df = apply_filters(campaign_df, st.session_state.detail_filters)

            # Show filtered summary matrix if filters are active
            if len(st.session_state.detail_filters) > 0 and not filtered_df.empty:
                st.subheader("Filtered Summary Matrix")
                st.markdown("*Aggregated metrics for filtered campaigns only*")

                filtered_summary = create_summary_matrix(filtered_df)

                st.dataframe(
                    format_results(filtered_summary),
                    use_container_width=True,
                    height=min(400, (len(filtered_summary) + 1) * 35 + 38),
                )

                st.caption(
                    f"Summary of {filtered_df['Campaign Name'].nunique()} filtered campaigns "
                    f"across {filtered_df['Date Range'].nunique()} date ranges"
                )

                st.divider()

            # Display formatted campaign details (without row index)
            st.dataframe(
                format_results(filtered_df),
                use_container_width=True,
                height=min(800, (len(filtered_df) + 1) * 35 + 38),
                hide_index=True,
            )

            num_date_ranges = filtered_df['Date Range'].nunique()
            num_campaigns = filtered_df['Campaign Name'].nunique()

            if len(st.session_state.detail_filters) > 0:
                st.caption(
                    f"Showing {len(filtered_df)} of {len(campaign_df)} campaign-date combinations "
                    f"({num_campaigns} campaigns × {num_date_ranges} date ranges) "
                    f"- {len(st.session_state.detail_filters)} filter(s) active"
                )
            else:
                st.caption(
                    f"Showing {len(filtered_df)} campaign-date combinations "
                    f"({num_campaigns} unique campaigns × {num_date_ranges} date ranges)"
                )

            # Download buttons — campaign details + multi-sheet Excel
            st.markdown("**Export**")
            _dl1, _dl2, _dl3, _ = st.columns([1, 1, 1, 3])
            with _dl1:
                st.download_button(
                    "⬇ CSV (details)",
                    data=filtered_df.to_csv(index=False),
                    file_name=f"mdr_campaign_details_{_today}.csv",
                    mime="text/csv",
                    key="mdr_detail_csv",
                    use_container_width=True,
                )
            with _dl2:
                # Multi-sheet Excel: Summary + Details (+ Filtered Summary if active)
                _sheets: dict = {"Summary Matrix": matrix, "Campaign Details": filtered_df}
                if len(st.session_state.detail_filters) > 0 and not filtered_df.empty:
                    _filtered_summary = create_summary_matrix(filtered_df)
                    _sheets["Filtered Summary"] = _filtered_summary
                st.download_button(
                    "⬇ Excel (all sheets)",
                    data=build_excel_bytes(_sheets),
                    file_name=f"mdr_comparison_{_today}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="mdr_full_xlsx",
                    use_container_width=True,
                )
            with _dl3:
                st.download_button(
                    "⬇ CSV (summary)",
                    data=matrix.reset_index().to_csv(index=False),
                    file_name=f"mdr_summary_{_today}.csv",
                    mime="text/csv",
                    key="mdr_summary_csv_detail",
                    use_container_width=True,
                )


if __name__ == "__main__":
    main()
