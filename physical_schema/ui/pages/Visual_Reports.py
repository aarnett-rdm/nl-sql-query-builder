"""
Visual Reports - Custom Chart Builder

Allows users to build custom visualizations by selecting:
- Platform (Google/Microsoft/Both)
- Metrics (multiple)
- Dimensions (campaign, account, date, etc.)
- Date range
- Chart type (line, bar, area, stacked bar, pie)
- Filters

Users can save chart configurations and export charts as images.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
import json
from typing import Optional

import pandas as pd
import streamlit as st

# Ensure tools/ is importable
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from tools.fabric_conn import FabricConnection  # noqa: E402
from tools.metric_resolver import MetricRegistry  # noqa: E402
from tools.spec_executor import execute_spec, normalize_spec  # noqa: E402
from ui.shared import format_results, init_fabric_state, render_fabric_sidebar  # noqa: E402
from ui.viz_utils import create_chart  # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG_DIR = _PROJECT_ROOT / "current"
METRIC_REGISTRY = CONFIG_DIR / "metric_registry.json"

PLATFORMS = ["google_ads", "microsoft_ads"]
PLATFORM_LABELS = {"google_ads": "Google Ads", "microsoft_ads": "Microsoft Ads"}

# Available dimensions for grouping
DIMENSIONS = [
    "None (Aggregate)",
    "Date",
    "CampaignName",
    "AccountName",
    "DeviceType",
    "Network",
]

DIMENSION_MAP = {
    "None (Aggregate)": None,
    "Date": "date",
    "CampaignName": "CampaignName",
    "AccountName": "AccountName",
    "DeviceType": "DeviceType",
    "Network": "Network",
}

# Chart types
CHART_TYPES = {
    "Line Chart": "line",
    "Bar Chart": "bar",
    "Horizontal Bar": "horizontal_bar",
    "Grouped Bar": "grouped_bar",
    "Area Chart": "area",
}

# ---------------------------------------------------------------------------
# Session State
# ---------------------------------------------------------------------------

init_fabric_state()

if "chart_config" not in st.session_state:
    st.session_state.chart_config = None
if "chart_data" not in st.session_state:
    st.session_state.chart_data = None
if "chart_sql" not in st.session_state:
    st.session_state.chart_sql = None
if "saved_charts" not in st.session_state:
    st.session_state.saved_charts = []

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def load_metric_names() -> list[str]:
    """Load available metric names from the registry."""
    registry = MetricRegistry.from_path(METRIC_REGISTRY)
    return sorted(registry.metrics.keys())


def build_chart_spec(
    platform: str,
    metrics: list[str],
    dimension: Optional[str],
    date_from: date,
    date_to: date,
    account_filter: str = "",
    campaign_filter: str = "",
) -> dict:
    """Build a spec for the custom chart query."""
    where_filters = []

    if account_filter:
        where_filters.append({
            "field": "AccountName",
            "value": account_filter
        })

    if campaign_filter:
        where_filters.append({
            "field": "CampaignName",
            "op": "contains",
            "value": campaign_filter,
            "case_insensitive": True
        })

    spec = {
        "metrics": metrics,
        "platform": platform,
        "grain": "campaign_calendar",
        "dimensions": [dimension] if dimension and dimension != "date" else [],
        "filters": {
            "date": {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
            },
            "where": where_filters
        },
    }

    # Special handling for date dimension
    if dimension == "date":
        spec["dimensions"] = ["CalendarDate"]

    return spec


def execute_chart_query(spec: dict) -> tuple[pd.DataFrame, str]:
    """Execute the chart query and return results + SQL."""
    spec = normalize_spec(spec)
    sql = execute_spec(spec)

    fc: FabricConnection = st.session_state.fabric_conn
    df = fc.execute(sql)

    return df, sql


def determine_chart_type_config(
    df: pd.DataFrame,
    chart_type: str,
    metrics: list[str],
    dimension: Optional[str]
) -> tuple[str, dict]:
    """
    Determine the appropriate chart type and configuration based on data.

    Returns:
        Tuple of (chart_type_code, config_dict)
    """
    # Map user-friendly chart type to internal chart type
    chart_type_code = CHART_TYPES.get(chart_type, "bar")

    config = {}

    # Identify columns
    dimension_col = None
    if dimension == "date":
        dimension_col = next((c for c in df.columns if "date" in c.lower()), None)
    elif dimension:
        dimension_col = dimension

    metric_cols = [col for col in df.columns if col.lower() in [m.lower() for m in metrics]]

    # Configure based on chart type
    if chart_type_code in ["line", "area"]:
        if dimension_col:
            if len(metric_cols) == 1:
                config = {
                    "x_col": dimension_col,
                    "y_col": metric_cols[0],
                    "sort_by": dimension_col
                }
            else:
                config = {
                    "x_col": dimension_col,
                    "y_cols": metric_cols,
                    "sort_by": dimension_col
                }
                chart_type_code = "multi_line"  # Use multi-line for multiple metrics
    elif chart_type_code == "bar":
        if dimension_col and metric_cols:
            config = {
                "x_col": dimension_col,
                "y_col": metric_cols[0],
                "sort_by": metric_cols[0]
            }
    elif chart_type_code == "horizontal_bar":
        if dimension_col and metric_cols:
            config = {
                "x_col": metric_cols[0],
                "y_col": dimension_col,
                "sort_by": metric_cols[0],
                "limit": min(20, len(df))
            }
    elif chart_type_code == "grouped_bar":
        if dimension_col and len(metric_cols) > 1:
            config = {
                "x_col": dimension_col,
                "y_cols": metric_cols
            }
        elif dimension_col and len(metric_cols) == 1:
            # Fall back to regular bar for single metric
            chart_type_code = "bar"
            config = {
                "x_col": dimension_col,
                "y_col": metric_cols[0]
            }

    return chart_type_code, config


def save_chart_config(name: str, config: dict):
    """Save a chart configuration for later reuse."""
    saved = {
        "name": name,
        "config": config
    }
    st.session_state.saved_charts.append(saved)
    st.success(f"✅ Chart '{name}' saved!")


def load_chart_config(name: str) -> Optional[dict]:
    """Load a saved chart configuration."""
    for saved in st.session_state.saved_charts:
        if saved["name"] == name:
            return saved["config"]
    return None


# ---------------------------------------------------------------------------
# Chart Templates
# ---------------------------------------------------------------------------

TEMPLATES = {
    "Campaign Performance (Last 30 Days)": {
        "metrics": ["impressions", "clicks", "cost", "conversions"],
        "dimension": "CampaignName",
        "chart_type": "Horizontal Bar",
        "days_back": 30,
    },
    "Daily Trend (Last 7 Days)": {
        "metrics": ["clicks", "conversions", "cost"],
        "dimension": "Date",
        "chart_type": "Line Chart",
        "days_back": 7,
    },
    "Platform Comparison": {
        "metrics": ["impressions", "clicks", "cost", "revenue"],
        "dimension": "None (Aggregate)",
        "chart_type": "Bar Chart",
        "days_back": 30,
    },
}


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------


def main():
    st.set_page_config(
        page_title="Visual Reports",
        page_icon="📊",
        layout="wide",
    )

    # Sidebar
    with st.sidebar:
        st.title("Visual Reports")
        st.divider()
        render_fabric_sidebar()
        st.divider()

        # Chart templates
        st.subheader("Templates")
        template_names = ["Custom"] + list(TEMPLATES.keys())
        selected_template = st.selectbox(
            "Load Template",
            options=template_names,
            index=0,
            help="Start with a pre-built chart template"
        )

        if selected_template != "Custom":
            template = TEMPLATES[selected_template]
            st.session_state.template_loaded = template
        else:
            st.session_state.template_loaded = None

        st.divider()

        # Saved charts
        if st.session_state.saved_charts:
            st.subheader("Saved Charts")
            saved_names = [s["name"] for s in st.session_state.saved_charts]
            load_saved = st.selectbox(
                "Load Saved",
                options=[""] + saved_names,
                help="Load a previously saved chart configuration"
            )
            if load_saved:
                config = load_chart_config(load_saved)
                if config:
                    st.session_state.loaded_config = config
                    st.success(f"Loaded '{load_saved}'")

        st.divider()

        if st.button("Clear Chart", use_container_width=True):
            st.session_state.chart_config = None
            st.session_state.chart_data = None
            st.session_state.chart_sql = None
            st.rerun()

    # Main content
    st.title("📊 Visual Reports - Chart Builder")
    st.markdown(
        "Build custom visualizations by selecting metrics, dimensions, and chart types. "
        "Use templates to get started quickly."
    )

    # Check Fabric connection
    if not st.session_state.fabric_connected:
        st.warning("⚠️ Connect to Fabric in the sidebar to build charts.")
        return

    # Load available metrics
    try:
        available_metrics = load_metric_names()
    except Exception as e:
        st.error(f"Failed to load metrics: {e}")
        return

    # Apply template if selected
    template = st.session_state.get("template_loaded")
    default_metrics = template["metrics"] if template else ["impressions", "clicks", "cost"]
    default_dimension = template["dimension"] if template else "None (Aggregate)"
    default_chart_type = template["chart_type"] if template else "Bar Chart"
    days_back = template.get("days_back", 7) if template else 7

    # Chart builder form
    with st.form("chart_builder"):
        st.subheader("📐 Chart Configuration")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Data Selection**")

            platform = st.selectbox(
                "Platform",
                options=PLATFORMS,
                format_func=lambda x: PLATFORM_LABELS.get(x, x),
                index=0,
                help="Select which platform to query"
            )

            selected_metrics = st.multiselect(
                "Metrics",
                options=available_metrics,
                default=[m for m in default_metrics if m in available_metrics],
                help="Select 1-5 metrics to visualize"
            )

            dimension = st.selectbox(
                "Group By",
                options=DIMENSIONS,
                index=DIMENSIONS.index(default_dimension) if default_dimension in DIMENSIONS else 0,
                help="Dimension to group data by (or None for aggregate)"
            )

            chart_type = st.selectbox(
                "Chart Type",
                options=list(CHART_TYPES.keys()),
                index=list(CHART_TYPES.keys()).index(default_chart_type) if default_chart_type in CHART_TYPES else 0,
                help="Type of chart to display"
            )

        with col2:
            st.markdown("**Filters & Date Range**")

            # Date range
            today = date.today()
            default_start = today - timedelta(days=days_back)
            default_end = today - timedelta(days=1)

            date_from = st.date_input(
                "Start Date",
                value=default_start,
                help="Query start date (inclusive)"
            )

            date_to = st.date_input(
                "End Date",
                value=default_end,
                help="Query end date (inclusive)"
            )

            # Filters
            account_filter = st.text_input(
                "Account Name",
                placeholder="e.g., Go-Performer-Sports",
                help="Filter by specific account (leave blank for all)"
            )

            campaign_filter = st.text_input(
                "Campaign Contains",
                placeholder="e.g., Brand, Performance",
                help="Filter campaigns by name (leave blank for all)"
            )

        # Submit button
        col_submit, col_save = st.columns([3, 1])
        with col_submit:
            submitted = st.form_submit_button(
                "🔍 Build Chart",
                type="primary",
                use_container_width=True
            )
        with col_save:
            save_chart = st.form_submit_button(
                "💾 Save",
                use_container_width=True
            )

    # Save chart configuration
    if save_chart:
        if not selected_metrics:
            st.error("Select at least one metric before saving.")
        else:
            save_name = st.text_input(
                "Chart Name",
                placeholder="e.g., Weekly Performance",
                key="save_name_input"
            )
            if save_name:
                config = {
                    "platform": platform,
                    "metrics": selected_metrics,
                    "dimension": dimension,
                    "chart_type": chart_type,
                    "date_from": date_from.isoformat(),
                    "date_to": date_to.isoformat(),
                    "account_filter": account_filter,
                    "campaign_filter": campaign_filter,
                }
                save_chart_config(save_name, config)

    # Build chart on submit
    if submitted:
        if not selected_metrics:
            st.error("❌ Please select at least one metric.")
            return

        if date_from > date_to:
            st.error("❌ Start date must be before or equal to end date.")
            return

        dimension_value = DIMENSION_MAP.get(dimension)

        with st.spinner("🔨 Building chart..."):
            try:
                # Build spec
                spec = build_chart_spec(
                    platform=platform,
                    metrics=selected_metrics,
                    dimension=dimension_value,
                    date_from=date_from,
                    date_to=date_to,
                    account_filter=account_filter.strip(),
                    campaign_filter=campaign_filter.strip(),
                )

                # Execute query
                df, sql = execute_chart_query(spec)

                # Store results
                st.session_state.chart_config = {
                    "platform": platform,
                    "metrics": selected_metrics,
                    "dimension": dimension_value,
                    "chart_type": chart_type,
                }
                st.session_state.chart_data = df
                st.session_state.chart_sql = sql

                st.success(f"✅ Chart built successfully! {len(df):,} rows returned.")

            except RuntimeError as e:
                # Connection lost
                st.session_state.fabric_connected = False
                st.error(f"❌ Connection lost: {e}")
            except Exception as e:
                st.error(f"❌ Chart build failed: {e}")
                st.exception(e)

    # Display chart if we have data
    if st.session_state.chart_data is not None:
        st.divider()

        df = st.session_state.chart_data
        config = st.session_state.chart_config

        # Show SQL
        with st.expander("📜 View Generated SQL", expanded=False):
            st.code(st.session_state.chart_sql, language="sql")

        # Display chart
        st.subheader("📊 Chart")

        if df.empty:
            st.warning("No data returned for this query. Try adjusting your filters or date range.")
        else:
            try:
                # Determine chart type and config
                chart_type_code, chart_config = determine_chart_type_config(
                    df,
                    config["chart_type"],
                    config["metrics"],
                    config["dimension"]
                )

                # Create and display chart
                fig = create_chart(df, chart_type_code, chart_config)
                st.plotly_chart(fig, use_container_width=True)

                # Export options
                col1, col2, col3 = st.columns([1, 1, 3])
                with col1:
                    # Export as HTML (Plotly interactive)
                    html_buffer = fig.to_html(include_plotlyjs='cdn')
                    st.download_button(
                        label="📥 Download HTML",
                        data=html_buffer,
                        file_name="chart.html",
                        mime="text/html",
                        use_container_width=True
                    )

                with col2:
                    # Export as PNG (static image)
                    try:
                        img_bytes = fig.to_image(format="png", width=1200, height=600)
                        st.download_button(
                            label="📥 Download PNG",
                            data=img_bytes,
                            file_name="chart.png",
                            mime="image/png",
                            use_container_width=True
                        )
                    except Exception as e:
                        st.caption("⚠️ PNG export requires kaleido")

            except Exception as e:
                st.error(f"Chart rendering failed: {e}")
                st.exception(e)

        # Show data table
        st.subheader("📋 Data Table")
        st.dataframe(
            format_results(df),
            use_container_width=True,
            hide_index=True,
            height=min(600, (len(df) + 1) * 35 + 38)
        )
        st.caption(f"Showing {len(df):,} rows × {len(df.columns)} columns")


if __name__ == "__main__":
    main()
