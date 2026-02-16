"""
Schema Explorer & Metric Catalog

Interactive catalog showing all available metrics, dimensions, and platform/grain support.
Helps users discover what's queryable without digging through JSON configs.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Ensure tools/ is importable (same pattern as multi_date.py)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from tools.dimension_extractor import DimensionExtractor  # noqa: E402
from tools.fabric_conn import FabricConnection  # noqa: E402
from tools.metric_resolver import MetricRegistry  # noqa: E402
from ui.shared import init_fabric_state, render_fabric_sidebar  # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG_DIR = _PROJECT_ROOT / "current"
METRIC_REGISTRY = CONFIG_DIR / "metric_registry.json"
PHYSICAL_SCHEMA = CONFIG_DIR / "physical_schema.json"

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def extract_platforms(metric_def: dict) -> str:
    """Extract unique platforms from preferred_fact_table structure."""
    platforms = set()
    for grain_map in metric_def.get("preferred_fact_table", {}).values():
        platforms.update(grain_map.keys())
    return ", ".join(sorted(platforms))


@st.cache_data(ttl=3600)
def load_metrics_catalog(registry_path: str) -> pd.DataFrame:
    """Load all metrics into a DataFrame for display."""
    registry = MetricRegistry.from_path(registry_path)
    rows = []

    for key, metric_def in registry.metrics.items():
        is_derived = metric_def.get("default_aggregation") == "derived"
        platforms = extract_platforms(metric_def)

        row = {
            "Metric": key,
            "Type": "Derived" if is_derived else "Base",
            "Class": metric_def.get("metric_class", ""),
            "Aggregation": metric_def.get("default_aggregation", ""),
            "Formula": metric_def.get("derived_formula", "") if is_derived else "",
            "Base Metrics": ", ".join(metric_def.get("base_metrics", [])) if is_derived else "",
            "Domains": ", ".join(metric_def.get("domains", [])),
            "Platforms": platforms,
            "Grains": ", ".join(metric_def.get("supported_grains", [])),
            "Default Grain": metric_def.get("default_grain", ""),
        }
        rows.append(row)

    return pd.DataFrame(rows)


@st.cache_data(ttl=3600)
def load_dimensions_catalog(schema_path: str) -> pd.DataFrame:
    """Load dimensions into a DataFrame for display using DimensionExtractor."""
    extractor = DimensionExtractor(schema_path)
    # Extract dimensions appearing in 2+ tables
    records = extractor.to_dataframe_records(min_occurrences=2)
    return pd.DataFrame(records)


@st.cache_data(ttl=3600)
def build_platform_grain_matrix(registry_path: str) -> pd.DataFrame:
    """Build a matrix showing which platform+grain combinations are supported."""
    registry = MetricRegistry.from_path(registry_path)

    # Collect all unique (platform, grain) pairs and count metrics
    matrix_data = {}
    platforms = set()
    grains = set()

    for metric_key, metric_def in registry.metrics.items():
        for grain, platform_map in metric_def.get("preferred_fact_table", {}).items():
            grains.add(grain)
            for platform in platform_map.keys():
                platforms.add(platform)
                key = (grain, platform)
                matrix_data[key] = matrix_data.get(key, 0) + 1

    # Build DataFrame: rows=grains, columns=platforms, values=metric counts
    platforms = sorted(platforms)
    grains = sorted(grains)

    matrix = pd.DataFrame(index=grains, columns=platforms, data=0)
    for (grain, platform), count in matrix_data.items():
        matrix.loc[grain, platform] = count

    return matrix


def apply_filters(
    df: pd.DataFrame,
    search: str,
    platform_filter: str,
    domain_filter: str,
) -> pd.DataFrame:
    """Apply search and filters to a DataFrame."""
    filtered = df.copy()

    # Search filter (case-insensitive, all columns)
    if search:
        filtered = filtered[
            filtered.apply(lambda row: search.lower() in row.to_string().lower(), axis=1)
        ]

    # Platform filter (only for metrics DataFrames with Platforms column)
    if platform_filter != "All" and "Platforms" in filtered.columns:
        filtered = filtered[
            filtered["Platforms"].str.contains(platform_filter, case=False, na=False)
        ]

    # Domain filter (only for metrics DataFrames with Domains column)
    if domain_filter != "All" and "Domains" in filtered.columns:
        filtered = filtered[
            filtered["Domains"].str.contains(domain_filter, case=False, na=False)
        ]

    return filtered


@st.cache_data(ttl=86400)  # Cache for 24 hours
def fetch_dimension_samples(dimension: str, source_tables_str: str) -> list[str]:
    """
    Fetch top 10 sample values for a dimension from Fabric.
    Uses the first source table from the dimension extractor.

    Args:
        dimension: Dimension column name
        source_tables_str: Comma-separated list of source tables from DimensionExtractor

    Returns:
        List of sample values
    """
    # Check if Fabric connection exists
    if not st.session_state.get("fabric_connected", False):
        raise RuntimeError("Fabric connection not established. Connect in the sidebar.")

    # Extract first table from the source_tables string
    if not source_tables_str or source_tables_str.strip() == "":
        raise RuntimeError(f"No source tables available for dimension '{dimension}'")

    # Get first table from comma-separated list
    tables = [t.strip() for t in source_tables_str.split(",")]
    table = tables[0]

    fc: FabricConnection = st.session_state.fabric_conn

    # Handle schema-qualified table names
    # Table names from physical_schema.json can be "schema.table" or just "table"
    if "." in table:
        # Schema-qualified: use as-is without brackets
        table_ref = table
    else:
        # Unqualified: wrap in brackets
        table_ref = f"[{table}]"

    # Query top 10 distinct values
    sql = f"""
    SELECT DISTINCT TOP 10 [{dimension}]
    FROM {table_ref}
    WHERE [{dimension}] IS NOT NULL
    ORDER BY [{dimension}]
    """

    try:
        df = fc.execute(sql)
        return df[dimension].tolist()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch samples from {table}: {e}")


# ---------------------------------------------------------------------------
# Main UI
# ---------------------------------------------------------------------------


def main():
    st.set_page_config(page_title="Schema Explorer", layout="wide")

    # Initialize Fabric state
    init_fabric_state()

    # Session state for selected metrics
    if "selected_metrics" not in st.session_state:
        st.session_state.selected_metrics = []

    # Sidebar
    with st.sidebar:
        st.title("Schema Explorer")
        st.caption("Discover available metrics and dimensions")
        st.divider()

        search = st.text_input("🔍 Search", placeholder="Search metrics, dimensions...")
        platform_filter = st.selectbox(
            "Platform",
            ["All", "google_ads", "microsoft_ads", "exchange", "gotickets"],
        )
        domain_filter = st.selectbox("Domain", ["All", "ads", "orders", "benchmark"])

        st.divider()
        st.caption("Filter controls affect the active tab")

        # Fabric connection sidebar
        st.divider()
        render_fabric_sidebar()

    # Load data
    try:
        metrics_df = load_metrics_catalog(str(METRIC_REGISTRY))
        dimensions_df = load_dimensions_catalog(str(PHYSICAL_SCHEMA))
        matrix_df = build_platform_grain_matrix(str(METRIC_REGISTRY))
    except Exception as e:
        st.error(f"Failed to load catalog data: {e}")
        st.stop()

    # Main content - tabs
    tab1, tab2, tab3 = st.tabs(
        ["📊 Metrics Browser", "📋 Dimensions Browser", "🔀 Platform/Grain Matrix"]
    )

    # --- Tab 1: Metrics Browser ---
    with tab1:
        st.header("Metrics Browser")
        st.caption(f"Showing {len(metrics_df)} available metrics")

        # Apply filters
        filtered_metrics = apply_filters(metrics_df, search, platform_filter, domain_filter)

        if len(filtered_metrics) == 0:
            st.warning("No metrics match the current filters.")
        else:
            # Metric selection interface
            st.subheader("Select Metrics for Query")

            col1, col2 = st.columns([3, 1])
            with col1:
                selected = st.multiselect(
                    "Choose metrics to add to query",
                    options=filtered_metrics["Metric"].tolist(),
                    default=st.session_state.selected_metrics,
                    key="metric_selector",
                )
            with col2:
                st.write("")  # Spacing
                st.write("")  # Spacing
                if st.button("➕ Add to Query", type="primary", use_container_width=True):
                    st.session_state.selected_metrics = selected
                    if selected:
                        st.success(f"Added {len(selected)} metric(s). Go to **Chat** to build your query.")
                    else:
                        st.warning("No metrics selected.")

            if st.session_state.selected_metrics:
                st.info(f"**Selected:** {', '.join(st.session_state.selected_metrics)}")

            st.divider()

            # Display metrics table
            st.subheader("All Metrics")
            st.dataframe(
                filtered_metrics,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Metric": st.column_config.TextColumn("Metric Name", width="medium"),
                    "Type": st.column_config.TextColumn("Type", width="small"),
                    "Class": st.column_config.TextColumn("Class", width="small"),
                    "Aggregation": st.column_config.TextColumn("Aggregation", width="small"),
                    "Formula": st.column_config.TextColumn("Formula", width="large"),
                    "Base Metrics": st.column_config.TextColumn("Base Metrics", width="medium"),
                    "Domains": st.column_config.TextColumn("Domains", width="small"),
                    "Platforms": st.column_config.TextColumn("Platforms", width="medium"),
                    "Grains": st.column_config.TextColumn("Grains", width="large"),
                    "Default Grain": st.column_config.TextColumn("Default Grain", width="medium"),
                },
            )

            # Show derived metric formulas in detail
            st.divider()
            st.subheader("Derived Metric Formulas")

            derived_metrics = filtered_metrics[filtered_metrics["Type"] == "Derived"]
            if len(derived_metrics) == 0:
                st.caption("No derived metrics in current view.")
            else:
                for _, metric in derived_metrics.iterrows():
                    with st.expander(f"📐 {metric['Metric']}"):
                        st.markdown(f"**Formula:** `{metric['Formula']}`")
                        st.markdown(f"**Base Metrics:** {metric['Base Metrics']}")
                        st.markdown(f"**Class:** {metric['Class']}")
                        st.markdown(f"**Platforms:** {metric['Platforms']}")
                        st.markdown(f"**Grains:** {metric['Grains']}")

    # --- Tab 2: Dimensions Browser ---
    with tab2:
        st.header("Dimensions Browser")
        st.caption(f"Showing {len(dimensions_df)} available dimensions")

        # Apply search filter (dimensions don't have platform/domain)
        filtered_dimensions = dimensions_df.copy()
        if search:
            filtered_dimensions = filtered_dimensions[
                filtered_dimensions.apply(
                    lambda row: search.lower() in row.to_string().lower(), axis=1
                )
            ]

        if len(filtered_dimensions) == 0:
            st.warning("No dimensions match the current search.")
        else:
            st.dataframe(
                filtered_dimensions,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "dimension": st.column_config.TextColumn("Dimension", width="medium"),
                    "description": st.column_config.TextColumn("Description", width="large"),
                    "data_type": st.column_config.TextColumn("Data Type", width="small"),
                    "source_tables": st.column_config.TextColumn("Source Tables", width="large"),
                    "table_count": st.column_config.NumberColumn("# Tables", width="small"),
                },
            )

            # Sample values section
            st.divider()
            st.subheader("Sample Values")

            if not st.session_state.get("fabric_connected", False):
                st.info("💡 Connect to Fabric in the sidebar to load sample dimension values.")
            else:
                st.caption("Click a button to load top 10 sample values for each dimension")

                # Show sample values for each dimension
                for _, dim in filtered_dimensions.iterrows():
                    dim_name = dim["dimension"]
                    source_tables = dim.get("source_tables", "")
                    with st.expander(f"🔍 {dim_name}"):
                        col1, col2 = st.columns([1, 4])
                        with col1:
                            if st.button(f"Load Samples", key=f"load_{dim_name}"):
                                with st.spinner(f"Fetching samples for {dim_name}..."):
                                    try:
                                        samples = fetch_dimension_samples(dim_name, source_tables)
                                        st.session_state[f"samples_{dim_name}"] = samples
                                    except RuntimeError as e:
                                        st.error(str(e))
                                    except Exception as e:
                                        st.error(f"Failed to load samples: {e}")

                        with col2:
                            if f"samples_{dim_name}" in st.session_state:
                                samples = st.session_state[f"samples_{dim_name}"]
                                if samples:
                                    st.markdown("**Sample values:**")
                                    for sample in samples:
                                        st.markdown(f"- {sample}")
                                else:
                                    st.caption("No sample values found.")
                            else:
                                st.caption("Click 'Load Samples' to fetch values from Fabric.")

    # --- Tab 3: Platform/Grain Matrix ---
    with tab3:
        st.header("Platform/Grain Matrix")
        st.caption("Shows number of metrics available for each platform+grain combination")

        # Apply platform filter if not "All"
        display_matrix = matrix_df.copy()
        if platform_filter != "All":
            # Filter to only show selected platform column
            if platform_filter in display_matrix.columns:
                display_matrix = display_matrix[[platform_filter]]
            else:
                st.warning(f"Platform '{platform_filter}' not found in matrix.")
                display_matrix = pd.DataFrame()

        if not display_matrix.empty:
            st.dataframe(
                display_matrix,
                use_container_width=True,
                column_config={
                    col: st.column_config.NumberColumn(
                        col.replace("_", " ").title(), format="%d metrics"
                    )
                    for col in display_matrix.columns
                },
            )

            st.divider()
            st.caption(
                "The numbers indicate how many metrics are available for each platform+grain combination."
            )
        else:
            st.warning("No data to display.")


if __name__ == "__main__":
    main()
