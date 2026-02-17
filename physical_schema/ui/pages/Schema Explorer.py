"""
Schema Explorer & Metric Catalog

Interactive catalog showing all available metrics, dimensions, and platform/grain support.
Helps users discover what's queryable without digging through JSON configs.
"""

from __future__ import annotations

import json
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
            "Description": metric_def.get("description", ""),
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


@st.cache_data(ttl=3600)
def build_relationship_data(
    registry_path: str, schema_path: str, platform: str
) -> tuple[list[str], list[dict], list[str]]:
    """
    Build node and edge data for the table relationship graph.

    Returns:
        (nodes, edges, fact_tables) where edges are plain dicts for cacheability.
    """
    from tools.join_planner import PhysicalSchema

    with open(registry_path) as f:
        registry_json = json.load(f)

    with open(schema_path) as f:
        schema_payload = json.load(f)

    # Collect fact tables referenced in the registry
    fact_tables: set[str] = set()
    for metric_def in registry_json["metrics"].values():
        for grain_dict in metric_def.get("preferred_fact_table", {}).values():
            for tables in grain_dict.values():
                fact_tables.update(tables)

    schema = PhysicalSchema(schema_payload)
    plat = None if platform == "All" else platform

    nodes: set[str] = set(fact_tables)
    edges: list[dict] = []
    seen: set[tuple[str, str]] = set()

    for table in list(fact_tables):
        try:
            for edge in schema.neighbors(table, platform=plat):
                key = (edge.from_table, edge.to_table)
                if key not in seen:
                    seen.add(key)
                    edges.append(
                        {
                            "from": edge.from_table,
                            "to": edge.to_table,
                            "confidence": edge.confidence,
                            "join_cols": (
                                f"{', '.join(edge.from_columns)}"
                                f" → {', '.join(edge.to_columns)}"
                            ),
                        }
                    )
                    nodes.add(edge.to_table)
        except Exception:
            # Skip tables that fail to resolve neighbors
            pass

    return list(nodes), edges, list(fact_tables)


def build_plotly_graph(
    nodes: list[str],
    edges: list[dict],
    fact_tables: list[str],
    high_only: bool,
) -> "go.Figure":
    """Build a Plotly network graph from node/edge data."""
    import networkx as nx
    import plotly.graph_objects as go

    fact_set = set(fact_tables)
    MAPPING_KEYWORDS = {"entitymap", "map", "mapping", "bridge", "xref", "junction"}

    def node_color(name: str) -> str:
        if name in fact_set:
            return "#4C72B0"  # blue — fact/metric table
        if any(k in name.lower() for k in MAPPING_KEYWORDS):
            return "#DD8452"  # orange — mapping/bridge table
        return "#55A868"  # green — dimension table

    def short_label(name: str) -> str:
        """Shorten long table names for display."""
        label = name
        label = label.replace("GoogleAds", "GA·")
        label = label.replace("MicrosoftAds", "MS·")
        label = label.replace("PerformanceMetric", "Perf")
        label = label.replace("AuctionInsightMetric", "Auction")
        label = label.replace("BidChange", "Bid")
        return label

    # Build NetworkX graph
    G = nx.DiGraph()
    G.add_nodes_from(nodes)
    for e in edges:
        if high_only and e["confidence"] != "high":
            continue
        G.add_edge(e["from"], e["to"], confidence=e["confidence"], join_cols=e["join_cols"])

    if G.number_of_nodes() == 0:
        return None

    pos = nx.spring_layout(G, seed=42, k=2.5)

    # --- Edge traces (one per confidence level for legend) ---
    conf_styles = {
        "high": dict(color="#444444", width=2, dash="solid"),
        "medium": dict(color="#999999", width=1.5, dash="dash"),
        "low": dict(color="#CCCCCC", width=1, dash="dot"),
    }
    conf_labels = {"high": "High confidence join", "medium": "Medium confidence", "low": "Low confidence"}
    conf_shown = {c: False for c in conf_styles}

    edge_traces = []
    for u, v, data in G.edges(data=True):
        conf = data.get("confidence", "medium")
        style = conf_styles.get(conf, conf_styles["medium"])
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        show = not conf_shown[conf]
        conf_shown[conf] = True
        edge_traces.append(
            go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode="lines",
                line=dict(color=style["color"], width=style["width"], dash=style["dash"]),
                hoverinfo="none",
                name=conf_labels[conf],
                legendgroup=conf,
                showlegend=show,
            )
        )

    # --- Node traces (grouped by type for legend) ---
    type_groups = {
        "Fact / Metric table": ("blue", []),
        "Dimension table": ("green", []),
        "Mapping / Bridge table": ("orange", []),
    }
    color_to_type = {
        "#4C72B0": "Fact / Metric table",
        "#55A868": "Dimension table",
        "#DD8452": "Mapping / Bridge table",
    }

    node_traces = []
    type_shown: dict[str, bool] = {}

    for node in G.nodes():
        color = node_color(node)
        node_type = color_to_type[color]
        x, y = pos[node]

        # Build hover text
        successors = list(G.successors(node))
        predecessors = list(G.predecessors(node))
        hover = f"<b>{node}</b><br>Type: {node_type}"
        if successors:
            hover += "<br>Joins to: " + ", ".join(successors[:4])
        if predecessors:
            hover += "<br>Referenced by: " + ", ".join(predecessors[:4])

        show = node_type not in type_shown
        type_shown[node_type] = True

        node_traces.append(
            go.Scatter(
                x=[x],
                y=[y],
                mode="markers+text",
                text=[short_label(node)],
                textposition="top center",
                textfont=dict(size=8),
                hovertext=[hover],
                hoverinfo="text",
                marker=dict(size=14, color=color, line=dict(width=1, color="white")),
                name=node_type,
                legendgroup=node_type,
                showlegend=show,
            )
        )

    fig = go.Figure(data=edge_traces + node_traces)
    fig.update_layout(
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        margin=dict(l=20, r=20, t=20, b=20),
        plot_bgcolor="white",
        height=650,
        hovermode="closest",
        legend=dict(
            orientation="v",
            x=1.01,
            y=1,
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor="#DDDDDD",
            borderwidth=1,
        ),
    )
    return fig


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
    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "📊 Metrics Browser",
            "📋 Dimensions Browser",
            "🔀 Platform/Grain Matrix",
            "🔗 Table Relationships",
        ]
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
                    "Description": st.column_config.TextColumn("Description", width="large"),
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
                        if metric.get("Description"):
                            st.markdown(f"**Description:** {metric['Description']}")
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

    # --- Tab 4: Table Relationships ---
    with tab4:
        st.header("Table Relationships")
        st.caption(
            "Visual map of fact tables (from the metric registry) and their dimension joins. "
            "Hover over a node for details."
        )

        # Controls
        col1, col2 = st.columns([2, 2])
        with col1:
            graph_platform = st.selectbox(
                "Filter edges by platform",
                ["All", "google_ads", "microsoft_ads", "exchange", "gotickets"],
                key="graph_platform",
            )
        with col2:
            high_only = st.checkbox(
                "Show high-confidence joins only",
                value=False,
                key="graph_high_only",
                help="Hides medium/low confidence inferred joins to reduce clutter",
            )

        # Load graph data
        try:
            with st.spinner("Building relationship graph..."):
                nodes, edges, fact_tables = build_relationship_data(
                    str(METRIC_REGISTRY), str(PHYSICAL_SCHEMA), graph_platform
                )
        except ImportError as e:
            st.error(
                f"Missing dependency: {e}. "
                "Run `pip install networkx plotly` in your UI environment."
            )
            st.stop()
        except Exception as e:
            st.error(f"Failed to build relationship data: {e}")
            st.stop()

        if not nodes:
            st.warning("No table relationships found.")
        else:
            # Build and display the graph
            try:
                import plotly.graph_objects  # noqa: F401 — confirm plotly available
                fig = build_plotly_graph(nodes, edges, fact_tables, high_only)
                if fig is None:
                    st.warning("No nodes to display after applying filters.")
                else:
                    st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                st.error("Plotly not installed. Run `pip install plotly` in your UI environment.")
                st.stop()

            # Summary stats below the graph
            st.divider()
            c1, c2, c3 = st.columns(3)
            c1.metric("Tables", len(nodes))
            c2.metric("Fact / Metric tables", len(fact_tables))
            c3.metric(
                "Join edges",
                sum(1 for e in edges if not high_only or e["confidence"] == "high"),
            )

            # Edge table (expandable)
            with st.expander("View all join edges"):
                edge_df = pd.DataFrame(
                    [
                        {
                            "From Table": e["from"],
                            "To Table": e["to"],
                            "Confidence": e["confidence"],
                            "Join Columns": e["join_cols"],
                        }
                        for e in edges
                        if not high_only or e["confidence"] == "high"
                    ]
                )
                if not edge_df.empty:
                    st.dataframe(edge_df, use_container_width=True, hide_index=True)
                else:
                    st.caption("No edges match the current filter.")


if __name__ == "__main__":
    main()
