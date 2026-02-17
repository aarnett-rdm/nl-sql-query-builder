"""
Visualization utilities for creating Plotly charts from query results.

Provides auto-detection of visualization opportunities and chart generation
for the NL SQL Query Builder UI.
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from typing import Optional, Dict, Any, List, Tuple
import re


# Platform-specific colors for consistency
PLATFORM_COLORS = {
    "google_ads": "#4285F4",  # Google blue
    "microsoft_ads": "#F25022",  # Microsoft orange
    "both": "#34A853",  # Green for combined
}

# Metric type colors
METRIC_COLORS = {
    "cost": "#EA4335",  # Red
    "revenue": "#34A853",  # Green
    "clicks": "#4285F4",  # Blue
    "impressions": "#FBBC04",  # Yellow
    "conversions": "#9C27B0",  # Purple
}


def detect_visualization_opportunity(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyzes a DataFrame to determine if/how it should be visualized.

    Returns:
        Dict with:
        - should_visualize: bool
        - chart_type: str (line, bar, grouped_bar, number, multi_line, etc.)
        - reason: str
        - config: dict (chart-specific configuration)
    """
    if df is None or df.empty:
        return {
            "should_visualize": False,
            "chart_type": None,
            "reason": "No data to visualize",
            "config": {}
        }

    num_rows = len(df)
    num_cols = len(df.columns)

    # Identify column types
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    date_cols = _identify_date_columns(df)
    categorical_cols = [col for col in df.columns if col not in numeric_cols and col not in date_cols]

    # Single value - show as big number
    if num_rows == 1 and len(numeric_cols) == 1:
        return {
            "should_visualize": True,
            "chart_type": "number",
            "reason": "Single metric value",
            "config": {
                "value_col": numeric_cols[0],
                "label": numeric_cols[0]
            }
        }

    # Too few rows to visualize meaningfully
    if num_rows < 2:
        return {
            "should_visualize": False,
            "chart_type": None,
            "reason": "Insufficient data rows",
            "config": {}
        }

    # Check for time series (has date column + numeric metrics)
    if date_cols and numeric_cols:
        date_col = date_cols[0]

        # Multiple metrics over time -> multi-line chart
        if len(numeric_cols) > 1:
            return {
                "should_visualize": True,
                "chart_type": "multi_line",
                "reason": f"Time series with {len(numeric_cols)} metrics",
                "config": {
                    "x_col": date_col,
                    "y_cols": numeric_cols,
                    "sort_by": date_col
                }
            }
        else:
            # Single metric over time -> line chart
            return {
                "should_visualize": True,
                "chart_type": "line",
                "reason": "Time series trend",
                "config": {
                    "x_col": date_col,
                    "y_col": numeric_cols[0],
                    "sort_by": date_col
                }
            }

    # Check for platform comparison
    platform_col = _identify_platform_column(df)
    if platform_col and numeric_cols:
        if len(numeric_cols) > 1:
            # Multiple metrics by platform -> grouped bar
            return {
                "should_visualize": True,
                "chart_type": "grouped_bar",
                "reason": "Platform comparison with multiple metrics",
                "config": {
                    "x_col": platform_col,
                    "y_cols": numeric_cols,
                    "group_by": platform_col
                }
            }
        else:
            # Single metric by platform -> simple bar
            return {
                "should_visualize": True,
                "chart_type": "bar",
                "reason": "Platform comparison",
                "config": {
                    "x_col": platform_col,
                    "y_col": numeric_cols[0]
                }
            }

    # Check for campaign/dimension comparison
    if categorical_cols and numeric_cols:
        dimension_col = categorical_cols[0]

        # Limit to top N for readability
        if num_rows > 20:
            return {
                "should_visualize": True,
                "chart_type": "horizontal_bar",
                "reason": f"Top campaigns by {numeric_cols[0]}",
                "config": {
                    "x_col": numeric_cols[0],
                    "y_col": dimension_col,
                    "sort_by": numeric_cols[0],
                    "limit": 15
                }
            }
        elif len(numeric_cols) > 1:
            # Multiple metrics by dimension -> grouped bar
            return {
                "should_visualize": True,
                "chart_type": "grouped_bar",
                "reason": "Multi-metric comparison",
                "config": {
                    "x_col": dimension_col,
                    "y_cols": numeric_cols
                }
            }
        else:
            # Single metric by dimension -> bar chart
            return {
                "should_visualize": True,
                "chart_type": "bar",
                "reason": "Dimension comparison",
                "config": {
                    "x_col": dimension_col,
                    "y_col": numeric_cols[0],
                    "sort_by": numeric_cols[0]
                }
            }

    # Default: show table only
    return {
        "should_visualize": False,
        "chart_type": None,
        "reason": "Data structure not suitable for auto-visualization",
        "config": {}
    }


def create_chart(df: pd.DataFrame, chart_type: str, config: Optional[Dict] = None) -> go.Figure:
    """
    Creates a Plotly chart based on type and configuration.

    Args:
        df: DataFrame with query results
        chart_type: One of: line, multi_line, bar, grouped_bar, horizontal_bar, area, number
        config: Chart-specific configuration dict

    Returns:
        Plotly Figure object
    """
    if config is None:
        config = {}

    # Apply sorting and limit (horizontal_bar* handle this internally for correct top-N behavior)
    if chart_type not in ("horizontal_bar", "horizontal_bar_multi"):
        if "sort_by" in config and config["sort_by"] in df.columns:
            df = df.sort_values(by=config["sort_by"])

        if "limit" in config:
            df = df.head(config["limit"])

    if chart_type == "number":
        return _create_number_display(df, config)
    elif chart_type == "line":
        return _create_line_chart(df, config)
    elif chart_type == "multi_line":
        return _create_multi_line_chart(df, config)
    elif chart_type == "area":
        return _create_area_chart(df, config)
    elif chart_type == "bar":
        return _create_bar_chart(df, config)
    elif chart_type == "grouped_bar":
        return _create_grouped_bar_chart(df, config)
    elif chart_type == "horizontal_bar":
        return _create_horizontal_bar_chart(df, config)
    elif chart_type == "horizontal_bar_multi":
        return _create_horizontal_bar_multi(df, config)
    else:
        raise ValueError(f"Unknown chart type: {chart_type}")


def format_chart_layout(
    fig: go.Figure,
    title: str = "",
    height: int = 400,
    show_legend: bool = True,
    **kwargs
) -> go.Figure:
    """
    Applies consistent styling to charts.

    Args:
        fig: Plotly figure to format
        title: Chart title
        height: Chart height in pixels
        show_legend: Whether to show legend
        **kwargs: Additional layout parameters

    Returns:
        Formatted figure
    """
    fig.update_layout(
        title=title,
        height=height,
        showlegend=show_legend,
        template="plotly_white",
        hovermode="x unified",
        font=dict(family="Arial, sans-serif", size=12),
        margin=dict(l=50, r=50, t=50, b=50),
        **kwargs
    )

    return fig


# ============================================================================
# Private helper functions
# ============================================================================

def _identify_date_columns(df: pd.DataFrame) -> List[str]:
    """Identifies columns that contain date/datetime data."""
    date_cols = []
    for col in df.columns:
        # Check dtype
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            date_cols.append(col)
        # Check column name
        elif any(keyword in col.lower() for keyword in ['date', 'day', 'month', 'year', 'time']):
            # Try to parse as date
            try:
                pd.to_datetime(df[col], errors='raise')
                date_cols.append(col)
            except (ValueError, TypeError):
                pass
    return date_cols


def _identify_platform_column(df: pd.DataFrame) -> Optional[str]:
    """Identifies the platform column if it exists."""
    for col in df.columns:
        if 'platform' in col.lower():
            return col
        # Check if values match platform keywords
        if df[col].dtype == 'object':
            unique_vals = df[col].unique()
            if any('google' in str(v).lower() or 'microsoft' in str(v).lower() for v in unique_vals):
                return col
    return None


def _create_number_display(df: pd.DataFrame, config: Dict) -> go.Figure:
    """Creates a big number display for single values."""
    value_col = config.get("value_col")
    label = config.get("label", value_col)

    value = df[value_col].iloc[0]

    # Format value based on column name
    if any(keyword in value_col.lower() for keyword in ['cost', 'revenue', 'profit', 'cpc']):
        formatted_value = f"${value:,.2f}"
    elif any(keyword in value_col.lower() for keyword in ['rate', 'percentage', 'ctr', 'roi']):
        formatted_value = f"{value * 100:.2f}%"
    else:
        formatted_value = f"{value:,.0f}"

    fig = go.Figure()
    fig.add_trace(go.Indicator(
        mode="number",
        value=value,
        number={'prefix': "$" if 'cost' in value_col.lower() or 'revenue' in value_col.lower() else "",
                'suffix': "%" if 'rate' in value_col.lower() or 'percentage' in value_col.lower() else ""},
        title={'text': label},
        domain={'x': [0, 1], 'y': [0, 1]}
    ))

    fig.update_layout(height=200)
    return fig


def _create_line_chart(df: pd.DataFrame, config: Dict) -> go.Figure:
    """Creates a line chart for time series data."""
    x_col = config.get("x_col")
    y_col = config.get("y_col")

    # Get color for metric
    color = _get_metric_color(y_col)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[x_col],
        y=df[y_col],
        mode='lines+markers',
        name=y_col,
        line=dict(color=color, width=2),
        marker=dict(size=6),
        fill='tonexty',
        fillcolor=f'rgba{_hex_to_rgba(color, 0.1)}'
    ))

    fig = format_chart_layout(
        fig,
        title=f"{y_col} over time",
        show_legend=False
    )

    fig.update_xaxes(title=x_col)
    fig.update_yaxes(title=y_col)

    return fig


def _create_multi_line_chart(df: pd.DataFrame, config: Dict) -> go.Figure:
    """Creates a multi-line chart for multiple metrics over time."""
    x_col = config.get("x_col")
    y_cols = config.get("y_cols", [])

    fig = go.Figure()

    for idx, y_col in enumerate(y_cols):
        color = _get_metric_color(y_col)

        fig.add_trace(go.Scatter(
            x=df[x_col],
            y=df[y_col],
            mode='lines+markers',
            name=y_col,
            line=dict(color=color, width=2),
            marker=dict(size=5)
        ))

    fig = format_chart_layout(
        fig,
        title=f"Metrics over time",
        show_legend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig.update_xaxes(title=x_col)
    fig.update_yaxes(title="Value")

    return fig


def _create_area_chart(df: pd.DataFrame, config: Dict) -> go.Figure:
    """Creates an area chart (line chart with filled area beneath)."""
    x_col = config.get("x_col")
    y_col = config.get("y_col")
    y_cols = config.get("y_cols")

    fig = go.Figure()

    # Multiple metrics = stacked area
    if y_cols and len(y_cols) > 1:
        for y_col in y_cols:
            color = _get_metric_color(y_col)
            fig.add_trace(go.Scatter(
                x=df[x_col],
                y=df[y_col],
                mode='lines',
                name=y_col,
                line=dict(color=color, width=2),
                fill='tonexty',
                stackgroup='one'
            ))

        fig = format_chart_layout(
            fig,
            title=f"Metrics over time (stacked)",
            show_legend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
    else:
        # Single metric = simple area chart
        if not y_col and y_cols:
            y_col = y_cols[0]

        color = _get_metric_color(y_col)
        fig.add_trace(go.Scatter(
            x=df[x_col],
            y=df[y_col],
            mode='lines',
            name=y_col,
            line=dict(color=color, width=2),
            fill='tozeroy',
            fillcolor=f'rgba{_hex_to_rgba(color, 0.3)}'
        ))

        fig = format_chart_layout(
            fig,
            title=f"{y_col} over time",
            show_legend=False
        )

    fig.update_xaxes(title=x_col)
    fig.update_yaxes(title="Value")

    return fig


def _create_bar_chart(df: pd.DataFrame, config: Dict) -> go.Figure:
    """Creates a bar chart for categorical comparisons."""
    x_col = config.get("x_col")
    y_col = config.get("y_col")

    # Sort by value if specified
    if config.get("sort_by") == y_col:
        df = df.sort_values(by=y_col, ascending=False)

    color = _get_metric_color(y_col)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df[x_col],
        y=df[y_col],
        name=y_col,
        marker=dict(color=color)
    ))

    fig = format_chart_layout(
        fig,
        title=f"{y_col} by {x_col}",
        show_legend=False
    )

    fig.update_xaxes(title=x_col)
    fig.update_yaxes(title=y_col)

    return fig


def _create_grouped_bar_chart(df: pd.DataFrame, config: Dict) -> go.Figure:
    """Creates a grouped bar chart for multi-metric comparisons."""
    x_col = config.get("x_col")
    y_cols = config.get("y_cols", [])

    fig = go.Figure()

    for y_col in y_cols:
        color = _get_metric_color(y_col)
        fig.add_trace(go.Bar(
            x=df[x_col],
            y=df[y_col],
            name=y_col,
            marker=dict(color=color)
        ))

    fig = format_chart_layout(
        fig,
        title=f"Metrics by {x_col}",
        show_legend=True,
        barmode='group',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig.update_xaxes(title=x_col)
    fig.update_yaxes(title="Value")

    return fig


def _create_horizontal_bar_chart(df: pd.DataFrame, config: Dict) -> go.Figure:
    """Creates a horizontal bar chart for top N items."""
    x_col = config.get("x_col")  # numeric
    y_col = config.get("y_col")  # categorical

    # Get top N by value (sort descending, take head), then re-sort ascending for display
    # (ascending order makes the highest-value bar appear at the top of horizontal charts)
    df = df.sort_values(by=x_col, ascending=False)
    if "limit" in config:
        df = df.head(config["limit"])
    df = df.sort_values(by=x_col, ascending=True)

    color = _get_metric_color(x_col)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df[x_col],
        y=df[y_col],
        orientation='h',
        marker=dict(color=color)
    ))

    fig = format_chart_layout(
        fig,
        title=f"Top {len(df)} by {x_col}",
        show_legend=False,
        height=max(400, len(df) * 25)  # Scale height with number of items
    )

    fig.update_xaxes(title=x_col)
    fig.update_yaxes(title=y_col)

    return fig


def _create_horizontal_bar_multi(df: pd.DataFrame, config: Dict) -> go.Figure:
    """
    Creates side-by-side subplot panels for multiple metrics on a horizontal bar chart.

    Each metric gets its own x-axis panel so differing scales (e.g. cost vs clicks)
    don't distort each other. Campaign names are shared on the left y-axis.
    Campaigns are ranked by the first selected metric.
    """
    x_cols = config.get("x_cols", [])  # list of numeric metric columns
    y_col = config.get("y_col")        # categorical (campaign name)
    limit = config.get("limit", 15)

    if not x_cols:
        raise ValueError("horizontal_bar_multi requires x_cols list")

    # Sort by first metric descending to get top N, then ascending for display
    sort_col = x_cols[0]
    df = df.sort_values(by=sort_col, ascending=False).head(limit)
    df = df.sort_values(by=sort_col, ascending=True)

    n = len(x_cols)
    fig = make_subplots(
        rows=1,
        cols=n,
        shared_yaxes=True,          # campaign names on left only
        horizontal_spacing=0.04,
        subplot_titles=x_cols,
    )

    for i, x_col in enumerate(x_cols, start=1):
        color = _get_metric_color(x_col)
        fig.add_trace(
            go.Bar(
                x=df[x_col],
                y=df[y_col],
                orientation="h",
                name=x_col,
                marker=dict(color=color),
                showlegend=False,
            ),
            row=1,
            col=i,
        )

    panel_height = max(400, len(df) * 28)
    fig.update_layout(
        height=panel_height,
        template="plotly_white",
        font=dict(family="Arial, sans-serif", size=11),
        margin=dict(l=10, r=20, t=50, b=30),
        title=f"Top {len(df)} campaigns",
    )

    return fig


def _get_metric_color(metric_name: str) -> str:
    """Gets color for a metric based on its name."""
    metric_lower = metric_name.lower()

    for keyword, color in METRIC_COLORS.items():
        if keyword in metric_lower:
            return color

    # Default color
    return "#757575"  # Gray


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    """Converts hex color to rgba string."""
    hex_color = hex_color.lstrip('#')
    r, g, b = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    return f"({r}, {g}, {b}, {alpha})"
