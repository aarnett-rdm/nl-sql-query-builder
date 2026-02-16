"""
Tests for visualization utilities (ui/viz_utils.py).
"""

import pandas as pd
import pytest
from datetime import date

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from ui.viz_utils import detect_visualization_opportunity, create_chart


class TestDetectVisualizationOpportunity:
    """Tests for detect_visualization_opportunity function."""

    def test_empty_dataframe(self):
        """Empty DataFrame should not be visualizable."""
        df = pd.DataFrame()
        result = detect_visualization_opportunity(df)

        assert result["should_visualize"] is False
        assert result["chart_type"] is None

    def test_single_value(self):
        """Single row with one numeric column should suggest number display."""
        df = pd.DataFrame({"revenue": [12345.67]})
        result = detect_visualization_opportunity(df)

        assert result["should_visualize"] is True
        assert result["chart_type"] == "number"
        assert result["config"]["value_col"] == "revenue"

    def test_time_series_single_metric(self):
        """Date column with single metric should suggest line chart."""
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=7),
            "clicks": [100, 120, 115, 130, 140, 135, 145]
        })
        result = detect_visualization_opportunity(df)

        assert result["should_visualize"] is True
        assert result["chart_type"] == "line"
        assert result["config"]["x_col"] == "date"
        assert result["config"]["y_col"] == "clicks"

    def test_time_series_multi_metric(self):
        """Date column with multiple metrics should suggest multi-line chart."""
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=5),
            "clicks": [100, 120, 115, 130, 140],
            "impressions": [1000, 1200, 1150, 1300, 1400],
            "cost": [50.0, 60.0, 55.0, 65.0, 70.0]
        })
        result = detect_visualization_opportunity(df)

        assert result["should_visualize"] is True
        assert result["chart_type"] == "multi_line"
        assert result["config"]["x_col"] == "date"
        assert len(result["config"]["y_cols"]) == 3

    def test_platform_comparison_single_metric(self):
        """Platform column with single metric should suggest bar chart."""
        df = pd.DataFrame({
            "platform": ["google_ads", "microsoft_ads"],
            "revenue": [5000.0, 3000.0]
        })
        result = detect_visualization_opportunity(df)

        assert result["should_visualize"] is True
        assert result["chart_type"] == "bar"
        assert result["config"]["x_col"] == "platform"
        assert result["config"]["y_col"] == "revenue"

    def test_platform_comparison_multi_metric(self):
        """Platform column with multiple metrics should suggest grouped bar."""
        df = pd.DataFrame({
            "platform": ["google_ads", "microsoft_ads"],
            "clicks": [1000, 800],
            "cost": [500.0, 400.0],
            "revenue": [5000.0, 3000.0]
        })
        result = detect_visualization_opportunity(df)

        assert result["should_visualize"] is True
        assert result["chart_type"] == "grouped_bar"
        assert result["config"]["x_col"] == "platform"
        assert len(result["config"]["y_cols"]) == 3

    def test_campaign_comparison_few_rows(self):
        """Few campaigns with single metric should suggest bar chart."""
        df = pd.DataFrame({
            "CampaignName": ["Brand", "Performance", "Retargeting"],
            "conversions": [50, 120, 30]
        })
        result = detect_visualization_opportunity(df)

        assert result["should_visualize"] is True
        assert result["chart_type"] == "bar"
        assert result["config"]["y_col"] == "conversions"

    def test_campaign_comparison_many_rows(self):
        """Many campaigns should suggest horizontal bar with limit."""
        campaigns = [f"Campaign {i}" for i in range(25)]
        df = pd.DataFrame({
            "CampaignName": campaigns,
            "revenue": list(range(25))
        })
        result = detect_visualization_opportunity(df)

        assert result["should_visualize"] is True
        assert result["chart_type"] == "horizontal_bar"
        assert result["config"]["limit"] == 15

    def test_insufficient_data(self):
        """Single row with multiple columns should not be visualizable (except number)."""
        df = pd.DataFrame({
            "clicks": [100],
            "impressions": [1000],
            "cost": [50.0]
        })
        result = detect_visualization_opportunity(df)

        # Should detect as number display (single row) but with multiple metrics
        # Actually, with multiple numeric columns and 1 row, it won't match number pattern
        # Let's check what actually happens
        assert result["should_visualize"] is False


class TestCreateChart:
    """Tests for create_chart function."""

    def test_create_line_chart(self):
        """Should create a line chart figure."""
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=5),
            "clicks": [100, 120, 115, 130, 140]
        })
        config = {"x_col": "date", "y_col": "clicks"}

        fig = create_chart(df, "line", config)

        assert fig is not None
        assert len(fig.data) == 1  # One trace
        assert fig.data[0].name == "clicks"

    def test_create_multi_line_chart(self):
        """Should create a multi-line chart with multiple traces."""
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=5),
            "clicks": [100, 120, 115, 130, 140],
            "impressions": [1000, 1200, 1150, 1300, 1400]
        })
        config = {
            "x_col": "date",
            "y_cols": ["clicks", "impressions"]
        }

        fig = create_chart(df, "multi_line", config)

        assert fig is not None
        assert len(fig.data) == 2  # Two traces
        assert fig.data[0].name == "clicks"
        assert fig.data[1].name == "impressions"

    def test_create_bar_chart(self):
        """Should create a bar chart."""
        df = pd.DataFrame({
            "platform": ["google_ads", "microsoft_ads"],
            "revenue": [5000.0, 3000.0]
        })
        config = {"x_col": "platform", "y_col": "revenue"}

        fig = create_chart(df, "bar", config)

        assert fig is not None
        assert len(fig.data) == 1

    def test_create_grouped_bar_chart(self):
        """Should create a grouped bar chart."""
        df = pd.DataFrame({
            "platform": ["google_ads", "microsoft_ads"],
            "clicks": [1000, 800],
            "revenue": [5000.0, 3000.0]
        })
        config = {
            "x_col": "platform",
            "y_cols": ["clicks", "revenue"]
        }

        fig = create_chart(df, "grouped_bar", config)

        assert fig is not None
        assert len(fig.data) == 2  # Two metric groups

    def test_create_horizontal_bar_chart(self):
        """Should create a horizontal bar chart."""
        df = pd.DataFrame({
            "CampaignName": ["Brand", "Performance", "Retargeting"],
            "conversions": [50, 120, 30]
        })
        config = {
            "x_col": "conversions",
            "y_col": "CampaignName",
            "sort_by": "conversions"
        }

        fig = create_chart(df, "horizontal_bar", config)

        assert fig is not None
        assert len(fig.data) == 1
        assert fig.data[0].orientation == 'h'

    def test_chart_with_limit(self):
        """Should respect limit in config."""
        df = pd.DataFrame({
            "CampaignName": [f"Campaign {i}" for i in range(20)],
            "revenue": list(range(20))
        })
        config = {
            "x_col": "revenue",
            "y_col": "CampaignName",
            "limit": 10
        }

        fig = create_chart(df, "horizontal_bar", config)

        # Chart should only show 10 items
        assert len(fig.data[0].x) == 10

    def test_invalid_chart_type(self):
        """Should raise error for invalid chart type."""
        df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})

        with pytest.raises(ValueError, match="Unknown chart type"):
            create_chart(df, "invalid_type", {})


class TestDateColumnDetection:
    """Tests for date column detection."""

    def test_datetime_column(self):
        """Should detect datetime dtype columns."""
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=5),
            "value": [1, 2, 3, 4, 5]
        })
        result = detect_visualization_opportunity(df)

        assert result["should_visualize"] is True
        assert "date" in str(result["config"].get("x_col", ""))

    def test_string_date_column(self):
        """Should detect date-named string columns and parse them."""
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "value": [1, 2, 3]
        })
        result = detect_visualization_opportunity(df)

        assert result["should_visualize"] is True
        # Detection should work even with string dates

    def test_no_date_column(self):
        """Should not detect date if no date-like columns exist."""
        df = pd.DataFrame({
            "category": ["A", "B", "C"],
            "value": [1, 2, 3]
        })
        result = detect_visualization_opportunity(df)

        # Should suggest bar chart for categorical data
        assert result["chart_type"] in ["bar", "grouped_bar", None]


class TestMetricColorSelection:
    """Tests for metric-based color selection."""

    def test_cost_metric_color(self):
        """Cost metrics should use red color."""
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=3),
            "cost": [100, 200, 150]
        })
        config = {"x_col": "date", "y_col": "cost"}

        fig = create_chart(df, "line", config)

        # Check if the line color is reddish (cost color)
        assert fig.data[0].line.color == "#EA4335"

    def test_revenue_metric_color(self):
        """Revenue metrics should use green color."""
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=3),
            "revenue": [1000, 1200, 1100]
        })
        config = {"x_col": "date", "y_col": "revenue"}

        fig = create_chart(df, "line", config)

        assert fig.data[0].line.color == "#34A853"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
