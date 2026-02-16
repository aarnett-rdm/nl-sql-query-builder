"""
Dimension Extractor

Extracts dimension metadata from physical_schema.json by identifying columns
that appear across multiple tables and look like dimensions (not metrics).
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class DimensionInfo:
    """Metadata for a discovered dimension."""

    dimension: str
    description: str
    data_type: str
    source_tables: list[str]
    table_count: int


class DimensionExtractor:
    """Extract dimension metadata from physical schema."""

    def __init__(self, schema_path: str | Path):
        """
        Initialize with path to physical_schema.json.

        Args:
            schema_path: Path to physical_schema.json file
        """
        with open(schema_path, "r", encoding="utf-8") as f:
            self.schema = json.load(f)
        self.tables = self.schema.get("tables", {})

    def extract_common_dimensions(self, min_occurrences: int = 2) -> list[DimensionInfo]:
        """
        Find columns that appear across multiple tables and look like dimensions.

        Args:
            min_occurrences: Minimum number of tables a column must appear in

        Returns:
            List of DimensionInfo objects sorted by dimension name

        Heuristics:
        - Column name matches common patterns (Name, Type, Status, etc.)
        - NOT a metric column (no numeric aggregation names like Sum, Count, Avg)
        - Appears in min_occurrences or more tables
        - Exclude primary keys and identifiers (unless they're named dimensions)
        """
        # Track column occurrences across tables
        column_occurrences: dict[str, list[str]] = defaultdict(list)
        column_types: dict[str, str] = {}  # col_name -> most common data_type

        for table_name, table_meta in self.tables.items():
            # Skip metric/performance tables (contain aggregated data)
            if self._is_fact_table(table_name):
                continue

            columns = table_meta.get("columns", {})
            for col_name, col_meta in columns.items():
                # Check if this looks like a dimension
                if self._looks_like_dimension(col_name, col_meta):
                    column_occurrences[col_name].append(table_name)

                    # Track data type (prefer varchar over int for display)
                    if col_name not in column_types:
                        column_types[col_name] = col_meta.get("data_type", "unknown")

        # Filter to columns appearing in enough tables
        dimensions = []
        for col_name, tables in column_occurrences.items():
            if len(tables) >= min_occurrences:
                description = self._generate_description(col_name)
                dimensions.append(
                    DimensionInfo(
                        dimension=col_name,
                        description=description,
                        data_type=column_types.get(col_name, "unknown"),
                        source_tables=sorted(tables[:5]),  # Limit to first 5 for display
                        table_count=len(tables),
                    )
                )

        return sorted(dimensions, key=lambda x: x.dimension)

    def _is_fact_table(self, table_name: str) -> bool:
        """Check if table name indicates a fact/metric table."""
        fact_indicators = [
            "Metric",
            "Performance",
            "Fact",
            "Aggregate",
            "Summary",
            "Event",  # Event tables often contain metrics
        ]
        return any(indicator in table_name for indicator in fact_indicators)

    def _looks_like_dimension(self, col_name: str, col_meta: dict) -> bool:
        """Heuristic: is this column a dimension?"""
        name_lower = col_name.lower()
        data_type = col_meta.get("data_type", "").lower()

        # Exclude metric-like column names
        metric_patterns = [
            "sum",
            "count",
            "avg",
            "total",
            "amount",
            "value",
            "rate",
            "percent",
            "ratio",
        ]
        if any(pattern in name_lower for pattern in metric_patterns):
            return False

        # Exclude identifiers (unless they end in Name/Type/Status)
        if "identifier" in name_lower or name_lower.endswith("id"):
            # Exception: allow ID columns that are categorical (Device, Network, etc.)
            categorical_patterns = ["device", "network", "platform", "channel"]
            if not any(pattern in name_lower for pattern in categorical_patterns):
                return False

        # Exclude date/time columns (treated separately)
        if "date" in name_lower or "time" in name_lower or data_type in [
            "date",
            "datetime",
            "datetime2",
            "timestamp",
        ]:
            return False

        # Include common dimension suffixes/patterns
        dimension_patterns = [
            "name",
            "type",
            "status",
            "category",
            "state",
            "device",
            "network",
            "currency",
            "language",
            "country",
        ]
        if any(pattern in name_lower for pattern in dimension_patterns):
            return True

        # Include if data type suggests categorical data
        if data_type in ["varchar", "nvarchar", "char", "text"]:
            return True

        # Include small integers (likely enums/categories)
        if data_type in ["tinyint", "smallint"] or (
            data_type == "int" and not any(x in name_lower for x in ["count", "sum", "total"])
        ):
            return True

        return False

    def _generate_description(self, col_name: str) -> str:
        """Generate a human-readable description for a dimension."""
        # Simple heuristic: convert CamelCase/PascalCase to readable text
        import re

        # Insert spaces before capital letters
        readable = re.sub(r"([A-Z])", r" \1", col_name).strip()

        # Specific descriptions for known dimensions
        descriptions = {
            "AccountName": "Account name",
            "CampaignName": "Campaign name",
            "AdGroupName": "Ad group name",
            "State": "US state code",
            "Device": "Device type (mobile, desktop, tablet)",
            "Network": "Ad network",
            "CampaignType": "Campaign type",
            "CampaignStatus": "Campaign status (active, paused, etc.)",
            "CurrencyCode": "Currency code (USD, EUR, etc.)",
            "VenueName": "Venue name",
            "PerformerName": "Performer name",
            "Platform": "Platform identifier",
            "Language": "Language code",
            "CountryCode": "Country code",
        }

        return descriptions.get(col_name, readable)

    def to_dataframe_records(self, min_occurrences: int = 2) -> list[dict[str, Any]]:
        """
        Extract dimensions and return as list of dicts suitable for pd.DataFrame.

        Args:
            min_occurrences: Minimum number of tables a column must appear in

        Returns:
            List of dicts with keys: dimension, description, data_type
        """
        dimensions = self.extract_common_dimensions(min_occurrences)
        return [
            {
                "dimension": dim.dimension,
                "description": dim.description,
                "data_type": dim.data_type,
                "source_tables": ", ".join(dim.source_tables),
                "table_count": dim.table_count,
            }
            for dim in dimensions
        ]


# ---------------------------------------------------------------------------
# CLI for testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Extract dimensions from physical schema")
    parser.add_argument(
        "--schema",
        type=str,
        default="current/physical_schema.json",
        help="Path to physical_schema.json",
    )
    parser.add_argument(
        "--min-occurrences",
        type=int,
        default=2,
        help="Minimum tables a column must appear in (default: 2)",
    )
    args = parser.parse_args()

    if not Path(args.schema).exists():
        print(f"Error: Schema file not found: {args.schema}", file=sys.stderr)
        sys.exit(1)

    extractor = DimensionExtractor(args.schema)
    dimensions = extractor.extract_common_dimensions(min_occurrences=args.min_occurrences)

    print(f"Found {len(dimensions)} dimensions:")
    print()

    for dim in dimensions:
        print(f"- {dim.dimension}")
        print(f"  Description: {dim.description}")
        print(f"  Type: {dim.data_type}")
        print(f"  Tables: {', '.join(dim.source_tables)} ({dim.table_count} total)")
        print()
