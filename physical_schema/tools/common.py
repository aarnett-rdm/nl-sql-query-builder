"""
common.py

Shared utilities for the NL-SQL query builder tools.

Extracted to avoid duplication across query_builder.py and join_planner.py.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

# ----------------------------
# Platform constants
# ----------------------------

PLATFORM_TOKEN: Dict[str, str] = {
    "google_ads": "googleads",
    "microsoft_ads": "microsoftads",
}

# Strong, deterministic preferences for the most common ambiguous Ads joins.
# Keyed by (platform, join_column) -> preferred dimension table.
DIM_PREFERENCE: Dict[Tuple[str, str], str] = {
    ("google_ads", "CampaignId"): "GoTicketsCoreEntity.GoogleAdsCampaign",
    ("google_ads", "AccountId"): "GoTicketsCoreEntity.GoogleAdsAccount",
    ("google_ads", "AdGroupId"): "GoTicketsCoreEntity.GoogleAdsAdGroup",
    ("microsoft_ads", "CampaignId"): "GoTicketsCoreEntity.MicrosoftAdsCampaign",
    ("microsoft_ads", "AccountId"): "GoTicketsCoreEntity.MicrosoftAdsAccount",
    ("microsoft_ads", "AdGroupId"): "GoTicketsCoreEntity.MicrosoftAdsAdGroup",
}


# ----------------------------
# SQL helpers
# ----------------------------

def bracket_ident(name: str) -> str:
    """T-SQL identifier quoting for a single identifier part."""
    return "[" + name.replace("]", "]]") + "]"


def sql_string_literal(value: str) -> str:
    """Escape a value for use as a T-SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def tsql_qualified_table(full_name: str) -> str:
    """Convert 'Schema.Table' -> '[Schema].[Table]'."""
    parts = full_name.split(".")
    if len(parts) == 2:
        return f"{bracket_ident(parts[0])}.{bracket_ident(parts[1])}"
    return bracket_ident(full_name)


# ----------------------------
# Alias helpers
# ----------------------------

def make_aliases(fact_table: str, join_steps: list) -> Dict[str, str]:
    """
    Stable aliases: fact => 'fact', then t1, t2, ...

    join_steps: list of objects with .right_table and .left_table attributes
    (typically JoinStep dataclass instances).
    """
    aliases: Dict[str, str] = {fact_table: "fact"}
    i = 1
    for s in join_steps:
        if s.right_table not in aliases:
            aliases[s.right_table] = f"t{i}"
            i += 1
        if s.left_table not in aliases:
            aliases[s.left_table] = f"t{i}"
            i += 1
    return aliases
