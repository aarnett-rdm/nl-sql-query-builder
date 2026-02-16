"""
Tests for column disambiguation (Priority 1.2).

Verifies that ambiguous dimension columns produce AmbiguousDimensionError
instead of crashing, and that platform affinity / prefer_fact resolve
common cases automatically.
"""
import pytest
from tools.query_builder import (
    AmbiguousDimensionError,
    _resolve_dimension_expression,
    _parse_dimension_spec,
)
from tools.common import bracket_ident as _bracket_ident
from tools.join_planner import PhysicalSchema


# -------------------------------------------------------
# Helpers: build a minimal schema for testing
# -------------------------------------------------------

def _mini_schema(tables: dict, aliases: dict = None) -> PhysicalSchema:
    """Build a minimal PhysicalSchema from a {table_name: {col: {}}} dict."""
    payload = {
        "tables": {
            t: {"columns": cols, "primary_key": []}
            for t, cols in tables.items()
        },
        "table_alias_resolution": {"aliases": aliases or {}},
    }
    return payload


def _make_aliases_from_list(fact: str, others: list) -> dict:
    aliases = {fact: "fact"}
    for i, t in enumerate(others, 1):
        aliases[t] = f"t{i}"
    return aliases


# -------------------------------------------------------
# Test: explicit table qualification always works
# -------------------------------------------------------

def test_explicit_table_qualification():
    """If user qualifies 'TableA.Revenue', no ambiguity."""
    tables = {
        "Schema.FactTable": {"Revenue": {}, "Cost": {}},
        "Schema.DimA": {"Revenue": {}, "Name": {}},
    }
    schema = PhysicalSchema(_mini_schema(tables))
    aliases = _make_aliases_from_list("Schema.FactTable", ["Schema.DimA"])

    dim = _parse_dimension_spec("Schema.DimA.Revenue")
    sel, grp = _resolve_dimension_expression(schema, aliases, dim, "Schema.FactTable", "prefer_fact")
    assert "t1" in sel  # DimA alias
    assert "[Revenue]" in sel


# -------------------------------------------------------
# Test: prefer_fact resolves ambiguity
# -------------------------------------------------------

def test_prefer_fact_resolves_ambiguity():
    """If column exists on fact and dim, prefer_fact picks the fact table."""
    tables = {
        "Schema.FactTable": {"CampaignName": {}, "Clicks": {}},
        "Schema.DimCampaign": {"CampaignName": {}, "CampaignType": {}},
    }
    schema = PhysicalSchema(_mini_schema(tables))
    aliases = _make_aliases_from_list("Schema.FactTable", ["Schema.DimCampaign"])

    dim = _parse_dimension_spec("CampaignName")
    sel, grp = _resolve_dimension_expression(schema, aliases, dim, "Schema.FactTable", "prefer_fact")
    assert "fact." in sel  # picked fact table


# -------------------------------------------------------
# Test: platform affinity resolves ambiguity
# -------------------------------------------------------

def test_platform_affinity_google():
    """Google platform should prefer GoogleAds table over MicrosoftAds."""
    tables = {
        "Schema.FactTable": {"Clicks": {}},
        "GoTicketsCoreEntity.GoogleAdsCampaign": {"CampaignName": {}},
        "GoTicketsCoreEntity.MicrosoftAdsCampaign": {"CampaignName": {}},
    }
    schema = PhysicalSchema(_mini_schema(tables))
    aliases = _make_aliases_from_list(
        "Schema.FactTable",
        ["GoTicketsCoreEntity.GoogleAdsCampaign", "GoTicketsCoreEntity.MicrosoftAdsCampaign"],
    )

    dim = _parse_dimension_spec("CampaignName")
    sel, grp = _resolve_dimension_expression(
        schema, aliases, dim, "Schema.FactTable", "prefer_fact", platform="google_ads"
    )
    assert "t1" in sel  # GoogleAdsCampaign


def test_platform_affinity_microsoft():
    """Microsoft platform should prefer MicrosoftAds table."""
    tables = {
        "Schema.FactTable": {"Clicks": {}},
        "GoTicketsCoreEntity.GoogleAdsCampaign": {"CampaignName": {}},
        "GoTicketsCoreEntity.MicrosoftAdsCampaign": {"CampaignName": {}},
    }
    schema = PhysicalSchema(_mini_schema(tables))
    aliases = _make_aliases_from_list(
        "Schema.FactTable",
        ["GoTicketsCoreEntity.GoogleAdsCampaign", "GoTicketsCoreEntity.MicrosoftAdsCampaign"],
    )

    dim = _parse_dimension_spec("CampaignName")
    sel, grp = _resolve_dimension_expression(
        schema, aliases, dim, "Schema.FactTable", "prefer_fact", platform="microsoft_ads"
    )
    assert "t2" in sel  # MicrosoftAdsCampaign


# -------------------------------------------------------
# Test: single non-fact table resolves ambiguity
# -------------------------------------------------------

def test_single_non_fact_resolves():
    """If column is on fact + one dim, and prefer_fact doesn't apply, pick the dim."""
    tables = {
        "Schema.FactTable": {"CampaignName": {}, "Clicks": {}},
        "Schema.DimCampaign": {"CampaignName": {}, "CampaignType": {}},
    }
    schema = PhysicalSchema(_mini_schema(tables))
    aliases = _make_aliases_from_list("Schema.FactTable", ["Schema.DimCampaign"])

    dim = _parse_dimension_spec("CampaignName")
    # Use "error" policy (not prefer_fact) - should still resolve via non-fact heuristic
    sel, grp = _resolve_dimension_expression(schema, aliases, dim, "Schema.FactTable", "error")
    assert "t1" in sel  # DimCampaign


# -------------------------------------------------------
# Test: true ambiguity raises AmbiguousDimensionError
# -------------------------------------------------------

def test_ambiguous_raises_error():
    """When column exists on 3+ tables with no resolution, raise AmbiguousDimensionError."""
    tables = {
        "Schema.FactTable": {"Clicks": {}},
        "Schema.DimA": {"Revenue": {}, "Name": {}},
        "Schema.DimB": {"Revenue": {}, "Code": {}},
        "Schema.DimC": {"Revenue": {}, "Type": {}},
    }
    schema = PhysicalSchema(_mini_schema(tables))
    aliases = _make_aliases_from_list("Schema.FactTable", ["Schema.DimA", "Schema.DimB", "Schema.DimC"])

    dim = _parse_dimension_spec("Revenue")
    with pytest.raises(AmbiguousDimensionError) as exc_info:
        _resolve_dimension_expression(schema, aliases, dim, "Schema.FactTable", "error")

    err = exc_info.value
    assert err.column == "Revenue"
    assert len(err.candidates) == 3
    assert "Schema.DimA" in err.candidates
    assert "Schema.DimB" in err.candidates
    assert "Schema.DimC" in err.candidates


def test_ambiguous_error_has_question():
    """AmbiguousDimensionError should carry a user-friendly question."""
    tables = {
        "Schema.FactTable": {"Clicks": {}},
        "Schema.DimA": {"Status": {}},
        "Schema.DimB": {"Status": {}},
    }
    schema = PhysicalSchema(_mini_schema(tables))
    aliases = _make_aliases_from_list("Schema.FactTable", ["Schema.DimA", "Schema.DimB"])

    dim = _parse_dimension_spec("Status")
    with pytest.raises(AmbiguousDimensionError) as exc_info:
        _resolve_dimension_expression(schema, aliases, dim, "Schema.FactTable", "error")

    err = exc_info.value
    assert "Status" in err.question
    assert "DimA" in err.question
    assert "DimB" in err.question


# -------------------------------------------------------
# Test: column not found still raises ValueError
# -------------------------------------------------------

def test_column_not_found_raises_value_error():
    """Missing column is a real error, not a disambiguation."""
    tables = {
        "Schema.FactTable": {"Clicks": {}},
        "Schema.DimA": {"Name": {}},
    }
    schema = PhysicalSchema(_mini_schema(tables))
    aliases = _make_aliases_from_list("Schema.FactTable", ["Schema.DimA"])

    dim = _parse_dimension_spec("NonExistentColumn")
    with pytest.raises(ValueError, match="not found"):
        _resolve_dimension_expression(schema, aliases, dim, "Schema.FactTable", "prefer_fact")
