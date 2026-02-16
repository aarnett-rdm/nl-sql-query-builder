from typing import Dict, Any, List, Optional
from .schema_index import SchemaIndex, normalize_term
from .build_physical_plan_for_question import build_physical_plan_for_question, build_group_by_and_select_list, build_where_clauses_from_time_window
from typing import Tuple
import re

def assemble_refined_structured_query_spec(
    question: str,
    schema: dict,
    schema_index: SchemaIndex,
    default_tz: str = "America/Los_Angeles",
) -> Dict[str, Any]:
    """
    Phase B.8 orchestrator:

    1) Get base full spec (B.7).
    2) Get B.6 debug info (grounded dimensions).
    3) Realign dimensions to primary fact table.
    4) Add text-derived filters (like 'mlb campaigns').
    5) Rebuild group_by / select_list / order_by.

    Returns a refined full StructuredQuerySpec.
    """
    # Step 1: base spec
    base_spec = assemble_full_structured_query_spec(
        question, schema, schema_index, default_tz=default_tz
    )

    # Step 2: B.6 debug (for dimension choices)
    b6 = build_physical_plan_for_question(
        question, schema, schema_index, default_tz=default_tz
    )

    # Step 3: Realign dimensions
    realign_dimensions_to_fact_table(base_spec, b6["debug"])

    # Step 4: Add filters from free text (e.g., 'mlb campaigns')
    add_text_derived_filters(question, base_spec, b6["debug"], schema_index)

    # Step 5: Rebuild group_by / select_list / order_by with updated logical_query
    rebuild_physical_lists_after_refinement(base_spec, schema_index)

    return base_spec

def assemble_full_structured_query_spec(
    question: str,
    schema: dict,
    schema_index: SchemaIndex,
    default_tz: str = "America/Los_Angeles",
) -> Dict[str, Any]:
    """
    Final assembly:
      - Reuse B.6 to get interpretation + logical_query + physical_plan skeleton
      - Fill physical_plan.group_by / select_list / where_clauses / order_by
      - Attach time_window + sorting + limit into logical_query
    """
    b6 = build_physical_plan_for_question(question, schema, schema_index, default_tz=default_tz)

    interpretation = b6["interpretation"]
    logical_query = b6["logical_query"]
    physical_plan = b6["physical_plan"]

    fact_tables = physical_plan.get("fact_tables") or []
    primary_fact_table = fact_tables[0]["table"] if fact_tables else None

    # 1) Group-by + select-list
    group_by, select_list = build_group_by_and_select_list(logical_query, primary_fact_table or "")

    # 2) Time-window-based where clauses
    time_window = logical_query.get("time_window")
    where_from_time, resolved_date_col = build_where_clauses_from_time_window(
        time_window, primary_fact_table or "", schema_index
    )

    # 3) Sorting + limit
    sorting, limit_block = build_basic_sorting_and_limit(logical_query)

    # 4) Update logical_query
    logical_query["sorting"] = sorting
    logical_query["limit"] = limit_block
    logical_query["time_window"] = time_window  # (updated with resolved_column if we found one)
    logical_query = normalize_logical_query_metrics_and_time(logical_query, schema)
    
    # 5) Update physical_plan
    physical_plan["group_by"] = group_by
    physical_plan["select_list"] = select_list
    physical_plan["where_clauses"] = where_from_time
    # order_by: mirror sorting for now
    physical_plan["order_by"] = [
        {
            "expression": s.get("semantic_name"),
            "direction": s.get("direction", "desc"),
            "source": "logical_query.sorting",
        }
        for s in sorting
    ]

    # 6) Wrap into full StructuredQuerySpec-style dict
    structured_spec = {
        "request_metadata": {
            "request_id": "req-demo-mlb-0001",
            "nl_query": question,
            "generated_at_utc": None,
            "user_locale": "en-US",
            "warehouse_dialect": "tsql",
            "default_platform": "google_ads",
            "default_time_zone": default_tz,
        },
        "interpretation": interpretation,
        "logical_query": logical_query,
        "physical_plan": physical_plan,
        "validation": {
            "is_semantically_valid": True,  # real implementation would run checks here
            "errors": [],
            "warnings": [],
            "confidence_scores": {},
            "needs_clarification": False,
            "clarification_questions": [],
            "llm_notes": [],
        },
    }

    return structured_spec
def build_basic_sorting_and_limit(
    logical_query: Dict[str, Any],
) -> (List[Dict[str, Any]], Dict[str, Any]):
    """
    Provide a default sorting + limit if none specified:
      - Sort by 'cost' metric desc if present
      - Otherwise no ordering
      - Default limit = 100 rows
    """
    sorting = logical_query.get("sorting") or []

    if not sorting:
        # Look for a metric with semantic_name containing 'cost'
        for m in logical_query.get("metrics", []):
            sem = (m.get("semantic_name") or "").lower()
            if "cost" in sem:
                sorting = [{
                    "field_type": "metric",
                    "semantic_name": sem,
                    "direction": "desc",
                    "nulls": "last",
                }]
                break

    limit_block = logical_query.get("limit")
    if not limit_block:
        limit_block = {
            "value": 100,
            "applied": True,
            "reason": "Default limit applied by planner.",
        }

    return sorting, limit_block

def normalize_logical_query_metrics_and_time(logical_query: dict, schema: dict) -> dict:
    """
    Goal B post-processing:
    - enforce better metric defaults (clicks sum, target roas avg)
    - normalize time_window to use a calendar dimension (DimCalendar / OGCalendar) when available
      instead of fact timestamps.

    Expects `schema` to have the shape:
        {
          "tables": [ { "logical_name": ..., "columns": [...] }, ... ],
          "relationships": [...]
        }
    """
    from copy import deepcopy

    spec = deepcopy(logical_query)

    # ---------- 1) Normalize metric aggregations ----------
    for m in spec.get("metrics", []):
        name = (m.get("semantic_name") or "").strip().lower()
        agg = (m.get("aggregation") or "").strip().lower()

        # clicks should always be SUM at campaign/adgroup/etc grain
        if name == "clicks":
            if agg != "sum":
                m["aggregation"] = "sum"

        # target roas value is a base column metric averaged, not "derived"
        if name == "target roas value":
            if not agg or agg == "derived":
                m["aggregation"] = "avg"
            if "derived_expression" in m:
                m["derived_expression"] = None
            if not m.get("metric_class"):
                m["metric_class"] = "target"

    # ---------- 2) Normalize time window to a calendar dimension ----------
    tw = spec.get("time_window") or {}
    field = (tw.get("field") or {})
    resolved_col = (field.get("resolved_column") or {})

    table = resolved_col.get("table")
    column = resolved_col.get("column")

    # Only rewrite if we're currently pointing at the fact table timestamp
    if table == "GoogleAdsCampaignPerformanceMetric" and column == "ChangedTimestampUtc":

        # 2a) Find a calendar table: prefer ones with 'dimcalendar' or 'calendar' in the logical_name
        calendar_table_def = None
        for tbl_def in schema.get("tables", []):
            logical_name = (tbl_def.get("logical_name") or "").lower()
            if "dimcalendar" in logical_name:
                calendar_table_def = tbl_def
                break

        if calendar_table_def is None:
            # fall back: any table with 'calendar' in the logical name
            for tbl_def in schema.get("tables", []):
                logical_name = (tbl_def.get("logical_name") or "").lower()
                if "calendar" in logical_name:
                    calendar_table_def = tbl_def
                    break

        if calendar_table_def:
            calendar_table_name = calendar_table_def["logical_name"]
            col_names = {c["name"] for c in calendar_table_def.get("columns", [])}

            # 2b) Pick a date-like column:
            #     - prefer PST_Date or UTC_Date
            #     - otherwise any column with 'date' in the name
            preferred = None
            for candidate in ("PST_Date", "UTC_Date", "Date"):
                if candidate in col_names:
                    preferred = candidate
                    break

            if preferred is None:
                # last resort: any column whose name contains 'date'
                date_like = [c for c in col_names if "date" in c.lower()]
                if date_like:
                    preferred = sorted(date_like)[0]

            if preferred:
                spec.setdefault("time_window", {})
                spec["time_window"].setdefault("field", {})
                spec["time_window"]["field"]["resolved_column"] = {
                    "table": calendar_table_name,
                    "column": preferred,
                }

    return spec

def realign_dimensions_to_fact_table(
    full_spec: Dict[str, Any],
    b6_debug: Dict[str, Any],
) -> None:
    """
    Ensure logical_query.dimensions use the same chosen columns as
    the B.6 grounded_dimensions (which were adjusted to the primary fact table).

    Modifies full_spec in place.
    """
    logical_query = full_spec["logical_query"]
    physical_plan = full_spec["physical_plan"]

    fact_tables = physical_plan.get("fact_tables") or []
    primary_fact_table = fact_tables[0]["table"] if fact_tables else None
    if not primary_fact_table:
        return

    grounded_dimensions: List[GroundedDimension] = b6_debug.get("grounded_dimensions") or []

    # Build a map: norm_term -> chosen ColumnIndexEntry
    dim_choice_by_norm: Dict[str, ColumnIndexEntry] = {}
    for gd in grounded_dimensions:
        if gd.chosen:
            dim_choice_by_norm[gd.norm_term] = gd.chosen

    # Patch logical_query.dimensions
    for dim in logical_query.get("dimensions", []):
        norm = normalize_term(dim.get("semantic_name") or dim.get("alias") or "")
        chosen_entry = dim_choice_by_norm.get(norm)
        if chosen_entry:
            dim["resolved_columns"] = [{
                "table": chosen_entry.table,
                "column": chosen_entry.column,
            }]

def add_text_derived_filters(
    question: str,
    full_spec: Dict[str, Any],
    b6_debug: Dict[str, Any],
    schema_index: SchemaIndex,
) -> None:
    """
    Add filters derived from free text such as 'mlb campaigns'.

    - For 'X campaigns', add a LIKE filter on CampaignName (or best campaign column)
      using '%X%' (case-insensitive from SQL perspective).
    """
    logical_query = full_spec["logical_query"]
    physical_plan = full_spec["physical_plan"]

    fact_tables = physical_plan.get("fact_tables") or []
    primary_fact_table = fact_tables[0]["table"] if fact_tables else None
    if not primary_fact_table:
        return

    filters = logical_query.get("filters") or []

    # 1) 'X campaigns' -> campaign name LIKE '%X%'
    token = extract_campaign_token_from_question(question)
    if token:
        campaign_col = find_campaign_name_column(primary_fact_table, schema_index)
        if campaign_col:
            filters.append({
                "semantic_name": "campaign_name_contains_token",
                "target_role": "dimension",
                "resolved_column": {
                    "table": primary_fact_table,
                    "column": campaign_col,
                },
                "operator": "LIKE",
                "values_raw": [f"%{token.upper()}%"],  # we can store as uppercase for clarity
                "values_resolved": [f"%{token.upper()}%"],
                "value_semantic_type": "string",
                "allowed_enum_values": None,
                "enum_valid": True,
                "source": "nl_text",
                "confidence": 0.9,
            })

            # Also add a physical where_clause
            where_clause = {
                "predicate_type": "string_pattern",
                "table": primary_fact_table,
                "column": campaign_col,
                "operator": "LIKE",
                "value": f"%{token.upper()}%",
                "source_filter": "nl_text_campaigns",
            }
            existing_where = physical_plan.get("where_clauses") or []
            existing_where.append(where_clause)
            physical_plan["where_clauses"] = existing_where

    logical_query["filters"] = filters

def extract_campaign_token_from_question(question: str) -> Optional[str]:
    """
    Look for phrases like 'mlb campaigns', 'nfl campaigns', 'concert campaigns'.
    Returns the token before 'campaigns' (lowercased), or None.
    """
    m = re.search(r"\b([a-z0-9]+)\s+campaigns?\b", question.lower())
    if m:
        token = m.group(1).strip()
        if token:
            return token
    return None


def find_campaign_name_column(
    fact_table: str,
    schema_index: SchemaIndex,
) -> Optional[str]:
    """
    Try to locate a 'CampaignName' (or similar) column on the fact table.

    Priority:
      - column containing both 'campaign' and 'name'
      - column containing 'campaignname'
      - fallback: column containing 'campaign'
    """
    cols = schema_index.columns_by_table.get(fact_table, {})
    best = None

    for cname in cols.keys():
        cl = cname.lower()
        if "campaign" in cl and "name" in cl:
            return cname
        if "campaignname" in cl:
            best = cname

    if best:
        return best

    for cname in cols.keys():
        if "campaign" in cname.lower():
            return cname

    return None

def rebuild_physical_lists_after_refinement(
    full_spec: Dict[str, Any],
    schema_index: SchemaIndex,
) -> None:
    """
    After we change dimensions/filters, we need to rebuild:
      - physical_plan.group_by
      - physical_plan.select_list
      - physical_plan.order_by
    based on the updated logical_query.

    Modifies full_spec in place.
    """
    logical_query = full_spec["logical_query"]
    physical_plan = full_spec["physical_plan"]

    fact_tables = physical_plan.get("fact_tables") or []
    primary_fact_table = fact_tables[0]["table"] if fact_tables else None

    if not primary_fact_table:
        return

    # Rebuild group_by + select_list using updated logical_query
    group_by, select_list = build_group_by_and_select_list(logical_query, primary_fact_table)
    physical_plan["group_by"] = group_by
    physical_plan["select_list"] = select_list

    # Rebuild sorting + limit and reflect into order_by
    sorting, limit_block = build_basic_sorting_and_limit(logical_query)
    logical_query["sorting"] = sorting
    logical_query["limit"] = limit_block

    physical_plan["order_by"] = [
        {
            "expression": s.get("semantic_name"),
            "direction": s.get("direction", "desc"),
            "source": "logical_query.sorting",
        }
        for s in sorting
    ]

