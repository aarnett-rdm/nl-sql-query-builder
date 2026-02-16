from typing import Dict, Any, Optional, List
from .grain import resolve_default_grain_from_registry
from .validator import validate_logical_query, ValidatorContext
from .derived import expand_derived_metrics_from_registry
from .binding import bind_metrics_from_registry
from .schema_index import SchemaIndex
from .spec_builder import assemble_refined_structured_query_spec
from typing import Tuple
from .build_physical_plan_for_question import build_relationship_index, RelationshipIndex, RelationshipEdge
from collections import defaultdict, deque


CORE_ENTITY_TABLES = {"Event", "Venue", "Performer", "Category", "Calendar"}

def run_derived_and_validation_pass(
    question: str,
    schema: dict,
    schema_index: SchemaIndex,
    default_tz: str = "America/Los_Angeles",
    metric_registry: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Phase B.9 orchestrator:

    1) Start from refined spec (B.8).
    2) Attach derived metrics (currently: conversion rate).
    3) Mark target ROAS as unresolved bidding_goal.
    4) Resolve registry-backed metrics into renderable base bindings (ex: commission).
    5) Validate multi-fact metrics (e.g., exchange revenue).
    6) Update validation section with warnings/errors.
    """
    spec = assemble_refined_structured_query_spec(
        question, schema, schema_index, default_tz=default_tz
    )

    # Derived metrics
    attach_conversion_rate_derived_metric(spec, schema_index)
    mark_target_roas_as_unresolved_goal(spec)

    # --- Registry resolution (THIS is where your screenshot code belongs) ---
    # If you loaded metric_registry.json earlier into a variable like `metric_registry`,
    # pass it here OR rely on the optional parameter.
    if metric_registry is None:
        metric_registry = globals().get("metric_registry", None)

    if metric_registry:
        target_grain = spec.get("logical_query", {}).get("grain")
        platform = spec.get("platform")
        for m in spec.get("logical_query", {}).get("metrics", []):
            resolve_metric_from_registry(
                m,
                registry=metric_registry,
                physical_plan=spec["physical_plan"],
                platform=platform,
                target_grain=target_grain,
            )

    # Multi-fact validation
    warnings, errors = validate_multi_fact_metrics(spec, schema, schema_index)

    # Update validation block
    validation = spec.get("validation") or {}
    validation.setdefault("warnings", [])
    validation.setdefault("errors", [])
    validation["warnings"].extend(warnings)
    validation["errors"].extend(errors)

    # Basic overall validity flag
    validation["is_semantically_valid"] = len(errors) == 0
    validation["needs_clarification"] = any(
        w.get("type") == "metric_unreachable_fact" for w in warnings
    )

    spec["validation"] = validation
    return spec

def attach_conversion_rate_derived_metric(
    spec: Dict[str, Any],
    schema_index: SchemaIndex,
) -> None:
    """
    If there is a 'conversion rate' metric in logical_query.metrics (currently unresolved/derived),
    attempt to define it as:

        SUM(conversions) / NULLIF(SUM(clicks), 0)

    by looking for columns on the primary fact table whose names contain 'conversion' and 'click'.
    """
    logical_query = spec["logical_query"]
    physical_plan = spec["physical_plan"]

    # Identify primary fact table
    fact_tables = physical_plan.get("fact_tables") or []
    if not fact_tables:
        return
    fact_table = fact_tables[0]["table"]

    # Find conversion-rate metric entry
    metrics = logical_query.get("metrics", [])
    conv_rate_metric = None
    for m in metrics:
        sem = (m.get("semantic_name") or "").lower()
        alias = (m.get("alias") or "").lower()
        if "conversion rate" in sem or "conversion rate" in alias:
            conv_rate_metric = m
            break

    if not conv_rate_metric:
        return

    # Find conversion + click columns on the fact table
    conv_cols = find_fact_table_columns_by_name(fact_table, schema_index, "conversion")
    click_cols = find_fact_table_columns_by_name(fact_table, schema_index, "click")

    if not conv_cols or not click_cols:
        # Can't define it concretely; keep as derived and let downstream validation warn later if needed
        return

    conv_col = conv_cols[0]
    click_col = click_cols[0]

    # Update metric definition
    conv_rate_metric["metric_class"] = "ratio"
    conv_rate_metric["aggregation"] = "derived"
    conv_rate_metric["resolved_columns"] = [
        {"table": fact_table, "column": conv_col},
        {"table": fact_table, "column": click_col},
    ]
    conv_rate_metric["derived_expression"] = {
        "type": "ratio",
        "numerator": {
            "aggregation": "SUM",
            "table": fact_table,
            "column": conv_col,
        },
        "denominator": {
            "aggregation": "SUM",
            "table": fact_table,
            "column": click_col,
            "null_safe": True,
        },
        "sql_template": "SUM({conv}) * 1.0 / NULLIF(SUM({click}), 0)",
    }

def mark_target_roas_as_unresolved_goal(spec: Dict[str, Any]) -> None:
    """
    For 'target roas' metrics, explicitly mark them as bidding_goal / unresolved.
    """
    logical_query = spec["logical_query"]
    metrics = logical_query.get("metrics", [])
    for m in metrics:
        sem = (m.get("semantic_name") or "").lower()
        alias = (m.get("alias") or "").lower()
        if "target roas" in sem or "target roas" in alias:
            m["metric_class"] = "bidding_goal"
            m["aggregation"] = "derived"
            m["derived_expression"] = None
            m["resolved_columns"] = []
            # Keep unresolved; validation/UI can clarify later
            m["required"] = False  # so query can still run without it

def validate_multi_fact_metrics(
    spec: Dict[str, Any],
    schema: dict,
    schema_index: SchemaIndex,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Validate metrics that live on tables different from the primary fact table.

    - If a metric's table is unreachable from primary fact table via relationship graph,
      add a validation warning and mark that metric as non-required.
    - Return (warnings, errors).
    """
    logical_query = spec["logical_query"]
    physical_plan = spec["physical_plan"]

    fact_tables = physical_plan.get("fact_tables") or []
    if not fact_tables:
        return [], []

    primary_fact_table = fact_tables[0]["table"]

    # Build relationship index
    rel_index = build_relationship_index(schema)

    warnings: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for m in logical_query.get("metrics", []):
        # Determine metric table from resolved_columns OR direct table binding
        t = None
        resolved_cols = m.get("resolved_columns") or []
        if resolved_cols:
            t = resolved_cols[0].get("table")
        elif m.get("table"):
            t = m.get("table")

        if not t or t == primary_fact_table:
            continue

        path = find_shortest_join_path(rel_index, primary_fact_table, t)
        if path is None:
            warnings.append({
                "type": "metric_unreachable_fact",
                "metric_alias": m.get("alias"),
                "metric_table": t,
                "primary_fact_table": primary_fact_table,
                "message": (
                    f"Metric '{m.get('alias')}' comes from '{t}', which has no join path "
                    f"to primary fact table '{primary_fact_table}'. Metric will be dropped unless clarified."
                ),
            })
            m["required"] = False

    return warnings, errors

def resolve_metric_from_registry(
    m: Dict[str, Any],
    registry: Dict[str, Any],
    physical_plan: Dict[str, Any],
    platform: Optional[str] = None,
    target_grain: Optional[str] = None,
) -> None:
    """
    Mutates metric dict `m` in-place using the metric_registry.json.

    What it does:
    - Fixes metrics incorrectly tagged as derived when they are base metrics (ex: commission).
    - Binds table/column from registry base_columns (preferring tables that exist in this physical_plan).
    - Optionally enforces registry rollup strategy by marking metric as non-required + warning hint fields.
      (We avoid hard exceptions here to keep notebook flow consistent.)
    """
    metrics_map = (registry or {}).get("metrics", {})

    key = (m.get("registry_key") or m.get("semantic_name") or m.get("alias") or "").strip().lower()
    reg = metrics_map.get(key)
    if not reg:
        return  # unknown metric; let existing logic handle it

    # Build list of fact tables present in current plan (strings)
    plan_fact_tables: List[str] = []
    for ft in (physical_plan.get("fact_tables") or []):
        if isinstance(ft, dict) and ft.get("table"):
            plan_fact_tables.append(ft["table"])
        elif isinstance(ft, str):
            plan_fact_tables.append(ft)

    # Registry aggregation defaults (registry uses default_aggregation)
    reg_default_agg = (reg.get("default_aggregation") or "sum").strip().upper()
    curr_agg = (m.get("aggregation") or m.get("default_aggregation") or "").strip().upper()

    # Registry may use "derived_formula" OR "derived_expression" depending on version
    reg_formula = reg.get("derived_formula")
    if reg_formula is None:
        reg_formula = reg.get("derived_expression")

    # 1) If metric is marked DERIVED but registry has no derived formula, normalize to base agg
    if curr_agg == "DERIVED" and not reg_formula:
        m["aggregation"] = reg_default_agg
        m["metric_class"] = m.get("metric_class") or reg.get("metric_class")
        m.pop("derived_expression", None)
        m["resolved_columns"] = []
        curr_agg = reg_default_agg

    # 2) If still derived, fill derived_expression from registry if missing
    if (m.get("aggregation") or "").strip().upper() == "DERIVED":
        if not m.get("derived_expression") and reg_formula:
            m["derived_expression"] = reg_formula

    # 3) Bind base table/column if missing
    tbl = m.get("table")
    col = m.get("column")
    resolved_cols = m.get("resolved_columns") or []

    if (not (tbl and col)) and (not resolved_cols) and reg.get("base_columns"):
        base_cols = reg["base_columns"]

        # Prefer a base column whose table is already in the current plan
        chosen = None
        for bc in base_cols:
            if bc.get("table") in plan_fact_tables:
                chosen = bc
                break
        if chosen is None:
            chosen = base_cols[0]

        m["table"] = chosen.get("table")
        m["column"] = chosen.get("column")

    # 4) Rollup policy: don't explode the notebook; instead, mark metric non-required if it can't roll up cleanly
    rollup = reg.get("rollup") or {}
    if rollup.get("strategy") == "attribution_required":

        # In THIS notebook, target_grain is a dict like {"entity_grain": "...", "time_grain": "..."}
        grain_key = None
        if isinstance(target_grain, dict):
            v = target_grain.get("entity_grain")
            if isinstance(v, str) and v.strip():
                grain_key = v.strip().lower()
        elif isinstance(target_grain, str) and target_grain.strip():
            grain_key = target_grain.strip().lower()

        raw_supported = reg.get("supported_grains") or []
        supported_keys = set()
        for g in raw_supported:
            if isinstance(g, str) and g.strip():
                supported_keys.add(g.strip().lower())
            elif isinstance(g, dict):
                # just in case registry ever stores dict grains
                v = g.get("entity_grain") or g.get("name") or g.get("grain")
                if isinstance(v, str) and v.strip():
                    supported_keys.add(v.strip().lower())

        if grain_key and supported_keys and grain_key not in supported_keys:
            m["required"] = False
            m.setdefault("notes", [])
            m["notes"].append(
                f"Registry rollup policy: attribution_required (requested entity_grain={grain_key}, supported={sorted(list(supported_keys))})"
            )

def find_shortest_join_path(
    rel_index: RelationshipIndex,
    start_table: str,
    target_table: str,
) -> Optional[List[RelationshipEdge]]:
    """
    BFS to find a shortest path of RelationshipEdge objects
    from start_table to target_table.

    Preference: paths that go through core entity tables are explored first.
    """
    if start_table == target_table:
        return []

    visited = set([start_table])
    # queue holds (current_table, path_edges)
    queue = deque([(start_table, [])])

    while queue:
        current, path = queue.popleft()
        neighbors = rel_index.adjacency.get(current, [])

        # Explore neighbors, but put core entities first to bias BFS a bit
        core_neighbors = []
        other_neighbors = []
        for edge in neighbors:
            if edge.to_table in CORE_ENTITY_TABLES:
                core_neighbors.append(edge)
            else:
                other_neighbors.append(edge)
        ordered = core_neighbors + other_neighbors

        for edge in ordered:
            nxt = edge.to_table
            if nxt in visited:
                continue
            new_path = path + [edge]
            if nxt == target_table:
                return new_path
            visited.add(nxt)
            queue.append((nxt, new_path))

    return None  # unreachable