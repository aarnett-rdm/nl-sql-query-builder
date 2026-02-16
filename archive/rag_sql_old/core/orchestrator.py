from typing import Dict, Any, Tuple, List

from .planner import run_derived_and_validation_pass
from .sanitize import sanitize_spec_for_validation, normalize_platform_and_campaign_token_filters
from .grain import resolve_default_grain_from_registry
from .validator import ValidatorContext, validate_logical_query
from .derived import expand_derived_metrics_from_registry
from .binding import bind_metrics_from_registry
from .sql_builder import build_sql_from_spec
from .schema_index import build_schema_index
from copy import deepcopy
from .build_physical_plan_for_question import build_relationship_index



# NOTE:
# This function is the frozen NL→SQL core.
# Product layers must treat it as a black box.
# ===============================
# GOAL F – SINGLE ORCHESTRATOR ENTRYPOINT
# ===============================



def orchestrate_nl_to_sql(
    question: str,
    schema: dict,
    metric_registry: dict,
    domain_policy: dict,
    filter_config: dict,
    validator_policy: dict,
    default_tz: str = "America/Los_Angeles",
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Goal F: NL question -> planner -> sanitizer -> validator -> registry-driven grain+binding -> SQL builder

    Returns:
      {
        "status": "ok" | "needs_clarification" | "error",
        "messages": [...],
        "sql": str | None,
        "spec": Dict[str, Any],   # full structured spec (useful for Goal G UI)
      }
    """
    schema_index = build_schema_index(schema)
    # 1) Planner (Goal B.9)
    try:
        spec = run_derived_and_validation_pass(
            question=question,
            schema=schema,
            schema_index=schema_index,
            default_tz=default_tz,
            metric_registry=metric_registry,
        )
    except Exception as e:
        return {
            "status": "error",
            "messages": [{"type": "planner_error", "detail": f"Planner failed: {repr(e)}"}],
            "sql": None,
            "spec": None,
        }

    if not isinstance(spec, dict) or "logical_query" not in spec:
        return {
            "status": "error",
            "messages": [{"type": "planner_error", "detail": "Planner did not return a valid spec with logical_query."}],
            "sql": None,
            "spec": spec,
        }
    # 1.5) Normalize physical_plan.fact_table shape for sql_builder compatibility    
    pp = (spec.get("physical_plan") or {})
    if not isinstance(pp.get("fact_table"), dict):
        fts = pp.get("fact_tables") or []
        if fts and isinstance(fts[0], dict):
            pp["fact_table"] = {
                "table": fts[0].get("table") or fts[0].get("logical_name")
            }
    spec["physical_plan"] = pp

    # 2) Sanitize + normalize planner output (deterministic; no silent pruning)
    spec, sanitize_msgs = sanitize_spec_for_validation(spec, metric_registry=metric_registry)
    spec, norm_msgs = normalize_platform_and_campaign_token_filters(spec, schema=schema)

    messages = []
    messages.extend(sanitize_msgs)
    messages.extend(norm_msgs)

    spec, role_msgs = mark_requested_metric_roles(spec)
    messages.extend(role_msgs)

    # 2.5) REGISTRY DEFAULT GRAIN RESOLUTION (pre-validation)
    spec, grain_msgs = resolve_default_grain_from_registry(spec, metric_registry)
    messages.extend(grain_msgs)

    logical_query = spec.get("logical_query")

    # 3) Validator (Goal E)
    try:
        ctx = ValidatorContext.from_project(schema=schema,
                                           schema_index=schema_index,
                                           metric_registry=metric_registry,
                                           domain_policy=domain_policy,
                                           filter_config=filter_config,
                                           validator_policy=validator_policy,
                                           )
        if debug:
            print("DEBUG pre-validator grain =", (spec.get("logical_query") or {}).get("grain"))
            print("DEBUG logical_query.filters =", (spec.get("logical_query") or {}).get("filters"))

        validation = validate_logical_query(
            logical_query=logical_query,
            ctx=ctx,
            nl_question=question,
        )
    except Exception as e:
        messages.append({
            "type": "validator_error",
            "detail": f"Validator crashed; continuing without hard block: {repr(e)}"
        })
        validation = {
            "status": "ok",
            "messages": [],
            "validated_spec": logical_query,
        }

    status = validation.get("status", "error")
    messages.extend(validation.get("messages") or [])
    validated = validation.get("validated_spec")

    # 4) If not OK, return structured messages, NO SQL
    if status != "ok" or not validated:
        return {
            "status": status,
            "messages": messages,
            "sql": None,
            "spec": spec,
        }

    # 5) Merge validated logical_query back into spec
    spec["logical_query"] = validated

    # 5.1) Ensure platform.resolved exists if validator only returned requested
    plat = (spec.get("logical_query") or {}).get("platform")
    if isinstance(plat, dict):
        if not plat.get("resolved") and plat.get("requested"):
            plat["resolved"] = plat["requested"]
            spec["logical_query"]["platform"] = plat

    # 5.15) Normalize metric aliases (ctr/cpc/rpc) to registry keys (CRITICAL)
    spec, name_msgs = normalize_metric_names_with_registry(spec, metric_registry)
    messages.extend(name_msgs)

    # 5.25) REGISTRY DEFAULT GRAIN RESOLUTION (post-validation, to prevent validator overriding with "none")
    spec, post_grain_msgs = resolve_default_grain_from_registry(spec, metric_registry)
    if post_grain_msgs:
        # Keep these as warnings to make the override explicit
        messages.extend(post_grain_msgs)

    if debug:
        print("DEBUG post-validator grain =", (spec.get("logical_query") or {}).get("grain"))

    # 5.3) Expand derived metrics from registry (CRITICAL)
    spec, derived_msgs = expand_derived_metrics_from_registry(spec, metric_registry)
    messages.extend(derived_msgs)
    
    # 5.5) Bind metrics to concrete table/column via registry (no guessing)
    try:
        spec, bind_msgs = bind_metrics_from_registry(spec, metric_registry, strict=True)
        messages.extend(bind_msgs)
    except Exception as e:
        return {
            "status": "error",
            "messages": messages + [{
                "type": "binding_error",
                "detail": f"Metric binding failed: {repr(e)}"
            }],
            "sql": None,
            "spec": spec,
        }

    # 6) Rebuild physical where clauses from validated logical filters (so SQL builder uses correct table/column)
    spec, pp_msgs = rebuild_physical_where_clauses_from_logical_filters(spec)
    messages.extend(pp_msgs)

    # 7) Build SQL
    try:
        sql = build_sql_from_spec(spec)
    except Exception as e:
        return {
            "status": "error",
            "messages": messages + [{"type": "sql_build_error", "detail": f"SQL builder failed: {repr(e)}"}],
            "sql": None,
            "spec": spec,
        }

    return {
        "status": "ok",
        "messages": messages,
        "sql": sql,
        "spec": spec,
    }

def mark_requested_metric_roles(spec: dict) -> tuple[dict, list]:
    """
    Mark all planner-originated metrics as explicitly user-requested.
    This prevents derived-expansion from accidentally promoting dependencies
    into visible SELECT columns.
    """
    msgs = []
    lq = spec.get("logical_query") or {}
    metrics = lq.get("metrics") or []

    if not metrics:
        return spec, msgs

    updated = 0
    for m in metrics:
        if not isinstance(m, dict):
            continue
        if not m.get("metric_role"):
            m["metric_role"] = "requested"
            updated += 1

    if updated:
        msgs.append({
            "type": "interpretation_warning",
            "detail": f"Marked {updated} planner metrics as metric_role='requested'."
        })

    lq["metrics"] = metrics
    spec["logical_query"] = lq
    return spec, msgs

def rebuild_physical_where_clauses_from_logical_filters(
    spec: Dict[str, Any]
) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    """
    Source of truth: logical_query.filters (after sanitizer/normalizer + validation).
    Rebuild physical_plan.where_clauses so SQL builder uses grounded filter columns.
    Also inject joins for any non-fact filter tables.
    """
    spec2 = deepcopy(spec)
    msgs: List[Dict[str, str]] = []

    lq = spec2.get("logical_query") or {}
    pp = spec2.get("physical_plan") or {}

    fact_table = _resolve_fact_table_name(pp)
    if not fact_table:
        fts = pp.get("fact_tables") or []
        if fts and isinstance(fts[0], dict):
            fact_table = fts[0].get("table")

    new_where: List[Dict[str, Any]] = []

    # Keep existing time_range predicates if planner put them there
    for wc in (pp.get("where_clauses") or []):
        if wc.get("predicate_type") == "time_range":
            new_where.append(wc)

    for f in (lq.get("filters") or []):
        if not isinstance(f, dict):
            continue

        rc = f.get("resolved_column") or {}
        table = rc.get("table")
        col = rc.get("column")
        op = (f.get("operator") or "").upper()

        # normalize value for string patterns
        val = None
        if op in ("LIKE", "CONTAINS", "ILIKE"):
            vals = f.get("values_resolved") or f.get("values_raw") or []
            if vals:
                val = vals[0]
            # map CONTAINS -> LIKE with %...%
            if op == "CONTAINS":
                op = "LIKE"
                if isinstance(val, str) and "%" not in val:
                    val = f"%{val}%"

        if table and col and val is not None and op == "LIKE":
            # inject join if filter table is not the fact table
            if fact_table and table != fact_table:
                ensure_join_for_filter_table(spec2, table)

            new_where.append({
                "predicate_type": "string_pattern",
                "table": table,
                "column": col,
                "value": val
            })

    pp["where_clauses"] = new_where
    spec2["physical_plan"] = pp

    msgs.append({
        "type": "interpretation_warning",
        "detail": "Rebuilt physical_plan.where_clauses from logical_query.filters so SQL builder uses grounded filter columns."
    })

    return spec2, msgs

def normalize_metric_names_with_registry(
    spec: dict,
    metric_registry: dict,
) -> tuple[dict, list]:
    """
    Rewrite logical_query.metrics[*].semantic_name using registry alias map.
    Deterministic. No guessing beyond explicit registry aliases.
    """
    msgs = []
    lq = spec.get("logical_query") or {}
    metrics = lq.get("metrics") or []
    if not metrics:
        return spec, msgs

    # Support multiple possible alias-map locations
    aliases = (
        metric_registry.get("aliases")
        or metric_registry.get("canonical_aliases")
        or metric_registry.get("canonical_metric_aliases")
        or metric_registry.get("metric_aliases")
        or {}
    )

    # If aliases are per-metric, build a global alias map too
    reg_metrics = metric_registry.get("metrics") or {}
    if not aliases and isinstance(reg_metrics, dict):
        aliases = {}
        for mname, mdef in reg_metrics.items():
            if not isinstance(mdef, dict):
                continue
            for a in (mdef.get("aliases") or mdef.get("canonical_aliases") or []):
                if isinstance(a, str) and a:
                    aliases[a] = mname

    # Make lookup case-tolerant
    aliases_lc = {str(k).lower(): v for k, v in aliases.items()}

    for m in metrics:
        if not isinstance(m, dict):
            continue

        name = m.get("semantic_name") or m.get("name")
        if not name:
            continue

        canon = aliases_lc.get(str(name).lower())
        if canon and canon != name:
            m["semantic_name"] = canon
            msgs.append({
                "type": "interpretation_warning",
                "detail": f"Normalized metric alias '{name}' -> '{canon}' using registry."
            })

    lq["metrics"] = metrics
    spec["logical_query"] = lq
    return spec, msgs

def _resolve_fact_table_name(pp: dict) -> str | None:
    # sql_builder expects pp["fact_table"] as dict; but we handle both shapes
    ft_obj = pp.get("fact_table") or {}
    if isinstance(ft_obj, dict):
        ft = ft_obj.get("table") or ft_obj.get("logical_name")
        if ft:
            return ft

    fts = pp.get("fact_tables") or []
    if fts and isinstance(fts[0], dict):
        return fts[0].get("table") or fts[0].get("logical_name")

    if isinstance(pp.get("fact_table"), str):
        return pp.get("fact_table")

    return None


def ensure_join_for_filter_table(spec: dict, table: str) -> None:
    """
    Ensure there is a join from fact table -> `table` in physical_plan['joins']
    in the exact shape sql_builder.build_from_and_joins expects.
    Only injects DIRECT (1-hop) joins using schema relationships.
    """
    if not table:
        return

    pp = spec.get("physical_plan") or {}
    schema = spec.get("schema") or {}

    fact_table = _resolve_fact_table_name(pp)
    if not fact_table or table == fact_table:
        return

    # If join already exists, no-op
    joins = pp.get("joins") or []
    for j in joins:
        rt = j.get("right_table") or j.get("table")
        if rt == table:
            return

    rel_idx = build_relationship_index(schema)
    edges = rel_idx.adjacency.get(fact_table, []) if hasattr(rel_idx, "adjacency") else []
    if not edges:
        return

    # Find direct edge fact_table -> table
    for e in edges:
        if getattr(e, "to_table", None) != table:
            continue

        from_cols = list(getattr(e, "from_columns", []) or [])
        to_cols = list(getattr(e, "to_columns", []) or [])
        if not from_cols or not to_cols:
            continue

        # Inject one join per column-pair (supports composite keys)
        for lc, rc in zip(from_cols, to_cols):
            joins.append({
                "join_type": (getattr(e, "join_type", None) or "LEFT").upper(),
                "right_table": table,
                "left_column": lc,
                "right_column": rc,
                # left_alias omitted: sql_builder defaults to fact alias
            })

        pp["joins"] = joins
        spec["physical_plan"] = pp
        return
