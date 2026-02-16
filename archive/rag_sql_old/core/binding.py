from typing import Dict, List, Tuple, Any

def bind_metrics_from_registry(
    spec: dict,
    metric_registry: dict,
    *,
    strict: bool = True,
) -> tuple[dict, list]:
    """
    Deterministically bind base metrics to (table, column) using:
      - metric_registry preferred_fact_table (by grain/platform)
      - metric_registry base_columns (table/column candidates)

    Returns (spec, messages). Raises ValueError if strict and cannot bind.
    """
    msgs = []

    lq = spec.get("logical_query") or {}
    metrics = lq.get("metrics") or []

    # Platform/grain shape can vary; be defensive.
    platform = None
    plat_obj = lq.get("platform")
    if isinstance(plat_obj, dict):
        resolved = plat_obj.get("resolved") or plat_obj.get("requested")
        if isinstance(resolved, list) and resolved:
            platform = resolved[0]
        elif isinstance(resolved, str):
            platform = resolved

    grain = None
    grain_obj = lq.get("grain")
    if isinstance(grain_obj, dict):
        # Prefer the actual physical grain field you use; fall back safely.
        grain = grain_obj.get("resolved") or grain_obj.get("default") or grain_obj.get("time_grain") or grain_obj.get("entity_grain")
    elif isinstance(grain_obj, str):
        grain = grain_obj

    # Registry metric lookup
    reg_metrics = metric_registry.get("metrics") or metric_registry.get("registry") or {}
    if not isinstance(reg_metrics, dict):
        reg_metrics = {}

    def _is_already_bound(m: dict) -> bool:
        return bool(m.get("table") and m.get("column")) or bool(m.get("resolved_column"))

    for m in metrics:
        if not isinstance(m, dict):
            continue

        # Skip metrics already bound
        if _is_already_bound(m):
            continue

        name = m.get("semantic_name") or m.get("name")
        if not name:
            continue

        reg = reg_metrics.get(name)
        if not reg:
            # If validator allowed it, keep it as-is and let SQL builder fail; but provide message
            msgs.append({
                "type": "binding_warning",
                "detail": f"Metric '{name}' not found in registry during binding step."
            })
            continue

        # If derived, don't bind as base (your derived-expansion should handle it)
        if reg.get("derived_formula") or m.get("aggregation") == "derived" or m.get("metric_class") == "derived":
            # Ensure it isn't mistakenly treated as base later
            m["metric_class"] = m.get("metric_class") or "derived"
            continue

        base_cols = reg.get("base_columns") or []
        # base_columns might be list[dict] or dict; normalize to list[dict(table,column)]
        candidates = []
        if isinstance(base_cols, list):
            for bc in base_cols:
                if isinstance(bc, dict) and bc.get("table") and bc.get("column"):
                    candidates.append({"table": bc["table"], "column": bc["column"]})
        elif isinstance(base_cols, dict):
            # If someone stored as {table: col}
            for t, c in base_cols.items():
                candidates.append({"table": t, "column": c})

        if not candidates:
            msg = f"Cannot bind base metric '{name}': registry has no base_columns."
            if strict:
                raise ValueError(msg)
            msgs.append({"type": "binding_warning", "detail": msg})
            continue

        # Choose preferred fact table if available
        preferred = None
        pft = reg.get("preferred_fact_table") or {}
        if grain and platform and isinstance(pft, dict):
            preferred = (pft.get(grain) or {}).get(platform)
            if isinstance(preferred, list) and preferred:
                preferred = preferred[0]
            elif isinstance(preferred, str):
                preferred = preferred

        chosen = None
        if preferred:
            for cand in candidates:
                if cand["table"] == preferred:
                    chosen = cand
                    break

        # Fallback: if only one candidate, choose it; otherwise error/clarify
        if not chosen:
            if len(candidates) == 1:
                chosen = candidates[0]
            else:
                # Multiple possible base tables and no preference => do NOT guess
                msg = (
                    f"Cannot bind '{name}': multiple base_columns candidates and no "
                    f"preferred_fact_table match for grain={grain}, platform={platform}."
                )
                if strict:
                    raise ValueError(msg)
                msgs.append({
                    "type": "clarification_request",
                    "reason": "ambiguous_metric_source",
                    "metric": name,
                    "grain": grain,
                    "platform": platform,
                    "options": [f"{c['table']}.{c['column']}" for c in candidates][:10],
                })
                continue

        # Write in the fields your SQL builder is looking for
        m["table"] = chosen["table"]
        m["column"] = chosen["column"]
        m["resolved_column"] = {"table": chosen["table"], "column": chosen["column"]}

    return spec, msgs