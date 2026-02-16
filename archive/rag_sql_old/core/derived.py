from typing import Dict, List, Tuple

def _as_scalar_name(x):
    """Return a stable string name from possibly-list/tuple inputs."""
    if x is None:
        return ""
    if isinstance(x, str):
        return x.strip()
    if isinstance(x, (list, tuple)):
        for item in x:
            if isinstance(item, str) and item.strip():
                return item.strip()
        # last resort: stringify
        return str(x)
    return str(x)


def expand_derived_metrics_from_registry(
    spec: dict,
    metric_registry: dict,
) -> tuple[dict, list]:
    """
    Expands derived metrics using the registry while preserving a clean metric lifecycle:

      - requested   = explicitly asked by user/planner
      - dependency  = auto-added base metrics required to compute a derived metric (NOT shown in SELECT)
      - derived     = computed metric shown in SELECT

    Deterministic. No guessing.
    """
    msgs = []
    lq = spec.get("logical_query") or {}
    metrics = lq.get("metrics") or []
    if not metrics:
        return spec, msgs

    reg_metrics = metric_registry.get("metrics") or {}

    # canonical alias map
    alias_map = (
        metric_registry.get("canonical_aliases")
        or metric_registry.get("canonical_metric_aliases")
        or {}
    )
    aliases_lc = {str(k).lower(): v for k, v in alias_map.items()} if isinstance(alias_map, dict) else {}

    # Ensure existing metrics are marked as requested unless already set
    for m in metrics:
        if isinstance(m, dict) and not m.get("metric_role"):
            m["metric_role"] = "requested"

    expanded = []
    seen = set()

    # First, keep original metrics in order
    for m in metrics:
        if not isinstance(m, dict):
            continue
        name = m.get("semantic_name") or m.get("name") or m.get("alias")
        if not name:
            continue
        
        # --- FIX: normalize name to a scalar string ---
        if isinstance(name, (list, tuple)):
            name = next((x for x in name if isinstance(x, str)), None)
            if not name:
                continue
        
        name = str(name).strip()
        
        canon = aliases_lc.get(name.lower(), name)
        
        m["semantic_name"] = canon
        expanded.append(m)
        seen.add(canon)

    # Now expand derived metrics based on registry formulas
    for m in list(expanded):
        if not isinstance(m, dict):
            continue

        name = m.get("semantic_name") or m.get("name")
        if not name:
            continue

        reg = reg_metrics.get(name) or {}
        formula = reg.get("derived_formula")
        base_metrics = reg.get("base_metrics") or []
        
        # If registry says it's derived, but planner didn't provide derived_expression,
        # we must attach one from registry.
        if not formula:
            continue
        
        # If the metric already has derived_expression, don’t re-expand
        if isinstance(m.get("derived_expression"), dict) and m["derived_expression"]:
            continue

        # Parse simple formulas like "cost / clicks"
        tokens = str(formula).replace("(", "").replace(")", "").split()
        if len(tokens) != 3:
            raise ValueError(f"Unsupported derived_formula format for '{name}': {formula}")

        left, op, right = tokens
        if op != "/":
            raise ValueError(f"Only ratio derived formulas supported right now. Got '{op}' for '{name}'")

        derived = {
            "semantic_name": name,
            "alias": m.get("alias") or name,
            "metric_class": "derived",
            "metric_role": "derived",
            "derived_expression": {
                "type": "ratio",
                "numerator": {"metric": left},
                "denominator": {"metric": right},
            },
        }

        # Replace the original metric entry with the derived rendering contract
        # (Keep role=requested if planner explicitly asked; derived is what renders)
        # If planner asked for this metric, it should remain visible; if it got added later, still visible as derived.
        # We'll just append derived and let SQL builder dedupe by semantic_name if needed.
        # Replace the existing requested metric (same semantic_name) with derived rendering contract
        for i in range(len(expanded)):
            if expanded[i].get("semantic_name") == name and expanded[i].get("metric_role") == "requested":
                expanded[i] = derived
                break
        else:
            expanded.append(derived)


        # Add dependencies as hidden plumbing metrics
        for base in base_metrics:
            if base not in seen:
                expanded.append({
                    "semantic_name": base,
                    "required": True,              # backward-compat with existing logic
                    "metric_role": "dependency",   # NEW: lifecycle role
                })
                seen.add(base)

        msgs.append({
            "type": "interpretation_warning",
            "detail": f"Expanded derived metric '{name}' and added dependency base metrics via registry."
        })

    lq["metrics"] = expanded
    spec["logical_query"] = lq
    return spec, msgs