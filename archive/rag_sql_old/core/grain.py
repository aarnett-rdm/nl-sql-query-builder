from typing import Dict, List, Tuple

def resolve_default_grain_from_registry(spec: dict, metric_registry: dict) -> tuple[dict, list]:
    """
    If user/planner did not specify a grain (or it is 'none'), choose a grain deterministically
    using the metric registry:

    - For each metric, take its default_grain if present.
    - Otherwise use global grain_preference_order (registry-level) intersected with supported_grains.

    Writes result into spec['logical_query']['grain'] in a consistent shape.
    """
    msgs = []
    lq = spec.get("logical_query") or {}

    # Detect "no grain"
    grain_obj = lq.get("grain")
    current = None
    if isinstance(grain_obj, dict):
        current = grain_obj.get("resolved") or grain_obj.get("time_grain") or grain_obj.get("default")
    elif isinstance(grain_obj, str):
        current = grain_obj

    if current and str(current).lower() != "none":
        return spec, msgs  # already set

    # Registry metric lookup
    reg_metrics = metric_registry.get("metrics") or metric_registry.get("registry") or {}
    pref_order = metric_registry.get("grain_preference_order") or [
        "campaign_calendar", "adgroup_calendar", "keyword_calendar", "search_term_calendar", "event_calendar"
    ]

    metrics = lq.get("metrics") or []
    metric_names = []
    for m in metrics:
        if isinstance(m, dict):
            metric_names.append(m.get("semantic_name") or m.get("name"))
    metric_names = [m for m in metric_names if m]

    # Collect candidate grains
    defaults = []
    supported_sets = []

    for name in metric_names:
        reg = reg_metrics.get(name) or {}
        dg = reg.get("default_grain")
        if dg:
            defaults.append(dg)

        sg = reg.get("supported_grains")
        if isinstance(sg, list) and sg:
            supported_sets.append(set(sg))

    chosen = None

    # If all metrics have same default_grain, use it
    if defaults and len(set(defaults)) == 1:
        chosen = defaults[0]

    # Else pick best grain by preference order, intersecting supported_grains across metrics if possible
    if not chosen:
        if supported_sets:
            intersection = set.intersection(*supported_sets)
        else:
            intersection = set(pref_order)  # fallback

        for g in pref_order:
            if g in intersection:
                chosen = g
                break

    # Final fallback
    if not chosen:
        chosen = "campaign_calendar"

    lq["grain"] = {
        "entity_grain": "campaign",   # keep if you already have a better entity inference
        "time_grain": chosen,
        "resolved": chosen,
        "explanation": "Default grain applied from metric registry."
    }
    spec["logical_query"] = lq

    msgs.append({
        "type": "interpretation_warning",
        "detail": f"No grain specified; defaulted to '{chosen}' using metric registry."
    })
    return spec, msgs