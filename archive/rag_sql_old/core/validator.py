from __future__ import annotations

from copy import deepcopy

from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from pathlib import Path

from .io import CONFIG_DIR, load_json

@dataclass
class ValidatorContext:
    """
    Holds schema + config needed for semantic validation.
    """
    schema: Dict[str, Any]
    schema_index: Dict[str, Any]
    metric_registry: Dict[str, Any]
    domain_policy: Dict[str, Any]
    filter_config: Dict[str, Any]
    validator_policy: Dict[str, Any]

    @classmethod
    def from_project(
        cls,
        schema: Dict[str, Any],
        schema_index: Dict[str, Any],
        config_dir: Path = CONFIG_DIR,
        metric_registry: Optional[Dict[str, Any]] = None,
        domain_policy: Optional[Dict[str, Any]] = None,
        filter_config: Optional[Dict[str, Any]] = None,
        validator_policy: Optional[Dict[str, Any]] = None,
    ) -> "ValidatorContext":
        # Load from disk if not provided
        metric_registry = metric_registry if metric_registry is not None else load_json(config_dir / "metric_registry.json")
        domain_policy   = domain_policy   if domain_policy   is not None else load_json(config_dir / "domain_policy.json")
        filter_config   = filter_config   if filter_config   is not None else load_json(config_dir / "filter_config.json")
        validator_policy= validator_policy if validator_policy is not None else load_json(config_dir / "validator_policy.json")

        return cls(
            schema=schema,
            schema_index=schema_index,
            metric_registry=metric_registry,
            domain_policy=domain_policy,
            filter_config=filter_config,
            validator_policy=validator_policy,
        )


def validate_logical_query(
    logical_query: Dict[str, Any],
    ctx: ValidatorContext,
    nl_question: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Main entry point for Goal E semantic validation.

    Returns:
        {
          "status": "ok | needs_clarification | error",
          "messages": [...],
          "validated_spec": { ... }  # copy of logical_query with minor safe enrichments, or null
        }
    """
    messages: List[Dict[str, str]] = []
    spec = deepcopy(logical_query)

    # 1) Metrics (includes derived/base semantics + aliases)
    metric_msgs, spec = _validate_metrics(spec, ctx)
    messages.extend(metric_msgs)

    # 2) Domains / platforms
    messages.extend(_validate_domains_and_platforms(spec, ctx))

    # 3) Filters & soft tokens
    messages.extend(_validate_filters_and_soft_tokens(spec, ctx, nl_question=nl_question))

    # 4) Time window
    messages.extend(_validate_time_window(spec, ctx))

    # 5) Retrieval confidence
    messages.extend(_validate_retrieval_confidence(spec, ctx))

    # --- Decide overall status --------------------------------------------
    status = "ok"
    has_error = any(m["type"] == "validation_error" for m in messages)
    needs_clar = any(
        m["type"] in {"ambiguous_metric", "ambiguous_platform", "missing_information", "metric_conflict"}
        for m in messages
    )

    if has_error:
        status = "error"
        validated_spec = None
    elif needs_clar:
        status = "needs_clarification"
        validated_spec = None
    else:
        status = "ok"
        validated_spec = spec

    result = {
        "status": status,
        "messages": messages,
        "validated_spec": validated_spec,
    }

    return result

def _validate_metrics(
    logical_query: Dict[str, Any],
    ctx: ValidatorContext,
) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    """
    Validate metrics against the metric registry, including:
      - alias-aware lookup
      - base vs derived metric enforcement
      - auto-injection of base metrics for derived metrics

    Returns:
        messages: list of validator messages
        updated_logical_query: copy of logical_query with safe enrichments
    """
    messages: List[Dict[str, str]] = []
    updated = deepcopy(logical_query)

    registry_all = ctx.metric_registry.get("metrics", {}) or {}
    metrics_list: List[Dict[str, Any]] = updated.get("metrics", []) or []

    # First pass: resolve all metrics, enforce basic aggregation + base_columns sanity
    resolved_defs: Dict[int, Optional[Dict[str, Any]]] = {}

    for idx, metric in enumerate(metrics_list):
        sem_name = metric.get("semantic_name")
        if not sem_name:
            messages.append({
                "type": "missing_information",
                "detail": "A metric entry is missing 'semantic_name' and will be ignored by metric semantics validation."
            })
            resolved_defs[idx] = None
            continue

        reg_def = _lookup_metric_def(sem_name, registry_all)

        if reg_def is None:
            # Unknown / unregistered metric
            if ctx.validator_policy.get("allow_ungrounded_metrics", True):
                messages.append({
                    "type": "ambiguous_metric",
                    "detail": f"Metric '{sem_name}' is not defined in metric_registry.json."
                })
            else:
                messages.append({
                    "type": "validation_error",
                    "detail": f"Metric '{sem_name}' is not defined in metric_registry.json and ungrounded metrics are disallowed."
                })
            resolved_defs[idx] = None
            continue

        resolved_defs[idx] = reg_def

        # Aggregation handling
        default_agg = reg_def.get("default_aggregation")
        agg = metric.get("aggregation")

        if not agg and default_agg:
            metric["aggregation"] = default_agg
        elif agg and default_agg and agg.lower() != default_agg.lower():
            messages.append({
                "type": "metric_conflict",
                "detail": (
                    f"Metric '{sem_name}' uses aggregation '{agg}', but metric_registry "
                    f"declares default aggregation '{default_agg}'."
                )
            })

        # Basic structural sanity: if registry defines base_columns, make sure resolved_columns tables match
        base_cols = reg_def.get("base_columns") or []
        if base_cols and metric.get("resolved_columns"):
            base_tables = {c["table"] for c in base_cols if "table" in c}
            resolved_tables = {c.get("table") for c in metric["resolved_columns"] if c.get("table")}
            if resolved_tables and not resolved_tables.issubset(base_tables):
                messages.append({
                    "type": "validation_error",
                    "detail": (
                        f"Metric '{sem_name}' is resolved to tables {sorted(resolved_tables)}, "
                        f"but metric_registry expects tables {sorted(base_tables)}."
                    )
                })

    # Second pass: enforce derived metrics + auto-inject their base metrics
    for idx, metric in enumerate(metrics_list):
        reg_def = resolved_defs.get(idx)
        if not reg_def:
            continue  # unknown metric, already handled above

        sem_name = metric.get("semantic_name")

        if reg_def.get("default_aggregation") != "derived":
            continue  # base metric

        base_metric_names = reg_def.get("base_metrics") or []
        if not base_metric_names:
            messages.append({
                "type": "validation_error",
                "detail": f"Derived metric '{sem_name}' has no base_metrics defined in the registry."
            })
            continue

        injected_any = False
        missing_any = False

        for base_name in base_metric_names:
            base_def = _lookup_metric_def(base_name, registry_all)
            if base_def is None:
                messages.append({
                    "type": "missing_information",
                    "detail": f"Derived metric '{sem_name}' references base metric '{base_name}', which is not defined in metric_registry.json."
                })
                missing_any = True
                continue

            if not base_def.get("base_columns"):
                messages.append({
                    "type": "validation_error",
                    "detail": f"Base metric '{base_name}' used by '{sem_name}' has no base_columns defined in the registry."
                })
                missing_any = True
                continue

            if _ensure_injected_metric(metrics_list, base_name, base_def):
                injected_any = True

        if injected_any:
            messages.append({
                "type": "interpretation_warning",
                "detail": f"Base metrics {base_metric_names} were automatically added for derived metric '{sem_name}'."
            })

        if missing_any:
            messages.append({
                "type": "metric_conflict",
                "detail": f"Derived metric '{sem_name}' could not be fully resolved because one or more base metrics were missing or invalid."
            })

    updated["metrics"] = metrics_list
    return messages, updated

def _lookup_metric_def(name: str, registry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Case-insensitive lookup of a metric in the registry using:
      - normalized user name
      - registry key
      - semantic_name
    """
    canon = _normalize_metric_name(name)
    if not canon:
        return None

    for key, mdef in registry.items():
        key_l = (key or "").strip().lower()
        sname_l = (mdef.get("semantic_name") or "").strip().lower()
        if key_l == canon or sname_l == canon:
            return mdef

    return None