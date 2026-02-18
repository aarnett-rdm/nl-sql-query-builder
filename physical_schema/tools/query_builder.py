# CORE CONTRACT — changes require test updates

#!/usr/bin/env python3
"""
query_builder.py

Deterministic, registry-driven SQL builder for Fabric (T-SQL).

This is the BASE query emitter.
All orchestration (ranking, comparisons, unions) happens outside this file.

Supports:
- Registry-driven metrics
- Deterministic join planning
- Dimensions with prefer_fact ambiguity handling
- Campaign name free-text filters (multi-value, AND/OR, platform-aware)
- CampaignId IN (...) filters (large numeric lists)
- PST-relative date helpers (last_n_days, yesterday, mtd)
- Date window offsets (for prior-period comparisons)
- Generic dimension WHERE filters (validated & safe)
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

try:
    from tools.join_planner import (
        PhysicalSchema, JoinStep, default_targets, plan_joins, emit_tsql_from_join_steps,
    )
    from tools.metric_resolver import MetricRegistry, MetricResolver
    from tools.common import (
        PLATFORM_TOKEN, bracket_ident as _bracket_ident,
        sql_string_literal as _sql_string_literal, make_aliases as _make_aliases,
    )
    from tools.exceptions import AmbiguousDimensionError, DateFilterError
except ImportError:
    from join_planner import (
        PhysicalSchema, JoinStep, default_targets, plan_joins, emit_tsql_from_join_steps,
    )
    from metric_resolver import MetricRegistry, MetricResolver
    from common import (
        PLATFORM_TOKEN, bracket_ident as _bracket_ident,
        sql_string_literal as _sql_string_literal, make_aliases as _make_aliases,
    )
    from exceptions import AmbiguousDimensionError, DateFilterError

# ----------------------------
# helpers
# ----------------------------

def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ----------------------------
# date helpers
# ----------------------------

def _today_pst_calendar() -> date:
    return date.today()


def _shift_iso_date(iso: Optional[str], days: int) -> Optional[str]:
    if not iso or days == 0:
        return iso
    y, m, d = iso.split("-")
    return (date(int(y), int(m), int(d)) - timedelta(days=days)).isoformat()


def _compute_relative_range(
    date_from: Optional[str],
    date_to: Optional[str],
    last_n_days: Optional[int],
    yesterday: bool,
    mtd: bool,
) -> Tuple[Optional[str], Optional[str]]:

    explicit = bool(date_from or date_to)
    relative = bool(last_n_days is not None or yesterday or mtd)

    if explicit and relative:
        raise DateFilterError(
            "Use explicit dates OR relative dates, not both",
            filter_type="conflicting",
        )

    if explicit:
        return date_from, date_to

    if not relative:
        return None, None

    if sum([last_n_days is not None, yesterday, mtd]) > 1:
        raise DateFilterError(
            "Choose only one relative date mode",
            filter_type="multiple_relative",
        )

    today = _today_pst_calendar()

    if last_n_days is not None:
        end = today - timedelta(days=1)
        start = end - timedelta(days=last_n_days - 1)
        return start.isoformat(), end.isoformat()

    if yesterday:
        y = today - timedelta(days=1)
        return y.isoformat(), y.isoformat()

    return today.replace(day=1).isoformat(), today.isoformat()


# ----------------------------
# dimensions
# ----------------------------

@dataclass(frozen=True)
class DimensionSpec:
    raw: str
    table: Optional[str]
    column: str
    out_alias: str


def _parse_dimension_spec(spec: str) -> DimensionSpec:
    raw = spec.strip()
    if ":" in raw:
        left, out_alias = raw.rsplit(":", 1)
    else:
        left, out_alias = raw, ""

    parts = [p.strip() for p in left.split(".") if p.strip()]
    table = ".".join(parts[:-1]) if len(parts) > 1 else None
    column = parts[-1]
    out_alias = out_alias or column

    return DimensionSpec(raw, table, column, out_alias)


def _dimension_required_targets(schema: PhysicalSchema, dims: List[DimensionSpec]) -> List[str]:
    out: List[str] = []
    for d in dims:
        if d.table:
            out.append(schema.resolve_table(d.table))
    return list(dict.fromkeys(out))


def _infer_missing_dimension_targets(
    schema: PhysicalSchema,
    current_targets: List[str],
    dim_specs: List[DimensionSpec],
    where_filters: List[Dict[str, Any]],
    platform: Optional[str],
) -> List[str]:
    """Infer join targets for unqualified dimension/filter columns missing
    from the current target set.

    For each unqualified column that doesn't exist in any target table,
    find the best candidate table using DIM_PREFERENCE and platform affinity.
    """
    from tools.common import DIM_PREFERENCE

    target_set = set(current_targets)

    # Collect unqualified columns that need resolution
    columns_needed: List[str] = []
    for d in dim_specs:
        if not d.table:
            columns_needed.append(d.column)
    for f in where_filters or []:
        table, col = _parse_field_ref(f["field"])
        if not table:
            columns_needed.append(col)

    if not columns_needed:
        return []

    extra: List[str] = []
    for col in dict.fromkeys(columns_needed):
        # Check if column already exists in a target table
        found_in_target = any(
            col in (schema.cols_by_table.get(t) or {})
            for t in target_set
        )
        if found_in_target:
            continue

        # Not in any target — find the right table to join.

        # 1) Check DIM_PREFERENCE for a direct hint (e.g., AccountId -> Account)
        #    DIM_PREFERENCE is keyed by ID columns. If our column is "AccountName",
        #    look for the ID variant (AccountId) to find the table.
        if platform:
            id_col = col.replace("Name", "Id") if col.endswith("Name") else col
            pref = DIM_PREFERENCE.get((platform, id_col))
            if pref and pref in schema.tables and col in (schema.cols_by_table.get(pref) or {}):
                if pref not in target_set:
                    extra.append(pref)
                    target_set.add(pref)
                continue

        # 2) Fall back: search all tables for the column, prefer platform match
        token = PLATFORM_TOKEN.get(platform or "", "")
        candidates = [
            t for t in schema.tables
            if col in (schema.cols_by_table.get(t) or {})
        ]
        if token:
            plat_cands = [t for t in candidates if token in t.lower()]
            if plat_cands:
                candidates = plat_cands

        # Prefer CoreEntity tables (dimension tables) over metrics/bronze
        core = [t for t in candidates if "coreentity" in t.lower()]
        if core:
            candidates = core

        if candidates:
            best = candidates[0]
            if best not in target_set:
                extra.append(best)
                target_set.add(best)

    return extra


def _resolve_dimension_expression(
    schema: PhysicalSchema,
    aliases: Dict[str, str],
    dim: DimensionSpec,
    fact_table: str,
    ambiguous_dim_policy: str,
    platform: Optional[str] = None,
) -> Tuple[str, str]:

    matches: List[str] = []

    if dim.table:
        t = schema.resolve_table(dim.table)
        expr = f"{aliases[t]}.{_bracket_ident(dim.column)}"
        return f"{expr} AS {_bracket_ident(dim.out_alias)}", expr

    for t in aliases:
        if dim.column in (schema.cols_by_table.get(t) or {}):
            matches.append(t)

    if not matches:
        raise ValueError(f"Dimension column '{dim.column}' not found")

    if len(matches) == 1:
        expr = f"{aliases[matches[0]]}.{_bracket_ident(dim.column)}"
        return f"{expr} AS {_bracket_ident(dim.out_alias)}", expr

    # --- Disambiguation strategies (most → least specific) ---

    # 1) prefer_fact: if column exists on fact table, use it
    if ambiguous_dim_policy == "prefer_fact" and fact_table in matches:
        expr = f"{aliases[fact_table]}.{_bracket_ident(dim.column)}"
        return f"{expr} AS {_bracket_ident(dim.out_alias)}", expr

    # 2) Platform affinity: prefer tables matching the active platform
    if platform:
        token = PLATFORM_TOKEN.get(platform, "")
        if token:
            plat_matches = [t for t in matches if token in t.lower()]
            if len(plat_matches) == 1:
                expr = f"{aliases[plat_matches[0]]}.{_bracket_ident(dim.column)}"
                return f"{expr} AS {_bracket_ident(dim.out_alias)}", expr

    # 3) Prefer dimension tables over fact/mapping tables (heuristic)
    non_fact = [t for t in matches if t != fact_table]
    if len(non_fact) == 1:
        expr = f"{aliases[non_fact[0]]}.{_bracket_ident(dim.column)}"
        return f"{expr} AS {_bracket_ident(dim.out_alias)}", expr

    # --- Cannot resolve: raise structured error for clarification flow ---
    friendly_names = [t.split(".")[-1] if "." in t else t for t in matches]
    raise AmbiguousDimensionError(
        column=dim.column,
        candidates=matches,
        question=f"'{dim.column}' exists in multiple tables: {', '.join(friendly_names)}. Which one should I use?",
    )


# ----------------------------
# campaign helpers
# ----------------------------

def _split_campaign_terms(campaign_args: Optional[List[str]]) -> List[str]:
    if not campaign_args:
        return []
    out: List[str] = []
    seen = set()
    for raw in campaign_args:
        parts = [p.strip() for p in str(raw).split(",") if p.strip()]
        for p in parts:
            k = p.lower()
            if k not in seen:
                out.append(p)
                seen.add(k)
    return out


def _parse_numeric_id_list(
    ids_csv: Optional[str],
    ids_repeat: Optional[List[str]],
) -> List[int]:
    raw: List[str] = []
    if ids_csv:
        raw.extend(p.strip() for p in ids_csv.split(",") if p.strip())
    if ids_repeat:
        raw.extend(p.strip() for p in ids_repeat if p.strip())

    out: List[int] = []
    seen = set()
    for p in raw:
        if not p.isdigit():
            raise ValueError(f"CampaignId must be numeric. Got: {p}")
        v = int(p)
        if v not in seen:
            out.append(v)
            seen.add(v)
    return out


def _campaign_required_targets(
    schema: PhysicalSchema,
    filter_cfg: Dict[str, Any],
    campaign_terms: List[str],
    platform: Optional[str],
) -> List[str]:

    if not campaign_terms:
        return []

    if not filter_cfg.get("campaign_free_text_enabled", False):
        raise ValueError("Campaign free-text filtering is disabled in filter_config.json")

    token = PLATFORM_TOKEN.get((platform or "").lower())
    out: List[str] = []

    for c in filter_cfg.get("campaign_filter_candidates", []):
        t = c.get("table")
        if not t:
            continue
        resolved = schema.resolve_table(t)
        if token and token not in resolved.lower():
            continue
        out.append(resolved)

    return list(dict.fromkeys(out))


# ----------------------------
# generic WHERE filters
# ----------------------------

def _parse_field_ref(field: str) -> Tuple[Optional[str], str]:
    parts = [p.strip() for p in field.split(".")]
    return (".".join(parts[:-1]) if len(parts) > 1 else None, parts[-1])


def _where_filters_required_targets(schema: PhysicalSchema, filters: List[Dict[str, Any]]) -> List[str]:
    out = []
    for f in filters or []:
        table, _ = _parse_field_ref(f["field"])
        if table:
            out.append(schema.resolve_table(table))
    return list(dict.fromkeys(out))


def _build_where_filters_predicates(
    schema: PhysicalSchema,
    aliases: Dict[str, str],
    fact_table: str,
    where_filters: List[Dict[str, Any]],
    ambiguous_dim_policy: str,
) -> List[str]:

    preds: List[str] = []

    for f in where_filters or []:
        table, col = _parse_field_ref(f["field"])
        op = f.get("op", "=")
        val: str = f.get("value", "")
        ci = f.get("case_insensitive", False)

        if table:
            t = schema.resolve_table(table)
            expr = f"{aliases[t]}.{_bracket_ident(col)}"
        else:
            # Search all joined tables for the column (prefer fact table)
            matches = [t for t in aliases if col in (schema.cols_by_table.get(t) or {})]
            if not matches:
                expr = f"{aliases[fact_table]}.{_bracket_ident(col)}"
            elif fact_table in matches:
                expr = f"{aliases[fact_table]}.{_bracket_ident(col)}"
            else:
                expr = f"{aliases[matches[0]]}.{_bracket_ident(col)}"

        if op == "contains":
            lit = f"%{val.lower() if ci else val}%"
            expr = f"LOWER({expr})" if ci else expr
            preds.append(f"{expr} LIKE {_sql_string_literal(lit)}")
        elif op == "not_contains":
            lit = f"%{val.lower() if ci else val}%"
            expr = f"LOWER({expr})" if ci else expr
            preds.append(f"{expr} NOT LIKE {_sql_string_literal(lit)}")
        elif op in (">", "<", ">=", "<=", "!="):
            # Numeric comparison if value looks numeric, otherwise string
            try:
                num = float(val)
                if num == int(num):
                    preds.append(f"{expr} {op} {int(num)}")
                else:
                    preds.append(f"{expr} {op} {num}")
            except (ValueError, TypeError):
                expr = f"LOWER({expr})" if ci else expr
                preds.append(f"{expr} {op} {_sql_string_literal(val.lower() if ci else val)}")
        else:
            # Default: equality
            expr = f"LOWER({expr})" if ci else expr
            preds.append(f"{expr} = {_sql_string_literal(val.lower() if ci else val)}")

    return preds


# ----------------------------
# core
# ----------------------------

def build_query(
    physical_path: str,
    registry_path: str,
    grain: str,
    platform: Optional[str],
    metrics: List[str],
    dimensions: Optional[List[str]] = None,
    extra_targets: Optional[List[str]] = None,
    ambiguous_dim_policy: str = "prefer_fact",
    filter_config_path: Optional[str] = None,
    campaign_args: Optional[List[str]] = None,
    campaign_mode: str = "any",
    campaign_case_insensitive: bool = True,
    campaign_ids_csv: Optional[str] = None,
    campaign_id_repeat: Optional[List[str]] = None,
    campaign_id_column: str = "CampaignId",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    last_n_days: Optional[int] = None,
    yesterday: bool = False,
    mtd: bool = False,
    date_column: str = "PST_Date",
    date_offset_days: int = 0,
    where_filters: Optional[List[Dict[str, Any]]] = None,
) -> str:

    schema = PhysicalSchema(_load_json(physical_path))
    reg = MetricRegistry(_load_json(registry_path))
    resolver = MetricResolver(reg)

    fact_table, resolved_metrics = resolver.resolve_metrics(metrics, grain, platform, fact_alias="fact")

    df, dt = _compute_relative_range(date_from, date_to, last_n_days, yesterday, mtd)
    if date_offset_days:
        df = _shift_iso_date(df, date_offset_days)
        dt = _shift_iso_date(dt, date_offset_days)

    dim_specs = [_parse_dimension_spec(d) for d in dimensions or []]
    where_filters = where_filters or []

    campaign_terms = _split_campaign_terms(campaign_args)
    campaign_ids = _parse_numeric_id_list(campaign_ids_csv, campaign_id_repeat)

    filter_cfg = _load_json(filter_config_path) if filter_config_path else {}

    targets = default_targets(grain, platform)
    targets += _dimension_required_targets(schema, dim_specs)
    targets += _where_filters_required_targets(schema, where_filters)
    targets += _campaign_required_targets(schema, filter_cfg, campaign_terms, platform)
    targets += _infer_missing_dimension_targets(schema, targets, dim_specs, where_filters, platform)

    if extra_targets:
        for t in extra_targets:
            targets.append(schema.resolve_table(t))

    if df or dt:
        targets.append(schema.resolve_table("Utility.DimCalendar"))

    targets = list(dict.fromkeys(targets))

    plan = plan_joins(schema, fact_table, targets, platform)
    js = [JoinStep(**x) for x in plan["join_steps"]]
    from_sql = emit_tsql_from_join_steps(plan["fact_table"], js)
    aliases = _make_aliases(plan["fact_table"], js)

    select_dims, group_by = [], []
    for d in dim_specs:
        sel, grp = _resolve_dimension_expression(schema, aliases, d, plan["fact_table"], ambiguous_dim_policy, platform)
        select_dims.append(sel)
        group_by.append(grp)

    predicates: List[str] = []

    if df:
        dc = f"{aliases['Utility.DimCalendar']}.{_bracket_ident(date_column)}"
        predicates.append(f"{dc} >= {_sql_string_literal(df)}")
    if dt:
        dc = f"{aliases['Utility.DimCalendar']}.{_bracket_ident(date_column)}"
        predicates.append(f"{dc} <= {_sql_string_literal(dt)}")

    if campaign_ids:
        predicates.append(
            f"fact.{_bracket_ident(campaign_id_column)} IN ({', '.join(str(i) for i in campaign_ids)})"
        )

    if campaign_terms:
        candidate_exprs: List[str] = []
        token = PLATFORM_TOKEN.get((platform or "").lower())
        for c in filter_cfg.get("campaign_filter_candidates", []):
            t = schema.resolve_table(c["table"])
            if token and token not in t.lower():
                continue
            candidate_exprs.append(f"{aliases[t]}.{_bracket_ident(c['column'])}")

        term_preds = []
        for term in campaign_terms:
            if campaign_case_insensitive:
                lit = _sql_string_literal(f"%{term.lower()}%")
                ors = [f"LOWER({ce}) LIKE {lit}" for ce in candidate_exprs]
            else:
                lit = _sql_string_literal(f"%{term}%")
                ors = [f"{ce} LIKE {lit}" for ce in candidate_exprs]
            term_preds.append("(" + " OR ".join(ors) + ")")

        if campaign_mode == "all":
            predicates.append("(" + " AND ".join(term_preds) + ")")
        else:
            predicates.append("(" + " OR ".join(term_preds) + ")")

    predicates += _build_where_filters_predicates(
        schema, aliases, plan["fact_table"], where_filters, ambiguous_dim_policy
    )

    sql = ["SELECT"]
    sql.append("  " + ",\n  ".join(select_dims + [m.select_sql for m in resolved_metrics]))
    sql.append(from_sql)

    if predicates:
        sql.append("WHERE")
        sql.append("  " + " AND ".join(predicates))

    if group_by:
        sql.append("GROUP BY")
        sql.append("  " + ", ".join(group_by))

    return "\n".join(sql) + "\n"
