# CORE CONTRACT — changes require test updates

# spec_executor.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from tools.query_builder import build_query
    from tools.metric_resolver import MetricRegistry, MetricResolver
except ImportError:
    from query_builder import build_query
    from metric_resolver import MetricRegistry, MetricResolver


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# -------------------------
# Spec normalization
# -------------------------

def normalize_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich an NL Spec with execution-time defaults.
    Keeps NL parsing infra-agnostic.
    """
    spec = dict(spec)  # shallow copy

    # Paths default (repo-relative)
    if "paths" not in spec:
        project_root = Path(__file__).resolve().parents[1]
        spec["paths"] = {
            "physical_schema": project_root / "current" / "physical_schema.json",
            "metric_registry": project_root / "current" / "metric_registry.json",
            "filter_config": project_root / "current" / "filter_config.json",
        }

    # Default grain for execution (registry-driven is ideal; this is the safe default you chose)
    if not spec.get("grain"):
        spec["grain"] = "campaign_calendar"

    # Ensure filters scaffolding exists
    spec.setdefault("filters", {})
    spec["filters"].setdefault("date", {})
    spec["filters"].setdefault("where", [])

    # Ensure lists exist
    spec.setdefault("metrics", [])
    spec.setdefault("dimensions", [])
    spec.setdefault("post", {})
    spec.setdefault("clarifications", [])

    return spec


# -------------------------
# Public entrypoint
# -------------------------

def execute_spec(spec: Dict[str, Any]) -> str:
    """
    Entry point: takes a canonical Spec and returns SQL (T-SQL / Fabric-compatible).
    """
    spec = normalize_spec(spec)

    # Base query only
    if not spec.get("compare") and not spec.get("post"):
        return _build_base(spec)

    # Comparison
    if spec.get("compare"):
        return _build_comparison(spec)

    # Ranking only
    base_sql = _build_base(spec)
    return _apply_post(base_sql, spec["post"])


# -------------------------
# Base query build
# -------------------------

def _build_base(spec: Dict[str, Any]) -> str:
    """
    Build the base query for a spec.

    IMPORTANT:
    - If platform is None, treat this as a "portfolio" request.
      We UNION Google + Microsoft and re-aggregate to keep behavior deterministic,
      without guessing a platform or changing the metric resolver.
    """
    platform = spec.get("platform")
    if platform:
        return _build_base_single_platform(spec, platform)

    # Portfolio mode: run both platforms and UNION ALL, then re-aggregate.
    # This prevents MetricResolver errors when preferred_fact_table is platform-scoped.
    platforms = ["google_ads", "microsoft_ads"]
    parts = []
    for p in platforms:
        s = dict(spec)
        s["platform"] = p
        parts.append(f"({ _build_base_single_platform(s, p) })")

    union_sql = "\nUNION ALL\n".join(parts)
    return _reaggregate_union(union_sql, spec)


def _build_base_single_platform(spec: Dict[str, Any], platform: str) -> str:
    registry_path = str(spec["paths"]["metric_registry"])
    reg = MetricRegistry(_load_json(registry_path))
    resolver = MetricResolver(reg)

    partitions = resolver.partition_metrics(spec["metrics"], spec["grain"], platform)

    if len(partitions) == 1:
        return _build_single_fact_query(spec, platform)

    return _build_multi_fact_cte(spec, platform, partitions)


def _build_single_fact_query(spec: Dict[str, Any], platform: str) -> str:
    """Build a standard single-fact-table query (original behavior)."""
    return build_query(
        physical_path=str(spec["paths"]["physical_schema"]),
        registry_path=str(spec["paths"]["metric_registry"]),
        grain=spec["grain"],
        platform=platform,
        metrics=spec["metrics"],
        dimensions=spec.get("dimensions"),
        date_from=spec.get("filters", {}).get("date", {}).get("date_from"),
        date_to=spec.get("filters", {}).get("date", {}).get("date_to"),
        last_n_days=spec.get("filters", {}).get("date", {}).get("last_n_days"),
        yesterday=spec.get("filters", {}).get("date", {}).get("yesterday", False),
        mtd=spec.get("filters", {}).get("date", {}).get("mtd", False),
        date_offset_days=spec.get("filters", {}).get("date", {}).get("offset_days", 0),
        where_filters=spec.get("filters", {}).get("where"),
        filter_config_path=str(spec["paths"].get("filter_config")) if spec.get("paths", {}).get("filter_config") else None,
        campaign_args=spec.get("filters", {}).get("campaign", {}).get("terms"),
        campaign_mode=spec.get("filters", {}).get("campaign", {}).get("mode", "any"),
        campaign_case_insensitive=True,
        campaign_ids_csv=(",".join(str(x) for x in spec.get("filters", {}).get("campaign_ids", []))
                          if isinstance(spec.get("filters", {}).get("campaign_ids"), list)
                          else spec.get("filters", {}).get("campaign_ids")),
    )


def _build_multi_fact_cte(
    spec: Dict[str, Any],
    platform: str,
    partitions: List[Tuple[str, List[str]]],
) -> str:
    """
    Build a multi-fact query when metrics span multiple fact tables.

    Each partition becomes a derived table (inline subquery) built by build_query().
    Derived tables are FULL OUTER JOINed on shared dimension columns.

    Uses derived tables instead of CTEs so the SQL is nestable inside
    UNION ALL (portfolio mode) and other subquery wrappers.
    """
    dims = spec.get("dimensions") or []
    all_metrics = spec.get("metrics") or []

    # Build a derived table for each partition
    dt_aliases: List[str] = []
    dt_sqls: List[str] = []
    # Track which derived table owns which metrics
    metric_to_dt: Dict[str, str] = {}

    for i, (fact_table, metric_subset) in enumerate(partitions):
        dt_alias = f"mf_{i}"
        dt_aliases.append(dt_alias)

        # Build a spec copy with only this partition's metrics
        part_spec = dict(spec)
        part_spec["metrics"] = list(metric_subset)

        dt_sqls.append(_build_single_fact_query(part_spec, platform))

        for m in metric_subset:
            metric_to_dt[m] = dt_alias

    # Build outer SELECT
    select_parts: List[str] = []

    if dims:
        for d in dims:
            coalesce_args = ", ".join(f"{a}.[{d}]" for a in dt_aliases)
            select_parts.append(f"COALESCE({coalesce_args}) AS [{d}]")

    for m in all_metrics:
        dt = metric_to_dt.get(m, dt_aliases[0])
        select_parts.append(f"{dt}.[{m}]")

    # Build FROM + FULL OUTER JOIN chain using derived tables
    first_dt = f"(\n{dt_sqls[0]}\n) AS {dt_aliases[0]}"

    if not dims:
        # No dimensions: CROSS JOIN (single-row aggregates)
        from_clause = f"FROM {first_dt}"
        for i in range(1, len(dt_aliases)):
            from_clause += f"\nCROSS JOIN (\n{dt_sqls[i]}\n) AS {dt_aliases[i]}"
    else:
        from_clause = f"FROM {first_dt}"
        for i in range(1, len(dt_aliases)):
            join_conds = " AND ".join(
                f"{dt_aliases[0]}.[{d}] = {dt_aliases[i]}.[{d}]" for d in dims
            )
            from_clause += f"\nFULL OUTER JOIN (\n{dt_sqls[i]}\n) AS {dt_aliases[i]}\n  ON {join_conds}"

    select_clause = "SELECT\n  " + ",\n  ".join(select_parts)

    return f"{select_clause}\n{from_clause}\n"


def _reaggregate_union(union_sql: str, spec: Dict[str, Any]) -> str:
    """
    Given a UNION ALL of per-platform base queries, re-aggregate so the output matches
    the shape you would have had if a single fact table existed.

    Assumes metric columns are aliased as [<metric>] and dimension columns as [<dimension>].
    """
    dims = spec.get("dimensions") or []
    mets = spec.get("metrics") or []

    # If there are no metrics, there's nothing to aggregate; just return union
    if not mets:
        return union_sql

    if not dims:
        # Total-only: SUM each metric across union
        select_exprs = [f"SUM([{m}]) AS [{m}]" for m in mets]
        return f"""
        SELECT
          {", ".join(select_exprs)}
        FROM (
          {union_sql}
        ) u
        """

    # With dimensions: group by dims, SUM metrics
    dim_select = ", ".join([f"[{d}]" for d in dims])
    metric_select = ", ".join([f"SUM([{m}]) AS [{m}]" for m in mets])
    group_by = ", ".join([f"[{d}]" for d in dims])

    return f"""
    SELECT
      {dim_select},
      {metric_select}
    FROM (
      {union_sql}
    ) u
    GROUP BY {group_by}
    """


# -------------------------
# Post-processing
# -------------------------

def _apply_post(sql: str, post: Dict[str, Any]) -> str:
    segments = (post or {}).get("rank_segments")
    if not segments:
        return sql

    pieces: List[str] = []
    for seg in segments:
        order = seg["order_by"][0]
        expr = order["expr"]
        direction = order["dir"].upper()
        limit = seg["limit"]
        label = seg["label"]

        pieces.append(
            f"""
            SELECT *, '{label}' AS Segment
            FROM (
                SELECT TOP {limit} *
                FROM ({sql}) base
                ORDER BY [{expr}] {direction}
            ) ranked
            """
        )

    return "\nUNION ALL\n".join(pieces)


# -------------------------
# Comparisons
# -------------------------

def _build_comparison(spec: Dict[str, Any]) -> str:
    cmp = spec["compare"]

    if cmp["type"] == "period_over_period":
        return _period_over_period(spec, cmp)

    if cmp["type"] == "cross_platform":
        cmp = _normalize_cross_platform_compare(spec, cmp)
        return _cross_platform(spec, cmp)

    raise ValueError(f"Unknown compare type {cmp['type']}")


def _period_over_period(spec: Dict[str, Any], cmp: Dict[str, Any]) -> str:
    base = dict(spec)
    metric = cmp["metric"]

    base["metrics"] = [metric]

    # current
    base["filters"] = dict(base.get("filters", {}))
    base["filters"]["date"] = cmp["current"]
    cur = _build_base(base)

    # prior
    base["filters"]["date"] = cmp["prior"]
    prev = _build_base(base)

    dim_cols = spec.get("dimensions") or []
    if not dim_cols:
        # no dimensions: single-row compare
        return f"""
        SELECT
          c.[{metric}] AS current_value,
          p.[{metric}] AS prior_value,
          (c.[{metric}] - p.[{metric}]) AS delta
        FROM ({cur}) c
        CROSS JOIN ({prev}) p
        """

    join_cond = " AND ".join([f"c.[{d}] = p.[{d}]" for d in dim_cols])

    return f"""
    SELECT
      c.*,
      p.[{metric}] AS prior_value,
      (c.[{metric}] - p.[{metric}]) AS delta
    FROM ({cur}) c
    JOIN ({prev}) p
      ON {join_cond}
    """


def _normalize_cross_platform_compare(spec: Dict[str, Any], cmp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Allow NL to provide only:
      {"type":"cross_platform","metrics":["clicks"]}
    and fill the rest deterministically.
    """
    out = dict(cmp)

    # metrics required
    if "metrics" not in out or not out["metrics"]:
        out["metrics"] = (spec.get("metrics") or ["clicks"])[:1]

    # left/right defaults
    out.setdefault("left", {"platform": "google_ads"})
    out.setdefault("right", {"platform": "microsoft_ads"})

    # join keys: if user is comparing by name, CampaignName is the safest default
    # (matches your earlier intent: compare campaigns by campaign name)
    out.setdefault("join_keys", ["CampaignName"])

    return out


def _cross_platform(spec: Dict[str, Any], cmp: Dict[str, Any]) -> str:
    base = dict(spec)

    base["platform"] = cmp["left"]["platform"]
    left = _build_base(base)

    base["platform"] = cmp["right"]["platform"]
    right = _build_base(base)

    key = cmp["join_keys"][0]
    metric = cmp["metrics"][0]

    return f"""
    SELECT
      COALESCE(l.[{key}], r.[{key}]) AS [{key}],
      l.[{metric}] AS left_value,
      r.[{metric}] AS right_value,
      l.[{metric}] / NULLIF(r.[{metric}], 0) AS ratio
    FROM ({left}) l
    FULL OUTER JOIN ({right}) r
      ON l.[{key}] = r.[{key}]
    """
