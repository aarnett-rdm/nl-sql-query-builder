# metric_resolver.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from tools.exceptions import MetricResolutionError
except ImportError:
    from exceptions import MetricResolutionError


def _norm_table_name(table: str) -> str:
    """
    Normalize table names so registry entries like 'GoogleAdsCampaignPerformanceMetric'
    can match schema-qualified names like 'GoTicketsPerformanceMetric.GoogleAdsCampaignPerformanceMetric'.
    """
    return table.split(".")[-1].lower()


def _safe_divide_sql(numer_sql: str, denom_sql: str) -> str:
    # Fabric / T-SQL safe divide
    return f"({numer_sql} / NULLIF({denom_sql}, 0))"


@dataclass(frozen=True)
class ResolvedMetric:
    requested_name: str          # what user typed (or canonical key)
    canonical_key: str           # registry key
    fact_table: str              # chosen fact table (possibly schema-qualified elsewhere)
    select_sql: str              # SELECT expression (with AS alias)
    is_derived: bool
    depends_on: Tuple[str, ...]  # canonical keys of base metrics used (for derived)


class MetricRegistry:
    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload
        self.metrics = payload.get("metrics", {})
        self.synonyms = payload.get("synonyms", {})  # maps alias -> canonical key

    @classmethod
    def from_path(cls, path: str) -> "MetricRegistry":
        import json
        with open(path, "r", encoding="utf-8") as f:
            return cls(json.load(f))

    def canonicalize(self, name: str) -> str:
        key = name.strip().lower()
        # direct hit
        if key in self.metrics:
            return key
        # synonym hit
        if key in self.synonyms:
            return self.synonyms[key].strip().lower()
        raise MetricResolutionError(f"Unknown metric: '{name}'")

    def get_metric_def(self, canonical_key: str) -> Dict[str, Any]:
        if canonical_key not in self.metrics:
            raise MetricResolutionError(f"Metric not found in registry: '{canonical_key}'")
        return self.metrics[canonical_key]


class MetricResolver:
    def __init__(self, registry: MetricRegistry):
        self.registry = registry

    def _get_metric_candidates(
        self,
        canonical_key: str,
        grain: str,
        platform_key: str,
    ) -> List[str]:
        """
        Get candidate fact tables for a single metric at the given grain/platform.
        Raises MetricResolutionError if grain unsupported or no candidates found.
        """
        m = self.registry.get_metric_def(canonical_key)

        # grain compatibility check
        supported = [g.lower() for g in m.get("supported_grains", [])]
        if grain.lower() not in supported:
            roll = m.get("rollup", {}) or {}
            if roll.get("allowed") is True:
                if roll.get("requires_explicit") is True:
                    raise MetricResolutionError(
                        f"Metric '{canonical_key}' is not valid at grain '{grain}' without an explicit rollup "
                        f"(policy: {roll.get('strategy') or 'unknown'})."
                    )
            raise MetricResolutionError(
                f"Metric '{canonical_key}' is not supported at grain '{grain}'. Supported: {supported}"
            )

        pft = m.get("preferred_fact_table", {}) or {}
        grain_map = pft.get(grain, {}) or {}

        options = grain_map.get(platform_key)
        if not options:
            if len(grain_map) == 1:
                options = next(iter(grain_map.values()))
            else:
                raise MetricResolutionError(
                    f"Metric '{canonical_key}' has no preferred_fact_table for grain='{grain}', platform='{platform_key}'."
                )

        return list(options)

    def choose_fact_table(
        self,
        canonical_keys: List[str],
        grain: str,
        platform: Optional[str],
    ) -> str:
        """
        Choose a single fact table that satisfies ALL requested metrics for (grain, platform).
        Strategy:
          - For each metric, take preferred_fact_table[grain][platform] (if present) else error.
          - Intersect across metrics; if intersection non-empty choose first metric's first option.
        """
        platform_key = (platform or "none").lower()

        candidates_per_metric: List[List[str]] = []
        for k in canonical_keys:
            candidates_per_metric.append(self._get_metric_candidates(k, grain, platform_key))

        # intersect
        sets = [set(map(_norm_table_name, opts)) for opts in candidates_per_metric]
        common = set.intersection(*sets) if sets else set()
        if not common:
            raise MetricResolutionError(
                f"Could not find a single fact table that supports all metrics at grain='{grain}', platform='{platform_key}'."
            )

        # pick the first option from the first metric that is in common (preserves registry preference order)
        first_opts = candidates_per_metric[0]
        for opt in first_opts:
            if _norm_table_name(opt) in common:
                return opt

        # fallback: pick any stable option
        return sorted(common)[0]

    def partition_metrics(
        self,
        metric_names: List[str],
        grain: str,
        platform: Optional[str],
    ) -> List[Tuple[str, List[str]]]:
        """
        Partition metrics into groups that can each be resolved by a single fact table.

        Returns list of (fact_table, [metric_names]) tuples.
        If ALL metrics share a single fact table, returns a single-element list.
        Otherwise, greedily groups metrics by compatible fact table.
        """
        platform_key = (platform or "none").lower()

        # Canonicalize all metrics and expand derived bases
        canonical_map: Dict[str, str] = {}  # original_name -> canonical_key
        for name in metric_names:
            canonical_map[name] = self.registry.canonicalize(name)

        # Expand derived metrics to include base metrics (same as resolve_metrics)
        expanded: List[str] = []
        for name in metric_names:
            k = canonical_map[name]
            m = self.registry.get_metric_def(k)
            if (m.get("default_aggregation") == "derived") or m.get("derived_formula"):
                for b in m.get("base_metrics", []) or []:
                    bk = self.registry.canonicalize(b)
                    if bk not in expanded:
                        expanded.append(bk)
            if k not in expanded:
                expanded.append(k)

        # Try single-table path first (fast path, no change to current behavior)
        try:
            table = self.choose_fact_table(expanded, grain, platform)
            return [(table, list(metric_names))]
        except MetricResolutionError:
            pass

        # Get candidates per expanded metric
        metric_candidates: Dict[str, List[str]] = {}
        for k in expanded:
            metric_candidates[k] = self._get_metric_candidates(k, grain, platform_key)

        # Build groups: each group is (fact_table_norm, set_of_candidate_norms, [canonical_keys])
        groups: List[Tuple[Set[str], List[str]]] = []  # (candidate_set_norm, [canonical_keys])

        # Track which group derived metrics' bases are in
        base_group_idx: Dict[str, int] = {}

        for k in expanded:
            cands_norm = set(map(_norm_table_name, metric_candidates[k]))

            # If this metric's base is already assigned, put it in the same group
            assigned = False
            for idx, (group_cands, group_keys) in enumerate(groups):
                overlap = group_cands & cands_norm
                if overlap:
                    groups[idx] = (overlap, group_keys + [k])
                    base_group_idx[k] = idx
                    assigned = True
                    break

            if not assigned:
                base_group_idx[k] = len(groups)
                groups.append((cands_norm, [k]))

        # Resolve each group to a concrete fact table and map back to original names
        result: List[Tuple[str, List[str]]] = []
        for cands_norm, group_keys in groups:
            # Choose fact table for this group
            table = self.choose_fact_table(group_keys, grain, platform)

            # Map canonical keys back to original metric names
            original_names = []
            group_set = set(group_keys)
            for name in metric_names:
                k = canonical_map[name]
                if k in group_set:
                    original_names.append(name)
                    # Also check if this derived metric's bases are in this group
                elif (self.registry.get_metric_def(k).get("default_aggregation") == "derived"
                      or self.registry.get_metric_def(k).get("derived_formula")):
                    bases = [self.registry.canonicalize(b) for b in
                             (self.registry.get_metric_def(k).get("base_metrics", []) or [])]
                    if any(b in group_set for b in bases):
                        original_names.append(name)

            if original_names:
                result.append((table, original_names))

        # Sort by number of metrics (largest group first)
        result.sort(key=lambda x: len(x[1]), reverse=True)

        return result

    def resolve_metrics(
        self,
        metric_names: List[str],
        grain: str,
        platform: Optional[str],
        fact_alias: str = "f",
        forced_fact_table: Optional[str] = None,
    ) -> Tuple[str, List[ResolvedMetric]]:
        """
        Returns (chosen_fact_table, resolved_metrics_in_select_order)

        Derived metrics:
          - Ensure bases are resolved first (and included in SELECT)
          - Derived expression uses base aggregated expressions
        """
        canonical_requested = [self.registry.canonicalize(x) for x in metric_names]

        # Expand derived metrics to include base metrics
        expanded: List[str] = []
        for k in canonical_requested:
            m = self.registry.get_metric_def(k)
            if (m.get("default_aggregation") == "derived") or m.get("derived_formula"):
                for b in m.get("base_metrics", []) or []:
                    bk = self.registry.canonicalize(b)
                    if bk not in expanded:
                        expanded.append(bk)
            if k not in expanded:
                expanded.append(k)

        chosen_fact = forced_fact_table or self.choose_fact_table(expanded, grain, platform)

        # First resolve all base metrics (non-derived) that exist in expanded
        base_resolved: Dict[str, ResolvedMetric] = {}
        select_list: List[ResolvedMetric] = []

        for k in expanded:
            m = self.registry.get_metric_def(k)
            is_derived = (m.get("default_aggregation") == "derived") or bool(m.get("derived_formula"))
            if is_derived:
                continue

            agg = (m.get("default_aggregation") or "sum").lower()
            if agg not in ("sum", "avg", "min", "max", "count"):
                raise MetricResolutionError(f"Unsupported aggregation '{agg}' for metric '{k}'")

            # Find the physical column for the chosen fact table
            base_cols = m.get("base_columns", []) or []
            match = None
            for bc in base_cols:
                t = bc.get("table", "")
                if _norm_table_name(t) == _norm_table_name(chosen_fact):
                    match = bc
                    break
            if not match:
                raise MetricResolutionError(
                    f"Metric '{k}' has no base_column for chosen fact table '{chosen_fact}'."
                )

            col = match["column"]
            expr = f"{agg.upper()}({fact_alias}.[{col}])"
            select_sql = f"{expr} AS [{k}]"
            rm = ResolvedMetric(
                requested_name=k,
                canonical_key=k,
                fact_table=chosen_fact,
                select_sql=select_sql,
                is_derived=False,
                depends_on=tuple(),
            )
            base_resolved[k] = rm
            select_list.append(rm)

        # Now resolve derived metrics (only those originally requested, not every base)
        for k in canonical_requested:
            m = self.registry.get_metric_def(k)
            is_derived = (m.get("default_aggregation") == "derived") or bool(m.get("derived_formula"))
            if not is_derived:
                continue

            formula = (m.get("derived_formula") or "").strip()
            bases = [self.registry.canonicalize(b) for b in (m.get("base_metrics", []) or [])]
            if not formula or not bases:
                raise MetricResolutionError(f"Derived metric '{k}' missing derived_formula or base_metrics")

            # Replace tokens in formula with the aggregated base expressions (without aliases)
            # We use the exact agg SQL for each base: e.g. SUM(f.[Clicks])
            token_map: Dict[str, str] = {}
            for bk in bases:
                if bk not in base_resolved:
                    raise MetricResolutionError(f"Derived metric '{k}' depends on '{bk}', which did not resolve.")
                base_expr = base_resolved[bk].select_sql.split(" AS ")[0]
                token_map[bk] = base_expr

            # Very small safe parser: replace whole-word tokens of base metric keys
            expr = formula
            for token, repl in token_map.items():
                expr = expr.replace(token, repl)

            # Optional: upgrade "a / b" patterns to safe divide if formula is exactly "x / y"
            # Keep it simple: if formula contains a single '/', wrap denom in NULLIF.
            if "/" in formula and formula.count("/") == 1 and "(" not in formula:
                left, right = expr.split("/", 1)
                expr = _safe_divide_sql(left.strip(), right.strip())

            select_sql = f"{expr} AS [{k}]"
            rm = ResolvedMetric(
                requested_name=k,
                canonical_key=k,
                fact_table=chosen_fact,
                select_sql=select_sql,
                is_derived=True,
                depends_on=tuple(bases),
            )
            select_list.append(rm)

        return chosen_fact, select_list

def _print_resolved(chosen_fact: str, resolved: List[ResolvedMetric], as_json: bool) -> None:
    if as_json:
        import json
        payload = {
            "chosen_fact_table": chosen_fact,
            "metrics": [
                {
                    "requested_name": m.requested_name,
                    "canonical_key": m.canonical_key,
                    "fact_table": m.fact_table,
                    "select_sql": m.select_sql,
                    "is_derived": m.is_derived,
                    "depends_on": list(m.depends_on),
                }
                for m in resolved
            ],
        }
        print(json.dumps(payload, indent=2))
        return

    print(f"CHOSEN FACT: {chosen_fact}\n")
    print("SELECT:")
    for m in resolved:
        print(f"  {m.select_sql}")


def main() -> None:
    import argparse
    import json
    from pathlib import Path

    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", required=True, help="Path to metric_registry.json")
    ap.add_argument("--metrics", nargs="+", required=True, help="Metric names/aliases, e.g. clicks impressions cost")
    ap.add_argument("--grain", required=True, help="Grain, e.g. campaign_calendar")
    ap.add_argument("--platform", default=None, help="Platform, e.g. google_ads / microsoft_ads")

    # match join_planner ergonomics
    ap.add_argument("--out", default=None, help="Write resolved output JSON to this path (like join_planner --out)")
    ap.add_argument("--fact_alias", default="fact", help="Alias used in SQL expressions (default matches join_planner: fact)")
    ap.add_argument("--force_fact", default=None, help="Force a specific fact table (optional)")

    args = ap.parse_args()

    reg = MetricRegistry.from_path(args.registry)
    resolver = MetricResolver(reg)

    try:
        chosen_fact, resolved = resolver.resolve_metrics(
            metric_names=args.metrics,
            grain=args.grain,
            platform=args.platform,
            fact_alias=args.fact_alias,
            forced_fact_table=args.force_fact,
        )
    except MetricResolutionError as e:
        raise SystemExit(f"[metric_resolver] ERROR: {e}")

    payload = {
        "grain": args.grain,
        "platform": args.platform,
        "requested_metrics": args.metrics,
        "chosen_fact_table": chosen_fact,
        "select": [m.select_sql for m in resolved],
        "resolved_metrics": [
            {
                "requested_name": m.requested_name,
                "canonical_key": m.canonical_key,
                "fact_table": m.fact_table,
                "select_sql": m.select_sql,
                "is_derived": m.is_derived,
                "depends_on": list(m.depends_on),
            }
            for m in resolved
        ],
    }

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()

