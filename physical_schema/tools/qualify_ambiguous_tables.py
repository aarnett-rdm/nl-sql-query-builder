#!/usr/bin/env python3
"""
Qualify ambiguous table names in metric_registry.json using physical_schema_enriched.json.

Policy:
- Only qualify when an unqualified table name maps to >1 physical tables.
- Prefer candidates that contain the referenced column(s) (for base_columns).
- For preferred_fact_table entries, prefer candidates already chosen in base_columns for the same metric.
- If still ambiguous, prefer schemas in a configured order; otherwise choose deterministically and emit a warning.

This keeps backward compatibility while preventing "wrong-table" resolution when duplicate names exist across schemas
(e.g., GoTicketsBronze.Order vs GoTicketsCoreEntity.Order).
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

PREFERRED_SCHEMAS = [
    "GoTicketsPerformanceMetric",
    "GoTicketsCoreEntity",
    "GoTicketsOrderMetric",
    "GoTicketsBronze",
]

def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def build_alias_map(physical_tables: Dict[str, dict]) -> Dict[str, List[str]]:
    alias: Dict[str, List[str]] = {}
    for fq in physical_tables.keys():
        name = fq.split(".", 1)[-1]
        alias.setdefault(name, []).append(fq)
    return alias

def table_has_columns(physical_tables: Dict[str, dict], fq: str, cols: List[str]) -> bool:
    tcols = set(physical_tables[fq]["columns"].keys())
    return all(c in tcols for c in cols)

def resolve_table(
    name: str,
    *,
    physical_tables: Dict[str, dict],
    alias_map: Dict[str, List[str]],
    required_cols: Optional[List[str]] = None,
    context_fq_candidates: Optional[Set[str]] = None,
    prefer_schemas: Optional[List[str]] = None,
) -> Tuple[str, Optional[str]]:
    """
    Returns (resolved_name, warning_or_none)
    """
    if name in physical_tables:
        return name, None
    if "." in name:
        # maybe case mismatch
        low = {k.lower(): k for k in physical_tables.keys()}
        fixed = low.get(name.lower())
        return (fixed or name), (None if fixed else f"table_not_found: {name}")

    cands = alias_map.get(name, [])
    if len(cands) <= 1:
        return name, (None if cands else f"table_not_found: {name}")

    # ambiguous
    filtered = list(cands)
    if required_cols:
        col_filtered = [fq for fq in filtered if table_has_columns(physical_tables, fq, required_cols)]
        if col_filtered:
            filtered = col_filtered

    if context_fq_candidates:
        for fq in filtered:
            if fq in context_fq_candidates:
                return fq, None

    prefer_schemas = prefer_schemas or []
    for sch in prefer_schemas:
        for fq in filtered:
            if fq.lower().startswith(sch.lower() + "."):
                return fq, None

    chosen = sorted(filtered)[0]
    return chosen, f"ambiguous_table_resolved_deterministically: {name} -> {chosen}"

def main(
    physical_schema_path: str,
    metric_registry_path: str,
    output_path: str,
) -> None:
    physical = load_json(Path(physical_schema_path))
    registry = load_json(Path(metric_registry_path))

    physical_tables: Dict[str, dict] = physical["tables"]
    alias_map = build_alias_map(physical_tables)

    changes = []
    warnings = []

    for metric_name, metric in registry.get("metrics", {}).items():
        ctx: Set[str] = set()

        # base_columns
        for bc in metric.get("base_columns", []) or []:
            t = bc.get("table")
            col = bc.get("column")
            new_t, warn = resolve_table(
                t,
                physical_tables=physical_tables,
                alias_map=alias_map,
                required_cols=[col] if col else None,
                prefer_schemas=PREFERRED_SCHEMAS,
            )
            if warn:
                warnings.append({"metric": metric_name, "where": "base_columns", "table": t, "warning": warn})
            if new_t != t:
                changes.append({"metric": metric_name, "where": "base_columns", "from": t, "to": new_t})
                bc["table"] = new_t
            if "." in bc["table"] and bc["table"] in physical_tables:
                ctx.add(bc["table"])

        # preferred_fact_table
        pft = metric.get("preferred_fact_table") or {}
        if isinstance(pft, dict):
            for grain, byplat in pft.items():
                if not isinstance(byplat, dict):
                    continue
                for plat, tlist in byplat.items():
                    if not isinstance(tlist, list):
                        continue
                    for i, t in enumerate(tlist):
                        new_t, warn = resolve_table(
                            t,
                            physical_tables=physical_tables,
                            alias_map=alias_map,
                            context_fq_candidates=ctx,
                            prefer_schemas=PREFERRED_SCHEMAS,
                        )
                        if warn:
                            warnings.append({"metric": metric_name, "where": f"preferred_fact_table.{grain}.{plat}", "table": t, "warning": warn})
                        if new_t != t:
                            changes.append({"metric": metric_name, "where": f"preferred_fact_table.{grain}.{plat}", "from": t, "to": new_t})
                            tlist[i] = new_t

    out = {
        "registry": registry,
        "_qualification_summary": {
            "changes": len(changes),
            "warnings": len(warnings),
            "changes_detail": changes[:50],
            "warnings_detail": warnings[:50],
        }
    }

    Path(output_path).write_text(json.dumps(out, indent=2), encoding="utf-8")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--physical", required=True, help="physical_schema_enriched.json")
    ap.add_argument("--registry", required=True, help="metric_registry.json")
    ap.add_argument("--out", required=True, help="output path")
    args = ap.parse_args()
    main(args.physical, args.registry, args.out)
