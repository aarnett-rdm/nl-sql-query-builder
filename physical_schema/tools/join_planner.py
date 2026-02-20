#!/usr/bin/env python3
"""
join_planner.py

Conservative join planner for Fabric warehouses when FK constraints are not declared.

Features:
- Platform-aware dimension preference for common Ads entities (Campaign/Account/AdGroup).
- Platform-based candidate filtering when multiple PK matches exist for an *Id column.
- Optional dimension-chaining cleanup (keeps join plans minimal & hierarchical).
- Optional T-SQL FROM/JOIN emission via --emit_sql.

Works directly off your physical_schema.json.
"""

from __future__ import annotations

import argparse
import heapq
import json
import math
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple


# ----------------------------
# Config / weights
# ----------------------------

CONF_WEIGHT = {"high": 1.0, "medium": 2.0, "low": 3.0}

try:
    from tools.common import (
        PLATFORM_TOKEN, DIM_PREFERENCE,
        bracket_ident as _bracket_ident, tsql_qualified_table as _tsql_qualified_table,
        make_aliases as _make_aliases,
    )
except ImportError:
    from common import (
        PLATFORM_TOKEN, DIM_PREFERENCE,
        bracket_ident as _bracket_ident, tsql_qualified_table as _tsql_qualified_table,
        make_aliases as _make_aliases,
    )


# ----------------------------
# Data structures
# ----------------------------

@dataclass(frozen=True)
class JoinEdge:
    from_table: str
    to_table: str
    from_columns: Tuple[str, ...]
    to_columns: Tuple[str, ...]
    confidence: str
    evidence: Dict[str, Any]

    @property
    def weight(self) -> float:
        return CONF_WEIGHT.get(self.confidence, 2.5)


@dataclass
class JoinStep:
    left_table: str
    right_table: str
    left_columns: Tuple[str, ...]
    right_columns: Tuple[str, ...]
    confidence: str
    evidence: Dict[str, Any]


# ----------------------------
# Physical schema wrapper
# ----------------------------

def _looks_like_mapping_table(table_name: str) -> bool:
    """
    Conservative heuristic: only allow composite-PK "Id column" joins
    to tables that look like mapping/bridge tables.
    """
    n = table_name.lower()
    return (
        "entitymap" in n
        or n.endswith("map")
        or "campaign" in n and "eventmap" in n
        or "adgroup" in n and "eventmap" in n
        or "map" in n
    )


class PhysicalSchema:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self.payload = payload
        self.tables: Dict[str, Any] = payload.get("tables", {}) or {}

        alias_map = (payload.get("table_alias_resolution") or {}).get("aliases") or {}
        self.aliases: Dict[str, str] = {}
        for k, v in alias_map.items():
            self.aliases[k] = v
            self.aliases[k.lower()] = v

        self.cols_by_table: Dict[str, Dict[str, Any]] = {}
        self.pk_by_table: Dict[str, Tuple[str, ...]] = {}

        for t, meta in self.tables.items():
            self.cols_by_table[t] = meta.get("columns") or {}
            self.pk_by_table[t] = tuple(meta.get("primary_key") or ())

        # PK index: pk_col -> [tables]
        # IMPORTANT: include composite PK tables too, because many bridge tables
        # have PK like (CampaignId, EventId) and we still want lookup by EventId.
        self.tables_by_pkcol: Dict[str, List[str]] = {}
        for t, pk in self.pk_by_table.items():
            for c in pk:
                self.tables_by_pkcol.setdefault(c, []).append(t)

        # seed edges if schema includes relationships already
        self.seed_from: Dict[str, List[JoinEdge]] = {}
        rel = payload.get("relationships") or {}
        for kind in ("declared_foreign_keys", "inferred_foreign_keys"):
            for fk in (rel.get(kind) or []):
                e = JoinEdge(
                    from_table=fk["from_table"],
                    to_table=fk["to_table"],
                    from_columns=tuple(fk["from_columns"]),
                    to_columns=tuple(fk["to_columns"]),
                    confidence=fk.get("confidence", "medium"),
                    evidence=fk.get("evidence", {"source": kind}),
                )
                self.seed_from.setdefault(e.from_table, []).append(e)

    def resolve_table(self, name: str) -> str:
        if name in self.tables:
            return name
        if name in self.aliases:
            return self.aliases[name]
        if name.lower() in self.aliases:
            return self.aliases[name.lower()]

        # unqualified suffix match (unique only)
        if "." not in name:
            matches = [t for t in self.tables if t.split(".")[-1].lower() == name.lower()]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                # Prefer CoreEntity > other schemas > Bronze (last resort)
                core_matches = [m for m in matches if m.startswith("GoTicketsCoreEntity.")]
                if core_matches:
                    return core_matches[0]  # Use CoreEntity if available

                # Exclude Bronze if other options exist
                non_bronze = [m for m in matches if not m.startswith("GoTicketsBronze.")]
                if non_bronze:
                    return non_bronze[0]  # Use non-Bronze if available

                # Last resort: use Bronze
                return matches[0]

        raise ValueError(f"Unknown table '{name}'")

    def _platform_filter(self, candidates: List[str], platform: Optional[str]) -> List[str]:
        """Prefer platform-matching candidates when platform is provided."""
        if not platform:
            return candidates

        token = PLATFORM_TOKEN.get(platform)
        if not token:
            return candidates

        # Always allow Utility.*
        keep = [c for c in candidates if c.startswith("Utility.")]
        plat = [c for c in candidates if token in c.lower()]

        if plat:
            out: List[str] = []
            seen = set()
            for c in keep + plat:
                if c not in seen:
                    out.append(c)
                    seen.add(c)
            return out

        return candidates

    def neighbors(self, table: str, platform: Optional[str] = None) -> List[JoinEdge]:
        """Outgoing edges from a table (seed + lazy inferred)."""
        t = self.resolve_table(table)
        edges: List[JoinEdge] = []
        edges.extend(self.seed_from.get(t, []))

        cols = self.cols_by_table.get(t, {})

        # CalendarId -> Utility.DimCalendar (special-cased)
        if (
            "CalendarId" in cols
            and "Utility.DimCalendar" in self.tables
            and self.pk_by_table.get("Utility.DimCalendar") == ("CalendarId",)
        ):
            edges.append(
                JoinEdge(
                    from_table=t,
                    to_table="Utility.DimCalendar",
                    from_columns=("CalendarId",),
                    to_columns=("CalendarId",),
                    confidence="high",
                    evidence={"rule": "calendarid_to_dimcalendar"},
                )
            )

        # Generic: XId -> any table with PK containing XId
        # - high confidence if PK is exactly (XId,)
        # - medium confidence if XId is part of composite PK, but ONLY for mapping/bridge-like tables
        for col in cols.keys():
            if not col.lower().endswith("id"):
                continue

            candidates = [c for c in self.tables_by_pkcol.get(col, []) if c != t]
            if not candidates:
                continue

            # 1) Hard preference for common Ads dims (when candidates include the preferred table)
            if platform:
                pref = DIM_PREFERENCE.get((platform, col))
                if pref and pref in candidates:
                    edges.append(
                        JoinEdge(
                            from_table=t,
                            to_table=pref,
                            from_columns=(col,),
                            to_columns=(col,),
                            confidence="high",
                            evidence={
                                "rule": "platform_dim_preference",
                                "platform": platform,
                                "preferred": pref,
                                "candidates_filtered_for_platform": self._platform_filter(candidates, platform),
                            },
                        )
                    )
                    # Deterministic: once we have a strong preference, do not emit alternates.
                    continue

            # 2) Platform filter if possible
            filtered = self._platform_filter(candidates, platform)

            # Split into single-PK and composite-PK candidates
            single_pk: List[str] = []
            composite_pk_mapping: List[str] = []

            for cand in filtered:
                pk = self.pk_by_table.get(cand, ())
                if not pk:
                    continue
                if pk == (col,):
                    single_pk.append(cand)
                elif col in pk and _looks_like_mapping_table(cand):
                    composite_pk_mapping.append(cand)

            # Prefer single-column PK matches
            if single_pk:
                if len(single_pk) == 1:
                    edges.append(
                        JoinEdge(
                            from_table=t,
                            to_table=single_pk[0],
                            from_columns=(col,),
                            to_columns=(col,),
                            confidence="high",
                            evidence={
                                "rule": "id_column_to_single_pk_match_after_platform_filter",
                                "platform": platform,
                                "candidates": candidates,
                                "filtered": filtered,
                            },
                        )
                    )
                else:
                    for cand in single_pk:
                        edges.append(
                            JoinEdge(
                                from_table=t,
                                to_table=cand,
                                from_columns=(col,),
                                to_columns=(col,),
                                confidence="medium",
                                evidence={
                                    "rule": "id_column_to_multi_pk_match",
                                    "platform": platform,
                                    "candidates": candidates,
                                    "filtered": filtered,
                                },
                            )
                        )
                continue

            # No single-PK matches; allow *mapping-table* composite-PK matches (medium)
            if composite_pk_mapping:
                # If exactly one, we can be slightly more confident
                conf = "medium" if len(composite_pk_mapping) > 1 else "high"
                for cand in composite_pk_mapping:
                    edges.append(
                        JoinEdge(
                            from_table=t,
                            to_table=cand,
                            from_columns=(col,),
                            to_columns=(col,),
                            confidence=conf,
                            evidence={
                                "rule": "id_column_in_composite_pk_mapping_table",
                                "platform": platform,
                                "pk": list(self.pk_by_table.get(cand, ())),
                                "candidates": candidates,
                                "filtered": filtered,
                            },
                        )
                    )

        return edges


# ----------------------------
# Pathfinding
# ----------------------------

def dijkstra(
    schema: PhysicalSchema,
    start: str,
    goal: str,
    platform: Optional[str],
    max_visits: int = 6000
) -> Optional[List[JoinEdge]]:
    start = schema.resolve_table(start)
    goal = schema.resolve_table(goal)

    pq: List[Tuple[float, str]] = [(0.0, start)]
    dist: Dict[str, float] = {start: 0.0}
    prev: Dict[str, Tuple[str, JoinEdge]] = {}
    visits = 0

    while pq and visits < max_visits:
        d, u = heapq.heappop(pq)
        visits += 1

        if u == goal:
            break
        if d > dist.get(u, math.inf):
            continue

        for e in schema.neighbors(u, platform=platform):
            v = e.to_table
            nd = d + e.weight
            if nd < dist.get(v, math.inf):
                dist[v] = nd
                prev[v] = (u, e)
                heapq.heappush(pq, (nd, v))

    if goal not in dist:
        return None

    path: List[JoinEdge] = []
    cur = goal
    while cur != start:
        pu, pe = prev[cur]
        path.append(pe)
        cur = pu
    path.reverse()
    return path


# ----------------------------
# Optimization: dimension chaining
# ----------------------------

def _prefer_dimension_chaining(steps: List[JoinStep]) -> List[JoinStep]:
    """
    Prefer Campaign -> Account over Fact -> Account when both exist in the plan.

    If the plan contains:
      - Fact -> Campaign
      - Campaign -> Account
      - Fact -> Account
    then Fact -> Account is redundant and will be removed.
    """
    if not steps:
        return steps

    # We'll treat the earliest left_table as "fact" for this cleanup.
    fact = steps[0].left_table

    def is_campaign(t: str) -> bool:
        return t.endswith(".GoogleAdsCampaign") or t.endswith(".MicrosoftAdsCampaign")

    def is_account(t: str) -> bool:
        return t.endswith(".GoogleAdsAccount") or t.endswith(".MicrosoftAdsAccount")

    campaigns = {s.right_table for s in steps if is_campaign(s.right_table)}
    accounts = {s.right_table for s in steps if is_account(s.right_table)}
    if not campaigns or not accounts:
        return steps

    # Determine if we have Campaign->Account join(s)
    campaign_to_account = {(s.left_table, s.right_table) for s in steps if s.left_table in campaigns and s.right_table in accounts}
    if not campaign_to_account:
        return steps

    # Any Fact->Account joins are redundant if Campaign->Account exists
    redundant_fact_to_account = {(s.left_table, s.right_table) for s in steps if s.left_table == fact and s.right_table in accounts}

    out: List[JoinStep] = []
    for s in steps:
        if (s.left_table, s.right_table) in redundant_fact_to_account:
            continue
        out.append(s)
    return out


# ----------------------------
# Planning
# ----------------------------

def plan_joins(schema: PhysicalSchema, fact_table: str, targets: List[str], platform: Optional[str] = None) -> Dict[str, Any]:
    fact = schema.resolve_table(fact_table)

    used = {fact}
    steps: List[JoinStep] = []
    missing: List[Dict[str, Any]] = []
    paths: Dict[str, List[Dict[str, Any]]] = {}

    for tgt in targets:
        try:
            goal = schema.resolve_table(tgt)
        except Exception as exc:
            missing.append({"target": tgt, "reason": str(exc)})
            continue

        if goal in used:
            continue

        path = dijkstra(schema, fact, goal, platform=platform)
        if not path:
            missing.append({"target": goal, "reason": f"No join path from {fact} -> {goal} using current inference rules."})
            continue

        cur_left = fact
        for edge in path:
            if edge.to_table in used:
                cur_left = edge.to_table
                continue

            steps.append(
                JoinStep(
                    left_table=cur_left,
                    right_table=edge.to_table,
                    left_columns=edge.from_columns,
                    right_columns=edge.to_columns,
                    confidence=edge.confidence,
                    evidence=edge.evidence,
                )
            )
            used.add(edge.to_table)
            cur_left = edge.to_table

        paths[goal] = [asdict(edge) for edge in path]

    # Prefer hierarchical joins (Campaign -> Account) over redundant Fact -> Account joins
    steps = _prefer_dimension_chaining(steps)

    return {
        "fact_table": fact,
        "targets": targets,
        "platform": platform,
        "join_steps": [asdict(s) for s in steps],
        "missing": missing,
        "paths": paths,
    }


def default_targets(grain: str, platform: Optional[str] = None) -> List[str]:
    g = grain.lower().strip()
    p = (platform or "").lower().strip()
    t: List[str] = []

    if g in ("campaign_calendar", "adgroup_calendar", "event_calendar"):
        t.append("Utility.DimCalendar")

    # campaign_calendar: keep minimal; campaign->account chaining can be requested later
    # Include Event table + mapping tables to support event date filtering
    if g in ("campaign_calendar", "campaign"):
        if p == "google_ads":
            t += ["GoTicketsCoreEntity.GoogleAdsCampaign"]
            # Add Event path: Campaign → CampaignEventMap → Event
            t += ["GoTicketsEntityMap.GoogleAdsCampaignEventMap", "GoTicketsCoreEntity.Event"]
        elif p == "microsoft_ads":
            t += ["GoTicketsCoreEntity.MicrosoftAdsCampaign"]
            # Add Event path: Campaign → CampaignEventMap → Event
            t += ["GoTicketsEntityMap.MicrosoftAdsCampaignEventMap", "GoTicketsCoreEntity.Event"]
        else:
            # For both platforms or no platform specified, add both Campaign tables + mapping paths
            t += [
                "GoTicketsCoreEntity.GoogleAdsCampaign",
                "GoTicketsCoreEntity.MicrosoftAdsCampaign",
                "GoTicketsEntityMap.GoogleAdsCampaignEventMap",
                "GoTicketsEntityMap.MicrosoftAdsCampaignEventMap",
                "GoTicketsCoreEntity.Event"
            ]

    # adgroup calendar keeps account/campaign because adgroup often needs them together
    if g in ("adgroup_calendar", "adgroup"):
        if p == "google_ads":
            t += ["GoTicketsCoreEntity.GoogleAdsAdGroup", "GoTicketsCoreEntity.GoogleAdsCampaign", "GoTicketsCoreEntity.GoogleAdsAccount"]
        elif p == "microsoft_ads":
            t += ["GoTicketsCoreEntity.MicrosoftAdsAdGroup", "GoTicketsCoreEntity.MicrosoftAdsCampaign", "GoTicketsCoreEntity.MicrosoftAdsAccount"]

    if g in ("event_calendar", "event"):
        t += ["GoTicketsCoreEntity.Event", "GoTicketsCoreEntity.Venue", "GoTicketsCoreEntity.Performer"]

    if g in ("order",):
        t += ["GoTicketsCoreEntity.Order", "GoTicketsCoreEntity.Event", "Utility.DimCalendar"]

    # de-dupe
    seen = set()
    out: List[str] = []
    for x in t:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


# ----------------------------
# T-SQL emission
# ----------------------------

def emit_tsql_from_join_steps(fact_table: str, join_steps: List[JoinStep]) -> str:
    """Return a T-SQL FROM/JOIN block (no SELECT/WHERE/GROUP BY). Uses LEFT JOIN."""
    aliases = _make_aliases(fact_table, join_steps)

    lines: List[str] = []
    lines.append(f"FROM {_tsql_qualified_table(fact_table)} AS {aliases[fact_table]}")

    for s in join_steps:
        left_alias = aliases[s.left_table]
        right_alias = aliases[s.right_table]

        conds: List[str] = []
        for lc, rc in zip(s.left_columns, s.right_columns):
            conds.append(f"{left_alias}.{_bracket_ident(lc)} = {right_alias}.{_bracket_ident(rc)}")

        on_clause = " AND ".join(conds) if conds else "1=1"
        lines.append(f"LEFT JOIN {_tsql_qualified_table(s.right_table)} AS {right_alias} ON {on_clause}")

    return "\n".join(lines)


# ----------------------------
# CLI
# ----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--physical", required=True)
    ap.add_argument("--fact", required=True)
    ap.add_argument("--targets", nargs="*", default=[])
    ap.add_argument("--grain", default=None)
    ap.add_argument("--platform", default=None)
    ap.add_argument("--out", default=None)

    ap.add_argument("--emit_sql", action="store_true")
    ap.add_argument("--sql_only", action="store_true")
    ap.add_argument("--sql_out", default=None)

    args = ap.parse_args()

    # Load schema
    with open(args.physical, "r") as f:
        payload = json.load(f)
    schema = PhysicalSchema(payload)

    # Build targets
    targets = list(args.targets)
    if args.grain:
        targets += default_targets(args.grain, args.platform)

    # Plan joins
    plan = plan_joins(schema, args.fact, targets, platform=args.platform)

    # Output JSON plan (unless sql_only)
    if not args.sql_only:
        if args.out:
            with open(args.out, "w") as f:
                json.dump(plan, f, indent=2)
        else:
            print(json.dumps(plan, indent=2))

    # Optional: emit T-SQL join block
    if args.emit_sql:
        js = [
            JoinStep(
                left_table=x["left_table"],
                right_table=x["right_table"],
                left_columns=tuple(x["left_columns"]),
                right_columns=tuple(x["right_columns"]),
                confidence=x.get("confidence", "medium"),
                evidence=x.get("evidence", {}),
            )
            for x in plan["join_steps"]
        ]

        sql = emit_tsql_from_join_steps(plan["fact_table"], js)

        if args.sql_out:
            with open(args.sql_out, "w") as f:
                f.write(sql + "\n")

        print("\n" + sql + "\n")


if __name__ == "__main__":
    main()
