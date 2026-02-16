from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional, Union


# -----------------------------
# Quoting helpers (SQL Server)
# -----------------------------

def _q_ident(name: str) -> str:
    return f"[{name}]"

def _q_col(alias: str, col: str) -> str:
    return f"{alias}.{_q_ident(col)}"

def _escape_sql_string(v: str) -> str:
    return v.replace("'", "''")


# ----------------------------------------
# Normalization helpers (shape hardening)
# ----------------------------------------

def _resolve_fact_table_name(physical_plan: Dict[str, Any]) -> Optional[str]:
    """
    sql_builder should accept these shapes:
      - physical_plan["fact_table"] = {"table": "..."} or {"logical_name": "..."}
      - physical_plan["fact_tables"] = [{"table": "..."}]
      - physical_plan["fact_table"] = "SomeTable"
    """
    ft = physical_plan.get("fact_table")

    if isinstance(ft, dict):
        return ft.get("table") or ft.get("logical_name")

    if isinstance(ft, str) and ft.strip():
        return ft.strip()

    fts = physical_plan.get("fact_tables") or []
    if fts and isinstance(fts[0], dict):
        return fts[0].get("table") or fts[0].get("logical_name")

    return None


def _get_where_items(physical_plan: Dict[str, Any]) -> List[Any]:
    """
    Supports multiple keys seen during notebook->module extraction:
      - where_clauses (preferred)
      - where_clauses (legacy typo)
      - where (string or list)
    """
    wheres = (
        physical_plan.get("where_clauses")
        or physical_plan.get("where_clauses")
        or physical_plan.get("where")
        or []
    )
    if isinstance(wheres, str):
        return [wheres]
    if isinstance(wheres, list):
        return wheres
    return []


def _get_group_by_items(physical_plan: Dict[str, Any]) -> List[Any]:
    gb = physical_plan.get("group_by") or []
    if isinstance(gb, str):
        return [gb]
    if isinstance(gb, list):
        return gb
    return []


def _get_order_by_items(physical_plan: Dict[str, Any]) -> List[Any]:
    ob = physical_plan.get("order_by") or []
    if isinstance(ob, str):
        return [ob]
    if isinstance(ob, list):
        return ob
    return []


# -----------------------------
# Alias map
# -----------------------------

def build_table_alias_map(physical_plan: Dict[str, Any]) -> Dict[str, str]:
    """
    Stable alias map:
      - fact table: f
      - joins: d1, d2, ...
    """
    alias_map: Dict[str, str] = {}
    fact_table = _resolve_fact_table_name(physical_plan) or "fact"
    alias_map[fact_table] = "f"
    alias_map["__fact__"] = "f"

    joins = physical_plan.get("joins") or []
    for i, j in enumerate(joins, start=1):
        t = None
        if isinstance(j, dict):
            t = j.get("right_table") or j.get("table")
        if t and t not in alias_map:
            alias_map[t] = f"d{i}"

    return alias_map


# -----------------------------
# SELECT
# -----------------------------

def build_select_list(spec: Dict[str, Any], alias_map: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Prefer physical_plan['select_list'] if present.
    Fallback: synthesize from logical_query metrics/dimensions.
    """
    pp = spec.get("physical_plan") or {}
    if isinstance(pp.get("select_list"), list) and pp["select_list"]:
        return pp["select_list"]

    lq = spec.get("logical_query") or {}
    fact_alias = alias_map.get("__fact__", "f")
    out: List[Dict[str, Any]] = []

    for d in (lq.get("dimensions") or []):
        if not isinstance(d, dict):
            continue
        col = d.get("resolved_column") or d.get("column")
        tbl = d.get("resolved_table") or d.get("table")
        alias = d.get("alias") or d.get("semantic_name") or col
        a = alias_map.get(tbl) if (tbl and tbl in alias_map) else fact_alias
        if col:
            out.append(
                {"expression_type": "dimension", "table": tbl, "column": col, "alias": alias, "table_alias": a}
            )

    for m in (lq.get("metrics") or []):
        if not isinstance(m, dict):
            continue
        col = m.get("resolved_column") or m.get("column")
        tbl = m.get("resolved_table") or m.get("table")
        alias = m.get("alias") or m.get("semantic_name") or col
        agg = (m.get("aggregation") or "sum").lower()
        a = alias_map.get(tbl) if (tbl and tbl in alias_map) else fact_alias
        if col:
            out.append(
                {"expression_type": "metric", "table": tbl, "column": col, "alias": alias, "aggregation": agg, "table_alias": a}
            )

    return out


def build_select_sql(spec: Dict[str, Any], alias_map: Dict[str, str]) -> Tuple[str, List[Dict[str, Any]]]:
    select_list = build_select_list(spec, alias_map)
    parts: List[str] = []

    for item in select_list:
        if not isinstance(item, dict):
            continue

        # Allow pre-rendered SQL
        if isinstance(item.get("sql"), str) and item["sql"].strip():
            alias = item.get("alias") or "value"
            parts.append(f"{item['sql'].strip()} AS {_q_ident(alias)}")
            continue

        et = (item.get("expression_type") or "").lower()

        tbl = item.get("table")
        a = item.get("table_alias") or (alias_map.get(tbl) if tbl else None) or alias_map.get("__fact__", "f")

        alias = item.get("alias") or item.get("name") or item.get("semantic_name") or item.get("column") or "value"

        if et == "dimension":
            col = item.get("column")
            if col:
                parts.append(f"{_q_col(a, col)} AS {_q_ident(alias)}")
            continue

        if et == "metric":
            col = item.get("column")
            agg = (item.get("aggregation") or "sum").lower()
            if col:
                if agg in ("sum", "avg", "min", "max", "count"):
                    parts.append(f"{agg.upper()}({_q_col(a, col)}) AS {_q_ident(alias)}")
                else:
                    parts.append(f"{_q_col(a, col)} AS {_q_ident(alias)}")
            continue

        if et == "derived":
            # Derived metric safe rendering
            num = item.get("numerator") or {}
            den = item.get("denominator") or {}

            na = (num.get("table_alias") if isinstance(num, dict) else None) or a
            da = (den.get("table_alias") if isinstance(den, dict) else None) or a

            ncol = num.get("column") if isinstance(num, dict) else None
            dcol = den.get("column") if isinstance(den, dict) else None

            nagg = (num.get("aggregation") if isinstance(num, dict) else None) or "sum"
            dagg = (den.get("aggregation") if isinstance(den, dict) else None) or "sum"

            if ncol and dcol:
                n_expr = f"{str(nagg).upper()}({_q_col(na, ncol)})"
                d_expr = f"{str(dagg).upper()}({_q_col(da, dcol)})"
                parts.append(f"({n_expr} / NULLIF({d_expr}, 0)) AS {_q_ident(alias)}")
            continue

        # last resort: raw column if present
        col = item.get("column")
        if col:
            parts.append(f"{_q_col(a, col)} AS {_q_ident(alias)}")

    if not parts:
        # deterministic fallback
        parts = ["1 AS [value]"]

    return ",\n       ".join(parts), select_list


# -----------------------------
# FROM + JOINS
# -----------------------------

def build_from_and_joins(physical_plan: Dict[str, Any], alias_map: Dict[str, str]) -> str:
    fact_table = _resolve_fact_table_name(physical_plan) or "fact"
    fact_alias = alias_map.get(fact_table) or alias_map.get("__fact__", "f")

    parts = [f"FROM {_q_ident(fact_table)} AS {fact_alias}"]

    joins = physical_plan.get("joins") or []
    for j in joins:
        if not isinstance(j, dict):
            continue
        jt = (j.get("join_type") or "LEFT").upper()
        rt = j.get("right_table") or j.get("table")
        if not rt:
            continue

        ra = alias_map.get(rt) or "d"
        l_alias = j.get("left_alias") or fact_alias
        l_col = j.get("left_column")
        r_col = j.get("right_column")

        # Hardening: only emit joins with both columns present
        if l_col and r_col:
            parts.append(
                f"{jt} JOIN {_q_ident(rt)} AS {ra} ON {_q_col(l_alias, l_col)} = {_q_col(ra, r_col)}"
            )

    return "\n".join(parts)


# -----------------------------
# WHERE
# -----------------------------

def _where_item_to_sql(item: Any, alias_map: Dict[str, str]) -> Optional[str]:
    if isinstance(item, str):
        s = item.strip()
        return s if s else None

    if not isinstance(item, dict):
        return None

    if isinstance(item.get("sql"), str) and item["sql"].strip():
        return item["sql"].strip()

    table = item.get("table")
    col = item.get("column")
    op = (item.get("operator") or "").upper()
    val = item.get("value")

    if table and col and op:
        a = alias_map.get(table) or alias_map.get("__fact__", "f")
        lhs = _q_col(a, col)

        if op in ("LIKE", "="):
            if val is None:
                return None
            if isinstance(val, (int, float)):
                rhs = str(val)
            else:
                rhs = f"'{_escape_sql_string(str(val))}'"
            return f"{lhs} {op} {rhs}"

        if op == "IN":
            if not isinstance(val, list) or not val:
                return None
            vals: List[str] = []
            for x in val:
                if isinstance(x, (int, float)):
                    vals.append(str(x))
                else:
                    vals.append(f"'{_escape_sql_string(str(x))}'")
            return f"{lhs} IN ({', '.join(vals)})"

    return None


def build_where_clause_sql(physical_plan: Dict[str, Any], alias_map: Dict[str, str]) -> str:
    wheres = _get_where_items(physical_plan)
    if not wheres:
        return ""

    rendered: List[str] = []
    for w in wheres:
        s = _where_item_to_sql(w, alias_map)
        if s:
            rendered.append(s)

    if not rendered:
        return ""
    return "WHERE " + "\n  AND ".join(rendered)


# -----------------------------
# GROUP BY / ORDER BY
# -----------------------------

def build_group_by_sql(physical_plan: Dict[str, Any], alias_map: Dict[str, str]) -> str:
    group_items = _get_group_by_items(physical_plan)
    if not group_items:
        return ""

    # Accept strings; ignore dicts for now (planner should pre-render)
    rendered = [g.strip() for g in group_items if isinstance(g, str) and g.strip()]
    if not rendered:
        return ""
    return "GROUP BY " + ", ".join(rendered)


def build_order_by_sql(physical_plan: Dict[str, Any], select_list: List[Dict[str, Any]]) -> str:
    order_items = _get_order_by_items(physical_plan)
    if not order_items:
        return ""

    rendered = [o.strip() for o in order_items if isinstance(o, str) and o.strip()]
    if not rendered:
        return ""
    return "ORDER BY " + ", ".join(rendered)


# -----------------------------
# FINAL SQL
# -----------------------------

def build_sql_from_spec(spec: Dict[str, Any]) -> str:
    physical_plan = spec.get("physical_plan") or {}
    logical_query = spec.get("logical_query") or {}

    limit_block = logical_query.get("limit") or {}
    limit_value = limit_block.get("value")
    top_clause = f"TOP ({limit_value}) " if isinstance(limit_value, int) else ""

    alias_map = build_table_alias_map(physical_plan)

    select_sql, select_list = build_select_sql(spec, alias_map)
    from_sql = build_from_and_joins(physical_plan, alias_map)
    where_sql = build_where_clause_sql(physical_plan, alias_map)
    group_by_sql = build_group_by_sql(physical_plan, alias_map)
    order_by_sql = build_order_by_sql(physical_plan, select_list)

    parts: List[str] = [
        f"SELECT {top_clause}{select_sql}",
        from_sql,
    ]
    if where_sql:
        parts.append(where_sql)
    if group_by_sql:
        parts.append(group_by_sql)
    if order_by_sql:
        parts.append(order_by_sql)

    return "\n".join(parts)
