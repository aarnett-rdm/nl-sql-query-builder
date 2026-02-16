"""
Convert a warehouse schema CSV export to the physical_schema.json format.

Reusable script: re-run whenever the warehouse schema is re-exported.

Usage:
    python schema_csv_to_json.py \\
        --csv  physical_schema/current/updated_schema.csv \\
        --existing physical_schema/current/physical_schema.json \\
        --output physical_schema/current/physical_schema.json
"""

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ── data-type → metadata key mapping ─────────────────────────────────────────
# Mirrors the column metadata format used by the existing physical_schema.json.
CHAR_TYPES = {"varchar", "char", "nvarchar", "nchar", "text", "ntext"}
NUMERIC_TYPES = {"bigint", "int", "smallint", "tinyint", "decimal", "numeric",
                 "float", "real", "money", "smallmoney"}
DATETIME_TYPES = {"datetime2", "datetime", "datetimeoffset", "date", "time",
                  "smalldatetime"}
# float uses numeric_precision only (no scale) in the existing JSON.
FLOAT_TYPES = {"float", "real"}


def _build_column_meta(row: dict) -> dict:
    """Map one CSV row to a column metadata dict."""
    dt = row["data_type"]
    nullable = row["is_nullable"] == "1"
    meta: dict = {"data_type": dt, "nullable": nullable}

    if dt in CHAR_TYPES:
        ml = int(row["max_length"])
        meta["char_max_length"] = ml  # -1 means varchar(max)

    elif dt in NUMERIC_TYPES:
        prec = int(row["precision"])
        if prec:
            meta["numeric_precision"] = prec
        if dt not in FLOAT_TYPES:
            meta["numeric_scale"] = int(row["scale"])

    elif dt in DATETIME_TYPES:
        # datetime2 CSV has precision=26, scale=6;
        # the JSON stores datetime_precision = scale (fractional seconds).
        meta["datetime_precision"] = int(row["scale"])

    # bit, uniqueidentifier, etc. → no extra keys
    return meta


def _parse_csv(csv_path: Path) -> dict:
    """Parse the CSV into per-table structures."""
    tables: dict = {}

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            schema_name = row["schema_name"]
            table_name = row["table_name"]
            fq = f"{schema_name}.{table_name}"

            if fq not in tables:
                tables[fq] = {
                    "schema": schema_name,
                    "name": table_name,
                    "type": "BASE TABLE",
                    "columns": {},
                    "_pk_parts": {},   # ordinal → column_name
                }

            tbl = tables[fq]

            # Column metadata
            col_name = row["column_name"]
            tbl["columns"][col_name] = _build_column_meta(row)

            # Primary key accumulation
            pk_name = row.get("pk_name", "NULL")
            if pk_name and pk_name != "NULL":
                ordinal = int(row["pk_ordinal"])
                tbl["_pk_parts"][ordinal] = col_name

    return tables


def _finalize_tables(raw_tables: dict) -> dict:
    """Convert raw parsed tables to the final JSON table format."""
    out = {}
    for fq in sorted(raw_tables):
        tbl = raw_tables[fq]
        pk_parts = tbl.pop("_pk_parts")
        pk = [pk_parts[o] for o in sorted(pk_parts)] if pk_parts else []

        has_pk = len(pk) > 0
        tbl["primary_key"] = pk
        tbl["unique_constraints"] = []
        tbl["constraints_present"] = {
            "primary_key": has_pk,
            "unique": False,
            "foreign_keys": False,
        }
        tbl["notes"] = []
        out[fq] = tbl

    return out


def _build_alias_map(tables: dict) -> dict:
    """Build table_alias_resolution: unqualified-lowercase → fq name.

    Only includes names that are unambiguous (appear in exactly one schema).
    """
    name_to_fq: dict[str, list[str]] = defaultdict(list)
    for fq, tbl in tables.items():
        name_to_fq[tbl["name"].lower()].append(fq)

    aliases = {}
    for name_lower, fq_list in sorted(name_to_fq.items()):
        if len(fq_list) == 1:
            aliases[name_lower] = fq_list[0]
    return aliases


def _merge_relationships(existing_json: dict | None, new_tables: dict) -> dict:
    """Carry forward relationships from existing JSON, dropping any that
    reference tables no longer present."""
    if not existing_json:
        return {"declared_foreign_keys": [], "inferred_foreign_keys": []}

    rels = existing_json.get("relationships", {})
    result = {}
    for kind in ("declared_foreign_keys", "inferred_foreign_keys"):
        kept = []
        for fk in rels.get(kind, []):
            ft = fk.get("from_table", "")
            tt = fk.get("to_table", "")
            if ft in new_tables and tt in new_tables:
                kept.append(fk)
            else:
                print(f"  [WARN] Dropping {kind} edge: {ft} → {tt} "
                      f"(table no longer exists)")
        result[kind] = kept
    return result


def convert(csv_path: Path, existing_path: Path | None,
            output_path: Path) -> None:
    """Main conversion pipeline."""
    # 1. Load existing JSON (optional, for merging relationships)
    existing_json = None
    if existing_path and existing_path.exists():
        existing_json = json.loads(existing_path.read_text(encoding="utf-8"))
        print(f"Loaded existing schema: {existing_path}")

    # 2. Parse CSV
    raw_tables = _parse_csv(csv_path)
    tables = _finalize_tables(raw_tables)
    print(f"Parsed {len(tables)} tables from {csv_path}")

    # 3. Count columns
    total_cols = sum(len(t["columns"]) for t in tables.values())
    tables_with_pk = [fq for fq, t in tables.items() if t["primary_key"]]
    tables_missing_pk = [fq for fq, t in tables.items() if not t["primary_key"]]

    # 4. Build alias map
    aliases = _build_alias_map(tables)
    print(f"Generated {len(aliases)} unique table aliases")

    # 5. Merge relationships
    relationships = _merge_relationships(existing_json, tables)
    n_fk = (len(relationships["declared_foreign_keys"])
            + len(relationships["inferred_foreign_keys"]))
    print(f"Carried forward {n_fk} relationship edges")

    # 6. Assemble final JSON
    schema_json = {
        "version": "physical_schema_v1",
        "generated_at_utc": datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"),
        "source": {
            "csv": csv_path.name,
            "info_schema_notes": [
                "Generated from warehouse CSV export via "
                "schema_csv_to_json.py.",
            ],
        },
        "summary": {
            "tables_total": len(tables),
            "views_total": 0,
            "columns_total": total_cols,
            "tables_with_primary_key": len(tables_with_pk),
            "tables_missing_primary_key": tables_missing_pk,
        },
        "tables": tables,
        "relationships": relationships,
        "table_alias_resolution": {
            "strategy": "unqualified_to_unique_fqtn",
            "aliases": aliases,
        },
    }

    # 7. Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(schema_json, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {output_path}  "
          f"({len(tables)} tables, {total_cols} columns)")


def main():
    parser = argparse.ArgumentParser(
        description="Convert warehouse schema CSV to physical_schema.json")
    parser.add_argument("--csv", required=True,
                        help="Path to the CSV export")
    parser.add_argument("--existing", default=None,
                        help="Path to existing physical_schema.json "
                             "(to merge relationships)")
    parser.add_argument("--output", required=True,
                        help="Output path for the new JSON")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    existing_path = Path(args.existing) if args.existing else None
    output_path = Path(args.output)

    if not csv_path.exists():
        parser.error(f"CSV not found: {csv_path}")

    convert(csv_path, existing_path, output_path)


if __name__ == "__main__":
    main()
