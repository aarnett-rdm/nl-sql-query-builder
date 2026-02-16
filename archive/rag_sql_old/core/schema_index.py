from dataclasses import dataclass, field
from collections import defaultdict
from typing import Dict, List
from .normalize_term import normalize_term
import re

@dataclass
class ColumnIndexEntry:
    """
    Lightweight reference to a column in the semantic schema.
    Used in metric/dimension dictionaries.
    """
    table: str
    column: str
    semantic_type: str | None = None
    metric_class: str | None = None
    is_key: bool = False
    is_timestamp: bool = False
    allowed_values: list[str] | None = None
    
@dataclass
class SchemaIndex:
    tables_by_name: dict = field(default_factory=dict)
    columns_by_table: dict = field(default_factory=dict)
    metric_terms: dict = field(default_factory=lambda: defaultdict(list))
    dimension_terms: dict = field(default_factory=lambda: defaultdict(list))
    enum_index: dict = field(default_factory=dict)
    platform_by_table: dict = field(default_factory=dict)
    grain_by_table: dict = field(default_factory=dict)

def normalize_term(term: str) -> str:
    """
    Normalize a free-text term for dictionary keys:
    - lowercase
    - collapse non-alphanumerics to spaces
    - strip extra spaces
    """
    if term is None:
        return ""
    term = term.lower()
    term = re.sub(r"[^a-z0-9]+", " ", term)
    return term.strip()

def build_schema_index(schema: dict) -> SchemaIndex:
    """
    Build all dictionary-style indexes from the enriched semantic schema.
    This is the main entry point for Phase B.1.
    """
    idx = SchemaIndex()

    # --- Tables and basic metadata ---
    for t in schema.get("tables", []):
        logical_name = t["logical_name"]
        idx.tables_by_name[logical_name] = t
        idx.platform_by_table[logical_name] = t.get("platform")
        idx.grain_by_table[logical_name] = t.get("grain", {})

        # Ensure we have a place for this table's columns
        idx.columns_by_table.setdefault(logical_name, {})

        for col in t.get("columns", []):
            col_name = col["name"]
            idx.columns_by_table[logical_name][col_name] = col

            entry = ColumnIndexEntry(
                table=logical_name,
                column=col_name,
                semantic_type=col.get("semantic_type"),
                metric_class=col.get("metric_class"),
                is_key=bool(col.get("is_key")),
                is_timestamp=bool(col.get("is_timestamp")),
                allowed_values=col.get("allowed_values"),
            )

            # Keep enum lookup handy
            if col.get("allowed_values"):
                idx.enum_index[(logical_name, col_name)] = col["allowed_values"]

            # --- Build term lists for indexing ---

            # Base terms: column name itself
            base_terms: list[str] = [col_name]

            # Add synonyms if present
            for syn in col.get("synonyms", []):
                base_terms.append(syn)

            # Add aliases at the table level if they help (e.g., "Google Ads Campaign" etc.)
            for table_alias in t.get("aliases", []):
                # Combine alias + column name for phrases like "campaign clicks"
                base_terms.append(f"{table_alias} {col_name}")

            # Normalize and deduplicate terms
            norm_terms = set()
            for term in base_terms:
                norm = normalize_term(term)
                if norm:
                    norm_terms.add(norm)

            # Decide whether this looks like a metric or dimension
            is_metric = bool(col.get("metric_class"))
            for norm in norm_terms:
                if is_metric:
                    idx.metric_terms[norm].append(entry)
                else:
                    idx.dimension_terms[norm].append(entry)

    return idx