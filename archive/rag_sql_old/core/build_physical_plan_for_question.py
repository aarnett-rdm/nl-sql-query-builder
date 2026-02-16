from dataclasses import dataclass, field
from collections import defaultdict, deque
from typing import List, Dict, Any, Optional
from .schema_index import SchemaIndex, normalize_term, ColumnIndexEntry
import re
import datetime
import calendar

PLATFORM_SYNONYMS = {
    "google ads": "google_ads",
    "adwords": "google_ads",
    "google adwords": "google_ads",
    "microsoft ads": "microsoft_ads",
    "bing ads": "microsoft_ads",
    "bing": "microsoft_ads",
    "exchange": "exchange",
    "funnel": "funnel",
    "inventory": "inventory",
}

RELATIVE_TIME_WORDS = {
    "today", "yesterday", "tomorrow",
    "last", "this", "next",
    "week", "weeks", "month", "months",
    "year", "years", "quarter", "quarters",
    "day", "days"
}

TIME_PHRASE_PATTERNS = [
    r"\b(january|february|march|april|may|june|july|august|september|october|november|december)\b",
    r"\b20\d{2}\b",                         # 2024, 2025, etc.
    r"\bq[1-4]\b",                          # Q1, Q2, Q3, Q4
    r"\bthis (year|month|quarter)\b",
    r"\blast (year|month|quarter)\b",
    r"\bnext (year|month|quarter)\b",
]

@dataclass
class ParsedCandidates:
    """
    Result of lightweight NL parsing.
    These are *candidates* only – Stage B.3 will ground them to concrete columns.
    """
    metric_terms: List[str] = field(default_factory=list)
    dimension_terms: List[str] = field(default_factory=list)
    entity_terms: List[str] = field(default_factory=list)
    platform_terms: List[str] = field(default_factory=list)
    time_phrases: List[str] = field(default_factory=list)
    filter_phrases: List[str] = field(default_factory=list)
    raw_ngrams: List[str] = field(default_factory=list)  # for debugging only

@dataclass
class GroundedMetric:
    raw_term: str
    norm_term: str
    candidates: List["ColumnIndexEntry"]
    chosen: Optional["ColumnIndexEntry"]
    metric_class: Optional[str]
    confidence: float


@dataclass
class GroundedDimension:
    raw_term: str
    norm_term: str
    candidates: List["ColumnIndexEntry"]
    chosen: Optional["ColumnIndexEntry"]
    confidence: float

@dataclass
class RelationshipEdge:
    from_table: str
    to_table: str
    from_columns: List[str]
    to_columns: List[str]
    join_type: str
    cardinality: str


@dataclass
class RelationshipIndex:
    # adjacency[from_table] -> list[RelationshipEdge]
    adjacency: Dict[str, List[RelationshipEdge]]

def tokenize(text: str) -> List[str]:
    """
    Very simple tokenizer: alphanumeric+apostrophe tokens in lowercase.
    """
    return re.findall(r"[A-Za-z0-9']+", text.lower())

def extract_time_phrases(text: str) -> List[str]:
    """
    Very lightweight time phrase extractor.
    We keep the raw phrases; actual date resolution happens in a later phase.
    """
    text_l = text.lower()
    phrases: set[str] = set()

    # Relative patterns like "last week", "last 30 days", "next month"
    for m in re.finditer(r"\b(last|this|next)\s+[0-9]*\s*(day|days|week|weeks|month|months|year|years|quarter|quarters)\b", text_l):
        phrases.add(m.group(0).strip())

    # Month + year patterns like "june 2025"
    for m in re.finditer(r"\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}\b", text_l):
        phrases.add(m.group(0).strip())

    # Simpler single-word time hints (e.g., "today", "yesterday")
    for word in RELATIVE_TIME_WORDS:
        if re.search(rf"\b{re.escape(word)}\b", text_l):
            # avoid adding generic words like "week" alone if a more specific phrase already exists
            phrases.add(word)

    return sorted(phrases)

def extract_filter_phrases(text: str) -> List[str]:
    """
    Grab rough filter-like fragments around words like 'for', 'in', 'where', 'from', 'only'.
    This is intentionally simple; later stages will do proper schema grounding.
    """
    text_l = text.lower()
    patterns = [
        r"\bfor\b[^,;]*",
        r"\bin\b[^,;]*",
        r"\bwhere\b[^,;]*",
        r"\bfrom\b[^,;]*",
        r"\bon\b[^,;]*",
        r"\bonly\b[^,;]*",
    ]
    fragments: set[str] = set()
    for pat in patterns:
        for m in re.finditer(pat, text_l):
            frag = m.group(0).strip()
            if len(frag) > 3:
                fragments.add(frag)
    return sorted(fragments)


def extract_entity_terms(original_text: str) -> List[str]:
    """
    Extract candidate entity terms from capitalization patterns:
    - Proper names like 'Taylor Swift'
    - ALL CAPS like 'MLB'
    """
    entities: set[str] = set()

    # Proper names: sequences of Capitalized words
    for m in re.finditer(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b", original_text):
        entities.add(m.group(1).strip())

    # ALL CAPS sequences: MLB, NFL, NBA, etc.
    for m in re.finditer(r"\b([A-Z]{2,})\b", original_text):
        entities.add(m.group(1).strip())

    return sorted(entities)

def build_physical_plan_for_question(
    question: str,
    schema: dict,
    schema_index: SchemaIndex,
    default_tz: str = "America/Los_Angeles",
) -> Dict[str, Any]:
    """
    High-level Phase B.6 entry point.

    Uses:
      - B.5 builder for refined interpretation + logical_query + debug grounding
      - Relationship graph for join path planning
    """
    # 1. Build refined top-level spec to reuse grounding + platforms + grain
    top = build_interpretation_and_logical_query_refined(question, schema_index, default_tz=default_tz)
    parsed = top["debug"]["parsed"]
    grounded_metrics: List[GroundedMetric] = top["debug"]["grounded_metrics"]
    grounded_dimensions: List[GroundedDimension] = top["debug"]["grounded_dimensions"]
    resolved_platforms = top["logical_query"]["platform"]["resolved"]
    entity_grain = top["logical_query"]["grain"]["entity_grain"]
    time_window = top["logical_query"]["time_window"]

    # 2. Choose primary fact table
    fact_choice = select_primary_fact_table(
        grounded_metrics, schema_index, resolved_platforms, entity_grain
    )
    primary_fact_table = fact_choice["table"]

    # 3. Adjust dimensions to prefer columns from that fact table
    adjust_dimensions_for_fact_table(grounded_dimensions, primary_fact_table)

    # 4. Build relationship index
    rel_index = build_relationship_index(schema)

    # 5. Determine dimension tables + join paths
    join_edges: List[RelationshipEdge] = []
    dimension_tables: Dict[str, Dict[str, Any]] = {}

    # dimension grain roles are approximate here; we refine later if needed
    for gd in grounded_dimensions:
        if not gd.chosen:
            continue
        dim_table = gd.chosen.table
        if dim_table == primary_fact_table:
            # no join needed
            dimension_tables.setdefault(dim_table, {
                "table": dim_table,
                "role": "fact_dimension",
                "reason": "Dimension lives directly on fact table.",
            })
            continue

        path = find_shortest_join_path(rel_index, primary_fact_table, dim_table)
        if not path:
            # unreachable dimension; we keep a note but don't crash
            dimension_tables.setdefault(dim_table, {
                "table": dim_table,
                "role": "unreachable",
                "reason": f"No join path found from {primary_fact_table} to {dim_table}.",
            })
            continue

        # Register tables and edges on the path
        for edge in path:
            join_edges.append(edge)
            # mark intermediate/target tables as dimension/mapping/calendar/lookup generically
            if edge.to_table != primary_fact_table:
                dimension_tables.setdefault(edge.to_table, {
                    "table": edge.to_table,
                    "role": "lookup",
                    "reason": "On join path from fact to dimension.",
                })

    # 6. Build physical_plan-like dict
    fact_tables_block = []
    if primary_fact_table:
        tmeta = schema_index.tables_by_name.get(primary_fact_table, {})
        fact_tables_block.append({
            "table": primary_fact_table,
            "schema_ref": f"semantic_schema.json#{primary_fact_table}",
            "table_type": tmeta.get("table_type", "fact"),
            "platform": schema_index.platform_by_table.get(primary_fact_table),
            "grain": tmeta.get("grain", {}),
            "reason": f"Selected as primary fact table based on metrics {fact_choice['metrics']} and score {fact_choice['score']:.2f}.",
        })

    dimension_tables_block = list(dimension_tables.values())

    join_path_block = {
        "primary_graph": [
            {
                "from_table": e.from_table,
                "from_keys": e.from_columns,
                "to_table": e.to_table,
                "to_keys": e.to_columns,
                "join_type": e.join_type,
                "cardinality": e.cardinality,
                "reason": "Auto-selected via relationship graph.",
            }
            for e in join_edges
        ],
        "shortest_path_preference": "via_core_entities",
        "platform_specific_overrides": [],
    }

    physical_plan = {
        "fact_tables": fact_tables_block,
        "dimension_tables": dimension_tables_block,
        "join_path": join_path_block,
        "group_by": [],         # filled in later (B.7)
        "select_list": [],      # filled in later (B.7)
        "where_clauses": [],    # filled in later (B.7)
        "order_by": [],         # filled in later (B.7)
    }

    return {
        "interpretation": top["interpretation"],
        "logical_query": top["logical_query"],
        "physical_plan": physical_plan,
        "debug": {
            "fact_choice": fact_choice,
            "grounded_metrics": grounded_metrics,
            "grounded_dimensions": grounded_dimensions,
            "resolved_platforms": resolved_platforms,
            "time_window": time_window,
        },
    }

def build_interpretation_and_logical_query_refined(
    question: str,
    schema_index: SchemaIndex,
    default_tz: str = "America/Los_Angeles",
) -> Dict[str, Any]:
    """
    Phase B.5 version of the top-level builder:
      - B.2: extract_candidates
      - B.3: ground_metrics/dimensions
      - B.5: refine metric choices + platforms
      - B.4: build interpretation + logical_query (re-using helpers)
    """
    parsed = extract_candidates(question, schema_index)
    grounded_metrics = ground_metrics(question, parsed, schema_index)
    grounded_dimensions = ground_dimensions(question, parsed, schema_index)

    # Initial guesses
    entity_grain = infer_entity_grain(question, grounded_dimensions)
    initial_platforms = resolve_platforms(parsed, grounded_metrics, schema_index)

    # Refine metrics based on those guesses
    refine_grounded_metrics(grounded_metrics, schema_index, initial_platforms, entity_grain)

    # Recompute platforms from refined metrics + parsed platforms
    resolved_platforms = recompute_platforms_from_grounded(parsed, grounded_metrics, schema_index)

    # Time window & question_time_scope from B.4 helper
    time_window, question_time_scope = parse_time_window_from_phrases(
        question, parsed, default_tz=default_tz
    )

    # --- interpretation ---
    intent_type = infer_intent_type(question)
    focus_entities = []
    if entity_grain == "campaign":
        focus_entities.append({"entity_type": "campaign", "resolution_status": "explicit"})

    interpretation = {
        "intent_type": intent_type,
        "primary_subject": question,
        "focus_entities": focus_entities,
        "question_time_scope": question_time_scope,
        "clarity_score": 0.85,  # slightly higher than the rough version
        "notes": [
            f"Raw filter-like phrases: {parsed.filter_phrases}",
            f"Raw entity candidates: {parsed.entity_terms}",
            f"Initial platform guess: {initial_platforms}",
        ],
    }

    # --- logical_query.metrics ---
    metrics_spec = []
    for gm in grounded_metrics:
        if gm.chosen:
            metrics_spec.append({
                "semantic_name": gm.norm_term,
                "alias": gm.raw_term.strip(),
                "metric_class": gm.metric_class,
                "aggregation": "sum" if (gm.metric_class and gm.metric_class not in ("ratio", "performance")) else "avg",
                "resolved_columns": [
                    {"table": gm.chosen.table, "column": gm.chosen.column}
                ],
                "derived_expression": None,
                "platform_constraints": [],
                "required": True,
                "confidence": gm.confidence,
            })
        else:
            metrics_spec.append({
                "semantic_name": gm.norm_term,
                "alias": gm.raw_term.strip(),
                "metric_class": None,
                "aggregation": "derived",
                "resolved_columns": [],
                "derived_expression": None,
                "platform_constraints": [],
                "required": True,
                "confidence": 0.0,
            })

    # --- logical_query.dimensions (same as B.4 for now) ---
    dimensions_spec = []
    for gd in grounded_dimensions:
        if gd.chosen:
            dimensions_spec.append({
                "semantic_name": gd.norm_term,
                "alias": gd.raw_term.strip(),
                "role": "group_by",
                "resolved_columns": [
                    {"table": gd.chosen.table, "column": gd.chosen.column}
                ],
                "grain_alignment": None,
                "time_grain": "none",
                "confidence": gd.confidence,
            })

    if not dimensions_spec and re.search(r"\bcampaigns?\b", question, flags=re.IGNORECASE):
        dimensions_spec.append({
            "semantic_name": "campaign",
            "alias": "campaign",
            "role": "group_by",
            "resolved_columns": [],
            "grain_alignment": None,
            "time_grain": "none",
            "confidence": 0.5,
        })

    # --- platform & grain blocks ---
    platform_block = {
        "requested": parsed.platform_terms,
        "resolved": resolved_platforms,
        "enum_valid": True,
    }

    logical_grain = {
        "entity_grain": entity_grain,
        "time_grain": "none" if time_window is None else "none",
        "explanation": f"Inferred entity grain={entity_grain} from question text.",
    }

    logical_query = {
        "platform": platform_block,
        "grain": logical_grain,
        "metrics": metrics_spec,
        "dimensions": dimensions_spec,
        "time_window": time_window,
        "filters": [],
        "sorting": [],
        "limit": None,
    }

    return {
        "interpretation": interpretation,
        "logical_query": logical_query,
        "debug": {
            "parsed": parsed,
            "grounded_metrics": grounded_metrics,
            "grounded_dimensions": grounded_dimensions,
            "initial_platforms": initial_platforms,
        }
    }

def extract_candidates(question: str, schema_index: SchemaIndex) -> ParsedCandidates:
    """
    Phase B.2 main entry point.

    - Tokenize the question
    - Build 1–4 word n-grams
    - Match them against metric/dimension dictionaries
    - Detect platform terms, time phrases, and rough filter fragments
    """
    tokens = tokenize(question)
    ngrams: List[str] = []

    # Build 1- to 4-word n-grams
    max_n = 4
    for n in range(1, max_n + 1):
        for i in range(len(tokens) - n + 1):
            ngram = " ".join(tokens[i : i + n])
            ngrams.append(ngram)

    metric_terms: set[str] = set()
    dimension_terms: set[str] = set()
    platform_terms: set[str] = set()

    # Check each n-gram against our schema dictionaries and platform synonyms
    for ng in ngrams:
        norm = normalize_term(ng)
        if not norm:
            continue

        # Metric/dimension hits
        if norm in schema_index.metric_terms:
            metric_terms.add(ng)
        if norm in schema_index.dimension_terms:
            dimension_terms.add(ng)

        # Platform hits
        if norm in PLATFORM_SYNONYMS:
            platform_terms.add(PLATFORM_SYNONYMS[norm])

    time_phrases = extract_time_phrases(question)
    filter_phrases = extract_filter_phrases(question)
    entity_terms = extract_entity_terms(question)

    return ParsedCandidates(
        metric_terms=sorted(metric_terms),
        dimension_terms=sorted(dimension_terms),
        entity_terms=entity_terms,
        platform_terms=sorted(platform_terms),
        time_phrases=time_phrases,
        filter_phrases=filter_phrases,
        raw_ngrams=ngrams,  # keep for debugging; can be dropped later
    )

def ground_metrics(question: str, parsed: "ParsedCandidates", schema_index: "SchemaIndex") -> List[GroundedMetric]:
    """
    Combine:
    - explicit metric_terms from B.2
    - metric phrase candidates from the early part of the question

    Then map them to schema metric columns using fuzzy lookup.
    """
    # Start with metric terms B.2 already flagged
    candidate_phrases: set[str] = set(parsed.metric_terms)

    # Add heuristic metric phrase extraction
    for ph in extract_metric_phrase_candidates(question):
        candidate_phrases.add(ph)

    # 🔧 Drop time expressions before grounding metrics
    candidate_phrases = {
        ph for ph in candidate_phrases
        if not is_time_phrase(ph)
    }

    grounded: List[GroundedMetric] = []
    seen_norms: set[str] = set()

    for phrase in sorted(candidate_phrases):
        norm = normalize_term(phrase)
        if not norm or norm in seen_norms:
            continue
        seen_norms.add(norm)

        candidates = lookup_metric_term_fuzzy(phrase, schema_index)
        chosen = candidates[0] if candidates else None
        metric_class = chosen.metric_class if chosen else None
        confidence = 1.0 if chosen else 0.0

        grounded.append(
            GroundedMetric(
                raw_term=phrase,
                norm_term=norm,
                candidates=candidates,
                chosen=chosen,
                metric_class=metric_class,
                confidence=confidence,
            )
        )

    return grounded


def ground_dimensions(question: str, parsed: "ParsedCandidates", schema_index: "SchemaIndex") -> List[GroundedDimension]:
    """
    Ground dimension terms mentioned in the question.
    For now we:
    - use parsed.dimension_terms directly (from B.2)
    - add a simple heuristic: if the word 'campaign' appears, treat it as a dimension candidate
    """
    candidate_phrases: set[str] = set(parsed.dimension_terms)

    # Heuristic: campaigns
    if re.search(r"\bcampaigns?\b", question, flags=re.IGNORECASE):
        candidate_phrases.add("campaign")

    # Sometimes dimension-like phrases appear early (e.g., "campaign name")
    for ph in extract_metric_phrase_candidates(question):
        candidate_phrases.add(ph)

    # 🔧 Drop time expressions before grounding dimensions
    candidate_phrases = {
        ph for ph in candidate_phrases
        if not is_time_phrase(ph)
    }

    grounded: List[GroundedDimension] = []
    seen_norms: set[str] = set()

    for phrase in sorted(candidate_phrases):
        norm = normalize_term(phrase)
        if not norm or norm in seen_norms:
            continue
        seen_norms.add(norm)

        candidates = lookup_dimension_term_fuzzy(phrase, schema_index)
        chosen = candidates[0] if candidates else None
        confidence = 1.0 if chosen else 0.0

        grounded.append(
            GroundedDimension(
                raw_term=phrase,
                norm_term=norm,
                candidates=candidates,
                chosen=chosen,
                confidence=confidence,
            )
        )

    return grounded

def infer_entity_grain(question: str, grounded_dimensions: list[GroundedDimension]) -> str:
    """
    For now:
      - if 'campaign' appears -> campaign
      - else none
    """
    ql = question.lower()

    # If any grounded dimension already explicitly refers to campaign, prefer that
    for gd in grounded_dimensions:
        if "campaign" in gd.norm_term:
            return "campaign"

    if "campaign" in ql or "campaigns" in ql:
        return "campaign"

    return "none"
    
def resolve_platforms(parsed: "ParsedCandidates", grounded_metrics: List[GroundedMetric], schema_index: "SchemaIndex") -> List[str]:
    """
    Decide which platforms are relevant.
    Priority:
    1) explicit platform_terms from B.2
    2) infer from platforms of tables backing the chosen metrics
    """
    platforms: set[str] = set()

    # 1) Explicit from text
    for p in parsed.platform_terms:
        platforms.add(p)

    # 2) Infer from metric tables if nothing explicit
    if not platforms:
        for gm in grounded_metrics:
            if gm.chosen:
                table = gm.chosen.table
                plat = schema_index.platform_by_table.get(table)
                if plat:
                    platforms.add(plat)

    return sorted(platforms)

def extract_metric_phrase_candidates(question: str) -> List[str]:
    """
    Heuristic: metrics are often listed at the start of the question, e.g.
    'Give me clicks, cost, commission, conversion rate...'
    We:
    - take text before the first ' for ' (if present)
    - strip common verbs like 'give me', 'show me'
    - split on ',' and 'and'
    """
    text = question.strip()
    lower = text.lower()
    # Take substring before " for " if present
    idx = lower.find(" for ")
    head = text if idx == -1 else text[:idx]

    # Remove common leading verbs
    head = re.sub(r"^(give me|show me|get|list|return)\s+", "", head, flags=re.IGNORECASE).strip()

    # Split on commas and ' and '
    parts: List[str] = []
    for chunk in head.split(","):
        chunk = chunk.strip()
        # further split "X and Y"
        subparts = re.split(r"\band\b", chunk, flags=re.IGNORECASE)
        for sp in subparts:
            sp = sp.strip()
            if sp:
                parts.append(sp)

    return parts

def is_time_phrase(text: str) -> bool:
    """
    Return True if the phrase is really a time expression
    (e.g. 'june 2025', 'next quarter', 'q3 2024', etc.).
    """
    if not text:
        return False
    t = text.lower()
    return any(re.search(p, t) for p in TIME_PHRASE_PATTERNS)

def lookup_metric_term_fuzzy(phrase: str, schema_index: "SchemaIndex") -> List["ColumnIndexEntry"]:
    """
    Fuzzy lookup for metrics:
    - exact normalized match
    - 'squashed' match (removing spaces) to handle camelCase like TargetRoasValue vs "target roas value"
    """
    norm = normalize_term(phrase)
    if not norm:
        return []

    # Direct hit
    direct = schema_index.metric_terms.get(norm)
    if direct:
        return direct

    # Squash spaces (target roas value -> targetroasvalue) and compare
    squashed = norm.replace(" ", "")
    for key, entries in schema_index.metric_terms.items():
        if key.replace(" ", "") == squashed:
            return entries

    return []


def lookup_dimension_term_fuzzy(phrase: str, schema_index: "SchemaIndex") -> List["ColumnIndexEntry"]:
    """
    Fuzzy lookup for dimensions:
    - exact normalized match
    - singular/plural variants (campaigns -> campaign)
    - squashed match (campaign name -> campaignname)
    - loose substring match as a last resort
    """
    norm = normalize_term(phrase)
    if not norm:
        return []

    variants = {norm}
    # crude singularization for plural nouns
    if norm.endswith("s") and len(norm) > 3:
        variants.add(norm[:-1])

    # 1) direct matches
    for v in list(variants):
        if v in schema_index.dimension_terms:
            return schema_index.dimension_terms[v]

    # 2) squashed matches
    squashed_variants = {v.replace(" ", "") for v in variants}
    for key, entries in schema_index.dimension_terms.items():
        ks = key.replace(" ", "")
        if ks in squashed_variants:
            return entries

    # 3) loose substring match (very forgiving, but we only use it if nothing else hits)
    hits: List["ColumnIndexEntry"] = []
    for key, entries in schema_index.dimension_terms.items():
        if any(v in key for v in variants) or any(key in v for v in variants):
            hits.extend(entries)

    return hits

def refine_grounded_metrics(
    grounded_metrics: List[GroundedMetric],
    schema_index: SchemaIndex,
    preferred_platforms: List[str],
    entity_grain: str,
) -> None:
    """
    In-place refinement of GroundedMetric.chosen based on scoring across all candidates.
    """
    for gm in grounded_metrics:
        if not gm.candidates:
            continue
        best_entry = gm.candidates[0]
        best_score = score_metric_candidate(
            gm.norm_term, best_entry, preferred_platforms, entity_grain, schema_index
        )

        for entry in gm.candidates[1:]:
            s = score_metric_candidate(
                gm.norm_term, entry, preferred_platforms, entity_grain, schema_index
            )
            if s > best_score:
                best_score = s
                best_entry = entry

        gm.chosen = best_entry
        gm.metric_class = best_entry.metric_class
        # Soft confidence: clamp to [0.5, 1.0] with simple transform
        gm.confidence = max(0.5, min(1.0, 0.7 + best_score / 20.0))

def recompute_platforms_from_grounded(
    parsed: ParsedCandidates,
    grounded_metrics: List[GroundedMetric],
    schema_index: SchemaIndex,
) -> List[str]:
    """
    After refinement, derive a better resolved platform list.

    - Start from platforms implied by chosen metric tables
    - If user explicitly mentioned platforms in text, intersect/union sensibly
    """
    implied_platforms: set[str] = set()
    for gm in grounded_metrics:
        if gm.chosen:
            plat = schema_index.platform_by_table.get(gm.chosen.table)
            if plat:
                implied_platforms.add(plat)

    # If we got something from metrics, prefer that
    if implied_platforms:
        # If user also mentioned platforms, intersect if possible, otherwise union
        text_platforms = set(parsed.platform_terms)
        if text_platforms:
            intersection = implied_platforms & text_platforms
            if intersection:
                return sorted(intersection)
            return sorted(implied_platforms | text_platforms)
        return sorted(implied_platforms)

    # If metrics don't give a clue, fall back to parsed text platforms
    return sorted(set(parsed.platform_terms))

def score_metric_candidate(
    metric_norm: str,
    entry: ColumnIndexEntry,
    preferred_platforms: List[str],
    entity_grain: str,
    schema_index: SchemaIndex,
) -> float:
    """
    Heuristic scoring for one candidate ColumnIndexEntry for a metric term.

    Higher score = better fit.
    This is intentionally simple and rule-based; we can refine later.
    """
    score = 0.0

    table = entry.table
    column = entry.column
    platform = schema_index.platform_by_table.get(table)
    tname = table.lower()
    cname = column.lower()
    mname = metric_norm.lower()

    # --- Platform preference ---
    if preferred_platforms:
        if platform in preferred_platforms:
            score += 10.0
        elif platform is None:
            score -= 1.0
        else:
            score -= 5.0

    # If no explicit preferred platform, still give a little credit for having a platform at all
    if not preferred_platforms and platform is not None:
        score += 1.0

    # --- Table name heuristics ---
    # Prefer Google Ads tables for google_ads queries
    if "googleads" in tname:
        score += 4.0
    if "microsoftads" in tname:
        # Slight penalty in a google_ads-focused question
        if "google_ads" in preferred_platforms:
            score -= 2.0

    # Avoid order-level metrics by default for ad-performance questions
    if "order" in tname:
        score -= 4.0

    # Avoid global "AllTime" tables for date-bounded questions
    if "alltime" in tname:
        score -= 3.0

    # Avoid auction insight when user just says "clicks" / "cost" etc.
    if "auctioninsight" in tname:
        score -= 2.0

    # Avoid pure exchange tables unless exchange explicitly requested
    if "exchange" in tname and "exchange" not in preferred_platforms:
        score -= 3.0

    # --- Grain heuristics ---
    if entity_grain == "campaign":
        if "campaign" in tname or "campaignid" in cname:
            score += 3.0
        if "adgroup" in tname:
            score -= 1.0

    # --- Metric-name-specific nudges ---
    if "click" in mname and "click" in cname:
        score += 2.0
    if mname == "clicks" and cname == "clicks":
        score += 2.0

    if "cost" in mname and "cost" in cname:
        score += 2.0

    if "revenue" in mname and "revenue" in cname:
        score += 2.0
    if "exchange" in mname and "exchange" in tname:
        score += 2.0

    # Fallback bias toward metrics that actually have a metric_class
    if entry.metric_class:
        score += 1.0

    return score

def parse_time_window_from_phrases(
    question: str,
    parsed: ParsedCandidates,
    default_tz: str = "America/Los_Angeles",
) -> tuple[Dict[str, Any] | None, str]:
    """
    Turn simple time phrases into a TimeWindow-like dict and a question_time_scope.
    For now we only handle:
      - 'june 2025', 'march 2024', etc.  -> absolute month range
    Everything else returns (None, 'all_time' or 'recent_period').
    """
    if not parsed.time_phrases:
        return None, "all_time"

    # For now pick the first phrase
    phrase = parsed.time_phrases[0].lower()

    # Month + year pattern: "june 2025"
    m = re.match(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})",
        phrase,
    )
    if m:
        month_name = m.group(1)
        year = int(m.group(2))

        # Use fixed month mapping (sets are unordered)
        MONTH_MAP = {
            "january": 1,
            "february": 2,
            "march": 3,
            "april": 4,
            "may": 5,
            "june": 6,
            "july": 7,
            "august": 8,
            "september": 9,
            "october": 10,
            "november": 11,
            "december": 12,
        }
        month_num = MONTH_MAP[month_name]

        start_date = datetime.date(year, month_num, 1)
        last_day = calendar.monthrange(year, month_num)[1]
        end_date = datetime.date(year, month_num, last_day)

        tw = {
            "semantic_role": "activity",
            "field": {
                "semantic_name": "activity_date",
                "resolved_column": None,  # filled in later when we know the fact table
            },
            "absolute": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
            "relative": None,
            "time_zone": default_tz,
        }
        return tw, "fixed_range"

    # Fallback: we saw *some* relative time words, but don't parse dates yet
    return None, "recent_period"

def infer_intent_type(question: str) -> str:
    """
    Super simple intent classifier.
    We'll refine later, but for now:
      - questions starting with 'how did', 'how has' -> time_series/metric_breakdown
      - everything else -> metric_breakdown
    """
    ql = question.lower().strip()
    if ql.startswith("how did") or ql.startswith("how has"):
        return "metric_breakdown"
    return "metric_breakdown"

def select_primary_fact_table(
    grounded_metrics: List[GroundedMetric],
    schema_index: SchemaIndex,
    preferred_platforms: List[str],
    entity_grain: str,
) -> Dict[str, Any]:
    """
    Choose a primary fact table based on refined grounded_metrics.
    Returns a dict describing the selection.
    """
    # Group metrics by chosen table
    table_to_metrics: Dict[str, List[GroundedMetric]] = defaultdict(list)
    for gm in grounded_metrics:
        if gm.chosen:
            table_to_metrics[gm.chosen.table].append(gm)

    if not table_to_metrics:
        return {
            "table": None,
            "reason": "No metrics had chosen columns; cannot select a primary fact table.",
            "score": 0.0,
        }

    # Score each candidate table
    best_table = None
    best_score = float("-inf")
    reasons = {}

    for table, mets in table_to_metrics.items():
        s = score_fact_table(table, mets, schema_index, preferred_platforms, entity_grain)
        reasons[table] = s
        if s > best_score:
            best_score = s
            best_table = table

    return {
        "table": best_table,
        "score": best_score,
        "table_scores": reasons,
        "metrics": [gm.raw_term for gm in table_to_metrics[best_table]],
    }

def score_fact_table(
    table: str,
    metrics_for_table: List[GroundedMetric],
    schema_index: SchemaIndex,
    preferred_platforms: List[str],
    entity_grain: str,
) -> float:
    """
    Score a potential primary fact table based on:
      - how many metrics it supports
      - table_type == 'fact'
      - platform compatibility
      - grain & name hints (e.g. campaign vs exchange / order / auction)
    """
    score = 0.0
    tmeta = schema_index.tables_by_name.get(table, {})
    table_type = tmeta.get("table_type")
    platform = schema_index.platform_by_table.get(table)

    # grain can be either a dict ({"entity_grain": ...}) or a simple string ("campaign")
    grain_raw = tmeta.get("grain")
    entity_in_grain = None
    if isinstance(grain_raw, dict):
        entity_in_grain = grain_raw.get("entity_grain")
    elif isinstance(grain_raw, str):
        entity_in_grain = grain_raw  # directly use the string as the entity_grain

    table_name_l = table.lower()

    # Metrics supported
    score += 5.0 * len(metrics_for_table)

    # Prefer fact tables
    if table_type == "fact":
        score += 4.0

    # Platform compatibility
    if preferred_platforms:
        if platform in preferred_platforms:
            score += 4.0
        elif platform is None:
            score -= 1.0
        else:
            score -= 3.0

    # Grain compatibility (very rough)
    if entity_grain and entity_in_grain and entity_in_grain == entity_grain:
        score += 3.0

    # Name-based nudges
    if "campaign" in table_name_l and entity_grain == "campaign":
        score += 3.0
    if "auctioninsight" in table_name_l:
        score -= 3.0
    if "order" in table_name_l:
        score -= 3.0
    if "alltime" in table_name_l:
        score -= 2.0

    return score

def adjust_dimensions_for_fact_table(
    grounded_dimensions: List[GroundedDimension],
    primary_fact_table: str,
) -> None:
    """
    If a dimension has multiple candidates, prefer one that lives in the primary fact table.
    This modifies GroundedDimension.chosen in place.
    """
    if not primary_fact_table:
        return

    for gd in grounded_dimensions:
        if not gd.candidates:
            continue
        # If current choice already uses the primary fact table, keep it
        if gd.chosen and gd.chosen.table == primary_fact_table:
            continue
        # Otherwise, see if any candidate is in the fact table
        for entry in gd.candidates:
            if entry.table == primary_fact_table:
                gd.chosen = entry
                gd.confidence = max(gd.confidence, 0.8)
                break

def build_relationship_index(schema: dict) -> RelationshipIndex:
    """
    Build an undirected adjacency list from schema['relationships'].

    Assumes each relationship dict looks roughly like:
      {
        "from_table": ...,
        "to_table": ...,
        "from_columns": [...],
        "to_columns": [...],
        "join_type": "inner" | "left" | ...,
        "cardinality": "many_to_one" | ...
      }

    We create edges in both directions so BFS can traverse either way.
    """
    adjacency: Dict[str, List[RelationshipEdge]] = defaultdict(list)

    for rel in schema.get("relationships", []):
        ft = rel.get("from_table")
        tt = rel.get("to_table")
        if not ft or not tt:
            continue

        from_cols = rel.get("from_columns") or rel.get("from_keys") or []
        to_cols = rel.get("to_columns") or rel.get("to_keys") or []
        join_type = rel.get("join_type") or "inner"
        cardinality = rel.get("cardinality") or rel.get("relationship_type") or "many_to_one"

        # forward
        e_fwd = RelationshipEdge(
            from_table=ft,
            to_table=tt,
            from_columns=list(from_cols),
            to_columns=list(to_cols),
            join_type=join_type,
            cardinality=cardinality,
        )
        adjacency[ft].append(e_fwd)

        # reverse (swap columns / direction)
        e_rev = RelationshipEdge(
            from_table=tt,
            to_table=ft,
            from_columns=list(to_cols),
            to_columns=list(from_cols),
            join_type=join_type,  # join_type is symmetric for our planning purposes
            cardinality=cardinality,
        )
        adjacency[tt].append(e_rev)

    return RelationshipIndex(adjacency=adjacency)

def build_group_by_and_select_list(
    logical_query: Dict[str, Any],
    fact_table: str,
) -> (List[Dict[str, Any]], List[Dict[str, Any]]):
    """
    Use logical_query.metrics + logical_query.dimensions to construct:
      - physical_plan.group_by
      - physical_plan.select_list
    """
    group_by: List[Dict[str, Any]] = []
    select_list: List[Dict[str, Any]] = []

    # 1) Dimensions -> group_by + select_list entries
    for dim in logical_query.get("dimensions", []):
        for rc in dim.get("resolved_columns", []):
            gb_entry = {
                "table": rc.get("table") or fact_table,
                "column": rc.get("column"),
                "alias": dim.get("alias") or dim.get("semantic_name"),
            }
            group_by.append(gb_entry)
            select_list.append({
                "expression_type": "dimension",
                "table": gb_entry["table"],
                "column": gb_entry["column"],
                "alias": gb_entry["alias"],
            })

    # 2) Metrics -> select_list entries
    for met in logical_query.get("metrics", []):
        agg = met.get("aggregation") or "sum"
        alias = met.get("alias") or met.get("semantic_name")
        resolved_cols = met.get("resolved_columns") or []

        if resolved_cols:
            rc = resolved_cols[0]
            select_list.append({
                "expression_type": "metric",
                "aggregation": agg.upper(),
                "table": rc.get("table") or fact_table,
                "column": rc.get("column"),
                "alias": alias,
            })
        else:
            # Unresolved / derived metric placeholder
            select_list.append({
                "expression_type": "derived",
                "aggregation": "derived",
                "table": None,
                "column": None,
                "alias": alias,
                "expression": None,   # to be filled by SQL generator later
            })

    return group_by, select_list

def build_where_clauses_from_time_window(
    time_window: Optional[Dict[str, Any]],
    fact_table: str,
    schema_index: SchemaIndex,
) -> (List[Dict[str, Any]], Optional[Dict[str, str]]):
    """
    Build logical where clauses for the time window.

    Returns:
      - list of where_clauses entries
      - the resolved date column (if any) that was used
    """
    clauses: List[Dict[str, Any]] = []
    if not time_window:
        return clauses, None

    # Resolve the date column if not already set
    field_info = time_window.get("field") or {}
    resolved_col = field_info.get("resolved_column")
    if not resolved_col:
        resolved_col = choose_date_column_for_fact_table(fact_table, schema_index)
        if resolved_col:
            field_info["resolved_column"] = resolved_col
            time_window["field"] = field_info

    if not resolved_col:
        # Can't build a concrete predicate, but keep the semantic info
        clauses.append({
            "predicate_type": "time_range_unresolved",
            "semantic_role": time_window.get("semantic_role", "activity"),
            "range": time_window.get("absolute") or time_window.get("relative"),
            "source_filter": "time_window",
        })
        return clauses, None

    if time_window.get("range_type") == "absolute" and time_window.get("absolute"):
        abs_rng = time_window["absolute"]
        clauses.append({
            "predicate_type": "time_range",
            "table": resolved_col["table"],
            "column": resolved_col["column"],
            "operator": "between",
            "start_date": abs_rng.get("start_date"),
            "end_date": abs_rng.get("end_date"),
            "source_filter": "time_window",
        })
    else:
        # For now we don't expand relative windows; just record it
        clauses.append({
            "predicate_type": "time_range_relative",
            "table": resolved_col["table"],
            "column": resolved_col["column"],
            "relative": time_window.get("relative"),
            "source_filter": "time_window",
        })

    return clauses, resolved_col

def choose_date_column_for_fact_table(
    fact_table: str,
    schema_index: SchemaIndex,
) -> Optional[Dict[str, str]]:
    """
    Heuristic: pick a reasonable date/timestamp column for the fact table.

    Priority:
      1) column with is_timestamp == True
      2) semantic_type in {'date', 'datetime', 'timestamp'}
      3) column name containing 'date'
      4) column name containing 'calendar'
    """
    cols = schema_index.columns_by_table.get(fact_table, {})
    # 1) explicit timestamp flag
    for cname, c in cols.items():
        if c.get("is_timestamp"):
            return {"table": fact_table, "column": cname}

    # 2) semantic_type
    for cname, c in cols.items():
        st = (c.get("semantic_type") or "").lower()
        if st in {"date", "datetime", "timestamp"}:
            return {"table": fact_table, "column": cname}

    # 3) name contains 'date'
    for cname, _ in cols.items():
        if "date" in cname.lower():
            return {"table": fact_table, "column": cname}

    # 4) name contains 'calendar'
    for cname, _ in cols.items():
        if "calendar" in cname.lower():
            return {"table": fact_table, "column": cname}

    return None

