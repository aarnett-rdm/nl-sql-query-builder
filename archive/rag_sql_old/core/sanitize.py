from typing import Dict, Any, List, Tuple, Optional
from copy import deepcopy
import re

_VERBS = ("show", "get", "give", "list", "return", "display", "provide", "plot", "print")

_PLATFORM_FLUFF_PATTERNS = [
    r"\s+for\s+google\s+ads.*$",
    r"\s+for\s+microsoft\s+ads.*$",
    r"\s+for\s+bing.*$",
    r"\s+for\s+campaigns?\s*$",
    r"\s+by\s+campaigns?\s*$",
]

_ALIAS_MAP = {
    "conversion rate": "conversion rate",
    "conv rate": "conversion rate",
    "cvr": "conversion rate",

    "click through rate": "ctr",
    "ctr": "ctr",

    "cost per click": "cpc",
    "cpc": "cpc",

    "revenue per click": "rpc",
    "rpc": "rpc",

    "absolute top impression share": "abs top impression share",
    "abs top impression share": "abs top impression share",
    "absolute top is": "abs top impression share",
    "abs top is": "abs top impression share",
}

def sanitize_spec_for_validation(
    spec: Dict[str, Any],
    metric_registry: dict,
) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    """
    Deterministically sanitize known planner output issues BEFORE validation.
    - Cleans metric semantic_name tokens
    - Drops obviously non-metric leaked phrases (with warning)
    - Normalizes aggregation for base metrics per metric_registry
    - Removes auto-injected CampaignId-like filters when grain is already campaign
    Returns (new_spec, messages_to_append). No silent behavior: warnings are emitted.
    """
    messages: List[Dict[str, str]] = []
    spec2 = deepcopy(spec)

    lq = spec2.get("logical_query") or {}
    metrics = lq.get("metrics") or []
    filters = lq.get("filters") or []

    # --- Metrics cleanup ---
    cleaned_any = False
    dropped_metrics: List[str] = []

    new_metrics = []
    for m in metrics:
        if not isinstance(m, dict):
            continue

        sem = m.get("semantic_name")
        cleaned, changed, dropped = _clean_metric_name(sem)

        if dropped:
            dropped_metrics.append(str(sem))
            cleaned_any = True
            continue  # drop it (it is not a real metric)
        if changed and cleaned:
            m["semantic_name"] = cleaned
            cleaned_any = True

        # Normalize aggregation for base metrics using registry defaults
        sem2 = m.get("semantic_name")
        md = _get_metric_def(metric_registry, sem2) if sem2 else None
        if md and isinstance(md, dict):
            default_agg = (md.get("default_aggregation") or "").lower().strip()
            # Only force if it's not a derived metric
            if default_agg and default_agg != "derived":
                if (m.get("aggregation") or "").lower().strip() != default_agg:
                    m["aggregation"] = default_agg
                    cleaned_any = True

        new_metrics.append(m)

    lq["metrics"] = new_metrics

    if dropped_metrics:
        messages.append({
            "type": "interpretation_warning",
            "detail": f"Dropped non-metric phrases that leaked into metrics from planner output: {dropped_metrics}"
        })

    if cleaned_any and not dropped_metrics:
        messages.append({
            "type": "interpretation_warning",
            "detail": "Sanitized one or more metric fields before validation (verb stripping, fluff removal, alias normalization, and/or base-metric aggregation normalization)."
        })

    # --- Campaign grain cleanup: remove auto CampaignId filters (hallucinated join key) ---
    if isinstance(filters, list) and _is_campaign_grain(lq):
        new_filters = []
        removed = 0
        for f in filters:
            if isinstance(f, dict) and _filter_is_campaign_id_like(f):
                    removed += 1
                    continue
            new_filters.append(f)

        if removed > 0:
            lq["filters"] = new_filters
            messages.append({
                "type": "interpretation_warning",
                "detail": f"Removed {removed} auto-injected CampaignId-like filter(s) under campaign grain to avoid referencing non-existent schema columns."
            })

    spec2["logical_query"] = lq
    return spec2, messages

def normalize_platform_and_campaign_token_filters(
    spec: Dict[str, Any],
    schema: dict,
) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    """
    Correct fix:
      - "Google Ads campaigns" => platform constraint only (no ADS token filter)
      - real tokens like "MLB campaigns" => CampaignName LIKE '%MLB%' (only when token looks campaign-like)
      - never allow LIKE on *Id columns (return validation_error message; no silent correction)
      - drop junk tokens (e.g., 'r' from 'for') so generic "by campaign" questions don't get CampaignName LIKE filters
    """
    messages: List[Dict[str, str]] = []
    spec2 = deepcopy(spec)
    lq = spec2.get("logical_query") or {}

    question = (
        spec2.get("nl_question")
        or spec2.get("question")
        or spec2.get("raw_question")
        or ""
    ).strip()
    q = question.lower()

    # -----------------------
    # 1) Platform inference
    # -----------------------
    platform_inferred: Optional[str] = None
    if "google ads" in q:
        platform_inferred = "google_ads"
    if ("microsoft ads" in q) or ("bing ads" in q):
        if platform_inferred and platform_inferred != "microsoft_ads":
            platform_inferred = None
        else:
            platform_inferred = "microsoft_ads"

    if platform_inferred:
        plat_obj = lq.get("platform") or {}
        if isinstance(plat_obj, dict):
            requested = plat_obj.get("requested") or []
            if platform_inferred not in requested:
                requested = requested + [platform_inferred]
            plat_obj["requested"] = requested
            lq["platform"] = plat_obj
        else:
            lq["platform"] = {"requested": [platform_inferred], "resolved": [platform_inferred], "enum_valid": True}

    plat_req = (lq.get("platform") or {}).get("requested") if isinstance(lq.get("platform"), dict) else []
    platform_resolved = plat_req[0] if isinstance(plat_req, list) and plat_req else None

    # -------------------------------
    # 2) Campaign token filter fixes
    # -------------------------------
    filters = lq.get("filters") or []
    new_filters: List[dict] = []
    removed = 0
    remapped = 0
    dropped = 0

    PLATFORM_TOKENS = {"ads", "google", "microsoft", "bing"}

    STOPWORDS = {
        "for","by","the","a","an","and","or","to","of","in","on","at","from",
        "show","get","give","list","return","display","provide",
        "click","clicks","cost","spend","conversions","revenue","profit",
        "campaign","campaigns","ad","ads","group","groups","account","accounts",
        "august","september","october","november","december","january","february","march","april","may","june","july",
        "jan","feb","mar","apr","jun","jul","aug","sep","sept","oct","nov","dec",
        "last","this","next","month","week","year","yesterday","today","tomorrow"
    }

    def _looks_like_year(s: str) -> bool:
        return s.isdigit() and len(s) == 4 and 1900 <= int(s) <= 2100

    def _is_explicit_campaign_name_question(q_lower: str) -> bool:
        # Only allow campaign-name filters when user asked for a specific campaign
        return (
            "campaign named" in q_lower
            or "campaign name" in q_lower
            or "campaign:" in q_lower
            or "contains" in q_lower
            or "matching" in q_lower
            or "match" in q_lower
            or "starts with" in q_lower
            or "ends with" in q_lower
            or "equals" in q_lower
            or "='" in q_lower
            or '="' in q_lower
            or "'" in q_lower
            or '"' in q_lower
        )

    explicit_campaign_name = _is_explicit_campaign_name_question(q)

    for f in filters:
        if not isinstance(f, dict):
            new_filters.append(f)
            continue

        sem = (f.get("semantic_name") or "").lower()
        rc = f.get("resolved_column") or {}
        col = (rc.get("column") or "").lower()
        op = (f.get("operator") or "").upper()

        # campaign token filter handling
        if sem == "campaign_name_contains_token":
            token = None
            for v in (f.get("values_resolved") or []) + (f.get("values_raw") or []):
                if isinstance(v, str) and v.strip():
                    token = v.strip()
                    break

            token_norm = token.replace("%", "").strip().lower() if isinstance(token, str) else ""

            # If platform is explicit/resolved, platform-derived tokens must NOT become campaign-name filters.
            if platform_resolved and token_norm in PLATFORM_TOKENS:
                removed += 1
                continue

            # Drop junk tokens unless user explicitly asked for campaign name filtering
            if (not explicit_campaign_name):
                dropped += 1
                continue

            # More junk token guards
            if not token_norm or len(token_norm) < 3 or token_norm in STOPWORDS or token_norm.isdigit() or _looks_like_year(token_norm):
                dropped += 1
                continue

            # Otherwise it's a real token => map to CampaignName for platform
            target = _pick_campaign_name_column_for_platform(schema, platform_resolved or "google_ads")
            if target:
                t_table, t_col = target
                f["resolved_column"] = {"table": t_table, "column": t_col}

                # Ensure LIKE pattern
                if token and isinstance(token, str):
                    if "%" not in token:
                        token = f"%{token}%"
                    f["values_raw"] = [token]
                    f["values_resolved"] = [token]

                # If operator isn't LIKE, normalize to LIKE for token filters
                if op in ("CONTAINS", "ILIKE", ""):
                    f["operator"] = "LIKE"

                remapped += 1
                new_filters.append(f)
                continue

            # If we couldn't pick a column, drop rather than emit a broken filter
            dropped += 1
            continue

        # Guardrail: never allow LIKE/CONTAINS on an ID column (no silent correction)
        if op in ("LIKE", "ILIKE", "CONTAINS") and col.endswith("id"):
            messages.append({
                "type": "validation_error",
                "detail": (
                    f"Planner produced an invalid text match filter on ID column "
                    f"{rc.get('table')}.{rc.get('column')}. "
                    f"Use a campaign name filter (e.g., 'campaign name contains ...') "
                    f"or provide CampaignIds for an IN filter."
                )
            })
            new_filters.append(f)
            continue

        new_filters.append(f)

    if removed:
        messages.append({
            "type": "interpretation_warning",
            "detail": (
                f"Removed {removed} campaign token filter(s) that were derived from platform terms "
                f"({sorted(PLATFORM_TOKENS)}) because platform was explicitly resolved."
            )
        })
    if dropped:
        messages.append({
            "type": "interpretation_warning",
            "detail": f"Dropped {dropped} campaign token filter(s) because the question did not specify a campaign name."
        })
    if remapped:
        messages.append({
            "type": "interpretation_warning",
            "detail": f"Remapped {remapped} campaign token filter(s) to CampaignName column for correct text matching."
        })

    lq["filters"] = new_filters
    spec2["logical_query"] = lq
    spec2["nl_question"] = question if question else spec2.get("nl_question", "")
    return spec2, messages


def _clean_metric_name(name: str) -> Tuple[Optional[str], bool, bool]:
    """
    Returns (cleaned_name_or_None, changed?, dropped?).
    dropped=True means: it's clearly not a real metric token (e.g. 'for google ads campaigns').
    """
    if not name or not isinstance(name, str):
        return name, False, False

    orig = name
    s = name.strip().lower()

    # Strip leading verbs: "show clicks" -> "clicks"
    for v in _VERBS:
        if s.startswith(v + " "):
            s = s[len(v):].strip()
            break

    # If it starts with "for ..." it's likely a leaked clause, not a metric
    if s.startswith("for "):
        # if it looks like "for google ads campaigns" etc, drop it
        if "ads" in s or "campaign" in s:
            return None, True, True
        # else keep cleaning

    # Remove trailing fluff like "... for google ads campaigns"
    for pat in _PLATFORM_FLUFF_PATTERNS:
        s2 = re.sub(pat, "", s).strip()
        s = s2

    # Normalize whitespace/punct
    s = re.sub(r"[\t\n\r]+", " ", s).strip()
    s = re.sub(r"\s{2,}", " ", s)

    # Alias normalization
    if s in _ALIAS_MAP:
        s = _ALIAS_MAP[s]

    changed = (s != orig.strip().lower())
    if not s:
        return None, True, True

    return s, changed, False

def _is_campaign_grain(logical_query: dict) -> bool:
    grain = logical_query.get("grain") or {}
    if isinstance(grain, dict):
        if (grain.get("entity_grain") or "").lower() == "campaign":
            return True

    # also check group_by dimensions if present
    group_by = logical_query.get("group_by") or logical_query.get("dimensions") or []
    if isinstance(group_by, list):
        gb = " ".join([str(x).lower() for x in group_by])
        if "campaign" in gb:
            return True

    return False

def _get_metric_def(metric_registry: dict, semantic_name: str) -> Optional[dict]:
    """
    Tries to find a metric definition in metric_registry for a given semantic_name.
    Handles common shapes:
      - {"metrics": { "clicks": {...}, ...}}
      - {"metrics": [ {"semantic_name":"clicks", ...}, ... ]}
    """
    if not metric_registry or not semantic_name:
        return None

    mr = metric_registry
    metrics = mr.get("metrics")

    # dict keyed by semantic name
    if isinstance(metrics, dict):
        return metrics.get(semantic_name)

    # list of metric defs
    if isinstance(metrics, list):
        for m in metrics:
            if isinstance(m, dict) and m.get("semantic_name") == semantic_name:
                return m

    return None

def _filter_is_campaign_id_like(f: dict) -> bool:
    """
    Detect planner-injected CampaignId filters that should not exist
    in a semantic (non-ID-exposing) schema.
    """
    table = (f.get("table") or f.get("table_name") or "").lower()
    col = (f.get("column") or f.get("column_name") or f.get("field") or "").lower()
    full = (f.get("full_name") or f.get("field_ref") or "").lower()

    # Explicitly catch your known hallucination
    if "googleadscampaignperformancemetric" in table and col == "campaignid":
        return True

    # Generic campaign id patterns
    if col in ("campaignid", "campaign_id"):
        return True

    if full.endswith(".campaignid") or full.endswith(".campaign_id"):
        return True

    return False

def _pick_campaign_name_column_for_platform(schema: dict, platform: str) -> Optional[Tuple[str, str]]:
    """
    Returns (table, column) for CampaignName based on platform.
    Adjust table names here if your semantic schema uses different logical table names.
    """
    platform = (platform or "").lower()
    if platform == "google_ads":
        table = "GoogleAdsCampaign"
    elif platform == "microsoft_ads":
        table = "MicrosoftAdsCampaign"
    else:
        return None

    cols = set(_schema_table_columns(schema, table))
    if "CampaignName" in cols:
        return (table, "CampaignName")

    # fallback candidates if schema differs
    for c in ("Campaign", "Name", "CampaignTitle"):
        if c in cols:
            return (table, c)
    return None

def _schema_table_columns(schema: dict, table_logical_name: str) -> List[str]:
    for t in schema.get("tables", []):
        # your schema uses logical_name (fallback name)
        tname = t.get("logical_name") or t.get("name")
        if tname == table_logical_name:
            return [c.get("name") for c in (t.get("columns") or []) if isinstance(c, dict)]
    return []