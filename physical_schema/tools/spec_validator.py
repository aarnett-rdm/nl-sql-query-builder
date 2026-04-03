"""
spec_validator.py

Post-LLM spec correction layer.

Takes a raw LLM-generated spec and the schema context, then:
  1. Fuzzy-matches unknown metric names against the registry + synonyms
  2. Validates dimensions; drops unknown ones silently
  3. Injects date default (last 30 days) if no date filter was provided
  4. Builds a human-readable interpretation summary for the UI

Returns (corrected_spec, interpretation) where interpretation is a dict
the UI renders as an "I'm running X for Y" card before showing results.
"""

from __future__ import annotations

import difflib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("nl_sql_service.spec_validator")

# ---------------------------------------------------------------------------
# Known dimension labels for human-readable output
# ---------------------------------------------------------------------------

_DIM_LABELS: Dict[str, str] = {
    "PST_Date": "Date (daily)",
    "EventDate": "Event Date",
    "CampaignName": "Campaign Name",
    "AccountName": "Account Name",
    "AdGroupName": "Ad Group",
    "State": "State",
    "Network": "Platform/Network",
    "Device": "Device",
    "CurrencyCode": "Currency",
    "CampaignType": "Campaign Type",
    "CampaignStatus": "Campaign Status",
}

_DEFAULT_DATE: Dict[str, Any] = {"last_n_days": 30}


# ---------------------------------------------------------------------------
# Human-readable formatting helpers
# ---------------------------------------------------------------------------

def _fmt_date(date_filter: Dict[str, Any]) -> str:
    """Convert a date filter dict into a short human-readable string."""
    if not date_filter:
        return "last 30 days (default)"
    if date_filter.get("yesterday"):
        return "yesterday"
    if date_filter.get("mtd"):
        return "month to date"
    n = date_filter.get("last_n_days")
    if n:
        return f"last {n} days"
    df = date_filter.get("date_from")
    dt = date_filter.get("date_to")
    if df and dt:
        return f"{df} → {dt}"
    if df:
        return f"from {df}"
    return "custom date range"


def _fmt_platform(platform: Optional[str]) -> str:
    if platform == "google_ads":
        return "Google Ads only"
    if platform == "microsoft_ads":
        return "Microsoft Ads only"
    return "all platforms"


def _fmt_dims(dimensions: List[str]) -> str:
    if not dimensions:
        return "no grouping (totals only)"
    labels = [_DIM_LABELS.get(d, d) for d in dimensions]
    return "grouped by " + ", ".join(labels)


def _fmt_metrics(metrics: List[str]) -> str:
    if not metrics:
        return "(no metrics)"
    return " + ".join(metrics)


# ---------------------------------------------------------------------------
# Fuzzy metric matching
# ---------------------------------------------------------------------------

def _fuzzy_match_metric(
    name: str,
    valid_metrics: List[str],
    synonyms: Dict[str, str],
    cutoff: float = 0.75,
) -> Optional[str]:
    """
    Try to resolve an unknown metric name to a canonical metric.

    Resolution order:
      1. Exact match (case-insensitive)
      2. Synonym lookup (case-insensitive)
      3. difflib close match against all valid metric names + synonym keys

    Returns canonical metric name, or None if no match found.
    """
    lower = name.lower().strip()

    # 1. Exact match (case-insensitive)
    for m in valid_metrics:
        if m.lower() == lower:
            return m

    # 2. Synonym lookup
    canonical = synonyms.get(lower)
    if canonical and canonical in valid_metrics:
        return canonical

    # 3. Fuzzy match against valid metric names
    candidates = valid_metrics + list(synonyms.keys())
    matches = difflib.get_close_matches(lower, [c.lower() for c in candidates], n=1, cutoff=cutoff)
    if matches:
        matched_lower = matches[0]
        # Resolve back to canonical
        for m in valid_metrics:
            if m.lower() == matched_lower:
                return m
        # It matched a synonym key
        syn_canonical = synonyms.get(matched_lower)
        if syn_canonical and syn_canonical in valid_metrics:
            return syn_canonical

    return None


def _fuzzy_match_dimension(
    name: str,
    valid_dims: List[str],
    cutoff: float = 0.80,
) -> Optional[str]:
    """
    Try to resolve an unknown dimension name to a known dimension.
    Returns the canonical dimension name, or None if no close match.
    """
    lower = name.lower().strip()

    # Exact match (case-insensitive)
    for d in valid_dims:
        if d.lower() == lower:
            return d

    # Fuzzy
    matches = difflib.get_close_matches(lower, [d.lower() for d in valid_dims], n=1, cutoff=cutoff)
    if matches:
        for d in valid_dims:
            if d.lower() == matches[0]:
                return d

    return None


# ---------------------------------------------------------------------------
# Main validator
# ---------------------------------------------------------------------------

def validate_and_correct(
    spec: Dict[str, Any],
    valid_metrics: List[str],
    valid_dims: List[str],
    synonyms: Dict[str, str],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Validate and auto-correct a raw LLM spec.

    Args:
        spec:          Raw spec dict from LLM (after _ensure_spec_structure).
        valid_metrics: List of canonical metric names from metric_registry.json.
        valid_dims:    List of known dimension names from SchemaContext.
        synonyms:      Synonym map {alias: canonical} from metric_registry.json.

    Returns:
        (corrected_spec, interpretation)

        corrected_spec: spec dict with unknown metrics/dims corrected or removed,
                        and date default injected if missing.
        interpretation: {
            "summary": str,           # one-line human-readable summary
            "metrics": list[str],     # resolved metric names
            "date_label": str,        # human-readable date range
            "platform_label": str,    # human-readable platform
            "dimensions_label": str,  # human-readable grouping
            "assumed": dict[str,str], # field → reason for any auto-correction/default
        }
    """
    import copy
    corrected = copy.deepcopy(spec)
    assumed: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # 1. Metric correction
    # ------------------------------------------------------------------
    raw_metrics: List[str] = corrected.get("metrics") or []
    resolved_metrics: List[str] = []

    for m in raw_metrics:
        if m in valid_metrics:
            resolved_metrics.append(m)
            continue

        canonical = _fuzzy_match_metric(m, valid_metrics, synonyms)
        if canonical:
            if canonical != m:
                assumed[f'metric "{m}"'] = f'auto-corrected to "{canonical}"'
                logger.info("Spec validator: metric '%s' → '%s'", m, canonical)
            resolved_metrics.append(canonical)
        else:
            assumed[f'metric "{m}"'] = "unknown — dropped"
            logger.warning("Spec validator: unknown metric '%s' dropped", m)

    corrected["metrics"] = resolved_metrics

    # ------------------------------------------------------------------
    # 2. Dimension correction
    # ------------------------------------------------------------------
    raw_dims: List[str] = corrected.get("dimensions") or []
    resolved_dims: List[str] = []

    for d in raw_dims:
        if d in valid_dims:
            resolved_dims.append(d)
            continue

        canonical_dim = _fuzzy_match_dimension(d, valid_dims)
        if canonical_dim:
            if canonical_dim != d:
                assumed[f'dimension "{d}"'] = f'auto-corrected to "{canonical_dim}"'
                logger.info("Spec validator: dimension '%s' → '%s'", d, canonical_dim)
            resolved_dims.append(canonical_dim)
        else:
            assumed[f'dimension "{d}"'] = "unknown — dropped"
            logger.warning("Spec validator: unknown dimension '%s' dropped", d)

    corrected["dimensions"] = resolved_dims

    # ------------------------------------------------------------------
    # 3. Date default injection
    # ------------------------------------------------------------------
    date_filter = (corrected.get("filters") or {}).get("date") or {}
    if not date_filter:
        if "filters" not in corrected or not isinstance(corrected.get("filters"), dict):
            corrected["filters"] = {}
        corrected["filters"]["date"] = _DEFAULT_DATE.copy()
        assumed["date"] = "defaulted to last 30 days (none specified)"
        logger.info("Spec validator: no date filter — defaulted to last_n_days=30")
        date_filter = _DEFAULT_DATE.copy()

    # ------------------------------------------------------------------
    # 4. Strip clarifications (should already be empty, but enforce it)
    # ------------------------------------------------------------------
    corrected["clarifications"] = []

    # ------------------------------------------------------------------
    # 5. Build interpretation
    # ------------------------------------------------------------------
    date_label = _fmt_date(date_filter)
    platform_label = _fmt_platform(corrected.get("platform"))
    dims_label = _fmt_dims(resolved_dims)
    metrics_label = _fmt_metrics(resolved_metrics)

    summary_parts = [f"Running {metrics_label}", date_label, platform_label]
    if resolved_dims:
        summary_parts.append(dims_label)
    summary = " · ".join(summary_parts)

    interpretation: Dict[str, Any] = {
        "summary": summary,
        "metrics": resolved_metrics,
        "date_label": date_label,
        "platform_label": platform_label,
        "dimensions_label": dims_label,
        "assumed": assumed,
    }

    return corrected, interpretation
