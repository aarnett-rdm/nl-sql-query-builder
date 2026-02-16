#!/usr/bin/env python3
"""
nl_to_spec.py

Conservative NL → Spec adapter.

Outputs a canonical Spec dict consumed by spec_executor.
Does NOT emit SQL.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional


# -------------------------
# Spec helpers
# -------------------------

def _empty_spec(raw_user_text: str) -> Dict[str, Any]:
    return {
        "grain": None,
        "platform": None,
        "metrics": [],
        "dimensions": [],
        "filters": {
            "date": {},
            "where": [],
        },
        "post": {},
        "compare": None,
        "clarifications": [],
        "notes": {
            "raw_user_text": raw_user_text,
        },
    }


def _add_clarification(
    spec: Dict[str, Any],
    field: str,
    reason: str,
    question: str,
    choices: Optional[List[str]] = None,
) -> None:
    item: Dict[str, Any] = {
        "field": field,
        "reason": reason,
        "question": question,
    }
    if choices:
        item["choices"] = choices
    spec["clarifications"].append(item)


def _dedupe_keep_order(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


# -------------------------
# Metric lexicon
# -------------------------

@dataclass
class MetricLexicon:
    canonical_metrics: List[str]
    synonyms: Dict[str, str]

    @classmethod
    def from_registry(cls, registry_path: str | Path) -> "MetricLexicon":
        reg = json.loads(Path(registry_path).read_text(encoding="utf-8"))
        metrics = sorted(reg.get("metrics", {}).keys(), key=len, reverse=True)

        synonyms = {k.lower(): v for k, v in reg.get("synonyms", {}).items()}
        synonyms |= {
            "spend": "cost",
            "total spend": "cost",
            "conv rate": "conversion rate",
            "cvr": "conversion rate",
            "rpc": "revenue per click",
            "click": "clicks",
        }

        return cls(metrics, synonyms)

    def extract(self, text: str) -> List[str]:
        t = text.lower()
        found: List[str] = []
        mask = t

        # exact canonical matches first
        for m in self.canonical_metrics:
            pat = rf"(?<!\w){re.escape(m)}(?!\w)"
            if re.search(pat, mask):
                found.append(m)
                mask = re.sub(pat, " ", mask)

        # synonyms
        for phrase, canon in self.synonyms.items():
            if canon not in found and re.search(rf"(?<!\w){re.escape(phrase)}(?!\w)", t):
                found.append(canon)

        return _dedupe_keep_order(found)


# -------------------------
# Extractors
# -------------------------

_QUOTED = r"[\"'""'']([^\"'""'']+)[\"'""'']"


# -------------------------
# Column alias vocabulary
# -------------------------

# Maps user-friendly terms (lowercase) -> physical column name.
# This is the rule-based fallback; the LLM integration will supersede this.
COLUMN_ALIASES: Dict[str, str] = {
    # State / geography
    "state": "State",
    "region": "State",
    # Account
    "account": "AccountName",
    "account name": "AccountName",
    "accountname": "AccountName",
    # Campaign
    "campaign name": "CampaignName",
    "campaign": "CampaignName",
    "campaignname": "CampaignName",
    # Campaign type / status
    "campaign type": "CampaignType",
    "campaign status": "CampaignStatus",
    "status": "CampaignStatus",
    # Ad group
    "ad group": "AdGroupName",
    "ad group name": "AdGroupName",
    "adgroup": "AdGroupName",
    # Network / device
    "network": "Network",
    "device": "Device",
    # Currency
    "currency": "CurrencyCode",
    "currency code": "CurrencyCode",
}

# Operator vocabulary: user phrases -> canonical op string
_OP_PHRASES: List[tuple] = [
    # order matters: longer phrases first to avoid partial matches
    ("greater than or equal to", ">="),
    ("greater than or equals", ">="),
    ("less than or equal to", "<="),
    ("less than or equals", "<="),
    ("not equal to", "!="),
    ("not equals", "!="),
    ("does not equal", "!="),
    ("doesn't equal", "!="),
    ("is not", "!="),
    ("isn't", "!="),
    ("greater than", ">"),
    ("more than", ">"),
    ("above", ">"),
    ("over", ">"),
    ("less than", "<"),
    ("fewer than", "<"),
    ("under", "<"),
    ("below", "<"),
    ("equal to", "="),
    ("equals", "="),
    ("containing", "contains"),
    ("contains", "contains"),
    ("like", "contains"),
    ("is", "="),
    ("=", "="),
    ("!=", "!="),
    (">=", ">="),
    ("<=", "<="),
    (">", ">"),
    ("<", "<"),
]


def _resolve_column(term: str) -> Optional[str]:
    """Map a user term to a physical column name via COLUMN_ALIASES."""
    low = term.strip().lower()
    return COLUMN_ALIASES.get(low)


def _extract_generic_where_filters(nl: str) -> List[Dict[str, Any]]:
    """
    Extract WHERE-style filters from natural language.

    Supported patterns:
      1. "where <field> <op> <value>"           -> explicit operator
      2. "for the <field> of <value>"           -> implicit equals
      3. "for the <value> <field>"              -> implicit equals (reversed)
      4. "<value> <field>" at phrase boundaries  -> implicit equals (e.g. "venue account")

    Returns list of {field, op, value, case_insensitive?} dicts.
    """
    low = nl.lower()
    filters: List[Dict[str, Any]] = []
    seen: set = set()

    def _add(field: str, op: str, value: str, case_insensitive: bool = False) -> None:
        key = (field, op, value.lower())
        if key not in seen:
            seen.add(key)
            entry: Dict[str, Any] = {"field": field, "op": op, "value": value}
            if case_insensitive:
                entry["case_insensitive"] = True
            filters.append(entry)

    # --- Pattern 1: "where <field> <op> <value>" ---
    # Also matches "filter by", "with <field> <op> <value>"
    for prefix_pat in [
        r"\bwhere\s+",
        r"\bfilter(?:\s+by)?\s+",
        r"\bwith\s+",
    ]:
        for alias_phrase, col_name in sorted(COLUMN_ALIASES.items(), key=lambda x: -len(x[0])):
            for op_phrase, op_canon in _OP_PHRASES:
                pat = (
                    prefix_pat
                    + re.escape(alias_phrase)
                    + r"\s+"
                    + re.escape(op_phrase)
                    + r"\s+"
                    + r"['\"]?(.+?)['\"]?"
                    + r"(?:\s+(?:and|or|by|grouped|last|yesterday|over|for)\b|$)"
                )
                m = re.search(pat, low)
                if m:
                    val = m.group(1).strip().strip("'\"")
                    if val:
                        ci = op_canon in ("=", "contains", "!=")
                        _add(col_name, op_canon, val, case_insensitive=ci)

    # --- Pattern 2: "for the <field> of <value>" ---
    # e.g. "for the state of Minnesota", "for the account of Venue"
    for alias_phrase, col_name in sorted(COLUMN_ALIASES.items(), key=lambda x: -len(x[0])):
        pat = (
            r"\bfor\s+the\s+"
            + re.escape(alias_phrase)
            + r"\s+of\s+"
            + r"['\"]?([A-Za-z0-9][\w\s\-&]{0,60}?)['\"]?"
            + r"(?:\s+(?:and|or|by|grouped|last|yesterday|over|for|where)\b|[,.]|$)"
        )
        m = re.search(pat, low)
        if m:
            val = m.group(1).strip().strip("'\"")
            if val and len(val) >= 2:
                _add(col_name, "=", val.title() if col_name == "State" else val)

    # --- Pattern 3: "for the <value> <field>" ---
    # e.g. "for the Minnesota state", "for the venue account"
    for alias_phrase, col_name in sorted(COLUMN_ALIASES.items(), key=lambda x: -len(x[0])):
        # Skip multi-word aliases for this pattern to avoid false matches
        if " " in alias_phrase:
            continue
        pat = (
            r"\bfor\s+the\s+"
            + r"['\"]?([A-Za-z0-9][\w\s\-&]{0,40}?)['\"]?"
            + r"\s+"
            + re.escape(alias_phrase)
            + r"\b"
        )
        m = re.search(pat, low)
        if m:
            val = m.group(1).strip().strip("'\"")
            if val and len(val) >= 2 and val not in {"the", "this", "that", "each", "every", "all"}:
                # "venue account" -> contains (partial match is more useful)
                _add(col_name, "contains", val, case_insensitive=True)

    # --- Pattern 4: implicit "<value> <field>" without "for the" ---
    # e.g. "venue account", "minnesota state" - but only at word boundaries
    # This is the most aggressive pattern, so we keep it conservative.
    for alias_phrase, col_name in sorted(COLUMN_ALIASES.items(), key=lambda x: -len(x[0])):
        if " " in alias_phrase:
            continue
        # Only match single-word values directly preceding the alias
        pat = (
            r"\b([a-z][a-z\-]{1,30})\s+"
            + re.escape(alias_phrase)
            + r"\b"
        )
        for m in re.finditer(pat, low):
            val = m.group(1).strip()
            # Skip common noise words
            noise = {
                "the", "this", "that", "each", "every", "all", "my", "our",
                "by", "per", "and", "or", "for", "from", "with", "last",
                "first", "new", "old", "total", "ad", "campaign", "google",
                "microsoft", "bing", "msft",
            }
            if val not in noise and len(val) >= 3:
                _add(col_name, "contains", val, case_insensitive=True)

    return filters


def _extract_last_n_days(low: str) -> Optional[int]:
    m = re.search(r"\blast\s+(\d+)\s+days\b", low)
    return int(m.group(1)) if m else None


def _extract_extended_date_filter(low: str) -> Optional[Dict[str, Any]]:
    """
    Extract date filters beyond the basic last_n_days/yesterday/mtd.

    Supported phrases:
      - "this week"              -> Monday of current week through today
      - "last week"              -> Monday-Sunday of previous week
      - "this month"             -> 1st of current month through today
      - "last month"             -> 1st-last of previous month
      - "this quarter"           -> 1st of current quarter through today
      - "last quarter"           -> full previous quarter
      - "Q1 2025", "Q2 2026"    -> explicit quarter of a year
      - "year to date" / "ytd"  -> Jan 1 of current year through today

    Returns a date dict with date_from/date_to, or None if no match.
    """
    from datetime import date as _date

    today = _date.today()

    # --- Year to date / YTD ---
    if re.search(r"\byear\s+to\s+date\b", low) or re.search(r"\bytd\b", low):
        return {
            "date_from": _date(today.year, 1, 1).isoformat(),
            "date_to": today.isoformat(),
        }

    # --- Explicit quarter: "Q1 2025", "q3 2026" ---
    m = re.search(r"\bq([1-4])\s+(\d{4})\b", low)
    if m:
        q, yr = int(m.group(1)), int(m.group(2))
        q_start_month = (q - 1) * 3 + 1
        q_start = _date(yr, q_start_month, 1)
        # End of quarter: start of next quarter minus 1 day
        if q < 4:
            q_end = _date(yr, q_start_month + 3, 1) - timedelta(days=1)
        else:
            q_end = _date(yr, 12, 31)
        return {
            "date_from": q_start.isoformat(),
            "date_to": q_end.isoformat(),
        }

    # --- This quarter ---
    if re.search(r"\bthis\s+quarter\b", low):
        q = (today.month - 1) // 3 + 1
        q_start_month = (q - 1) * 3 + 1
        return {
            "date_from": _date(today.year, q_start_month, 1).isoformat(),
            "date_to": today.isoformat(),
        }

    # --- Last quarter ---
    if re.search(r"\blast\s+quarter\b", low):
        q = (today.month - 1) // 3 + 1
        prev_q = q - 1 if q > 1 else 4
        prev_yr = today.year if q > 1 else today.year - 1
        q_start_month = (prev_q - 1) * 3 + 1
        q_start = _date(prev_yr, q_start_month, 1)
        if prev_q < 4:
            q_end = _date(prev_yr, q_start_month + 3, 1) - timedelta(days=1)
        else:
            q_end = _date(prev_yr, 12, 31)
        return {
            "date_from": q_start.isoformat(),
            "date_to": q_end.isoformat(),
        }

    # --- Last month ---
    if re.search(r"\blast\s+month\b", low):
        first_of_this_month = today.replace(day=1)
        last_of_prev_month = first_of_this_month - timedelta(days=1)
        first_of_prev_month = last_of_prev_month.replace(day=1)
        return {
            "date_from": first_of_prev_month.isoformat(),
            "date_to": last_of_prev_month.isoformat(),
        }

    # --- This month (but not "month to date" which is already handled as mtd) ---
    if re.search(r"\bthis\s+month\b", low):
        return {
            "date_from": today.replace(day=1).isoformat(),
            "date_to": today.isoformat(),
        }

    # --- Last week ---
    if re.search(r"\blast\s+week\b", low):
        # Monday of last week through Sunday of last week
        days_since_monday = today.weekday()  # 0=Monday
        this_monday = today - timedelta(days=days_since_monday)
        last_monday = this_monday - timedelta(days=7)
        last_sunday = this_monday - timedelta(days=1)
        return {
            "date_from": last_monday.isoformat(),
            "date_to": last_sunday.isoformat(),
        }

    # --- This week ---
    if re.search(r"\bthis\s+week\b", low):
        days_since_monday = today.weekday()
        this_monday = today - timedelta(days=days_since_monday)
        return {
            "date_from": this_monday.isoformat(),
            "date_to": today.isoformat(),
        }

    return None


def _extract_campaign_ids(nl: str) -> List[int]:
    """
    Extract campaign ids only from phrases like:
      "campaign id 1,2,3" / "campaign ids: 1 2 3" / "these campaign IDs: 1, 2, 3"
    Avoid capturing unrelated numbers like "last 30 days".
    """
    low = nl.lower()

    m = re.search(r"\bcampaign\s+ids?\b\s*[:\-]?\s*(.*)$", low)
    if not m:
        return []

    tail = m.group(1)
    tail = re.split(r"\b(for|over|in|during|last|yesterday|mtd|month)\b", tail)[0]

    ids = re.findall(r"\b\d+\b", tail)
    return [int(x) for x in ids]


def _extract_campaign_name_terms(nl: str) -> List[str]:
    # returns only quoted terms
    return re.findall(_QUOTED, nl)


def _wants_period_over_period(low: str) -> bool:
    return bool(re.search(r"\b(compared to|vs\.?|versus|prior)\b", low))


def _wants_cross_platform(low: str) -> bool:
    return bool(
        re.search(r"\bcompare\b", low)
        and re.search(r"\bgoogle\b", low)
        and re.search(r"\b(microsoft|msft|bing)\b", low)
    )

def _extract_campaign_free_text(nl: str) -> List[str]:
    """
    Campaign name free-text extraction.

    Supported patterns (examples):
      - where campaign name contains 'spring training'
      - where campaign name containing 'spring training'
      - campaign name includes 'spring training'
      - campaign name has 'spring training'
      - campaign name matches 'spring training'
      - campaigns with 'spring training'
      - where 'spring training' is in the campaign name
      - with 'spring training' in the campaign name
      - filter campaigns to those with campaign name containing 'spring training'

    Returns a list of terms (strings). Prefers quoted phrases.
    Conservative: only triggers when user clearly indicates campaign-name intent.
    """
    low = nl.lower()
    quoted = _extract_campaign_name_terms(nl)
    terms: List[str] = []

    # --- Helpers ---
    def _stop_tail(tail: str) -> str:
        # Stop at common clause boundaries so we don't swallow date/comparison/grouping.
        tail = re.split(
            r"\b("
            r"grouped\s+by|group\s+by|by|"
            r"and|or|"
            r"last|yesterday|mtd|month|month\s+to\s+date|"
            r"compare|vs|versus|prior|"
            r"platform|google|microsoft|msft|bing"
            r")\b",
            tail,
            flags=re.IGNORECASE,
        )[0]
        return tail.strip(" \"'“”‘’.,:;()[]{}")

    def _capture_after_verb(verb_pat: str) -> Optional[str]:
        """
        Capture everything after a verb phrase (contains/includes/has/matches/with/having)
        while staying conservative.
        """
        m = re.search(
            rf"\b{verb_pat}\b\s+(.+)$",
            nl,
            flags=re.IGNORECASE,
        )
        if not m:
            return None
        tail = _stop_tail(m.group(1))
        return tail or None

    # ------------------------------------------------------------------
    # 1) Strongest signal: explicit "campaign name <verb> ..."
    # ------------------------------------------------------------------
    if re.search(r"\bcampaign\s*name\b", low) and re.search(
        r"\b(contains|containing|includes|including|has|having|matches|match)\b",
        low,
    ):
        if quoted:
            return quoted[:]

        # Capture tail after the first matching verb, but only after "campaign name ..."
        m = re.search(
            r"\bcampaign\s*name\b.*\b(contains|containing|includes|including|has|having|matches|match)\b\s+(.+)$",
            nl,
            flags=re.IGNORECASE,
        )
        if m:
            tail = _stop_tail(m.group(2))
            if tail:
                return [tail]

    # ------------------------------------------------------------------
    # 2) Clear campaign intent with "campaign(s) with/having/containing ..."
    #    (still conservative: requires the word campaign)
    # ------------------------------------------------------------------
    if re.search(r"\bcampaigns?\b", low) and re.search(
        r"\b(with|having|contains|containing|includes|including)\b",
        low,
    ):
        if quoted:
            return quoted[:]

        # Capture tail after "campaign(s) <verb> ..."
        m = re.search(
            r"\bcampaigns?\b.*\b(with|having|contains|containing|includes|including)\b\s+(.+)$",
            nl,
            flags=re.IGNORECASE,
        )
        if m:
            tail = _stop_tail(m.group(2))
            # Avoid capturing extremely short/noisy tails
            if tail and len(tail) >= 3:
                return [tail]

    # ------------------------------------------------------------------
    # 3) "<term> in the campaign name" or "in the name"
    #    Prefer quoted; unquoted is risky, so only use quoted.
    # ------------------------------------------------------------------
    if (re.search(r"\bin\s+the\s+campaign\s+name\b", low) or re.search(r"\bin\s+the\s+name\b", low)):
        if quoted:
            return quoted[:]

        # Conservative unquoted support:
        # Look for "... with <tail> in the (campaign) name"
        m = re.search(
            r"\bwith\b\s+(.+?)\s+\bin\s+the\s+(?:campaign\s+)?name\b",
            nl,
            flags=re.IGNORECASE,
        )
        if m:
            tail = _stop_tail(m.group(1))
            if tail and len(tail.split()) <= 6:
                return [tail]

    # ------------------------------------------------------------------
    # 4) Conservative fallback: "<term> campaigns" ONLY when filter intent words exist
    # ------------------------------------------------------------------
    if not terms and re.search(r"\b(with|having|containing|contains|includes|named)\b", low):
        m = re.search(
            r"\b([a-z0-9][a-z0-9\s\-\&]{2,60}?)\s+campaigns?\b",
            low,
            flags=re.IGNORECASE,
        )
        if m:
            tail = m.group(1).strip()
            if tail not in {"all", "my", "these", "those"} and len(tail.split()) <= 6:
                return [tail]

    return terms

# -------------------------
# Adapter
# -------------------------

class NLToSpecAdapter:
    def __init__(self, metric_registry_path: str | Path) -> None:
        self.metrics = MetricLexicon.from_registry(metric_registry_path)

    def parse(self, nl: str) -> Dict[str, Any]:
        spec = _empty_spec(nl)
        low = nl.lower()

        # -------- Platform detection --------
        g = bool(re.search(r"\bgoogle\b", low))
        m = bool(re.search(r"\b(microsoft|msft|bing)\b", low))

        if g ^ m:
            spec["platform"] = "google_ads" if g else "microsoft_ads"

        # -------- Date --------
        if "yesterday" in low:
            spec["filters"]["date"] = {"yesterday": True}

        last_n = _extract_last_n_days(low)
        if last_n is not None:
            spec["filters"]["date"] = {"last_n_days": last_n}

        if "month to date" in low or re.search(r"\bmtd\b", low):
            spec["filters"]["date"] = {"mtd": True}

        # Extended date phrases (this week, last month, Q1 2025, YTD, etc.)
        # Only apply if no date filter was already set above
        if not spec["filters"]["date"]:
            ext_date = _extract_extended_date_filter(low)
            if ext_date:
                spec["filters"]["date"] = ext_date

        # -------- Metrics --------
        spec["metrics"] = self.metrics.extract(nl)
        if not spec["metrics"]:
            _add_clarification(
                spec,
                "metrics",
                "No known metrics recognized.",
                "Which metrics do you want?",
            )

        # -------- Dimensions --------
        if "by account" in low:
            spec["dimensions"].append("AccountName")
        if "by campaign" in low:
            spec["dimensions"].append("CampaignName")
        if "by campaign name" in low:
            spec["dimensions"].append("CampaignName")

        # (Common compare phrasing implies campaign-level even if not explicit)
        if (
            ("campaign" in low)
            and ("CampaignName" not in spec["dimensions"])
            and (_wants_period_over_period(low) or _wants_cross_platform(low))
        ):
            spec["dimensions"].append("CampaignName")

        spec["dimensions"] = _dedupe_keep_order(spec["dimensions"])

        # -------- Campaign name filters --------
        terms = _extract_campaign_free_text(nl)
        if terms:
            mode = "all" if re.search(r"\b(both|and)\b", low) else "any"
            spec.setdefault("filters", {}).setdefault("campaign", {})
            # keep spec contract stable; query_builder controls case-insensitive via filter_config
            spec["filters"]["campaign"] = {"terms": terms, "mode": mode}

        # -------- Campaign ID filters --------
        ids = _extract_campaign_ids(nl)
        if ids:
            spec["filters"]["campaign_ids"] = ids

        # -------- Generic WHERE filters --------
        spec["filters"]["where"].extend(_extract_generic_where_filters(nl))

        # -------- Comparisons --------
        # Period-over-period: last N days vs prior N days (offset)
        if _wants_period_over_period(low):
            n = _extract_last_n_days(low)
            if n is not None:
                compare_metric = None
                if "conversion rate" in low or "conv rate" in low or "cvr" in low:
                    compare_metric = "conversion rate"
                elif spec["metrics"]:
                    compare_metric = spec["metrics"][0]

                if compare_metric:
                    spec["compare"] = {
                        "type": "period_over_period",
                        "metric": compare_metric,
                        "current": {"last_n_days": n, "offset_days": 0},
                        "prior": {"last_n_days": n, "offset_days": n},
                    }
                else:
                    _add_clarification(
                        spec,
                        "compare.metric",
                        "Comparison requested but no metric identified.",
                        "Which metric should I compare across periods?",
                    )

        # Cross-platform compare: google vs microsoft
        if _wants_cross_platform(low):
            spec["compare"] = {
                "type": "cross_platform",
                "metrics": ["clicks"] if "click" in low else (spec["metrics"][:1] or ["clicks"]),
            }

        return spec


def nl_to_spec(nl: str, metric_registry_path: str | Path) -> Dict[str, Any]:
    return NLToSpecAdapter(metric_registry_path).parse(nl)
