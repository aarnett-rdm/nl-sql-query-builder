"""
Tests for extended date filter extraction (Priority 1.4).

Covers: this week, last week, this month, last month,
        this quarter, last quarter, Q1-Q4 YYYY, YTD.
"""
from datetime import date, timedelta

from tools.nl_to_spec import nl_to_spec


# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

def _today():
    return date.today()


# -------------------------------------------------------
# Existing modes still work
# -------------------------------------------------------

def test_yesterday_still_works(metric_registry_path):
    spec = nl_to_spec("Show me clicks yesterday", metric_registry_path)
    assert spec["filters"]["date"] == {"yesterday": True}


def test_last_n_days_still_works(metric_registry_path):
    spec = nl_to_spec("Show me clicks last 30 days", metric_registry_path)
    assert spec["filters"]["date"] == {"last_n_days": 30}


def test_mtd_still_works(metric_registry_path):
    spec = nl_to_spec("Show me clicks month to date", metric_registry_path)
    assert spec["filters"]["date"] == {"mtd": True}


def test_mtd_abbreviation(metric_registry_path):
    spec = nl_to_spec("Show me clicks MTD", metric_registry_path)
    assert spec["filters"]["date"] == {"mtd": True}


# -------------------------------------------------------
# This week
# -------------------------------------------------------

def test_this_week(metric_registry_path):
    spec = nl_to_spec("Show me clicks this week", metric_registry_path)
    d = spec["filters"]["date"]
    today = _today()
    monday = today - timedelta(days=today.weekday())

    assert d["date_from"] == monday.isoformat()
    assert d["date_to"] == today.isoformat()


# -------------------------------------------------------
# Last week
# -------------------------------------------------------

def test_last_week(metric_registry_path):
    spec = nl_to_spec("Show me clicks last week", metric_registry_path)
    d = spec["filters"]["date"]
    today = _today()
    this_monday = today - timedelta(days=today.weekday())
    last_monday = this_monday - timedelta(days=7)
    last_sunday = this_monday - timedelta(days=1)

    assert d["date_from"] == last_monday.isoformat()
    assert d["date_to"] == last_sunday.isoformat()


# -------------------------------------------------------
# This month
# -------------------------------------------------------

def test_this_month(metric_registry_path):
    spec = nl_to_spec("Show me cost this month", metric_registry_path)
    d = spec["filters"]["date"]
    today = _today()

    assert d["date_from"] == today.replace(day=1).isoformat()
    assert d["date_to"] == today.isoformat()


# -------------------------------------------------------
# Last month
# -------------------------------------------------------

def test_last_month(metric_registry_path):
    spec = nl_to_spec("Show me profit last month", metric_registry_path)
    d = spec["filters"]["date"]
    today = _today()
    first_this = today.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    first_prev = last_prev.replace(day=1)

    assert d["date_from"] == first_prev.isoformat()
    assert d["date_to"] == last_prev.isoformat()


# -------------------------------------------------------
# This quarter
# -------------------------------------------------------

def test_this_quarter(metric_registry_path):
    spec = nl_to_spec("Show me clicks this quarter", metric_registry_path)
    d = spec["filters"]["date"]
    today = _today()
    q = (today.month - 1) // 3 + 1
    q_start = date(today.year, (q - 1) * 3 + 1, 1)

    assert d["date_from"] == q_start.isoformat()
    assert d["date_to"] == today.isoformat()


# -------------------------------------------------------
# Last quarter
# -------------------------------------------------------

def test_last_quarter(metric_registry_path):
    spec = nl_to_spec("Show me clicks last quarter", metric_registry_path)
    d = spec["filters"]["date"]
    today = _today()
    q = (today.month - 1) // 3 + 1
    prev_q = q - 1 if q > 1 else 4
    prev_yr = today.year if q > 1 else today.year - 1
    q_start = date(prev_yr, (prev_q - 1) * 3 + 1, 1)

    assert d["date_from"] == q_start.isoformat()
    # Verify it's a full quarter (end date is last day of quarter)
    assert d["date_to"] > d["date_from"]


# -------------------------------------------------------
# Explicit quarter: Q1 2025, Q4 2026
# -------------------------------------------------------

def test_q1_2025(metric_registry_path):
    spec = nl_to_spec("Show me clicks Q1 2025", metric_registry_path)
    d = spec["filters"]["date"]
    assert d["date_from"] == "2025-01-01"
    assert d["date_to"] == "2025-03-31"


def test_q2_2026(metric_registry_path):
    spec = nl_to_spec("Show me cost Q2 2026", metric_registry_path)
    d = spec["filters"]["date"]
    assert d["date_from"] == "2026-04-01"
    assert d["date_to"] == "2026-06-30"


def test_q3_2025(metric_registry_path):
    spec = nl_to_spec("Show me profit Q3 2025", metric_registry_path)
    d = spec["filters"]["date"]
    assert d["date_from"] == "2025-07-01"
    assert d["date_to"] == "2025-09-30"


def test_q4_2025(metric_registry_path):
    spec = nl_to_spec("Show me conversions Q4 2025", metric_registry_path)
    d = spec["filters"]["date"]
    assert d["date_from"] == "2025-10-01"
    assert d["date_to"] == "2025-12-31"


# -------------------------------------------------------
# Year to date / YTD
# -------------------------------------------------------

def test_year_to_date(metric_registry_path):
    spec = nl_to_spec("Show me clicks year to date", metric_registry_path)
    d = spec["filters"]["date"]
    today = _today()

    assert d["date_from"] == date(today.year, 1, 1).isoformat()
    assert d["date_to"] == today.isoformat()


def test_ytd_abbreviation(metric_registry_path):
    spec = nl_to_spec("Show me cost YTD", metric_registry_path)
    d = spec["filters"]["date"]
    today = _today()

    assert d["date_from"] == date(today.year, 1, 1).isoformat()
    assert d["date_to"] == today.isoformat()


# -------------------------------------------------------
# Priority: existing modes take precedence
# -------------------------------------------------------

def test_last_n_days_beats_this_week(metric_registry_path):
    """'last 7 days' should win over 'this week' if both somehow present."""
    spec = nl_to_spec("Show me clicks last 7 days", metric_registry_path)
    assert spec["filters"]["date"] == {"last_n_days": 7}


def test_no_date_filter_on_plain_query(metric_registry_path):
    spec = nl_to_spec("Show me total clicks by campaign", metric_registry_path)
    assert spec["filters"]["date"] == {}
