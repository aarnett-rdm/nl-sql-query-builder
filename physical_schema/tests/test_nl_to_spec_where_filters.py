from tools.nl_to_spec import nl_to_spec


# ==========================================
# Existing tests (must continue to pass)
# ==========================================

def test_state_filter(metric_registry_path):
    q = "Show me profit by account for the state of Minnesota"
    spec = nl_to_spec(q, metric_registry_path)

    assert {"field": "State", "op": "=", "value": "Minnesota"} in spec["filters"]["where"]


def test_account_contains_filter(metric_registry_path):
    q = "Show me profit for the venue account"
    spec = nl_to_spec(q, metric_registry_path)

    assert {
        "field": "AccountName",
        "op": "contains",
        "value": "venue",
        "case_insensitive": True,
    } in spec["filters"]["where"]


# ==========================================
# New: "for the <field> of <value>" pattern
# ==========================================

def test_state_filter_texas(metric_registry_path):
    q = "Show me clicks for the state of Texas"
    spec = nl_to_spec(q, metric_registry_path)

    assert {"field": "State", "op": "=", "value": "Texas"} in spec["filters"]["where"]


def test_state_filter_new_york(metric_registry_path):
    q = "Show me cost by campaign for the state of New York last 30 days"
    spec = nl_to_spec(q, metric_registry_path)

    assert {"field": "State", "op": "=", "value": "New York"} in spec["filters"]["where"]


# ==========================================
# New: "where <field> <op> <value>" pattern
# ==========================================

def test_where_status_equals(metric_registry_path):
    q = "Show me clicks where status is active"
    spec = nl_to_spec(q, metric_registry_path)

    where = spec["filters"]["where"]
    match = [f for f in where if f["field"] == "CampaignStatus" and f["op"] == "=" and f["value"].lower() == "active"]
    assert len(match) >= 1, f"Expected CampaignStatus = active, got: {where}"


def test_where_device_equals(metric_registry_path):
    q = "Show me impressions where device is mobile"
    spec = nl_to_spec(q, metric_registry_path)

    where = spec["filters"]["where"]
    match = [f for f in where if f["field"] == "Device" and f["value"].lower() == "mobile"]
    assert len(match) >= 1, f"Expected Device filter for mobile, got: {where}"


# ==========================================
# New: multiple filters in one query
# ==========================================

def test_multiple_filters(metric_registry_path):
    q = "Show me clicks for the state of California for the venue account"
    spec = nl_to_spec(q, metric_registry_path)

    where = spec["filters"]["where"]
    state_filters = [f for f in where if f["field"] == "State"]
    account_filters = [f for f in where if f["field"] == "AccountName"]

    assert len(state_filters) >= 1, f"Expected State filter, got: {where}"
    assert len(account_filters) >= 1, f"Expected AccountName filter, got: {where}"


# ==========================================
# New: no false positives
# ==========================================

def test_no_false_positive_on_plain_metric_query(metric_registry_path):
    q = "Show me total clicks by campaign last 7 days"
    spec = nl_to_spec(q, metric_registry_path)

    # Should not produce spurious WHERE filters for a plain metric query
    assert spec["filters"]["where"] == [], f"Expected no WHERE filters, got: {spec['filters']['where']}"


def test_no_false_positive_on_comparison(metric_registry_path):
    q = "Compare Google vs Microsoft clicks by campaign last 30 days"
    spec = nl_to_spec(q, metric_registry_path)

    # "google" and "microsoft" should not become WHERE filters
    where = spec["filters"]["where"]
    bad = [f for f in where if f["value"].lower() in ("google", "microsoft", "bing")]
    assert len(bad) == 0, f"Platform names should not become WHERE filters: {bad}"
