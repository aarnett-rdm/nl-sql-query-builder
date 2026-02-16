from tools.nl_to_spec import nl_to_spec

def test_total_spend_yesterday(metric_registry_path):
    q = "What was our total spend yesterday?"
    spec = nl_to_spec(q, metric_registry_path)

    assert spec["metrics"] == ["cost"]
    assert spec["filters"]["date"] == {"yesterday": True}
    assert spec["dimensions"] == []
    assert spec["clarifications"] == []


def test_profit_by_account_google(metric_registry_path):
    q = "Show me profit by account for all google accounts"
    spec = nl_to_spec(q, metric_registry_path)

    assert spec["platform"] == "google_ads"
    assert spec["metrics"] == ["profit"]
    assert spec["dimensions"] == ["AccountName"]
    assert spec["clarifications"] == []
