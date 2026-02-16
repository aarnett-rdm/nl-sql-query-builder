from tools.nl_to_spec import nl_to_spec


def test_campaign_name_all_terms_mtd(metric_registry_path):
    q = "For campaigns with both 'tickets' AND 'nba' in the name, show spend MTD"
    spec = nl_to_spec(q, metric_registry_path)

    assert spec["metrics"] == ["cost"]
    assert spec["filters"]["date"] == {"mtd": True}

    campaign = spec["filters"]["campaign"]
    assert campaign["terms"] == ["tickets", "nba"]
    assert campaign["mode"] == "all"


def test_campaign_ids_last_30_days(metric_registry_path):
    q = (
        "For the last 30 days, show cost and profit "
        "for campaign IDs 101, 102, 103, 104"
    )
    spec = nl_to_spec(q, metric_registry_path)

    assert spec["filters"]["date"] == {"last_n_days": 30}
    assert spec["filters"]["campaign_ids"] == [101, 102, 103, 104]
    assert set(spec["metrics"]) == {"cost", "profit"}

def test_campaign_name_contains_last_7_days(metric_registry_path):
    q = (
        "Show clicks, cost, profit for Google Ads campaigns last 7 days "
        "where campaign name contains 'spring training', grouped by campaign."
    )
    spec = nl_to_spec(q, metric_registry_path)

    assert spec["platform"] == "google_ads"
    assert set(spec["metrics"]) == {"clicks", "cost", "profit"}
    assert spec["dimensions"] == ["CampaignName"]
    assert spec["filters"]["date"] == {"last_n_days": 7}

    campaign = spec["filters"]["campaign"]
    assert campaign["terms"] == ["spring training"]
    assert campaign["mode"] == "any"

