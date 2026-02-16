from tools.nl_to_spec import nl_to_spec


def test_biggest_drop_conversion_rate(metric_registry_path):
    q = (
        "Which campaigns saw the biggest drop in conversion rate "
        "in the last 7 days compared to the prior 7 days"
    )
    spec = nl_to_spec(q, metric_registry_path)

    assert spec["compare"] is not None
    assert spec["compare"]["type"] == "period_over_period"

    assert spec["compare"]["metric"] == "conversion rate"

    assert spec["compare"]["current"]["last_n_days"] == 7
    assert spec["compare"]["current"].get("offset_days", 0) == 0

    assert spec["compare"]["prior"]["last_n_days"] == 7
    assert spec["compare"]["prior"]["offset_days"] == 7


def test_cross_platform_click_ratio(metric_registry_path):
    q = (
        "Compare google and microsoft campaigns by campaign name "
        "and show the ratio of clicks"
    )
    spec = nl_to_spec(q, metric_registry_path)

    assert spec["compare"] is not None
    assert spec["compare"]["type"] == "cross_platform"
    assert spec["compare"]["metrics"] == ["clicks"]
    assert "CampaignName" in spec["dimensions"]
