from tools.nl_to_spec import nl_to_spec
from tools.spec_executor import execute_spec


def test_smoke_yesterday_spend(metric_registry_path):
    spec = nl_to_spec("What was our total spend yesterday?", metric_registry_path)
    sql = execute_spec(spec)

    assert "SELECT" in sql.upper()
    assert "FROM" in sql.upper()


def test_smoke_campaign_ids(metric_registry_path):
    spec = nl_to_spec(
        "For the last 30 days show cost for campaign IDs 101, 102, 103",
        metric_registry_path,
    )
    sql = execute_spec(spec)

    assert "IN (101, 102, 103)" in sql.replace("\n", " ")
