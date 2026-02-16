from fastapi.testclient import TestClient

from api.app import app

client = TestClient(app)


def test_query_returns_clarifications_or_sql():
    """
    The API must return EITHER:
    - clarifications with no SQL, OR
    - SQL with no clarifications

    (Never both, never neither)
    """
    resp = client.post(
        "/query",
        json={"question": "show performance"},
    )

    assert resp.status_code == 200
    body = resp.json()

    assert "request_id" in body
    assert isinstance(body["spec"], dict)

    has_sql = body["sql"] is not None
    has_clarifications = bool(body["clarifications"])

    assert has_sql ^ has_clarifications  # exactly one must be true


def test_query_returns_sql_when_fully_specified():
    """
    Fully specified NL should return SQL and no clarifications.
    """
    resp = client.post(
        "/query",
        json={"question": "show clicks by campaign last 7 days"},
    )

    assert resp.status_code == 200
    body = resp.json()

    assert "request_id" in body
    assert body["clarifications"] == []
    assert isinstance(body["sql"], str)

    # whitespace-safe SQL assertion
    assert body["sql"].lstrip().lower().startswith("select")


def test_continue_query_only_if_clarifications_exist():
    """
    /query/continue should:
    - Apply answers if clarifications exist
    - Otherwise be unnecessary
    """
    initial = client.post(
        "/query",
        json={"question": "show clicks"},
    ).json()

    assert "request_id" in initial
    assert isinstance(initial["spec"], dict)

    # Case 1: No clarifications → SQL already returned
    if not initial["clarifications"]:
        assert isinstance(initial["sql"], str)
        assert initial["sql"].lstrip().lower().startswith("select")
        return

    # Case 2: Clarifications exist → continue flow
    spec = initial["spec"]

    answers = {}
    for c in initial["clarifications"]:
        answers[c["field"]] = c.get("options", [None])[0]

    resp = client.post(
        "/query/continue",
        json={
            "spec": spec,
            "answers": answers,
        },
    )

    assert resp.status_code == 200
    body = resp.json()

    assert body["clarifications"] == []
    assert isinstance(body["sql"], str)
    assert body["sql"].lstrip().lower().startswith("select")


# ---------------------------------------------------------------------------
# /feedback endpoint
# ---------------------------------------------------------------------------

_VALID_FEEDBACK = {
    "request_id": "test-req-123",
    "original_question": "show spend yesterday",
    "original_spec": {
        "grain": None,
        "platform": None,
        "metrics": ["spend"],
        "dimensions": [],
        "filters": {"date": {"yesterday": True}, "where": []},
    },
    "corrected_spec": {
        "grain": None,
        "platform": None,
        "metrics": ["cost"],
        "dimensions": [],
        "filters": {"date": {"yesterday": True}, "where": []},
    },
    "correction_type": "metric_mismatch",
    "notes": "spend should map to cost",
}


def test_feedback_valid_correction():
    resp = client.post("/feedback", json=_VALID_FEEDBACK)
    assert resp.status_code == 200
    body = resp.json()
    assert "feedback_id" in body
    assert body["status"] == "recorded"


def test_feedback_invalid_type():
    bad = {**_VALID_FEEDBACK, "correction_type": "not_a_real_type"}
    resp = client.post("/feedback", json=bad)
    assert resp.status_code == 422


def test_feedback_missing_question():
    bad = {**_VALID_FEEDBACK, "original_question": ""}
    resp = client.post("/feedback", json=bad)
    assert resp.status_code == 422
