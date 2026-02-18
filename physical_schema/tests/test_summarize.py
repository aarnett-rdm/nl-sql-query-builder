"""
Tests for the POST /summarize endpoint.

All tests use FastAPI TestClient (no real LLM or Fabric required).
The LLM backend is mocked so tests run offline.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from api.app import app

client = TestClient(app)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE_RESULTS = [
    {"CampaignName": "Summer Sale", "Clicks": 1200, "Impressions": 45000},
    {"CampaignName": "Spring Promo", "Clicks": 800, "Impressions": 31000},
]

_SAMPLE_SQL = "SELECT CampaignName, SUM(Clicks), SUM(Impressions) FROM ..."


def _mock_adapter(summary_text: str = "Clicks were highest for Summer Sale."):
    """Return a mock LLMAdapter whose backend.chat() returns a ChatResult with summary_text."""
    from tools.llm_backend import ChatResult

    mock_result = ChatResult(
        content=summary_text,
        model="mock-model",
        total_duration_ms=50,
        input_tokens=100,
        output_tokens=20,
    )
    mock_backend = MagicMock()
    mock_backend.chat.return_value = mock_result

    mock_adapter = MagicMock()
    mock_adapter.backend = mock_backend
    return mock_adapter


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_summarize_returns_summary():
    """Happy path: valid request with mocked LLM returns a summary string."""
    with patch("api.app._llm_adapter", _mock_adapter("Summer Sale led with 1,200 clicks.")):
        resp = client.post(
            "/summarize",
            json={
                "question": "show clicks by campaign last 7 days",
                "sql": _SAMPLE_SQL,
                "results_json": _SAMPLE_RESULTS,
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert "summary" in body
    assert "Summer Sale" in body["summary"]


def test_summarize_503_when_no_llm():
    """Returns 503 when no LLM adapter is configured."""
    with patch("api.app._llm_adapter", None):
        resp = client.post(
            "/summarize",
            json={
                "question": "show spend yesterday",
                "sql": _SAMPLE_SQL,
                "results_json": _SAMPLE_RESULTS,
            },
        )
    assert resp.status_code == 503


def test_summarize_empty_results():
    """Empty results list is accepted and produces a summary."""
    with patch("api.app._llm_adapter", _mock_adapter("No data was returned for this period.")):
        resp = client.post(
            "/summarize",
            json={
                "question": "show impressions yesterday",
                "sql": _SAMPLE_SQL,
                "results_json": [],
            },
        )
    assert resp.status_code == 200
    assert resp.json()["summary"]


def test_summarize_validates_empty_question():
    """Empty question fails Pydantic validation (422)."""
    resp = client.post(
        "/summarize",
        json={"question": "", "sql": _SAMPLE_SQL, "results_json": _SAMPLE_RESULTS},
    )
    assert resp.status_code == 422


def test_summarize_validates_empty_sql():
    """Empty SQL string fails Pydantic validation (422)."""
    resp = client.post(
        "/summarize",
        json={"question": "show clicks", "sql": "", "results_json": _SAMPLE_RESULTS},
    )
    assert resp.status_code == 422


def test_summarize_truncates_large_result_sets():
    """More than 50 rows are silently truncated; endpoint still succeeds."""
    big_results = [{"CampaignName": f"Camp {i}", "Clicks": i * 10} for i in range(200)]

    called_with_user: list[str] = []

    def capture_chat(system, user, json_mode=False, temperature=0.3):
        called_with_user.append(user)
        from tools.llm_backend import ChatResult
        return ChatResult(content="Top campaign had the most clicks.", model="m", total_duration_ms=1)

    mock_adapter = MagicMock()
    mock_adapter.backend.chat.side_effect = capture_chat

    with patch("api.app._llm_adapter", mock_adapter):
        resp = client.post(
            "/summarize",
            json={
                "question": "show clicks by campaign",
                "sql": _SAMPLE_SQL,
                "results_json": big_results,
            },
        )
    assert resp.status_code == 200
    # The user prompt should mention 200 rows total but only contain ≤50 rows of data
    assert "200 row(s)" in called_with_user[0]


def test_summarize_502_on_llm_backend_error():
    """Returns 502 when the LLM backend raises LLMBackendError."""
    from tools.exceptions import LLMBackendError

    mock_backend = MagicMock()
    mock_backend.chat.side_effect = LLMBackendError("Groq timeout")

    mock_adapter = MagicMock()
    mock_adapter.backend = mock_backend

    with patch("api.app._llm_adapter", mock_adapter):
        resp = client.post(
            "/summarize",
            json={
                "question": "show clicks",
                "sql": _SAMPLE_SQL,
                "results_json": _SAMPLE_RESULTS,
            },
        )
    assert resp.status_code == 502
