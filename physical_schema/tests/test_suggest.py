"""
Tests for the POST /suggest endpoint.

All tests use FastAPI TestClient (no real LLM or Fabric required).
The LLM backend is mocked so tests run offline.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from api.app import app, _suggest_cache

client = TestClient(app)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE_SPEC = {
    "metrics": ["clicks", "impressions"],
    "dimensions": ["campaignname"],
    "grain": "campaign_calendar",
    "platform": "google_ads",
    "filters": {"date": {"date_from": "2026-02-11", "date_to": "2026-02-17"}, "where": []},
}

_SAMPLE_QUESTION = "show clicks and impressions by campaign last 7 days"

_SAMPLE_SUGGESTIONS = [
    "Break out by ad group instead of campaign",
    "Compare clicks last 7 days vs. the prior 7 days",
    "Show conversion rate and cost per click for the same period",
]


def _mock_adapter(suggestions: list[str] | None = None, raise_error: bool = False):
    """Return a mock LLMAdapter whose backend.chat() returns suggestions JSON."""
    from tools.llm_backend import ChatResult
    from tools.exceptions import LLMBackendError

    if raise_error:
        mock_backend = MagicMock()
        mock_backend.chat.side_effect = LLMBackendError("backend offline")
    else:
        payload = json.dumps({"suggestions": suggestions or _SAMPLE_SUGGESTIONS})
        mock_result = ChatResult(
            content=payload,
            model="mock-model",
            total_duration_ms=50,
            input_tokens=80,
            output_tokens=30,
        )
        mock_backend = MagicMock()
        mock_backend.chat.return_value = mock_result

    mock_adapter = MagicMock()
    mock_adapter.backend = mock_backend
    return mock_adapter


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_suggest_returns_three_suggestions():
    """Happy path: valid request returns exactly the 3 LLM suggestions."""
    _suggest_cache.clear()
    with patch("api.app._llm_adapter", _mock_adapter()):
        resp = client.post(
            "/suggest",
            json={"question": _SAMPLE_QUESTION, "spec": _SAMPLE_SPEC},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert "suggestions" in body
    assert len(body["suggestions"]) == 3
    assert body["suggestions"][0] == _SAMPLE_SUGGESTIONS[0]


def test_suggest_503_when_no_llm():
    """Returns 503 when no LLM adapter is configured."""
    _suggest_cache.clear()
    with patch("api.app._llm_adapter", None):
        resp = client.post(
            "/suggest",
            json={"question": _SAMPLE_QUESTION, "spec": _SAMPLE_SPEC},
        )
    assert resp.status_code == 503


def test_suggest_502_on_llm_backend_error():
    """Returns 502 when the LLM backend raises LLMBackendError."""
    _suggest_cache.clear()
    with patch("api.app._llm_adapter", _mock_adapter(raise_error=True)):
        resp = client.post(
            "/suggest",
            json={"question": _SAMPLE_QUESTION, "spec": _SAMPLE_SPEC},
        )
    assert resp.status_code == 502


def test_suggest_cache_hit_skips_llm():
    """Second identical request uses the in-process cache — LLM is not called again."""
    _suggest_cache.clear()
    mock_adapter = _mock_adapter()

    with patch("api.app._llm_adapter", mock_adapter):
        client.post("/suggest", json={"question": _SAMPLE_QUESTION, "spec": _SAMPLE_SPEC})
        client.post("/suggest", json={"question": _SAMPLE_QUESTION, "spec": _SAMPLE_SPEC})

    # chat() should have been called exactly once
    assert mock_adapter.backend.chat.call_count == 1


def test_suggest_different_questions_call_llm_twice():
    """Two different questions each hit the LLM (different cache keys)."""
    _suggest_cache.clear()
    mock_adapter = _mock_adapter()

    with patch("api.app._llm_adapter", mock_adapter):
        client.post("/suggest", json={"question": "show clicks last week", "spec": _SAMPLE_SPEC})
        client.post("/suggest", json={"question": "show spend by platform", "spec": _SAMPLE_SPEC})

    assert mock_adapter.backend.chat.call_count == 2


def test_suggest_caps_at_three():
    """Even if LLM returns more than 3, only the first 3 are returned."""
    _suggest_cache.clear()
    five_suggestions = [f"Question {i}" for i in range(5)]
    with patch("api.app._llm_adapter", _mock_adapter(suggestions=five_suggestions)):
        resp = client.post(
            "/suggest",
            json={"question": _SAMPLE_QUESTION, "spec": _SAMPLE_SPEC},
        )
    assert resp.status_code == 200
    assert len(resp.json()["suggestions"]) == 3


def test_suggest_malformed_json_returns_empty_list():
    """If LLM returns garbage JSON, the endpoint returns 200 with an empty list."""
    _suggest_cache.clear()
    from tools.llm_backend import ChatResult

    bad_result = ChatResult(
        content="Sorry, I cannot help with that.",
        model="mock-model",
        total_duration_ms=10,
    )
    mock_backend = MagicMock()
    mock_backend.chat.return_value = bad_result
    mock_adapter = MagicMock()
    mock_adapter.backend = mock_backend

    with patch("api.app._llm_adapter", mock_adapter):
        resp = client.post(
            "/suggest",
            json={"question": _SAMPLE_QUESTION, "spec": _SAMPLE_SPEC},
        )
    assert resp.status_code == 200
    assert resp.json()["suggestions"] == []


def test_suggest_validates_empty_question():
    """Empty question string fails Pydantic validation (422)."""
    resp = client.post(
        "/suggest",
        json={"question": "", "spec": _SAMPLE_SPEC},
    )
    assert resp.status_code == 422


def test_suggest_validates_missing_spec():
    """Missing spec field fails Pydantic validation (422)."""
    resp = client.post(
        "/suggest",
        json={"question": _SAMPLE_QUESTION},
    )
    assert resp.status_code == 422
