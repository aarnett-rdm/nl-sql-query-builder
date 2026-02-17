"""
Tests for the Groq LLM backend (tools/groq_backend.py).

All tests are offline (no real Groq API calls) — they use mocks.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from tools.groq_backend import GroqBackend, GroqError
from tools.llm_backend import LLMBackend, ChatResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_mock_response(content: str = '{"metrics": ["clicks"]}', model: str = "llama-3.3-70b-versatile"):
    """Build a mock Groq API response."""
    mock_choice = MagicMock()
    mock_choice.message.content = content

    mock_usage = MagicMock()
    mock_usage.prompt_tokens = 100
    mock_usage.completion_tokens = 50

    mock_response = MagicMock()
    mock_response.choices = [mock_choice]
    mock_response.usage = mock_usage
    mock_response.model = model

    return mock_response


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------

class TestGroqBackendProtocol:
    """Verify GroqBackend satisfies the LLMBackend Protocol."""

    def test_satisfies_llm_backend_protocol(self):
        """GroqBackend must be recognized as an LLMBackend at runtime."""
        with patch("tools.groq_backend.Groq"):
            backend = GroqBackend(api_key="gsk_test", model="llama-3.3-70b-versatile")
            assert isinstance(backend, LLMBackend)

    def test_model_name_property(self):
        """model_name property must return the configured model."""
        with patch("tools.groq_backend.Groq"):
            backend = GroqBackend(api_key="gsk_test", model="llama-3.1-8b-instant")
            assert backend.model_name == "llama-3.1-8b-instant"

    def test_default_model(self):
        """Default model should be llama-3.3-70b-versatile."""
        with patch("tools.groq_backend.Groq"):
            backend = GroqBackend(api_key="gsk_test")
            assert backend.model_name == "llama-3.3-70b-versatile"


# ---------------------------------------------------------------------------
# Chat method
# ---------------------------------------------------------------------------

class TestGroqBackendChat:
    """Tests for the chat() method."""

    def test_chat_returns_chat_result(self):
        """chat() must return a ChatResult dataclass."""
        with patch("tools.groq_backend.Groq") as mock_groq_cls:
            mock_client = MagicMock()
            mock_groq_cls.return_value = mock_client
            mock_client.chat.completions.create.return_value = _make_mock_response()

            backend = GroqBackend(api_key="gsk_test")
            result = backend.chat(system="You are helpful.", user="Show clicks")

            assert isinstance(result, ChatResult)
            assert result.content == '{"metrics": ["clicks"]}'
            assert result.model == "llama-3.3-70b-versatile"
            assert result.input_tokens == 100
            assert result.output_tokens == 50
            assert result.total_duration_ms >= 0

    def test_chat_passes_json_mode(self):
        """When json_mode=True, response_format must be set."""
        with patch("tools.groq_backend.Groq") as mock_groq_cls:
            mock_client = MagicMock()
            mock_groq_cls.return_value = mock_client
            mock_client.chat.completions.create.return_value = _make_mock_response()

            backend = GroqBackend(api_key="gsk_test")
            backend.chat(system="sys", user="user", json_mode=True)

            call_kwargs = mock_client.chat.completions.create.call_args[1]
            assert call_kwargs["response_format"] == {"type": "json_object"}

    def test_chat_no_json_mode(self):
        """When json_mode=False, response_format must not be set."""
        with patch("tools.groq_backend.Groq") as mock_groq_cls:
            mock_client = MagicMock()
            mock_groq_cls.return_value = mock_client
            mock_client.chat.completions.create.return_value = _make_mock_response()

            backend = GroqBackend(api_key="gsk_test")
            backend.chat(system="sys", user="user", json_mode=False)

            call_kwargs = mock_client.chat.completions.create.call_args[1]
            assert "response_format" not in call_kwargs

    def test_chat_sends_correct_messages(self):
        """Messages must include system and user roles."""
        with patch("tools.groq_backend.Groq") as mock_groq_cls:
            mock_client = MagicMock()
            mock_groq_cls.return_value = mock_client
            mock_client.chat.completions.create.return_value = _make_mock_response()

            backend = GroqBackend(api_key="gsk_test")
            backend.chat(system="You are a SQL assistant.", user="Show me clicks")

            call_kwargs = mock_client.chat.completions.create.call_args[1]
            messages = call_kwargs["messages"]
            assert messages[0] == {"role": "system", "content": "You are a SQL assistant."}
            assert messages[1] == {"role": "user", "content": "Show me clicks"}


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestGroqBackendErrors:
    """Tests for error handling and GroqError exceptions."""

    def test_missing_api_key_raises_groq_error(self):
        """Empty API key must raise GroqError immediately."""
        with patch("tools.groq_backend.Groq"):
            with pytest.raises(GroqError, match="GROQ_API_KEY"):
                GroqBackend(api_key="")

    def test_groq_error_is_llm_backend_error(self):
        """GroqError must inherit from LLMBackendError for consistent API error handling."""
        from tools.exceptions import LLMBackendError
        assert issubclass(GroqError, LLMBackendError)

    def test_auth_error_raises_groq_error(self):
        """Groq AuthenticationError must be converted to GroqError."""
        from groq import AuthenticationError

        with patch("tools.groq_backend.Groq") as mock_groq_cls:
            mock_client = MagicMock()
            mock_groq_cls.return_value = mock_client
            mock_client.chat.completions.create.side_effect = AuthenticationError(
                "Invalid API key", response=MagicMock(status_code=401), body={}
            )

            backend = GroqBackend(api_key="gsk_bad_key")
            with pytest.raises(GroqError, match="authentication"):
                backend.chat(system="sys", user="user")

    def test_rate_limit_raises_groq_error(self):
        """Groq RateLimitError must be converted to GroqError with helpful message."""
        from groq import RateLimitError

        with patch("tools.groq_backend.Groq") as mock_groq_cls:
            mock_client = MagicMock()
            mock_groq_cls.return_value = mock_client
            mock_client.chat.completions.create.side_effect = RateLimitError(
                "Rate limit exceeded", response=MagicMock(status_code=429), body={}
            )

            backend = GroqBackend(api_key="gsk_test")
            with pytest.raises(GroqError, match="rate limit"):
                backend.chat(system="sys", user="user")


# ---------------------------------------------------------------------------
# is_available
# ---------------------------------------------------------------------------

class TestGroqBackendAvailability:
    """Tests for is_available() method."""

    def test_available_when_api_works(self):
        """is_available() should return True when models.list() succeeds."""
        with patch("tools.groq_backend.Groq") as mock_groq_cls:
            mock_client = MagicMock()
            mock_groq_cls.return_value = mock_client
            mock_client.models.list.return_value = []

            backend = GroqBackend(api_key="gsk_test")
            assert backend.is_available() is True

    def test_unavailable_when_api_fails(self):
        """is_available() should return False when models.list() raises."""
        with patch("tools.groq_backend.Groq") as mock_groq_cls:
            mock_client = MagicMock()
            mock_groq_cls.return_value = mock_client
            mock_client.models.list.side_effect = Exception("Connection failed")

            backend = GroqBackend(api_key="gsk_test")
            assert backend.is_available() is False


# ---------------------------------------------------------------------------
# build_llm_adapter integration
# ---------------------------------------------------------------------------

class TestBuildLlmAdapterGroq:
    """Tests for Groq auto-selection in build_llm_adapter()."""

    def test_groq_selected_when_provider_env_set(self):
        """build_llm_adapter() should use GroqBackend when NL_SQL_LLM_PROVIDER=groq."""
        import os
        from tools.llm_adapter import build_llm_adapter

        with patch("tools.groq_backend.Groq"), \
             patch.dict(os.environ, {"NL_SQL_LLM_PROVIDER": "groq", "GROQ_API_KEY": "gsk_test"}):

            adapter = build_llm_adapter()
            assert isinstance(adapter.ollama, GroqBackend)

    def test_ollama_selected_by_default(self):
        """build_llm_adapter() should use OllamaClient when provider is not groq."""
        import os
        from tools.llm_adapter import build_llm_adapter, OllamaClient

        with patch.dict(os.environ, {"NL_SQL_LLM_PROVIDER": "ollama"}):
            adapter = build_llm_adapter()
            assert isinstance(adapter.ollama, OllamaClient)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
