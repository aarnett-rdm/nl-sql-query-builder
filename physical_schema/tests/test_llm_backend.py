"""Tests for the LLM backend abstraction (llm_backend.py + exception hierarchy)."""

import pytest

from tools.llm_backend import LLMBackend, ChatResult
from tools.exceptions import NLSQLError, LLMBackendError, OllamaError


class TestChatResult:
    def test_fields(self):
        r = ChatResult(content="hello", model="m", total_duration_ms=100)
        assert r.content == "hello"
        assert r.model == "m"
        assert r.total_duration_ms == 100

    def test_frozen(self):
        r = ChatResult(content="hello", model="m", total_duration_ms=100)
        with pytest.raises(AttributeError):
            r.content = "changed"

    def test_slots(self):
        assert hasattr(ChatResult, "__slots__")
        r = ChatResult(content="hello", model="m", total_duration_ms=100)
        with pytest.raises((AttributeError, TypeError)):
            r.extra_field = "nope"


class TestLLMBackendProtocol:
    def test_runtime_checkable_good(self):
        class Good:
            @property
            def model_name(self) -> str:
                return "x"

            def is_available(self) -> bool:
                return True

            def chat(self, system, user, json_mode=True, temperature=0.1):
                return ChatResult("", "x", 0)

        assert isinstance(Good(), LLMBackend)

    def test_incomplete_impl_not_backend(self):
        class Bad:
            pass

        assert not isinstance(Bad(), LLMBackend)

    def test_ollama_client_is_backend(self):
        from tools.llm_adapter import OllamaClient

        client = OllamaClient(base_url="http://localhost:99999")
        assert isinstance(client, LLMBackend)

    def test_ollama_model_name_property(self):
        from tools.llm_adapter import OllamaClient

        client = OllamaClient(model="test-model", base_url="http://localhost:99999")
        assert client.model_name == "test-model"
        # .model attribute still works too
        assert client.model == "test-model"


class TestExceptionHierarchy:
    def test_ollama_error_is_backend_error(self):
        assert issubclass(OllamaError, LLMBackendError)

    def test_backend_error_is_nlsql_error(self):
        assert issubclass(LLMBackendError, NLSQLError)

    def test_catch_backend_catches_ollama(self):
        with pytest.raises(LLMBackendError):
            raise OllamaError("test")

    def test_to_dict_works(self):
        e = LLMBackendError("timeout")
        d = e.to_dict()
        assert d["error_type"] == "LLMBackendError"
        assert d["message"] == "timeout"
