"""Tests for FailoverBackend (automatic primary → fallback switching)."""

import pytest

from tools.llm_backend import ChatResult
from tools.exceptions import LLMBackendError
from tools.llm_adapter import FailoverBackend


# ---------------------------------------------------------------------------
# Helpers — minimal stub backends
# ---------------------------------------------------------------------------

def _result(model: str = "stub") -> ChatResult:
    return ChatResult(content='{"metrics":["spend"]}', model=model, total_duration_ms=10)


class OKBackend:
    """Always succeeds."""

    def __init__(self, name: str = "ok-model"):
        self._name = name

    @property
    def model_name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        return True

    def chat(self, system, user, json_mode=True, temperature=0.1) -> ChatResult:
        return _result(self._name)


class FailingBackend:
    """Always raises LLMBackendError."""

    def __init__(self, name: str = "failing-model"):
        self._name = name

    @property
    def model_name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        return False

    def chat(self, system, user, json_mode=True, temperature=0.1) -> ChatResult:
        raise LLMBackendError(f"{self._name} is unavailable")


class OnceFailingBackend:
    """Fails on the first call, succeeds on subsequent ones."""

    def __init__(self, name: str = "flaky-model"):
        self._name = name
        self._calls = 0

    @property
    def model_name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        return True

    def chat(self, system, user, json_mode=True, temperature=0.1) -> ChatResult:
        self._calls += 1
        if self._calls == 1:
            raise LLMBackendError(f"{self._name} first-call failure")
        return _result(self._name)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFailoverBackendInit:
    def test_starts_on_primary(self):
        fb = FailoverBackend(OKBackend("primary"), OKBackend("fallback"))
        assert fb.model_name == "primary"
        assert fb.using_fallback is False

    def test_properties(self):
        p, f = OKBackend("p"), OKBackend("f")
        fb = FailoverBackend(p, f)
        assert fb.primary is p
        assert fb.fallback is f


class TestIsAvailable:
    def test_both_ok(self):
        fb = FailoverBackend(OKBackend(), OKBackend())
        assert fb.is_available() is True

    def test_primary_down_fallback_ok(self):
        fb = FailoverBackend(FailingBackend(), OKBackend())
        assert fb.is_available() is True

    def test_primary_ok_fallback_down(self):
        fb = FailoverBackend(OKBackend(), FailingBackend())
        assert fb.is_available() is True

    def test_both_down(self):
        fb = FailoverBackend(FailingBackend(), FailingBackend())
        assert fb.is_available() is False


class TestChatHappyPath:
    def test_uses_primary_when_ok(self):
        fb = FailoverBackend(OKBackend("primary"), OKBackend("fallback"))
        result = fb.chat("sys", "user")
        assert result.model == "primary"
        assert fb.using_fallback is False
        assert fb.model_name == "primary"

    def test_primary_fails_uses_fallback(self):
        fb = FailoverBackend(FailingBackend("primary"), OKBackend("fallback"))
        result = fb.chat("sys", "user")
        assert result.model == "fallback"
        assert fb.using_fallback is True
        assert fb.model_name == "fallback"

    def test_both_fail_raises(self):
        fb = FailoverBackend(FailingBackend("p"), FailingBackend("f"))
        with pytest.raises(LLMBackendError) as exc_info:
            fb.chat("sys", "user")
        msg = str(exc_info.value)
        assert "p" in msg
        assert "f" in msg

    def test_primary_recovers_after_failover(self):
        """After a transient failure, primary should be tried again on the next call."""
        primary = OnceFailingBackend("flaky-primary")
        fb = FailoverBackend(primary, OKBackend("fallback"))

        # First call: primary fails → fallback used
        r1 = fb.chat("sys", "user")
        assert r1.model == "fallback"
        assert fb.using_fallback is True

        # Second call: primary succeeds again
        r2 = fb.chat("sys", "user")
        assert r2.model == "flaky-primary"
        assert fb.using_fallback is False

    def test_fallback_state_resets_on_primary_success(self):
        primary = OnceFailingBackend("p")
        fb = FailoverBackend(primary, OKBackend("f"))

        fb.chat("sys", "user")  # fallback
        assert fb.using_fallback is True

        fb.chat("sys", "user")  # primary back
        assert fb.using_fallback is False
        assert fb.model_name == "p"


class TestPassThroughArgs:
    def test_args_forwarded(self):
        received = {}

        class RecordingBackend:
            model_name = "recorder"

            def is_available(self):
                return True

            def chat(self, system, user, json_mode=True, temperature=0.1):
                received["system"] = system
                received["user"] = user
                received["json_mode"] = json_mode
                received["temperature"] = temperature
                return _result()

        fb = FailoverBackend(RecordingBackend(), OKBackend())
        fb.chat("my-sys", "my-user", json_mode=False, temperature=0.5)
        assert received["system"] == "my-sys"
        assert received["user"] == "my-user"
        assert received["json_mode"] is False
        assert received["temperature"] == 0.5
