"""
llm_backend.py

Abstract LLM backend interface (Protocol) and shared types.

Concrete backends (OllamaClient, future AzureOpenAIClient, etc.)
implement the LLMBackend protocol.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class ChatResult:
    """Structured return type from LLMBackend.chat().

    Attributes:
        content: The model's response text (typically JSON).
        model: Identifier for the model that generated the response.
        total_duration_ms: Wall-clock time for the request in milliseconds.
        input_tokens: Number of prompt/input tokens (0 if unavailable).
        output_tokens: Number of completion/output tokens (0 if unavailable).
    """

    content: str
    model: str
    total_duration_ms: int
    input_tokens: int = 0
    output_tokens: int = 0


@runtime_checkable
class LLMBackend(Protocol):
    """Protocol that all LLM backends must satisfy.

    Implementations: OllamaClient (tools/llm_adapter.py).
    Future: AzureOpenAIClient, AnthropicClient, etc.
    """

    @property
    def model_name(self) -> str:
        """Human-readable name of the active model."""
        ...

    def is_available(self) -> bool:
        """Return True if the backend is reachable and ready."""
        ...

    def chat(
        self,
        system: str,
        user: str,
        json_mode: bool = True,
        temperature: float = 0.1,
    ) -> ChatResult:
        """Send a chat completion request.

        Raises LLMBackendError (or subclass) on communication failure.
        """
        ...
