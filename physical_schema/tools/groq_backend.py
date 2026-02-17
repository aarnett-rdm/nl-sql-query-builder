"""
groq_backend.py

Groq cloud LLM backend implementing the LLMBackend Protocol.

Groq is a free-tier cloud service with very fast inference.
Free tier: 6,000 requests/day, 30 requests/minute.
Recommended models: llama-3.3-70b-versatile, llama-3.1-8b-instant

Usage:
    Set env vars:
        NL_SQL_LLM_PROVIDER=groq
        GROQ_API_KEY=gsk_...
        GROQ_MODEL=llama-3.3-70b-versatile   # optional, this is the default

    The build_llm_adapter() factory in llm_adapter.py will auto-instantiate this
    when NL_SQL_LLM_PROVIDER=groq.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

try:
    from groq import Groq
    from groq import AuthenticationError, RateLimitError, APIConnectionError, APIStatusError
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False

try:
    from tools.llm_backend import ChatResult
    from tools.exceptions import LLMBackendError
except ImportError:
    from llm_backend import ChatResult
    from exceptions import LLMBackendError

logger = logging.getLogger(__name__)


class GroqError(LLMBackendError):
    """Groq API communication failure (auth, rate limit, network, etc.)."""
    pass


class GroqBackend:
    """
    Groq cloud LLM backend implementing the LLMBackend Protocol.

    Uses the official `groq` Python SDK. Supports any Groq-hosted model.

    Free-tier limits (as of Feb 2026):
        - 6,000 requests / day
        - 30 requests / minute
        - 6,000 tokens / minute (varies by model)

    Recommended models for SQL generation:
        - llama-3.3-70b-versatile  (best quality, still free)
        - llama-3.1-8b-instant     (faster, slightly lower quality)
        - mixtral-8x7b-32768       (good for structured output)
    """

    DEFAULT_MODEL = "llama-3.3-70b-versatile"

    def __init__(self, api_key: str, model: Optional[str] = None):
        if not GROQ_AVAILABLE:
            raise GroqError(
                "groq package not installed. Run: pip install groq>=0.11.0"
            )
        if not api_key:
            raise GroqError(
                "GROQ_API_KEY is required. Get a free key at https://console.groq.com"
            )

        self._model = model or self.DEFAULT_MODEL
        try:
            self._client = Groq(api_key=api_key)
        except Exception as exc:
            raise GroqError(f"Failed to initialize Groq client: {exc}") from exc

        logger.info("GroqBackend initialized: model=%s", self._model)

    # ------------------------------------------------------------------
    # LLMBackend Protocol
    # ------------------------------------------------------------------

    @property
    def model_name(self) -> str:
        return self._model

    def is_available(self) -> bool:
        """Probe Groq by listing available models."""
        try:
            self._client.models.list()
            return True
        except Exception:
            return False

    def chat(
        self,
        system: str,
        user: str,
        json_mode: bool = True,
        temperature: float = 0.1,
    ) -> ChatResult:
        """
        Send a chat completion request to Groq.

        Args:
            system: System prompt (schema context, instructions, etc.)
            user: User question / NL query
            json_mode: If True, requests JSON output (Groq supports this natively)
            temperature: Sampling temperature (low = more deterministic)

        Returns:
            ChatResult with content, model, timing, and token counts

        Raises:
            GroqError: On any Groq API failure
        """
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]

        kwargs: dict = {
            "model": self._model,
            "messages": messages,
            "temperature": temperature,
        }

        # Groq supports JSON mode natively (like OpenAI)
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        start_ms = int(time.monotonic() * 1000)

        try:
            response = self._client.chat.completions.create(**kwargs)
        except AuthenticationError as exc:
            raise GroqError(
                "Groq authentication failed. Check your GROQ_API_KEY at https://console.groq.com"
            ) from exc
        except RateLimitError as exc:
            raise GroqError(
                "Groq rate limit hit. Free tier: 30 req/min, 6000 req/day. "
                "Wait a moment and try again."
            ) from exc
        except APIConnectionError as exc:
            raise GroqError(
                f"Cannot reach Groq API. Check your internet connection: {exc}"
            ) from exc
        except APIStatusError as exc:
            raise GroqError(f"Groq API error {exc.status_code}: {exc.message}") from exc
        except Exception as exc:
            raise GroqError(f"Unexpected Groq error: {exc}") from exc

        elapsed_ms = int(time.monotonic() * 1000) - start_ms

        content = response.choices[0].message.content or ""
        usage = response.usage

        logger.debug(
            "Groq response: model=%s tokens=%d/%d latency=%dms",
            response.model,
            usage.prompt_tokens,
            usage.completion_tokens,
            elapsed_ms,
        )

        return ChatResult(
            content=content,
            model=response.model,
            total_duration_ms=elapsed_ms,
            input_tokens=usage.prompt_tokens,
            output_tokens=usage.completion_tokens,
        )
