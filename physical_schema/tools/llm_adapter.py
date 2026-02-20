"""
llm_adapter.py

LLM integration layer: "LLM Plans, Python Generates"

The LLM converts natural language questions into structured Spec JSON.
The existing deterministic pipeline (spec_executor -> query_builder) generates SQL.
The LLM NEVER sees or generates SQL.

Supports:
  - Ollama (local, default)
  - Fallback to rule-based nl_to_spec.py when LLM is unavailable
  - Schema context retrieval via hybrid_retriever (BM25)
  - Spec validation against registry + schema
  - LLM-based column disambiguation
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from tools.exceptions import OllamaError, LLMBackendError
    from tools.llm_backend import LLMBackend, ChatResult
except ImportError:
    from exceptions import OllamaError, LLMBackendError
    from llm_backend import LLMBackend, ChatResult

logger = logging.getLogger("nl_sql_service.llm")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_PROMPTS_DIR = Path(__file__).resolve().parents[1] / "prompts"
_DEFAULT_OLLAMA_URL = "http://192.168.12.51:11434"
_DEFAULT_MODEL = "qwen3:14b"
_DEFAULT_TIMEOUT = 300  # seconds (34B models need time to load on first call)


def _env(key: str, default: str) -> str:
    return os.getenv(key, default)


# ---------------------------------------------------------------------------
# Schema context builder
# ---------------------------------------------------------------------------

class SchemaContext:
    """
    Builds a compact schema summary for injection into the LLM system prompt.
    Keeps token count manageable by summarizing rather than dumping raw schema.
    """

    def __init__(self, registry_path: str | Path, physical_schema_path: Optional[str | Path] = None):
        self._registry = json.loads(Path(registry_path).read_text(encoding="utf-8"))
        self._physical: Optional[Dict[str, Any]] = None
        if physical_schema_path and Path(physical_schema_path).exists():
            self._physical = json.loads(Path(physical_schema_path).read_text(encoding="utf-8"))

    @property
    def metric_names(self) -> List[str]:
        return sorted(self._registry.get("metrics", {}).keys())

    @property
    def metric_synonyms(self) -> Dict[str, str]:
        return self._registry.get("synonyms", {})

    @property
    def dimension_names(self) -> List[str]:
        return [
            "PST_Date",  # Date dimension for daily/time-based grouping
            "EventDate",  # Event performance date (EventDateTimeLocal)
            "CampaignName", "AccountName", "AdGroupName", "State",
            "Network", "Device", "CurrencyCode", "CampaignType", "CampaignStatus",
        ]

    def metric_names_str(self) -> str:
        return ", ".join(self.metric_names)

    def metric_synonyms_str(self) -> str:
        lines = []
        for alias, canonical in sorted(self.metric_synonyms.items()):
            if alias != canonical:
                lines.append(f'  "{alias}" → "{canonical}"')
        return "\n".join(lines) if lines else "(none)"

    def dimension_names_str(self) -> str:
        return ", ".join(self.dimension_names)

    def schema_context_str(self, retrieved_chunks: Optional[List[Dict[str, Any]]] = None) -> str:
        """
        Build the schema context block for the system prompt.
        If retrieved_chunks is provided (from hybrid_retriever), include them.
        Otherwise provide a minimal summary.
        """
        parts: List[str] = []

        # Always include metric + dimension summary
        parts.append(f"Available metrics: {self.metric_names_str()}")
        parts.append(f"Available dimensions: {self.dimension_names_str()}")
        parts.append(f"Platforms: google_ads, microsoft_ads")

        # Include retrieved schema chunks if available
        if retrieved_chunks:
            parts.append("\nRelevant schema details (retrieved from database schema):")
            for i, chunk in enumerate(retrieved_chunks[:8], 1):
                text = chunk.get("text", "")
                if len(text) > 400:
                    text = text[:400] + "..."
                parts.append(f"  [{i}] {text}")

        return "\n".join(parts)


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

class PromptBuilder:
    """Builds system and user prompts from templates + schema context."""

    def __init__(self, schema_ctx: SchemaContext, prompts_dir: Optional[Path] = None):
        self._ctx = schema_ctx
        self._dir = prompts_dir or _PROMPTS_DIR
        self._system_template = self._load("system_prompt.txt")
        self._few_shot = self._load_json("few_shot_examples.json")
        self._disambig_template = self._load("disambiguation_prompt.txt")

    def _load(self, name: str) -> str:
        p = self._dir / name
        if p.exists():
            return p.read_text(encoding="utf-8")
        logger.warning("Prompt template not found: %s", p)
        return ""

    def _load_json(self, name: str) -> List[Dict[str, Any]]:
        p = self._dir / name
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
        return []

    def build_system_prompt(self, retrieved_chunks: Optional[List[Dict[str, Any]]] = None) -> str:
        return self._system_template.format(
            metric_names=self._ctx.metric_names_str(),
            metric_synonyms=self._ctx.metric_synonyms_str(),
            dimension_names=self._ctx.dimension_names_str(),
            schema_context=self._ctx.schema_context_str(retrieved_chunks),
            current_date=datetime.date.today().isoformat(),
        )

    def build_user_prompt(
        self, question: str, previous_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Build user message with few-shot examples prepended.

        If previous_context is provided (keys: 'question', 'spec'), it is injected
        before the current question so the LLM can handle follow-up queries.
        """
        parts: List[str] = []

        # Few-shot examples (keep compact)
        if self._few_shot:
            parts.append("Here are examples of question → spec mappings:\n")
            for ex in self._few_shot[:8]:
                q = ex["question"]
                spec = json.dumps(ex["spec"], separators=(",", ":"))
                parts.append(f"Q: {q}\nA: {spec}\n")

        # Inject previous query context if available
        if previous_context:
            prev_q = previous_context.get("question", "")
            prev_spec = previous_context.get("spec", {})
            # Strip internal metadata fields to keep the context compact
            display_spec = {
                k: v for k, v in prev_spec.items()
                if k not in ("notes", "clarifications")
            }
            prev_spec_str = json.dumps(display_spec, separators=(",", ":"))
            parts.append(
                "[CONTEXT FROM PREVIOUS QUERY]\n"
                f'Previous question: "{prev_q}"\n'
                f"Previous spec: {prev_spec_str}\n\n"
                "FOLLOW-UP RULES:\n"
                "1. Use the previous spec as the BASE. Start with all its fields as defaults.\n"
                "2. Only modify fields the user explicitly mentions in the new question.\n"
                "3. Short modification phrases ('remove X', 'add filter', 'change date', 'exclude Y', "
                "'now show', 'same but') are follow-ups — ALWAYS inherit metrics, platform, grain, "
                "dimensions, and date from the previous spec unless the user changes them.\n"
                "4. NEVER add a clarification for metrics, platform, or date if they are already "
                "set in the previous spec.\n"
                "5. Only treat the question as completely new if it introduces an entirely different "
                "subject with no connection to the previous query.\n"
                "[END CONTEXT]\n"
            )

        parts.append(f"Now convert this question to a Spec JSON:\nQ: {question}\nA:")
        return "\n".join(parts)

    def build_disambiguation_prompt(
        self, question: str, column: str, candidates: List[str],
    ) -> str:
        candidate_lines = "\n".join(f"  - {c}" for c in candidates)
        return self._disambig_template.format(
            question=question,
            column=column,
            candidates=candidate_lines,
        )


# ---------------------------------------------------------------------------
# Ollama HTTP client
# ---------------------------------------------------------------------------

class OllamaClient:
    """Thin HTTP wrapper around the Ollama /api/chat endpoint.

    Implements the LLMBackend protocol.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        timeout: int = _DEFAULT_TIMEOUT,
    ):
        self.base_url = (base_url or _env("OLLAMA_URL", _DEFAULT_OLLAMA_URL)).rstrip("/")
        self.model = model or _env("OLLAMA_MODEL", _DEFAULT_MODEL)
        self.timeout = timeout

    @property
    def model_name(self) -> str:
        """LLMBackend protocol: human-readable model identifier."""
        return self.model

    def is_available(self) -> bool:
        """Check if Ollama server is reachable."""
        try:
            req = urllib.request.Request(f"{self.base_url}/api/tags", method="GET")
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status == 200
        except (urllib.error.URLError, OSError, TimeoutError):
            return False

    def chat(
        self,
        system: str,
        user: str,
        json_mode: bool = True,
        temperature: float = 0.1,
    ) -> ChatResult:
        """Send a chat completion request to Ollama.

        Returns ChatResult with response content, model name, and duration.
        Raises OllamaError on failure.
        """
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": 2048,
            },
        }
        if json_mode:
            payload["format"] = "json"

        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}/api/chat",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        t0 = time.time()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError as e:
            raise OllamaError(f"Ollama request failed: {e}") from e
        except TimeoutError:
            raise OllamaError(f"Ollama request timed out after {self.timeout}s")

        elapsed_ms = int((time.time() - t0) * 1000)

        content = ""
        msg = data.get("message", {})
        if isinstance(msg, dict):
            content = msg.get("content", "")

        return ChatResult(
            content=content,
            model=data.get("model", self.model),
            total_duration_ms=elapsed_ms,
            input_tokens=data.get("prompt_eval_count", 0) or 0,
            output_tokens=data.get("eval_count", 0) or 0,
        )


# ---------------------------------------------------------------------------
# Spec validation
# ---------------------------------------------------------------------------

def validate_spec(spec: Dict[str, Any], schema_ctx: SchemaContext) -> Tuple[bool, List[str]]:
    """
    Validate an LLM-generated spec against known metrics and dimensions.
    Returns (is_valid, list_of_warnings).
    """
    warnings: List[str] = []
    valid_metrics = set(schema_ctx.metric_names)
    valid_dims = set(schema_ctx.dimension_names)
    valid_platforms = {"google_ads", "microsoft_ads", None}

    # Check platform
    platform = spec.get("platform")
    if platform and platform not in valid_platforms:
        warnings.append(f"Unknown platform: '{platform}'")

    # Check metrics exist in registry
    for m in spec.get("metrics", []):
        if m not in valid_metrics:
            # Try synonym resolution
            synonyms = schema_ctx.metric_synonyms
            if m.lower() in synonyms:
                continue  # will be resolved by MetricRegistry.canonicalize
            warnings.append(f"Unknown metric: '{m}'")

    # Check dimensions
    for d in spec.get("dimensions", []):
        if d not in valid_dims:
            warnings.append(f"Unknown dimension: '{d}' (may still resolve at build time)")

    # Check date filter structure
    date_f = spec.get("filters", {}).get("date", {})
    if date_f:
        valid_date_keys = {"last_n_days", "yesterday", "mtd", "date_from", "date_to", "offset_days"}
        for k in date_f:
            if k not in valid_date_keys:
                warnings.append(f"Unknown date filter key: '{k}'")

    # Check where filter operators
    for wf in spec.get("filters", {}).get("where", []):
        op = wf.get("op", "=")
        if op not in ("=", "!=", ">", "<", ">=", "<=", "contains", "not_contains", "in", "not in"):
            warnings.append(f"Unknown where filter operator: '{op}'")

    is_valid = not any("Unknown metric" in w for w in warnings)
    return is_valid, warnings


def _clean_llm_json(raw: str) -> str:
    """
    Extract JSON from LLM response, handling common issues:
    - Markdown code fences
    - Leading/trailing text
    - BOM characters
    """
    text = raw.strip()

    # Remove markdown fences
    text = re.sub(r"^```(?:json)?\s*\n?", "", text)
    text = re.sub(r"\n?```\s*$", "", text)

    # Find first { and last }
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        text = text[start:end + 1]

    return text.strip()


def _ensure_spec_structure(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure the LLM output has all required Spec fields with correct types."""
    spec: Dict[str, Any] = {}
    spec["grain"] = raw.get("grain")
    spec["platform"] = raw.get("platform")
    spec["metrics"] = raw.get("metrics") if isinstance(raw.get("metrics"), list) else []
    spec["dimensions"] = raw.get("dimensions") if isinstance(raw.get("dimensions"), list) else []

    filters = raw.get("filters") if isinstance(raw.get("filters"), dict) else {}
    spec["filters"] = {
        "date": filters.get("date") if isinstance(filters.get("date"), dict) else {},
        "where": filters.get("where") if isinstance(filters.get("where"), list) else [],
    }
    # Campaign filters
    if isinstance(filters.get("campaign"), dict):
        spec["filters"]["campaign"] = filters["campaign"]
    if isinstance(filters.get("campaign_ids"), list):
        spec["filters"]["campaign_ids"] = filters["campaign_ids"]

    spec["compare"] = raw.get("compare")
    spec["post"] = raw.get("post") if isinstance(raw.get("post"), dict) else {}
    spec["clarifications"] = raw.get("clarifications") if isinstance(raw.get("clarifications"), list) else []
    spec["notes"] = raw.get("notes") if isinstance(raw.get("notes"), dict) else {}

    return spec


# ---------------------------------------------------------------------------
# Failover backend
# ---------------------------------------------------------------------------

class FailoverBackend:
    """Wraps two LLMBackend instances with automatic primary→fallback switching.

    On every chat() call, the primary backend is tried first.  If it raises
    LLMBackendError the call is transparently retried on the fallback backend.
    The `using_fallback` property reflects which backend served the last call.

    is_available() returns True if EITHER backend is reachable.
    """

    def __init__(self, primary: LLMBackend, fallback: LLMBackend) -> None:
        self._primary = primary
        self._fallback = fallback
        self._active: LLMBackend = primary
        self._using_fallback: bool = False

    # ------------------------------------------------------------------
    # LLMBackend protocol
    # ------------------------------------------------------------------

    @property
    def model_name(self) -> str:
        return self._active.model_name

    def is_available(self) -> bool:
        return self._primary.is_available() or self._fallback.is_available()

    def chat(
        self,
        system: str,
        user: str,
        json_mode: bool = True,
        temperature: float = 0.1,
    ) -> ChatResult:
        try:
            result = self._primary.chat(system, user, json_mode, temperature)
            self._active = self._primary
            self._using_fallback = False
            return result
        except LLMBackendError as primary_err:
            logger.warning(
                "Primary LLM backend '%s' failed — activating fallback '%s': %s",
                self._primary.model_name,
                self._fallback.model_name,
                primary_err,
            )
            try:
                result = self._fallback.chat(system, user, json_mode, temperature)
                self._active = self._fallback
                self._using_fallback = True
                return result
            except LLMBackendError as fallback_err:
                raise LLMBackendError(
                    f"Both backends failed. "
                    f"Primary '{self._primary.model_name}': {primary_err}. "
                    f"Fallback '{self._fallback.model_name}': {fallback_err}."
                ) from fallback_err

    # ------------------------------------------------------------------
    # Introspection helpers (not part of protocol)
    # ------------------------------------------------------------------

    @property
    def using_fallback(self) -> bool:
        """True if the last chat() call was served by the fallback backend."""
        return self._using_fallback

    @property
    def primary(self) -> LLMBackend:
        return self._primary

    @property
    def fallback(self) -> LLMBackend:
        return self._fallback


# ---------------------------------------------------------------------------
# Main adapter
# ---------------------------------------------------------------------------

class LLMAdapter:
    """
    Main entry point for LLM-powered NL → Spec conversion.

    Architecture:
      1. Build schema context from registry + optional retriever chunks
      2. Send question to Ollama with structured prompt
      3. Parse + validate response as Spec JSON
      4. Fall back to rule-based parser if LLM fails or is unavailable
    """

    def __init__(
        self,
        registry_path: str | Path,
        physical_schema_path: Optional[str | Path] = None,
        retriever: Any = None,
        ollama_url: Optional[str] = None,
        ollama_model: Optional[str] = None,
        ollama_timeout: int = _DEFAULT_TIMEOUT,
        prompts_dir: Optional[Path] = None,
        backend: Optional[LLMBackend] = None,
    ):
        self.registry_path = Path(registry_path)
        self.schema_ctx = SchemaContext(registry_path, physical_schema_path)
        self.prompt_builder = PromptBuilder(self.schema_ctx, prompts_dir)
        self.backend: LLMBackend = backend or OllamaClient(
            ollama_url, ollama_model, ollama_timeout
        )
        self._retriever = retriever  # Optional HybridRetriever instance
        self._last_chat_result: Optional[ChatResult] = None

    @property
    def ollama(self) -> LLMBackend:
        """Deprecated alias for self.backend. Kept for backward compatibility."""
        return self.backend

    def parse_nl_to_spec(
        self, question: str, previous_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Convert natural language to Spec JSON.

        Tries LLM first; falls back to rule-based parser on any failure.
        Returns a Spec dict ready for spec_executor.execute_spec().

        Args:
            question: Natural language question from the user.
            previous_context: Optional dict with keys 'question' and 'spec' from
                              the last successful query. Injected into the prompt so
                              the LLM can handle follow-up questions intelligently.
        """
        # Try LLM path
        if self.backend.is_available():
            try:
                spec = self._llm_parse(question, previous_context)
                spec["notes"] = spec.get("notes") or {}
                spec["notes"]["raw_user_text"] = question
                spec["notes"]["parser"] = "llm"
                spec["notes"]["model"] = self.backend.model_name
                spec["notes"]["input_tokens"] = getattr(
                    self._last_chat_result, "input_tokens", 0
                )
                spec["notes"]["output_tokens"] = getattr(
                    self._last_chat_result, "output_tokens", 0
                )
                spec["notes"]["latency_ms"] = getattr(
                    self._last_chat_result, "total_duration_ms", 0
                )
                return spec
            except Exception as e:
                logger.warning("LLM parsing failed, falling back to rule-based: %s", e)

        # Fallback to rule-based parser
        return self._rule_based_parse(question)

    def disambiguate(
        self, question: str, column: str, candidates: List[str],
    ) -> Optional[str]:
        """
        Use LLM to resolve column ambiguity.

        Returns the chosen table name, or None if LLM can't decide.
        """
        if not self.backend.is_available():
            return None

        try:
            prompt = self.prompt_builder.build_disambiguation_prompt(
                question, column, candidates,
            )
            result = self.backend.chat(
                system="You are a database schema expert. Respond with JSON only.",
                user=prompt,
                json_mode=True,
                temperature=0.0,
            )
            raw = _clean_llm_json(result.content)
            data = json.loads(raw)
            chosen = data.get("chosen_table")
            if chosen and chosen in candidates:
                logger.info(
                    "LLM disambiguated '%s' → '%s' (reason: %s)",
                    column, chosen, data.get("reason", "n/a"),
                )
                return chosen
        except Exception as e:
            logger.warning("LLM disambiguation failed: %s", e)

        return None

    def _llm_parse(
        self, question: str, previous_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Core LLM parsing logic."""

        # Retrieve relevant schema chunks if retriever is available
        retrieved_chunks = None
        if self._retriever is not None:
            try:
                result = self._retriever.retrieve(question)
                retrieved_chunks = result.get("retrieved_chunks", [])
                logger.info(
                    "Retrieved %d schema chunks (confidence: %s)",
                    len(retrieved_chunks),
                    result.get("retrieval_confidence", "unknown"),
                )
            except Exception as e:
                logger.warning("Schema retrieval failed: %s", e)

        # Build prompts
        system_prompt = self.prompt_builder.build_system_prompt(retrieved_chunks)
        user_prompt = self.prompt_builder.build_user_prompt(question, previous_context)

        # Call LLM backend
        t0 = time.time()
        result = self.backend.chat(
            system=system_prompt,
            user=user_prompt,
            json_mode=True,
            temperature=0.1,
        )
        self._last_chat_result = result
        elapsed_ms = int((time.time() - t0) * 1000)

        # Parse response
        raw_json = _clean_llm_json(result.content)
        if not raw_json:
            raise ValueError("LLM returned empty response")

        try:
            raw_spec = json.loads(raw_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"LLM returned invalid JSON: {e}\nRaw: {raw_json[:500]}")

        # Normalize structure
        spec = _ensure_spec_structure(raw_spec)

        # Validate against known schema
        is_valid, warnings = validate_spec(spec, self.schema_ctx)
        if warnings:
            logger.info("Spec validation warnings: %s", warnings)
            # Add non-fatal warnings to notes
            spec.setdefault("notes", {})["validation_warnings"] = warnings

        if not is_valid:
            raise ValueError(f"LLM spec has invalid metrics: {warnings}")

        logger.info(
            "LLM parse complete: model=%s, elapsed=%dms, metrics=%s, dims=%s",
            result.model, elapsed_ms, spec["metrics"], spec["dimensions"],
        )

        return spec

    def _rule_based_parse(self, question: str) -> Dict[str, Any]:
        """Fallback to the existing rule-based NL parser."""
        try:
            from tools.nl_to_spec import nl_to_spec
        except ImportError:
            from nl_to_spec import nl_to_spec

        spec = nl_to_spec(question, str(self.registry_path))
        spec.setdefault("notes", {})["parser"] = "rule_based"
        return spec


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------

def build_llm_adapter(
    registry_path: Optional[str | Path] = None,
    physical_schema_path: Optional[str | Path] = None,
    retriever_chunks_dir: Optional[str | Path] = None,
    ollama_url: Optional[str] = None,
    ollama_model: Optional[str] = None,
    backend: Optional[LLMBackend] = None,
) -> LLMAdapter:
    """
    Factory function to build a fully configured LLMAdapter.

    Uses project defaults if paths not provided.
    Optionally initializes the hybrid retriever for schema context.

    Backend selection (when `backend` param is None):
        - NL_SQL_LLM_PROVIDER=groq  → GroqBackend (requires GROQ_API_KEY)
        - NL_SQL_LLM_PROVIDER=ollama → OllamaClient (default)
    """
    project_root = Path(__file__).resolve().parents[1]

    if registry_path is None:
        registry_path = project_root / "current" / "metric_registry.json"
    if physical_schema_path is None:
        physical_schema_path = project_root / "current" / "physical_schema.json"

    # Build retriever if chunks directory is available
    retriever = None
    if retriever_chunks_dir:
        try:
            from tools.schema_retriever import build_default_hybrid_retriever
        except ImportError:
            try:
                from schema_retriever import build_default_hybrid_retriever
            except ImportError:
                build_default_hybrid_retriever = None

        if build_default_hybrid_retriever is not None:
            try:
                retriever = build_default_hybrid_retriever(str(retriever_chunks_dir))
                logger.info("Hybrid retriever initialized from %s", retriever_chunks_dir)
            except Exception as e:
                logger.warning("Failed to initialize retriever: %s", e)

    # Auto-select backend from env if not explicitly provided
    if backend is None:
        import os
        provider = os.getenv("NL_SQL_LLM_PROVIDER", "ollama").lower()
        fallback_provider = os.getenv("NL_SQL_LLM_FALLBACK", "").lower()

        def _make_groq() -> LLMBackend:
            try:
                from tools.groq_backend import GroqBackend
            except ImportError:
                from groq_backend import GroqBackend  # type: ignore[no-redef]
            api_key = os.getenv("GROQ_API_KEY", "")
            model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
            return GroqBackend(api_key=api_key, model=model)

        def _make_ollama() -> LLMBackend:
            url = ollama_url or os.getenv("OLLAMA_URL", _DEFAULT_OLLAMA_URL)
            model = ollama_model or os.getenv("OLLAMA_MODEL", _DEFAULT_MODEL)
            return OllamaClient(url, model)

        if provider == "groq":
            primary_backend: LLMBackend = _make_groq()
            logger.info("Using Groq as primary backend: model=%s", primary_backend.model_name)
        else:
            primary_backend = _make_ollama()
            logger.info("Using Ollama as primary backend: model=%s", primary_backend.model_name)

        if fallback_provider and fallback_provider != provider:
            if fallback_provider == "groq":
                fallback_backend: LLMBackend = _make_groq()
            else:
                fallback_backend = _make_ollama()
            backend = FailoverBackend(primary_backend, fallback_backend)
            logger.info(
                "Failover enabled: primary=%s fallback=%s",
                provider,
                fallback_provider,
            )
        else:
            backend = primary_backend

    return LLMAdapter(
        registry_path=registry_path,
        physical_schema_path=physical_schema_path,
        retriever=retriever,
        ollama_url=ollama_url,
        ollama_model=ollama_model,
        backend=backend,
    )


# ---------------------------------------------------------------------------
# CLI entrypoint (for testing)
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    import sys

    ap = argparse.ArgumentParser(description="NL → Spec via LLM")
    ap.add_argument("question", nargs="?", help="Natural language question")
    ap.add_argument("--registry", default=None, help="Path to metric_registry.json")
    ap.add_argument("--schema", default=None, help="Path to physical_schema.json")
    ap.add_argument("--chunks-dir", default=None, help="Path to semantic_chunks directory")
    ap.add_argument("--model", default=None, help="Ollama model name")
    ap.add_argument("--url", default=None, help="Ollama base URL")
    ap.add_argument("--check", action="store_true", help="Just check if Ollama is available")
    args = ap.parse_args()

    adapter = build_llm_adapter(
        registry_path=args.registry,
        physical_schema_path=args.schema,
        retriever_chunks_dir=args.chunks_dir,
        ollama_url=args.url,
        ollama_model=args.model,
    )

    if args.check:
        available = adapter.backend.is_available()
        print(json.dumps({
            "ollama_available": available,
            "model": adapter.backend.model_name,
            "url": getattr(adapter.backend, "base_url", "n/a"),
        }, indent=2))
        sys.exit(0 if available else 1)

    if not args.question:
        print("Usage: python llm_adapter.py 'your question here'", file=sys.stderr)
        sys.exit(1)

    spec = adapter.parse_nl_to_spec(args.question)
    print(json.dumps(spec, indent=2, default=str))


if __name__ == "__main__":
    main()
