"""
Tests for LLM adapter (llm_adapter.py).

Tests cover:
  - SchemaContext (metric/dimension extraction from registry)
  - PromptBuilder (template rendering)
  - Spec validation (_ensure_spec_structure, validate_spec)
  - JSON cleaning (_clean_llm_json)
  - Rule-based fallback path
  - Ollama integration (requires running Ollama server)
"""

import json
import pytest
from pathlib import Path

from tools.llm_adapter import (
    SchemaContext,
    PromptBuilder,
    OllamaClient,
    LLMAdapter,
    build_llm_adapter,
    validate_spec,
    _clean_llm_json,
    _ensure_spec_structure,
)
from tools.llm_backend import ChatResult, LLMBackend

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = PROJECT_ROOT / "current" / "metric_registry.json"
SCHEMA_PATH = PROJECT_ROOT / "current" / "physical_schema.json"
PROMPTS_DIR = PROJECT_ROOT / "prompts"


# ---------------------------------------------------------------------------
# SchemaContext tests
# ---------------------------------------------------------------------------

class TestSchemaContext:
    def test_metric_names_from_registry(self):
        ctx = SchemaContext(REGISTRY_PATH)
        names = ctx.metric_names
        assert "clicks" in names
        assert "impressions" in names
        assert "cost" in names
        assert "conversion rate" in names
        assert "profit" in names

    def test_metric_synonyms(self):
        ctx = SchemaContext(REGISTRY_PATH)
        syns = ctx.metric_synonyms
        assert "ctr" in syns
        assert "cpc" in syns
        assert "margin" in syns

    def test_dimension_names(self):
        ctx = SchemaContext(REGISTRY_PATH)
        dims = ctx.dimension_names
        assert "CampaignName" in dims
        assert "AccountName" in dims
        assert "State" in dims

    def test_schema_context_str_no_chunks(self):
        ctx = SchemaContext(REGISTRY_PATH)
        text = ctx.schema_context_str()
        assert "clicks" in text
        assert "CampaignName" in text
        assert "google_ads" in text

    def test_schema_context_str_with_chunks(self):
        ctx = SchemaContext(REGISTRY_PATH)
        fake_chunks = [
            {"text": "GoogleAdsCampaign table has CampaignName column"},
            {"text": "MicrosoftAdsAccount table has AccountName column"},
        ]
        text = ctx.schema_context_str(fake_chunks)
        assert "GoogleAdsCampaign" in text
        assert "Relevant schema details" in text


# ---------------------------------------------------------------------------
# PromptBuilder tests
# ---------------------------------------------------------------------------

class TestPromptBuilder:
    def test_system_prompt_renders(self):
        ctx = SchemaContext(REGISTRY_PATH)
        pb = PromptBuilder(ctx, PROMPTS_DIR)
        prompt = pb.build_system_prompt()
        assert "clicks" in prompt
        assert "CampaignName" in prompt
        assert "JSON" in prompt

    def test_user_prompt_includes_question(self):
        ctx = SchemaContext(REGISTRY_PATH)
        pb = PromptBuilder(ctx, PROMPTS_DIR)
        prompt = pb.build_user_prompt("show clicks by campaign last 7 days")
        assert "show clicks by campaign last 7 days" in prompt
        assert "Q:" in prompt

    def test_user_prompt_includes_few_shot(self):
        ctx = SchemaContext(REGISTRY_PATH)
        pb = PromptBuilder(ctx, PROMPTS_DIR)
        prompt = pb.build_user_prompt("test question")
        # Should contain few-shot examples
        assert "examples" in prompt.lower() or "Q:" in prompt

    def test_disambiguation_prompt(self):
        ctx = SchemaContext(REGISTRY_PATH)
        pb = PromptBuilder(ctx, PROMPTS_DIR)
        prompt = pb.build_disambiguation_prompt(
            "show revenue by campaign",
            "CampaignName",
            ["GoogleAdsCampaign", "MicrosoftAdsCampaign"],
        )
        assert "CampaignName" in prompt
        assert "GoogleAdsCampaign" in prompt


# ---------------------------------------------------------------------------
# JSON cleaning tests
# ---------------------------------------------------------------------------

class TestCleanLLMJson:
    def test_plain_json(self):
        raw = '{"metrics": ["clicks"]}'
        assert json.loads(_clean_llm_json(raw)) == {"metrics": ["clicks"]}

    def test_markdown_fenced(self):
        raw = '```json\n{"metrics": ["clicks"]}\n```'
        assert json.loads(_clean_llm_json(raw)) == {"metrics": ["clicks"]}

    def test_leading_text(self):
        raw = 'Here is the result:\n{"metrics": ["clicks"]}'
        assert json.loads(_clean_llm_json(raw)) == {"metrics": ["clicks"]}

    def test_trailing_text(self):
        raw = '{"metrics": ["clicks"]}\nLet me know if you need anything else.'
        assert json.loads(_clean_llm_json(raw)) == {"metrics": ["clicks"]}

    def test_whitespace(self):
        raw = '  \n  {"metrics": ["clicks"]}  \n  '
        assert json.loads(_clean_llm_json(raw)) == {"metrics": ["clicks"]}


# ---------------------------------------------------------------------------
# Spec structure normalization
# ---------------------------------------------------------------------------

class TestEnsureSpecStructure:
    def test_minimal_input(self):
        spec = _ensure_spec_structure({})
        assert spec["grain"] is None
        assert spec["platform"] is None
        assert spec["metrics"] == []
        assert spec["dimensions"] == []
        assert spec["filters"]["date"] == {}
        assert spec["filters"]["where"] == []
        assert spec["compare"] is None
        assert spec["clarifications"] == []

    def test_preserves_valid_fields(self):
        raw = {
            "grain": "campaign_calendar",
            "platform": "google_ads",
            "metrics": ["clicks", "cost"],
            "dimensions": ["CampaignName"],
            "filters": {
                "date": {"last_n_days": 7},
                "where": [{"field": "State", "op": "=", "value": "TX"}],
                "campaign": {"terms": ["spring"], "mode": "any"},
            },
            "compare": None,
            "clarifications": [],
        }
        spec = _ensure_spec_structure(raw)
        assert spec["metrics"] == ["clicks", "cost"]
        assert spec["dimensions"] == ["CampaignName"]
        assert spec["filters"]["date"] == {"last_n_days": 7}
        assert spec["filters"]["campaign"] == {"terms": ["spring"], "mode": "any"}

    def test_coerces_bad_types(self):
        raw = {
            "metrics": "clicks",         # string instead of list
            "dimensions": None,           # None instead of list
            "filters": "bad",             # string instead of dict
            "clarifications": "also bad",
        }
        spec = _ensure_spec_structure(raw)
        assert spec["metrics"] == []       # coerced
        assert spec["dimensions"] == []    # coerced
        assert spec["filters"]["date"] == {}
        assert spec["clarifications"] == []


# ---------------------------------------------------------------------------
# Spec validation
# ---------------------------------------------------------------------------

class TestValidateSpec:
    def test_valid_spec(self):
        ctx = SchemaContext(REGISTRY_PATH)
        spec = {
            "platform": "google_ads",
            "metrics": ["clicks", "cost"],
            "dimensions": ["CampaignName"],
            "filters": {"date": {"last_n_days": 7}, "where": []},
        }
        is_valid, warnings = validate_spec(spec, ctx)
        assert is_valid
        assert len(warnings) == 0

    def test_unknown_metric(self):
        ctx = SchemaContext(REGISTRY_PATH)
        spec = {
            "metrics": ["fake_metric"],
            "dimensions": [],
            "filters": {"date": {}, "where": []},
        }
        is_valid, warnings = validate_spec(spec, ctx)
        assert not is_valid
        assert any("Unknown metric" in w for w in warnings)

    def test_synonym_metric_is_valid(self):
        ctx = SchemaContext(REGISTRY_PATH)
        spec = {
            "metrics": ["ctr"],  # synonym for "click through rate"
            "dimensions": [],
            "filters": {"date": {}, "where": []},
        }
        is_valid, warnings = validate_spec(spec, ctx)
        # ctr is a synonym, should not fail
        assert is_valid

    def test_unknown_platform_warns(self):
        ctx = SchemaContext(REGISTRY_PATH)
        spec = {
            "platform": "facebook_ads",
            "metrics": ["clicks"],
            "dimensions": [],
            "filters": {"date": {}, "where": []},
        }
        _, warnings = validate_spec(spec, ctx)
        assert any("Unknown platform" in w for w in warnings)


# ---------------------------------------------------------------------------
# LLMAdapter rule-based fallback
# ---------------------------------------------------------------------------

class TestLLMAdapterFallback:
    def test_fallback_when_ollama_unavailable(self):
        adapter = LLMAdapter(
            registry_path=REGISTRY_PATH,
            physical_schema_path=SCHEMA_PATH,
            ollama_url="http://localhost:99999",  # unreachable
        )
        spec = adapter.parse_nl_to_spec("show clicks by campaign last 7 days")
        assert spec["notes"]["parser"] == "rule_based"
        assert "clicks" in spec["metrics"]

    def test_fallback_preserves_metrics(self):
        adapter = LLMAdapter(
            registry_path=REGISTRY_PATH,
            physical_schema_path=SCHEMA_PATH,
            ollama_url="http://localhost:99999",
        )
        spec = adapter.parse_nl_to_spec("show impressions and cost yesterday for google")
        assert "impressions" in spec["metrics"]
        assert "cost" in spec["metrics"]
        assert spec["platform"] == "google_ads"


# ---------------------------------------------------------------------------
# OllamaClient connectivity (live test - skipped if Ollama not running)
# ---------------------------------------------------------------------------

class TestOllamaClient:
    @pytest.fixture
    def client(self):
        return OllamaClient()

    def test_is_available(self, client):
        # This is a live test - it either passes or is informational
        result = client.is_available()
        # Just assert it returns a bool
        assert isinstance(result, bool)

    @pytest.mark.skipif(
        not OllamaClient().is_available(),
        reason="Ollama server not running",
    )
    def test_chat_returns_json(self, client):
        result = client.chat(
            system="You are a test assistant. Respond with JSON only.",
            user='Respond with: {"test": true}',
            json_mode=True,
            temperature=0.0,
        )
        assert isinstance(result, ChatResult)
        assert result.content
        data = json.loads(result.content)
        assert isinstance(data, dict)


# ---------------------------------------------------------------------------
# Full LLM integration test (live - skipped if Ollama not running)
# ---------------------------------------------------------------------------

class TestLLMIntegration:
    """Live LLM tests. Skipped when Ollama is not running.

    NOTE: The first call to a large model (e.g. 34B) may take several minutes
    while the model loads into GPU memory. Subsequent calls are much faster.
    """

    @pytest.fixture(scope="class")
    def adapter(self):
        """Shared adapter instance so model stays loaded across tests."""
        return build_llm_adapter(
            registry_path=REGISTRY_PATH,
            physical_schema_path=SCHEMA_PATH,
        )

    @pytest.mark.skipif(
        not OllamaClient().is_available(),
        reason="Ollama server not running",
    )
    @pytest.mark.timeout(360)
    def test_llm_parse_simple_question(self, adapter):
        spec = adapter.parse_nl_to_spec("show clicks by campaign last 7 days")
        assert spec["notes"]["parser"] == "llm"
        assert "clicks" in spec["metrics"]

    @pytest.mark.skipif(
        not OllamaClient().is_available(),
        reason="Ollama server not running",
    )
    @pytest.mark.timeout(360)
    def test_llm_parse_with_platform(self, adapter):
        spec = adapter.parse_nl_to_spec("google ads cost yesterday")
        assert spec["notes"]["parser"] == "llm"
        assert "cost" in spec["metrics"]
        assert spec.get("platform") in ("google_ads", None)


# ---------------------------------------------------------------------------
# Backend injection tests
# ---------------------------------------------------------------------------

class _FakeBackend:
    """Minimal LLMBackend implementation for testing injection."""

    def __init__(self, response_json: str = '{"metrics":["clicks"],"dimensions":[],"filters":{"date":{"last_n_days":7},"where":[]},"grain":null,"platform":null,"compare":null,"clarifications":[]}'):
        self._response = response_json

    @property
    def model_name(self) -> str:
        return "fake-test-model"

    def is_available(self) -> bool:
        return True

    def chat(self, system: str, user: str, json_mode: bool = True, temperature: float = 0.1) -> ChatResult:
        return ChatResult(content=self._response, model="fake-test-model", total_duration_ms=1)


class TestLLMAdapterBackendInjection:
    """Verify that LLMAdapter accepts an injected backend."""

    def test_injected_backend_used(self):
        fake = _FakeBackend()
        adapter = LLMAdapter(
            registry_path=REGISTRY_PATH,
            physical_schema_path=SCHEMA_PATH,
            backend=fake,
        )
        spec = adapter.parse_nl_to_spec("show clicks last 7 days")
        assert spec["notes"]["parser"] == "llm"
        assert spec["notes"]["model"] == "fake-test-model"
        assert "clicks" in spec["metrics"]

    def test_ollama_alias_returns_backend(self):
        fake = _FakeBackend()
        adapter = LLMAdapter(
            registry_path=REGISTRY_PATH,
            physical_schema_path=SCHEMA_PATH,
            backend=fake,
        )
        assert adapter.ollama is adapter.backend
        assert adapter.ollama is fake

    def test_factory_accepts_backend(self):
        fake = _FakeBackend()
        adapter = build_llm_adapter(
            registry_path=REGISTRY_PATH,
            physical_schema_path=SCHEMA_PATH,
            backend=fake,
        )
        assert adapter.backend is fake
        assert adapter.backend.model_name == "fake-test-model"
