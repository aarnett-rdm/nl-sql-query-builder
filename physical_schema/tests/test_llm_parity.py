"""
test_llm_parity.py

Priority 4.3: LLM vs rule-based parser parity tests.

Compares the LLM adapter output against the rule-based parser for the same
questions. Both should agree on: metrics, platform, and date filter presence.

These tests require a live Ollama server (192.168.12.51:11434).
They are automatically skipped when the server is unreachable.
"""

import pytest
import sqlglot

from tools.nl_to_spec import nl_to_spec
from tools.spec_executor import execute_spec

try:
    from tools.llm_adapter import LLMAdapter, build_llm_adapter
    LLM_IMPORT_OK = True
except ImportError:
    LLM_IMPORT_OK = False


# ===================================================================
# Fixtures
# ===================================================================

@pytest.fixture(scope="module")
def llm_adapter(metric_registry_path):
    """Build LLM adapter; skip entire module if LLM is unreachable."""
    if not LLM_IMPORT_OK:
        pytest.skip("llm_adapter not importable")

    adapter = build_llm_adapter(registry_path=metric_registry_path)
    if not adapter.ollama.is_available():
        pytest.skip("Ollama server not reachable")
    return adapter


# ===================================================================
# Parity questions (rule-based parser handles these correctly)
# ===================================================================

PARITY_QUESTIONS = [
    "What was our total spend yesterday?",
    "Show profit last 7 days",
    "Show clicks by campaign last 30 days",
    "Google ads impressions by account this month",
    "Microsoft ads cost by campaign yesterday",
    "What is our CTR by campaign for microsoft ads?",
    "Show conversion rate by campaign last 30 days",
    "Show clicks, cost, profit by campaign for google ads last 7 days",
]


# ===================================================================
# Parity tests
# ===================================================================

class TestLLMMetricParity:
    """LLM should recognize the same metrics as the rule-based parser."""

    @pytest.mark.parametrize("question", PARITY_QUESTIONS)
    def test_metrics_match(self, metric_registry_path, llm_adapter, question):
        rule_spec = nl_to_spec(question, metric_registry_path)
        llm_spec = llm_adapter.parse_nl_to_spec(question)

        rule_metrics = set(rule_spec["metrics"])
        llm_metrics = set(llm_spec["metrics"])

        # LLM must at least include all rule-based metrics (superset OK)
        missing = rule_metrics - llm_metrics
        assert not missing, (
            f"LLM missed metrics {missing} for: '{question}'\n"
            f"  Rule: {rule_metrics}\n  LLM:  {llm_metrics}"
        )


class TestLLMPlatformParity:
    """LLM should detect the same platform as the rule-based parser."""

    @pytest.mark.parametrize("question", PARITY_QUESTIONS)
    def test_platform_match(self, metric_registry_path, llm_adapter, question):
        rule_spec = nl_to_spec(question, metric_registry_path)
        llm_spec = llm_adapter.parse_nl_to_spec(question)

        assert rule_spec["platform"] == llm_spec["platform"], (
            f"Platform mismatch for: '{question}'\n"
            f"  Rule: {rule_spec['platform']}\n  LLM:  {llm_spec['platform']}"
        )


class TestLLMDateParity:
    """LLM should detect date filters when the rule-based parser does."""

    @pytest.mark.parametrize("question", PARITY_QUESTIONS)
    def test_date_presence_match(self, metric_registry_path, llm_adapter, question):
        rule_spec = nl_to_spec(question, metric_registry_path)
        llm_spec = llm_adapter.parse_nl_to_spec(question)

        rule_has_date = bool(rule_spec["filters"].get("date"))
        llm_has_date = bool(llm_spec["filters"].get("date"))

        if rule_has_date:
            assert llm_has_date, (
                f"LLM missed date filter for: '{question}'\n"
                f"  Rule date: {rule_spec['filters']['date']}\n"
                f"  LLM date:  {llm_spec['filters'].get('date')}"
            )


class TestLLMSpecExecutable:
    """LLM-generated specs should produce valid SQL through the pipeline."""

    @pytest.mark.parametrize("question", PARITY_QUESTIONS)
    def test_llm_spec_produces_valid_sql(self, metric_registry_path, llm_adapter, question):
        llm_spec = llm_adapter.parse_nl_to_spec(question)

        # Must have metrics to generate SQL
        if not llm_spec.get("metrics"):
            pytest.skip(f"LLM returned no metrics for: {question}")

        sql = execute_spec(llm_spec)
        assert sql and sql.strip()

        # Validate T-SQL syntax
        try:
            result = sqlglot.parse(sql, read="tsql")
            assert len(result) >= 1
        except sqlglot.errors.ParseError as e:
            pytest.fail(
                f"LLM spec produced invalid SQL for: '{question}'\n"
                f"SQL:\n{sql}\nError: {e}"
            )
