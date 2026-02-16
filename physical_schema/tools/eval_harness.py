"""
eval_harness.py

Evaluation framework for the NL-to-SQL parser pipeline.

Runs a golden dataset through both the rule-based and LLM parsers,
scores accuracy, and tracks token usage / cost per query.

Usage:
    python tools/eval_harness.py [--dataset PATH] [--tags basic,parity] [--output-dir evals/]
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("nl_sql_service.eval")

# ---------------------------------------------------------------------------
# Scoring helpers (pure functions, no I/O)
# ---------------------------------------------------------------------------


def _classify_date_filter(date_dict: Dict[str, Any]) -> str:
    """Map a spec's filters.date dict to a taxonomy string.

    Returns one of: 'yesterday', 'last_n_days', 'mtd', 'date_range', 'none'.
    """
    if not date_dict:
        return "none"
    if date_dict.get("yesterday"):
        return "yesterday"
    if date_dict.get("last_n_days"):
        return "last_n_days"
    if date_dict.get("mtd"):
        return "mtd"
    if date_dict.get("date_from") or date_dict.get("date_to"):
        return "date_range"
    return "none"


def _score_set(expected: List[str], actual: List[str]) -> Dict[str, float]:
    """Set-based precision / recall / F1 for metrics or dimensions."""
    exp = set(e.lower() for e in expected)
    act = set(a.lower() for a in actual)

    if not exp and not act:
        return {"precision": 1.0, "recall": 1.0, "f1": 1.0}
    if not act:
        return {"precision": 1.0 if not exp else 0.0, "recall": 0.0, "f1": 0.0}
    if not exp:
        return {"precision": 0.0, "recall": 1.0, "f1": 0.0}

    tp = len(exp & act)
    precision = tp / len(act)
    recall = tp / len(exp)
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
    return {"precision": round(precision, 4), "recall": round(recall, 4), "f1": round(f1, 4)}


def _score_exact(expected: Any, actual: Any) -> float:
    """Exact-match score. Returns 1.0 on match, 0.0 on mismatch.

    If expected is None, the field is not scored (returns 1.0).
    """
    if expected is None:
        return 1.0
    return 1.0 if expected == actual else 0.0


# Scoring weights — metrics matter most, grain least
_WEIGHTS = {
    "metrics": 0.40,
    "platform": 0.20,
    "dimensions": 0.15,
    "date_filter": 0.15,
    "grain": 0.10,
}


def score_entry(expected: Dict[str, Any], actual_spec: Dict[str, Any]) -> Dict[str, Any]:
    """Score a single eval entry against parser output.

    Returns a dict of per-field scores + weighted overall score.
    """
    scores: Dict[str, Any] = {}

    scores["metrics"] = _score_set(
        expected.get("metrics", []),
        actual_spec.get("metrics", []),
    )
    scores["platform"] = _score_exact(
        expected.get("platform"),
        actual_spec.get("platform"),
    )
    scores["dimensions"] = _score_set(
        expected.get("dimensions", []),
        actual_spec.get("dimensions", []),
    )
    scores["grain"] = _score_exact(
        expected.get("grain"),
        actual_spec.get("grain"),
    )

    # Date filter type
    expected_date_type = expected.get("date_filter_type")
    actual_date_type = _classify_date_filter(
        actual_spec.get("filters", {}).get("date", {})
    )
    scores["date_filter"] = _score_exact(expected_date_type, actual_date_type)

    # Weighted overall
    overall = sum(
        _WEIGHTS[k] * (scores[k]["f1"] if isinstance(scores[k], dict) else scores[k])
        for k in _WEIGHTS
    )
    scores["overall"] = round(overall, 4)

    return scores


# ---------------------------------------------------------------------------
# Eval orchestrator
# ---------------------------------------------------------------------------


def run_eval(
    dataset_path: Path,
    registry_path: Optional[Path] = None,
    physical_schema_path: Optional[Path] = None,
    ollama_url: Optional[str] = None,
    ollama_model: Optional[str] = None,
    cost_per_input_token: float = 0.0,
    cost_per_output_token: float = 0.0,
    tag_filter: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run the evaluation harness and return a complete results dict."""

    # Import parsers
    try:
        from tools.nl_to_spec import nl_to_spec
        from tools.llm_adapter import build_llm_adapter
    except ImportError:
        from nl_to_spec import nl_to_spec
        from llm_adapter import build_llm_adapter

    project_root = Path(__file__).resolve().parents[1]
    if registry_path is None:
        registry_path = project_root / "current" / "metric_registry.json"
    if physical_schema_path is None:
        physical_schema_path = project_root / "current" / "physical_schema.json"

    # Load dataset
    dataset = json.loads(dataset_path.read_text(encoding="utf-8"))
    entries = dataset.get("entries", [])

    # Filter by tags if requested
    if tag_filter:
        tag_set = set(tag_filter)
        entries = [e for e in entries if tag_set & set(e.get("tags", []))]

    # Build LLM adapter
    adapter = build_llm_adapter(
        registry_path=registry_path,
        physical_schema_path=physical_schema_path,
        ollama_url=ollama_url,
        ollama_model=ollama_model,
    )
    llm_available = adapter.backend.is_available()
    model_name = adapter.backend.model_name if llm_available else None

    results: List[Dict[str, Any]] = []

    for entry in entries:
        qid = entry["id"]
        question = entry["question"]
        expected = entry["expected"]
        result: Dict[str, Any] = {
            "id": qid,
            "question": question,
            "tags": entry.get("tags", []),
            "expected": expected,
        }

        # --- Rule-based parser ---
        try:
            t0 = time.time()
            rule_spec = nl_to_spec(question, str(registry_path))
            rule_ms = int((time.time() - t0) * 1000)
            result["rule_spec"] = rule_spec
            result["rule_timing_ms"] = rule_ms
            result["rule_score"] = score_entry(expected, rule_spec)
        except Exception as exc:
            result["rule_error"] = str(exc)
            result["rule_score"] = {"overall": 0.0}
            result["rule_timing_ms"] = 0

        # --- LLM parser ---
        if llm_available:
            try:
                t0 = time.time()
                llm_spec = adapter.parse_nl_to_spec(question)
                llm_ms = int((time.time() - t0) * 1000)
                notes = llm_spec.get("notes", {})

                result["llm_spec"] = llm_spec
                result["llm_timing_ms"] = llm_ms
                result["llm_score"] = score_entry(expected, llm_spec)
                result["llm_tokens"] = {
                    "input": notes.get("input_tokens", 0),
                    "output": notes.get("output_tokens", 0),
                }
                result["llm_cost_usd"] = (
                    result["llm_tokens"]["input"] * cost_per_input_token
                    + result["llm_tokens"]["output"] * cost_per_output_token
                )
            except Exception as exc:
                result["llm_error"] = str(exc)
                result["llm_score"] = {"overall": 0.0}
                result["llm_timing_ms"] = 0
                result["llm_tokens"] = {"input": 0, "output": 0}
                result["llm_cost_usd"] = 0.0

        results.append(result)

    # --- Compute summary ---
    summary: Dict[str, Any] = {}

    # Rule-based summary
    rb_scores = [r["rule_score"]["overall"] for r in results if "rule_score" in r]
    rb_metric_f1s = [
        r["rule_score"]["metrics"]["f1"]
        for r in results
        if isinstance(r.get("rule_score", {}).get("metrics"), dict)
    ]
    rb_latencies = [r.get("rule_timing_ms", 0) for r in results]
    summary["rule_based"] = {
        "avg_overall": round(sum(rb_scores) / len(rb_scores), 4) if rb_scores else 0.0,
        "avg_metrics_f1": round(sum(rb_metric_f1s) / len(rb_metric_f1s), 4) if rb_metric_f1s else 0.0,
        "avg_latency_ms": round(sum(rb_latencies) / len(rb_latencies), 1) if rb_latencies else 0.0,
        "entries_scored": len(rb_scores),
    }

    # LLM summary
    if llm_available:
        llm_scores = [r["llm_score"]["overall"] for r in results if "llm_score" in r]
        llm_metric_f1s = [
            r["llm_score"]["metrics"]["f1"]
            for r in results
            if isinstance(r.get("llm_score", {}).get("metrics"), dict)
        ]
        llm_latencies = [r.get("llm_timing_ms", 0) for r in results if "llm_score" in r]
        total_input = sum(r.get("llm_tokens", {}).get("input", 0) for r in results)
        total_output = sum(r.get("llm_tokens", {}).get("output", 0) for r in results)
        total_cost = sum(r.get("llm_cost_usd", 0.0) for r in results)

        summary["llm"] = {
            "avg_overall": round(sum(llm_scores) / len(llm_scores), 4) if llm_scores else 0.0,
            "avg_metrics_f1": round(sum(llm_metric_f1s) / len(llm_metric_f1s), 4) if llm_metric_f1s else 0.0,
            "avg_latency_ms": round(sum(llm_latencies) / len(llm_latencies), 1) if llm_latencies else 0.0,
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_tokens": total_input + total_output,
            "total_cost_usd": round(total_cost, 6),
            "entries_scored": len(llm_scores),
        }
    else:
        summary["llm"] = None

    return {
        "run_id": str(uuid.uuid4()),
        "timestamp": datetime.datetime.now().isoformat(),
        "model": model_name,
        "llm_available": llm_available,
        "dataset_path": str(dataset_path),
        "dataset_size": len(entries),
        "tag_filter": tag_filter,
        "summary": summary,
        "results": results,
    }


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(description="NL-to-SQL Evaluation Harness")
    ap.add_argument(
        "--dataset", default=None,
        help="Path to eval_dataset.json (default: evals/eval_dataset.json)",
    )
    ap.add_argument("--registry", default=None, help="Path to metric_registry.json")
    ap.add_argument("--schema", default=None, help="Path to physical_schema.json")
    ap.add_argument("--model", default=None, help="Ollama model name")
    ap.add_argument("--url", default=None, help="Ollama base URL")
    ap.add_argument(
        "--output-dir", default=None,
        help="Directory for output JSON (default: evals/)",
    )
    ap.add_argument(
        "--cost-input", type=float, default=0.0,
        help="Cost per input token in USD",
    )
    ap.add_argument(
        "--cost-output", type=float, default=0.0,
        help="Cost per output token in USD",
    )
    ap.add_argument(
        "--tags", default=None,
        help="Comma-separated tags to filter entries (e.g. 'parity,basic')",
    )
    args = ap.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    dataset_path = Path(args.dataset) if args.dataset else project_root / "evals" / "eval_dataset.json"
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "evals"
    registry_path = Path(args.registry) if args.registry else None
    schema_path = Path(args.schema) if args.schema else None

    if not dataset_path.exists():
        print(f"Error: dataset not found: {dataset_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading dataset: {dataset_path}")
    tag_filter = args.tags.split(",") if args.tags else None
    if tag_filter:
        print(f"Filtering by tags: {tag_filter}")

    result = run_eval(
        dataset_path=dataset_path,
        registry_path=registry_path,
        physical_schema_path=schema_path,
        ollama_url=args.url,
        ollama_model=args.model,
        cost_per_input_token=args.cost_input,
        cost_per_output_token=args.cost_output,
        tag_filter=tag_filter,
    )

    # Write output
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    output_path = output_dir / f"run_{timestamp}.json"
    output_path.write_text(
        json.dumps(result, indent=2, default=str),
        encoding="utf-8",
    )

    # Print summary
    summary = result["summary"]
    rb = summary.get("rule_based", {})
    llm = summary.get("llm")

    print(f"\nEval run complete: {output_path}")
    print(f"  Dataset size: {result['dataset_size']}")
    print(f"  Rule-based accuracy: {rb.get('avg_overall', 0):.3f}  (metrics F1: {rb.get('avg_metrics_f1', 0):.3f})")
    print(f"  Rule-based avg latency: {rb.get('avg_latency_ms', 0):.0f}ms")

    if llm:
        print(f"  LLM accuracy:     {llm.get('avg_overall', 0):.3f}  (metrics F1: {llm.get('avg_metrics_f1', 0):.3f})")
        print(f"  LLM avg latency:  {llm.get('avg_latency_ms', 0):.0f}ms")
        print(f"  LLM total tokens: {llm.get('total_tokens', 0)}")
        print(f"  LLM total cost:   ${llm.get('total_cost_usd', 0):.6f}")
    else:
        print("  LLM: unavailable (skipped)")


if __name__ == "__main__":
    main()
