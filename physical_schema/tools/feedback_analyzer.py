"""
feedback_analyzer.py

Analyzes accumulated user corrections and generates a RECOMMENDATIONS.md
file for Claude Code review.

Usage:
    python tools/feedback_analyzer.py [--input PATH] [--output PATH] [--min-count N]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from tools.eval_harness import _classify_date_filter, score_entry
from tools.feedback_store import (
    CorrectionRecord,
    DATE_FILTER_WRONG,
    DIMENSION_WRONG,
    FeedbackStore,
    METRIC_MISMATCH,
    PLATFORM_WRONG,
)


# ---------------------------------------------------------------------------
# Pattern analysis functions (pure, no I/O)
# ---------------------------------------------------------------------------


def find_metric_gaps(records: List[CorrectionRecord]) -> List[Dict[str, Any]]:
    """Identify missing metric synonyms from metric_mismatch corrections."""
    pairs: Counter = Counter()
    for r in records:
        if r.correction_type != METRIC_MISMATCH:
            continue
        orig = set(m.lower() for m in r.original_spec.get("metrics", []))
        corr = set(m.lower() for m in r.corrected_spec.get("metrics", []))
        # Terms in original that were replaced
        removed = orig - corr
        added = corr - orig
        for rm in removed:
            for ad in added:
                pairs[(rm, ad)] += 1

    return [
        {
            "original": pair[0],
            "corrected": pair[1],
            "count": count,
            "file": "current/metric_registry.json",
            "action": "add synonym",
        }
        for pair, count in pairs.most_common()
    ]


def find_dimension_patterns(records: List[CorrectionRecord]) -> List[Dict[str, Any]]:
    """Identify consistent table preferences for ambiguous columns."""
    # Track (column -> qualified_table.column) preferences
    prefs: Counter = Counter()
    for r in records:
        if r.correction_type != DIMENSION_WRONG:
            continue
        orig_dims = r.original_spec.get("dimensions", [])
        corr_dims = r.corrected_spec.get("dimensions", [])
        for dim in corr_dims:
            if "." in dim:
                # User qualified the column with a table
                table, col = dim.rsplit(".", 1)
                # Only count if the original had the unqualified column
                if col in orig_dims or dim not in orig_dims:
                    prefs[(col, table)] += 1

    return [
        {
            "column": pair[0],
            "preferred_table": pair[1],
            "count": count,
            "action": "update DIM_PREFERENCE in join_planner.py",
        }
        for pair, count in prefs.most_common()
    ]


def find_date_filter_gaps(records: List[CorrectionRecord]) -> List[Dict[str, Any]]:
    """Identify date filter misinterpretations."""
    mismatches: Counter = Counter()
    examples: Dict[tuple, str] = {}
    for r in records:
        if r.correction_type != DATE_FILTER_WRONG:
            continue
        orig_date = r.original_spec.get("filters", {}).get("date", {})
        corr_date = r.corrected_spec.get("filters", {}).get("date", {})
        orig_type = _classify_date_filter(orig_date)
        corr_type = _classify_date_filter(corr_date)
        if orig_type != corr_type:
            key = (orig_type, corr_type)
            mismatches[key] += 1
            if key not in examples:
                examples[key] = r.original_question

    return [
        {
            "parsed_as": pair[0],
            "should_be": pair[1],
            "count": count,
            "example_question": examples.get(pair, ""),
            "action": "update nl_to_spec.py date patterns",
        }
        for pair, count in mismatches.most_common()
    ]


def find_platform_gaps(records: List[CorrectionRecord]) -> List[Dict[str, Any]]:
    """Identify missing platform aliases."""
    mismatches: Counter = Counter()
    examples: Dict[tuple, str] = {}
    for r in records:
        if r.correction_type != PLATFORM_WRONG:
            continue
        orig_plat = r.original_spec.get("platform")
        corr_plat = r.corrected_spec.get("platform")
        if orig_plat != corr_plat:
            key = (orig_plat, corr_plat)
            mismatches[key] += 1
            if key not in examples:
                examples[key] = r.original_question

    return [
        {
            "parsed_as": pair[0],
            "should_be": pair[1],
            "count": count,
            "example_question": examples.get(pair, ""),
            "action": "add platform alias in nl_to_spec.py",
        }
        for pair, count in mismatches.most_common()
    ]


def find_few_shot_candidates(
    records: List[CorrectionRecord],
) -> List[Dict[str, Any]]:
    """Find corrections where the parser output was very different from correct."""
    candidates: List[Dict[str, Any]] = []
    for r in records:
        # Build an expected dict from the corrected spec
        expected = {
            "metrics": r.corrected_spec.get("metrics", []),
            "platform": r.corrected_spec.get("platform"),
            "dimensions": r.corrected_spec.get("dimensions", []),
            "grain": None,  # don't score grain
            "date_filter_type": _classify_date_filter(
                r.corrected_spec.get("filters", {}).get("date", {})
            ),
        }
        # Score the original spec against the corrected one
        scores = score_entry(expected, r.original_spec)
        if scores["overall"] < 0.5:
            candidates.append(
                {
                    "question": r.original_question,
                    "corrected_spec": r.corrected_spec,
                    "original_score": scores["overall"],
                    "action": "add to prompts/few_shot_examples.json",
                }
            )
    return candidates


# ---------------------------------------------------------------------------
# Markdown generator
# ---------------------------------------------------------------------------


def generate_recommendations(
    records: List[CorrectionRecord],
    min_count: int = 1,
) -> str:
    """Generate a RECOMMENDATIONS.md from correction patterns."""
    if not records:
        return (
            "# Feedback Recommendations\n\n"
            "No corrections recorded yet. Submit feedback via POST /feedback.\n"
        )

    lines: List[str] = ["# Feedback Recommendations\n"]

    # Summary
    type_counts: Counter = Counter(r.correction_type for r in records)
    timestamps = [r.timestamp for r in records]
    lines.append("## Summary\n")
    lines.append(f"- **Total corrections:** {len(records)}")
    lines.append(f"- **Date range:** {min(timestamps)[:10]} to {max(timestamps)[:10]}")
    lines.append("- **Breakdown by type:**")
    for ct, cnt in type_counts.most_common():
        lines.append(f"  - `{ct}`: {cnt}")
    lines.append("")

    # Metric Synonym Gaps
    metric_gaps = [g for g in find_metric_gaps(records) if g["count"] >= min_count]
    if metric_gaps:
        lines.append("## Metric Synonym Gaps\n")
        lines.append(
            "These terms were used by users but not recognized. "
            "Add them as synonyms in `current/metric_registry.json`.\n"
        )
        lines.append("| User Term | Should Map To | Count | Action |")
        lines.append("|-----------|--------------|-------|--------|")
        for g in metric_gaps:
            lines.append(
                f"| `{g['original']}` | `{g['corrected']}` | {g['count']} | Add synonym |"
            )
        lines.append("")

    # Dimension Disambiguation Patterns
    dim_patterns = [
        p for p in find_dimension_patterns(records) if p["count"] >= min_count
    ]
    if dim_patterns:
        lines.append("## Dimension Disambiguation Patterns\n")
        lines.append(
            "Users consistently prefer these tables for ambiguous columns. "
            "Consider updating `DIM_PREFERENCE` in `tools/join_planner.py`.\n"
        )
        lines.append("| Column | Preferred Table | Count | Action |")
        lines.append("|--------|----------------|-------|--------|")
        for p in dim_patterns:
            lines.append(
                f"| `{p['column']}` | `{p['preferred_table']}` | {p['count']} | Update DIM_PREFERENCE |"
            )
        lines.append("")

    # Date Filter Misinterpretations
    date_gaps = [
        g for g in find_date_filter_gaps(records) if g["count"] >= min_count
    ]
    if date_gaps:
        lines.append("## Date Filter Misinterpretations\n")
        lines.append(
            "The parser misclassifies these date patterns. "
            "Update date handling in `tools/nl_to_spec.py`.\n"
        )
        lines.append("| Parsed As | Should Be | Count | Example Question |")
        lines.append("|-----------|-----------|-------|-----------------|")
        for g in date_gaps:
            lines.append(
                f"| `{g['parsed_as']}` | `{g['should_be']}` | {g['count']} "
                f"| {g['example_question'][:60]} |"
            )
        lines.append("")

    # Platform Detection Gaps
    plat_gaps = [
        g for g in find_platform_gaps(records) if g["count"] >= min_count
    ]
    if plat_gaps:
        lines.append("## Platform Detection Gaps\n")
        lines.append(
            "These platform references are not being recognized correctly. "
            "Add aliases in `tools/nl_to_spec.py`.\n"
        )
        lines.append("| Parsed As | Should Be | Count | Example Question |")
        lines.append("|-----------|-----------|-------|-----------------|")
        for g in plat_gaps:
            parsed = g["parsed_as"] or "(none)"
            lines.append(
                f"| `{parsed}` | `{g['should_be']}` | {g['count']} "
                f"| {g['example_question'][:60]} |"
            )
        lines.append("")

    # Few-Shot Example Candidates
    fsc = find_few_shot_candidates(records)
    if fsc:
        lines.append("## Few-Shot Example Candidates\n")
        lines.append(
            "These corrections had very low parser accuracy (< 0.5). "
            "Consider adding them to `prompts/few_shot_examples.json`.\n"
        )
        for i, c in enumerate(fsc, 1):
            lines.append(f"### Candidate {i} (score: {c['original_score']:.2f})\n")
            lines.append(f"**Question:** {c['question']}\n")
            lines.append("**Corrected spec:**")
            lines.append(f"```json\n{json.dumps(c['corrected_spec'], indent=2)}\n```\n")

    return "\n".join(lines)


def generate_feedback_log(records: List[CorrectionRecord], max_recent: int = 50) -> str:
    """Generate a human-readable FEEDBACK_LOG.md for uploading to Claude Code."""
    if not records:
        return (
            "# Feedback Log\n\n"
            "No feedback submitted yet.\n"
        )

    lines: List[str] = ["# Feedback Log\n"]
    lines.append(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    # Summary stats
    type_counts: Counter = Counter(r.correction_type for r in records)
    timestamps = [r.timestamp for r in records]

    # Count this week
    now = datetime.now()
    week_ago = now - timedelta(days=7)
    week_count = sum(1 for r in records if r.timestamp >= week_ago.isoformat())

    lines.append("## Summary\n")
    lines.append(f"- **Total feedback:** {len(records)}")
    lines.append(f"- **This week:** {week_count}")
    lines.append(f"- **Date range:** {min(timestamps)[:10]} to {max(timestamps)[:10]}")
    lines.append(f"- **Most common:** {', '.join(f'{ct} ({cnt})' for ct, cnt in type_counts.most_common(3))}")
    lines.append("")

    # Recent feedback (newest first)
    lines.append(f"## Recent Feedback (Last {min(max_recent, len(records))})\n")

    for r in reversed(records[-max_recent:]):
        # Format correction type as title case
        corr_type_display = r.correction_type.replace("_", " ").title()

        lines.append(f"### [{r.timestamp[:16]}] {corr_type_display} - ID: {r.feedback_id[:8]}")
        lines.append(f"**User Question:** \"{r.original_question}\"")
        lines.append("")

        # What the system did
        lines.append("**What the system did:**")
        orig_spec = r.original_spec
        lines.append(f"- **Metrics:** {', '.join(orig_spec.get('metrics', [])) or '(none)'}")
        lines.append(f"- **Platform:** {orig_spec.get('platform', '(none)')}")
        lines.append(f"- **Dimensions:** {', '.join(orig_spec.get('dimensions', [])) or '(none)'}")
        date_filter = orig_spec.get("filters", {}).get("date", {})
        if date_filter:
            lines.append(f"- **Date:** {_format_date_filter(date_filter)}")
        lines.append("")

        # What it should have been (if different)
        corr_spec = r.corrected_spec
        if corr_spec != orig_spec:
            lines.append("**What it should have been:**")
            if corr_spec.get("metrics") != orig_spec.get("metrics"):
                lines.append(f"- **Metrics:** {', '.join(corr_spec.get('metrics', []))}")
            if corr_spec.get("platform") != orig_spec.get("platform"):
                lines.append(f"- **Platform:** {corr_spec.get('platform')}")
            if corr_spec.get("dimensions") != orig_spec.get("dimensions"):
                lines.append(f"- **Dimensions:** {', '.join(corr_spec.get('dimensions', []))}")
            corr_date = corr_spec.get("filters", {}).get("date", {})
            if corr_date != date_filter:
                lines.append(f"- **Date:** {_format_date_filter(corr_date)}")
            lines.append("")

        # User notes
        if r.notes:
            lines.append(f"**User Notes:** \"{r.notes}\"")
            lines.append("")

        lines.append("---\n")

    return "\n".join(lines)


def _format_date_filter(date_filter: Dict[str, Any]) -> str:
    """Format a date filter dict as a human-readable string."""
    if not date_filter:
        return "(none)"
    if "preset" in date_filter:
        return f"preset: {date_filter['preset']}"
    if "relative" in date_filter:
        rel = date_filter["relative"]
        return f"relative: {rel.get('offset', 0)} {rel.get('unit', 'days')}"
    if "start" in date_filter and "end" in date_filter:
        return f"{date_filter['start']} to {date_filter['end']}"
    return str(date_filter)


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Feedback Analyzer — generates RECOMMENDATIONS.md and FEEDBACK_LOG.md"
    )
    ap.add_argument(
        "--input",
        default=None,
        help="Path to corrections.jsonl (default: feedback/corrections.jsonl)",
    )
    ap.add_argument(
        "--output-dir",
        default=None,
        help="Directory for output files (default: feedback/)",
    )
    ap.add_argument(
        "--min-count",
        type=int,
        default=1,
        help="Minimum occurrences before recommending (default: 1)",
    )
    ap.add_argument(
        "--max-recent",
        type=int,
        default=50,
        help="Maximum recent feedback items in log (default: 50)",
    )
    args = ap.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    input_path = Path(args.input) if args.input else project_root / "feedback" / "corrections.jsonl"
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "feedback"

    if not input_path.exists():
        print(f"No corrections file found: {input_path}", file=sys.stderr)
        print("Submit feedback via POST /feedback first.", file=sys.stderr)
        sys.exit(1)

    store = FeedbackStore(input_path)
    records = store.load_all()

    print(f"Loaded {len(records)} corrections from {input_path}\n")

    # Generate RECOMMENDATIONS.md
    recommendations_md = generate_recommendations(records, min_count=args.min_count)
    recommendations_path = output_dir / "RECOMMENDATIONS.md"
    recommendations_path.parent.mkdir(parents=True, exist_ok=True)
    recommendations_path.write_text(recommendations_md, encoding="utf-8")
    print(f"✅ Recommendations written to {recommendations_path}")

    # Generate FEEDBACK_LOG.md
    feedback_log_md = generate_feedback_log(records, max_recent=args.max_recent)
    feedback_log_path = output_dir / "FEEDBACK_LOG.md"
    feedback_log_path.write_text(feedback_log_md, encoding="utf-8")
    print(f"✅ Feedback log written to {feedback_log_path}\n")

    # Print summary
    print("Summary:")
    type_counts = Counter(r.correction_type for r in records)
    for ct, cnt in type_counts.most_common():
        print(f"  {ct}: {cnt}")


if __name__ == "__main__":
    main()
