"""
Feedback Dashboard - View and analyze user feedback/corrections.

This page shows:
- Summary statistics
- Recent feedback
- Pattern analysis
- Links to generated markdown files
"""

from __future__ import annotations

import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st

# Ensure tools/ is importable
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from tools.feedback_store import FeedbackStore  # noqa: E402
from tools.feedback_analyzer import (  # noqa: E402
    find_metric_gaps,
    find_dimension_patterns,
    find_date_filter_gaps,
    find_platform_gaps,
    generate_recommendations,
    generate_feedback_log,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FEEDBACK_FILE = _PROJECT_ROOT / "feedback" / "corrections.jsonl"
RECOMMENDATIONS_FILE = _PROJECT_ROOT / "feedback" / "RECOMMENDATIONS.md"
FEEDBACK_LOG_FILE = _PROJECT_ROOT / "feedback" / "FEEDBACK_LOG.md"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def load_feedback():
    """Load all feedback records."""
    if not FEEDBACK_FILE.exists():
        return []
    store = FeedbackStore(FEEDBACK_FILE)
    return store.load_all()


def count_this_week(records):
    """Count records from the past 7 days."""
    now = datetime.now()
    week_ago = now - timedelta(days=7)
    return sum(1 for r in records if r.timestamp >= week_ago.isoformat())


def regenerate_markdown(records):
    """Regenerate both markdown files."""
    recommendations_md = generate_recommendations(records, min_count=1)
    feedback_log_md = generate_feedback_log(records, max_recent=50)

    RECOMMENDATIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RECOMMENDATIONS_FILE.write_text(recommendations_md, encoding="utf-8")
    FEEDBACK_LOG_FILE.write_text(feedback_log_md, encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="Feedback Dashboard",
        page_icon="📊",
        layout="wide",
    )

    st.title("📊 Feedback Dashboard")
    st.caption("Track user corrections and system improvements")

    # Load feedback
    records = load_feedback()

    if not records:
        st.info("No feedback submitted yet. Users can submit feedback via the Query Builder.")
        st.markdown(
            """
            **How to collect feedback:**
            1. Users interact with the Query Builder
            2. After each query, they can click 👍 or 👎
            3. If they click 👎, they can provide details about what was wrong
            4. Feedback is stored in `feedback/corrections.jsonl`
            5. This dashboard analyzes patterns and generates recommendations
            """
        )
        return

    # Summary metrics
    st.subheader("Summary Statistics")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Feedback", len(records))

    with col2:
        week_count = count_this_week(records)
        st.metric("This Week", week_count)

    with col3:
        type_counts = Counter(r.correction_type for r in records)
        most_common_type = type_counts.most_common(1)[0][0] if type_counts else "N/A"
        st.metric("Most Common", most_common_type.replace("_", " ").title())

    with col4:
        timestamps = [r.timestamp for r in records]
        days_span = (datetime.fromisoformat(max(timestamps)) - datetime.fromisoformat(min(timestamps))).days
        st.metric("Days Tracked", max(1, days_span))

    st.divider()

    # Feedback breakdown
    st.subheader("Feedback Breakdown by Type")
    type_counts = Counter(r.correction_type for r in records)

    col1, col2 = st.columns([1, 2])

    with col1:
        for ct, cnt in type_counts.most_common():
            st.metric(ct.replace("_", " ").title(), cnt)

    with col2:
        # Bar chart of types
        import pandas as pd
        df_types = pd.DataFrame(
            [(ct.replace("_", " ").title(), cnt) for ct, cnt in type_counts.most_common()],
            columns=["Type", "Count"],
        )
        st.bar_chart(df_types.set_index("Type"))

    st.divider()

    # Top issues
    st.subheader("🔥 Top Issues Detected")

    tabs = st.tabs(["Metric Gaps", "Dimension Patterns", "Date Filters", "Platform Aliases"])

    with tabs[0]:
        metric_gaps = find_metric_gaps(records)
        if metric_gaps:
            st.caption("Missing synonyms - users expect these terms to work:")
            for g in metric_gaps[:10]:
                st.write(f"- **\"{g['original']}\"** should map to **{g['corrected']}** ({g['count']} occurrences)")
        else:
            st.info("No metric synonym gaps detected.")

    with tabs[1]:
        dim_patterns = find_dimension_patterns(records)
        if dim_patterns:
            st.caption("Users consistently prefer these tables for ambiguous columns:")
            for p in dim_patterns[:10]:
                st.write(f"- **{p['column']}** → prefer **{p['preferred_table']}** ({p['count']} occurrences)")
        else:
            st.info("No dimension disambiguation patterns detected.")

    with tabs[2]:
        date_gaps = find_date_filter_gaps(records)
        if date_gaps:
            st.caption("Date filter misinterpretations:")
            for g in date_gaps[:10]:
                st.write(f"- Parsed as **{g['parsed_as']}**, should be **{g['should_be']}** ({g['count']} occurrences)")
                if g['example_question']:
                    st.caption(f"  Example: \"{g['example_question']}\"")
        else:
            st.info("No date filter misinterpretations detected.")

    with tabs[3]:
        plat_gaps = find_platform_gaps(records)
        if plat_gaps:
            st.caption("Platform detection gaps:")
            for g in plat_gaps[:10]:
                parsed = g['parsed_as'] or "(none)"
                st.write(f"- Parsed as **{parsed}**, should be **{g['should_be']}** ({g['count']} occurrences)")
        else:
            st.info("No platform detection gaps found.")

    st.divider()

    # Recent feedback
    st.subheader("Recent Feedback (Last 10)")

    for r in reversed(records[-10:]):
        with st.expander(f"[{r.timestamp[:16]}] {r.original_question}"):
            st.write(f"**Type:** {r.correction_type.replace('_', ' ').title()}")
            st.write(f"**Feedback ID:** {r.feedback_id[:8]}...")

            col1, col2 = st.columns(2)

            with col1:
                st.caption("**What the system did:**")
                st.json(r.original_spec)

            with col2:
                if r.corrected_spec != r.original_spec:
                    st.caption("**What it should have been:**")
                    st.json(r.corrected_spec)

            if r.notes:
                st.info(f"**User notes:** {r.notes}")

    st.divider()

    # Actions
    st.subheader("📥 Export & Actions")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("🔄 Regenerate Markdown Files", use_container_width=True):
            with st.spinner("Generating markdown files..."):
                regenerate_markdown(records)
                st.success("✅ Generated RECOMMENDATIONS.md and FEEDBACK_LOG.md")

    with col2:
        if RECOMMENDATIONS_FILE.exists():
            st.download_button(
                label="📄 Download RECOMMENDATIONS.md",
                data=RECOMMENDATIONS_FILE.read_text(encoding="utf-8"),
                file_name="RECOMMENDATIONS.md",
                mime="text/markdown",
                use_container_width=True,
            )
        else:
            st.button("📄 Download RECOMMENDATIONS.md", disabled=True, use_container_width=True)

    with col3:
        if FEEDBACK_LOG_FILE.exists():
            st.download_button(
                label="📋 Download FEEDBACK_LOG.md",
                data=FEEDBACK_LOG_FILE.read_text(encoding="utf-8"),
                file_name="FEEDBACK_LOG.md",
                mime="text/markdown",
                use_container_width=True,
            )
        else:
            st.button("📋 Download FEEDBACK_LOG.md", disabled=True, use_container_width=True)

    st.divider()

    # Instructions
    with st.expander("ℹ️ How to use this feedback"):
        st.markdown(
            """
            ## Workflow

            1. **Review this dashboard** to see patterns and top issues
            2. **Regenerate markdown files** to get latest analysis
            3. **Download RECOMMENDATIONS.md** or **FEEDBACK_LOG.md**
            4. **Upload to Claude Code** for automated fixes
            5. **Claude reviews patterns** and implements improvements
            6. **Changes are pushed to GitHub** automatically
            7. **Users get improvements** via `git pull` or `start_app.bat`

            ## File Purposes

            - **FEEDBACK_LOG.md**: Raw feedback log for detailed review
            - **RECOMMENDATIONS.md**: Analyzed patterns with actionable fixes

            ## Tips

            - Review dashboard weekly to catch emerging issues
            - Upload RECOMMENDATIONS.md to Claude when you see high-frequency patterns
            - Check "This Week" metric to track improvement velocity
            - Use feedback to prioritize feature improvements
            """
        )


if __name__ == "__main__":
    main()
