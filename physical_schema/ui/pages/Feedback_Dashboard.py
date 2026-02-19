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
import uuid as _uuid
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st

# Ensure tools/ is importable
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from tools.feedback_store import (  # noqa: E402
    CorrectionRecord,
    FeedbackLockedError,
    FeedbackStore,
    LOCK_TIMEOUT_SECS,
    get_feedback_path,
)
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

FEEDBACK_FILE = get_feedback_path()
RECOMMENDATIONS_FILE = FEEDBACK_FILE.parent / "RECOMMENDATIONS.md"
FEEDBACK_LOG_FILE = FEEDBACK_FILE.parent / "FEEDBACK_LOG.md"

# Stub spec used for manually-entered feedback (no real query ran)
_STUB_SPEC: dict = {
    "grain": None,
    "platform": None,
    "metrics": [],
    "dimensions": [],
    "filters": {"date": {}, "where": []},
    "compare": None,
    "post": {},
    "clarifications": [],
    "notes": {},
}

_CORRECTION_TYPE_MAP = {
    "Metric Gaps": "metric_mismatch",
    "Dimension Patterns": "dimension_wrong",
    "Date Filters": "date_filter_wrong",
    "Platform Aliases": "platform_wrong",
    "Wrong Filters": "filter_wrong",
    "Other / General observation": "other",
}


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

    # ── Manual feedback form (always visible) ────────────────────────────────
    with st.expander("➕ Submit Manual Feedback", expanded=False):
        st.caption(
            "Log a known issue or general observation directly — "
            "no query run required."
        )
        with st.form("manual_feedback_form", clear_on_submit=True):
            mf_question = st.text_area(
                "Describe the issue or question",
                placeholder=(
                    "E.g. 'When I ask for revenue by account it sometimes "
                    "groups by campaign instead'"
                ),
                height=80,
                key="mf_question",
            )
            mf_type_display = st.selectbox(
                "What category best describes the issue?",
                options=list(_CORRECTION_TYPE_MAP.keys()),
                key="mf_type",
            )
            mf_notes = st.text_area(
                "Additional notes / expected behaviour",
                placeholder="E.g. 'Should default to AccountName, not CampaignName'",
                height=80,
                key="mf_notes",
            )
            submitted = st.form_submit_button("Submit Feedback", type="primary")

        if submitted:
            if not mf_question.strip():
                st.warning("Please describe the issue before submitting.")
            else:
                record = CorrectionRecord(
                    feedback_id=str(_uuid.uuid4()),
                    timestamp=datetime.now().isoformat(),
                    request_id="manual",
                    original_question=mf_question.strip(),
                    original_spec=_STUB_SPEC,
                    corrected_spec=_STUB_SPEC,
                    correction_type=_CORRECTION_TYPE_MAP[mf_type_display],
                    notes=mf_notes.strip(),
                )
                try:
                    FeedbackStore(FEEDBACK_FILE).append(record)
                    st.success("✅ Feedback recorded!")
                    st.rerun()
                except FeedbackLockedError as exc:
                    remaining = max(0, LOCK_TIMEOUT_SECS - exc.age_secs)
                    st.warning(
                        f"⚠️ Another user is currently submitting feedback. "
                        f"Please wait about {remaining} seconds and try again."
                    )

    # Load feedback
    records = load_feedback()

    if not records:
        st.info("No feedback submitted yet. Users can submit feedback via the Query Builder or the form above.")
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

    tabs = st.tabs(["Metric Gaps", "Dimension Patterns", "Date Filters", "Platform Aliases", "Wrong Filters", "Other / General Observations"])

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

    with tabs[4]:
        filter_records = [r for r in records if r.correction_type == "filter_wrong"]
        if filter_records:
            st.caption(f"{len(filter_records)} filter issue(s) reported:")
            for r in reversed(filter_records[-10:]):
                with st.expander(f"[{r.timestamp[:16]}] {r.original_question[:80]}"):
                    if r.request_id != "manual":
                        orig_where = r.original_spec.get("filters", {}).get("where", [])
                        corr_where = r.corrected_spec.get("filters", {}).get("where", [])
                        if orig_where != corr_where:
                            col1, col2 = st.columns(2)
                            with col1:
                                st.caption("**Original filters:**")
                                st.json(orig_where)
                            with col2:
                                st.caption("**Corrected filters:**")
                                st.json(corr_where)
                    if r.notes:
                        st.info(f"**Notes:** {r.notes}")
                    elif r.request_id == "manual":
                        st.caption("*(Manual entry — see question description above)*")
        else:
            st.info("No filter issues reported yet.")

    with tabs[5]:
        other_records = [r for r in records if r.correction_type == "other"]
        if other_records:
            st.caption(f"{len(other_records)} general observation(s):")
            for r in reversed(other_records[-10:]):
                with st.expander(f"[{r.timestamp[:16]}] {r.original_question[:80]}"):
                    if r.notes:
                        st.info(f"**Notes:** {r.notes}")
                    else:
                        st.caption("No additional notes provided.")
                    if r.request_id == "manual":
                        st.caption("Source: manually entered via dashboard")
        else:
            st.info("No general observations yet.")

    st.divider()

    # Recent feedback
    st.subheader("Recent Feedback (Last 10)")

    for r in reversed(records[-10:]):
        with st.expander(f"[{r.timestamp[:16]}] {r.original_question}"):
            st.write(f"**Type:** {r.correction_type.replace('_', ' ').title()}")
            st.write(f"**Feedback ID:** {r.feedback_id[:8]}...")
            if r.request_id == "manual":
                st.caption("Source: manually entered via dashboard")

            col1, col2 = st.columns(2)

            with col1:
                if r.request_id != "manual":
                    st.caption("**What the system did:**")
                    st.json(r.original_spec)
                else:
                    st.caption("*(Manual entry — no query spec)*")

            with col2:
                if r.request_id != "manual" and r.corrected_spec != r.original_spec:
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
