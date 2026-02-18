"""
Query History & Favorites

Displays all past queries (from history/queries.jsonl) and bookmarked
favorites (from history/favorites.json) with search, filter, re-run,
upvote, and shareable URL generation.
"""

from __future__ import annotations

import base64
import json
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import List

import streamlit as st

# Ensure tools/ is importable when running from physical_schema/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from tools.query_history_store import QueryHistoryStore, QueryRecord  # noqa: E402
from tools.favorites_store import FavoritesStore, FavoriteRecord  # noqa: E402

# ---------------------------------------------------------------------------
# Storage paths
# ---------------------------------------------------------------------------

_HISTORY_PATH = _PROJECT_ROOT / "history" / "queries.jsonl"
_FAVORITES_PATH = _PROJECT_ROOT / "history" / "favorites.json"

_history_store = QueryHistoryStore(_HISTORY_PATH)
_favorites_store = FavoritesStore(_FAVORITES_PATH)

# ---------------------------------------------------------------------------
# QB page path (for st.switch_page and URL generation)
# ---------------------------------------------------------------------------

_QB_PAGE = "Query Builder.py"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ts_display(iso: str) -> str:
    """Format ISO timestamp as 'Feb 17, 2026 14:32'."""
    try:
        dt = datetime.fromisoformat(iso)
        return dt.strftime("%b %d, %Y %H:%M").replace(" 0", " ")
    except Exception:
        return iso[:16]


def _make_share_url(question: str) -> str:
    """Return a URL that opens QB with the question pre-filled."""
    encoded = urllib.parse.quote(question)
    # Streamlit multi-page app URL structure
    return f"?q={encoded}"


def _chips(items: List[str], color: str = "#1f77b4") -> str:
    """Render a list of items as inline HTML chips."""
    chips = " ".join(
        f'<span style="background:{color};color:white;padding:2px 8px;'
        f'border-radius:12px;font-size:0.78em;margin-right:4px">{i}</span>'
        for i in items
    )
    return chips


def _platform_icon(platform: str) -> str:
    icons = {"google_ads": "G", "microsoft_ads": "M", "all": "*"}
    return icons.get(platform, platform[:1].upper())


def _add_to_favorites(record: QueryRecord) -> None:
    """Star a history record into favorites."""
    import uuid
    fav = FavoriteRecord(
        favorite_id=str(uuid.uuid4()),
        history_id=record.history_id,
        created_at=datetime.now().isoformat(),
        user_question=record.user_question,
        spec=record.spec,
        sql=record.sql,
        platform=record.platform,
        metrics=record.metrics,
        dimensions=record.dimensions,
        grain=record.grain,
    )
    _favorites_store.append(fav)
    st.success("Added to favorites!")


# ---------------------------------------------------------------------------
# History tab
# ---------------------------------------------------------------------------


def render_history_tab(records: List[QueryRecord]) -> None:
    if not records:
        st.info("No query history yet. Ask a question in Query Builder to get started.")
        return

    # --- Search & filter controls ---
    col_search, col_platform, col_days, col_unique = st.columns([3, 1, 1, 1])
    with col_search:
        search = st.text_input("Search", placeholder="Filter by keyword…", label_visibility="collapsed")
    with col_platform:
        platforms = sorted({r.platform for r in records if r.platform})
        platform_filter = st.selectbox("Platform", ["All"] + platforms, label_visibility="collapsed")
    with col_days:
        days_filter = st.selectbox("Time range", ["All time", "Last 7 days", "Last 30 days", "Last 90 days"], label_visibility="collapsed")
    with col_unique:
        unique_only = st.toggle("Unique only", value=True, key="history_unique_toggle")

    # Apply filters
    filtered = list(reversed(records))  # Newest first
    if platform_filter != "All":
        filtered = [r for r in filtered if r.platform == platform_filter]
    if days_filter != "All time":
        day_map = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90}
        cutoff = (datetime.now()).timestamp() - day_map[days_filter] * 86400
        filtered = [r for r in filtered if datetime.fromisoformat(r.timestamp).timestamp() >= cutoff]
    if search:
        kw = search.lower()
        filtered = [
            r for r in filtered
            if kw in r.user_question.lower()
            or kw in r.sql.lower()
            or any(kw in m.lower() for m in r.metrics)
        ]

    # Deduplicate by question (keep most recent occurrence, already sorted newest-first)
    if unique_only:
        seen: set = set()
        deduped = []
        for r in filtered:
            key = r.user_question.strip().lower()
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        filtered = deduped

    st.caption(f"{len(filtered)} of {len(records)} queries")

    for record in filtered:
        already_fav = _favorites_store.contains(record.history_id)
        star = "⭐" if already_fav else "☆"
        header = f"{star} [{_ts_display(record.timestamp)}] {record.user_question}"
        with st.expander(header, expanded=False):
            # Metadata chips
            metric_html = _chips(record.metrics, "#1f77b4")
            dim_html = _chips(record.dimensions, "#2ca02c") if record.dimensions else ""
            st.markdown(
                f"**Platform:** `{record.platform}` &nbsp; **Grain:** `{record.grain}`<br>"
                f"**Metrics:** {metric_html}"
                + (f"<br>**Dimensions:** {dim_html}" if dim_html else ""),
                unsafe_allow_html=True,
            )
            if record.row_count is not None:
                st.caption(f"{record.row_count:,} rows returned")

            st.code(record.sql, language="sql")

            # Action buttons
            btn_col1, btn_col2, btn_col3, btn_col4 = st.columns(4)
            with btn_col1:
                if st.button("▶ Re-run", key=f"rerun_{record.history_id}"):
                    st.session_state["prefill_question"] = record.user_question
                    st.switch_page(_QB_PAGE)
            with btn_col2:
                if not already_fav:
                    if st.button("⭐ Favorite", key=f"fav_{record.history_id}"):
                        _add_to_favorites(record)
                        st.rerun()
                else:
                    st.caption("Already favorited")
            with btn_col3:
                if st.button("🔗 Share", key=f"share_{record.history_id}"):
                    st.session_state[f"show_share_{record.history_id}"] = True
            with btn_col4:
                if st.button("🗑 Delete", key=f"del_{record.history_id}"):
                    st.session_state[f"confirm_del_{record.history_id}"] = True

            # Confirm delete
            if st.session_state.get(f"confirm_del_{record.history_id}"):
                st.warning("Delete this entry from history?")
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Yes, delete", key=f"del_yes_{record.history_id}", type="primary"):
                        _history_store.delete(record.history_id)
                        st.session_state.pop(f"confirm_del_{record.history_id}", None)
                        st.rerun()
                with c2:
                    if st.button("Cancel", key=f"del_no_{record.history_id}"):
                        st.session_state.pop(f"confirm_del_{record.history_id}", None)
                        st.rerun()

            if st.session_state.get(f"show_share_{record.history_id}"):
                url = _make_share_url(record.user_question)
                st.info(
                    "Copy the query param below and append it to your Query Builder URL:\n\n"
                    f"`{url}`\n\n"
                    "Or copy the question directly:"
                )
                st.code(record.user_question, language="text")


# ---------------------------------------------------------------------------
# Favorites tab
# ---------------------------------------------------------------------------


def render_favorites_tab(favorites: List[FavoriteRecord]) -> None:
    if not favorites:
        st.info("No favorites yet. Star a query from the History tab.")
        return

    # Sort: most votes first; ties broken by newest first (two-pass stable sort)
    sorted_favs = sorted(favorites, key=lambda f: f.created_at, reverse=True)
    sorted_favs = sorted(sorted_favs, key=lambda f: f.votes, reverse=True)

    for fav in sorted_favs:
        display_name = fav.name if fav.name else fav.user_question[:70]
        header = f"⬆ {fav.votes} | {display_name}"
        with st.expander(header, expanded=False):
            # Editable metadata section
            edit_key = f"edit_{fav.favorite_id}"
            if st.session_state.get(edit_key):
                new_name = st.text_input("Name", value=fav.name, key=f"name_{fav.favorite_id}")
                new_desc = st.text_area("Description", value=fav.description, key=f"desc_{fav.favorite_id}", height=80)
                tags_raw = st.text_input(
                    "Tags (comma-separated)", value=", ".join(fav.tags), key=f"tags_{fav.favorite_id}"
                )
                save_col, cancel_col = st.columns(2)
                with save_col:
                    if st.button("Save", key=f"save_{fav.favorite_id}"):
                        new_tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
                        _favorites_store.update(fav.favorite_id, new_name, new_desc, new_tags)
                        st.session_state[edit_key] = False
                        st.rerun()
                with cancel_col:
                    if st.button("Cancel", key=f"cancel_{fav.favorite_id}"):
                        st.session_state[edit_key] = False
                        st.rerun()
            else:
                if fav.name:
                    st.markdown(f"**{fav.name}**")
                if fav.description:
                    st.caption(fav.description)
                if fav.tags:
                    tag_html = _chips(fav.tags, "#7f7f7f")
                    st.markdown(tag_html, unsafe_allow_html=True)

                metric_html = _chips(fav.metrics, "#1f77b4")
                st.markdown(
                    f"**Platform:** `{fav.platform}` &nbsp; **Grain:** `{fav.grain}`<br>"
                    f"**Metrics:** {metric_html}",
                    unsafe_allow_html=True,
                )
                st.markdown(f'*"{fav.user_question}"*')
                st.code(fav.sql, language="sql")

            # Action row
            a_col1, a_col2, a_col3, a_col4 = st.columns(4)
            with a_col1:
                if st.button(f"⬆ Upvote ({fav.votes})", key=f"upvote_{fav.favorite_id}"):
                    _favorites_store.upvote(fav.favorite_id)
                    st.rerun()
            with a_col2:
                if st.button("▶ Re-run", key=f"frerun_{fav.favorite_id}"):
                    st.session_state["prefill_question"] = fav.user_question
                    st.switch_page(_QB_PAGE)
            with a_col3:
                if st.button("✏ Edit", key=f"fedit_{fav.favorite_id}"):
                    st.session_state[f"edit_{fav.favorite_id}"] = True
                    st.rerun()
            with a_col4:
                if st.button("🗑 Remove", key=f"fdel_{fav.favorite_id}"):
                    _favorites_store.delete(fav.favorite_id)
                    st.rerun()

            # Share
            if st.button("🔗 Share URL", key=f"fshare_{fav.favorite_id}"):
                st.session_state[f"fshow_share_{fav.favorite_id}"] = True

            if st.session_state.get(f"fshow_share_{fav.favorite_id}"):
                url = _make_share_url(fav.user_question)
                st.info(
                    "Append this to your Query Builder URL:\n\n"
                    f"`{url}`"
                )
                st.code(fav.user_question, language="text")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    st.set_page_config(
        page_title="Query History",
        page_icon="📋",
        layout="wide",
    )
    st.title("📋 Query History")
    st.caption("Browse, re-run, bookmark, and share past queries.")

    # Summary row
    total = _history_store.count()
    fav_count = _favorites_store.count()
    m1, m2 = st.columns(2)
    with m1:
        st.metric("Total Queries", total)
    with m2:
        st.metric("Favorites", fav_count)

    st.divider()

    records = _history_store.load_all()
    favorites = _favorites_store.load_all()

    tab_history, tab_favorites = st.tabs([f"History ({len(records)})", f"⭐ Favorites ({len(favorites)})"])

    with tab_history:
        render_history_tab(records)

    with tab_favorites:
        render_favorites_tab(favorites)


if __name__ == "__main__":
    main()
