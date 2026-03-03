"""
Streamlit Chat UI for the NL SQL Query Builder.

Run:
    cd physical_schema
    python -m streamlit run ui/chat.py

Requires the FastAPI backend running on http://localhost:8000.
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

# Ensure tools/ is importable when running from physical_schema/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(_PROJECT_ROOT / ".env", override=False)
except ImportError:
    pass

from tools.fabric_conn import FabricConnection  # noqa: E402
from tools.query_history_store import QueryHistoryStore, QueryRecord  # noqa: E402
from ui.shared import format_results, build_totals_row, init_fabric_state, render_fabric_sidebar, sanitize_filename, build_excel_bytes  # noqa: E402
from ui.viz_utils import detect_visualization_opportunity, create_chart  # noqa: E402

# ---------------------------------------------------------------------------
# Query history store (module-level singleton)
# ---------------------------------------------------------------------------

_history_store = QueryHistoryStore(_PROJECT_ROOT / "history" / "queries.jsonl")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_API_URL = os.getenv("NL_SQL_API_URL", "http://localhost:8000")
CONTEXT_TIMEOUT_SECS = 600  # Auto-expire previous query context after 10 minutes

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

if "messages" not in st.session_state:
    st.session_state.messages = []
if "pending_spec" not in st.session_state:
    st.session_state.pending_spec = None
if "pending_clars" not in st.session_state:
    st.session_state.pending_clars = None
if "pending_question" not in st.session_state:
    st.session_state.pending_question = None

# Conversational context — stores last successful query for follow-up chaining
if "ctx_spec" not in st.session_state:
    st.session_state.ctx_spec = None
if "ctx_question" not in st.session_state:
    st.session_state.ctx_question = None
if "ctx_time" not in st.session_state:
    st.session_state.ctx_time = 0.0

# Initialize Fabric connection state
init_fabric_state()


# ---------------------------------------------------------------------------
# Conversational context helpers
# ---------------------------------------------------------------------------

def get_active_context() -> dict | None:
    """Return {question, spec} if a valid non-expired context exists, else None."""
    if st.session_state.get("ctx_spec") is None:
        return None
    elapsed = time.time() - st.session_state.get("ctx_time", 0.0)
    if elapsed > CONTEXT_TIMEOUT_SECS:
        st.session_state.ctx_spec = None
        st.session_state.ctx_question = None
        return None
    return {
        "question": st.session_state.ctx_question,
        "spec": st.session_state.ctx_spec,
    }


def save_context(question: str, spec: dict) -> None:
    """Persist the last successful query as context for the next request."""
    st.session_state.ctx_spec = spec
    st.session_state.ctx_question = question
    st.session_state.ctx_time = time.time()


def clear_context() -> None:
    """Discard the active query context."""
    st.session_state.ctx_spec = None
    st.session_state.ctx_question = None
    st.session_state.ctx_time = 0.0


# ---------------------------------------------------------------------------
# Query history helpers
# ---------------------------------------------------------------------------


def _save_to_history(
    question: str,
    sql: str,
    spec: dict,
    request_id: str,
    row_count: int | None = None,
) -> None:
    """Persist a successful query to the append-only history store."""
    try:
        record = QueryRecord(
            history_id=str(uuid.uuid4()),
            timestamp=datetime.now().isoformat(),
            request_id=request_id,
            user_question=question,
            spec=spec,
            sql=sql,
            platform=spec.get("platform", ""),
            metrics=spec.get("metrics", []),
            dimensions=spec.get("dimensions", []),
            grain=spec.get("grain", ""),
            row_count=row_count,
            parser_used=spec.get("notes", {}).get("parser"),
        )
        _history_store.append(record)
    except Exception:
        pass  # Never let history persistence break the main flow


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def get_api_url() -> str:
    return st.session_state.get("api_url", DEFAULT_API_URL).rstrip("/")


def check_api_alive() -> bool:
    """Fast liveness check via GET /healthz (does not contact Ollama)."""
    try:
        r = requests.get(f"{get_api_url()}/healthz", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


def check_llm_status() -> dict | None:
    """Call GET /ready for LLM details. May be slow when Ollama is down."""
    try:
        r = requests.get(f"{get_api_url()}/ready", timeout=8)
        return r.json()
    except Exception:
        return None


def get_providers_status() -> dict:
    """Fetch /providers with a 30-second session-state cache.

    Returns a dict with keys 'current_provider' and 'providers' (list).
    Falls back to an empty dict on error.
    """
    cache = st.session_state.get("_providers_cache", {})
    if cache.get("ts", 0) > time.time() - 30:
        return cache["data"]
    try:
        r = requests.get(f"{get_api_url()}/providers", timeout=12)
        data = r.json()
        st.session_state["_providers_cache"] = {"ts": time.time(), "data": data}
        return data
    except Exception:
        return {"current_provider": "unknown", "providers": []}


def post_query(question: str) -> dict:
    """POST /query with a natural-language question. Returns parsed JSON.

    Automatically includes the previous query context (if any) so the LLM
    can handle follow-up questions without the user repeating platform/date info.
    """
    payload: dict = {"question": question}
    ctx = get_active_context()
    if ctx:
        payload["previous_context"] = ctx
    r = requests.post(
        f"{get_api_url()}/query",
        json=payload,
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def post_continue(spec: dict, answers: dict) -> dict:
    """POST /query/continue with a spec and clarification answers."""
    r = requests.post(
        f"{get_api_url()}/query/continue",
        json={"spec": spec, "answers": answers},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def post_summarize(question: str, sql: str, results_json: list) -> str:
    """POST /summarize and return the plain-English summary string."""
    r = requests.post(
        f"{get_api_url()}/summarize",
        json={"question": question, "sql": sql, "results_json": results_json},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["summary"]


def post_suggest(question: str, spec: dict) -> list[str]:
    """POST /suggest and return a list of follow-up question strings."""
    r = requests.post(
        f"{get_api_url()}/suggest",
        json={"question": question, "spec": spec},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("suggestions", [])


def post_feedback(
    request_id: str,
    original_question: str,
    original_spec: dict,
    corrected_spec: dict,
    correction_type: str,
    notes: str = "",
) -> dict:
    """POST /feedback with user correction."""
    r = requests.post(
        f"{get_api_url()}/feedback",
        json={
            "request_id": request_id,
            "original_question": original_question,
            "original_spec": original_spec,
            "corrected_spec": corrected_spec,
            "correction_type": correction_type,
            "notes": notes,
        },
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


# Results formatting is now in ui/shared.py


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def render_sidebar():
    with st.sidebar:
        st.title("NL SQL Query Builder")
        st.divider()

        # API URL setting
        api_url = st.text_input("API URL", value=DEFAULT_API_URL, key="api_url")

        # Status check — fast healthz first
        api_alive = check_api_alive()
        if not api_alive:
            st.error("API unreachable")
        else:
            st.success("API connected")

            # --- LLM Provider selector ---
            providers_data = get_providers_status()
            providers = providers_data.get("providers", [])
            current = providers_data.get("current_provider", "")

            if providers:
                option_names = [p["name"] for p in providers]
                option_labels = [
                    (
                        f"{'✅' if p['available'] else '❌'} "
                        f"{p['label']}"
                        + ("" if p["configured"] else " ⚠ not configured")
                    )
                    for p in providers
                ]
                current_idx = option_names.index(current) if current in option_names else 0

                # No widget key — use index so we can reset it freely on error/switch
                selected_idx = st.selectbox(
                    "LLM Provider",
                    options=range(len(option_names)),
                    format_func=lambda i: option_labels[i],
                    index=current_idx,
                )
                selected = option_names[selected_idx]

                # Show model caption and failover status
                selected_info = providers[selected_idx]
                using_failover = providers_data.get("using_fallback", False)
                fallback_name = providers_data.get("fallback_provider", "")
                if selected_info["available"]:
                    st.caption(f"Model: {selected_info['model']}")
                else:
                    st.caption("LLM offline — fallback to rule-based parser")
                if using_failover and fallback_name:
                    st.warning(
                        f"⚠ Primary LLM down — using **{fallback_name}** as fallback",
                        icon=None,
                    )

                # If user picked a different provider, hot-swap it
                if selected != current:
                    switch_error = None
                    with st.spinner(f"Switching to {selected}…"):
                        try:
                            resp = requests.post(
                                f"{get_api_url()}/provider",
                                json={"provider": selected},
                                timeout=15,
                            )
                            resp.raise_for_status()
                        except Exception as exc:
                            switch_error = str(exc)

                    # Rerun/error outside spinner so it closes cleanly first
                    if switch_error:
                        st.error(f"Switch failed: {switch_error}")
                    else:
                        st.session_state["_providers_cache"] = {}
                        st.rerun()
            else:
                st.caption("LLM status unavailable")

        st.divider()

        # Fabric Data Warehouse connection
        render_fabric_sidebar()

        st.divider()

        # SQL display preference
        st.toggle("Auto-expand SQL", value=False, key="auto_expand_sql")

        st.divider()

        # Conversational context indicator
        ctx = get_active_context()
        if ctx:
            st.caption("🔗 Context active")
            prev_q = ctx["question"]
            display_q = f'"{prev_q[:55]}…"' if len(prev_q) > 55 else f'"{prev_q}"'
            st.markdown(f"*{display_q}*")
            if st.button("Clear context", key="clear_ctx_btn", use_container_width=True):
                clear_context()
                st.rerun()
            st.divider()

        # Clear chat button (keeps Fabric connection alive)
        if st.button("Clear chat", use_container_width=True):
            st.session_state.messages = []
            st.session_state.pending_spec = None
            st.session_state.pending_clars = None
            st.session_state.pending_question = None
            clear_context()
            st.rerun()


# ---------------------------------------------------------------------------
# Feedback UI
# ---------------------------------------------------------------------------

def render_feedback_ui(msg: dict, idx: int):
    """Render feedback buttons and correction form for a message with SQL."""
    # Skip if feedback already submitted
    if msg.get("feedback_submitted"):
        st.caption("✅ Thank you for your feedback!")
        return

    # Get the original question (look back in messages for user message)
    original_question = ""
    for i in range(idx - 1, -1, -1):
        if st.session_state.messages[i]["role"] == "user":
            original_question = st.session_state.messages[i]["content"]
            break

    st.divider()

    col1, col2, col3 = st.columns([1, 1, 8])
    with col1:
        thumbs_up = st.button("👍 Correct", key=f"thumbs_up_{idx}", use_container_width=True)
    with col2:
        thumbs_down = st.button("👎 Wrong", key=f"thumbs_down_{idx}", use_container_width=True)

    if thumbs_up:
        msg["feedback_submitted"] = True
        st.success("✅ Thanks! Glad it worked.")
        st.rerun()

    if thumbs_down or msg.get("show_feedback_form"):
        msg["show_feedback_form"] = True

        with st.expander("🔧 Help us improve", expanded=True):
            st.caption("Tell us what was wrong so we can fix it:")

            # Correction type selection
            correction_type_map = {
                "Wrong metrics": "metric_mismatch",
                "Wrong dimensions/columns": "dimension_wrong",
                "Wrong platform (Google/Microsoft)": "platform_wrong",
                "Wrong date range": "date_filter_wrong",
                "Wrong filters": "filter_wrong",
                "Other": "other",
            }

            correction_type_display = st.selectbox(
                "What was wrong?",
                options=list(correction_type_map.keys()),
                key=f"correction_type_{idx}",
            )
            correction_type = correction_type_map[correction_type_display]

            # Free-form notes
            notes = st.text_area(
                "What should it have been? (optional but helpful)",
                placeholder="E.g., 'Should use revenue metric instead of impressions' or 'Date should be last month not last week'",
                key=f"notes_{idx}",
                height=100,
            )

            # Submit button
            col_submit, col_cancel = st.columns([1, 1])
            with col_submit:
                if st.button("Submit Feedback", key=f"submit_feedback_{idx}", type="primary", use_container_width=True):
                    # Get spec and request_id from message (if available)
                    spec = msg.get("spec", {})
                    request_id = msg.get("request_id", "")

                    if not spec:
                        st.error("Cannot submit feedback: no spec available.")
                    else:
                        try:
                            # For now, corrected_spec is same as original (user provides notes)
                            # In future, could have UI to edit the spec
                            feedback_resp = post_feedback(
                                request_id=request_id,
                                original_question=original_question,
                                original_spec=spec,
                                corrected_spec=spec,  # TODO: allow editing
                                correction_type=correction_type,
                                notes=notes,
                            )
                            msg["feedback_submitted"] = True
                            msg["show_feedback_form"] = False
                            st.success(f"✅ Feedback submitted! ID: {feedback_resp.get('feedback_id', '')[:8]}")
                            st.caption("Your feedback will help improve the system. Thank you!")
                            st.rerun()
                        except requests.HTTPError as e:
                            if e.response is not None and e.response.status_code == 409:
                                detail = e.response.json().get("detail", "Another user is submitting feedback.")
                                st.warning(f"⚠️ {detail}")
                            else:
                                st.error(f"Failed to submit feedback: {e}")
                        except Exception as e:
                            st.error(f"Failed to submit feedback: {e}")

            with col_cancel:
                if st.button("Cancel", key=f"cancel_feedback_{idx}", use_container_width=True):
                    msg["show_feedback_form"] = False
                    st.rerun()


# ---------------------------------------------------------------------------
# Chat rendering
# ---------------------------------------------------------------------------

def _render_suggestions(msg: dict, idx: int, is_latest_assistant: bool) -> None:
    """Render follow-up suggestion pills for an assistant message with SQL.

    Auto-fetches suggestions from /suggest for the most recent message only
    (to avoid spamming the LLM on every re-render). Once fetched they are
    cached in msg["suggestions"] and shown as clickable buttons.
    """
    spec = msg.get("spec")
    if not spec:
        return

    # Retrieve the original user question for this result
    original_q = ""
    for i in range(idx - 1, -1, -1):
        if st.session_state.messages[i]["role"] == "user":
            original_q = st.session_state.messages[i]["content"]
            break
    if not original_q:
        return

    # Auto-fetch for the latest assistant message only
    if is_latest_assistant and msg.get("suggestions") is None and not msg.get("suggestions_fetching"):
        msg["suggestions_fetching"] = True
        try:
            suggestions = post_suggest(original_q, spec)
            msg["suggestions"] = suggestions
        except Exception:
            msg["suggestions"] = []  # Silently suppress — suggestions are non-critical
        msg["suggestions_fetching"] = False

    suggestions = msg.get("suggestions") or []
    if not suggestions:
        return

    st.markdown("**💡 Follow-up questions:**")
    cols = st.columns(len(suggestions))
    for j, suggestion in enumerate(suggestions):
        with cols[j]:
            if st.button(suggestion, key=f"suggest_btn_{idx}_{j}", use_container_width=True):
                st.session_state["prefill_question"] = suggestion
                st.rerun()


def render_chat_history():
    """Render all past messages from session state."""
    # Pre-compute the index of the last assistant message with SQL (for auto-suggest)
    last_assistant_idx = -1
    for i, m in enumerate(st.session_state.messages):
        if m.get("role") == "assistant" and m.get("sql"):
            last_assistant_idx = i

    for idx, msg in enumerate(st.session_state.messages):
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("sql"):
                expanded = st.session_state.get("auto_expand_sql", False)
                with st.expander("View Generated SQL", expanded=expanded):
                    edited_sql = st.text_area(
                        "SQL",
                        value=msg["sql"],
                        height=min(400, max(150, msg["sql"].count("\n") * 22)),
                        key=f"sql_{idx}",
                        label_visibility="collapsed",
                    )

                # Run Query button uses the (possibly edited) SQL
                render_run_query(msg, idx, edited_sql)

            # Show previously fetched results
            if msg.get("results") is not None:
                df = msg["results"]

                # --- AI Summary (above chart/table) ---
                if msg.get("sql"):
                    if msg.get("summary"):
                        with st.expander("✨ AI Summary", expanded=True):
                            st.markdown(msg["summary"])
                    elif not msg.get("summary_fetching"):
                        if st.button("✨ Summarize results", key=f"summarize_{idx}"):
                            # Retrieve the original user question for this result
                            original_q = ""
                            for i in range(idx - 1, -1, -1):
                                if st.session_state.messages[i]["role"] == "user":
                                    original_q = st.session_state.messages[i]["content"]
                                    break
                            msg["summary_fetching"] = True
                            with st.spinner("Generating AI summary…"):
                                try:
                                    summary = post_summarize(
                                        question=original_q,
                                        sql=msg["sql"],
                                        results_json=df.to_dict(orient="records"),
                                    )
                                    msg["summary"] = summary
                                except Exception as _e:
                                    msg["summary"] = f"⚠ Could not generate summary: {_e}"
                            msg["summary_fetching"] = False
                            st.rerun()

                # Detect visualization opportunity
                viz_info = detect_visualization_opportunity(df)

                # Show chart if suitable
                if viz_info["should_visualize"]:
                    # Initialize chart visibility state for this message
                    # NOTE: use a distinct key from button widget keys to avoid Streamlit collision
                    chart_key = f"chart_visible_{idx}"
                    if chart_key not in st.session_state:
                        st.session_state[chart_key] = True  # Auto-show by default

                    # Toggle button + reason caption
                    col1, col2 = st.columns([1, 4])
                    with col1:
                        if st.session_state[chart_key]:
                            if st.button("📊 Hide Chart", key=f"hide_chart_{idx}", use_container_width=True):
                                st.session_state[chart_key] = False
                                st.rerun()
                        else:
                            if st.button("📊 Show Chart", key=f"show_chart_btn_{idx}", use_container_width=True):
                                st.session_state[chart_key] = True
                                st.rerun()

                    with col2:
                        st.caption(f"💡 {viz_info['reason']}")

                    # Display chart if visible
                    if st.session_state[chart_key]:
                        chart_config = dict(viz_info["config"])  # mutable copy

                        # For horizontal bar charts, let the user pick one or more metrics
                        effective_chart_type = viz_info["chart_type"]
                        if viz_info["chart_type"] == "horizontal_bar":
                            numeric_cols = df.select_dtypes(include="number").columns.tolist()
                            if len(numeric_cols) > 1:
                                metric_state_key = f"chart_metrics_{idx}"
                                default_metric = chart_config.get("x_col", numeric_cols[0])
                                if metric_state_key not in st.session_state:
                                    st.session_state[metric_state_key] = [default_metric]

                                # Sanitise stored selection against current columns
                                valid = [
                                    m for m in st.session_state[metric_state_key]
                                    if m in numeric_cols
                                ]
                                if not valid:
                                    valid = [default_metric]

                                selected_metrics = st.multiselect(
                                    "Metrics to chart",
                                    options=numeric_cols,
                                    default=valid,
                                    key=f"chart_metric_sel_{idx}",
                                    label_visibility="collapsed",
                                )
                                if selected_metrics:
                                    st.session_state[metric_state_key] = selected_metrics
                                else:
                                    selected_metrics = valid  # keep last valid selection

                                if len(selected_metrics) == 1:
                                    chart_config["x_col"] = selected_metrics[0]
                                    chart_config["sort_by"] = selected_metrics[0]
                                else:
                                    effective_chart_type = "horizontal_bar_multi"
                                    chart_config["x_cols"] = selected_metrics

                        try:
                            fig = create_chart(df, effective_chart_type, chart_config)
                            st.plotly_chart(fig, use_container_width=True, key=f"chart_{idx}")
                        except Exception as e:
                            st.error(f"Chart generation failed: {e}")

                # Show totals row above the data table (when >1 rows with numeric data)
                totals_row = build_totals_row(df)
                if totals_row is not None:
                    st.markdown("**Totals**")
                    st.dataframe(format_results(totals_row), use_container_width=True, hide_index=True)

                # Show data table
                st.dataframe(format_results(df), use_container_width=True)
                st.caption(f"Showing {len(df):,} row(s)")

                # Download buttons
                _orig_q = ""
                for _i in range(idx - 1, -1, -1):
                    if st.session_state.messages[_i]["role"] == "user":
                        _orig_q = st.session_state.messages[_i]["content"]
                        break
                _today = datetime.now().strftime("%Y%m%d")
                _stem = sanitize_filename(_orig_q or "query")
                _dl_csv, _dl_xlsx, _ = st.columns([1, 1, 4])
                with _dl_csv:
                    st.download_button(
                        "⬇ CSV",
                        data=df.to_csv(index=False),
                        file_name=f"{_stem}_{_today}.csv",
                        mime="text/csv",
                        key=f"dl_csv_{idx}",
                        use_container_width=True,
                    )
                with _dl_xlsx:
                    st.download_button(
                        "⬇ Excel",
                        data=build_excel_bytes({"Results": df}),
                        file_name=f"{_stem}_{_today}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_xlsx_{idx}",
                        use_container_width=True,
                    )

            if msg.get("error_detail"):
                with st.expander("Error details"):
                    st.json(msg["error_detail"])

            # Follow-up suggestions (only for assistant messages with SQL)
            if msg["role"] == "assistant" and msg.get("sql"):
                _render_suggestions(msg, idx, is_latest_assistant=(idx == last_assistant_idx))

            # Feedback UI (only for assistant messages with SQL)
            if msg["role"] == "assistant" and msg.get("sql"):
                render_feedback_ui(msg, idx)


def render_run_query(msg: dict, idx: int, sql: str | None = None):
    """Show a Run Query button for a message with SQL, execute against Fabric."""
    if not st.session_state.fabric_connected:
        st.caption("Connect to Fabric in the sidebar to run queries.")
        return

    # Don't show button if results already fetched
    if msg.get("results") is not None:
        return

    run_sql = sql or msg["sql"]
    if st.button("Run Query", key=f"run_{idx}", type="secondary"):
        fc: FabricConnection = st.session_state.fabric_conn
        with st.spinner("Running query against Fabric..."):
            try:
                df = fc.execute(run_sql)
                msg["results"] = df
                st.rerun()
            except RuntimeError as e:
                # Connection lost
                st.session_state.fabric_connected = False
                st.error(f"Connection lost: {e}")
            except Exception as e:
                err_str = str(e)
                if "permission" in err_str.lower() or "denied" in err_str.lower():
                    st.error("Access denied. Check your Fabric permissions.")
                elif "timeout" in err_str.lower():
                    st.error("Query timed out. Try adding date filters to narrow the range.")
                else:
                    st.error(f"Query failed: {err_str}")


def append_user_message(text: str):
    st.session_state.messages.append({"role": "user", "content": text})


def append_assistant_message(
    content: str,
    sql: str | None = None,
    error_detail=None,
    spec: dict | None = None,
    request_id: str | None = None,
):
    msg = {"role": "assistant", "content": content}
    if sql:
        msg["sql"] = sql
    if error_detail:
        msg["error_detail"] = error_detail
    if spec:
        msg["spec"] = spec
    if request_id:
        msg["request_id"] = request_id
    st.session_state.messages.append(msg)


# ---------------------------------------------------------------------------
# Clarification UI
# ---------------------------------------------------------------------------

def render_clarification_form():
    """Render radio buttons for each pending clarification, plus a submit button."""
    clars = st.session_state.pending_clars
    if not clars:
        return

    st.info("The system needs more information to build your query.")

    answers: dict[str, str] = {}
    for i, clar in enumerate(clars):
        field = clar.get("field", f"field_{i}")
        question = clar.get("question") or clar.get("prompt", "Please choose:")
        choices = clar.get("choices") or clar.get("options", [])

        if choices:
            selected = st.radio(question, choices, key=f"clar_{i}")
            answers[field] = selected
        else:
            typed = st.text_input(question, key=f"clar_{i}")
            answers[field] = typed

    if st.button("Submit answers", type="primary"):
        spec = st.session_state.pending_spec
        st.session_state.pending_spec = None
        st.session_state.pending_clars = None

        # Log the clarification answers as a user message
        answers_text = ", ".join(f"{k}: {v}" for k, v in answers.items())
        append_user_message(f"(clarification) {answers_text}")

        with st.spinner("Generating query..."):
            try:
                data = post_continue(spec, answers)
                handle_query_response(data)
            except requests.HTTPError as e:
                handle_http_error(e)
            except Exception as e:
                append_assistant_message(
                    "Something went wrong. Please try again.",
                    error_detail={"error": str(e)},
                )
        st.rerun()


# ---------------------------------------------------------------------------
# Response handling
# ---------------------------------------------------------------------------

def handle_query_response(data: dict):
    """Process a QueryResponse from /query or /query/continue."""
    sql = data.get("sql")
    clars = data.get("clarifications") or []
    request_id = data.get("request_id", "")
    spec = data.get("spec", {})
    parser = spec.get("notes", {}).get("parser", "")

    if sql and not clars:
        # Success — we have SQL
        meta_parts = []
        if request_id:
            meta_parts.append(f"Request: `{request_id}`")
        if parser:
            meta_parts.append(f"Parser: {parser}")
        meta = "  \n".join(meta_parts)
        content = "Here's your query:" + (f"\n\n{meta}" if meta else "")
        append_assistant_message(content, sql=sql, spec=spec, request_id=request_id)

        # Save context for follow-up chaining: use the raw user question from messages
        user_question = ""
        for msg in reversed(st.session_state.messages):
            if msg["role"] == "user":
                user_question = msg["content"]
                break
        if user_question:
            save_context(user_question, spec)
            _save_to_history(user_question, sql, spec, request_id)

    elif clars:
        # Need clarification
        st.session_state.pending_spec = spec
        st.session_state.pending_clars = clars
        append_assistant_message("I need a bit more detail to build your query.")

    else:
        # Unexpected — no SQL and no clarifications
        append_assistant_message(
            "The server returned an unexpected response.",
            error_detail=data,
        )


def handle_http_error(exc: requests.HTTPError):
    """Convert HTTP errors into user-friendly assistant messages."""
    try:
        body = exc.response.json()
    except Exception:
        body = {"raw": exc.response.text}

    status = exc.response.status_code

    if status == 400:
        msg = body.get("message", body.get("detail", "Bad request."))
        append_assistant_message(f"Could not process your question: {msg}", error_detail=body)
    elif status == 409:
        # Clarifications needed (from /query/sql endpoint — shouldn't happen here)
        detail = body if isinstance(body, dict) else {}
        clars = detail.get("clarifications") or detail.get("detail", {}).get("clarifications", [])
        spec = detail.get("spec") or detail.get("detail", {}).get("spec", {})
        if clars and spec:
            st.session_state.pending_spec = spec
            st.session_state.pending_clars = clars
            append_assistant_message("I need a bit more detail to build your query.")
        else:
            append_assistant_message("Clarification needed but no options were returned.", error_detail=body)
    elif status == 422:
        append_assistant_message("The request was invalid. Please rephrase your question.", error_detail=body)
    elif status >= 500:
        append_assistant_message(
            "The server encountered an error. Please try again.",
            error_detail=body,
        )
    else:
        append_assistant_message(f"Unexpected error (HTTP {status}).", error_detail=body)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="Query Builder",
        page_icon="magnifying_glass_tilted_left",
        layout="wide",
    )

    render_sidebar()

    # Handle re-run from Query History: prefill_question set via session state
    if "prefill_question" in st.session_state and st.session_state.prefill_question:
        prefill = st.session_state.pop("prefill_question")
        st.info(f"Re-running: *{prefill}*")
        append_user_message(prefill)
        with st.spinner("Generating query..."):
            try:
                data = post_query(prefill)
                handle_query_response(data)
            except requests.HTTPError as e:
                handle_http_error(e)
            except Exception as e:
                append_assistant_message("Something went wrong.", error_detail={"error": str(e)})
        st.rerun()

    # Handle shareable URL: ?q=<encoded question>
    q_param = st.query_params.get("q", "")
    if q_param and not st.session_state.get("_q_param_handled"):
        st.session_state["_q_param_handled"] = True
        st.query_params.clear()
        append_user_message(q_param)
        with st.spinner("Generating query..."):
            try:
                data = post_query(q_param)
                handle_query_response(data)
            except requests.HTTPError as e:
                handle_http_error(e)
            except Exception as e:
                append_assistant_message("Something went wrong.", error_detail={"error": str(e)})
        st.rerun()

    render_chat_history()

    # Show clarification form if pending
    if st.session_state.pending_clars:
        render_clarification_form()
        return  # Don't show chat input while clarification is active

    # Show pre-populated metrics from Schema Explorer
    if "selected_metrics" in st.session_state and st.session_state.selected_metrics:
        metrics_list = ", ".join(st.session_state.selected_metrics)

        col1, col2 = st.columns([4, 1])
        with col1:
            st.info(f"**📊 Selected metrics from Schema Explorer:** {metrics_list}")
            st.caption("You can ask questions about these metrics, e.g., 'Show these metrics for last week'")
        with col2:
            if st.button("Clear Selection", key="clear_metrics"):
                st.session_state.selected_metrics = []
                st.rerun()

    # Chat input
    if prompt := st.chat_input("Ask a question about your marketing data..."):
        append_user_message(prompt)

        # Show the user message immediately
        with st.chat_message("user"):
            st.markdown(prompt)

        # Call the API
        with st.chat_message("assistant"):
            with st.spinner("Generating query..."):
                try:
                    data = post_query(prompt)
                    handle_query_response(data)
                except requests.HTTPError as e:
                    handle_http_error(e)
                except requests.ConnectionError:
                    append_assistant_message(
                        "Cannot reach the API server. Is it running?",
                        error_detail={"api_url": get_api_url()},
                    )
                except Exception as e:
                    append_assistant_message(
                        "Something went wrong. Please try again.",
                        error_detail={"error": str(e)},
                    )
        st.rerun()


if __name__ == "__main__":
    main()
