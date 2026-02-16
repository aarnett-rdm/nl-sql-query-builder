"""
Streamlit Chat UI for the NL SQL Query Builder.

Run:
    cd physical_schema
    python -m streamlit run ui/chat.py

Requires the FastAPI backend running on http://localhost:8000.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

# Ensure tools/ is importable when running from physical_schema/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from tools.fabric_conn import FabricConnection  # noqa: E402
from ui.shared import format_results, init_fabric_state, render_fabric_sidebar  # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_API_URL = "http://localhost:8000"

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

# Initialize Fabric connection state
init_fabric_state()


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


def post_query(question: str) -> dict:
    """POST /query with a natural-language question. Returns parsed JSON."""
    r = requests.post(
        f"{get_api_url()}/query",
        json={"question": question},
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

        # Status check — two-phase: fast healthz, then slower /ready for LLM info
        api_alive = check_api_alive()
        if not api_alive:
            st.error("API unreachable")
        else:
            st.success("API connected")
            status = check_llm_status()
            if status and status.get("ok"):
                llm = status.get("llm", {})
                if llm.get("backend_available"):
                    st.caption(f"LLM: {llm.get('model', 'unknown')}")
                else:
                    st.caption("Parser: rule-based (LLM unavailable)")
            else:
                st.caption("Parser: rule-based (status unknown)")

        st.divider()

        # Fabric Data Warehouse connection
        render_fabric_sidebar()

        st.divider()

        # SQL display preference
        st.toggle("Auto-expand SQL", value=False, key="auto_expand_sql")

        st.divider()

        # Clear chat button (keeps Fabric connection alive)
        if st.button("Clear chat", use_container_width=True):
            st.session_state.messages = []
            st.session_state.pending_spec = None
            st.session_state.pending_clars = None
            st.session_state.pending_question = None
            st.rerun()


# ---------------------------------------------------------------------------
# Chat rendering
# ---------------------------------------------------------------------------

def render_chat_history():
    """Render all past messages from session state."""
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
                st.dataframe(format_results(df), use_container_width=True)
                st.caption(f"Showing {len(df):,} row(s)")

            if msg.get("error_detail"):
                with st.expander("Error details"):
                    st.json(msg["error_detail"])


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


def append_assistant_message(content: str, sql: str | None = None, error_detail=None):
    msg = {"role": "assistant", "content": content}
    if sql:
        msg["sql"] = sql
    if error_detail:
        msg["error_detail"] = error_detail
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
        append_assistant_message(content, sql=sql)

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
