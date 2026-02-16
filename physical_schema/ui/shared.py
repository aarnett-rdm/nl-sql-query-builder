"""
Shared utilities for Streamlit UI pages.

Provides common functionality for:
- Results table formatting (currency, percentages, commas)
- Fabric Data Warehouse connection management
- Session state initialization
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from tools.fabric_conn import FabricConnection

# ---------------------------------------------------------------------------
# Results Formatting
# ---------------------------------------------------------------------------

# Column names (lowercased) that represent dollar amounts
_CURRENCY_COLS = {"cost", "profit", "revenue", "spend", "cpc", "roi", "exchange revenue"}
# Column names (lowercased) that represent rates / percentages
_RATE_COLS = {"ctr", "conversion rate", "click through rate"}


def format_results(df: pd.DataFrame):
    """
    Return a pandas Styler with currency, comma, and percentage formatting.

    Args:
        df: Raw DataFrame from query execution

    Returns:
        Styled DataFrame with formatted numeric columns, or raw df if no numeric columns
    """
    # Create a copy to avoid modifying the original
    df_display = df.copy()
    fmt: dict[str, str] = {}

    for col in df_display.columns:
        if not pd.api.types.is_numeric_dtype(df_display[col]):
            continue
        col_lower = col.lower().replace("_", " ").strip()
        if any(kw in col_lower for kw in _CURRENCY_COLS):
            fmt[col] = "${:,.2f}"
        elif any(kw in col_lower for kw in _RATE_COLS):
            # Multiply rate columns by 100 for percentage display
            df_display[col] = df_display[col] * 100
            fmt[col] = "{:,.2f}%"
        elif pd.api.types.is_integer_dtype(df_display[col]):
            fmt[col] = "{:,}"
        else:
            fmt[col] = "{:,.2f}"
    return df_display.style.format(fmt) if fmt else df_display


# ---------------------------------------------------------------------------
# Fabric Connection UI
# ---------------------------------------------------------------------------

def init_fabric_state():
    """Initialize Fabric connection session state keys if not present."""
    if "fabric_conn" not in st.session_state:
        st.session_state.fabric_conn = None
    if "fabric_connected" not in st.session_state:
        st.session_state.fabric_connected = False


def render_fabric_sidebar():
    """
    Render the Fabric Data Warehouse connection section in the sidebar.

    Shows current connection status and Connect/Disconnect buttons.
    Manages st.session_state.fabric_conn and st.session_state.fabric_connected.
    """
    st.subheader("Fabric Connection")
    fc: FabricConnection | None = st.session_state.fabric_conn

    # Show current connection status
    if st.session_state.fabric_connected and fc is not None:
        st.success("Fabric: connected")
        st.caption(f"Server: {fc.server[:30]}...")
        st.caption(f"Database: {fc.database}")
        if st.button("Disconnect", use_container_width=True):
            fc.close()
            st.session_state.fabric_connected = False
            st.rerun()
    else:
        st.warning("Fabric: not connected")
        if st.button("Connect to Fabric", type="primary", use_container_width=True):
            with st.spinner("Authenticating via Azure AD..."):
                try:
                    conn = FabricConnection()
                    conn.connect()
                    st.session_state.fabric_conn = conn
                    st.session_state.fabric_connected = True
                    st.rerun()
                except Exception as e:
                    st.error(f"Connection failed: {e}")
