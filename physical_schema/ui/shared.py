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

# Rate metrics that must be recalculated from base sums rather than summed directly.
# Maps rate keyword → (numerator keyword, denominator keyword) — all lowercased.
_RATE_FORMULAS = {
    "ctr": ("clicks", "impressions"),
    "click through rate": ("clicks", "impressions"),
    "conversion rate": ("conversions", "clicks"),
    "cpc": ("cost", "clicks"),
    "roi": ("profit", "cost"),
    "revenue per click": ("revenue", "clicks"),
    "revenue per conversion": ("revenue", "conversions"),
}


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


def build_totals_row(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Build a single 'Total' summary row for numeric columns.

    Base metrics (cost, clicks, etc.) are summed. Rate/percentage metrics
    (CTR, conversion rate, etc.) are recalculated from the summed bases so
    the aggregate is correct rather than an average of row-level rates.

    Returns None when there are ≤1 rows (totals would be redundant) or when
    no numeric columns exist.
    """
    if df is None or len(df) <= 1:
        return None

    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if not numeric_cols:
        return None

    col_sums = {col: df[col].sum() for col in numeric_cols}
    totals: dict = {}

    # Rate keyword detection — matches MDR's rate_keywords list
    _rate_kws = ["rate", "percentage", "percent", "share", "roi", "ctr", "cpc",
                 "revenue per click", "revenue per conversion"]

    # Fill categorical columns with a label
    for col in df.columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            totals[col] = "Total"

    # For each numeric column decide: sum, recalculate from base metrics, or mean
    recalculated = set()
    for col in numeric_cols:
        col_lower = col.lower().replace("_", " ").strip()
        for rate_kw, (num_kw, denom_kw) in _RATE_FORMULAS.items():
            if rate_kw in col_lower:
                # Recalculate from summed base metrics (most accurate)
                num_col = next(
                    (c for c in numeric_cols if num_kw in c.lower().replace("_", " ")), None
                )
                denom_col = next(
                    (c for c in numeric_cols if denom_kw in c.lower().replace("_", " ")), None
                )
                if num_col and denom_col and col_sums.get(denom_col, 0) != 0:
                    totals[col] = col_sums[num_col] / col_sums[denom_col]
                else:
                    # Base metrics not in result — use mean (matches MDR fallback behavior)
                    totals[col] = df[col].mean()
                recalculated.add(col)
                break

        if col not in recalculated:
            col_lower = col.lower().replace("_", " ").strip()
            if any(kw in col_lower for kw in _rate_kws):
                # Unknown rate metric — mean is safer than sum
                totals[col] = df[col].mean()
            else:
                totals[col] = col_sums[col]

    return pd.DataFrame([totals], columns=df.columns)


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
