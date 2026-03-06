"""
NordicLens — Streamlit Dashboard

4-page analytics dashboard for Norwegian banking transaction data.

Pages:
  1. Executive KPIs       — volume, fraud rate, active customers, top categories
  2. Transaction Explorer — time-series + hourly heatmap with filters
  3. Anomaly Intelligence — flag table, trend, correlation with interest rate
  4. Macro Context        — CPI + Norges Bank rate overlay and scatter

Run:
  streamlit run dashboard/app.py
"""

import os
import logging
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import snowflake.connector

logger = logging.getLogger(__name__)

# ── Snowflake connection ───────────────────────────────────────────────────────

@st.cache_resource(show_spinner="Connecting to Snowflake…")
def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Create and cache a Snowflake connection using environment variables."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "NORDICLENS_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "NORDICLENS_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "NORDICLENS_DB"),
        schema=os.environ.get("SNOWFLAKE_MARTS_SCHEMA", "MARTS"),
    )


@st.cache_data(ttl=3600, show_spinner="Loading data…")
def query(_conn: snowflake.connector.SnowflakeConnection, sql: str) -> pd.DataFrame:
    """Execute SQL and return result as a DataFrame (cached for 1 hour)."""
    return pd.read_sql(sql, _conn)


# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="NordicLens",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

conn = get_snowflake_connection()

# ── Sidebar navigation ────────────────────────────────────────────────────────

st.sidebar.title("NordicLens 🏦")
st.sidebar.caption("Norwegian Banking Analytics")
page = st.sidebar.radio(
    "Navigate",
    ["Executive KPIs", "Transaction Explorer", "Anomaly Intelligence", "Macro Context"],
)

# ── Page 1: Executive KPIs ────────────────────────────────────────────────────

if page == "Executive KPIs":
    st.title("Executive KPIs")
    st.caption("Monthly summary — NordicLens synthetic transaction data")

    # TODO (Phase 6): implement KPI cards and charts
    st.info("Phase 6 — to be implemented.")

# ── Page 2: Transaction Explorer ─────────────────────────────────────────────

elif page == "Transaction Explorer":
    st.title("Transaction Explorer")

    # TODO (Phase 6): date range filter, segment filter, line chart, heatmap
    st.info("Phase 6 — to be implemented.")

# ── Page 3: Anomaly Intelligence ─────────────────────────────────────────────

elif page == "Anomaly Intelligence":
    st.title("Anomaly Intelligence")

    # TODO (Phase 6): flagged transaction table, trend chart, rate correlation
    st.info("Phase 6 — to be implemented.")

# ── Page 4: Macro Context ─────────────────────────────────────────────────────

elif page == "Macro Context":
    st.title("Macro Context")

    # TODO (Phase 6): CPI line chart, rate overlay, scatter plot
    st.info("Phase 6 — to be implemented.")
