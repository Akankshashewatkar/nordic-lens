"""
NordicLens — Streamlit Dashboard

4-page analytics dashboard for Norwegian banking transaction data.

Pages:
  1. Executive KPIs       — volume, fraud rate, active customers, top categories
  2. Transaction Explorer — time-series + hourly heatmap with filters
  3. Anomaly Intelligence — flag table, trend, anomaly vs interest rate correlation
  4. Macro Context        — CPI + Norges Bank rate overlay and volume scatter

Run:
  streamlit run dashboard/app.py
"""

import os
import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import snowflake.connector

logger = logging.getLogger(__name__)

# ── Page config (must be first Streamlit call) ────────────────────────────────

st.set_page_config(
    page_title="NordicLens",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Snowflake helpers ─────────────────────────────────────────────────────────

def _secret(key: str, default: str = "") -> str:
    """Read from st.secrets (Streamlit Cloud) with fallback to os.environ (local)."""
    try:
        return st.secrets[key]
    except (KeyError, Exception):
        return os.environ.get(key, default)


@st.cache_resource(show_spinner="Connecting to Snowflake…")
def _get_conn() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=_secret("SNOWFLAKE_ACCOUNT"),
        user=_secret("SNOWFLAKE_USER"),
        password=_secret("SNOWFLAKE_PASSWORD"),
        role=_secret("SNOWFLAKE_ROLE") or "NORDICLENS_ROLE",
        warehouse=_secret("SNOWFLAKE_WAREHOUSE") or "NORDICLENS_WH",
        database=_secret("SNOWFLAKE_DATABASE") or "NORDICLENS_DB",
        schema="MARTS",
    )


@st.cache_data(ttl=3600, show_spinner="Querying Snowflake…")
def _query(_conn: snowflake.connector.SnowflakeConnection, sql: str) -> pd.DataFrame:
    """Run SQL and return a DataFrame. Cached for 1 hour; _conn excluded from hash key."""
    cur = _conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0].lower() for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


# ── Number formatting helpers ─────────────────────────────────────────────────

def _nok(n: float) -> str:
    return f"NOK {n:,.0f}"


def _pct(n: float) -> str:
    return f"{n:.2f} %"


def _delta_pct(current: float, previous: float) -> str:
    if previous == 0:
        return "n/a"
    return f"{((current - previous) / previous) * 100:+.1f} % vs prev month"


# ── App shell ─────────────────────────────────────────────────────────────────

conn = _get_conn()

with st.sidebar:
    st.title("NordicLens")
    st.caption("Norwegian Banking Analytics")
    st.divider()
    page = st.radio(
        "Navigate",
        ["Executive KPIs", "Transaction Explorer", "Anomaly Intelligence", "Macro Context"],
    )
    st.divider()
    st.caption("Data: synthetic · 2023–2024")
    if st.button("Clear cache"):
        st.cache_data.clear()
        st.rerun()


# =============================================================================
# Page 1 — Executive KPIs
# =============================================================================

if page == "Executive KPIs":
    st.title("Executive KPIs")
    st.caption("Monthly summary — sourced from `MARTS.RPT_FINANCIAL_HEALTH_KPIS`")

    kpi_df = _query(
        conn,
        """
        SELECT
            report_month,
            total_volume_nok,
            avg_transaction_nok,
            transaction_count,
            active_customer_count,
            fraud_rate_pct,
            anomaly_rate_pct,
            customer_churn_risk_count,
            norges_bank_rate,
            cpi_index
        FROM NORDICLENS_DB.MARTS.RPT_FINANCIAL_HEALTH_KPIS
        ORDER BY report_month DESC
        """,
    )
    kpi_df["report_month"] = pd.to_datetime(kpi_df["report_month"])

    if kpi_df.empty:
        st.warning("No KPI data found. Run the ingestion + dbt pipelines first.")
        st.stop()

    cur = kpi_df.iloc[0]
    prev = kpi_df.iloc[1] if len(kpi_df) > 1 else cur

    # ── KPI metric cards ──────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        label="Total Volume (this month)",
        value=_nok(cur["total_volume_nok"]),
        delta=_delta_pct(cur["total_volume_nok"], prev["total_volume_nok"]),
    )
    c2.metric(
        label="Anomaly Rate",
        value=_pct(cur["anomaly_rate_pct"]),
        delta=_delta_pct(cur["anomaly_rate_pct"], prev["anomaly_rate_pct"]),
        delta_color="inverse",
    )
    c3.metric(
        label="Active Customers",
        value=f"{int(cur['active_customer_count']):,}",
        delta=_delta_pct(cur["active_customer_count"], prev["active_customer_count"]),
    )
    c4.metric(
        label="Fraud Rate",
        value=_pct(cur["fraud_rate_pct"]),
        delta=_delta_pct(cur["fraud_rate_pct"], prev["fraud_rate_pct"]),
        delta_color="inverse",
    )

    st.divider()

    # ── Monthly volume trend ──────────────────────────────────────────────────
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("Monthly Transaction Volume")
        fig_vol = px.bar(
            kpi_df.sort_values("report_month"),
            x="report_month",
            y="total_volume_nok",
            labels={"report_month": "Month", "total_volume_nok": "Volume (NOK)"},
            color="anomaly_rate_pct",
            color_continuous_scale="RdYlGn_r",
            color_continuous_midpoint=kpi_df["anomaly_rate_pct"].mean(),
        )
        fig_vol.update_layout(
            coloraxis_colorbar_title="Anomaly %",
            xaxis_tickformat="%b %Y",
            height=350,
        )
        st.plotly_chart(fig_vol, width='stretch')

    # ── Top 5 merchant categories ─────────────────────────────────────────────
    with col_right:
        st.subheader("Top 5 Merchant Categories")
        top_cats = _query(
            conn,
            """
            SELECT
                mcc_category,
                COUNT(*)        AS transaction_count,
                SUM(amount_nok) AS total_volume_nok
            FROM NORDICLENS_DB.MARTS.FCT_TRANSACTIONS
            GROUP BY mcc_category
            ORDER BY total_volume_nok DESC
            LIMIT 5
            """,
        )
        fig_cats = px.bar(
            top_cats,
            x="total_volume_nok",
            y="mcc_category",
            orientation="h",
            labels={"total_volume_nok": "Volume (NOK)", "mcc_category": ""},
            color="total_volume_nok",
            color_continuous_scale="Blues",
        )
        fig_cats.update_layout(showlegend=False, coloraxis_showscale=False, height=350)
        st.plotly_chart(fig_cats, width='stretch')

    # ── Secondary metrics ─────────────────────────────────────────────────────
    st.divider()
    st.subheader("Trend Metrics")
    m1, m2, m3 = st.columns(3)
    m1.metric("Avg Transaction", _nok(cur["avg_transaction_nok"]))
    m2.metric("Churn Risk Customers", f"{int(cur['customer_churn_risk_count']):,}")
    m3.metric("Norges Bank Rate", f"{cur['norges_bank_rate']:.2f} %" if cur["norges_bank_rate"] else "—")


# =============================================================================
# Page 2 — Transaction Explorer
# =============================================================================

elif page == "Transaction Explorer":
    st.title("Transaction Explorer")

    # ── Filters ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.subheader("Filters")
        date_range = st.date_input(
            "Date range",
            value=(pd.Timestamp("2023-01-01"), pd.Timestamp("2024-12-31")),
        )
        segments = st.multiselect(
            "Customer segment",
            ["Champions", "Loyal", "At Risk", "New", "Dormant", "Potential Loyalist"],
            default=["Champions", "Loyal", "At Risk", "New", "Dormant", "Potential Loyalist"],
        )
        channels = st.multiselect(
            "Channel",
            ["mobile", "atm", "online", "branch"],
            default=["mobile", "atm", "online", "branch"],
        )

    if len(date_range) != 2:
        st.warning("Select a start and end date.")
        st.stop()

    start_date, end_date = str(date_range[0]), str(date_range[1])
    seg_list = "', '".join(segments) if segments else "''"
    chan_list = "', '".join(channels) if channels else "''"

    # ── Daily volume trend ────────────────────────────────────────────────────
    daily_df = _query(
        conn,
        f"""
        SELECT
            transaction_date::date    AS txn_date,
            COUNT(*)                  AS transaction_count,
            SUM(amount_nok)           AS total_volume_nok,
            AVG(amount_nok)           AS avg_amount_nok
        FROM NORDICLENS_DB.MARTS.FCT_TRANSACTIONS
        WHERE transaction_date::date BETWEEN '{start_date}' AND '{end_date}'
          AND rfm_segment  IN ('{seg_list}')
          AND channel       IN ('{chan_list}')
        GROUP BY 1
        ORDER BY 1
        """,
    )
    daily_df["txn_date"] = pd.to_datetime(daily_df["txn_date"])

    st.subheader("Daily Transaction Volume")
    fig_daily = px.line(
        daily_df,
        x="txn_date",
        y="total_volume_nok",
        labels={"txn_date": "Date", "total_volume_nok": "Volume (NOK)"},
        color_discrete_sequence=["#1f77b4"],
    )
    fig_daily.add_scatter(
        x=daily_df["txn_date"],
        y=daily_df["avg_amount_nok"],
        name="Avg transaction",
        yaxis="y2",
        line=dict(color="#ff7f0e", dash="dot"),
    )
    fig_daily.update_layout(
        yaxis2=dict(overlaying="y", side="right", title="Avg Amount (NOK)"),
        legend=dict(orientation="h", y=1.1),
        xaxis_tickformat="%d %b %Y",
        height=380,
    )
    st.plotly_chart(fig_daily, width='stretch')

    st.divider()

    # ── Hour × Day-of-week heatmap ────────────────────────────────────────────
    st.subheader("Transaction Frequency — Hour of Day × Day of Week")

    heatmap_df = _query(
        conn,
        f"""
        SELECT
            hour_of_day,
            DAYOFWEEK(transaction_date) AS dow,   -- 0=Sun … 6=Sat in Snowflake
            COUNT(*) AS txn_count
        FROM NORDICLENS_DB.MARTS.FCT_TRANSACTIONS
        WHERE transaction_date::date BETWEEN '{start_date}' AND '{end_date}'
          AND rfm_segment IN ('{seg_list}')
        GROUP BY 1, 2
        """,
    )

    if not heatmap_df.empty:
        dow_labels = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
        heatmap_df["day_name"] = heatmap_df["dow"].map(dow_labels)
        pivot = (
            heatmap_df.pivot_table(
                index="hour_of_day", columns="day_name", values="txn_count", aggfunc="sum"
            )
            .reindex(columns=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])
            .fillna(0)
        )
        fig_heat = px.imshow(
            pivot,
            labels=dict(x="Day of Week", y="Hour of Day", color="Transactions"),
            color_continuous_scale="Blues",
            aspect="auto",
            height=500,
        )
        fig_heat.update_yaxes(tickmode="linear", tick0=0, dtick=2)
        st.plotly_chart(fig_heat, width='stretch')
    else:
        st.info("No data for selected filters.")


# =============================================================================
# Page 3 — Anomaly Intelligence
# =============================================================================

elif page == "Anomaly Intelligence":
    st.title("Anomaly Intelligence")

    # ── Flagged transaction table ─────────────────────────────────────────────
    st.subheader("Recently Flagged Transactions")

    flags_df = _query(
        conn,
        """
        SELECT
            transaction_date::date      AS date,
            transaction_id,
            customer_id,
            mcc_category,
            amount_nok,
            channel,
            country_code,
            rfm_segment,
            CASE
                WHEN is_statistical_outlier   THEN 'Statistical Outlier'
                WHEN is_velocity_spike        THEN 'Velocity Spike'
                WHEN is_large_round_amount    THEN 'Large Round Amount'
                WHEN is_suspicious_late_night THEN 'Suspicious Late Night'
                WHEN is_unusual_geography     THEN 'Unusual Geography'
                ELSE 'Multiple / Other'
            END                         AS primary_flag_type,
            has_any_anomaly_flag
        FROM NORDICLENS_DB.MARTS.FCT_TRANSACTIONS
        WHERE has_any_anomaly_flag = TRUE
        ORDER BY transaction_date DESC
        LIMIT 500
        """,
    )

    if not flags_df.empty:
        flag_filter = st.multiselect(
            "Filter by flag type",
            flags_df["primary_flag_type"].unique().tolist(),
            default=flags_df["primary_flag_type"].unique().tolist(),
        )
        filtered = flags_df[flags_df["primary_flag_type"].isin(flag_filter)]

        color_map = {
            "Statistical Outlier": "#e74c3c",
            "Velocity Spike": "#e67e22",
            "Large Round Amount": "#9b59b6",
            "Suspicious Late Night": "#2c3e50",
            "Unusual Geography": "#27ae60",
            "Multiple / Other": "#95a5a6",
        }

        col_tbl, col_pie = st.columns([2, 1])
        with col_tbl:
            st.dataframe(
                filtered[["date", "mcc_category", "amount_nok", "channel", "country_code", "rfm_segment", "primary_flag_type"]]
                .rename(columns={"primary_flag_type": "flag_type", "amount_nok": "amount (NOK)"}),
                width='stretch',
                height=350,
            )
        with col_pie:
            pie_data = filtered["primary_flag_type"].value_counts().reset_index()
            pie_data.columns = ["flag_type", "count"]
            fig_pie = px.pie(
                pie_data,
                names="flag_type",
                values="count",
                color="flag_type",
                color_discrete_map=color_map,
                hole=0.45,
            )
            fig_pie.update_layout(showlegend=True, height=350, margin=dict(t=10))
            st.plotly_chart(fig_pie, width='stretch')
    else:
        st.info("No flagged transactions found.")

    st.divider()

    # ── Anomaly rate trend + Norges Bank rate overlay ─────────────────────────
    st.subheader("Anomaly Rate vs Norges Bank Key Policy Rate")

    trend_df = _query(
        conn,
        """
        SELECT
            report_month,
            anomaly_rate_pct,
            fraud_rate_pct,
            norges_bank_rate
        FROM NORDICLENS_DB.MARTS.RPT_FINANCIAL_HEALTH_KPIS
        ORDER BY report_month
        """,
    )
    trend_df["report_month"] = pd.to_datetime(trend_df["report_month"])

    if not trend_df.empty:
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=trend_df["report_month"], y=trend_df["anomaly_rate_pct"],
            name="Anomaly Rate %", line=dict(color="#e74c3c", width=2),
        ))
        fig_trend.add_trace(go.Scatter(
            x=trend_df["report_month"], y=trend_df["fraud_rate_pct"],
            name="Fraud Rate %", line=dict(color="#e67e22", width=2, dash="dot"),
        ))
        fig_trend.add_trace(go.Scatter(
            x=trend_df["report_month"], y=trend_df["norges_bank_rate"],
            name="Policy Rate %", line=dict(color="#2980b9", width=2),
            yaxis="y2",
        ))
        fig_trend.update_layout(
            yaxis=dict(title="Rate (%)"),
            yaxis2=dict(title="Norges Bank Rate (%)", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.1),
            xaxis_tickformat="%b %Y",
            height=380,
        )
        st.plotly_chart(fig_trend, width='stretch')

        # ── Scatter: anomaly rate vs policy rate ──────────────────────────────
        st.subheader("Correlation: Anomaly Rate vs Policy Rate")
        fig_scatter = px.scatter(
            trend_df.dropna(subset=["norges_bank_rate"]),
            x="norges_bank_rate",
            y="anomaly_rate_pct",
            trendline="ols",
            labels={
                "norges_bank_rate": "Norges Bank Policy Rate (%)",
                "anomaly_rate_pct": "Anomaly Rate (%)",
            },
            hover_data=["report_month"],
            color="anomaly_rate_pct",
            color_continuous_scale="Reds",
            height=350,
        )
        fig_scatter.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_scatter, width='stretch')


# =============================================================================
# Page 4 — Macro Context
# =============================================================================

elif page == "Macro Context":
    st.title("Macro Context")
    st.caption("Norwegian CPI and Norges Bank policy rate vs transaction behaviour")

    macro_df = _query(
        conn,
        """
        SELECT
            report_month,
            total_volume_nok,
            transaction_count,
            avg_transaction_nok,
            norges_bank_rate,
            cpi_index
        FROM NORDICLENS_DB.MARTS.RPT_FINANCIAL_HEALTH_KPIS
        WHERE norges_bank_rate IS NOT NULL
           OR cpi_index IS NOT NULL
        ORDER BY report_month
        """,
    )
    macro_df["report_month"] = pd.to_datetime(macro_df["report_month"])

    if macro_df.empty:
        st.warning("No macro data found. Check that SSB and Norges Bank extractors ran successfully.")
        st.stop()

    # ── CPI + Policy Rate dual-axis chart ─────────────────────────────────────
    st.subheader("Norwegian CPI Index vs Key Policy Rate")

    fig_macro = go.Figure()
    fig_macro.add_trace(go.Scatter(
        x=macro_df["report_month"],
        y=macro_df["cpi_index"],
        name="CPI Index (1998=100)",
        line=dict(color="#27ae60", width=2.5),
        fill="tozeroy",
        fillcolor="rgba(39, 174, 96, 0.08)",
    ))
    fig_macro.add_trace(go.Scatter(
        x=macro_df["report_month"],
        y=macro_df["norges_bank_rate"],
        name="Policy Rate (%)",
        line=dict(color="#e74c3c", width=2.5),
        yaxis="y2",
    ))
    fig_macro.update_layout(
        yaxis=dict(title="CPI Index"),
        yaxis2=dict(title="Policy Rate (%)", overlaying="y", side="right"),
        legend=dict(orientation="h", y=1.1),
        xaxis_tickformat="%b %Y",
        height=400,
    )
    st.plotly_chart(fig_macro, width='stretch')

    st.divider()
    col_l, col_r = st.columns(2)

    # ── Transaction volume vs CPI scatter ─────────────────────────────────────
    with col_l:
        st.subheader("Volume vs CPI Index")
        fig_cpi_vol = px.scatter(
            macro_df.dropna(subset=["cpi_index"]),
            x="cpi_index",
            y="total_volume_nok",
            trendline="ols",
            labels={
                "cpi_index": "CPI Index",
                "total_volume_nok": "Monthly Volume (NOK)",
            },
            hover_data=["report_month"],
            color="report_month",
            color_continuous_scale="Viridis",
            height=350,
        )
        fig_cpi_vol.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_cpi_vol, width='stretch')

    # ── Transaction volume vs policy rate scatter ─────────────────────────────
    with col_r:
        st.subheader("Volume vs Policy Rate")
        fig_rate_vol = px.scatter(
            macro_df.dropna(subset=["norges_bank_rate"]),
            x="norges_bank_rate",
            y="total_volume_nok",
            trendline="ols",
            labels={
                "norges_bank_rate": "Policy Rate (%)",
                "total_volume_nok": "Monthly Volume (NOK)",
            },
            hover_data=["report_month"],
            color="norges_bank_rate",
            color_continuous_scale="RdYlGn_r",
            height=350,
        )
        fig_rate_vol.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_rate_vol, width='stretch')

    # ── Avg transaction size over time with rate bands ────────────────────────
    st.divider()
    st.subheader("Average Transaction Amount vs Rate Environment")

    fig_avg = go.Figure()
    fig_avg.add_trace(go.Scatter(
        x=macro_df["report_month"],
        y=macro_df["avg_transaction_nok"],
        name="Avg Transaction (NOK)",
        line=dict(color="#8e44ad", width=2),
        fill="tonexty" if False else None,
    ))
    fig_avg.add_trace(go.Scatter(
        x=macro_df["report_month"],
        y=macro_df["norges_bank_rate"],
        name="Policy Rate (%)",
        line=dict(color="#e74c3c", width=1.5, dash="dash"),
        yaxis="y2",
    ))
    fig_avg.update_layout(
        yaxis=dict(title="Avg Transaction (NOK)"),
        yaxis2=dict(title="Policy Rate (%)", overlaying="y", side="right"),
        legend=dict(orientation="h", y=1.1),
        xaxis_tickformat="%b %Y",
        height=350,
    )
    st.plotly_chart(fig_avg, width='stretch')
