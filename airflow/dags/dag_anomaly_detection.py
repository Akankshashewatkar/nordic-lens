"""
NordicLens — Anomaly Detection DAG

Schedule: 08:00 UTC daily (after dbt transform completes)
Pipeline:
    query_flagged_transactions
        >> compute_statistical_checks
        >> write_anomaly_summary

Applies three layers of statistical anomaly detection on top of the
rule-based flags already computed in dbt (int_anomaly_flags):

  1. Z-score per MCC category (scipy)      — flags amount outliers within category
  2. IQR fences per RFM segment (pandas)   — non-parametric amount outlier per segment
  3. Isolation Forest (sklearn)            — multivariate anomaly on (amount, hour, recency)

Results are written to MARTS.ANOMALY_DAILY_SUMMARY, one row per run date.

XCom pattern: the DataFrame (up to 100k rows) is too large for the Airflow
metadata DB. Tasks communicate via a temp Parquet file; the summary dict
(small, JSON-serialisable) is passed directly through XCom return values.
"""

import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "nordiclens",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# Temp file written by task 1, read by task 2
_TEMP_PARQUET = Path(tempfile.gettempdir()) / "nordiclens_fct_transactions.parquet"

# DDL for the daily summary table (CREATE IF NOT EXISTS)
_SUMMARY_DDL = """
CREATE TABLE IF NOT EXISTS NORDICLENS_DB.MARTS.ANOMALY_DAILY_SUMMARY (
    report_date                     DATE            NOT NULL,
    total_transactions              INTEGER         NOT NULL,
    -- dbt rule-based flag counts
    rule_statistical_outlier_count  INTEGER,
    rule_velocity_spike_count       INTEGER,
    rule_large_round_amount_count   INTEGER,
    rule_suspicious_late_night_count INTEGER,
    rule_unusual_geography_count    INTEGER,
    rule_any_anomaly_count          INTEGER,
    -- scipy / sklearn statistical flag counts
    stat_zscore_outlier_count       INTEGER,
    stat_iqr_outlier_count          INTEGER,
    stat_isolation_forest_count     INTEGER,
    -- combined
    total_anomaly_count             INTEGER,
    anomaly_rate_pct                FLOAT,
    -- context
    top_anomaly_mcc_category        VARCHAR(100),
    top_anomaly_rfm_segment         VARCHAR(50),
    _loaded_at                      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
""".strip()


# ── Task 1: Query FCT_TRANSACTIONS ────────────────────────────────────────────

def _query_flagged_transactions(**ctx) -> str:
    """
    Pull the last 7 days of transactions from MARTS.FCT_TRANSACTIONS.

    A 7-day window gives enough data for statistical checks while keeping
    the payload manageable. Saves result to a temp Parquet file and returns
    the file path via XCom.
    """
    import pandas as pd
    import snowflake.connector

    execution_date: str = ctx["ds"]  # YYYY-MM-DD
    logger.info("Querying FCT_TRANSACTIONS up to %s (7-day window)", execution_date)

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "NORDICLENS_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "NORDICLENS_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "NORDICLENS_DB"),
        schema="MARTS",
    )
    try:
        sql = """
            SELECT
                transaction_id,
                customer_id,
                merchant_id,
                mcc_category,
                amount_nok,
                hour_of_day,
                is_weekend,
                channel,
                country_code,
                is_simulated_fraud,
                rfm_segment,
                days_since_last_txn,
                rolling_7d_avg_amount,
                -- dbt rule-based flags
                is_statistical_outlier,
                is_velocity_spike,
                is_large_round_amount,
                is_suspicious_late_night,
                is_unusual_geography,
                has_any_anomaly_flag,
                transaction_date
            FROM NORDICLENS_DB.MARTS.FCT_TRANSACTIONS
            WHERE transaction_date >= DATEADD('day', -7, %(exec_date)s::date)
              AND transaction_date <  DATEADD('day',  1, %(exec_date)s::date)
        """
        df = pd.read_sql(sql, conn, params={"exec_date": execution_date})
    finally:
        conn.close()

    logger.info("Retrieved %d transactions for anomaly analysis", len(df))
    df.to_parquet(_TEMP_PARQUET, index=False, engine="pyarrow")
    return str(_TEMP_PARQUET)


# ── Task 2: Statistical anomaly checks ───────────────────────────────────────

def _compute_statistical_checks(**ctx) -> str:
    """
    Apply three layers of statistical anomaly detection:

      1. Z-score per MCC category (scipy.stats.zscore)
         Flags transactions where |z| > 3 within their merchant category.

      2. IQR fences per RFM segment (non-parametric, no normality assumption)
         Flags transactions outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR] per segment.

      3. Isolation Forest (sklearn)
         Multivariate anomaly detection on (amount_nok, hour_of_day,
         days_since_last_txn). Contamination = 5% matches the simulated fraud rate.

    Returns a JSON-serialised summary dict via XCom.
    """
    import numpy as np
    import pandas as pd
    from scipy import stats
    from sklearn.ensemble import IsolationForest
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import StandardScaler

    parquet_path = ctx["ti"].xcom_pull(task_ids="query_flagged_transactions")
    df = pd.read_parquet(parquet_path)

    if df.empty:
        logger.warning("No transactions in window — returning empty summary")
        return json.dumps({"total_transactions": 0})

    execution_date: str = ctx["ds"]
    logger.info("Running statistical checks on %d transactions", len(df))

    # ── 1. Z-score per MCC category ───────────────────────────────────────────
    df["z_score"] = df.groupby("mcc_category")["amount_nok"].transform(
        lambda x: stats.zscore(x, nan_policy="omit") if len(x) > 1 else 0.0
    )
    df["stat_zscore_outlier"] = df["z_score"].abs() > 3

    # ── 2. IQR fences per RFM segment ────────────────────────────────────────
    def _iqr_outlier(series: pd.Series) -> pd.Series:
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        fence_lo, fence_hi = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
        return (series < fence_lo) | (series > fence_hi)

    df["stat_iqr_outlier"] = df.groupby("rfm_segment")["amount_nok"].transform(
        _iqr_outlier
    )

    # ── 3. Isolation Forest (multivariate) ────────────────────────────────────
    feature_cols = ["amount_nok", "hour_of_day", "days_since_last_txn"]
    features = df[feature_cols].copy()

    # Impute nulls (days_since_last_txn is NULL for first-ever transaction)
    imputer = SimpleImputer(strategy="median")
    features_imputed = imputer.fit_transform(features)

    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features_imputed)

    iso_forest = IsolationForest(
        n_estimators=100,
        contamination=0.05,  # matches simulated fraud rate
        random_state=42,
        n_jobs=-1,
    )
    df["stat_isolation_forest"] = iso_forest.fit_predict(features_scaled) == -1

    # ── Build summary ──────────────────────────────────────────────────────────
    n = len(df)
    total_anomaly = (
        df["stat_zscore_outlier"] | df["stat_iqr_outlier"] | df["stat_isolation_forest"]
    )

    # Top anomaly MCC category (most anomalies by any method)
    anomaly_df = df[total_anomaly]
    top_mcc = (
        anomaly_df["mcc_category"].value_counts().idxmax()
        if not anomaly_df.empty else None
    )
    top_segment = (
        anomaly_df["rfm_segment"].value_counts().idxmax()
        if not anomaly_df.empty else None
    )

    summary: dict[str, Any] = {
        "report_date": execution_date,
        "total_transactions": n,
        # dbt rule-based counts
        "rule_statistical_outlier_count":   int(df["is_statistical_outlier"].sum()),
        "rule_velocity_spike_count":        int(df["is_velocity_spike"].sum()),
        "rule_large_round_amount_count":    int(df["is_large_round_amount"].sum()),
        "rule_suspicious_late_night_count": int(df["is_suspicious_late_night"].sum()),
        "rule_unusual_geography_count":     int(df["is_unusual_geography"].sum()),
        "rule_any_anomaly_count":           int(df["has_any_anomaly_flag"].sum()),
        # statistical counts
        "stat_zscore_outlier_count":        int(df["stat_zscore_outlier"].sum()),
        "stat_iqr_outlier_count":           int(df["stat_iqr_outlier"].sum()),
        "stat_isolation_forest_count":      int(df["stat_isolation_forest"].sum()),
        # combined
        "total_anomaly_count":  int(total_anomaly.sum()),
        "anomaly_rate_pct":     round(float(total_anomaly.mean() * 100), 4),
        "top_anomaly_mcc_category": top_mcc,
        "top_anomaly_rfm_segment":  top_segment,
    }

    logger.info(
        "Anomaly summary — total: %d, z-score: %d, IQR: %d, IsoForest: %d, rate: %.2f%%",
        summary["total_anomaly_count"],
        summary["stat_zscore_outlier_count"],
        summary["stat_iqr_outlier_count"],
        summary["stat_isolation_forest_count"],
        summary["anomaly_rate_pct"],
    )
    return json.dumps(summary)


# ── Task 3: Write daily summary to Snowflake ──────────────────────────────────

def _write_anomaly_summary(**ctx) -> None:
    """
    Write the statistical summary dict to MARTS.ANOMALY_DAILY_SUMMARY.
    Creates the table if it does not yet exist.
    Upserts by report_date (DELETE + INSERT) for idempotent reruns.
    """
    import snowflake.connector

    summary_json: str = ctx["ti"].xcom_pull(task_ids="compute_statistical_checks")
    summary: dict[str, Any] = json.loads(summary_json)

    if summary.get("total_transactions", 0) == 0:
        logger.warning("Empty summary — skipping write to ANOMALY_DAILY_SUMMARY")
        return

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "NORDICLENS_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "NORDICLENS_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "NORDICLENS_DB"),
        schema="MARTS",
    )

    try:
        with conn.cursor() as cur:
            # Create table if needed
            cur.execute(_SUMMARY_DDL)

            # Idempotent upsert: delete existing row for this date, then insert
            cur.execute(
                "DELETE FROM NORDICLENS_DB.MARTS.ANOMALY_DAILY_SUMMARY WHERE report_date = %s",
                (summary["report_date"],),
            )

            cur.execute(
                """
                INSERT INTO NORDICLENS_DB.MARTS.ANOMALY_DAILY_SUMMARY (
                    report_date,
                    total_transactions,
                    rule_statistical_outlier_count,
                    rule_velocity_spike_count,
                    rule_large_round_amount_count,
                    rule_suspicious_late_night_count,
                    rule_unusual_geography_count,
                    rule_any_anomaly_count,
                    stat_zscore_outlier_count,
                    stat_iqr_outlier_count,
                    stat_isolation_forest_count,
                    total_anomaly_count,
                    anomaly_rate_pct,
                    top_anomaly_mcc_category,
                    top_anomaly_rfm_segment
                ) VALUES (
                    %(report_date)s,
                    %(total_transactions)s,
                    %(rule_statistical_outlier_count)s,
                    %(rule_velocity_spike_count)s,
                    %(rule_large_round_amount_count)s,
                    %(rule_suspicious_late_night_count)s,
                    %(rule_unusual_geography_count)s,
                    %(rule_any_anomaly_count)s,
                    %(stat_zscore_outlier_count)s,
                    %(stat_iqr_outlier_count)s,
                    %(stat_isolation_forest_count)s,
                    %(total_anomaly_count)s,
                    %(anomaly_rate_pct)s,
                    %(top_anomaly_mcc_category)s,
                    %(top_anomaly_rfm_segment)s
                )
                """,
                summary,
            )
            conn.commit()

        logger.info(
            "Wrote anomaly summary for %s — %d total anomalies (%.2f%%)",
            summary["report_date"],
            summary["total_anomaly_count"],
            summary["anomaly_rate_pct"],
        )
    finally:
        conn.close()

    # Clean up temp parquet
    _TEMP_PARQUET.unlink(missing_ok=True)


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="nordiclens_anomaly_detection",
    description="Statistical anomaly detection (Z-score, IQR, IsoForest) → ANOMALY_DAILY_SUMMARY",
    schedule_interval="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nordiclens", "anomaly"],
    doc_md=__doc__,
) as dag:

    query_flagged_transactions = PythonOperator(
        task_id="query_flagged_transactions",
        python_callable=_query_flagged_transactions,
    )

    compute_statistical_checks = PythonOperator(
        task_id="compute_statistical_checks",
        python_callable=_compute_statistical_checks,
    )

    write_anomaly_summary = PythonOperator(
        task_id="write_anomaly_summary",
        python_callable=_write_anomaly_summary,
    )

    query_flagged_transactions >> compute_statistical_checks >> write_anomaly_summary
