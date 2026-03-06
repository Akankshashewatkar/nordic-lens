"""
NordicLens — Anomaly Detection DAG

Schedule: 08:00 UTC daily (after dbt transform completes)
Pipeline: query_flagged_transactions >> compute_statistical_checks >>
          write_anomaly_summary

Queries MARTS.FCT_TRANSACTIONS, applies pandas/scipy statistical checks
beyond the rule-based dbt flags, and writes a daily summary to
MARTS.ANOMALY_DAILY_SUMMARY.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "nordiclens",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _query_flagged_transactions(**ctx) -> None:
    """
    Pull today's transactions from MARTS.FCT_TRANSACTIONS.
    Pushes DataFrame to XCom as JSON for downstream tasks.
    """
    # TODO (Phase 5): query Snowflake, push to XCom
    raise NotImplementedError


def _compute_statistical_checks(**ctx) -> None:
    """
    Apply additional statistical anomaly detection using scipy:
      - Z-score per MCC category
      - IQR fences per customer segment
      - Isolation Forest on (amount, hour_of_day, recency_days) features
    Pushes anomaly summary dict to XCom.
    """
    # TODO (Phase 5): implement scipy + sklearn anomaly detection
    raise NotImplementedError


def _write_anomaly_summary(**ctx) -> None:
    """
    Write the daily anomaly summary record to MARTS.ANOMALY_DAILY_SUMMARY.
    Table is created if it does not exist (CTAS pattern).
    """
    # TODO (Phase 5): write summary to Snowflake
    raise NotImplementedError


with DAG(
    dag_id="nordiclens_anomaly_detection",
    description="Statistical anomaly detection on daily transactions → summary report",
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
