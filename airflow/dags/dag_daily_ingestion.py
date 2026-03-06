"""
NordicLens — Daily Ingestion DAG

Schedule: 06:00 UTC daily
Pipeline: extract_ssb >> extract_norges_bank >> simulate_transactions >>
          upload_to_s3 >> load_to_snowflake_raw

All tasks are idempotent — safe to re-run on failure.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "nordiclens",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _extract_ssb(**ctx) -> str:
    """
    Pull SSB CPI data and save locally as Parquet.
    Returns the local Parquet path via XCom.
    """
    # TODO (Phase 5): import SSBCPIExtractor, call .run(), push path to XCom
    raise NotImplementedError


def _extract_norges_bank(**ctx) -> str:
    """
    Pull Norges Bank rate data and save locally as Parquet.
    Returns the local Parquet path via XCom.
    """
    # TODO (Phase 5): import NorgesBankRateExtractor, call .run(), push path to XCom
    raise NotImplementedError


def _simulate_transactions(**ctx) -> str:
    """
    Generate synthetic transactions and save as Parquet.
    Returns the local Parquet path via XCom.
    """
    # TODO (Phase 5): import TransactionSimulator, call .run() + save_parquet()
    raise NotImplementedError


def _upload_to_s3(**ctx) -> None:
    """
    Upload all locally generated Parquet files to S3.
    Reads paths from upstream XCom values.
    """
    # TODO (Phase 5): import S3Uploader, upload each extracted file
    raise NotImplementedError


def _load_to_snowflake(**ctx) -> None:
    """
    COPY INTO Snowflake RAW tables from S3 paths.
    """
    # TODO (Phase 5): import SnowflakeLoader, call .load_all()
    raise NotImplementedError


with DAG(
    dag_id="nordiclens_daily_ingestion",
    description="Extract Norwegian macro data + simulate transactions, load to Snowflake RAW",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nordiclens", "ingestion"],
    doc_md=__doc__,
) as dag:

    extract_ssb = PythonOperator(
        task_id="extract_ssb",
        python_callable=_extract_ssb,
    )

    extract_norges_bank = PythonOperator(
        task_id="extract_norges_bank",
        python_callable=_extract_norges_bank,
    )

    simulate_transactions = PythonOperator(
        task_id="simulate_transactions",
        python_callable=_simulate_transactions,
    )

    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=_upload_to_s3,
    )

    load_to_snowflake_raw = PythonOperator(
        task_id="load_to_snowflake_raw",
        python_callable=_load_to_snowflake,
    )

    [extract_ssb, extract_norges_bank, simulate_transactions] >> upload_to_s3 >> load_to_snowflake_raw
