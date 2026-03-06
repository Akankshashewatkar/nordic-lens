"""
NordicLens — Daily Ingestion DAG

Schedule: 06:00 UTC daily
Pipeline:
    [extract_ssb, extract_norges_bank, simulate_transactions]
        >> upload_to_s3
        >> load_to_snowflake_raw

All tasks are idempotent — safe to re-run on failure.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "nordiclens",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

_DATA_DIR = Path("/opt/airflow/data/raw")


def _extract_ssb(**ctx) -> str:
    """Pull SSB CPI data → Parquet. Pushes local path via return value (XCom)."""
    from ingestion.extractors.ssb_api import SSBCPIExtractor

    path = SSBCPIExtractor(output_dir=_DATA_DIR / "macro/ssb_cpi").run()
    return str(path)


def _extract_norges_bank(**ctx) -> str:
    """Pull Norges Bank rate data → Parquet. Pushes local path via return value."""
    from ingestion.extractors.norges_bank_api import NorgesBankRateExtractor

    path = NorgesBankRateExtractor(output_dir=_DATA_DIR / "macro/norges_bank_rates").run()
    return str(path)


def _simulate_transactions(**ctx) -> str:
    """Generate synthetic transactions → Parquet. Pushes local path via return value."""
    from ingestion.extractors.transaction_simulator import TransactionSimulator

    sim = TransactionSimulator()
    df = sim.run()
    path = sim.save_parquet(df, output_dir=_DATA_DIR / "transactions")
    return str(path)


def _upload_to_s3(**ctx) -> None:
    """Upload all locally generated Parquet files to their S3 prefixes."""
    from ingestion.loaders.s3_uploader import S3Uploader

    ti = ctx["ti"]
    uploader = S3Uploader()

    uploads = {
        ti.xcom_pull(task_ids="extract_ssb"):          "macro/ssb_cpi",
        ti.xcom_pull(task_ids="extract_norges_bank"):   "macro/norges_bank_rates",
        ti.xcom_pull(task_ids="simulate_transactions"): "transactions",
    }

    for local_path, s3_prefix in uploads.items():
        if local_path:
            uploader.upload_file(
                local_path=Path(local_path),
                s3_prefix=s3_prefix,
                partition_by_date=True,
                partition_date=ctx["ds"],  # Airflow execution date YYYY-MM-DD
            )


def _load_to_snowflake(**ctx) -> None:
    """COPY INTO Snowflake RAW tables from S3."""
    from ingestion.loaders.snowflake_loader import SnowflakeLoader

    with SnowflakeLoader() as loader:
        loader.ensure_infrastructure()
        results = loader.load_all()

    for table, n_rows in results.items():
        logger.info("Loaded RAW.%s — %d rows", table, n_rows)


with DAG(
    dag_id="nordiclens_daily_ingestion",
    description="Extract Norwegian macro data + simulate transactions → Snowflake RAW",
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
