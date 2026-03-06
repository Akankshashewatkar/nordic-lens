"""
NordicLens — dbt Transformation DAG

Schedule: 07:00 UTC daily (after ingestion DAG completes)
Pipeline: dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts >>
          dbt_test >> dbt_snapshot >> dbt_docs_generate

On dbt_test failure: logs full test output and raises for Airflow alert.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "nordiclens",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

DBT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"

with DAG(
    dag_id="nordiclens_dbt_transform",
    description="Run dbt staging → intermediate → marts → test → snapshot → docs",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nordiclens", "dbt"],
    doc_md=__doc__,
) as dag:

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select staging"
        ),
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select intermediate"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select marts"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} "
            f"|| (echo '❌ dbt tests FAILED — see output above' && exit 1)"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt snapshot --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt docs generate --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    (
        dbt_run_staging
        >> dbt_run_intermediate
        >> dbt_run_marts
        >> dbt_test
        >> dbt_snapshot
        >> dbt_docs_generate
    )
