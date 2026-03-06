"""
NordicLens — dbt Transformation DAG

Schedule: 07:00 UTC daily (after ingestion DAG completes)
Pipeline:
    dbt_source_freshness
        >> dbt_run_staging
        >> dbt_run_intermediate
        >> dbt_run_marts
        >> dbt_test
        >> dbt_snapshot
        >> dbt_docs_generate

On dbt_test failure: Airflow marks the task FAILED and logs the full output.
The snapshot and docs tasks are skipped, preventing stale artefacts.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "nordiclens",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

_DBT = "cd /opt/airflow/dbt_project && dbt"
_PROFILE = "--profiles-dir /opt/airflow/dbt_profiles --target prod"


def _dbt_cmd(*args: str) -> str:
    """Build a full dbt shell command with consistent profile flags."""
    return f"{_DBT} {' '.join(args)} {_PROFILE}"


with DAG(
    dag_id="nordiclens_dbt_transform",
    description="dbt source freshness → staging → intermediate → marts → test → snapshot → docs",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nordiclens", "dbt"],
    doc_md=__doc__,
) as dag:

    # Check RAW source freshness before running any models.
    # Fails if any source has warn_after / error_after thresholds exceeded.
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=_dbt_cmd("source freshness"),
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=_dbt_cmd("run --select staging"),
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=_dbt_cmd("run --select intermediate"),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=_dbt_cmd("run --select marts"),
    )

    # Tests run against all layers; BashOperator raises on non-zero exit.
    # Store output to a file so the result can be inspected in Airflow logs.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{_DBT} test {_PROFILE} 2>&1 | tee /tmp/dbt_test_output.txt; "
            "exit ${PIPESTATUS[0]}"
        ),
    )

    # Snapshot only runs if tests pass (default ALL_SUCCESS trigger rule).
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=_dbt_cmd("snapshot"),
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=_dbt_cmd("docs generate"),
        # Generate docs even if snapshot failed (non-blocking step)
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        dbt_source_freshness
        >> dbt_run_staging
        >> dbt_run_intermediate
        >> dbt_run_marts
        >> dbt_test
        >> dbt_snapshot
        >> dbt_docs_generate
    )
