"""
airflow/dags/customer_transactions_dag.py
DAG: customer_transactions_pipeline
Architecture: ELT — BashOperator for ingestion (not PythonOperator + importlib)
"""
from __future__ import annotations
import logging
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner":             "data-platform",
    "depends_on_past":   False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

DBT_PROJECT_DIR  = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
INGEST_SCRIPT    = "/opt/airflow/scripts/ingest_data.py"


def on_failure_alert(context: dict) -> None:
    """
    Centralised failure handler.
    Replace logger.error with a Slack/PagerDuty webhook in production:
        requests.post(SLACK_WEBHOOK_URL, json={"text": f"FAILED: {dag_id}.{task_id}"})
    """
    logger.error(
        "PIPELINE FAILURE | dag=%s | task=%s | execution_date=%s | log=%s",
        context["dag"].dag_id,
        context["task_instance"].task_id,
        context["execution_date"],
        context["task_instance"].log_url,
    )


with DAG(
    dag_id="customer_transactions_pipeline",
    description="ELT: CSV -> raw PostgreSQL -> dbt staging -> dbt marts",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ebury", "platform", "transactions"],
    on_failure_callback=on_failure_alert,
) as dag:

    # Task 1 — Ingest raw data
    # Why BashOperator and not PythonOperator + importlib:
    #   - Runs in its own OS process: no risk of contaminating the Airflow worker.
    #   - {{ run_id }} is a Jinja template resolved natively by Airflow at runtime.
    #   - append_env=True inherits DW_HOST, DW_PASSWORD etc. from the worker env.
    #   - Non-zero exit code is detected correctly and triggers retries.
    ingest_raw = BashOperator(
        task_id="ingest_raw_data",
        bash_command=f"python {INGEST_SCRIPT}",
        env={"PIPELINE_RUN_ID": "{{ run_id }}"},
        append_env=True,
        on_failure_callback=on_failure_alert,
    )

    dbt_staging_run = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} --select staging --target prod"
        ),
        on_failure_callback=on_failure_alert,
    )

    dbt_staging_test = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} --select staging --target prod"
        ),
        on_failure_callback=on_failure_alert,
    )

    dbt_marts_run = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} --select marts --target prod"
        ),
        on_failure_callback=on_failure_alert,
    )

    dbt_marts_test = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} --select marts --target prod"
        ),
        on_failure_callback=on_failure_alert,
    )

    ingest_raw >> dbt_staging_run >> dbt_staging_test >> dbt_marts_run >> dbt_marts_test
