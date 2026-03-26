from __future__ import annotations
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

DBT_DIR = "/opt/airflow/dbt"

def on_failure_alert(context):
    logger.error("FAILURE | dag=%s | task=%s | exec=%s",
                 context["dag"].dag_id,
                 context["task_instance"].task_id,
                 context["execution_date"])

def run_ingestion(**context):
    import importlib.util, os
    os.environ["PIPELINE_RUN_ID"] = context["run_id"]
    spec = importlib.util.spec_from_file_location(
        "ingest_data", "/opt/airflow/scripts/ingest_data.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()

with DAG(
    dag_id="customer_transactions_pipeline",
    description="CSV -> raw -> staging -> marts",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ebury", "platform"],
    on_failure_callback=on_failure_alert,
) as dag:

    ingest_raw = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=run_ingestion,
        on_failure_callback=on_failure_alert,
    )

    dbt_staging_run = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --select staging --target prod",
        on_failure_callback=on_failure_alert,
    )

    dbt_staging_test = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --select staging --target prod",
        on_failure_callback=on_failure_alert,
    )

    dbt_marts_run = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --select marts --target prod",
        on_failure_callback=on_failure_alert,
    )

    dbt_marts_test = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --select marts --target prod",
        on_failure_callback=on_failure_alert,
    )

    ingest_raw >> dbt_staging_run >> dbt_staging_test >> dbt_marts_run >> dbt_marts_test
