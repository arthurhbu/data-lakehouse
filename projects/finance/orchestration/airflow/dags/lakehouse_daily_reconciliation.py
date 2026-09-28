"""Reconciliação completa diária da origem até a Silver."""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from projects.finance.scripts.orchestration.airflow_callbacks import notify_failure


with DAG(
    dag_id="lakehouse_daily_reconciliation",
    description="Reconciliação completa Postgres x Bronze x Silver",
    start_date=datetime(2026, 9, 21, tzinfo=timezone.utc),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": notify_failure,
    },
    tags=["elysium", "reconciliation", "daily"],
) as dag:
    BashOperator(
        task_id="reconcile_full_pipeline",
        cwd="/opt/lakehouse",
        bash_command="python -m projects.finance.scripts.reconciliation.check_pipeline",
        execution_timeout=timedelta(minutes=30),
    )
