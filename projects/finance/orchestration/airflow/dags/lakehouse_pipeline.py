"""Micro-batch Bronze -> Silver -> Gold executado a cada 15 minutos."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, Variable, task

from projects.finance.scripts.orchestration.airflow_callbacks import notify_failure


PIPELINE_CHECKPOINT_KEY = "lakehouse_pipeline_checkpoint"
LEGACY_LSN_CHECKPOINT_KEY = "lakehouse_last_successful_lsn"
TABLES = [
    "partners",
    "accounts",
    "transactions",
    "payment_events",
    "ledger_entries",
]


@task(task_id="capture_batch")
def capture_batch() -> dict:
    from projects.finance.scripts.orchestration.capture_manifest import capture_manifest

    checkpoint = Variable.get(
        PIPELINE_CHECKPOINT_KEY,
        default=None,
        deserialize_json=True,
    )
    if checkpoint is None:
        legacy_lsn = Variable.get(LEGACY_LSN_CHECKPOINT_KEY, default=None)
        checkpoint = {
            "lsn": int(legacy_lsn) if legacy_lsn is not None else None,
            "offsets": {},
        }

    batch = capture_manifest(
        checkpoint.get("offsets", {}),
        checkpoint.get("lsn"),
    )
    print(json.dumps(batch, ensure_ascii=False))
    return batch


@task.short_circuit(task_id="has_new_files")
def has_new_files(batch: dict) -> bool:
    if not batch["has_files"]:
        print("Nenhum objeto Bronze novo; DagRun encerrada sem processamento.")
        return False
    return True


@task(task_id="commit_checkpoint")
def commit_checkpoint(batch: dict) -> None:
    cutoff_lsn = int(batch["max_lsn"])
    previous = Variable.get(
        PIPELINE_CHECKPOINT_KEY,
        default=None,
        deserialize_json=True,
    )
    if previous is not None and cutoff_lsn < int(previous["lsn"]):
        raise ValueError(
            f"Checkpoint não pode regredir: atual={previous['lsn']}, "
            f"recebido={cutoff_lsn}"
        )

    checkpoint = {
        "lsn": cutoff_lsn,
        "offsets": batch["next_checkpoints"],
    }
    Variable.set(
        PIPELINE_CHECKPOINT_KEY,
        checkpoint,
        description="LSN e offsets confirmados pelo pipeline lakehouse.",
        serialize_json=True,
    )
    print(f"Checkpoint confirmado: {json.dumps(checkpoint, ensure_ascii=False)}")


with DAG(
    dag_id="lakehouse_pipeline",
    description="Micro-batch incremental Bronze -> Silver -> Gold",
    start_date=datetime(2026, 9, 21, tzinfo=timezone.utc),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": notify_failure,
    },
    tags=["elysium", "cdc", "micro-batch"],
    params={
        "force_quality_gate_failure": Param(
            False,
            type="boolean",
            title="Forçar falha controlada",
            description="Falha proposital antes da Gold para estudar retry.",
        ),
    },
) as dag:
    batch = capture_batch()
    batch_available = has_new_files(batch)
    silver_tasks = []
    cutoff_template = "{{ ti.xcom_pull(task_ids='capture_batch')['max_lsn'] }}"

    for table in TABLES:
        files_template = (
            "{{ ti.xcom_pull(task_ids='capture_batch')['tables']"
            f".get('{table}', "
            "{'files': []})['files'] | tojson }}"
        )
        silver_task = BashOperator(
            task_id=f"apply_silver_{table}",
            cwd="/opt/lakehouse",
            bash_command=(
                "python -m projects.finance.ingestion.silver.apply_silver"
                f" --table {table}"
                ' --bronze-files-json "$BRONZE_FILES_JSON"'
                ' --max-lsn "$MAX_LSN"'
            ),
            env={
                "BRONZE_FILES_JSON": files_template,
                "MAX_LSN": cutoff_template,
            },
            append_env=True,
            execution_timeout=timedelta(minutes=10),
        )
        silver_tasks.append(silver_task)

    reconcile_batch = BashOperator(
        task_id="reconcile_batch",
        cwd="/opt/lakehouse",
        bash_command="python -m projects.finance.scripts.reconciliation.check_batch",
        env={
            "BATCH_MANIFEST_JSON": (
                "{{ ti.xcom_pull(task_ids='capture_batch') | tojson }}"
            ),
        },
        append_env=True,
        execution_timeout=timedelta(minutes=15),
    )

    controlled_failure = BashOperator(
        task_id="controlled_failure",
        bash_command="""
            set -e
            if [ "{{ params.force_quality_gate_failure | lower }}" = "true" ]; then
                echo "Falha controlada solicitada pela DagRun."
                exit 42
            fi
            echo "Quality gate liberado."
        """,
        retries=1,
        retry_delay=timedelta(seconds=15),
        execution_timeout=timedelta(minutes=2),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        cwd="/opt/lakehouse/projects/finance/transform/dbt_project",
        bash_command="dbt build --profiles-dir .",
        execution_timeout=timedelta(minutes=30),
    )

    checkpoint = commit_checkpoint(batch)

    for silver_task in silver_tasks:
        batch_available >> silver_task >> reconcile_batch

    reconcile_batch >> controlled_failure >> dbt_build >> checkpoint
