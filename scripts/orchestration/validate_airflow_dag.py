"""Valida o contrato estrutural da DAG principal."""

from airflow.dag_processing.dagbag import DagBag


EXPECTED_UPSTREAM = {
    "capture_batch": set(),
    "has_new_files": {"capture_batch"},
    "reconcile_batch": {
        "apply_silver_partners",
        "apply_silver_accounts",
        "apply_silver_transactions",
        "apply_silver_payment_events",
        "apply_silver_ledger_entries",
    },
    "controlled_failure": {"reconcile_batch"},
    "dbt_build": {"controlled_failure"},
    "commit_checkpoint": {"capture_batch", "dbt_build"},
}


def main() -> None:
    dag_bag = DagBag("/opt/airflow/dags")
    if dag_bag.import_errors:
        raise RuntimeError(f"Erros de importação: {dag_bag.import_errors}")

    dag = dag_bag.get_dag("lakehouse_pipeline")
    if dag is None:
        raise RuntimeError("DAG lakehouse_pipeline não encontrada.")

    for task_id, expected in EXPECTED_UPSTREAM.items():
        actual = dag.get_task(task_id).upstream_task_ids
        if actual != expected:
            raise RuntimeError(
                f"Dependências inválidas em {task_id}: "
                f"esperado={sorted(expected)}, atual={sorted(actual)}"
            )

    if str(dag.schedule) != "*/15 * * * *":
        raise RuntimeError(f"Schedule inesperado: {dag.schedule}")
    if dag.catchup:
        raise RuntimeError("catchup deve permanecer desabilitado.")
    if dag.max_active_runs != 1:
        raise RuntimeError("max_active_runs deve permanecer igual a 1.")

    print("Contrato estrutural da DAG validado com sucesso.")


if __name__ == "__main__":
    main()
