"""Reconcilia Bronze e Silver usando o mesmo cutoff de LSN."""

from __future__ import annotations

import argparse
from decimal import Decimal

import duckdb
from rich.console import Console
from rich.table import Table

from projects.finance.ingestion.cdc_contract import create_latest_cdc_view
from projects.finance.ingestion.silver.silver_schemas import TABLE_CONFIGS
from projects.finance.scripts.reconciliation.check_pipeline import (
    _configure_duckdb_s3,
    get_catalog,
    get_silver_count,
    get_silver_ledger_total,
)


console = Console()


def get_bronze_state(
    table_name: str,
    config: dict,
    max_lsn: int,
) -> tuple[int, Decimal | None]:
    """Retorna contagem ativa e, para o ledger, seu total no cutoff."""

    connection = duckdb.connect()
    try:
        _configure_duckdb_s3(connection)
        path = f"s3://bronze/topics/cdc.public.{table_name}/*/*/*/*.json"
        create_latest_cdc_view(
            connection,
            path,
            config["schema"],
            config["primary_key"],
            max_lsn=max_lsn,
        )
        active_count = connection.execute(
            "SELECT COUNT(*) FROM bronze_latest WHERE op != 'd'"
        ).fetchone()[0]

        ledger_total = None
        if table_name == "ledger_entries":
            ledger_total = connection.execute(
                """
                SELECT COALESCE(SUM(amount), 0)
                FROM bronze_latest
                WHERE op != 'd'
                """
            ).fetchone()[0]

        return active_count, ledger_total
    finally:
        connection.close()


def check_cutoff(max_lsn: int) -> None:
    catalog = get_catalog()
    counts_match = True
    bronze_ledger_total = None

    report = Table(title=f"Reconciliação Bronze x Silver — LSN <= {max_lsn}")
    report.add_column("Tabela", style="cyan")
    report.add_column("Bronze", style="yellow", justify="right")
    report.add_column("Silver", style="blue", justify="right")
    report.add_column("Status", justify="center")

    for table_name, config in TABLE_CONFIGS.items():
        bronze_count, ledger_total = get_bronze_state(
            table_name,
            config,
            max_lsn,
        )
        silver_count = get_silver_count(catalog, table_name)
        synchronized = bronze_count == silver_count
        counts_match = counts_match and synchronized

        if ledger_total is not None:
            bronze_ledger_total = ledger_total

        report.add_row(
            table_name,
            str(bronze_count),
            str(silver_count),
            "[bold green]OK[/bold green]"
            if synchronized
            else "[bold red]DIVERGENTE[/bold red]",
        )

    silver_ledger_total = get_silver_ledger_total(catalog)
    ledger_matches = bronze_ledger_total == silver_ledger_total == 0

    console.print(report)
    console.print(
        f"Ledger Bronze={bronze_ledger_total} | Silver={silver_ledger_total} | "
        f"Soma Zero={'OK' if ledger_matches else 'FALHOU'}"
    )

    if not counts_match or not ledger_matches:
        raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcilia Bronze e Silver no mesmo cutoff de LSN."
    )
    parser.add_argument("--max-lsn", required=True, type=int)
    args = parser.parse_args()
    check_cutoff(args.max_lsn)


if __name__ == "__main__":
    main()
