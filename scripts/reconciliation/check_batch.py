"""Valida se as versões do micro-batch chegaram corretamente à Silver."""

from __future__ import annotations

import argparse
import json
import os
import uuid

import duckdb
from rich.console import Console
from rich.table import Table

from ingestion.cdc_contract import create_latest_cdc_view
from ingestion.silver.silver_schemas import TABLE_CONFIGS
from scripts.reconciliation.check_pipeline import (
    _configure_duckdb_s3,
    get_catalog,
    get_silver_ledger_total,
    get_silver_table,
)


console = Console()


def _normalized_key(value) -> str:
    if isinstance(value, (bytes, bytearray)) and len(value) == 16:
        return str(uuid.UUID(bytes=bytes(value)))
    return str(value)


def check_table_batch(catalog, table_name: str, files: list[str], max_lsn: int) -> int:
    config = TABLE_CONFIGS[table_name]
    connection = duckdb.connect()
    try:
        _configure_duckdb_s3(connection)
        create_latest_cdc_view(
            connection,
            files,
            config["schema"],
            config["primary_key"],
            max_lsn=max_lsn,
        )
        incoming = connection.execute(
            f"SELECT {config['primary_key']}, _cdc_lsn, _cdc_deleted "
            "FROM bronze_latest"
        ).fetchall()
    finally:
        connection.close()

    silver = get_silver_table(catalog, table_name).scan(
        selected_fields=(config["primary_key"], "_cdc_lsn", "_cdc_deleted")
    ).to_arrow()
    silver_by_key = {
        _normalized_key(key): (lsn, deleted)
        for key, lsn, deleted in zip(
            silver.column(config["primary_key"]).to_pylist(),
            silver.column("_cdc_lsn").to_pylist(),
            silver.column("_cdc_deleted").to_pylist(),
        )
    }

    failures = []
    for key, incoming_lsn, incoming_deleted in incoming:
        normalized_key = _normalized_key(key)
        current = silver_by_key.get(normalized_key)
        if current is None:
            failures.append(f"{normalized_key}: ausente na Silver")
            continue

        silver_lsn, silver_deleted = current
        if silver_lsn < incoming_lsn:
            failures.append(
                f"{normalized_key}: Silver LSN {silver_lsn} < Bronze LSN {incoming_lsn}"
            )
        elif silver_lsn == incoming_lsn and silver_deleted != incoming_deleted:
            failures.append(
                f"{normalized_key}: tombstone divergente no LSN {incoming_lsn}"
            )

    if failures:
        raise RuntimeError(
            f"Micro-batch divergente em {table_name}: " + "; ".join(failures[:10])
        )
    return len(incoming)


def check_batch(manifest: dict) -> None:
    catalog = get_catalog()
    max_lsn = int(manifest["max_lsn"])
    report = Table(title=f"Gate incremental — LSN <= {max_lsn}")
    report.add_column("Tabela", style="cyan")
    report.add_column("Chaves validadas", justify="right")
    report.add_column("Status", justify="center")

    for table_name, table_batch in manifest["tables"].items():
        if table_name not in TABLE_CONFIGS:
            continue
        checked = check_table_batch(
            catalog,
            table_name,
            table_batch["files"],
            max_lsn,
        )
        report.add_row(table_name, str(checked), "[bold green]OK[/bold green]")

    silver_ledger_total = get_silver_ledger_total(catalog)
    ledger_ok = silver_ledger_total == 0
    console.print(report)
    console.print(
        f"Ledger Silver={silver_ledger_total} | "
        f"Soma Zero={'OK' if ledger_ok else 'FALHOU'}"
    )
    if not ledger_ok:
        raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Valida na Silver somente as chaves do micro-batch."
    )
    parser.add_argument(
        "--manifest-json",
        default=os.getenv("BATCH_MANIFEST_JSON"),
    )
    args = parser.parse_args()
    if not args.manifest_json:
        parser.error("Informe --manifest-json ou BATCH_MANIFEST_JSON.")
    check_batch(json.loads(args.manifest_json))


if __name__ == "__main__":
    main()
