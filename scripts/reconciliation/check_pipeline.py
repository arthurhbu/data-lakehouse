"""Reconciliação de contagem e invariante contábil entre as camadas."""

from __future__ import annotations

import os
from decimal import Decimal
from urllib.parse import urlparse

import duckdb
import psycopg2
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from rich.console import Console
from rich.table import Table

from ingestion.cdc_contract import create_latest_cdc_view
from ingestion.silver.silver_schemas import TABLE_CONFIGS


load_dotenv()
console = Console()


def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "datalakehouse"),
        user=os.getenv("POSTGRES_USER", "lakehouse"),
        password=os.environ["POSTGRES_PASSWORD"],
    )


def get_catalog():
    return load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": os.getenv("CATALOG_URI", "http://localhost:8181").rstrip("/") + "/",
            "s3.endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            "s3.access-key-id": os.environ["MINIO_ROOT_USER"],
            "s3.secret-access-key": os.environ["MINIO_ROOT_PASSWORD"],
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def get_postgres_count(connection, table_name: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]


def _configure_duckdb_s3(connection) -> None:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    parsed = urlparse(endpoint)
    endpoint_host = parsed.netloc or parsed.path
    use_ssl = str(endpoint.startswith("https://")).lower()
    user = os.environ["MINIO_ROOT_USER"].replace("'", "''")
    password = os.environ["MINIO_ROOT_PASSWORD"].replace("'", "''")
    endpoint_host = endpoint_host.replace("'", "''")

    connection.execute("INSTALL httpfs; LOAD httpfs;")
    connection.execute(
        f"""
        CREATE OR REPLACE SECRET minio_secret (
            TYPE S3,
            KEY_ID '{user}',
            SECRET '{password}',
            REGION 'us-east-1',
            ENDPOINT '{endpoint_host}',
            USE_SSL {use_ssl},
            URL_STYLE 'path'
        )
        """
    )


def get_bronze_count(table_name: str, config: dict) -> int:
    connection = duckdb.connect()
    try:
        _configure_duckdb_s3(connection)
        path = f"s3://bronze/topics/cdc.public.{table_name}/*/*/*/*.json"
        create_latest_cdc_view(
            connection,
            path,
            config["schema"],
            config["primary_key"],
        )
        return connection.execute(
            "SELECT COUNT(*) FROM bronze_latest WHERE op != 'd'"
        ).fetchone()[0]
    finally:
        connection.close()


def get_silver_table(catalog, table_name: str):
    return catalog.load_table(f"silver.{table_name}")


def get_silver_count(catalog, table_name: str) -> int:
    return len(
        get_silver_table(catalog, table_name)
        .scan(row_filter=EqualTo("_cdc_deleted", False))
        .to_arrow()
    )


def get_postgres_ledger_total(connection) -> Decimal:
    with connection.cursor() as cursor:
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM ledger_entries")
        return cursor.fetchone()[0]


def get_silver_ledger_total(catalog) -> Decimal:
    ledger = (
        get_silver_table(catalog, "ledger_entries")
        .scan(row_filter=EqualTo("_cdc_deleted", False))
        .to_arrow()
    )
    connection = duckdb.connect()
    try:
        connection.register("silver_ledger", ledger)
        return connection.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM silver_ledger"
        ).fetchone()[0]
    finally:
        connection.close()


def main() -> None:
    postgres = get_postgres_connection()
    catalog = get_catalog()
    counts_match = True

    report = Table(title="Reconciliação ponta a ponta")
    report.add_column("Tabela", style="cyan")
    report.add_column("Postgres", style="magenta", justify="right")
    report.add_column("Bronze atual", style="yellow", justify="right")
    report.add_column("Silver", style="blue", justify="right")
    report.add_column("Status", justify="center")

    try:
        for table_name, config in TABLE_CONFIGS.items():
            postgres_count = get_postgres_count(postgres, table_name)
            bronze_count = get_bronze_count(table_name, config)
            silver_count = get_silver_count(catalog, table_name)
            synchronized = postgres_count == bronze_count == silver_count
            counts_match = counts_match and synchronized
            status = (
                "[bold green]OK[/bold green]"
                if synchronized
                else "[bold red]DIVERGENTE[/bold red]"
            )
            report.add_row(
                table_name,
                str(postgres_count),
                str(bronze_count),
                str(silver_count),
                status,
            )

        postgres_total = get_postgres_ledger_total(postgres)
        silver_total = get_silver_ledger_total(catalog)
    finally:
        postgres.close()

    console.print(report)
    console.print(
        f"Ledger Postgres={postgres_total} | Silver={silver_total} | "
        f"Soma Zero={'OK' if postgres_total == silver_total == 0 else 'FALHOU'}"
    )

    if not counts_match or postgres_total != silver_total or silver_total != 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
