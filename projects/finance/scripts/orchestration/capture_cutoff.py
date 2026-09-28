"""Captura o maior LSN atualmente visível na Bronze."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import duckdb
from dotenv import load_dotenv

from projects.finance.ingestion.cdc_contract import build_normalized_cdc_sql
from projects.finance.ingestion.silver.silver_schemas import TABLE_CONFIGS


load_dotenv()


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


def capture_cutoff() -> int:
    connection = duckdb.connect()
    try:
        _configure_duckdb_s3(connection)
        table_cutoffs = []

        for table_name, config in TABLE_CONFIGS.items():
            path = (
                f"s3://bronze/topics/cdc.public.{table_name}"
                "/*/*/*/*.json"
            )
            connection.execute(
                build_normalized_cdc_sql(
                    path,
                    config["schema"],
                    config["primary_key"],
                )
            )
            max_lsn = connection.execute(
                "SELECT MAX(source_lsn) FROM bronze_normalized"
            ).fetchone()[0]
            if max_lsn is not None:
                table_cutoffs.append(max_lsn)

        if not table_cutoffs:
            raise RuntimeError("Nenhum LSN foi encontrado na Bronze.")

        return max(table_cutoffs)
    finally:
        connection.close()


def main() -> None:
    print(capture_cutoff())


if __name__ == "__main__":
    main()
