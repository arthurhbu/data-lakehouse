"""Captura os arquivos Bronze ainda não processados."""

from __future__ import annotations

import json
import os

import boto3
import duckdb
from dotenv import load_dotenv

from projects.finance.ingestion.bronze_manifest import build_manifest
from projects.finance.ingestion.cdc_contract import build_normalized_cdc_sql
from projects.finance.ingestion.silver.silver_schemas import TABLE_CONFIGS
from projects.finance.scripts.orchestration.capture_cutoff import _configure_duckdb_s3


load_dotenv()


def list_bronze_keys() -> list[str]:
    """Lista todas as chaves de dados publicadas pelo S3 Sink."""

    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )
    paginator = client.get_paginator("list_objects_v2")
    keys = []

    for page in paginator.paginate(Bucket="bronze", Prefix="topics/"):
        keys.extend(item["Key"] for item in page.get("Contents", []))

    return keys


def capture_manifest(
    checkpoints: dict[str, dict[str, int]],
    last_successful_lsn: int | None = None,
) -> dict:
    """Monta o lote incremental a partir do estado confirmado."""

    manifest = build_manifest(list_bronze_keys(), checkpoints)
    batch_lsns = []
    connection = duckdb.connect()
    try:
        _configure_duckdb_s3(connection)
        for table_name, table_batch in manifest["tables"].items():
            config = TABLE_CONFIGS.get(table_name)
            if config is None:
                continue
            connection.execute(
                build_normalized_cdc_sql(
                    table_batch["files"],
                    config["schema"],
                    config["primary_key"],
                )
            )
            max_lsn = connection.execute(
                "SELECT MAX(source_lsn) FROM bronze_normalized"
            ).fetchone()[0]
            if max_lsn is not None:
                batch_lsns.append(max_lsn)
    finally:
        connection.close()

    known_lsns = batch_lsns.copy()
    if last_successful_lsn is not None:
        known_lsns.append(last_successful_lsn)
    if not known_lsns:
        raise RuntimeError("Nenhum LSN foi encontrado para o micro-batch.")

    manifest["max_lsn"] = max(known_lsns)
    manifest["has_files"] = bool(manifest["tables"])
    return manifest


def main() -> None:
    checkpoints = json.loads(os.getenv("BRONZE_FILE_CHECKPOINTS", "{}"))
    last_lsn = os.getenv("LAST_SUCCESSFUL_LSN")
    manifest = capture_manifest(
        checkpoints,
        int(last_lsn) if last_lsn else None,
    )
    print(json.dumps(manifest, separators=(",", ":")))


if __name__ == "__main__":
    main()
