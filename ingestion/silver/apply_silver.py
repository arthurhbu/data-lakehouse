"""Aplica o estado mais recente da Bronze em tabelas Iceberg Silver."""

from __future__ import annotations

import argparse
import os
import traceback
import uuid
from urllib.parse import urlparse

import duckdb
import pyarrow as pa
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import In
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import DayTransform
from pyiceberg.types import UUIDType

from ingestion.cdc_contract import create_latest_cdc_view
from ingestion.silver.silver_schemas import TABLE_CONFIGS


load_dotenv()


class SchemaContractError(RuntimeError):
    """Indica incompatibilidade entre o contrato e uma tabela já existente."""


def _endpoint_without_scheme(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    return parsed.netloc or parsed.path


def _schema_signature(schema):
    return [
        (field.name, str(field.field_type), field.required) for field in schema.fields
    ]


def apply_keyed_microbatch(
    table,
    primary_key: str,
    upsert_data: pa.Table,
) -> tuple[int, int]:
    """Substitui atomicamente apenas as chaves afetadas pelo micro-batch.

    ``Table.upsert`` não suporta agrupamento por ``arrow.uuid`` na combinação
    atual de PyIceberg/PyArrow. Delete + append dentro da mesma transação
    Iceberg mantém o resultado idempotente sem sobrescrever a tabela inteira.
    """

    upsert_keys = upsert_data.column(primary_key).to_pylist()
    candidate_keys = list(dict.fromkeys(upsert_keys))
    if not candidate_keys:
        return 0, 0

    existing_lsn = {}
    if table.current_snapshot() is not None:
        existing = table.scan(
            selected_fields=(primary_key, "_cdc_lsn"),
        ).to_arrow()
        existing_lsn = {
            key: lsn
            for key, lsn in zip(
                existing.column(primary_key).to_pylist(),
                existing.column("_cdc_lsn").to_pylist(),
            )
        }

    incoming_lsn = upsert_data.column("_cdc_lsn").to_pylist()
    apply_mask = [
        key not in existing_lsn or lsn > existing_lsn[key]
        for key, lsn in zip(upsert_keys, incoming_lsn)
    ]
    accepted_upserts = upsert_data.filter(pa.array(apply_mask))
    accepted_upsert_keys = accepted_upserts.column(primary_key).to_pylist()
    affected_keys = list(dict.fromkeys(accepted_upsert_keys))
    ignored_stale = upsert_data.num_rows - accepted_upserts.num_rows
    if not affected_keys:
        return 0, ignored_stale

    with table.transaction() as transaction:
        keys_to_replace = [key for key in affected_keys if key in existing_lsn]
        if keys_to_replace:
            transaction.delete(
                In(primary_key, keys_to_replace),
                snapshot_properties={
                    "pipeline": "bronze-to-silver",
                    "cdc.action": "replace-affected-keys",
                },
            )
        if accepted_upserts.num_rows:
            transaction.append(
                accepted_upserts,
                snapshot_properties={
                    "pipeline": "bronze-to-silver",
                    "cdc.action": "append-latest-state",
                },
            )

    return accepted_upserts.num_rows, ignored_stale


class SilverJob:
    def __init__(
        self,
        table_name: str,
        primary_key: str,
        schema,
        date_str: str | None = None,
        timestamp_col: str | None = None,
    ):
        self.table_name = table_name
        self.primary_key = primary_key
        self.date_str = date_str
        self.timestamp_col = timestamp_col
        self.schema = schema
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        self.catalog_uri = os.getenv("CATALOG_URI", "http://localhost:8181")

        self.con = duckdb.connect()
        self._setup_extensions_and_secret()
        self._setup_iceberg_catalog()

    def _setup_iceberg_catalog(self):
        print(f"[{self.table_name}] Conectando ao Iceberg REST catalog...")
        self.catalog = load_catalog(
            "default",
            **{
                "type": "rest",
                "uri": self.catalog_uri.rstrip("/") + "/",
                "s3.endpoint": self.minio_endpoint,
                "s3.access-key-id": os.environ["MINIO_ROOT_USER"],
                "s3.secret-access-key": os.environ["MINIO_ROOT_PASSWORD"],
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",
            },
        )
        namespaces = [namespace[0] for namespace in self.catalog.list_namespaces()]
        if "silver" not in namespaces:
            self.catalog.create_namespace(
                "silver", properties={"location": "s3://silver/"}
            )

    def _setup_extensions_and_secret(self):
        self.con.execute("INSTALL httpfs; LOAD httpfs;")
        self.con.execute(
            f"""
            CREATE OR REPLACE SECRET minio_secret (
                TYPE S3,
                KEY_ID {self._sql_literal(os.environ['MINIO_ROOT_USER'])},
                SECRET {self._sql_literal(os.environ['MINIO_ROOT_PASSWORD'])},
                REGION 'us-east-1',
                ENDPOINT {self._sql_literal(_endpoint_without_scheme(self.minio_endpoint))},
                USE_SSL {str(self.minio_endpoint.startswith('https://')).lower()},
                URL_STYLE 'path'
            )
            """
        )

    @staticmethod
    def _sql_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def bronze_path(self) -> str:
        if self.date_str:
            year, month, day = self.date_str.split("-")
            return (
                f"s3://bronze/topics/cdc.public.{self.table_name}/"
                f"year={year}/month={month}/day={day}/*.json"
            )
        return f"s3://bronze/topics/cdc.public.{self.table_name}/*/*/*/*.json"

    def retrieve_data(self) -> None:
        print(f"[{self.table_name}] Validando e deduplicando a Bronze...")
        create_latest_cdc_view(
            self.con,
            self.bronze_path(),
            self.schema,
            self.primary_key,
        )

    def _partition_spec(self) -> PartitionSpec:
        if not self.timestamp_col:
            return PartitionSpec()

        timestamp_field = self.schema.find_field(self.timestamp_col)
        return PartitionSpec(
            PartitionField(
                source_id=timestamp_field.field_id,
                field_id=1000,
                transform=DayTransform(),
                name=f"{self.timestamp_col}_day",
            )
        )

    def _load_or_create_table(self):
        table_identifier = f"silver.{self.table_name}"
        try:
            table = self.catalog.load_table(table_identifier)
        except NoSuchTableError:
            table = self.catalog.create_table(
                table_identifier,
                schema=self.schema,
                partition_spec=self._partition_spec(),
                properties={
                    "format-version": "2",
                    "write.target-file-size-bytes": "134217728",
                },
            )
            print(f"[{self.table_name}] Tabela Iceberg criada.")
            return table

        if _schema_signature(table.schema()) != _schema_signature(self.schema):
            raise SchemaContractError(
                f"Schema existente de {table_identifier} diverge do contrato. "
                "Faça uma migração explícita antes de processar novos dados."
            )
        return table

    def _arrow_for_upsert(self, table) -> pa.Table:
        columns = ", ".join(field.name for field in self.schema.fields)
        arrow_table = self.con.execute(
            f"SELECT {columns} FROM bronze_latest"
        ).to_arrow_table()

        converted_columns = []
        for field in self.schema.fields:
            column = arrow_table.column(field.name)
            if isinstance(field.field_type, UUIDType):
                values = column.to_pylist()
                converted_columns.append(
                    pa.array(
                        [uuid.UUID(str(value)).bytes if value is not None else None for value in values],
                        type=pa.binary(16),
                    )
                )
            else:
                converted_columns.append(column)

        normalized = pa.Table.from_arrays(
            converted_columns, names=arrow_table.column_names
        )
        return normalized.cast(schema_to_pyarrow(table.schema()))

    def run(self) -> None:
        self.retrieve_data()
        table = self._load_or_create_table()

        upsert_data = self._arrow_for_upsert(table)
        replaced_rows, ignored_stale = apply_keyed_microbatch(
            table,
            self.primary_key,
            upsert_data,
        )
        tombstones = self.con.execute(
            "SELECT COUNT(*) FROM bronze_latest WHERE op = 'd'"
        ).fetchone()[0]
        print(
            f"[{self.table_name}] Estado aplicado: {replaced_rows} upserts, "
            f"{tombstones} tombstones no lote, "
            f"{ignored_stale} eventos antigos/repetidos ignorados."
        )

        print(f"[{self.table_name}] Concluído com sucesso.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aplica eventos CDC da Bronze em uma tabela Iceberg Silver."
    )
    parser.add_argument("--table", required=True, choices=sorted(TABLE_CONFIGS))
    parser.add_argument("--date", help="Data de processamento em YYYY-MM-DD")
    args = parser.parse_args()

    config = TABLE_CONFIGS[args.table]
    job = None
    try:
        job = SilverJob(
            table_name=args.table,
            primary_key=config["primary_key"],
            schema=config["schema"],
            date_str=args.date,
            timestamp_col=config.get("timestamp_col"),
        )
        job.run()
    except Exception:
        traceback.print_exc()
        raise
    finally:
        if job is not None:
            job.con.close()


if __name__ == "__main__":
    main()
