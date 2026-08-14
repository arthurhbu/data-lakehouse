"""Contrato compartilhado para leitura dos eventos CDC persistidos na Bronze.

O S3 Sink pode gravar o envelope Debezium diretamente ou envolvido em
``payload``. A normalização aceita os dois formatos, mas falha imediatamente
quando os metadados necessários para idempotência não estão presentes.
"""

from __future__ import annotations

from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
    UUIDType,
)


SUPPORTED_CDC_OPERATIONS = ("c", "r", "u", "d")


class CdcContractError(ValueError):
    """Indica que um lote Bronze não atende ao contrato mínimo do CDC."""


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _json_scalar(json_column: str, *paths: str) -> str:
    extracts = ", ".join(
        f"json_extract_string({json_column}, {_sql_literal(path)})" for path in paths
    )
    return f"COALESCE({extracts})"


def _duckdb_type(field_type) -> str:
    if isinstance(field_type, UUIDType):
        return "UUID"
    if isinstance(field_type, DecimalType):
        return f"DECIMAL({field_type.precision}, {field_type.scale})"
    if isinstance(field_type, DoubleType):
        return "DOUBLE"
    if isinstance(field_type, TimestamptzType):
        return "TIMESTAMPTZ"
    if isinstance(field_type, TimestampType):
        return "TIMESTAMP"
    if isinstance(field_type, StringType):
        return "VARCHAR"
    if isinstance(field_type, (IntegerType, LongType)):
        return "BIGINT"
    if isinstance(field_type, BooleanType):
        return "BOOLEAN"
    raise TypeError(f"Tipo Iceberg ainda não suportado no contrato CDC: {field_type}")


def build_normalized_cdc_sql(path: str, schema: Schema, primary_key: str) -> str:
    """Cria o SQL que normaliza envelopes Debezium wrapped e unwrapped."""

    lsn_expr = _json_scalar(
        "event_json", "$.source.lsn", "$.payload.source.lsn"
    )
    op_expr = _json_scalar("event_json", "$.op", "$.payload.op")
    field_exprs = []
    for field in schema.fields:
        if field.name == "_cdc_lsn":
            field_exprs.append(f"CAST({lsn_expr} AS BIGINT) AS _cdc_lsn")
            continue
        if field.name == "_cdc_deleted":
            field_exprs.append(f"CAST({op_expr} = 'd' AS BOOLEAN) AS _cdc_deleted")
            continue

        after_expr = _json_scalar(
            "event_json",
            f"$.after.{field.name}",
            f"$.payload.after.{field.name}",
        )
        if field.name == primary_key:
            before_expr = _json_scalar(
                "event_json",
                f"$.before.{field.name}",
                f"$.payload.before.{field.name}",
            )
            raw_expr = f"COALESCE({after_expr}, {before_expr})"
        else:
            raw_expr = after_expr

        field_exprs.append(
            f"CAST({raw_expr} AS {_duckdb_type(field.field_type)}) AS {field.name}"
        )

    select_fields = ",\n                ".join(field_exprs)

    return f"""
        CREATE OR REPLACE TEMP TABLE bronze_normalized AS
        WITH bronze_raw AS (
            SELECT json AS event_json
            FROM read_json_objects({_sql_literal(path)}, format = 'newline_delimited')
        )
        SELECT
            {op_expr} AS op,
            CAST({lsn_expr} AS BIGINT) AS source_lsn,
            {select_fields}
        FROM bronze_raw
    """


def create_latest_cdc_view(
    connection,
    path: str,
    schema: Schema,
    primary_key: str,
) -> None:
    """Normaliza, valida e deduplica um lote CDC pelo maior LSN por chave."""

    connection.execute(build_normalized_cdc_sql(path, schema, primary_key))

    invalid = connection.execute(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE op IS NULL) AS missing_op,
            COUNT(*) FILTER (WHERE source_lsn IS NULL) AS missing_lsn,
            COUNT(*) FILTER (WHERE {primary_key} IS NULL) AS missing_primary_key,
            COUNT(*) FILTER (
                WHERE op IS NOT NULL
                  AND op NOT IN {SUPPORTED_CDC_OPERATIONS}
            ) AS unsupported_op
        FROM bronze_normalized
        """
    ).fetchone()

    labels = ("op ausente", "source.lsn ausente", "PK ausente", "op inválida")
    failures = [f"{label}: {count}" for label, count in zip(labels, invalid) if count]
    if failures:
        raise CdcContractError("Lote Bronze inválido — " + "; ".join(failures))

    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW bronze_latest AS
        SELECT *
        FROM bronze_normalized
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY {primary_key}
            ORDER BY source_lsn DESC
        ) = 1
        """
    )
