"""Preserva tabelas Silver legadas antes da adoção do contrato CDC v2."""

from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

from ingestion.silver.silver_schemas import TABLE_CONFIGS


LEGACY_NAMESPACE = "silver_legacy"
MIGRATION_SUFFIX = "pre_contract_v2"


def _signature(schema):
    return [(field.name, str(field.field_type), field.required) for field in schema.fields]


def get_catalog():
    return load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": os.environ["CATALOG_URI"].rstrip("/") + "/",
            "s3.endpoint": os.environ["MINIO_ENDPOINT"],
            "s3.access-key-id": os.environ["MINIO_ROOT_USER"],
            "s3.secret-access-key": os.environ["MINIO_ROOT_PASSWORD"],
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def migration_plan(catalog) -> list[tuple[str, str]]:
    plan = []
    for table_name, config in TABLE_CONFIGS.items():
        source = f"silver.{table_name}"
        try:
            table = catalog.load_table(source)
        except NoSuchTableError:
            continue

        if _signature(table.schema()) == _signature(config["schema"]):
            continue
        target = f"{LEGACY_NAMESPACE}.{table_name}_{MIGRATION_SUFFIX}"
        try:
            catalog.load_table(target)
        except NoSuchTableError:
            plan.append((source, target))
        else:
            raise RuntimeError(
                f"Backup de destino já existe: {target}. "
                "A migração foi interrompida para não sobrescrever dados."
            )
    return plan


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Move tabelas Silver incompatíveis para silver_legacy."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Executa a migração. Sem esta flag, apenas exibe o plano.",
    )
    args = parser.parse_args()

    load_dotenv(".env")
    catalog = get_catalog()
    plan = migration_plan(catalog)
    if not plan:
        print("Nenhuma tabela Silver incompatível encontrada.")
        return

    print("Plano de migração:")
    for source, target in plan:
        print(f"  {source} -> {target}")

    if not args.apply:
        print("Dry-run concluído. Use --apply para executar.")
        return

    namespaces = {namespace[0] for namespace in catalog.list_namespaces()}
    if LEGACY_NAMESPACE not in namespaces:
        catalog.create_namespace(
            LEGACY_NAMESPACE,
            properties={"location": "s3://silver/legacy/"},
        )

    for source, target in plan:
        catalog.rename_table(source, target)
        print(f"Preservada: {target}")


if __name__ == "__main__":
    main()
