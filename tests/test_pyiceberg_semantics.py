import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

from ingestion.silver.apply_silver import apply_keyed_microbatch
from ingestion.silver.silver_schemas import TABLE_CONFIGS


def _warehouse_uri(path):
    # PyArrow no Windows aceita file://C:/..., sem a terceira barra.
    return "file://" + str(path.resolve()).replace("\\", "/")


def _ledger_arrow(schema, entry_id, amount, lsn, deleted=False):
    return pa.Table.from_pylist(
        [
            {
                "entry_id": uuid.UUID(entry_id).bytes,
                "transaction_id": uuid.UUID(
                    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
                ).bytes,
                "account_id": uuid.UUID(
                    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
                ).bytes,
                "entry_type": "debit",
                "amount": Decimal(amount),
                "currency": "BRL",
                "description": "teste",
                "created_at": datetime(2026, 8, 13, 12, tzinfo=timezone.utc),
                "_cdc_lsn": lsn,
                "_cdc_deleted": deleted,
            }
        ],
        schema=schema_to_pyarrow(schema),
    )


def test_pyiceberg_apply_is_idempotent_and_tombstone_blocks_stale_replay(tmp_path):
    schema = TABLE_CONFIGS["ledger_entries"]["schema"]
    catalog = load_catalog(
        "test", type="in-memory", warehouse=_warehouse_uri(tmp_path)
    )
    catalog.create_namespace("silver")
    table = catalog.create_table("silver.ledger_entries", schema=schema)

    first_id = "11111111-1111-1111-1111-111111111111"
    second_id = "22222222-2222-2222-2222-222222222222"
    apply_keyed_microbatch(
        table, "entry_id", _ledger_arrow(schema, first_id, "10.0000", 10)
    )
    apply_keyed_microbatch(
        table, "entry_id", _ledger_arrow(schema, first_id, "12.3400", 20)
    )
    apply_keyed_microbatch(
        table, "entry_id", _ledger_arrow(schema, first_id, "12.3400", 20)
    )
    apply_keyed_microbatch(
        table, "entry_id", _ledger_arrow(schema, second_id, "5.0000", 15)
    )
    # Um replay fora de ordem não pode regredir o estado mais novo (LSN 20).
    apply_keyed_microbatch(
        table, "entry_id", _ledger_arrow(schema, first_id, "1.0000", 11)
    )
    apply_keyed_microbatch(
        table,
        "entry_id",
        _ledger_arrow(schema, second_id, "5.0000", 30, deleted=True),
    )
    # Um create antigo também não pode ressuscitar uma chave deletada.
    apply_keyed_microbatch(
        table, "entry_id", _ledger_arrow(schema, second_id, "9.0000", 20)
    )

    rows = table.scan().to_arrow().to_pylist()
    by_id = {row["entry_id"]: row for row in rows}
    assert len(rows) == 2
    assert by_id[uuid.UUID(first_id)]["amount"] == Decimal("12.3400")
    assert by_id[uuid.UUID(first_id)]["_cdc_lsn"] == 20
    assert by_id[uuid.UUID(first_id)]["_cdc_deleted"] is False
    assert by_id[uuid.UUID(second_id)]["_cdc_lsn"] == 30
    assert by_id[uuid.UUID(second_id)]["_cdc_deleted"] is True
