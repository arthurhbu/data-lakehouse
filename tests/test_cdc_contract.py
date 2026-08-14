import json
import uuid
from decimal import Decimal

import duckdb
import pytest
from pyiceberg.types import DecimalType, TimestamptzType

from ingestion.cdc_contract import CdcContractError, create_latest_cdc_view
from ingestion.silver.silver_schemas import TABLE_CONFIGS


def _event(entry_id, lsn, op, amount=None, wrapped=False):
    before = None
    after = None
    if op == "d":
        before = {"entry_id": entry_id}
    else:
        after = {
            "entry_id": entry_id,
            "transaction_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "entry_type": "debit",
            "amount": amount,
            "currency": "BRL",
            "description": "teste",
            "created_at": "2026-08-13T12:00:00Z",
        }

    payload = {
        "before": before,
        "after": after,
        "source": {"lsn": lsn},
        "op": op,
    }
    return {"schema": {"type": "struct"}, "payload": payload} if wrapped else payload


def _write_json_lines(path, events):
    path.write_text(
        "\n".join(json.dumps(event) for event in events) + "\n",
        encoding="utf-8",
    )


def test_contract_supports_wrapped_and_unwrapped_and_uses_latest_lsn(tmp_path):
    first_id = "11111111-1111-1111-1111-111111111111"
    deleted_id = "22222222-2222-2222-2222-222222222222"
    source = tmp_path / "ledger.json"
    _write_json_lines(
        source,
        [
            _event(first_id, 10, "c", "10.0000"),
            _event(first_id, 20, "u", "12.3400", wrapped=True),
            _event(deleted_id, 15, "c", "5.0000"),
            _event(deleted_id, 30, "d", wrapped=True),
        ],
    )

    config = TABLE_CONFIGS["ledger_entries"]
    connection = duckdb.connect()
    try:
        create_latest_cdc_view(
            connection, str(source), config["schema"], config["primary_key"]
        )
        rows = connection.execute(
            "SELECT entry_id, op, amount, _cdc_lsn, _cdc_deleted "
            "FROM bronze_latest ORDER BY entry_id"
        ).fetchall()
    finally:
        connection.close()

    assert rows == [
        (uuid.UUID(first_id), "u", Decimal("12.3400"), 20, False),
        (uuid.UUID(deleted_id), "d", None, 30, True),
    ]


def test_contract_fails_when_lsn_is_missing(tmp_path):
    source = tmp_path / "invalid.json"
    event = _event("33333333-3333-3333-3333-333333333333", 10, "c", "1.0000")
    del event["source"]["lsn"]
    _write_json_lines(source, [event])

    config = TABLE_CONFIGS["ledger_entries"]
    connection = duckdb.connect()
    try:
        with pytest.raises(CdcContractError, match="source.lsn ausente"):
            create_latest_cdc_view(
                connection, str(source), config["schema"], config["primary_key"]
            )
    finally:
        connection.close()


def test_invalid_decimal_fails_fast(tmp_path):
    source = tmp_path / "invalid_decimal.json"
    _write_json_lines(
        source,
        [_event("44444444-4444-4444-4444-444444444444", 10, "c", "NaN")],
    )

    config = TABLE_CONFIGS["ledger_entries"]
    connection = duckdb.connect()
    try:
        with pytest.raises(duckdb.ConversionException):
            create_latest_cdc_view(
                connection, str(source), config["schema"], config["primary_key"]
            )
    finally:
        connection.close()


def test_financial_and_temporal_types_are_exact():
    transaction_fields = {
        field.name: field.field_type
        for field in TABLE_CONFIGS["transactions"]["schema"].fields
    }
    ledger_fields = {
        field.name: field.field_type
        for field in TABLE_CONFIGS["ledger_entries"]["schema"].fields
    }

    assert transaction_fields["amount"] == DecimalType(18, 4)
    assert transaction_fields["fx_rate"] == DecimalType(18, 8)
    assert transaction_fields["created_at"] == TimestamptzType()
    assert ledger_fields["amount"] == DecimalType(18, 4)


@pytest.mark.parametrize("table_name", ["partners", "accounts"])
def test_dimension_contracts_keep_status_and_audit_timestamps(table_name):
    fields = {
        field.name for field in TABLE_CONFIGS[table_name]["schema"].fields
    }
    assert {"status", "created_at", "updated_at"} <= fields
