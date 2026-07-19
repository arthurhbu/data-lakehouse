from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType, TimestampType, UUIDType

TABLE_CONFIGS = {
    "transactions": {
        "primary_key": "transaction_id",
        "timestamp_col": "created_at",
        "schema": Schema(
            NestedField(1, "transaction_id", UUIDType(), required=True),
            NestedField(2, "source_account_id", UUIDType(), required=False),
            NestedField(3, "destination_account_id", UUIDType(), required=False),
            NestedField(4, "amount", DoubleType(), required=False),
            NestedField(5, "currency", StringType(), required=False),
            NestedField(6, "fx_rate", DoubleType(), required=False),
            NestedField(7, "converted_amount", DoubleType(), required=False),
            NestedField(8, "converted_currency", StringType(), required=False),
            NestedField(9, "payment_method", StringType(), required=False),
            NestedField(10, "status", StringType(), required=False),
            NestedField(11, "description", StringType(), required=False),
            NestedField(12, "created_at", TimestampType(), required=False),
            NestedField(13, "updated_at", TimestampType(), required=False)
        )
    },
    "payment_events": {
        "primary_key": "event_id",
        "timestamp_col": "created_at",
        "schema": Schema(
            NestedField(1, "event_id", UUIDType(), required=True),
            NestedField(2, "transaction_id", UUIDType(), required=False),
            NestedField(3, "event_type", StringType(), required=False),
            NestedField(4, "metadata", StringType(), required=False),
            NestedField(5, "created_at", TimestampType(), required=False)
        )
    },
    "ledger_entries": {
        "primary_key": "entry_id",
        "timestamp_col": "created_at",
        "schema": Schema(
            NestedField(1, "entry_id", UUIDType(), required=True),
            NestedField(2, "transaction_id", UUIDType(), required=False),
            NestedField(3, "account_id", UUIDType(), required=False),
            NestedField(4, "entry_type", StringType(), required=False),
            NestedField(5, "amount", DoubleType(), required=False),
            NestedField(6, "currency", StringType(), required=False),
            NestedField(7, "description", StringType(), required=False),
            NestedField(8, "created_at", TimestampType(), required=False)
        )
    },
    "accounts": {
        "primary_key": "account_id",
        "timestamp_col": None,
        "schema": Schema(
            NestedField(1, "account_id", UUIDType(), required=True),
            NestedField(2, "partner_id", UUIDType(), required=False),
            NestedField(3, "currency", StringType(), required=False),
            NestedField(4, "account_type", StringType(), required=False)
        )
    },
    "partners": {
        "primary_key": "partner_id",
        "timestamp_col": None,
        "schema": Schema(
            NestedField(1, "partner_id", UUIDType(), required=True),
            NestedField(2, "name", StringType(), required=False),
            NestedField(3, "country", StringType(), required=False),
            NestedField(4, "partner_type", StringType(), required=False)
        )
    }
}