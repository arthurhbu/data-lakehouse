import json
from pathlib import Path

import pytest

from scripts.register_connectors import load_connector_payload


CONNECTOR_DIRECTORY = Path("infra/kafka/connectors")


@pytest.fixture(autouse=True)
def connector_environment(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_password")
    monkeypatch.setenv("POSTGRES_DB", "test_database")
    monkeypatch.setenv("MINIO_ROOT_USER", "test_minio")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "test_minio_password")


@pytest.mark.parametrize(
    ("filename", "expected_name"),
    [
        ("elysium-datalake.json", "elysium-datalake"),
        ("s3-sink-bronze.json", "s3-sink-bronze"),
    ],
)
def test_connector_payloads_are_uniform_and_expand_secrets(filename, expected_name):
    name, config = load_connector_payload(CONNECTOR_DIRECTORY / filename)

    assert name == expected_name
    assert "${" not in json.dumps(config)
    assert "connector.class" in config


def test_missing_required_environment_variable_fails(monkeypatch):
    monkeypatch.delenv("POSTGRES_PASSWORD")

    with pytest.raises(RuntimeError, match="POSTGRES_PASSWORD"):
        load_connector_payload(CONNECTOR_DIRECTORY / "elysium-datalake.json")
