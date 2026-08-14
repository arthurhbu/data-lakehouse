"""Registra ou atualiza os conectores Kafka Connect de forma idempotente."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import httpx
from dotenv import load_dotenv


PLACEHOLDER = re.compile(r"^\$\{([A-Z][A-Z0-9_]*)\}$")
CONNECTOR_DIRECTORY = Path("infra/kafka/connectors")


def _expand_environment(value):
    if isinstance(value, dict):
        return {key: _expand_environment(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_environment(item) for item in value]
    if isinstance(value, str):
        match = PLACEHOLDER.fullmatch(value)
        if match:
            variable = match.group(1)
            if variable not in os.environ:
                raise RuntimeError(f"Variável obrigatória não definida: {variable}")
            return os.environ[variable]
    return value


def load_connector_payload(path: Path) -> tuple[str, dict]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if set(document) != {"name", "config"}:
        raise ValueError(
            f"{path} deve conter exatamente as chaves de topo 'name' e 'config'."
        )
    return document["name"], _expand_environment(document["config"])


def register_connector(client: httpx.Client, path: Path) -> None:
    name, config = load_connector_payload(path)
    response = client.put(f"/connectors/{name}/config", json=config)
    response.raise_for_status()
    print(f"{name}: registrado/atualizado")


def main() -> None:
    load_dotenv()
    connect_url = os.getenv("CONNECT_REST_URL", "http://localhost:8083")
    connector_paths = sorted(CONNECTOR_DIRECTORY.glob("*.json"))
    if not connector_paths:
        raise RuntimeError(f"Nenhum conector encontrado em {CONNECTOR_DIRECTORY}")

    with httpx.Client(base_url=connect_url, timeout=30.0) as client:
        health = client.get("/")
        health.raise_for_status()
        for path in connector_paths:
            register_connector(client, path)


if __name__ == "__main__":
    main()
