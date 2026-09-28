"""Publica no Kafka uma observação por aeronave de um snapshot no MinIO.

O envio é manual por chave explícita; o original no MinIO permite replay.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv
from minio import Minio


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TOPIC = "aviation.opensky.state_vectors.v1"
ICAO24_PATTERN = re.compile(r"[0-9a-fA-F]{6}\Z")


class SenderError(RuntimeError):
    """Falha de configuração, contrato do snapshot ou publicação."""


@dataclass(frozen=True)
class SenderConfig:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    kafka_broker: str
    kafka_topic: str

    @classmethod
    def from_env(cls) -> SenderConfig:
        def required(name: str) -> str:
            value = os.getenv(name, "").strip()
            if not value:
                raise SenderError(f"Variável obrigatória ausente: {name}")
            return value

        return cls(
            minio_endpoint=required("MINIO_ENDPOINT"),
            minio_access_key=required("MINIO_ROOT_USER"),
            minio_secret_key=required("MINIO_ROOT_PASSWORD"),
            minio_bucket=required("MINIO_BUCKET_BRONZE"),
            kafka_broker=required("KAFKA_BROKER"),
            kafka_topic=os.getenv("AVIATION_KAFKA_TOPIC", DEFAULT_TOPIC).strip() or DEFAULT_TOPIC,
        )


@dataclass(frozen=True)
class StoredSnapshot:
    source_time: int
    states: list[list[object]]
    snapshot_id: str
    request_id: str
    collected_at: str
    bucket: str
    object_key: str
    sha256: str


@dataclass(frozen=True)
class KafkaEvent:
    schema_version: int
    event_id: str
    source: str
    request_id: str
    snapshot_id: str
    source_time: int
    ingested_at: str
    source_bucket: str
    source_object_key: str
    source_checksum: str
    icao24: str
    callsign: str | None
    origin_country: str
    observed_at: str | None
    last_contact_at: str | None
    longitude: float | None
    latitude: float | None
    baro_altitude_m: float | None
    on_ground: bool
    velocity_m_s: float | None
    true_track_deg: float | None
    vertical_rate_m_s: float | None
    sensors: list[int] | None
    geo_altitude_m: float | None
    squawk: str | None
    spi: bool
    position_source: int
    category: int | None


def minio_from_config(config: SenderConfig) -> Minio:
    endpoint = urlsplit(config.minio_endpoint)
    if (
        endpoint.scheme not in ("http", "https")
        or not endpoint.hostname
        or endpoint.username
        or endpoint.password
        or endpoint.path not in ("", "/")
        or endpoint.query
        or endpoint.fragment
    ):
        raise SenderError("MINIO_ENDPOINT deve ser uma URL como http://localhost:9000")
    return Minio(
        endpoint.netloc,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        secure=endpoint.scheme == "https",
    )


def _metadata_value(metadata: dict, name: str) -> str:
    for key, value in metadata.items():
        if key.lower().removeprefix("x-amz-meta-") == name and isinstance(value, str) and value:
            return value
    raise SenderError(f"Metadado obrigatório ausente no objeto: {name}")


def _integer(value: object, name: str, *, nullable: bool = False) -> int | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise SenderError(f"Campo inteiro inválido: {name}")
    return value


def _timestamp(value: object, name: str, *, nullable: bool = False) -> str | None:
    seconds = _integer(value, name, nullable=nullable)
    if seconds is None:
        return None
    try:
        return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError) as exc:
        raise SenderError(f"Timestamp fora do intervalo: {name}") from exc


def _number(value: object, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise SenderError(f"Campo numérico inválido: {name}")
    return float(value)


def _string(value: object, name: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise SenderError(f"Campo texto inválido: {name}")
    return value.strip()


def _boolean(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise SenderError(f"Campo booleano inválido: {name}")
    return value


def retrieve_snapshot(client: Minio, bucket: str, object_key: str) -> StoredSnapshot:
    """Lê o original e exige os metadados gravados pelo coletor."""
    try:
        stored = client.stat_object(bucket, object_key)
        response = client.get_object(bucket, object_key)
        try:
            raw_bytes = response.read()
        finally:
            response.close()
            response.release_conn()
    except Exception as exc:
        raise SenderError(f"Falha ao ler objeto do MinIO: {bucket}/{object_key}") from exc

    metadata = stored.metadata or {}
    checksum = hashlib.sha256(raw_bytes).hexdigest()
    if checksum != _metadata_value(metadata, "sha256"):
        raise SenderError("Checksum do snapshot diverge do metadado no MinIO")
    if stored.size is not None and stored.size != len(raw_bytes):
        raise SenderError("Tamanho do snapshot diverge do metadado no MinIO")

    try:
        payload = json.loads(raw_bytes)
    except (ValueError, UnicodeDecodeError) as exc:
        raise SenderError("Objeto do MinIO não contém JSON válido") from exc
    if not isinstance(payload, dict):
        raise SenderError("Snapshot deve ser um objeto JSON")
    source_time = _integer(payload.get("time"), "time")
    states = payload.get("states")
    if states is None:
        states = []
    if not isinstance(states, list):
        raise SenderError("Campo states deve ser lista ou null")
    if str(source_time) != _metadata_value(metadata, "source-time"):
        raise SenderError("Horário do snapshot diverge do metadado no MinIO")
    if str(len(states)) != _metadata_value(metadata, "state-count"):
        raise SenderError("Quantidade de vetores diverge do metadado no MinIO")

    collected_at = _metadata_value(metadata, "collected-at")
    try:
        parsed_collected_at = datetime.fromisoformat(collected_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SenderError("Metadado collected-at inválido") from exc
    if parsed_collected_at.tzinfo is None:
        raise SenderError("Metadado collected-at deve ter timezone")
    return StoredSnapshot(
        source_time=source_time,
        states=states,
        snapshot_id=_metadata_value(metadata, "snapshot-id"),
        request_id=_metadata_value(metadata, "request-id"),
        collected_at=(
            parsed_collected_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        ),
        bucket=bucket,
        object_key=object_key,
        sha256=checksum,
    )


def event_from_state(snapshot: StoredSnapshot, state: object) -> KafkaEvent:
    """Converte os índices definidos pela OpenSky em campos nomeados."""
    if not isinstance(state, list) or len(state) not in (17, 18):
        raise SenderError("State vector deve conter 17 ou 18 posições")
    icao24 = _string(state[0], "icao24")
    if not ICAO24_PATTERN.fullmatch(icao24):
        raise SenderError("icao24 inválido no state vector")
    sensors = state[12]
    if sensors is not None and (
        not isinstance(sensors, list)
        or any(isinstance(item, bool) or not isinstance(item, int) for item in sensors)
    ):
        raise SenderError("Campo sensors inválido")

    # Hash do vetor inteiro: republicar a mesma observação mantém o event_id.
    try:
        event_id = hashlib.sha256(
            json.dumps(
                state, ensure_ascii=False, separators=(",", ":"), allow_nan=False
            ).encode("utf-8")
        ).hexdigest()
    except (TypeError, ValueError) as exc:
        raise SenderError("State vector contém valores incompatíveis com JSON") from exc
    return KafkaEvent(
        schema_version=1,
        event_id=event_id,
        source="opensky",
        request_id=snapshot.request_id,
        snapshot_id=snapshot.snapshot_id,
        source_time=snapshot.source_time,
        ingested_at=snapshot.collected_at,
        source_bucket=snapshot.bucket,
        source_object_key=snapshot.object_key,
        source_checksum=snapshot.sha256,
        icao24=icao24.lower(),
        callsign=_string(state[1], "callsign", nullable=True),
        origin_country=_string(state[2], "origin_country"),
        observed_at=_timestamp(state[3], "time_position", nullable=True),
        last_contact_at=_timestamp(state[4], "last_contact", nullable=True),
        longitude=_number(state[5], "longitude"),
        latitude=_number(state[6], "latitude"),
        baro_altitude_m=_number(state[7], "baro_altitude"),
        on_ground=_boolean(state[8], "on_ground"),
        velocity_m_s=_number(state[9], "velocity"),
        true_track_deg=_number(state[10], "true_track"),
        vertical_rate_m_s=_number(state[11], "vertical_rate"),
        sensors=sensors,
        geo_altitude_m=_number(state[13], "geo_altitude"),
        squawk=_string(state[14], "squawk", nullable=True),
        spi=_boolean(state[15], "spi"),
        position_source=_integer(state[16], "position_source"),
        category=_integer(state[17], "category", nullable=True) if len(state) == 18 else None,
    )


def events_from_snapshot(snapshot: StoredSnapshot) -> list[KafkaEvent]:
    """Valida todos os vetores antes de publicar qualquer mensagem."""
    return [event_from_state(snapshot, state) for state in snapshot.states]


def publish_events(producer: Producer, topic: str, events: list[KafkaEvent]) -> int:
    """Espera os delivery reports; a fila local não equivale à entrega."""
    delivered = 0
    failures = 0
    enqueue_error: Exception | None = None

    def on_delivery(error, _message) -> None:
        nonlocal delivered, failures
        if error is None:
            delivered += 1
        else:
            failures += 1

    try:
        for event in events:
            value = json.dumps(asdict(event), ensure_ascii=False, allow_nan=False).encode("utf-8")
            producer.produce(
                topic, key=event.icao24.encode("ascii"), value=value, on_delivery=on_delivery
            )
            producer.poll(0)
    except (BufferError, KafkaException) as exc:
        enqueue_error = exc

    remaining = producer.flush(35)
    if enqueue_error is not None:
        raise SenderError(
            "Kafka rejeitou o enfileiramento; o envio pode ter sido parcial"
        ) from enqueue_error
    if remaining or failures or delivered != len(events):
        raise SenderError(
            f"Publicação incompleta: {delivered}/{len(events)} confirmados, "
            f"{failures} falharam, {remaining} pendentes"
        )
    return delivered


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publica um snapshot OpenSky do MinIO no Kafka")
    parser.add_argument(
        "--object-key", required=True, help="chave exata no bucket, sem o nome do bucket"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="valida e mostra uma amostra, sem publicar"
    )
    args = parser.parse_args(argv)

    load_dotenv(PROJECT_ROOT / ".env")
    try:
        config = SenderConfig.from_env()
        client = minio_from_config(config)
        snapshot = retrieve_snapshot(client, config.minio_bucket, args.object_key)
        events = events_from_snapshot(snapshot)
        if args.dry_run:
            print(json.dumps({
                "status": "validated",
                "snapshot_id": snapshot.snapshot_id,
                "state_count": len(events),
                "sample_event": asdict(events[0]) if events else None,
            }, ensure_ascii=False))
            return 0

        producer = Producer({
            "bootstrap.servers": config.kafka_broker,
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 30000,
        })
        count = publish_events(producer, config.kafka_topic, events)
    except (SenderError, KafkaException) as exc:
        print(f"Envio falhou: {exc}", file=sys.stderr)
        return 1

    print(json.dumps({
        "status": "published",
        "topic": config.kafka_topic,
        "snapshot_id": snapshot.snapshot_id,
        "object_key": snapshot.object_key,
        "event_count": count,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
