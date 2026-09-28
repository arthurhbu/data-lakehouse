"""Seleção incremental dos objetos produzidos pelo S3 Sink."""

from __future__ import annotations

import re
from dataclasses import dataclass

S3_SINK_KEY_PATTERN = re.compile(
    r"^topics/(?P<topic>[^/]+)/"
    r"year=\d{4}/month=\d{2}/day=\d{2}/"
    r"(?P=topic)\+(?P<partition>\d+)\+(?P<offset>\d+)\.json$"
)


@dataclass(frozen=True)
class BronzeObject:
    """Identifica um arquivo imutável produzido pelo S3 Sink."""

    key: str
    topic: str
    partition: int
    start_offset: int

    @property
    def table_name(self) -> str:
        return self.topic.rsplit(".", maxsplit=1)[-1]

    @property
    def uri(self) -> str:
        return f"s3://bronze/{self.key}"


def parse_bronze_key(key: str) -> BronzeObject | None:
    match = S3_SINK_KEY_PATTERN.fullmatch(key)

    if match is None:
        return None

    return BronzeObject(
        key=key,
        topic=match.group("topic"),
        partition=int(match.group("partition")),
        start_offset=int(match.group("offset")),
    )


def select_unprocessed_objects(
    keys: list[str],
    checkpoints: dict[str, dict[str, int]],
) -> list[BronzeObject]:
    """Seleciona objetos posteriores ao checkpoint de cada tópico/partição."""

    selected = []

    for key in keys:
        bronze_object = parse_bronze_key(key)
        if bronze_object is None:
            continue

        topic_checkpoints = checkpoints.get(bronze_object.topic, {})
        last_offset = topic_checkpoints.get(
            str(bronze_object.partition),
            -1,
        )

        if bronze_object.start_offset > last_offset:
            selected.append(bronze_object)

    return sorted(
        selected,
        key=lambda item: (
            item.topic,
            item.partition,
            item.start_offset,
        ),
    )


def build_manifest(
    keys: list[str],
    checkpoints: dict[str, dict[str, int]],
) -> dict:
    """Agrupa arquivos novos por tabela e calcula os próximos checkpoints."""
    selected = select_unprocessed_objects(keys, checkpoints)

    tables = {}

    next_checkpoints = {
        topic: {
            partition: int(offset)
            for partition, offset in partitions.items()
        }
        for topic, partitions in checkpoints.items()
    }

    for bronze_object in selected:
        table = tables.setdefault(
            bronze_object.table_name,
            {"files": []},
        )
        table["files"].append(bronze_object.uri)

        topic_checkpoint = next_checkpoints.setdefault(
            bronze_object.topic,
            {},
        )
        partition = str(bronze_object.partition)

        topic_checkpoint[partition] = max(
            topic_checkpoint.get(partition, -1),
            bronze_object.start_offset,
        )

    return {
        "tables": tables,
        "next_checkpoints": next_checkpoints,
    }
