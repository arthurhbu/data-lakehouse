"""Testes locais do envio OpenSky; não acessam MinIO nem Kafka reais."""

import hashlib
import io
import json
from types import SimpleNamespace

import pytest

from projects.aviation.collector.kafka_sender import (
    SenderError,
    event_from_state,
    events_from_snapshot,
    publish_events,
    retrieve_snapshot,
)


OBJECT_KEY = "aviation/opensky/dt=2026-09-25/hour=14/snapshot_id=snapshot-1.json"
STATE = [
    "e49ef3", "GLO7480 ", "Brazil", 1790339077, 1790339077,
    -46.2582, -23.5054, 2987.04, False, 148.21, 191.82, 17.56,
    None, 3139.44, None, False, 0,
]


class FakeResponse:
    def __init__(self, raw):
        self.stream = io.BytesIO(raw)
        self.closed = False
        self.released = False

    def read(self):
        return self.stream.read()

    def close(self):
        self.closed = True

    def release_conn(self):
        self.released = True


class FakeMinio:
    def __init__(self, states, *, metadata_override=None):
        raw = json.dumps({"time": 1790339077, "states": states}).encode()
        self.response = FakeResponse(raw)
        metadata = {
            "X-Amz-Meta-snapshot-id": "snapshot-1",
            "X-Amz-Meta-request-id": "request-1",
            "X-Amz-Meta-collected-at": "2026-09-25T14:48:21+00:00",
            "X-Amz-Meta-source-time": "1790339077",
            "X-Amz-Meta-state-count": str(len(states or [])),
            "X-Amz-Meta-sha256": hashlib.sha256(raw).hexdigest(),
        }
        metadata.update(metadata_override or {})
        self.stored = SimpleNamespace(metadata=metadata, size=len(raw))

    def stat_object(self, bucket, key):
        assert (bucket, key) == ("bronze", OBJECT_KEY)
        return self.stored

    def get_object(self, bucket, key):
        assert (bucket, key) == ("bronze", OBJECT_KEY)
        return self.response


class FakeProducer:
    def __init__(self, *, fail=False, pending=0):
        self.messages = []
        self.fail = fail
        self.pending = pending
        self.callbacks = []

    def produce(self, topic, *, key, value, on_delivery):
        self.messages.append((topic, key, json.loads(value)))
        self.callbacks.append(on_delivery)

    def poll(self, timeout):
        assert timeout == 0

    def flush(self, timeout):
        assert timeout == 35
        for callback in self.callbacks:
            callback("delivery error" if self.fail else None, None)
        return self.pending


def test_snapshot_e_metadados_originais_sao_preservados():
    minio = FakeMinio([STATE])
    snapshot = retrieve_snapshot(minio, "bronze", OBJECT_KEY)
    assert snapshot.snapshot_id == "snapshot-1"
    assert snapshot.request_id == "request-1"
    assert snapshot.collected_at == "2026-09-25T14:48:21Z"
    assert snapshot.source_time == 1790339077
    assert snapshot.states == [STATE]
    assert minio.response.closed and minio.response.released


def test_um_evento_por_aeronave_com_campos_nomeados_e_id_estavel():
    snapshot = retrieve_snapshot(
        FakeMinio([STATE, [*STATE[:1], "AZU1000 ", *STATE[2:]]]),
        "bronze",
        OBJECT_KEY,
    )
    events = events_from_snapshot(snapshot)
    assert len(events) == 2
    assert events[0].icao24 == "e49ef3"
    assert events[0].callsign == "GLO7480"
    assert events[0].longitude == -46.2582
    assert events[0].latitude == -23.5054
    assert events[0].observed_at == "2026-09-25T12:24:37Z"
    assert events[0].squawk is None
    assert events[0].category is None
    assert events[0].snapshot_id == "snapshot-1"
    assert events[0].event_id == event_from_state(snapshot, STATE).event_id
    assert events[0].event_id != events[1].event_id


def test_nulls_e_categoria_opcional():
    state = [*STATE]
    state[3] = None
    state[5] = None
    state[6] = None
    state[17:] = [4]
    event = events_from_snapshot(retrieve_snapshot(FakeMinio([state]), "bronze", OBJECT_KEY))[0]
    assert event.observed_at is None
    assert event.longitude is None
    assert event.latitude is None
    assert event.category == 4


def test_snapshot_vazio_e_valido():
    snapshot = retrieve_snapshot(FakeMinio(None), "bronze", OBJECT_KEY)
    assert events_from_snapshot(snapshot) == []


def test_checksum_divergente_bloqueia_publicacao():
    minio = FakeMinio([STATE], metadata_override={"X-Amz-Meta-sha256": "wrong"})
    with pytest.raises(SenderError, match="Checksum"):
        retrieve_snapshot(minio, "bronze", OBJECT_KEY)


def test_vetor_invalido_bloqueia_antes_do_envio():
    snapshot = retrieve_snapshot(FakeMinio([STATE, ["bad"]]), "bronze", OBJECT_KEY)
    with pytest.raises(SenderError, match="17 ou 18"):
        events_from_snapshot(snapshot)


def test_publica_json_individual_com_chave_icao24_e_confirmacao():
    events = events_from_snapshot(retrieve_snapshot(FakeMinio([STATE]), "bronze", OBJECT_KEY))
    producer = FakeProducer()
    assert publish_events(producer, "aviation.opensky.state_vectors.v1", events) == 1
    topic, key, value = producer.messages[0]
    assert topic == "aviation.opensky.state_vectors.v1"
    assert key == b"e49ef3"
    assert value["event_id"] == events[0].event_id
    assert value["source_bucket"] == "bronze"
    assert value["source_object_key"] == OBJECT_KEY


@pytest.mark.parametrize("fail,pending", [(True, 0), (False, 1)])
def test_falha_de_entrega_nao_retorna_sucesso(fail, pending):
    events = events_from_snapshot(retrieve_snapshot(FakeMinio([STATE]), "bronze", OBJECT_KEY))
    with pytest.raises(SenderError, match="Publicação incompleta"):
        publish_events(FakeProducer(fail=fail, pending=pending), "topic", events)
