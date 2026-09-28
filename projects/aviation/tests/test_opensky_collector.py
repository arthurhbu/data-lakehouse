"""Testes locais: nenhuma chamada real à OpenSky ou ao MinIO."""

import hashlib
from types import SimpleNamespace
from urllib.parse import parse_qs

import httpx
import pytest

from projects.aviation.collector.opensky_to_minio import (
    CollectionError,
    CollectorConfig,
    collect_once,
    run_periodically,
)


@pytest.fixture
def config():
    return CollectorConfig(
        opensky_client_id="test-client",
        opensky_client_secret="test-secret",
        minio_endpoint="http://localhost:9000",
        minio_access_key="test-access",
        minio_secret_key="test-secret",
        minio_bucket="bronze",
        lamin=-23.8,
        lomin=-46.9,
        lamax=-23.3,
        lomax=-46.2,
    )


class FakeMinio:
    def __init__(self):
        self.objects = {}
        self.put_calls = 0

    def bucket_exists(self, bucket):
        return bucket == "bronze"

    def put_object(self, bucket, key, data, length, *, content_type, metadata):
        self.put_calls += 1
        content = data.read()
        assert len(content) == length
        self.objects[(bucket, key)] = (content, content_type, metadata)

    def stat_object(self, bucket, key):
        return SimpleNamespace(size=len(self.objects[(bucket, key)][0]))


def mock_http(states_response, *, status=200, headers=None):
    def handle(request):
        if request.url.path.endswith("/token"):
            data = parse_qs(request.content.decode())
            assert data["grant_type"] == ["client_credentials"]
            return httpx.Response(200, json={"access_token": "test-token"})
        assert request.headers["Authorization"] == "Bearer test-token"
        assert request.url.params["lamin"] == "-23.8"
        return httpx.Response(status, content=states_response, headers=headers)

    return httpx.Client(transport=httpx.MockTransport(handle))


def test_preserva_bytes_originais_e_metadados(config):
    raw = b'{ "time": 1790339077, "states": [["e49ef3", null]] }\n'
    minio = FakeMinio()
    with mock_http(raw, headers={"X-Rate-Limit-Remaining": "3999"}) as client:
        result = collect_once(config, client, minio, sleep=lambda _: None)

    content, content_type, metadata = minio.objects[(result.bucket, result.object_key)]
    assert content == raw
    assert content_type == "application/json"
    assert result.state_count == 1
    assert result.source_time == 1790339077
    assert result.rate_limit_remaining == "3999"
    assert result.object_key.startswith("aviation/opensky/dt=")
    assert metadata["sha256"] == result.sha256 == hashlib.sha256(raw).hexdigest()
    assert metadata["snapshot-id"] == result.snapshot_id


@pytest.mark.parametrize("states", [b"[]", b"null"])
def test_resposta_sem_aeronaves_e_snapshot_valido(config, states):
    minio = FakeMinio()
    raw = b'{"time":1790339077,"states":' + states + b"}"
    with mock_http(raw) as client:
        result = collect_once(config, client, minio, sleep=lambda _: None)
    assert result.state_count == 0
    assert minio.put_calls == 1


@pytest.mark.parametrize("raw", [b"not-json", b'{"states":[]}', b'{"time":1,"states":{}}'])
def test_resposta_invalida_nao_e_gravada(config, raw):
    minio = FakeMinio()
    with mock_http(raw) as client, pytest.raises(CollectionError):
        collect_once(config, client, minio, sleep=lambda _: None)
    assert minio.put_calls == 0


def test_rate_limit_nao_tenta_novamente(config):
    minio = FakeMinio()
    with mock_http(b"", status=429, headers={"X-Rate-Limit-Retry-After-Seconds": "120"}) as client:
        with pytest.raises(CollectionError, match="120s"):
            collect_once(config, client, minio, sleep=lambda _: None)
    assert minio.put_calls == 0


def test_token_e_renovado_uma_vez_apos_401(config):
    calls = {"token": 0, "states": 0}

    def handle(request):
        if request.url.path.endswith("/token"):
            calls["token"] += 1
            return httpx.Response(200, json={"access_token": f"token-{calls['token']}"})
        calls["states"] += 1
        if calls["states"] == 1:
            return httpx.Response(401)
        assert request.headers["Authorization"] == "Bearer token-2"
        return httpx.Response(200, json={"time": 1790339077, "states": []})

    minio = FakeMinio()
    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        collect_once(config, client, minio, sleep=lambda _: None)
    assert calls == {"token": 2, "states": 2}
    assert minio.put_calls == 1


def test_erro_temporario_da_api_tenta_novamente(config):
    attempts = 0
    pauses = []

    def handle(request):
        nonlocal attempts
        if request.url.path.endswith("/token"):
            return httpx.Response(200, json={"access_token": "test-token"})
        attempts += 1
        if attempts == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"time": 1790339077, "states": []})

    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        result = collect_once(config, client, FakeMinio(), sleep=pauses.append)
    assert result.state_count == 0
    assert attempts == 2
    assert pauses == [1.0]


def test_bbox_grande_e_bloqueado_antes_de_gastar_creditos(config):
    too_large = CollectorConfig(**{**config.__dict__, "lamin": -30, "lamax": 0, "lomin": -50, "lomax": -20})
    minio = FakeMinio()
    with mock_http(b"{}") as client, pytest.raises(CollectionError, match="25 graus"):
        collect_once(too_large, client, minio, sleep=lambda _: None)
    assert minio.put_calls == 0


def test_falha_no_minio_nao_retorna_sucesso(config):
    class FailedMinio(FakeMinio):
        def put_object(self, *args, **kwargs):
            raise OSError("storage down")

    with mock_http(b'{"time":1790339077,"states":[]}') as client:
        with pytest.raises(CollectionError, match="MinIO"):
            collect_once(config, client, FailedMinio(), sleep=lambda _: None)


def test_coleta_periodica_reutiliza_token_e_espera_entre_ciclos(config):
    calls = {"token": 0, "states": 0}

    def handle(request):
        if request.url.path.endswith("/token"):
            calls["token"] += 1
            return httpx.Response(200, json={"access_token": "test-token", "expires_in": 1800})
        calls["states"] += 1
        return httpx.Response(200, json={"time": 1790339077, "states": []})

    minio = FakeMinio()
    delays = []
    records = []
    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        run_periodically(
            config, client, minio, 60, max_cycles=2, wait=delays.append, emit=records.append
        )
    assert calls == {"token": 1, "states": 2}
    assert minio.put_calls == 2
    assert len(minio.objects) == 2
    assert delays == [60]
    assert [record["status"] for record in records] == ["collected", "collected"]


def test_coleta_periodica_respeita_retry_after_do_429(config):
    minio = FakeMinio()
    delays = []
    records = []
    with mock_http(b"", status=429, headers={"X-Rate-Limit-Retry-After-Seconds": "120"}) as client:
        run_periodically(
            config, client, minio, 60, max_cycles=2, wait=delays.append, emit=records.append
        )
    assert minio.put_calls == 0
    assert delays == [120]
    assert [record["status"] for record in records] == ["rate_limited", "rate_limited"]


def test_coleta_periodica_rejeita_intervalo_curto(config):
    with mock_http(b"{}") as client:
        with pytest.raises(CollectionError, match="30 segundos"):
            run_periodically(config, client, FakeMinio(), 10, max_cycles=1)
