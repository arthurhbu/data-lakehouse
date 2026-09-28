"""Consulta um snapshot OpenSky e preserva o JSON original no MinIO.

Pode executar uma vez ou periodicamente. Não publica no Kafka nem envia
arquivos ao Databricks.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import os
import signal
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from dotenv import load_dotenv
from minio import Minio


TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)
STATES_URL = "https://opensky-network.org/api/states/all"
PROJECT_ROOT = Path(__file__).resolve().parents[3]


class CollectionError(Exception):
    """Falha controlada da coleta; nunca inclui segredos ou corpo HTTP."""


class RateLimitError(CollectionError):
    """Cota esgotada; o próximo ciclo deve respeitar retry_after_seconds."""

    def __init__(self, retry_after_seconds: float):
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"Cota OpenSky esgotada: HTTP 429; aguarde {retry_after_seconds:g}s")


@dataclass(frozen=True)
class CollectorConfig:
    opensky_client_id: str
    opensky_client_secret: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    lamin: float
    lomin: float
    lamax: float
    lomax: float

    @classmethod
    def from_env(cls) -> CollectorConfig:
        def required(name: str) -> str:
            value = os.getenv(name, "").strip()
            if not value:
                raise CollectionError(f"Variável obrigatória ausente: {name}")
            return value

        def coordinate(name: str) -> float:
            value = required(name)
            try:
                return float(value)
            except ValueError as exc:
                raise CollectionError(f"Coordenada inválida em {name}") from exc

        config = cls(
            opensky_client_id=required("OPENSKY_CLIENT_ID"),
            opensky_client_secret=required("OPENSKY_CLIENT_SECRET"),
            minio_endpoint=required("MINIO_ENDPOINT"),
            minio_access_key=required("MINIO_ROOT_USER"),
            minio_secret_key=required("MINIO_ROOT_PASSWORD"),
            minio_bucket=required("MINIO_BUCKET_BRONZE"),
            lamin=coordinate("AVIATION_BBOX_LAMIN"),
            lomin=coordinate("AVIATION_BBOX_LOMIN"),
            lamax=coordinate("AVIATION_BBOX_LAMAX"),
            lomax=coordinate("AVIATION_BBOX_LOMAX"),
        )
        config.validate()
        return config

    def validate(self) -> None:
        bounds = (self.lamin, self.lomin, self.lamax, self.lomax)
        if not all(math.isfinite(value) for value in bounds):
            raise CollectionError("Bounding box deve conter coordenadas finitas")
        if not (-90 <= self.lamin < self.lamax <= 90):
            raise CollectionError("Limites de latitude inválidos")
        if not (-180 <= self.lomin < self.lomax <= 180):
            raise CollectionError("Limites de longitude inválidos")
        area = (self.lamax - self.lamin) * (self.lomax - self.lomin)
        if area > 25:
            raise CollectionError("Bounding box excede 25 graus quadrados (mais créditos)")

    @property
    def bbox(self) -> dict[str, float]:
        return {
            "lamin": self.lamin,
            "lomin": self.lomin,
            "lamax": self.lamax,
            "lomax": self.lomax,
        }


@dataclass(frozen=True)
class CollectionResult:
    bucket: str
    object_key: str
    snapshot_id: str
    request_id: str
    collected_at: str
    source_time: int
    state_count: int
    size_bytes: int
    sha256: str
    rate_limit_remaining: str | None


class TokenManager:
    """Reutiliza o token entre ciclos e o renova antes de expirar."""

    def __init__(
        self,
        client: httpx.Client,
        config: CollectorConfig,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.client = client
        self.config = config
        self.clock = clock
        self._token: str | None = None
        self._expires_at = 0.0

    def get(self, *, force_refresh: bool = False) -> str:
        if not force_refresh and self._token and self.clock() < self._expires_at:
            return self._token
        try:
            response = self.client.post(
                TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.config.opensky_client_id,
                    "client_secret": self.config.opensky_client_secret,
                },
            )
        except httpx.RequestError as exc:
            raise CollectionError("Falha de rede ao autenticar na OpenSky") from exc
        if response.status_code != 200:
            raise CollectionError(f"Autenticação OpenSky falhou: HTTP {response.status_code}")
        try:
            body = response.json()
            token = body["access_token"]
        except (ValueError, KeyError, TypeError) as exc:
            raise CollectionError("Resposta OAuth OpenSky sem access_token válido") from exc
        if not isinstance(token, str) or not token:
            raise CollectionError("Resposta OAuth OpenSky sem access_token válido")
        expires_in = body.get("expires_in", 1800)
        try:
            lifetime = float(expires_in)
        except (TypeError, ValueError):
            lifetime = 1800.0
        if not math.isfinite(lifetime) or lifetime <= 0:
            lifetime = 1800.0
        self._token = token
        self._expires_at = self.clock() + max(1.0, lifetime - min(30.0, lifetime / 10))
        return token


def _fetch_snapshot(
    client: httpx.Client,
    config: CollectorConfig,
    sleep: Callable[[float], None],
    tokens: TokenManager,
) -> httpx.Response:
    token = tokens.get()
    refreshed = False
    transient_failures = 0
    while True:
        try:
            response = client.get(
                STATES_URL,
                headers={"Authorization": f"Bearer {token}"},
                params=config.bbox,
            )
        except httpx.RequestError as exc:
            if transient_failures >= 2:
                raise CollectionError("Falha de rede ao consultar a OpenSky") from exc
            transient_failures += 1
            sleep(float(transient_failures))
            continue

        if response.status_code == 401 and not refreshed:
            token = tokens.get(force_refresh=True)
            refreshed = True
            continue
        if response.status_code == 429:
            retry_after = response.headers.get("X-Rate-Limit-Retry-After-Seconds", "")
            try:
                delay = float(retry_after)
            except ValueError:
                delay = 3600.0
            if not math.isfinite(delay) or delay <= 0:
                delay = 3600.0
            raise RateLimitError(delay)
        if 500 <= response.status_code < 600 and transient_failures < 2:
            transient_failures += 1
            sleep(float(transient_failures))
            continue
        if response.status_code != 200:
            raise CollectionError(f"Consulta OpenSky falhou: HTTP {response.status_code}")
        return response


def _inspect_payload(raw_bytes: bytes) -> tuple[int, int]:
    try:
        payload = json.loads(raw_bytes)
    except (ValueError, UnicodeDecodeError) as exc:
        raise CollectionError("OpenSky retornou JSON inválido") from exc
    if not isinstance(payload, dict):
        raise CollectionError("OpenSky retornou objeto JSON inesperado")
    source_time = payload.get("time")
    states = payload.get("states")
    if isinstance(source_time, bool) or not isinstance(source_time, int):
        raise CollectionError("Resposta OpenSky sem campo time válido")
    if states is not None and not isinstance(states, list):
        raise CollectionError("Resposta OpenSky com campo states inválido")
    return source_time, len(states) if states is not None else 0


def collect_once(
    config: CollectorConfig,
    http_client: httpx.Client,
    minio_client: Minio,
    *,
    sleep: Callable[[float], None] = time.sleep,
    tokens: TokenManager | None = None,
) -> CollectionResult:
    """Salva exatamente o corpo recebido; não transforma ou deduplica aeronaves."""
    config.validate()
    request_id = str(uuid4())
    response = _fetch_snapshot(http_client, config, sleep, tokens or TokenManager(http_client, config))
    raw_bytes = response.content
    source_time, state_count = _inspect_payload(raw_bytes)
    collected_at = datetime.now(timezone.utc)
    snapshot_id = str(uuid4())
    object_key = (
        f"aviation/opensky/dt={collected_at:%Y-%m-%d}/hour={collected_at:%H}/"
        f"snapshot_id={snapshot_id}.json"
    )
    checksum = hashlib.sha256(raw_bytes).hexdigest()
    metadata = {
        "snapshot-id": snapshot_id,
        "request-id": request_id,
        "collected-at": collected_at.isoformat(),
        "source-time": str(source_time),
        "state-count": str(state_count),
        "sha256": checksum,
        "bbox": ",".join(str(value) for value in config.bbox.values()),
    }
    try:
        if not minio_client.bucket_exists(config.minio_bucket):
            raise CollectionError(f"Bucket MinIO inexistente: {config.minio_bucket}")
        minio_client.put_object(
            config.minio_bucket,
            object_key,
            io.BytesIO(raw_bytes),
            len(raw_bytes),
            content_type="application/json",
            metadata=metadata,
        )
        stored = minio_client.stat_object(config.minio_bucket, object_key)
    except CollectionError:
        raise
    except Exception as exc:
        raise CollectionError(f"Falha de armazenamento MinIO; verifique {object_key}") from exc
    if stored.size != len(raw_bytes):
        raise CollectionError(f"Tamanho divergente no MinIO; verifique {object_key}")

    return CollectionResult(
        bucket=config.minio_bucket,
        object_key=object_key,
        snapshot_id=snapshot_id,
        request_id=request_id,
        collected_at=collected_at.isoformat(),
        source_time=source_time,
        state_count=state_count,
        size_bytes=len(raw_bytes),
        sha256=checksum,
        rate_limit_remaining=response.headers.get("X-Rate-Limit-Remaining"),
    )


def _emit_json(record: dict) -> None:
    print(json.dumps(record, ensure_ascii=False), flush=True)


def run_periodically(
    config: CollectorConfig,
    http_client: httpx.Client,
    minio_client: Minio,
    interval_seconds: float,
    *,
    stop_event: threading.Event | None = None,
    max_cycles: int | None = None,
    wait: Callable[[float], object] | None = None,
    emit: Callable[[dict], None] = _emit_json,
) -> None:
    """Executa ciclos sequenciais; uma falha não inicia chamadas em paralelo."""
    if not math.isfinite(interval_seconds) or interval_seconds < 30:
        raise CollectionError("Intervalo de coleta deve ser de pelo menos 30 segundos")
    if max_cycles is not None and max_cycles < 1:
        raise CollectionError("max_cycles deve ser positivo")

    stop = stop_event or threading.Event()
    pause = wait or stop.wait
    tokens = TokenManager(http_client, config)
    cycles = 0
    while not stop.is_set():
        try:
            result = collect_once(config, http_client, minio_client, tokens=tokens)
        except RateLimitError as exc:
            delay = max(interval_seconds, exc.retry_after_seconds)
            emit({"status": "rate_limited", "message": str(exc), "next_attempt_seconds": delay})
        except CollectionError as exc:
            delay = interval_seconds
            emit({"status": "error", "message": str(exc), "next_attempt_seconds": delay})
        else:
            delay = interval_seconds
            emit({"status": "collected", **asdict(result)})

        cycles += 1
        if max_cycles is not None and cycles >= max_cycles:
            break
        pause(delay)


def _minio_from_config(config: CollectorConfig) -> Minio:
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
        raise CollectionError("MINIO_ENDPOINT deve ser uma URL como http://localhost:9000")
    return Minio(
        endpoint.netloc,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        secure=endpoint.scheme == "https",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Coleta snapshots OpenSky para o MinIO")
    parser.add_argument("--continuous", action="store_true", help="repete a coleta até ser interrompido")
    parser.add_argument("--interval-seconds", type=float, help="intervalo entre ciclos; mínimo 30s")
    parser.add_argument("--max-cycles", type=int, help="limita ciclos no modo contínuo para testes")
    args = parser.parse_args(argv)
    if args.max_cycles is not None and not args.continuous:
        parser.error("--max-cycles exige --continuous")

    load_dotenv(PROJECT_ROOT / ".env")
    try:
        config = CollectorConfig.from_env()
        minio_client = _minio_from_config(config)
        with httpx.Client(timeout=10.0) as http_client:
            if args.continuous:
                interval_text = args.interval_seconds
                if interval_text is None:
                    interval_text = os.getenv("AVIATION_POLL_INTERVAL_SECONDS", "60")
                try:
                    interval = float(interval_text)
                except (TypeError, ValueError) as exc:
                    raise CollectionError("AVIATION_POLL_INTERVAL_SECONDS inválido") from exc
                stop_event = threading.Event()
                signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
                run_periodically(
                    config,
                    http_client,
                    minio_client,
                    interval,
                    stop_event=stop_event,
                    max_cycles=args.max_cycles,
                )
                return 0
            result = collect_once(config, http_client, minio_client)
    except CollectionError as exc:
        print(f"Coleta falhou: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Coleta interrompida pelo usuário", file=sys.stderr)
        return 130
    print(json.dumps(asdict(result), ensure_ascii=False))
    return 0
