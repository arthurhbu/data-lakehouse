"""Callbacks operacionais compartilhados pelas DAGs locais."""

from __future__ import annotations

import json
import logging
import os
from urllib.request import Request, urlopen


logger = logging.getLogger(__name__)


def notify_failure(context) -> None:
    task_instance = context.get("task_instance")
    payload = {
        "event": "airflow_task_failed",
        "dag_id": getattr(task_instance, "dag_id", None),
        "task_id": getattr(task_instance, "task_id", None),
        "run_id": context.get("run_id"),
        "try_number": getattr(task_instance, "try_number", None),
        "log_url": getattr(task_instance, "log_url", None),
    }
    logger.error("Alerta Airflow: %s", json.dumps(payload, ensure_ascii=False))

    webhook_url = os.getenv("AIRFLOW_ALERT_WEBHOOK_URL")
    if not webhook_url:
        return

    request = Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=5):
            pass
    except OSError as error:
        logger.exception("Falha ao enviar alerta do Airflow: %s", error)
