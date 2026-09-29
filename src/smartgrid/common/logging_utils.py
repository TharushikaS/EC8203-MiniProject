"""Structured (JSON-lines) logging used by every pipeline stage.

Each log line carries the ``service`` that wrote it, the pipeline ``stage``
(ingestion | processing | storage | serving | orchestration), an ``event`` name and
arbitrary context fields (trace_id, batch_id, sim_date, counts...). Because the output
is one JSON object per line, `docker compose logs <svc> | jq` or any log shipper
(Loki, ELK) can filter and aggregate without parsing free text.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

_RESERVED = set(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class StructLogger:
    """Tiny wrapper so call sites read as ``log.info("event_name", key=value)``."""

    def __init__(self, logger: logging.Logger, stage: str):
        self._logger = logger
        self._stage = stage

    def _log(self, level: int, event: str, exc_info=None, **fields: Any) -> None:
        fields.setdefault("stage", self._stage)
        self._logger.log(level, event, extra=fields, exc_info=exc_info)

    def debug(self, event: str, **fields: Any) -> None:
        self._log(logging.DEBUG, event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        self._log(logging.INFO, event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._log(logging.WARNING, event, **fields)

    def error(self, event: str, exc_info=None, **fields: Any) -> None:
        self._log(logging.ERROR, event, exc_info=exc_info, **fields)

    def exception(self, event: str, **fields: Any) -> None:
        self._log(logging.ERROR, event, exc_info=True, **fields)


_configured: set[str] = set()


def get_logger(service: str, stage: str, name: str | None = None) -> StructLogger:
    logger_name = name or service
    logger = logging.getLogger(logger_name)
    if logger_name not in _configured:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter(service))
        logger.handlers = [handler]
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
        logger.propagate = False
        _configured.add(logger_name)
    return StructLogger(logger, stage)
