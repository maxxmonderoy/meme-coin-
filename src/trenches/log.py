"""Structured JSON logging.

One event per line so seven days of unattended output stays greppable.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import sys
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": dt.datetime.fromtimestamp(record.created, tz=dt.UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        extra = getattr(record, "extra_fields", None)
        if extra:
            payload.update(extra)
        return json.dumps(payload, default=str, separators=(",", ":"))


def setup(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())


def get(name: str) -> logging.Logger:
    return logging.getLogger(name)


def kv(logger: logging.Logger, level: int, msg: str, **fields: Any) -> None:
    """Log `msg` with structured `fields` attached."""
    logger.log(level, msg, extra={"extra_fields": fields})
