"""Structured JSON logging.

One event per line so seven days of unattended output stays greppable.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
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


#: Third-party loggers held at WARNING regardless of our level.
#:
#: httpx logs one INFO line per request. The RugCheck poller runs every two
#: seconds, so at INFO that is ~30 lines a minute of "HTTP/1.1 200 OK" and
#: nothing else -- every launch detected, position opened and structural event
#: is buried in it. That is not cosmetic: 3.8.8 says the canary is a DROP in
#: detections, and a drop is invisible in a log that is 99% transport chatter.
#:
#: Raise any of these deliberately with TRENCHES_LOG_NOISY=1 when debugging the
#: transport itself.
NOISY_LOGGERS = ("httpx", "httpcore", "websockets", "asyncio", "urllib3", "hpack")


def setup(level: str = "INFO", *, quiet_third_party: bool = True) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())

    # Set explicitly in BOTH directions. setup() can be called more than once in
    # a process, and only lowering the level means a later call asking for the
    # chatter back silently keeps the old WARNING -- the switch would look like
    # it does nothing.
    quiet = quiet_third_party and not os.environ.get("TRENCHES_LOG_NOISY")
    for name in NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING if quiet else logging.NOTSET)


def get(name: str) -> logging.Logger:
    return logging.getLogger(name)


def kv(logger: logging.Logger, level: int, msg: str, **fields: Any) -> None:
    """Log `msg` with structured `fields` attached."""
    logger.log(level, msg, extra={"extra_fields": fields})
