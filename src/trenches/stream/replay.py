"""Replay consumer: reads recorded events from disk.

Runs with no network and no credentials, which makes it the feed the pipeline,
dedupe and journal are actually exercised against. Replaying the same capture
twice reproduces the duplicate guarantee from 3.8.4 exactly.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
from collections.abc import AsyncIterator
from pathlib import Path

from ..log import get
from .base import Consumer, EventKind, RawEvent

log = get(__name__)


class ReplayConsumer(Consumer):
    provider = "replay"
    idle_timeout_seconds = 1e9  # a file cannot stall

    def __init__(self, path: Path, *, speed: float = 0.0, loop: bool = False) -> None:
        self._path = Path(path)
        self._speed = speed
        self._loop = loop
        self.finite = not loop

    def subscription_descriptor(self) -> dict:
        return {"provider": self.provider, "path": str(self._path),
                "speed": self._speed, "loop": self._loop}

    def _files(self) -> list[Path]:
        if self._path.is_file():
            return [self._path]
        if self._path.is_dir():
            return sorted(self._path.glob("*.jsonl"))
        return []

    async def stream(self) -> AsyncIterator[RawEvent]:
        files = self._files()
        if not files:
            raise FileNotFoundError(
                f"no .jsonl captures under {self._path}. Record some with "
                "`trenches stream --record <dir>`, or point TRENCHES_REPLAY_PATH elsewhere."
            )
        while True:
            for file in files:
                for line_no, raw_line in enumerate(file.read_text().splitlines(), start=1):
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        log.warning("skipping malformed capture line %s:%d", file, line_no)
                        continue
                    event = self._to_event(record)
                    if event is None:
                        continue
                    if self._speed:
                        await asyncio.sleep(self._speed)
                    yield event
            if not self._loop:
                return

    @staticmethod
    def _to_event(record: dict) -> RawEvent | None:
        # A raw provider frame rather than a normalised event: mapping it needs
        # that provider's schema, which is the provider consumer's job.
        if "frame" in record and "provider" not in record:
            return None
        block_time = record.get("block_time")
        # Preserve the RECORDED arrival time. Stamping "now" instead collapses
        # every inter-feed gap to zero, which silently destroys the one
        # measurement (3.2 upgrade trigger 2) that replay exists to let us
        # re-run offline.
        received_at = record.get("received_at")
        return RawEvent(
            provider=record.get("provider", "replay"),
            event_kind=record.get("event_kind", EventKind.OTHER),
            mint=record.get("mint"),
            signature=record.get("signature"),
            slot=record.get("slot"),
            commitment=record.get("commitment", "processed"),
            block_time=dt.datetime.fromisoformat(block_time) if block_time else None,
            payload=record.get("payload"),
            **({"received_at": dt.datetime.fromisoformat(received_at)}
               if received_at else {}),
        )
