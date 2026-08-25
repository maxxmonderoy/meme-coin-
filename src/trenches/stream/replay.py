"""Replay consumer: reads recorded frames from disk.

The only feed that runs with no subscription and no credentials, which makes it
the one the pipeline, dedupe and journal are actually exercised against before
any money is spent on a provider.

It is also how duplicates get tested honestly. Reconnect and replay guarantee
duplicates (3.8.4); replaying the same capture twice reproduces that exactly.
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

    def __init__(
        self,
        path: Path,
        *,
        speed: float = 0.0,
        loop: bool = False,
    ) -> None:
        """`speed` 0 replays as fast as possible; >0 sleeps that many seconds
        between events, which is how the slot watchdog gets exercised."""
        self._path = Path(path)
        self._speed = speed
        self._loop = loop
        self.finite = not loop

    def subscription_descriptor(self) -> dict:
        return {
            "provider": self.provider,
            "path": str(self._path),
            "speed": self._speed,
            "loop": self._loop,
        }

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
                for line_no, line in enumerate(file.read_text().splitlines(), start=1):
                    line = line.strip()
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
        """Capture rows are RawEvent dicts as written by the recorder."""
        if "signature" not in record and "frame" in record:
            # A raw provider frame rather than a normalised event: replaying it
            # would require the provider's mapping, which is the caller's job.
            return None
        block_time = record.get("block_time")
        return RawEvent(
            signature=record["signature"],
            slot=int(record["slot"]),
            provider=record.get("provider", "replay"),
            event_kind=record.get("event_kind", EventKind.OTHER),
            commitment=record.get("commitment", "processed"),
            program_id=record.get("program_id"),
            block_time=dt.datetime.fromisoformat(block_time) if block_time else None,
            filter_source=record.get("filter_source"),
            payload=record.get("payload"),
        )
