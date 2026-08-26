"""Writes normalised events to JSONL so a run can be replayed later.

Free feeds have no replay window at all, so a local capture is the only way to
run the same input past the pipeline twice.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from .base import RawEvent


class Recorder:
    def __init__(self, directory: Path, *, prefix: str = "events") -> None:
        self._dir = Path(directory)
        self._dir.mkdir(parents=True, exist_ok=True)
        stamp = dt.datetime.now(tz=dt.UTC).strftime("%Y%m%dT%H%M%SZ")
        self.path = self._dir / f"{prefix}-{stamp}.jsonl"
        self._fh = self.path.open("a", encoding="utf-8")
        self.written = 0

    def write(self, event: RawEvent) -> None:
        self._fh.write(json.dumps({
            "provider": event.provider,
            "event_kind": event.event_kind,
            "mint": event.mint,
            "signature": event.signature,
            "slot": event.slot,
            "commitment": event.commitment,
            "block_time": event.block_time.isoformat() if event.block_time else None,
            "payload": event.payload,
            "received_at": event.received_at.isoformat(),
        }, separators=(",", ":"), default=str) + "\n")
        self.written += 1

    def flush(self) -> None:
        self._fh.flush()

    def close(self) -> None:
        self.flush()
        self._fh.close()
