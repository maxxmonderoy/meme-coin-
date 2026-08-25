"""Writes normalised events to JSONL so a run can be replayed later.

Captures are the substitute for a provider replay window. Chainstack keeps
about 100 slots (~35s); anything longer than that is gone permanently
(3.8.6), so a local capture is the only way to re-run a decision against the
same input twice.
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
        self._fh.write(
            json.dumps(
                {
                    "signature": event.signature,
                    "slot": event.slot,
                    "provider": event.provider,
                    "event_kind": event.event_kind,
                    "commitment": event.commitment,
                    "program_id": event.program_id,
                    "block_time": event.block_time.isoformat() if event.block_time else None,
                    "filter_source": event.filter_source,
                    "payload": event.payload,
                    "received_at": event.received_at.isoformat(),
                },
                separators=(",", ":"),
                default=str,
            )
            + "\n"
        )
        self.written += 1

    def flush(self) -> None:
        self._fh.flush()

    def close(self) -> None:
        self.flush()
        self._fh.close()
