"""Per-minute stream counters.

3.8.8: monitor DETECTION RATE, not just error rate. A decoder broken by a
program layout change emits corrupt output rather than exceptions, so an
error-rate monitor never fires. A sudden drop to zero creates is the canary,
and it is only visible if creates are counted per minute rather than in total.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field


def _minute(ts: dt.datetime) -> dt.datetime:
    return ts.replace(second=0, microsecond=0)


@dataclass(slots=True)
class MinuteCounters:
    events_total: int = 0
    events_create: int = 0
    events_other: int = 0
    dupes: int = 0
    decode_failures: int = 0
    queue_high_water: int = 0
    queue_drops: int = 0
    reconnects: int = 0
    slot_gaps: int = 0
    max_slot: int = 0


@dataclass(slots=True)
class HealthTracker:
    """Accumulates counters bucketed by minute, ready to flush to Postgres."""

    buckets: dict[dt.datetime, MinuteCounters] = field(default_factory=dict)
    last_slot: int = 0

    def bucket(self, ts: dt.datetime | None = None) -> MinuteCounters:
        ts = ts or dt.datetime.now(tz=dt.UTC)
        return self.buckets.setdefault(_minute(ts), MinuteCounters())

    def record_event(self, kind: str, *, ts: dt.datetime | None = None) -> None:
        b = self.bucket(ts)
        b.events_total += 1
        if kind == "create":
            b.events_create += 1
        else:
            b.events_other += 1

    def record_dupe(self, ts: dt.datetime | None = None) -> None:
        self.bucket(ts).dupes += 1

    def record_decode_failure(self, ts: dt.datetime | None = None) -> None:
        self.bucket(ts).decode_failures += 1

    def record_queue(self, depth: int, *, dropped: int = 0,
                     ts: dt.datetime | None = None) -> None:
        b = self.bucket(ts)
        b.queue_high_water = max(b.queue_high_water, depth)
        b.queue_drops += dropped

    def record_reconnect(self, ts: dt.datetime | None = None) -> None:
        self.bucket(ts).reconnects += 1

    def observe_slot(self, slot: int, *, ts: dt.datetime | None = None) -> int:
        """Record a slot; return the size of any gap that opened.

        A gap is reported, not acted on. Solana genuinely skips slots when a
        leader fails (3.8.5), so treating every gap as data loss pages
        constantly and teaches you to ignore the page.
        """
        b = self.bucket(ts)
        gap = 0
        if slot > self.last_slot:
            if self.last_slot and slot > self.last_slot + 1:
                gap = slot - self.last_slot - 1
                b.slot_gaps += gap
            self.last_slot = slot
        b.max_slot = max(b.max_slot, slot)
        return gap

    def drain(self) -> list[tuple[dt.datetime, MinuteCounters]]:
        """Return and clear all buckets except the current minute."""
        current = _minute(dt.datetime.now(tz=dt.UTC))
        ready = [(m, c) for m, c in sorted(self.buckets.items()) if m < current]
        for minute, _ in ready:
            del self.buckets[minute]
        return ready
