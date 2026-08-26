"""Per-(minute, feed) counters.

3.8.8: monitor DETECTION RATE, not just error rate. A feed serving cached
responses or a decoder broken by a schema change produces plausible output, not
exceptions, so an error-rate monitor never fires. Counters are per-feed because
the whole point of running two is noticing when one of them quietly stops.
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
    schema_mismatches: int = 0
    queue_high_water: int = 0
    queue_drops: int = 0
    reconnects: int = 0
    stale_responses: int = 0
    wins: int = 0


@dataclass(slots=True)
class HealthTracker:
    buckets: dict[tuple[dt.datetime, str], MinuteCounters] = field(default_factory=dict)

    def bucket(self, feed: str, ts: dt.datetime | None = None) -> MinuteCounters:
        ts = ts or dt.datetime.now(tz=dt.UTC)
        return self.buckets.setdefault((_minute(ts), feed), MinuteCounters())

    def record_event(self, feed: str, kind: str, *, ts: dt.datetime | None = None) -> None:
        b = self.bucket(feed, ts)
        b.events_total += 1
        if kind == "create":
            b.events_create += 1
        else:
            b.events_other += 1

    def record_dupe(self, feed: str, ts: dt.datetime | None = None) -> None:
        self.bucket(feed, ts).dupes += 1

    def record_win(self, feed: str, ts: dt.datetime | None = None) -> None:
        """This feed saw a mint first. The 3.2 upgrade-trigger evidence."""
        self.bucket(feed, ts).wins += 1

    def record_decode_failure(self, feed: str, ts: dt.datetime | None = None) -> None:
        self.bucket(feed, ts).decode_failures += 1

    def record_schema_mismatch(self, feed: str, ts: dt.datetime | None = None) -> None:
        self.bucket(feed, ts).schema_mismatches += 1

    def record_stale(self, feed: str, ts: dt.datetime | None = None) -> None:
        self.bucket(feed, ts).stale_responses += 1

    def record_queue(self, feed: str, depth: int, *, dropped: int = 0,
                     ts: dt.datetime | None = None) -> None:
        b = self.bucket(feed, ts)
        b.queue_high_water = max(b.queue_high_water, depth)
        b.queue_drops += dropped

    def record_reconnect(self, feed: str, ts: dt.datetime | None = None) -> None:
        self.bucket(feed, ts).reconnects += 1

    def drain(
        self, now: dt.datetime | None = None
    ) -> list[tuple[tuple[dt.datetime, str], MinuteCounters]]:
        """Return and clear every bucket except the one still being filled.

        `now` is injectable so this is testable against a fixed clock rather
        than against whatever time the suite happens to run at.
        """
        current = _minute(now or dt.datetime.now(tz=dt.UTC))
        ready = [(k, v) for k, v in sorted(self.buckets.items()) if k[0] < current]
        for key, _ in ready:
            del self.buckets[key]
        return ready
