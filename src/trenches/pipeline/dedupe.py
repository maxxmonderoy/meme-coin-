"""(signature, slot) dedupe with a TTL window.

Duplicates are guaranteed on replay and reconnect (3.8.4). The in-memory cache
is the fast path; the raw_events primary key is the durable truth. Both exist
because the cache is bounded and a restart empties it.

The buy path does not exist yet, but the ordering rule it will need is set
here: a duplicate detection firing two buys is worse than a missed launch, so
`seen()` marks BEFORE the caller acts, never after.
"""
from __future__ import annotations

import time
from collections import OrderedDict


class DedupeCache:
    """Bounded, time-windowed set of (signature, slot) keys."""

    __slots__ = ("_entries", "_max", "_ttl", "hits", "misses")

    def __init__(self, ttl_seconds: int = 90, max_entries: int = 200_000) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self._ttl = ttl_seconds
        self._max = max_entries
        self._entries: OrderedDict[tuple[str, int], float] = OrderedDict()
        self.hits = 0
        self.misses = 0

    def __len__(self) -> int:
        return len(self._entries)

    def _expire(self, now: float) -> None:
        """Drop entries older than the TTL. Insertion order is age order."""
        cutoff = now - self._ttl
        while self._entries:
            _, stamp = next(iter(self._entries.items()))
            if stamp > cutoff:
                break
            self._entries.popitem(last=False)

    def _trim(self) -> None:
        """Enforce the size bound. Runs AFTER insertion, so the cache never
        exceeds max_entries -- trimming first leaves it at max+1."""
        while len(self._entries) > self._max:
            self._entries.popitem(last=False)

    def seen(self, key: tuple[str, int], *, now: float | None = None) -> bool:
        """Return True if `key` was already seen; mark it seen either way.

        Marking happens before the caller acts on the result. That ordering is
        the whole point: it is the idempotency-key-before-the-call rule from
        3.6, applied at detection rather than at execution.
        """
        now = time.monotonic() if now is None else now
        self._expire(now)
        if key in self._entries:
            self._entries.move_to_end(key)
            self._entries[key] = now
            self.hits += 1
            return True
        self._entries[key] = now
        self._trim()
        self.misses += 1
        return False

    def reset_counters(self) -> None:
        self.hits = 0
        self.misses = 0
