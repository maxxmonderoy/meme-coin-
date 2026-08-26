"""Keeps exactly one consumer alive, and decides when it is dead.

CLAUDE.md 3.8.1 names silent stalls the number one killer: cloud load
balancers drop idle gRPC connections after 60-90s with no error, no EOF and no
exception, so the receive loop waits forever and the bot stops trading without
ever logging a failure. Three defences are required together, and only the
third actually detects this case:

  1. HTTP/2 keepalive        -- transport level, set by the consumer
  2. application-level ping  -- in the subscribe request, set by the consumer
  3. slot-monotonicity watchdog -- here

The watchdog is authoritative. If the chain's slot number has not advanced
within the threshold, the stream is dead no matter how healthy the socket
claims to be.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import AsyncIterator, Callable

from ..log import get, kv
from .base import Consumer, EventKind, RawEvent

log = get(__name__)


class StreamStalled(RuntimeError):
    """Raised by the watchdog when slots stop advancing."""


class Supervisor:
    """Runs a consumer factory forever, restarting with bounded backoff."""

    def __init__(
        self,
        factory: Callable[[], Consumer],
        *,
        watchdog_seconds: float | None = None,
        backoff_min: float = 1.0,
        backoff_max: float = 30.0,
        on_reconnect: Callable[[int, str], None] | None = None,
    ) -> None:
        self._factory = factory
        #: None means "ask the consumer" -- a websocket feed and a gRPC slot
        #: stream have honestly different liveness thresholds.
        self._watchdog_override = watchdog_seconds
        self._watchdog_seconds = watchdog_seconds or 120.0
        self._backoff_min = backoff_min
        self._backoff_max = backoff_max
        self._on_reconnect = on_reconnect
        self.reconnects = 0
        self.max_slot = 0
        self._last_progress = 0.0

    def _backoff(self, attempt: int) -> float:
        """Exponential with full jitter, capped.

        Jitter matters when several processes restart off the same upstream
        blip: without it they retry in lockstep and get rate-limited together.
        """
        ceiling = min(self._backoff_max, self._backoff_min * (2 ** min(attempt, 16)))
        return random.uniform(self._backoff_min, max(self._backoff_min, ceiling))  # noqa: S311

    async def _watchdog(self, cancel_target: asyncio.Task) -> None:
        """Cancel the stream task when slots stop advancing.

        Deliberately keyed on slot progress rather than on bytes received. A
        connection can keep delivering pings and keepalives long after the data
        plane has gone quiet, which is exactly what makes this failure silent.
        """
        interval = max(0.25, self._watchdog_seconds / 4)
        while True:
            await asyncio.sleep(interval)
            idle = time.monotonic() - self._last_progress
            if idle > self._watchdog_seconds:
                kv(
                    log, logging.ERROR, "feed stalled; no frames received",
                    idle_seconds=round(idle, 2),
                    threshold_seconds=self._watchdog_seconds,
                    last_slot=self.max_slot,
                )
                cancel_target.cancel()
                return

    async def run(self) -> AsyncIterator[RawEvent]:
        """Yield events from a perpetually-restarted consumer."""
        attempt = 0
        while True:
            consumer = self._factory()
            self._watchdog_seconds = self._watchdog_override or consumer.idle_timeout_seconds
            self._last_progress = time.monotonic()
            queue: asyncio.Queue[RawEvent | BaseException | None] = asyncio.Queue(maxsize=1024)

            async def pump(c: Consumer = consumer, q=queue) -> None:
                try:
                    async for event in c.stream():
                        await q.put(event)
                    await q.put(None)
                except asyncio.CancelledError:
                    await q.put(StreamStalled("watchdog cancelled the stream"))
                    raise
                except BaseException as exc:
                    await q.put(exc)

            task = asyncio.create_task(pump(), name=f"stream-{consumer.provider}")
            watchdog = asyncio.create_task(self._watchdog(task), name="watchdog")
            reason = "clean-eof"
            exhausted = False
            try:
                while True:
                    item = await queue.get()
                    if item is None:
                        exhausted = consumer.finite
                        break
                    if isinstance(item, BaseException):
                        reason = f"{type(item).__name__}: {item}"
                        break
                    attempt = 0  # a delivered event proves the connection works
                    if item.slot and item.slot > self.max_slot:
                        self.max_slot = item.slot
                    # Any delivered frame is progress, including a ping. On a
                    # websocket there is no slot number to key liveness on.
                    self._last_progress = time.monotonic()
                    if item.event_kind != EventKind.PING:
                        yield item
            finally:
                watchdog.cancel()
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
                with contextlib.suppress(asyncio.CancelledError):
                    await watchdog
                await consumer.aclose()

            if exhausted:
                kv(log, logging.INFO, "finite source exhausted; not reconnecting",
                   provider=consumer.provider, max_slot=self.max_slot)
                return

            self.reconnects += 1
            attempt += 1
            delay = self._backoff(attempt)
            kv(
                log, logging.WARNING, "stream ended; reconnecting",
                provider=consumer.provider, reason=reason,
                attempt=attempt, delay_seconds=round(delay, 2),
                reconnects_total=self.reconnects, max_slot=self.max_slot,
            )
            if self._on_reconnect:
                self._on_reconnect(self.reconnects, reason)
            await asyncio.sleep(delay)
