"""Race several feeds concurrently and merge them into one event stream.

3.3: two vendors on different infrastructure beats one faster vendor, and at
$0 there is no reason not to run both. Each feed gets its own supervisor, so a
PumpPortal disconnect does not stop RugCheck polling and vice versa.
"""
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable

from ..log import get
from .base import Consumer, RawEvent
from .supervisor import Supervisor

log = get(__name__)

_SENTINEL = object()


class FeedMultiplexer:
    def __init__(
        self,
        factories: dict[str, Callable[[], Consumer]],
        *,
        backoff_min: float = 1.0,
        backoff_max: float = 30.0,
        on_reconnect: Callable[[str, int, str], None] | None = None,
        queue_maxsize: int = 2048,
    ) -> None:
        if not factories:
            raise ValueError("at least one feed is required")
        self._factories = factories
        self.supervisors = {
            name: Supervisor(
                factory,
                backoff_min=backoff_min,
                backoff_max=backoff_max,
                on_reconnect=(
                    (lambda n, reason, _name=name: on_reconnect(_name, n, reason))
                    if on_reconnect else None
                ),
            )
            for name, factory in factories.items()
        }
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)

    async def _drain(self, name: str, supervisor: Supervisor) -> None:
        try:
            async for event in supervisor.run():
                await self._queue.put(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("feed %s terminated: %s", name, exc)
        finally:
            await self._queue.put((_SENTINEL, name))

    async def run(self) -> AsyncIterator[RawEvent]:
        tasks = [
            asyncio.create_task(self._drain(name, sup), name=f"feed-{name}")
            for name, sup in self.supervisors.items()
        ]
        alive = len(tasks)
        try:
            while alive:
                item = await self._queue.get()
                if isinstance(item, tuple) and item and item[0] is _SENTINEL:
                    alive -= 1
                    log.info("feed %s finished; %d still running", item[1], alive)
                    continue
                yield item
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
