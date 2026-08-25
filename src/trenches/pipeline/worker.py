"""Receive -> bounded queue -> worker pool -> Postgres.

The receive loop does nothing but enqueue (3.8.2). When the queue is full it
drops and counts the drop rather than blocking the reader: blocking the reader
fills the server's channel and gets the connection dropped, turning a local
backlog into a reconnect storm.
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging

import asyncpg

from ..config import Config
from ..db import repo
from ..log import get, kv
from ..stream.base import EventKind, RawEvent
from ..stream.recorder import Recorder
from .dedupe import DedupeCache
from .health import HealthTracker

log = get(__name__)

#: How often accumulated per-minute counters are written to Postgres.
HEALTH_FLUSH_SECONDS = 20


class Ingest:
    def __init__(
        self,
        pool: asyncpg.Pool,
        cfg: Config,
        stream_id: int,
        *,
        recorder: Recorder | None = None,
    ) -> None:
        self._pool = pool
        self._cfg = cfg
        self._stream_id = stream_id
        self._recorder = recorder
        self.queue: asyncio.Queue[RawEvent] = asyncio.Queue(maxsize=cfg.queue_maxsize)
        self.dedupe = DedupeCache(ttl_seconds=cfg.dedupe_ttl_seconds)
        self.health = HealthTracker()
        self.dropped = 0
        self._stopping = asyncio.Event()

    # -- receive side ----------------------------------------------------
    def offer(self, event: RawEvent) -> bool:
        """Enqueue without ever awaiting. Returns False if dropped."""
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1
            self.health.record_queue(self.queue.qsize(), dropped=1)
            if self.dropped % 100 == 1:
                kv(log, logging.ERROR, "queue full; dropping events",
                   dropped_total=self.dropped, maxsize=self._cfg.queue_maxsize)
            return False
        self.health.record_queue(self.queue.qsize())
        return True

    async def consume(self, events) -> None:
        """Drain the supervisor's event iterator onto the queue."""
        async for event in events:
            if event.event_kind == EventKind.SLOT:
                gap = self.health.observe_slot(event.slot)
                if gap:
                    # Recorded, not acted on: Solana skips slots legitimately
                    # when a leader fails (3.8.5).
                    await repo.record_slot_gap(
                        self._pool, self._stream_id,
                        event.slot - gap - 1, event.slot,
                    )
                continue
            self.offer(event)

    # -- worker side -----------------------------------------------------
    def _retain(self, event: RawEvent) -> bool:
        mode = self._cfg.retention
        if event.is_ephemeral:
            return False
        if mode == "all":
            return True
        return event.event_kind == EventKind.CREATE

    async def _handle(self, event: RawEvent) -> None:
        self.health.record_event(event.event_kind, ts=event.received_at)

        if event.payload and event.payload.get("decode_error"):
            self.health.record_decode_failure(ts=event.received_at)

        # Fast path first, durable key second. Both are required: the cache is
        # bounded and empty after a restart, the key survives.
        if self.dedupe.seen(event.dedupe_key):
            self.health.record_dupe(ts=event.received_at)
            return

        if self._retain(event):
            inserted = await repo.insert_raw_event(self._pool, event, self._stream_id)
            if not inserted:
                self.health.record_dupe(ts=event.received_at)
                return
            if self._recorder:
                self._recorder.write(event)

        if event.event_kind == EventKind.CREATE:
            await self._record_creates(event)

    async def _record_creates(self, event: RawEvent) -> None:
        for create in (event.payload or {}).get("creates", []):
            mint = create.get("mint")
            if not mint:
                continue
            signer = create.get("user")
            declared = create.get("creator")
            block_time = None
            ts = create.get("timestamp")
            if isinstance(ts, int) and ts > 0:
                with contextlib.suppress(OverflowError, OSError, ValueError):
                    block_time = dt.datetime.fromtimestamp(ts, tz=dt.UTC)

            is_new = await repo.upsert_token(
                self._pool, mint=mint, stream_id=self._stream_id,
                signature=event.signature, slot=event.slot,
                program_id=event.program_id or "",
                fields={
                    "signer": signer,
                    "declared_creator": declared,
                    "launchpad": "pump.fun",
                    "bonding_curve": create.get("bonding_curve"),
                    "quote_mint": create.get("quote_mint"),
                    "token_program": create.get("token_program"),
                    "name": create.get("name"),
                    "symbol": create.get("symbol"),
                    "uri": create.get("uri"),
                    "token_total_supply": create.get("token_total_supply"),
                    "virtual_sol_reserves": create.get("virtual_sol_reserves"),
                    "virtual_token_reserves": create.get("virtual_token_reserves"),
                    "real_token_reserves": create.get("real_token_reserves"),
                    "is_mayhem_mode": create.get("is_mayhem_mode"),
                    "is_cashback_enabled": create.get("is_cashback_enabled"),
                    "block_time": block_time,
                },
            )
            if not is_new:
                continue
            # Both identities accumulate history. See repo.bump_creator.
            await repo.bump_creator(self._pool, signer or "", "signer")
            if declared and declared != signer:
                await repo.bump_creator(self._pool, declared, "declared")

    async def worker(self, name: str) -> None:
        while not self._stopping.is_set():
            try:
                event = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except TimeoutError:
                continue
            try:
                await self._handle(event)
            except asyncpg.PostgresError:
                kv(log, logging.ERROR, "database error handling event",
                   worker=name, signature=event.signature, slot=event.slot)
            except Exception:
                log.exception("worker %s failed on %s", name, event.signature)
            finally:
                self.queue.task_done()

    # -- periodic --------------------------------------------------------
    async def health_flusher(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=HEALTH_FLUSH_SECONDS)
            except TimeoutError:
                pass
            with contextlib.suppress(asyncpg.PostgresError):
                await repo.flush_health(self._pool, self._stream_id, self.health.drain())

    async def final_flush(self) -> None:
        """Flush every bucket including the current minute."""
        buckets = sorted(self.health.buckets.items())
        self.health.buckets.clear()
        with contextlib.suppress(asyncpg.PostgresError):
            await repo.flush_health(self._pool, self._stream_id, buckets)

    def stop(self) -> None:
        self._stopping.set()
