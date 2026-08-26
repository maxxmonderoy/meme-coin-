"""Receive -> bounded queue -> worker pool -> journal.

Nothing is processed on the receive path (3.8.2). Dedupe is on MINT (3.3),
because two independent feeds describe the same launch with different
identifiers and the mint is the only one they agree on.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging

from ..config import Config
from ..db import repo
from ..db.dialect import Database
from ..log import get, kv
from ..stream.base import EventKind, RawEvent
from ..stream.recorder import Recorder
from .dedupe import DedupeCache
from .health import HealthTracker

log = get(__name__)

HEALTH_FLUSH_SECONDS = 20


class Ingest:
    def __init__(
        self, db: Database, cfg: Config, stream_id: int, *, recorder: Recorder | None = None
    ) -> None:
        self._db = db
        self._cfg = cfg
        self._stream_id = stream_id
        self._recorder = recorder
        self.queue: asyncio.Queue[RawEvent] = asyncio.Queue(maxsize=cfg.queue_maxsize)
        #: Candidate dedupe, keyed on mint. TTL is long here because the same
        #: launch reaching us from the second feed minutes later must still be
        #: recognised as the same candidate -- that gap is the measurement.
        self.dedupe = DedupeCache(ttl_seconds=cfg.dedupe_ttl_seconds)
        self.health = HealthTracker()
        self.dropped = 0
        self._stopping = asyncio.Event()

    # -- receive side ----------------------------------------------------
    def offer(self, event: RawEvent) -> bool:
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1
            self.health.record_queue(event.provider, self.queue.qsize(), dropped=1)
            if self.dropped % 100 == 1:
                kv(log, logging.ERROR, "queue full; dropping events",
                   dropped_total=self.dropped, maxsize=self._cfg.queue_maxsize)
            return False
        self.health.record_queue(event.provider, self.queue.qsize())
        return True

    async def consume(self, events) -> None:
        async for event in events:
            if event.is_ephemeral:
                continue
            self.offer(event)

    # -- worker side -----------------------------------------------------
    def _retain(self, event: RawEvent) -> bool:
        if event.is_ephemeral:
            return False
        if self._cfg.retention == "all":
            return True
        return event.event_kind in (EventKind.CREATE, EventKind.MIGRATE)

    async def _handle(self, event: RawEvent) -> None:
        self.health.record_event(event.provider, event.event_kind, ts=event.received_at)
        payload = event.payload or {}
        if payload.get("schema_mismatch"):
            self.health.record_schema_mismatch(event.provider, ts=event.received_at)
        if payload.get("decode_error"):
            self.health.record_decode_failure(event.provider, ts=event.received_at)

        # raw_events is keyed on (feed, event_id), so the SAME launch seen by
        # BOTH feeds is stored twice on purpose. That is the race record.
        if self._retain(event) and event.event_id:
            stored = await repo.insert_raw_event(self._db, event, self._stream_id)
            if not stored:
                self.health.record_dupe(event.provider, ts=event.received_at)
                return
            if self._recorder:
                self._recorder.write(event)

        if not event.is_candidate or not event.mint:
            return
        await self._handle_candidate(event)

    async def _handle_candidate(self, event: RawEvent) -> None:
        mint = event.mint
        assert mint is not None

        # Mark before acting. A duplicate detection firing twice is worse than
        # a missed launch (3.8.4), and this is the same
        # idempotency-key-before-the-call rule from 3.6 applied at detection.
        already_seen = self.dedupe.seen((mint, 0))

        if already_seen:
            self.health.record_dupe(event.provider, ts=event.received_at)
            # The losing feed still carries information: how far behind it was.
            await repo.record_second_sight(
                self._db, mint=mint, feed=event.provider, seen_at=event.received_at
            )
            return

        fields = self._token_fields(event)
        is_new = await repo.upsert_token(
            self._db, mint=mint, stream_id=self._stream_id, feed=event.provider, fields=fields
        )
        await repo.record_first_sight(
            self._db, mint=mint, feed=event.provider,
            seen_at=event.received_at, stream_id=self._stream_id,
        )
        if not is_new:
            return

        self.health.record_win(event.provider, ts=event.received_at)
        signer = fields.get("signer")
        declared = fields.get("declared_creator")
        if signer:
            await repo.bump_creator(self._db, signer, "signer")
        if declared and declared != signer:
            await repo.bump_creator(self._db, declared, "declared")

    @staticmethod
    def _token_fields(event: RawEvent) -> dict:
        """Per-feed field mapping, kept at the edge rather than in the schema."""
        raw = (event.payload or {}).get("raw") or {}
        if event.provider == "pumpportal":
            from ..stream.pumpportal import token_fields

            fields = token_fields(raw)
        elif event.provider == "rugcheck":
            from ..stream.rugcheck_feed import token_fields

            fields = token_fields(raw)
        else:
            fields = dict(raw) if isinstance(raw, dict) else {}
        fields.setdefault("signature", event.signature)
        fields.setdefault("slot", event.slot)
        fields["block_time"] = event.block_time
        return fields

    async def worker(self, name: str) -> None:
        while not self._stopping.is_set():
            try:
                event = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except TimeoutError:
                continue
            try:
                await self._handle(event)
            except Exception:
                log.exception("worker %s failed on %s", name, event.event_id)
            finally:
                self.queue.task_done()

    # -- periodic --------------------------------------------------------
    async def health_flusher(self) -> None:
        while not self._stopping.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=HEALTH_FLUSH_SECONDS)
            with contextlib.suppress(Exception):
                await repo.flush_health(self._db, self._stream_id, self.health.drain())

    async def final_flush(self) -> None:
        buckets = sorted(self.health.buckets.items())
        self.health.buckets.clear()
        with contextlib.suppress(Exception):
            await repo.flush_health(self._db, self._stream_id, buckets)

    def stop(self) -> None:
        self._stopping.set()
