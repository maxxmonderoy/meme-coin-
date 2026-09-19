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
from ..version import code_version
from .dedupe import DedupeCache
from .health import HealthTracker

log = get(__name__)

HEALTH_FLUSH_SECONDS = 20


class Ingest:
    def __init__(
        self, db: Database, cfg: Config, stream_id: int, *,
        recorder: Recorder | None = None, paper=None, cascade=None,
    ) -> None:
        """`paper` is a PaperRunner and `cascade` the 3.4 stages, both optional.

        With neither, this is exactly the week-1 ingest: it decides nothing and
        opens nothing, which is what `stream --no-paper` gives you.
        """
        self._db = db
        self._cfg = cfg
        self._stream_id = stream_id
        self._recorder = recorder
        #: One queue PER WORKER, and an event is routed by its mint.
        #:
        #: A single shared queue with N workers reorders events for the same
        #: mint: the create that arms a position can be handled after that
        #: mint's first trades, and two ticks can hit the exit ladder out of
        #: order. Both are silent -- the ladder just acts on a stale price.
        #: Routing on mint keeps per-mint order exact while keeping the pool
        #: parallel across different mints, which is where the parallelism was
        #: actually wanted.
        self._queues: list[asyncio.Queue[RawEvent]] = [
            asyncio.Queue(maxsize=max(1, cfg.queue_maxsize // max(1, cfg.workers)))
            for _ in range(max(1, cfg.workers))
        ]
        #: Candidate dedupe, keyed on mint. TTL is long here because the same
        #: launch reaching us from the second feed minutes later must still be
        #: recognised as the same candidate -- that gap is the measurement.
        self.dedupe = DedupeCache(ttl_seconds=cfg.dedupe_ttl_seconds)
        self.health = HealthTracker()
        self.dropped = 0
        self._paper = paper
        self._cascade = cascade
        self.ticks_stored = 0
        self.decisions_made = 0
        self.armed = 0
        self._stopping = asyncio.Event()

    # -- receive side ----------------------------------------------------
    @property
    def queue(self) -> asyncio.Queue[RawEvent]:
        """Back-compat view for tests and shutdown; depth is the sum."""
        return self._queues[0]

    def qsize(self) -> int:
        return sum(q.qsize() for q in self._queues)

    def _route(self, event: RawEvent) -> asyncio.Queue[RawEvent]:
        """Same mint always lands on the same worker. Unattributed events
        spread by signature so they still parallelise."""
        key = event.mint or event.signature or ""
        return self._queues[hash(key) % len(self._queues)]

    def offer(self, event: RawEvent) -> bool:
        try:
            self._route(event).put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1
            self.health.record_queue(event.provider, self.qsize(), dropped=1)
            if self.dropped % 100 == 1:
                kv(log, logging.ERROR, "queue full; dropping events",
                   dropped_total=self.dropped, maxsize=self._cfg.queue_maxsize)
            return False
        self.health.record_queue(event.provider, self.qsize())
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

        if event.event_kind == EventKind.TRADE:
            await self._handle_trade(event)
            return

        if not event.is_candidate or not event.mint:
            return
        await self._handle_candidate(event)

    async def _handle_trade(self, event: RawEvent) -> None:
        """Store one price observation and let the ladder act on it.

        The tick is persisted BEFORE the simulator sees it. If the process dies
        mid-position the tape survives, and `trenches exits` can rebuild the
        position from it; the reverse order would lose the observation that
        moved the ladder.
        """
        if not event.mint or not event.signature:
            return
        raw = (event.payload or {}).get("raw") or {}
        price = (event.payload or {}).get("price_sol")
        stored = await repo.insert_trade_tick(self._db, mint=event.mint,
                                              signature=event.signature, fields={
            "observed_at": event.received_at,
            "is_buy": (event.payload or {}).get("is_buy"),
            "sol_amount": str(raw.get("solAmount")) if raw.get("solAmount") is not None else None,
            "token_amount": (
                str(raw.get("tokenAmount")) if raw.get("tokenAmount") is not None else None
            ),
            "price_sol": price,
            "trader": raw.get("traderPublicKey"),
            "pool": raw.get("pool"),
            "source": event.provider,
            "payload": raw,
        })
        if stored:
            self.ticks_stored += 1
        if self._paper is None:
            return
        from decimal import Decimal

        from ..paper.simulator import Tick

        await self._paper.on_tick(event.mint, Tick(
            at=event.received_at,
            price_sol=Decimal(price) if price else None,
            signature=event.signature,
            is_buy=(event.payload or {}).get("is_buy"),
        ))

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
        await self._decide(mint, fields)
        signer = fields.get("signer")
        declared = fields.get("declared_creator")
        if signer:
            await repo.bump_creator(self._db, signer, "signer")
        if declared and declared != signer:
            await repo.bump_creator(self._db, declared, "declared")

    async def _decide(self, mint: str, fields: dict) -> None:
        """Run the cascade on what is knowable NOW, journal it, and arm on accept.

        Deliberately does not fetch. At detection the structural facts have not
        been retrieved for this mint, so stage 1 records `unfetched` -- which is
        the honest state at t=0 and exactly what Part 2 describes. `trenches
        structural` backfills them afterwards for analysis; it does not
        retroactively change a decision that was already made and journalled.
        """
        if self._cascade is None:
            return
        import time

        started = time.perf_counter()
        facts = {"mint": mint, **fields}
        verdict, trail = self._cascade.run(facts)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        await repo.record_decision(self._db, mint=mint, fields={
            "stream_id": self._stream_id,
            "mode": "PAPER",
            "outcome": "accept" if verdict.accept else "reject",
            "reject_stage": None if verdict.accept else verdict.stage,
            "reject_reason": verdict.reason,
            "inputs": {str(v.stage): v.inputs for v in trail},
            "cascade_ms": {str(v.stage): elapsed_ms for v in trail},
            "decision_latency_ms": elapsed_ms,
            "thresholds": self._cascade.thresholds(),
            "code_version": code_version(),
        })
        self.decisions_made += 1
        if verdict.accept and self._paper is not None:
            # Arming is not buying. The position opens at the first REAL traded
            # price; there is no price at detection because nobody has traded
            # yet, and entering at an invented number is how a simulator
            # manufactures an edge.
            self._paper.arm(mint)
            self.armed += 1

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

    async def worker(self, name: str, index: int = 0) -> None:
        queue = self._queues[index % len(self._queues)]
        while not self._stopping.is_set():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=1.0)
            except TimeoutError:
                continue
            try:
                await self._handle(event)
            except Exception:
                log.exception("worker %s failed on %s", name, event.event_id)
            finally:
                queue.task_done()

    # -- periodic --------------------------------------------------------
    async def health_flusher(self) -> None:
        while not self._stopping.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=HEALTH_FLUSH_SECONDS)
            with contextlib.suppress(Exception):
                await repo.flush_health(self._db, self._stream_id, self.health.drain())

    async def join(self) -> None:
        """Wait for every queue to drain."""
        for queue in self._queues:
            await queue.join()

    async def final_flush(self) -> None:
        buckets = sorted(self.health.buckets.items())
        self.health.buckets.clear()
        with contextlib.suppress(Exception):
            await repo.flush_health(self._db, self._stream_id, buckets)

    def stop(self) -> None:
        self._stopping.set()
