"""Polls RugCheck for held/watched tokens, with cache bypass, and records events.

CACHE BYPASS IS THE POINT. RugCheck's report endpoint is cached behind its own
rug detector: research saw a token that rugged at 23:23:11 still returning
`score: 1, rugged: false` thirty seconds later. Reusing a cached verdict for a
held position is exactly the failure this module exists to catch, so every poll
here asks for a bypass and every stored row records whether one was requested.

Note the honest limit: `refresh=true` is paid-tier, so on the free tier a bypass
is REQUESTED and cannot be guaranteed. The flag records the request, not a
guarantee, and the field name says so.
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import itertools
import logging

from ..db import repo
from ..db.dialect import Database
from ..enrich.rugcheck import RugCheckClient
from ..log import get, kv
from . import detectors

log = get(__name__)

#: RugCheck is free and unauthenticated, so it is rate limited by rules we
#: cannot see. Sequential with a gap; this work is not latency sensitive.
DEFAULT_GAP_SECONDS = 1.0


class StructuralPoller:
    def __init__(
        self,
        db: Database,
        *,
        client: RugCheckClient | None = None,
        thresholds: detectors.Thresholds | None = None,
        gap_seconds: float = DEFAULT_GAP_SECONDS,
    ) -> None:
        self._db = db
        self._client = client or RugCheckClient("https://api.rugcheck.xyz/v1")
        self._thresholds = thresholds or detectors.Thresholds()
        self._gap = gap_seconds
        #: Last report per mint, so a change can be detected at all. Held in
        #: memory only; a restart simply loses one comparison rather than
        #: inventing a baseline.
        self._previous: dict[str, dict] = {}
        self.polls = 0
        self.events = 0

    async def poll_mint(self, mint: str, *, now: dt.datetime | None = None) -> list:
        now = now or dt.datetime.now(tz=dt.UTC)
        probe = await self._client.report(mint, fresh=True)   # cache bypass requested
        self.polls += 1
        if probe.error or not isinstance(probe.payload, dict):
            return []

        before = self._previous.get(mint)
        found = detectors.from_rugcheck(
            before, probe.payload, observed_at=now, thresholds=self._thresholds
        )
        self._previous[mint] = probe.payload
        for event in found:
            stored = await repo.record_structural_event(self._db, mint=mint, fields={
                "event_type": event.event_type, "detected_at": event.detected_at,
                "observed_at": event.observed_at, "derived_from": event.derived_from,
                "before_value": event.before_value, "after_value": event.after_value,
                "delta_pct": event.delta_pct, "severity": event.severity,
                "payload": {**event.payload, "bypass_requested": probe.bypassed_cache},
            })
            if stored:
                self.events += 1
                kv(log, logging.INFO, "structural event", mint=mint,
                   event=event.event_type, severity=event.severity,
                   delta_pct=event.delta_pct)
        return found

    async def scan_paths(self, mint: str) -> list:
        """Derive liquidity events from consecutive stored observations.

        Free: it reads rows already collected by the sampler rather than
        spending a request. An LP pull shows up here before it shows up as a
        fill that is not there.
        """
        rows = await repo.path_for_mint(self._db, mint)
        found = []
        for before, after in itertools.pairwise(rows):
            for event in detectors.from_price_path(
                before, after, thresholds=self._thresholds
            ):
                stored = await repo.record_structural_event(
                    self._db, mint=mint, fields={
                        "event_type": event.event_type, "detected_at": event.detected_at,
                        "observed_at": event.observed_at,
                        "derived_from": event.derived_from,
                        "before_value": event.before_value,
                        "after_value": event.after_value, "delta_pct": event.delta_pct,
                        "severity": event.severity, "payload": event.payload,
                    })
                if stored:
                    self.events += 1
                found.append(event)
        return found

    async def run(self, *, interval_seconds: float = 60.0, limit: int = 50) -> None:
        """Loop over active watch-set members. Never raises into the caller."""
        while True:
            try:
                rows = await repo.due_for_sampling(self._db, limit=limit)
                for row in rows:
                    await self.scan_paths(row["mint"])
                    await self.poll_mint(row["mint"])
                    await asyncio.sleep(self._gap)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("structural poll failed; continuing")
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(interval_seconds)
