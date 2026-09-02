"""The sampling loop.

DECOUPLED FROM INGEST BY CONSTRUCTION. It reads mints from the database and
writes its own tables. It shares no queue, no socket and no task group with the
feed path, so if it dies or falls behind the stream does not notice -- which is
the property the tests assert rather than assume.

Everything it does is read-only observation. It cannot sign anything and holds
no key material.
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging
from decimal import Decimal, InvalidOperation

from ..db import repo
from ..db.dialect import Database
from ..label import venues
from ..label.dexscreener import BATCH_SIZE, RATE_LIMIT_PER_MINUTE, DexScreenerClient
from ..log import get, kv
from .cadence import (
    DEFAULT_CADENCE,
    DEFAULT_MAX_AGE_SECONDS,
    Budget,
    compute_budget,
    describe,
    interval_for_age,
)
from .watchset import Admitter, expiry

log = get(__name__)

#: A mint younger than this that DexScreener has no pair for is almost
#: certainly not yet indexed rather than dead. Research saw a token under 60
#: seconds old return {"pairs": null} and only get indexed hours later, so this
#: is deliberately generous -- calling a live token dead is far more expensive
#: than carrying an "unknown" for an extra hour.
NOT_YET_INDEXED_GRACE_SECONDS = 3600

STATUS_INDEXED = "indexed"
STATUS_NOT_YET = "not_yet_indexed"
STATUS_NO_POOL = "no_pool"
STATUS_ERROR = "error"


def _dec(value) -> Decimal | None:
    if value is None:
        return None
    with contextlib.suppress(InvalidOperation, ValueError, TypeError):
        return Decimal(str(value))
    return None


class Sampler:
    def __init__(
        self,
        db: Database,
        *,
        client: DexScreenerClient | None = None,
        cadence: tuple[tuple[int, int], ...] = DEFAULT_CADENCE,
        max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
        control_share: float = 0.5,
        batch_size: int = BATCH_SIZE,
        requests_per_minute: int = RATE_LIMIT_PER_MINUTE,
    ) -> None:
        self._db = db
        self._client = client or DexScreenerClient()
        self._cadence = cadence
        self._max_age = max_age_seconds
        self.budget: Budget = compute_budget(
            batch_size=batch_size, requests_per_minute=requests_per_minute,
            cadence=cadence, max_age_seconds=max_age_seconds,
        )
        self.admitter = Admitter(
            watch_set_max=self.budget.watch_set_max, control_share=control_share
        )
        self.observations = 0
        self.admitted = 0

    def log_budget(self) -> None:
        for line in describe(self.budget):
            log.info("budget | %s", line)

    # -- admission -------------------------------------------------------
    async def admit(
        self, mint: str, first_seen: dt.datetime, *, passes_filter: bool,
        now: dt.datetime | None = None,
    ) -> str | None:
        """`now` is injectable so scheduling is testable against a fixed clock."""
        def now_fn() -> dt.datetime:
            return now or dt.datetime.now(tz=dt.UTC)

        counts = await repo.watch_set_counts(self._db)
        decision = self.admitter.consider(mint, passes_filter=passes_filter, current=counts)
        if not decision.admit:
            return None
        # The cadence clock starts at LAUNCH, not at admission. A mint admitted
        # three minutes late belongs on the launch-relative schedule; starting
        # its clock at admission would put a three-minute-old token on the
        # 30-second cadence meant for a brand new one, and would make two
        # tokens of the same age carry different sampling densities purely
        # because of when we happened to notice them.
        age_at_admission = max(0.0, (now_fn() - first_seen).total_seconds())
        interval = interval_for_age(
            age_at_admission, self._cadence, max_age_seconds=self._max_age
        )
        if interval is None:
            return None
        ok = await repo.admit_to_watch_set(
            self._db, mint=mint, cohort=decision.cohort or "filtered",
            first_seen=first_seen, expires_at=expiry(first_seen, self._max_age),
            next_due_at=first_seen + dt.timedelta(seconds=age_at_admission + interval),
        )
        if ok:
            self.admitted += 1
        return decision.cohort if ok else None

    # -- observation -----------------------------------------------------
    async def tick(self, *, now: dt.datetime | None = None) -> dict:
        """One pass: take everything due, in batches, and record observations."""
        now = now or dt.datetime.now(tz=dt.UTC)
        due = await repo.due_for_sampling(self._db, limit=self.budget.batch_size * 4, now=now)
        if not due:
            return {"due": 0, "observed": 0, "batches": 0}

        by_mint = {row["mint"]: row for row in due}
        mints = list(by_mint)
        observed = batches = 0
        for start in range(0, len(mints), self.budget.batch_size):
            chunk = mints[start : start + self.budget.batch_size]
            batches += 1
            result = await self._client.tokens(chunk)
            observed += await self._record_chunk(chunk, by_mint, result, now)
        self.observations += observed
        return {"due": len(due), "observed": observed, "batches": batches}

    async def _record_chunk(self, chunk, by_mint, result, now) -> int:
        recorded = 0
        for mint in chunk:
            row = by_mint[mint]
            first_seen = dt.datetime.fromisoformat(row["first_seen_at"])
            scheduled = dt.datetime.fromisoformat(row["next_due_at"])
            age = (now - first_seen).total_seconds()
            view = (result.found or {}).get(mint) if result is not None else None

            fields = {
                "observed_at": now,
                "scheduled_for": scheduled,
                "lateness_ms": max(0, int((now - scheduled).total_seconds() * 1000)),
                "source": "dexscreener",
                "age_seconds": int(age),
            }
            if result is not None and result.error:
                fields["status"] = STATUS_ERROR
                fields["payload"] = {"error": result.error}
            elif view is None:
                # The cold start. Absence early is "not indexed yet"; absence
                # later is a real answer. Conflating them manufactures deaths
                # for exactly the youngest tokens this exists to observe.
                fields["status"] = (
                    STATUS_NOT_YET if age < NOT_YET_INDEXED_GRACE_SECONDS else STATUS_NO_POOL
                )
            else:
                fields["status"] = STATUS_INDEXED
                fields.update({
                    # PairView carries dex_id, not marketType. `classify`
                    # returns DEX for anything unrecognised, which is the
                    # conservative direction: mislabelling a curve as a pool
                    # inflates graduation and gets noticed, the reverse hides.
                    "venue_kind": venues.classify(getattr(view, "dex_id", None)),
                    "price_usd": getattr(view, "price_usd", None),

                    "liquidity_usd": getattr(view, "liquidity_usd", None),
                    "fdv_usd": getattr(view, "fdv_usd", None),
                    "market_cap_usd": getattr(view, "market_cap_usd", None),
                    "volume_m5": getattr(view, "volume_m5", None),
                    "volume_h1": getattr(view, "volume_h1", None),
                    "txns_m5_buys": getattr(view, "txns_m5_buys", None),
                    "txns_m5_sells": getattr(view, "txns_m5_sells", None),
                    "pair_address": getattr(view, "pair_address", None),
                    "dex_id": getattr(view, "dex_id", None),
                })
            if await repo.record_observation(self._db, mint=mint, fields=fields):
                recorded += 1
            next_interval = interval_for_age(age, self._cadence, max_age_seconds=self._max_age)
            await repo.advance_watch(
                self._db, mint=mint, observed_at=now,
                next_due_at=(now + dt.timedelta(seconds=next_interval))
                if next_interval else None,
                missed=fields["status"] == STATUS_ERROR,
            )
        return recorded

    async def run(self, *, interval_seconds: float = 5.0) -> None:
        """Loop until cancelled. Never raises into the caller."""
        self.log_budget()
        while True:
            try:
                report = await self.tick()
                if report["observed"]:
                    kv(log, logging.INFO, "sampled", **report)
            except asyncio.CancelledError:
                raise
            except Exception:
                # A sampler failure must never take anything else down with it.
                log.exception("sampler tick failed; continuing")
            await asyncio.sleep(interval_seconds)
