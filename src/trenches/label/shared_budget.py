"""A token bucket shared across processes, backed by the database.

WHY NOT AN IN-PROCESS BUCKET. `trenches label` and `trenches sample` both call
DexScreener and each built its own limiter at the documented 60 req/min, so
running both meant 120 req/min against a 60 limit. DexScreener exposes no
rate-limit headers, so the first symptom would have been a ban rather than a
warning. A module-level singleton does not help: these are separate processes.
The database is the only state they already share.

CORRECTNESS. Refill and spend happen in one optimistic compare-and-swap: the
UPDATE carries the timestamp it read, so if another process moved the row first
the update matches nothing and this one retries. That is why two processes
cannot both spend the same token, and it works identically on SQLite and
Postgres without either dialect's locking syntax.

FAIL CLOSED. If the row cannot be read or written, `acquire` waits rather than
proceeding. A limiter that fails open is not a limiter.
"""
from __future__ import annotations

import asyncio
import datetime as dt

from ..db.dialect import Database, iso, parse_ts
from ..log import get

log = get(__name__)

#: Name of the budget every DexScreener caller shares.
DEXSCREENER = "dexscreener"

#: Burst allowance. One second's worth, so a batch of callers starting together
#: does not immediately exceed the per-minute rate.
DEFAULT_CAPACITY_SECONDS = 1.0


class SharedBudget:
    def __init__(
        self,
        db: Database,
        *,
        name: str = DEXSCREENER,
        rate_per_minute: int = 60,
        capacity_seconds: float = DEFAULT_CAPACITY_SECONDS,
        max_wait_seconds: float = 30.0,
    ) -> None:
        self._db = db
        self._name = name
        self._rate = float(rate_per_minute)
        self._capacity = max(1.0, self._rate * capacity_seconds / 60.0)
        self._max_wait = max_wait_seconds

    async def ensure(self) -> None:
        """Create the row if it is missing. Safe to call concurrently."""
        now = dt.datetime.now(tz=dt.UTC)
        await self._db.execute(
            "insert into rate_budget (name, tokens, rate_per_minute, capacity, updated_at) "
            "values (?,?,?,?,?) on conflict (name) do nothing",
            self._name, self._capacity, self._rate, self._capacity, iso(now),
        )

    async def try_acquire(self, *, now: dt.datetime | None = None) -> bool:
        """Spend one token if the budget allows. Never blocks."""
        now = now or dt.datetime.now(tz=dt.UTC)
        row = await self._db.fetchrow(
            "select tokens, updated_at, rate_per_minute, capacity, spent_total "
            "from rate_budget where name = ?", self._name)
        if row is None:
            await self.ensure()
            return False

        previous = parse_ts(row["updated_at"])
        if previous is None:
            return False
        elapsed = max(0.0, (now - previous).total_seconds())
        rate = float(row["rate_per_minute"]) or self._rate
        capacity = float(row["capacity"]) or self._capacity
        tokens = min(capacity, float(row["tokens"]) + elapsed * rate / 60.0)
        if tokens < 1.0:
            return False

        # Compare-and-swap on updated_at: if another process spent from this row
        # first, its timestamp no longer matches and this update affects nothing.
        status = await self._db.execute(
            "update rate_budget set tokens = ?, updated_at = ?, spent_total = ? "
            "where name = ? and updated_at = ?",
            tokens - 1.0, iso(now), int(row["spent_total"]) + 1,
            self._name, row["updated_at"],
        )
        return not status.endswith(" 0")

    async def acquire(self, *, poll_seconds: float = 0.1) -> None:
        """Block until a token is available.

        Fails CLOSED: on a persistent database problem this waits rather than
        letting the caller through, because a limiter that fails open is not a
        limiter -- it is a ban with extra steps.
        """
        waited = 0.0
        while True:
            try:
                if await self.try_acquire():
                    return
            except Exception:
                log.exception("shared rate budget unavailable; holding the caller")
            await asyncio.sleep(poll_seconds)
            waited += poll_seconds
            if waited >= self._max_wait:
                log.warning(
                    "waited %.0fs for the shared %s budget; still waiting. Are two "
                    "collectors running against one budget on purpose?",
                    waited, self._name)
                waited = 0.0
