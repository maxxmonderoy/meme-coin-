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

#: ALLOCATION, not just a ceiling.
#:
#: A single shared bucket prevents a ban but not starvation: whichever collector
#: asks most often wins, and the labeler at a high batch limit will consume
#: nearly the whole 60 req/min and leave the sampler a fraction of the budget
#: its own cadence arithmetic assumes. Measured: a labeler at 1,200 mints/min
#: takes 48 of 60 req/min on its own.
#:
#: So each collector gets a NAMED share with a guaranteed floor. The shares are
#: validated to sum within the account limit, which turns a silent contention
#: bug into a startup error.
ACCOUNT_LIMIT_PER_MINUTE = 60

DEFAULT_SHARES: dict[str, float] = {
    # Depth: dense paths on a bounded watch set. Given the larger share because
    # a trailing stop cannot be backtested on four data points, and the exit
    # work is what the paths exist for.
    "sample": 0.65,
    # Breadth: one poll per horizon across every mint. Still useful for rug
    # labelling over the whole market rather than the sampled slice.
    "label": 0.35,
}


class BudgetAllocationError(ValueError):
    pass


def validate_shares(shares: dict[str, float], *, limit: int = ACCOUNT_LIMIT_PER_MINUTE
                    ) -> dict[str, int]:
    """Turn shares into per-collector request rates, or refuse.

    Refusing is the point: two collectors quietly over-subscribing an account
    limit is exactly the failure that has no symptom until the ban.
    """
    if not shares:
        raise BudgetAllocationError("no shares given")
    total = sum(shares.values())
    if total > 1.0 + 1e-9:
        raise BudgetAllocationError(
            f"shares sum to {total:.2f}, which over-subscribes the {limit} req/min "
            "account limit. Collectors would contend and the loser would silently "
            "run below the rate its own arithmetic assumes."
        )
    rates = {name: int(limit * share) for name, share in shares.items()}
    for name, rate in rates.items():
        if rate < 1:
            raise BudgetAllocationError(
                f"share for {name!r} rounds to 0 req/min; it would never run")
    return rates

#: Burst allowance. One second's worth, so a batch of callers starting together
#: does not immediately exceed the per-minute rate.
DEFAULT_CAPACITY_SECONDS = 1.0


class SharedBudget:
    def __init__(
        self,
        db: Database,
        *,
        name: str = DEXSCREENER,
        rate_per_minute: int = ACCOUNT_LIMIT_PER_MINUTE,
        capacity_seconds: float = DEFAULT_CAPACITY_SECONDS,
        max_wait_seconds: float = 30.0,
    ) -> None:
        self._db = db
        self._name = name
        self._rate = float(rate_per_minute)
        self._capacity = max(1.0, self._rate * capacity_seconds / 60.0)
        self._max_wait = max_wait_seconds

    @classmethod
    def allocated(
        cls, db: Database, collector: str, *, shares: dict[str, float] | None = None,
        limit: int = ACCOUNT_LIMIT_PER_MINUTE,
    ) -> SharedBudget:
        """A budget for one named collector, holding a guaranteed share.

        Each collector gets its own row, so a busy one cannot spend another's
        allowance. The shares are validated against the account limit up front.
        """
        rates = validate_shares(shares or DEFAULT_SHARES, limit=limit)
        if collector not in rates:
            raise BudgetAllocationError(
                f"no share allocated for {collector!r}; known: {sorted(rates)}")
        return cls(db, name=f"{DEXSCREENER}:{collector}",
                   rate_per_minute=rates[collector])

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
