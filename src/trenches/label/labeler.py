"""The labeling job: observe every mint at fixed horizons and record what it found.

DECOUPLING IS A HARD REQUIREMENT, not a preference. This reads mints from the
database and writes to `outcomes`. It never touches the feed path, the feed
queue, or the ingest writer, and it imports nothing from pipeline.worker. When
run alongside `stream` it gets its OWN database connection and runs behind a
supervisor that swallows every exception, so a labeler that crashes in a loop
is invisible to ingest apart from log lines.

The reason for that severity: ingest is collecting data that cannot be
recovered later. A labeler bug must never be able to cost a launch.
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging
import random
from dataclasses import dataclass, field

from ..db import repo
from ..db.dialect import Database, iso, parse_ts
from ..enrich.rugcheck import RugCheckClient
from ..log import get, kv
from . import HORIZON_SECONDS, HORIZONS
from .dexscreener import BATCH_SIZE, DexScreenerClient
from .shared_budget import SharedBudget
from .venues import DEX, NONE, classify_markets, dex_liquidity_usd

log = get(__name__)

#: Below this, a pool exists but nothing could realistically be sold into it.
#: Configurable because it is a judgement, not a fact -- and because the right
#: floor is exactly the kind of thing this table is being built to calibrate.
DEFAULT_DEAD_LIQUIDITY_USD = 1000.0

#: Attempts before a horizon is written as status='error'. Bounded on purpose:
#: 'late is fine, missing is not', so a row that says we tried and failed is
#: worth more than a gap that cannot be told from 'never came due'.
MAX_ATTEMPTS = 3

#: RugCheck calls allowed per tick, on top of restricting the fallback to the
#: short horizons. Unbounded, the first live run made 694 RugCheck calls against
#: 28 DexScreener ones and crawled -- a secondary source had become the primary
#: one. The budget is the backstop; the horizon restriction does the real work.
#:
#: NOTE the cost of the budget: a mint skipped because it ran out is recorded
#: no_pool with venue_kind NULL, so "we did not check" is distinguishable from
#: "checked, found only a curve" (venue_kind='bonding_curve'). Never read a
#: NULL venue as evidence of absence.
DEFAULT_FALLBACK_BUDGET = 100


@dataclass(slots=True)
class LabelerStats:
    observed: int = 0
    no_pool: int = 0
    alive: int = 0
    dead: int = 0
    errors: int = 0
    requests: int = 0
    ticks: int = 0
    last_error: str | None = None
    by_horizon: dict[str, int] = field(default_factory=dict)


class Labeler:
    def __init__(
        self,
        db: Database,
        *,
        client: DexScreenerClient | None = None,
        rugcheck: RugCheckClient | None = None,
        dead_liquidity_usd: float = DEFAULT_DEAD_LIQUIDITY_USD,
        batch_limit: int = 500,
        fallback_budget: int = DEFAULT_FALLBACK_BUDGET,
        backfill: bool = False,
    ) -> None:
        self._db = db
        # Cross-process budget: `sample` may be running too.
        self._budget = SharedBudget(db)
        self._client = client or DexScreenerClient(shared_budget=self._budget)
        #: Fallback only. DexScreener saying nothing is ambiguous for a young
        #: mint, so before concluding 'no pool' we ask a second free source.
        self._rugcheck = rugcheck
        self._dead_floor = dead_liquidity_usd
        self._batch_limit = batch_limit
        self._fallback_budget = fallback_budget
        self._fallback_spent = 0
        self._backfill = backfill
        #: (mint, horizon) -> consecutive failed attempts. In memory on
        #: purpose: a provisional row in `outcomes` would occupy the primary
        #: key and block the real observation when the vendor recovers. The
        #: cost is that a restart resets the count, which only ever means we
        #: retry a few more times before giving up -- never that we skip.
        self._failures: dict[tuple[str, str], int] = {}
        self.stats = LabelerStats()

    # -- classification ----------------------------------------------------

    def _classify(self, *, pool_found: bool, liquidity_usd: str | None) -> str:
        """no_pool / alive / dead. Never 'unknown'.

        `no_pool` is a LABEL, not a failure to collect. The median launch never
        becomes tradeable, and recording that as an outcome is the entire point
        -- an unlabeled journal cannot tell a filter that killed a corpse from
        one that killed a winner.
        """
        if not pool_found:
            return "no_pool"
        try:
            liq = float(liquidity_usd) if liquidity_usd is not None else 0.0
        except (TypeError, ValueError):
            liq = 0.0
        return "alive" if liq >= self._dead_floor else "dead"

    async def _fallback_rugcheck(self, mint: str) -> dict | None:
        """Ask RugCheck where a token is trading before concluding nowhere.

        DexScreener indexes GRADUATED pools -- a young or never-graduated mint
        returns nothing, and reading that alone as 'no pool' would label most
        of the market dead. RugCheck sees the bonding curve too.

        Returns None when the budget is spent or nothing was found. Never
        collapses curve and pool liquidity into one number: `totalMarketLiquidity`
        sums both, which reports a token as deeper than anything it graduated
        into.
        """
        if self._rugcheck is None or self._fallback_spent >= self._fallback_budget:
            return None
        self._fallback_spent += 1
        probe = await self._rugcheck.report(mint)
        if not isinstance(probe.payload, dict):
            return None
        markets = probe.payload.get("markets") or []
        venue_kind, market_type = classify_markets(markets)
        if venue_kind == NONE:
            return None
        return {
            "venue_kind": venue_kind,
            "market_type": market_type,
            # DEX liquidity only for a graduated token; for a curve, the curve
            # reserve is the honest number and is labelled as such by venue_kind.
            "liquidity_usd": (
                dex_liquidity_usd(markets) if venue_kind == DEX
                else _curve_liquidity(markets)
            ),
            "payload": probe.payload,
        }

    # -- one pass ----------------------------------------------------------

    async def _observe_batch(self, horizon: str, rows: list[dict]) -> None:
        mints = [r["mint"] for r in rows]
        detected = {r["mint"]: parse_ts(r["detected_at"]) for r in rows}
        result = await self._client.tokens(mints)
        self.stats.requests += 1
        now = dt.datetime.now(tz=dt.UTC)
        horizon_s = HORIZON_SECONDS[horizon]

        for mint in mints:
            scheduled = (detected[mint] or now) + dt.timedelta(seconds=horizon_s)
            lateness = max(0, int((now - scheduled).total_seconds()))
            base = {
                "scheduled_for": iso(scheduled),
                "observed_at": iso(now),
                "lateness_seconds": lateness,
                "backfilled": self._backfill,
            }

            if not result.ok:
                # Transient until proven otherwise. Do not burn the horizon on
                # a network blip -- leave it unobserved so the next tick
                # retries. But 'late is fine, missing is not': after a bounded
                # number of attempts write status='error', because a row saying
                # we tried and failed is distinguishable from a gap, and a gap
                # is not distinguishable from 'never came due'.
                key = (mint, horizon)
                attempts = self._failures.get(key, 0) + 1
                self._failures[key] = attempts
                if attempts < MAX_ATTEMPTS:
                    continue
                await repo.record_outcome(self._db, mint=mint, horizon=horizon, fields={
                    **base, "status": "error", "source": "none", "pool_found": False,
                    "attempts": attempts, "error": result.error or f"http {result.status_code}",
                    "payload": None,
                })
                self._failures.pop(key, None)
                self.stats.errors += 1
                continue

            self._failures.pop((mint, horizon), None)

            view = result.found.get(mint)
            pool_found = view is not None
            liquidity = view.liquidity_usd if view else None
            payload = view.raw if view else []
            source = "dexscreener"
            # DexScreener only indexes graduated pools, so anything it answers
            # for is by definition a DEX venue.
            venue_kind = DEX if pool_found else NONE
            market_type = view.dex_id if view else None

            # The fallback exists for ONE case: a token that graduated but is
            # not indexed yet. DexScreener indexes graduated pools, so at 24h
            # and 7d its silence is not ambiguous at all -- it means "did not
            # graduate", which is the single most common outcome and a complete
            # label on its own. Asking a second vendor there spends the budget
            # confirming that a bonding curve exists, which is true of every
            # launch from birth and worth nothing.
            if not pool_found and horizon in ("15m", "1h"):
                fallback = await self._fallback_rugcheck(mint)
                if fallback:
                    pool_found = True
                    venue_kind = fallback["venue_kind"]
                    market_type = fallback["market_type"]
                    liquidity = fallback["liquidity_usd"]
                    payload = fallback["payload"]
                    source = "rugcheck"

            status = self._classify(pool_found=pool_found, liquidity_usd=liquidity)
            # An empty answer at 15m may mean 'not indexed yet' rather than
            # 'never had a pool'. Flag it so it cannot contaminate a
            # 'died instantly' label. At 24h/7d an empty answer is genuine.
            ambiguous = status == "no_pool" and horizon == "15m"

            fields = {
                **base,
                "status": status,
                "ambiguous_no_pool": ambiguous,
                "source": source if pool_found else "none",
                "pool_found": pool_found,
                "venue_kind": venue_kind,
                "market_type": market_type,
                "payload": payload,
            }
            if view is not None:
                fields.update(
                    price_usd=view.price_usd, liquidity_usd=view.liquidity_usd,
                    fdv_usd=view.fdv_usd, market_cap_usd=view.market_cap_usd,
                    volume_m5=view.volume_m5, volume_h1=view.volume_h1,
                    volume_h24=view.volume_h24,
                    txns_m5_buys=view.txns_m5_buys, txns_m5_sells=view.txns_m5_sells,
                    txns_h1_buys=view.txns_h1_buys, txns_h1_sells=view.txns_h1_sells,
                    txns_h24_buys=view.txns_h24_buys, txns_h24_sells=view.txns_h24_sells,
                    price_change_h24=view.price_change_h24,
                    pair_address=view.pair_address, pair_created_at=view.pair_created_at,
                    dex_id=view.dex_id,
                )
            elif liquidity is not None:
                fields["liquidity_usd"] = liquidity

            if await repo.record_outcome(self._db, mint=mint, horizon=horizon, fields=fields):
                self.stats.observed += 1
                self.stats.by_horizon[horizon] = self.stats.by_horizon.get(horizon, 0) + 1
                setattr(self.stats, status, getattr(self.stats, status) + 1)

            # Peak is updated on EVERY observation, including ones where the
            # pool is gone -- that is what makes it a running maximum rather
            # than a snapshot of the horizons we happened to catch.
            if view is not None:
                await repo.bump_peak(
                    self._db, mint, price_usd=view.price_usd,
                    liquidity_usd=view.liquidity_usd, observed_at=iso(now),
                )

    async def tick(self) -> int:
        """One pass over every horizon. Returns rows observed."""
        before = self.stats.observed
        self._fallback_spent = 0
        for horizon in HORIZONS:
            rows = await repo.due_horizons(
                self._db, horizon, HORIZON_SECONDS[horizon], limit=self._batch_limit
            )
            for i in range(0, len(rows), BATCH_SIZE):
                await self._observe_batch(horizon, rows[i:i + BATCH_SIZE])
        self.stats.ticks += 1
        return self.stats.observed - before

    async def backlog(self) -> dict[str, int]:
        return {
            h: await repo.due_count(self._db, h, HORIZON_SECONDS[h]) for h in HORIZONS
        }


async def run_forever(
    labeler: Labeler,
    *,
    interval_seconds: float = 60.0,
    backoff_min: float = 1.0,
    backoff_max: float = 60.0,
    stop: asyncio.Event | None = None,
) -> None:
    """Supervise the labeler. Never propagates an exception to the caller.

    This is the barrier that makes the decoupling claim true rather than
    intended. `stream --label` awaits this as a sibling task; if it could raise,
    a labeler bug would take ingest down with it.
    """
    attempt = 0
    while not (stop and stop.is_set()):
        try:
            observed = await labeler.tick()
            attempt = 0
            if observed:
                kv(log, logging.INFO, "labeled", rows=observed,
                   requests=labeler.stats.requests,
                   no_pool=labeler.stats.no_pool, alive=labeler.stats.alive,
                   dead=labeler.stats.dead)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # the barrier is the point: never propagate
            attempt += 1
            labeler.stats.errors += 1
            labeler.stats.last_error = f"{type(exc).__name__}: {exc}"
            delay = min(backoff_max, backoff_min * (2 ** min(attempt, 10)))
            delay = random.uniform(backoff_min, max(backoff_min, delay))  # noqa: S311
            kv(log, logging.WARNING, "labeler tick failed; ingest unaffected",
               error=labeler.stats.last_error, attempt=attempt,
               delay_seconds=round(delay, 2))
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(delay)
            continue
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(interval_seconds)


def _curve_liquidity(markets: list) -> str | None:
    """Reserve sitting in a bonding curve, as the vendor's exact string.

    Kept separate from pool liquidity by `venue_kind`. It is real -- you can
    sell into a curve -- but it is not evidence a token found a market.
    """
    for market in markets or []:
        if not isinstance(market, dict):
            continue
        raw = (market.get("lp") or {}).get("baseUSD")
        if raw is not None:
            return str(raw)
    return None
