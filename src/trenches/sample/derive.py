"""Derive outcome horizons from the dense price path.

WHY THIS EXISTS. `label` polls each mint once per horizon and `sample` observes
it every 30 seconds. Run separately they duplicate work, compete for one 60
req/min budget, and the polled version is strictly worse: a single observation
taken whenever the poller got to it, versus the nearest sample from a path.

So the path becomes the source and `outcomes` becomes a projection of it. That
keeps `creators.n_rugged` -- and therefore stage 2, the only cascade rule that
rejects on creator history -- working unchanged, while removing the second
collector entirely.

WHAT IS AND IS NOT IMPROVED. The horizon is measured from the same launch time
either way, so this does not make a 15-minute observation appear where none was
possible. What it does is pick the observation NEAREST the checkpoint from a
path sampled every 30 seconds, and record exactly how far off it was. A polled
label that arrived two hours late and a derived one thirty seconds off are both
honest; only one is useful.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from ..db import repo
from ..db.dialect import Database
from ..label import HORIZON_SECONDS, HORIZONS
from ..log import get

log = get(__name__)

#: How far from a checkpoint an observation may be and still represent it.
#: Generous relative to the 30s cadence early on, tight enough that a 24h
#: checkpoint is not answered by a sample from the following afternoon.
DEFAULT_TOLERANCE = {
    "15m": 5 * 60,
    "1h": 15 * 60,
    "24h": 2 * 3600,
    "7d": 12 * 3600,
}


def _dec(value) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None


@dataclass(slots=True)
class DeriveReport:
    mints: int = 0
    written: int = 0
    skipped_no_observation: int = 0
    by_horizon: dict[str, int] = None

    def __post_init__(self) -> None:
        if self.by_horizon is None:
            self.by_horizon = {}


def nearest(
    observations: list[dict], target: dt.datetime, tolerance_seconds: int
) -> tuple[dict | None, float]:
    """The observation closest to `target`, and its offset in seconds.

    Returns (None, 0) when nothing falls inside the tolerance. Deliberately not
    "the last observation before the checkpoint": for a token that stops being
    observed, that would silently answer a 24h checkpoint with a 20-minute-old
    sample and call it a 24h outcome.
    """
    best, best_gap = None, None
    for row in observations:
        at = dt.datetime.fromisoformat(row["observed_at"])
        gap = abs((at - target).total_seconds())
        if best_gap is None or gap < best_gap:
            best, best_gap = row, gap
    if best is None or best_gap is None or best_gap > tolerance_seconds:
        return None, 0.0
    return best, best_gap


def status_for(row: dict, liquidity_floor: float) -> str:
    """Map a path observation onto an `outcomes` status.

    `not_yet_indexed` maps to no_pool with the ambiguity preserved by the
    caller: at a 15-minute checkpoint the vendor genuinely may not have indexed
    the token yet, and Part 2 forbids reading that as death.
    """
    if row["status"] == "error":
        return "error"
    if row["status"] in ("not_yet_indexed", "no_pool"):
        return "no_pool"
    liquidity = _dec(row.get("liquidity_usd"))
    if liquidity is None or float(liquidity) < liquidity_floor:
        return "dead"
    return "alive"


async def derive_for_mint(
    db: Database, mint: str, *, first_seen: dt.datetime, liquidity_floor: float = 1000.0,
    tolerance: dict[str, int] | None = None,
) -> dict[str, bool]:
    """Write outcome rows for every horizon the path can answer."""
    tolerance = tolerance or DEFAULT_TOLERANCE
    observations = await repo.path_for_mint(db, mint)
    if not observations:
        return {}

    written: dict[str, bool] = {}
    for horizon in HORIZONS:
        target = first_seen + dt.timedelta(seconds=HORIZON_SECONDS[horizon])
        row, gap = nearest(observations, target, tolerance.get(horizon, 3600))
        if row is None:
            continue
        status = status_for(row, liquidity_floor)
        written[horizon] = await repo.record_outcome(db, mint=mint, horizon=horizon, fields={
            "scheduled_for": target,
            "observed_at": dt.datetime.fromisoformat(row["observed_at"]),
            "lateness_seconds": int(gap),
            "status": status,
            # A 15m checkpoint answered by `not_yet_indexed` is ambiguous, not a
            # death -- the same flag the poller sets, for the same reason.
            "ambiguous_no_pool": int(
                row["status"] == "not_yet_indexed" and horizon in ("15m", "1h")),
            "backfilled": 1,
            "source": "price_path",
            "pool_found": int(row["status"] == "indexed"),
            "price_usd": row.get("price_usd"),
            "liquidity_usd": row.get("liquidity_usd"),
            "fdv_usd": row.get("fdv_usd"),
            "market_cap_usd": row.get("market_cap_usd"),
            "volume_m5": row.get("volume_m5"),
            "volume_h1": row.get("volume_h1"),
            "txns_m5_buys": row.get("txns_m5_buys"),
            "txns_m5_sells": row.get("txns_m5_sells"),
            "pair_address": row.get("pair_address"),
            "dex_id": row.get("dex_id"),
            "venue_kind": row.get("venue_kind"),
            "payload": {"derived_from": "price_path", "offset_seconds": int(gap)},
        })
    return written


async def derive_all(
    db: Database, *, limit: int = 1000, liquidity_floor: float = 1000.0
) -> DeriveReport:
    report = DeriveReport()
    rows = await db.fetch(
        "select mint, first_seen_at from watch_set order by admitted_at desc limit ?", limit)
    for row in rows:
        report.mints += 1
        written = await derive_for_mint(
            db, row["mint"],
            first_seen=dt.datetime.fromisoformat(row["first_seen_at"]),
            liquidity_floor=liquidity_floor,
        )
        if not written:
            report.skipped_no_observation += 1
        for horizon, ok in written.items():
            if ok:
                report.written += 1
                report.by_horizon[horizon] = report.by_horizon.get(horizon, 0) + 1
    return report
