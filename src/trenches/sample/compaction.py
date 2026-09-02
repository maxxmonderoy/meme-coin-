"""Downsample old paths so the table does not outgrow the disk.

At 30-second resolution the first hour of one token is 120 rows, and the watch
set holds thousands. Designing retention now rather than when the disk is full
and the stream has stopped is the whole point of this module existing before it
is needed.

WHAT IS PRESERVED. Compaction keeps the first and last observation in every
kept interval plus every LOCAL EXTREME -- the running peak and trough. A
trailing stop is a function of peaks and the falls from them, so dropping an
extreme is the one thing that would change a replayed result. Dropping a
mid-slope point does not.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from ..db import repo
from ..db.dialect import Database, iso
from ..log import get

log = get(__name__)

#: Paths younger than this keep full resolution. Beyond it the dense samples
#: have already served their purpose: the exit decisions they inform all happen
#: inside the first hours.
DEFAULT_COMPACT_AFTER_SECONDS = 24 * 3600

#: Resolution kept after compaction.
DEFAULT_KEPT_INTERVAL_SECONDS = 300


def _price(row: dict) -> Decimal | None:
    value = row.get("price_usd")
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def select_survivors(
    rows: list[dict], *, kept_interval_seconds: int = DEFAULT_KEPT_INTERVAL_SECONDS
) -> list[dict]:
    """Which observations survive compaction.

    Extremes are always kept. Everything else is thinned to one row per interval.
    """
    if len(rows) <= 2:
        return list(rows)

    keep: set[str] = {rows[0]["observed_at"], rows[-1]["observed_at"]}

    prices = [(r, _price(r)) for r in rows]
    priced = [(r, p) for r, p in prices if p is not None]
    if priced:
        # Running extremes: the peaks a trailing stop trails from, and the
        # troughs a stop triggers at.
        # KEEP NEW PEAKS AND EVERY DEEPENING DRAWDOWN FROM THEM.
        #
        # The obvious version -- keep new running highs and new running lows --
        # is WRONG for the rule this exists to preserve. A trailing stop fires
        # on the descent from a peak, and points on that descent are not global
        # lows, so they get dropped: a path peaking at 3.0 and falling through
        # 1.9 to 1.2 loses the 1.9, and the replayed stop fires at 1.2 instead.
        # That is a materially worse fill invented by compaction.
        #
        # Drawdown-from-peak is what a trailing stop actually keys on, so a
        # point is kept whenever the drawdown deepens. On a flat path the
        # drawdown never deepens and nothing extra is kept, which is where the
        # rows to remove actually are.
        running_max = priced[0][1]
        deepest_drawdown = Decimal(0)
        keep.add(priced[0][0]["observed_at"])
        for row, price in priced[1:]:
            if price > running_max:
                running_max = price
                deepest_drawdown = Decimal(0)
                keep.add(row["observed_at"])
                continue
            if running_max > 0:
                drawdown = (running_max - price) / running_max
                if drawdown > deepest_drawdown:
                    deepest_drawdown = drawdown
                    keep.add(row["observed_at"])

    last_kept: dt.datetime | None = None
    for row in rows:
        at = dt.datetime.fromisoformat(row["observed_at"])
        if last_kept is None or (at - last_kept).total_seconds() >= kept_interval_seconds:
            keep.add(row["observed_at"])
            last_kept = at

    return [r for r in rows if r["observed_at"] in keep]


@dataclass(slots=True)
class CompactionReport:
    mints: int = 0
    rows_before: int = 0
    rows_after: int = 0

    @property
    def removed(self) -> int:
        return self.rows_before - self.rows_after


async def compact(
    db: Database,
    *,
    older_than_seconds: int = DEFAULT_COMPACT_AFTER_SECONDS,
    kept_interval_seconds: int = DEFAULT_KEPT_INTERVAL_SECONDS,
    now: dt.datetime | None = None,
    limit: int = 500,
) -> CompactionReport:
    now = now or dt.datetime.now(tz=dt.UTC)
    cutoff = now - dt.timedelta(seconds=older_than_seconds)
    report = CompactionReport()

    candidates = await db.fetch(
        "select mint from watch_set where state in ('expired','evicted') "
        "and admitted_at < ? limit ?", iso(cutoff), limit)
    for row in candidates:
        mint = row["mint"]
        rows = await repo.path_for_mint(db, mint)
        uncompacted = [r for r in rows if not r.get("compacted")]
        if len(uncompacted) <= 2:
            continue
        survivors = {r["observed_at"] for r in select_survivors(
            uncompacted, kept_interval_seconds=kept_interval_seconds)}
        dropped = [r["observed_at"] for r in uncompacted if r["observed_at"] not in survivors]
        if not dropped:
            continue
        for at in dropped:
            await db.execute(
                "delete from price_path where mint = ? and observed_at = ?", mint, at)
        await db.execute(
            "update price_path set compacted = 1 where mint = ?", mint)
        await db.execute(
            "insert into path_compaction (mint, compacted_at, from_age_s, "
            "kept_interval_s, rows_before, rows_after) values (?,?,?,?,?,?) "
            "on conflict (mint, compacted_at) do nothing",
            mint, iso(now), older_than_seconds, kept_interval_seconds,
            len(uncompacted), len(survivors))
        report.mints += 1
        report.rows_before += len(uncompacted)
        report.rows_after += len(survivors)
    return report
