"""The exit loop, as its own process.

3.1: "Entry loop and exit loop are separate processes sharing a position store.
The most common way homemade bots die is an entry-side crash or rate-limit stall
leaving an open bag unwatched."

So this reads open positions out of the database rather than holding them in
memory alongside the entry side. It can be started, stopped and restarted
independently of `trenches stream`, and a crash on either side leaves the other
running.

REBUILDING STATE. A position's live state -- tokens still held, whether the
stake came off, the running peak -- is derived from rows rather than kept in
RAM, so a restart resumes exactly where it left off:

  * tokens held    = tokens bought minus every token sold in `paper_fills`
  * stake recovered = a `tp` fill exists
  * peak price      = max price on `trade_ticks` at or after entry

The peak is recomputed from the tape instead of stored, which means it survives
a crash without needing a write on every tick, and cannot drift from the
observations it is supposed to summarise.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

from ..db import repo
from ..db.dialect import Database
from ..log import get
from .fees import FeeModel
from .simulator import ExitPlan, Position, Tick

log = get(__name__)


async def rebuild(db: Database, row: dict, model: FeeModel | None = None) -> Position | None:
    """Reconstruct an open position from the journal. None if unusable."""
    if not row.get("entry_price_sol") or not row.get("tokens_bought"):
        return None
    plan = ExitPlan(
        tp_multiple=Decimal(str(row["plan_tp_multiple"])),
        trail_pct=Decimal(str(row["plan_trail_pct"])),
        timeout_seconds=int(row["plan_timeout_s"]),
    )
    position = Position(
        mint=row["mint"], bankroll_sol=Decimal(row["bankroll_sol"]),
        size_pct=Decimal(str(row["size_pct"])), plan=plan, model=model or FeeModel(),
    )
    entry_at = dt.datetime.fromisoformat(row["entry_at"])
    position.entry_at = entry_at
    position.opened_at = dt.datetime.fromisoformat(row["opened_at"])
    position.entry_tick_sig = row.get("entry_tick_sig")

    fills = await db.fetch(
        "select * from paper_fills where position_id = ? order by id", row["id"]
    )
    entry_fill = next((f for f in fills if f["kind"] == "entry"), None)
    if entry_fill is None:
        return None
    # Rebuild the entry so the take-profit trigger compares against the same
    # fill price the original decision used, not the quoted price.
    from .fees import Fill

    position.entry = Fill(
        quote_price_sol=Decimal(entry_fill["quote_price_sol"]),
        fill_price_sol=Decimal(entry_fill["fill_price_sol"]),
        slippage_pct=Decimal(str(entry_fill["slippage_pct"] or 0)),
        tokens=Decimal(entry_fill["tokens"]),
        gross_sol=Decimal(entry_fill["gross_sol"]),
        fee_launchpad_sol=Decimal(entry_fill["fee_launchpad_sol"]),
        fee_priority_sol=Decimal(entry_fill["fee_priority_sol"]),
        fee_tip_sol=Decimal(entry_fill["fee_tip_sol"]),
    )
    sold = sum(
        (Decimal(f["tokens"]) for f in fills if f["kind"] != "entry"), Decimal(0)
    )
    position.tokens_held = Decimal(row["tokens_bought"]) - sold
    position.stake_recovered = any(f["kind"] == "tp" for f in fills)
    position.exits = [
        (f["kind"], position.entry, dt.datetime.fromisoformat(f["filled_at"]))
        for f in fills if f["kind"] != "entry"
    ]

    ticks = await repo.ticks_for_mint(db, row["mint"])
    prices = [
        Decimal(t["price_sol"]) for t in ticks
        if t.get("price_sol") and dt.datetime.fromisoformat(t["observed_at"]) >= entry_at
    ]
    position.peak_price = max(prices) if prices else Decimal(entry_fill["quote_price_sol"])
    position.ticks_seen = len(ticks)
    return position


async def sweep(
    db: Database, *, now: dt.datetime | None = None, model: FeeModel | None = None
) -> dict:
    """Close every open position whose plan says it is over.

    Applies any ticks that arrived since the position was opened, then the
    timeout. A dead token stops producing ticks, which is exactly when a
    position most needs closing -- so the timeout is what actually rescues a
    bag from a rug, not the trailing stop.
    """
    now = now or dt.datetime.now(tz=dt.UTC)
    rows = await repo.open_positions(db)
    report = {"open": len(rows), "closed": 0, "unusable": 0, "still_open": 0}

    for row in rows:
        position = await rebuild(db, row, model)
        if position is None:
            report["unusable"] += 1
            log.warning("position %s cannot be rebuilt from the journal", row["id"])
            continue

        entry_at = position.entry_at
        assert entry_at is not None
        ticks = await repo.ticks_for_mint(db, row["mint"])
        seen_sigs = {
            f["tick_signature"] for f in
            await db.fetch("select tick_signature from paper_fills where position_id = ?",
                           row["id"])
        }
        for t in ticks:
            at = dt.datetime.fromisoformat(t["observed_at"])
            if at < entry_at or t["signature"] in seen_sigs:
                continue
            for kind, fill in position.tick(Tick(
                at=at,
                price_sol=Decimal(t["price_sol"]) if t.get("price_sol") else None,
                signature=t["signature"],
            )):
                await _write_fill(db, row["id"], kind, fill, at, t["signature"])
            if not position.is_open:
                break

        if position.is_open and (now - entry_at).total_seconds() >= plan_timeout(row):
            fill = position._close("timeout", position.peak_price or Decimal(0), now,
                                   distressed=True)
            await _write_fill(db, row["id"], "timeout", fill, now, None)

        if position.is_open:
            report["still_open"] += 1
            continue
        await repo.close_paper_position(db, row["id"], {
            "closed_at": position.closed_at, "exit_reason": position.exit_reason,
            "realised_sol": str(position.proceeds_sol), "fees_sol": str(position.fees_sol),
            "pnl_sol": str(position.pnl_sol), "pnl_pct": float(position.pnl_pct),
            "peak_price_sol": str(position.peak_price) if position.peak_price else None,
            "ticks_seen": position.ticks_seen,
        })
        report["closed"] += 1
    return report


def plan_timeout(row: dict) -> int:
    return int(row["plan_timeout_s"])


async def _write_fill(db, position_id, kind, fill, at, signature) -> None:
    await repo.record_fill(db, position_id, {
        "kind": kind, "filled_at": at, "tick_signature": signature,
        "quote_price_sol": str(fill.quote_price_sol),
        "fill_price_sol": str(fill.fill_price_sol),
        "slippage_pct": fill.slippage_pct, "tokens": str(fill.tokens),
        "gross_sol": str(fill.gross_sol),
        "fee_launchpad_sol": str(fill.fee_launchpad_sol),
        "fee_priority_sol": str(fill.fee_priority_sol),
        "fee_tip_sol": str(fill.fee_tip_sol),
        "fee_total_sol": str(fill.fee_total_sol), "net_sol": str(fill.net_sol),
    })
