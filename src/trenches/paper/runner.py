"""Drives paper positions from the live trade tape.

SEPARATE FROM THE ENTRY LOOP BY DESIGN (3.1). "The most common way homemade
bots die is an entry-side crash or rate-limit stall leaving an open bag
unwatched." The exit side owns its own loop over its own queue and shares only
the position store, so a stall on the entry side cannot stop an exit.

It cannot open a position by itself: it acts on candidates already accepted and
journalled by the cascade. It never calls the cascade, and the cascade never
calls it.
"""
from __future__ import annotations

import datetime as dt
import logging
from decimal import Decimal

from ..db import repo
from ..db.dialect import Database
from ..log import get, kv
from ..version import code_version
from .fees import FeeModel
from .simulator import ExitPlan, Position, Tick

log = get(__name__)


class PaperRunner:
    def __init__(
        self,
        db: Database,
        *,
        bankroll_sol: Decimal,
        size_pct: Decimal,
        plan: ExitPlan | None = None,
        model: FeeModel | None = None,
        mode: str = "PAPER",
        tracker=None,
    ) -> None:
        """`tracker` is the PumpPortal consumer, if one is running. Opening a
        position asks it to subscribe to that mint's trades on the SHARED
        socket; there is no second connection."""
        self._db = db
        self._bankroll = bankroll_sol
        self._size_pct = size_pct
        self._plan = plan or ExitPlan()
        self._model = model or FeeModel()
        self._mode = mode
        self._tracker = tracker
        self.positions: dict[str, Position] = {}
        self.position_ids: dict[str, int] = {}
        self.pending: set[str] = set()

    # -- entry side ------------------------------------------------------
    def arm(self, mint: str) -> None:
        """Mark a mint as wanting a position at its next observed trade.

        Entry is deferred to a real tick rather than taken at an invented price.
        There is no price at the moment a launch is detected -- nobody has
        traded yet -- so a simulator that entered "at detection" would be
        entering at a number it made up.
        """
        if mint in self.positions or mint in self.pending:
            return
        self.pending.add(mint)
        if self._tracker is not None:
            self._tracker.track(mint)

    async def on_tick(self, mint: str, tick: Tick) -> None:
        """Advance whatever this mint has. The only entry point for prices."""
        if mint in self.pending and tick.price_sol and tick.price_sol > 0:
            await self._open(mint, tick)
            return
        position = self.positions.get(mint)
        if position is None or not position.is_open:
            return
        for kind, fill in position.tick(tick):
            await repo.record_fill(self._db, self.position_ids[mint], {
                "kind": kind, "filled_at": tick.at, "tick_signature": tick.signature,
                "quote_price_sol": str(fill.quote_price_sol),
                "fill_price_sol": str(fill.fill_price_sol),
                "slippage_pct": fill.slippage_pct, "tokens": str(fill.tokens),
                "gross_sol": str(fill.gross_sol),
                "fee_launchpad_sol": str(fill.fee_launchpad_sol),
                "fee_priority_sol": str(fill.fee_priority_sol),
                "fee_tip_sol": str(fill.fee_tip_sol),
                "fee_total_sol": str(fill.fee_total_sol), "net_sol": str(fill.net_sol),
            })
        if not position.is_open:
            await self._settle(mint, position)

    async def _open(self, mint: str, tick: Tick) -> None:
        self.pending.discard(mint)
        position = Position(
            mint=mint, bankroll_sol=self._bankroll, size_pct=self._size_pct,
            plan=self._plan, model=self._model,
        )
        entry = position.open_at(tick)
        position_id = await repo.open_paper_position(self._db, mint=mint, fields={
            "mode": self._mode, "opened_at": tick.at, "bankroll_sol": str(self._bankroll),
            "size_pct": self._size_pct, "size_sol": str(position.size_sol),
            "entry_at": tick.at, "entry_price_sol": str(entry.fill_price_sol),
            "entry_tick_sig": tick.signature, "tokens_bought": str(entry.tokens),
            "entry_slippage_pct": float(entry.slippage_pct),
            "plan_tp_multiple": self._plan.tp_multiple,
            "plan_trail_pct": self._plan.trail_pct,
            "plan_timeout_s": self._plan.timeout_seconds,
            "code_version": code_version(),
            "params": {"fees": self._model.as_dict(), "plan": self._plan.as_dict()},
        })
        self.positions[mint] = position
        self.position_ids[mint] = position_id
        await repo.record_fill(self._db, position_id, {
            "kind": "entry", "filled_at": tick.at, "tick_signature": tick.signature,
            "quote_price_sol": str(entry.quote_price_sol),
            "fill_price_sol": str(entry.fill_price_sol),
            "slippage_pct": entry.slippage_pct, "tokens": str(entry.tokens),
            "gross_sol": str(entry.gross_sol),
            "fee_launchpad_sol": str(entry.fee_launchpad_sol),
            "fee_priority_sol": str(entry.fee_priority_sol),
            "fee_tip_sol": str(entry.fee_tip_sol),
            "fee_total_sol": str(entry.fee_total_sol), "net_sol": str(entry.net_sol),
        })
        kv(log, logging.INFO, "paper position opened", mint=mint, mode=self._mode,
           size_sol=str(position.size_sol), entry=str(entry.fill_price_sol))

    async def _settle(self, mint: str, position: Position) -> None:
        await repo.close_paper_position(self._db, self.position_ids[mint], {
            "closed_at": position.closed_at, "exit_reason": position.exit_reason,
            "realised_sol": str(position.proceeds_sol), "fees_sol": str(position.fees_sol),
            "pnl_sol": str(position.pnl_sol), "pnl_pct": float(position.pnl_pct),
            "peak_price_sol": str(position.peak_price) if position.peak_price else None,
            "ticks_seen": position.ticks_seen,
        })
        kv(log, logging.INFO, "paper position closed", mint=mint,
           reason=position.exit_reason, pnl_sol=str(position.pnl_sol),
           fees_sol=str(position.fees_sol))
        self.positions.pop(mint, None)
        self.position_ids.pop(mint, None)
        if self._tracker is not None:
            self._tracker.untrack(mint)

    async def sweep_timeouts(self, now: dt.datetime | None = None) -> int:
        """Close positions whose timeout passed with no further trades.

        Necessary because the ladder is tick-driven and a dead token stops
        producing ticks -- which is exactly when a position most needs closing.
        Without this, a rug leaves an open bag forever: the very failure 3.1
        separates the exit loop to prevent.
        """
        now = now or dt.datetime.now(tz=dt.UTC)
        closed = 0
        for mint, position in list(self.positions.items()):
            if not position.is_open or position.entry_at is None:
                continue
            if (now - position.entry_at).total_seconds() < position.plan.timeout_seconds:
                continue
            price = position.peak_price or Decimal(0)
            fill = position._close("timeout", price, now, distressed=True)
            await repo.record_fill(self._db, self.position_ids[mint], {
                "kind": "timeout", "filled_at": now, "tick_signature": None,
                "quote_price_sol": str(fill.quote_price_sol),
                "fill_price_sol": str(fill.fill_price_sol),
                "slippage_pct": fill.slippage_pct, "tokens": str(fill.tokens),
                "gross_sol": str(fill.gross_sol),
                "fee_launchpad_sol": str(fill.fee_launchpad_sol),
                "fee_priority_sol": str(fill.fee_priority_sol),
                "fee_tip_sol": str(fill.fee_tip_sol),
                "fee_total_sol": str(fill.fee_total_sol), "net_sol": str(fill.net_sol),
            })
            await self._settle(mint, position)
            closed += 1
        return closed
