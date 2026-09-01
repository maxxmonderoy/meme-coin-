"""Position simulator: drives the 1.10 exit ladder over the real trade tape.

THE EXITS ARE DECIDED BEFORE ENTRY (1.10). The plan -- take-profit multiple,
trailing stop, timeout -- is fixed when the position opens and stored on the
row. Nothing here can widen a stop after the fact, which is the discretion this
whole design exists to remove.

WHAT DRIVES IT. Every trade on a held mint is a price observation. That is the
resolution the ladder needs: 85% of the profitable sniper cohort is fully out
inside five minutes (1.5) and median rugged lifespan is ~14 minutes (1.2), so a
15-minute outcome sample cannot see any of this. It is also the honest limit of
what we can claim -- see `Position.tick`, which can only act on prices someone
else actually traded at.

NOT A LIVE PATH. Nothing here signs anything. `mode` is a column (3.9.1); going
live replaces `fees.buy`/`fees.sell` with real execution and changes nothing
else in this file.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from decimal import Decimal

from . import fees as fee_mod
from .fees import FeeModel, Fill

#: 1.10: original stake off at 2x.
DEFAULT_TP_MULTIPLE = Decimal("2.0")

#: Trailing stop on the remainder, from the peak.
DEFAULT_TRAIL_PCT = Decimal("0.35")

#: "Diamond hands on a two-hour-old token is a hope, not a plan" (1.10). A
#: timeout is not an opinion about the token; it is a refusal to hold something
#: whose entire cohort has already exited.
DEFAULT_TIMEOUT_SECONDS = 30 * 60

#: 1.9: 1-2% of speculative bankroll per position, HARD CAP. Enforced here, in
#: the `paper_positions` check constraint, and in the `decisions` constraint.
#: Three places on purpose -- this is the parameter that decides whether losing
#: is slow or fast, and it is the one most likely to get argued with later.
MAX_SIZE_PCT = Decimal("2.0")
DEFAULT_SIZE_PCT = Decimal("1.0")


class SizingError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ExitPlan:
    tp_multiple: Decimal = DEFAULT_TP_MULTIPLE
    trail_pct: Decimal = DEFAULT_TRAIL_PCT
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS

    def as_dict(self) -> dict[str, str]:
        return {
            "tp_multiple": str(self.tp_multiple),
            "trail_pct": str(self.trail_pct),
            "timeout_seconds": str(self.timeout_seconds),
        }


@dataclass(slots=True)
class Tick:
    """One observed trade. `price_sol` may be None -- see trade_price_sol."""

    at: dt.datetime
    price_sol: Decimal | None
    signature: str | None = None
    is_buy: bool | None = None


@dataclass(slots=True)
class Position:
    """One simulated position, advanced tick by tick.

    Deliberately a state machine over observations rather than a loop over a
    price series: live, ticks arrive one at a time and the exit process has to
    decide on each one with no knowledge of the future. Simulating any other way
    would let the paper results use information the live system will not have.
    """

    mint: str
    bankroll_sol: Decimal
    size_pct: Decimal
    plan: ExitPlan
    model: FeeModel = field(default_factory=FeeModel)

    size_sol: Decimal = field(init=False)
    opened_at: dt.datetime | None = None
    entry: Fill | None = None
    entry_at: dt.datetime | None = None
    entry_tick_sig: str | None = None

    tokens_held: Decimal = Decimal(0)
    stake_recovered: bool = False
    peak_price: Decimal | None = None
    ticks_seen: int = 0

    exits: list[tuple[str, Fill, dt.datetime]] = field(default_factory=list)
    closed_at: dt.datetime | None = None
    exit_reason: str | None = None

    def __post_init__(self) -> None:
        if self.size_pct <= 0:
            raise SizingError("size_pct must be positive")
        if self.size_pct > MAX_SIZE_PCT:
            # 1.9: a trader with a +61% edge goes broke at 20% sizing. This is
            # not a preference.
            raise SizingError(
                f"size_pct {self.size_pct} exceeds the {MAX_SIZE_PCT}% hard cap (1.9)"
            )
        self.size_sol = self.bankroll_sol * self.size_pct / Decimal(100)

    # -- lifecycle -------------------------------------------------------
    @property
    def is_open(self) -> bool:
        return self.entry is not None and self.closed_at is None

    def open_at(self, tick: Tick) -> Fill:
        """Enter on the first usable observed price.

        A tick with no derivable price cannot open a position. Guessing an entry
        price is how a simulator invents an edge.
        """
        if self.entry is not None:
            raise RuntimeError("position already opened")
        if tick.price_sol is None or tick.price_sol <= 0:
            raise ValueError("cannot open a position on a tick with no usable price")
        self.entry = fee_mod.buy(self.model, tick.price_sol, self.size_sol)
        self.entry_at = tick.at
        self.opened_at = tick.at
        self.entry_tick_sig = tick.signature
        self.tokens_held = self.entry.tokens
        self.peak_price = tick.price_sol
        return self.entry

    def tick(self, tick: Tick) -> list[tuple[str, Fill]]:
        """Advance one observation. Returns any fills it produced.

        Ladder order matters and is deliberate:
          1. timeout, because a stale position should not first take a profit it
             only reached after the horizon it was supposed to exit at;
          2. take-profit, which recovers the original stake;
          3. trailing stop on whatever remains.
        """
        if not self.is_open:
            return []
        self.ticks_seen += 1
        produced: list[tuple[str, Fill]] = []

        if self._timed_out(tick.at):
            price = tick.price_sol or self.peak_price
            if price is not None:
                produced.append(("timeout", self._close("timeout", price, tick.at,
                                                        distressed=True)))
            return produced

        if tick.price_sol is None or tick.price_sol <= 0:
            return produced
        if self.peak_price is None or tick.price_sol > self.peak_price:
            self.peak_price = tick.price_sol

        assert self.entry is not None
        if not self.stake_recovered and tick.price_sol >= (
            self.entry.fill_price_sol * self.plan.tp_multiple
        ):
            produced.append(("tp", self._take_profit(tick)))

        if self.tokens_held > 0 and self.peak_price is not None:
            floor = self.peak_price * (Decimal(1) - self.plan.trail_pct)
            if tick.price_sol <= floor:
                produced.append(("trail", self._close("trail", tick.price_sol, tick.at)))
        return produced

    # -- exits -----------------------------------------------------------
    def _timed_out(self, now: dt.datetime) -> bool:
        if self.entry_at is None:
            return False
        return (now - self.entry_at).total_seconds() >= self.plan.timeout_seconds

    def _take_profit(self, tick: Tick) -> Fill:
        """Sell enough to recover the original stake; the rest rides free (1.10).

        Sizes the sale on the CASH actually put in, not on token count, so the
        stake is genuinely recovered after fees rather than nominally.
        """
        assert tick.price_sol is not None
        target_net = self.size_sol
        probe = fee_mod.sell(self.model, tick.price_sol, self.tokens_held)
        if probe.net_sol <= 0:
            return self._close("tp", tick.price_sol, tick.at)
        fraction = min(Decimal(1), target_net / probe.net_sol)
        tokens = self.tokens_held * fraction
        fill = fee_mod.sell(self.model, tick.price_sol, tokens)
        self.tokens_held -= tokens
        self.stake_recovered = True
        self.exits.append(("tp", fill, tick.at))
        if self.tokens_held <= 0:
            self.closed_at = tick.at
            self.exit_reason = "tp"
        return fill

    def _close(
        self, reason: str, price: Decimal, at: dt.datetime, *, distressed: bool = False
    ) -> Fill:
        fill = fee_mod.sell(self.model, price, self.tokens_held, distressed=distressed)
        self.tokens_held = Decimal(0)
        self.closed_at = at
        self.exit_reason = reason
        self.exits.append((reason, fill, at))
        return fill

    def close_rugged(self, at: dt.datetime) -> Fill:
        """Liquidity collapsed. Exit at zero, and keep paying the fees.

        Not a price of zero with costs waived: the attempt still burns a
        priority fee and a tip whether or not anything comes back.
        """
        return self._close("rug", Decimal(0), at, distressed=True)

    # -- accounting ------------------------------------------------------
    @property
    def fees_sol(self) -> Decimal:
        total = self.entry.fee_total_sol if self.entry else Decimal(0)
        return total + sum((f.fee_total_sol for _, f, _ in self.exits), Decimal(0))

    @property
    def proceeds_sol(self) -> Decimal:
        return sum((f.net_sol for _, f, _ in self.exits), Decimal(0))

    @property
    def pnl_sol(self) -> Decimal:
        """Net of everything. Fees are also exposed separately (1.10)."""
        if self.entry is None:
            return Decimal(0)
        return self.proceeds_sol - self.size_sol

    @property
    def pnl_pct(self) -> Decimal:
        if self.entry is None or self.size_sol == 0:
            return Decimal(0)
        return self.pnl_sol / self.size_sol * Decimal(100)
