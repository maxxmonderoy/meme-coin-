"""Replay a ruleset over a stored path, with impact and full costs applied.

The loop walks observations forward one at a time and hands each rule a cursor
frozen at that instant. A rule cannot see past it -- the cursor raises. That is
the difference between a backtest and a story.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from decimal import Decimal

from ..paper import fees as fee_mod
from ..paper.fees import FeeModel
from . import impact as impact_mod
from .cursor import Event, Observation, PathCursor
from .rules import ExitSignal, Rule, RuleState


@dataclass(slots=True)
class Leg:
    at: dt.datetime
    kind: str
    reason: str
    fraction: Decimal
    quote_price: Decimal
    gross_value: Decimal
    filled_value: Decimal
    filled_fraction: Decimal
    impact_pct: Decimal
    fees_sol: Decimal
    net_value: Decimal
    liquidity_usd: Decimal | None


@dataclass(slots=True)
class Replay:
    mint: str
    cohort: str
    rule: str
    entry_at: dt.datetime | None = None
    entry_price: Decimal | None = None
    size: Decimal = Decimal(0)
    legs: list[Leg] = field(default_factory=list)
    exit_reason: str | None = None
    unfilled_fraction: Decimal = Decimal(0)
    peak_seen: Decimal | None = None
    observations: int = 0
    skipped: str | None = None

    @property
    def proceeds(self) -> Decimal:
        return sum((leg.net_value for leg in self.legs), Decimal(0))

    @property
    def fees(self) -> Decimal:
        return sum((leg.fees_sol for leg in self.legs), Decimal(0))

    @property
    def pnl(self) -> Decimal:
        return self.proceeds - self.size if self.size else Decimal(0)

    @property
    def return_multiple(self) -> Decimal:
        return (self.proceeds / self.size) if self.size else Decimal(0)


class ExitEngine:
    def __init__(self, model: FeeModel | None = None,
                 max_consumption: Decimal | None = None) -> None:
        self._model = model or FeeModel()
        self._max_consumption = (
            max_consumption if max_consumption is not None
            else impact_mod.MAX_DEPTH_CONSUMPTION
        )

    def replay(
        self, *, mint: str, cohort: str, rule: Rule,
        observations: list[Observation], events: list[Event], size_usd: Decimal,
    ) -> Replay:
        """`size_usd` MUST be denominated the same as `liquidity_usd`.

        This is not a formality. Impact is computed by comparing position value
        against pool depth, so a size expressed in "units of stake" against a
        pool in dollars makes a position larger than the entire pool look like a
        rounding error -- which is precisely the lie this engine exists to
        avoid. The parameter is named for its units so the mismatch cannot be
        made silently.
        """
        out = Replay(mint=mint, cohort=cohort, rule=getattr(rule, "name", "rule"))
        tradeable = [o for o in observations if o.tradeable]
        if not tradeable:
            # Never indexed, or only ever `not_yet_indexed`. Not a loss and not a
            # win: an entry that could not have been priced never happened.
            out.skipped = "no tradeable observation"
            return out

        entry = tradeable[0]
        assert entry.price_usd is not None
        out.entry_at, out.entry_price, out.size = entry.at, entry.price_usd, size_usd
        remaining = Decimal(1)
        tp_taken = False

        for obs in observations:
            if obs.at < entry.at:
                continue
            out.observations += 1
            cursor = PathCursor(observations, events, obs.at, entry)
            out.peak_seen = cursor.peak_price()
            state = RuleState(entry_price=entry.price_usd,
                              remaining_fraction=remaining, tp_taken=tp_taken)
            signal = rule.decide(state, cursor)
            if signal is None:
                continue

            leg = self._execute(signal, remaining, obs, cursor, size_usd)
            if leg is None:
                continue
            out.legs.append(leg)
            remaining -= leg.fraction * leg.filled_fraction
            if signal.kind == "tp":
                tp_taken = True
            if remaining <= Decimal("0.0001") or signal.fraction >= Decimal(1):
                out.exit_reason = signal.kind
                # Anything the pool could not absorb is left stranded, not
                # quietly assumed sold at the quoted price.
                out.unfilled_fraction = max(Decimal(0), remaining)
                break

        if out.exit_reason is None:
            # The path ran out while still holding. That is NOT a total loss and
            # must not enter the return distribution as 0x -- the position still
            # exists, we simply have no observation of what it would have sold
            # for. Folding a 0x in here would make any rule with a long horizon
            # look catastrophic purely because the path is short.
            out.exit_reason = "path_ended"
            out.unfilled_fraction = remaining
            if not out.legs:
                out.skipped = "path ended with the position still open"
        return out

    def _execute(
        self, signal: ExitSignal, remaining: Decimal, obs: Observation,
        cursor: PathCursor, size_usd: Decimal,
    ) -> Leg | None:
        price = obs.price_usd if obs.tradeable else cursor.latest() and \
            cursor.latest().price_usd
        if price is None or cursor.entry.price_usd is None:
            return None
        fraction = min(signal.fraction, remaining)
        if fraction <= 0:
            return None

        # Value of the slice at the observed price, in the SAME currency as
        # pool liquidity. Both sides of the impact comparison must be in
        # dollars or the comparison is meaningless.
        gross = size_usd * fraction * (price / cursor.entry.price_usd)
        outcome = impact_mod.sell_into(
            gross, obs.liquidity_usd, max_consumption=self._max_consumption
        )
        # Full costs on top of impact (3.9.2): platform, priority, tip, and a
        # haircut wider than quote. An exit rule that only wins before fees has
        # not won.
        distressed = signal.kind in ("structural", "stop", "timeout")
        fill = fee_mod.sell(
            self._model, Decimal(1), outcome.filled_value, distressed=distressed
        )
        return Leg(
            at=obs.at, kind=signal.kind, reason=signal.reason, fraction=fraction,
            quote_price=price, gross_value=gross, filled_value=outcome.filled_value,
            filled_fraction=outcome.filled_fraction, impact_pct=outcome.impact_pct,
            fees_sol=fill.fee_total_sol, net_value=fill.net_sol,
            liquidity_usd=obs.liquidity_usd,
        )
