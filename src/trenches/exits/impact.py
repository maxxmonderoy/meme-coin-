"""Price impact, and the fills you do not get.

THIS IS THE MODULE THAT DECIDES WHETHER THE BACKTEST IS REAL. A simulation that
assumes you fill at the observed price is the exact lie that makes every
memecoin strategy look profitable: the observed price is what somebody else
traded at, in whatever size they traded, and says nothing about whether a pool
with $2,000 in it will absorb your exit.

THE MODEL, and its assumptions stated rather than implied.

Constant product. Selling `x` tokens into a pool holding `T` tokens and `Q`
quote moves the price by the usual xy=k arithmetic, so the average realised
price is the quote price scaled by `1 / (1 + x/T)`. Equivalently, in quote
terms, selling a position worth `V` into quote-side depth `D` realises
`V / (1 + V/D)`.

Two approximations, both pessimistic-leaning and both load-bearing:

  1. DexScreener reports TOTAL pool liquidity, not per-side reserves. A balanced
     pool is assumed, so quote-side depth is taken as half the reported figure.
     A pool that is actually unbalanced against us has less depth than this
     assumes.
  2. Liquidity is a SNAPSHOT from the last observation. Mid-rug the true depth
     is lower than the last sample -- possibly far lower, since the whole point
     of a rug is that the LP leaves between observations. The impact computed
     here is therefore OPTIMISTIC, and every report says so rather than
     presenting it as precise.

If depth is below the position size the result is a partial fill or no fill.
That case is modelled explicitly and never silently rounded into a fill.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

#: Fraction of reported total liquidity treated as quote-side depth. 0.5 assumes
#: a balanced pool; see assumption 1.
QUOTE_SIDE_FRACTION = Decimal("0.5")

#: Refuse to model a fill that would consume more than this share of quote-side
#: depth. Beyond it the constant-product curve is not a description of anything
#: you could actually execute -- there is no counterparty at that size, and a
#: number produced here would be arithmetic rather than a fill.
MAX_DEPTH_CONSUMPTION = Decimal("0.30")


@dataclass(frozen=True, slots=True)
class FillOutcome:
    """What a sell attempt actually achieves against observed depth."""

    requested_value: Decimal        # what we tried to sell, at quote
    filled_value: Decimal           # what came back, before fees
    filled_fraction: Decimal        # 0 = no fill, 1 = fully filled
    impact_pct: Decimal             # realised shortfall vs quote
    depth_used: Decimal
    available_depth: Decimal
    reason: str

    @property
    def filled(self) -> bool:
        return self.filled_fraction > 0

    @property
    def partial(self) -> bool:
        return 0 < self.filled_fraction < 1


def quote_depth(liquidity_usd: Decimal | None) -> Decimal:
    """Quote-side depth from reported total liquidity. See assumption 1."""
    if liquidity_usd is None or liquidity_usd <= 0:
        return Decimal(0)
    return liquidity_usd * QUOTE_SIDE_FRACTION


def sell_into(
    value: Decimal,
    liquidity_usd: Decimal | None,
    *,
    max_consumption: Decimal = MAX_DEPTH_CONSUMPTION,
) -> FillOutcome:
    """Sell `value` (quote terms) into a pool of `liquidity_usd` total.

    Returns what is actually achievable, which for an illiquid token is
    routinely less than what was asked for and sometimes nothing at all.
    """
    depth = quote_depth(liquidity_usd)
    if depth <= 0:
        return FillOutcome(value, Decimal(0), Decimal(0), Decimal(1), Decimal(0),
                           depth, "no liquidity: nothing to sell into")

    cap = depth * max_consumption
    if value <= cap:
        # Fully fillable, but not at quote: xy=k still moves the price.
        realised = value / (Decimal(1) + value / depth)
        impact = (value - realised) / value if value else Decimal(0)
        return FillOutcome(value, realised, Decimal(1), impact, value, depth,
                           "filled with impact")

    # Only the part inside the cap is executable. The rest has no counterparty.
    realised = cap / (Decimal(1) + cap / depth)
    fraction = cap / value
    impact = (value - realised) / value
    return FillOutcome(
        value, realised, fraction, impact, cap, depth,
        f"partial fill: position is {value / depth:.1%} of quote-side depth, "
        f"capped at {max_consumption:.0%}",
    )
