"""Derive rug labels from outcomes we already collected.

WHAT THIS ACTUALLY MEASURES, stated before anything else: **liquidity
collapse**, not proven malice. A token that had real DEX liquidity and then had
effectively none is what we can observe. Whether the deployer pulled it, a
whale exited, or demand simply evaporated is not visible from price and
liquidity snapshots, and pretending otherwise would put a confident label on a
guess. `n_rugged` is the column 3.4 stage 2 reads, so that is what we fill --
but every docstring here says collapse, and so should any claim built on it.

WHY NOT SolRPDS. The README names it (n=22,195) and it is a fine external
source, but it is a fixed historical dataset and cannot label the tokens
launching this week. These labels come from our own observations of our own
candidates, which is the only way stage 2's cache stays current.

THREE THINGS THIS DELIBERATELY WILL NOT COUNT.

1. **Bonding curves.** A curve token that never graduated and went quiet did
   not rug; it failed to launch, which is the modal outcome for ~95% of the
   market (1.3). Counting those as rugs would label almost every creator a
   rugger and destroy the signal stage 2 exists to carry. Only a token that
   reached a real DEX pool can collapse out of one.

2. **A gentle decline.** Liquidity drifting down is not a pull. The collapse
   fraction is what separates "this went to zero" from "this got quieter", and
   it is a parameter rather than a constant because where to put it is an
   empirical question this data is still too young to answer.

3. **Anything we only saw once.** A single observation cannot show a
   transition, so a mint with one horizon is unlabelled rather than assumed
   healthy.

SAMPLING LIMIT, and it is a real one. We observe at 15m/1h/24h/7d. Median
rugged-token lifespan is ~14 minutes (1.2), so a token that launched and rugged
inside the first fifteen minutes is indistinguishable here from one that never
had a pool at all. This method therefore UNDERCOUNTS the fastest rugs -- which
are the most common kind. Never read a derived rug rate as the true rate.
"""
from __future__ import annotations

from dataclasses import dataclass

#: DEX liquidity a token must have reached before a collapse is meaningful.
#: Below this there was never anything to pull.
DEFAULT_LIQUIDITY_FLOOR_USD = 1000.0

#: Fraction of peak observed liquidity remaining that still counts as collapse.
#: 0.1 means "lost 90% or more". A parameter, not a truth.
DEFAULT_COLLAPSE_FRACTION = 0.1


@dataclass(slots=True)
class Collapse:
    mint: str
    signer: str | None
    peak_liquidity_usd: float
    final_liquidity_usd: float
    first_seen_at: str
    collapsed_at: str
    lifetime_seconds: int | None
    from_horizon: str
    to_horizon: str


def _as_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def find_collapse(
    observations: list[dict],
    *,
    liquidity_floor: float = DEFAULT_LIQUIDITY_FLOOR_USD,
    collapse_fraction: float = DEFAULT_COLLAPSE_FRACTION,
) -> tuple[str, str, float, float] | None:
    """Given one mint's observations in time order, find a collapse.

    Returns (from_horizon, to_horizon, peak_liquidity, final_liquidity) or None.

    The peak is taken from DEX observations only, for the reason in the module
    docstring: a bonding curve reserve is not a pool, and treating it as one
    would call every failed launch a rug.
    """
    peak = 0.0
    peak_horizon: str | None = None
    for obs in observations:
        if obs.get("venue_kind") == "dex":
            liq = _as_float(obs.get("liquidity_usd")) or 0.0
            if liq > peak:
                peak, peak_horizon = liq, obs.get("horizon")
        elif peak_horizon is None:
            continue

        if peak_horizon is None or peak < liquidity_floor:
            continue
        # Only observations AFTER the peak can be a collapse.
        if obs.get("horizon") == peak_horizon:
            continue
        status = obs.get("status")
        liq_now = _as_float(obs.get("liquidity_usd")) or 0.0
        collapsed = status == "no_pool" or liq_now <= peak * collapse_fraction
        if collapsed:
            return peak_horizon, obs.get("horizon"), peak, liq_now
    return None


def summarise(collapses: list[Collapse]) -> dict:
    """Counts and the lifetime distribution, for the CLI to print."""
    lifetimes = sorted(c.lifetime_seconds for c in collapses if c.lifetime_seconds is not None)
    median = lifetimes[len(lifetimes) // 2] if lifetimes else None
    by_signer: dict[str, int] = {}
    for c in collapses:
        if c.signer:
            by_signer[c.signer] = by_signer.get(c.signer, 0) + 1
    return {
        "collapses": len(collapses),
        "distinct_signers": len(by_signer),
        "median_lifetime_seconds": median,
        "worst_signer_count": max(by_signer.values(), default=0),
    }
