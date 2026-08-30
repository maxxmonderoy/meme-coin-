"""Which venue a token is trading on, and whether that venue is a graduation.

THE DISTINCTION THIS MODULE EXISTS FOR.

A pump.fun bonding curve and a Raydium pool are both places you can buy a
token, and treating them as one thing destroys the label. Every pump.fun launch
has a curve the moment it is created -- so "has a market" is true of
essentially every mint and carries no information. Graduating to a real pool is
the rare event: measured graduation rates run from 0.198% to 6.7% depending on
the window (CLAUDE.md 1.3).

The first live labeling run marked 30% of 15-minute observations `alive` by
counting curves as pools. That number should have been impossible, and it was.

`status` still answers "could this have been sold". `venue_kind` answers "where",
and the two together are what makes a label calibratable.
"""
from __future__ import annotations

BONDING_CURVE = "bonding_curve"
DEX = "dex"
NONE = "none"

#: Launchpad bonding curves. Present from the moment a token is created, so
#: their presence says nothing about whether a token found demand.
#:
#: Sourced from marketType values observed in live RugCheck reports on
#: 2026-08-30, not from a vendor's documentation -- RugCheck publishes no
#: enumeration of this field. Anything unrecognised is deliberately NOT
#: assumed to be a curve; see classify().
CURVE_MARKET_TYPES: frozenset[str] = frozenset({
    "pump_fun",
    "meteora_dbc",        # Meteora dynamic bonding curve
    "raydium_launchlab",  # LetsBonk / LaunchLab curve
})

#: Real pools a token reaches by graduating. Observed in the same sample.
DEX_MARKET_TYPES: frozenset[str] = frozenset({
    "pump_fun_amm",       # PumpSwap, i.e. post-graduation pump.fun
    "raydium_clmm",
    "raydium_cpmm",
    "raydium_amm_v4",
    "meteora_damm_v2",
    "meteora_dlmm",
    "orca",
    "orca_whirlpool",
    "fluxbeam",
})


def classify(market_type: str | None) -> str:
    """bonding_curve / dex / none for one marketType string.

    An unrecognised value returns DEX rather than BONDING_CURVE. That is the
    conservative direction here: mislabelling a curve as a pool inflates the
    graduation rate and would be noticed, while mislabelling a real pool as a
    curve would quietly hide winners from the calibration -- the expensive
    error. Unknown types are surfaced in `market_type` so they can be sorted.
    """
    if not market_type:
        return NONE
    key = market_type.strip().lower()
    if key in CURVE_MARKET_TYPES:
        return BONDING_CURVE
    return DEX


def classify_markets(markets: list[dict]) -> tuple[str, str | None]:
    """Reduce a RugCheck `markets` array to (venue_kind, market_type).

    A DEX pool anywhere in the list wins: a token that has graduated still has
    its curve listed alongside the pool, and the pool is the answer to
    "did this graduate".
    """
    best_kind, best_type = NONE, None
    for market in markets or []:
        if not isinstance(market, dict):
            continue
        mtype = market.get("marketType")
        kind = classify(mtype)
        if kind == DEX:
            return DEX, mtype
        if kind == BONDING_CURVE and best_kind == NONE:
            best_kind, best_type = BONDING_CURVE, mtype
    return best_kind, best_type


def dex_liquidity_usd(markets: list[dict]) -> str | None:
    """Liquidity of the deepest DEX pool only, ignoring curves.

    Curve liquidity is not pool liquidity: it is the launchpad's reserve, and
    summing the two would report a token as deeper than anything it graduated
    into. Returned as the vendor's exact string -- no float in an amount path.
    """
    best: str | None = None
    best_value = -1.0
    for market in markets or []:
        if not isinstance(market, dict) or classify(market.get("marketType")) != DEX:
            continue
        raw = (market.get("lp") or {}).get("baseUSD")
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value > best_value:
            best_value, best = value, str(raw)
    return best
