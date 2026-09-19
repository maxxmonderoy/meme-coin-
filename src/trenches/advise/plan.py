"""One coin in, four answers out: enter, size, take profit, stop.

WHAT THIS IS. Every other module here answers one question well and leaves the
composition to a person. This composes them: given a mint, it runs the cascade,
measures the pool, and turns 1.9's sizing arithmetic and 1.10's exit ladder into
numbers for THIS coin rather than defaults in a docstring.

WHAT IT REFUSES TO DO, and why the refusal is the feature.

  1. IT NEVER SAYS "ENTER". The cascade removes disqualifiers; it does not find
     edge. 1.9 is explicit that on the realistic distribution a single trade is
     about -5% gross and -7.85% after a 3% round trip, and Kelly for a
     negative-edge game is f* ~ 0. A system that turned "no disqualifier found"
     into "buy" would be asserting an edge nobody measured. So the verdict is
     REJECT, NO DISQUALIFIER, or INSUFFICIENT DATA -- and the third is the
     honest answer for almost everything today.

  2. IT NEVER HIDES WHAT IT COULD NOT CHECK. A clean report over four unfetched
     stages looks identical to a clean report over four checked ones unless the
     difference is printed, and Part 2 names reading empty as clean the most
     expensive mistake available here.

  3. THE STOP IS STRUCTURAL FIRST, PRICE SECOND. A price stop assumes a bid. On
     a token with $2,000 of liquidity mid-rug there is no bid, the stop fills
     far below where it triggered, or not at all. The triggers that fire BEFORE
     the price move completes -- liquidity collapse, `rugged` flipping true --
     are the only ones that work on the tokens this system watches, and they are
     the half a price-only stop-loss cannot give you.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from ..exits.impact import MAX_DEPTH_CONSUMPTION, quote_depth, sell_into
from ..structural import detectors

#: Exit impact to size against, as a fraction. 1.6 puts explicit friction at
#: ~4% per round trip and says you need a 4% edge just to break even. Exit
#: impact is ON TOP of that, so sizing to keep it small is what stops the real
#: round trip drifting away from the number the cost model assumes.
#:
#: 2% IS A CHOICE, NOT A MEASUREMENT. 1.6 gives the 4% friction figure; it does
#: not give an impact budget. This splits the difference visibly rather than
#: silently, and it is a parameter so the report always says which number
#: produced the size.
DEFAULT_TARGET_EXIT_IMPACT = Decimal("0.02")

#: 1.9, hard cap: 1-2% of speculative bankroll per position. At 20% a trader
#: with a +61% edge -- better than any documented memecoin trader -- still goes
#: broke. This is the one number in the file that is not negotiable.
DEFAULT_SIZE_PCT = Decimal("1.0")
MAX_SIZE_PCT = Decimal("2.0")

#: 1.10's ladder: original stake off at 2x, trailing stop on the remainder.
DEFAULT_TP_MULTIPLE = Decimal("2.0")
DEFAULT_TP_FRACTION = Decimal("0.5")
DEFAULT_TRAIL_PCT = Decimal("0.35")

#: 1.10: "diamond hands on a two-hour-old token is a hope, not a plan". 1.5:
#: 85% of the profitable sniper cohort is out within five minutes. 1.2: median
#: rugged lifespan ~14 minutes.
DEFAULT_TIME_STOP_SECONDS = 30 * 60

#: Structural triggers to arm on a held position (3.4 stage 6). Stated as the
#: conditions rather than as prices, because these are what fire BEFORE the
#: price move completes -- which is the only kind of stop that works on a token
#: whose bid disappears mid-rug.
def structural_triggers(t: detectors.Thresholds | None = None) -> list[str]:
    t = t or detectors.Thresholds()
    return [
        f"liquidity falls {t.liquidity_drop_pct:.0%} between observations",
        "`rugged` flips true (poll with cache bypass; a cached verdict is the "
        "documented failure in Part 2)",
        "the pool disappears entirely",
        f"LP unlock comes within {t.lock_horizon_seconds / 3600:.0f}h",
        f"bundler share falls {t.bundler_drop_pct:.0%} (distribution into you)",
        f"dev share falls {t.dev_drop_pct:.0%}",
        f"sniper share falls {t.sniper_drop_pct:.0%}",
    ]


REJECT = "REJECT"
NO_DISQUALIFIER = "NO DISQUALIFIER"
INSUFFICIENT_DATA = "INSUFFICIENT DATA"

#: Journal markers that mean a stage ran but had nothing to read. Keyed by
#: stage so a new marker cannot silently start counting as "checked".
NO_DATA_MARKERS: dict[int, tuple[str, tuple[str, ...]]] = {
    1: ("structural", ("unfetched",)),
    2: ("creator", ("unknown",)),
    3: ("liquidity", ("unfetched",)),
    4: ("concentration", ("unfetched", "cold_start")),
    5: ("clustering", ("unfetched", "no_edges", "labels_missing", "unmeasurable")),
}


@dataclass(frozen=True, slots=True)
class StageReport:
    stage: int
    accepted: bool
    had_data: bool
    note: str | None = None


@dataclass(frozen=True, slots=True)
class SizeAdvice:
    """What you can put in, and which constraint is actually binding."""

    bankroll_cap: Decimal | None            # 1.9, from bankroll
    depth_cap: Decimal | None               # from the pool, at target impact
    hard_depth_cap: Decimal | None          # beyond this there is no counterparty
    recommended: Decimal | None
    binding: str                            # "bankroll" | "pool" | "unknown"
    target_exit_impact: Decimal
    size_pct: Decimal
    note: str


@dataclass(frozen=True, slots=True)
class TakeProfitAdvice:
    multiple: Decimal
    fraction: Decimal
    trail_pct: Decimal
    exit_value_at_tp: Decimal | None
    impact_at_tp: Decimal | None
    exitable: bool | None
    note: str


@dataclass(frozen=True, slots=True)
class StopAdvice:
    structural: list[str]
    price_stop_pct: Decimal
    realised_at_stop_pct: Decimal | None
    time_stop_seconds: int
    note: str


@dataclass(slots=True)
class Advice:
    mint: str
    verdict: str
    verdict_reason: str
    stages: list[StageReport] = field(default_factory=list)
    size: SizeAdvice | None = None
    take_profit: TakeProfitAdvice | None = None
    stop: StopAdvice | None = None
    unchecked: dict[str, str] = field(default_factory=dict)
    observed: dict[str, Any] = field(default_factory=dict)

    @property
    def checked_stages(self) -> int:
        return sum(1 for s in self.stages if s.had_data)


def stage_reports(trail: Any) -> list[StageReport]:
    """Which stages ran, and which of them actually read something.

    Stage 0 is always counted as data: it reads the mint out of the feed and
    there is nothing to be missing.
    """
    out: list[StageReport] = []
    for verdict in trail or ():
        key = NO_DATA_MARKERS.get(verdict.stage)
        note = None
        had_data = True
        if key is not None:
            field_name, empty_values = key
            marker = (verdict.inputs or {}).get(field_name)
            if marker in empty_values:
                had_data, note = False, str(marker)
        out.append(StageReport(verdict.stage, verdict.accept, had_data, note))
    return out


def size_for_impact(
    liquidity_usd: Decimal | None, target_impact: Decimal
) -> Decimal | None:
    """Largest position whose EXIT costs no more than `target_impact`.

    Constant product: selling a position worth V into quote-side depth D gives
    impact r/(1+r) with r = V/D, so a target impact t inverts to V = D*t/(1-t).
    Inverted rather than searched because the closed form is exact and a binary
    search would invite an off-by-one at the boundary that nobody would notice.
    """
    depth = quote_depth(liquidity_usd)
    if depth <= 0 or target_impact <= 0 or target_impact >= 1:
        return None
    return depth * target_impact / (Decimal(1) - target_impact)


def size_advice(
    *,
    liquidity_usd: Decimal | None,
    bankroll: Decimal | None,
    size_pct: Decimal = DEFAULT_SIZE_PCT,
    target_exit_impact: Decimal = DEFAULT_TARGET_EXIT_IMPACT,
) -> SizeAdvice:
    """1.9's cap and the pool's cap. The smaller one wins, and it is named.

    THE POOL IS OFTEN THE BINDING CONSTRAINT AND PEOPLE DO NOT EXPECT THAT. 1.9
    is about surviving variance and says 1-2% of bankroll. It says nothing about
    whether the position can be sold, and on a thin pool the exitable size is far
    below any sane bankroll fraction. Reporting which one bound the answer is the
    difference between "I sized small" and "I sized small because I could not
    have got out of anything bigger".
    """
    if size_pct > MAX_SIZE_PCT:
        raise ValueError(f"size_pct {size_pct} exceeds 1.9's hard cap of {MAX_SIZE_PCT}%")

    bankroll_cap = (
        bankroll * size_pct / Decimal(100) if bankroll and bankroll > 0 else None
    )
    depth_cap = size_for_impact(liquidity_usd, target_exit_impact)
    hard_cap = quote_depth(liquidity_usd) * MAX_DEPTH_CONSUMPTION if liquidity_usd else None

    if bankroll_cap is None and depth_cap is None:
        return SizeAdvice(None, None, None, None, "unknown", target_exit_impact, size_pct,
                          "no bankroll given and no liquidity observed: nothing to size against")
    if depth_cap is None:
        return SizeAdvice(bankroll_cap, None, None, bankroll_cap, "bankroll",
                          target_exit_impact, size_pct,
                          "NO LIQUIDITY OBSERVED. This is 1.9's cap only; nothing here "
                          "establishes the position could be exited at all")
    if bankroll_cap is None:
        return SizeAdvice(None, depth_cap, hard_cap, depth_cap, "pool",
                          target_exit_impact, size_pct,
                          "no bankroll given: this is the pool's cap only")

    if depth_cap < bankroll_cap:
        return SizeAdvice(bankroll_cap, depth_cap, hard_cap, depth_cap, "pool",
                          target_exit_impact, size_pct,
                          "THE POOL IS BINDING, not your bankroll. A larger position is "
                          "one you could not sell at this impact")
    return SizeAdvice(bankroll_cap, depth_cap, hard_cap, bankroll_cap, "bankroll",
                      target_exit_impact, size_pct,
                      "1.9's cap is binding; the pool would absorb more")


def take_profit_advice(
    *,
    position_value: Decimal | None,
    liquidity_usd: Decimal | None,
    multiple: Decimal = DEFAULT_TP_MULTIPLE,
    fraction: Decimal = DEFAULT_TP_FRACTION,
    trail_pct: Decimal = DEFAULT_TRAIL_PCT,
) -> TakeProfitAdvice:
    """1.10's ladder, checked against whether the take-profit is reachable.

    A 2x you cannot sell into is not a 2x. The sell at target is modelled
    against CURRENT depth, which is optimistic in the usual direction: a token
    that has doubled has usually seen liquidity move too, and we have no
    observation of what it will be.
    """
    if position_value is None or position_value <= 0:
        return TakeProfitAdvice(multiple, fraction, trail_pct, None, None, None,
                                "no position size, so nothing to check exitability against")
    at_target = position_value * multiple * fraction
    outcome = sell_into(at_target, liquidity_usd)
    return TakeProfitAdvice(
        multiple, fraction, trail_pct, at_target, outcome.impact_pct,
        outcome.filled_fraction >= 1,
        "sell at target modelled against TODAY's depth; if liquidity leaves on the "
        "way up this is optimistic",
    )


def stop_advice(
    *,
    position_value: Decimal | None,
    liquidity_usd: Decimal | None,
    triggers: list[str] | None = None,
    price_stop_pct: Decimal = DEFAULT_TRAIL_PCT,
    time_stop_seconds: int = DEFAULT_TIME_STOP_SECONDS,
) -> StopAdvice:
    """Structural triggers first, then a price stop, then a clock.

    `triggers` DEFAULTS TO THE FULL STRUCTURAL SET rather than to nothing. An
    empty default would mean a caller that forgot the argument silently got the
    price-only stop this whole module exists to argue against, and it would look
    correct on screen -- a test caught exactly that.

    THE ORDER IS THE ADVICE. On an illiquid token a price stop is a request, not
    a guarantee: by the time price has fallen 35% mid-rug the bid that would
    have filled it is gone. The structural triggers fire on the CAUSE rather
    than the symptom and are the reason this can say anything a price-only
    stop-loss cannot.
    """
    realised = None
    if position_value and position_value > 0:
        remaining = position_value * (Decimal(1) - price_stop_pct)
        outcome = sell_into(remaining, liquidity_usd)
        if outcome.filled:
            # What the stop actually returns, as a loss from entry.
            realised = (position_value - outcome.filled_value) / position_value
        else:
            realised = Decimal(1)
    return StopAdvice(
        structural=list(triggers if triggers is not None else structural_triggers()),
        price_stop_pct=price_stop_pct,
        realised_at_stop_pct=realised,
        time_stop_seconds=time_stop_seconds,
        note="a price stop assumes a bid; the structural triggers do not",
    )
