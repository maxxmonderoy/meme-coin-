"""Aggregate replays into the comparison the change exists to produce.

MEDIAN AND MEAN WILL DIVERGE HUGELY AND THE MEAN IS THE MISLEADING ONE (1.9:
mean x0.46, median x0.036 on the realistic distribution). Both are reported,
always together, with the full distribution behind them -- a mean alone on a
fat-tailed payoff is a screenshot, not a result.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from .engine import Replay


def _quantile(values: list[Decimal], q: float) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(-(-q * len(ordered) // 1)) - 1))
    return ordered[idx]


@dataclass(slots=True)
class CohortSummary:
    rule: str
    cohort: str
    n: int = 0
    skipped: int = 0
    median_return: Decimal | None = None
    mean_return: Decimal | None = None
    win_rate: float | None = None
    p10: Decimal | None = None
    p90: Decimal | None = None
    best: Decimal | None = None
    worst: Decimal | None = None
    max_drawdown: Decimal | None = None
    total_fees: Decimal = Decimal(0)
    mean_impact: Decimal | None = None
    stranded_rate: float | None = None
    big_winner_capture: float | None = None
    exit_reasons: dict[str, int] = field(default_factory=dict)


#: A path that reached this multiple at any observed point is a "big winner".
#: The question is what share of them a rule actually captured -- a rule that
#: exits everything at 1.2x has a fine win rate and misses the entire payoff.
BIG_WINNER_MULTIPLE = Decimal("3")


def summarise(replays: list[Replay], *, rule: str, cohort: str) -> CohortSummary:
    used = [r for r in replays if r.skipped is None]
    out = CohortSummary(rule=rule, cohort=cohort, n=len(used),
                        skipped=len(replays) - len(used))
    if not used:
        return out

    returns = [r.return_multiple for r in used]
    out.median_return = _quantile(returns, 0.5)
    out.mean_return = sum(returns, Decimal(0)) / len(returns)
    out.win_rate = sum(1 for r in returns if r > 1) / len(returns)
    out.p10, out.p90 = _quantile(returns, 0.10), _quantile(returns, 0.90)
    out.best, out.worst = max(returns), min(returns)
    out.total_fees = sum((r.fees for r in used), Decimal(0))
    out.max_drawdown = Decimal(1) - min(returns) if returns else None

    impacts = [leg.impact_pct for r in used for leg in r.legs]
    out.mean_impact = sum(impacts, Decimal(0)) / len(impacts) if impacts else None
    out.stranded_rate = sum(1 for r in used if r.unfilled_fraction > 0) / len(used)

    # What share of the paths that COULD have paid, did.
    big = [r for r in used if r.peak_seen and r.entry_price
           and r.peak_seen >= r.entry_price * BIG_WINNER_MULTIPLE]
    if big:
        out.big_winner_capture = sum(
            1 for r in big if r.return_multiple >= BIG_WINNER_MULTIPLE / 2) / len(big)
    for r in used:
        key = r.exit_reason or "?"
        out.exit_reasons[key] = out.exit_reasons.get(key, 0) + 1
    return out


@dataclass(slots=True)
class TimingComparison:
    """Did the structural signal fire before the price stop would have?

    The question the whole change exists to answer. Counting only the mints
    where BOTH would eventually have fired, because a comparison over mints
    where only one fired is a comparison of different populations.
    """

    mints_compared: int = 0
    structural_first: int = 0
    price_first: int = 0
    simultaneous: int = 0
    median_lead_seconds: float | None = None
    better_fill_count: int = 0
    median_fill_gain: Decimal | None = None

    @property
    def structural_first_rate(self) -> float | None:
        return self.structural_first / self.mints_compared if self.mints_compared else None


def compare_timing(
    structural: dict[str, Replay], price: dict[str, Replay]
) -> TimingComparison:
    out = TimingComparison()
    leads: list[float] = []
    gains: list[Decimal] = []
    for mint, s in structural.items():
        p = price.get(mint)
        if p is None or s.skipped or p.skipped or not s.legs or not p.legs:
            continue
        s_at, p_at = s.legs[-1].at, p.legs[-1].at
        out.mints_compared += 1
        if s_at < p_at:
            out.structural_first += 1
            leads.append((p_at - s_at).total_seconds())
        elif p_at < s_at:
            out.price_first += 1
        else:
            out.simultaneous += 1
        if s.return_multiple > p.return_multiple:
            out.better_fill_count += 1
            gains.append(s.return_multiple - p.return_multiple)
    if leads:
        out.median_lead_seconds = float(_quantile([Decimal(x) for x in leads], 0.5) or 0)
    if gains:
        out.median_fill_gain = _quantile(gains, 0.5)
    return out


def format_summary(rows: list[CohortSummary]) -> list[str]:
    lines = [
        f"  {'rule':<20}{'cohort':<10}{'n':>6}{'median':>9}{'mean':>9}"
        f"{'win':>7}{'p10':>8}{'p90':>8}{'impact':>8}{'strand':>8}",
        "  " + "-" * 93,
    ]
    for r in rows:
        def f(v, spec=".3f"):
            return format(float(v), spec) if v is not None else "-"
        lines.append(
            f"  {r.rule:<20}{r.cohort:<10}{r.n:>6}{f(r.median_return):>9}"
            f"{f(r.mean_return):>9}{f(r.win_rate, '.0%'):>7}{f(r.p10):>8}"
            f"{f(r.p90):>8}{f(r.mean_impact, '.1%'):>8}{f(r.stranded_rate, '.0%'):>8}"
        )
    lines.append("")
    not_exited = sum(r.skipped for r in rows)
    if not_exited:
        lines.append(f"  {not_exited} replay(s) excluded: never entered, or the path ended")
        lines.append("  while still holding. Neither is a 0x -- counting them as one would")
        lines.append("  punish long-horizon rules for short paths.")
        lines.append("")
    lines.append("  median vs mean: 1.9 measures x0.036 median against x0.46 mean on the")
    lines.append("  realistic distribution. The mean is the number that gets screenshotted;")
    lines.append("  the median is the one that happens to you.")
    return lines
