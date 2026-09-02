"""Sampling cadence, and the watch-set size the rate budget can actually afford.

THE BUDGET IS DERIVED, NOT CHOSEN. DexScreener allows 60 requests per minute
shared across endpoints and each request covers BATCH_SIZE mints, so the ceiling
on how many tokens can be densely sampled follows arithmetically from the
cadence table. Hardcoding a watch-set number would mean the two could drift
apart silently -- change an interval, and the "limit" would quietly stop being
one.

WHAT THE ARITHMETIC SAYS (defaults, batch 25, 60 req/min):

      bucket   interval   resident   obs/min   share
       0-10m       30s         10      20.0    5.8%
      10-60m       60s         50      50.0   14.5%
        1-6h      300s        300      60.0   17.3%
       6-24h      900s       1080      72.0   20.8%
        1-7d     3600s       8640     144.0   41.6%

The last row is why DEFAULT_MAX_AGE_SECONDS is 24 hours rather than 7 days. The
1-7d tail costs 41.6% of the entire rate budget to observe the least informative
window there is -- median rugged lifespan is ~14 minutes (1.2) and 85% of the
profitable sniper cohort has exited by five (1.5). Dropping it raises the
sustainable admit rate from 4.3 to 7.4 tokens/min and coverage of the ~42,000
daily launches (1.6) from 14.9% to 25.5%. Both numbers are logged at startup so
the trade stays visible instead of buried in a constant.
"""
from __future__ import annotations

from dataclasses import dataclass

#: (bucket upper bound in seconds, sampling interval in seconds).
#: The first hour is dense on purpose; see the module docstring.
DEFAULT_CADENCE: tuple[tuple[int, int], ...] = (
    (10 * 60, 30),
    (60 * 60, 60),
    (6 * 3600, 300),
    (24 * 3600, 900),
    (7 * 86400, 3600),
)

#: Stop sampling past this age. 24h by default -- see the docstring for the
#: 41.6% figure that decides it. Raise to 7*86400 to keep the tail.
DEFAULT_MAX_AGE_SECONDS = 24 * 3600

#: Never sampled faster than this regardless of the table, so a misconfigured
#: cadence cannot turn into a request storm against a free endpoint.
MIN_INTERVAL_SECONDS = 15


class CadenceError(ValueError):
    pass


def validate(cadence: tuple[tuple[int, int], ...]) -> None:
    if not cadence:
        raise CadenceError("cadence is empty")
    last = 0
    for bound, interval in cadence:
        if bound <= last:
            raise CadenceError(f"cadence bounds must increase; {bound} follows {last}")
        if interval < MIN_INTERVAL_SECONDS:
            raise CadenceError(
                f"interval {interval}s is below the {MIN_INTERVAL_SECONDS}s floor"
            )
        last = bound


def interval_for_age(
    age_seconds: float,
    cadence: tuple[tuple[int, int], ...] = DEFAULT_CADENCE,
    *,
    max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
) -> int | None:
    """Seconds until the next observation, or None when the token is done.

    Ages at or beyond `max_age_seconds` return None: that is how a token leaves
    the watch set and frees its budget for a younger one.
    """
    if age_seconds < 0:
        raise CadenceError("age cannot be negative")
    if age_seconds >= max_age_seconds:
        return None
    for bound, interval in cadence:
        if age_seconds < bound:
            return interval
    return None


@dataclass(frozen=True, slots=True)
class BucketDemand:
    lower_s: int
    upper_s: int
    interval_s: int
    resident_per_admit: float   # tokens present per 1 token/min admitted
    obs_per_minute: float       # observations/min those tokens need
    share: float


@dataclass(frozen=True, slots=True)
class Budget:
    """What the rate limit can actually afford, derived from the cadence."""

    batch_size: int
    requests_per_minute: int
    observations_per_minute: int
    demand_per_admitted: float
    admit_rate_per_minute: float
    watch_set_max: int
    buckets: tuple[BucketDemand, ...]
    max_age_seconds: int

    def coverage_of(self, launches_per_day: int) -> float:
        """Fraction of daily launches we can afford to watch."""
        per_minute = launches_per_day / 1440
        return self.admit_rate_per_minute / per_minute if per_minute else 0.0

    def per_cohort_admit_rate(self, control_share: float = 0.5) -> tuple[float, float]:
        """(filtered, control) admit rates.

        Shrink BOTH under pressure; never drop the control arm to fit more
        filtered tokens. A filtered-only dataset cannot tell you whether the
        filter did anything.
        """
        control = self.admit_rate_per_minute * control_share
        return self.admit_rate_per_minute - control, control


def compute_budget(
    *,
    batch_size: int,
    requests_per_minute: int,
    cadence: tuple[tuple[int, int], ...] = DEFAULT_CADENCE,
    max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
    utilisation: float = 0.8,
) -> Budget:
    """Derive the sustainable watch-set size from the rate budget.

    `utilisation` leaves headroom below the documented limit. The bucket is the
    only thing standing between this and a ban -- DexScreener exposes no
    rate-limit headers -- so running at 100% of a limit measured by somebody
    else's clock is not worth the extra tokens.

    Steady-state reasoning: admitting one token per minute puts `duration`
    tokens in a bucket of that duration, each needing 60/interval observations
    per minute. Summing across buckets gives the observation demand generated by
    a unit admit rate, and the budget divided by that demand is the admit rate
    the endpoint can sustain.
    """
    validate(cadence)
    if batch_size < 1 or requests_per_minute < 1:
        raise CadenceError("batch size and request rate must be positive")
    if not 0 < utilisation <= 1:
        raise CadenceError("utilisation must be in (0, 1]")

    buckets: list[BucketDemand] = []
    total = 0.0
    lower = 0
    for bound, interval in cadence:
        upper = min(bound, max_age_seconds)
        if upper <= lower:
            break
        duration_min = (upper - lower) / 60
        resident = duration_min          # per 1 token/min admitted
        obs = resident * (60.0 / interval)
        buckets.append(BucketDemand(lower, upper, interval, resident, obs, 0.0))
        total += obs
        lower = upper
        if upper >= max_age_seconds:
            break

    buckets = [
        BucketDemand(b.lower_s, b.upper_s, b.interval_s, b.resident_per_admit,
                     b.obs_per_minute, b.obs_per_minute / total if total else 0.0)
        for b in buckets
    ]

    obs_capacity = int(requests_per_minute * batch_size * utilisation)
    admit_rate = obs_capacity / total if total else 0.0
    residency_minutes = sum(b.resident_per_admit for b in buckets)
    return Budget(
        batch_size=batch_size,
        requests_per_minute=requests_per_minute,
        observations_per_minute=obs_capacity,
        demand_per_admitted=total,
        admit_rate_per_minute=admit_rate,
        watch_set_max=int(admit_rate * residency_minutes),
        buckets=tuple(buckets),
        max_age_seconds=max_age_seconds,
    )


def describe(budget: Budget, launches_per_day: int = 42_000) -> list[str]:
    """Human-readable budget, logged at startup and printed by `stats`."""
    lines = [
        f"batch {budget.batch_size} x {budget.requests_per_minute} req/min "
        f"= {budget.observations_per_minute} observations/min usable",
        f"demand {budget.demand_per_admitted:.1f} obs/min per token/min admitted",
        f"=> admit {budget.admit_rate_per_minute:.2f} tokens/min, "
        f"watch set max {budget.watch_set_max:,}",
        f"=> {budget.coverage_of(launches_per_day):.1%} of ~{launches_per_day:,} "
        "daily launches",
        f"max age {budget.max_age_seconds / 3600:.0f}h",
    ]
    for b in budget.buckets:
        lines.append(
            f"    {b.lower_s // 60:>5}-{b.upper_s // 60:<5}min  every {b.interval_s:>4}s  "
            f"{b.obs_per_minute:>6.1f} obs/min  {b.share:>5.1%} of budget"
        )
    return lines
