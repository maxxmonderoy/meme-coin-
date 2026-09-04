"""Stages 0-4 of the 3.4 cascade, as pure functions.

ORDERING SAVES MORE MONEY THAN CACHING (3.4). Stages run cheapest-first so the
~95% of candidates that die early never reach a paid lookup. Each stage here is
a pure `facts -> Verdict`; nothing fetches, so the cost ordering is a property
of the caller and is visible rather than hidden inside a predicate.

A REJECTION IS A RECORD, NOT A DELETION (3.9.3). Every verdict is journalled
with its reason and the inputs it saw, because a rejection that later 10x'd is
training data and `rules-report` already knows how to find those.

THRESHOLDS ARE NOT INVENTED HERE. The numbers below are 3.4's stated starting
points, carried as parameters and snapshotted into every decision row so that
changing one does not silently invalidate the comparability of earlier rows.
Whether they are any good is an empirical question that `rules-report` answers
against labeled outcomes -- not something this module asserts.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

#: 3.4 stage 2. Note this rule CANNOT FIRE YET: rug_rate needs `n_rugged`, and
#: nothing labels rugs, so every creator reads 0.0. It is wired and journalled
#: so the day labelling lands it starts working, and until then stage 2 passes
#: everything on "unknown" -- which is 3.4's instruction anyway, because fresh
#: wallets are free and unknown is the modal case carrying almost no signal.
DEFAULT_CREATOR_MIN_MINTS = 3
DEFAULT_CREATOR_MAX_RUG_RATE = 0.6


@dataclass(slots=True)
class Verdict:
    """The outcome of one stage. `accept=True` means "keep going", not "buy"."""

    accept: bool
    stage: int
    reason: str | None = None
    inputs: dict[str, Any] = field(default_factory=dict)

    @property
    def rejected(self) -> bool:
        return not self.accept


def stage0_ingest(facts: dict) -> Verdict:
    """Free. Everything already in the feed payload.

    The only rejection here is a candidate we cannot identify. A launch with no
    mint cannot be deduped, stored, or acted on, and letting it through would
    put a null-keyed row into the journal.
    """
    mint = facts.get("mint")
    if not mint:
        return Verdict(False, 0, "no mint", {"mint": mint})
    return Verdict(True, 0, None, {"mint": mint, "launchpad": facts.get("launchpad")})


def stage1_structural(facts: dict) -> Verdict:
    """Free, ~1s. Structural facts that land immediately (Part 2).

    Part 2's two latency regimes matter here: mint/freeze/close authority,
    Token-2022 upgradable flags, transfer hooks and fees are all knowable within
    about a second of a launch. Everything behavioural -- bundle share, sniper
    count, insider clusters, holder concentration, and the vendor's own score --
    has a cold start and is NOT checked at this stage, because at a minute old
    those fields are empty rather than clean, and reading empty as clean is
    Part 2's single most expensive mistake.

    UNKNOWN IS NOT A REJECT. A missing fact means we have not fetched it, not
    that it is safe and not that it is dangerous. Rejecting on absence would
    reject the entire market before any structural data arrives; accepting on
    absence is honest as long as the absence is journalled, which it is.
    """
    reasons = []
    seen: dict[str, Any] = {}

    for flag, label in (
        ("mintable", "mint authority live"),
        ("freezable", "freeze authority live"),
        ("closable", "close authority live"),
        ("balance_mutable_authority", "balance mutable"),
        ("transfer_fee_upgradable", "transfer fee upgradable"),
        ("transfer_hook_upgradable", "transfer hook upgradable"),
        ("metadata_mutable", "metadata mutable"),
        ("default_account_state_upgradable", "default account state upgradable"),
        ("non_transferable", "non-transferable"),
    ):
        value = facts.get(flag)
        if value is None:
            continue
        seen[flag] = value
        if _truthy(value):
            reasons.append(label)

    hook = facts.get("transfer_hook")
    if hook:
        seen["transfer_hook"] = hook
        reasons.append("transfer hook present")

    fee = facts.get("transfer_fee")
    if fee is not None:
        seen["transfer_fee"] = fee
        if _positive(fee):
            reasons.append("transfer fee > 0")

    if facts.get("malicious_address") is not None:
        seen["malicious_address"] = facts["malicious_address"]
        if _truthy(facts["malicious_address"]):
            reasons.append("flagged malicious address")

    if not seen:
        # Nothing was fetched. Say so in the journal rather than implying a pass.
        return Verdict(True, 1, None, {"structural": "unfetched"})
    if reasons:
        return Verdict(False, 1, "; ".join(reasons), seen)
    return Verdict(True, 1, None, seen)


def stage2_creator(
    facts: dict,
    *,
    min_mints: int = DEFAULT_CREATOR_MIN_MINTS,
    max_rug_rate: float = DEFAULT_CREATOR_MAX_RUG_RATE,
) -> Verdict:
    """Near-free after warm-up, entirely in-house (3.4 stage 2).

    Reads the local creator cache. On a miss the candidate PROCEEDS on
    "unknown": blocking would gate latency on a cold lookup, and fresh wallets
    are free to make so unknown is the modal case and carries almost no
    information.

    Keyed on BOTH identities when both exist. A cache keyed on the signer alone
    is bypassed by rotating the declared creator, and vice versa. Note that
    PumpPortal carries no `creator`, so from that feed only the signer is known
    and the journal records exactly that rather than pretending otherwise.
    """
    n_mints = facts.get("creator_n_mints")
    n_rugged = facts.get("creator_n_rugged")
    seen = {
        "signer": facts.get("signer"),
        "declared_creator": facts.get("declared_creator"),
        "creator_n_mints": n_mints,
        "creator_n_rugged": n_rugged,
    }
    if n_mints is None:
        return Verdict(True, 2, None, {**seen, "creator": "unknown"})

    rug_rate = (n_rugged or 0) / n_mints if n_mints else 0.0
    seen["rug_rate"] = round(rug_rate, 4)
    if n_mints >= min_mints and rug_rate >= max_rug_rate:
        return Verdict(
            False, 2,
            f"creator has {n_mints} mints at rug rate {rug_rate:.0%}",
            seen,
        )
    return Verdict(True, 2, None, seen)


#: 3.4 stage 3. Liquidity floor in USD.
#:
#: UNCALIBRATED. 3.4 does not give a number, and 1.2's $1,000 figure is Solidus's
#: measure of a token being effectively dead, not a floor for entry. This is a
#: starting point to be tuned against matched controls once the dense paths have
#: accumulated -- until then it is an assumption the journal records rather than
#: a finding, and `thresholds()` snapshots it so tuning later cannot silently
#: rewrite what earlier rows meant.
DEFAULT_MIN_LIQUIDITY_USD = 5_000.0

#: Reject if the LP unlocks inside the holding horizon. 3.4: "check the locker's
#: unlockDate -- reject if it unlocks inside the holding horizon. Almost nobody
#: does that last one and it's free." A lock that expires while you hold is not
#: a lock, it is a countdown.
DEFAULT_HOLD_HORIZON_SECONDS = 6 * 3600

#: LP states that count as locked. Anything else -- including absent -- is
#: treated as UNKNOWN rather than safe.
LP_SAFE_STATES = ("burned", "locked")


def stage3_liquidity(
    facts: dict,
    *,
    min_liquidity_usd: float = DEFAULT_MIN_LIQUIDITY_USD,
    hold_horizon_seconds: int = DEFAULT_HOLD_HORIZON_SECONDS,
    now: dt.datetime | None = None,
) -> Verdict:
    """One request's worth of liquidity facts (3.4 stage 3).

    Four rejections, in the order they cost you money:

      1. `rugged` already true. Cheapest possible answer and the only one that
         needs no interpretation.
      2. Liquidity below the floor. A pool you cannot exit is not an entry, and
         the impact model already measures what "cannot exit" means: a $500
         position into a $300 pool fills under 5% of itself.
      3. LP neither burned nor locked. The deployer can withdraw at will.
      4. The lock expires inside the holding horizon.

    UNKNOWN IS NOT A REJECT, and it is not a pass either -- it is recorded as
    unknown. At the cold start none of these fields exist (Part 2), and
    rejecting on their absence would reject the entire market for the first few
    minutes of every token's life while accepting on it would read empty as
    clean, which Part 2 names the single most expensive mistake available here.
    """
    now = now or dt.datetime.now(tz=dt.UTC)
    seen: dict[str, Any] = {}
    #: Which of stage 3's fields were actually supplied. Counted rather than
    #: inferred from `seen`, because `seen` also carries the "unknown" markers
    #: and would otherwise never be empty -- making "unfetched" unreachable and
    #: turning a token nobody looked at into one that passed three checks.
    supplied = 0

    if facts.get("rugged") is not None:
        supplied += 1
        seen["rugged"] = facts["rugged"]
        if _truthy(facts["rugged"]):
            return Verdict(False, 3, "already flagged rugged", seen)

    liquidity = _number(facts.get("liquidity_usd"))
    observed_at = _timestamp(facts.get("liquidity_observed_at"))
    if observed_at is not None:
        # A liquidity number is only as good as its timestamp. There is no age
        # gate here -- 3.4 gives no number and inventing one would be a fiction
        # -- but recording WHEN the reading was taken means a later calibration
        # can ask whether stale readings decided anything.
        seen["liquidity_observed_at"] = observed_at.isoformat()
        seen["liquidity_age_seconds"] = int((now - observed_at).total_seconds())
    if liquidity is None:
        seen["liquidity_usd"] = "unknown"
    else:
        supplied += 1
        seen["liquidity_usd"] = liquidity
        if liquidity < min_liquidity_usd:
            return Verdict(
                False, 3,
                f"liquidity ${liquidity:,.0f} below the ${min_liquidity_usd:,.0f} floor",
                seen,
            )

    lp_state = facts.get("lp_state")
    if lp_state is None:
        seen["lp_state"] = "unknown"
    else:
        supplied += 1
        seen["lp_state"] = lp_state
        if str(lp_state).strip().lower() not in LP_SAFE_STATES:
            return Verdict(False, 3, f"LP neither burned nor locked ({lp_state})", seen)

    unlock = _timestamp(facts.get("lp_unlock_date"))
    if unlock is None:
        seen["lp_unlock_date"] = "unknown"
    else:
        supplied += 1
        seen["lp_unlock_date"] = unlock.isoformat()
        remaining = (unlock - now).total_seconds()
        seen["lp_unlock_in_seconds"] = int(remaining)
        if remaining <= 0:
            return Verdict(False, 3, "LP lock has already expired", seen)
        if remaining < hold_horizon_seconds:
            # 3.4's free check. A lock expiring mid-hold is a countdown, and the
            # deployer chose when it ends.
            return Verdict(
                False, 3,
                f"LP unlocks in {remaining / 3600:.1f}h, inside the "
                f"{hold_horizon_seconds / 3600:.0f}h holding horizon",
                seen,
            )

    if supplied == 0:
        return Verdict(True, 3, None, {"liquidity": "unfetched"})
    return Verdict(True, 3, None, seen)


#: 3.4 stage 4. Every number here is quoted from 3.4 and gates by default.
#: `dev.percentage > 5`, `snipers.total > 20`, `insiders.total > 20`,
#: `bundlers.total > 15` or `bundlers.count >= 100`.
DEFAULT_MAX_DEV_PCT = 5.0
DEFAULT_MAX_SNIPERS = 20
DEFAULT_MAX_INSIDERS = 20
DEFAULT_MAX_BUNDLER_PCT = 15.0
DEFAULT_MAX_BUNDLER_COUNT = 100

#: How old a mint must be before a ZERO in a behavioural field is believed.
#:
#: This is the number stage 4 cannot work without. Part 2: every behavioural
#: signal -- bundle %, sniper %, insider clusters, concentration, the score --
#: has a cold start and needs transaction history that has not happened yet.
#: All of 3.4's stage-4 rules are "reject if greater than N", so a payload full
#: of structural zeros does not merely fail to reject a young mint, it ACTIVELY
#: PASSES it. That is worse than not running the stage.
#:
#: Part 2 puts the populate window at 2-10 minutes. The conservative end is
#: taken deliberately: below it the zeros are recorded as a cold start rather
#: than as a clean result, and a later calibration can drop those rows instead
#: of discovering it fitted noise.
DEFAULT_COLD_START_SECONDS = 600


def top10_share(
    holders: Any, *, exclude: Any = ()
) -> tuple[float | None, int, list[str]]:
    """Top-10 supply share EXCLUDING pool, curve and locker addresses (3.4).

    3.4: "Compute top-10 excluding pool, bonding-curve and locker addresses or
    you'll reject on the curve itself." On a pump.fun launch the curve holds
    essentially the entire supply, so a naive top-10 reads ~100% for every
    token alive and carries no information whatsoever.

    Takes an ALREADY NORMALISED list of (address, percentage) pairs -- or dicts
    with those two keys -- rather than a vendor payload. Mapping a payload into
    this shape requires knowing the vendor's field names, and inventing those is
    the one thing Part 0 rule 2 forbids outright; keeping the mapping outside
    this function means the rule can be tested and calibrated while the supplier
    is still missing.

    Returns (share, n_counted, excluded_addresses_seen). `share` is None when
    nothing countable remained, which is NOT zero concentration -- it is no
    information, and the caller must keep those apart.
    """
    excluded = {str(a) for a in (exclude or ()) if a}
    hit: list[str] = []
    kept: list[float] = []
    for entry in holders or []:
        if isinstance(entry, dict):
            address, pct = entry.get("address"), entry.get("percentage")
        elif isinstance(entry, (tuple, list)) and len(entry) == 2:
            address, pct = entry
        else:
            continue
        value = _number(pct)
        if value is None:
            continue
        if address is not None and str(address) in excluded:
            hit.append(str(address))
            continue
        kept.append(value)
    if not kept:
        return None, 0, hit
    top = sorted(kept, reverse=True)[:10]
    return round(sum(top), 4), len(top), hit


def stage4_concentration(
    facts: dict,
    *,
    max_dev_pct: float = DEFAULT_MAX_DEV_PCT,
    max_snipers: int = DEFAULT_MAX_SNIPERS,
    max_insiders: int = DEFAULT_MAX_INSIDERS,
    max_bundler_pct: float = DEFAULT_MAX_BUNDLER_PCT,
    max_bundler_count: int = DEFAULT_MAX_BUNDLER_COUNT,
    cold_start_seconds: int = DEFAULT_COLD_START_SECONDS,
    max_top10_pct: float | None = None,
    max_bundler_distribution_pct: float | None = None,
    now: dt.datetime | None = None,
) -> Verdict:
    """Coordination and concentration (3.4 stage 4). Same request as stage 3.

    Five rejections, all with numbers 3.4 states: dev share above 5%, more than
    20 snipers, more than 20 insiders, bundler share above 15%, or 100+
    bundlers. Every reason is collected rather than short-circuited -- they come
    from one payload, so there is no cost to reading all of them and a row
    saying "snipers AND insiders AND bundlers" is worth more later than one
    saying "snipers".

    TWO QUANTITIES ARE COMPUTED AND JOURNALLED BUT DO NOT GATE BY DEFAULT,
    because 3.4 gives no number for either and inventing one is forbidden:

    - Top-10 share excluding pool/curve/locker addresses. 1.5 gives a DELTA
      ("first 10 buyers hold 17 percentage points more supply than low-risk"),
      never a level, and a delta cannot be applied to a single token.
    - The bundlers' `totalInitialPercentage` - `totalPercentage` delta. 3.4:
      "high initial + low current means they already distributed into you.
      That delta is more informative than either level and is free in the
      payload." Informative, but with no threshold attached.

    Both have opt-in knobs that default to off. Journalling them now is what
    makes calibrating them possible later (3.9.3); gating on a number nobody
    measured would be a guess wearing a filter's clothes.

    THE COLD START IS ENFORCED, NOT NOTED. See DEFAULT_COLD_START_SECONDS: a
    behavioural payload read too early is all zeros, and all of these rules pass
    on zero. Below the floor the zeros are recorded as `cold_start` and no rule
    is allowed to fire on them in either direction.
    """
    now = now or dt.datetime.now(tz=dt.UTC)
    seen: dict[str, Any] = {}
    reasons: list[str] = []
    supplied = 0

    age = _age_seconds(facts, now=now)
    if age is not None:
        seen["token_age_seconds"] = age

    numeric = (
        ("dev_percentage", max_dev_pct, "dev holds {value:.1f}% (> {limit:g}%)", False),
        ("snipers_total", max_snipers, "{value:.0f} snipers (> {limit:g})", False),
        ("insiders_total", max_insiders, "{value:.0f} insiders (> {limit:g})", False),
        ("bundlers_total", max_bundler_pct,
         "bundlers hold {value:.1f}% (> {limit:g}%)", False),
        ("bundlers_count", max_bundler_count,
         "{value:.0f} bundlers (>= {limit:g})", True),
    )
    for key, limit, template, inclusive in numeric:
        value = _number(facts.get(key))
        if value is None:
            seen[key] = "unknown"
            continue
        supplied += 1
        seen[key] = value
        # 3.4's comparators are not uniform: bundlers.count is >=, the rest are
        # strictly >. Copying them exactly matters at the boundary, which is
        # exactly where a filter gets argued about.
        if (value >= limit) if inclusive else (value > limit):
            reasons.append(template.format(value=value, limit=limit))

    initial = _number(facts.get("bundlers_total_initial_percentage"))
    current = _number(facts.get("bundlers_total_percentage"))
    if initial is not None and current is not None:
        supplied += 1
        seen["bundlers_total_initial_percentage"] = initial
        seen["bundlers_total_percentage"] = current
        distributed = round(initial - current, 4)
        seen["bundlers_distributed_pct"] = distributed
        if max_bundler_distribution_pct is not None and distributed >= max_bundler_distribution_pct:
            reasons.append(
                f"bundlers already distributed {distributed:.1f}% of supply "
                f"({initial:.1f}% -> {current:.1f}%)"
            )

    share, counted, excluded_hit = top10_share(
        facts.get("holders"), exclude=facts.get("excluded_addresses") or ()
    )
    if share is None:
        seen["top10_pct"] = "unknown"
    else:
        supplied += 1
        seen["top10_pct"] = share
        seen["top10_counted"] = counted
        # Recorded so a row can be read later without guessing whether the
        # exclusion actually ran. An empty list on a pump.fun mint means the
        # curve was NOT excluded and the number is close to meaningless.
        seen["top10_excluded"] = excluded_hit
        if max_top10_pct is not None and share > max_top10_pct:
            reasons.append(f"top 10 hold {share:.1f}% (> {max_top10_pct:g}%)")

    if supplied == 0:
        # Terse on purpose: a wall of "unknown" markers reads like a report.
        # Nothing was supplied, and the age (if we have it) is the only fact
        # worth keeping, because it is what says whether it was ever going to be.
        return Verdict(True, 4, None, {
            "concentration": "unfetched",
            **{k: v for k, v in seen.items() if v != "unknown"},
        })

    if age is not None and age < cold_start_seconds:
        # Part 2. The fields arrived, but too early to mean anything, and every
        # rule above passes on a zero. Neither accepting nor rejecting on these
        # values is defensible, so the row says which one it was.
        return Verdict(True, 4, None, {
            **seen,
            "concentration": "cold_start",
            "cold_start_floor_seconds": cold_start_seconds,
        })

    if reasons:
        return Verdict(False, 4, "; ".join(reasons), seen)
    return Verdict(True, 4, None, seen)


def _age_seconds(facts: dict, *, now: dt.datetime) -> int | None:
    """Token age, from an explicit value or from `detected_at`."""
    explicit = _number(facts.get("token_age_seconds"))
    if explicit is not None:
        return int(explicit)
    detected = _timestamp(facts.get("detected_at"))
    if detected is None:
        return None
    return int((now - detected).total_seconds())


def _number(value: Any) -> float | None:
    """Parse a numeric field, or None. Never raises, never guesses a default."""
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(str(value).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed and parsed not in (float("inf"), float("-inf")) else None


def _timestamp(value: Any) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt.UTC)
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
        seconds = value / 1000 if value > 10**11 else value
        try:
            return dt.datetime.fromtimestamp(seconds, tz=dt.UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str) and value.strip():
        try:
            parsed = dt.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)
    return None


def _truthy(value: Any) -> bool:
    """Vendor booleans arrive as 1/0, "1"/"0", true/false. All mean the same."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return False


def _positive(value: Any) -> bool:
    try:
        return float(str(value)) > 0
    except (TypeError, ValueError):
        return False


@dataclass(slots=True)
class Cascade:
    """Runs stages in cost order and stops at the first rejection.

    Short-circuiting IS the cost model (3.4): if 95% die at stages 1-2, the
    expensive stages see a twentieth of the volume. The stage that rejected is
    recorded so `rules-report` can ask which ones are earning their keep.
    """

    min_mints: int = DEFAULT_CREATOR_MIN_MINTS
    max_rug_rate: float = DEFAULT_CREATOR_MAX_RUG_RATE
    min_liquidity_usd: float = DEFAULT_MIN_LIQUIDITY_USD
    hold_horizon_seconds: int = DEFAULT_HOLD_HORIZON_SECONDS
    max_dev_pct: float = DEFAULT_MAX_DEV_PCT
    max_snipers: int = DEFAULT_MAX_SNIPERS
    max_insiders: int = DEFAULT_MAX_INSIDERS
    max_bundler_pct: float = DEFAULT_MAX_BUNDLER_PCT
    max_bundler_count: int = DEFAULT_MAX_BUNDLER_COUNT
    cold_start_seconds: int = DEFAULT_COLD_START_SECONDS
    #: Off unless set. 3.4 gives no number for either; see stage4_concentration.
    max_top10_pct: float | None = None
    max_bundler_distribution_pct: float | None = None

    def thresholds(self) -> dict:
        """Snapshotted into every decision row (3.9.3).

        Without this, tuning a threshold silently changes what earlier rows
        mean and any comparison across time becomes a comparison of two
        different systems.
        """
        return {
            "creator_min_mints": self.min_mints,
            "creator_max_rug_rate": self.max_rug_rate,
            "min_liquidity_usd": self.min_liquidity_usd,
            "hold_horizon_seconds": self.hold_horizon_seconds,
            "max_dev_pct": self.max_dev_pct,
            "max_snipers": self.max_snipers,
            "max_insiders": self.max_insiders,
            "max_bundler_pct": self.max_bundler_pct,
            "max_bundler_count": self.max_bundler_count,
            "cold_start_seconds": self.cold_start_seconds,
            "max_top10_pct": self.max_top10_pct,
            "max_bundler_distribution_pct": self.max_bundler_distribution_pct,
            # Stage 4's five gating numbers are quoted from 3.4. Stage 3's
            # floor and stage 4's two derived quantities are NOT measurements:
            # until they are tuned against matched controls they are
            # assumptions, and recording that alongside them keeps a later
            # calibration honest.
            "calibrated": False,
            "stages": [0, 1, 2, 3, 4],
        }

    def run(self, facts: dict) -> tuple[Verdict, list[Verdict]]:
        """Return the deciding verdict and the trail of every stage that ran."""
        trail: list[Verdict] = []
        for verdict in (
            stage0_ingest(facts),
            stage1_structural(facts),
            stage2_creator(
                facts, min_mints=self.min_mints, max_rug_rate=self.max_rug_rate
            ),
            stage3_liquidity(
                facts, min_liquidity_usd=self.min_liquidity_usd,
                hold_horizon_seconds=self.hold_horizon_seconds,
            ),
            stage4_concentration(
                facts,
                max_dev_pct=self.max_dev_pct,
                max_snipers=self.max_snipers,
                max_insiders=self.max_insiders,
                max_bundler_pct=self.max_bundler_pct,
                max_bundler_count=self.max_bundler_count,
                cold_start_seconds=self.cold_start_seconds,
                max_top10_pct=self.max_top10_pct,
                max_bundler_distribution_pct=self.max_bundler_distribution_pct,
            ),
        ):
            trail.append(verdict)
            if verdict.rejected:
                return verdict, trail
        return trail[-1], trail
