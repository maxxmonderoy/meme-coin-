"""Stages 0-2 of the 3.4 cascade, as pure functions.

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

    def thresholds(self) -> dict:
        """Snapshotted into every decision row (3.9.3).

        Without this, tuning a threshold silently changes what earlier rows
        mean and any comparison across time becomes a comparison of two
        different systems.
        """
        return {
            "creator_min_mints": self.min_mints,
            "creator_max_rug_rate": self.max_rug_rate,
            "stages": [0, 1, 2],
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
        ):
            trail.append(verdict)
            if verdict.rejected:
                return verdict, trail
        return trail[-1], trail
