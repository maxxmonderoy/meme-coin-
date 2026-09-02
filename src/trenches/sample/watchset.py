"""Admission to the watch set: who gets a price path, and in which cohort.

TWO COHORTS, AND THE CONTROL ARM IS NOT OPTIONAL.

  filtered -- tokens that passed whatever entry criteria are being tested
  control  -- a UNIFORM RANDOM sample of all launches, ignoring every filter

Without the control arm, every backtest is selection-biased in a way that cannot
be detected from inside the data: the only tokens with paths would be ones the
filter already liked, so "this exit rule works" and "this filter picks tokens
that happen to suit this exit rule" produce identical numbers. Under budget
pressure BOTH cohorts shrink; the control arm is never sacrificed to fit more
filtered tokens.

COHORT ASSIGNMENT IS A SEEDED HASH, NOT A COIN FLIP. A random draw at admission
re-rolls on restart and on replay, so the same mint could land in different
cohorts across runs and the split would stop being reproducible. Hashing the
mint under a fixed seed is uniform, stable forever, and needs no stored state to
reproduce.
"""
from __future__ import annotations

import datetime as dt
import hashlib
from dataclasses import dataclass

FILTERED = "filtered"
CONTROL = "control"

#: Changing this reassigns every future mint and breaks comparability with
#: everything already collected. It is a constant on purpose.
COHORT_SEED = "trenches-cohort-v1"

#: Fraction of the admit budget reserved for the control arm.
DEFAULT_CONTROL_SHARE = 0.5

#: Floor on the control share. Below this the control arm stops being able to
#: say anything with confidence, and a filtered-only dataset is what the whole
#: two-cohort design exists to avoid.
MIN_CONTROL_SHARE = 0.30


class WatchSetError(ValueError):
    pass


def control_draw(mint: str, *, seed: str = COHORT_SEED) -> float:
    """Uniform [0, 1) draw for a mint. Deterministic across processes and runs."""
    digest = hashlib.sha256(f"{seed}:{mint}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / float(1 << 64)


def assign_cohort(mint: str, *, control_share: float = DEFAULT_CONTROL_SHARE,
                  seed: str = COHORT_SEED) -> str:
    """Which cohort this mint belongs to if admitted.

    Note this decides cohort MEMBERSHIP, not admission. A mint drawn into the
    control arm is admitted regardless of what any filter thinks of it -- that
    is the entire point.
    """
    if not 0 <= control_share <= 1:
        raise WatchSetError("control_share must be within [0, 1]")
    return CONTROL if control_draw(mint, seed=seed) < control_share else FILTERED


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    admit: bool
    cohort: str | None
    reason: str


@dataclass(slots=True)
class Admitter:
    """Decides admission against a derived capacity, per cohort.

    Capacity is split rather than pooled so a burst of filtered candidates
    cannot crowd out the control arm -- which is exactly what would happen with
    a single shared cap, silently, and only in the periods where the filter was
    firing most.
    """

    watch_set_max: int
    control_share: float = DEFAULT_CONTROL_SHARE
    seed: str = COHORT_SEED

    def __post_init__(self) -> None:
        if self.control_share < MIN_CONTROL_SHARE:
            raise WatchSetError(
                f"control share {self.control_share} is below the {MIN_CONTROL_SHARE} "
                "floor. Shrink both cohorts instead of starving the control arm; a "
                "filtered-only dataset cannot tell you whether the filter did anything."
            )

    def capacity(self, cohort: str) -> int:
        share = self.control_share if cohort == CONTROL else 1 - self.control_share
        return int(self.watch_set_max * share)

    def consider(
        self, mint: str, *, passes_filter: bool, current: dict[str, int]
    ) -> AdmissionDecision:
        """`current` maps cohort -> how many are already active."""
        cohort = assign_cohort(mint, control_share=self.control_share, seed=self.seed)
        if cohort == FILTERED and not passes_filter:
            return AdmissionDecision(False, None, "did not pass the filter")
        used = current.get(cohort, 0)
        cap = self.capacity(cohort)
        if used >= cap:
            return AdmissionDecision(False, cohort, f"{cohort} cohort full ({used}/{cap})")
        return AdmissionDecision(True, cohort, "admitted")


def expiry(first_seen: dt.datetime, max_age_seconds: int) -> dt.datetime:
    return first_seen + dt.timedelta(seconds=max_age_seconds)
