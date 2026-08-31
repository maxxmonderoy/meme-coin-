"""Derived rug labels. Offline, pure functions over observation lists.

The property asserted throughout: this must UNDER-report rather than over-report.
Stage 2 rejects a wallet on this number, so a false rug is a person filtered out
for having launched during a bad week.
"""
from __future__ import annotations

from trenches.label.rugs import Collapse, find_collapse, summarise


def obs(horizon, *, status="alive", venue="dex", liq=None, when=None):
    return {
        "horizon": horizon,
        "scheduled_for": when or f"2026-08-30T00:{horizon}",
        "status": status,
        "venue_kind": venue,
        "liquidity_usd": None if liq is None else str(liq),
    }


# -- what counts ------------------------------------------------------------

def test_a_pool_that_vanishes_is_a_collapse():
    found = find_collapse([
        obs("15m", liq=50_000),
        obs("24h", status="no_pool", venue=None, liq=None),
    ])
    assert found is not None
    from_h, to_h, peak, final = found
    assert (from_h, to_h) == ("15m", "24h")
    assert peak == 50_000 and final == 0.0


def test_losing_ninety_percent_is_a_collapse():
    found = find_collapse([obs("15m", liq=100_000), obs("24h", status="dead", liq=500)])
    assert found is not None


def test_a_gentle_decline_is_not_a_collapse():
    """Liquidity drifting down is not a pull, and calling it one would label
    every fading token a rug."""
    assert find_collapse([obs("15m", liq=100_000), obs("24h", liq=60_000)]) is None


# -- what deliberately does not count --------------------------------------

def test_a_bonding_curve_that_goes_quiet_is_not_a_rug():
    """~95% of launches never graduate. Counting those as rugs would call
    almost every creator a rugger and destroy the signal entirely."""
    found = find_collapse([
        obs("15m", venue="bonding_curve", liq=5_000),
        obs("24h", status="no_pool", venue=None),
    ])
    assert found is None


def test_a_token_that_never_reached_the_liquidity_floor_cannot_collapse():
    """There was never anything to pull."""
    assert find_collapse([obs("15m", liq=50), obs("24h", status="no_pool", venue=None)]) is None


def test_a_single_observation_is_never_a_collapse():
    assert find_collapse([obs("15m", liq=90_000)]) is None


def test_a_token_that_only_ever_had_no_pool_is_not_a_collapse():
    found = find_collapse([
        obs("15m", status="no_pool", venue=None),
        obs("24h", status="no_pool", venue=None),
    ])
    assert found is None


def test_liquidity_arriving_later_is_not_read_backwards():
    """A token that was empty at 15m and deep at 24h has not collapsed."""
    assert find_collapse([
        obs("15m", status="no_pool", venue=None),
        obs("24h", liq=80_000),
    ]) is None


# -- thresholds are parameters, not truths ---------------------------------

def test_the_collapse_fraction_moves_the_boundary():
    observations = [obs("15m", liq=10_000), obs("24h", liq=2_000)]   # lost 80%
    assert find_collapse(observations, collapse_fraction=0.1) is None
    assert find_collapse(observations, collapse_fraction=0.5) is not None


def test_the_liquidity_floor_moves_the_boundary():
    observations = [obs("15m", liq=800), obs("24h", status="no_pool", venue=None)]
    assert find_collapse(observations, liquidity_floor=1000) is None
    assert find_collapse(observations, liquidity_floor=500) is not None


def test_the_peak_is_taken_from_dex_observations_only():
    """A curve reserve is not pool depth and must not set the peak."""
    found = find_collapse([
        obs("15m", venue="bonding_curve", liq=900_000),
        obs("1h", liq=5_000),
        obs("24h", status="no_pool", venue=None),
    ])
    assert found is not None
    _, _, peak, _ = found
    assert peak == 5_000, "the curve reserve leaked into the pool peak"


def test_malformed_liquidity_does_not_crash_the_pass():
    assert find_collapse([obs("15m", liq="not-a-number"), obs("24h", status="no_pool")]) is None


# -- summary ---------------------------------------------------------------

def test_summary_counts_signers_and_finds_the_median_lifetime():
    rows = [
        Collapse("m1", "sigA", 1.0, 0.0, "t", "t", 600, "15m", "1h"),
        Collapse("m2", "sigA", 1.0, 0.0, "t", "t", 1800, "15m", "1h"),
        Collapse("m3", "sigB", 1.0, 0.0, "t", "t", 3600, "15m", "24h"),
    ]
    out = summarise(rows)
    assert out["collapses"] == 3
    assert out["distinct_signers"] == 2
    assert out["worst_signer_count"] == 2
    assert out["median_lifetime_seconds"] == 1800


def test_summary_survives_missing_lifetimes():
    out = summarise([Collapse("m", None, 1.0, 0.0, "t", "t", None, "15m", "1h")])
    assert out["median_lifetime_seconds"] is None
    assert out["distinct_signers"] == 0
