"""The decision cascade, stages 0-3. Offline, pure functions.

The property that matters most here and is asserted repeatedly: an UNKNOWN fact
is never treated as a clean one. Part 2's cold start means that on a young mint
the behavioural fields are empty rather than safe, and reading empty as clean is
named there as the single most expensive mistake available.
"""
from __future__ import annotations

import datetime as dt

import pytest
from conftest import pubkey

from trenches.decide import (
    Cascade,
    stage0_ingest,
    stage1_structural,
    stage2_creator,
    stage3_liquidity,
)


def facts(**kw) -> dict:
    base = {"mint": pubkey(1), "signer": pubkey(2), "launchpad": "pump.fun"}
    base.update(kw)
    return base


# -- stage 0 ---------------------------------------------------------------

def test_a_candidate_without_a_mint_is_rejected():
    v = stage0_ingest({"signer": pubkey(2)})
    assert v.rejected and v.stage == 0


def test_a_normal_candidate_passes_stage_0():
    assert stage0_ingest(facts()).accept


# -- stage 1: structural ---------------------------------------------------

@pytest.mark.parametrize("flag", [
    "mintable", "freezable", "closable", "balance_mutable_authority",
    "transfer_fee_upgradable", "transfer_hook_upgradable", "metadata_mutable",
    "default_account_state_upgradable", "non_transferable",
])
def test_each_live_authority_or_upgradable_flag_rejects(flag):
    v = stage1_structural(facts(**{flag: 1}))
    assert v.rejected, f"{flag} should reject"
    assert v.stage == 1


def test_a_transfer_hook_rejects():
    assert stage1_structural(facts(transfer_hook=["SomeProgram"])).rejected


def test_a_nonzero_transfer_fee_rejects():
    assert stage1_structural(facts(transfer_fee="1.5")).rejected
    assert stage1_structural(facts(transfer_fee="0")).accept


def test_vendor_booleans_are_read_in_every_shape_they_arrive_in():
    for value in (1, "1", True, "true", "TRUE", "yes"):
        assert stage1_structural(facts(mintable=value)).rejected, value
    for value in (0, "0", False, "false", ""):
        assert stage1_structural(facts(mintable=value)).accept, value


def test_all_clear_structural_facts_pass():
    v = stage1_structural(facts(mintable=0, freezable=0, transfer_fee="0"))
    assert v.accept
    assert v.inputs["mintable"] == 0


def test_unfetched_structural_facts_pass_but_say_so():
    """Absence must not read as a clean bill of health in the journal."""
    v = stage1_structural(facts())
    assert v.accept
    assert v.inputs == {"structural": "unfetched"}


def test_a_single_bad_flag_among_good_ones_still_rejects():
    v = stage1_structural(facts(mintable=0, freezable=0, transfer_hook_upgradable=1))
    assert v.rejected
    assert "transfer hook upgradable" in v.reason


# -- stage 2: creator reputation -------------------------------------------

def test_an_unknown_creator_proceeds_rather_than_blocking():
    """3.4: fresh wallets are free, so unknown is the modal case and blocking
    on it would gate latency on a cold lookup for almost no information."""
    v = stage2_creator(facts())
    assert v.accept
    assert v.inputs["creator"] == "unknown"


def test_a_serial_rugger_is_rejected():
    v = stage2_creator(facts(creator_n_mints=10, creator_n_rugged=8))
    assert v.rejected
    assert v.stage == 2
    assert "10 mints" in v.reason


def test_a_prolific_but_clean_creator_is_not_rejected():
    assert stage2_creator(facts(creator_n_mints=50, creator_n_rugged=0)).accept


def test_a_high_rug_rate_on_too_few_mints_does_not_reject():
    """One rug out of one launch is not evidence; the mint floor is what stops
    a single unlucky sample from condemning a wallet."""
    assert stage2_creator(facts(creator_n_mints=1, creator_n_rugged=1)).accept


def test_the_rule_cannot_fire_while_nothing_labels_rugs():
    """Today every creator reads n_rugged=0, so this documents WHY stage 2
    currently rejects nobody -- it is not broken, it is unlabelled."""
    v = stage2_creator(facts(creator_n_mints=372, creator_n_rugged=0))
    assert v.accept
    assert v.inputs["rug_rate"] == 0.0


def test_both_identities_are_journalled_even_when_one_is_missing():
    """PumpPortal carries no creator. The journal must record that, not hide it."""
    v = stage2_creator(facts(declared_creator=None, creator_n_mints=2))
    assert v.inputs["signer"] is not None
    assert v.inputs["declared_creator"] is None


# -- stage 3: liquidity, LP lock state, unlock horizon ---------------------

NOW = dt.datetime(2026, 9, 3, 12, 0, tzinfo=dt.UTC)


def liq(**kw) -> dict:
    """Facts that pass stage 3, so each test can break exactly one thing."""
    base = facts(
        rugged=False,
        liquidity_usd=50_000,
        lp_state="burned",
        lp_unlock_date=NOW + dt.timedelta(days=30),
    )
    base.update(kw)
    return base


def test_an_already_rugged_token_is_rejected_first():
    """Cheapest answer, and the only one needing no interpretation."""
    v = stage3_liquidity(liq(rugged=True), now=NOW)
    assert v.rejected and v.stage == 3
    assert "rugged" in v.reason


def test_liquidity_below_the_floor_is_rejected():
    v = stage3_liquidity(liq(liquidity_usd=800), now=NOW)
    assert v.rejected
    assert "$800" in v.reason and "floor" in v.reason


def test_liquidity_exactly_at_the_floor_passes():
    """The floor is a floor, not a gap: >= passes, < rejects."""
    from trenches.decide.cascade import DEFAULT_MIN_LIQUIDITY_USD
    assert stage3_liquidity(liq(liquidity_usd=DEFAULT_MIN_LIQUIDITY_USD), now=NOW).accept
    assert stage3_liquidity(
        liq(liquidity_usd=DEFAULT_MIN_LIQUIDITY_USD - 0.01), now=NOW
    ).rejected


def test_an_unlocked_lp_is_rejected():
    v = stage3_liquidity(liq(lp_state="unlocked"), now=NOW)
    assert v.rejected
    assert "neither burned nor locked" in v.reason


@pytest.mark.parametrize("state", ["burned", "Burned", " LOCKED ", "locked"])
def test_burned_and_locked_are_the_only_safe_states_and_case_does_not_matter(state):
    assert stage3_liquidity(liq(lp_state=state), now=NOW).accept


def test_a_lock_expiring_inside_the_holding_horizon_is_rejected():
    """3.4's free check: a lock that ends while you hold is a countdown, and
    the deployer chose when it ends."""
    v = stage3_liquidity(
        liq(lp_unlock_date=NOW + dt.timedelta(hours=2)),
        hold_horizon_seconds=6 * 3600, now=NOW,
    )
    assert v.rejected
    assert "inside the" in v.reason
    assert v.inputs["lp_unlock_in_seconds"] == 2 * 3600


def test_an_expired_lock_is_rejected_separately_from_a_short_one():
    v = stage3_liquidity(liq(lp_unlock_date=NOW - dt.timedelta(minutes=1)), now=NOW)
    assert v.rejected
    assert "already expired" in v.reason


def test_a_lock_beyond_the_horizon_passes():
    assert stage3_liquidity(
        liq(lp_unlock_date=NOW + dt.timedelta(hours=7)),
        hold_horizon_seconds=6 * 3600, now=NOW,
    ).accept


@pytest.mark.parametrize("value", [
    1788436800,                 # epoch seconds
    1788436800000,              # epoch milliseconds
    "2026-09-03T12:00:00Z",     # ISO with Z
    "2026-09-03T12:00:00+00:00",
    "2026-09-03T12:00:00",      # naive, read as UTC
])
def test_unlock_dates_are_parsed_in_every_shape_a_vendor_might_send(value):
    """Whatever the shape, an unlock at or before now must not read as absent --
    an unparsed date silently becomes "no lock to worry about"."""
    v = stage3_liquidity(liq(lp_unlock_date=value), now=NOW)
    assert v.rejected, value


def test_an_unparseable_unlock_date_reads_as_unknown_not_as_no_lock():
    v = stage3_liquidity(liq(lp_unlock_date="soon"), now=NOW)
    assert v.accept
    assert v.inputs["lp_unlock_date"] == "unknown"


def test_unknown_fields_are_recorded_as_unknown_rather_than_assumed_clean():
    """Part 2: at the cold start these fields are EMPTY, not safe. A journal row
    that cannot tell 'we looked and it was fine' from 'we never looked' is the
    row that later justifies a loss."""
    v = stage3_liquidity(facts(liquidity_usd=50_000), now=NOW)
    assert v.accept
    assert v.inputs["lp_state"] == "unknown"
    assert v.inputs["lp_unlock_date"] == "unknown"
    assert "rugged" not in v.inputs


def test_a_candidate_nobody_fetched_says_unfetched_rather_than_listing_unknowns():
    """The distinction that makes 'unfetched' worth having: nothing supplied at
    all is a different fact from three fields checked and one missing."""
    v = stage3_liquidity(facts(), now=NOW)
    assert v.accept
    assert v.inputs == {"liquidity": "unfetched"}


def test_a_single_supplied_field_is_not_unfetched():
    v = stage3_liquidity(facts(rugged=False), now=NOW)
    assert v.accept
    assert v.inputs != {"liquidity": "unfetched"}
    assert v.inputs["rugged"] is False


def test_vendor_liquidity_strings_are_parsed_not_dropped():
    """DexScreener values arrive as exact TEXT, never floats (schema 006)."""
    assert stage3_liquidity(liq(liquidity_usd="800.0"), now=NOW).rejected
    assert stage3_liquidity(liq(liquidity_usd="$1,200"), now=NOW).rejected
    assert stage3_liquidity(liq(liquidity_usd="not a number"), now=NOW).accept


def test_the_age_of_the_liquidity_reading_is_journalled():
    """A liquidity number is only as good as its timestamp. No age gate -- 3.4
    gives no number -- but a later calibration must be able to ask."""
    v = stage3_liquidity(
        liq(liquidity_observed_at=NOW - dt.timedelta(minutes=45)), now=NOW,
    )
    assert v.accept
    assert v.inputs["liquidity_age_seconds"] == 45 * 60


def test_stage_3_never_raises_on_junk():
    """It runs on vendor payloads, so every branch must survive garbage."""
    for junk in ({"liquidity_usd": object()}, {"lp_unlock_date": []},
                 {"rugged": {}}, {"liquidity_usd": float("nan")},
                 {"lp_unlock_date": -1}, {"liquidity_usd": float("inf")}):
        stage3_liquidity(facts(**junk), now=NOW)


# -- the cascade -----------------------------------------------------------

def test_the_cascade_stops_at_the_first_rejection():
    """Short-circuiting IS the cost model: later stages must not be reached."""
    verdict, trail = Cascade().run(facts(mintable=1, creator_n_mints=10, creator_n_rugged=9))
    assert verdict.rejected and verdict.stage == 1
    assert [v.stage for v in trail] == [0, 1]      # stage 2 never ran


def test_a_clean_candidate_runs_every_stage():
    verdict, trail = Cascade().run(facts(mintable=0, freezable=0))
    assert verdict.accept
    assert [v.stage for v in trail] == [0, 1, 2, 3]


def test_the_cascade_reaches_stage_3_and_can_reject_there():
    verdict, trail = Cascade().run(facts(mintable=0, liquidity_usd=100))
    assert verdict.rejected and verdict.stage == 3
    assert [v.stage for v in trail] == [0, 1, 2, 3]


def test_stage_3_runs_last_because_it_costs_a_request():
    """3.4: ordering saves more money than caching. A candidate that dies on a
    free structural flag must never reach the stage that spends a lookup."""
    _, trail = Cascade().run(facts(mintable=1, liquidity_usd=100))
    assert [v.stage for v in trail] == [0, 1]


def test_thresholds_are_snapshotted_for_the_journal():
    """Tuning a threshold otherwise silently invalidates earlier rows."""
    t = Cascade(min_mints=5, max_rug_rate=0.9, min_liquidity_usd=2_500,
                hold_horizon_seconds=3600).thresholds()
    assert t["creator_min_mints"] == 5
    assert t["creator_max_rug_rate"] == 0.9
    assert t["min_liquidity_usd"] == 2_500
    assert t["hold_horizon_seconds"] == 3600
    assert t["stages"] == [0, 1, 2, 3]


def test_the_journal_records_that_the_thresholds_are_uncalibrated():
    """3.4 gives no liquidity floor and the matched-control data does not exist
    yet, so the number is an assumption. A row that does not say so reads later
    as a finding."""
    assert Cascade().thresholds()["calibrated"] is False


def test_every_stage_records_the_inputs_it_saw():
    """3.9.3: a rejection that later 10x'd is training data, and it is only
    training data if the inputs behind it were kept."""
    _, trail = Cascade().run(facts(mintable=1))
    assert all(isinstance(v.inputs, dict) for v in trail)
    assert trail[1].inputs["mintable"] == 1


def test_nothing_in_this_package_can_sign():
    """Structural guard: week 2 is paper only, and there is no execute path."""
    import trenches.decide as pkg
    source = (pkg.__file__, )
    from pathlib import Path
    text = "\n".join(Path(f).read_text() for f in source)
    for banned in ("Keypair", "sign(", "send_transaction", "private_key"):
        assert banned not in text
