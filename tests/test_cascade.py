"""The decision cascade, stages 0-2. Offline, pure functions.

The property that matters most here and is asserted repeatedly: an UNKNOWN fact
is never treated as a clean one. Part 2's cold start means that on a young mint
the behavioural fields are empty rather than safe, and reading empty as clean is
named there as the single most expensive mistake available.
"""
from __future__ import annotations

import pytest
from conftest import pubkey

from trenches.decide import Cascade, stage0_ingest, stage1_structural, stage2_creator


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


# -- the cascade -----------------------------------------------------------

def test_the_cascade_stops_at_the_first_rejection():
    """Short-circuiting IS the cost model: later stages must not be reached."""
    verdict, trail = Cascade().run(facts(mintable=1, creator_n_mints=10, creator_n_rugged=9))
    assert verdict.rejected and verdict.stage == 1
    assert [v.stage for v in trail] == [0, 1]      # stage 2 never ran


def test_a_clean_candidate_runs_every_stage():
    verdict, trail = Cascade().run(facts(mintable=0, freezable=0))
    assert verdict.accept
    assert [v.stage for v in trail] == [0, 1, 2]


def test_thresholds_are_snapshotted_for_the_journal():
    """Tuning a threshold otherwise silently invalidates earlier rows."""
    t = Cascade(min_mints=5, max_rug_rate=0.9).thresholds()
    assert t["creator_min_mints"] == 5
    assert t["creator_max_rug_rate"] == 0.9


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
