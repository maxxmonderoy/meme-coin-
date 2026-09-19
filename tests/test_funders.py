"""Deriving shared-funder labels from our own graph instead of buying a list.

The property this file exists to protect: THE DERIVATION MUST NOT STRIP SERIAL
DEPLOYERS. A deployer's funding wallet appears across hundreds of mints and
looks exactly like infrastructure by a count test. 1.5 measured 178,109 of them,
85.3% net profitable against buyers -- stripping one erases the most extractive
actor in the market from the clustering built to find it. That is a worse
outcome than having no labels at all, so it is tested from both directions.
"""
from __future__ import annotations

import pytest

from trenches.decide import EMPTY, LabelSet, cluster, stage5_clustering
from trenches.decide.funders import (
    FunderStat,
    derive,
    distribution,
    widest_gap,
    withheld_funding_creators,
)


def stat(address: str, n_tokens: int, n_funded: int = 0) -> FunderStat:
    return FunderStat(address, n_tokens, n_funded or n_tokens * 3)


# -- the statistic ---------------------------------------------------------

def test_the_cutoff_applies_to_distinct_tokens_not_out_degree():
    """THE design decision. One deployer funding forty sniper wallets inside a
    single launch has out-degree 40 and is the actor stage 5 exists to catch."""
    dev = stat("DEV", n_tokens=1, n_funded=40)
    exchange = stat("CEX", n_tokens=400, n_funded=40)
    out = derive([dev, exchange], cutoff_tokens=50)
    assert sorted(out.labels) == ["CEX"]
    assert "DEV" not in out.labels, "out-degree alone would have stripped the target"


def test_a_one_token_cutoff_is_refused_outright():
    """A funder seen in exactly one token is the single-launch case by
    definition. Allowing it would let a config typo disarm the stage."""
    with pytest.raises(ValueError, match="one-token funder"):
        derive([stat("X", 5)], cutoff_tokens=1)


def test_everything_below_the_cutoff_is_left_alone():
    out = derive([stat("A", 9), stat("B", 10), stat("C", 11)], cutoff_tokens=10)
    assert sorted(out.labels) == ["B", "C"]
    assert out.n_considered == 3


# -- the serial-deployer guard, from both directions -----------------------

def test_a_known_creator_is_never_labelled_however_many_tokens_it_touches():
    out = derive([stat("SERIAL", 353)], cutoff_tokens=10, creators=["SERIAL"])
    assert out.labels == {}
    assert "SERIAL" in out.withheld


def test_the_withheld_set_is_a_finding_not_a_silent_skip():
    """Each one is a candidate serial deployer. Dropping it quietly would throw
    away the most useful thing the derivation produces."""
    out = derive([stat("SERIAL", 353, 900)], cutoff_tokens=10, creators=["SERIAL"])
    assert out.withheld["SERIAL"].n_tokens == 353
    assert out.withheld["SERIAL"].n_funded == 900


def test_a_deployer_that_signs_with_a_different_wallet_is_still_caught():
    """A serial deployer need not sign with the wallet that pays for things, so
    matching on the creator address alone misses the treasury behind it."""
    edges = [("TREASURY", "DEPLOYER_SIGNER"), ("TREASURY", "RANDO")]
    caught = withheld_funding_creators(
        edges, labelled=["TREASURY", "CEX"], creators=["DEPLOYER_SIGNER"])
    assert caught == {"TREASURY"}


def test_the_guard_does_not_fire_on_an_exchange_that_funded_nobody_relevant():
    caught = withheld_funding_creators(
        [("CEX", "SHOPPER")], labelled=["CEX"], creators=["DEPLOYER"])
    assert caught == set()


def test_the_guard_is_inert_with_no_creators_or_no_candidates():
    assert withheld_funding_creators([("A", "B")], labelled=[], creators=["B"]) == set()
    assert withheld_funding_creators([("A", "B")], labelled=["A"], creators=[]) == set()


# -- choosing a cutoff from data rather than from a hunch ------------------

def test_the_distribution_is_reported_rather_than_reduced_to_a_number():
    dist = distribution([stat("A", 1), stat("B", 1), stat("C", 400)])
    assert dist == {1: 2, 400: 1}


def test_a_bimodal_graph_shows_the_gap_the_cutoff_can_sit_in():
    """The good case: a mass of one-token funders and a tail of infrastructure,
    with nothing between. Any line in the gap gives the same answer."""
    stats = [stat("D1", 1), stat("D2", 2), stat("CEX", 300), stat("BRIDGE", 410)]
    assert widest_gap(stats) == (2, 300)     # 150x beats the 1 -> 2 step


def test_smooth_counts_report_no_gap_because_there_is_no_natural_cutoff():
    """Reporting None here is the honest answer, not a failure to compute one."""
    assert widest_gap([stat(f"F{i}", i) for i in range(2, 12)]) is None


def test_the_break_is_ranked_by_ratio_not_by_absolute_width():
    """These counts span orders of magnitude. Ranked by difference, a 100->200
    step (2x, both plainly infrastructure) beats the 1->100 step (100x, the
    actual boundary) purely for being one integer wider -- which is what a
    synthetic graph did before this was fixed."""
    stats = [stat("D", 1), stat("BRIDGE", 100), stat("CEX", 200), stat("HOT", 300)]
    assert widest_gap(stats) == (1, 100)


def test_the_single_token_mass_is_one_side_of_the_real_boundary():
    """Excluding it hides the break that matters. `derive` refuses a cutoff
    below 2 anyway, so `below == 1` reads as 'anywhere from 2 up to above'."""
    assert widest_gap([stat("D", 1), stat("A", 50), stat("B", 51)]) == (1, 50)


# -- what the labels do once derived ---------------------------------------

def test_a_derived_set_makes_stage_5_able_to_reject():
    """The whole point: no vendor, and the stage stops being inert."""
    labels = EMPTY.with_shared_funders(["CEX"], "derived(cutoff=100)")
    assert labels.usable
    assert labels.source == "derived(cutoff=100)"

    facts = {
        "cluster_addresses": ["A", "B", "C"],
        "cluster_edges": [("A", "B", "funding"), ("B", "C", "funding")],
        "holdings": {"A": 10.0, "B": 10.0, "C": 10.0},
    }
    assert stage5_clustering(facts, labels=labels).rejected


def test_a_derived_set_breaks_the_false_cluster_the_same_way_a_bought_one_does():
    holders = [f"W{i}" for i in range(6)]
    facts = {
        "cluster_addresses": [*holders, "HOT"],
        "cluster_edges": [(w, "HOT", "funding") for w in holders],
        "holdings": dict.fromkeys(holders, 10.0),
    }
    assert stage5_clustering(facts, labels=EMPTY).inputs["largest_cluster_pct"] == 60.0

    derived = EMPTY.with_shared_funders(["HOT"], "derived(cutoff=100)")
    labelled = stage5_clustering(facts, labels=derived)
    assert labelled.accept
    assert labelled.inputs["n_multi_address_clusters"] == 0


def test_derived_labels_are_not_called_cex_because_nobody_established_that():
    labels = EMPTY.with_shared_funders(["X"], "derived(cutoff=100)")
    assert labels.cex == frozenset()
    assert labels.strip_map()["X"] == "shared_funder"
    assert cluster(["X", "A"], [("A", "X", "funding")],
                   strip=labels.strip_map()).stripped == {"X": "shared_funder"}


def test_dex_pool_and_locker_sets_do_not_make_stage_5_usable():
    """Those addresses are not funders. Stripping them does nothing about the
    false cluster, and treating them as sufficient would re-arm the inversion."""
    assert LabelSet(dex=frozenset({"D"}), pool=frozenset({"P"}),
                    locker=frozenset({"L"})).usable is False


def test_a_bought_set_and_a_derived_set_compose_rather_than_replace():
    bought = LabelSet(cex=frozenset({"BINANCE"}), source="labels.json")
    both = bought.with_shared_funders(["BRIDGE"], "derived(cutoff=100)")
    assert both.cex == frozenset({"BINANCE"})
    assert both.shared_funder == frozenset({"BRIDGE"})
    assert both.source == "labels.json+derived(cutoff=100)"
    assert set(both.strip_map()) == {"BINANCE", "BRIDGE"}


def test_an_empty_derivation_leaves_the_label_set_exactly_as_it_was():
    assert EMPTY.with_shared_funders([], "derived") is EMPTY
    assert EMPTY.with_shared_funders(None, "derived") is EMPTY
