"""Funding-chain clustering (3.4 stage 5): union-find, label sets, the rule.

The property asserted hardest here is the one that makes stage 5 dangerous
rather than merely absent: WITHOUT A CEX LABEL SET THE ANSWER INVERTS. Every
wallet funded from one exchange hot wallet unions into a single cluster, that
cluster spans most of the holder set, and the stage would reject the entire
market while looking like it found coordination in all of it. A confident wrong
answer is worse than a gap, so the stage must decline to gate.
"""
from __future__ import annotations

import json

import pytest
from conftest import pubkey

from trenches.decide import (
    CO_BUY,
    EMPTY,
    FUNDING,
    Cascade,
    LabelSet,
    UnionFind,
    cluster,
    load_labels,
    stage5_clustering,
)


def addr(n: int) -> str:
    return pubkey(n)


# -- union-find ------------------------------------------------------------

def test_disjoint_addresses_are_their_own_clusters():
    uf = UnionFind(["a", "b", "c"])
    assert sorted(len(g) for g in uf.groups()) == [1, 1, 1]


def test_a_funding_chain_collapses_to_one_cluster():
    """The whole point: a -> b -> c -> d is one actor, not four holders."""
    uf = UnionFind()
    for a, b in (("a", "b"), ("b", "c"), ("c", "d")):
        uf.union(a, b)
    groups = uf.groups()
    assert len(groups) == 1 and groups[0] == {"a", "b", "c", "d"}


def test_union_is_idempotent_and_order_independent():
    forward, backward = UnionFind(), UnionFind()
    for a, b in (("a", "b"), ("b", "c"), ("a", "c"), ("a", "b")):
        forward.union(a, b)
    for a, b in (("c", "a"), ("b", "c"), ("b", "a")):
        backward.union(a, b)
    assert forward.groups() == backward.groups()


def test_find_survives_a_long_chain():
    """Path compression is not decoration -- a funder trace can be deep and
    this runs per candidate."""
    uf = UnionFind()
    for i in range(2_000):
        uf.union(f"n{i}", f"n{i + 1}")
    assert uf.find("n0") == uf.find("n2000")
    assert len(uf.groups()) == 1


# -- stripping, which is the part that decides whether any of this works ---

def test_stripping_the_exchange_breaks_the_false_cluster():
    """Two strangers who both withdrew from Binance share a funder and nothing
    else. Not stripping it makes them the same actor."""
    edges = [("A", "CEX", FUNDING), ("B", "CEX", FUNDING)]
    naive = cluster(["A", "B", "CEX"], edges, strip={})
    assert len(naive.clusters) == 1          # A, B and the exchange: one "actor"

    stripped = cluster(["A", "B", "CEX"], edges, strip={"CEX": "cex"})
    assert sorted(len(c) for c in stripped.clusters) == [1, 1]
    assert stripped.stripped == {"CEX": "cex"}
    assert stripped.edges_dropped == 2


def test_a_labelled_funder_two_hops_out_is_still_recorded():
    """The endpoint need not be in the address list to have been stripped, and
    a row that does not say so cannot be read later."""
    found = cluster(["A"], [("A", "CEX", FUNDING)], strip={"CEX": "cex"})
    assert found.stripped == {"CEX": "cex"}


def test_real_coordination_survives_stripping():
    """The test that stops the fix from becoming the bug: stripping exchanges
    must not also erase the thing we are looking for."""
    edges = [("DEV", "S1", FUNDING), ("DEV", "S2", FUNDING), ("S2", "S3", FUNDING),
             ("HONEST", "CEX", FUNDING)]
    found = cluster(["DEV", "S1", "S2", "S3", "HONEST"], edges, strip={"CEX": "cex"})
    sizes = sorted(len(c) for c in found.clusters)
    assert sizes == [1, 4]                    # the ring, and the honest buyer


# -- share, which is what the threshold is actually about ------------------

def test_the_largest_cluster_is_by_supply_not_by_member_count():
    """Ten wallets holding dust are not a risk; two holding 30% are."""
    edges = [("A", "B", FUNDING)] + [(f"D{i}", f"D{i + 1}", FUNDING) for i in range(9)]
    found = cluster(["A", "B", *[f"D{i}" for i in range(10)]], edges, strip={})
    holdings = {"A": 20.0, "B": 10.0, **{f"D{i}": 0.1 for i in range(10)}}
    share, members = found.largest(holdings)
    assert share == 30.0 and members == {"A", "B"}


def test_an_unknown_holding_is_not_a_zero_holding():
    found = cluster(["A", "B"], [("A", "B", FUNDING)], strip={})
    assert found.largest(None) == (None, set())
    assert found.largest({}) == (None, set())


def test_coverage_says_whether_the_share_means_anything():
    """A 4% largest cluster over addresses covering 6% of supply is a finding
    about how little was observed, not about concentration."""
    found = cluster(["A", "B"], [("A", "B", FUNDING)], strip={})
    assert found.coverage({"A": 4.0, "B": 2.0, "SOMEONE_ELSE": 90.0}) == 6.0


def test_clustering_never_raises_on_junk_edges():
    found = cluster(
        ["A", "B", None, "  ", "A"],
        [("A", "B"), None, 42, ("A",), ("A", None), ("A", "A"), {"from": "A", "to": "B"}],
        strip={},
    )
    assert len(found.clusters) == 1
    assert found.relations == {FUNDING}      # an unlabelled edge is 3.4's trace


# -- label sets ------------------------------------------------------------

def test_an_empty_label_set_is_not_usable_and_the_shipped_example_is_empty():
    """The example file must stay empty: a hot-wallet address recalled from
    memory is exactly the invented constant rule 2 forbids, and exchanges
    rotate them."""
    assert EMPTY.usable is False
    shipped = load_labels("labels.example.json")
    assert shipped.counts() == {"cex": 0, "dex": 0, "pool": 0, "locker": 0}
    assert shipped.usable is False


def test_usability_turns_on_the_cex_set_specifically_not_the_total():
    """DEX and pool labels do not fix the failure. Only CEX addresses do,
    because the exchange is the shared funder that creates the false cluster."""
    assert LabelSet(dex=frozenset({"D"}), pool=frozenset({"P"})).usable is False
    assert LabelSet(cex=frozenset({"C"})).usable is True


def test_a_label_set_loads_and_ignores_spare_columns(tmp_path):
    path = tmp_path / "labels.json"
    path.write_text(json.dumps(
        {"cex": ["C1", "C2"], "locker": ["L1"], "exported_at": "whenever"}))
    labels = load_labels(path)
    assert labels.counts() == {"cex": 2, "dex": 0, "pool": 0, "locker": 1}
    assert labels.strip_map()["C1"] == "cex"
    assert labels.usable


def test_a_missing_label_file_is_an_error_but_an_unset_path_is_not():
    """Unset means "I have not got one". A path that points nowhere means the
    file moved, and silently continuing would turn stage 5 off invisibly."""
    assert load_labels(None) is EMPTY
    assert load_labels("") is EMPTY
    with pytest.raises(FileNotFoundError):
        load_labels("/nonexistent/labels.json")


def test_per_token_pool_addresses_ride_alongside_the_maintained_set():
    labels = LabelSet(cex=frozenset({"C"}))
    strip = labels.strip_map({"CURVE": "pool"})
    assert strip == {"C": "cex", "CURVE": "pool"}


# -- the rule --------------------------------------------------------------

USABLE = LabelSet(cex=frozenset({"CEX"}), source="test")


def ring(**kw) -> dict:
    """A four-wallet funded ring holding 24% between them."""
    base = {
        "mint": addr(1),
        "cluster_addresses": ["DEV", "S1", "S2", "HONEST"],
        "cluster_edges": [("DEV", "S1", FUNDING), ("S1", "S2", FUNDING)],
        "holdings": {"DEV": 10.0, "S1": 8.0, "S2": 6.0, "HONEST": 30.0},
    }
    base.update(kw)
    return base


def test_a_funded_ring_above_the_threshold_is_rejected():
    v = stage5_clustering(ring(), labels=USABLE)
    assert v.rejected and v.stage == 5
    assert "24.0% of supply" in v.reason
    assert v.inputs["largest_cluster_pct"] == 24.0


def test_a_single_large_honest_holder_is_not_a_cluster():
    """HONEST holds 30% alone -- more than the ring -- and stage 5 must not
    reject on it. That is stage 4's question, and answering it here would make
    concentration look like coordination."""
    v = stage5_clustering(
        ring(holdings={"DEV": 1.0, "S1": 1.0, "S2": 1.0, "HONEST": 30.0}),
        labels=USABLE)
    assert v.accept
    # Both numbers are in the row. The gate ran on the cluster (3%), and the
    # 30% is recorded as what it is -- a large holder, not a coordinated one.
    assert v.inputs["largest_cluster_pct"] == 3.0
    assert v.inputs["largest_single_holder_pct"] == 30.0


def test_the_threshold_is_3_4s_and_is_strict():
    at = ring(holdings={"DEV": 10.0, "S1": 5.0, "S2": 0.0, "HONEST": 1.0})
    assert stage5_clustering(at, labels=USABLE).accept          # exactly 15
    over = ring(holdings={"DEV": 10.0, "S1": 5.01, "S2": 0.0, "HONEST": 1.0})
    assert stage5_clustering(over, labels=USABLE).rejected


# -- the four reasons it declines to gate ----------------------------------

def test_no_cex_label_set_suppresses_the_rejection_and_says_why():
    """THE headline failure mode. The same ring that rejects above must not
    reject when the labels that make 'non-CEX' meaningful are absent."""
    v = stage5_clustering(ring(), labels=EMPTY)
    assert v.accept
    assert v.inputs["clustering"] == "labels_missing"
    assert v.inputs["largest_cluster_pct"] == 24.0   # computed, just not acted on


def test_without_stripping_the_market_would_be_rejected_wholesale():
    """Why that suppression exists, demonstrated rather than asserted: six
    unrelated wallets that each withdrew from one exchange become one cluster
    holding 60% of supply."""
    holders = [f"W{i}" for i in range(6)]
    facts = {
        "cluster_addresses": [*holders, "CEX"],
        "cluster_edges": [(w, "CEX", FUNDING) for w in holders],
        "holdings": {w: 10.0 for w in holders},
    }
    unlabelled = stage5_clustering(facts, labels=EMPTY)
    assert unlabelled.inputs["largest_cluster_pct"] == 60.0
    assert unlabelled.accept, "an unlabelled run must never reject"

    # Stripped, the same six wallets are six lone holders and no cluster at all.
    labelled = stage5_clustering(facts, labels=USABLE)
    assert labelled.accept
    assert labelled.inputs["n_multi_address_clusters"] == 0
    assert labelled.inputs["largest_single_holder_pct"] == 10.0


def test_addresses_with_no_edges_are_not_a_clustering_result():
    """With no funding edges every address is its own cluster, so the 'largest
    cluster' is just the largest holder -- stage 4's question, answered worse."""
    v = stage5_clustering(
        ring(cluster_edges=[], holdings={"HONEST": 90.0}), labels=USABLE)
    assert v.accept
    assert v.inputs["clustering"] == "no_edges"


def test_co_buy_edges_alone_do_not_borrow_3_4s_funding_threshold():
    """3.4's 15% was stated for funding chains. Applying it to a different
    relation would be inventing a number for that relation."""
    v = stage5_clustering(
        ring(cluster_edges=[("DEV", "S1", CO_BUY), ("S1", "S2", CO_BUY)]),
        labels=USABLE)
    assert v.accept
    assert v.inputs["clustering"] == "no_edges"
    assert v.inputs["relations"] == [CO_BUY]


def test_a_cluster_with_no_known_holdings_is_unmeasurable_not_clean():
    v = stage5_clustering(ring(holdings=None), labels=USABLE)
    assert v.accept
    assert v.inputs["clustering"] == "unmeasurable"
    assert "largest_cluster_pct" not in v.inputs


def test_no_addresses_at_all_says_unfetched():
    v = stage5_clustering({"mint": addr(1)}, labels=USABLE)
    assert v.accept
    assert v.inputs["clustering"] == "unfetched"


def test_the_label_set_in_force_is_journalled_on_every_row():
    """The same clustering means different things with and without a CEX set.
    A row that does not record which it had cannot be compared with one that
    did."""
    v = stage5_clustering(ring(), labels=USABLE)
    assert v.inputs["labels_source"] == "test"
    assert v.inputs["labels"]["cex"] == 1


def test_the_per_token_pool_address_is_stripped_too():
    """3.4 says pool and curve addresses come out as well, and those are
    per-token -- they cannot live in a maintained label set."""
    v = stage5_clustering(
        ring(cluster_addresses=["DEV", "S1", "CURVE"],
             cluster_edges=[("DEV", "CURVE", FUNDING), ("S1", "CURVE", FUNDING)],
             excluded_addresses=["CURVE"],
             holdings={"DEV": 40.0, "S1": 40.0}),
        labels=USABLE)
    assert v.accept, "the curve must not union every buyer into one actor"
    assert v.inputs["n_stripped"] == 1


def test_stage_5_never_raises_on_junk():
    for junk in ({"cluster_addresses": "not a list"}, {"cluster_edges": 42},
                 {"holdings": []}, {"holdings": {"A": "abc"}},
                 {"cluster_addresses": [None]}, {"excluded_addresses": None}):
        stage5_clustering({"mint": addr(1), **junk}, labels=USABLE)


# -- the cascade -----------------------------------------------------------

def test_the_cascade_runs_stage_5_last_and_can_reject_there():
    import datetime as dt

    facts = {
        "mint": addr(1), "mintable": 0, "liquidity_usd": 90_000,
        "detected_at": dt.datetime.now(tz=dt.UTC) - dt.timedelta(hours=1),
        **ring(),
    }
    verdict, trail = Cascade(labels=USABLE).run(facts)
    assert verdict.rejected and verdict.stage == 5
    assert [v.stage for v in trail] == [0, 1, 2, 3, 4, 5]


def test_the_default_cascade_cannot_reject_at_stage_5():
    """Shipped configuration has no label set, so this must hold until the
    operator supplies one."""
    verdict, trail = Cascade().run({"mint": addr(1), **ring()})
    assert verdict.accept
    assert trail[-1].inputs["clustering"] == "labels_missing"
