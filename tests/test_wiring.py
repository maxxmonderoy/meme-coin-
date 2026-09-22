"""Is every cascade stage actually REACHABLE from the database?

WHY THIS FILE IS SEPARATE FROM test_cascade.py. Those tests call the stage
functions with hand-built facts dicts and prove the RULES are right. They prove
nothing about whether the facts ever arrive. The failure this file exists to
catch has bitten repeatedly: a column is renamed or a supplier is never written,
the key stops matching, the stage reads `unknown`, everything passes, and
nothing raises. A stage that cannot be fed is indistinguishable from a stage
that found nothing wrong.

So each test here seeds the database with data that SHOULD cause exactly one
stage to reject, then drives the real production path -- candidates_for_decision
-> facts_from_row -> attach_first_buyers -> attach_funding_edges -> Cascade.run
-- and asserts the rejection lands on that stage. Column names, join shapes and
attach helpers are all under test as a consequence.

THE AUDIT THAT PRODUCED IT found stage 5 unreachable: funding_edges had a
writer, an aggregate query and a label derivation, but no read path into the
facts, so it would have reported `no_edges` forever on a full graph.
"""
from __future__ import annotations

import datetime as dt

import pytest

from trenches.db import repo
from trenches.decide import (
    Cascade,
    LabelSet,
    attach_first_buyers,
    attach_funding_edges,
    facts_from_row,
)

T0 = dt.datetime(2026, 9, 1, 12, 0, tzinfo=dt.UTC)
OLD = T0 - dt.timedelta(hours=6)          # past stage 4's cold-start floor


async def seed(db, mint: str, **token_fields) -> None:
    await repo.open_stream(db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(db, mint=mint, stream_id=1, feed="pumpportal",
                            fields=token_fields)


async def run_real_path(db, mint: str, *, labels=None):
    """Exactly what `trenches decide` does, nothing hand-fed."""
    rows = await repo.candidates_for_decision(db)
    row = next(r for r in rows if r["mint"] == mint)
    buyers = (await repo.first_buyers(db)).get(mint)
    edges = (await repo.funding_edges_for(db, [mint])).get(mint)
    facts = attach_funding_edges(
        attach_first_buyers(facts_from_row(row), buyers), edges
    )
    cascade = Cascade(labels=labels) if labels else Cascade()
    return cascade.run(facts)


# -- stage 0 ---------------------------------------------------------------

async def test_stage_0_is_reachable(any_db):
    await seed(any_db, "S0")
    _, trail = await run_real_path(any_db, "S0")
    assert trail[0].stage == 0 and trail[0].accept


# -- stage 1: structural facts come from token_structural ------------------

async def test_stage_1_rejects_from_a_stored_structural_row(any_db):
    await seed(any_db, "S1")
    await repo.record_structural(any_db, mint="S1", fields={
        "source": "goplus", "fetched_at": T0, "status_code": 200,
        "mintable": True, "fields_present": 1,
    })
    verdict, _ = await run_real_path(any_db, "S1")
    assert verdict.rejected and verdict.stage == 1
    assert "mint authority live" in verdict.reason


# -- stage 2: creator reputation comes from the creators cache -------------

async def test_stage_2_rejects_from_the_creator_cache(any_db):
    """The join is on tokens_seen.signer -> creators.address with role
    'signer'. Get either half wrong and stage 2 reads `unknown` forever."""
    await seed(any_db, "S2", signer="RUGGER")
    for _ in range(4):
        await repo.bump_creator(any_db, "RUGGER", "signer")
    await repo.apply_rug_counts(any_db, {"RUGGER": 4})

    verdict, _ = await run_real_path(any_db, "S2")
    assert verdict.rejected and verdict.stage == 2
    assert "rug rate" in verdict.reason


async def test_the_rugs_pipeline_is_what_makes_stage_2_fire(any_db):
    """Without apply_rug_counts the identical token passes. This is the whole
    reason `trenches rugs` exists, asserted rather than assumed."""
    await seed(any_db, "S2B", signer="UNKNOWN_DEV")
    for _ in range(4):
        await repo.bump_creator(any_db, "UNKNOWN_DEV", "signer")
    verdict, _ = await run_real_path(any_db, "S2B")
    assert verdict.accept


# -- stage 3: liquidity comes from the price path --------------------------

async def test_stage_3_rejects_from_the_newest_priced_observation(any_db):
    await seed(any_db, "S3")
    for minutes, liq in ((5, "90000"), (10, "150")):
        at = T0 + dt.timedelta(minutes=minutes)
        await repo.record_observation(any_db, mint="S3", fields={
            "observed_at": at, "scheduled_for": at, "source": "dexscreener",
            "status": "indexed", "liquidity_usd": liq, "age_seconds": 600,
        })
    verdict, _ = await run_real_path(any_db, "S3")
    assert verdict.rejected and verdict.stage == 3
    assert "$150" in verdict.reason, "must use the NEWEST reading, not the first"


async def test_stage_3_rejects_from_a_recorded_rug_event(any_db):
    await seed(any_db, "S3R")
    await repo.record_structural_event(any_db, mint="S3R", fields={
        "event_type": "rugged", "detected_at": T0, "observed_at": T0,
        "derived_from": "rugcheck_poll", "severity": "critical",
    })
    verdict, _ = await run_real_path(any_db, "S3R")
    assert verdict.rejected and verdict.stage == 3
    assert "rugged" in verdict.reason


# -- stage 4: NOT REACHABLE, and that is the finding -----------------------

async def test_stage_4_cannot_be_fed_from_the_database_at_all(any_db):
    """No column anywhere carries dev share, sniper/insider/bundler counts or a
    holder list, so stage 4 reads nothing no matter what is collected. Asserted
    so the day a supplier lands, this test fails and says so."""
    await seed(any_db, "S4", detected_at=OLD)
    _, trail = await run_real_path(any_db, "S4")
    stage4 = next(v for v in trail if v.stage == 4)
    assert stage4.accept
    assert stage4.inputs["concentration"] == "unfetched"


async def test_stage_4_does_reject_once_something_supplies_it(any_db):
    """The rule works; only the supplier is missing. Fed by hand HERE ONLY, to
    keep the two failures apart -- 'rule is broken' and 'nothing feeds it' are
    different bugs with different fixes."""
    await seed(any_db, "S4B")
    rows = await repo.candidates_for_decision(any_db)
    row = next(r for r in rows if r["mint"] == "S4B")
    facts = facts_from_row(row)
    facts.update({"detected_at": OLD, "snipers_total": 99, "dev_percentage": 40})
    verdict, _ = Cascade().run(facts)
    assert verdict.rejected and verdict.stage == 4


# -- stage 5: reachable only via the read path this audit added ------------

async def test_stage_5_rejects_from_stored_funding_edges(any_db):
    """THE REGRESSION TEST for the bug this file found. funding_edges had a
    writer and no reader, so a full graph still produced `no_edges`."""
    await seed(any_db, "S5", bonding_curve="CURVE")
    now = T0
    for i, trader in enumerate(("W0", "W1", "W2")):
        await repo.insert_trade_tick(any_db, mint="S5", signature=f"t{i}", fields={
            "observed_at": now + dt.timedelta(seconds=i), "is_buy": 1,
            "trader": trader, "source": "pumpportal",
        })
    await repo.record_funding_edges(any_db, mint="S5", source="test", edges=[
        ("W0", "W1"), ("W1", "W2"),
    ])
    labels = LabelSet(cex=frozenset({"SOME_EXCHANGE"}), source="test")

    rows = await repo.candidates_for_decision(any_db)
    row = next(r for r in rows if r["mint"] == "S5")
    buyers = (await repo.first_buyers(any_db)).get("S5")
    edges = (await repo.funding_edges_for(any_db, ["S5"])).get("S5")
    facts = attach_funding_edges(
        attach_first_buyers(facts_from_row(row), buyers), edges
    )
    facts["holdings"] = {"W0": 10.0, "W1": 10.0, "W2": 10.0}

    verdict, trail = Cascade(labels=labels).run(facts)
    stage5 = next(v for v in trail if v.stage == 5)
    assert stage5.inputs.get("clustering") != "no_edges", (
        "stored funding edges did not reach stage 5 -- the read path is broken"
    )
    assert verdict.rejected and verdict.stage == 5
    assert "30.0% of supply" in verdict.reason


async def test_stage_5_reads_first_buyers_as_its_address_set(any_db):
    await seed(any_db, "S5B")
    for i, trader in enumerate(("A", "B")):
        await repo.insert_trade_tick(any_db, mint="S5B", signature=f"b{i}", fields={
            "observed_at": T0 + dt.timedelta(seconds=i), "is_buy": 1,
            "trader": trader, "source": "pumpportal",
        })
    _, trail = await run_real_path(any_db, "S5B")
    stage5 = next(v for v in trail if v.stage == 5)
    assert stage5.inputs["n_addresses"] == 2


async def test_an_empty_funding_table_says_no_edges_not_unfetched(any_db):
    """Addresses but no edges is a different fact from nothing at all, and the
    journal has to tell them apart."""
    await seed(any_db, "S5C")
    await repo.insert_trade_tick(any_db, mint="S5C", signature="x", fields={
        "observed_at": T0, "is_buy": 1, "trader": "A", "source": "pumpportal"})
    _, trail = await run_real_path(any_db, "S5C")
    assert next(v for v in trail if v.stage == 5).inputs["clustering"] == "no_edges"


# -- cost ordering, on the real path ---------------------------------------

async def test_a_cheap_rejection_stops_the_expensive_stages_running(any_db):
    """3.4: ordering saves more money than caching. Asserted end to end rather
    than on a hand-built dict."""
    await seed(any_db, "SORD", signer="RUGGER2")
    await repo.record_structural(any_db, mint="SORD", fields={
        "source": "goplus", "fetched_at": T0, "freezable": True,
        "fields_present": 1})
    at = T0 + dt.timedelta(minutes=5)
    await repo.record_observation(any_db, mint="SORD", fields={
        "observed_at": at, "scheduled_for": at, "source": "dexscreener",
        "status": "indexed", "liquidity_usd": "10", "age_seconds": 300})

    verdict, trail = await run_real_path(any_db, "SORD")
    assert verdict.stage == 1, "the free stage must decide before the paid ones"
    assert [v.stage for v in trail] == [0, 1]


@pytest.mark.parametrize("mint,expected", [("W1", 1), ("W3", 3)])
async def test_every_stage_records_the_inputs_behind_its_verdict(
    any_db, mint, expected
):
    """3.9.3: a rejection is only training data if the inputs were kept."""
    await seed(any_db, mint)
    if expected == 1:
        await repo.record_structural(any_db, mint=mint, fields={
            "source": "goplus", "fetched_at": T0, "mintable": True,
            "fields_present": 1})
    else:
        at = T0 + dt.timedelta(minutes=5)
        await repo.record_observation(any_db, mint=mint, fields={
            "observed_at": at, "scheduled_for": at, "source": "dexscreener",
            "status": "indexed", "liquidity_usd": "5", "age_seconds": 300})

    verdict, trail = await run_real_path(any_db, mint)
    assert verdict.stage == expected
    assert verdict.inputs, "a rejection with no inputs cannot be calibrated later"
    assert all(isinstance(v.inputs, dict) for v in trail)
