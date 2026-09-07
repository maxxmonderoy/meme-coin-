"""The cost model and the exit ladder.

3.9.2 says an unhonest cost model makes the whole paper exercise theatre, so
these tests are mostly about the model being PESSIMISTIC in the right places
rather than merely correct in the arithmetic.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal as D

import pytest

from trenches.paper import fees as fee_mod
from trenches.paper.fees import FeeModel, round_trip_cost_pct
from trenches.paper.simulator import (
    MAX_SIZE_PCT,
    ExitPlan,
    Position,
    SizingError,
    Tick,
)

T0 = dt.datetime(2026, 9, 1, tzinfo=dt.UTC)


def tick(seconds: int, price: str | None, sig: str | None = None) -> Tick:
    return Tick(at=T0 + dt.timedelta(seconds=seconds),
                price_sol=D(price) if price is not None else None,
                signature=sig or f"sig{seconds}")


def position(size_pct: str = "1", **kw) -> Position:
    return Position(mint="M", bankroll_sol=D("10"), size_pct=D(size_pct),
                    plan=kw.pop("plan", ExitPlan()), **kw)


# -- the cost model --------------------------------------------------------

def test_entry_haircut_raises_the_price_paid_so_you_get_fewer_tokens():
    """The haircut must hurt the token count, not just the notional.

    Applying it to notional alone would understate the damage: the position also
    exits from a smaller token count.
    """
    model = FeeModel()
    fill = fee_mod.buy(model, D("0.001"), D("0.1"))
    assert fill.fill_price_sol > fill.quote_price_sol
    clean = fee_mod.buy(FeeModel(entry_slippage_pct=D(0)), D("0.001"), D("0.1"))
    assert fill.tokens < clean.tokens


def test_exit_haircut_lowers_the_price_received():
    model = FeeModel()
    fill = fee_mod.sell(model, D("0.001"), D("1000"))
    assert fill.fill_price_sol < fill.quote_price_sol


def test_distressed_exit_is_strictly_worse_than_a_calm_one():
    """A rug does not fill at the last printed price."""
    model = FeeModel()
    calm = fee_mod.sell(model, D("0.001"), D("1000"))
    panic = fee_mod.sell(model, D("0.001"), D("1000"), distressed=True)
    assert panic.net_sol < calm.net_sol


def test_explicit_friction_is_near_the_1_6_budget():
    """1.6 budgets ~4% explicit friction. Ours is ~3% because 3.6 routes through
    PumpPortal Local instead of paying a terminal's ~2%. Far below 2.5% would
    mean a fee went missing."""
    pct = round_trip_cost_pct(FeeModel(), D("0.1"), include_slippage=False)
    assert D("0.025") < pct < D("0.045")


def test_slippage_roughly_doubles_the_cost_of_a_round_trip():
    model = FeeModel()
    explicit = round_trip_cost_pct(model, D("0.1"), include_slippage=False)
    total = round_trip_cost_pct(model, D("0.1"))
    assert total > explicit * D("2")


def test_pessimistic_model_is_strictly_harsher():
    """3.9.6 wants net positive after PESSIMISTIC slippage, not just modelled."""
    base = round_trip_cost_pct(FeeModel(), D("0.1"))
    harsh = round_trip_cost_pct(FeeModel().pessimistic(), D("0.1"))
    assert harsh > base


def test_fees_are_itemised_not_netted():
    """1.10: fees logged separately from PnL."""
    fill = fee_mod.buy(FeeModel(), D("0.001"), D("0.1"))
    assert fill.fee_total_sol == (
        fill.fee_launchpad_sol + fill.fee_priority_sol + fill.fee_tip_sol
    )
    assert fill.fee_launchpad_sol > 0 and fill.fee_priority_sol > 0


def test_a_position_too_small_to_cover_fixed_costs_is_refused():
    """At 1.9 sizing the fixed SOL costs dominate; a trade that cannot pay them
    is uneconomic and must not be silently simulated as if it filled."""
    with pytest.raises(ValueError, match="fixed costs"):
        fee_mod.buy(FeeModel(), D("0.001"), D("0.0001"))


def test_no_floats_anywhere_in_the_amount_path():
    """3.10: BigInt/exact base units everywhere, no floats in any amount path."""
    fill = fee_mod.buy(FeeModel(), D("0.001"), D("0.1"))
    for value in (fill.fill_price_sol, fill.tokens, fill.gross_sol,
                  fill.fee_total_sol, fill.net_sol):
        assert isinstance(value, D)


# -- sizing ----------------------------------------------------------------

def test_sizing_hard_cap_is_enforced_in_code():
    """1.9: a trader with a +61% edge goes broke at 20% sizing."""
    with pytest.raises(SizingError, match="hard cap"):
        position(size_pct="20")
    with pytest.raises(SizingError):
        position(size_pct=str(MAX_SIZE_PCT + D("0.01")))


def test_two_percent_is_allowed_and_is_the_ceiling():
    assert position(size_pct="2").size_sol == D("0.2")


# -- the exit ladder -------------------------------------------------------

def test_stake_comes_off_at_2x_and_the_rest_rides():
    """1.10: original stake off at 2x, remainder rides as a free option."""
    p = position()
    p.open_at(tick(0, "0.000001"))
    produced = p.tick(tick(10, "0.0000025"))
    assert [k for k, _ in produced] == ["tp"]
    assert p.stake_recovered is True
    assert p.tokens_held > 0        # the remainder still rides
    assert p.is_open


def test_take_profit_recovers_the_cash_stake_not_a_token_fraction():
    """Recovering "half the tokens" is not recovering the stake once fees and
    the haircut are paid. The sale is sized on cash actually put in."""
    p = position()
    p.open_at(tick(0, "0.000001"))
    p.tick(tick(10, "0.0000025"))
    recovered = sum(f.net_sol for _, f, _ in p.exits)
    assert recovered >= p.size_sol * D("0.98")


def test_trailing_stop_fires_from_the_peak_not_from_entry():
    p = position(plan=ExitPlan(tp_multiple=D("99"), trail_pct=D("0.35")))
    p.open_at(tick(0, "0.000001"))
    p.tick(tick(10, "0.000002"))        # peak
    assert p.is_open
    p.tick(tick(20, "0.0000012"))       # -40% from peak, still above entry
    assert not p.is_open
    assert p.exit_reason == "trail"


def test_timeout_closes_a_position_that_goes_nowhere():
    p = position(plan=ExitPlan(timeout_seconds=60))
    p.open_at(tick(0, "0.000001"))
    produced = p.tick(tick(61, "0.000001"))
    assert [k for k, _ in produced] == ["timeout"]
    assert p.exit_reason == "timeout"


def test_timeout_beats_a_take_profit_reached_after_the_horizon():
    """A stale position must not book a profit it only reached after the point
    it was supposed to have exited."""
    p = position(plan=ExitPlan(timeout_seconds=60))
    p.open_at(tick(0, "0.000001"))
    produced = p.tick(tick(61, "0.00001"))   # 10x, but too late
    assert [k for k, _ in produced] == ["timeout"]


def test_a_flat_token_still_loses_money():
    """The friction is the point. An unchanged price is a losing trade."""
    p = position(plan=ExitPlan(timeout_seconds=60))
    p.open_at(tick(0, "0.000001"))
    p.tick(tick(61, "0.000001"))
    assert p.pnl_sol < 0
    assert p.pnl_pct < D("-5")


def test_a_rug_loses_more_than_the_stake():
    """Fees are still paid on the way out of something worth nothing."""
    p = position()
    p.open_at(tick(0, "0.000001"))
    p.close_rugged(T0 + dt.timedelta(seconds=120))
    assert p.pnl_sol < -p.size_sol
    assert p.fees_sol > 0


def test_a_3x_runner_nets_far_less_than_3x():
    """Sanity on the whole chain: the ladder exits on the trail, not the peak,
    and friction takes a bite on both sides."""
    p = position()
    p.open_at(tick(0, "0.000001"))
    for s, price in ((10, "0.000002"), (20, "0.000003"), (30, "0.0000018")):
        p.tick(tick(s, price))
    assert not p.is_open
    assert D("0") < p.pnl_pct < D("150")


def test_ticks_without_a_price_cannot_open_a_position():
    """Guessing an entry price is how a simulator invents an edge."""
    p = position()
    with pytest.raises(ValueError, match="no usable price"):
        p.open_at(tick(0, None))


def test_priceless_ticks_are_counted_but_do_not_move_the_ladder():
    p = position()
    p.open_at(tick(0, "0.000001"))
    before = p.peak_price
    assert p.tick(tick(5, None)) == []
    assert p.peak_price == before
    assert p.ticks_seen == 1


def test_pnl_and_fees_are_separable():
    p = position()
    p.open_at(tick(0, "0.000001"))
    p.tick(tick(10, "0.0000025"))
    p.tick(tick(20, "0.0000005"))
    assert p.fees_sol > 0
    assert p.pnl_sol == p.proceeds_sol - p.size_sol


# -- persistence and the runner -------------------------------------------

async def test_paper_tables_round_trip(any_db):
    """Both dialects, like every other storage test."""
    from trenches.db import repo

    assert await repo.insert_trade_tick(any_db, mint="M1", signature="S1", fields={
        "observed_at": T0, "is_buy": True, "sol_amount": "1.5",
        "token_amount": "30000000", "price_sol": "5E-8", "source": "pumpportal",
    }) is True
    # Dedupe on (mint, signature): the tape repeats on reconnect.
    assert await repo.insert_trade_tick(any_db, mint="M1", signature="S1",
                                        fields={"observed_at": T0}) is False
    rows = await repo.ticks_for_mint(any_db, "M1")
    assert len(rows) == 1 and rows[0]["price_sol"] == "5E-8"


async def test_runner_defers_entry_until_a_real_priced_tick(any_db):
    """There is no price at detection -- nobody has traded yet. Entering "at
    detection" would be entering at an invented number."""
    from trenches.db import repo
    from trenches.paper.runner import PaperRunner

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="M9", stream_id=1, feed="pumpportal", fields={})

    runner = PaperRunner(any_db, bankroll_sol=D("10"), size_pct=D("1"))
    runner.arm("M9")
    await runner.on_tick("M9", tick(0, None))       # unpriced: no entry
    assert "M9" not in runner.positions
    await runner.on_tick("M9", tick(1, "0.000001"))  # priced: entry
    assert runner.positions["M9"].is_open

    stored = await any_db.fetchrow("select * from paper_positions where mint = ?", "M9")
    assert stored["status"] == "open" and stored["mode"] == "PAPER"
    fills = await any_db.fetch("select * from paper_fills where position_id = ?",
                               stored["id"])
    assert [f["kind"] for f in fills] == ["entry"]


async def test_runner_settles_a_closed_position_to_the_journal(any_db):
    from trenches.db import repo
    from trenches.paper.runner import PaperRunner
    from trenches.paper.simulator import ExitPlan

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="M8", stream_id=1, feed="pumpportal", fields={})

    runner = PaperRunner(any_db, bankroll_sol=D("10"), size_pct=D("1"),
                         plan=ExitPlan(tp_multiple=D("99"), trail_pct=D("0.35")))
    runner.arm("M8")
    await runner.on_tick("M8", tick(0, "0.000001"))
    await runner.on_tick("M8", tick(10, "0.000002"))
    await runner.on_tick("M8", tick(20, "0.0000011"))

    row = await any_db.fetchrow("select * from paper_positions where mint = ?", "M8")
    assert row["status"] == "closed" and row["exit_reason"] == "trail"
    assert D(row["fees_sol"]) > 0
    assert row["pnl_sol"] is not None


async def test_sweep_closes_a_position_whose_token_stopped_trading(any_db):
    """A dead token stops producing ticks, which is exactly when the position
    most needs closing. Without the sweep a rug leaves an open bag forever --
    the failure 3.1 separates the exit loop to prevent."""
    from trenches.db import repo
    from trenches.paper.runner import PaperRunner
    from trenches.paper.simulator import ExitPlan

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="M7", stream_id=1, feed="pumpportal", fields={})

    runner = PaperRunner(any_db, bankroll_sol=D("10"), size_pct=D("1"),
                         plan=ExitPlan(timeout_seconds=60))
    runner.arm("M7")
    await runner.on_tick("M7", tick(0, "0.000001"))
    assert await runner.sweep_timeouts(now=T0 + dt.timedelta(seconds=30)) == 0
    assert await runner.sweep_timeouts(now=T0 + dt.timedelta(seconds=120)) == 1

    row = await any_db.fetchrow("select * from paper_positions where mint = ?", "M7")
    assert row["status"] == "closed" and row["exit_reason"] == "timeout"
    assert "M7" not in runner.positions


async def test_opening_a_position_subscribes_to_that_mints_trades(any_db):
    """On the SHARED socket -- 3.2 permits exactly one PumpPortal connection."""
    from trenches.db import repo
    from trenches.paper.runner import PaperRunner
    from trenches.stream.pumpportal import PumpPortalConsumer

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="M6", stream_id=1, feed="pumpportal", fields={})

    consumer = PumpPortalConsumer()
    runner = PaperRunner(any_db, bankroll_sol=D("10"), size_pct=D("1"), tracker=consumer)
    runner.arm("M6")
    assert "M6" in consumer.tracked
    await runner.on_tick("M6", tick(0, "0.000001"))
    await runner.on_tick("M6", tick(10, "0.0000005"))
    assert "M6" not in consumer.tracked     # untracked once closed


async def test_summary_reports_the_3_9_6_concentration_check(any_db):
    """No single trade above 20% of simulated profit."""
    from trenches.db import repo

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    for i, pnl in enumerate(["1.0", "0.1", "0.1", "-0.2"]):
        mint = f"C{i}"
        await repo.upsert_token(any_db, mint=mint, stream_id=1, feed="f", fields={})
        pid = await repo.open_paper_position(any_db, mint=mint, fields={
            "mode": "PAPER", "opened_at": T0, "bankroll_sol": "10", "size_pct": D("1"),
            "size_sol": "0.1", "plan_tp_multiple": D("2"), "plan_trail_pct": D("0.35"),
            "plan_timeout_s": 1800, "code_version": "v", "params": {},
        })
        await repo.close_paper_position(any_db, pid, {
            "closed_at": T0, "exit_reason": "tp", "realised_sol": "0",
            "fees_sol": "0.004", "pnl_sol": pnl, "pnl_pct": 0.0, "ticks_seen": 1,
        })
    s = await repo.paper_summary(any_db)
    assert int(s["closed"]) == 4
    # 1.0 of 1.2 gross profit is 83%: one trade carrying the whole result.
    assert s["largest_win_share"] > 0.20


# -- stage 1 structural fetch ---------------------------------------------

def test_extract_keeps_absent_and_false_distinct():
    """Part 2's core distinction: a field GoPlus omitted is not a clean field.
    Defaulting an absent flag to False would turn "we do not know" into "safe"
    -- the most expensive mistake available here."""
    from trenches.enrich.structural import extract

    out = extract({"mintable": "0", "freezable": {"status": "1"}})
    assert out["mintable"] is False          # present and clean
    assert out["freezable"] is True          # present and dangerous
    assert out["closable"] is None           # ABSENT, not clean
    assert out["fields_present"] == 2


def test_extract_reads_vendor_boolean_variants():
    from trenches.enrich.structural import extract

    assert extract({"mintable": 1})["mintable"] is True
    assert extract({"mintable": "true"})["mintable"] is True
    assert extract({"mintable": "0"})["mintable"] is False


def test_extract_reports_nothing_present_for_an_empty_payload():
    from trenches.enrich.structural import extract

    out = extract({})
    assert out["fields_present"] == 0
    assert all(out[f] is None for f in ("mintable", "freezable", "closable"))


async def test_structural_row_round_trips_and_reaches_the_cascade(any_db):
    """The join is what lets stage 1 stop saying `unfetched`."""
    from trenches.db import repo
    from trenches.decide import Cascade

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MS", stream_id=1, feed="f", fields={})
    await repo.record_structural(any_db, mint="MS", fields={
        "source": "goplus", "fetched_at": T0, "status_code": 200,
        "mintable": False, "freezable": True, "fields_present": 2,
    })
    row, = [r for r in await repo.candidates_for_decision(any_db) if r["mint"] == "MS"]
    assert row["freezable"] == 1

    _, trail = Cascade().run(dict(row))
    stage1 = next(v for v in trail if v.stage == 1)
    assert stage1.rejected
    assert "freeze authority live" in (stage1.reason or "")


async def test_a_mint_with_no_probe_still_reports_unfetched(any_db):
    """Absence must not silently become a pass."""
    from trenches.db import repo
    from trenches.decide import Cascade

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MU", stream_id=1, feed="f", fields={})
    row, = [r for r in await repo.candidates_for_decision(any_db) if r["mint"] == "MU"]

    _, trail = Cascade().run(dict(row))
    stage1 = next(v for v in trail if v.stage == 1)
    assert stage1.accept
    assert stage1.inputs.get("structural") == "unfetched"


async def test_stage_3_reads_the_latest_stored_liquidity_observation(any_db):
    """Stage 3 spends no request: it reads the price path the sampler already
    collected. The subquery is correlated rather than a window function so the
    same SQL text runs on both dialects -- which is what this asserts."""
    import datetime as dt

    from trenches.db import repo
    from trenches.decide import Cascade

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="ML", stream_id=1, feed="f", fields={})
    for minutes, liquidity in ((5, "90000"), (10, "120"), (12, None)):
        at = T0 + dt.timedelta(minutes=minutes)
        await repo.record_observation(any_db, mint="ML", fields={
            "observed_at": at, "scheduled_for": at, "source": "dexscreener",
            "status": "indexed", "liquidity_usd": liquidity, "age_seconds": 60,
        })

    row, = [r for r in await repo.candidates_for_decision(any_db) if r["mint"] == "ML"]
    # The newest row with a liquidity value wins; the later null does not erase it.
    assert str(row["liquidity_usd"]) == "120"
    assert row["liquidity_observed_at"] is not None

    _, trail = Cascade().run(dict(row))
    stage3 = next(v for v in trail if v.stage == 3)
    assert stage3.rejected
    assert "below the" in (stage3.reason or "")
    assert stage3.inputs["liquidity_age_seconds"] > 0


async def test_a_recorded_rug_event_rejects_at_stage_3(any_db):
    """The cheapest rejection there is, and it comes from a table we already
    fill -- no extra call."""
    from trenches.db import repo
    from trenches.decide import Cascade

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MR", stream_id=1, feed="f", fields={})
    await repo.record_structural_event(any_db, mint="MR", fields={
        "event_type": "rugged", "detected_at": T0, "observed_at": T0,
        "derived_from": "rugcheck_poll", "severity": "critical",
    })

    row, = [r for r in await repo.candidates_for_decision(any_db) if r["mint"] == "MR"]
    verdict, _ = Cascade().run(dict(row))
    assert verdict.rejected and verdict.stage == 3
    assert "rugged" in (verdict.reason or "")


async def test_a_mint_with_no_observations_reaches_stage_3_as_unfetched(any_db):
    """Nothing collected must not read as three checks passed."""
    from trenches.db import repo
    from trenches.decide import Cascade

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MN", stream_id=1, feed="f", fields={})
    row, = [r for r in await repo.candidates_for_decision(any_db) if r["mint"] == "MN"]

    _, trail = Cascade().run(dict(row))
    stage3 = next(v for v in trail if v.stage == 3)
    assert stage3.accept
    assert stage3.inputs == {"liquidity": "unfetched"}


async def test_stage_4_gets_the_curve_and_pool_addresses_to_exclude(any_db):
    """3.4: compute top-10 excluding pool, bonding-curve and locker addresses
    'or you'll reject on the curve itself'. Two of the three come from here."""
    import datetime as dt

    from trenches.db import repo
    from trenches.decide import facts_from_row

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MX", stream_id=1, feed="f",
                            fields={"bonding_curve": "CURVE1"})
    at = T0 + dt.timedelta(minutes=5)
    await repo.record_observation(any_db, mint="MX", fields={
        "observed_at": at, "scheduled_for": at, "source": "dexscreener",
        "status": "indexed", "pair_address": "POOL1", "age_seconds": 300})

    row, = [r for r in await repo.candidates_for_decision(any_db) if r["mint"] == "MX"]
    built = facts_from_row(row)
    assert built["excluded_addresses"] == ["CURVE1", "POOL1"]
    assert built["detected_at"] is not None


async def test_stage_4_journals_unfetched_because_nothing_supplies_it(any_db):
    """The honest state of stage 4 today, asserted so it cannot silently drift
    into looking like a stage that ran."""
    from trenches.db import repo
    from trenches.decide import Cascade

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MY", stream_id=1, feed="f", fields={})
    row, = [r for r in await repo.candidates_for_decision(any_db) if r["mint"] == "MY"]

    _, trail = Cascade().run(dict(row))
    stage4 = next(v for v in trail if v.stage == 4)
    assert stage4.accept
    assert stage4.inputs["concentration"] == "unfetched"


async def test_first_buyers_are_free_and_ordered_and_deduplicated(any_db):
    """Stage 5's address set, half of it, from data already collected. The
    window function has to run on both dialects, which is what any_db checks."""
    import datetime as dt

    from trenches.db import repo

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MB", stream_id=1, feed="f", fields={})
    # W1 buys twice; a sell must not make a buyer; nulls must not become one.
    ticks = [
        ("s1", "W1", 1, 1), ("s2", "W2", 1, 2), ("s3", "W1", 1, 3),
        ("s4", "W3", 0, 4), ("s5", None, 1, 5), ("s6", "W4", 1, 6),
    ]
    for sig, trader, is_buy, minute in ticks:
        await repo.insert_trade_tick(any_db, mint="MB", signature=sig, fields={
            "observed_at": T0 + dt.timedelta(minutes=minute),
            "is_buy": is_buy, "trader": trader, "source": "pumpportal"})

    buyers = await repo.first_buyers(any_db, per_mint=10)
    assert buyers["MB"] == ["W1", "W2", "W4"]


async def test_first_buyers_respects_the_per_mint_cap(any_db):
    import datetime as dt

    from trenches.db import repo

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MC", stream_id=1, feed="f", fields={})
    for i in range(10):
        await repo.insert_trade_tick(any_db, mint="MC", signature=f"s{i}", fields={
            "observed_at": T0 + dt.timedelta(seconds=i), "is_buy": 1,
            "trader": f"W{i}", "source": "pumpportal"})

    assert await repo.first_buyers(any_db, per_mint=3) == {"MC": ["W0", "W1", "W2"]}


async def test_stage_5_journals_unfetched_when_no_buyer_was_observed(any_db):
    """The honest state of stage 5 today: the funder trace needs RPC and there
    is no RPC client here, so even with an address set there are no edges."""
    from trenches.db import repo
    from trenches.decide import Cascade, attach_first_buyers, facts_from_row

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MZ", stream_id=1, feed="f", fields={})
    row, = [r for r in await repo.candidates_for_decision(any_db) if r["mint"] == "MZ"]
    buyers = await repo.first_buyers(any_db)

    facts = attach_first_buyers(facts_from_row(row), buyers.get("MZ"))
    _, trail = Cascade().run(facts)
    stage5 = next(v for v in trail if v.stage == 5)
    assert stage5.accept
    assert stage5.inputs["clustering"] == "unfetched"


async def test_coverage_counts_asked_separately_from_answered(any_db):
    """"Asked and got nothing" and "never asked" are different facts."""
    from trenches.db import repo

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    for mint in ("A", "B", "C"):
        await repo.upsert_token(any_db, mint=mint, stream_id=1, feed="f", fields={})
    await repo.record_structural(any_db, mint="A", fields={
        "source": "goplus", "fetched_at": T0, "mintable": True, "fields_present": 1})
    await repo.record_structural(any_db, mint="B", fields={
        "source": "goplus", "fetched_at": T0, "fields_present": 0, "error": "timeout"})

    cov = await repo.structural_coverage(any_db)
    assert cov["tokens"] == 3
    assert cov["probed"] == 2        # asked twice
    assert cov["with_fields"] == 1   # answered once
    assert cov["would_reject"] == 1


# -- the live wiring -------------------------------------------------------

async def _ingest(db, **kw):
    """An Ingest wired the way `trenches stream --paper` wires it.

    Config comes from `from_env` rather than being hand-constructed: a literal
    field list here goes stale every time Config gains a setting, and the
    failure looks like a product bug rather than a stale test.
    """
    import os

    from trenches.config import Config
    from trenches.decide import Cascade
    from trenches.paper.runner import PaperRunner
    from trenches.pipeline.worker import Ingest

    os.environ.setdefault("TRENCHES_DSN", "sqlite://:memory:")
    os.environ.setdefault("TRENCHES_FEEDS", "pumpportal")
    cfg = Config.from_env()
    runner = PaperRunner(db, bankroll_sol=D("10"), size_pct=D("1"))
    return Ingest(db, cfg, 1, paper=runner, cascade=Cascade()), runner


async def test_a_trade_event_is_stored_as_a_tick(any_db):
    """The live loop dropped trade frames entirely before this."""
    from trenches.db import repo
    from trenches.stream.base import EventKind, RawEvent

    await repo.open_stream(any_db, feeds=["pumpportal"], subscription={}, code_version="v")
    ingest, _ = await _ingest(any_db)
    await ingest._handle_trade(RawEvent(
        provider="pumpportal", event_kind=EventKind.TRADE, mint="MT", signature="TS1",
        payload={"raw": {"solAmount": 1.5, "tokenAmount": 30000000,
                         "traderPublicKey": "T", "pool": "pump"},
                 "price_sol": "5E-8", "is_buy": True},
    ))
    rows = await repo.ticks_for_mint(any_db, "MT")
    assert len(rows) == 1
    assert rows[0]["price_sol"] == "5E-8"
    assert rows[0]["pool"] == "pump"
    assert ingest.ticks_stored == 1


async def test_an_accepted_candidate_is_journalled_and_armed(any_db):
    """Accept must reach the paper runner. Before this, `arm` was never called
    anywhere outside the replay command."""
    from trenches.db import repo

    await repo.open_stream(any_db, feeds=["pumpportal"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MA", stream_id=1, feed="pumpportal", fields={})
    ingest, runner = await _ingest(any_db)

    await ingest._decide("MA", {"signer": "S1"})
    row = await any_db.fetchrow("select * from decisions where mint = ?", "MA")
    assert row["outcome"] == "accept" and row["mode"] == "PAPER"
    assert "MA" in runner.pending
    assert ingest.armed == 1


async def test_a_rejected_candidate_is_journalled_and_not_armed(any_db):
    """3.9.3: rejections are recorded. And they must not open a position."""
    from trenches.db import repo

    await repo.open_stream(any_db, feeds=["pumpportal"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MR", stream_id=1, feed="pumpportal", fields={})
    await repo.record_structural(any_db, mint="MR", fields={
        "source": "goplus", "fetched_at": T0, "freezable": True, "fields_present": 1})
    ingest, runner = await _ingest(any_db)

    await ingest._decide("MR", {"signer": "S1", "freezable": 1})
    row = await any_db.fetchrow("select * from decisions where mint = ?", "MR")
    assert row["outcome"] == "reject" and row["reject_stage"] == 1
    assert "MR" not in runner.pending
    assert ingest.armed == 0


async def test_decisions_at_detection_record_structural_as_unfetched(any_db):
    """At t=0 nothing has been fetched for a brand new mint, and the journal
    must say so rather than imply a clean pass (Part 2)."""
    from trenches.db import repo

    await repo.open_stream(any_db, feeds=["pumpportal"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MN", stream_id=1, feed="pumpportal", fields={})
    ingest, _ = await _ingest(any_db)
    await ingest._decide("MN", {"signer": "S1"})

    row = await any_db.fetchrow("select inputs from decisions where mint = ?", "MN")
    assert "unfetched" in row["inputs"]


# -- the separate exit loop (3.1) -----------------------------------------

async def test_exit_loop_rebuilds_a_position_from_the_journal(any_db):
    """3.1 wants the exit loop in its own process, so state must come from rows
    rather than from memory shared with the entry side."""
    from trenches.db import repo
    from trenches.paper import exits as exit_mod
    from trenches.paper.runner import PaperRunner

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MX", stream_id=1, feed="f", fields={})
    runner = PaperRunner(any_db, bankroll_sol=D("10"), size_pct=D("1"))
    runner.arm("MX")
    await runner.on_tick("MX", tick(0, "0.000001"))
    await repo.insert_trade_tick(any_db, mint="MX", signature="t0", fields={
        "observed_at": T0, "price_sol": "0.000001"})
    await repo.insert_trade_tick(any_db, mint="MX", signature="t1", fields={
        "observed_at": T0 + dt.timedelta(seconds=10), "price_sol": "0.000002"})

    row = await any_db.fetchrow("select * from paper_positions where mint = ?", "MX")
    rebuilt = await exit_mod.rebuild(any_db, row)
    assert rebuilt is not None
    assert rebuilt.tokens_held > 0
    assert rebuilt.peak_price == D("0.000002")     # recomputed from the tape


async def test_exit_loop_closes_a_bag_the_entry_side_abandoned(any_db):
    """The 3.1 failure: an entry-side crash leaves an open position. A second
    process must be able to close it with no shared memory at all."""
    from trenches.db import repo
    from trenches.paper import exits as exit_mod
    from trenches.paper.runner import PaperRunner
    from trenches.paper.simulator import ExitPlan

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MZ", stream_id=1, feed="f", fields={})
    runner = PaperRunner(any_db, bankroll_sol=D("10"), size_pct=D("1"),
                         plan=ExitPlan(timeout_seconds=60))
    runner.arm("MZ")
    await runner.on_tick("MZ", tick(0, "0.000001"))
    del runner        # the entry side is gone; nothing is in memory

    report = await exit_mod.sweep(any_db, now=T0 + dt.timedelta(seconds=300))
    assert report["closed"] == 1
    row = await any_db.fetchrow("select * from paper_positions where mint = ?", "MZ")
    assert row["status"] == "closed" and row["exit_reason"] == "timeout"


async def test_exit_loop_applies_ticks_the_entry_side_never_saw(any_db):
    """Ticks that arrived while the entry side was down still move the ladder."""
    from trenches.db import repo
    from trenches.paper import exits as exit_mod
    from trenches.paper.runner import PaperRunner
    from trenches.paper.simulator import ExitPlan

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MY", stream_id=1, feed="f", fields={})
    runner = PaperRunner(any_db, bankroll_sol=D("10"), size_pct=D("1"),
                         plan=ExitPlan(tp_multiple=D("99"), trail_pct=D("0.35"),
                                       timeout_seconds=99999))
    runner.arm("MY")
    await runner.on_tick("MY", tick(0, "0.000001"))
    del runner
    # Peak then collapse, recorded only on the tape.
    await repo.insert_trade_tick(any_db, mint="MY", signature="a", fields={
        "observed_at": T0 + dt.timedelta(seconds=10), "price_sol": "0.000002"})
    await repo.insert_trade_tick(any_db, mint="MY", signature="b", fields={
        "observed_at": T0 + dt.timedelta(seconds=20), "price_sol": "0.0000011"})

    report = await exit_mod.sweep(any_db, now=T0 + dt.timedelta(seconds=30))
    assert report["closed"] == 1
    row = await any_db.fetchrow("select * from paper_positions where mint = ?", "MY")
    assert row["exit_reason"] == "trail"


async def test_exit_loop_is_idempotent_across_runs(any_db):
    """It runs on a timer; a second pass must not re-close or double-fill."""
    from trenches.db import repo
    from trenches.paper import exits as exit_mod
    from trenches.paper.runner import PaperRunner
    from trenches.paper.simulator import ExitPlan

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MI", stream_id=1, feed="f", fields={})
    runner = PaperRunner(any_db, bankroll_sol=D("10"), size_pct=D("1"),
                         plan=ExitPlan(timeout_seconds=60))
    runner.arm("MI")
    await runner.on_tick("MI", tick(0, "0.000001"))
    del runner

    first = await exit_mod.sweep(any_db, now=T0 + dt.timedelta(seconds=300))
    second = await exit_mod.sweep(any_db, now=T0 + dt.timedelta(seconds=600))
    assert first["closed"] == 1
    assert second["open"] == 0 and second["closed"] == 0
    fills = await any_db.fetch("select kind from paper_fills")
    assert sorted(f["kind"] for f in fills) == ["entry", "timeout"]
