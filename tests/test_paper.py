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
