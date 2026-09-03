"""The exit engine: impact, the lookahead guard, and hand-checked replays.

Two of these decide whether the whole thing produces evidence or fiction: a
backtest that fills at the observed price is the lie that makes every memecoin
strategy look profitable, and a rule that can peek at the future simply looks
brilliant.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal as D

import pytest

from trenches.exits import impact as impact_mod
from trenches.exits.cursor import Event, LookaheadError, Observation, PathCursor
from trenches.exits.engine import ExitEngine
from trenches.exits.rules import (
    PriceStop,
    Ruleset,
    StructuralStop,
    TakeProfitLadder,
    TimeStop,
    TrailingStop,
    library,
)

T0 = dt.datetime(2026, 9, 1, tzinfo=dt.UTC)


def obs(seconds, price, liquidity="1000000", status="indexed"):
    return Observation(
        at=T0 + dt.timedelta(seconds=seconds), status=status,
        price_usd=D(price) if price is not None else None,
        liquidity_usd=D(liquidity) if liquidity is not None else None,
        age_seconds=seconds,
    )


def evt(seconds, event_type, severity="exit"):
    return Event(at=T0 + dt.timedelta(seconds=seconds), event_type=event_type,
                 severity=severity)


# -- price impact ----------------------------------------------------------

def test_a_tiny_position_in_a_deep_pool_fills_near_quote():
    out = impact_mod.sell_into(D("10"), D("1000000"))
    assert out.filled_fraction == 1
    assert out.impact_pct < D("0.001")


def test_a_position_larger_than_the_pool_does_not_fill_at_quote():
    """The lie this exists to prevent."""
    out = impact_mod.sell_into(D("5000"), D("2000"))
    assert out.filled_fraction < 1
    assert out.impact_pct > D("0.5")
    assert "partial" in out.reason


def test_no_liquidity_means_no_fill_not_a_free_exit():
    out = impact_mod.sell_into(D("500"), D("0"))
    assert out.filled is False
    assert out.filled_value == 0


def test_impact_grows_monotonically_as_the_pool_thins():
    impacts = [impact_mod.sell_into(D("500"), D(liq)).impact_pct
               for liq in ("1000000", "100000", "10000", "1000")]
    assert impacts == sorted(impacts)


def test_depth_is_half_of_reported_liquidity():
    """DexScreener reports TOTAL pool liquidity, not per-side reserves."""
    assert impact_mod.quote_depth(D("1000")) == D("500")


# -- the lookahead guard ---------------------------------------------------

def test_reading_a_future_observation_raises():
    path = [obs(0, "1"), obs(30, "2"), obs(60, "3")]
    cursor = PathCursor(path, [], T0 + dt.timedelta(seconds=30), path[0])
    assert cursor.at(1).price_usd == D("2")
    with pytest.raises(LookaheadError):
        cursor.at(2)


def test_peak_is_the_peak_seen_so_far_not_the_path_maximum():
    """The classic way a trailing-stop backtest cheats."""
    path = [obs(0, "1"), obs(30, "2"), obs(60, "9")]
    cursor = PathCursor(path, [], T0 + dt.timedelta(seconds=30), path[0])
    assert cursor.peak_price() == D("2")


def test_history_and_events_are_both_truncated_at_now():
    path = [obs(0, "1"), obs(60, "2")]
    events = [evt(10, "rugged"), evt(120, "pool_disappeared")]
    cursor = PathCursor(path, events, T0 + dt.timedelta(seconds=60), path[0])
    assert len(cursor.history()) == 2
    assert [e.event_type for e in cursor.events_so_far()] == ["rugged"]


def test_not_yet_indexed_is_not_tradeable():
    """A vendor that has not indexed a token has not priced it at zero."""
    assert obs(0, None, status="not_yet_indexed").tradeable is False
    assert obs(0, "1").tradeable is True


# -- hand-computed replays -------------------------------------------------

def test_trailing_stop_matches_a_hand_computed_result():
    """Peak 3.0, 35% trail fires at <= 1.95, so the 1.9 observation triggers."""
    engine = ExitEngine()
    path = [obs(0, "1.0"), obs(30, "2.0"), obs(60, "3.0"), obs(90, "1.9")]
    out = engine.replay(mint="M", cohort="control",
                        rule=Ruleset(name="t", rules=(TrailingStop(),)),
                        observations=path, events=[], size_usd=D("100"))
    assert len(out.legs) == 1
    assert out.legs[0].quote_price == D("1.9")
    assert out.exit_reason == "trail"
    assert out.peak_seen == D("3.0")


def test_the_same_path_in_a_thin_pool_returns_far_less():
    """The point of the change: the price path alone says 1.9x; the pool says
    otherwise."""
    engine = ExitEngine()
    deep = [obs(0, "1.0", "500000"), obs(30, "2.0", "500000"),
            obs(60, "3.0", "500000"), obs(90, "1.9", "500000")]
    thin = [obs(0, "1.0", "300"), obs(30, "2.0", "300"),
            obs(60, "3.0", "300"), obs(90, "1.9", "300")]
    rule = Ruleset(name="t", rules=(TrailingStop(),))
    rich = engine.replay(mint="M", cohort="c", rule=rule, observations=deep,
                         events=[], size_usd=D("500"))
    poor = engine.replay(mint="M", cohort="c", rule=rule, observations=thin,
                         events=[], size_usd=D("500"))
    assert rich.return_multiple > D("1.5")
    assert poor.return_multiple < D("0.5")
    assert poor.unfilled_fraction > 0        # stranded, not silently sold


def test_take_profit_then_trail_produces_two_legs():
    engine = ExitEngine()
    path = [obs(0, "1.0"), obs(30, "2.1"), obs(60, "3.0"), obs(90, "1.8")]
    out = engine.replay(mint="M", cohort="c",
                        rule=Ruleset(name="lt", rules=(TakeProfitLadder(), TrailingStop())),
                        observations=path, events=[], size_usd=D("100"))
    assert [leg.kind for leg in out.legs] == ["tp", "trail"]


def test_time_stop_fires_regardless_of_price():
    engine = ExitEngine()
    path = [obs(0, "1.0"), obs(60, "1.0"), obs(2000, "1.0")]
    out = engine.replay(mint="M", cohort="c",
                        rule=Ruleset(name="ts", rules=(TimeStop(seconds=600),)),
                        observations=path, events=[], size_usd=D("100"))
    assert out.exit_reason == "timeout"
    assert out.legs[0].at == T0 + dt.timedelta(seconds=2000)


def test_structural_stop_exits_on_an_event():
    engine = ExitEngine()
    path = [obs(0, "1.0"), obs(30, "1.0"), obs(60, "1.0")]
    out = engine.replay(mint="M", cohort="c",
                        rule=Ruleset(name="ss", rules=(StructuralStop(),)),
                        observations=path, events=[evt(30, "liquidity_collapse")],
                        size_usd=D("100"))
    assert out.exit_reason == "structural"
    assert out.legs[0].at == T0 + dt.timedelta(seconds=30)


def test_precedence_is_explicit_and_changes_the_outcome():
    """Order is data, not an accident of evaluation."""
    engine = ExitEngine()
    path = [obs(0, "1.0"), obs(30, "0.5")]
    events = [evt(30, "liquidity_collapse")]
    structural_first = engine.replay(
        mint="M", cohort="c",
        rule=Ruleset(name="a", rules=(StructuralStop(), PriceStop())),
        observations=path, events=events, size_usd=D("100"))
    price_first = engine.replay(
        mint="M", cohort="c",
        rule=Ruleset(name="b", rules=(PriceStop(), StructuralStop())),
        observations=path, events=events, size_usd=D("100"))
    assert structural_first.exit_reason == "structural"
    assert price_first.exit_reason == "stop"


def test_a_mint_that_was_never_indexed_is_skipped_not_counted_as_a_loss():
    """An entry that could not have been priced never happened."""
    engine = ExitEngine()
    path = [obs(0, None, status="not_yet_indexed"), obs(60, None, status="not_yet_indexed")]
    out = engine.replay(mint="M", cohort="c", rule=library()["ladder_trail"],
                        observations=path, events=[], size_usd=D("100"))
    assert out.skipped == "no tradeable observation"
    assert out.legs == []


def test_fees_are_charged_on_every_leg():
    """3.9.2: an exit rule that only wins before fees has not won."""
    engine = ExitEngine()
    path = [obs(0, "1.0"), obs(30, "5.0"), obs(60, "2.0")]
    out = engine.replay(mint="M", cohort="c",
                        rule=Ruleset(name="lt", rules=(TakeProfitLadder(), TrailingStop())),
                        observations=path, events=[], size_usd=D("100"))
    assert out.fees > 0
    assert all(leg.fees_sol > 0 for leg in out.legs)


def test_a_flat_path_loses_money_after_costs():
    engine = ExitEngine()
    path = [obs(0, "1.0"), obs(60, "1.0"), obs(2000, "1.0")]
    out = engine.replay(mint="M", cohort="c",
                        rule=Ruleset(name="ts", rules=(TimeStop(seconds=600),)),
                        observations=path, events=[], size_usd=D("100"))
    assert out.return_multiple < 1


# -- reporting -------------------------------------------------------------

def test_summary_reports_median_and_mean_together():
    """1.9: mean x0.46 against median x0.036. Reporting one alone misleads."""
    from trenches.exits.report import summarise

    engine = ExitEngine()
    rule = Ruleset(name="ts", rules=(TimeStop(seconds=60),))
    replays = []
    for mult in ("0.1", "0.1", "0.1", "50.0"):     # three zeros and one moonshot
        path = [obs(0, "1.0"), obs(120, mult)]
        replays.append(engine.replay(mint=f"M{mult}", cohort="control", rule=rule,
                                     observations=path, events=[], size_usd=D("100")))
    s = summarise(replays, rule="ts", cohort="control")
    assert s.n == 4
    assert s.mean_return > s.median_return * 5     # the divergence is the point


def test_timing_comparison_reports_which_fired_first():
    from trenches.exits.report import compare_timing

    engine = ExitEngine()
    path = [obs(0, "1.0"), obs(30, "1.0"), obs(60, "0.5")]
    events = [evt(30, "liquidity_collapse")]
    structural = {"M": engine.replay(
        mint="M", cohort="c", rule=Ruleset(name="s", rules=(StructuralStop(),)),
        observations=path, events=events, size_usd=D("100"))}
    price = {"M": engine.replay(
        mint="M", cohort="c", rule=Ruleset(name="p", rules=(PriceStop(),)),
        observations=path, events=events, size_usd=D("100"))}
    cmp_ = compare_timing(structural, price)
    assert cmp_.mints_compared == 1
    assert cmp_.structural_first == 1
    assert cmp_.median_lead_seconds == 30


# -- compaction ------------------------------------------------------------

def _row(seconds, price):
    return {"observed_at": (T0 + dt.timedelta(seconds=seconds)).isoformat(),
            "price_usd": price, "compacted": 0}


def test_compaction_keeps_every_extreme():
    """A trailing stop is a function of peaks and the falls from them, so
    dropping an extreme is the one thing that would change a replayed result."""
    from trenches.sample.compaction import select_survivors

    rows = [_row(i * 30, str(p)) for i, p in enumerate(
        [1.0, 1.2, 3.0, 2.0, 1.1, 0.4, 0.9, 1.0])]
    kept = select_survivors(rows, kept_interval_seconds=300)
    prices = {r["price_usd"] for r in kept}
    assert "3.0" in prices        # the peak
    assert "0.4" in prices        # the trough
    assert kept[0] == rows[0] and kept[-1] == rows[-1]


def test_compaction_actually_removes_rows():
    from trenches.sample.compaction import select_survivors

    rows = [_row(i * 30, "1.0") for i in range(60)]     # flat, 30 minutes
    kept = select_survivors(rows, kept_interval_seconds=300)
    assert len(kept) < len(rows)


def test_a_trailing_stop_replays_the_same_after_compaction():
    """The stated tolerance: identical, because extremes are preserved."""
    from trenches.sample.compaction import select_survivors

    raw = [_row(i * 30, str(p)) for i, p in enumerate(
        [1.0, 1.4, 2.2, 3.0, 2.6, 1.9, 1.5, 1.2])]
    kept = select_survivors(raw, kept_interval_seconds=300)

    def replay(rows):
        engine = ExitEngine()
        obs = [Observation(at=dt.datetime.fromisoformat(r["observed_at"]),
                           status="indexed", price_usd=D(r["price_usd"]),
                           liquidity_usd=D("1000000"), age_seconds=0) for r in rows]
        return engine.replay(mint="M", cohort="c",
                             rule=Ruleset(name="t", rules=(TrailingStop(),)),
                             observations=obs, events=[], size_usd=D("100"))

    full, compacted = replay(raw), replay(kept)
    assert full.exit_reason == compacted.exit_reason
    assert full.legs[0].quote_price == compacted.legs[0].quote_price
    assert abs(full.return_multiple - compacted.return_multiple) < D("0.001")


async def test_compaction_records_what_it_did(any_db):
    from trenches.db import repo
    from trenches.sample.compaction import compact

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MC", stream_id=1, feed="f", fields={})
    await repo.admit_to_watch_set(
        any_db, mint="MC", cohort="control", first_seen=T0,
        expires_at=T0 + dt.timedelta(hours=24), next_due_at=T0)
    # Pin admitted_at instead of letting it default to wall clock: compaction
    # selects on it, so a wall-clock value makes this test pass or fail
    # depending on what time of day the suite runs.
    await any_db.execute(
        "update watch_set set state = 'expired', admitted_at = ? where mint = ?",
        T0.isoformat(), "MC")
    for i in range(40):
        await repo.record_observation(any_db, mint="MC", fields={
            "observed_at": T0 + dt.timedelta(seconds=30 * i),
            "scheduled_for": T0 + dt.timedelta(seconds=30 * i),
            "source": "dexscreener", "status": "indexed", "price_usd": "1.0",
            "liquidity_usd": "5000", "age_seconds": 30 * i})

    report = await compact(any_db, now=T0 + dt.timedelta(days=3))
    assert report.mints == 1
    assert report.rows_after < report.rows_before
    audit = await any_db.fetchrow("select * from path_compaction where mint = ?", "MC")
    assert audit["rows_before"] == 40


def test_a_position_still_open_at_the_end_is_not_a_zero_x_loss():
    """Folding an unexited position in as 0x would punish long-horizon rules
    purely for being replayed against a short path."""
    engine = ExitEngine()
    path = [obs(0, "1.0"), obs(30, "1.1")]        # ends while still holding
    out = engine.replay(mint="M", cohort="c",
                        rule=Ruleset(name="ts", rules=(TimeStop(seconds=99999),)),
                        observations=path, events=[], size_usd=D("100"))
    assert out.exit_reason == "path_ended"
    assert out.skipped is not None
    assert out.legs == []


def test_excluded_replays_do_not_enter_the_distribution():
    from trenches.exits.report import summarise

    engine = ExitEngine()
    rule = Ruleset(name="ts", rules=(TimeStop(seconds=99999),))
    open_ended = engine.replay(mint="A", cohort="c", rule=rule,
                               observations=[obs(0, "1.0"), obs(30, "1.1")],
                               events=[], size_usd=D("100"))
    closed = engine.replay(mint="B", cohort="c",
                           rule=Ruleset(name="t", rules=(TrailingStop(),)),
                           observations=[obs(0, "1.0"), obs(30, "2.0"), obs(60, "1.0")],
                           events=[], size_usd=D("100"))
    s = summarise([open_ended, closed], rule="mixed", cohort="c")
    assert s.n == 1 and s.skipped == 1


def test_simultaneous_firings_are_counted_separately():
    """Structural and price firing on the same observation is a tie, not a loss
    for structural -- reporting only 'first' would read as if it never won."""
    from trenches.exits.report import compare_timing

    engine = ExitEngine()
    path = [obs(0, "1.0"), obs(30, "0.05", "400")]
    events = [evt(30, "liquidity_collapse")]
    s = {"M": engine.replay(mint="M", cohort="c",
                            rule=Ruleset(name="s", rules=(StructuralStop(),)),
                            observations=path, events=events, size_usd=D("100"))}
    p = {"M": engine.replay(mint="M", cohort="c",
                            rule=Ruleset(name="p", rules=(PriceStop(),)),
                            observations=path, events=events, size_usd=D("100"))}
    cmp_ = compare_timing(s, p)
    assert cmp_.simultaneous == 1
    assert cmp_.structural_first == 0 and cmp_.price_first == 0
