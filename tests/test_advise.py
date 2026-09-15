"""Decision support for one coin: enter, size, take profit, stop.

Three properties are asserted harder than the arithmetic, because they are the
design rather than the implementation:

  1. It never says "enter". An accept is NO DISQUALIFIER, never a buy signal.
  2. Stages that read nothing downgrade the verdict to INSUFFICIENT DATA. A
     clean report over unfetched stages must not look like a clean one over
     checked stages.
  3. The pool can bind the position size instead of the bankroll, and when it
     does the report says so -- "I sized small" and "I could not have got out of
     anything bigger" are different facts.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from trenches.advise import (
    INSUFFICIENT_DATA,
    MAX_SIZE_PCT,
    NO_DISQUALIFIER,
    REJECT,
    build,
    size_advice,
    size_for_impact,
    stage_reports,
    stop_advice,
    structural_triggers,
    take_profit_advice,
    to_decimal,
)
from trenches.decide import Verdict

D = Decimal


def trail(*specs) -> list[Verdict]:
    """(stage, accept, inputs) triples -> a cascade trail."""
    return [Verdict(accept, stage, None, inputs) for stage, accept, inputs in specs]


FED = trail(
    (0, True, {"mint": "M"}),
    (1, True, {"mintable": 0}),
    (2, True, {"creator_n_mints": 4}),
    (3, True, {"liquidity_usd": 90_000.0}),
    (4, True, {"snipers_total": 3}),
    (5, True, {"largest_cluster_pct": 4.0}),
)


# -- the verdict -----------------------------------------------------------

def test_a_fully_fed_clean_cascade_says_no_disqualifier_never_enter():
    """1.9: expected return is negative on the realistic distribution and Kelly
    is f* ~ 0. Turning 'nothing disqualified it' into 'buy' would assert an edge
    nobody measured."""
    advice = build(mint="M", verdict=FED[-1], trail=FED, liquidity_usd=D("90000"))
    assert advice.verdict == NO_DISQUALIFIER
    assert "does not establish an edge" in advice.verdict_reason
    assert "enter" not in advice.verdict.lower()


def test_unfed_stages_downgrade_the_verdict_and_name_themselves():
    """THE property. Today almost every real mint lands here."""
    partial = trail(
        (0, True, {"mint": "M"}),
        (1, True, {"structural": "unfetched"}),
        (2, True, {"creator": "unknown"}),
        (3, True, {"liquidity_usd": 90_000.0}),
        (4, True, {"concentration": "cold_start"}),
        (5, True, {"clustering": "labels_missing"}),
    )
    advice = build(mint="M", verdict=partial[-1], trail=partial, liquidity_usd=D("90000"))
    assert advice.verdict == INSUFFICIENT_DATA
    assert "1, 2, 4, 5" in advice.verdict_reason
    assert "not a clean bill of health" in advice.verdict_reason
    assert advice.checked_stages == 2


def test_a_rejection_is_reported_with_the_stage_and_the_reason():
    rejected = Verdict(False, 3, "liquidity $120 below the $5,000 floor", {})
    advice = build(mint="M", verdict=rejected, trail=[*FED[:3], rejected],
                   liquidity_usd=D("120"))
    assert advice.verdict == REJECT
    assert "stage 3" in advice.verdict_reason
    assert "below the" in advice.verdict_reason


@pytest.mark.parametrize("stage,marker", [
    (1, "unfetched"), (2, "unknown"), (3, "unfetched"),
    (4, "cold_start"), (4, "unfetched"),
    (5, "no_edges"), (5, "labels_missing"), (5, "unmeasurable"), (5, "unfetched"),
])
def test_every_no_data_marker_is_recognised(stage, marker):
    """A new marker must not silently start counting as 'checked'."""
    field = {1: "structural", 2: "creator", 3: "liquidity",
             4: "concentration", 5: "clustering"}[stage]
    reports = stage_reports(trail((stage, True, {field: marker})))
    assert reports[0].had_data is False
    assert reports[0].note == marker


def test_stage_0_always_counts_as_data():
    """It reads the mint out of the feed; there is nothing to be missing."""
    assert stage_reports(trail((0, True, {"mint": "M"})))[0].had_data is True


# -- size ------------------------------------------------------------------

def test_size_for_impact_inverts_the_constant_product_exactly():
    """V = D*t/(1-t). Checked against the impact model rather than restated."""
    from trenches.exits.impact import sell_into

    value = size_for_impact(D("10000"), D("0.02"))     # depth = 5,000
    assert value == D("5000") * D("0.02") / D("0.98")
    assert abs(sell_into(value, D("10000")).impact_pct - D("0.02")) < D("1e-12")


def test_the_bankroll_binds_on_a_deep_pool():
    s = size_advice(liquidity_usd=D("900000"), bankroll=D("1000"), size_pct=D("1"))
    assert s.binding == "bankroll"
    assert s.recommended == D("10")


def test_the_pool_binds_on_a_thin_one_and_the_report_says_so():
    """The case people do not expect. 1.9 is about surviving variance and says
    nothing about whether the position can be sold."""
    s = size_advice(liquidity_usd=D("4000"), bankroll=D("100000"), size_pct=D("1"))
    assert s.binding == "pool"
    assert s.recommended == s.depth_cap < s.bankroll_cap
    assert "POOL IS BINDING" in s.note


def test_no_liquidity_observation_gives_the_bankroll_cap_with_a_warning():
    s = size_advice(liquidity_usd=None, bankroll=D("1000"))
    assert s.recommended == D("10")
    assert s.depth_cap is None
    assert "NO LIQUIDITY OBSERVED" in s.note


def test_nothing_to_size_against_is_unknown_not_zero():
    s = size_advice(liquidity_usd=None, bankroll=None)
    assert s.recommended is None and s.binding == "unknown"


def test_1_9s_hard_cap_cannot_be_argued_past():
    """The parameter most likely to get argued with later (simulator.py says so
    in as many words), so it raises rather than clamping quietly."""
    with pytest.raises(ValueError, match="hard cap"):
        size_advice(liquidity_usd=D("4000"), bankroll=D("1000"),
                    size_pct=MAX_SIZE_PCT + D("0.5"))


def test_the_no_counterparty_cap_sits_above_the_impact_cap():
    s = size_advice(liquidity_usd=D("10000"), bankroll=D("1000000"), size_pct=D("1"))
    assert s.depth_cap < s.hard_depth_cap


# -- take profit -----------------------------------------------------------

def test_the_ladder_is_1_10s_and_is_checked_for_exitability():
    tp = take_profit_advice(position_value=D("10"), liquidity_usd=D("4000"))
    assert tp.multiple == D("2.0") and tp.fraction == D("0.5")
    assert tp.exitable is True
    assert tp.impact_at_tp < D("0.01")


def test_a_take_profit_you_could_not_sell_into_is_flagged():
    """A 2x you cannot sell is not a 2x."""
    tp = take_profit_advice(position_value=D("5000"), liquidity_usd=D("4000"))
    assert tp.exitable is False


def test_no_position_size_means_no_exitability_claim():
    tp = take_profit_advice(position_value=None, liquidity_usd=D("4000"))
    assert tp.exitable is None and tp.impact_at_tp is None


# -- stop ------------------------------------------------------------------

def test_the_structural_triggers_come_first_and_are_conditions_not_prices():
    """These fire before the price move completes, which is the only kind of
    stop that works when the bid disappears mid-rug."""
    stop = stop_advice(position_value=D("10"), liquidity_usd=D("4000"))
    assert stop.structural
    assert any("liquidity falls" in t for t in stop.structural)
    assert any("rugged" in t for t in stop.structural)
    assert "assumes a bid" in stop.note


def test_the_realised_stop_is_worse_than_the_trigger_and_says_by_how_much():
    stop = stop_advice(position_value=D("400"), liquidity_usd=D("4000"))
    assert stop.realised_at_stop_pct > stop.price_stop_pct


def test_a_stop_into_no_liquidity_is_a_total_loss_not_a_35_percent_one():
    """The honest answer when there is nothing to sell into."""
    stop = stop_advice(position_value=D("100"), liquidity_usd=None)
    assert stop.realised_at_stop_pct == D(1)


def test_the_triggers_are_derived_from_the_detectors_not_retyped():
    from trenches.structural import detectors

    t = detectors.Thresholds(liquidity_drop_pct=0.9)
    assert any("90%" in trigger for trigger in structural_triggers(t))


# -- composition -----------------------------------------------------------

def test_the_advice_carries_the_gap_list_so_it_can_be_printed():
    advice = build(mint="M", verdict=FED[-1], trail=FED, liquidity_usd=D("90000"),
                   unchecked={"holders": "no normalised holder list is stored"})
    assert "holders" in advice.unchecked


def test_build_never_raises_on_missing_everything():
    advice = build(mint="M", verdict=None, trail=None, liquidity_usd=None)
    assert advice.verdict == NO_DISQUALIFIER      # nothing ran, nothing rejected
    assert advice.size.recommended is None
    assert advice.stop.structural


def test_to_decimal_parses_exact_strings_and_refuses_junk():
    assert to_decimal("1234.5678") == D("1234.5678")
    assert to_decimal(None) is None
    assert to_decimal("abc") is None
    assert to_decimal(True) is None
    assert to_decimal("NaN") is None
