"""Rule evaluation. Offline, pure functions over joined rows.

The property under test throughout: a rejection whose outcome we cannot
determine must never be scored as a success. That is the exact way a filter
proves itself against an empty journal.
"""
from __future__ import annotations

from decimal import Decimal

from trenches.label import HORIZON_SECONDS
from trenches.label.rules import (
    RULES,
    Rule,
    evaluate,
    is_late,
    peak_multiple,
)


def row(**kw) -> dict:
    base = {
        "mint": "M1", "signer": "S1", "declared_creator": None, "launchpad": "pump.fun",
        "symbol": "TST", "detected_at": "2026-08-30T00:00:00+00:00", "is_mayhem_mode": 0,
        "initial_buy_base": "1000", "creator_n_mints": 1,
        "max_price_usd_seen": None, "peak_observations": 0,
        "status_15m": None, "price_15m": None, "lateness_15m": 0, "backfilled_15m": 0,
        "status_24h": None, "price_24h": None, "lateness_24h": 0,
        "status_7d": None,
    }
    base.update(kw)
    return base


# -- peak multiple ---------------------------------------------------------

def test_peak_multiple_needs_both_sides():
    assert peak_multiple(row()) is None
    assert peak_multiple(row(max_price_usd_seen="1")) is None
    assert peak_multiple(row(price_15m="1")) is None


def test_peak_multiple_never_divides_by_zero():
    assert peak_multiple(row(max_price_usd_seen="1", price_15m="0")) is None


def test_peak_multiple_is_decimal_not_float():
    got = peak_multiple(row(max_price_usd_seen="0.003", price_15m="0.001"))
    assert got == Decimal("3")


# -- lateness guard --------------------------------------------------------

def test_a_horizon_observed_within_its_own_window_is_not_stale():
    assert not is_late(row(lateness_15m=60), HORIZON_SECONDS["15m"], "lateness_15m")


def test_a_15m_label_backfilled_days_later_is_stale():
    """It is a 'what does this look like now' reading wearing a 15m name."""
    assert is_late(row(lateness_15m=3 * 86400), HORIZON_SECONDS["15m"], "lateness_15m")


def test_missing_lateness_is_not_treated_as_stale():
    assert not is_late(row(lateness_15m=None), HORIZON_SECONDS["15m"], "lateness_15m")


# -- scoring ---------------------------------------------------------------

ALWAYS = Rule("always", 1, "rejects everything", lambda r: True)


def test_a_rejection_that_died_counts_as_correct():
    out = evaluate([row(status_24h="no_pool")], rules=(ALWAYS,))[0]
    assert (out.rejected, out.correct, out.wrong, out.undeterminable) == (1, 1, 0, 0)


def test_a_rejection_that_mooned_counts_as_wrong():
    r = row(status_24h="alive", max_price_usd_seen="0.010", price_15m="0.001")
    out = evaluate([r], rules=(ALWAYS,))[0]
    assert (out.correct, out.wrong) == (0, 1)


def test_a_dead_token_that_still_peaked_is_scored_wrong_not_correct():
    """It went 10x before dying. A filter that killed it cost us the trade,
    and the fact that it later died does not undo that."""
    r = row(status_24h="dead", max_price_usd_seen="0.010", price_15m="0.001")
    out = evaluate([r], rules=(ALWAYS,))[0]
    assert (out.correct, out.wrong) == (0, 1)


def test_an_unknowable_rejection_is_never_counted_as_success():
    """The whole point. With no outcome, a filter must not look vindicated."""
    out = evaluate([row(status_24h=None, status_7d=None, status_15m=None)], rules=(ALWAYS,))[0]
    assert out.rejected == 1
    assert out.correct == 0
    assert out.undeterminable == 1
    assert out.precision() is None


def test_precision_excludes_undeterminable_from_the_denominator():
    rows = [
        row(status_24h="no_pool"),                                        # correct
        row(status_24h="alive", max_price_usd_seen="1", price_15m="0.1"), # wrong (10x)
        row(status_24h=None),                                             # unknowable
    ]
    out = evaluate(rows, rules=(ALWAYS,))[0]
    assert (out.correct, out.wrong, out.undeterminable) == (1, 1, 1)
    assert out.precision() == 0.5


def test_a_rule_that_rejects_nothing_scores_nothing():
    never = Rule("never", 1, "rejects nothing", lambda r: False)
    out = evaluate([row(status_24h="no_pool")], rules=(never,))[0]
    assert out.rejected == 0
    assert out.precision() is None


def test_a_raising_rule_does_not_kill_the_report():
    def boom(_r):
        raise ValueError("bad rule")

    rules = (Rule("boom", 1, "explodes", boom), ALWAYS)
    out = evaluate([row(status_24h="no_pool")], rules=rules)
    assert out[0].rejected == 0        # the broken rule scored nothing
    assert out[1].rejected == 1        # and the good one still ran


def test_the_shipped_rules_all_evaluate():
    rows = [row(), row(is_mayhem_mode=1, creator_n_mints=12, launchpad=None, symbol="")]
    out = evaluate(rows, rules=RULES)
    assert len(out) == len(RULES)
    assert {o.name for o in out} == {r.name for r in RULES}


def test_unknown_launchpad_control_rule_catches_unverified_pools():
    """The control exists so a real rule can be compared against a coin flip."""
    out = evaluate([row(launchpad="unverified:bonk", status_24h="no_pool")],
                   rules=tuple(r for r in RULES if r.name == "unknown_launchpad"))[0]
    assert out.rejected == 1


def test_report_runs_with_zero_decisions_recorded():
    """Week 1 has no decisions rows; the report must still work."""
    out = evaluate([row(status_24h="no_pool"), row(status_24h="alive")], rules=RULES)
    assert all(o.rejected >= 0 for o in out)
