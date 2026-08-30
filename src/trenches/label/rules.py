"""Hypothetical rejection rules, and what each one would have cost.

THE QUESTION THIS EXISTS TO ANSWER: which rejection rules earn their keep, and
which just cost us winners? In an unlabeled journal a well-calibrated filter and
a filter that rejects everything look identical -- both show a high rejection
rate and no losses, because the losses are invisible.

A rule is a small predicate over a joined row, not hardcoded SQL, so adding one
is writing a function and appending it to RULES. Nothing here reads `decisions`:
week 1 has no decision rows, so rules are evaluated hypothetically against
tokens_seen + outcomes and the report works the moment stage gating lands.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from ..log import get

log = get(__name__)

#: A rejection is "correct" when the token went on to be untradeable or
#: illiquid, and "wrong" when it went on to a meaningful peak.
CORRECT_STATUSES = ("no_pool", "dead")

#: Peak multiple over the 15m price that counts as a token we would have minded
#: missing. A judgement, exposed as a parameter so it can be moved.
DEFAULT_PEAK_MULTIPLE = Decimal("2.0")


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


@dataclass(frozen=True, slots=True)
class Rule:
    """`predicate(row) -> True` means this rule WOULD REJECT the candidate."""

    name: str
    stage: int
    describe: str
    predicate: Callable[[dict], bool]


def _mayhem(row: dict) -> bool:
    return bool(row.get("is_mayhem_mode"))


def _prolific_creator(row: dict, threshold: int = 10) -> bool:
    n = row.get("creator_n_mints")
    return bool(n is not None and n >= threshold)


def _serial_creator(row: dict, threshold: int = 3) -> bool:
    n = row.get("creator_n_mints")
    return bool(n is not None and n >= threshold)


def _no_launchpad(row: dict) -> bool:
    """Reject anything we could not attribute to a known launchpad.

    Included as a control: it should reject a lot and be roughly uninformative.
    A rule that scores no better than this one is not a filter, it is a coin.
    """
    lp = row.get("launchpad")
    return lp is None or str(lp).startswith("unverified:")


def _thin_initial_buy(row: dict, floor: int = 1) -> bool:
    v = row.get("initial_buy_base")
    if v in (None, ""):
        return False
    try:
        return int(v) < floor
    except (TypeError, ValueError):
        return False


def _no_symbol(row: dict) -> bool:
    return not (row.get("symbol") or "").strip()


#: Add a rule by appending here. No SQL, no report edits.
RULES: tuple[Rule, ...] = (
    Rule("mayhem_mode", 1, "reject pump.fun mayhem-mode launches", _mayhem),
    Rule("creator_ge_3", 2, "reject creators with >= 3 prior launches", _serial_creator),
    Rule("creator_ge_10", 2, "reject creators with >= 10 prior launches", _prolific_creator),
    Rule("unknown_launchpad", 1, "reject launches we could not attribute (control)",
         _no_launchpad),
    Rule("no_symbol", 1, "reject launches with no ticker", _no_symbol),
    Rule("thin_initial_buy", 1, "reject a zero initial buy", _thin_initial_buy),
)


@dataclass(slots=True)
class RuleOutcome:
    name: str
    stage: int
    describe: str
    rejected: int = 0
    correct: int = 0          # rejected AND ended no_pool/dead
    wrong: int = 0            # rejected AND reached the peak multiple
    undeterminable: int = 0   # rejected but we cannot say -- NOT a success
    status_split: dict[str, int] | None = None

    def precision(self) -> float | None:
        """Of the rejections we can judge, what share were right?

        `undeterminable` is excluded from the denominator rather than counted
        as correct. Folding missing data into success is how a filter proves
        itself with an empty journal.
        """
        judged = self.correct + self.wrong
        return None if judged == 0 else self.correct / judged


def peak_multiple(row: dict) -> Decimal | None:
    """max_price_seen / price at 15m.

    None when either side is missing or the 15m price is zero. There is no
    entry price in week 1, so the 15m observation is the closest thing to
    "what it cost when we saw it".
    """
    peak = _dec(row.get("max_price_usd_seen"))
    entry = _dec(row.get("price_15m"))
    if peak is None or entry is None or entry == 0:
        return None
    return peak / entry


def is_late(row: dict, horizon_seconds: int, key: str) -> bool:
    """A horizon observed later than the horizon itself is nominal only.

    A 24h label taken at 26h is usable. A 15m label backfilled three days after
    the fact is a "what does this look like now" reading wearing a 15m name,
    and letting it into calibration would make the 15m thresholds meaningless.
    """
    late = row.get(key)
    return late is not None and int(late) > horizon_seconds


def evaluate(
    rows: list[dict],
    *,
    rules: tuple[Rule, ...] = RULES,
    peak_threshold: Decimal = DEFAULT_PEAK_MULTIPLE,
) -> list[RuleOutcome]:
    """Score every rule against labeled history."""
    out = [RuleOutcome(r.name, r.stage, r.describe, status_split={}) for r in rules]
    for rule, acc in zip(rules, out, strict=True):
        for row in rows:
            try:
                rejects = rule.predicate(row)
            except Exception as exc:  # a broken rule must not kill the whole report
                log.warning("rule %s raised on mint %s: %s", rule.name, row.get("mint"), exc)
                continue
            if not rejects:
                continue
            acc.rejected += 1
            status = row.get("status_24h") or row.get("status_7d") or row.get("status_15m")
            if status:
                acc.status_split[status] = acc.status_split.get(status, 0) + 1
            multiple = peak_multiple(row)
            if multiple is not None and multiple >= peak_threshold:
                acc.wrong += 1
            elif status in CORRECT_STATUSES:
                acc.correct += 1
            else:
                acc.undeterminable += 1
    return out
