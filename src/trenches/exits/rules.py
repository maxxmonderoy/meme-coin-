"""Declarative exit rules.

Rules are objects, not branches in the engine, so a new one is a new class and
never an edit to the replay loop. Each returns an ExitSignal or None given only
what the cursor exposes at the simulated instant.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .cursor import PathCursor

#: 1.10: original stake off at 2x, trailing stop on the remainder.
DEFAULT_TP_MULTIPLE = Decimal("2.0")
DEFAULT_TP_FRACTION = Decimal("0.5")
DEFAULT_TRAIL_PCT = Decimal("0.35")

#: 1.10 again: "diamond hands on a two-hour-old token is a hope, not a plan",
#: and 85% of the profitable sniper cohort is out inside five minutes (1.5).
DEFAULT_TIME_STOP_SECONDS = 30 * 60


@dataclass(frozen=True, slots=True)
class ExitSignal:
    kind: str
    fraction: Decimal          # of the CURRENT holding
    reason: str


@dataclass(frozen=True, slots=True)
class RuleState:
    """What the engine tells a rule about the position, not about the future."""

    entry_price: Decimal
    remaining_fraction: Decimal
    tp_taken: bool


class Rule:
    name: str = "rule"

    def decide(self, state: RuleState, cursor: PathCursor) -> ExitSignal | None:
        raise NotImplementedError

    def describe(self) -> dict:
        return {"name": self.name}


@dataclass(frozen=True, slots=True)
class TakeProfitLadder(Rule):
    """Sell a fraction at a multiple of entry."""

    multiple: Decimal = DEFAULT_TP_MULTIPLE
    fraction: Decimal = DEFAULT_TP_FRACTION
    name: str = "take_profit"

    def decide(self, state, cursor):
        if state.tp_taken:
            return None
        latest = cursor.latest()
        if latest is None or latest.price_usd is None:
            return None
        if latest.price_usd >= state.entry_price * self.multiple:
            return ExitSignal("tp", self.fraction,
                              f"reached {self.multiple}x entry")
        return None

    def describe(self):
        return {"name": self.name, "multiple": str(self.multiple),
                "fraction": str(self.fraction)}


@dataclass(frozen=True, slots=True)
class TrailingStop(Rule):
    """Exit the remainder when price falls `pct` from the running peak.

    The peak comes from the cursor, so it is the peak SEEN SO FAR -- never the
    path maximum, which is the classic way a trailing-stop backtest cheats.
    """

    pct: Decimal = DEFAULT_TRAIL_PCT
    name: str = "trailing_stop"

    def decide(self, state, cursor):
        latest = cursor.latest()
        peak = cursor.peak_price()
        if latest is None or peak is None or latest.price_usd is None:
            return None
        if latest.price_usd <= peak * (Decimal(1) - self.pct):
            return ExitSignal("trail", Decimal(1),
                              f"fell {self.pct:.0%} from peak {peak}")
        return None

    def describe(self):
        return {"name": self.name, "pct": str(self.pct)}


@dataclass(frozen=True, slots=True)
class TimeStop(Rule):
    seconds: int = DEFAULT_TIME_STOP_SECONDS
    name: str = "time_stop"

    def decide(self, state, cursor):
        if cursor.elapsed_seconds() >= self.seconds:
            return ExitSignal("timeout", Decimal(1), f"held {self.seconds}s")
        return None

    def describe(self):
        return {"name": self.name, "seconds": self.seconds}


@dataclass(frozen=True, slots=True)
class StructuralStop(Rule):
    """Exit on a structural event.

    The rule this whole change exists to test: on an illiquid token a price stop
    triggers into an absent bid, while LP-pulled or bundlers-distributing is
    observable before the price move completes.
    """

    severities: tuple[str, ...] = ("exit",)
    event_types: tuple[str, ...] = ()
    name: str = "structural_stop"

    def decide(self, state, cursor):
        for event in cursor.events_so_far(severities=self.severities or None):
            if self.event_types and event.event_type not in self.event_types:
                continue
            return ExitSignal("structural", Decimal(1),
                              f"{event.event_type} ({event.severity})")
        return None

    def describe(self):
        return {"name": self.name, "severities": list(self.severities),
                "event_types": list(self.event_types)}


@dataclass(frozen=True, slots=True)
class PriceStop(Rule):
    """A plain stop-loss at a fraction below entry.

    Included so the comparison the brief asks for has a baseline: on an illiquid
    token this is the rule that triggers at -20% and fills far below it.
    """

    pct: Decimal = Decimal("0.30")
    name: str = "price_stop"

    def decide(self, state, cursor):
        latest = cursor.latest()
        if latest is None or latest.price_usd is None:
            return None
        if latest.price_usd <= state.entry_price * (Decimal(1) - self.pct):
            return ExitSignal("stop", Decimal(1), f"fell {self.pct:.0%} below entry")
        return None

    def describe(self):
        return {"name": self.name, "pct": str(self.pct)}


@dataclass(frozen=True, slots=True)
class Ruleset(Rule):
    """Several rules with EXPLICIT precedence: first match in order wins.

    Order is data, not an accident of evaluation. Put a structural stop ahead of
    a trailing stop and the structural signal pre-empts it; reverse them and it
    does not. That is exactly the comparison being measured, so it has to be
    stated rather than emergent.
    """

    rules: tuple[Rule, ...] = ()
    name: str = "ruleset"

    def decide(self, state, cursor):
        for rule in self.rules:
            signal = rule.decide(state, cursor)
            if signal is not None:
                return signal
        return None

    def describe(self):
        return {"name": self.name, "precedence": [r.describe() for r in self.rules]}


def library() -> dict[str, Ruleset]:
    """Named rulesets the CLI can replay by name."""
    return {
        "ladder_trail": Ruleset(name="ladder_trail", rules=(
            TimeStop(), TakeProfitLadder(), TrailingStop())),
        "structural_first": Ruleset(name="structural_first", rules=(
            StructuralStop(), TimeStop(), TakeProfitLadder(), TrailingStop())),
        "price_stop_only": Ruleset(name="price_stop_only", rules=(
            TimeStop(), PriceStop())),
        "hold_to_timeout": Ruleset(name="hold_to_timeout", rules=(TimeStop(),)),
    }
