"""Decision support for one coin: enter, size, take profit, stop.

READ plan.py's module docstring before changing anything here. The three
refusals stated there -- never say "enter", never hide what was not checked,
structural stop before price stop -- are the design, not caveats bolted onto it.

Nothing in this package fetches, signs, or spends. It composes what the cascade,
the pool observation and the event timeline already know into the four numbers a
person actually needs before a trade, and says "unknown" everywhere it does not
have one.
"""
from __future__ import annotations

from .build import build, structural_triggers, to_decimal
from .plan import (
    DEFAULT_SIZE_PCT,
    DEFAULT_TARGET_EXIT_IMPACT,
    INSUFFICIENT_DATA,
    MAX_SIZE_PCT,
    NO_DISQUALIFIER,
    REJECT,
    Advice,
    SizeAdvice,
    StageReport,
    StopAdvice,
    TakeProfitAdvice,
    size_advice,
    size_for_impact,
    stage_reports,
    stop_advice,
    take_profit_advice,
)

__all__ = [
    "DEFAULT_SIZE_PCT",
    "DEFAULT_TARGET_EXIT_IMPACT",
    "INSUFFICIENT_DATA",
    "MAX_SIZE_PCT",
    "NO_DISQUALIFIER",
    "REJECT",
    "Advice",
    "SizeAdvice",
    "StageReport",
    "StopAdvice",
    "TakeProfitAdvice",
    "build",
    "size_advice",
    "size_for_impact",
    "stage_reports",
    "stop_advice",
    "structural_triggers",
    "take_profit_advice",
    "to_decimal",
]
