"""Assemble one Advice from the cascade, the pool, and the event timeline."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from .plan import (
    DEFAULT_SIZE_PCT,
    DEFAULT_TARGET_EXIT_IMPACT,
    DEFAULT_TIME_STOP_SECONDS,
    DEFAULT_TP_MULTIPLE,
    DEFAULT_TRAIL_PCT,
    INSUFFICIENT_DATA,
    NO_DISQUALIFIER,
    REJECT,
    Advice,
    size_advice,
    stage_reports,
    stop_advice,
    structural_triggers,
    take_profit_advice,
)


def build(
    *,
    mint: str,
    verdict: Any,
    trail: Any,
    liquidity_usd: Decimal | None,
    bankroll: Decimal | None = None,
    size_pct: Decimal = DEFAULT_SIZE_PCT,
    target_exit_impact: Decimal = DEFAULT_TARGET_EXIT_IMPACT,
    tp_multiple: Decimal = DEFAULT_TP_MULTIPLE,
    trail_pct: Decimal = DEFAULT_TRAIL_PCT,
    time_stop_seconds: int = DEFAULT_TIME_STOP_SECONDS,
    unchecked: dict[str, str] | None = None,
    observed: dict[str, Any] | None = None,
) -> Advice:
    """Compose the four answers. Never raises on missing inputs."""
    stages = stage_reports(trail)

    if verdict is not None and not verdict.accept:
        headline, reason = REJECT, (
            f"stage {verdict.stage}: {verdict.reason}"
        )
    else:
        missing = [s.stage for s in stages if not s.had_data]
        if missing:
            headline = INSUFFICIENT_DATA
            reason = (
                f"no disqualifier found, but stages {', '.join(map(str, missing))} "
                f"had nothing to read. That is not a clean bill of health."
            )
        else:
            headline = NO_DISQUALIFIER
            reason = (
                "every stage ran on real data and none rejected. This removes "
                "known disqualifiers; it does not establish an edge (1.9)."
            )

    size = size_advice(
        liquidity_usd=liquidity_usd, bankroll=bankroll,
        size_pct=size_pct, target_exit_impact=target_exit_impact,
    )
    position = size.recommended
    return Advice(
        mint=mint,
        verdict=headline,
        verdict_reason=reason,
        stages=stages,
        size=size,
        take_profit=take_profit_advice(
            position_value=position, liquidity_usd=liquidity_usd,
            multiple=tp_multiple, trail_pct=trail_pct,
        ),
        stop=stop_advice(
            position_value=position, liquidity_usd=liquidity_usd,
            triggers=structural_triggers(),
            price_stop_pct=trail_pct, time_stop_seconds=time_stop_seconds,
        ),
        unchecked=dict(unchecked or {}),
        observed=dict(observed or {}),
    )


def to_decimal(value: Any) -> Decimal | None:
    """Parse a stored exact-decimal string. Never raises, never guesses."""
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None
