"""Pure `(before, after) -> events` detection.

Nothing here fetches. Detection is a function of two observations, so the whole
timeline is testable offline and a detector cannot quietly acquire a network
call inside a predicate.

THRESHOLDS ARE PARAMETERS, NOT TRUTHS. Where CLAUDE.md 3.4 states a number this
uses it; where it does not, the default is a starting point to be calibrated
against collected paths, and is named as such. None of them assert that a given
level is meaningful -- that is what the exit engine is for.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

# -- event types -----------------------------------------------------------
RUGGED = "rugged"
LIQUIDITY_COLLAPSE = "liquidity_collapse"
POOL_DISAPPEARED = "pool_disappeared"
LOCK_EXPIRING = "lock_expiring"
LOCK_EXPIRED = "lock_expired"
BUNDLERS_DISTRIBUTING = "bundlers_distributing"
DEV_SELLING = "dev_selling"
SNIPERS_EXITING = "snipers_exiting"
INSIDERS_EXITING = "insiders_exiting"
HOLDERS_INVERTING = "holders_inverting"

#: Liquidity fall within one observation gap that counts as a collapse rather
#: than drift. A starting point: it is well above ordinary volatility and well
#: below "gone", so it should catch a pull in progress rather than only after.
DEFAULT_LIQUIDITY_DROP_PCT = 0.50

#: Share-of-supply falls that count as distribution. 3.4 already says the
#: delta matters more than the level -- "high initial + low current means they
#: already distributed into you" -- so these are on the CHANGE, not a threshold
#: on the level.
DEFAULT_BUNDLER_DROP_PCT = 0.25
DEFAULT_DEV_DROP_PCT = 0.20
DEFAULT_SNIPER_DROP_PCT = 0.30

#: 3.4 stage 3: reject if the locker unlocks inside the holding horizon. Applied
#: here as a forward-looking event so a held position can react to it.
DEFAULT_LOCK_HORIZON_SECONDS = 6 * 3600


class Severity:
    INFO = "info"
    WARN = "warn"
    EXIT = "exit"      # 3.4 stage 6 calls for an exit on these


@dataclass(frozen=True, slots=True)
class StructuralEvent:
    event_type: str
    detected_at: dt.datetime
    observed_at: dt.datetime
    derived_from: str
    before_value: str | None = None
    after_value: str | None = None
    delta_pct: float | None = None
    severity: str = Severity.WARN
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Thresholds:
    liquidity_drop_pct: float = DEFAULT_LIQUIDITY_DROP_PCT
    bundler_drop_pct: float = DEFAULT_BUNDLER_DROP_PCT
    dev_drop_pct: float = DEFAULT_DEV_DROP_PCT
    sniper_drop_pct: float = DEFAULT_SNIPER_DROP_PCT
    lock_horizon_seconds: int = DEFAULT_LOCK_HORIZON_SECONDS


def _dec(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None


def _fall(before: Any, after: Any) -> float | None:
    """Fractional fall from before to after, or None if not computable.

    Returns None rather than 0 when `before` is zero or missing: a token that
    had no liquidity and still has none has not collapsed, and reporting that as
    a 0% drop would put a meaningless event on the timeline.
    """
    b, a = _dec(before), _dec(after)
    if b is None or a is None or b <= 0:
        return None
    return float((b - a) / b)


def from_price_path(
    before: dict, after: dict, *, thresholds: Thresholds | None = None
) -> list[StructuralEvent]:
    """Events derivable from two consecutive price observations.

    Liquidity is the only structural fact the price path carries, but it is the
    most important one: an LP pull shows up here before it shows up as a fill
    you cannot get.
    """
    t = thresholds or Thresholds()
    at = after["observed_at"]
    if isinstance(at, str):
        at = dt.datetime.fromisoformat(at)
    events: list[StructuralEvent] = []

    was_indexed = before.get("status") == "indexed"
    now_gone = after.get("status") in ("no_pool",)
    if was_indexed and now_gone:
        events.append(StructuralEvent(
            POOL_DISAPPEARED, at, at, "price_path_delta",
            before_value=str(before.get("liquidity_usd")), after_value=None,
            delta_pct=1.0, severity=Severity.EXIT,
        ))
        return events

    fall = _fall(before.get("liquidity_usd"), after.get("liquidity_usd"))
    if fall is not None and fall >= t.liquidity_drop_pct:
        events.append(StructuralEvent(
            LIQUIDITY_COLLAPSE, at, at, "price_path_delta",
            before_value=str(before.get("liquidity_usd")),
            after_value=str(after.get("liquidity_usd")),
            delta_pct=fall,
            severity=Severity.EXIT if fall >= 0.8 else Severity.WARN,
        ))
    return events


def _pct(report: dict, *path: str) -> Any:
    node: Any = report
    for key in path:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def from_rugcheck(
    before: dict | None, after: dict, *, observed_at: dt.datetime,
    thresholds: Thresholds | None = None,
) -> list[StructuralEvent]:
    """Events derivable from two RugCheck reports.

    `before` may be None for the first poll; only facts that are true in
    isolation (rugged, an expired lock) can fire then. A change needs two
    observations by definition, and inventing a baseline would make the first
    poll on any token look like a collapse.
    """
    t = thresholds or Thresholds()
    events: list[StructuralEvent] = []

    if after.get("rugged") is True and (before or {}).get("rugged") is not True:
        events.append(StructuralEvent(
            RUGGED, observed_at, observed_at, "rugcheck_poll",
            before_value=str((before or {}).get("rugged")), after_value="True",
            severity=Severity.EXIT,
        ))

    unlock = after.get("lockerUnlockDate") or _pct(after, "markets", "lp", "unlockDate")
    if unlock:
        when = _parse_time(unlock)
        if when is not None:
            remaining = (when - observed_at).total_seconds()
            if remaining <= 0:
                events.append(StructuralEvent(
                    LOCK_EXPIRED, observed_at, observed_at, "rugcheck_poll",
                    after_value=when.isoformat(), severity=Severity.EXIT))
            elif remaining <= t.lock_horizon_seconds:
                events.append(StructuralEvent(
                    LOCK_EXPIRING, observed_at, observed_at, "rugcheck_poll",
                    after_value=when.isoformat(), delta_pct=None,
                    severity=Severity.WARN,
                    payload={"seconds_remaining": int(remaining)}))

    if before is None:
        return events

    # 3.4: comparing initial against current share is more informative than
    # either level, because it is what distinguishes "they hold a lot" from
    # "they are selling it to you right now".
    for label, path, limit, severity in (
        (BUNDLERS_DISTRIBUTING, ("bundlers", "totalPercentage"),
         t.bundler_drop_pct, Severity.EXIT),
        (DEV_SELLING, ("dev", "percentage"), t.dev_drop_pct, Severity.EXIT),
        (SNIPERS_EXITING, ("snipers", "totalPercentage"),
         t.sniper_drop_pct, Severity.WARN),
        (INSIDERS_EXITING, ("insiders", "totalPercentage"),
         t.sniper_drop_pct, Severity.WARN),
    ):
        fall = _fall(_pct(before, *path), _pct(after, *path))
        if fall is not None and fall >= limit:
            events.append(StructuralEvent(
                label, observed_at, observed_at, "rugcheck_poll",
                before_value=str(_pct(before, *path)),
                after_value=str(_pct(after, *path)),
                delta_pct=fall, severity=severity,
            ))

    before_holders, after_holders = _dec(before.get("totalHolders")), _dec(
        after.get("totalHolders"))
    if before_holders and after_holders and after_holders < before_holders:
        fall = float((before_holders - after_holders) / before_holders)
        if fall >= 0.1:
            events.append(StructuralEvent(
                HOLDERS_INVERTING, observed_at, observed_at, "rugcheck_poll",
                before_value=str(before_holders), after_value=str(after_holders),
                delta_pct=fall, severity=Severity.WARN,
            ))
    return events


def _parse_time(value: Any) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt.UTC)
    if isinstance(value, (int, float)) and value > 0:
        seconds = value / 1000 if value > 10**11 else value
        try:
            return dt.datetime.fromtimestamp(seconds, tz=dt.UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str) and value:
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)
    return None
