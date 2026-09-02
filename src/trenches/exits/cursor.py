"""The lookahead guard.

A rule may only see data at or before the simulated instant. This is the easiest
bug to introduce in a backtest and the hardest to notice, because a rule with a
peek at the future simply looks brilliant -- so it is enforced in code and
raises, rather than being left to review.

Rules never touch the path. They get a cursor bound to `now`, and every read
goes through it.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal


class LookaheadError(AssertionError):
    """A rule tried to read data from after the simulated instant."""


@dataclass(frozen=True, slots=True)
class Observation:
    at: dt.datetime
    status: str
    price_usd: Decimal | None
    liquidity_usd: Decimal | None
    age_seconds: int
    venue_kind: str | None = None

    @property
    def tradeable(self) -> bool:
        """A price we could plausibly have acted on.

        `not_yet_indexed` is explicitly NOT tradeable: it means the vendor had
        not indexed the token, not that the token was worthless.
        """
        return self.status == "indexed" and self.price_usd is not None


@dataclass(frozen=True, slots=True)
class Event:
    at: dt.datetime
    event_type: str
    severity: str
    delta_pct: float | None = None


class PathCursor:
    """A window over one mint's path and events, frozen at `now`."""

    __slots__ = ("_entry", "_events", "_now", "_obs", "reads")

    def __init__(
        self,
        observations: list[Observation],
        events: list[Event],
        now: dt.datetime,
        entry: Observation,
    ) -> None:
        self._obs = observations
        self._events = events
        self._now = now
        self._entry = entry
        self.reads = 0

    @property
    def now(self) -> dt.datetime:
        return self._now

    @property
    def entry(self) -> Observation:
        return self._entry

    def _check(self, at: dt.datetime) -> None:
        if at > self._now:
            raise LookaheadError(
                f"rule tried to read an observation at {at.isoformat()} while the "
                f"simulated clock is {self._now.isoformat()}. A rule may only see "
                "data at or before the simulated instant."
            )

    def at(self, index: int) -> Observation:
        """Positional read, guarded. Raises rather than silently clamping."""
        obs = self._obs[index]
        self._check(obs.at)
        self.reads += 1
        return obs

    def history(self) -> list[Observation]:
        """Everything observed up to and including now."""
        self.reads += 1
        return [o for o in self._obs if o.at <= self._now]

    def latest(self) -> Observation | None:
        seen = [o for o in self.history() if o.tradeable]
        return seen[-1] if seen else None

    def peak_price(self) -> Decimal | None:
        """Running maximum over what has been seen. Never the whole path."""
        prices = [o.price_usd for o in self.history() if o.tradeable]
        return max(prices) if prices else None

    def events_so_far(self, *, severities: tuple[str, ...] | None = None) -> list[Event]:
        out = [e for e in self._events if e.at <= self._now]
        if severities:
            out = [e for e in out if e.severity in severities]
        self.reads += 1
        return out

    def elapsed_seconds(self) -> float:
        return (self._now - self._entry.at).total_seconds()

    def advanced_to(self, now: dt.datetime) -> PathCursor:
        return PathCursor(self._obs, self._events, now, self._entry)
