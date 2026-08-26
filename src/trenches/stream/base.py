"""Transport-neutral event shape and the Consumer contract.

Identity note. Week 1 races two independent free feeds (3.3), and they do not
agree on identifiers: PumpPortal reports a signature, RugCheck's new_tokens
listing does not. So there are two identities and they do different jobs:

  * `mint` is the CANDIDATE identity. Dedupe across feeds is on mint, because
    it is the only thing both feeds agree on for the same launch.
  * `(provider, event_id)` is the STORAGE identity for raw_events, so the same
    launch seen by both feeds is stored twice on purpose -- that is what makes
    the feed race measurable.
"""
from __future__ import annotations

import abc
import datetime as dt
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Final


class EventKind:
    CREATE: Final = "create"
    MIGRATE: Final = "migrate"
    TRADE: Final = "trade"
    OTHER: Final = "other"
    PING: Final = "ping"      # liveness only, never journalled


EPHEMERAL_KINDS: Final = frozenset({EventKind.PING})


def _now() -> dt.datetime:
    return dt.datetime.now(tz=dt.UTC)


@dataclass(frozen=True, slots=True)
class RawEvent:
    provider: str
    event_kind: str
    mint: str | None = None
    signature: str | None = None
    slot: int | None = None
    commitment: str = "processed"
    block_time: dt.datetime | None = None
    payload: dict | None = None
    received_at: dt.datetime = field(default_factory=_now)

    @property
    def event_id(self) -> str:
        """Storage identity within a feed.

        Prefers the signature where the feed provides one, since a single mint
        can legitimately produce more than one event from the same feed.
        """
        return self.signature or self.mint or ""

    @property
    def is_ephemeral(self) -> bool:
        return self.event_kind in EPHEMERAL_KINDS

    @property
    def is_candidate(self) -> bool:
        """A launch worth putting on the desk."""
        return self.event_kind in (EventKind.CREATE, EventKind.MIGRATE) and bool(self.mint)


class Consumer(abc.ABC):
    """A feed that yields RawEvents until it fails or is cancelled.

    Implementations must not retry internally. Reconnection, backoff and
    liveness belong to the supervisor so every transport gets identical stall
    detection rather than three subtly different versions of it.
    """

    provider: str

    #: True when the source is exhaustible (a capture file). A live feed's clean
    #: EOF means the server hung up and the supervisor must reconnect; a finite
    #: source's EOF means the work is done and reconnecting replays it forever.
    finite: bool = False

    #: Seconds of total silence after which the supervisor declares the feed
    #: dead and restarts it, regardless of what the socket claims.
    #:
    #: 3.8.1's 3-second rule is specifically a slot-monotonicity watchdog on a
    #: gRPC stream, and there are no slots on a websocket. Launch events arrive
    #: irregularly -- roughly 42k/day is one every couple of seconds on average
    #: but bursty -- so a 3s threshold on event arrival would restart a healthy
    #: feed constantly. Each consumer sets its own honest threshold.
    idle_timeout_seconds: float = 120.0

    @abc.abstractmethod
    def subscription_descriptor(self) -> dict:
        """Exact subscription requested, stored verbatim in `streams`."""

    @abc.abstractmethod
    def stream(self) -> AsyncIterator[RawEvent]:
        """Yield events. Raise on failure; never swallow and retry."""

    async def aclose(self) -> None:
        """Release transport resources. Must be safe to call twice."""
        return None
