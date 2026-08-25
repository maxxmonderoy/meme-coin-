"""Transport-neutral event shape and the Consumer contract.

Everything downstream of here is transport-agnostic. That is the point: the
week-1 soak can run on a replay feed, and swapping in gRPC later must not
touch the pipeline, the journal, or the dedupe invariant.
"""
from __future__ import annotations

import abc
import datetime as dt
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Final


class EventKind:
    CREATE: Final = "create"
    TRADE: Final = "trade"
    MIGRATE: Final = "migrate"
    OTHER: Final = "other"
    SLOT: Final = "slot"      # never journalled; drives the watchdog and gap detection
    PING: Final = "ping"      # never journalled; liveness only


#: Kinds that are bookkeeping rather than observations. Never written to
#: raw_events regardless of retention mode.
EPHEMERAL_KINDS: Final = frozenset({EventKind.SLOT, EventKind.PING})


def _now() -> dt.datetime:
    return dt.datetime.now(tz=dt.UTC)


@dataclass(frozen=True, slots=True)
class RawEvent:
    """One observation from a feed.

    `commitment` is carried on every event because CLAUDE.md 3.8.7 forbids
    treating `processed` as authoritative -- the reconcile tier has to be
    visible at the point of storage, not inferred later.
    """

    signature: str
    slot: int
    provider: str
    event_kind: str
    commitment: str = "processed"
    program_id: str | None = None
    block_time: dt.datetime | None = None
    filter_source: str | None = None
    payload: dict | None = None
    received_at: dt.datetime = field(default_factory=_now)

    @property
    def dedupe_key(self) -> tuple[str, int]:
        """The 3.8.4 identity. Duplicates are guaranteed on replay/reconnect."""
        return (self.signature, self.slot)

    @property
    def is_ephemeral(self) -> bool:
        return self.event_kind in EPHEMERAL_KINDS


class Consumer(abc.ABC):
    """A feed that yields RawEvents until it fails or is cancelled.

    Implementations must not retry internally. Reconnection, backoff and
    liveness are the supervisor's job, so that every transport gets identical
    stall detection rather than three subtly different versions of it.
    """

    provider: str

    #: True when the source is exhaustible (a capture file) rather than a live
    #: feed. A live feed's clean EOF means the server closed the connection and
    #: the supervisor must reconnect; a finite source's EOF means the work is
    #: done and reconnecting would replay it forever.
    finite: bool = False

    @abc.abstractmethod
    def subscription_descriptor(self) -> dict:
        """Exact subscription being requested, stored verbatim in `streams`.

        This is what makes the 3.3 strict-vs-naive filter diff reproducible
        after the fact instead of a thing you remember doing.
        """

    @abc.abstractmethod
    def stream(self) -> AsyncIterator[RawEvent]:
        """Yield events. Raise on failure; never swallow and retry."""

    async def aclose(self) -> None:
        """Release transport resources. Must be safe to call twice."""
        return None
