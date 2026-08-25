"""Supervisor behaviour: restart on failure, and detect a stall that the
socket never reports.

CLAUDE.md 3.8.1 calls silent stalls the number one killer -- no error, no EOF,
no exception, the loop just waits forever. A test that only exercises a raised
exception does not test that case at all, so the stall test below hangs a
consumer deliberately and asserts the watchdog kills it.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from trenches.stream.base import Consumer, EventKind, RawEvent
from trenches.stream.supervisor import Supervisor


def _event(slot: int, kind: str = EventKind.OTHER) -> RawEvent:
    return RawEvent(signature=f"sig{slot}", slot=slot, provider="test", event_kind=kind)


class ScriptedConsumer(Consumer):
    """Yields a batch, then does whatever the script says: fail, hang, or end."""

    provider = "test"
    attempts = 0

    def __init__(self, batches: list[list[RawEvent]], *, then: str = "eof") -> None:
        self._batches = batches
        self._then = then
        self.closed = 0

    def subscription_descriptor(self) -> dict:
        return {"provider": self.provider}

    async def stream(self) -> AsyncIterator[RawEvent]:
        index = ScriptedConsumer.attempts
        ScriptedConsumer.attempts += 1
        if index < len(self._batches):
            for event in self._batches[index]:
                yield event
        if self._then == "raise":
            raise ConnectionResetError("upstream dropped the connection")
        if self._then == "hang":
            # The failure mode being tested: connection alive, data plane dead.
            await asyncio.Event().wait()

    async def aclose(self) -> None:
        self.closed += 1


@pytest.fixture(autouse=True)
def _reset():
    ScriptedConsumer.attempts = 0


async def test_reconnects_after_error_and_resumes():
    consumer = ScriptedConsumer([[_event(1)], [_event(2)], [_event(3)]], then="raise")
    sup = Supervisor(lambda: consumer, backoff_min=0.001, backoff_max=0.002)

    seen: list[int] = []
    async for event in sup.run():
        seen.append(event.slot)
        if len(seen) == 3:
            break
    assert seen == [1, 2, 3]
    assert sup.reconnects >= 2


async def test_watchdog_kills_a_silent_stall():
    """No exception is ever raised by the consumer here. Only the watchdog can
    notice, and it must notice via slot progress rather than socket health."""
    consumer = ScriptedConsumer([[_event(10)], [_event(11)]], then="hang")
    sup = Supervisor(
        lambda: consumer, watchdog_seconds=0.2, backoff_min=0.001, backoff_max=0.002
    )

    seen: list[int] = []
    async def drain():
        async for event in sup.run():
            seen.append(event.slot)
            if len(seen) == 2:
                return

    await asyncio.wait_for(drain(), timeout=5.0)
    assert seen == [10, 11]
    assert sup.reconnects >= 1, "watchdog did not restart a stalled stream"


async def test_backoff_is_bounded_and_jittered():
    sup = Supervisor(lambda: ScriptedConsumer([]), backoff_min=1.0, backoff_max=30.0)
    delays = [sup._backoff(attempt) for attempt in range(1, 40) for _ in range(5)]
    assert all(1.0 <= d <= 30.0 for d in delays)
    assert len(set(delays)) > 1, "no jitter: simultaneous restarts would retry in lockstep"


async def test_consumer_is_closed_on_every_restart():
    consumer = ScriptedConsumer([[_event(1)], [_event(2)]], then="raise")
    sup = Supervisor(lambda: consumer, backoff_min=0.001, backoff_max=0.002)
    seen = 0
    async for _ in sup.run():
        seen += 1
        if seen == 2:
            break
    assert consumer.closed >= 1, "transport resources leaked across a reconnect"


async def test_max_slot_tracks_progress():
    consumer = ScriptedConsumer([[_event(5), _event(9), _event(7)]], then="raise")
    sup = Supervisor(lambda: consumer, backoff_min=0.001, backoff_max=0.002)
    seen = 0
    async for _ in sup.run():
        seen += 1
        if seen == 3:
            break
    assert sup.max_slot == 9


class FiniteConsumer(ScriptedConsumer):
    """A capture file: exhaustible, so EOF is completion, not a disconnect."""

    finite = True


async def test_finite_source_is_not_restarted_on_eof():
    """A live feed's clean EOF means reconnect. A capture file's means done --
    restarting it replays the same events forever and inflates every counter."""
    consumer = FiniteConsumer([[_event(1), _event(2)]], then="eof")
    sup = Supervisor(lambda: consumer, backoff_min=0.001, backoff_max=0.002)

    seen = [e.slot async for e in sup.run()]
    assert seen == [1, 2]
    assert sup.reconnects == 0


async def test_live_source_still_reconnects_on_eof():
    consumer = ScriptedConsumer([[_event(1)], [_event(2)]], then="eof")
    sup = Supervisor(lambda: consumer, backoff_min=0.001, backoff_max=0.002)
    seen = []
    async for event in sup.run():
        seen.append(event.slot)
        if len(seen) == 2:
            break
    assert seen == [1, 2]
    assert sup.reconnects >= 1
