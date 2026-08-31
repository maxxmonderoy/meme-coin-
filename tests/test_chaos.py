"""Fault injection: cause the failures instead of waiting seven days for them.

WHY THIS EXISTS. The original week-1 gate was a seven-day soak, and its purpose
was to prove the 3.8.1 machinery works -- silent stalls, backpressure,
duplicates on replay, decoder breakage. But a soak only proves that whatever
happened to occur was survived; it cannot prove a failure that did not happen
during the window was handled. Worse, on a shared machine the soak mostly
measures how often somebody logs out.

These tests break the stream on purpose and assert recovery. That is strictly
better evidence than passive uptime: a soak says "nothing went wrong", this says
"we made it go wrong and nothing was lost".

None of these touch the network.
"""
from __future__ import annotations

import asyncio

import pytest
from conftest import pubkey

from trenches.db import repo
from trenches.stream.base import Consumer, EventKind, RawEvent
from trenches.stream.supervisor import Supervisor


def _event(n: int, provider: str = "fake") -> RawEvent:
    return RawEvent(
        provider=provider, event_kind=EventKind.CREATE,
        mint=pubkey(n), signature=f"sig{n}", slot=n,
    )


class ScriptedConsumer(Consumer):
    """A feed that fails exactly how the script says, then behaves."""

    provider = "fake"
    idle_timeout_seconds = 0.3

    #: shared across factory-produced instances so a test can count restarts
    def __init__(self, script: list, state: dict) -> None:
        self._script = script
        self._state = state

    def subscription_descriptor(self) -> dict:
        return {"scripted": True}

    async def stream(self):
        self._state["starts"] = self._state.get("starts", 0) + 1
        step = self._script[min(self._state["starts"] - 1, len(self._script) - 1)]
        if step == "raise":
            raise ConnectionResetError("socket died mid-stream")
        if step == "silence":
            # The 3.8.1 killer: socket open, no error, no EOF, nothing arriving.
            await asyncio.sleep(60)
            return
        if step == "eof":
            return
        for n in step:
            yield _event(n)
            await asyncio.sleep(0)

    async def aclose(self) -> None:
        return None


async def _drain(sup: Supervisor, want: int, deadline: float = 5.0) -> list[RawEvent]:
    got: list[RawEvent] = []

    async def pump():
        async for event in sup.run():
            got.append(event)
            if len(got) >= want:
                return

    await asyncio.wait_for(pump(), timeout=deadline)
    return got


# -- 3.8.1: the silent stall ------------------------------------------------

async def test_a_silent_socket_is_detected_and_restarted():
    """No error, no EOF, nothing arriving -- the case a soak may never see.

    The watchdog is the only defence that fires here: the socket is perfectly
    healthy and would wait forever.
    """
    state: dict = {}
    sup = Supervisor(
        lambda: ScriptedConsumer(["silence", [1, 2]], state),
        watchdog_seconds=0.3, backoff_min=0.01, backoff_max=0.02,
    )
    got = await _drain(sup, 2)
    assert [e.slot for e in got] == [1, 2]
    assert sup.reconnects >= 1, "the watchdog never fired on a silent feed"


async def test_a_dead_socket_reconnects_and_keeps_delivering():
    state: dict = {}
    sup = Supervisor(
        lambda: ScriptedConsumer(["raise", [7, 8, 9]], state),
        watchdog_seconds=5, backoff_min=0.01, backoff_max=0.02,
    )
    got = await _drain(sup, 3)
    assert [e.slot for e in got] == [7, 8, 9]
    assert sup.reconnects >= 1


async def test_a_clean_eof_from_a_live_feed_is_not_the_end():
    """A live feed's EOF means the server hung up, not that work is done."""
    state: dict = {}
    sup = Supervisor(
        lambda: ScriptedConsumer(["eof", [4]], state),
        watchdog_seconds=5, backoff_min=0.01, backoff_max=0.02,
    )
    got = await _drain(sup, 1)
    assert [e.slot for e in got] == [4]


async def test_backoff_is_bounded_and_recovers_after_repeated_failure():
    """Five failures in a row must not become an unbounded retry storm."""
    state: dict = {}
    sup = Supervisor(
        lambda: ScriptedConsumer(["raise", "raise", "raise", "raise", [5]], state),
        watchdog_seconds=5, backoff_min=0.01, backoff_max=0.05,
    )
    got = await _drain(sup, 1, deadline=10.0)
    assert [e.slot for e in got] == [5]
    assert sup.reconnects == 4


# -- 3.8.4: duplicates are guaranteed on replay -----------------------------

async def test_replayed_events_do_not_double_write(sqlite_db):
    """A reconnect replays the tail. Storage must absorb it silently."""
    stream_id = await repo.open_stream(
        sqlite_db, feeds=["fake"], subscription={}, code_version="chaos")
    event = _event(42)
    assert await repo.insert_raw_event(sqlite_db, event, stream_id) is True
    for _ in range(5):
        assert await repo.insert_raw_event(sqlite_db, event, stream_id) is False
    n = await sqlite_db.fetchval("select count(*) from raw_events where mint = ?", event.mint)
    assert n == 1


async def test_the_same_launch_from_two_feeds_is_stored_twice_on_purpose(sqlite_db):
    """Dedupe is on mint for CANDIDATES; storage identity is (feed, event_id).
    Collapsing these would destroy the feed-race measurement."""
    stream_id = await repo.open_stream(
        sqlite_db, feeds=["a", "b"], subscription={}, code_version="chaos")
    mint = pubkey(43)
    a = RawEvent(provider="a", event_kind=EventKind.CREATE, mint=mint, signature="sigA")
    b = RawEvent(provider="b", event_kind=EventKind.CREATE, mint=mint, signature=None)
    assert await repo.insert_raw_event(sqlite_db, a, stream_id) is True
    assert await repo.insert_raw_event(sqlite_db, b, stream_id) is True
    n = await sqlite_db.fetchval("select count(*) from raw_events where mint = ?", mint)
    assert n == 2


# -- 3.8.8: decoder breakage is silent, so watch the RATE -------------------

async def test_a_ping_is_liveness_only_and_never_journalled():
    """A feed delivering only pings looks alive and produces no data --
    exactly the shape of a decoder that broke without erroring."""
    state: dict = {}

    class PingOnly(ScriptedConsumer):
        async def stream(self):
            self._state["starts"] = self._state.get("starts", 0) + 1
            if self._state["starts"] == 1:
                for _ in range(3):
                    yield RawEvent(provider="fake", event_kind=EventKind.PING)
                    await asyncio.sleep(0)
                await asyncio.sleep(60)   # then go quiet
            else:
                yield _event(99)

    sup = Supervisor(lambda: PingOnly([], state),
                     watchdog_seconds=0.3, backoff_min=0.01, backoff_max=0.02)
    got = await _drain(sup, 1)
    assert [e.slot for e in got] == [99]
    assert all(e.event_kind != EventKind.PING for e in got), "pings must not reach the journal"


# -- gap attribution --------------------------------------------------------

async def test_a_clean_shutdown_gap_is_attributed_to_the_host(sqlite_db):
    """The reformulated gate: a gap is only forgivable if a SIGTERM explains it."""
    s1 = await repo.open_stream(sqlite_db, feeds=["f"], subscription={}, code_version="c")
    await repo.close_stream(sqlite_db, s1, "clean shutdown")
    report = await repo.gap_attribution(sqlite_db, days=7)
    assert report["system_caused"] == 0


async def test_an_unexplained_gap_counts_against_the_system(sqlite_db):
    """A session that vanished with no stop reason is NOT given the benefit of
    the doubt -- otherwise a crash hides behind 'probably the logout'."""
    s1 = await repo.open_stream(sqlite_db, feeds=["f"], subscription={}, code_version="c")
    await sqlite_db.execute(
        "update streams set stopped_at = ?, stop_reason = ? where id = ?",
        "2026-08-01T00:00:00+00:00", "ConnectionResetError: boom", s1)
    await repo.open_stream(sqlite_db, feeds=["f"], subscription={}, code_version="c")
    report = await repo.gap_attribution(sqlite_db, days=3650)
    assert report["system_caused"] == 1
    assert any("ConnectionResetError" in g["reason"] for g in report["gaps"])


@pytest.mark.parametrize("reason", ["clean shutdown"])
async def test_only_a_clean_shutdown_earns_host_attribution(sqlite_db, reason):
    s1 = await repo.open_stream(sqlite_db, feeds=["f"], subscription={}, code_version="c")
    await sqlite_db.execute(
        "update streams set stopped_at = ?, stop_reason = ? where id = ?",
        "2026-08-01T00:00:00+00:00", reason, s1)
    await repo.open_stream(sqlite_db, feeds=["f"], subscription={}, code_version="c")
    report = await repo.gap_attribution(sqlite_db, days=3650)
    assert report["host_caused"] == 1
    assert report["system_caused"] == 0
