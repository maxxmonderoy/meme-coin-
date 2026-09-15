"""Structural events: the signals that fire before the price does.

A price stop assumes a bid that is not there mid-rug. These detectors are the
alternative, so what matters is that they fire on real state changes, do NOT
fire on noise, and carry timestamps that can be ordered against the price path.
"""
from __future__ import annotations

import datetime as dt

import pytest

from trenches.structural import detectors as d

T0 = dt.datetime(2026, 9, 1, tzinfo=dt.UTC)


def obs(seconds: int, *, liquidity=None, status="indexed"):
    return {"observed_at": (T0 + dt.timedelta(seconds=seconds)).isoformat(),
            "liquidity_usd": liquidity, "status": status}


# -- from the price path ---------------------------------------------------

def test_liquidity_collapse_fires_on_a_pull():
    events = d.from_price_path(obs(0, liquidity="10000"), obs(30, liquidity="1000"))
    assert [e.event_type for e in events] == [d.LIQUIDITY_COLLAPSE]
    assert events[0].delta_pct == pytest.approx(0.9)
    assert events[0].severity == d.Severity.EXIT


def test_ordinary_drift_is_not_a_collapse():
    """Firing on noise makes the timeline useless for the ordering question."""
    assert d.from_price_path(obs(0, liquidity="10000"), obs(30, liquidity="9200")) == []


def test_a_pool_disappearing_is_its_own_event():
    events = d.from_price_path(obs(0, liquidity="10000"), obs(30, status="no_pool"))
    assert [e.event_type for e in events] == [d.POOL_DISAPPEARED]
    assert events[0].severity == d.Severity.EXIT


def test_not_yet_indexed_is_never_a_pool_disappearance():
    """The cold start must not manufacture a structural death."""
    assert d.from_price_path(obs(0, liquidity="10000"),
                             obs(30, status="not_yet_indexed")) == []


def test_zero_liquidity_before_is_not_a_collapse():
    """A token that had nothing and still has nothing has not collapsed."""
    assert d.from_price_path(obs(0, liquidity="0"), obs(30, liquidity="0")) == []


def test_liquidity_rising_produces_nothing():
    assert d.from_price_path(obs(0, liquidity="1000"), obs(30, liquidity="9000")) == []


def test_event_timestamp_matches_the_observation_it_came_from():
    """The whole question is whether structural fires BEFORE price, so the
    timestamps must be comparable to the path at the same precision."""
    events = d.from_price_path(obs(0, liquidity="10000"), obs(45, liquidity="500"))
    assert events[0].observed_at == T0 + dt.timedelta(seconds=45)
    assert events[0].detected_at == events[0].observed_at


# -- from RugCheck ---------------------------------------------------------

def test_rugged_flipping_true_fires_once():
    first = d.from_rugcheck({"rugged": False}, {"rugged": True}, observed_at=T0)
    assert [e.event_type for e in first] == [d.RUGGED]
    again = d.from_rugcheck({"rugged": True}, {"rugged": True}, observed_at=T0)
    assert again == []


def test_rugged_fires_on_the_first_poll_with_no_baseline():
    """Some facts are true in isolation and must not wait for a second poll."""
    events = d.from_rugcheck(None, {"rugged": True}, observed_at=T0)
    assert [e.event_type for e in events] == [d.RUGGED]


def test_a_change_needs_two_observations():
    """Inventing a baseline would make the first poll on any token look like a
    collapse."""
    assert d.from_rugcheck(None, {"bundlers": {"totalPercentage": 5}},
                           observed_at=T0) == []


def test_bundlers_distributing_fires_on_the_fall_not_the_level():
    """3.4: the delta is more informative than either level -- it distinguishes
    'they hold a lot' from 'they are selling it to you right now'."""
    events = d.from_rugcheck(
        {"bundlers": {"totalPercentage": 40}},
        {"bundlers": {"totalPercentage": 12}}, observed_at=T0)
    assert d.BUNDLERS_DISTRIBUTING in [e.event_type for e in events]
    # A high but STABLE share is not distribution.
    assert d.from_rugcheck({"bundlers": {"totalPercentage": 40}},
                           {"bundlers": {"totalPercentage": 39}}, observed_at=T0) == []


def test_dev_selling_is_detected():
    events = d.from_rugcheck({"dev": {"percentage": 5}}, {"dev": {"percentage": 1}},
                             observed_at=T0)
    assert d.DEV_SELLING in [e.event_type for e in events]


def test_holder_count_inverting_is_detected():
    events = d.from_rugcheck({"totalHolders": 500}, {"totalHolders": 300},
                             observed_at=T0)
    assert d.HOLDERS_INVERTING in [e.event_type for e in events]


def test_holders_growing_is_not_an_event():
    assert d.from_rugcheck({"totalHolders": 300}, {"totalHolders": 500},
                           observed_at=T0) == []


def test_lock_expiry_inside_the_horizon_warns_and_past_it_exits():
    soon = (T0 + dt.timedelta(hours=2)).isoformat()
    warn = d.from_rugcheck({}, {"lockerUnlockDate": soon}, observed_at=T0)
    assert [e.event_type for e in warn] == [d.LOCK_EXPIRING]
    assert warn[0].severity == d.Severity.WARN

    past = (T0 - dt.timedelta(hours=1)).isoformat()
    gone = d.from_rugcheck({}, {"lockerUnlockDate": past}, observed_at=T0)
    assert [e.event_type for e in gone] == [d.LOCK_EXPIRED]
    assert gone[0].severity == d.Severity.EXIT


def test_a_distant_lock_expiry_is_not_an_event():
    far = (T0 + dt.timedelta(days=30)).isoformat()
    assert d.from_rugcheck({}, {"lockerUnlockDate": far}, observed_at=T0) == []


def test_unlock_dates_accept_epoch_seconds_and_milliseconds():
    epoch = int((T0 + dt.timedelta(hours=1)).timestamp())
    assert d.from_rugcheck({}, {"lockerUnlockDate": epoch}, observed_at=T0)
    assert d.from_rugcheck({}, {"lockerUnlockDate": epoch * 1000}, observed_at=T0)


def test_thresholds_are_parameters():
    strict = d.Thresholds(liquidity_drop_pct=0.05)
    assert d.from_price_path(obs(0, liquidity="1000"), obs(30, liquidity="900"),
                             thresholds=strict)
    assert d.from_price_path(obs(0, liquidity="1000"), obs(30, liquidity="900")) == []


# -- persistence and cache bypass -----------------------------------------

class FakeRugCheck:
    def __init__(self, payloads):
        self._payloads = list(payloads)
        self.fresh_flags: list[bool] = []

    async def report(self, mint, *, fresh=False):
        from trenches.enrich.rugcheck import Probe

        self.fresh_flags.append(fresh)
        payload = self._payloads.pop(0) if self._payloads else {}
        return Probe(mint, 200, 5, fresh, payload)


async def test_poller_always_requests_a_cache_bypass(any_db):
    """RugCheck's report is cached behind its own rug detector: a token that
    rugged still returned rugged:false thirty seconds later. Reusing a cached
    verdict on a held position is the exact failure this catches."""
    from trenches.db import repo
    from trenches.structural.poller import StructuralPoller

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MP", stream_id=1, feed="f", fields={})

    client = FakeRugCheck([{"rugged": False}, {"rugged": True}])
    poller = StructuralPoller(any_db, client=client)
    await poller.poll_mint("MP", now=T0)
    await poller.poll_mint("MP", now=T0 + dt.timedelta(seconds=30))

    assert client.fresh_flags == [True, True]
    events = await repo.events_for_mint(any_db, "MP")
    assert [e["event_type"] for e in events] == [d.RUGGED]


async def test_events_persist_with_before_and_after_values(any_db):
    from trenches.db import repo
    from trenches.structural.poller import StructuralPoller

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MQ", stream_id=1, feed="f", fields={})
    client = FakeRugCheck([
        {"bundlers": {"totalPercentage": 40}},
        {"bundlers": {"totalPercentage": 10}},
    ])
    poller = StructuralPoller(any_db, client=client)
    await poller.poll_mint("MQ", now=T0)
    await poller.poll_mint("MQ", now=T0 + dt.timedelta(minutes=1))

    row = (await repo.events_for_mint(any_db, "MQ"))[0]
    assert row["event_type"] == d.BUNDLERS_DISTRIBUTING
    assert row["before_value"] == "40" and row["after_value"] == "10"
    assert row["delta_pct"] == pytest.approx(0.75)


async def test_liquidity_events_are_derived_from_stored_paths_for_free(any_db):
    """Reads rows the sampler already collected rather than spending a request."""
    from trenches.db import repo
    from trenches.structural.poller import StructuralPoller

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MR", stream_id=1, feed="f", fields={})
    for i, liq in enumerate(["9000", "8800", "400"]):
        await repo.record_observation(any_db, mint="MR", fields={
            "observed_at": T0 + dt.timedelta(seconds=30 * i),
            "scheduled_for": T0 + dt.timedelta(seconds=30 * i),
            "source": "dexscreener", "status": "indexed",
            "liquidity_usd": liq, "age_seconds": 30 * i})

    poller = StructuralPoller(any_db, client=FakeRugCheck([]))
    found = await poller.scan_paths("MR")
    assert [e.event_type for e in found] == [d.LIQUIDITY_COLLAPSE]
    stored = await repo.events_for_mint(any_db, "MR")
    assert len(stored) == 1
    # Ordering against the path is preserved exactly.
    assert stored[0]["observed_at"] == (T0 + dt.timedelta(seconds=60)).isoformat()


async def test_scanning_twice_does_not_duplicate_events(any_db):
    from trenches.db import repo
    from trenches.structural.poller import StructuralPoller

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="MS", stream_id=1, feed="f", fields={})
    for i, liq in enumerate(["9000", "100"]):
        await repo.record_observation(any_db, mint="MS", fields={
            "observed_at": T0 + dt.timedelta(seconds=30 * i),
            "scheduled_for": T0 + dt.timedelta(seconds=30 * i),
            "source": "dexscreener", "status": "indexed",
            "liquidity_usd": liq, "age_seconds": 30 * i})
    poller = StructuralPoller(any_db, client=FakeRugCheck([]))
    await poller.scan_paths("MS")
    await poller.scan_paths("MS")
    assert len(await repo.events_for_mint(any_db, "MS")) == 1
