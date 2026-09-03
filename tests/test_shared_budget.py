"""The cross-process rate budget, and the outcomes derivation it makes possible."""
from __future__ import annotations

import datetime as dt

from trenches.label.shared_budget import SharedBudget
from trenches.sample import derive as derive_mod

T0 = dt.datetime(2026, 9, 3, tzinfo=dt.UTC)


# -- shared budget ---------------------------------------------------------

async def test_two_clients_share_one_budget(any_db):
    """The whole point: `label` and `sample` are separate processes, so an
    in-process bucket lets them reach 120 req/min against a documented 60."""
    a = SharedBudget(any_db, rate_per_minute=60, capacity_seconds=1.0)
    b = SharedBudget(any_db, rate_per_minute=60, capacity_seconds=1.0)
    await a.ensure()

    now = T0
    spent = 0
    for _ in range(10):
        if await a.try_acquire(now=now):
            spent += 1
        if await b.try_acquire(now=now):
            spent += 1
    # One second of capacity at 60/min is one token; a frozen clock refills none.
    assert spent == 1


async def test_tokens_refill_over_time(any_db):
    budget = SharedBudget(any_db, rate_per_minute=60, capacity_seconds=1.0)
    await budget.ensure()
    assert await budget.try_acquire(now=T0) is True
    assert await budget.try_acquire(now=T0) is False
    # 60/min is one per second.
    assert await budget.try_acquire(now=T0 + dt.timedelta(seconds=1.5)) is True


async def test_capacity_bounds_the_burst(any_db):
    """A long idle period must not bank an unlimited burst."""
    budget = SharedBudget(any_db, rate_per_minute=60, capacity_seconds=1.0)
    await budget.ensure()
    later = T0 + dt.timedelta(hours=1)
    assert await budget.try_acquire(now=later) is True
    assert await budget.try_acquire(now=later) is False


async def test_spend_is_recorded(any_db):
    budget = SharedBudget(any_db, rate_per_minute=60)
    await budget.ensure()
    await budget.try_acquire(now=T0)
    row = await any_db.fetchrow("select * from rate_budget where name = ?", "dexscreener")
    assert row["spent_total"] == 1


async def test_ensure_is_idempotent(any_db):
    budget = SharedBudget(any_db)
    await budget.ensure()
    await budget.ensure()
    assert await any_db.fetchval("select count(*) from rate_budget") == 1


def test_the_client_accepts_a_shared_budget():
    """Regression: both collectors built their own limiter and neither knew
    about the other."""
    from trenches.label.dexscreener import DexScreenerClient

    assert DexScreenerClient().shared_budget is None
    assert DexScreenerClient(shared_budget="x").shared_budget == "x"


def test_both_collectors_default_to_the_shared_budget(any_db):
    from trenches.label.labeler import Labeler
    from trenches.sample.sampler import Sampler

    assert Sampler(any_db)._client.shared_budget is not None
    assert Labeler(any_db)._client.shared_budget is not None


# -- deriving outcomes from the path ---------------------------------------

def _obs(seconds, *, status="indexed", liquidity="5000"):
    return {"observed_at": (T0 + dt.timedelta(seconds=seconds)).isoformat(),
            "status": status, "liquidity_usd": liquidity, "price_usd": "0.001"}


def test_nearest_picks_the_closest_observation_either_side():
    rows = [_obs(0), _obs(880), _obs(910), _obs(1200)]
    target = T0 + dt.timedelta(seconds=900)
    row, gap = derive_mod.nearest(rows, target, 300)
    assert row["observed_at"] == _obs(910)["observed_at"]
    assert gap == 10


def test_nearest_refuses_an_observation_outside_tolerance():
    """A 24h checkpoint must not be answered by a 20-minute-old sample just
    because the token stopped being observed."""
    rows = [_obs(1200)]
    target = T0 + dt.timedelta(hours=24)
    row, _ = derive_mod.nearest(rows, target, 7200)
    assert row is None


def test_status_mapping_keeps_the_cold_start_out_of_death():
    floor = 1000.0
    assert derive_mod.status_for(_obs(0, liquidity="5000"), floor) == "alive"
    assert derive_mod.status_for(_obs(0, liquidity="10"), floor) == "dead"
    assert derive_mod.status_for(_obs(0, status="not_yet_indexed"), floor) == "no_pool"
    assert derive_mod.status_for(_obs(0, status="error"), floor) == "error"


async def test_derivation_writes_horizon_rows_from_the_path(any_db):
    from trenches.db import repo

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="DV", stream_id=1, feed="f", fields={})
    for seconds in (0, 300, 900, 1800, 3600, 7200):
        await repo.record_observation(any_db, mint="DV", fields={
            "observed_at": T0 + dt.timedelta(seconds=seconds),
            "scheduled_for": T0 + dt.timedelta(seconds=seconds),
            "source": "dexscreener", "status": "indexed", "venue_kind": "dex",
            "price_usd": "0.001", "liquidity_usd": "5000", "age_seconds": seconds})

    written = await derive_mod.derive_for_mint(any_db, "DV", first_seen=T0)
    assert written.get("15m") is True
    assert written.get("1h") is True

    row = await any_db.fetchrow(
        "select * from outcomes where mint = ? and horizon = ?", "DV", "15m")
    assert row["status"] == "alive"
    assert row["source"] == "price_path"
    assert row["backfilled"] == 1
    assert row["lateness_seconds"] == 0          # exact 900s sample exists


async def test_a_cold_start_checkpoint_is_marked_ambiguous(any_db):
    """Part 2: at 15 minutes an unindexed token is unknown, not dead."""
    from trenches.db import repo

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="DC", stream_id=1, feed="f", fields={})
    await repo.record_observation(any_db, mint="DC", fields={
        "observed_at": T0 + dt.timedelta(seconds=900),
        "scheduled_for": T0 + dt.timedelta(seconds=900),
        "source": "dexscreener", "status": "not_yet_indexed", "age_seconds": 900})

    await derive_mod.derive_for_mint(any_db, "DC", first_seen=T0)
    row = await any_db.fetchrow(
        "select * from outcomes where mint = ? and horizon = ?", "DC", "15m")
    assert row["status"] == "no_pool"
    assert row["ambiguous_no_pool"] == 1


async def test_derivation_records_how_far_off_the_sample_was(any_db):
    """A 30-second-off sample and a two-hour-late poll are both honest; only
    one is useful, so the offset is stored rather than discarded."""
    from trenches.db import repo

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="DL", stream_id=1, feed="f", fields={})
    await repo.record_observation(any_db, mint="DL", fields={
        "observed_at": T0 + dt.timedelta(seconds=960),      # 60s past the 15m mark
        "scheduled_for": T0 + dt.timedelta(seconds=960),
        "source": "dexscreener", "status": "indexed",
        "price_usd": "0.001", "liquidity_usd": "5000", "age_seconds": 960})

    await derive_mod.derive_for_mint(any_db, "DL", first_seen=T0)
    row = await any_db.fetchrow(
        "select * from outcomes where mint = ? and horizon = ?", "DL", "15m")
    assert row["lateness_seconds"] == 60


async def test_derive_all_reports_what_it_could_not_answer(any_db):
    from trenches.db import repo

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="NP", stream_id=1, feed="f", fields={})
    await repo.admit_to_watch_set(any_db, mint="NP", cohort="control", first_seen=T0,
                                  expires_at=T0 + dt.timedelta(hours=24), next_due_at=T0)
    report = await derive_mod.derive_all(any_db)
    assert report.mints == 1
    assert report.skipped_no_observation == 1
    assert report.written == 0


# -- reporting honesty -----------------------------------------------------

async def test_lateness_is_reported_per_horizon(any_db):
    """A horizon observed hours late is indistinguishable in the report from one
    observed on time, and the two mean entirely different things."""
    from trenches.db import repo

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    for i, late in enumerate([0, 0, 9000, 9000]):
        mint = f"LT{i}"
        await repo.upsert_token(any_db, mint=mint, stream_id=1, feed="f", fields={})
        await repo.record_outcome(any_db, mint=mint, horizon="15m", fields={
            "scheduled_for": T0, "observed_at": T0 + dt.timedelta(seconds=late),
            "lateness_seconds": late, "status": "dead", "source": "dexscreener"})

    out = await repo.lateness_by_horizon(any_db, ("15m",))
    assert out["15m"]["n"] == 4
    assert out["15m"]["on_time"] == 2
    assert out["15m"]["p90"] == 9000


async def test_unique_detection_rate_is_not_the_sum_of_feed_sightings(any_db):
    """Two feeds seeing the SAME launch is one detection, not two. Counting
    events overstates the rate by roughly the overlap, which is most of it."""
    from trenches.db import repo
    from trenches.stream.base import EventKind, RawEvent

    sid = await repo.open_stream(any_db, feeds=["a", "b"], subscription={},
                                 code_version="v")
    for i in range(5):
        await repo.upsert_token(any_db, mint=f"UQ{i}", stream_id=sid, feed="pumpportal",
                                fields={})
        for feed in ("pumpportal", "rugcheck"):
            await repo.insert_raw_event(any_db, RawEvent(
                provider=feed, event_kind=EventKind.CREATE, mint=f"UQ{i}",
                signature=f"{feed}-{i}"), sid)

    rate = await repo.unique_detection_rate(any_db, hours=1)
    assert rate["unique_mints"] == 5          # not 10
    assert await any_db.fetchval("select count(*) from raw_events") == 10
