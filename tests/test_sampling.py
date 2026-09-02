"""Cadence, budget derivation, cohorts, and the cold start.

The budget arithmetic is the load-bearing decision in this change: get it wrong
and either the endpoint bans us or the watch set is a fraction of what the rate
limit could carry.
"""
from __future__ import annotations

import datetime as dt
from collections import Counter

import pytest

from trenches.sample.cadence import (
    DEFAULT_MAX_AGE_SECONDS,
    CadenceError,
    compute_budget,
    interval_for_age,
    validate,
)
from trenches.sample.watchset import (
    CONTROL,
    FILTERED,
    Admitter,
    WatchSetError,
    assign_cohort,
    control_draw,
)

T0 = dt.datetime(2026, 9, 1, tzinfo=dt.UTC)


# -- cadence ---------------------------------------------------------------

@pytest.mark.parametrize("age,expected", [
    (0, 30), (299, 30), (599, 30),          # 0-10m dense
    (600, 60), (3599, 60),                  # 10-60m
    (3600, 300), (21599, 300),              # 1-6h
    (21600, 900), (86399, 900),             # 6-24h
])
def test_interval_per_age_bucket(age, expected):
    assert interval_for_age(age) == expected


def test_sampling_stops_at_max_age():
    """None is how a token leaves the watch set and frees its budget."""
    assert interval_for_age(DEFAULT_MAX_AGE_SECONDS) is None
    assert interval_for_age(DEFAULT_MAX_AGE_SECONDS + 1) is None


def test_the_first_ten_minutes_are_the_densest_bucket():
    """Median rugged lifespan is ~14 minutes and 85% of the sniper cohort exits
    by five, so a cadence that is not densest at the start cannot see the thing
    it exists to observe."""
    intervals = [interval_for_age(a) for a in (0, 300, 900, 5000, 40000)]
    assert intervals == sorted(intervals)      # monotonically coarser
    assert intervals[0] == 30


def test_negative_age_is_an_error_not_a_default():
    with pytest.raises(CadenceError):
        interval_for_age(-1)


def test_cadence_validation_rejects_a_request_storm():
    with pytest.raises(CadenceError, match="floor"):
        validate(((600, 1),))
    with pytest.raises(CadenceError, match="increase"):
        validate(((600, 30), (300, 60)))


# -- budget derivation -----------------------------------------------------

def test_budget_is_derived_from_batch_and_cadence_not_hardcoded():
    small = compute_budget(batch_size=10, requests_per_minute=60)
    large = compute_budget(batch_size=25, requests_per_minute=60)
    assert large.watch_set_max > small.watch_set_max
    assert large.admit_rate_per_minute / small.admit_rate_per_minute == pytest.approx(2.5)


def test_a_denser_cadence_shrinks_the_watch_set():
    """More observations per token means fewer tokens. The budget must respond
    to the cadence, or the 'limit' stops being one."""
    base = compute_budget(batch_size=25, requests_per_minute=60)
    dense = compute_budget(batch_size=25, requests_per_minute=60,
                           cadence=((600, 15), (3600, 30), (86400, 300)))
    assert dense.watch_set_max < base.watch_set_max


def test_the_seven_day_tail_costs_most_of_the_budget():
    """The finding that sets the 24h default: the 1-7d tail is the least
    informative window and the most expensive to observe."""
    day = compute_budget(batch_size=25, requests_per_minute=60,
                         max_age_seconds=24 * 3600)
    week = compute_budget(batch_size=25, requests_per_minute=60,
                          max_age_seconds=7 * 86400)
    assert week.admit_rate_per_minute < day.admit_rate_per_minute
    assert day.admit_rate_per_minute / week.admit_rate_per_minute > 1.5


def test_budget_leaves_headroom_below_the_documented_limit():
    """DexScreener exposes no rate-limit headers, so the bucket is the only
    thing preventing a ban. Running at 100% of somebody else's clock is not
    worth the extra tokens."""
    b = compute_budget(batch_size=25, requests_per_minute=60)
    assert b.observations_per_minute < 60 * 25


def test_bucket_shares_sum_to_one():
    b = compute_budget(batch_size=25, requests_per_minute=60)
    assert sum(x.share for x in b.buckets) == pytest.approx(1.0)


def test_coverage_against_real_launch_volume_is_reported():
    b = compute_budget(batch_size=25, requests_per_minute=60)
    coverage = b.coverage_of(42_000)
    assert 0 < coverage < 1        # we can never watch everything
    assert coverage == pytest.approx(b.admit_rate_per_minute / (42_000 / 1440))


# -- cohorts ---------------------------------------------------------------

def test_control_cohort_is_uniform():
    mints = [f"mint{i:06d}" for i in range(20_000)]
    counts = Counter(assign_cohort(m) for m in mints)
    assert 0.48 < counts[CONTROL] / len(mints) < 0.52


def test_cohort_assignment_is_stable_across_restarts():
    """A random draw at admission re-rolls on restart and on replay, so the same
    mint could land in different cohorts across runs."""
    mints = [f"mint{i}" for i in range(500)]
    first = [assign_cohort(m) for m in mints]
    second = [assign_cohort(m) for m in mints]
    assert first == second


def test_control_share_is_respected():
    mints = [f"mint{i:06d}" for i in range(20_000)]
    counts = Counter(assign_cohort(m, control_share=0.30) for m in mints)
    assert 0.28 < counts[CONTROL] / len(mints) < 0.32


def test_draws_span_the_unit_interval():
    draws = [control_draw(f"m{i}") for i in range(5000)]
    assert min(draws) < 0.01 and max(draws) > 0.99
    assert 0.48 < sum(draws) / len(draws) < 0.52


def test_a_control_mint_is_admitted_even_when_it_fails_the_filter():
    """This IS the control arm. If the filter could veto it, it would not be a
    control sample of all launches."""
    admitter = Admitter(watch_set_max=1000)
    mint = next(m for m in (f"m{i}" for i in range(1000))
                if assign_cohort(m) == CONTROL)
    assert admitter.consider(mint, passes_filter=False, current={}).admit is True


def test_a_filtered_mint_failing_the_filter_is_not_admitted():
    admitter = Admitter(watch_set_max=1000)
    mint = next(m for m in (f"m{i}" for i in range(1000))
                if assign_cohort(m) == FILTERED)
    decision = admitter.consider(mint, passes_filter=False, current={})
    assert decision.admit is False


def test_cohort_capacities_are_split_not_pooled():
    """A burst of filtered candidates must not crowd out the control arm --
    which is what a single shared cap would allow, silently, exactly in the
    periods when the filter is firing hardest."""
    admitter = Admitter(watch_set_max=1000, control_share=0.5)
    full_filtered = {FILTERED: 500, CONTROL: 0}
    control_mint = next(m for m in (f"m{i}" for i in range(1000))
                        if assign_cohort(m) == CONTROL)
    assert admitter.consider(control_mint, passes_filter=True,
                             current=full_filtered).admit is True


def test_starving_the_control_arm_is_refused():
    with pytest.raises(WatchSetError, match="floor"):
        Admitter(watch_set_max=1000, control_share=0.05)


# -- the sampler against a database, with a fake client --------------------

class FakeClient:
    """Stands in for DexScreener. Records what it was asked for."""

    def __init__(self, found=None, error=None, batch_size=25):
        self._found = found or {}
        self._error = error
        self.calls: list[list[str]] = []
        self.batch_size = batch_size

    async def tokens(self, mints):
        from trenches.label.dexscreener import BatchResult

        self.calls.append(list(mints))
        return BatchResult(
            requested=list(mints),
            found={m: self._found[m] for m in mints if m in self._found},
            status_code=None if self._error else 200,
            latency_ms=1, raw=None, error=self._error,
        )


def _view(mint, price="0.001", liquidity="5000"):
    from trenches.label.dexscreener import PairView

    return PairView(
        mint=mint, pair_address="P", dex_id="raydium", price_usd=price,
        liquidity_usd=liquidity, fdv_usd=None, market_cap_usd=None, volume_m5=None,
        volume_h1=None, volume_h24=None, txns_m5_buys=None, txns_m5_sells=None,
        txns_h1_buys=None, txns_h1_sells=None, txns_h24_buys=None, txns_h24_sells=None,
        price_change_h24=None, pair_created_at=None, raw={},
    )


async def _seed(db, mint="M1", first_seen=None):
    from trenches.db import repo

    await repo.open_stream(db, feeds=["x"], subscription={}, code_version="v")
    await repo.upsert_token(db, mint=mint, stream_id=1, feed="pumpportal", fields={})
    return first_seen or T0


async def test_a_brand_new_mint_with_no_pair_is_not_yet_indexed(any_db):
    """DexScreener returns nothing for a mint minutes old. Recording that as
    dead would manufacture deaths for exactly the youngest tokens."""
    from trenches.db import repo
    from trenches.sample.sampler import STATUS_NOT_YET, Sampler

    await _seed(any_db)
    sampler = Sampler(any_db, client=FakeClient(found={}))
    await sampler.admit("M1", T0, passes_filter=True, now=T0)
    await sampler.tick(now=T0 + dt.timedelta(seconds=60))

    path = await repo.path_for_mint(any_db, "M1")
    assert len(path) == 1
    assert path[0]["status"] == STATUS_NOT_YET


async def test_the_same_absence_hours_later_is_a_real_no_pool(any_db):
    """Early absence and late absence are different facts."""
    from trenches.db import repo
    from trenches.sample.sampler import STATUS_NO_POOL, Sampler

    await _seed(any_db, "M2")
    sampler = Sampler(any_db, client=FakeClient(found={}))
    await sampler.admit("M2", T0, passes_filter=True, now=T0)
    await sampler.tick(now=T0 + dt.timedelta(hours=3))

    path = await repo.path_for_mint(any_db, "M2")
    assert path[0]["status"] == STATUS_NO_POOL


async def test_an_indexed_pair_records_price_and_liquidity(any_db):
    from trenches.db import repo
    from trenches.sample.sampler import STATUS_INDEXED, Sampler

    await _seed(any_db, "M3")
    sampler = Sampler(any_db, client=FakeClient(found={"M3": _view("M3")}))
    await sampler.admit("M3", T0, passes_filter=True, now=T0)
    await sampler.tick(now=T0 + dt.timedelta(seconds=60))

    row = (await repo.path_for_mint(any_db, "M3"))[0]
    assert row["status"] == STATUS_INDEXED
    assert row["price_usd"] == "0.001"
    assert row["liquidity_usd"] == "5000"
    assert row["venue_kind"] == "dex"


async def test_next_due_follows_the_cadence_and_ages_out(any_db):
    from trenches.sample.sampler import Sampler

    await _seed(any_db, "M4")
    sampler = Sampler(any_db, client=FakeClient(found={"M4": _view("M4")}))
    await sampler.admit("M4", T0, passes_filter=True, now=T0)

    await sampler.tick(now=T0 + dt.timedelta(seconds=60))
    row = await any_db.fetchrow("select * from watch_set where mint = ?", "M4")
    gap = dt.datetime.fromisoformat(row["next_due_at"]) - (T0 + dt.timedelta(seconds=60))
    assert gap.total_seconds() == 30          # still in the 0-10m bucket

    await sampler.tick(now=T0 + dt.timedelta(hours=25))
    row = await any_db.fetchrow("select * from watch_set where mint = ?", "M4")
    assert row["state"] == "expired"          # past max age, budget freed
    assert row["next_due_at"] is None


async def test_no_duplicate_observations_for_one_scheduled_slot(any_db):
    """A restart must not double-record. The (mint, observed_at) key is the
    durable half of that guarantee."""
    from trenches.db import repo
    from trenches.sample.sampler import Sampler

    await _seed(any_db, "M5")
    sampler = Sampler(any_db, client=FakeClient(found={"M5": _view("M5")}))
    await sampler.admit("M5", T0, passes_filter=True, now=T0)
    at = T0 + dt.timedelta(seconds=60)
    await sampler.tick(now=at)
    await sampler.tick(now=at)      # same instant, e.g. after a restart

    assert len(await repo.path_for_mint(any_db, "M5")) == 1


async def test_nothing_due_is_not_an_error(any_db):
    from trenches.sample.sampler import Sampler

    await _seed(any_db, "M6")
    sampler = Sampler(any_db, client=FakeClient())
    await sampler.admit("M6", T0, passes_filter=True, now=T0)
    report = await sampler.tick(now=T0)      # first observation not due yet
    assert report == {"due": 0, "observed": 0, "batches": 0}


async def test_requests_are_chunked_to_the_batch_size(any_db):
    from trenches.db import repo
    from trenches.sample.sampler import Sampler

    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")
    client = FakeClient()
    sampler = Sampler(any_db, client=client, batch_size=5)
    for i in range(12):
        mint = f"B{i}"
        await repo.upsert_token(any_db, mint=mint, stream_id=1, feed="f", fields={})
        await sampler.admit(mint, T0, passes_filter=True, now=T0)
    await sampler.tick(now=T0 + dt.timedelta(seconds=60))

    assert all(len(c) <= 5 for c in client.calls)
    assert sum(len(c) for c in client.calls) == 12


async def test_a_vendor_error_is_recorded_and_does_not_lose_the_watch(any_db):
    """A failed observation must not silently drop the token from the set."""
    from trenches.db import repo
    from trenches.sample.sampler import STATUS_ERROR, Sampler

    await _seed(any_db, "M7")
    sampler = Sampler(any_db, client=FakeClient(error="timeout"))
    await sampler.admit("M7", T0, passes_filter=True, now=T0)
    await sampler.tick(now=T0 + dt.timedelta(seconds=60))

    row = (await repo.path_for_mint(any_db, "M7"))[0]
    assert row["status"] == STATUS_ERROR
    watch = await any_db.fetchrow("select * from watch_set where mint = ?", "M7")
    assert watch["state"] == "active" and watch["misses"] == 1


async def test_sampler_failure_does_not_touch_the_ingest_path(any_db):
    """3.1/brief: if the sampler dies the stream must not notice. It shares no
    queue, no socket and no task group -- this asserts the isolation rather than
    assuming it."""
    import os

    from trenches.config import Config
    from trenches.db import repo
    from trenches.pipeline.worker import Ingest
    from trenches.sample.sampler import Sampler
    from trenches.stream.base import EventKind, RawEvent
    os.environ.setdefault("TRENCHES_DSN", "sqlite://:memory:")
    await repo.open_stream(any_db, feeds=["x"], subscription={}, code_version="v")

    class Exploding(FakeClient):
        async def tokens(self, mints):
            raise RuntimeError("vendor exploded")

    sampler = Sampler(any_db, client=Exploding())
    await repo.upsert_token(any_db, mint="M8", stream_id=1, feed="f", fields={})
    await sampler.admit("M8", T0, passes_filter=True, now=T0)
    with pytest.raises(RuntimeError):
        await sampler.tick(now=T0 + dt.timedelta(seconds=60))

    # The ingest path is entirely unaffected: still accepts and stores events.
    ingest = Ingest(any_db, Config.from_env(), 1)
    assert ingest.offer(RawEvent(provider="pumpportal", event_kind=EventKind.CREATE,
                                 mint="M9", signature="S9")) is True
    await ingest._handle(RawEvent(provider="pumpportal", event_kind=EventKind.CREATE,
                                  mint="M9", signature="S9", payload={"creates": []}))
    assert await any_db.fetchval("select count(*) from raw_events") == 1


async def test_a_late_admission_lands_on_the_launch_relative_schedule(any_db):
    """Two tokens of the same age must carry the same sampling density,
    regardless of when we happened to notice them."""
    from trenches.sample.sampler import Sampler

    await _seed(any_db, "LATE")
    sampler = Sampler(any_db, client=FakeClient())
    # Admitted 20 minutes after launch: that is the 10-60m bucket, 60s cadence,
    # not the 30s cadence a brand new mint would get.
    admitted_at = T0 + dt.timedelta(minutes=20)
    await sampler.admit("LATE", T0, passes_filter=True, now=admitted_at)

    row = await any_db.fetchrow("select * from watch_set where mint = ?", "LATE")
    due = dt.datetime.fromisoformat(row["next_due_at"])
    assert (due - admitted_at).total_seconds() == 60
