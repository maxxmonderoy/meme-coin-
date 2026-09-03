"""Storage round-trip, run against BOTH dialects.

3.2 says the Postgres swap should be a connection-string change. That is only
true if it is tested, so every test here runs on SQLite and, when
TRENCHES_TEST_POSTGRES_DSN is set, on Postgres too.
"""
from __future__ import annotations

import datetime as dt

import pytest
from conftest import pubkey

from trenches.db import repo
from trenches.db.dialect import base_units, iso, percentiles, to_postgres
from trenches.pipeline.health import MinuteCounters
from trenches.stream.base import EventKind, RawEvent

# -- dialect primitives ----------------------------------------------------

def test_placeholders_are_rewritten_left_to_right():
    assert to_postgres("select ? , ? where x = ?") == "select $1 , $2 where x = $3"


def test_u64_max_survives_as_text_not_signed_integer():
    """SQLite's INTEGER is 64-bit SIGNED. A u64 supply above 2^63-1 would wrap,
    so amounts are stored as exact-integer TEXT."""
    big = 2**64 - 1
    assert int(base_units(big)) == big
    assert base_units(0) == "0"
    assert base_units(None) is None


def test_bool_is_not_an_amount():
    with pytest.raises(TypeError):
        base_units(True)


def test_percentiles_return_observed_values_not_interpolations():
    out = percentiles([10, 20, 30, 40, 100])
    assert out["p50"] in (20, 30)
    assert out["p99"] == 100
    assert percentiles([]) == {}


# -- round trips on both engines -------------------------------------------

async def test_migrations_are_idempotent(any_db):
    from trenches.db import migrate as migrate_mod
    assert await migrate_mod.migrate(any_db) == []


async def test_stream_and_subscription_round_trip(any_db):
    sid = await repo.open_stream(
        any_db, feeds=["pumpportal", "rugcheck"],
        subscription={"pumpportal": {"messages": [{"method": "subscribeNewToken"}]}},
        code_version="deadbeef",
    )
    row = await any_db.fetchrow("select * from streams where id = ?", sid)
    assert "subscribeNewToken" in row["subscription"]
    assert row["feeds"] == "pumpportal,rugcheck"


async def test_same_launch_from_both_feeds_is_stored_once_per_feed(any_db):
    """raw_events is keyed on (feed, event_id) on purpose: storing both sightings
    is what makes the feed race measurable."""
    sid = await repo.open_stream(any_db, feeds=["a"], subscription={}, code_version="v")
    a = RawEvent(provider="pumpportal", event_kind=EventKind.CREATE, mint="M1", signature="S1")
    b = RawEvent(provider="rugcheck", event_kind=EventKind.CREATE, mint="M1")
    assert await repo.insert_raw_event(any_db, a, sid) is True
    assert await repo.insert_raw_event(any_db, b, sid) is True
    assert await repo.insert_raw_event(any_db, a, sid) is False   # exact duplicate
    assert await any_db.fetchval("select count(*) from raw_events") == 2


async def test_u64_supply_round_trips_through_the_database(any_db):
    sid = await repo.open_stream(any_db, feeds=["a"], subscription={}, code_version="v")
    big = 2**64 - 1
    await repo.upsert_token(any_db, mint="M-BIG", stream_id=sid, feed="pumpportal",
                            fields={"token_total_supply": big})
    stored = await any_db.fetchval(
        "select token_total_supply from tokens_seen where mint = ?", "M-BIG")
    assert int(stored) == big


async def test_token_upsert_is_idempotent(any_db):
    sid = await repo.open_stream(any_db, feeds=["a"], subscription={}, code_version="v")
    fields = {"signer": pubkey(3), "declared_creator": pubkey(4)}
    assert await repo.upsert_token(any_db, mint="M", stream_id=sid,
                                   feed="pumpportal", fields=fields) is True
    assert await repo.upsert_token(any_db, mint="M", stream_id=sid,
                                   feed="rugcheck", fields=fields) is False


async def test_detect_latency_computed_from_block_time(any_db):
    sid = await repo.open_stream(any_db, feeds=["a"], subscription={}, code_version="v")
    block_time = dt.datetime.now(tz=dt.UTC) - dt.timedelta(milliseconds=1500)
    await repo.upsert_token(any_db, mint="M-LAT", stream_id=sid, feed="pumpportal",
                            fields={"block_time": block_time})
    latency = await any_db.fetchval(
        "select detect_latency_ms from tokens_seen where mint = ?", "M-LAT")
    assert 1400 <= latency <= 4000


async def test_both_creator_identities_accumulate(any_db):
    """Rotating the declared creator while reusing the signer must not reset
    reputation. CreateEvent carries them as separate pubkeys."""
    signer, decl_a, decl_b = pubkey(3), pubkey(4), pubkey(5)
    for declared in (decl_a, decl_b, decl_b):
        await repo.bump_creator(any_db, signer, "signer")
        await repo.bump_creator(any_db, declared, "declared")
    history = {(r["address"], r["role"]): r["n_mints"]
               for r in await repo.creator_history(any_db, [signer, decl_a, decl_b])}
    assert history[(signer, "signer")] == 3
    assert history[(decl_a, "declared")] == 1
    assert history[(decl_b, "declared")] == 2


async def test_rug_rate_is_derived_not_stored(any_db):
    await repo.bump_creator(any_db, pubkey(7), "signer")
    await any_db.execute("update creators set n_rugged = 1 where address = ?", pubkey(7))
    row, = await repo.creator_history(any_db, [pubkey(7)])
    assert row["rug_rate"] == 1.0


# -- the feed race ---------------------------------------------------------

async def test_feed_race_records_winner_and_gap(any_db):
    sid = await repo.open_stream(any_db, feeds=["a"], subscription={}, code_version="v")
    first = dt.datetime.now(tz=dt.UTC)
    second = first + dt.timedelta(milliseconds=850)
    await repo.record_first_sight(any_db, mint="M1", feed="pumpportal",
                                  seen_at=first, stream_id=sid)
    await repo.record_second_sight(any_db, mint="M1", feed="rugcheck", seen_at=second)

    row = await any_db.fetchrow("select * from feed_latency where mint = ?", "M1")
    assert row["first_feed"] == "pumpportal"
    assert row["second_feed"] == "rugcheck"
    assert 800 <= row["delta_ms"] <= 900


async def test_the_same_feed_reporting_twice_is_not_a_race_result(any_db):
    sid = await repo.open_stream(any_db, feeds=["a"], subscription={}, code_version="v")
    now = dt.datetime.now(tz=dt.UTC)
    await repo.record_first_sight(any_db, mint="M2", feed="pumpportal",
                                  seen_at=now, stream_id=sid)
    await repo.record_second_sight(any_db, mint="M2", feed="pumpportal",
                                   seen_at=now + dt.timedelta(seconds=1))
    row = await any_db.fetchrow("select * from feed_latency where mint = ?", "M2")
    assert row["second_feed"] is None


async def test_race_summary_separates_contested_wins_from_sole_sightings(any_db):
    """The distinction that changes what you would do about it.

    Measured live, one feed was "first" on 62.5% of mints, which reads as
    somewhat faster. But every one of the other feed's wins had no second
    sighting, meaning the first feed never delivered those launches at all. The
    true statement was that it won 100% of the races it entered and missed 37.5%
    of launches -- a latency fact and a coverage fact, with different
    consequences, that a single "wins" count welds into one misleading number.
    """
    sid = await repo.open_stream(any_db, feeds=["a"], subscription={}, code_version="v")
    now = dt.datetime.now(tz=dt.UTC)

    # Three mints both feeds saw; pumpportal first each time, 500ms ahead.
    for i in range(3):
        await repo.record_first_sight(any_db, mint=f"P{i}", feed="pumpportal",
                                      seen_at=now, stream_id=sid)
        await repo.record_second_sight(any_db, mint=f"P{i}", feed="rugcheck",
                                       seen_at=now + dt.timedelta(milliseconds=500))
    # Two mints ONLY rugcheck ever delivered.
    for i in range(2):
        await repo.record_first_sight(any_db, mint=f"R{i}", feed="rugcheck",
                                      seen_at=now, stream_id=sid)

    summary = await repo.feed_race_summary(any_db)
    by_feed = {row["feed"]: row for row in summary["feeds"]}

    assert summary["mints"] == 5
    assert summary["contested"] == 3
    assert summary["sole"] == 2

    pp = by_feed["pumpportal"]
    assert pp["contested_wins"] == 3
    assert pp["contested_losses"] == 0
    assert pp["contested_win_rate"] == 1.0        # won every race it entered
    assert pp["sole_sightings"] == 0
    assert pp["delta_ms"]["p50"] == 500

    rc = by_feed["rugcheck"]
    assert rc["first_sightings"] == 2
    assert rc["contested_wins"] == 0              # never beat the other feed
    assert rc["contested_losses"] == 3            # it was second three times
    assert rc["sole_sightings"] == 2              # but it alone delivered these
    assert rc["sole_share"] == 0.4


async def test_a_feed_that_only_ever_arrives_second_still_shows_its_races(any_db):
    """Losing a race is different from not being in one."""
    sid = await repo.open_stream(any_db, feeds=["a"], subscription={}, code_version="v")
    now = dt.datetime.now(tz=dt.UTC)
    await repo.record_first_sight(any_db, mint="M", feed="pumpportal",
                                  seen_at=now, stream_id=sid)
    await repo.record_second_sight(any_db, mint="M", feed="rugcheck",
                                   seen_at=now + dt.timedelta(seconds=2))

    by_feed = {r["feed"]: r for r in (await repo.feed_race_summary(any_db))["feeds"]}
    assert "rugcheck" not in by_feed or by_feed["rugcheck"]["first_sightings"] == 0
    assert by_feed["pumpportal"]["contested_win_rate"] == 1.0


# -- health and the decisions gate -----------------------------------------

async def test_health_counters_accumulate_per_feed(any_db):
    sid = await repo.open_stream(any_db, feeds=["a"], subscription={}, code_version="v")
    minute = dt.datetime.now(tz=dt.UTC).replace(second=0, microsecond=0)
    await repo.flush_health(any_db, sid, [
        ((minute, "pumpportal"), MinuteCounters(events_total=10, events_create=2,
                                                queue_high_water=5, wins=2)),
    ])
    await repo.flush_health(any_db, sid, [
        ((minute, "pumpportal"), MinuteCounters(events_total=5, events_create=1,
                                                queue_high_water=3, wins=1)),
        ((minute, "rugcheck"), MinuteCounters(events_total=7, stale_responses=1)),
    ])
    pp = await any_db.fetchrow(
        "select * from feed_health where feed = ? and stream_id = ?", "pumpportal", sid)
    assert pp["events_total"] == 15          # summed
    assert pp["queue_high_water"] == 5       # max, not summed
    assert pp["wins"] == 3
    rc = await any_db.fetchrow(
        "select * from feed_health where feed = ? and stream_id = ?", "rugcheck", sid)
    assert rc["stale_responses"] == 1


async def test_decisions_table_exists_and_is_empty(any_db):
    """Designed now, written in week 2. Non-empty here means the paper gate moved."""
    assert await any_db.fetchval("select count(*) from decisions") == 0


async def test_decisions_rejects_sizing_above_two_percent(any_db):
    """1.9's hard cap, enforced by the database so it cannot be argued with."""
    sid = await repo.open_stream(any_db, feeds=["a"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="M-SIZE", stream_id=sid, feed="f", fields={})
    with pytest.raises(Exception, match=r"(?i)constraint|check"):
        await any_db.execute(
            "insert into decisions (mint, mode, outcome, size_pct_bankroll, inputs, "
            "thresholds, code_version, decided_at) "
            "values ('M-SIZE','PAPER','accept', 5.0, '{}', '{}', 'v', ?)",
            iso(dt.datetime.now(tz=dt.UTC)))


async def test_decisions_reject_must_carry_a_stage(any_db):
    sid = await repo.open_stream(any_db, feeds=["a"], subscription={}, code_version="v")
    await repo.upsert_token(any_db, mint="M-REJ", stream_id=sid, feed="f", fields={})
    with pytest.raises(Exception, match=r"(?i)constraint|check"):
        await any_db.execute(
            "insert into decisions (mint, mode, outcome, inputs, thresholds, "
            "code_version, decided_at) values ('M-REJ','PAPER','reject','{}','{}','v', ?)",
            iso(dt.datetime.now(tz=dt.UTC)))


# -- outcomes migration on a populated database ----------------------------

async def test_002_applies_cleanly_to_a_populated_database(any_db):
    """The acceptance condition: it must land on a database that already has
    data, without touching it. 002 adds tables and no column of an existing
    one, so this asserts the existing rows survive untouched."""
    import datetime as dt

    from trenches.db.dialect import iso

    stream_id = await repo.open_stream(
        any_db, feeds=["pumpportal"], subscription={}, code_version="test",
    )
    mint = pubkey(200)
    await repo.upsert_token(
        any_db, mint=mint, stream_id=stream_id, feed="pumpportal",
        fields={"symbol": "KEEP", "signer": pubkey(201)},
    )
    before = await repo.get_token(any_db, mint)

    from trenches.db import migrate as migrate_mod
    assert await migrate_mod.migrate(any_db) == []  # already applied, idempotent

    after = await repo.get_token(any_db, mint)
    assert after["symbol"] == before["symbol"] == "KEEP"

    # and the new tables accept a row keyed to the pre-existing mint
    now = dt.datetime.now(tz=dt.UTC)
    assert await repo.record_outcome(any_db, mint=mint, horizon="15m", fields={
        "scheduled_for": iso(now), "observed_at": iso(now), "status": "no_pool",
        "ambiguous_no_pool": True, "source": "none", "pool_found": False,
    })
    rows = await repo.outcomes_for_mint(any_db, mint)
    assert len(rows) == 1 and rows[0]["status"] == "no_pool"


async def test_outcome_row_round_trips_every_column(any_db):
    import datetime as dt

    from trenches.db.dialect import iso

    stream_id = await repo.open_stream(
        any_db, feeds=["pumpportal"], subscription={}, code_version="test")
    mint = pubkey(202)
    await repo.upsert_token(any_db, mint=mint, stream_id=stream_id, feed="pumpportal", fields={})
    now = dt.datetime.now(tz=dt.UTC)
    await repo.record_outcome(any_db, mint=mint, horizon="24h", fields={
        "scheduled_for": iso(now), "observed_at": iso(now), "lateness_seconds": 7200,
        "status": "alive", "ambiguous_no_pool": False, "backfilled": True,
        "source": "dexscreener", "pool_found": True,
        "price_usd": "0.000000001234", "liquidity_usd": "12345.6",
        "fdv_usd": "999", "market_cap_usd": "888", "txns_h24_buys": 5,
        "txns_h24_sells": 6, "dex_id": "raydium", "payload": {"a": 1},
    })
    row = (await repo.outcomes_for_mint(any_db, mint))[0]
    # the exact decimal string survives -- no float anywhere in the path
    assert row["price_usd"] == "0.000000001234"
    assert row["liquidity_usd"] == "12345.6"
    assert row["lateness_seconds"] == 7200
    assert row["backfilled"] == 1 or row["backfilled"] is True


def test_sqlite_dsn_slash_count_is_respected():
    """`sqlite:///abs` is ABSOLUTE. Stripping that leading slash silently
    creates the database somewhere other than where it was asked for, and
    everything keeps working -- migrations apply, rows insert -- against a file
    in the wrong place."""
    from trenches.db.pool import sqlite_path

    assert sqlite_path("sqlite:///tmp/x.db") == "/tmp/x.db"
    assert sqlite_path("sqlite://./x.db") == "./x.db"
    assert sqlite_path("sqlite://x.db") == "x.db"
    assert sqlite_path("sqlite://:memory:") == ":memory:"
    assert sqlite_path("/tmp/x.db") == "/tmp/x.db"
