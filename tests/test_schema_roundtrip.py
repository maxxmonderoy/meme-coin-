"""Schema round-trip against a real Postgres.

The journal is the product. A schema that silently truncates a u64 supply or
loses the commitment tier is a data-integrity bug you find months later, when
the journal is the only evidence you have and it is wrong.

Skipped when no database is reachable; set TRENCHES_TEST_DSN to run it.
"""
from __future__ import annotations

import datetime as dt

import asyncpg
import pytest
from conftest import TEST_DSN, pubkey

from trenches.db import migrate as migrate_mod
from trenches.db import repo
from trenches.stream.base import EventKind, RawEvent


@pytest.fixture
async def pool():
    try:
        p = await asyncpg.create_pool(TEST_DSN, min_size=1, max_size=4, timeout=5)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"no test database reachable: {exc}")
    assert p is not None
    await migrate_mod.migrate(p)
    async with p.acquire() as conn:
        await conn.execute(
            "truncate decisions, enrich_cache, slot_gaps, stream_health, "
            "raw_events, tokens_seen, creators, streams restart identity cascade"
        )
    try:
        yield p
    finally:
        await p.close()


@pytest.fixture
async def stream_id(pool):
    return await repo.open_stream(
        pool, provider="test", filter_mode="both",
        subscription={"filters": {"strict": {"account_required": ["x"]}}},
        code_version="deadbeef",
    )


async def test_migrations_are_idempotent(pool):
    assert await migrate_mod.migrate(pool) == []


async def test_subscription_survives_round_trip(pool, stream_id):
    """The 3.3 filter diff is only reproducible if the exact filter is stored."""
    row = await pool.fetchrow("select * from streams where id = $1", stream_id)
    assert row["filter_mode"] == "both"
    assert '"account_required"' in row["subscription"]


async def test_raw_event_dedupe_is_enforced_by_the_database(pool, stream_id):
    """The in-memory cache is bounded and empty after a restart. The primary
    key is the half of the 3.8.4 invariant that survives one."""
    event = RawEvent(
        signature="sig-A", slot=42, provider="test", event_kind=EventKind.CREATE,
        commitment="confirmed", program_id="prog", filter_source="strict",
        payload={"creates": []},
    )
    assert await repo.insert_raw_event(pool, event, stream_id) is True
    assert await repo.insert_raw_event(pool, event, stream_id) is False
    assert await pool.fetchval("select count(*) from raw_events") == 1


async def test_same_signature_different_slot_both_stored(pool, stream_id):
    a = RawEvent(signature="sig-B", slot=1, provider="t", event_kind=EventKind.OTHER)
    b = RawEvent(signature="sig-B", slot=2, provider="t", event_kind=EventKind.OTHER)
    assert await repo.insert_raw_event(pool, a, stream_id) is True
    assert await repo.insert_raw_event(pool, b, stream_id) is True


async def test_commitment_tier_is_preserved(pool, stream_id):
    """3.8.7 forbids treating `processed` as authoritative. The tier has to be
    readable back off the row, not inferred."""
    for tier in ("processed", "confirmed", "finalized"):
        await repo.insert_raw_event(
            pool,
            RawEvent(signature=f"sig-{tier}", slot=7, provider="t",
                     event_kind=EventKind.OTHER, commitment=tier),
            stream_id,
        )
    rows = await pool.fetch("select commitment from raw_events order by signature")
    assert {r["commitment"] for r in rows} == {"processed", "confirmed", "finalized"}


async def test_u64_supply_survives_without_precision_loss(pool, stream_id):
    """A float would round this. numeric(40,0) must not."""
    big = 2**64 - 1
    await repo.upsert_token(
        pool, mint="MINT-BIG", stream_id=stream_id, signature="s", slot=1,
        program_id="p", fields={"token_total_supply": big, "signer": pubkey(3)},
    )
    stored = await pool.fetchval(
        "select token_total_supply from tokens_seen where mint = 'MINT-BIG'"
    )
    assert int(stored) == big


async def test_token_insert_is_idempotent(pool, stream_id):
    fields = {"signer": pubkey(3), "declared_creator": pubkey(4)}
    assert await repo.upsert_token(
        pool, mint="M", stream_id=stream_id, signature="s", slot=1,
        program_id="p", fields=fields) is True
    assert await repo.upsert_token(
        pool, mint="M", stream_id=stream_id, signature="s", slot=1,
        program_id="p", fields=fields) is False


async def test_detect_latency_is_computed_from_block_time(pool, stream_id):
    block_time = dt.datetime.now(tz=dt.UTC) - dt.timedelta(milliseconds=1500)
    await repo.upsert_token(
        pool, mint="M-LAT", stream_id=stream_id, signature="s", slot=1,
        program_id="p", fields={"block_time": block_time},
    )
    latency = await pool.fetchval(
        "select detect_latency_ms from tokens_seen where mint = 'M-LAT'"
    )
    assert 1400 <= latency <= 3000


async def test_both_creator_identities_accumulate(pool):
    """Rotating the declared creator while reusing the signer (or the reverse)
    must not reset reputation. Both addresses carry history."""
    signer, declared_a, declared_b = pubkey(3), pubkey(4), pubkey(5)
    for declared in (declared_a, declared_b, declared_b):
        await repo.bump_creator(pool, signer, "signer")
        await repo.bump_creator(pool, declared, "declared")

    history = {
        (r["address"], r["role"]): r["n_mints"]
        for r in await repo.creator_history(pool, [signer, declared_a, declared_b])
    }
    assert history[(signer, "signer")] == 3       # signer history survives rotation
    assert history[(declared_a, "declared")] == 1
    assert history[(declared_b, "declared")] == 2


async def test_rug_rate_is_derived_not_stored(pool):
    await repo.bump_creator(pool, pubkey(7), "signer")
    await pool.execute(
        "update creators set n_rugged = 1 where address = $1", pubkey(7)
    )
    row, = await repo.creator_history(pool, [pubkey(7)])
    assert float(row["rug_rate"]) == 1.0


async def test_health_counters_accumulate_on_conflict(pool, stream_id):
    from trenches.pipeline.health import MinuteCounters

    minute = dt.datetime.now(tz=dt.UTC).replace(second=0, microsecond=0)
    first = MinuteCounters(events_total=10, events_create=2, queue_high_water=5, max_slot=100)
    second = MinuteCounters(events_total=5, events_create=1, queue_high_water=3, max_slot=90)
    await repo.flush_health(pool, stream_id, [(minute, first)])
    await repo.flush_health(pool, stream_id, [(minute, second)])

    row = await pool.fetchrow("select * from stream_health where stream_id = $1", stream_id)
    assert row["events_total"] == 15          # summed
    assert row["events_create"] == 3
    assert row["queue_high_water"] == 5       # maximum, not summed
    assert row["max_slot"] == 100


async def test_stats_reports_detection_rate_inputs(pool, stream_id):
    from trenches.pipeline.health import MinuteCounters

    minute = dt.datetime.now(tz=dt.UTC).replace(second=0, microsecond=0)
    await repo.flush_health(
        pool, stream_id, [(minute, MinuteCounters(events_total=100, events_create=7))]
    )
    data = await repo.stats(pool, hours=1)
    assert data["events"] == 100
    assert data["creates"] == 7
    assert data["minutes_observed"] == 1


async def test_decisions_table_exists_and_is_empty(pool):
    """Designed now, written to in week 2. If this ever fails non-empty, the
    paper gate was skipped."""
    assert await pool.fetchval("select count(*) from decisions") == 0


async def test_decisions_rejects_sizing_above_two_percent(pool, stream_id):
    """1.9: correct sizing is 1-2% of speculative bankroll, hard cap. Enforced
    in the database so it cannot be argued with at 2am."""
    await repo.upsert_token(
        pool, mint="M-SIZE", stream_id=stream_id, signature="s", slot=1,
        program_id="p", fields={},
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await pool.execute(
            """insert into decisions
               (mint, mode, outcome, size_pct_bankroll, inputs, thresholds, code_version)
               values ('M-SIZE','PAPER','accept', 5.0, '{}', '{}', 'v')"""
        )


async def test_decisions_reject_must_carry_a_stage(pool, stream_id):
    """A rejection without a stage is not training data, it is a shrug."""
    await repo.upsert_token(
        pool, mint="M-REJ", stream_id=stream_id, signature="s", slot=1,
        program_id="p", fields={},
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await pool.execute(
            """insert into decisions (mint, mode, outcome, inputs, thresholds, code_version)
               values ('M-REJ','PAPER','reject','{}','{}','v')"""
        )
