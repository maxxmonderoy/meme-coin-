"""Every SQL statement in the project.

Written in the portable subset both dialects accept: `?` placeholders (rewritten
for Postgres in dialect.py), no interval arithmetic, no ordered-set aggregates,
no dialect-specific casts. Time cutoffs and percentiles are computed in Python.
"""
from __future__ import annotations

import datetime as dt
from typing import Any

from ..stream.base import RawEvent
from .dialect import Database, base_units, dumps, iso, loads, percentiles


def _now() -> dt.datetime:
    return dt.datetime.now(tz=dt.UTC)


def _cutoff(hours: float) -> str:
    return iso(_now() - dt.timedelta(hours=hours))


# -- streams ---------------------------------------------------------------

async def open_stream(
    db: Database, *, feeds: list[str], subscription: dict, code_version: str
) -> int:
    await db.execute(
        "insert into streams (feeds, subscription, code_version, started_at) "
        "values (?, ?, ?, ?)",
        ",".join(feeds), dumps(subscription), code_version, iso(_now()),
    )
    return await db.fetchval("select max(id) from streams")


async def close_stream(db: Database, stream_id: int, reason: str) -> None:
    await db.execute(
        "update streams set stopped_at = ?, stop_reason = ? where id = ?",
        iso(_now()), reason[:500], stream_id,
    )


# -- raw events ------------------------------------------------------------

async def insert_raw_event(db: Database, event: RawEvent, stream_id: int) -> bool:
    """Returns False when (feed, event_id) was already stored."""
    status = await db.execute(
        "insert into raw_events "
        "(feed, event_id, mint, event_kind, signature, slot, stream_id, commitment, "
        " block_time, received_at, payload) "
        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "on conflict (feed, event_id) do nothing",
        event.provider, event.event_id, event.mint, event.event_kind, event.signature or None,
        event.slot or None, stream_id, event.commitment, iso(event.block_time),
        iso(event.received_at), dumps(event.payload),
    )
    return not status.endswith(" 0")


async def prune_raw_events(db: Database, days: int) -> int:
    cutoff = iso(_now() - dt.timedelta(days=days))
    before = await db.fetchval("select count(*) from raw_events") or 0
    await db.execute("delete from raw_events where received_at < ?", cutoff)
    after = await db.fetchval("select count(*) from raw_events") or 0
    return before - after


# -- tokens ----------------------------------------------------------------

async def upsert_token(db: Database, *, mint: str, stream_id: int, feed: str, fields: dict) -> bool:
    """Insert a newly-seen mint. Returns False if already known.

    Mint is the dedupe identity (3.3): two independent feeds report the same
    launch with different identifiers, and the mint is the only one they agree
    on.
    """
    block_time = fields.get("block_time")
    detect_latency_ms = None
    if isinstance(block_time, dt.datetime):
        detect_latency_ms = int((_now() - block_time).total_seconds() * 1000)

    status = await db.execute(
        "insert into tokens_seen ("
        " mint, signer, declared_creator, program_id, launchpad, bonding_curve, quote_mint,"
        " token_program, name, symbol, uri, token_total_supply, virtual_sol_reserves,"
        " virtual_token_reserves, real_token_reserves, initial_buy_base, is_mayhem_mode,"
        " is_cashback_enabled, first_feed, first_signature, first_slot, stream_id,"
        " block_time, detected_at, detect_latency_ms"
        ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "on conflict (mint) do nothing",
        mint, fields.get("signer"), fields.get("declared_creator"), fields.get("program_id"),
        fields.get("launchpad"), fields.get("bonding_curve"), fields.get("quote_mint"),
        fields.get("token_program"), fields.get("name"), fields.get("symbol"),
        fields.get("uri"),
        base_units(fields.get("token_total_supply")),
        base_units(fields.get("virtual_sol_reserves")),
        base_units(fields.get("virtual_token_reserves")),
        base_units(fields.get("real_token_reserves")),
        base_units(fields.get("initial_buy_base")),
        int(bool(fields["is_mayhem_mode"])) if fields.get("is_mayhem_mode") is not None else None,
        int(bool(fields["is_cashback_enabled"]))
        if fields.get("is_cashback_enabled") is not None else None,
        feed, fields.get("signature"), fields.get("slot"), stream_id,
        iso(block_time) if isinstance(block_time, dt.datetime) else None,
        iso(_now()), detect_latency_ms,
    )
    return not status.endswith(" 0")


async def get_token(db: Database, mint: str) -> dict | None:
    return await db.fetchrow("select * from tokens_seen where mint = ?", mint)


# -- feed race -------------------------------------------------------------

async def record_first_sight(
    db: Database, *, mint: str, feed: str, seen_at: dt.datetime, stream_id: int
) -> None:
    await db.execute(
        "insert into feed_latency (mint, first_feed, first_seen_at, stream_id) "
        "values (?, ?, ?, ?) on conflict (mint) do nothing",
        mint, feed, iso(seen_at), stream_id,
    )


async def record_second_sight(
    db: Database, *, mint: str, feed: str, seen_at: dt.datetime
) -> None:
    """Record the losing feed and the gap.

    This table is the only evidence that can ever fire 3.2's upgrade trigger 2.
    Without it, "would a paid feed have won me candidates" is a feeling.
    """
    row = await db.fetchrow("select first_feed, first_seen_at, second_feed from feed_latency "
                            "where mint = ?", mint)
    if row is None or row["second_feed"] or row["first_feed"] == feed:
        return
    first_at = dt.datetime.fromisoformat(row["first_seen_at"])
    delta_ms = int((seen_at - first_at).total_seconds() * 1000)
    await db.execute(
        "update feed_latency set second_feed = ?, second_seen_at = ?, delta_ms = ? "
        "where mint = ? and second_feed is null",
        feed, iso(seen_at), delta_ms, mint,
    )


async def feed_race_summary(db: Database, hours: float = 24) -> list[dict]:
    rows = await db.fetch(
        "select first_feed, count(*) as wins from feed_latency "
        "where first_seen_at >= ? group by first_feed order by wins desc",
        _cutoff(hours),
    )
    deltas = await db.fetch(
        "select first_feed, delta_ms from feed_latency "
        "where first_seen_at >= ? and delta_ms is not null",
        _cutoff(hours),
    )
    by_feed: dict[str, list[int]] = {}
    for row in deltas:
        by_feed.setdefault(row["first_feed"], []).append(int(row["delta_ms"]))
    for row in rows:
        row["delta_ms"] = percentiles(by_feed.get(row["first_feed"], []))
        row["both_saw"] = len(by_feed.get(row["first_feed"], []))
    return rows


# -- creators --------------------------------------------------------------

async def bump_creator(db: Database, address: str, role: str) -> None:
    """Increment a creator's mint count for one identity.

    Called for BOTH the signer and the declared creator: pump.fun's CreateEvent
    emits them as separate pubkeys, so a cache keyed on one is bypassed by
    rotating the other.
    """
    if not address:
        return
    now = iso(_now())
    await db.execute(
        "insert into creators (address, role, n_mints, first_seen, last_seen) "
        "values (?, ?, 1, ?, ?) "
        # Table-qualified on the left of the arithmetic: Postgres treats a bare
        # `n_mints = n_mints + 1` in DO UPDATE as ambiguous between the existing
        # row and the proposed one. SQLite accepts the qualified form too, so
        # this is the portable spelling.
        "on conflict (address, role) do update set "
        "  n_mints = creators.n_mints + 1, last_seen = excluded.last_seen",
        address, role, now, now,
    )


async def creator_history(db: Database, addresses: list[str]) -> list[dict]:
    if not addresses:
        return []
    placeholders = ",".join("?" for _ in addresses)
    rows = await db.fetch(
        f"select address, role, n_mints, n_rugged, median_lifetime_s, first_seen, "  # noqa: S608
        f"last_seen, backfilled_at from creators where address in ({placeholders})",
        *addresses,
    )
    for row in rows:
        n = row["n_mints"] or 0
        # Derived, never stored: a stored rate goes stale the moment a rug is
        # labelled and nothing recomputes it.
        row["rug_rate"] = (row["n_rugged"] / n) if n else None
    return rows


# -- health ----------------------------------------------------------------

async def flush_health(db: Database, stream_id: int, buckets: list[tuple]) -> None:
    if not buckets:
        return
    await db.executemany(
        "insert into feed_health ("
        " stream_id, feed, minute, events_total, events_create, events_other, dupes,"
        " decode_failures, schema_mismatches, queue_high_water, queue_drops, reconnects,"
        " stale_responses, wins"
        ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        # Every existing-row reference is table-qualified; see bump_creator.
        # max() is spelled greatest() on Postgres, so the high-water column is
        # resolved with a portable CASE instead.
        "on conflict (stream_id, feed, minute) do update set "
        "  events_total      = feed_health.events_total      + excluded.events_total,"
        "  events_create     = feed_health.events_create     + excluded.events_create,"
        "  events_other      = feed_health.events_other      + excluded.events_other,"
        "  dupes             = feed_health.dupes             + excluded.dupes,"
        "  decode_failures   = feed_health.decode_failures   + excluded.decode_failures,"
        "  schema_mismatches = feed_health.schema_mismatches + excluded.schema_mismatches,"
        "  queue_high_water  = case when excluded.queue_high_water >"
        "                           feed_health.queue_high_water"
        "                      then excluded.queue_high_water"
        "                      else feed_health.queue_high_water end,"
        "  queue_drops       = feed_health.queue_drops       + excluded.queue_drops,"
        "  reconnects        = feed_health.reconnects        + excluded.reconnects,"
        "  stale_responses   = feed_health.stale_responses   + excluded.stale_responses,"
        "  wins              = feed_health.wins              + excluded.wins",
        [
            (stream_id, feed, iso(minute), c.events_total, c.events_create, c.events_other,
             c.dupes, c.decode_failures, c.schema_mismatches, c.queue_high_water,
             c.queue_drops, c.reconnects, c.stale_responses, c.wins)
            for (minute, feed), c in buckets
        ],
    )


# -- stats -----------------------------------------------------------------

async def stats(db: Database, hours: float = 24) -> dict:
    cutoff = _cutoff(hours)
    totals = await db.fetchrow(
        "select coalesce(sum(events_total),0) as events,"
        "       coalesce(sum(events_create),0) as creates,"
        "       coalesce(sum(dupes),0) as dupes,"
        "       coalesce(sum(decode_failures),0) as decode_failures,"
        "       coalesce(sum(schema_mismatches),0) as schema_mismatches,"
        "       coalesce(sum(stale_responses),0) as stale_responses,"
        "       coalesce(sum(reconnects),0) as reconnects,"
        "       coalesce(sum(queue_drops),0) as queue_drops,"
        "       coalesce(max(queue_high_water),0) as queue_high_water,"
        "       count(distinct minute) as minutes_observed "
        "from feed_health where minute >= ?",
        cutoff,
    ) or {}
    per_feed = await db.fetch(
        "select feed, coalesce(sum(events_total),0) as events,"
        "       coalesce(sum(events_create),0) as creates,"
        "       coalesce(sum(stale_responses),0) as stale,"
        "       coalesce(sum(schema_mismatches),0) as mismatches,"
        "       coalesce(sum(reconnects),0) as reconnects "
        "from feed_health where minute >= ? group by feed order by events desc",
        cutoff,
    )
    totals["unique_mints"] = await db.fetchval(
        "select count(*) from tokens_seen where detected_at >= ?", cutoff
    ) or 0
    latencies = await db.fetch(
        "select detect_latency_ms from tokens_seen "
        "where detected_at >= ? and detect_latency_ms is not null",
        cutoff,
    )
    totals["detect_latency_ms"] = percentiles([int(r["detect_latency_ms"]) for r in latencies])
    totals["per_feed"] = per_feed
    totals["feed_race"] = await feed_race_summary(db, hours)
    return totals


# -- enrichment ------------------------------------------------------------

async def store_enrichment(
    db: Database, *, provider: str, endpoint: str, key: str, status_code: int | None,
    payload: Any, latency_ms: int, bypassed_cache: bool,
) -> None:
    await db.execute(
        "insert into enrich_cache "
        "(provider, endpoint, key, fetched_at, status_code, bypassed_cache, latency_ms, payload) "
        "values (?, ?, ?, ?, ?, ?, ?, ?)",
        provider, endpoint, key, iso(_now()), status_code,
        int(bypassed_cache), latency_ms, dumps(payload),
    )


async def cached_enrichment(
    db: Database, *, provider: str, endpoint: str, key: str, ttl_seconds: int
) -> dict | None:
    cutoff = iso(_now() - dt.timedelta(seconds=ttl_seconds))
    row = await db.fetchrow(
        "select * from enrich_cache where provider = ? and endpoint = ? and key = ? "
        "and fetched_at >= ? order by fetched_at desc limit 1",
        provider, endpoint, key, cutoff,
    )
    if row:
        row["payload"] = loads(row["payload"])
    return row
