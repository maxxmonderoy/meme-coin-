"""Every SQL statement in the project lives here.

All statements are literals with bound parameters. Nothing interpolates
caller-supplied text into SQL.
"""
from __future__ import annotations

import datetime as dt
import json
from typing import Any

import asyncpg

from ..pipeline.health import MinuteCounters
from ..stream.base import RawEvent


def _json(value: Any) -> str | None:
    return None if value is None else json.dumps(value, default=str)


# -- streams ---------------------------------------------------------------

async def open_stream(
    pool: asyncpg.Pool, *, provider: str, filter_mode: str,
    subscription: dict, code_version: str,
) -> int:
    return await pool.fetchval(
        """
        insert into streams (provider, filter_mode, subscription, code_version)
        values ($1, $2, $3::jsonb, $4)
        returning id
        """,
        provider, filter_mode, _json(subscription), code_version,
    )


async def close_stream(pool: asyncpg.Pool, stream_id: int, reason: str) -> None:
    await pool.execute(
        "update streams set stopped_at = now(), stop_reason = $2 where id = $1",
        stream_id, reason[:500],
    )


# -- raw events ------------------------------------------------------------

async def insert_raw_event(
    pool: asyncpg.Pool, event: RawEvent, stream_id: int
) -> bool:
    """Insert one event. Returns False when the (signature, slot) key already
    exists -- the durable half of the 3.8.4 dedupe invariant."""
    status = await pool.execute(
        """
        insert into raw_events (
            signature, slot, stream_id, provider, event_kind, program_id,
            commitment, block_time, received_at, filter_source, payload
        ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
        on conflict (signature, slot) do nothing
        """,
        event.signature, event.slot, stream_id, event.provider, event.event_kind,
        event.program_id, event.commitment, event.block_time, event.received_at,
        event.filter_source, _json(event.payload),
    )
    return status.endswith(" 1")


async def prune_raw_events(pool: asyncpg.Pool, days: int) -> int:
    status = await pool.execute(
        "delete from raw_events where received_at < now() - ($1 || ' days')::interval",
        str(days),
    )
    return int(status.rsplit(" ", 1)[-1] or 0)


# -- tokens ----------------------------------------------------------------

async def upsert_token(
    pool: asyncpg.Pool, *, mint: str, stream_id: int, signature: str, slot: int,
    program_id: str, fields: dict,
) -> bool:
    """Insert a newly-seen mint. Returns False if the mint was already known."""
    block_time: dt.datetime | None = fields.get("block_time")
    detect_latency_ms = None
    if block_time is not None:
        delta = dt.datetime.now(tz=dt.UTC) - block_time
        detect_latency_ms = int(delta.total_seconds() * 1000)

    status = await pool.execute(
        """
        insert into tokens_seen (
            mint, signer, declared_creator, program_id, launchpad, bonding_curve,
            quote_mint, token_program, name, symbol, uri, token_total_supply,
            virtual_sol_reserves, virtual_token_reserves, real_token_reserves,
            is_mayhem_mode, is_cashback_enabled, first_signature, first_slot,
            stream_id, block_time, detect_latency_ms
        ) values (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22
        )
        on conflict (mint) do nothing
        """,
        mint, fields.get("signer"), fields.get("declared_creator"), program_id,
        fields.get("launchpad"), fields.get("bonding_curve"), fields.get("quote_mint"),
        fields.get("token_program"), fields.get("name"), fields.get("symbol"),
        fields.get("uri"), fields.get("token_total_supply"),
        fields.get("virtual_sol_reserves"), fields.get("virtual_token_reserves"),
        fields.get("real_token_reserves"), fields.get("is_mayhem_mode"),
        fields.get("is_cashback_enabled"), signature, slot, stream_id,
        block_time, detect_latency_ms,
    )
    return status.endswith(" 1")


async def get_token(pool: asyncpg.Pool, mint: str) -> asyncpg.Record | None:
    return await pool.fetchrow("select * from tokens_seen where mint = $1", mint)


# -- creators --------------------------------------------------------------

async def bump_creator(pool: asyncpg.Pool, address: str, role: str) -> None:
    """Increment a creator's mint count.

    Called for BOTH the signer and the declared creator. CreateEvent carries
    them as separate pubkeys and they need not match, so a reputation cache
    keyed on one address alone has a trivial bypass: rotate the other.
    """
    if not address:
        return
    await pool.execute(
        """
        insert into creators (address, role, n_mints)
        values ($1, $2, 1)
        on conflict (address, role) do update
            set n_mints = creators.n_mints + 1,
                last_seen = now()
        """,
        address, role,
    )


async def creator_history(pool: asyncpg.Pool, addresses: list[str]) -> list[asyncpg.Record]:
    return await pool.fetch(
        """
        select address, role, n_mints, n_rugged, median_lifetime_s,
               first_seen, last_seen, backfilled_at,
               case when n_mints > 0 then n_rugged::numeric / n_mints else null end as rug_rate
        from creators
        where address = any($1::text[])
        """,
        addresses,
    )


# -- health ----------------------------------------------------------------

async def flush_health(
    pool: asyncpg.Pool, stream_id: int, buckets: list[tuple[dt.datetime, MinuteCounters]]
) -> None:
    if not buckets:
        return
    await pool.executemany(
        """
        insert into stream_health (
            stream_id, minute, events_total, events_create, events_other, dupes,
            decode_failures, queue_high_water, queue_drops, reconnects, slot_gaps, max_slot
        ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        on conflict (stream_id, minute) do update set
            events_total     = stream_health.events_total     + excluded.events_total,
            events_create    = stream_health.events_create    + excluded.events_create,
            events_other     = stream_health.events_other     + excluded.events_other,
            dupes            = stream_health.dupes            + excluded.dupes,
            decode_failures  = stream_health.decode_failures  + excluded.decode_failures,
            queue_high_water = greatest(stream_health.queue_high_water,
                                        excluded.queue_high_water),
            queue_drops      = stream_health.queue_drops      + excluded.queue_drops,
            reconnects       = stream_health.reconnects       + excluded.reconnects,
            slot_gaps        = stream_health.slot_gaps        + excluded.slot_gaps,
            max_slot         = greatest(stream_health.max_slot, excluded.max_slot)
        """,
        [
            (stream_id, minute, c.events_total, c.events_create, c.events_other,
             c.dupes, c.decode_failures, c.queue_high_water, c.queue_drops,
             c.reconnects, c.slot_gaps, c.max_slot or None)
            for minute, c in buckets
        ],
    )


async def record_slot_gap(
    pool: asyncpg.Pool, stream_id: int, from_slot: int, to_slot: int
) -> None:
    await pool.execute(
        """
        insert into slot_gaps (stream_id, from_slot, to_slot, resolution)
        values ($1, $2, $3, 'unknown')
        on conflict (stream_id, from_slot, to_slot) do nothing
        """,
        stream_id, from_slot, to_slot,
    )


# -- stats -----------------------------------------------------------------

async def stats(pool: asyncpg.Pool, hours: int = 24) -> dict:
    row = await pool.fetchrow(
        """
        select
            coalesce(sum(events_total), 0)    as events,
            coalesce(sum(events_create), 0)   as creates,
            coalesce(sum(dupes), 0)           as dupes,
            coalesce(sum(decode_failures), 0) as decode_failures,
            coalesce(sum(slot_gaps), 0)       as slot_gaps,
            coalesce(sum(reconnects), 0)      as reconnects,
            coalesce(max(queue_high_water),0) as queue_high_water,
            count(*)                          as minutes_observed,
            max(max_slot)                     as max_slot
        from stream_health
        where minute >= now() - ($1 || ' hours')::interval
        """,
        str(hours),
    )
    uniques = await pool.fetchval(
        "select count(*) from tokens_seen where detected_at >= now() - ($1 || ' hours')::interval",
        str(hours),
    )
    latency = await pool.fetchrow(
        """
        select
            percentile_disc(0.5) within group (order by detect_latency_ms)  as p50,
            percentile_disc(0.9) within group (order by detect_latency_ms)  as p90,
            percentile_disc(0.99) within group (order by detect_latency_ms) as p99
        from tokens_seen
        where detected_at >= now() - ($1 || ' hours')::interval
          and detect_latency_ms is not null
        """,
        str(hours),
    )
    out = dict(row) if row else {}
    out["unique_mints"] = uniques or 0
    out["detect_latency_ms"] = dict(latency) if latency else {}
    return out


async def filter_diff(pool: asyncpg.Pool, hours: int = 1) -> list[asyncpg.Record]:
    """Which mints each named subscription caught.

    This is the 3.3 experiment: run FILTER_MODE=both, then compare. If `strict`
    misses mints that `naive` caught, the account_required premise is wrong for
    those launches and the filter must not be trusted.
    """
    return await pool.fetch(
        """
        select
            coalesce(filter_source, '<none>') as filter_source,
            count(*)                          as events,
            count(*) filter (where event_kind = 'create') as creates
        from raw_events
        where received_at >= now() - ($1 || ' hours')::interval
        group by 1
        order by events desc
        """,
        str(hours),
    )


# -- enrichment ------------------------------------------------------------

async def store_enrichment(
    pool: asyncpg.Pool, *, provider: str, endpoint: str, key: str,
    status_code: int | None, payload: Any, latency_ms: int, bypassed_cache: bool,
) -> None:
    await pool.execute(
        """
        insert into enrich_cache
            (provider, endpoint, key, status_code, payload, latency_ms, bypassed_cache)
        values ($1,$2,$3,$4,$5::jsonb,$6,$7)
        """,
        provider, endpoint, key, status_code, _json(payload), latency_ms, bypassed_cache,
    )


async def cached_enrichment(
    pool: asyncpg.Pool, *, provider: str, endpoint: str, key: str, ttl_seconds: int
) -> asyncpg.Record | None:
    return await pool.fetchrow(
        """
        select * from enrich_cache
        where provider = $1 and endpoint = $2 and key = $3
          and fetched_at >= now() - ($4 || ' seconds')::interval
        order by fetched_at desc
        limit 1
        """,
        provider, endpoint, key, str(ttl_seconds),
    )
