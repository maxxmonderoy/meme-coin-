"""Every SQL statement in the project.

Written in the portable subset both dialects accept: `?` placeholders (rewritten
for Postgres in dialect.py), no interval arithmetic, no ordered-set aggregates,
no dialect-specific casts. Time cutoffs and percentiles are computed in Python.
"""
from __future__ import annotations

import datetime as dt
import itertools
from decimal import Decimal, InvalidOperation
from typing import Any

from ..stream.base import RawEvent
from .dialect import Database, base_units, dumps, iso, loads, parse_ts, percentiles


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


# -- outcomes --------------------------------------------------------------
#
# Read-only observation of what happened to a mint. Nothing here touches the
# ingest path: the labeler holds its own connection and these statements are
# never called from a feed worker.

async def due_horizons(
    db: Database, horizon: str, horizon_seconds: int, *, limit: int = 500
) -> list[dict]:
    """Mints whose `horizon` came due and which have no row for it yet.

    Due-ness is derived rather than queued. A queue table would be a second
    source of truth that drifts from tokens_seen the first time a backfill runs.

    The cutoff is computed in Python and bound as a parameter -- SQLite has no
    interval type, so date arithmetic in SQL would not survive the Postgres
    swap in either direction (dialect.py).
    """
    cutoff = iso(_now() - dt.timedelta(seconds=horizon_seconds))
    return await db.fetch(
        "select t.mint, t.detected_at from tokens_seen t "
        "left join outcomes o on o.mint = t.mint and o.horizon = ? "
        "where t.detected_at <= ? and o.mint is null "
        "order by t.detected_at asc limit ?",
        horizon, cutoff, limit,
    )


async def record_outcome(db: Database, *, mint: str, horizon: str, fields: dict) -> bool:
    """Write one observation. Returns False if (mint, horizon) already existed.

    `on conflict do nothing` rather than an upsert: a horizon is observed once.
    Re-observing it later would overwrite a measurement taken at the right time
    with one taken at the wrong time, which is the whole failure this table
    exists to avoid.
    """
    status = await db.execute(
        "insert into outcomes "
        "(mint, horizon, scheduled_for, observed_at, lateness_seconds, status, "
        " ambiguous_no_pool, backfilled, source, pool_found, price_usd, liquidity_usd, "
        " fdv_usd, market_cap_usd, volume_m5, volume_h1, volume_h24, "
        " txns_m5_buys, txns_m5_sells, txns_h1_buys, txns_h1_sells, "
        " txns_h24_buys, txns_h24_sells, price_change_h24, pair_address, "
        " pair_created_at, dex_id, venue_kind, market_type, attempts, error, payload) "
        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        "        ?, ?, ?, ?, ?, ?, ?) "
        "on conflict (mint, horizon) do nothing",
        mint, horizon, fields["scheduled_for"], fields["observed_at"],
        fields.get("lateness_seconds", 0), fields["status"],
        1 if fields.get("ambiguous_no_pool") else 0,
        1 if fields.get("backfilled") else 0,
        fields.get("source", "none"), 1 if fields.get("pool_found") else 0,
        fields.get("price_usd"), fields.get("liquidity_usd"), fields.get("fdv_usd"),
        fields.get("market_cap_usd"), fields.get("volume_m5"), fields.get("volume_h1"),
        fields.get("volume_h24"), fields.get("txns_m5_buys"), fields.get("txns_m5_sells"),
        fields.get("txns_h1_buys"), fields.get("txns_h1_sells"), fields.get("txns_h24_buys"),
        fields.get("txns_h24_sells"), fields.get("price_change_h24"),
        fields.get("pair_address"), fields.get("pair_created_at"), fields.get("dex_id"),
        fields.get("venue_kind"), fields.get("market_type"),
        fields.get("attempts", 1), fields.get("error"), dumps(fields.get("payload")),
    )
    return not status.endswith(" 0")


async def bump_peak(
    db: Database, mint: str, *, price_usd: str | None, liquidity_usd: str | None, observed_at: str
) -> None:
    """Update the running maximum for a mint.

    Compared as Decimal in Python, not in SQL: the columns are TEXT holding the
    vendor's exact decimal string, and TEXT compares lexically -- "9" would beat
    "10". Same reason percentiles are computed in Python (dialect.py).

    Called on EVERY observation, not only at horizons, and still only a LOWER
    BOUND on the true peak. We sample; we do not stream.
    """
    row = await db.fetchrow(
        "select max_price_usd_seen, max_liquidity_usd_seen, observations "
        "from mint_peaks where mint = ?", mint,
    )
    if row is None:
        await db.execute(
            "insert into mint_peaks (mint, max_price_usd_seen, max_liquidity_usd_seen, "
            " observations, first_observed_at, last_observed_at) "
            "values (?, ?, ?, ?, ?, ?) on conflict (mint) do nothing",
            mint, price_usd or "0", liquidity_usd, 1, observed_at, observed_at,
        )
        return
    await db.execute(
        "update mint_peaks set max_price_usd_seen = ?, max_liquidity_usd_seen = ?, "
        "observations = observations + 1, last_observed_at = ? where mint = ?",
        _max_decimal(row["max_price_usd_seen"], price_usd) or "0",
        _max_decimal(row["max_liquidity_usd_seen"], liquidity_usd),
        observed_at, mint,
    )


def _max_decimal(current: str | None, candidate: str | None) -> str | None:
    """Larger of two decimal strings, preserving the original text."""
    if candidate is None:
        return current
    if current is None:
        return candidate
    try:
        return candidate if Decimal(candidate) > Decimal(current) else current
    except (InvalidOperation, ValueError):
        return current


async def outcome_coverage(db: Database, horizons: tuple[str, ...]) -> dict:
    """Rows per horizon, status split, and how many are due but unobserved."""
    rows = await db.fetch(
        "select horizon, status, backfilled, venue_kind, count(*) as n from outcomes "
        "group by horizon, status, backfilled, venue_kind"
    )
    total_tokens = await db.fetchval("select count(*) from tokens_seen") or 0
    out: dict[str, Any] = {"total_tokens": total_tokens, "horizons": {}}
    for h in horizons:
        out["horizons"][h] = {
            "observed": 0, "backfilled": 0, "status": {}, "venue": {}, "due_unobserved": 0,
        }
    for r in rows:
        bucket = out["horizons"].setdefault(
            r["horizon"],
            {"observed": 0, "backfilled": 0, "status": {}, "venue": {}, "due_unobserved": 0},
        )
        bucket["observed"] += r["n"]
        if r["backfilled"]:
            bucket["backfilled"] += r["n"]
        bucket["status"][r["status"]] = bucket["status"].get(r["status"], 0) + r["n"]
        venue = r["venue_kind"] or "none"
        bucket["venue"][venue] = bucket["venue"].get(venue, 0) + r["n"]
    return out


async def due_count(db: Database, horizon: str, horizon_seconds: int) -> int:
    cutoff = iso(_now() - dt.timedelta(seconds=horizon_seconds))
    return await db.fetchval(
        "select count(*) from tokens_seen t "
        "left join outcomes o on o.mint = t.mint and o.horizon = ? "
        "where t.detected_at <= ? and o.mint is null",
        horizon, cutoff,
    ) or 0


async def outcomes_for_mint(db: Database, mint: str) -> list[dict]:
    return await db.fetch(
        "select horizon, scheduled_for, observed_at, lateness_seconds, status, "
        "ambiguous_no_pool, backfilled, source, price_usd, liquidity_usd, fdv_usd, "
        "market_cap_usd, txns_h24_buys, txns_h24_sells, pair_created_at, dex_id, "
        "venue_kind, market_type, error "
        "from outcomes where mint = ? order by scheduled_for",
        mint,
    )


async def peak_for_mint(db: Database, mint: str) -> dict | None:
    return await db.fetchrow("select * from mint_peaks where mint = ?", mint)


async def rule_evaluation_rows(db: Database, *, limit: int = 200_000) -> list[dict]:
    """Every mint joined to its outcomes and peak, flattened one row per mint.

    Deliberately returns raw columns rather than a verdict. Rules are Python
    predicates over these dicts (label/rules.py) so a new rule is a function,
    not a new query.
    """
    return await db.fetch(
        "select t.mint, t.signer, t.declared_creator, t.launchpad, t.symbol, "
        "       t.detected_at, t.is_mayhem_mode, t.initial_buy_base, "
        "       t.virtual_sol_reserves, t.token_total_supply, "
        "       c.n_mints as creator_n_mints, "
        "       p.max_price_usd_seen, p.observations as peak_observations, "
        "       o24.venue_kind as venue_24h, o7.venue_kind as venue_7d, "
        "       o15.venue_kind as venue_15m, "
        "       o15.status as status_15m, o15.price_usd as price_15m, "
        "       o15.liquidity_usd as liquidity_15m, o15.lateness_seconds as lateness_15m, "
        "       o15.backfilled as backfilled_15m, o15.ambiguous_no_pool as ambiguous_15m, "
        "       o24.status as status_24h, o24.price_usd as price_24h, "
        "       o24.liquidity_usd as liquidity_24h, o24.lateness_seconds as lateness_24h, "
        "       o24.backfilled as backfilled_24h, "
        "       o7.status as status_7d, o7.liquidity_usd as liquidity_7d "
        "from tokens_seen t "
        "left join mint_peaks p on p.mint = t.mint "
        "left join creators c on c.address = t.signer and c.role = 'signer' "
        "left join outcomes o15 on o15.mint = t.mint and o15.horizon = '15m' "
        "left join outcomes o24 on o24.mint = t.mint and o24.horizon = '24h' "
        "left join outcomes o7  on o7.mint  = t.mint and o7.horizon  = '7d' "
        "order by t.detected_at desc limit ?",
        limit,
    )


# -- gate attribution ------------------------------------------------------

async def gap_attribution(db: Database, days: float = 7) -> dict:
    """Every gap between stream sessions, attributed to the host or the system.

    THE POINT. The original week-1 gate counted consecutive hours, which scores
    "the laptop got logged out" and "the collector crashed" as the same number.
    They are not the same: one is somebody else using the machine, the other is
    a bug. This separates them.

    A gap is forgiven ONLY when the preceding session recorded a clean shutdown
    -- i.e. the process was signalled. Anything else, including a session that
    simply vanished with no stop reason at all, counts against the system. An
    unexplained gap must never get the benefit of the doubt, because that is
    exactly where a crash would hide.
    """
    cutoff = iso(_now() - dt.timedelta(days=days))
    rows = await db.fetch(
        "select id, started_at, stopped_at, stop_reason from streams "
        "where started_at >= ? order by started_at",
        cutoff,
    )
    gaps: list[dict] = []
    host = system = 0
    for prev, nxt in itertools.pairwise(rows):
        started = parse_ts(nxt["started_at"])
        stopped = parse_ts(prev["stopped_at"])
        reason = prev["stop_reason"]
        if stopped is None:
            # Vanished: no stopped_at was ever written, so nothing signalled it.
            seconds = None
            attributed = "system"
            reason = reason or "vanished (no stopped_at recorded)"
        else:
            seconds = max(0, int((started - stopped).total_seconds()))
            attributed = "host" if reason == "clean shutdown" else "system"
        if attributed == "host":
            host += 1
        else:
            system += 1
        gaps.append({
            "after_stream": prev["id"], "seconds": seconds,
            "reason": reason or "(none recorded)", "attributed": attributed,
        })
    return {
        "window_days": days,
        "sessions": len(rows),
        "gaps": gaps,
        "host_caused": host,
        "system_caused": system,
    }


# -- decisions -------------------------------------------------------------

async def candidates_for_decision(db: Database, *, limit: int = 5000) -> list[dict]:
    """Candidates with no decision row yet, joined to what stages 0-2 need.

    Creator counts come from the local cache (3.4 stage 2), so this is one
    query and no network call. Structural facts are NOT joined: nothing has
    fetched them, and stage 1 reports `unfetched` rather than implying a pass.
    """
    return await db.fetch(
        "select t.mint, t.signer, t.declared_creator, t.launchpad, t.symbol, "
        "       t.is_mayhem_mode, t.stream_id, "
        "       c.n_mints as creator_n_mints, c.n_rugged as creator_n_rugged "
        "from tokens_seen t "
        "left join creators c on c.address = t.signer and c.role = 'signer' "
        "left join decisions d on d.mint = t.mint "
        "where d.mint is null "
        "order by t.detected_at desc limit ?",
        limit,
    )


async def record_decision(db: Database, *, mint: str, fields: dict) -> None:
    """Journal one verdict, accept or reject, with the inputs behind it.

    3.9.3: rejections are recorded, never dropped -- a rejection that later 10x'd
    is training data and rules-report is built to find exactly those. `mode` is
    a column, not a fork in the code (3.9.1).
    """
    await db.execute(
        "insert into decisions "
        "(mint, stream_id, decided_at, mode, outcome, reject_stage, reject_reason, "
        " inputs, cascade_ms, decision_latency_ms, thresholds, code_version) "
        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        mint, fields.get("stream_id"), iso(_now()), fields.get("mode", "PAPER"),
        fields["outcome"], fields.get("reject_stage"), fields.get("reject_reason"),
        dumps(fields.get("inputs")), dumps(fields.get("cascade_ms")),
        fields.get("decision_latency_ms"), dumps(fields["thresholds"]),
        fields["code_version"],
    )


async def decision_summary(db: Database) -> dict:
    """Accept/reject split and which stage did the rejecting."""
    rows = await db.fetch(
        "select outcome, reject_stage, count(*) as n from decisions "
        "group by outcome, reject_stage"
    )
    out = {"total": 0, "accept": 0, "reject": 0, "by_stage": {}}
    for r in rows:
        out["total"] += r["n"]
        out[r["outcome"]] = out.get(r["outcome"], 0) + r["n"]
        if r["reject_stage"] is not None:
            out["by_stage"][int(r["reject_stage"])] = (
                out["by_stage"].get(int(r["reject_stage"]), 0) + r["n"]
            )
    return out
