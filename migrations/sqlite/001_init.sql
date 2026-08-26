-- Trenches week 1, SQLite. Mirror of migrations/postgres/001_init.sql.
--
-- Two storage conventions differ from what a Postgres-only schema would do,
-- and both are deliberate:
--
--  * Token amounts are TEXT holding an exact integer. SQLite's INTEGER is
--    64-bit SIGNED, so a u64 supply above 2^63-1 wraps silently. REAL is
--    forbidden outright -- no floats in any amount path (3.10).
--  * Timestamps are ISO-8601 UTC TEXT. Sorts correctly as a string and stays
--    readable by eye, which matters because reading the journal by eye is what
--    week 1 is for.

create table if not exists schema_migrations (
    version    text primary key,
    applied_at text not null
);

-- One row per consumer run. `subscription` stores the exact request sent, so a
-- feed's behaviour can be reconstructed after the fact rather than remembered.
create table if not exists streams (
    id           integer primary key autoincrement,
    feeds        text    not null,
    subscription text    not null,
    code_version text    not null,
    started_at   text    not null,
    stopped_at   text,
    stop_reason  text
);

-- Retained event payloads. Identity is (feed, event_id) where event_id is the
-- signature when the feed provides one and the mint when it does not --
-- RugCheck's new_tokens listing has no signature.
create table if not exists raw_events (
    feed        text not null,
    event_id    text not null,
    mint        text,
    event_kind  text not null,
    signature   text,
    slot        integer,
    stream_id   integer not null references streams(id),
    commitment  text not null default 'processed',
    block_time  text,
    received_at text not null,
    payload     text,
    primary key (feed, event_id)
);
create index if not exists raw_events_received_idx on raw_events (received_at desc);
create index if not exists raw_events_mint_idx     on raw_events (mint);

-- One row per mint. Candidate dedupe is on mint (3.3), not on (sig, slot):
-- two independent feeds report the same launch with different identifiers, so
-- the mint is the only identity both agree on.
create table if not exists tokens_seen (
    mint                text primary key,
    signer              text,
    declared_creator    text,
    program_id          text,
    launchpad           text,
    bonding_curve       text,
    quote_mint          text,
    token_program       text,
    name                text,
    symbol              text,
    uri                 text,
    mint_authority      text,
    freeze_authority    text,
    decimals            integer,
    token_total_supply      text,
    virtual_sol_reserves    text,
    virtual_token_reserves  text,
    real_token_reserves     text,
    initial_buy_base        text,
    is_mayhem_mode      integer,
    is_cashback_enabled integer,
    first_feed          text not null,
    first_signature     text,
    first_slot          integer,
    stream_id           integer references streams(id),
    block_time          text,
    detected_at         text not null,
    detect_latency_ms   integer,
    structural_fetched_at text
);
create index if not exists tokens_seen_detected_idx on tokens_seen (detected_at desc);
create index if not exists tokens_seen_signer_idx   on tokens_seen (signer);
create index if not exists tokens_seen_creator_idx  on tokens_seen (declared_creator);

-- THE table that decides whether paid gRPC is ever worth buying (3.2 upgrade
-- trigger 2, 3.3). Which feed saw each mint first, and by how much. Without
-- this the upgrade decision is a feeling.
create table if not exists feed_latency (
    mint            text primary key,
    first_feed      text not null,
    first_seen_at   text not null,
    second_feed     text,
    second_seen_at  text,
    delta_ms        integer,
    stream_id       integer references streams(id)
);
create index if not exists feed_latency_first_idx on feed_latency (first_feed);

-- Permanent creator reputation cache (3.4 stage 2). Keyed on (address, role)
-- because pump.fun's CreateEvent carries `user` and `creator` as separate
-- pubkeys: a cache keyed on one address alone is bypassed by rotating the
-- other. rug_rate is derived at read time, never stored.
create table if not exists creators (
    address           text not null,
    role              text not null,
    n_mints           integer not null default 0,
    n_rugged          integer not null default 0,
    median_lifetime_s integer,
    first_seen        text not null,
    last_seen         text not null,
    backfilled_at     text,
    primary key (address, role)
);

-- Per-minute counters. Detection RATE is the 3.8.8 canary; an error-rate
-- monitor never fires on a decoder that broke silently.
create table if not exists feed_health (
    stream_id        integer not null references streams(id),
    feed             text    not null,
    minute           text    not null,
    events_total     integer not null default 0,
    events_create    integer not null default 0,
    events_other     integer not null default 0,
    dupes            integer not null default 0,
    decode_failures  integer not null default 0,
    schema_mismatches integer not null default 0,
    queue_high_water integer not null default 0,
    queue_drops      integer not null default 0,
    reconnects       integer not null default 0,
    stale_responses  integer not null default 0,
    wins             integer not null default 0,
    primary key (stream_id, feed, minute)
);

-- DESIGNED NOW, WRITTEN TO IN WEEK 2. Deliberately empty.
-- mode is a column, not a fork of the code path (3.9.1). thresholds and
-- code_version are snapshotted per row because tuning a threshold otherwise
-- silently invalidates every earlier row's comparability.
create table if not exists decisions (
    id                  integer primary key autoincrement,
    mint                text not null references tokens_seen(mint),
    stream_id           integer references streams(id),
    decided_at          text not null,
    mode                text not null check (mode in ('PAPER','LIVE')),
    outcome             text not null check (outcome in ('accept','reject')),
    reject_stage        integer check (reject_stage between 0 and 6),
    reject_reason       text,
    inputs              text not null,
    cascade_ms          text,
    score               real,
    size_pct_bankroll   real check (size_pct_bankroll is null or size_pct_bankroll <= 2.0),
    size_lamports       text,
    modelled_fees       text,
    decision_latency_ms integer,
    thresholds          text not null,
    code_version        text not null,
    check ((outcome = 'reject') = (reject_stage is not null))
);
create index if not exists decisions_mint_idx    on decisions (mint, decided_at desc);
create index if not exists decisions_outcome_idx on decisions (outcome, decided_at desc);

-- Manual-command-only vendor responses (Part 4). `bypassed_cache` records
-- whether a refresh was REQUESTED, not that it worked -- RugCheck's
-- refresh=true is paid-tier, so on the free tier a bypass cannot be assumed.
create table if not exists enrich_cache (
    provider       text not null,
    endpoint       text not null,
    key            text not null,
    fetched_at     text not null,
    status_code    integer,
    bypassed_cache integer not null default 0,
    latency_ms     integer,
    payload        text,
    primary key (provider, endpoint, key, fetched_at)
);
create index if not exists enrich_cache_lookup_idx
    on enrich_cache (provider, endpoint, key, fetched_at desc);
