-- Trenches week 1: ingest + journal.
--
-- Design notes that are not obvious from the DDL:
--
--  * raw_events is NOT partitioned. Under the selected retention mode
--    (creates_full) this table receives roughly one row per launch, order
--    42k/day, and a plain table lets the primary key be exactly
--    (signature, slot) -- the CLAUDE.md 3.8.4 dedupe invariant, enforced by
--    the database rather than only by an in-memory cache. Partitioning
--    would force received_at into the key and reduce that guarantee to
--    "unique within a day". If retention is ever switched to `all`, add a
--    partitioning migration then; do not pre-build it now.
--
--  * Every token amount is numeric(40,0) holding BASE UNITS. No floats
--    anywhere in an amount path (3.10). numeric(40,0) covers u64 and u128
--    supply values without loss.
--
--  * decisions is created empty and stays empty until week 2. The shape is
--    fixed now because thresholds and code_version must be snapshotted per
--    decision -- without them, "what was my win rate where bundlers were
--    10-15%" becomes unanswerable the moment a threshold is tuned.

create table if not exists schema_migrations (
    version     text primary key,
    applied_at  timestamptz not null default now()
);

-- ---------------------------------------------------------------------------
-- streams: one row per consumer run.
-- The exact subscription is stored so the 3.3 strict-vs-naive filter diff can
-- be reconstructed after the fact. Without this the experiment is unrepeatable.
-- ---------------------------------------------------------------------------
create table if not exists streams (
    id              bigserial primary key,
    provider        text        not null,   -- yellowstone | pumpportal | replay
    filter_mode     text        not null,   -- strict | naive | both
    subscription    jsonb       not null,   -- verbatim request sent to the server
    code_version    text        not null,   -- git sha at process start
    started_at      timestamptz not null default now(),
    stopped_at      timestamptz,
    stop_reason     text
);
create index if not exists streams_started_idx on streams (started_at desc);

-- ---------------------------------------------------------------------------
-- raw_events: retained event payloads.
-- Under creates_full only creates land here; other kinds are counted in
-- stream_health and dropped. commitment is recorded because 3.8.7 forbids
-- treating `processed` as authoritative -- the reconcile tier must be visible.
-- ---------------------------------------------------------------------------
create table if not exists raw_events (
    signature       text        not null,
    slot            bigint      not null,
    stream_id       bigint      not null references streams(id),
    provider        text        not null,
    event_kind      text        not null,   -- create | trade | migrate | other
    program_id      text,
    commitment      text        not null,   -- processed | confirmed | finalized
    block_time      timestamptz,
    received_at     timestamptz not null default now(),
    filter_source   text,                   -- which subscription matched: strict|naive|both
    payload         jsonb,
    primary key (signature, slot)
);
create index if not exists raw_events_received_idx on raw_events (received_at desc);
create index if not exists raw_events_kind_idx     on raw_events (event_kind, received_at desc);

-- ---------------------------------------------------------------------------
-- tokens_seen: one row per mint, plus the structural facts that land in ~1s.
--
-- signer vs declared_creator: pump.fun's `create` instruction takes `creator`
-- as an explicit argument and CreateEvent emits `user` and `creator` as
-- SEPARATE pubkeys. They are frequently the same and need not be. Storing only
-- one of them gives the 3.4 stage-2 reputation cache a trivial bypass: rotate
-- the declared creator, keep signing from the same wallet (or the reverse).
-- Both are stored; stage 2 must consider the worse of the two.
-- ---------------------------------------------------------------------------
create table if not exists tokens_seen (
    mint                text        primary key,
    signer              text,               -- CreateEvent.user  (pays, signs)
    declared_creator    text,               -- CreateEvent.creator (fee recipient)
    program_id          text        not null,
    launchpad           text,
    bonding_curve       text,
    quote_mint          text,               -- non-SOL curve pairs exist
    token_program       text,               -- spl-token vs token-2022
    name                text,
    symbol              text,
    uri                 text,
    -- structural facts (Part 2: these land fast; behavioural ones do not)
    mint_authority      text,
    freeze_authority    text,
    decimals            smallint,
    token_total_supply  numeric(40,0),
    virtual_sol_reserves    numeric(40,0),
    virtual_token_reserves  numeric(40,0),
    real_token_reserves     numeric(40,0),
    is_mayhem_mode      boolean,
    is_cashback_enabled boolean,
    -- provenance
    first_signature     text        not null,
    first_slot          bigint      not null,
    stream_id           bigint      references streams(id),
    block_time          timestamptz,        -- on-chain create time
    detected_at         timestamptz not null default now(),
    detect_latency_ms   integer,            -- detected_at - block_time; stage-0 metric
    structural_fetched_at timestamptz       -- null until authorities are resolved
);
create index if not exists tokens_seen_detected_idx on tokens_seen (detected_at desc);
create index if not exists tokens_seen_signer_idx   on tokens_seen (signer);
create index if not exists tokens_seen_creator_idx  on tokens_seen (declared_creator);

-- ---------------------------------------------------------------------------
-- creators: permanent reputation cache (3.4 stage 2). Never expires --
-- creators recur and history does not go stale. Populated as a byproduct of
-- ingest in week 1; rug labelling arrives week 2.
--
-- `role` distinguishes how the address appeared, so a rotated declared-creator
-- and a persistent signer both accumulate history independently.
-- ---------------------------------------------------------------------------
create table if not exists creators (
    address             text        not null,
    role                text        not null,   -- signer | declared
    n_mints             integer     not null default 0,
    n_rugged            integer     not null default 0,
    median_lifetime_s   integer,
    first_seen          timestamptz not null default now(),
    last_seen           timestamptz not null default now(),
    backfilled_at       timestamptz,            -- null => "unknown", proceed (3.4)
    primary key (address, role)
);
-- rug_rate is derived, never stored: storing it invites it going stale.
create index if not exists creators_lastseen_idx on creators (last_seen desc);

-- ---------------------------------------------------------------------------
-- stream_health: per-minute counters. This is what makes `stats` report
-- DETECTION RATE rather than error rate -- 3.8.8's canary is a drop in
-- detections, which an error-rate monitor never fires on.
-- Non-create traffic under creates_full retention is counted here and dropped.
-- ---------------------------------------------------------------------------
create table if not exists stream_health (
    stream_id       bigint      not null references streams(id),
    minute          timestamptz not null,
    events_total    integer     not null default 0,
    events_create   integer     not null default 0,
    events_other    integer     not null default 0,
    dupes           integer     not null default 0,
    decode_failures integer     not null default 0,
    queue_high_water integer    not null default 0,
    queue_drops     integer     not null default 0,
    reconnects      integer     not null default 0,
    slot_gaps       integer     not null default 0,
    max_slot        bigint,
    primary key (stream_id, minute)
);

-- ---------------------------------------------------------------------------
-- slot_gaps: 3.8.5 -- Solana genuinely skips slots when a leader fails, so a
-- gap is not necessarily data loss. Recorded unresolved and reconciled later
-- rather than triggering expensive backfill on sight.
-- ---------------------------------------------------------------------------
create table if not exists slot_gaps (
    stream_id       bigint      not null references streams(id),
    from_slot       bigint      not null,
    to_slot         bigint      not null,
    detected_at     timestamptz not null default now(),
    resolution      text,       -- skipped | missed | unknown
    resolved_at     timestamptz,
    primary key (stream_id, from_slot, to_slot)
);

-- ---------------------------------------------------------------------------
-- enrich_cache: manual-command-only in week 1. Exists so the cold-start
-- finding in Part 2 can be reproduced by hand against real mints.
-- bypassed_cache records whether an explicit refresh was used -- for held
-- positions (3.4 stage 6) reusing a cached verdict is the exact documented
-- failure, so the flag must be auditable rather than assumed.
-- ---------------------------------------------------------------------------
create table if not exists enrich_cache (
    provider        text        not null,
    endpoint        text        not null,
    key             text        not null,   -- usually the mint
    fetched_at      timestamptz not null default now(),
    status_code     integer,
    bypassed_cache  boolean     not null default false,
    latency_ms      integer,
    payload         jsonb,
    primary key (provider, endpoint, key, fetched_at)
);
create index if not exists enrich_cache_lookup_idx on enrich_cache (provider, endpoint, key, fetched_at desc);

-- ---------------------------------------------------------------------------
-- decisions: DESIGNED NOW, WRITTEN TO IN WEEK 2. Deliberately empty.
--
-- mode is a column, not a separate table and not a separate code path (3.9.1).
-- thresholds and code_version are snapshotted per row because tuning a
-- threshold otherwise silently invalidates every prior row's comparability.
-- Rejections are journaled with full inputs (3.9.3): a rejection that later
-- 10x'd is training data, and it is only training data if the inputs survive.
-- ---------------------------------------------------------------------------
create table if not exists decisions (
    id                  bigserial   primary key,
    mint                text        not null references tokens_seen(mint),
    stream_id           bigint      references streams(id),
    decided_at          timestamptz not null default now(),
    mode                text        not null check (mode in ('PAPER','LIVE')),
    outcome             text        not null check (outcome in ('accept','reject')),
    reject_stage        smallint    check (reject_stage between 0 and 6),
    reject_reason       text,
    inputs              jsonb       not null,
    cascade_ms          jsonb,      -- per-stage timing; proves the 3.4 ordering pays
    score               numeric(10,4),
    size_pct_bankroll   numeric(6,4) check (size_pct_bankroll is null
                                            or size_pct_bankroll <= 2.0),
    size_lamports       bigint,
    modelled_fees       jsonb,      -- fees journaled separately from PnL (1.10)
    decision_latency_ms integer,
    thresholds          jsonb       not null,
    code_version        text        not null,
    constraint reject_has_stage
        check ((outcome = 'reject') = (reject_stage is not null))
);
create index if not exists decisions_mint_idx    on decisions (mint, decided_at desc);
create index if not exists decisions_outcome_idx on decisions (outcome, decided_at desc);

insert into schema_migrations (version) values ('001_init')
    on conflict (version) do nothing;
