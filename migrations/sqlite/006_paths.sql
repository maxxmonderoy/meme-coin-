-- Dense price paths, structural events, and the bounded watch set.
--
-- WHY THIS REPLACES FIXED HORIZONS. A trailing stop cannot be backtested
-- against four data points. Median rugged lifespan is ~14 minutes (1.2) and 85%
-- of the profitable sniper cohort is fully out inside five (1.5), so everything
-- that decides an exit happens before a one-hour checkpoint would fire.
--
-- `outcomes` and `mint_peaks` are NOT dropped. They feed creators.n_rugged and
-- therefore stage 2, the only rule in the cascade that can ever reject on
-- creator history. Their horizon rows are derived FROM the dense path instead,
-- so rug labelling keeps working and gets more accurate rather than breaking.

-- Which mints we are densely sampling, and why.
--
-- TWO COHORTS, and the control one is what makes the rest mean anything.
-- `filtered` holds tokens that passed whatever entry criteria are being tested;
-- `control` is a uniform random sample of ALL launches, ignoring every filter.
-- Without the control arm every backtest is selection-biased in a way that
-- cannot be detected from inside the data, because the only tokens with paths
-- would be ones the filter already liked.
create table if not exists watch_set (
    mint            text primary key references tokens_seen(mint),
    cohort          text not null check (cohort in ('filtered','control')),
    admitted_at     text not null,
    expires_at      text not null,
    first_seen_at   text not null,   -- launch time; the cadence clock starts here
    state           text not null default 'active'
                        check (state in ('active','expired','evicted','error')),
    last_observed_at text,
    next_due_at     text,
    observations    integer not null default 0,
    misses          integer not null default 0,
    evicted_reason  text
);
create index if not exists watch_set_due_idx    on watch_set (state, next_due_at);
create index if not exists watch_set_cohort_idx on watch_set (cohort, admitted_at desc);

-- One row per observation. This is the price PATH -- the thing an exit rule
-- actually replays over.
--
-- `status` carries the cold start explicitly. DexScreener returns nothing for a
-- mint that is minutes old: during research a token under 60 seconds old
-- returned {"pairs": null} and was only indexed hours later. Early emptiness
-- means NOT YET INDEXED, not dead, and conflating the two would manufacture
-- deaths for exactly the youngest tokens -- the ones this whole change exists
-- to observe.
create table if not exists price_path (
    mint            text not null references tokens_seen(mint),
    observed_at     text not null,
    scheduled_for   text not null,
    lateness_ms     integer not null default 0,
    source          text not null,          -- dexscreener | rugcheck_fallback
    status          text not null check (status in
                        ('indexed','not_yet_indexed','no_pool','error')),
    venue_kind      text,                   -- bonding_curve | dex
    price_usd       text,
    price_native    text,
    liquidity_usd   text,
    fdv_usd         text,
    market_cap_usd  text,
    volume_m5       text,
    volume_h1       text,
    txns_m5_buys    integer,
    txns_m5_sells   integer,
    pair_address    text,
    dex_id          text,
    age_seconds     integer not null,
    compacted       integer not null default 0,
    payload         text,
    primary key (mint, observed_at)
);
create index if not exists price_path_mint_time_idx on price_path (mint, observed_at);
create index if not exists price_path_status_idx    on price_path (status, observed_at);

-- State CHANGES rather than prices. The whole question this table answers is
-- whether a structural signal fires BEFORE the price move completes -- which is
-- the only kind of stop that works on an illiquid token, because a price stop
-- assumes a bid that is not there mid-rug.
--
-- before/after are exact text, never floats, so a 3% liquidity move and a 97%
-- one stay distinguishable after storage.
create table if not exists structural_events (
    mint         text not null references tokens_seen(mint),
    event_type   text not null,
    detected_at  text not null,
    observed_at  text not null,   -- timestamp of the data the event came from
    derived_from text not null,   -- rugcheck_poll | price_path_delta
    before_value text,
    after_value  text,
    delta_pct    real,
    severity     text,
    payload      text,
    primary key (mint, event_type, detected_at)
);
create index if not exists structural_events_mint_idx on structural_events (mint, detected_at);
create index if not exists structural_events_type_idx on structural_events (event_type, detected_at);

-- What compaction has already downsampled, so a replay knows the fidelity it is
-- working at rather than silently assuming full resolution.
create table if not exists path_compaction (
    mint           text not null,
    compacted_at   text not null,
    from_age_s     integer not null,
    kept_interval_s integer not null,
    rows_before    integer not null,
    rows_after     integer not null,
    primary key (mint, compacted_at)
);
