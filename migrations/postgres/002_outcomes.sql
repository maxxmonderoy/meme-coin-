-- Outcome labeling. Postgres. Mirror of migrations/sqlite/002_outcomes.sql.
--
-- Identical to the SQLite schema: no identity column here, so the two files
-- differ only in this header. Monetary fields stay TEXT on BOTH engines --
-- numeric would be better typed, but using it would put coercion in the DAL
-- and make 3.2's connection-string swap a lie.
--
-- WHY THIS IS ADDITIVE AND URGENT: every other part of this system is code, and
-- code is reversible -- a filter stage can be added in week 9 with no penalty.
-- This is data collection, and data collection is time-irreversible. Nobody
-- stores what happened to the tokens launching right now, so if we do not
-- record it we cannot ever ask.
--
-- Storage conventions are inherited from 001_init.sql and not re-litigated:
-- timestamps are ISO-8601 UTC TEXT, and every monetary field is TEXT holding
-- the vendor's decimal string verbatim. Not REAL: 3.10 forbids floats in any
-- amount path, and a price is an amount. Comparison happens in Python via
-- Decimal, the same way percentiles are computed in Python rather than in SQL.

-- One row per (mint, horizon). The horizons are not arbitrary: median rugged
-- token lifespan is ~14 minutes and 85% of the profitable sniper cohort has
-- exited by 5 (1.2, 1.5), so a first checkpoint at 1h would miss the entire
-- lifecycle of the median token. 15m is the shortest horizon that can still
-- see a rug before it is over.
create table if not exists outcomes (
    mint              text not null references tokens_seen(mint),
    horizon           text not null check (horizon in ('15m','1h','24h','7d')),

    -- scheduled_for is detected_at + horizon. observed_at is when we actually
    -- looked. They differ under backlog, and 'late is fine, missing is not' --
    -- a 24h label taken at 26h is usable, a missing row is not.
    scheduled_for     text not null,
    observed_at       text not null,
    lateness_seconds  integer not null default 0,

    -- Derived by us, never vendor-supplied. no_pool is a REAL OUTCOME, not a
    -- collection failure: most launches never become tradeable at all, and
    -- that is the single most common thing that happens to a token.
    --   no_pool  no tradeable pool found (never had one, or it is gone)
    --   alive    pool exists, liquidity at or above the floor
    --   dead     pool exists, liquidity below the floor
    --   error    we could not observe -- distinct from observing nothing
    status            text not null check (status in ('no_pool','alive','dead','error')),

    -- DexScreener returns nothing for a brand-new mint: a token under 60s old
    -- returned {"pairs": null} and was only indexed hours later. So an empty
    -- response at 15m may mean 'no pool YET' rather than 'no pool EVER'. This
    -- flag keeps that ambiguity from contaminating a 'died instantly' label.
    -- At 24h/7d an empty response is unambiguous and this stays 0.
    ambiguous_no_pool integer not null default 0,

    -- Rows observed after the fact. Late by definition, and kept separate so
    -- they are never silently mixed with on-schedule observations.
    backfilled        integer not null default 0,

    source            text not null,
    pool_found        integer not null default 0,

    price_usd         text,
    liquidity_usd     text,
    fdv_usd           text,
    market_cap_usd    text,
    volume_m5         text,
    volume_h1         text,
    volume_h24        text,
    txns_m5_buys      integer,
    txns_m5_sells     integer,
    txns_h1_buys      integer,
    txns_h1_sells     integer,
    txns_h24_buys     integer,
    txns_h24_sells    integer,
    price_change_h24  text,
    pair_address      text,
    pair_created_at   text,
    dex_id            text,

    attempts          integer not null default 1,
    error             text,
    payload           text,
    primary key (mint, horizon)
);
create index if not exists outcomes_horizon_status_idx on outcomes (horizon, status);
create index if not exists outcomes_observed_idx       on outcomes (observed_at desc);
create index if not exists outcomes_backfilled_idx     on outcomes (backfilled, horizon);

-- Running maximum across EVERY observation of a mint, not only at horizons.
--
-- max_price_usd_seen IS A LOWER BOUND ON THE TRUE PEAK. We sample at four
-- horizons; we do not stream prices. A token that 50x'd and round-tripped
-- between two observations reads here as whatever it was worth when we looked.
-- Nothing downstream may treat this as the actual high -- it answers "did this
-- token reach at least X", never "how high did it get".
create table if not exists mint_peaks (
    mint                   text primary key references tokens_seen(mint),
    max_price_usd_seen     text not null,
    max_liquidity_usd_seen text,
    observations           integer not null default 0,
    first_observed_at      text not null,
    last_observed_at       text not null
);
