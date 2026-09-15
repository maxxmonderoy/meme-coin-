-- A rate budget shared across PROCESSES.
--
-- `trenches label` and `trenches sample` both call DexScreener and each built
-- its own in-process token bucket at 60 req/min, so running both put the
-- account at 120 req/min against a documented 60 -- with no rate-limit headers
-- exposed, which means the first sign of trouble would have been a ban.
--
-- A module-level singleton does not fix this: the two are separate processes.
-- The database is the only state they already share, so the bucket lives here.
-- One row per named budget, updated by optimistic compare-and-swap so two
-- processes cannot both spend the same token.
create table if not exists rate_budget (
    name        text primary key,
    tokens      real not null,
    rate_per_minute real not null,
    capacity    real not null,
    updated_at  text not null,
    spent_total integer not null default 0
);
