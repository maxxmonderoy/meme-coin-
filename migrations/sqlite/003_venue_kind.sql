-- Distinguish a bonding curve from a graduated DEX pool. SQLite.
-- Mirror of migrations/postgres/003_venue_kind.sql.
--
-- WHY: the first live run labeled 30% of 15-minute observations `alive`, against
-- a documented graduation rate under 1% (1.3). That gap was the bug.
--
-- RugCheck's `markets` array is dominated by `marketType: pump_fun` -- 275 of
-- 305 markets in the first sample. That is the BONDING CURVE, which every
-- pump.fun launch has by construction, not a pool it graduated into. Reading it
-- as "has liquidity" makes `alive` mean "this token exists", which is true of
-- everything and therefore worth nothing to calibrate against.
--
-- It is equally wrong to call a curve dead: you really can buy and sell against
-- one, so a token with $2k in its curve is genuinely tradeable. The two states
-- are simply not the same question, and a single flag cannot carry both. So the
-- venue is recorded and `status` keeps meaning "could this have been sold".
--
-- This also explains why DexScreener answered for only 28 of 861 observations
-- while RugCheck answered for 694: DexScreener indexes graduated pools. Its
-- silence was never "no pool", it was "not graduated" -- which is the single
-- most common outcome and now has a name.

alter table outcomes add column venue_kind text;
alter table outcomes add column market_type text;

create index if not exists outcomes_venue_idx on outcomes (venue_kind, horizon);
