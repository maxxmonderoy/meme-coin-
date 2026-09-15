-- Paper trading: a price series at trade resolution, and simulated positions.
--
-- WHY A NEW PRICE TABLE. The `outcomes` horizons start at 15 minutes, which is
-- after the ~14 minute median rugged lifespan (1.2) and three times longer than
-- the five minutes inside which 85% of the profitable sniper cohort has fully
-- exited (1.5). A 2x take-profit and a trailing stop (1.10) cannot be simulated
-- against a sample that arrives after the trade would already be over.
-- `trade_ticks` is that missing resolution: every trade on a held token is a
-- price observation.

-- Every trade seen on a token we are tracking. One row per on-chain trade.
create table if not exists trade_ticks (
    mint          text not null,
    signature     text not null,
    observed_at   text not null,   -- when WE saw it; the feed carries no block time
    is_buy        integer,
    sol_amount    text,            -- exact decimal string, never a float
    token_amount  text,
    price_sol     text,            -- sol_amount / token_amount, computed exactly
    trader        text,
    pool          text,
    source        text not null,
    payload       text,
    primary key (mint, signature)
);
create index if not exists trade_ticks_mint_time_idx on trade_ticks (mint, observed_at);

-- One row per simulated position. `mode` is a COLUMN, not a separate table and
-- not a separate code path (3.9.1) -- LIVE would write here too, with the same
-- logic and real fills.
create table if not exists paper_positions (
    id                integer primary key autoincrement,
    mint              text not null references tokens_seen(mint),
    decision_id       integer,
    mode              text not null check (mode in ('PAPER','LIVE')),
    opened_at         text not null,
    closed_at         text,
    status            text not null check (status in ('open','closed','abandoned')),

    -- sizing. 1.9's hard cap is enforced here as well as in `decisions`.
    bankroll_sol      text not null,
    size_pct          real not null check (size_pct > 0 and size_pct <= 2.0),
    size_sol          text not null,

    -- entry
    entry_at          text,
    entry_price_sol   text,
    entry_tick_sig    text,
    tokens_bought     text,
    entry_slippage_pct real,

    -- exits are decided BEFORE entry (1.10) and stored so the plan cannot be
    -- rewritten after the fact.
    plan_tp_multiple  real not null,
    plan_trail_pct    real not null,
    plan_timeout_s    integer not null,

    -- outcome
    exit_reason       text,
    realised_sol      text,
    fees_sol          text,           -- logged SEPARATELY from pnl (1.10)
    pnl_sol           text,
    pnl_pct           real,
    peak_price_sol    text,
    ticks_seen        integer not null default 0,
    code_version      text not null,
    params            text not null
);
create index if not exists paper_positions_status_idx on paper_positions (status, opened_at);
create index if not exists paper_positions_mint_idx   on paper_positions (mint);

-- Every simulated fill, entry and exit. Fees are itemised rather than netted so
-- 1.10's "fees logged separately from PnL" is a property of the schema.
create table if not exists paper_fills (
    id             integer primary key autoincrement,
    position_id    integer not null references paper_positions(id),
    kind           text not null check (kind in ('entry','tp','trail','timeout','stop','rug')),
    filled_at      text not null,
    tick_signature text,
    quote_price_sol text,          -- what the tick said
    fill_price_sol  text,          -- what we modelled after the haircut
    slippage_pct   real,
    tokens         text,
    gross_sol      text,
    fee_launchpad_sol text,
    fee_priority_sol  text,
    fee_tip_sol       text,
    fee_total_sol     text,
    net_sol        text
);
create index if not exists paper_fills_position_idx on paper_fills (position_id);
