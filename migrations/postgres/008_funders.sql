-- Funding edges and derived shared-funder labels, Postgres.
-- Mirror of migrations/sqlite/008_funders.sql. Identical; no identity columns.
-- WHY THIS EXISTS RATHER THAN A VENDOR LIST. Stage 5 must strip CEX addresses
-- before the union-find or its answer inverts: every wallet funded from one
-- exchange hot wallet becomes one cluster spanning most of the holder set. But
-- an exchange address list is a maintained dataset we have to buy, refresh, and
-- trust -- and a stale entry stops stripping SILENTLY.
--
-- The label does not actually need to say "this is Binance". It needs to say
-- "this funder's presence is not evidence of coordination", and that is a
-- property of the graph: infrastructure funds wallets across many UNRELATED
-- tokens, while a dev's funding wallet funds wallets inside its own launches.
-- So the discriminator stored here is `n_tokens`, not raw out-degree.

-- One row per observed funding relationship, per token whose graph it appeared
-- in. Keyed by mint as well as the pair, because the same funder showing up
-- across many tokens is exactly the signal, and deduplicating it away globally
-- would delete the thing being measured.
create table if not exists funding_edges (
    mint          text not null references tokens_seen(mint),
    funder        text not null,
    funded        text not null,
    relation      text not null default 'funding',
    hops          integer not null default 1,
    source        text not null,
    discovered_at text not null,
    primary key (mint, funder, funded)
);
create index if not exists funding_edges_funder_idx on funding_edges (funder);
create index if not exists funding_edges_mint_idx   on funding_edges (mint);

-- Derived labels. Rewritten wholesale by `trenches funders`, never edited in
-- place, so the cutoff that produced a row is always the cutoff recorded on it.
--
-- `n_tokens` is the number of DISTINCT mints in whose funding graph this
-- address appeared as a funder. `n_funded` is raw out-degree, kept only because
-- it is free and useful in the report -- it is NOT what the cutoff is applied
-- to, because a single dev funding forty sniper wallets inside one launch has a
-- high out-degree and is precisely the actor stage 5 exists to catch.
create table if not exists funder_labels (
    address       text primary key,
    label         text not null,
    n_tokens      integer not null,
    n_funded      integer not null,
    cutoff_tokens integer not null,
    derived_at    text not null,
    source        text not null
);
create index if not exists funder_labels_label_idx on funder_labels (label);
