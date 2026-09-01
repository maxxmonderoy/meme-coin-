-- Structural facts, Postgres. Mirror of migrations/sqlite/005_structural.sql.
-- Identical: this migration adds no identity column, so there is nothing to
-- differ. See 001_init.sql for why the types match across engines.
--
-- These are the fields that land within ~1 second of a launch: mint, freeze and
-- close authority, the Token-2022 upgradable flags, transfer hooks and fees.
-- Behavioural fields -- bundle share, sniper counts, insider clusters, holder
-- concentration, and any vendor score -- are DELIBERATELY ABSENT from this
-- table. They have a cold start, at a minute old they are empty rather than
-- clean, and Part 2 names reading empty as clean the most expensive mistake
-- available here. A separate table makes that boundary structural instead of a
-- convention someone erodes later.
--
-- `fetched_at` non-null with all flags null means "we asked and the vendor had
-- nothing", which is a different fact from never having asked. Stage 1 needs to
-- tell those apart to keep reporting `unfetched` honestly.

create table if not exists token_structural (
    mint            text primary key references tokens_seen(mint),
    source          text not null,
    fetched_at      text not null,
    status_code     integer,

    mintable                        integer,
    freezable                       integer,
    closable                        integer,
    balance_mutable_authority       integer,
    transfer_fee_upgradable         integer,
    transfer_hook_upgradable        integer,
    metadata_mutable                integer,
    default_account_state_upgradable integer,
    non_transferable                integer,
    transfer_hook                   text,
    transfer_fee                    text,
    malicious_address               integer,

    fields_present  integer not null default 0,
    error           text,
    payload         text
);
create index if not exists token_structural_fetched_idx on token_structural (fetched_at desc);
