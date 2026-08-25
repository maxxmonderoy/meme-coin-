# Trenches — week 1: ingest + journal

Reads Solana launch events, decodes pump.fun `create`, and writes a journal.

**It does not decide anything and it cannot spend anything.** There is no
`decide()`, no `execute()`, no position tracker and no signing key in this
codebase. That is the week-1 scope from `CLAUDE.md` Part 4, and the point of it
is to prove one thing before anything downstream matters: *can this hold a
stable stream for seven days unattended, and what does real launch volume
actually look like?*

## What exists

| Piece | State |
|---|---|
| Yellowstone gRPC consumer | written, **untested against a live endpoint** (no subscription yet) |
| PumpPortal consumer | transport + frame recorder written; **frame mapping deliberately unimplemented** |
| Replay consumer | working — the only feed that needs no credentials |
| Supervisor: backoff + slot watchdog | working, tested |
| Bounded queue + worker pool | working, tested |
| `(signature, slot)` dedupe | working, tested (memory cache + database primary key) |
| pump.fun `CreateEvent` decoder | working, tested — layout read from the vendored IDL |
| Postgres schema + migrations | working, tested against Postgres 16 |
| `decisions` table | **designed, empty, unwritten** — week 2 |
| Enrichment client (RugCheck) | written, **manual command only**, never in the ingest path |

## Quick start

```bash
uv venv .venv
uv pip install --python .venv/bin/python --require-hashes --only-binary :all: -r requirements.txt
docker compose up -d db

cp .env.example trenches.local.conf     # NOT `.env` — see 3.10.6
export TRENCHES_ENV_FILE=trenches.local.conf

trenches migrate
trenches stream --record captures/      # replay feed by default; needs no subscription
trenches stats --hours 24
trenches inspect <mint>
trenches idl-check
```

## The four things worth knowing before you touch this

**1. Detection rate is the canary, not the error rate.** A program layout
change produces corrupt output, not exceptions (3.8.8). `trenches stats` prints
creates/min and shouts if the window contains zero creates. Watch that number,
not the log volume.

**2. The decoder's field layout comes from `vendor/idl/pump.json`, not from
constants in the source.** `trenches idl-check` diffs the vendored copy against
upstream and separates additive changes from breaking ones (changed
discriminators, changed event layouts, changed program address). Run it on a
schedule. As of the last run both IDLs were clean.

**3. `signer` and `declared_creator` are stored separately.** pump.fun's
`create` takes `creator` as an explicit argument and `CreateEvent` emits `user`
and `creator` as distinct pubkeys. They are usually equal and need not be. The
stage-2 reputation cache (week 2) keys on **both**, because a cache keyed on
one is bypassed by rotating the other.

**4. 13 instruction discriminators are byte-identical between the bonding curve
and the AMM** — `buy` and `sell` among them. Every decode path keys on program
id first. Never on sighash.

## Filter modes

`TRENCHES_FILTER_MODE` selects the Yellowstone subscription:

- `naive` — `account_include[pump]`. Matches every buy and sell; ~99% is
  discarded client-side.
- `strict` — adds `account_required[metaplex token metadata]`. `create` CPIs
  into the metadata program; `buy`, `sell`, `buy_v2`, `sell_v2` and `migrate`
  do not (verified against the IDL account lists), so this pushes
  discrimination server-side.
- `both` — declares both as separately-named filters in one request, so every
  update reports which matched.

**Run `both` for at least an hour and check `trenches stats` before trusting
`strict`.** pump.fun's create account list has changed before. The
`by filter` block in `stats` is that comparison; if `strict` catches fewer
creates than `naive`, the premise is broken for those launches.

## Retention

`TRENCHES_RETENTION=creates_full` (default) keeps the full payload for creates
and counts everything else in `stream_health` without storing it. At roughly
42k launches/day that is a few hundred thousand rows a week.

`raw_events` is intentionally **not partitioned**: at this volume a plain table
lets the primary key be exactly `(signature, slot)`, which is the dedupe
invariant from 3.8.4 enforced by the database. Switching to
`TRENCHES_RETENTION=all` changes the volume by orders of magnitude and should
come with a partitioning migration.

## Security posture in this repo

- Dependencies are pinned with hashes and installed `--require-hashes
  --only-binary :all:` (3.10.10).
- `requirements.txt` is compiled with `uv --exclude-newer`, a **30-day minimum
  release age** — the Python analogue of the npm control in 3.10.3, and the one
  control in the incident record that never failed.
- No Node, no npm, no install scripts anywhere in the tree.
- `.env` is gitignored and is *not* the intended config filename; point
  `TRENCHES_ENV_FILE` at something that isn't on a stealer's grep list.
- There are no keys here. Nothing in this package can construct, sign or send
  a transaction. When that changes — week 6 — it changes behind the
  decode-before-sign gate, not before it.

## Not done yet

- The Yellowstone consumer has **never run against a real endpoint.** Its
  request shape is verified against the real `geyser.proto` and its channel
  options are set per 3.8.1/3.8.3, but "compiles and builds a correct
  SubscribeRequest" is not "works".
- `PumpPortalConsumer.map_frame` raises `NotImplementedError`. The docs host
  was unreachable, so the frame schema is unverified and guessing it would
  produce a consumer that looks fine and emits wrong mints. Record real frames
  with `--record`, read them, then implement it.
- **The seven-day soak has not been run.** That is the actual week-1
  deliverable and it needs a host that isn't a laptop.
