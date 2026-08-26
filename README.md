# Trenches — week 1: ingest + journal

Races two free Solana launch feeds, dedupes them, and writes a journal.

**Budget: $0.** No paid service, no API key, no subscription. Per §3.2 that is
the intended path through week 5, not a fallback.

**It decides nothing and it cannot spend anything.** There is no `decide()`, no
`execute()`, no position tracker and no key material in this codebase. Nothing
here can construct or sign a transaction.

## What week 1 is for

Proving two free feeds can run seven days unattended, seeing what real launch
volume looks like — and collecting the **feed race data** that is the only
evidence which could ever justify paying for a faster feed later.

## Quick start

```bash
uv venv .venv
uv pip install --python .venv/bin/python --require-hashes --only-binary :all: -r requirements.txt

cp .env.example trenches.local.conf        # NOT `.env` — see §3.10.6
export TRENCHES_ENV_FILE=trenches.local.conf

trenches migrate
trenches stream --record captures/
trenches stats --hours 24
trenches verify-capture captures/
trenches inspect <mint> --enrich
```

No database server to install. SQLite is the default; Postgres is a
connection-string change and both are covered by the same tests.

## The five things worth knowing

**1. The feed race is the point, not a nicety.** `feed_latency` records which
feed saw each mint first and by how many milliseconds. `trenches stats` prints
it. §3.2's upgrade trigger 2 says prove from the journal that feed gaps cost
you candidates — this table is that proof, or its absence. A feed that never
wins, or wins by milliseconds you cannot act on, is not worth upgrading to.

**2. PumpPortal's payload schema is UNVERIFIED and the code says so.** Their
own repo documents the subscribe side exactly (`wss://pumpportal.fun/api/data`,
`{"method":"subscribeNewToken"}`) but never publishes the response shape. So
field names here are a declared hypothesis: every frame is validated, a frame
missing `mint` or `signature` is recorded as a mismatch rather than becoming a
create with a null mint, and the raw frame is always preserved. Run
`trenches verify-capture` on a recording to confirm or correct it, then set
`NEW_TOKEN_SCHEMA.verified = True`.

Note: their docs list three methods and **`subscribeMigration` is not among
them**, so it is not subscribed despite appearing in §3.2.

**3. The RugCheck feed refuses to start if it is stale.** Without a
cache-buster that endpoint returns ~3-hour-old entries — no error, nothing
looks broken, and you get a feed that reports launches all day while being
useless. Every request carries a buster, and `assert_fresh()` runs at startup
and aborts if the newest entry is older than 10 minutes. Finding that out on
day seven of a seven-day soak wastes the week.

**4. Dedupe is on mint; storage is per feed.** Two feeds describe the same
launch with different identifiers and the mint is the only one they agree on.
But `raw_events` is keyed `(feed, event_id)` so both sightings are stored — that
is what makes the race measurable.

**5. `signer` and `declared_creator` are stored separately.** pump.fun's
`create` takes `creator` as an explicit argument and `CreateEvent` emits `user`
and `creator` as distinct pubkeys. The stage-2 reputation cache keys on **both**,
because one keyed on either alone is bypassed by rotating the other.

## Storage

SQLite with WAL. Amounts are stored as exact-integer **TEXT**, not INTEGER:
SQLite's INTEGER is 64-bit *signed*, so a u64 supply above 2^63−1 would wrap
silently. No floats anywhere in an amount path (§3.10).

Timestamps are ISO-8601 UTC text on **both** dialects. Postgres could use
native types, but that would put coercion in the DAL and make the
connection-string swap a lie. The two migration files differ only in the
identity column.

## Security posture

Dependencies hash-pinned, installed `--require-hashes --only-binary :all:`
(§3.10.10), compiled with `uv --exclude-newer` for a **30-day minimum release
age** — the Python analogue of §3.10.3, and the one control in the incident
record that never failed. No Node, no npm, no install scripts. `.env` and
`*.local.conf` are gitignored.

## Not done yet

- **The seven-day soak has not run.** That is the actual week-1 deliverable.
- **PumpPortal's schema is unconfirmed** — `verify-capture` exists precisely to
  close that, and needs real recorded frames.
- **`n_rugged` is never written.** `creators` counts mints as an ingest
  byproduct, but nothing labels rugs yet, so §3.4 stage 2's
  `n_mints >= 3 AND rug_rate >= 0.6` cannot fire. That is week-2 work and it
  needs SolRPDS with matched controls.
- **Yellowstone gRPC is parked in `stream/future/`.** Complete and verified
  against the real `geyser.proto`, but it costs money and no §3.2 upgrade
  trigger has fired.
