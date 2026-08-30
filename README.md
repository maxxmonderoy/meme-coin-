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
uv pip install --python .venv/bin/python -e . --no-deps   # provides the `trenches` command

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

## Outcome labeling

For every mint we record, snapshot what actually happened to it at **15m, 1h,
24h and 7d**. This is the one part of the system that is time-irreversible:
code can be added in week 9 with no penalty, but nobody stores what happened to
the tokens launching right now, so an outcome not collected today cannot be
collected later.

```bash
trenches label --once          # one pass
trenches label                 # supervised loop
trenches stream --label        # alongside ingest, isolated from it
trenches rules-report          # what each rejection rule would have cost
```

**`no_pool` is a label, not a collection failure.** Most launches never become
tradeable, and recording that is the point: an unlabeled journal cannot tell a
filter that killed a corpse from one that killed a winner.

**A bonding curve is not a graduated pool.** The first live run marked 30% of
15-minute observations `alive` against a documented graduation rate under 1%.
The cause: RugCheck's `markets` array is dominated by `marketType: pump_fun` --
275 of 305 markets in the first sample -- which is the launch curve every
pump.fun token has from birth, not a pool it graduated into. Counting those as
pools made `alive` mean "this token exists". `venue_kind` now separates
`bonding_curve` from `dex`, and with that split the measured graduation rate came
out at **6.67%**, against §1.3's post-BOOST 6.7%. A curve is still genuinely
tradeable, so it is not called dead -- the two are simply different questions.

This is also why DexScreener answered for only 24 of 450 observations while
RugCheck answered for 74: DexScreener indexes *graduated* pools. Its silence was
never "no pool", it was "not graduated" -- which is a complete label on its own,
and why the RugCheck fallback now runs only at 15m and 1h where "not indexed
yet" is still plausible.

**`max_price_usd_seen` is a lower bound.** We sample at four horizons; we do not
stream prices. It answers "did this reach at least X", never "how high did it
get".

**Late is fine, missing is not.** A 24h label taken at 26h is usable data and
records both `scheduled_for` and `observed_at`. A 15m label backfilled three
days late is a "what does it look like now" reading wearing a 15m name, so
`rules-report` excludes rows observed later than their own horizon unless you
pass `--include-stale`.

**The report is the reason the rest exists.** It asks which rejection rules earn
their keep. A rejection whose outcome cannot be determined is counted as `?`,
never as a success -- folding missing data into "correctly killed" is how a
filter proves itself against an empty journal. `unknown_launchpad` is shipped as
a deliberate control: a rule that scores no better than it is a coin flip.

Rate limiting is a token bucket at 60 req/min shared across DexScreener
endpoints, because the case that gets an IP banned is the backlog burst after an
outage, not the steady state. Batch size is 25, set below the 30 whose
correctness was verified by a union test on 2026-08-30.

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

## Endpoint paths — all verified 2026-08-26

Every host was unreachable from the environment this was built in, so these
started as guesses. All of them have now answered a live request:

| Where | Path | Status |
|---|---|---|
| `stream/pumpportal.py` | `wss://pumpportal.fun/api/data` + `subscribeNewToken` | **verified** — 67 `create` frames received |
| `stream/pumpportal.py` | response field names | **corrected** — see below; two names were wrong |
| `stream/rugcheck_feed.py` | `/v1/stats/new_tokens` | **verified** — 200, newest entry 3.4s old |
| `stream/rugcheck_feed.py` | entry shape and timestamp key | **verified** — freshness check resolves it |
| `enrich/rugcheck.py` | `/v1/tokens/{mint}/report` | **verified** — 200 in 779ms |
| `enrich/goplus.py` | `https://api.gopluslabs.io/api/v1/solana/token_security` | **verified** — 200 in 1234ms, written from recall and correct |

**TLS had to be fixed first.** `websockets` uses `ssl.create_default_context()`,
which on a python.org macOS build loads **zero** CAs until someone runs
`Install Certificates.command` by hand. PumpPortal failed every connect with
`CERTIFICATE_VERIFY_FAILED` while the supervisor logged healthy exponential
backoff and reconnected forever — a §3.8.1 silent stall arriving through TLS.
RugCheck was unaffected because httpx already ships certifi. The PumpPortal
consumer now pins the same trust store explicitly, so both feeds agree and the
result no longer depends on how the host's Python was installed.

## The schema was wrong in three ways

`verify-capture` over 67 live frames:

- **`creator` does not exist.** The signer arrives as `traderPublicKey`. pump.fun's
  `create` takes `creator` as an argument distinct from the signer, so §3.4
  stage 2 can only key on the signer from this feed — the declared creator needs
  the on-chain `CreateEvent`. `declared_creator` is now **null** rather than a
  copy of the signer: duplicating it made a two-key cache that keyed one value
  twice, which is precisely the rotation bypass keying on both was meant to stop.
- **`timestamp` does not exist.** No frame carries an on-chain time, so
  `block_time` is always null here and feed latency is measurable only as
  relative arrival order — never as true launch-to-detection.
- **`is_mayhem_mode` was undeclared**, present in 65 of 67.

**And the finding that matters most: `subscribeNewToken` is not a pump.fun feed.**
The capture carried `pool=pump` (65) and `pool=bonk` (2), and **they do not agree
on fields** — pump carries `bondingCurveKey`/`vTokensInBondingCurve`/
`vSolInBondingCurve`, bonk carries `tokensInPool`/`newTokenBalance` instead.
`token_fields` hardcoded `launchpad = "pump.fun"` for every frame, which
mislabels every non-pump launch, misattributes its deployer in the stage-2
reputation table, and invites §3.5's pump.fun curve maths onto a token that is
not on a pump.fun curve. Pools other than `pump` are now stored as
`unverified:<pool>` rather than given a guessed product name.

The schema is **still `verified=False`** on purpose: 75 seconds is not seven
days and cannot show a rarer variant. The soak's capture is what should promote
it.

Note the install above must be **editable** (`-e .`). Migrations are read from
the repo's `migrations/` directory relative to the source tree, so a regular
install would not find them.

## Not done yet

- **The seven-day soak has not run.** That is the actual week-1 deliverable.
- **PumpPortal's schema is corrected but not promoted** — it matches 67/67 live
  frames; flip `NEW_TOKEN_SCHEMA.verified` once the soak's capture agrees.
- **`n_rugged` is never written.** `creators` counts mints as an ingest
  byproduct, but nothing labels rugs yet, so §3.4 stage 2's
  `n_mints >= 3 AND rug_rate >= 0.6` cannot fire. That is week-2 work and it
  needs SolRPDS with matched controls.
- **Yellowstone gRPC is parked in `stream/future/`.** Complete and verified
  against the real `geyser.proto`, but it costs money and no §3.2 upgrade
  trigger has fired.
