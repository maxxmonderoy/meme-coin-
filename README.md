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

Comments are deliberately kept OUT of this block: zsh does not treat `#` as a
comment in an interactive shell unless `interactive_comments` is set, so a
pasted block with inline comments fails on every one of them.

Everything below assumes you are INSIDE the repository directory. `.venv` lives
there, not in your home directory.

```zsh
git clone https://github.com/maxxmonderoy/meme-coin-.git
cd meme-coin-
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e . --no-deps
cp .env.example trenches.local.conf
export TRENCHES_ENV_FILE=trenches.local.conf
trenches migrate
```

`uv` is faster if you have it — `uv venv .venv` and `uv pip install --python
.venv/bin/python ...` — but it is not required, and the hash-pinned install
(§3.10.10) is `pip install --require-hashes -r requirements.txt` either way.

Without activating the venv, prefix every command with `.venv/bin/` instead:
`.venv/bin/trenches migrate`. The console script is installed there and is not
on your PATH otherwise.

### Collecting

```bash
trenches stream --record captures/   # dual free feeds + paper positions
trenches exits                       # exit loop, its own process (§3.1)
trenches sample                      # dense price paths, its own process
trenches structural --limit 200      # stage-1 structural facts (free, keyless)
```

Run `stream`, `exits` and `sample` in separate terminals. They share only the
database — that is the point, and an entry-side stall must not stop an exit.

### Looking at what you collected

```bash
trenches stats --hours 24            # feeds, watch set, budget utilisation, cohorts
trenches inspect <mint>              # price path and event timeline together
trenches decide                      # run the cascade, journal every verdict
trenches paper                       # the paper journal and §3.9.6 progress
trenches replay-exits                # compare exit rulesets, split by cohort
trenches sample --compact            # downsample expired paths
trenches backup --keep 7             # verified copy; the journal cannot be rebuilt
trenches verify-capture captures/    # confirm the PumpPortal frame schema
```

**Back up the database.** Everything else here is code and code is reversible.
The journal is a record of tokens that launched at a particular moment, and
nobody else stores what happened to them -- an observation not taken today
cannot be taken later, so losing the file costs calendar time rather than
developer time. `trenches backup` is safe to run while the collectors are
writing (it uses `VACUUM INTO`, not a file copy, which is the classic way to get
a backup that restores to a corrupt database), and it verifies row counts and
runs an integrity check on the copy.

**Running `label` and `sample` together splits one 60 req/min budget.** They
hold guaranteed shares rather than contending -- 65% to `sample`, 35% to
`label` -- because a single shared bucket prevents a ban but not starvation: at
a high batch limit the labeler alone takes 48 of 60 req/min. The two collect
different things and the choice is real: `label` covers 100% of launches with
four data points each, `sample` covers ~18% with a point every 30 seconds. A
trailing stop cannot be backtested on four points, so the exit work needs
`sample`.

Start with `trenches sample --once` rather than `trenches sample`: it prints what
it admitted and what it observed, and tells you what is missing if the answer is
nothing. Two things that look broken and are not — only the `control` cohort
fills until `trenches decide` has run, and `not_yet_indexed` dominates early
because DexScreener has not indexed a brand new mint yet.

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

## The week-1 gate, reformulated

The original gate was seven consecutive days of stream coverage. Measured on a
shared machine it scored the wrong thing: **22 of 22 stream stops were
`clean shutdown`** -- a SIGTERM from a logout or a reboot -- and not one was a
crash, stall, or unhandled error. Counting consecutive hours reported 0.6%
while nothing in the software had failed in five days.

So the gate now asks the question the soak was for:

```bash
trenches gate --days 7 -v
```

> Over the window, **zero gaps attributable to the system**. A gap is forgiven
> only when the preceding session recorded a clean shutdown. Anything else --
> including a session that simply vanished with no stop reason -- counts against
> the system, because an unexplained gap is exactly where a crash would hide.
> All health counters at zero.

**This is a weaker claim than the original and the README says so out loud.**
It certifies that nothing we wrote broke. It does not certify that the process
stays up unattended for a week, which is a property of the host, and which
becomes nearly free to demonstrate on a machine with no lid to close.

Passive uptime is replaced by **fault injection** (`tests/test_chaos.py`): the
socket is killed mid-stream, the feed is made silently idle so only the
watchdog can catch it, a live feed sends a clean EOF, four failures fire in a
row to test bounded backoff, and events are replayed to prove dedupe. A soak
says "nothing went wrong"; this says "we made it go wrong and nothing was lost".

## The cascade, stages 0-4 (paper only)

```bash
trenches decide --limit 5000     # journal a verdict for every candidate
```

Nothing in `decide/` can construct or sign a transaction, and there is no
`execute()`. `mode` is a column on `decisions`, never a branch in the code
(3.9.1) -- a decide path that forks on mode is one that was never really tested.

Every stage is a pure `facts -> Verdict`. Nothing fetches, which keeps 3.4's
cost ordering an explicit caller decision rather than a network call hidden
inside a predicate, and makes the whole cascade testable offline.

**The first run accepted 5,000 of 5,000, and that is the correct result to
report rather than hide.** Two reasons, both stated in the command's own output:

- **Stage 1 has nothing to read.** Mint/freeze/close authority, the Token-2022
  upgradable flags, transfer hooks and fees are all knowable within ~1s of a
  launch (Part 2), but nothing fetches them yet. The stage records
  `structural: unfetched` rather than implying a pass, because **absence is not
  a clean bill of health** -- reading empty as clean is named in Part 2 as the
  single most expensive mistake available here.
- **Stage 2 cannot fire.** `rug_rate` needs `n_rugged`, and nothing labels rugs,
  so all 28,766 creators read 0.0 -- including the one with 372 launches. The
  rule is wired and journalled so it starts working the day labelling lands.

A cascade that accepts everything is not a filter. It is a filter with no data,
and the difference is worth keeping visible.

### Stage 3: liquidity, LP lock state, unlock horizon

Four rejections, in the order they cost money: already flagged `rugged`;
liquidity below the floor; LP neither burned nor locked; and 3.4's free check
that almost nobody does -- **the lock expiring inside the holding horizon**. A
lock that ends while you hold is not a lock, it is a countdown, and the deployer
chose when it ends.

```bash
TRENCHES_DECIDE_MIN_LIQUIDITY_USD=5000        # UNCALIBRATED, see below
TRENCHES_DECIDE_HOLD_HORIZON_SECONDS=21600
```

**The floor is an assumption, not a finding, and every journal row says so.**
3.4 gives no liquidity number, and 1.2's $1,000 figure is Solidus's measure of a
token being effectively *dead*, not a floor for entry. `thresholds()` therefore
records `calibrated: false` alongside the number, so a later tuning pass against
matched controls cannot quietly rewrite what earlier rows meant.

**Stage 3 spends no request.** Liquidity comes from the most recent `price_path`
observation the sampler already collected, and `rugged` from the structural
event timeline. Both have verified sources in this repo -- DexScreener's
`liquidity.usd` and RugCheck's top-level `rugged`.

**Half of stage 3 has no supplier, and that is stated rather than faked.** LP
lock state and the locker's `unlockDate` are real fields 3.4 names, but nothing
here has confirmed which field of a RugCheck report carries them, so they
journal as `unknown` -- never as a pass. Part 0 rule 2: a plausible-sounding
field path is worse than a gap.

The distinction the code works hardest to keep is between **unknown** and
**clean**. A candidate nobody fetched records `{"liquidity": "unfetched"}`; a
candidate with liquidity but no lock data records the liquidity and marks the
rest `unknown`. Those are different facts, and a journal that cannot tell them
apart is the one that later justifies a loss.

### Stage 4: concentration, coordination, and the cold start

Five rejections, and every number is quoted from 3.4 rather than chosen here:
`dev.percentage > 5`, `snipers.total > 20`, `insiders.total > 20`,
`bundlers.total > 15`, `bundlers.count >= 100`. The comparators are copied
exactly — bundler *count* is `>=` while the rest are strictly `>` — because the
boundary is where a filter gets argued about. All reasons are collected rather
than short-circuited: they come from one payload, so reading all of them is
free, and a row saying "snipers AND insiders AND bundlers" is worth more later
than one saying "snipers".

**The cold start is enforced here, not noted.** This is the part of stage 4 that
matters most. Every one of those rules is *reject if greater than N*, and Part 2
says every behavioural field starts empty. So an unguarded stage 4 does not
merely fail to reject a thirty-second-old mint — it **actively passes it**, on a
payload of structural zeros, and the journal would record a clean sweep. Below
`TRENCHES_DECIDE_COLD_START_SECONDS` the values are recorded as `cold_start` and
**no rule may fire on them in either direction**: a number too early to believe
cannot condemn a token any more than it can clear one. The default is 600s — the
conservative end of Part 2's 2–10 minute populate window.

`decide` therefore reports its accepts split three ways, because an accept on
real data and an accept on an absence are opposite facts:

```
decided 3  accept=1  reject=2
  of 1 accepts, stage 4 saw NOTHING for 1 and cold-start zeros for 0
  -- those are not clean bills of health, they are absences.
```

**Top-10 share excludes the curve**, per 3.4: "compute top-10 excluding pool,
bonding-curve and locker addresses or you'll reject on the curve itself." On a
live pump.fun curve the curve holds essentially the whole supply, so a naive
top-10 reads ~100% for every token that exists and carries no information. The
curve address comes from the launch and the pool address from the price path;
**locker addresses have no source here**, so the exclusion list actually applied
is journalled rather than assumed complete.

**Two quantities are computed and journalled but do not gate by default**,
because 3.4 gives no number for either:

- **Top-10 share.** 1.5 gives a *delta* — "first 10 buyers hold 17 percentage
  points more supply than low-risk" — never a level, and a delta cannot be
  applied to a single token.
- **The bundler distribution delta**, `totalInitialPercentage - totalPercentage`.
  3.4: "high initial + low current means they already distributed into you. That
  delta is more informative than either level and is free in the payload."
  Informative — with no threshold attached.

Both have opt-in knobs (`max_top10_pct`, `max_bundler_distribution_pct`) that
default to off. Journalling them now is what makes calibrating them possible;
gating on a number nobody measured would be a guess wearing a filter's clothes.

**Stage 4 has no supplier and says so.** Nothing in this repo collects dev share,
sniper, insider or bundler counts, or a holder list, so today stage 4 journals
`unfetched` for every candidate and `decide` prints the gap list:

```
  facts no supplier in this repo provides yet:
    holders            2,153 candidates   no normalised holder list is stored
    snipers_total      2,153 candidates   behavioural, cold start; nothing collects it
    ...
```

That list lives in `decide/facts.py` as data, not as a comment nobody reads —
the difference between a known gap and a silent one.

## Derived rug labels

```bash
trenches rugs --dry-run     # see what it would label
trenches rugs               # fill creators.n_rugged
```

Stage 2 needs `n_rugged` and nothing was writing it. Rather than reach for
SolRPDS -- a fine dataset, but a fixed historical one that cannot label the
tokens launching this week -- these labels come from outcomes we already
collected.

**It measures liquidity COLLAPSE, not proven malice**, and the code says so in
every docstring. A token that had real DEX liquidity and then had effectively
none is what a price-and-liquidity snapshot can show; whether the deployer
pulled it, a whale left, or demand evaporated is not visible. Stage 2 rejects a
wallet on this number, so a false rug is a person filtered out for launching
during a bad week.

Three things it refuses to count, each with a test:

- **A bonding curve going quiet.** ~95% of launches never graduate. Counting
  those would call nearly every creator a rugger.
- **A gentle decline.** Drifting down is not a pull; the collapse fraction is
  what separates "went to zero" from "got quieter".
- **A single observation.** One snapshot cannot show a transition.

**It undercounts, and that is structural.** Median rugged lifespan is ~14
minutes (1.2) and the first observation is at 15m, so the fastest rugs -- the
most common kind -- are invisible here and look identical to a token that never
had a pool. Never read a derived rug rate as the true rate.

First run: **100 collapses across 59 signers** out of 99,993 mints examined, and
**0 creators** clear stage 2's `>=3 mints AND >=60% rug rate`. That is the
honest state of a five-day-old dataset with no 7d horizons due yet, not a
finding about the market.

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
