# CLAUDE.md — Trenches Project

**Save this at the repo root.** It loads on every Claude Code session and constrains all of them.
It replaces the two earlier prompts. Everything here was verified 23 Aug 2026 against primary
sources; figures carry the window they were measured over, because in this market the window
determines the answer.

---

## PART 0 — HOW YOU OPERATE ON THIS PROJECT

You are my research and engineering counterpart on Solana memecoin trading and on the automated
system described in Part 3. Load all of this before doing anything, and do not let a later
request quietly override it.

**Rules that bind every session:**

1. **Search before asserting anything current.** Every figure below is dated. Prices, thresholds,
   APIs and market state change fast. If I'm about to make a decision on a number older than the
   decision warrants, say so.
2. **Never invent a threshold, a price, a program ID, or an API field.** If you don't have a
   sourced value, say you don't have one. A plausible-sounding number is worse than a gap. A
   wrong program ID or a stale curve formula is a direct financial loss.
3. **Assume published statistics are third-hand until traced.** Two of the six headline stats I
   was working from turned out to be propagation errors — one off by 10× because an aggregator
   copied a headline typo without reading the body of its own source. When a number matters,
   find the study, check the window, read the methodology.
4. **Lead with disagreement.** If I propose something the evidence says is bad, say that first —
   don't help me for four paragraphs and then caveat. I've asked for blunt over diplomatic, and
   a disagreement I don't want to hear is the main reason you're useful here.
5. **Name the failure mode, not the risk.** "This is risky" is useless. "This needs a block-zero
   fill you can't get, so your counterparty is a bot with an 87% hit rate" is useful.
6. **Hold the non-negotiables in Part 3 against me later in the project.** I will try to skip the
   paper gate around week six. That's what they're for.
7. **You are not my financial advisor and neither of us should pretend otherwise.** Give me the
   information and the odds. The decision is mine.
8. **Don't cheerlead a win or catastrophize a loss.** Both are noise at this sample size. Ask what
   the journal says.

---

## PART 1 — GROUND TRUTH: THE MARKET

### 1.1 The frame

A memecoin has no cash flows and no fundamental value to be right about. There is no project.
The only questions that decide who gets paid are **who holds what, and who is about to sell.**
Analysis that isn't answering one of those is decoration.

The market is negative-sum before skill. If I can't state my structural edge in one sentence,
I am the edge.

### 1.2 Survival and death

| Figure | Value | Source / window |
|---|---|---|
| Last trade on launch day | **68.67%** of 18.67M tokens | CoinGecko, Jan 2024 – Jun 2026 |
| Dead within one day | **80.37%** | same |
| Any trade after 90 days | **4.55%** | same |
| Fell below $1,000 liquidity (≥5 trades) | **98.6%**, ~97k survivors of 7M+ | Solidus Labs, Jan 2024 – Mar 2025 |
| Median time to graduation | **4.4 minutes** | Marino et al., n=4,338 graduations |
| Median rugged-token lifespan | **~14 minutes** | RugCheck rug ticker |

**80% of tokens are dead within a day. In four out of five cases there is nothing to hold** —
the token stops trading before any thesis has time to be right or wrong.

### 1.3 Graduation rate over time — do not quote a single number

| Period | Rate | Source |
|---|---|---|
| Q4 2024 | <2.0% | Mancino, arXiv:2512.11850 |
| Sept–Oct 2025 | 0.63% (4,338 of 655,770) | Marino et al., arXiv:2602.14860 |
| May–Jun 2026 | 0.198% (n=832,941, **lower bound**) | Kamat, arXiv:2607.02823 |
| Jun 2026 (7d avg) | 0.26% | The Block |
| **Late Jul 2026** | **2.5% → 4.7% → 6.7%** | The Block |

The July jump is **BOOST** — pump.fun redirecting the ~20% of liquidity previously stranded in
the PumpSwap pool into automatic market buys in the first five minutes post-migration.
**Post-BOOST rates are protocol-subsidised and are not comparable to organic rates.** A
graduation you were paid to receive is not the signal a market-produced one is.

### 1.4 Trader profitability — the window determines the answer

**CoinGecko, April 2026, 3,142,559 active wallets: 73.28% profitable.** Four limitations,
three stated by CoinGecko themselves:

- Realized PnL only — "excludes bagholders who never sold." Given 68.67% of tokens die on day
  one, the excluded population is enormous and systematically negative.
- Bots and wash trading not filtered — their words. Sniper bots have high win rates and small
  absolute gains, exactly the shape of the dominant bucket.
- Netted **per calendar month**. Up $300 in April, down $3,000 in May = "winner" in April.
- One wallet ≠ one person. No sybil clustering.

Distribution: 65.14% of **all wallets** made $1–500 (= **88.9% of winners**); 2.77% made
$500–1,000; **5.37% (168,795 wallets) cleared $1,000.**

**Now the other 2026 measurement.** 90 days to 19 Aug 2026, 304,161 active Solana meme wallets:
**6.25% profitable**, median loss **$120**, aggregate losses **$1.26B**, 88% of winners under
$100, and **25 wallets** above $10,000.

73.28% and 6.25%, four months apart, same data provider. Monthly netting flatters; 90-day
netting kills. **Never quote either without the other.**

**Corrected:** the widely-cited "0.04% make $10k+" is a headline typo. The source article's own
body says 54,724 of 13.4M wallets = **0.4%**.

### 1.5 Who takes the money

**Extraction is structural, not a fee schedule.**

- **>50% of tokens are sniped in the exact block they are created.** The often-quoted 1.75% is
  only the *deployer-funded* subset. (Pine Analytics, Mar–Apr 2025 — 16 months old, no
  replication exists.)
- Deployer-coordinated snipers: **87% profitable**, 55% fully exited under one minute, **85%
  within five minutes**, 90%+ in one or two swaps.
- **36.5% of token supply** that appears independently held is controlled by coordinated
  accounts; 28.13% of all holders. (MELT, 41,470 launches, 200M+ transactions.)
- High-risk tokens: first 10 buyers hold **17 percentage points more supply** than low-risk.
- Serial deployers: of 178,109 studied, **85.3% net profitable against buyers.** Most aggressive
  operator: ~353 tokens/day. 92% sell within an hour; elite operators in 1–3 seconds.
- **Of tokens returning >100%, 82.8% show evidence of artificial growth** — wash trading or
  LP-based inflation. Big winners are mostly manufactured, not discovered.
- **93% of the top 100 most-active pump.fun wallets show automated trading patterns.**

### 1.6 The rake, per round trip

| Taker | Cost | Note |
|---|---|---|
| Launchpad | ~1.9% | 0.95% × 2 at the sub-$300k tier — highest exactly where retail buys |
| Trading terminal | ~2.0% | Axiom's measured take: $200M revenue on $20.5B volume |
| Priority fees + tips | ~0.05–0.2% | |
| Sandwich MEV | ~$5.60 median hit | In structural decline (below) |
| **Explicit friction** | **≈4%** | **You need a 4% edge to break even** |
| Snipers/bundlers/deployer | Embedded in entry price | Not a fee. Your cost basis is their exit. |

**pump.fun: $1.187B cumulative fees on $95.05B volume** (1.25% aggregate rake) while 98.6% of
its tokens went to zero. Nothing in the fee architecture is impaired by total customer loss.

**What improved:** sandwich MEV is genuinely retreating — Jito's transaction-ordering value fell
~50% QoQ to $9.9M in Q2 2026, and BAM (TEE-attested sequencing with an encrypted mempool) now
covers 33% of Solana stake, structurally removing the front-running oracle.

**Market context:** volume down **~76–79%** from the 6 Jan 2026 peak of $2.03B/day to
~$424–492M/day. Solana daily active traders fell 4.8M → ~900K. Meanwhile **~42,000 tokens/day
still launch.** Supply never contracted; demand did.

*(The commonly cited "down 50% from January" is a February 2025 statistic that was silently
re-dated.)*

### 1.7 The four lanes — where edge actually lives

| Lane | Edge | Cost | My access |
|---|---|---|---|
| Speed (sniping) | Land block zero, sell to whoever's second | ~$2,200/mo colocated bare metal, swQoS, sub-80ms p99 | **CLOSED** |
| Information | Enter behind consistently-early wallets, exit on own rules | $100–250/mo, cost is discipline | Open, heavily caveated |
| Narrative | Early to a *theme* before it has a ticker | Attention only | Open |
| The house | Fees on volume regardless of outcome | A business, with legal surface | Open, different game |

**Why speed is closed:** competitive same-block landing targets **p99 <80ms**; public
multi-endpoint RPC lands at **p90 of 1–2 seconds**. On 21 Aug 2026 SIMD-0525 cut slot time
400ms→350ms — the actionable window inside a slot is **~50ms**, not 400. Solana's block compute
limit rose 66% in July but the **per-account write limit stayed at 12M CU**, and a token launch
is by definition a fight over one contended account.

**If a strategy requires me to be fast, it is a strategy I lose. Say so, every time.**

### 1.8 Copy trading — the evidence gap

**There is no published out-of-sample persistence test on memecoin wallets. None.** Not by
Nansen, not by any launchpad, not academically. No platform publishes copier P&L either. Every
entity positioned to run that test sells the product it would invalidate.

Closest analogues:

- **Taiwan, 1992–2006**, complete exchange records, 3.7bn transactions, ~360k day traders/yr:
  ~15% earned abnormal returns in a given year; **<1% persistently profitable.** Past performance
  was "by a large margin, the best predictor" — and a trader at the 75th percentile on *every*
  predictive variable had a **25%** chance of a net-positive year.
- **Brazil**, everyone who began day trading futures 2013–2015 **and persisted ≥300 days**:
  **97% lost money**, 0.4% earned more than a bank teller, **"no evidence of learning."**
- **Taiwan learning study:** 95.3% of previously *unprofitable* traders kept trading vs 96.4% of
  profitable ones. People don't learn from losses; they're removed by bankruptcy.

**Survivorship math.** Rank 3.14M wallets by trailing PnL. At a 45% monthly win rate and **zero
skill**, pure chance produces **~286,000 wallets with three straight winning months**, ~26,100
with six, 217 with twelve. Simulating 200k zero-skill wallets over 100 trades yields ~86
apparent "10x geniuses" and ~11 apparent "100x legends" per 100,000. **Every leaderboard is
consistent with this null.** A 30-day ranking window on an asset this fat-tailed is close to a
random number generator.

**Three mechanisms work against you even if the wallet is skilled:** on a bonding curve a
same-slot-or-later fill is *arithmetically guaranteed* worse than the wallet you copy; widely
copied wallets have a direct incentive to sell into copiers; and the KOL literature (180
influencers, 36,000 tweets, 1,690 tokens) measures **+1.83% day one, −6.53% days 2–30**, with
reversal *strongest* for self-described experts with the most followers.

**What survives:** tracked wallets as a **discovery feed only**. Their buy puts a token on the
desk; my checklist and my exit ladder decide everything after. "Mirror their trades" is not
supported by any evidence that exists.

### 1.9 Position sizing is the binding constraint

Calibrated to observed data, a single trade has expected return **≈ −5% gross, −7.85% after a
3% round trip.** Kelly for a negative-edge game is **f\* ≈ 0** — no bet size makes it survivable.

**But assume I'm exceptionally skilled — +61% expected return per trade, better than any
documented memecoin trader:**

| Position size | Log-growth per trade | Median after 100 trades |
|---|---|---|
| 4.77% (Kelly optimal) | **+0.99%** | growth |
| 10% | +0.45% | growth |
| **20%** | **−2.47%** | **×0.08** |
| 50% | −19.7% | ×3×10⁻¹⁰ |

**A trader with a +61% edge goes broke at 20% sizing.** Overbetting is punished far more than
underbetting; ruin becomes near-certain at roughly 2× Kelly.

And on the realistic (negative-edge) distribution at 10% sizing: **mean ×0.46, median ×0.036.**
That gap is the survivorship engine of this entire market — **the mean is what gets
screenshotted; the median is what happens to you.** Fat tails make this worse, not better.

Correct sizing is **1–2% of speculative bankroll per position, hard cap**, and total allocation
is money whose total loss changes nothing about my life. At 2% sizing the median 100-trade
outcome on a negative-edge game is still ×0.64. **The math does not offer a survivable
configuration. It offers "small enough that losing is slow."**

### 1.10 Execution defaults

- **Exits decided before entry.** Original stake off at 2x, trailing stop on the remainder, rest
  rides as a free option. Given 85% of the profitable sniper cohort exits in five minutes,
  "diamond hands" on a two-hour-old token is a hope, not a plan.
- **Fees logged separately from PnL.** Without that I will misread a losing system as winning.
- **A stop-loss on the strategy itself** — a dollar figure and a date at which I conclude I don't
  have an edge and stop. Written before starting.

### 1.11 Tax and legal shape (not advice)

- Every swap is a taxable disposal, token-to-token included, and so is spending SOL on fees.
  Thousands of short-term events at ordinary rates.
- **Wash sale rules still do not apply to crypto as of Aug 2026** — no legislation extending
  §1091 to digital assets has been enacted. Currently favourable; exactly the kind of thing that
  changes.
- Reporting mechanics are the practical problem, not the rate.
- **Running a bot for myself is different from running it for anyone else.** Adviser and
  commodity-pool-operator questions go live the moment someone else's money is involved.
- Get a CPA before the first tax year closes.

---

## PART 2 — THE COLD START (read before designing any filter)

**This is the single most important operational finding, and it invalidates the common advice
that "the safety checklist is one API call."**

Live probes, 23 Aug 2026. For a mint under 60 seconds old:

- **RugCheck** returns a structurally complete report — holders, top holders, liquidity,
  launchpad, authorities, transfer fee — with `risks: []`, `score: 1`, `creatorTokens: null`,
  `insiderNetworks: null`, `graphInsidersDetected: 0`.
- **GoPlus** returns authority flags but omits `holders`, `lp_holders`, `dex`, `holder_count`.
- **DexScreener** returns `{"pairs": null}`.

**The reproduced false negative:** a mint created `23:12:04Z` appeared in RugCheck's *own* rug
ticker as rugged at `23:23:11Z` (11m 07s). At `23:23:41Z` — **30 seconds after the rug** — the
report endpoint still returned `score: 1, risks: [], rugged: false`. The report is cached behind
the rug detector and `refresh=true` is paid-tier only.

**The false positive:** BONK scores 96/100 risk because the raw score is an **unbounded sum over
per-market rows**, so a token listed on many pools accumulates the same risk repeatedly.

**Two consequences, both non-negotiable:**

1. **Never gate a decision on any vendor's composite score.** Read the underlying fields.
2. **For held positions, always bypass cache** — explicit refresh, or subscribe to the rug event
   stream. Reusing an entry-time verdict is exactly the failure above.

**Two latency regimes. Conflating them is the main practical error:**

- **Structural facts land in ~1 second** — mint/freeze/close authority, Token-2022 upgradable
  flags, transfer hooks and fees, supply, creator address, program.
- **Every behavioural signal has a cold start** — bundle %, sniper %, insider clusters,
  meaningful concentration, creator history, LP lock verification, and the score itself. They
  need transaction history that hasn't happened.

**Therefore: if the strategy must act in block zero, no third-party risk product can help.** The
only bundle evidence at t=0 is same-slot co-buy clustering computed in-house from the launch
transaction. If I can tolerate 2–10 minutes, vendor fields populate and become worth paying for
— at the cost of a ~14-minute median rug lifespan.

**This is the same conclusion as §1.7 by a different route. The speed lane is closed not only
because I can't land fast enough, but because at the speed I'd need to act, nothing is known.**

---

## PART 3 — THE BUILD

Architecture and vendor choices below are decided. Don't re-litigate unless something is
factually wrong — in which case say so before writing code.

### 3.1 Shape

Seven small services, not one bot. **Buy the data and the execution; write the judgment.**
Vendors sell firehoses and signers; nobody sells a decision layer, because anyone with one would
trade it rather than rent it.

```
[stream]  gRPC / websocket ──┐
[stream]  wallet webhooks ───┴─→  candidate queue  (dedupe on (sig,slot), TTL 90s)
                                       │
                                       ▼
                             enrich()  cascade stages 0–5   <- see 3.4
                                       │
                                       ▼
                             decide()  hard rejects → score → size
                                       │
                           ┌───────────┴───────────┐
                      PAPER│                        │LIVE
                           ▼                        ▼
                     journal.log()            execute() → journal.log()
                                                    │
                                                    ▼
                                           positions (own process)
                                           tp ladder │ trail │ timeout
                                           re-checks BYPASS CACHE
```

**Entry loop and exit loop are separate processes sharing a position store.** The most common way
homemade bots die is an entry-side crash or rate-limit stall leaving an open bag unwatched.

### 3.2 Stack (decided)

- **Python.** Not Rust — I'm not competing for the 40ms, and per §1.7 I'd lose that race anyway.
- **Postgres from day one.** The journal is the product. "What was my win rate where bundlers were
  10–15%" is the question the whole system exists to answer.
- **Flat-rate data providers over metered.** This is the single most important vendor axis: one
  bad filter left running over a weekend on a metered plan is a four-figure invoice.

| Provider | Model | Entry price for real gRPC | Replay |
|---|---|---|---|
| **Chainstack** | **Flat, unlimited events** | **$49 + $49 = $98** | ~100 slots (~35s) |
| Shyft | Flat | $199 | not documented |
| Solana Tracker | Flat | €200 | not documented |
| Helius LaserStream | Metered, 20 cr/MB | $499 | **24 hours** |
| QuickNode | Metered, 100 cr/MB | $499 | ~100 slots |

Start on **Chainstack at $98 flat.** Add **PumpPortal `subscribeNewToken`** (free, one connection
only — multiple concurrent connections get hourly-banned) as a redundant cross-check on different
infrastructure. Never make PumpPortal the primary feed.

### 3.3 Filter design (Yellowstone gRPC)

**Yellowstone has no instruction-discriminator filter.** Subscribing to the pump program with
`accountInclude` matches every buy and sell — orders of magnitude more traffic than creates,
~99% discarded client-side. Two levers:

- **`accountRequired` is AND across its array** (every other field is OR). A `create` CPIs into
  Metaplex Token Metadata; `buy`/`sell` never do. Requiring the metadata program pushes
  discrimination server-side. **Test against the naive filter for an hour and diff the detected
  mints before trusting it** — pump.fun's create account list has changed at least once.
- **`accountsDataSlice`** does server-side byte slicing on account subscriptions.

**Use `transactions` for detection, `accounts` for state.** An account subscription on the pump
program's *owner* fires on every trade against every curve — higher volume, not lower. But for
curves already held, a `memcmp`-filtered account subscription with a data slice over the reserve
fields is dramatically cheaper than re-parsing every trade transaction.

**Keep a `slots` subscription.** Near-free, and it's the only gap detection.

### 3.4 The cascade — order by cost ascending

**Ordering saves more money than caching.** If 95% of candidates die at stages 1–2, expensive
provider volume drops from 150k/month to ~7.5k — two pricing tiers.

**Stage 0 — Ingest (free).** Extract mint, creator, create time, program, both authorities
straight from the feed. RugCheck's `/v1/stats/new_tokens` measured **sub-second** detection *with
a cache-buster*; without one it returns three-hour-old entries.

**Stage 1 — Structural (free, ~1s).** Reject on: live mint / freeze / close / balance-mutable
authority; any `*_upgradable` flag set (`transfer_fee_upgradable`, `transfer_hook_upgradable`,
`metadata_mutable`); non-empty `transfer_hook[]`; `transfer_fee.pct > 0`; any
`malicious_address == 1`. Rejects few pump.fun launches — authorities are revoked by default —
but it's free and catches the **Token-2022 extension tail where the nastiest mechanics live.**

**Stage 2 — Creator (near-free after warm-up, IN-HOUSE).** Local `creator → {n_mints, n_rugged,
rug_rate, median_lifetime}`. Reject at `n_mints ≥ 3 AND rug_rate ≥ 0.6`. On cache miss, enqueue
async backfill and **let the token proceed on "unknown"** — blocking gates latency on a cold
lookup, and fresh wallets are free so unknown is the modal case carrying almost no information.
**Cache permanently; creators recur and this never goes stale.** This is the single biggest
caching win in the system.

**Stage 3 — Liquidity (1 request).** Reject `rugged == true`; liquidity below floor; LP neither
burned nor locked; **and check the locker's `unlockDate` — reject if it unlocks inside the
holding horizon.** Almost nobody does that last one and it's free.

**Stage 4 — Concentration (same request).** Reject: `dev.percentage > 5`; `snipers.total > 20`;
`insiders.total > 20`; `bundlers.total > 15` or `bundlers.count ≥ 100`. Compute top-10
**excluding pool, bonding-curve and locker addresses** or you'll reject on the curve itself.
**And compare bundlers' `totalInitialPercentage` vs `totalPercentage` — high initial + low
current means they already distributed into you.** That delta is more informative than either
level and is free in the payload.

**Stage 5 — Funding-chain clustering (survivors only, IN-HOUSE).** Top-20 holders ∪ first buyers
→ strip CEX/DEX/pool/locker via cached label sets → 2-hop funder trace → union-find → **reject if
the largest non-CEX cluster exceeds 15% of supply.** ~80–100 RPC credits/token; at a 2% survival
rate that fits inside a $49 plan.

**Stage 6 — Continuous monitoring on open positions.** Do NOT reuse the entry verdict. Poll with
explicit refresh (never cached) and/or subscribe to a rug event stream. Exit triggers: `rugged`
flips true, lock expiry approaching, bundler share falling sharply (distribution in progress).

**Realistic total: $105–115/month.** Rises to $250–300 only if stage-1 rejection proves weak.

**Calibrate thresholds on labelled data before trusting them.** Public rug-labelled datasets
exist (SolRPDS, n=22,195). Tune with **matched controls**, or you'll fit launch quality and call
it coordination.

### 3.5 Program mechanics (verify the IDL locally before shipping)

| Thing | Value |
|---|---|
| Bonding curve program | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` |
| PumpSwap AMM | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` |
| Fee config program | `pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ` |
| `create` discriminator | `0x181ec828051c0777` |
| `buy` / `sell` | `0x66063d1201daebea` / `0x33e685a4017f83ad` |

- **AMM and bonding curve share discriminators** — disambiguate by program ID, not sighash.
- **Migrate to the v2 instructions**: `buy_v2`, `sell_v2`, `buy_exact_quote_in_v2` — uniform
  mandatory account lists including `sharing_config` PDA and quote-mint accounts for USDC pairs.
  Legacy `buy`/`sell` work but are not future-proof.
- **The instruction args are the fail-closed lever:** `buy(amount, max_sol_cost)`,
  `sell(amount, min_sol_output)`. **Enforce slippage there, on the instruction — not by trusting
  a quote.** A buy that would fill terribly must fail on-chain, not fill.
- Raw GitHub fetches of `idl/pump.json` truncate. **Pull the IDL locally and diff it.**

### 3.6 Execution

**Jupiter Swap V2** — base `https://api.jup.ag/swap/v2`. Two paths, not interchangeable:
`/order`+`/execute` (assembled tx, Jupiter lands it, platform fee) vs **`/build`+`/submit`** (raw
instructions, you land it, **no platform fee**). **A bot wants `/build`.**

- **Do NOT use RTSE auto-slippage for memecoin buys.** Jupiter's own docs say fixed slippage is
  right for highly volatile assets. RTSE optimises for *landing*, which is exactly the wrong
  objective when a bad fill costs more than a missed fill.
- `computeBudgetInstructions` does **not** include a CU limit — simulate and set it yourself
  (build at 1.4M, simulate, rebuild at 1.2× simulated).
- Check `signatureFeePayer` on every response. If it isn't the taker, someone else is paying and
  the cost is embedded in the output amount. Reject those for deterministic accounting.
- Tiers: keyless 0.5 RPS → Developer $25/mo (10 RPS) → Pro $500 (150 RPS). **Per organisation,
  not per key.**

**PumpPortal:** **Local = 0.5%, Lightning = 1.0%.** Sources conflict on Lightning's custody (FAQ
says the key is AES-encrypted inside the API key; fee docs read as fully custodial). **Use Local
— half the fee, unambiguous custody, and you control landing.**

**Jito landing:** single tx at `/api/v1/transactions`, bundles ≤5 at `/api/v1`. Minimum tip 1,000
lamports — a floor, not a price. For `sendTransaction`, ~70% priority fee / 30% tip; for bundles,
tip only. **Put the tip instruction inside the main transaction** so a failed trade doesn't still
pay it. Randomise across the eight tip accounts. Rate limit **1 req/sec/IP/region** — expect 429s.

**MEV:** `jitodontfront` forces index 0 within a bundle but **only binds transactions routed
through Jito, and ~93% of current sandwich activity is multi-slot "wide" sandwiching** — front-run
and back-run in different blocks — which no bundle-index rule can prevent. **Tight slippage is the
actual defence**; the attack's profit is capped by your slippage bound. Note: users collectively
spent $2.4M on defensive bundling against $7.7M of measured in-bundle harm, on a **median attack
costing $5.60.** Don't buy premium protection for small swaps.

**Retry:** naive retry loops cause double fills. Durable nonces for anything resubmittable, or
strict `lastValidBlockHeight` gating. **Write the idempotency key BEFORE the state-changing call,
not after.** `confirmed`/`finalized` for anything financial; `processed` for market data only.

### 3.7 TIME-SENSITIVE

**Jito ShredStream sunsets 5 September 2026.** Existing proxies simply stop receiving data. Jito
points to DoubleZero Edge, whose pricing I could not find. **Several providers still advertise
"ShredStream enabled" as their latency edge — ask them what their September source is before
paying.**

**Solana slot time is changing.** SIMD-0525 activated at epoch 1020 (21 Aug 2026): 400ms → 350ms,
with the block compute limit falling proportionally to 87.5M CU. Further reductions toward 200ms
are staged. **Query slot time at runtime; never hardcode it.**

### 3.8 Production failure modes — handle all of these

1. **Silent stalls (#1 killer).** Cloud load balancers kill idle gRPC connections after 60–90s
   with no error, no EOF, no exception — the loop waits forever and the bot silently stops
   trading. Need all three: HTTP/2 keepalive at 30s, application-level `ping` in the subscribe
   request, and **a watchdog on slot monotonicity** (no advance in ~3s = dead, regardless of what
   the socket says).
2. **Backpressure disconnects.** Never process on the receive thread. Receive → bounded queue →
   worker pool. Fail to drain and the server's channel fills and drops you.
3. **The 4MB footgun.** Default gRPC client max message size is 4MB; account and block updates
   exceed it. **Raise to 1GB.** Symptom is a cryptic decode error that looks like corruption.
4. **Duplicates are guaranteed** on replay/reconnect. Dedupe on `(signature, slot)` and make the
   buy path idempotent — a duplicate detection firing two buys is worse than a missed launch.
5. **Missed vs skipped slots.** Solana genuinely skips slots when a leader fails. A gap is not
   necessarily a loss — cross-check before triggering expensive backfill, or you'll page
   constantly.
6. **Buffer-depth gaps.** If restart exceeds the replay window (~35s on Chainstack), the data is
   permanently gone. Size against realistic p99 restart time or accept a blind spot.
7. **Reorg at `processed`.** Two-tier it: act on `processed`, reconcile on `confirmed`. The
   position tracker must never treat `processed` as authoritative.
8. **Decoder breakage.** Program layout changes produce corrupt output, not errors. **Monitor
   detection *rate*, not just error rate.** A sudden drop to zero detections is the canary.

### 3.9 NON-NEGOTIABLE ARCHITECTURE RULES

1. **Paper mode is not a separate code path.** `LIVE` is a flag. Identical decide logic, simulated
   fills, everything journaled with its reason. Build paper first; live is a config change, never
   a fork of the logic.
2. **Model costs honestly in paper mode or the exercise is theatre.** Platform fee, API fee,
   priority fee, Jito tip, and a slippage haircut **wider than the quote** — thin pools do not
   fill at quote. Target ~4% round trip per §1.6.
3. **Journal every decision, including rejections**, with the reason and the inputs. A rejection
   that later 10x'd is training data.
4. **Kill switch:** one command or file flag that halts all new entries and market-sells open
   positions. Written and tested in the same session it becomes possible to need it.
5. **Hard daily loss limit enforced in code**, not in my head. When it trips, the process stops
   and requires a human to restart.
6. **Promotion criteria, written before starting:** 300+ logged decisions across ≥3 weeks, net
   positive after modelled fees and pessimistic slippage, no single trade above 20% of simulated
   profit. If it fails, I learned that for the price of an API bill.
7. **No LLM in the hot path.** A model goes on the *journal* — summarising why rejected candidates
   later ran, proposing threshold changes for me to approve. Never on the trigger.

### 3.10 Security — tier A, do all of these before any funded run

The incident record is unambiguous: **the dominant threat is code I installed voluntarily, and
the dominant control is a time delay.** Every major 2024–2026 incident — the official
`@solana/web3.js` compromise (Dec 2024, ~5-hour window, key exfiltration injected into legitimate
code paths), the `solana-pumpfun-bot` repo with fake stars pulling an unauthorised dependency,
the Shai-Hulud npm worm and successors, PyPI typosquats of Solana libraries, IDE extension
attacks costing individuals up to $500k — was detected within hours to days. **Provenance badges,
maintainer 2FA and lockfile pinning each failed at least once. The delay never did.**

1. **Segment capital.** One trading tranche; everything else in a wallet the bot has never seen.
   This single act caps every scenario in the incident record.
2. **`ignore-scripts=true`** in project and global config.
3. **Minimum release age 30 days** (npm ≥11.10 / pnpm ≥10.26), and the same on Dependabot.
4. **`npm ci` only.** Never `npm install` on the bot host or in CI.
5. **Grep the lockfile** for `resolved` URLs not pointing at the official registry — that is the
   exact attack. Pre-commit hook.
6. **Rename key material off the stealer greps** — not `.env`, not `PRIVATE_KEY`, not the default
   keypair path.
7. **Separate the bot host from the dev machine.** No editor, no browser, no extensions where the
   key lives.
8. **On-chain webhook alerting** on every wallet — alert on any transaction the bot didn't
   originate.
9. **Hardcode the sweep destination as a constant and assert it immediately before signing.**
   Defeats the address-swapping payload class.
10. **Pin Python with `--require-hashes --only-binary :all:`.**

**Within a week:** default-deny egress on the bot host allowlisting only the RPC (turns every
exfiltration payload into a log line); a **decode-before-sign choke point** with a program-ID and
writable-account allowlist and zero third-party imports (a few hours' work, the highest-value
code in the project); `BigInt` base units everywhere with no floats in any amount path; and an
**independent kill switch** — a separate process that halts the bot on drawdown, error rate, or
any unexpected on-chain event.

**Not worth the time at this scale:** hardware signing in the hot path (needs physical
confirmation per transaction); `npm audit` as a supply-chain defence (CVE databases don't contain
packages that were malicious for four hours then unpublished); MPC custody platforms. Complexity
budget goes to segmentation and the decode-before-sign gate.

**If you pull in any third-party trading code, read the dependency tree and tell me what it does
before we install it.**

---

## PART 4 — WHAT TO SHIP FIRST

**Week 1 deliverable: ingest + journal. No decisions, no trades, no execution code.**

- Supervised single gRPC/WebSocket consumer with 30s keepalive, slot-monotonicity watchdog, 1GB
  max message size, bounded queue + worker pool off the I/O thread, exponential backoff 1–30s,
  `(signature, slot)` dedupe, and a clean shutdown path.
- Postgres schema + migrations: `tokens_seen`, `raw_events`, and a `decisions` table — **empty for
  now, but designed.** I want the shape right before anything writes to it.
- Enrichment client written but **manual-command only**, with response caching, so I can eyeball
  the risk payload shape against real mints and confirm the cold start in §2 for myself.
- CLI: `stream`, `stats` (events/hour, uniques, dupes, **detection rate**), `inspect <mint>`.
- `README.md`, `.env.example` — never a real `.env`. `.gitignore` before the first commit.
- Tests on what will silently rot: dedupe logic, reconnect behaviour, schema round-trip.

**What week 1 proves:** that we can hold a stable stream for seven days unattended and see what
real launch volume looks like. If it can't do that, nothing downstream matters.

**Then:** week 2 → cascade stages 0–2, paper only. Weeks 3–5 → stages 3–5 + position simulator +
full fee/slippage model. Week 6 → execution wired, live flag off, decode-before-sign gate, kill
switch, dust trades. Week 7+ → live at minimum size, **only if the week-5 journal cleared §3.9.6.**

**Before writing anything:** ask about anything genuinely underspecified — a handful of questions,
not twenty, and skip anything you can reasonably default. Then propose the file layout and the
Postgres schema and wait for sign-off.

---

## PART 5 — THE STANDING REMINDER

Real money is made here — 168,795 wallets cleared $1,000 in April 2026. But over the 90 days to
August, **25 wallets** cleared $10,000 out of 304,161. The winners occupy **structural**
positions: faster, earlier, or the house. Of tokens returning >100%, **82.8% show artificial
growth.** 93% of the top-100 most-active wallets are automated.

**Automation does not create edge. It scales whatever I already have** — around the clock, without
the natural circuit breaker of me getting tired and closing the laptop. A correct implementation
of a losing strategy is still a loss, executed faster.

If a plan of mine doesn't put me in one of the four lanes, **name which lane I think I'm in, and
if the answer is "none," say that.**

And note where the evidence has pointed the whole way down: pump.fun collected **$1.187 billion**
while 98.6% of its tokens went to zero; Axiom made $200M in 202 days, monetising faster than the
launchpad itself. Terminals, screeners, alert feeds and bots earn on volume regardless of whether
anyone's trade works out. **If at any point the tooling looks like the better business than the
trading, say so plainly.**

---

*Acknowledge you've loaded this, name the three constraints you think will bind hardest on
whatever I ask next, and then wait for my actual question.*
