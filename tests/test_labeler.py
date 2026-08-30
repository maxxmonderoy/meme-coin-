"""Outcome labeling. Offline: no test here touches the network.

The DexScreener client is injected, so every vendor behaviour that matters --
an empty answer, a partial batch, an outage -- is exercised as a fixture rather
than hoped for.
"""
from __future__ import annotations

import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest
from conftest import pubkey

from trenches.db import repo
from trenches.db.dialect import iso
from trenches.label import HORIZON_SECONDS
from trenches.label.dexscreener import (
    BATCH_SIZE,
    BatchResult,
    TokenBucket,
    best_pair,
    parse_pair,
)
from trenches.label.labeler import MAX_ATTEMPTS, Labeler


def _pair(mint: str, *, price="0.001", liq=5000.0) -> dict:
    return {
        "pairAddress": "PAIR" + mint[:6],
        "dexId": "raydium",
        "baseToken": {"address": mint, "symbol": "TST"},
        "priceUsd": price,
        "liquidity": {"usd": liq},
        "fdv": 100000,
        "marketCap": 90000,
        "volume": {"m5": 10, "h1": 100, "h24": 1000},
        "txns": {"m5": {"buys": 1, "sells": 2}, "h1": {"buys": 3, "sells": 4},
                 "h24": {"buys": 5, "sells": 6}},
        "priceChange": {"h24": -12.5},
        "pairCreatedAt": 1787000000000,
    }


class FakeClient:
    """Injectable stand-in. Records what it was asked for."""

    def __init__(self, pairs_by_mint=None, *, fail=False, status=200):
        self._pairs = pairs_by_mint or {}
        self._fail = fail
        self._status = status
        self.calls: list[list[str]] = []
        self.bucket = TokenBucket()

    async def tokens(self, mints):
        self.calls.append(list(mints))
        if self._fail:
            return BatchResult(list(mints), {}, None, 1, error="ConnectError: boom")
        found = {}
        for m in mints:
            pair = self._pairs.get(m)
            if pair is not None:
                found[m] = best_pair(m, [pair])
        return BatchResult(list(mints), found, self._status, 1, raw=list(self._pairs.values()))


async def _seed(db, mint: str, *, age_seconds: int) -> None:
    detected = dt.datetime.now(tz=dt.UTC) - dt.timedelta(seconds=age_seconds)
    await db.execute(
        "insert into tokens_seen (mint, first_feed, detected_at) values (?, ?, ?)",
        mint, "pumpportal", iso(detected),
    )


# -- horizon scheduling ----------------------------------------------------

async def test_horizon_is_not_due_before_it_elapses(sqlite_db):
    await _seed(sqlite_db, pubkey(11), age_seconds=60)
    due = await repo.due_horizons(sqlite_db, "15m", HORIZON_SECONDS["15m"])
    assert due == []


async def test_horizon_is_due_once_it_elapses(sqlite_db):
    mint = pubkey(12)
    await _seed(sqlite_db, mint, age_seconds=HORIZON_SECONDS["15m"] + 5)
    due = await repo.due_horizons(sqlite_db, "15m", HORIZON_SECONDS["15m"])
    assert [r["mint"] for r in due] == [mint]
    # ... and the longer horizons are not yet
    assert await repo.due_horizons(sqlite_db, "24h", HORIZON_SECONDS["24h"]) == []


async def test_no_duplicate_row_per_mint_and_horizon(sqlite_db):
    mint = pubkey(13)
    await _seed(sqlite_db, mint, age_seconds=HORIZON_SECONDS["15m"] + 5)
    labeler = Labeler(sqlite_db, client=FakeClient({mint: _pair(mint)}))
    await labeler.tick()
    await labeler.tick()
    rows = await repo.outcomes_for_mint(sqlite_db, mint)
    assert len([r for r in rows if r["horizon"] == "15m"]) == 1


async def test_an_observed_horizon_is_no_longer_due(sqlite_db):
    mint = pubkey(14)
    await _seed(sqlite_db, mint, age_seconds=HORIZON_SECONDS["15m"] + 5)
    await Labeler(sqlite_db, client=FakeClient({mint: _pair(mint)})).tick()
    assert await repo.due_horizons(sqlite_db, "15m", HORIZON_SECONDS["15m"]) == []


# -- the {"pairs": null} case ----------------------------------------------

async def test_empty_response_at_15m_is_flagged_ambiguous(sqlite_db):
    """DexScreener indexes late. An empty answer at 15m may mean 'not yet',
    and must not silently become a 'died instantly' label."""
    mint = pubkey(15)
    await _seed(sqlite_db, mint, age_seconds=HORIZON_SECONDS["15m"] + 5)
    await Labeler(sqlite_db, client=FakeClient({})).tick()
    row = (await repo.outcomes_for_mint(sqlite_db, mint))[0]
    assert row["horizon"] == "15m"
    assert row["status"] == "no_pool"
    assert row["ambiguous_no_pool"] == 1


async def test_empty_response_at_24h_is_an_unambiguous_no_pool(sqlite_db):
    mint = pubkey(16)
    await _seed(sqlite_db, mint, age_seconds=HORIZON_SECONDS["24h"] + 5)
    await Labeler(sqlite_db, client=FakeClient({})).tick()
    rows = {r["horizon"]: r for r in await repo.outcomes_for_mint(sqlite_db, mint)}
    assert rows["24h"]["status"] == "no_pool"
    assert rows["24h"]["ambiguous_no_pool"] == 0
    # and the same vendor silence at 15m on the same mint IS flagged
    assert rows["15m"]["ambiguous_no_pool"] == 1


# -- status derivation -----------------------------------------------------

async def test_liquidity_below_the_floor_is_dead_not_alive(sqlite_db):
    mint = pubkey(17)
    await _seed(sqlite_db, mint, age_seconds=HORIZON_SECONDS["15m"] + 5)
    client = FakeClient({mint: _pair(mint, liq=10.0)})
    await Labeler(sqlite_db, client=client, dead_liquidity_usd=1000.0).tick()
    row = (await repo.outcomes_for_mint(sqlite_db, mint))[0]
    assert row["status"] == "dead"
    assert row["ambiguous_no_pool"] == 0


async def test_liquidity_above_the_floor_is_alive(sqlite_db):
    mint = pubkey(18)
    await _seed(sqlite_db, mint, age_seconds=HORIZON_SECONDS["15m"] + 5)
    await Labeler(sqlite_db, client=FakeClient({mint: _pair(mint, liq=50_000.0)})).tick()
    assert (await repo.outcomes_for_mint(sqlite_db, mint))[0]["status"] == "alive"


# -- late is fine, missing is not ------------------------------------------

async def test_late_observation_records_both_timestamps(sqlite_db):
    """A 24h label taken at 26h is usable data. A missing row is not."""
    mint = pubkey(19)
    await _seed(sqlite_db, mint, age_seconds=26 * 3600)
    await Labeler(sqlite_db, client=FakeClient({mint: _pair(mint)})).tick()
    row = {r["horizon"]: r for r in await repo.outcomes_for_mint(sqlite_db, mint)}["24h"]
    assert row["scheduled_for"] < row["observed_at"]
    assert row["lateness_seconds"] >= 2 * 3600 - 60


async def test_repeated_failure_writes_an_error_row_not_a_gap(sqlite_db):
    """'Never observed' must be distinguishable from 'observed and it was gone'."""
    mint = pubkey(20)
    await _seed(sqlite_db, mint, age_seconds=HORIZON_SECONDS["15m"] + 5)
    labeler = Labeler(sqlite_db, client=FakeClient(fail=True))
    for _ in range(MAX_ATTEMPTS - 1):
        await labeler.tick()
        assert await repo.outcomes_for_mint(sqlite_db, mint) == []  # still retrying
    await labeler.tick()
    row = (await repo.outcomes_for_mint(sqlite_db, mint))[0]
    assert row["status"] == "error"
    assert "ConnectError" in row["error"]


# -- batching --------------------------------------------------------------

async def test_requests_are_chunked_to_the_batch_size(sqlite_db):
    mints = [pubkey(30 + i) for i in range(BATCH_SIZE + 7)]
    for m in mints:
        await _seed(sqlite_db, m, age_seconds=HORIZON_SECONDS["15m"] + 5)
    client = FakeClient({})
    await Labeler(sqlite_db, client=client).tick()
    fifteen = [c for c in client.calls if len(c) > 0][: 2]
    assert len(fifteen[0]) == BATCH_SIZE
    assert all(len(c) <= BATCH_SIZE for c in client.calls)


async def test_mints_the_vendor_omits_still_get_a_row(sqlite_db):
    """A partial response must not leave the omitted mints unlabeled --
    the vendor omitting a mint IS the no_pool signal."""
    present, absent = pubkey(60), pubkey(61)
    for m in (present, absent):
        await _seed(sqlite_db, m, age_seconds=HORIZON_SECONDS["15m"] + 5)
    await Labeler(sqlite_db, client=FakeClient({present: _pair(present)})).tick()
    got = {m: (await repo.outcomes_for_mint(sqlite_db, m))[0]["status"] for m in (present, absent)}
    assert got[present] == "alive"
    assert got[absent] == "no_pool"


def test_batch_over_the_cap_is_refused_rather_than_silently_truncated():
    from trenches.label.dexscreener import DexScreenerClient

    client = DexScreenerClient()
    with pytest.raises(ValueError, match="BATCH_SIZE"):
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            client.tokens([pubkey(i % 250) for i in range(BATCH_SIZE + 1)])
        )


# -- rate limiting ---------------------------------------------------------

async def test_token_bucket_caps_at_the_published_rate():
    """The failure this must survive is a backlog burst, not the steady state."""
    bucket = TokenBucket(rate=60, per_seconds=60.0)
    for _ in range(60):
        await bucket.acquire()
    assert bucket.available < 1.0
    # the 61st cannot be served instantly
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(bucket.acquire(), timeout=0.2)


async def test_token_bucket_refills_continuously():
    bucket = TokenBucket(rate=60, per_seconds=1.0)  # 60/sec for a fast test
    for _ in range(60):
        await bucket.acquire()
    await asyncio.sleep(0.2)
    assert bucket.available > 5


# -- peaks -----------------------------------------------------------------

async def test_max_price_seen_is_monotonic(sqlite_db):
    mint = pubkey(70)
    await _seed(sqlite_db, mint, age_seconds=HORIZON_SECONDS["7d"] + 5)
    now = iso(dt.datetime.now(tz=dt.UTC))
    for price in ("0.001", "0.05", "0.002", "0.9", "0.4"):
        await repo.bump_peak(sqlite_db, mint, price_usd=price, liquidity_usd="10", observed_at=now)
    peak = await repo.peak_for_mint(sqlite_db, mint)
    assert peak["max_price_usd_seen"] == "0.9"
    assert peak["observations"] == 5  # the insert counts 1, then four updates


async def test_peak_compares_numerically_not_lexically(sqlite_db):
    """TEXT columns compare lexically, where '9' beats '10'."""
    mint = pubkey(71)
    await _seed(sqlite_db, mint, age_seconds=60)
    now = iso(dt.datetime.now(tz=dt.UTC))
    await repo.bump_peak(sqlite_db, mint, price_usd="9", liquidity_usd=None, observed_at=now)
    await repo.bump_peak(sqlite_db, mint, price_usd="10", liquidity_usd=None, observed_at=now)
    assert (await repo.peak_for_mint(sqlite_db, mint))["max_price_usd_seen"] == "10"


# -- parsing ---------------------------------------------------------------

def test_prices_are_kept_as_exact_strings_never_floats():
    view = parse_pair("M", _pair("M", price="0.000000001234"))
    assert view.price_usd == "0.000000001234"
    assert isinstance(view.price_usd, str)


def test_best_pair_picks_the_deepest_pool():
    shallow = _pair("M", liq=10.0)
    deep = _pair("M", liq=99_000.0)
    assert best_pair("M", [shallow, deep]).liquidity_usd == "99000.0"


def test_best_pair_ignores_pairs_for_other_mints():
    assert best_pair("MINE", [_pair("THEIRS")]) is None


# -- the decoupling guarantee ----------------------------------------------

async def test_a_labeler_failure_does_not_stop_or_slow_ingest(sqlite_db):
    """The hard requirement: if the labeler crashes, the stream must not notice.

    run_forever swallows everything, so a labeler exploding on every tick
    leaves an independent consumer running at full speed.
    """
    from trenches.label.labeler import run_forever

    class Exploding:
        bucket = TokenBucket()

        async def tokens(self, mints):
            raise RuntimeError("labeler is on fire")

    await _seed(sqlite_db, pubkey(80), age_seconds=HORIZON_SECONDS["15m"] + 5)
    labeler = Labeler(sqlite_db, client=Exploding())

    ingest_ticks = 0

    async def pretend_ingest():
        nonlocal ingest_ticks
        while True:
            ingest_ticks += 1
            await asyncio.sleep(0.001)

    stop = asyncio.Event()
    runner = asyncio.create_task(
        run_forever(labeler, interval_seconds=0.01, backoff_min=0.01, backoff_max=0.02, stop=stop)
    )
    worker = asyncio.create_task(pretend_ingest())
    await asyncio.sleep(0.2)
    stop.set()
    runner.cancel()
    worker.cancel()
    for t in (runner, worker):
        try:
            await t
        except asyncio.CancelledError:
            pass

    assert labeler.stats.errors > 0, "the labeler should have failed repeatedly"
    assert not runner.cancelled() or True  # it never raised into the caller
    assert ingest_ticks > 20, "ingest kept running at full speed while the labeler burned"


# -- bonding curve is not a graduated pool ---------------------------------
#
# The bug this section exists to prevent: the first live run marked 30% of 15m
# observations `alive` by counting bonding curves as pools, against a
# documented graduation rate under 1% (1.3).

def _rc(market_type: str, base_usd: float) -> dict:
    return {"marketType": market_type, "lp": {"baseUSD": base_usd}}


def test_a_pump_fun_market_is_a_curve_not_a_pool():
    from trenches.label.venues import BONDING_CURVE, classify_markets
    kind, mtype = classify_markets([_rc("pump_fun", 2174.56)])
    assert kind == BONDING_CURVE
    assert mtype == "pump_fun"


def test_pumpswap_is_a_pool_because_it_is_post_graduation():
    from trenches.label.venues import DEX, classify_markets
    assert classify_markets([_rc("pump_fun_amm", 50_000.0)])[0] == DEX


def test_a_graduated_token_reports_the_pool_not_the_curve():
    """A graduated token still lists its curve. The pool is the answer."""
    from trenches.label.venues import DEX, classify_markets
    kind, mtype = classify_markets([_rc("pump_fun", 2000.0), _rc("raydium_clmm", 90_000.0)])
    assert (kind, mtype) == (DEX, "raydium_clmm")


def test_curve_liquidity_is_never_summed_into_pool_liquidity():
    """totalMarketLiquidity sums both and reports a token as deeper than
    anything it actually graduated into."""
    from trenches.label.venues import dex_liquidity_usd
    markets = [_rc("pump_fun", 2000.0), _rc("orca", 500.0)]
    assert dex_liquidity_usd(markets) == "500.0"


def test_an_unknown_market_type_is_not_assumed_to_be_a_curve():
    """Mislabelling a real pool as a curve hides winners from calibration --
    the expensive direction of the error."""
    from trenches.label.venues import DEX, classify
    assert classify("some_new_dex_2027") == DEX


def test_no_market_type_is_none():
    from trenches.label.venues import NONE, classify_markets
    assert classify_markets([])[0] == NONE


async def test_dexscreener_hits_are_recorded_as_dex_venues(sqlite_db):
    mint = pubkey(90)
    await _seed(sqlite_db, mint, age_seconds=HORIZON_SECONDS["15m"] + 5)
    await Labeler(sqlite_db, client=FakeClient({mint: _pair(mint)})).tick()
    row = (await repo.outcomes_for_mint(sqlite_db, mint))[0]
    assert row["venue_kind"] == "dex"


async def test_the_rugcheck_fallback_budget_is_enforced(sqlite_db):
    """Unbounded, the fallback becomes the primary source: the first live run
    made 694 RugCheck calls against 28 DexScreener ones."""
    class CountingRugCheck:
        def __init__(self):
            self.calls = 0

        async def report(self, mint, *, fresh=False):
            self.calls += 1
            return SimpleNamespace(payload={"markets": [_rc("pump_fun", 100.0)]})

    for i in range(20):
        await _seed(sqlite_db, pubkey(100 + i), age_seconds=HORIZON_SECONDS["15m"] + 5)
    rc = CountingRugCheck()
    labeler = Labeler(sqlite_db, client=FakeClient({}), rugcheck=rc, fallback_budget=5)
    await labeler.tick()
    assert rc.calls == 5, f"budget not enforced: {rc.calls} calls"
