"""DexScreener client for outcome observation. Free, no key, 60 req/min.

SOURCING NOTE.

The batch endpoint is `/tokens/v1/{chainId}/{tokenAddresses}` with
comma-separated addresses. DexScreener does not document the batch cap, so it
was probed against real mints on 2026-08-30 rather than guessed:

    n=30           -> 200, covered 2 of 30 requested mints
    3 x n=10       -> 200, covered the same 2 mints
    n=60           -> 200, no error

The union test is the one that matters: splitting the same 30 addresses into
three requests returned no mint that the single batch of 30 missed, so 30 is
not silently truncated. Above 30 the endpoint still answers 200, but no
truncation test was possible -- almost none of those mints have pools, so
coverage could not distinguish "absent because truncated" from "absent because
it never had a pool". BATCH_SIZE is therefore set BELOW the size that was
actually verified, and the untested region above 30 is left alone.

No rate-limit headers are exposed on the response, so the token bucket is the
only thing standing between us and a ban.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from ..log import get

log = get(__name__)

BASE_URL = "https://api.dexscreener.com"

#: Conservative, and deliberately below the 30 whose correctness was verified
#: by the union test above. Raising this needs a new probe, not an argument.
BATCH_SIZE = 25

#: Documented limit for /tokens/v1 and shared across DexScreener endpoints, so
#: one bucket governs every call this package makes.
RATE_LIMIT_PER_MINUTE = 60


class TokenBucket:
    """Continuous-refill rate limiter.

    A bucket rather than a sleep between calls, because the failure this must
    survive is a BACKLOG: after an outage the labeler has thousands of overdue
    horizons and will issue requests as fast as it can select them. Sleeping a
    fixed interval paces the steady state and does nothing about the burst; a
    bucket caps the burst too, which is the case that gets an IP banned.
    """

    def __init__(self, rate: int = RATE_LIMIT_PER_MINUTE, per_seconds: float = 60.0) -> None:
        self._capacity = float(rate)
        self._tokens = float(rate)
        self._per = per_seconds
        self._refill_per_second = rate / per_seconds
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    def _replenish(self) -> None:
        now = time.monotonic()
        gained = (now - self._updated) * self._refill_per_second
        self._tokens = min(self._capacity, self._tokens + gained)
        self._updated = now

    async def acquire(self, tokens: int = 1) -> None:
        """Block until `tokens` are available. Never overshoots the rate."""
        while True:
            async with self._lock:
                self._replenish()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                deficit = tokens - self._tokens
                wait = deficit / self._refill_per_second
            await asyncio.sleep(wait)

    @property
    def available(self) -> float:
        self._replenish()
        return self._tokens


@dataclass(slots=True)
class PairView:
    """The subset of a DexScreener pair we store. Raw payload is kept as well."""

    mint: str
    pair_address: str | None = None
    dex_id: str | None = None
    price_usd: str | None = None
    liquidity_usd: str | None = None
    fdv_usd: str | None = None
    market_cap_usd: str | None = None
    volume_m5: str | None = None
    volume_h1: str | None = None
    volume_h24: str | None = None
    txns_m5_buys: int | None = None
    txns_m5_sells: int | None = None
    txns_h1_buys: int | None = None
    txns_h1_sells: int | None = None
    txns_h24_buys: int | None = None
    txns_h24_sells: int | None = None
    price_change_h24: str | None = None
    pair_created_at: str | None = None
    raw: dict = field(default_factory=dict)


def _num(value: Any) -> str | None:
    """Keep a vendor number as its exact decimal string.

    Never float(). 3.10 forbids floats in any amount path and a price is an
    amount; `1e-9` parsed and reformatted is a different string than what the
    vendor sent, and the difference is invisible until it is not.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, (int, float)):
        return repr(value) if isinstance(value, float) else str(value)
    return None


def _int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ms_to_iso(value: Any) -> str | None:
    """pairCreatedAt is milliseconds since epoch."""
    ms = _int(value)
    if ms is None or ms <= 0:
        return None
    import datetime as dt

    try:
        return dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC).isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def parse_pair(mint: str, pair: dict) -> PairView:
    txns = pair.get("txns") or {}
    volume = pair.get("volume") or {}
    liq = pair.get("liquidity") or {}
    change = pair.get("priceChange") or {}

    def side(window: str, key: str) -> int | None:
        return _int((txns.get(window) or {}).get(key))

    return PairView(
        mint=mint,
        pair_address=pair.get("pairAddress"),
        dex_id=pair.get("dexId"),
        price_usd=_num(pair.get("priceUsd")),
        liquidity_usd=_num(liq.get("usd")),
        fdv_usd=_num(pair.get("fdv")),
        market_cap_usd=_num(pair.get("marketCap")),
        volume_m5=_num(volume.get("m5")),
        volume_h1=_num(volume.get("h1")),
        volume_h24=_num(volume.get("h24")),
        txns_m5_buys=side("m5", "buys"), txns_m5_sells=side("m5", "sells"),
        txns_h1_buys=side("h1", "buys"), txns_h1_sells=side("h1", "sells"),
        txns_h24_buys=side("h24", "buys"), txns_h24_sells=side("h24", "sells"),
        price_change_h24=_num(change.get("h24")),
        pair_created_at=_ms_to_iso(pair.get("pairCreatedAt")),
        raw=pair,
    )


def best_pair(mint: str, pairs: list[dict]) -> PairView | None:
    """Pick the deepest pool for a mint.

    A mint can list on several DEXes. Liquidity decides, because the question
    every horizon is asking is "could this have been sold", and that is
    answered by the deepest pool, not by an average across venues.
    """
    mine = [
        p for p in pairs
        if isinstance(p, dict) and (p.get("baseToken") or {}).get("address") == mint
    ]
    if not mine:
        return None
    def depth(p: dict) -> float:
        try:
            return float((p.get("liquidity") or {}).get("usd") or 0)
        except (TypeError, ValueError):
            return 0.0
    return parse_pair(mint, max(mine, key=depth))


@dataclass(slots=True)
class BatchResult:
    """One batch lookup. `found` omits mints the vendor said nothing about."""

    requested: list[str]
    found: dict[str, PairView]
    status_code: int | None
    latency_ms: int
    raw: Any = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.status_code == 200

    def missing(self) -> list[str]:
        """Requested mints the vendor returned no pair for.

        This is the common case, not an error path: most launches never get a
        tradeable pool at all.
        """
        return [m for m in self.requested if m not in self.found]


class DexScreenerClient:
    def __init__(
        self,
        base_url: str = BASE_URL,
        *,
        chain: str = "solana",
        timeout: float = 20.0,
        bucket: TokenBucket | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._chain = chain
        self._timeout = timeout
        #: Shared across every endpoint this client touches -- the published
        #: 60/min is an account-wide budget, not a per-endpoint one.
        self.bucket = bucket or TokenBucket()

    async def tokens(self, mints: list[str]) -> BatchResult:
        """Look up up to BATCH_SIZE mints in one request."""
        if len(mints) > BATCH_SIZE:
            raise ValueError(
                f"batch of {len(mints)} exceeds BATCH_SIZE={BATCH_SIZE}; chunk before calling"
            )
        url = f"{self._base}/tokens/v1/{self._chain}/{','.join(mints)}"
        await self.bucket.acquire()
        started = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(url)
            latency = int((time.monotonic() - started) * 1000)
            try:
                body = response.json()
            except ValueError:
                return BatchResult(mints, {}, response.status_code, latency,
                                   raw=response.text[:2000], error="non-JSON response")
            # The endpoint has been observed returning both a bare list and
            # {"pairs": [...] | null}. Both mean the same thing; neither shape
            # is documented, so both are accepted rather than assumed.
            pairs = body if isinstance(body, list) else (body or {}).get("pairs") or []
            found = {}
            for mint in mints:
                view = best_pair(mint, pairs if isinstance(pairs, list) else [])
                if view is not None:
                    found[mint] = view
            return BatchResult(mints, found, response.status_code, latency, raw=body)
        except httpx.HTTPError as exc:
            latency = int((time.monotonic() - started) * 1000)
            return BatchResult(mints, {}, None, latency, error=f"{type(exc).__name__}: {exc}")
