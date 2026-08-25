"""RugCheck client.

Two findings from CLAUDE.md Part 2 are encoded here as behaviour, not comments:

1. Never gate on the composite score. `score` is an unbounded sum over
   per-market rows, so a token listed on many pools accumulates the same risk
   repeatedly -- BONK reads 96/100. This client surfaces the underlying fields
   and refuses to reduce them to a verdict.

2. For a held position, always bypass cache. A mint that rugged at 11m07s
   still returned `score: 1, risks: [], rugged: false` thirty seconds later,
   because the report endpoint is cached behind the rug detector. `fresh=True`
   records that a bypass was requested so the journal can prove which verdicts
   were cached.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx

#: Fields that populate immediately. Part 2: structural facts land in ~1s.
STRUCTURAL_FIELDS = (
    "mintAuthority", "freezeAuthority", "token", "tokenType",
    "transferFee", "rugged", "creator",
)

#: Fields with a cold start. These need transaction history that has not
#: happened yet for a mint under a minute old, and reading them as "clean"
#: rather than "unknown" is the single most expensive mistake in Part 2.
COLD_START_FIELDS = (
    "score", "score_normalised", "risks", "topHolders", "markets",
    "totalHolders", "insiderNetworks", "graphInsidersDetected", "creatorTokens",
)


@dataclass(slots=True)
class Probe:
    mint: str
    status_code: int | None
    latency_ms: int
    bypassed_cache: bool
    payload: Any
    error: str | None = None

    def cold_start_report(self) -> dict[str, str]:
        """Classify each cold-start field as populated, empty or absent.

        'empty' is the dangerous one: an empty risks array reads as a clean
        token and is indistinguishable from a token nobody has traded yet.
        """
        if not isinstance(self.payload, dict):
            return {}
        out: dict[str, str] = {}
        for field in COLD_START_FIELDS:
            if field not in self.payload:
                out[field] = "absent"
                continue
            value = self.payload[field]
            if value is None or value == [] or value == {} or value == 0:
                out[field] = "empty"
            else:
                out[field] = "populated"
        return out


class RugCheckClient:
    def __init__(self, base_url: str, *, timeout: float = 15.0) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout

    async def report(self, mint: str, *, fresh: bool = False) -> Probe:
        """Fetch a token report.

        `fresh` adds a cache-buster. Note that RugCheck's documented
        `refresh=true` is paid-tier only, so on a free key a bypass cannot be
        guaranteed -- which is precisely why the flag is recorded alongside the
        payload rather than assumed to have worked.
        """
        url = f"{self._base}/tokens/{mint}/report"
        params = {"_cb": str(time.time_ns())} if fresh else None
        started = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(url, params=params)
            latency = int((time.monotonic() - started) * 1000)
            try:
                payload = response.json()
            except ValueError:
                payload = {"_raw": response.text[:2000]}
            return Probe(mint, response.status_code, latency, fresh, payload)
        except httpx.HTTPError as exc:
            latency = int((time.monotonic() - started) * 1000)
            return Probe(mint, None, latency, fresh, None, error=str(exc))

    async def new_tokens(self) -> Probe:
        """`/stats/new_tokens`.

        Always cache-busted: without one this endpoint has been measured
        returning three-hour-old entries, and with one it returns sub-second
        detections (3.4 stage 0). The difference is not a tuning detail.
        """
        url = f"{self._base}/stats/new_tokens"
        started = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(url, params={"_cb": str(time.time_ns())})
            latency = int((time.monotonic() - started) * 1000)
            try:
                payload = response.json()
            except ValueError:
                payload = {"_raw": response.text[:2000]}
            return Probe("<new_tokens>", response.status_code, latency, True, payload)
        except httpx.HTTPError as exc:
            latency = int((time.monotonic() - started) * 1000)
            return Probe("<new_tokens>", None, latency, True, None, error=str(exc))
