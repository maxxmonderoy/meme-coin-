"""RugCheck `/v1/stats/new_tokens` -- the second free feed (3.2, 3.3).

The failure this module exists to prevent: WITHOUT a cache-busting parameter
this endpoint returns entries roughly three hours old. It does not error, it
does not look broken, and a poller built on it will happily report launches all
day while being completely useless as a feed. That is a silent failure of
exactly the 3.8.8 kind.

So two defences, not one:

  1. Every request carries a cache-buster.
  2. `assert_fresh()` runs AT STARTUP and refuses to begin if the newest entry
     is older than the threshold. A feed that is already stale when it starts
     will never recover on its own, and finding that out on day seven of a
     seven-day soak wastes the week.

Freshness is also re-checked continuously while running; a feed that goes stale
mid-run increments stale_responses and is visible in `trenches stats`.
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import time
from collections.abc import AsyncIterator

import httpx

from ..log import get
from .base import Consumer, EventKind, RawEvent

log = get(__name__)

DEFAULT_BASE = "https://api.rugcheck.xyz/v1"
NEW_TOKENS_PATH = "/stats/new_tokens"

#: If the newest entry is older than this at startup, the cache-buster is not
#: working and the feed is worthless. Generous relative to the ~3 hour cached
#: response it is meant to catch, tight enough to catch it every time.
STARTUP_MAX_AGE_SECONDS = 600.0

#: Candidate keys for the creation timestamp, most specific first. RugCheck's
#: listing shape is not pinned by published docs available here, so freshness
#: detection tries several rather than assuming one.
_TIME_KEYS = ("createAt", "creation_time", "createdAt", "created_at", "detectedAt", "time")
_MINT_KEYS = ("mint", "address", "tokenMint", "id")


class FeedStaleError(RuntimeError):
    """The endpoint is serving cached data; the cache-buster is not working."""


def extract_mint(entry: dict) -> str | None:
    for key in _MINT_KEYS:
        value = entry.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def extract_created(entry: dict) -> dt.datetime | None:
    for key in _TIME_KEYS:
        if key not in entry:
            continue
        value = entry[key]
        if isinstance(value, (int, float)) and value > 0:
            seconds = value / 1000 if value > 10**11 else value
            with contextlib.suppress(OverflowError, OSError, ValueError):
                return dt.datetime.fromtimestamp(seconds, tz=dt.UTC)
        if isinstance(value, str) and value:
            with contextlib.suppress(ValueError):
                parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)
    return None


def newest_age_seconds(entries: list[dict], *, now: dt.datetime | None = None) -> float | None:
    now = now or dt.datetime.now(tz=dt.UTC)
    ages = [
        (now - created).total_seconds()
        for created in (extract_created(e) for e in entries)
        if created is not None
    ]
    return min(ages) if ages else None


class RugCheckNewTokensConsumer(Consumer):
    provider = "rugcheck"
    #: Polled, so silence means the poll loop died rather than the market being
    #: quiet. Tighter than the websocket's threshold for that reason.
    idle_timeout_seconds = 90.0

    def __init__(
        self,
        base_url: str = DEFAULT_BASE,
        *,
        interval_seconds: float = 2.0,
        timeout: float = 10.0,
        startup_max_age_seconds: float = STARTUP_MAX_AGE_SECONDS,
        assert_fresh_on_start: bool = True,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._interval = interval_seconds
        self._timeout = timeout
        self._startup_max_age = startup_max_age_seconds
        self._assert_fresh = assert_fresh_on_start
        self._client: httpx.AsyncClient | None = None
        self.stale_responses = 0

    def subscription_descriptor(self) -> dict:
        return {
            "provider": self.provider,
            "url": f"{self._base}{NEW_TOKENS_PATH}",
            "interval_seconds": self._interval,
            "cache_buster": True,
            "startup_max_age_seconds": self._startup_max_age,
            "role": "second launch feed, cross-check on different infrastructure (3.3)",
        }

    async def _fetch(self) -> list[dict]:
        assert self._client is not None
        # The cache-buster. Without it this endpoint serves ~3-hour-old entries
        # and the whole feed is decorative.
        response = await self._client.get(
            f"{self._base}{NEW_TOKENS_PATH}", params={"_cb": str(time.time_ns())}
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            for key in ("tokens", "data", "results"):
                if isinstance(payload.get(key), list):
                    return payload[key]
            return []
        return payload if isinstance(payload, list) else []

    async def assert_fresh(self) -> float:
        """Refuse to start on a stale endpoint. Returns the observed age."""
        entries = await self._fetch()
        if not entries:
            raise FeedStaleError(
                f"{self._base}{NEW_TOKENS_PATH} returned no entries at startup; cannot "
                "establish that the feed is live"
            )
        age = newest_age_seconds(entries)
        if age is None:
            raise FeedStaleError(
                "could not find a creation timestamp on any entry, so freshness cannot be "
                f"verified. Observed keys on the first entry: {sorted(entries[0])}"
            )
        if age > self._startup_max_age:
            raise FeedStaleError(
                f"newest entry is {age:.0f}s old (limit {self._startup_max_age:.0f}s). "
                "This is the documented cached-response failure: the endpoint returns "
                "~3-hour-old entries when the cache-buster is not taking effect. The feed "
                "would look like it is working while being useless."
            )
        log.info("rugcheck new_tokens freshness OK: newest entry %.1fs old", age)
        return age

    async def stream(self) -> AsyncIterator[RawEvent]:
        self._client = httpx.AsyncClient(timeout=self._timeout, follow_redirects=True)
        if self._assert_fresh:
            await self.assert_fresh()

        seen: set[str] = set()
        while True:
            entries = await self._fetch()
            age = newest_age_seconds(entries)
            if age is not None and age > self._startup_max_age:
                self.stale_responses += 1
                log.warning("rugcheck response went stale: newest entry %.0fs old", age)

            for entry in entries:
                mint = extract_mint(entry)
                if not mint or mint in seen:
                    continue
                seen.add(mint)
                yield RawEvent(
                    provider=self.provider,
                    event_kind=EventKind.CREATE,
                    mint=mint,
                    block_time=extract_created(entry),
                    payload={"raw": entry},
                )
            # Bound the local set so a week-long run does not grow without limit.
            if len(seen) > 50_000:
                seen = set(list(seen)[-25_000:])
            # A heartbeat so the supervisor can tell a quiet market from a dead
            # poll loop.
            yield RawEvent(provider=self.provider, event_kind=EventKind.PING)
            await asyncio.sleep(self._interval)

    async def aclose(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                await self._client.aclose()
            self._client = None


def token_fields(entry: dict) -> dict:
    """Map a listing entry onto tokens_seen columns."""
    return {
        # NO fallback to `program`. That field is the SPL Token / Token-2022
        # program the mint is owned by, not a launchpad, and it is already
        # stored verbatim as program_id below. The fallback put a token program
        # ID into a column meaning "which launchpad launched this" for 21.6% of
        # rows -- every one an exact duplicate of program_id, so it carried no
        # information and only made `group by launchpad` wrong. Part 0 rule 2:
        # a gap beats an invented value.
        "launchpad": entry.get("launchpad"),
        "name": entry.get("name") or (entry.get("fileMeta") or {}).get("name"),
        "symbol": entry.get("symbol") or (entry.get("fileMeta") or {}).get("symbol"),
        "uri": entry.get("uri") or entry.get("metadataUri"),
        "declared_creator": entry.get("creator"),
        "program_id": entry.get("program"),
    }
