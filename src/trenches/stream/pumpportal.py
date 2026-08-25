"""PumpPortal websocket consumer -- transport complete, mapping deliberately absent.

CLAUDE.md 3.2 puts PumpPortal here as a redundant cross-check on different
infrastructure, never as the primary feed, and limits it to ONE connection:
multiple concurrent connections get hourly-banned.

The frame schema could not be sourced when this was written (the documentation
host was unreachable), so `map_frame` raises rather than guessing at field
names. A guessed mapping does not fail loudly -- it silently produces mints
that are wrong or absent, which is exactly the decoder-breakage failure in
3.8.8 shipped on purpose.

To finish it:
    trenches stream --feed pumpportal --record captures/pumpportal
    # let it run, then read the captured frames and implement map_frame()
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
from collections.abc import AsyncIterator
from pathlib import Path

import websockets

from ..log import get
from .base import Consumer, EventKind, RawEvent

log = get(__name__)

SUBSCRIBE_NEW_TOKEN = {"method": "subscribeNewToken"}

#: Process-wide guard. PumpPortal bans on concurrent connections, and the
#: cheapest way to violate that is a supervisor restart racing its predecessor.
#: This cannot stop a second copy of the program; it stops this one.
_CONNECTION_HELD = asyncio.Lock()


class FrameMappingNotImplemented(NotImplementedError):
    """Raised when a live frame is received but no verified mapping exists."""


class PumpPortalConsumer(Consumer):
    provider = "pumpportal"

    def __init__(
        self,
        url: str = "wss://pumpportal.fun/api/data",
        *,
        record_dir: Path | None = None,
        keepalive_seconds: int = 30,
    ) -> None:
        self._url = url
        self._record_dir = record_dir
        self._keepalive_seconds = keepalive_seconds
        self._ws: websockets.ClientConnection | None = None
        self._recorder = None

    def subscription_descriptor(self) -> dict:
        return {
            "provider": self.provider,
            "url": self._url,
            "messages": [SUBSCRIBE_NEW_TOKEN],
            "recording": str(self._record_dir) if self._record_dir else None,
            "role": "cross-check only, never primary (3.2)",
        }

    def _open_recorder(self):
        if not self._record_dir:
            return None
        self._record_dir.mkdir(parents=True, exist_ok=True)
        stamp = dt.datetime.now(tz=dt.UTC).strftime("%Y%m%dT%H%M%SZ")
        path = self._record_dir / f"pumpportal-{stamp}.jsonl"
        log.info("recording pumpportal frames to %s", path)
        return path.open("a", encoding="utf-8")

    async def stream(self) -> AsyncIterator[RawEvent]:
        if _CONNECTION_HELD.locked():
            raise RuntimeError(
                "a PumpPortal connection is already open in this process; opening a "
                "second one risks an hourly ban (CLAUDE.md 3.2)"
            )
        async with _CONNECTION_HELD:
            self._recorder = self._open_recorder()
            async with websockets.connect(
                self._url,
                ping_interval=self._keepalive_seconds,
                ping_timeout=self._keepalive_seconds,
                max_size=None,
            ) as ws:
                self._ws = ws
                await ws.send(json.dumps(SUBSCRIBE_NEW_TOKEN))
                async for message in ws:
                    frame = self._record(message)
                    if frame is None:
                        continue
                    event = self.map_frame(frame)
                    if event is not None:
                        yield event

    def _record(self, message: str | bytes) -> dict | None:
        text = message.decode() if isinstance(message, bytes) else message
        try:
            frame = json.loads(text)
        except json.JSONDecodeError:
            log.warning("pumpportal sent non-JSON frame of %d bytes", len(text))
            return None
        if self._recorder:
            self._recorder.write(
                json.dumps(
                    {"received_at": dt.datetime.now(tz=dt.UTC).isoformat(), "frame": frame},
                    separators=(",", ":"),
                )
                + "\n"
            )
            self._recorder.flush()
        return frame

    def map_frame(self, frame: dict) -> RawEvent | None:
        """Map a PumpPortal frame to a RawEvent.

        NOT IMPLEMENTED ON PURPOSE. Implementing this from an assumed schema
        would produce a consumer that appears to work and quietly emits wrong
        mints. Record real frames first, read them, then write this against
        what the feed actually sends.
        """
        # Subscription acknowledgements carry no market data and are not an
        # excuse to guess at the rest of the schema.
        if isinstance(frame, dict) and frame.keys() <= {"message"}:
            return RawEvent(
                signature="", slot=0, provider=self.provider,
                event_kind=EventKind.PING, commitment="processed",
            )
        raise FrameMappingNotImplemented(
            "PumpPortal frame mapping is unverified. Capture frames with "
            "`trenches stream --feed pumpportal --record <dir>`, inspect them, then "
            "implement PumpPortalConsumer.map_frame. Observed keys: "
            f"{sorted(frame)[:12]}"
        )

    async def aclose(self) -> None:
        if self._recorder:
            with contextlib.suppress(Exception):
                self._recorder.close()
            self._recorder = None
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None
