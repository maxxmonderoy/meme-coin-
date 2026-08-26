"""PumpPortal websocket -- the primary launch feed on the $0 stack (3.2).

SOURCING NOTE, read before trusting a decoded field.

The subscribe side is verified against PumpPortal's own repository
(thetateman/Pump-Fun-API README): the endpoint is `wss://pumpportal.fun/api/data`
and the request is exactly `{"method": "subscribeNewToken"}` with no keys.
That README documents three methods -- subscribeNewToken, subscribeTokenTrade,
subscribeAccountTrade -- and does NOT document `subscribeMigration`, so that
method is not requested here despite appearing in 3.2.

The RESPONSE payload is not documented anywhere primary. The README's own
example just logs the parsed frame. The field names below are therefore marked
UNVERIFIED and are treated as a hypothesis to be checked against reality, not
as truth:

  * Every frame is validated against the declared schema.
  * A frame missing a REQUIRED field raises with the observed keys attached,
    rather than yielding an event with a null mint.
  * The complete raw frame is preserved in `payload` regardless, so a wrong
    guess here costs nothing permanent -- the data is still on disk.

Run `trenches verify-capture <dir>` against recorded frames to confirm or
correct the schema before relying on any of it.
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path

import websockets

from ..log import get
from .base import Consumer, EventKind, RawEvent

log = get(__name__)

WS_URL = "wss://pumpportal.fun/api/data"

#: Verified against the official README.
SUBSCRIBE_NEW_TOKEN = {"method": "subscribeNewToken"}


@dataclass(frozen=True, slots=True)
class FrameSchema:
    """Declared expectation for a frame kind. UNVERIFIED until a capture confirms it."""

    kind: str
    required: tuple[str, ...]
    optional: tuple[str, ...] = ()
    verified: bool = False

    @property
    def known(self) -> frozenset[str]:
        return frozenset(self.required) | frozenset(self.optional)


#: Field names reported by third-party write-ups, never by PumpPortal's own docs.
#: `mint` and `signature` are required because without them the frame cannot be
#: deduped or stored; everything else is optional so an upstream addition or
#: rename degrades the record rather than dropping the launch.
NEW_TOKEN_SCHEMA = FrameSchema(
    kind=EventKind.CREATE,
    required=("mint", "signature"),
    optional=(
        "traderPublicKey", "txType", "name", "symbol", "uri", "initialBuy",
        "solAmount", "bondingCurveKey", "vTokensInBondingCurve",
        "vSolInBondingCurve", "marketCapSol", "pool", "creator", "timestamp",
    ),
    verified=False,
)


class FrameSchemaMismatch(ValueError):
    """A frame did not match the declared schema."""

    def __init__(self, message: str, frame: dict) -> None:
        super().__init__(message)
        self.frame = frame
        self.observed_keys = sorted(frame)


@dataclass(slots=True)
class ValidationReport:
    frames: int = 0
    matched: int = 0
    missing_required: dict[str, int] = field(default_factory=dict)
    unknown_fields: dict[str, int] = field(default_factory=dict)
    absent_optional: dict[str, int] = field(default_factory=dict)

    def observe(self, frame: dict, schema: FrameSchema) -> None:
        self.frames += 1
        missing = [f for f in schema.required if f not in frame]
        for name in missing:
            self.missing_required[name] = self.missing_required.get(name, 0) + 1
        for name in set(frame) - schema.known:
            self.unknown_fields[name] = self.unknown_fields.get(name, 0) + 1
        for name in schema.optional:
            if name not in frame:
                self.absent_optional[name] = self.absent_optional.get(name, 0) + 1
        if not missing:
            self.matched += 1


def validate(frame: dict, schema: FrameSchema = NEW_TOKEN_SCHEMA) -> dict:
    """Return the frame if it satisfies the schema, else raise with observed keys."""
    missing = [f for f in schema.required if f not in frame or frame[f] in (None, "")]
    if missing:
        raise FrameSchemaMismatch(
            f"frame is missing required field(s) {missing}. The declared PumpPortal "
            f"schema is UNVERIFIED (their docs do not publish the payload shape). "
            f"Observed keys: {sorted(frame)}",
            frame,
        )
    return frame


def _as_int(value: object) -> int | None:
    """Coerce to an exact integer or give up.

    Never returns a float. 3.10 forbids floats in any amount path, and a JSON
    number that arrived as 1e21 is not a token amount we can trust.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value)
    return None


class PumpPortalConsumer(Consumer):
    provider = "pumpportal"
    #: Launches arrive irregularly. websockets' own ping/pong keeps the socket
    #: honest; this catches the case where the socket is open and silent.
    idle_timeout_seconds = 120.0

    def __init__(
        self,
        url: str = WS_URL,
        *,
        record_dir: Path | None = None,
        keepalive_seconds: int = 30,
        strict: bool = False,
    ) -> None:
        """`strict=True` raises on any schema mismatch. Default records the
        mismatch, keeps the raw frame, and continues -- a single malformed
        frame should not take down a feed that has been running for six days."""
        self._url = url
        self._record_dir = record_dir
        self._keepalive_seconds = keepalive_seconds
        self._strict = strict
        self._ws = None
        self._recorder = None
        self.mismatches = 0
        #: Guards the ONE-connection rule (3.2/3.3). Instance-level rather than
        #: module-level so the supervisor's close-before-reopen is what enforces
        #: it, but a second concurrent stream() on the same object still fails.
        self._in_use = asyncio.Lock()

    def subscription_descriptor(self) -> dict:
        return {
            "provider": self.provider,
            "url": self._url,
            "messages": [SUBSCRIBE_NEW_TOKEN],
            "subscribe_verified": True,
            "payload_schema_verified": NEW_TOKEN_SCHEMA.verified,
            "required_fields": list(NEW_TOKEN_SCHEMA.required),
            "recording": str(self._record_dir) if self._record_dir else None,
            "role": "primary launch feed ($0 stack, 3.2)",
        }

    def _open_recorder(self):
        if not self._record_dir:
            return None
        self._record_dir.mkdir(parents=True, exist_ok=True)
        stamp = dt.datetime.now(tz=dt.UTC).strftime("%Y%m%dT%H%M%SZ")
        path = self._record_dir / f"pumpportal-frames-{stamp}.jsonl"
        log.info("recording raw pumpportal frames to %s", path)
        return path.open("a", encoding="utf-8")

    async def stream(self) -> AsyncIterator[RawEvent]:
        if self._in_use.locked():
            raise RuntimeError(
                "a PumpPortal connection is already open on this consumer. Opening a "
                "second concurrent connection earns an hourly ban (3.2) -- the "
                "supervisor must close before it reopens."
            )
        async with self._in_use:
            self._recorder = self._open_recorder()
            async with websockets.connect(
                self._url,
                ping_interval=self._keepalive_seconds,
                ping_timeout=self._keepalive_seconds,
                max_size=None,
            ) as ws:
                self._ws = ws
                await ws.send(json.dumps(SUBSCRIBE_NEW_TOKEN))
                if not NEW_TOKEN_SCHEMA.verified:
                    log.warning(
                        "pumpportal payload schema is UNVERIFIED; validating every frame "
                        "against the declared shape and preserving raw frames. Run "
                        "`trenches verify-capture` on a recording to confirm it."
                    )
                async for message in ws:
                    frame = self._record(message)
                    if frame is None:
                        continue
                    event = self.map_frame(frame)
                    if event is not None:
                        yield event

    def _record(self, message) -> dict | None:
        text = message.decode() if isinstance(message, bytes) else message
        try:
            frame = json.loads(text)
        except json.JSONDecodeError:
            log.warning("pumpportal sent a non-JSON frame of %d bytes", len(text))
            return None
        if self._recorder:
            self._recorder.write(json.dumps(
                {"received_at": dt.datetime.now(tz=dt.UTC).isoformat(), "frame": frame},
                separators=(",", ":"),
            ) + "\n")
            self._recorder.flush()
        return frame if isinstance(frame, dict) else None

    def map_frame(self, frame: dict) -> RawEvent | None:
        # Subscription acknowledgement. Carries no market data.
        if frame.keys() <= {"message", "method"}:
            return RawEvent(provider=self.provider, event_kind=EventKind.PING, payload=frame)

        try:
            validate(frame, NEW_TOKEN_SCHEMA)
        except FrameSchemaMismatch as exc:
            self.mismatches += 1
            if self._strict:
                raise
            log.warning("pumpportal frame failed schema validation: %s", exc)
            # The launch is still recorded, with a null mint flagged, rather
            # than silently discarded. Losing a launch is worse than storing an
            # imperfect row we can repair from the preserved raw frame.
            return RawEvent(
                provider=self.provider, event_kind=EventKind.OTHER,
                signature=frame.get("signature"),
                payload={"raw": frame, "schema_mismatch": str(exc)},
            )

        block_time = None
        ts = _as_int(frame.get("timestamp"))
        if ts and ts > 0:
            with contextlib.suppress(OverflowError, OSError, ValueError):
                # Reported in milliseconds by most writeups; guard both.
                block_time = dt.datetime.fromtimestamp(
                    ts / 1000 if ts > 10**11 else ts, tz=dt.UTC
                )

        return RawEvent(
            provider=self.provider,
            event_kind=EventKind.CREATE,
            mint=frame["mint"],
            signature=frame["signature"],
            block_time=block_time,
            payload={"raw": frame},
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


def token_fields(frame: dict) -> dict:
    """Map a validated frame onto tokens_seen columns.

    Kept separate from map_frame so the field-name hypothesis lives in exactly
    two places, both marked, and correcting it after a capture is a small diff.
    """
    return {
        "signer": frame.get("traderPublicKey"),
        "declared_creator": frame.get("creator") or frame.get("traderPublicKey"),
        "launchpad": "pump.fun",
        "bonding_curve": frame.get("bondingCurveKey"),
        "name": frame.get("name"),
        "symbol": frame.get("symbol"),
        "uri": frame.get("uri"),
        "pool": frame.get("pool"),
        "initial_buy_base": _as_int(frame.get("initialBuy")),
        "virtual_token_reserves": _as_int(frame.get("vTokensInBondingCurve")),
        "signature": frame.get("signature"),
    }
