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
import ssl
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path

import certifi
import websockets

from ..log import get
from .base import Consumer, EventKind, RawEvent

log = get(__name__)

WS_URL = "wss://pumpportal.fun/api/data"


def _tls_context() -> ssl.SSLContext:
    """Trust store pinned to certifi rather than whatever the host happens to have.

    `websockets` defaults to `ssl.create_default_context()`, which on a python.org
    macOS build loads ZERO certificate authorities until someone runs
    `Install Certificates.command` by hand. The failure is
    `CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate`, and the
    supervisor treats it as a transient network fault and reconnects forever --
    a feed that looks alive in the logs and never delivers a frame. That is the
    §3.8.1 silent-stall class, arriving through TLS instead of a load balancer.

    httpx does not have the problem because it already ships certifi, which is
    why the RugCheck feed came up on the same run this one failed on. Binding
    here makes the two consumers agree and makes the result independent of how
    the host's Python was installed.
    """
    return ssl.create_default_context(cafile=certifi.where())


#: Built once. Contexts are reusable across connections and parsing the CA
#: bundle on every reconnect would be wasted work during a backoff storm.
TLS = _tls_context()

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


#: CORRECTED against 67 live `create` frames captured 2026-08-26 (75s window).
#: What that capture changed, versus the third-hand write-ups this started from:
#:
#:   * `creator` does NOT exist. The signer arrives as `traderPublicKey`. This
#:     matters for 3.4 stage 2: pump.fun's `create` takes `creator` as an
#:     explicit argument distinct from the signer, so the reputation cache can
#:     only be keyed on the signer from THIS feed. Getting the declared creator
#:     needs the on-chain CreateEvent, not PumpPortal.
#:   * `timestamp` does NOT exist. No frame carries an on-chain time, so
#:     `block_time` is always None here and feed latency is measurable only as
#:     relative arrival order, never as true launch-to-detection.
#:   * `is_mayhem_mode` was undeclared and present in 65/67.
#:
#: THE FRAME SHAPE DEPENDS ON `pool`, which is the important finding:
#: `subscribeNewToken` is not a pump.fun feed, it is a multi-launchpad feed.
#: The capture carried pool=pump (65) and pool=bonk (2), and they do not agree
#: on fields. pump carries bondingCurveKey / vTokensInBondingCurve /
#: vSolInBondingCurve; bonk carries tokensInPool / newTokenBalance instead.
#: A consumer that assumes the pump.fun curve will read garbage off a bonk
#: launch -- 3.5's program IDs and curve maths apply to pump ONLY.
#:
#: `mint` and `signature` stay the only required fields even though eight are
#: always present: required means "cannot be stored without", and keeping the
#: rest optional means an upstream rename degrades a record instead of dropping
#: a launch.
#:
#: STILL `verified=False` ON PURPOSE. 75 seconds is not seven days, and a
#: window that short cannot show a rarer variant -- another pool, a migration
#: frame, a field that only appears on some launches. The soak's capture is
#: what should promote this. Re-run `trenches verify-capture` against it and
#: flip this flag then.
NEW_TOKEN_SCHEMA = FrameSchema(
    kind=EventKind.CREATE,
    required=("mint", "signature"),
    optional=(
        # present in every observed frame
        "traderPublicKey", "txType", "initialBuy", "solAmount", "marketCapSol",
        "pool",
        # pool=pump shape
        "bondingCurveKey", "vTokensInBondingCurve", "vSolInBondingCurve",
        "name", "symbol", "uri", "is_mayhem_mode",
        # pool=bonk shape. `solInPool` appeared in 82 of 33,979 frames -- it
        # did not exist in the first 67 and is exactly the rare variant that
        # kept this schema at verified=False.
        "tokensInPool", "newTokenBalance", "solInPool",
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
            # ssl= is only meaningful for wss://; passing it for a plaintext
            # ws:// URL (a local capture replay server) is an error.
            tls = TLS if self._url.startswith("wss://") else None
            async with websockets.connect(
                self._url,
                ping_interval=self._keepalive_seconds,
                ping_timeout=self._keepalive_seconds,
                max_size=None,
                ssl=tls,
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

        # No `timestamp` appeared in any of the 67 captured frames, so this is
        # always None in practice. Kept rather than deleted: if PumpPortal ever
        # adds one, reading it is better than silently ignoring it.
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


def _launchpad(pool: str | None) -> str | None:
    """Resolve `pool` to a launchpad. It is NOT always pump.fun.

    Hardcoding "pump.fun" here was wrong: the 2026-08-26 capture carried
    pool=bonk alongside pool=pump, and mislabelling those poisons two things at
    once -- the 3.4 stage-2 creator reputation table (a deployer's rug rate gets
    attributed to the wrong launchpad) and any consumer that applies 3.5's
    pump.fun curve maths to a token that is not on a pump.fun curve.

    Unrecognised pools are stored marked rather than guessed at. Part 0 rule 2:
    a plausible-sounding product name is worse than a gap, and a pool string we
    have not traced to a program is exactly that gap.
    """
    if pool is None:
        return None
    if pool == "pump":
        return "pump.fun"
    return f"unverified:{pool}"


def token_fields(frame: dict) -> dict:
    """Map a validated frame onto tokens_seen columns.

    Kept separate from map_frame so the field-name hypothesis lives in exactly
    two places, both marked, and correcting it after a capture is a small diff.
    """
    return {
        "signer": frame.get("traderPublicKey"),
        # NO fallback to the signer. `creator` never appears in this feed, so a
        # fallback would write the signer into both columns and make the
        # two-key stage-2 reputation cache an illusion -- it would look like it
        # keyed on signer AND creator while keying on one value twice, which is
        # exactly the rotation bypass keying on both was meant to stop. A null
        # says truthfully that this feed does not carry it; the on-chain
        # CreateEvent does.
        "declared_creator": frame.get("creator"),
        "launchpad": _launchpad(frame.get("pool")),
        "bonding_curve": frame.get("bondingCurveKey"),
        "name": frame.get("name"),
        "symbol": frame.get("symbol"),
        "uri": frame.get("uri"),
        "pool": frame.get("pool"),
        "initial_buy_base": _as_int(frame.get("initialBuy")),
        "virtual_token_reserves": _as_int(frame.get("vTokensInBondingCurve")),
        # Present in 65 of 67 captured frames and previously dropped on the
        # floor, which is why the `mayhem_mode` rule in label/rules.py has
        # never once fired -- it reads a column nothing wrote.
        "is_mayhem_mode": (
            None if frame.get("is_mayhem_mode") is None
            else int(bool(frame["is_mayhem_mode"]))
        ),
        "signature": frame.get("signature"),
    }


# DELIBERATELY NOT MAPPED, and this is not an oversight:
#
#   vSolInBondingCurve -> virtual_sol_reserves
#     The frame reports 30, and pump.fun's initial virtual reserve is 30 SOL --
#     so the field is DENOMINATED IN SOL, not lamports. Writing 30 into a
#     base-units column would be wrong by 1e9. It also goes fractional after
#     any trade, and _as_int correctly refuses a float in an amount path
#     (3.10). Mapping it needs a decided unit convention, not a one-liner.
#
#   marketCapSol
#     A float, and there is no column for it. Storing it as an amount would
#     put a float in the money path.
#
# initial_buy_base has the same shape of problem and is why it is only ~11%
# populated: pump frames send an integer, bonk frames send 999999999.990125,
# and converting tokens to base units exactly needs `decimals`, which this
# feed does not carry. A null is honest; a rounded integer would not be.
