"""Yellowstone gRPC consumer -- NOT on the week-1 path.

This costs money and 3.2 says do not buy until an upgrade trigger fires. Left
here complete because the request shape is verified against the real
geyser.proto and re-deriving it later would be waste.


Filter design follows CLAUDE.md 3.3. Two subscriptions are declared as
separately-named transaction filters inside one request so that every update
carries the names it matched, which is what makes the strict-vs-naive diff a
measurement rather than a memory:

  naive  = account_include[pump]
  strict = account_include[pump] AND account_required[metaplex metadata]

`account_required` is AND across its array while every other field is OR. A
`create` CPIs into Metaplex Token Metadata; `buy`/`sell` never do. Requiring
the metadata program therefore pushes discrimination server-side instead of
discarding ~99% of traffic client-side.

Run FILTER_MODE=both for at least an hour and diff the detected mints before
trusting `strict` alone -- pump.fun's create account list has changed before.
"""
from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import AsyncIterator

import base58
import grpc

from ...decode import idl
from ...decode.pumpfun import CREATE_EVENT, DecodeError, parse_create_events
from ...pb import geyser_pb2 as pb
from ...pb import geyser_pb2_grpc as pb_grpc
from ..base import Consumer, EventKind, RawEvent

#: 3.8.3 -- the default 4MB client limit is below real account and block
#: updates. The symptom of leaving it is a decode error that looks like data
#: corruption rather than a size limit.
MAX_RECV_BYTES = 1024 * 1024 * 1024

NAIVE_FILTER = "naive"
STRICT_FILTER = "strict"
SLOTS_FILTER = "slots"


def _channel_options(keepalive_seconds: int) -> list[tuple[str, int]]:
    ms = keepalive_seconds * 1000
    return [
        ("grpc.max_receive_message_length", MAX_RECV_BYTES),
        ("grpc.max_send_message_length", 64 * 1024 * 1024),
        # Transport-level keepalive. Necessary but NOT sufficient: it keeps the
        # socket warm without proving the data plane is alive. The supervisor's
        # slot watchdog is what actually detects a silent stall.
        ("grpc.keepalive_time_ms", ms),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
        ("grpc.http2.min_time_between_pings_ms", ms),
    ]


class YellowstoneConsumer(Consumer):
    provider = "yellowstone"

    def __init__(
        self,
        endpoint: str,
        token: str = "",
        *,
        tls: bool = True,
        filter_mode: str = "both",
        keepalive_seconds: int = 30,
        commitment: str = "PROCESSED",
        ping_seconds: int = 15,
    ) -> None:
        if "://" in endpoint:
            raise ValueError("endpoint must be host:port with no scheme")
        self._endpoint = endpoint
        self._token = token
        self._tls = tls
        self._filter_mode = filter_mode
        self._keepalive_seconds = keepalive_seconds
        self._commitment = commitment
        self._ping_seconds = ping_seconds
        self._channel: grpc.aio.Channel | None = None
        self._closing = asyncio.Event()

    # -- subscription ----------------------------------------------------
    def _wanted_filters(self) -> tuple[str, ...]:
        if self._filter_mode == "naive":
            return (NAIVE_FILTER,)
        if self._filter_mode == "strict":
            return (STRICT_FILTER,)
        return (NAIVE_FILTER, STRICT_FILTER)

    def build_request(self) -> pb.SubscribeRequest:
        request = pb.SubscribeRequest()
        request.commitment = pb.CommitmentLevel.Value(self._commitment)

        for name in self._wanted_filters():
            tx = request.transactions[name]
            tx.vote = False
            tx.failed = False
            tx.account_include.append(idl.PUMP_PROGRAM_ID)
            if name == STRICT_FILTER:
                tx.account_required.append(idl.METAPLEX_TOKEN_METADATA_PROGRAM_ID)

        # Near-free, and the only source of gap detection (3.3).
        request.slots[SLOTS_FILTER].filter_by_commitment = False

        # Application-level ping, distinct from HTTP/2 keepalive (3.8.1).
        request.ping.id = 1
        return request

    def subscription_descriptor(self) -> dict:
        return {
            "provider": self.provider,
            "endpoint": self._endpoint,
            "filter_mode": self._filter_mode,
            "commitment": self._commitment,
            "keepalive_seconds": self._keepalive_seconds,
            "max_recv_bytes": MAX_RECV_BYTES,
            "filters": {
                NAIVE_FILTER: {"account_include": [idl.PUMP_PROGRAM_ID]},
                STRICT_FILTER: {
                    "account_include": [idl.PUMP_PROGRAM_ID],
                    "account_required": [idl.METAPLEX_TOKEN_METADATA_PROGRAM_ID],
                },
            },
            "slots": True,
        }

    # -- transport -------------------------------------------------------
    def _connect(self) -> grpc.aio.Channel:
        options = _channel_options(self._keepalive_seconds)
        if self._tls:
            return grpc.aio.secure_channel(
                self._endpoint, grpc.ssl_channel_credentials(), options=options
            )
        return grpc.aio.insecure_channel(self._endpoint, options=options)

    async def _requests(self) -> AsyncIterator[pb.SubscribeRequest]:
        """Initial subscribe, then periodic pings on the same stream."""
        yield self.build_request()
        ping_id = 1
        while not self._closing.is_set():
            try:
                await asyncio.wait_for(self._closing.wait(), timeout=self._ping_seconds)
                return
            except TimeoutError:
                ping_id += 1
                keepalive = pb.SubscribeRequest()
                keepalive.ping.id = ping_id
                yield keepalive

    async def stream(self) -> AsyncIterator[RawEvent]:
        self._closing.clear()
        self._channel = self._connect()
        stub = pb_grpc.GeyserStub(self._channel)
        metadata = [("x-token", self._token)] if self._token else None

        async for update in stub.Subscribe(self._requests(), metadata=metadata):
            event = self._to_event(update)
            if event is not None:
                yield event

    async def aclose(self) -> None:
        self._closing.set()
        if self._channel is not None:
            await self._channel.close()
            self._channel = None

    # -- mapping ---------------------------------------------------------
    def _to_event(self, update: pb.SubscribeUpdate) -> RawEvent | None:
        which = update.WhichOneof("update_oneof")
        if which == "slot":
            return RawEvent(
                signature="", slot=update.slot.slot, provider=self.provider,
                event_kind=EventKind.SLOT, commitment=self._commitment.lower(),
            )
        if which in ("ping", "pong"):
            return RawEvent(
                signature="", slot=0, provider=self.provider,
                event_kind=EventKind.PING, commitment=self._commitment.lower(),
            )
        if which != "transaction":
            return None

        tx = update.transaction
        info = tx.transaction
        signature = base58.b58encode(info.signature).decode("ascii")
        logs = list(info.meta.log_messages) if info.meta else []
        matched = list(update.filters)

        kind = EventKind.OTHER
        payload: dict = {
            "filters": matched,
            "is_vote": info.is_vote,
            "index": info.index,
            "log_count": len(logs),
        }

        try:
            creates = parse_create_events(logs)
        except DecodeError as exc:
            # Surfaced, never swallowed: a decode failure here is the 3.8.8
            # canary and must reach stream_health.decode_failures.
            payload["decode_error"] = str(exc)
            creates = []
            kind = EventKind.OTHER
        if creates:
            kind = EventKind.CREATE
            payload["creates"] = [c.raw for c in creates]

        block_time = None
        if update.HasField("created_at"):
            block_time = update.created_at.ToDatetime().replace(tzinfo=dt.UTC)

        return RawEvent(
            signature=signature,
            slot=tx.slot,
            provider=self.provider,
            event_kind=kind,
            commitment=self._commitment.lower(),
            program_id=idl.PUMP_PROGRAM_ID,
            block_time=block_time,
            filter_source=",".join(matched) if matched else None,
            payload=payload,
        )


__all__ = ["CREATE_EVENT", "MAX_RECV_BYTES", "YellowstoneConsumer"]
