"""pump.fun create detection.

Anchor emits events as base64 `Program data:` log lines: an 8-byte event
discriminator followed by the Borsh-encoded struct. Both the discriminator and
the field layout come from the vendored IDL, so an upstream layout change shows
up as an idl-check diff rather than as silently misaligned fields.
"""
from __future__ import annotations

import base64
import binascii
import datetime as dt
from dataclasses import dataclass

from . import idl
from .borsh import BorshError, decode_struct

PROGRAM_DATA_PREFIX = "Program data: "
CREATE_EVENT = "CreateEvent"


class DecodeError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CreateEvent:
    mint: str
    signer: str            # CreateEvent.user -- pays and signs
    declared_creator: str  # CreateEvent.creator -- fee recipient, need not match signer
    bonding_curve: str
    name: str
    symbol: str
    uri: str
    token_program: str | None
    quote_mint: str | None
    token_total_supply: int | None
    virtual_sol_reserves: int | None
    virtual_token_reserves: int | None
    real_token_reserves: int | None
    virtual_quote_reserves: int | None
    is_mayhem_mode: bool | None
    is_cashback_enabled: bool | None
    block_time: dt.datetime | None
    raw: dict

    @property
    def creator_rotated(self) -> bool:
        """True when the declared creator is not the signer.

        Relevant to the stage-2 reputation cache: an operator who rotates the
        declared creator while signing from one wallet (or the reverse) defeats
        a cache keyed on either address alone.
        """
        return self.signer != self.declared_creator


def _decode_program_data(line: str) -> bytes | None:
    if not line.startswith(PROGRAM_DATA_PREFIX):
        return None
    blob = line[len(PROGRAM_DATA_PREFIX) :].strip()
    try:
        return base64.b64decode(blob, validate=True)
    except (binascii.Error, ValueError):
        return None


def parse_create_events(logs: list[str]) -> list[CreateEvent]:
    """Extract every CreateEvent from a transaction's log messages."""
    want = idl.event_discriminators("pump")[CREATE_EVENT]
    fields = list(idl.event_fields("pump", CREATE_EVENT))
    out: list[CreateEvent] = []

    for line in logs:
        data = _decode_program_data(line)
        if data is None or len(data) < 8 or data[:8] != want:
            continue
        try:
            decoded = decode_struct(data[8:], fields)
        except BorshError as exc:
            raise DecodeError(f"CreateEvent failed to decode: {exc}") from exc
        out.append(_to_event(decoded))
    return out


def _ts(value: object) -> dt.datetime | None:
    if not isinstance(value, int) or value <= 0:
        return None
    try:
        return dt.datetime.fromtimestamp(value, tz=dt.UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _to_event(d: dict) -> CreateEvent:
    missing = [k for k in ("mint", "user", "creator", "bonding_curve") if k not in d]
    if missing:
        raise DecodeError(
            f"CreateEvent decoded without required field(s) {missing}. The vendored IDL "
            "no longer matches the on-chain program; run `trenches idl-check`."
        )
    return CreateEvent(
        mint=d["mint"],
        signer=d["user"],
        declared_creator=d["creator"],
        bonding_curve=d["bonding_curve"],
        name=d.get("name", ""),
        symbol=d.get("symbol", ""),
        uri=d.get("uri", ""),
        token_program=d.get("token_program"),
        quote_mint=d.get("quote_mint"),
        token_total_supply=d.get("token_total_supply"),
        virtual_sol_reserves=d.get("virtual_sol_reserves"),
        virtual_token_reserves=d.get("virtual_token_reserves"),
        real_token_reserves=d.get("real_token_reserves"),
        virtual_quote_reserves=d.get("virtual_quote_reserves"),
        is_mayhem_mode=d.get("is_mayhem_mode"),
        is_cashback_enabled=d.get("is_cashback_enabled"),
        block_time=_ts(d.get("timestamp")),
        raw=d,
    )
