"""Minimal Borsh reader, driven by Anchor IDL type descriptions.

Supports exactly the types the vendored IDLs actually use. Anything else
raises. This is deliberate: silently skipping an unknown type would shift
every subsequent field offset and produce plausible garbage, which is the
CLAUDE.md 3.8.8 failure mode.
"""
from __future__ import annotations

import base58


class BorshError(ValueError):
    pass


class Reader:
    __slots__ = ("_buf", "_pos")

    def __init__(self, buf: bytes) -> None:
        self._buf = buf
        self._pos = 0

    @property
    def remaining(self) -> int:
        return len(self._buf) - self._pos

    def _take(self, n: int) -> bytes:
        if n < 0:
            raise BorshError(f"negative read length {n}")
        if self.remaining < n:
            raise BorshError(f"need {n} bytes, {self.remaining} remain at offset {self._pos}")
        chunk = self._buf[self._pos : self._pos + n]
        self._pos += n
        return chunk

    def u8(self) -> int:
        return self._take(1)[0]

    def boolean(self) -> bool:
        v = self.u8()
        if v > 1:
            raise BorshError(f"bool byte was {v}, expected 0 or 1")
        return v == 1

    def uint(self, size: int) -> int:
        return int.from_bytes(self._take(size), "little", signed=False)

    def sint(self, size: int) -> int:
        return int.from_bytes(self._take(size), "little", signed=True)

    def string(self) -> str:
        length = self.uint(4)
        return self._take(length).decode("utf-8", errors="replace")

    def pubkey(self) -> str:
        return base58.b58encode(self._take(32)).decode("ascii")


_UINTS = {"u8": 1, "u16": 2, "u32": 4, "u64": 8, "u128": 16, "u256": 32}
_SINTS = {"i8": 1, "i16": 2, "i32": 4, "i64": 8, "i128": 16, "i256": 32}


def read_type(reader: Reader, ty: object) -> object:
    """Read one value described by an Anchor IDL `type` node."""
    if isinstance(ty, str):
        if ty == "bool":
            return reader.boolean()
        if ty == "string":
            return reader.string()
        if ty in ("pubkey", "publicKey"):
            return reader.pubkey()
        if ty == "bytes":
            return reader._take(reader.uint(4)).hex()
        if ty in _UINTS:
            return reader.uint(_UINTS[ty])
        if ty in _SINTS:
            return reader.sint(_SINTS[ty])
        raise BorshError(f"unsupported IDL primitive {ty!r}")

    if isinstance(ty, dict):
        if "option" in ty:
            return read_type(reader, ty["option"]) if reader.boolean() else None
        if "vec" in ty:
            count = reader.uint(4)
            return [read_type(reader, ty["vec"]) for _ in range(count)]
        if "array" in ty:
            inner, count = ty["array"]
            return [read_type(reader, inner) for _ in range(count)]
        if "defined" in ty:
            raise BorshError(
                f"IDL type {ty['defined']!r} is a user-defined struct; add an explicit "
                "layout for it rather than guessing at its fields"
            )
    raise BorshError(f"unrecognised IDL type node: {ty!r}")


def decode_struct(data: bytes, fields: list[dict], *, strict_length: bool = False) -> dict:
    """Decode `data` as a Borsh struct described by IDL `fields`.

    strict_length=True rejects trailing bytes. Left False by default because
    Anchor appends nothing today but adding a field upstream would otherwise
    turn every event into a hard failure rather than a partial decode plus a
    visible IDL diff.
    """
    reader = Reader(data)
    out: dict[str, object] = {}
    for f in fields:
        out[f["name"]] = read_type(reader, f["type"])
    if strict_length and reader.remaining:
        raise BorshError(f"{reader.remaining} trailing bytes after struct")
    out["_trailing_bytes"] = reader.remaining
    return out
