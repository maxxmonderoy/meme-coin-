from __future__ import annotations

import os
import struct
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import base58

from trenches.decode import idl

#: Override with TRENCHES_TEST_DSN. Tests that need a database SKIP rather
#: than fail when none is reachable -- but a skipped schema test proves
#: nothing, so check the summary for `s` before trusting a green run.
TEST_DSN = os.environ.get(
    "TRENCHES_TEST_DSN", "postgresql://trenches@127.0.0.1:5433/trenches"
)


def pubkey(seed: int) -> str:
    return base58.b58encode(bytes([seed]) * 32).decode()


def encode_create_event(**overrides) -> bytes:
    """Borsh-encode a CreateEvent using the layout from the vendored IDL.

    The layout is read from the IDL rather than restated here, so this fixture
    cannot drift away from the decoder it is testing while still passing.
    """
    values = {
        "name": "Test Coin", "symbol": "TEST", "uri": "https://example/meta.json",
        "mint": pubkey(1), "bonding_curve": pubkey(2), "user": pubkey(3),
        "creator": pubkey(4), "timestamp": 1756085000,
        "virtual_token_reserves": 1_073_000_000_000_000,
        "virtual_sol_reserves": 30_000_000_000,
        "real_token_reserves": 793_100_000_000_000,
        "token_total_supply": 1_000_000_000_000_000,
        "token_program": pubkey(5), "is_mayhem_mode": False,
        "is_cashback_enabled": True, "quote_mint": pubkey(6),
        "virtual_quote_reserves": 0,
    }
    values.update(overrides)

    out = b""
    for field in idl.event_fields("pump", "CreateEvent"):
        ty, value = field["type"], values[field["name"]]
        if ty == "string":
            raw = value.encode()
            out += struct.pack("<I", len(raw)) + raw
        elif ty == "pubkey":
            out += base58.b58decode(value)
        elif ty == "u64":
            out += struct.pack("<Q", value)
        elif ty == "i64":
            out += struct.pack("<q", value)
        elif ty == "bool":
            out += bytes([1 if value else 0])
        else:  # pragma: no cover - guards against a new IDL primitive
            raise AssertionError(f"fixture cannot encode IDL type {ty!r}")
    return idl.event_discriminators("pump")["CreateEvent"] + out


def create_log_line(**overrides) -> str:
    import base64
    return "Program data: " + base64.b64encode(encode_create_event(**overrides)).decode()


@pytest.fixture
def create_logs():
    return lambda **kw: [
        "Program log: Instruction: Create",
        create_log_line(**kw),
        "Program log: Program consumed 12345 compute units",
    ]
