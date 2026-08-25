"""The decoder must be exactly right or loudly wrong. Never quietly plausible."""
from __future__ import annotations

import base64

import pytest
from conftest import create_log_line, encode_create_event, pubkey

from trenches.decode.borsh import BorshError, Reader, decode_struct
from trenches.decode.pumpfun import DecodeError, parse_create_events


def test_round_trip_exact(create_logs):
    event, = parse_create_events(create_logs())
    assert event.mint == pubkey(1)
    assert event.signer == pubkey(3)
    assert event.declared_creator == pubkey(4)
    assert event.symbol == "TEST"
    assert event.token_total_supply == 1_000_000_000_000_000
    assert event.raw["_trailing_bytes"] == 0


def test_amounts_are_integers_not_floats(create_logs):
    """No float may ever touch an amount path (3.10)."""
    event, = parse_create_events(create_logs())
    for value in (
        event.token_total_supply, event.virtual_sol_reserves,
        event.virtual_token_reserves, event.real_token_reserves,
    ):
        assert isinstance(value, int)
        assert not isinstance(value, bool)


def test_signer_and_declared_creator_are_distinguished(create_logs):
    """CreateEvent carries `user` and `creator` separately and they need not
    match. A reputation cache keyed on one alone is bypassed by rotating the
    other, so the decoder must never collapse them."""
    rotated, = parse_create_events(create_logs())
    assert rotated.creator_rotated is True

    same = pubkey(9)
    event, = parse_create_events([create_log_line(user=same, creator=same)])
    assert event.signer == event.declared_creator == same
    assert event.creator_rotated is False


def test_truncated_payload_raises_rather_than_guessing():
    blob = encode_create_event()[:60]
    line = "Program data: " + base64.b64encode(blob).decode()
    with pytest.raises(DecodeError):
        parse_create_events([line])


def test_foreign_discriminator_is_ignored():
    blob = bytes(8) + b"\x00" * 64
    line = "Program data: " + base64.b64encode(blob).decode()
    assert parse_create_events([line]) == []


def test_non_program_data_lines_ignored():
    assert parse_create_events(["Program log: hello", "", "Program data: !!!not-base64"]) == []


def test_multiple_creates_in_one_transaction():
    logs = [create_log_line(mint=pubkey(11)), create_log_line(mint=pubkey(12))]
    assert [e.mint for e in parse_create_events(logs)] == [pubkey(11), pubkey(12)]


def test_borsh_rejects_unknown_primitive():
    with pytest.raises(BorshError, match="unsupported IDL primitive"):
        decode_struct(b"\x00" * 8, [{"name": "x", "type": "f64"}])


def test_borsh_rejects_defined_struct():
    with pytest.raises(BorshError, match="user-defined struct"):
        decode_struct(b"\x00" * 8, [{"name": "x", "type": {"defined": "Foo"}}])


def test_borsh_bool_must_be_zero_or_one():
    with pytest.raises(BorshError, match="bool byte"):
        Reader(b"\x07").boolean()


def test_borsh_string_length_beyond_buffer():
    with pytest.raises(BorshError, match="need"):
        decode_struct(b"\xff\xff\xff\xff", [{"name": "s", "type": "string"}])
