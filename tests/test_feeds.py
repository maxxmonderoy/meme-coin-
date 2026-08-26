"""Feed behaviour: the schema hypothesis, the staleness trap, and the race."""
from __future__ import annotations

import datetime as dt

import pytest

from trenches.stream.base import EventKind, RawEvent
from trenches.stream.pumpportal import (
    NEW_TOKEN_SCHEMA,
    SUBSCRIBE_NEW_TOKEN,
    FrameSchemaMismatch,
    PumpPortalConsumer,
    ValidationReport,
    token_fields,
    validate,
)
from trenches.stream.rugcheck_feed import (
    FeedStaleError,
    RugCheckNewTokensConsumer,
    extract_created,
    extract_mint,
    newest_age_seconds,
)

GOOD_FRAME = {
    "signature": "5xSig", "mint": "MintAddr111", "traderPublicKey": "Creator111",
    "txType": "create", "name": "Test", "symbol": "TST", "uri": "https://x/y",
    "initialBuy": 100000000, "solAmount": 1, "bondingCurveKey": "Curve111",
    "vTokensInBondingCurve": 900000000, "vSolInBondingCurve": 31, "marketCapSol": 32.7,
}


# -- the subscribe side is sourced from PumpPortal's own repo ---------------

def test_subscribe_payload_matches_official_docs():
    assert SUBSCRIBE_NEW_TOKEN == {"method": "subscribeNewToken"}


def test_schema_is_marked_unverified():
    """It must stay False until `verify-capture` confirms it against real frames.

    If this test ever fails, someone flipped the flag without running the
    verification -- which is exactly the guess-and-hope this design prevents.
    """
    assert NEW_TOKEN_SCHEMA.verified is False


# -- fail loud, never silently wrong ---------------------------------------

def test_good_frame_maps_to_a_create():
    event = PumpPortalConsumer().map_frame(dict(GOOD_FRAME))
    assert event.event_kind == EventKind.CREATE
    assert event.mint == "MintAddr111"
    assert event.signature == "5xSig"
    assert event.payload["raw"] == GOOD_FRAME


def test_missing_mint_never_yields_a_null_mint_create():
    """The whole point: a wrong schema must not produce a plausible bad row."""
    frame = {k: v for k, v in GOOD_FRAME.items() if k != "mint"}
    event = PumpPortalConsumer().map_frame(frame)
    assert event.event_kind != EventKind.CREATE
    assert event.mint is None
    assert "schema_mismatch" in event.payload
    assert event.payload["raw"] == frame  # raw frame preserved for repair


def test_strict_mode_raises_with_observed_keys():
    frame = {k: v for k, v in GOOD_FRAME.items() if k != "mint"}
    with pytest.raises(FrameSchemaMismatch) as exc:
        PumpPortalConsumer(strict=True).map_frame(frame)
    assert "mint" in str(exc.value)
    assert "signature" in exc.value.observed_keys


def test_validate_rejects_empty_required_value():
    with pytest.raises(FrameSchemaMismatch):
        validate({**GOOD_FRAME, "mint": ""})


def test_subscription_ack_is_a_ping_not_a_mismatch():
    event = PumpPortalConsumer().map_frame({"message": "Successfully subscribed"})
    assert event.event_kind == EventKind.PING


def test_amounts_never_become_floats():
    fields = token_fields(GOOD_FRAME)
    for key in ("initial_buy_base", "virtual_token_reserves"):
        assert isinstance(fields[key], int)
    # marketCapSol is a float in the frame and must not reach an amount column
    assert "marketCapSol" not in fields


def test_validation_report_counts_drift():
    report = ValidationReport()
    report.observe(dict(GOOD_FRAME), NEW_TOKEN_SCHEMA)
    report.observe({**GOOD_FRAME, "brandNewField": 1}, NEW_TOKEN_SCHEMA)
    report.observe({"signature": "x"}, NEW_TOKEN_SCHEMA)
    assert report.frames == 3
    assert report.matched == 2
    assert report.missing_required["mint"] == 1
    assert report.unknown_fields["brandNewField"] == 1


# -- the cached-response trap ----------------------------------------------

def _entry(age_seconds: float) -> dict:
    created = dt.datetime.now(tz=dt.UTC) - dt.timedelta(seconds=age_seconds)
    return {"mint": f"M{age_seconds}", "createAt": created.isoformat()}


def test_newest_age_picks_the_freshest_entry():
    age = newest_age_seconds([_entry(9000), _entry(5), _entry(600)])
    assert 0 <= age < 30


def test_three_hour_old_listing_is_detected_as_stale():
    """The documented failure: no cache-buster, ~3h old entries, no error."""
    age = newest_age_seconds([_entry(10800), _entry(11000)])
    assert age > RugCheckNewTokensConsumer().subscription_descriptor()["startup_max_age_seconds"]


async def test_assert_fresh_refuses_to_start_on_stale_data(monkeypatch):
    consumer = RugCheckNewTokensConsumer()
    monkeypatch.setattr(consumer, "_fetch", lambda: _async([_entry(10800)]))
    with pytest.raises(FeedStaleError, match="cache-buster"):
        await consumer.assert_fresh()


async def test_assert_fresh_passes_on_live_data(monkeypatch):
    consumer = RugCheckNewTokensConsumer()
    monkeypatch.setattr(consumer, "_fetch", lambda: _async([_entry(3)]))
    assert await consumer.assert_fresh() < 30


async def test_assert_fresh_refuses_when_no_timestamp_field_found(monkeypatch):
    """Cannot verify freshness is not the same as fresh."""
    consumer = RugCheckNewTokensConsumer()
    monkeypatch.setattr(consumer, "_fetch", lambda: _async([{"mint": "M1"}]))
    with pytest.raises(FeedStaleError, match="freshness cannot be verified"):
        await consumer.assert_fresh()


async def test_assert_fresh_refuses_on_empty_listing(monkeypatch):
    consumer = RugCheckNewTokensConsumer()
    monkeypatch.setattr(consumer, "_fetch", lambda: _async([]))
    with pytest.raises(FeedStaleError):
        await consumer.assert_fresh()


def test_cache_buster_is_declared_in_the_subscription():
    assert RugCheckNewTokensConsumer().subscription_descriptor()["cache_buster"] is True


def test_mint_and_timestamp_extraction_tolerates_key_variants():
    assert extract_mint({"address": "A1"}) == "A1"
    assert extract_mint({"tokenMint": "A2"}) == "A2"
    assert extract_mint({"nothing": 1}) is None
    assert extract_created({"createAt": 1756085000}) is not None
    assert extract_created({"createAt": 1756085000000}) is not None  # milliseconds
    assert extract_created({"nope": 1}) is None


async def _async(value):
    return value


# -- event identity --------------------------------------------------------

def test_event_id_prefers_signature_then_mint():
    assert RawEvent(provider="p", event_kind="create", mint="M", signature="S").event_id == "S"
    assert RawEvent(provider="p", event_kind="create", mint="M").event_id == "M"


def test_only_create_and_migrate_with_a_mint_are_candidates():
    assert RawEvent(provider="p", event_kind="create", mint="M").is_candidate
    assert RawEvent(provider="p", event_kind="migrate", mint="M").is_candidate
    assert not RawEvent(provider="p", event_kind="create").is_candidate
    assert not RawEvent(provider="p", event_kind="trade", mint="M").is_candidate
    assert not RawEvent(provider="p", event_kind="ping").is_candidate


# -- replay fidelity -------------------------------------------------------

def test_replay_preserves_recorded_arrival_time():
    """Replay must reproduce the inter-feed gap, not flatten it.

    Stamping `now` on replayed events makes every feed-race delta zero, which
    destroys the evidence the race table exists to collect.
    """
    from trenches.stream.replay import ReplayConsumer

    recorded = "2026-08-26T10:00:01.500000+00:00"
    event = ReplayConsumer._to_event({
        "provider": "rugcheck", "event_kind": "create", "mint": "M1",
        "received_at": recorded,
    })
    assert event.received_at == dt.datetime.fromisoformat(recorded)


def test_replay_without_a_recorded_time_falls_back_to_now():
    from trenches.stream.replay import ReplayConsumer

    event = ReplayConsumer._to_event({"provider": "x", "event_kind": "create", "mint": "M"})
    assert (dt.datetime.now(tz=dt.UTC) - event.received_at).total_seconds() < 5
