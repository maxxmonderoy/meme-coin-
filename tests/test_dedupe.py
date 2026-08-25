"""Dedupe is the invariant that stops one detection becoming two buys."""
from __future__ import annotations

import pytest

from trenches.pipeline.dedupe import DedupeCache


def test_first_sight_is_not_a_dupe():
    cache = DedupeCache(ttl_seconds=90)
    assert cache.seen(("sig", 1)) is False
    assert cache.seen(("sig", 1)) is True


def test_same_signature_different_slot_is_distinct():
    """A signature can reappear at a different slot on a reorg. The identity is
    the pair, not the signature."""
    cache = DedupeCache(ttl_seconds=90)
    assert cache.seen(("sig", 1)) is False
    assert cache.seen(("sig", 2)) is False


def test_marks_before_reporting():
    """seen() must mark on the miss path too, or two workers racing the same
    event both get False and both act. Idempotency key before the call (3.6)."""
    cache = DedupeCache(ttl_seconds=90)
    cache.seen(("sig", 1))
    assert len(cache) == 1


def test_entries_expire_after_ttl():
    cache = DedupeCache(ttl_seconds=90)
    assert cache.seen(("sig", 1), now=1000.0) is False
    assert cache.seen(("sig", 1), now=1089.0) is True
    assert cache.seen(("sig", 1), now=1200.0) is False


def test_bounded_by_max_entries():
    cache = DedupeCache(ttl_seconds=9999, max_entries=10)
    for i in range(50):
        cache.seen(("sig", i), now=1000.0 + i)
    assert len(cache) <= 10
    # oldest evicted, newest retained
    assert cache.seen(("sig", 49), now=1050.0) is True


def test_counters_track_hits_and_misses():
    cache = DedupeCache(ttl_seconds=90)
    cache.seen(("a", 1))
    cache.seen(("a", 1))
    cache.seen(("b", 1))
    assert (cache.misses, cache.hits) == (2, 1)


def test_replaying_a_capture_twice_is_fully_deduped():
    """Duplicates are guaranteed on replay and reconnect (3.8.4)."""
    events = [(f"sig{i}", i) for i in range(100)]
    cache = DedupeCache(ttl_seconds=9999, max_entries=1000)
    first = sum(1 for e in events if not cache.seen(e))
    second = sum(1 for e in events if not cache.seen(e))
    assert (first, second) == (100, 0)


def test_ttl_must_be_positive():
    with pytest.raises(ValueError, match="positive"):
        DedupeCache(ttl_seconds=0)
