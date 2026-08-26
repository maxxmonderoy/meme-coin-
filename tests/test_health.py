"""Detection rate is the canary (3.8.8). These guard the counters behind it.

Counters are per-(minute, feed) because the point of running two feeds is
noticing when one of them quietly stops -- which a single combined counter
hides completely.
"""
from __future__ import annotations

import datetime as dt

from trenches.pipeline.health import HealthTracker


def _at(minute: int, second: int = 0) -> dt.datetime:
    return dt.datetime(2026, 8, 26, 12, minute, second, tzinfo=dt.UTC)


def test_events_bucket_by_minute_and_feed():
    h = HealthTracker()
    h.record_event("pumpportal", "create", ts=_at(1, 5))
    h.record_event("pumpportal", "other", ts=_at(1, 59))
    h.record_event("rugcheck", "create", ts=_at(1, 30))
    h.record_event("pumpportal", "create", ts=_at(2, 0))

    pp1 = h.buckets[(_at(1), "pumpportal")]
    assert (pp1.events_total, pp1.events_create, pp1.events_other) == (2, 1, 1)
    assert h.buckets[(_at(1), "rugcheck")].events_create == 1
    assert h.buckets[(_at(2), "pumpportal")].events_create == 1


def test_one_feed_going_silent_is_visible_per_feed():
    """The failure a combined counter hides: total looks fine, one feed is dead."""
    h = HealthTracker()
    for _ in range(10):
        h.record_event("pumpportal", "create", ts=_at(5))
    assert (_at(5), "rugcheck") not in h.buckets
    assert h.buckets[(_at(5), "pumpportal")].events_create == 10


def test_wins_are_tracked_separately_from_events():
    """A feed can deliver plenty of events and never win the race."""
    h = HealthTracker()
    h.record_event("rugcheck", "create", ts=_at(1))
    h.record_event("rugcheck", "create", ts=_at(1))
    h.record_win("rugcheck", ts=_at(1))
    b = h.buckets[(_at(1), "rugcheck")]
    assert (b.events_create, b.wins) == (2, 1)


def test_stale_and_schema_mismatch_are_distinct_counters():
    """Different failures: one is a cached feed, the other a changed payload."""
    h = HealthTracker()
    h.record_stale("rugcheck", ts=_at(1))
    h.record_schema_mismatch("pumpportal", ts=_at(1))
    assert h.buckets[(_at(1), "rugcheck")].stale_responses == 1
    assert h.buckets[(_at(1), "rugcheck")].schema_mismatches == 0
    assert h.buckets[(_at(1), "pumpportal")].schema_mismatches == 1


def test_queue_high_water_is_a_maximum_not_a_sum():
    h = HealthTracker()
    for depth in (10, 4, 7):
        h.record_queue("pumpportal", depth, ts=_at(1))
    assert h.buckets[(_at(1), "pumpportal")].queue_high_water == 10


def test_queue_drops_accumulate():
    h = HealthTracker()
    h.record_queue("pumpportal", 5, dropped=1, ts=_at(1))
    h.record_queue("pumpportal", 5, dropped=2, ts=_at(1))
    assert h.buckets[(_at(1), "pumpportal")].queue_drops == 3


def test_drain_leaves_the_current_minute_open():
    """Closed minutes flush; the one still being filled stays put.

    The clock is injected rather than read, so this does not pass or fail
    depending on what time of day the suite runs.
    """
    h = HealthTracker()
    h.record_event("pumpportal", "create", ts=_at(1))
    h.record_event("pumpportal", "create", ts=_at(2))
    ready = h.drain(now=_at(2, 30))
    assert [key for key, _ in ready] == [(_at(1), "pumpportal")]
    assert len(h.buckets) == 1  # the in-progress minute survives
    assert (_at(2), "pumpportal") in h.buckets


def test_drain_is_idempotent_on_already_flushed_minutes():
    h = HealthTracker()
    h.record_event("pumpportal", "create", ts=_at(1))
    assert len(h.drain(now=_at(3))) == 1
    assert h.drain(now=_at(3)) == []
