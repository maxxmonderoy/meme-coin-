"""Detection rate is the canary. These guard the counters it is computed from."""
from __future__ import annotations

import datetime as dt

from trenches.pipeline.health import HealthTracker


def _at(minute: int, second: int = 0) -> dt.datetime:
    return dt.datetime(2026, 8, 25, 12, minute, second, tzinfo=dt.UTC)


def test_events_bucket_by_minute():
    h = HealthTracker()
    h.record_event("create", ts=_at(1, 5))
    h.record_event("other", ts=_at(1, 59))
    h.record_event("create", ts=_at(2, 0))
    assert h.buckets[_at(1)].events_total == 2
    assert h.buckets[_at(1)].events_create == 1
    assert h.buckets[_at(1)].events_other == 1
    assert h.buckets[_at(2)].events_create == 1


def test_slot_gap_detected_and_sized():
    h = HealthTracker()
    assert h.observe_slot(100, ts=_at(1)) == 0     # first slot establishes the baseline
    assert h.observe_slot(101, ts=_at(1)) == 0
    assert h.observe_slot(105, ts=_at(1)) == 3     # 102, 103, 104 missing
    assert h.buckets[_at(1)].slot_gaps == 3


def test_out_of_order_slot_does_not_create_a_negative_gap():
    h = HealthTracker()
    h.observe_slot(100, ts=_at(1))
    assert h.observe_slot(98, ts=_at(1)) == 0
    assert h.last_slot == 100


def test_queue_high_water_is_a_maximum_not_a_sum():
    h = HealthTracker()
    h.record_queue(10, ts=_at(1))
    h.record_queue(4, ts=_at(1))
    h.record_queue(7, ts=_at(1))
    assert h.buckets[_at(1)].queue_high_water == 10


def test_drain_leaves_the_current_minute_open():
    h = HealthTracker()
    h.record_event("create", ts=_at(1))
    h.record_event("create")  # now
    ready = h.drain()
    assert [m for m, _ in ready] == [_at(1)]
    assert len(h.buckets) == 1  # the in-progress minute survives
