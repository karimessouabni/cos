"""Tests du choix du point de restauration et du recovery range."""
from datetime import datetime, timezone

import pytest

from cos_service.services import recovery_range_service as svc


def a_range(range_id="r1", start="2026-09-01T00:00:00.000Z", end="2026-09-10T00:00:00.000Z", created=None) -> dict:
    return {
        "recovery_range_id": range_id,
        "range_start_time": start,
        "range_end_time": end,
        "range_create_time": created or start,
        "source_resource_crn": "crn:bucket",
        "backup_policy_name": "bp-7d",
    }


class TestParseTime:
    def test_zulu(self):
        assert svc.parse_time("2026-09-05T10:30:00Z") == datetime(2026, 9, 5, 10, 30, tzinfo=timezone.utc)

    def test_ibm_format_with_milliseconds(self):
        assert svc.parse_time("2026-09-05T10:30:00.250Z").microsecond == 250000

    def test_offset_is_converted_to_utc(self):
        assert svc.parse_time("2026-09-05T12:30:00+02:00") == datetime(2026, 9, 5, 10, 30, tzinfo=timezone.utc)

    def test_naive_is_read_as_utc(self):
        assert svc.parse_time("2026-09-05T10:30:00") == datetime(2026, 9, 5, 10, 30, tzinfo=timezone.utc)

    def test_missing_value(self):
        with pytest.raises(svc.RecoveryRangeError, match="is required"):
            svc.parse_time(None)

    def test_garbage(self):
        with pytest.raises(svc.RecoveryRangeError, match="not a valid ISO 8601"):
            svc.parse_time("hier midi")


def test_format_time_matches_the_ibm_format():
    moment = datetime(2026, 9, 5, 10, 30, 0, 250000, tzinfo=timezone.utc)

    assert svc.format_time(moment) == "2026-09-05T10:30:00.250Z"
    assert svc.parse_time(svc.format_time(moment)) == moment


class TestSelectRecoveryRange:
    def test_point_inside_the_only_range(self):
        point = svc.parse_time("2026-09-05T00:00:00Z")

        assert svc.select_recovery_range([a_range()], point)["recovery_range_id"] == "r1"

    def test_bounds_are_inclusive(self):
        ranges = [a_range()]

        assert svc.select_recovery_range(ranges, svc.parse_time("2026-09-01T00:00:00Z"))["recovery_range_id"] == "r1"
        assert svc.select_recovery_range(ranges, svc.parse_time("2026-09-10T00:00:00Z"))["recovery_range_id"] == "r1"

    def test_point_outside_every_range_is_refused_with_the_windows(self):
        with pytest.raises(svc.RecoveryRangeError) as excinfo:
            svc.select_recovery_range([a_range()], svc.parse_time("2026-09-11T00:00:00Z"))

        assert "not covered by any recovery range" in str(excinfo.value)
        assert "r1 [2026-09-01T00:00:00.000Z -> 2026-09-10T00:00:00.000Z]" in str(excinfo.value)

    def test_no_range_at_all(self):
        with pytest.raises(svc.RecoveryRangeError, match="no recovery range exists"):
            svc.select_recovery_range([], svc.parse_time("2026-09-05T00:00:00Z"))

    def test_the_range_covering_the_point_wins_over_the_most_recent(self):
        """Après un renommage de policy, l'ancien range couvre encore les anciens points."""
        old = a_range("old", "2026-08-01T00:00:00.000Z", "2026-09-10T00:00:00.000Z")
        new = a_range("new", "2026-09-10T00:00:01.000Z", "2026-09-16T00:00:00.000Z")

        assert svc.select_recovery_range([new, old], svc.parse_time("2026-09-01T00:00:00Z"))["recovery_range_id"] == "old"
        assert svc.select_recovery_range([new, old], svc.parse_time("2026-09-15T00:00:00Z"))["recovery_range_id"] == "new"

    def test_overlapping_ranges_pick_the_most_recently_created(self):
        first = a_range("first", "2026-09-01T00:00:00.000Z", "2026-09-10T00:00:00.000Z", created="2026-09-01T00:00:00.000Z")
        second = a_range("second", "2026-09-01T00:00:00.000Z", "2026-09-10T00:00:00.000Z", created="2026-09-02T00:00:00.000Z")

        assert svc.select_recovery_range([first, second], svc.parse_time("2026-09-05T00:00:00Z"))["recovery_range_id"] == "second"

    def test_explicit_range_must_exist(self):
        with pytest.raises(svc.RecoveryRangeError, match="recovery_range_id nope not found"):
            svc.select_recovery_range([a_range()], svc.parse_time("2026-09-05T00:00:00Z"), recovery_range_id="nope")

    def test_explicit_range_must_contain_the_point(self):
        with pytest.raises(svc.RecoveryRangeError, match="is outside recovery range r1"):
            svc.select_recovery_range([a_range()], svc.parse_time("2026-09-20T00:00:00Z"), recovery_range_id="r1")

    def test_explicit_range_containing_the_point(self):
        chosen = svc.select_recovery_range([a_range()], svc.parse_time("2026-09-05T00:00:00Z"), recovery_range_id="r1")

        assert chosen["recovery_range_id"] == "r1"
