"""Contrat du schéma de rétention : format historique (jours implicites) et
format courant (*_days / *_years), conversion en jours, écho pour le state."""
import pytest
from pydantic import ValidationError

from cos_service.schemas.bucket_retention import DAYS, YEARS, BucketRetention, max_retention


def test_legacy_payload_is_mapped_to_days():
    retention = BucketRetention(retention_enabled=True, default=30, minimum=1, maximum=90)

    assert retention.legacy_format is True
    assert retention.unit == DAYS
    assert (retention.default_days, retention.minimum_days, retention.maximum_days) == (30, 1, 90)
    assert [retention.in_days(k) for k in ("default", "minimum", "maximum")] == [30, 1, 90]
    assert retention.as_sent() == {}


def test_days_payload():
    retention = BucketRetention(retention_enabled=True, default_days=30, minimum_days=1, maximum_days=90)

    assert retention.legacy_format is False
    assert retention.unit == DAYS
    assert retention.value("default") == 30
    assert retention.in_days("maximum") == 90
    assert retention.as_sent() == {"default_days": 30, "minimum_days": 1, "maximum_days": 90}


def test_years_payload_is_converted_to_days():
    retention = BucketRetention(retention_enabled=True, default_years=2, minimum_years=1, maximum_years=5)

    assert retention.unit == YEARS
    assert retention.value("default") == 2
    assert retention.in_days("default") == 730
    assert retention.in_days("maximum") == 1825
    assert retention.as_sent() == {"default_years": 2, "minimum_years": 1, "maximum_years": 5}


def test_mixing_legacy_and_suffixed_fields_is_rejected():
    with pytest.raises(ValidationError, match="not both"):
        BucketRetention(retention_enabled=True, default=30, default_days=30, minimum_days=1, maximum_days=90)


def test_mixed_units_have_no_unit():
    retention = BucketRetention(retention_enabled=True, default_days=30, minimum_years=1, maximum_years=5)

    assert retention.unit is None
    assert retention.value("default") is None
    assert retention.in_days("default") is None


def test_partial_legacy_payload_is_still_mapped():
    retention = BucketRetention(default=30)

    assert retention.legacy_format is True
    assert retention.default_days == 30
    assert retention.minimum_days is None


def test_is_empty_looks_at_bounds_only():
    assert BucketRetention().is_empty()
    assert BucketRetention(retention_enabled=False).is_empty()
    assert not BucketRetention(default=30).is_empty()
    assert not BucketRetention(maximum_years=5).is_empty()


def test_max_retention_per_unit():
    assert max_retention(YEARS) == 5
    assert max_retention(DAYS) == 1825


def test_bounds_without_flag_auto_enable_retention():
    assert BucketRetention(default_days=30, minimum_days=1, maximum_days=90).retention_enabled is True
    assert BucketRetention(maximum_years=5).retention_enabled is True
    assert BucketRetention(default=30, minimum=1, maximum=90).retention_enabled is True


def test_explicit_false_flag_is_kept_with_bounds():
    assert BucketRetention(retention_enabled=False, default_days=30).retention_enabled is False


def test_no_bounds_leave_the_flag_untouched():
    assert BucketRetention().retention_enabled is None
    assert BucketRetention(retention_enabled=False).retention_enabled is False
