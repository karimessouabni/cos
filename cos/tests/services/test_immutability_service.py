"""Tests du service d'immutabilité : logique pure, sans Airflow ni base."""
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from cos_service.schemas.bucket_backup import BucketBackup
from cos_service.schemas.bucket_retention import DAYS, YEARS, BucketRetention
from cos_service.schemas.immutability import Immutability
from cos_service.services import immutability_service as svc


# --- helpers ------------------------------------------------------------------

def fresh_immutability(versioning: bool = False) -> dict:
    """Bloc immutability vierge, tel que le DAG de création le construit."""
    return {
        "object_locking_enabled": False,
        "object_versioning_enabled": versioning,
        "object_lock_duration_days": None,
        "object_lock_duration_years": None,
        "retention": {"retention_enabled": False, "default": None, "minimum": None, "maximum": None},
        "backup": {"backup_enabled": False, "backup_vault_sub_id": None, "backup_retention_days": None},
    }


def retention_days(default=30, minimum=10, maximum=60, enabled=True) -> BucketRetention:
    return BucketRetention(
        retention_enabled=enabled, default_days=default, minimum_days=minimum, maximum_days=maximum
    )


def retention_years(default=1, minimum=1, maximum=2, enabled=True) -> BucketRetention:
    return BucketRetention(
        retention_enabled=enabled, default_years=default, minimum_years=minimum, maximum_years=maximum
    )


def backup_enabled(vault_name="vault-a", retention_days=7, sub_id="bv-sub") -> BucketBackup:
    return BucketBackup(
        backup_enabled=True,
        backup_vault_name=vault_name,
        backup_retention_days=retention_days,
        backup_vault_sub_id=sub_id,
    )


def compute(choice=Immutability.NONE, retention=None, days=None, years=None,
            versioning=False, backup=None, immutability=None) -> dict:
    return svc.compute_bucket_new_immutability(
        choice, retention, days, years, versioning,
        immutability if immutability is not None else fresh_immutability(versioning),
        backup,
    )


def bucket_row(**overrides) -> dict:
    row = {
        "object_lock_duration_days": None,
        "object_lock_duration_years": None,
        "object_versioning_enabled": False,
        "retention_enabled": False,
        "retention_default": None,
        "retention_minimum": None,
        "retention_maximum": None,
        "backup_enabled": False,
        "backup_vault_subscription_id": None,
        "backup_retention_days": None,
    }
    row.update(overrides)
    return row


# --- compute_bucket_new_immutability : choix NONE / absent ----------------------

class TestNewImmutabilityNone:
    def test_nothing_requested_only_applies_versioning(self):
        result = compute(versioning=True)

        assert result["object_versioning_enabled"] is True
        assert result["object_locking_enabled"] is False
        assert result["retention"]["retention_enabled"] is False
        assert result["backup"]["backup_enabled"] is False
        assert result["immutability_choice"] == Immutability.NONE.value

    def test_missing_choice_is_treated_as_none(self):
        result = compute(choice=None, versioning=False)

        assert result["immutability_choice"] == Immutability.NONE.value

    def test_retention_and_object_lock_together_are_declined(self):
        with pytest.raises(DeclineDemandException, match="not compatible"):
            compute(retention=retention_days(), days=30)


# --- rétention ------------------------------------------------------------------

class TestNewImmutabilityRetention:
    def test_retention_in_days_is_inferred_and_stored(self):
        result = compute(retention=retention_days(30, 10, 60))

        assert result["immutability_choice"] == Immutability.RETENTION.value
        assert result["retention"] == {
            "retention_enabled": True, "default": 30, "minimum": 10, "maximum": 60
        }
        assert result["object_locking_enabled"] is False

    def test_retention_in_years_is_converted_to_days(self):
        result = compute(retention=retention_years(1, 1, 2))

        assert result["retention"]["default"] == 365
        assert result["retention"]["minimum"] == 365
        assert result["retention"]["maximum"] == 730

    def test_retention_with_versioning_is_declined(self):
        with pytest.raises(DeclineDemandException, match="Retention and versioning"):
            compute(retention=retention_days(), versioning=True)

    def test_retention_with_backup_is_declined(self):
        with pytest.raises(DeclineDemandException, match="Retention and bucket backup"):
            compute(retention=retention_days(), backup=backup_enabled())

    def test_explicit_retention_choice_with_disabled_flag_is_declined(self):
        with pytest.raises(DeclineDemandException, match="Retention must not be empty"):
            compute(choice=Immutability.RETENTION_DAILY, retention=retention_days(enabled=False))

    def test_explicit_retention_choice_with_empty_retention_is_declined(self):
        with pytest.raises(DeclineDemandException, match="must not be empty for choice"):
            compute(choice=Immutability.RETENTION_YEARLY, retention=BucketRetention())

    def test_daily_choice_requires_day_values(self):
        with pytest.raises(DeclineDemandException, match="must be set in days"):
            compute(choice=Immutability.RETENTION_DAILY, retention=retention_years())

    def test_yearly_choice_drops_day_values_and_keeps_years(self):
        retention = BucketRetention(
            retention_enabled=True,
            default_days=30, minimum_days=10, maximum_days=60,
            default_years=1, minimum_years=1, maximum_years=2,
        )

        result = compute(choice=Immutability.RETENTION_YEARLY, retention=retention)

        assert retention.default_days is None
        assert result["retention"]["default"] == 365
        assert result["immutability_choice"] == Immutability.RETENTION_YEARLY.value

    def test_inconsistent_bounds_are_declined_on_create(self):
        with pytest.raises(DeclineDemandException, match="Retention default"):
            compute(retention=retention_days(default=100, minimum=10, maximum=50))

    def test_bounds_are_checked_in_the_given_unit(self):
        with pytest.raises(DeclineDemandException, match="superior to 5 years"):
            compute(retention=retention_years(default=2, minimum=1, maximum=6))
        with pytest.raises(DeclineDemandException, match="1825 days"):
            compute(retention=retention_days(default=30, minimum=10, maximum=2000))


# --- object lock ------------------------------------------------------------------

class TestNewImmutabilityObjectLock:
    def test_object_lock_in_days_forces_versioning(self):
        result = compute(days=30, versioning=True)

        assert result["immutability_choice"] == Immutability.OBJECT_LOCK.value
        assert result["object_locking_enabled"] is True
        assert result["object_versioning_enabled"] is True
        assert result["object_lock_duration_days"] == 30
        assert result["object_lock_duration_years"] is None

    def test_object_lock_in_years_leaves_days_empty(self):
        result = compute(years=2, versioning=True)

        assert result["object_lock_duration_days"] is None
        assert result["object_lock_duration_years"] == 2

    def test_object_lock_without_versioning_is_declined(self):
        with pytest.raises(DeclineDemandException, match="Versioning should be enabled"):
            compute(days=30, versioning=False)

    def test_object_lock_zero_is_declined(self):
        with pytest.raises(DeclineDemandException, match="inferior or equal to ZERO"):
            compute(days=0, versioning=True)

    def test_object_lock_above_five_years_is_declined(self):
        with pytest.raises(DeclineDemandException, match="cannot be superior"):
            compute(years=6, versioning=True)

    def test_object_lock_in_both_units_is_declined(self):
        with pytest.raises(DeclineDemandException, match="not both"):
            compute(choice=Immutability.OBJECT_LOCK, days=30, years=1, versioning=True)

    def test_yearly_choice_requires_years(self):
        with pytest.raises(DeclineDemandException, match="must be set in years"):
            compute(choice=Immutability.OBJECT_LOCK_YEARLY, days=30, versioning=True)

    def test_yearly_choice_ignores_days(self):
        result = compute(choice=Immutability.OBJECT_LOCK_YEARLY, days=30, years=2, versioning=True)

        assert result["object_lock_duration_days"] is None
        assert result["object_lock_duration_years"] == 2
        assert result["immutability_choice"] == Immutability.OBJECT_LOCK_YEARLY.value

    def test_explicit_object_lock_choice_without_duration_is_declined(self):
        with pytest.raises(DeclineDemandException, match="must be not empty"):
            compute(choice=Immutability.OBJECT_LOCK, versioning=True)


# --- backup ------------------------------------------------------------------------

class TestNewImmutabilityBackup:
    def test_backup_with_versioning_is_stored(self):
        result = compute(versioning=True, backup=backup_enabled(sub_id="bv-sub", retention_days=7))

        assert result["backup"] == {
            "backup_enabled": True, "backup_vault_sub_id": "bv-sub", "backup_retention_days": 7
        }

    def test_backup_without_versioning_is_declined(self):
        with pytest.raises(DeclineDemandException, match="Versioning should be enabled to enable bucket backup"):
            compute(versioning=False, backup=backup_enabled())

    def test_backup_missing_retention_days_is_declined(self):
        with pytest.raises(DeclineDemandException, match="not fully set"):
            compute(versioning=True, backup=backup_enabled(retention_days=None))

    def test_backup_with_object_lock_is_allowed(self):
        result = compute(days=30, versioning=True, backup=backup_enabled())

        assert result["object_locking_enabled"] is True
        assert result["backup"]["backup_enabled"] is True

    def test_explicitly_disabled_backup_without_versioning_is_declined_by_the_service(self):
        """Comportement du service que le DAG de création contourne en ne lui
        passant jamais un backup désactivé."""
        disabled = BucketBackup(backup_enabled=False)

        with pytest.raises(DeclineDemandException, match="to disable bucket backup"):
            compute(versioning=False, backup=disabled)


# --- validations unitaires -----------------------------------------------------------

class TestCheckRetentionBounds:
    def test_consistent_bounds_give_no_error(self):
        errors = []
        svc.check_retention_bounds(DAYS, 30, 10, 60, errors)
        assert errors == []

    def test_minimum_zero_is_an_error(self):
        errors = []
        svc.check_retention_bounds(DAYS, 30, 0, 60, errors)
        assert any("minimum" in e for e in errors)

    def test_default_not_below_maximum_is_an_error(self):
        errors = []
        svc.check_retention_bounds(DAYS, 60, 10, 60, errors)
        assert any("default" in e for e in errors)

    def test_maximum_above_limit_is_an_error(self):
        errors = []
        svc.check_retention_bounds(DAYS, 30, 10, 1826, errors)
        assert any("maximum" in e for e in errors)


class TestCheckSingleUnit:
    def test_days_only(self):
        assert svc.check_single_unit(30, None, "x", "d", "y", []) == DAYS

    def test_years_only(self):
        assert svc.check_single_unit(None, 2, "x", "d", "y", []) == YEARS

    def test_nothing(self):
        assert svc.check_single_unit(None, None, "x", "d", "y", []) is None

    def test_both_is_an_error(self):
        errors = []
        assert svc.check_single_unit(30, 2, "x", "d", "y", errors) is None
        assert errors and "not both" in errors[0]


class TestFormatLimit:
    def test_years(self):
        assert svc.format_limit(YEARS) == "5 years"

    def test_days_mentions_the_day_equivalent(self):
        assert svc.format_limit(DAYS) == "5 years (1825 days)"


# --- update : reconstruction depuis la ligne bucket ----------------------------------

class TestComputeBucketImmutabilityChoice:
    def test_object_lock_wins(self):
        assert svc.compute_bucket_immutability_choice(
            bucket_row(object_lock_duration_days=30, retention_enabled=True)
        ) is Immutability.OBJECT_LOCK

    def test_retention(self):
        assert svc.compute_bucket_immutability_choice(bucket_row(retention_enabled=True)) is Immutability.RETENTION

    def test_none(self):
        assert svc.compute_bucket_immutability_choice(bucket_row()) is Immutability.NONE


class TestComputeBucketImmutabilityForUpdate:
    def test_plain_bucket(self):
        result = svc.compute_bucket_immutability_for_update_bucket(bucket_row(), session=None)

        assert result["immutability_choice"] == Immutability.NONE.value
        assert result["object_locking_enabled"] is False
        assert result["retention"] == {
            "retention_enabled": False, "default": None, "minimum": None, "maximum": None
        }
        assert result["backup"]["backup_enabled"] is False

    def test_both_lock_units_keeps_days(self, caplog):
        row = bucket_row(object_lock_duration_days=30, object_lock_duration_years=1)

        result = svc.compute_bucket_immutability_for_update_bucket(row, session=None)

        assert result["object_lock_duration_days"] == 30
        assert result["object_lock_duration_years"] is None
        assert "both units" in caplog.text

    def test_retention_values_are_read_when_enabled(self):
        row = bucket_row(retention_enabled=True, retention_default=30, retention_minimum=10, retention_maximum=60)

        result = svc.compute_bucket_immutability_for_update_bucket(row, session=None)

        assert result["immutability_choice"] == Immutability.RETENTION.value
        assert result["retention"] == {
            "retention_enabled": True, "default": 30, "minimum": 10, "maximum": 60
        }

    def test_backup_vault_id_is_read_from_the_relation(self):
        """to_dict() expose la relation backup_vault, pas la colonne FK."""
        row = bucket_row(backup_enabled=True, backup_retention_days=7, backup_vault={"subscription_id": "bv-sub", "crn": "crn:bv"})
        del row["backup_vault_subscription_id"]

        result = svc.compute_bucket_immutability_for_update_bucket(row, session=None)

        assert result["backup"] == {
            "backup_enabled": True, "backup_vault_sub_id": "bv-sub", "backup_retention_days": 7
        }

    def test_backup_vault_id_falls_back_to_the_column(self):
        row = bucket_row(backup_enabled=True, backup_vault_subscription_id="bv-col", backup_retention_days=7)

        result = svc.compute_bucket_immutability_for_update_bucket(row, session=None)

        assert result["backup"]["backup_vault_sub_id"] == "bv-col"

    def test_backup_without_vault_is_kept_with_a_warning(self, caplog):
        row = bucket_row(backup_enabled=True, backup_retention_days=7)

        result = svc.compute_bucket_immutability_for_update_bucket(row, session=None)

        assert result["backup"]["backup_vault_sub_id"] is None
        assert "no backup vault attached" in caplog.text


class TestValidateImmutabilityForUpdate:
    def validate(self, row, retention=None, days=None, years=None, versioning=None,
                 has_contents=False, backup=None):
        return svc.validate_immutability_for_update_bucket(
            row, retention, days, years, versioning, has_contents, backup, session=None
        )

    def test_plain_bucket_only_toggles_versioning(self):
        result = self.validate(bucket_row(), versioning=True)

        assert result["object_versioning_enabled"] is True
        assert result["immutability_choice"] == Immutability.NONE.value

    def test_object_locked_bucket_refuses_retention(self):
        with pytest.raises(DeclineDemandException, match="object-lock is already enabled"):
            self.validate(bucket_row(object_lock_duration_days=30, object_versioning_enabled=True),
                          retention=retention_days())

    def test_object_locked_bucket_refuses_disabling_versioning(self):
        with pytest.raises(DeclineDemandException, match="Disabling versioning"):
            self.validate(bucket_row(object_lock_duration_days=30, object_versioning_enabled=True),
                          versioning=False)

    def test_object_locked_bucket_replaces_the_duration(self):
        result = self.validate(bucket_row(object_lock_duration_days=30, object_versioning_enabled=True), years=2)

        assert result["object_lock_duration_days"] is None
        assert result["object_lock_duration_years"] == 2

    def test_object_locked_bucket_keeps_its_duration_when_nothing_is_sent(self):
        result = self.validate(bucket_row(object_lock_duration_days=30, object_versioning_enabled=True))

        assert result["object_lock_duration_days"] == 30

    def test_retention_bucket_with_contents_is_declined(self):
        row = bucket_row(retention_enabled=True, retention_default=30, retention_minimum=10, retention_maximum=60)

        with pytest.raises(DeclineDemandException, match="already contains objects"):
            self.validate(row, has_contents=True)

    def test_retention_bucket_refuses_object_lock(self):
        row = bucket_row(retention_enabled=True, retention_default=30, retention_minimum=10, retention_maximum=60)

        with pytest.raises(DeclineDemandException, match="retention is already enabled"):
            self.validate(row, days=30)

    def test_retention_bucket_updates_its_values(self):
        row = bucket_row(retention_enabled=True, retention_default=30, retention_minimum=10, retention_maximum=60)

        result = self.validate(row, retention=retention_days(40, 20, 80))

        assert result["retention"] == {
            "retention_enabled": True, "default": 40, "minimum": 20, "maximum": 80
        }

    def test_plain_bucket_can_receive_a_backup(self):
        result = self.validate(bucket_row(object_versioning_enabled=True), backup=backup_enabled())

        assert result["backup"]["backup_enabled"] is True


# --- compatibilité : format historique de rétention (jours implicites) ---------------

def retention_legacy(default=30, minimum=10, maximum=60, enabled=True) -> BucketRetention:
    return BucketRetention(retention_enabled=enabled, default=default, minimum=minimum, maximum=maximum)


class TestLegacyRetentionPayload:
    """Un client existant envoie default/minimum/maximum sans unité : même résultat qu'avant."""

    EXPECTED = {"retention_enabled": True, "default": 30, "minimum": 10, "maximum": 60}

    def test_inferred_choice(self):
        result = compute(retention=retention_legacy())

        assert result["retention"] == self.EXPECTED
        assert result["immutability_choice"] == Immutability.RETENTION.value

    def test_legacy_choice(self):
        result = compute(choice=Immutability.RETENTION, retention=retention_legacy())

        assert result["retention"] == self.EXPECTED

    def test_daily_choice_accepts_the_legacy_format(self):
        result = compute(choice=Immutability.RETENTION_DAILY, retention=retention_legacy())

        assert result["retention"] == self.EXPECTED

    def test_yearly_choice_still_requires_years(self):
        with pytest.raises(DeclineDemandException, match="must be set in years"):
            compute(choice=Immutability.RETENTION_YEARLY, retention=retention_legacy())

    def test_same_result_as_the_days_format(self):
        assert compute(retention=retention_legacy())["retention"] == compute(retention=retention_days(30, 10, 60))["retention"]

    def test_legacy_bounds_are_checked_in_days(self):
        with pytest.raises(DeclineDemandException, match="1825 days"):
            compute(retention=retention_legacy(maximum=2000))

    def test_bounds_without_flag_create_a_retention(self):
        result = compute(retention=BucketRetention(default_days=30, minimum_days=10, maximum_days=60))

        assert result["retention"] == self.EXPECTED
        assert result["immutability_choice"] == Immutability.RETENTION.value

    def test_update_with_a_single_bound_and_no_flag_is_applied(self):
        bucket = bucket_row(retention_enabled=True, retention_default=30, retention_minimum=10, retention_maximum=60)
        existing = {"retention": {"retention_enabled": True, "default": 30, "minimum": 10, "maximum": 60}}
        errors = []

        svc.validate_retention_update(BucketRetention(maximum_years=5), bucket, existing, errors)

        assert errors == []
        assert existing["retention"]["maximum"] == 1825

    def test_update_accepts_years_and_fills_missing_from_bucket(self):
        bucket = bucket_row(retention_enabled=True, retention_default=30, retention_minimum=10, retention_maximum=60)
        existing = {"retention": {"retention_enabled": True, "default": 30, "minimum": 10, "maximum": 60}}
        errors = []

        svc.validate_retention_update(BucketRetention(retention_enabled=True, maximum_years=5), bucket, existing, errors)

        assert errors == []
        assert existing["retention"] == {"retention_enabled": True, "default": 30, "minimum": 10, "maximum": 1825}


class TestRetentionStateForClient:
    DAYS_STATE = {"retention_enabled": True, "default": 30, "minimum": 10, "maximum": 60}

    def test_no_payload_keeps_the_effective_block(self):
        assert svc.retention_state_for_client(None, self.DAYS_STATE) == self.DAYS_STATE

    def test_legacy_payload_keeps_legacy_keys_and_adds_unit(self):
        assert svc.retention_state_for_client(retention_legacy(), self.DAYS_STATE) == {**self.DAYS_STATE, "unit": "days"}

    def test_years_payload_is_echoed_as_sent_next_to_days(self):
        years = {"retention_enabled": True, "default": 365, "minimum": 365, "maximum": 730}

        state = svc.retention_state_for_client(retention_years(1, 1, 2), years)

        assert state == {**years, "unit": "years", "default_years": 1, "minimum_years": 1, "maximum_years": 2}

    def test_empty_payload_adds_nothing(self):
        disabled = {"retention_enabled": False, "default": None, "minimum": None, "maximum": None}

        assert svc.retention_state_for_client(BucketRetention(), disabled) == disabled
