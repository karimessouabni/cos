"""Contrat complet de ``cos_service.schemas.bucket_retention``.

Couvre : les conversions années -> jours (bissextiles), les plafonds, le
format historique (jours implicites), l'activation automatique du drapeau,
le validateur ``_validate`` (erreurs accumulées), les propriétés de lecture,
``in_days``, ``__iter__``, ``is_empty``, ``to_cos_payload`` et ``as_sent``.

``date.today()`` est figé au 2025-03-01 par la fixture ``frozen_today`` :
1 an = 365 j, 2 ans = 730 j, 5 ans = 1826 j (un seul 29 février, en 2028).
"""
from datetime import date

import pytest
from pydantic import ValidationError

from cos_service.schemas.bucket_retention import (
    DAYS,
    MAX_RETENTION_YEARS,
    YEARS,
    BucketRetention,
    max_retention,
    max_retention_days,
    years_to_days,
)


def errors_of(exc: ValidationError) -> str:
    return str(exc.value)


# --- conversions et plafonds ---------------------------------------------------------

class TestYearsToDays:
    def test_without_leap_day(self):
        assert years_to_days(1, date(2025, 3, 1)) == 365

    def test_with_one_leap_day(self):
        assert years_to_days(1, date(2027, 3, 1)) == 366  # 29/02/2028 dans la fenêtre

    def test_two_years(self):
        assert years_to_days(2, date(2025, 3, 1)) == 730
        assert years_to_days(2, date(2026, 9, 26)) == 731

    def test_five_years_has_one_or_two_leap_days(self):
        assert years_to_days(5, date(2025, 3, 1)) == 1826
        assert years_to_days(5, date(2024, 1, 1)) == 1827  # 2024 et 2028

    def test_default_start_is_today(self, frozen_today):
        assert years_to_days(1) == years_to_days(1, frozen_today)

    def test_zero_years(self):
        assert years_to_days(0, date(2025, 3, 1)) == 0


class TestMaxRetention:
    def test_max_retention_days_depends_on_the_start_date(self):
        assert max_retention_days(date(2025, 3, 1)) == 1826
        assert max_retention_days(date(2024, 1, 1)) == 1827

    def test_max_retention_days_defaults_to_today(self, frozen_today):
        assert max_retention_days() == max_retention_days(frozen_today)

    def test_max_retention_per_unit(self):
        assert max_retention(YEARS) == MAX_RETENTION_YEARS == 5
        assert max_retention(DAYS) == 1826


# --- construction : format courant ---------------------------------------------------

class TestDaysPayload:
    def test_fields_and_unit(self):
        r = BucketRetention(retention_enabled=True, default_days=30, minimum_days=10, maximum_days=60)

        assert r.unit == DAYS
        assert (r.default, r.minimum, r.maximum) == (30, 10, 60)
        assert r.retention_enabled is True

    def test_in_days_is_identity(self):
        r = BucketRetention(default_days=30, minimum_days=10, maximum_days=60)

        assert [r.in_days(k) for k in ("default", "minimum", "maximum")] == [30, 10, 60]

    def test_partial_payload_is_allowed(self):
        r = BucketRetention(maximum_days=60)

        assert r.unit == DAYS
        assert (r.default, r.minimum, r.maximum) == (None, None, 60)
        assert r.in_days("default") is None

    def test_max_allowed_in_days(self):
        assert BucketRetention(default_days=1).max_allowed == 1826


class TestYearsPayload:
    def test_fields_and_unit(self):
        r = BucketRetention(retention_enabled=True, default_years=1, minimum_years=1, maximum_years=2)

        assert r.unit == YEARS
        assert (r.default, r.minimum, r.maximum) == (1, 1, 2)

    def test_in_days_converts_with_leap_years_from_today(self):
        r = BucketRetention(default_years=1, minimum_years=1, maximum_years=2)

        assert r.in_days("default") == 365
        assert r.in_days("maximum") == 730

    def test_five_years_is_the_ceiling(self):
        r = BucketRetention(maximum_years=5)

        assert r.in_days("maximum") == 1826
        assert r.max_allowed == 5


class TestEmptyPayload:
    def test_defaults(self):
        r = BucketRetention()

        assert r.retention_enabled is False
        assert r.unit is None
        assert (r.default, r.minimum, r.maximum) == (None, None, None)
        assert r.max_allowed is None
        assert r.in_days("default") is None
        assert r.is_empty()

    def test_flag_only_is_still_empty(self):
        assert BucketRetention(retention_enabled=True).is_empty()
        assert BucketRetention(retention_enabled=False).is_empty()

    def test_non_dict_input_is_left_to_pydantic(self):
        with pytest.raises(ValidationError):
            BucketRetention.model_validate("not a dict")


# --- activation automatique du drapeau ------------------------------------------------

class TestAutoEnable:
    def test_bounds_without_flag_enable_retention(self):
        assert BucketRetention(default_days=30, minimum_days=10, maximum_days=60).retention_enabled is True
        assert BucketRetention(maximum_years=5).retention_enabled is True

    def test_explicit_false_is_kept(self):
        assert BucketRetention(retention_enabled=False, default_days=30).retention_enabled is False

    def test_explicit_true_is_kept(self):
        assert BucketRetention(retention_enabled=True, default_days=30).retention_enabled is True

    def test_no_bounds_no_flag_stays_false(self):
        assert BucketRetention().retention_enabled is False

    def test_null_flag_counts_as_not_given(self):
        # Terraform envoie null pour un attribut optional non renseigné.
        assert BucketRetention.model_validate({"retention_enabled": None, "default_days": 30}).retention_enabled is True
        assert BucketRetention.model_validate({"retention_enabled": None}).retention_enabled is False

    def test_bounds_set_to_null_do_not_enable(self):
        r = BucketRetention.model_validate({"default_days": None, "maximum_years": None})

        assert r.retention_enabled is False
        assert r.is_empty()


# --- format historique (jours implicites) ---------------------------------------------

class TestLegacyFormat:
    def test_is_mapped_to_days(self):
        r = BucketRetention(retention_enabled=True, default=30, minimum=10, maximum=60)

        assert r.unit == DAYS
        assert (r.default_days, r.minimum_days, r.maximum_days) == (30, 10, 60)
        assert (r.default, r.minimum, r.maximum) == (30, 10, 60)
        assert [r.in_days(k) for k in ("default", "minimum", "maximum")] == [30, 10, 60]

    def test_legacy_keys_are_not_fields(self):
        r = BucketRetention(default=30, minimum=10, maximum=60)

        assert "default" not in BucketRetention.model_fields
        assert r.model_dump(exclude_none=True) == {
            "retention_enabled": True, "default_days": 30, "minimum_days": 10, "maximum_days": 60
        }

    def test_legacy_values_are_type_checked_like_the_days_fields(self):
        with pytest.raises(ValidationError, match="default_days"):
            BucketRetention(default="thirty")

    def test_non_dict_input_is_left_untouched(self):
        # Un objet déjà construit repasse dans le validateur sans être réinterprété.
        r = BucketRetention(default=30, minimum=10, maximum=60)

        assert BucketRetention.model_validate(r) == r

    def test_same_model_as_the_days_format(self):
        legacy = BucketRetention(retention_enabled=True, default=30, minimum=10, maximum=60)
        days = BucketRetention(retention_enabled=True, default_days=30, minimum_days=10, maximum_days=60)

        assert legacy == days

    def test_raw_dict_like_the_orchestrator_payload(self):
        r = BucketRetention.model_validate({"retention_enabled": True, "default": 30, "minimum": 10, "maximum": 60})

        assert r.default_days == 30

    def test_partial_legacy_payload(self):
        r = BucketRetention(maximum=60)

        assert r.unit == DAYS
        assert r.maximum == 60
        assert r.default is None

    def test_auto_enable_applies_to_the_legacy_format(self):
        assert BucketRetention(default=30, minimum=10, maximum=60).retention_enabled is True
        assert BucketRetention(retention_enabled=False, default=30).retention_enabled is False

    def test_legacy_bounds_are_validated_like_days(self):
        with pytest.raises(ValidationError, match="1826 days"):
            BucketRetention(default=30, minimum=10, maximum=2000)
        with pytest.raises(ValidationError, match="default cannot be superior to maximum"):
            BucketRetention(default=100, minimum=10, maximum=50)

    def test_mixing_legacy_and_suffixed_fields_is_rejected(self):
        with pytest.raises(ValidationError, match="not both"):
            BucketRetention(default=30, default_days=30)
        with pytest.raises(ValidationError, match="not both"):
            BucketRetention(default=30, maximum_years=5)

    def test_legacy_keys_set_to_null_are_ignored(self):
        r = BucketRetention.model_validate({"default": None, "minimum": None, "maximum": None, "maximum_years": 5})

        assert r.unit == YEARS
        assert r.maximum == 5

    def test_is_logged_as_a_warning(self, caplog):
        with caplog.at_level("WARNING", logger="cos_service.schemas.bucket_retention"):
            BucketRetention(default=30, minimum=10, maximum=60)

        assert "Legacy retention payload" in caplog.text
        assert "'default': 30" in caplog.text


# --- _validate : contrôles du payload -----------------------------------------------

class TestValidate:
    def test_valid_days(self):
        BucketRetention(default_days=30, minimum_days=10, maximum_days=60)

    def test_valid_years(self):
        BucketRetention(default_years=2, minimum_years=1, maximum_years=5)

    def test_mixed_units_are_rejected(self):
        with pytest.raises(ValidationError) as exc:
            BucketRetention(default_days=30, minimum_years=1, maximum_years=2)

        assert "not a mix of both" in errors_of(exc)

    def test_mixed_units_skip_bound_checks(self):
        # Sans unité non ambiguë, seuls le mélange et le signe sont signalés.
        with pytest.raises(ValidationError) as exc:
            BucketRetention(default_days=5000, minimum_years=1, maximum_years=2)

        assert "not a mix of both" in errors_of(exc)
        assert "cannot be superior to 5 years" not in errors_of(exc)

    @pytest.mark.parametrize("field", [
        "default_days", "minimum_days", "maximum_days", "default_years", "minimum_years", "maximum_years",
    ])
    @pytest.mark.parametrize("value", [0, -1])
    def test_non_positive_values_are_rejected(self, field, value):
        with pytest.raises(ValidationError) as exc:
            BucketRetention(**{field: value})

        assert f"{field} must be superior to 0." in errors_of(exc)

    def test_days_above_five_years(self):
        with pytest.raises(ValidationError) as exc:
            BucketRetention(maximum_days=1827)

        assert "maximum_days (1827 days) cannot be superior to 5 years (1826 days, leap years included)." in errors_of(exc)

    def test_days_at_the_ceiling_are_accepted(self):
        assert BucketRetention(maximum_days=1826).maximum == 1826

    def test_years_above_five(self):
        with pytest.raises(ValidationError) as exc:
            BucketRetention(default_years=6)

        assert "default_years (6 years) cannot be superior to 5 years (5 years, leap years included)." in errors_of(exc)

    def test_years_at_the_ceiling_are_accepted(self):
        assert BucketRetention(maximum_years=5).maximum == 5

    def test_minimum_above_maximum(self):
        with pytest.raises(ValidationError) as exc:
            BucketRetention(minimum_days=60, maximum_days=10)

        assert "Retention minimum cannot be superior to maximum." in errors_of(exc)

    def test_default_below_minimum(self):
        with pytest.raises(ValidationError) as exc:
            BucketRetention(default_days=5, minimum_days=10)

        assert "Retention default cannot be inferior to minimum." in errors_of(exc)

    def test_default_above_maximum(self):
        with pytest.raises(ValidationError) as exc:
            BucketRetention(default_days=100, maximum_days=50)

        assert "Retention default cannot be superior to maximum." in errors_of(exc)

    def test_equal_bounds_are_accepted(self):
        r = BucketRetention(default_days=30, minimum_days=30, maximum_days=30)

        assert (r.default, r.minimum, r.maximum) == (30, 30, 30)

    def test_partial_bounds_are_checked_pairwise_only(self):
        assert BucketRetention(default_days=30).default == 30
        assert BucketRetention(minimum_days=10, maximum_days=60).default is None

    def test_errors_are_accumulated_in_one_message(self):
        with pytest.raises(ValidationError) as exc:
            BucketRetention(default_days=-1, minimum_days=60, maximum_days=10)

        message = errors_of(exc)
        assert "default_days must be superior to 0." in message
        assert "Retention minimum cannot be superior to maximum." in message
        assert "Retention default cannot be inferior to minimum." in message
        assert " | " in message

    def test_ceiling_and_order_errors_are_both_reported(self):
        with pytest.raises(ValidationError) as exc:
            BucketRetention(default_years=6, minimum_years=1, maximum_years=5)

        message = errors_of(exc)
        assert "default_years (6 years) cannot be superior to 5 years" in message
        assert "Retention default cannot be superior to maximum." in message

    def test_non_integer_values_are_rejected_by_pydantic(self):
        with pytest.raises(ValidationError):
            BucketRetention(default_days="thirty")

    def test_flag_is_not_validated_against_bounds(self):
        # retention_enabled=True sans borne est accepté ici ; c'est le service
        # qui décline ("Retention must not be empty").
        assert BucketRetention(retention_enabled=True).is_empty()


# --- lecture ---------------------------------------------------------------------------

class TestIteration:
    def test_iter_yields_unit_and_bounds_in_the_unit_given(self):
        r = BucketRetention(default_years=1, minimum_years=1, maximum_years=2)

        assert dict(r) == {"unit": YEARS, "default": 1, "minimum": 1, "maximum": 2}

    def test_iter_on_empty(self):
        assert dict(BucketRetention()) == {"unit": None, "default": None, "minimum": None, "maximum": None}

    def test_iter_on_legacy_reads_as_days(self):
        assert dict(BucketRetention(default=30, minimum=10, maximum=60)) == {
            "unit": DAYS, "default": 30, "minimum": 10, "maximum": 60
        }


class TestToCosPayload:
    def test_empty(self):
        assert BucketRetention().to_cos_payload() == {}
        assert BucketRetention(retention_enabled=True).to_cos_payload() == {}

    def test_full_in_the_unit_given(self):
        r = BucketRetention(default_years=1, minimum_years=1, maximum_years=2)

        assert r.to_cos_payload() == {"unit": YEARS, "default": 1, "minimum": 1, "maximum": 2}

    def test_partial_drops_missing_bounds(self):
        assert BucketRetention(maximum_days=60).to_cos_payload() == {"unit": DAYS, "maximum": 60}


class TestAsSent:
    def test_empty(self):
        assert BucketRetention().as_sent() == {}

    def test_days(self):
        r = BucketRetention(default_days=30, minimum_days=10, maximum_days=60)

        assert r.as_sent() == {"default_days": 30, "minimum_days": 10, "maximum_days": 60}

    def test_years_partial(self):
        assert BucketRetention(maximum_years=5).as_sent() == {"maximum_years": 5}

    def test_legacy_is_echoed_under_the_days_names(self):
        assert BucketRetention(default=30, minimum=10, maximum=60).as_sent() == {
            "default_days": 30, "minimum_days": 10, "maximum_days": 60
        }


class TestAssignmentAfterValidation:
    """apply_choice_unit (service) remet à None l'unité non retenue : les
    lectures doivent suivre l'état courant, pas celui de la construction."""

    def test_dropping_a_unit_changes_the_unit_read(self):
        r = BucketRetention.model_construct(default_days=30, default_years=1)

        assert r.unit == DAYS
        r.default_days = None
        assert r.unit == YEARS
        assert r.default == 1
        assert r.in_days("default") == 365
