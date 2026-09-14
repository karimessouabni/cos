import logging
from datetime import date

from dateutil.relativedelta import relativedelta
from cos_service.schemas.bucket_backup import BucketBackup
from cos_service.schemas.immutability import Immutability
from bp2i_airflow_library.exceptions.flow_control import (
    DeclineDemandException
)
from bp2i_airflow_library.dependencies import SASession
from cos_service.schemas.bucket_retention import (
    DAYS,
    MAX_RETENTION_YEARS,
    YEARS,
    BucketRetention,
    max_retention,
    _RETENTION_KEYS
)

_OBJECT_LOCK_LABEL = "Object lock retention"
_OBJECT_LOCK_FIELDS = ("object_lock_duration_days", "object_lock_duration_years")



def check_duration_bounds(unit, duration, label, errors) -> None:
    """Durée > 0 et ≤ 5 ans, comparée dans l'unité saisie."""
    if duration <= 0:
        logging.info(f"check_duration_bounds {label} zero")
        errors.append(f"{label} ({duration} {unit}) cannot be inferior or equal to ZERO.")

    if duration > max_retention(unit):
        logging.info(f"check_duration_bounds {label} limit")
        errors.append(
            f"{label} ({duration} {unit}) cannot be superior to {format_limit(unit)}."
        )

def resolve_object_lock(days, years, errors):
    """(unité, durée) de l'object lock, validées. (None, None) si rien n'est saisi."""
    unit = check_single_unit(days, years, _OBJECT_LOCK_LABEL, *_OBJECT_LOCK_FIELDS, errors)
    if unit is None:
        return None, None

    duration = days if unit == DAYS else years
    check_duration_bounds(unit, duration, _OBJECT_LOCK_LABEL, errors)
    return unit, duration


def apply_choice_unit(
    immutability_choice: Immutability,
    payload_retention: BucketRetention,
    object_lock_duration_days,
    object_lock_duration_years,
):
    """Applique l'unité portée par le choix (…_DAILY / …_YEARLY).

    Seule l'unité choisie est prise en compte : les valeurs saisies dans l'autre
    unité sont ignorées (remises à None) et l'unité choisie doit être renseignée.
    Les choix génériques (RETENTION, OBJECT_LOCK, NONE) ne sont pas touchés.
    Retourne (payload_retention, object_lock_duration_days, object_lock_duration_years).
    """
    if not immutability_choice.has_explicit_unit:
        return payload_retention, object_lock_duration_days, object_lock_duration_years

    keep, drop = (YEARS, DAYS) if immutability_choice.is_yearly else (DAYS, YEARS)
    errors = []

    if immutability_choice.is_object_lock:
        if immutability_choice.is_yearly:
            if object_lock_duration_days is not None:
                logging.info(
                    f"apply_choice_unit {immutability_choice} : object_lock_duration_days={object_lock_duration_days} ignoré"
                )
            object_lock_duration_days = None
            if object_lock_duration_years is None:
                errors.append(f"{_OBJECT_LOCK_LABEL} must be set in years ({_OBJECT_LOCK_FIELDS[1]}) for choice {immutability_choice.value}.")
        else:
            if object_lock_duration_years is not None:
                logging.info(
                    f"apply_choice_unit {immutability_choice} : object_lock_duration_years={object_lock_duration_years} ignoré"
                )
            object_lock_duration_years = None
            if object_lock_duration_days is None:
                errors.append(f"{_OBJECT_LOCK_LABEL} must be set in days ({_OBJECT_LOCK_FIELDS[0]}) for choice {immutability_choice.value}.")

    if immutability_choice.is_retention:
        if payload_retention is None or payload_retention.is_empty():
            errors.append(f"Retention must not be empty for choice {immutability_choice.value}.")
        else:
            for key in _RETENTION_KEYS:
                dropped = getattr(payload_retention, f"{key}_{drop}", None)
                if dropped is not None:
                    logging.info(f"apply_choice_unit {immutability_choice} : {key}_{drop}={dropped} ignoré")
                    setattr(payload_retention, f"{key}_{drop}", None)
            missing = [f"{key}_{keep}" for key in _RETENTION_KEYS if getattr(payload_retention, f"{key}_{keep}", None) is None]
            if missing:
                errors.append(
                    f"Retention must be set in {keep} ({', '.join(missing)}) for choice {immutability_choice.value}."
                )

    raise_on_errors(errors)
    logging.info(
        f"apply_choice_unit {immutability_choice} : retention={payload_retention}, "
        f"days={object_lock_duration_days}, years={object_lock_duration_years}"
    )
    return payload_retention, object_lock_duration_days, object_lock_duration_years


def compute_bucket_new_immutability(
        immutability_choice: Immutability | None,
        payload_retention: BucketRetention,
        object_lock_duration_days: int,
        object_lock_duration_years: int,
        enable_versioning: bool,
        immutability: dict,
        backup: BucketBackup
) -> dict:
    """Construit le bloc immutability d'un bucket qui n'en avait pas encore.

    Sans choix explicite dans le payload, le choix est déduit de ce qui est
    saisi : rétention -> RETENTION, durée d'object-lock -> OBJECT_LOCK, sinon
    NONE (seule la bascule de versioning est appliquée).

    Un choix absent (None) est traité comme NONE : c'est le seul endroit où
    la valeur est normalisée, tout ce qui est appelé en dessous reçoit un enum.
    """
    if immutability_choice is None or immutability_choice is Immutability.NONE:
        immutability_choice = _infer_immutability_choice(
            payload_retention, object_lock_duration_days, object_lock_duration_years
        )
        logging.info(f"compute_bucket_new_immutability choix déduit : {immutability_choice}")

    payload_retention, object_lock_duration_days, object_lock_duration_years = apply_choice_unit(
        immutability_choice, payload_retention, object_lock_duration_days, object_lock_duration_years
    )

    if  immutability_choice.is_retention:
        immutability = _new_retention_immutability(
            payload_retention, enable_versioning, backup, immutability
        )
    elif immutability_choice.is_object_lock:
        immutability = _new_object_lock_immutability(
            object_lock_duration_days,
            object_lock_duration_years,
            enable_versioning,
            immutability,
        )
    else:
        _apply_versioning_only_update(immutability, enable_versioning)

    if _backup_requested(backup):
        immutability = compute_bucket_backup(immutability, backup)

    logging.info(f"compute_bucket_new_immutability : {immutability}")
    return immutability



def write_object_lock(immutability, unit, duration) -> None:
    """Écrit l'object lock dans l'unité saisie ; l'autre unité reste à None."""
    immutability["object_locking_enabled"] = True
    immutability["object_versioning_enabled"] = True
    immutability["object_lock_duration_days"] = duration if unit == DAYS else None
    immutability["object_lock_duration_years"] = duration if unit == YEARS else None



# --- UPDATE ------------------------------------------------------------------
def validate_object_lock_update(
    existing_immutability: dict,
    object_lock_duration_days,
    object_lock_duration_years,
    errors,
) -> None:
    """Object lock côté update : la saisie remplace l'existant, sinon on le garde."""
    if object_lock_duration_days is None and object_lock_duration_years is None:
        logging.info("validate_immutability4")
        object_lock_duration_days = existing_immutability.get("object_lock_duration_days")
        object_lock_duration_years = existing_immutability.get("object_lock_duration_years")

    unit, duration = resolve_object_lock(
        object_lock_duration_days,
        object_lock_duration_years,
        errors,
    )

    # unit None sans erreur = pas d'object lock sur ce bucket, rien à valider.
    if unit is not None:
        write_object_lock(existing_immutability, unit, duration)
        logging.info(f"validate_immutability5 : {existing_immutability}")


def validate_retention_update(
    retention,
    bucket: dict,
    existing_immutability: dict,
    errors,
) -> None:
    """Rétention côté update : tout est normalisé, comparé et stocké en jours.

    [RECONSTITUTION] Docstring repliée sur la photo (lignes 131-136) :
    seule la première ligne était visible. Le corps ci-dessous est complet.
    """
    existing_retention = existing_immutability["retention"]

    if not retention or retention.is_empty() or not retention.retention_enabled:
        logging.info("validate_immutability6")
        values = {key: existing_retention[key] for key in _RETENTION_KEYS}
    else:
        values = {key: retention.in_days(key) for key in _RETENTION_KEYS}
        for key in _RETENTION_KEYS:
            if values[key] is None:
                values[key] = bucket[f"retention_{key}"]

    default, minimum, maximum = values["default"], values["minimum"], values["maximum"]

    if default is None or minimum is None or maximum is None:
        logging.info("validate_immutability7")
        errors.append(
            "Retention configuration is not valid. You must set default, minimum and maximum "
            "in the same unit, either in days or in years."
        )
        return

    check_retention_bounds(DAYS, default, minimum, maximum, errors)

    existing_immutability["retention"] = {
        "retention_enabled": True,
        "default": default,
        "minimum": minimum,
        "maximum": maximum,
    }
    logging.info(f"validate_immutability8 : {existing_immutability}")


def check_retention_bounds(unit, default, minimum, maximum, errors) -> None:
    """Applique min ≤ default ≤ max et le plafond 5 ans, dans l'unité saisie."""
    if minimum <= 0 or minimum > default:
        logging.info("check_retention_bounds1")
        errors.append(
            f"Retention minimum ({minimum} {unit}) cannot be inferior or equal to 0 "
            f"nor superior to default ({default} {unit})."
        )

    if default < minimum or default >= maximum:
        logging.info("check_retention_bounds2")
        errors.append(
            f"Retention default ({default} {unit}) cannot be inferior to minimum "
            f"({minimum} {unit}) nor superior or equal to maximum ({maximum} {unit})."
        )

    if maximum > max_retention(unit) or maximum <= default:
        logging.info("check_retention_bounds3")
        errors.append(
            f"Retention maximum ({maximum} {unit}) cannot be inferior or equal to default "
            f"({default} {unit}) nor superior to {format_limit(unit)}."
        )


def validate_immutability_for_update_bucket(
        bucket: dict,
        retention: BucketRetention,
        object_lock_duration_days: int,
        object_lock_duration_years: int,
        enable_versioning: bool,
        has_contents: bool,
        backup: BucketBackup,
        session: SASession
) -> dict:
    """Orchestration de l'update : valide la demande contre l'état actuel du bucket.

    [RECONSTITUTION] Docstring repliée sur la photo (lignes 203-208) :
    seule la première ligne était visible. Le corps ci-dessous est complet.
    """
    existing_immutability = compute_bucket_immutability_for_update_bucket(bucket, session)
    existing_immutability_choice = compute_bucket_immutability_choice(bucket)
    errors = []

    if existing_immutability_choice is Immutability.NONE:
        logging.info("validate_immutability2")
        existing_immutability = compute_bucket_new_immutability(
            existing_immutability_choice,
            retention,
            object_lock_duration_days,
            object_lock_duration_years,
            enable_versioning,
            existing_immutability,
            backup,
        )

    if existing_immutability["retention"]["retention_enabled"] and has_contents:
        errors.append("Setting a retention is not possible when the bucket already contains objects.")

    if existing_immutability_choice.is_object_lock:
        logging.info("validate_immutability3")
        _update_object_locked_bucket(
            existing_immutability,
            retention,
            enable_versioning,
            object_lock_duration_days,
            object_lock_duration_years,
            errors,
        )

    if existing_immutability_choice.is_retention:
        logging.info("validate_immutability8")
        _update_retention_bucket(
            existing_immutability,
            bucket,
            retention,
            backup,
            enable_versioning,
            object_lock_duration_days,
            object_lock_duration_years,
            errors,
        )

    raise_on_errors(errors)

    if _backup_requested(backup) and (
        existing_immutability_choice == Immutability.NONE or existing_immutability_choice.is_object_lock
    ):
        logging.info(f"validate_immutability14 : {backup}")
        existing_immutability = compute_bucket_backup(existing_immutability, backup)

    logging.info(f"validate_immutability15 : {existing_immutability}")
    return existing_immutability




def raise_on_errors(errors) -> None:
    if errors:
        global_message = " | ".join(errors)
        raise DeclineDemandException(global_message)


def compute_bucket_immutability_for_update_bucket(bucket: dict, session) -> dict:
    """Reconstruit le bloc immutability existant depuis la ligne bucket en base.

    [RECONSTITUTION] Docstring repliée sur la photo (lignes 273-277) :
    seule la première ligne était visible. Le corps ci-dessous est complet.
    """
    object_lock_duration_days = bucket["object_lock_duration_days"]
    object_lock_duration_years = bucket.get("object_lock_duration_years")

    if object_lock_duration_days is not None and object_lock_duration_years is not None:
        # Ligne héritée d'un ancien update qui n'effaçait pas l'autre unité :
        # on garde les jours (colonne d'origine) plutôt que de bloquer le client.
        logging.warning(
            "bucket stores object lock in both units "
            f"(days={object_lock_duration_days}, years={object_lock_duration_years}), keeping days"
        )
        object_lock_duration_years = None

    immutability = {
        "object_locking_enabled": object_lock_duration_days is not None or object_lock_duration_years is not None,
        "object_versioning_enabled": bool(bucket.get("object_versioning_enabled")),
        "object_lock_duration_days": object_lock_duration_days,
        "object_lock_duration_years": object_lock_duration_years,
        "retention": _retention_from_bucket(bucket),
        "backup": _backup_from_bucket(bucket, session),
    }
    logging.info(f"compute_bucket_immutability_for_update_bucket : {immutability}")
    return immutability

def compute_bucket_immutability_choice(bucket: dict) -> Immutability :
    if bucket["object_lock_duration_days"] is not None or  bucket["object_lock_duration_years"] is not None:
        logging.info("compute_bucket_immutability_choice1")
        return Immutability.OBJECT_LOCK
    if bucket["retention_enabled"]:
        logging.info("compute_bucket_immutability_choice2")
        return Immutability.RETENTION
    logging.info("compute_bucket_immutability_choice3")
    return Immutability.NONE

def compute_bucket_retention(payload_retention: BucketRetention, immutability: dict) -> dict:
    """Valide la rétention saisie et la reporte dans immutability["retention"].

    [RECONSTITUTION] Docstring repliée sur la photo (lignes 312-318) :
    seule la première ligne était visible. Le corps ci-dessous est complet.
    """
    unit = payload_retention.unit
    default, minimum, maximum = payload_retention.default, payload_retention.minimum, payload_retention.maximum

    if None in (unit, default, minimum, maximum):
        logging.info("compute_bucket_retention1")
        raise DeclineDemandException(
            "Retention configuration is not valid. You must set default, minimum and maximum "
            "in the same unit, either in days (…_days) or in years (…_years)."
        )

    immutability["retention"]["retention_enabled"] = payload_retention.retention_enabled
    for key in _RETENTION_KEYS:
        immutability["retention"][key] = payload_retention.in_days(key)

    logging.info(f"compute_bucket_retention2 {immutability}")
    return immutability

def check_single_unit(days, years, label, days_field, years_field, errors) -> str | None:
    """Vérifie qu'une seule unité est saisie et renvoie laquelle ("days"/"years"/None)."""
    if days is not None and years is not None:
        logging.info(f"check_single_unit {label} both")
        errors.append(
            f"{label} must be set either in days ({days_field}) "
            f"or in years ({years_field}), not both."
        )
        return None
    if days is not None:
        return DAYS
    if years is not None:
        return YEARS
    return None

def format_limit(unit) -> str:
    """Plafond lisible dans l'unité saisie : "5 years" ou "5 years (1825 days)"."""
    if unit == YEARS:
        return f"{MAX_RETENTION_YEARS} years"
    return f"{MAX_RETENTION_YEARS} years "

def compute_bucket_object_lock(
    immutability,
    object_lock_duration_days,
    object_lock_duration_years,
):
    errors = []

    unit, duration = resolve_object_lock(
        object_lock_duration_days,
        object_lock_duration_years,
        errors,
    )

    if unit is None and not errors:
        logging.info("compute_bucket_object_lock1")
        errors.append(
            f"{_OBJECT_LOCK_LABEL} must be set, either in days "
            f"({_OBJECT_LOCK_FIELDS[0]}) or in years ({_OBJECT_LOCK_FIELDS[1]})."
        )

    raise_on_errors(errors)

    write_object_lock(immutability, unit, duration)
    logging.info(f"compute_bucket_object_lock2 {immutability}")
    return immutability

def compute_bucket_backup(immutability, backup):
    if backup.backup_enabled is True:
        logging.info("compute_bucket_backup1")
        errors = []
        if not backup.backup_vault_name or not backup.backup_retention_days:
            errors.append("Bucket backup specifications are not fully set.")
        if not immutability["object_versioning_enabled"]:
            errors.append("Versioning should be enabled to enable bucket backup.")

        if errors:
            global_message = " | ".join(errors)
            raise DeclineDemandException(global_message)

        immutability["backup"]["backup_enabled"] = True
        immutability["backup"]["backup_vault_sub_id"] = backup.backup_vault_sub_id
        immutability["backup"]["backup_retention_days"] = backup.backup_retention_days
    else:
        logging.info("compute_bucket_backup2")
        if not immutability["object_versioning_enabled"]:
            raise DeclineDemandException("Versioning should be enabled to disable bucket backup.")
        immutability["backup"]["backup_enabled"] = False
        immutability["backup"]["backup_vault_sub_id"] = None
        immutability["backup"]["backup_retention_days"] = None

    logging.info(f"compute_bucket_backup3 {immutability}")
    return immutability

def _infer_immutability_choice(payload_retention, object_lock_duration_days, object_lock_duration_years):
    """Choix implicite quand le payload n'en donne pas : déduit de ce qui est saisi."""
    retention_given = _retention_requested(payload_retention)
    lock_given = _object_lock_requested(object_lock_duration_days, object_lock_duration_years)

    if retention_given and lock_given:
        raise DeclineDemandException(
            "Retention and object Lock are not compatible. "
            "We cannot activate both of them simultaneously."
        )
    if retention_given:
        return Immutability.RETENTION
    if lock_given:
        return Immutability.OBJECT_LOCK
    return Immutability.NONE

def _new_retention_immutability(payload_retention, enable_versioning, backup, immutability: dict) -> dict:
    """Nouvelle immutabilité en mode rétention : incompatibilités puis calcul."""
    errors = []

    if enable_versioning:
        errors.append(
            "Retention and versioning are not compatible. "
            "We cannot activate both of them simultaneously."
        )
    if _backup_requested(backup):
        errors.append(
            "Retention and bucket backup are not compatible. "
            "We cannot activate backup when retention is enabled."
        )
    if not _retention_requested(payload_retention):
        errors.append("Retention must not be empty.")

    raise_on_errors(errors)
    return compute_bucket_retention(payload_retention, immutability)

def _new_object_lock_immutability(
    object_lock_duration_days,
    object_lock_duration_years,
    enable_versioning,
    immutability: dict,
) -> dict:
    """Nouvelle immutabilité en mode object-lock : versioning requis, durée obligatoire."""
    errors = []

    if not enable_versioning:
        errors.append("Versioning should be enabled to enable object-lock.")
    if not _object_lock_requested(object_lock_duration_days, object_lock_duration_years):
        errors.append("Object Lock Duration Days or Years must be not empty.")

    raise_on_errors(errors)
    return compute_bucket_object_lock(
        immutability, object_lock_duration_days, object_lock_duration_years
    )

def _retention_requested(payload_retention) -> bool:
    return bool(payload_retention and not payload_retention.is_empty()
                and payload_retention.retention_enabled)

def _object_lock_requested(object_lock_duration_days, object_lock_duration_years) -> bool:
    return object_lock_duration_days is not None or object_lock_duration_years is not None

def _retention_from_bucket(bucket: dict) -> dict:
    """Bloc retention (en jours) depuis la ligne bucket ; valeurs à None si désactivée."""
    enabled = bool(bucket["retention_enabled"])
    return {
        "retention_enabled": enabled,
        **{key: bucket[f"retention_{key}"] if enabled else None for key in _RETENTION_KEYS},
    }

def _backup_from_bucket(bucket: dict, session) -> dict:
    """Bloc backup depuis la ligne bucket ; le vault n'est résolu que si nécessaire."""
    if not bucket["backup_enabled"]:
        return {"backup_enabled": False, "backup_vault_sub_id": None, "backup_retention_days": None}

    from cos_service.services.backup_vault_service import get_backup_vault_by_sub_id

    backup_vault = get_backup_vault_by_sub_id(bucket["backup_vault_subscription_id"], session)
    return {
        "backup_enabled": True,
        "backup_vault_sub_id": backup_vault["subscription_id"],
        "backup_retention_days": bucket["backup_retention_days"],
    }

def _backup_requested(backup) -> bool:
    return bool(backup and not backup.is_empty())

def _apply_versioning_only_update(existing_immutability: dict, enable_versioning) -> None:
    """Choix NONE : seule la bascule de versioning est appliquée, le reste est conservé."""
    if enable_versioning is None:
        enable_versioning = existing_immutability["object_versioning_enabled"]
    existing_immutability["object_versioning_enabled"] = enable_versioning
    logging.info(f"validate_immutability1 : {existing_immutability}")

def _update_object_locked_bucket(
    existing_immutability: dict,
    retention,
    enable_versioning,
    object_lock_duration_days,
    object_lock_duration_years,
    errors,
) -> None:
    """Bucket déjà en object-lock : ni rétention ni arrêt du versioning, durée remplaçable."""
    if retention is not None:
        errors.append("Setting a retention is not possible when object-lock is already enabled.")

    if enable_versioning is False:
        errors.append("Disabling versioning is not possible when object-lock is already enabled.")

    validate_object_lock_update(
        existing_immutability,
        object_lock_duration_days,
        object_lock_duration_years,
        errors,
    )

def _update_retention_bucket(
    existing_immutability: dict,
    bucket: dict,
    retention,
    backup,
    enable_versioning,
    object_lock_duration_days,
    object_lock_duration_years,
    errors,
) -> None:
    """Bucket déjà en rétention : ni object-lock, ni versioning, ni backup."""
    if object_lock_duration_days is not None or object_lock_duration_years is not None:
        errors.append("Setting an object-lock is not possible when retention is already enabled.")

    if enable_versioning:
        errors.append("Enabling versioning is not possible when retention is already enabled.")

    if _backup_requested(backup): #TODO : verify this
        errors.append(
            "Retention and bucket backup are not compatible. "
            "We cannot activate backup when retention is enabled."
        )

    validate_retention_update(retention, bucket, existing_immutability, errors)

