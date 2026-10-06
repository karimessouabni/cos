"""Matrice des scénarios Terraform : QUOI envoyer à l'orchestrateur, run après run.

Ce fichier ne dit jamais ce que l'orchestrateur doit répondre : c'est
``dag_oracle.py`` qui le calcule en rejouant le code des DAGs, et
``generate_tests.py`` qui écrit ``terraform/tests/*.tftest.hcl`` avec les
assertions correspondantes (accepté + relecture du payload, ou refusé + motif).

Un scénario = un fichier ``.tftest.hcl`` = une suite de runs qui partagent le
même state ; chaque run donne les buckets qu'il ajoute ou modifie (clés de
``var.buckets`` de main.tf). Entre deux runs :

* un bucket accepté est **conservé tel quel** (pas de nouvelle demande) ;
* un bucket refusé à la création disparaît du run suivant ;
* un update refusé revient à son dernier payload accepté au run suivant ;
* ``drop`` retire un bucket (sa destruction est vérifiée).

Pour ajouter un cas : une ligne dans un ``Run``, puis
``python generate_tests.py`` (le test ``test_scenario_matrix.py`` échoue tant
que les fichiers générés ne sont pas à jour).
"""
from __future__ import annotations

from dataclasses import dataclass, field

from dag_oracle import UNKNOWN_COS_INSTANCE, UNKNOWN_VAULT_NAME


@dataclass
class Run:
    name: str
    buckets: dict[str, dict] = field(default_factory=dict)  # clé -> payload (ajout ou modification)
    drop: tuple[str, ...] = ()  # buckets retirés (destruction)
    note: str = ""  # commentaire au-dessus du run


@dataclass
class Scenario:
    number: int
    slug: str
    title: str
    doc: str
    runs: list[Run]

    @property
    def filename(self) -> str:
        return f"{self.number}_{self.slug}.tftest.hcl"


# --- raccourcis de payload -----------------------------------------------------

def days(minimum, default, maximum, **extra) -> dict:
    return {"minimum_days": minimum, "default_days": default, "maximum_days": maximum, **extra}


def years(minimum, default, maximum, **extra) -> dict:
    return {"minimum_years": minimum, "default_years": default, "maximum_years": maximum, **extra}


def legacy(minimum, default, maximum, **extra) -> dict:
    """Format historique (ADR 0001) : jours implicites, déprécié mais accepté."""
    return {"minimum": minimum, "default": default, "maximum": maximum, **extra}


def retention(bounds: dict, **extra) -> dict:
    return {"retention": bounds, **extra}


def lock_days(duration, choice: str | None = "object_lock_daily", versioning: bool = True, **extra) -> dict:
    payload = {"enable_versioning": versioning, "object_lock_duration_days": duration, **extra}
    if choice:
        payload["immutability_choice"] = choice
    return payload


def lock_years(duration, choice: str | None = "object_lock_yearly", versioning: bool = True, **extra) -> dict:
    payload = {"enable_versioning": versioning, "object_lock_duration_years": duration, **extra}
    if choice:
        payload["immutability_choice"] = choice
    return payload


def backup(retention_days, versioning: bool = True, **extra) -> dict:
    return {"enable_versioning": versioning, "backup_retention_days": retention_days, **extra}


VERSIONED = {"enable_versioning": True}
RET_DAYS = retention(days(1, 2, 3))
LOCK_DAILY = lock_days(1)


# --- la matrice -----------------------------------------------------------------

SCENARIOS: list[Scenario] = [
    Scenario(
        20, "bucket_lifecycle", "Cycle de vie d'un bucket standard",
        "Création de trois buckets sans immutabilité, mises à jour en place "
        "(versioning, permissions, retour en arrière du versioning), puis "
        "suppression d'un seul bucket pendant que les autres restent.",
        [
            Run("create", {
                "basic": {},
                "permissions": {"enable_custom_permissions": True},
                "versioned": VERSIONED,
            }),
            Run("update_enable_versioning", {"basic": {"enable_versioning": True}}),
            Run("update_custom_permissions", {"basic": {"enable_versioning": True, "enable_custom_permissions": True}}),
            Run("update_disable_versioning", {"versioned": {"enable_versioning": False}},
                note="Sans immutabilité, le versioning se désactive librement."),
            Run("delete_one_bucket", drop=("permissions",),
                note="cos.bucket.v1.delete sur un bucket vide ; les deux autres ne bougent pas."),
        ],
    ),
    Scenario(
        21, "storage_classes", "Classes de stockage",
        "Une création par classe (la classe n'est pas modifiable ensuite), "
        "puis une classe inconnue, refusée par le schéma du payload.",
        [
            Run("one_bucket_per_class", {
                "vault": {"storage_class": "vault"},
                "cold": {"storage_class": "cold"},
                "smart": {"storage_class": "smart"},
            }),
            Run("unknown_class", {"glacier": {"storage_class": "glacier"}}),
        ],
    ),
    Scenario(
        25, "immutability_after_create", "Immutabilité ajoutée après la création",
        "Un bucket créé sans immutabilité peut en recevoir une par update : "
        "rétention (bucket vide, sans versioning), object lock (versioning "
        "requis), sauvegarde (versioning requis, vault du scénario).",
        [
            Run("create_plain_buckets", {
                "to_retention": {},
                "to_object_lock": VERSIONED,
                "to_backup": VERSIONED,
            }),
            Run("add_retention", {"to_retention": RET_DAYS}),
            Run("add_object_lock", {"to_object_lock": LOCK_DAILY}),
            Run("add_backup", {"to_backup": backup(7)}),
        ],
    ),
    Scenario(
        30, "retention", "Rétention (ADR 0001) : jours, années, format historique",
        "Créations dans chaque unité et avec un choix explicite ; bornes "
        "fournies avec retention_enabled = false (bucket créé sans rétention) ; "
        "mises à jour des bornes, changement d'unité, borne partielle (les "
        "autres sont relues en base).",
        [
            Run("create", {
                "days": RET_DAYS,
                "years": retention(years(1, 2, 5)),
                "legacy": retention(legacy(1, 2, 3)),
                "daily_choice": retention(days(1, 2, 3), immutability_choice="retention_daily"),
                "yearly_choice": retention(years(1, 2, 3), immutability_choice="retention_yearly"),
                "disabled_flag": retention(days(1, 2, 3, retention_enabled=False)),
            }),
            Run("update_days_bounds", {"days": retention(days(1, 5, 10))}),
            Run("update_years_to_days", {"years": retention(days(1, 400, 800))},
                note="Les bornes en base sont en jours : on peut repasser en jours."),
            Run("update_legacy_bounds", {"legacy": retention(legacy(1, 5, 10))}),
            Run("update_partial_default", {"days": retention({"default_days": 7})},
                note="Seule la borne envoyée change, minimum et maximum sont relus en base."),
        ],
    ),
    Scenario(
        31, "retention_limits", "Rétention aux bornes acceptées",
        "Exactement 5 ans, 1826 jours (plafond en jours = 1826 ou 1827 selon "
        "les bissextiles, 1826 passe toujours), format historique au plafond, "
        "bornes égales. Puis un update quelconque du bucket aux bornes égales : "
        "la règle d'update (minimum < default < maximum, stricte) le refuse "
        "alors que la création l'avait accepté — incohérence à trancher côté DAG.",
        [
            Run("ceilings", {
                "five_years": retention(years(1, 5, 5)),
                "days_ceiling": retention(days(1, 1826, 1826)),
                "legacy_ceiling": retention(legacy(1, 1826, 1826)),
                "equal_bounds": retention(days(30, 30, 30)),
            }),
            Run("equal_bounds_then_permissions", {
                "equal_bounds": retention(days(30, 30, 30), enable_custom_permissions=True),
            }),
        ],
    ),
    Scenario(
        35, "retention_rules", "Rétention : refus à la création",
        "Chaque run envoie un payload de rétention invalide : plafond de 5 ans "
        "dans chaque format, unités mélangées, bornes incohérentes ou "
        "incomplètes, choix _daily/_yearly contredit par les valeurs, "
        "incompatibilités (versioning, object lock, sauvegarde).",
        [
            Run("days_over_five_years", {"r": retention(days(1, 1900, 1900))}),
            Run("years_over_five", {"r": retention(years(1, 6, 6))}),
            Run("legacy_over_five_years", {"r": retention(legacy(1, 1900, 1900))}),
            Run("days_and_years_mixed", {"r": retention({"minimum_days": 1, "default_days": 2, "maximum_years": 3})}),
            Run("legacy_and_days_mixed", {"r": retention({"minimum": 1, "default_days": 2, "maximum_days": 3})}),
            Run("legacy_and_years_mixed", {"r": retention({"minimum": 1, "default": 2, "maximum_years": 3})}),
            Run("minimum_above_maximum", {"r": retention(days(3, 2, 1))}),
            Run("default_below_minimum", {"r": retention(days(5, 2, 10))}),
            Run("default_above_maximum", {"r": retention(days(1, 20, 10))}),
            Run("zero_bound", {"r": retention(days(0, 1, 2))}),
            Run("negative_bound", {"r": retention(years(-1, 1, 2))}),
            Run("only_default", {"r": retention({"default_days": 2})}),
            Run("only_minimum_and_maximum", {"r": retention({"minimum_days": 1, "maximum_days": 3})}),
            Run("yearly_choice_with_days", {"r": retention(days(1, 2, 3), immutability_choice="retention_yearly")}),
            Run("daily_choice_with_years", {"r": retention(years(1, 2, 3), immutability_choice="retention_daily")}),
            Run("choice_without_values", {"r": {"immutability_choice": "retention"}}),
            Run("daily_choice_without_values", {"r": {"immutability_choice": "retention_daily"}}),
            Run("with_versioning", {"r": retention(days(1, 2, 3), enable_versioning=True)}),
            Run("with_object_lock", {"r": retention(days(1, 2, 3), object_lock_duration_days=1)}),
            Run("with_object_lock_and_versioning", {"r": retention(days(1, 2, 3), object_lock_duration_days=1, enable_versioning=True)}),
            Run("with_backup", {"r": retention(days(1, 2, 3), backup_retention_days=7)}),
        ],
    ),
    Scenario(
        40, "object_lock", "Object lock : jours, années, choix explicite ou déduit",
        "Créations avec chaque choix (daily, yearly, générique, déduit sans "
        "choix) et un payload yearly qui porte aussi des jours (ignorés) ; "
        "mises à jour de la durée, jusqu'au plafond, et changement d'unité.",
        [
            Run("create", {
                "daily": LOCK_DAILY,
                "yearly": lock_years(1),
                "generic_choice_days": lock_days(2, choice="object_lock"),
                "generic_choice_years": lock_years(2, choice="object_lock"),
                "inferred_years": lock_years(2, choice=None),
                "yearly_ignores_days": lock_years(1, object_lock_duration_days=1),
            }),
            Run("update_daily_duration", {"daily": lock_days(2)}),
            Run("update_yearly_ceiling", {"yearly": lock_years(5)}),
            Run("update_daily_to_years", {"daily": lock_years(1)}),
            Run("update_inferred_to_days", {"inferred_years": lock_days(3, choice=None)}),
        ],
    ),
    Scenario(
        45, "object_lock_rules", "Object lock : refus à la création",
        "Sans versioning, deux unités, durée nulle ou négative, au-delà de "
        "5 ans dans chaque unité, choix _daily/_yearly sans la durée attendue, "
        "choix générique sans durée.",
        [
            Run("without_versioning", {"r": lock_days(1, versioning=False)}),
            Run("both_units", {"r": lock_days(1, choice=None, object_lock_duration_years=1)}),
            Run("zero_days", {"r": lock_days(0)}),
            Run("negative_years", {"r": lock_years(-1)}),
            Run("days_over_five_years", {"r": lock_days(1900)}),
            Run("years_over_five", {"r": lock_years(6)}),
            Run("daily_choice_without_days", {"r": lock_years(1, choice="object_lock_daily")}),
            Run("yearly_choice_without_years", {"r": lock_days(1, choice="object_lock_yearly")}),
            Run("generic_choice_without_duration", {"r": {"enable_versioning": True, "immutability_choice": "object_lock"}}),
            Run("unknown_choice", {"r": {"enable_versioning": True, "immutability_choice": "object_lock_monthly"}}),
        ],
    ),
    Scenario(
        50, "backup", "Sauvegarde : vault, bucket sauvegardé, désactivation",
        "Le vault du scénario, un bucket sauvegardé, un bucket créé avec "
        "backup_enabled = false (équivaut à pas de sauvegarde) ; mise à jour "
        "de la rétention des sauvegardes, désactivation puis réactivation.",
        [
            Run("create", {
                "saved": backup(7),
                "disabled_flag": {"enable_versioning": True, "backup_enabled": False},
            }),
            Run("update_backup_retention", {"saved": backup(2)}),
            Run("disable_backup", {"saved": {"enable_versioning": True, "backup_enabled": False}}),
            Run("enable_backup_again", {"saved": backup(7)}),
        ],
    ),
    Scenario(
        55, "backup_rules", "Sauvegarde : refus",
        "Sans versioning, sans rétention de sauvegarde, vault inconnu, nom de "
        "vault vide, avec une rétention ; puis en update : désactiver la "
        "sauvegarde d'un bucket sans versioning, et rattacher un vault inconnu.",
        [
            Run("create_plain_buckets", {"plain": {}, "versioned": VERSIONED}),
            Run("without_versioning", {"r": backup(7, versioning=False)}),
            Run("without_retention_days", {"r": {"enable_versioning": True, "backup_enabled": True}}),
            Run("unknown_vault", {"r": backup(7, backup_vault_name=UNKNOWN_VAULT_NAME)}),
            Run("empty_vault_name", {"r": backup(7, backup_vault_name="")}),
            Run("with_retention_and_versioning", {"r": backup(7, retention=days(1, 2, 3))}),
            Run("disable_backup_without_versioning", {"plain": {"backup_enabled": False}}),
            Run("update_with_unknown_vault", {"versioned": backup(7, backup_vault_name=UNKNOWN_VAULT_NAME)}),
        ],
    ),
    Scenario(
        60, "update_rules", "Mises à jour d'un bucket déjà immuable",
        "Un bucket en rétention et un en object lock ; ce qui est refusé "
        "(l'autre immutabilité, le versioning, la sauvegarde, bornes égales, "
        "deux unités, 5 ans dépassés) et ce qui est accepté (bornes dans "
        "l'autre unité, retention_enabled = false sans effet, changement "
        "d'unité de l'object lock).",
        [
            Run("create", {"ret": RET_DAYS, "lock": LOCK_DAILY}),
            Run("ret_add_object_lock", {"ret": retention(days(1, 2, 3), object_lock_duration_days=1)}),
            Run("ret_enable_versioning", {"ret": retention(days(1, 2, 3), enable_versioning=True)}),
            Run("ret_add_backup", {"ret": retention(days(1, 2, 3), backup_retention_days=7)}),
            Run("ret_equal_bounds", {"ret": retention(days(5, 5, 5))}),
            Run("ret_bounds_in_years", {"ret": retention(years(1, 2, 3))}),
            Run("ret_disabled_flag_keeps_bounds", {"ret": retention(years(1, 2, 3, retention_enabled=False))},
                note="retention_enabled = false : les bornes en base sont conservées, rien ne change."),
            Run("lock_add_retention", {"lock": lock_days(1, retention=days(1, 2, 3))}),
            Run("lock_disable_versioning", {"lock": lock_days(1, versioning=False)}),
            Run("lock_both_units", {"lock": lock_days(1, object_lock_duration_years=1)}),
            Run("lock_over_five_years", {"lock": lock_days(1900)}),
            Run("lock_switch_to_years", {"lock": lock_years(1)}),
        ],
    ),
    Scenario(
        70, "context_rules", "Contexte de la demande",
        "Instance COS inconnue : refus par validate_request du DAG.",
        [
            Run("unknown_cos_instance", {"r": {"cos_instance": UNKNOWN_COS_INSTANCE}}),
        ],
    ),
]
