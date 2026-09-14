"""Doublures des modules ``cos_service.schemas`` absents de ce dépôt.

Elles ne sont installées par ``conftest.py`` QUE si le vrai module ne peut pas
être importé : sur le projet complet, les tests tournent contre les vrais
schémas. Les interfaces sont déduites de leur usage dans
``immutability_service.py`` et dans le DAG de création.
"""
from dataclasses import dataclass
from enum import Enum

DAYS = "days"
YEARS = "years"
MAX_RETENTION_YEARS = 5
_RETENTION_KEYS = ("default", "minimum", "maximum")


def max_retention(unit: str) -> int:
    """Plafond de rétention exprimé dans l'unité demandée."""
    return MAX_RETENTION_YEARS if unit == YEARS else MAX_RETENTION_YEARS * 365


@dataclass
class BucketRetention:
    retention_enabled: bool | None = None
    default_days: int | None = None
    minimum_days: int | None = None
    maximum_days: int | None = None
    default_years: int | None = None
    minimum_years: int | None = None
    maximum_years: int | None = None

    @property
    def unit(self) -> str | None:
        if any(getattr(self, f"{key}_{DAYS}") is not None for key in _RETENTION_KEYS):
            return DAYS
        if any(getattr(self, f"{key}_{YEARS}") is not None for key in _RETENTION_KEYS):
            return YEARS
        return None

    def _in_unit(self, key: str):
        unit = self.unit
        return None if unit is None else getattr(self, f"{key}_{unit}")

    @property
    def default(self):
        return self._in_unit("default")

    @property
    def minimum(self):
        return self._in_unit("minimum")

    @property
    def maximum(self):
        return self._in_unit("maximum")

    def is_empty(self) -> bool:
        return all(
            getattr(self, f"{key}_{unit}") is None
            for key in _RETENTION_KEYS
            for unit in (DAYS, YEARS)
        )

    def in_days(self, key: str) -> int | None:
        days = getattr(self, f"{key}_{DAYS}")
        if days is not None:
            return days
        years = getattr(self, f"{key}_{YEARS}")
        return None if years is None else years * 365


@dataclass
class BucketBackup:
    backup_enabled: bool | None = None
    backup_vault_name: str | None = None
    backup_retention_days: int | None = None
    backup_vault_sub_id: str | None = None

    def is_empty(self) -> bool:
        return (
            self.backup_enabled is None
            and self.backup_vault_name is None
            and self.backup_retention_days is None
        )


class Status(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    INPROGRESS = "in_progress"


class SubscriptionStatus(str, Enum):
    ACTIVE = "active"
    CREATING = "creating"
    LOCKED = "locked"
