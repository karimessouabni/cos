"""Doublures des modules ``cos_service.schemas`` absents de ce dépôt.

Elles ne sont installées par ``conftest.py`` QUE si le vrai module ne peut pas
être importé : sur le projet complet, les tests tournent contre les vrais
schémas. Les interfaces sont déduites de leur usage dans
``immutability_service.py`` et dans le DAG de création.
"""
from dataclasses import dataclass
from enum import Enum

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
    TERMINATING = "terminating"
    TERMINATED = "terminated"
