"""Modèles SQLAlchemy.

Importer ici chaque modèle : SQLAlchemy résout les ``relationship("Nom")`` par
nom à la première requête, et ne trouve que les classes déjà importées. Un DAG
qui n'importe qu'un seul service plantait sinon ("expression 'Context' failed
to locate a name"). Tous les modèles du schéma ``cos_service`` sont dans ce dépôt ; ``Base.metadata``
est ce qu'Alembic compare à la base (``alembic/env.py``).
"""
from cos_service.models.base import Base
from cos_service.models.BackupVault import BackupVault
from cos_service.models.BackupVaultRestore import BackupVaultRestore
from cos_service.models.Bucket import Bucket
from cos_service.models.BucketMigration import BucketMigration
from cos_service.models.Context import Context
from cos_service.models.Cos import Cos
from cos_service.models.CrossAccountServiceId import CrossAccountServiceId
from cos_service.models.LifecyclePolicyRule import LifecyclePolicyRule
from cos_service.models.Workspace import Workspace

__all__ = [
    "Base",
    "BackupVault",
    "BackupVaultRestore",
    "Bucket",
    "BucketMigration",
    "Context",
    "Cos",
    "CrossAccountServiceId",
    "LifecyclePolicyRule",
    "Workspace",
]
