"""Modèles SQLAlchemy.

Importer ici chaque modèle : SQLAlchemy résout les ``relationship("Nom")`` par
nom à la première requête, et ne trouve que les classes déjà importées. Un DAG
qui n'importe qu'un seul service plantait sinon ("expression 'Context' failed
to locate a name"). Compléter avec les modèles absents de ce dépôt :
BackupVault, BackupVaultRestore, Context, Cos, LifecyclePolicyRule, Workspace.
"""
from cos_service.models.base import Base
from cos_service.models.Bucket import Bucket

__all__ = ["Base", "Bucket"]
