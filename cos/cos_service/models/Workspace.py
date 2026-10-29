"""Ligne ``workspace`` : l'espace Schematics qui porte le Terraform d'une
souscription (instance, bucket, migration, service id, backup vault : une
clé étrangère par type, une seule renseignée).

- colonnes en ``Text`` comme la base réelle ;
- ``action_id`` : présente en pprod, absente de l'ancien modèle. Déclarée ici
  pour qu'Alembic ne propose plus de la supprimer ; à retirer par une révision
  dédiée si personne ne la lit ;
- ``inputs`` (JSON) est dans ``to_dict()``, l'original l'oubliait.
"""
import logging

from sqlalchemy import JSON, Column, ForeignKey, Sequence, Text

from cos_service.models._dict import row_to_dict
from cos_service.models.base import Base, BigId

logger = logging.getLogger(__name__)


class Workspace(Base):
    __tablename__ = "workspace"

    id = Column(BigId, Sequence("workspace_id_seq"), primary_key=True)
    workspace_id = Column(Text, nullable=True)
    cos_subscription_id = Column(Text, ForeignKey("cos_instance.subscription_id"))
    bucket_subscription_id = Column(Text, ForeignKey("bucket.subscription_id"))
    bucket_migration_subscription_id = Column(Text, ForeignKey("bucket_migration.subscription_id"))
    cross_account_service_id_subscription_id = Column(Text, ForeignKey("cross_account_service_id.subscription_id"))
    backup_vault_subscription_id = Column(Text, ForeignKey("backup_vault.subscription_id"))
    name = Column(Text, nullable=True)
    action = Column(Text, nullable=True)
    action_id = Column(Text, nullable=True)
    location = Column(Text, nullable=True)
    version_type = Column(Text, nullable=True)
    status = Column(Text, nullable=True)
    type = Column(Text, nullable=True)
    git_repo = Column(Text, nullable=True)
    inputs = Column(JSON, nullable=True)

    def __init__(self, **columns):
        """Colonnes en mots-clés uniquement ; ce qui n'est pas donné reste à None."""
        super().__init__(**columns)

    def to_dict(self) -> dict:
        return row_to_dict(self)

    def __iter__(self):
        yield from self.to_dict().items()

    def __repr__(self) -> str:
        return f"<Workspace {self.id} {self.workspace_id!r} status={self.status}>"
