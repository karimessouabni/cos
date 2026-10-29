"""Ligne ``backup_vault`` : un backup vault IBM et ce que l'orchestrateur en sait.

Aligné sur le schéma réel de pprod (``alembic check`` contre une copie de la
base, 2026-10-28) plutôt que sur l'ancien modèle :

- les colonnes sont en ``Text`` : la base n'a jamais eu de ``varchar(n)``, et
  garder ``String(20)`` dans le modèle faisait générer à chaque autogenerate des
  ``ALTER COLUMN ... TYPE VARCHAR`` que personne ne voulait en prod ;
- ``name`` et ``action`` acceptent NULL : l'ancien constructeur les écrivait à
  None à l'insertion, la base les accepte donc déjà ;
- même ergonomie que ``Bucket`` : constructeur par mots-clés, ``to_dict()``
  construit depuis les colonnes, ``__iter__`` le rejoue, relation absente = None.

La clé étrangère ``cos_subscription_id -> cos_instance`` est déclarée ici mais
n'existe pas encore en pprod : elle fait partie de la première révision Alembic
réellement exécutée, après vérification qu'aucune ligne n'est orpheline.
"""
import logging

from sqlalchemy import TIMESTAMP, Column, ForeignKey, Text
from sqlalchemy.orm import relationship

from cos_service.models._dict import row_to_dict
from cos_service.models.base import Base

logger = logging.getLogger(__name__)

RELATIONS = ("cos", "workspace")


class BackupVault(Base):
    __tablename__ = "backup_vault"

    subscription_id = Column(Text, primary_key=True)
    name = Column(Text, nullable=True)
    crn = Column(Text, nullable=True)
    id = Column(Text, nullable=True)
    region = Column(Text, nullable=False)
    action = Column(Text, nullable=True)
    status = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    kms_crn = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, nullable=False)
    created_by = Column(Text, nullable=False)
    updated_at = Column(TIMESTAMP, nullable=True)
    updated_by = Column(Text, nullable=True)
    cos_subscription_id = Column(Text, ForeignKey("cos_instance.subscription_id"))
    environment = Column(Text, nullable=False)

    cos = relationship("Cos", uselist=False, backref="backup_vault")
    workspace = relationship("Workspace", uselist=False, backref="backup_vault")

    def __init__(self, **columns):
        """Colonnes en mots-clés uniquement ; ce qui n'est pas donné reste à None."""
        super().__init__(**columns)

    def to_dict(self) -> dict:
        return row_to_dict(self, RELATIONS)

    def __iter__(self):
        yield from self.to_dict().items()

    def __repr__(self) -> str:
        return f"<BackupVault {self.subscription_id} {self.name!r} status={self.status}>"
