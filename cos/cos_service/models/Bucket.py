"""Ligne ``bucket`` : un bucket COS et ce que l'orchestrateur en sait.

Améliorations par rapport au modèle d'origine :

- plus de constructeur à 23 arguments positionnels : le constructeur
  déclaratif prend les colonnes en mots-clés, les autres restent à None ;
- ``to_dict()`` est l'unique source, construite depuis les colonnes de la
  table : ``__iter__`` la rejoue. L'original en avait deux, divergentes
  (``workspace`` et ``expiration_rule_created_at`` manquaient à ``to_dict``,
  ``created_by`` à ``__iter__``, et ``dict(self.workspace)`` plantait sur un
  bucket sans workspace) ;
- une relation absente vaut None, jamais une exception ;
- colonnes de la période de grâce du clean (ADR 0003) et unité de rétention
  saisie par le client (ADR 0001) ;
- colonnes texte en ``Text`` : la base réelle (tables créées par les scripts SQL
  manuels) n'a jamais eu de ``varchar(n)``, et ``String(n)`` dans le modèle
  faisait générer à chaque autogenerate des ``ALTER COLUMN ... TYPE`` indésirables.
"""
import logging

from sqlalchemy import TIMESTAMP, Boolean, Column, ForeignKey, Integer, Text
from sqlalchemy.orm import relationship

from cos_service.models._dict import row_to_dict
from cos_service.models.base import Base

logger = logging.getLogger(__name__)

RELATIONS = ("cos", "workspace", "backup_vault")


class Bucket(Base):
    __tablename__ = "bucket"

    subscription_id = Column(Text, nullable=False, primary_key=True)
    name = Column(Text, nullable=False)
    bucket_crn = Column(Text, nullable=True)
    storage_class = Column(Text, nullable=False)
    region = Column(Text, nullable=False)
    action = Column(Text, nullable=False)
    status = Column(Text, nullable=False)
    # Déclarée NOT NULL dans l'original alors que son constructeur l'écrivait à
    # None à l'insertion : aligné sur ce que la base accepte réellement.
    clean_status = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    activity_tracker_crn = Column(Text, nullable=True)
    monitoring_crn = Column(Text, nullable=True)
    kms_crn = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, nullable=False)
    created_by = Column(Text, nullable=False)
    updated_at = Column(TIMESTAMP, nullable=True)
    updated_by = Column(Text, nullable=True)
    cos_subscription_id = Column(Text, ForeignKey("cos_instance.subscription_id"))
    cos = relationship("Cos", uselist=False, backref="bucket")
    workspace = relationship("Workspace", uselist=False, backref="bucket")
    virtual_server_endpoint = Column(Text, nullable=False)
    environment = Column(Text, nullable=False)

    # Rétention : le jour est l'unité canonique (ADR 0001). ``retention_unit``
    # garde l'unité saisie par le client ("days" / "years") pour la lui rendre.
    retention_enabled = Column(Boolean, nullable=False)
    retention_default = Column(Integer, nullable=True)
    retention_minimum = Column(Integer, nullable=True)
    retention_maximum = Column(Integer, nullable=True)
    retention_unit = Column(Text, nullable=True)

    enable_custom_permissions = Column(Boolean, nullable=False)
    object_lock_duration_days = Column(Integer, nullable=True)
    object_lock_duration_years = Column(Integer, nullable=True)
    has_expiration_rule = Column(Boolean, nullable=False)
    expiration_rule_created_at = Column(TIMESTAMP, nullable=True)
    object_versioning_enabled = Column(Boolean, nullable=False)
    backup_enabled = Column(Boolean, nullable=False)
    backup_vault_subscription_id = Column(Text, ForeignKey("backup_vault.subscription_id"))
    backup_vault = relationship("BackupVault", uselist=False, backref="bucket")
    backup_retention_days = Column(Integer, nullable=True)

    # Période de grâce du clean (ADR 0003) : dates de la demande et de
    # l'exécution, workspace Schematics de la quarantaine tant qu'elle est posée.
    clean_requested_at = Column(TIMESTAMP(timezone=True), nullable=True)
    clean_execute_at = Column(TIMESTAMP(timezone=True), nullable=True)
    clean_cbr_workspace_id = Column(Text, nullable=True)

    def __init__(self, **columns):
        """Colonnes en mots-clés uniquement ; ce qui n'est pas donné reste à None."""
        super().__init__(**columns)

    def to_dict(self) -> dict:
        """Toutes les colonnes, plus les relations en dict (None si absente).
        C'est ce que lisent les DAGs : ``bucket["workspace"]["workspace_id"]``…"""
        return row_to_dict(self, RELATIONS)

    def __iter__(self):
        yield from self.to_dict().items()

    def __repr__(self) -> str:
        return f"<Bucket {self.subscription_id} {self.name!r} status={self.status} clean={self.clean_status}>"
