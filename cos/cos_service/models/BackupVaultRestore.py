"""Ligne ``backup_vault_restore`` : une demande de restauration d'un bucket
depuis un backup vault, et son avancement.

Table récente, créée depuis ce modèle : les ``String(n)`` correspondent bien
aux ``varchar(n)`` de pprod et sont conservés. Corrections par rapport au
modèle d'origine :

- ``created_at`` avait ``default=datetime.now(...)`` : la valeur était calculée
  une seule fois, à l'import du module, et toutes les lignes recevaient la même
  date. Le défaut est maintenant un appelable, évalué à chaque insertion ;
- plus de dépendance à ``pytz`` (absente de ``pyproject.toml``) : ``zoneinfo``
  de la bibliothèque standard ;
- ``RestoreStatus`` est importé dans les helpers, pas en tête de module : le
  modèle se charge même quand les schémas ne sont pas sur le chemin.

L'index ``idx_restore_target_bucket`` est déclaré ici mais absent de pprod :
il fait partie de la première révision Alembic réellement exécutée.
"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import TIMESTAMP, Column, ForeignKey, Index, Integer, Sequence, String, UniqueConstraint
from sqlalchemy.orm import relationship

from cos_service.models.base import Base

logger = logging.getLogger(__name__)

PARIS = ZoneInfo("Europe/Paris")


def _now_paris() -> datetime:
    """Heure de Paris, sans fuseau : la colonne est ``timestamp without time zone``."""
    return datetime.now(PARIS).replace(tzinfo=None)


class BackupVaultRestore(Base):
    __tablename__ = "backup_vault_restore"

    id = Column(Integer, Sequence("backup_vault_restore_id_seq"), primary_key=True)
    subscription_id = Column(String(50), nullable=False, primary_key=True)

    # lien avec le backup vault
    backup_vault_subscription_id = Column(String(50), ForeignKey("backup_vault.subscription_id"), nullable=False)
    backup_vault_name = Column(String(100), nullable=False)

    # identifiant IBM de la restauration
    restore_id = Column(String(255), nullable=True)

    # cible
    target_bucket_crn = Column(String(255), nullable=False)
    target_bucket_name = Column(String(100), nullable=True)
    src_bucket_name = Column(String(100), nullable=True)

    # logique restore
    recovery_range_id = Column(String(255), nullable=False)
    restore_point_in_time = Column(String(100), nullable=False)
    restore_type = Column(String(50), nullable=False)

    # état, aligné sur le vault : status + action
    status = Column(String(50), nullable=False)
    action = Column(String(50), nullable=False)

    # erreurs
    error_cause = Column(String(500), nullable=True)

    # audit
    created_at = Column(TIMESTAMP, nullable=False, default=lambda: _now_paris())
    created_by = Column(String(50), nullable=False)
    updated_at = Column(TIMESTAMP, nullable=True)
    updated_by = Column(String(50), nullable=True)
    completed_at = Column(TIMESTAMP, nullable=True)

    backup_vault = relationship("BackupVault", backref="restores")

    __table_args__ = (
        # idempotence : pas deux fois la même demande de restauration
        UniqueConstraint(
            "backup_vault_name", "recovery_range_id", "restore_point_in_time", "target_bucket_crn",
            name="uq_restore_request",
        ),
        # recherche des restaurations par bucket cible
        Index("idx_restore_target_bucket", "target_bucket_crn"),
    )

    def __init__(self, **columns):
        """Colonnes en mots-clés uniquement ; ce qui n'est pas donné reste à None."""
        super().__init__(**columns)

    # --------- helpers ---------
    def get_status_enum(self):
        from cos_service.schemas.restore_status import RestoreStatus  # schémas hors du chemin dans certains tests

        return RestoreStatus(self.status)

    def is_active(self) -> bool:
        return self.get_status_enum().is_active()

    def is_finished(self) -> bool:
        return self.get_status_enum().is_finished()

    def to_dict(self) -> dict:
        def serialize(value):
            return value.isoformat() if isinstance(value, datetime) else value

        return {column.name: serialize(getattr(self, column.name)) for column in self.__table__.columns}

    def __iter__(self):
        yield from self.to_dict().items()

    def __repr__(self) -> str:
        return f"<BackupVaultRestore {self.id} {self.subscription_id} status={self.status}>"
