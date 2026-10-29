"""Ligne ``cross_account_service_id`` : le service id IBM partagé entre deux
comptes, et le chemin Vault de sa clé d'API."""
import logging

from sqlalchemy import TIMESTAMP, Column, Text
from sqlalchemy.orm import relationship

from cos_service.models._dict import row_to_dict
from cos_service.models.base import Base

logger = logging.getLogger(__name__)

RELATIONS = ("workspace",)


class CrossAccountServiceId(Base):
    __tablename__ = "cross_account_service_id"

    subscription_id = Column(Text, primary_key=True)
    cos_instance_source = Column(Text, nullable=True)
    cos_instance_backup = Column(Text, nullable=True)
    action = Column(Text, nullable=True)
    vault_api_key_path = Column(Text, nullable=True)
    secret_name = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, nullable=True)
    created_by = Column(Text, nullable=True)
    status = Column(Text, nullable=True)
    region = Column(Text, nullable=True)
    environment = Column(Text, nullable=True)

    workspace = relationship("Workspace", uselist=False, backref="cross_account_service_id")

    def __init__(self, **columns):
        """Colonnes en mots-clés uniquement ; ce qui n'est pas donné reste à None."""
        super().__init__(**columns)

    def to_dict(self) -> dict:
        return row_to_dict(self, RELATIONS)

    def __iter__(self):
        yield from self.to_dict().items()

    def __repr__(self) -> str:
        return f"<CrossAccountServiceId {self.subscription_id} status={self.status}>"
