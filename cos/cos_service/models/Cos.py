"""Ligne ``cos_instance`` : une instance COS et ce que l'orchestrateur en sait.

Colonnes en ``Text`` comme la base réelle (tables historiques créées par les
scripts SQL manuels, jamais en ``varchar(n)``). Constructeur par mots-clés,
``to_dict()`` construit depuis les colonnes, relation absente = None.
"""
import logging

from sqlalchemy import TIMESTAMP, Column, Text
from sqlalchemy.orm import relationship

from cos_service.models._dict import row_to_dict
from cos_service.models.base import Base

logger = logging.getLogger(__name__)

RELATIONS = ("context", "workspace")


class Cos(Base):
    __tablename__ = "cos_instance"

    subscription_id = Column(Text, primary_key=True)
    name = Column(Text, nullable=True)
    action = Column(Text, nullable=True)
    crn = Column(Text, nullable=True)
    description = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP, nullable=False)
    created_by = Column(Text, nullable=False)
    updated_at = Column(TIMESTAMP, nullable=True)
    updated_by = Column(Text, nullable=True)
    status = Column(Text, nullable=False)
    plan = Column(Text, nullable=False)
    region = Column(Text, nullable=False)
    environment = Column(Text, nullable=False)

    context = relationship("Context", uselist=False, backref="cos_instance")
    workspace = relationship("Workspace", uselist=False, backref="cos_instance")

    def __init__(self, **columns):
        """Colonnes en mots-clés uniquement ; ce qui n'est pas donné reste à None."""
        super().__init__(**columns)

    def to_dict(self) -> dict:
        return row_to_dict(self, RELATIONS)

    def __iter__(self):
        yield from self.to_dict().items()

    def __repr__(self) -> str:
        return f"<Cos {self.subscription_id} {self.name!r} status={self.status}>"
