"""Ligne ``lifecycle_policy_rule`` : une règle d'expiration posée sur un bucket."""
import logging

from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer, Sequence, Text
from sqlalchemy.orm import relationship

from cos_service.models._dict import row_to_dict
from cos_service.models.base import Base, BigId

logger = logging.getLogger(__name__)


class LifecyclePolicyRule(Base):
    __tablename__ = "lifecycle_policy_rule"

    id = Column(BigId, Sequence("lifecycle_policy_rule_id_seq"), primary_key=True)
    bucket_subscription_id = Column(Text, ForeignKey("bucket.subscription_id"))
    rule_id = Column(Text, nullable=True)
    filter_prefix = Column(Text, nullable=True)
    status = Column(Text, nullable=True)
    expiration_days = Column(Integer, nullable=True)
    non_current_version_expiration_days = Column(Integer, nullable=True)
    created_at = Column(TIMESTAMP, nullable=True)
    created_by = Column(Text, nullable=True)
    deleted_at = Column(TIMESTAMP, nullable=True)
    deleted_by = Column(Text, nullable=True)

    # un bucket porte plusieurs règles : ``bucket.lifecycle_policy_rule`` est une liste
    bucket = relationship("Bucket", uselist=False, backref="lifecycle_policy_rule")

    def __init__(self, **columns):
        """Colonnes en mots-clés uniquement ; ce qui n'est pas donné reste à None."""
        super().__init__(**columns)

    def to_dict(self) -> dict:
        return row_to_dict(self)

    def __iter__(self):
        yield from self.to_dict().items()

    def __repr__(self) -> str:
        return f"<LifecyclePolicyRule {self.id} rule={self.rule_id} status={self.status}>"
