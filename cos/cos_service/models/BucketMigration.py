"""Ligne ``bucket_migration`` : une migration de bucket entre deux instances COS,
planifiée par cron et exécutée dans un namespace OpenShift."""
import logging

from sqlalchemy import JSON, TIMESTAMP, Column, ForeignKey, Integer, Text
from sqlalchemy.orm import relationship

from cos_service.models._dict import row_to_dict
from cos_service.models.base import Base

logger = logging.getLogger(__name__)

RELATIONS = ("cross_account_sid", "workspace")


class BucketMigration(Base):
    __tablename__ = "bucket_migration"

    subscription_id = Column(Text, primary_key=True)
    cos_instance_source = Column(Text, nullable=True)
    bucket_source = Column(Text, nullable=True)
    cos_instance_backup = Column(Text, nullable=True)
    bucket_backup = Column(Text, nullable=True)
    oc_namespace = Column(Text, nullable=True)
    oc_cluster = Column(Text, nullable=True)
    app_code = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    action = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, nullable=True)
    created_by = Column(Text, nullable=True)
    updated_at = Column(TIMESTAMP, nullable=True)
    updated_by = Column(Text, nullable=True)
    region = Column(Text, nullable=True)
    environment = Column(Text, nullable=True)
    realm = Column(Text, nullable=True)
    ecosystem = Column(Text, nullable=True)
    status = Column(Text, nullable=True)
    cron_expression = Column(Text, nullable=True)
    initial_cron_schedule_date_time = Column(Text, nullable=True)  # stockée en texte en base
    sync_history = Column(JSON, nullable=True)
    cross_account_sid_api_key_vault_path = Column(Text, nullable=True)
    cross_account_sid_workspace_id = Column(Text, nullable=True)
    pod_ram_size = Column(Integer, nullable=True)

    cross_account_sid_sub_id = Column(Text, ForeignKey("cross_account_service_id.subscription_id"))
    cross_account_sid = relationship("CrossAccountServiceId", uselist=False, backref="bucket_migration")
    workspace = relationship("Workspace", uselist=False, backref="bucket_migration")

    def __init__(self, **columns):
        """Colonnes en mots-clés uniquement ; ce qui n'est pas donné reste à None."""
        super().__init__(**columns)

    def to_dict(self) -> dict:
        return row_to_dict(self, RELATIONS)

    def __iter__(self):
        yield from self.to_dict().items()

    def __repr__(self) -> str:
        return f"<BucketMigration {self.subscription_id} {self.bucket_source!r} status={self.status}>"
