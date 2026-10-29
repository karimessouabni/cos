"""Ligne ``context`` : le contexte client (realm, app code, comptes BUHUB / WKLAPP)
d'une instance COS. ``to_dict()`` expose désormais toutes les colonnes, ``id`` et
``cos_subscription_id`` compris (l'original les omettait)."""
import logging

from sqlalchemy import Column, ForeignKey, Sequence, Text

from cos_service.models._dict import row_to_dict
from cos_service.models.base import Base, BigId

logger = logging.getLogger(__name__)


class Context(Base):
    __tablename__ = "context"

    id = Column(BigId, Sequence("context_id_seq"), primary_key=True)
    cos_subscription_id = Column(Text, ForeignKey("cos_instance.subscription_id"))
    realm = Column(Text, nullable=True)
    app_code = Column(Text, nullable=True)
    code_bu = Column(Text, nullable=True)
    buhub_account_id = Column(Text, nullable=True)
    buhub_account_number = Column(Text, nullable=True)
    buhub_account_name = Column(Text, nullable=True)
    wklapp_account_id = Column(Text, nullable=True)
    wklapp_account_number = Column(Text, nullable=True)
    wklapp_account_name = Column(Text, nullable=True)

    def __init__(self, **columns):
        """Colonnes en mots-clés uniquement ; ce qui n'est pas donné reste à None."""
        super().__init__(**columns)

    def to_dict(self) -> dict:
        return row_to_dict(self)

    def __iter__(self):
        yield from self.to_dict().items()

    def __repr__(self) -> str:
        return f"<Context {self.id} realm={self.realm} app_code={self.app_code}>"
