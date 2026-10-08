"""Le modèle ``Bucket`` réel, sur SQLite en mémoire avec le vrai SQLAlchemy.

Le reste de la suite tourne sur des doublures de ``sqlalchemy`` et des modèles
(conftest) : cette fixture les écarte le temps du test et importe les vrais
modules depuis le disque. Les modèles liés absents du dépôt (Cos, Workspace,
BackupVault) sont déclarés ici a minima, sur la même Base : c'est aussi ce
qui prouve que les ``relationship("Nom")`` se résolvent quand les classes sont
chargées.
"""
import importlib
import sys
from datetime import datetime, timezone

import pytest

pytest.importorskip("sqlalchemy", reason="SQLAlchemy réel absent : pip install -r requirements-test.txt")


_REAL: dict = {}  # modules réels et modèles de test, importés une seule fois par processus


def _load_real_models() -> dict:
    """SQLAlchemy ne se réimporte pas proprement deux fois dans un processus
    (extensions compilées) : les vrais modules et les modèles de test sont
    chargés une fois, puis réinstallés dans sys.modules à chaque test."""
    sqlalchemy = importlib.import_module("sqlalchemy")
    orm = importlib.import_module("sqlalchemy.orm")
    models = importlib.import_module("cos_service.models")
    bucket_module = importlib.import_module("cos_service.models.Bucket")
    Base = models.Base
    Column, String, ForeignKey = sqlalchemy.Column, sqlalchemy.String, sqlalchemy.ForeignKey

    class Cos(Base):
        __tablename__ = "cos_instance"
        subscription_id = Column(String(50), primary_key=True)
        name = Column(String(50))

        def to_dict(self):
            return {"subscription_id": self.subscription_id, "name": self.name}

    class Workspace(Base):
        __tablename__ = "workspace"
        bucket_subscription_id = Column(String(50), ForeignKey("bucket.subscription_id"), primary_key=True)
        workspace_id = Column(String(50))

        def __iter__(self):  # comme le vrai : dict(workspace)
            yield "workspace_id", self.workspace_id

    class BackupVault(Base):
        __tablename__ = "backup_vault"
        subscription_id = Column(String(50), primary_key=True)
        crn = Column(String(255))

        def to_dict(self):
            return {"subscription_id": self.subscription_id, "crn": self.crn}

    real_modules = {name: module for name, module in sys.modules.items()
                    if name == "sqlalchemy" or name.startswith(("sqlalchemy.", "cos_service.models"))}
    return {"modules": real_modules, "sqlalchemy": sqlalchemy, "orm": orm, "Base": Base,
            "Bucket": bucket_module.Bucket, "Cos": Cos, "Workspace": Workspace, "BackupVault": BackupVault}


@pytest.fixture
def real_models(monkeypatch):
    """Vrais ``sqlalchemy`` et ``cos_service.models`` le temps du test ; les doublures reviennent après."""
    for name in [n for n in sys.modules if n == "sqlalchemy" or n.startswith(("sqlalchemy.", "cos_service.models"))]:
        monkeypatch.delitem(sys.modules, name)
    if not _REAL:
        _REAL.update(_load_real_models())
    for name, module in _REAL["modules"].items():
        monkeypatch.setitem(sys.modules, name, module)

    engine = _REAL["sqlalchemy"].create_engine("sqlite://")
    _REAL["Base"].metadata.create_all(engine)
    namespace = type("Models", (), {})()
    for key in ("Bucket", "Cos", "Workspace", "BackupVault"):
        setattr(namespace, key, _REAL[key])
    namespace.select = _REAL["sqlalchemy"].select
    namespace.session = _REAL["orm"].sessionmaker(bind=engine)()
    yield namespace
    namespace.session.close()
    engine.dispose()


def minimal_bucket(models, **overrides):
    columns = dict(
        subscription_id="sub-1", name="bucket-a", storage_class="standard", region="eu-de", action="apply",
        status="creating", created_at=datetime(2026, 10, 8, 10, 0), created_by="karim",
        virtual_server_endpoint="vip", environment="dev", retention_enabled=False,
        enable_custom_permissions=False, has_expiration_rule=False, object_versioning_enabled=False,
        backup_enabled=False,
    )
    columns.update(overrides)
    return models.Bucket(**columns)


class TestConstruction:
    def test_keyword_only_with_the_rest_left_to_none(self, real_models):
        bucket = minimal_bucket(real_models)

        assert bucket.clean_status is None and bucket.bucket_crn is None and bucket.retention_unit is None
        assert bucket.clean_requested_at is None and bucket.clean_cbr_workspace_id is None

    def test_positional_arguments_are_refused(self, real_models):
        with pytest.raises(TypeError):
            real_models.Bucket("sub-1", "desc")

    def test_unknown_column_is_refused(self, real_models):
        with pytest.raises(TypeError):
            minimal_bucket(real_models, retention_default_days=3)


class TestRoundTrip:
    def test_insert_and_read_back_with_relations(self, real_models):
        m = real_models
        cos = m.Cos(subscription_id="cos-sub", name="cos-a")
        vault = m.BackupVault(subscription_id="bv-sub", crn="crn:bv")
        bucket = minimal_bucket(
            m, retention_enabled=True, retention_default=365, retention_minimum=365, retention_maximum=730,
            retention_unit="years", clean_status="scheduled",
            clean_requested_at=datetime(2026, 10, 8, 10, 0, tzinfo=timezone.utc),
            clean_execute_at=datetime(2026, 10, 15, 10, 0, tzinfo=timezone.utc),
            clean_cbr_workspace_id="ws-cbr-1",
        )
        bucket.cos, bucket.backup_vault = cos, vault
        bucket.workspace = m.Workspace(workspace_id="ws-1")
        m.session.add(bucket)
        m.session.commit()

        row = m.session.execute(m.select(m.Bucket).where(m.Bucket.subscription_id == "sub-1")).scalar_one()

        assert (row.retention_default, row.retention_unit) == (365, "years")
        assert row.clean_status == "scheduled" and row.clean_cbr_workspace_id == "ws-cbr-1"
        assert row.clean_execute_at.replace(tzinfo=timezone.utc) == datetime(2026, 10, 15, 10, 0, tzinfo=timezone.utc)
        assert row.cos.name == "cos-a" and row.backup_vault.crn == "crn:bv" and row.workspace.workspace_id == "ws-1"


class TestDictViews:
    def test_to_dict_has_every_column_and_every_relation(self, real_models):
        m = real_models
        bucket = minimal_bucket(m, description="my bucket")
        bucket.cos = m.Cos(subscription_id="cos-sub", name="cos-a")
        bucket.workspace = m.Workspace(workspace_id="ws-1")
        m.session.add(bucket)
        m.session.commit()

        data = bucket.to_dict()

        assert set(data) == {c.name for c in m.Bucket.__table__.columns} | {"cos", "workspace", "backup_vault"}
        assert data["workspace"] == {"workspace_id": "ws-1"}  # manquait à l'original : les DAGs le lisent
        assert data["cos"] == {"subscription_id": "cos-sub", "name": "cos-a"}
        assert data["backup_vault"] is None
        assert data["created_by"] == "karim" and data["expiration_rule_created_at"] is None
        assert data["cos_subscription_id"] == "cos-sub"

    def test_missing_relations_give_none_not_an_error(self, real_models):
        data = minimal_bucket(real_models).to_dict()

        assert (data["cos"], data["workspace"], data["backup_vault"]) == (None, None, None)

    def test_iter_and_dict_are_the_same_view(self, real_models):
        bucket = minimal_bucket(real_models)

        assert dict(bucket) == bucket.to_dict()

    def test_repr_is_short_and_useful(self, real_models):
        assert repr(minimal_bucket(real_models, clean_status="scheduled")) == "<Bucket sub-1 'bucket-a' status=creating clean=scheduled>"


def test_the_stubs_are_back_after_the_model_test():
    """Le reste de la suite doit retrouver ses doublures."""
    import sqlalchemy

    assert not hasattr(sqlalchemy, "create_engine")
