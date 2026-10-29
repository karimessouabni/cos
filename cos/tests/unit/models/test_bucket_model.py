"""Le modèle ``Bucket`` réel, sur SQLite en mémoire avec le vrai SQLAlchemy.

Le reste de la suite tourne sur des doublures de ``sqlalchemy`` et des modèles
(conftest) : cette fixture les écarte le temps du test et importe les vrais
modules depuis le disque. Tous les modèles sont réels : c'est aussi ce qui
prouve que les ``relationship("Nom")`` se résolvent quand les classes sont
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
    Cos, Workspace = models.Cos, models.Workspace
    BackupVault, BackupVaultRestore = models.BackupVault, models.BackupVaultRestore

    real_modules = {name: module for name, module in sys.modules.items()
                    if name == "sqlalchemy" or name.startswith(("sqlalchemy.", "cos_service.models"))}
    return {"modules": real_modules, "sqlalchemy": sqlalchemy, "orm": orm, "Base": models.Base, "Bucket": bucket_module.Bucket, "Cos": Cos, "Workspace": Workspace,
            "BackupVault": BackupVault, "BackupVaultRestore": BackupVaultRestore, "models": models}


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
    for key in ("Bucket", "Cos", "Workspace", "BackupVault", "BackupVaultRestore", "models"):
        setattr(namespace, key, _REAL[key])
    namespace.select = _REAL["sqlalchemy"].select
    namespace.session = _REAL["orm"].sessionmaker(bind=engine)()
    yield namespace
    namespace.session.close()
    engine.dispose()


def minimal_vault(models, **overrides):
    columns = dict(subscription_id="bv-sub", region="eu-de", status="created", created_at=datetime(2026, 10, 8, 9, 0),
                   created_by="karim", environment="dev")
    columns.update(overrides)
    return models.BackupVault(**columns)


def minimal_restore(models, **overrides):
    # ``id`` est donné : SQLite n'a pas de séquence et n'auto-incrémente pas une clé composite (PostgreSQL le fait)
    columns = dict(id=hash(overrides.get("subscription_id", "rst-1")) % 10_000, subscription_id="rst-1", backup_vault_subscription_id="bv-sub", backup_vault_name="vault-a",
                   target_bucket_crn="crn:bucket-b", recovery_range_id="rr-1", restore_point_in_time="2026-10-01T00:00:00Z",
                   restore_type="point_in_time", status="REQUESTED", action="restore", created_by="karim")
    columns.update(overrides)
    return models.BackupVaultRestore(**columns)


def minimal_cos(models, **overrides):
    columns = dict(subscription_id="cos-sub", name="cos-a", description="instance", created_at=datetime(2026, 10, 8, 8, 0),
                   created_by="karim", status="created", plan="standard", region="eu-de", environment="dev")
    columns.update(overrides)
    return models.Cos(**columns)


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
        cos = minimal_cos(m)
        vault = minimal_vault(m, crn="crn:bv")
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
        bucket.cos = minimal_cos(m)
        bucket.workspace = m.Workspace(workspace_id="ws-1")
        m.session.add(bucket)
        m.session.commit()

        data = bucket.to_dict()

        assert set(data) == {c.name for c in m.Bucket.__table__.columns} | {"cos", "workspace", "backup_vault"}
        assert data["workspace"]["workspace_id"] == "ws-1"  # manquait à l'original : les DAGs le lisent
        assert data["cos"]["subscription_id"] == "cos-sub" and data["cos"]["name"] == "cos-a"
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


class TestBackupVault:
    def test_keyword_only_and_nullable_name_action(self, real_models):
        vault = minimal_vault(real_models)

        assert vault.name is None and vault.action is None and vault.crn is None
        with pytest.raises(TypeError):
            real_models.BackupVault("bv-sub", "desc")

    def test_round_trip_and_dict_views(self, real_models):
        m = real_models
        vault = minimal_vault(m, name="vault-a", kms_crn="crn:kms")
        vault.cos = minimal_cos(m)
        m.session.add(vault)
        m.session.commit()

        row = m.session.execute(m.select(m.BackupVault).where(m.BackupVault.subscription_id == "bv-sub")).scalar_one()
        data = row.to_dict()

        assert set(data) == {c.name for c in m.BackupVault.__table__.columns} | {"cos", "workspace"}
        assert data["cos"]["name"] == "cos-a" and data["cos"]["context"] is None and data["workspace"] is None
        assert data["kms_crn"] == "crn:kms" and dict(row) == data
        assert repr(row) == "<BackupVault bv-sub 'vault-a' status=created>"

    def test_columns_are_text_like_the_pprod_schema(self, real_models):
        from sqlalchemy import JSON, Boolean, Integer, Text

        m = real_models.models
        legacy = (m.BackupVault, m.Bucket, m.BucketMigration, m.Context, m.Cos, m.CrossAccountServiceId,
                  m.LifecyclePolicyRule, m.Workspace)
        for model in legacy:
            for column in model.__table__.columns:
                if column.name.endswith(("_at", "_time")):
                    continue
                assert isinstance(column.type, (Text, Integer, Boolean, JSON)), f"{model.__tablename__}.{column.name}"

    def test_every_table_of_pprod_is_declared(self, real_models):
        assert set(real_models.models.Base.metadata.tables) == {
            "backup_vault", "backup_vault_restore", "bucket", "bucket_migration", "context", "cos_instance",
            "cross_account_service_id", "lifecycle_policy_rule", "workspace",
        }


class TestBackupVaultRestore:
    def test_created_at_is_evaluated_per_row(self, real_models, monkeypatch):
        """L'ancien ``default=datetime.now(...)`` figeait la date à l'import du module."""
        module = sys.modules["cos_service.models.BackupVaultRestore"]  # l'attribut du package est la classe

        clock = iter([datetime(2026, 10, 8, 10, 0), datetime(2026, 10, 8, 11, 0)])
        monkeypatch.setattr(module, "_now_paris", lambda: next(clock))
        m = real_models
        m.session.add(minimal_vault(m))
        m.session.add_all([minimal_restore(m, subscription_id="rst-1"),
                           minimal_restore(m, subscription_id="rst-2", target_bucket_crn="crn:bucket-c")])
        m.session.commit()

        rows = m.session.execute(m.select(m.BackupVaultRestore).order_by(m.BackupVaultRestore.subscription_id)).scalars().all()

        assert [r.created_at for r in rows] == [datetime(2026, 10, 8, 10, 0), datetime(2026, 10, 8, 11, 0)]

    def test_same_request_twice_is_refused(self, real_models):
        from sqlalchemy.exc import IntegrityError

        m = real_models
        m.session.add(minimal_vault(m))
        m.session.add(minimal_restore(m, subscription_id="rst-1"))
        m.session.commit()
        m.session.add(minimal_restore(m, subscription_id="rst-2"))

        with pytest.raises(IntegrityError):
            m.session.commit()
        m.session.rollback()

    def test_to_dict_serialises_dates_and_relation_works(self, real_models):
        m = real_models
        vault = minimal_vault(m, name="vault-a")
        m.session.add(vault)
        m.session.add(minimal_restore(m, created_at=datetime(2026, 10, 8, 10, 0), error_cause=None))
        m.session.commit()

        row = m.session.execute(m.select(m.BackupVaultRestore)).scalar_one()
        data = row.to_dict()

        assert data["created_at"] == "2026-10-08T10:00:00" and data["completed_at"] is None
        assert set(data) == {c.name for c in m.BackupVaultRestore.__table__.columns}
        assert row.backup_vault.name == "vault-a" and vault.restores == [row]
        assert dict(row) == data

    def test_index_and_unique_constraint_are_declared(self, real_models):
        table = real_models.BackupVaultRestore.__table__

        assert {i.name for i in table.indexes} == {"idx_restore_target_bucket"}
        assert "uq_restore_request" in {c.name for c in table.constraints}


def test_the_stubs_are_back_after_the_model_test():
    """Le reste de la suite doit retrouver ses doublures."""
    import sqlalchemy

    assert not hasattr(sqlalchemy, "create_engine")
