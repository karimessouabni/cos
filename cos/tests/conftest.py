"""Configuration pytest commune.

* met la racine ``cos/`` dans ``sys.path`` pour importer ``cos_service`` ;
* installe les doublures de ``tests/stubs`` pour chaque module externe ou
  déduit qui n'est pas importable dans l'environnement courant. Sur le projet
  complet, rien n'est remplacé et les tests tournent contre le vrai code.

Lancement :

    python -m pytest tests
"""
import importlib
import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.stubs import bp2i, orm as orm_stubs, schemas as schema_stubs  # noqa: E402

DAGS_DIR = ROOT / "cos_service" / "dags" / "bucket" / "v1"
DAG_PATH = DAGS_DIR / "cos.bucket.v1.create.py"


def _install_if_missing(name: str, module: types.ModuleType) -> None:
    try:
        importlib.import_module(name)
    except ImportError:
        sys.modules[name] = module


def _schema_module(name: str, **attrs) -> types.ModuleType:
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


for _name, _module in bp2i.build_modules().items():
    _install_if_missing(_name, _module)

_install_if_missing(
    "cos_service.schemas.bucket_retention",
    _schema_module(
        "cos_service.schemas.bucket_retention",
        DAYS=schema_stubs.DAYS,
        YEARS=schema_stubs.YEARS,
        MAX_RETENTION_YEARS=schema_stubs.MAX_RETENTION_YEARS,
        _RETENTION_KEYS=schema_stubs._RETENTION_KEYS,
        max_retention=schema_stubs.max_retention,
        BucketRetention=schema_stubs.BucketRetention,
    ),
)
_constants = _schema_module(
    "cos_service.utils.constants",
    TERRAFORM_REPOSITORY="https://gitlab.example/cos/cos.git",
)
_utils = _schema_module("cos_service.utils", constants=_constants)
_utils.__path__ = []  # package, so that "from cos_service.utils import constants" resolves
_install_if_missing("cos_service.utils", _utils)
_install_if_missing("cos_service.utils.constants", _constants)
_sqlalchemy = _schema_module("sqlalchemy", update=orm_stubs.update)
_sqlalchemy.__path__ = []
_sqlalchemy_orm = _schema_module("sqlalchemy.orm", joinedload=orm_stubs.joinedload)
_sqlalchemy.orm = _sqlalchemy_orm
_install_if_missing("sqlalchemy", _sqlalchemy)
_install_if_missing("sqlalchemy.orm", _sqlalchemy_orm)

_models = _schema_module("cos_service.models")
_models.__path__ = []
_install_if_missing("cos_service.models", _models)
for _model_name, _model_cls in (("Bucket", orm_stubs.Bucket), ("Cos", orm_stubs.Cos), ("Workspace", orm_stubs.Workspace)):
    _install_if_missing(
        f"cos_service.models.{_model_name}",
        _schema_module(f"cos_service.models.{_model_name}", **{_model_name: _model_cls}),
    )
_install_if_missing(
    "cos_service.schemas.action",
    _schema_module("cos_service.schemas.action", Action=orm_stubs.Action),
)
_install_if_missing(
    "cos_service.schemas.bucket_backup",
    _schema_module("cos_service.schemas.bucket_backup", BucketBackup=schema_stubs.BucketBackup),
)
_install_if_missing(
    "cos_service.schemas.status",
    _schema_module("cos_service.schemas.status", Status=schema_stubs.Status),
)
_install_if_missing(
    "cos_service.schemas.subscription_status",
    _schema_module(
        "cos_service.schemas.subscription_status",
        SubscriptionStatus=schema_stubs.SubscriptionStatus,
    ),
)


# --- fixtures partagées -----------------------------------------------------

SERVICE_MODULES = (
    "contextService",
    "cosService",
    "backup_vault_service",
    "bucketService",
    "ibm_iam_service",
    "schematics_service",
    "vault_service",
    "workspaceService",
)


@pytest.fixture
def services(monkeypatch):
    """Remplace chaque module ``cos_service.services.*`` importé dans les étapes
    par un ``MagicMock``. ``immutability_service`` reste le vrai module."""
    mocks = {}
    for name in SERVICE_MODULES:
        mock = MagicMock(name=name)
        monkeypatch.setitem(sys.modules, f"cos_service.services.{name}", mock)
        mocks[name] = mock
    mocks["schematics_service"].TERRAFORM_VERSION = "1.12"
    return SimpleNamespace(**mocks)


@pytest.fixture
def load_dag(monkeypatch):
    """Importe un DAG avec ``step`` remplacé par un enregistreur.

    Renvoie ``module`` (le module du DAG) et ``steps`` (nom -> fonction brute).
    Les dépendances ``depends(...)`` valent ``None`` : les tests passent
    explicitement ``payload``, ``session``, ``state_manager``, ``tf``, ``vault``.
    """
    modules = bp2i.build_modules()
    for name in bp2i.DAG_OVERRIDES:
        monkeypatch.setitem(sys.modules, name, modules[name])
    registry = modules["bp2i_airflow_library.dag"].registry

    def loader(filename: str):
        registry.clear()
        spec = importlib.util.spec_from_file_location(filename.replace(".", "_"), DAGS_DIR / filename)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        steps = {name: step.fn for name, step in registry.items()}
        return SimpleNamespace(module=module, steps=steps)

    return loader


@pytest.fixture
def dag(load_dag):
    """Le DAG de création."""
    return load_dag("cos.bucket.v1.create.py")
