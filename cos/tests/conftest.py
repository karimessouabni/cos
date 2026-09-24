"""Configuration pytest commune.

* met la racine du projet dans ``sys.path`` pour importer ``cos_service`` ;
* installe les doublures de ``tests/stubs`` :
  - TOUJOURS (sauf ``COS_TESTS_FORCE_STUBS=0``) pour la frontière
    d'infrastructure dont les tests unitaires dépendent : le framework
    ``bp2i_airflow_library`` / ``bp2i_terraform``, ``airflow``, ``sqlalchemy``
    et les modèles ``cos_service.models.*`` (enregistreur ``update``, colonnes
    comparables, ``to_dict`` prévisible) ;
  - SEULEMENT si le vrai module manque pour le code projet (``cos_service.schemas``,
    ``cos_service.utils``, ``cos_service.repository``) : sur un venv complet,
    les tests tournent contre les vrais schémas et constantes.

Les services et les étapes de DAG testés sont toujours le vrai code.

Lancement :

    python -m pytest                                   # unitaires
    COS_TESTS_FORCE_STUBS=0 python -m pytest -m integration   # DagBag réel (venv complet)
"""
import importlib
import importlib.util
import os
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# La vraie lib lit ces variables dès son import (config/environment.py fait
# os.environ[...] sans défaut) et importer n'importe quel sous-module charge
# tout Airflow. On les fixe avant le premier import, quel que soit le mode.
for _key, _value in {
    "ENVIRONMENT": "int",
    "DEFAULT_PRODUCT_BRANCH": "main",
    "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
    "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
}.items():
    os.environ.setdefault(_key, _value)

from tests.stubs import bp2i, orm as orm_stubs, schemas as schema_stubs  # noqa: E402

DAGS_DIR = ROOT / "cos_service" / "dags" / "bucket" / "v1"
DAG_PATH = DAGS_DIR / "cos.bucket.v1.create.py"


FORCE_STUBS = os.environ.get("COS_TESTS_FORCE_STUBS", "1") != "0"


def _install_if_missing(name: str, module: types.ModuleType) -> None:
    try:
        importlib.import_module(name)
    except ImportError:
        sys.modules[name] = module


def _install_forced(name: str, module: types.ModuleType) -> None:
    """Doublure d'infrastructure : remplace le vrai module, sauf COS_TESTS_FORCE_STUBS=0."""
    if FORCE_STUBS:
        sys.modules[name] = module
    else:
        _install_if_missing(name, module)


def _schema_module(name: str, **attrs) -> types.ModuleType:
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


for _name, _module in bp2i.build_modules().items():
    _install_forced(_name, _module)

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
_install_forced("sqlalchemy", _sqlalchemy)
_install_forced("sqlalchemy.orm", _sqlalchemy_orm)

_models = _schema_module("cos_service.models")
_models.__path__ = []
_install_forced("cos_service.models", _models)
for _model_name, _model_cls in (
    ("Bucket", orm_stubs.Bucket),
    ("Cos", orm_stubs.Cos),
    ("Workspace", orm_stubs.Workspace),
    ("BackupVault", orm_stubs.BackupVault),
    ("BackupVaultRestore", orm_stubs.BackupVaultRestore),
):
    _install_forced(
        f"cos_service.models.{_model_name}",
        _schema_module(f"cos_service.models.{_model_name}", **{_model_name: _model_cls}),
    )
_install_if_missing(
    "cos_service.schemas.action",
    _schema_module("cos_service.schemas.action", Action=orm_stubs.Action),
)
_install_if_missing(
    "cos_service.schemas.restore_status",
    _schema_module("cos_service.schemas.restore_status", RestoreStatus=orm_stubs.RestoreStatus),
)
_sensors_base = _schema_module("airflow.sensors.base", PokeReturnValue=orm_stubs.PokeReturnValue)
_install_if_missing("airflow.sensors.base", _sensors_base)
_repository = _schema_module("cos_service.repository")
_repository.__path__ = []
_install_if_missing("cos_service.repository", _repository)
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


def _is_real_module(name: str) -> bool:
    """True si ``name`` est importable et n'est pas une doublure de tests/stubs."""
    try:
        module = importlib.import_module(name)
    except Exception:  # noqa: BLE001 - import cassé (re2, KeyError d'env...) = pas de vraie lib
        return False
    return bool(getattr(module, "__file__", None))


# Vraie lib utilisable pour charger les DAGs : uniquement avec COS_TESTS_FORCE_STUBS=0
# et si bp2i_airflow_library + Airflow s'importent réellement.
USE_REAL_LIB = (
    not FORCE_STUBS
    and _is_real_module("bp2i_airflow_library")
    and _is_real_module("bp2i_airflow_library.airflow.decorators.product_action")
)


# --- fixtures partagées -----------------------------------------------------

SERVICE_MODULES = (
    "contextService",
    "cosService",
    "backup_vault_service",
    "bucketService",
    "ibm_iam_service",
    "restore_service",
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
    restore_repository = MagicMock(name="backup_vault_restore_repository")
    monkeypatch.setitem(sys.modules, "cos_service.repository.backup_vault_restore_repository", restore_repository)
    mocks["backup_vault_restore_repository"] = restore_repository
    return SimpleNamespace(**mocks)


def _unwrap(fn):
    """Remonte la chaîne ``__wrapped__`` (functools.wraps) jusqu'à la fonction d'origine."""
    seen = set()
    while hasattr(fn, "__wrapped__") and id(fn) not in seen:
        seen.add(id(fn))
        fn = fn.__wrapped__
    return fn


def _load_dag_with_real_library(monkeypatch, filename: str) -> SimpleNamespace:
    """Charge un DAG avec la VRAIE lib : ``product_action`` construit le DAG Airflow,
    on récupère la fonction Python d'origine de chaque tâche définie dans le
    fichier du DAG via ``python_callable`` (déballé de ses wrappers).

    Les ``depends(...)`` restent les marqueurs réels dans les défauts : les tests
    passent toujours ``payload``, ``session``, ``state_manager``, ``tf``, ``vault``
    explicitement, donc ils ne sont jamais résolus.
    """
    product_action_module = importlib.import_module("bp2i_airflow_library.airflow.decorators.product_action")
    real_dag_cls = product_action_module.DAG
    created: list = []

    class RecordingDAG(real_dag_cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

    monkeypatch.setattr(product_action_module, "DAG", RecordingDAG)

    module_name = filename.replace(".", "_")
    spec = importlib.util.spec_from_file_location(module_name, DAGS_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    dags = created or [v for v in vars(module).values() if isinstance(v, real_dag_cls)]
    if not dags:
        raise RuntimeError(
            f"{filename}: product_action n'a construit aucun DAG (ni via DAG(...), ni dans les globals du module)."
        )
    dag = dags[-1]

    steps: dict = {}
    foreign: list[str] = []
    for task in dag.tasks:
        callable_ = getattr(task, "python_callable", None)
        if callable_ is None:
            foreign.append(f"{task.task_id} ({type(task).__name__}, sans python_callable)")
            continue
        raw = _unwrap(callable_)
        if getattr(raw, "__module__", None) != module_name:
            foreign.append(f"{task.task_id} -> {getattr(raw, '__module__', '?')}.{getattr(raw, '__name__', '?')}")
            continue
        steps[raw.__name__] = raw
    # dag.tasks suit l'ordre d'instanciation (le câblage) ; la doublure suit l'ordre
    # de définition. On aligne sur ce dernier pour que les deux modes coïncident.
    steps = dict(sorted(steps.items(), key=lambda item: item[1].__code__.co_firstlineno))
    if not steps:
        raise RuntimeError(
            f"{filename}: aucune étape retrouvée dans dag.tasks. Tâches vues : {foreign}. "
            "Le wrapper de @step n'expose sans doute pas __wrapped__ : voir tests/README.md."
        )
    return SimpleNamespace(module=module, steps=steps, dag=dag, framework_tasks=foreign)


@pytest.fixture
def load_dag(monkeypatch):
    """Importe un DAG et renvoie ``module`` (le module du DAG) et ``steps``
    (nom -> fonction brute), plus ``dag`` et ``framework_tasks`` avec la vraie lib.

    - par défaut : ``step`` est remplacé par un enregistreur et ``depends(...)``
      vaut ``None`` (doublures de ``tests/stubs/bp2i.py``) ;
    - avec ``COS_TESTS_FORCE_STUBS=0`` et la vraie lib importable : le vrai
      ``product_action`` construit le DAG Airflow et les étapes sont extraites des
      tâches (``_load_dag_with_real_library``).
    Dans les deux cas, les tests passent explicitement ``payload``, ``session``,
    ``state_manager``, ``tf``, ``vault``.
    """
    if USE_REAL_LIB:
        return lambda filename: _load_dag_with_real_library(monkeypatch, filename)

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
