"""Doublures de ``sqlalchemy`` et des modèles ``cos_service.models`` absents d'ici.

Installées par ``conftest.py`` uniquement quand le vrai module manque.
``update(...)`` renvoie un enregistreur chaîné : les tests lisent ``.table``,
``.values_`` et ``.where_`` sur l'objet passé à ``session.execute``.
"""
from enum import Enum


class Column:
    """Attribut de classe d'un modèle, juste assez pour écrire des filtres."""

    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other):  # noqa: D105 - filtre "col == valeur"
        return ("==", self.name, other)

    def __hash__(self):
        return hash(self.name)

    def contains(self, other):
        return ("contains", self.name, other)

    def is_(self, other):
        return ("is", self.name, other)

    def __add__(self, other):
        return Column(f"{self.name}+{other}")

    def __le__(self, other):
        return ("<=", self.name, other)


class Model:
    """Modèle minimal : kwargs -> attributs, ``to_dict`` et ``dict(row)``."""

    columns: tuple[str, ...] = ()

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def to_dict(self) -> dict:
        return {k: v for k, v in vars(self).items() if not k.startswith("_")}

    def keys(self):
        return self.to_dict().keys()

    def __getitem__(self, key):
        return getattr(self, key)


def _model(name: str, *columns: str) -> type:
    cls = type(name, (Model,), {col: Column(col) for col in columns})
    cls.columns = columns
    return cls


Bucket = _model(
    "Bucket", "subscription_id", "name", "has_expiration_rule", "expiration_rule_created_at",
    "cos", "workspace", "backup_vault",
)
Cos = _model("Cos", "subscription_id", "context", "workspace")
Workspace = _model("Workspace", "bucket_subscription_id", "workspace_id")
BackupVault = _model("BackupVault", "subscription_id", "name")
BackupVaultRestore = _model("BackupVaultRestore", "id", "subscription_id", "status")


class Action(str, Enum):
    APPLY = "apply"
    DESTROY = "destroy"


class RestoreStatus(str, Enum):
    REQUESTED = "requested"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


class PokeReturnValue:
    def __init__(self, is_done: bool, xcom_value=None):
        self.is_done = is_done
        self.xcom_value = xcom_value


class FakeUpdate:
    def __init__(self, table):
        self.table = table
        self.values_ = {}
        self.where_ = None

    def values(self, **kwargs):
        self.values_.update(kwargs)
        return self

    def where(self, condition):
        self.where_ = condition
        return self


def update(table):
    return FakeUpdate(table)


class _JoinedLoad:
    def __init__(self, attr):
        self.chain = [attr]

    def joinedload(self, attr):
        self.chain.append(attr)
        return self


def joinedload(attr):
    return _JoinedLoad(attr)
