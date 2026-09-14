"""Doublures minimales de ``bp2i_airflow_library`` et ``bp2i_terraform``.

Deux usages :

* ``conftest.py`` les installe dans ``sys.modules`` quand la vraie lib n'est
  pas installée, pour que ``immutability_service`` et le DAG s'importent.
* la fixture ``dag`` des tests de DAG remplace TOUJOURS ``dag``,
  ``dependencies``, ``schemas`` et ``config`` par ces doublures : le décorateur
  ``step`` enregistre la fonction brute de chaque étape dans ``registry`` au
  lieu de construire une tâche Airflow, ce qui permet d'appeler les étapes
  comme de simples fonctions.

``DeclineDemandException`` est la vraie classe quand la lib est présente, pour
que les ``pytest.raises`` fonctionnent quel que soit le module qui lève.
"""
import types

try:  # vraie lib présente : on réutilise ses exceptions
    from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
except ImportError:  # pragma: no cover - dépend de l'environnement

    class DeclineDemandException(Exception):
        """Refus fonctionnel d'une demande."""


def mark_task_as_declined(*args, **kwargs):
    return None


class LazyResult:
    """Valeur renvoyée par l'appel d'une étape pendant le câblage du DAG."""

    def __init__(self, step_name: str):
        self.step_name = step_name


class Step:
    """Remplace la tâche Airflow : garde la fonction brute, n'exécute rien."""

    def __init__(self, fn, registry: dict):
        self.fn = fn
        self.__name__ = fn.__name__
        registry[fn.__name__] = self

    def __call__(self, *args, **kwargs):
        return LazyResult(self.fn.__name__)


class FieldInfo:
    def __init__(self, default=None, **extra):
        self.default = default
        self.extra = extra


def Field(default=None, **extra):
    return FieldInfo(default, **extra)


class ProductCreatePayload:
    """Payload sans validation : les champs annotés prennent le kwarg ou le défaut."""

    realm: str = None
    apcode: str = None
    environment: str = None
    region: str = None
    subscription_id: str = None

    def __init__(self, **kwargs):
        for cls in reversed(type(self).__mro__):
            for name in getattr(cls, "__annotations__", {}):
                default = cls.__dict__.get(name)
                if isinstance(default, FieldInfo):
                    default = default.default
                setattr(self, name, kwargs.pop(name, default))
        for name, value in kwargs.items():
            setattr(self, name, value)


class TerraformVar:
    def __init__(self, value, sensitive: bool = False):
        self.value = value
        self.sensitive = sensitive


class SASession: ...


class SchematicsBackend: ...


class StateManager: ...


class Vault: ...


def build_modules() -> dict[str, types.ModuleType]:
    """Construit un jeu frais de modules doublures, indexé par nom qualifié."""
    registry: dict[str, Step] = {}

    def step(fn):
        return Step(fn, registry)

    def product_action(name, tags=None, payload=None):
        def decorator(fn):
            def run():
                fn()
                return registry

            run.dag_name = name
            run.payload = payload
            return run

        return decorator

    def depends(dependency):
        return None

    root = types.ModuleType("bp2i_airflow_library")
    root.add_project_to_path = lambda: None

    config = types.ModuleType("bp2i_airflow_library.config")
    config.ENVIRONMENT = "test"

    dag = types.ModuleType("bp2i_airflow_library.dag")
    dag.step = step
    dag.product_action = product_action
    dag.registry = registry

    dependencies = types.ModuleType("bp2i_airflow_library.dependencies")
    dependencies.SASession = SASession
    dependencies.SchematicsBackend = SchematicsBackend
    dependencies.StateManager = StateManager
    dependencies.Vault = Vault
    dependencies.depends = depends
    for marker in (
        "airflow_context_dependency",
        "payload_dependency",
        "smart_schematics_backend_dependency",
        "sqlalchemy_session_dependency",
        "state_manager_dependency",
        "vault_dependency",
    ):
        setattr(dependencies, marker, object())

    exceptions = types.ModuleType("bp2i_airflow_library.exceptions")
    flow_control = types.ModuleType("bp2i_airflow_library.exceptions.flow_control")
    flow_control.DeclineDemandException = DeclineDemandException
    flow_control.mark_task_as_declined = mark_task_as_declined

    schemas = types.ModuleType("bp2i_airflow_library.schemas")
    schemas.Field = Field
    schemas.ProductCreatePayload = ProductCreatePayload

    terraform = types.ModuleType("bp2i_terraform")
    backends = types.ModuleType("bp2i_terraform.backends")
    schematics = types.ModuleType("bp2i_terraform.backends.schematics")
    schematics.TerraformVar = TerraformVar

    return {
        "bp2i_airflow_library": root,
        "bp2i_airflow_library.config": config,
        "bp2i_airflow_library.dag": dag,
        "bp2i_airflow_library.dependencies": dependencies,
        "bp2i_airflow_library.exceptions": exceptions,
        "bp2i_airflow_library.exceptions.flow_control": flow_control,
        "bp2i_airflow_library.schemas": schemas,
        "bp2i_terraform": terraform,
        "bp2i_terraform.backends": backends,
        "bp2i_terraform.backends.schematics": schematics,
    }


# Modules que la fixture ``dag`` remplace même quand la vraie lib est là.
DAG_OVERRIDES = (
    "bp2i_airflow_library",
    "bp2i_airflow_library.config",
    "bp2i_airflow_library.dag",
    "bp2i_airflow_library.dependencies",
    "bp2i_airflow_library.schemas",
)
