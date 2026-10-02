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
from enum import Enum

# Importer la vraie lib charge toute sa config, qui lit des variables
# d'environnement sans défaut (ORCHESTRATOR_MULTIREADER_SERVICE_PORT,
# READER_URL, ...) : KeyError en CI, où la lib est installée sans la
# plateforme. En mode stubs (défaut), on ne l'importe donc jamais ; seul
# COS_TESTS_FORCE_STUBS=0 (venv complet, tests d'intégration) réutilise sa
# classe d'exception, pour que les pytest.raises fonctionnent quel que soit
# le module qui lève.
import os

if os.environ.get("COS_TESTS_FORCE_STUBS", "1") == "0":
    try:
        from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
    except Exception:  # pragma: no cover - dépend de l'environnement
        DeclineDemandException = None
else:
    DeclineDemandException = None

if DeclineDemandException is None:

    class DeclineDemandException(Exception):
        """Refus fonctionnel d'une demande."""


def mark_task_as_declined(*args, **kwargs):
    return None


class LazyResult:
    """Valeur renvoyée par l'appel d'une étape pendant le câblage du DAG.

    Imite ce que les DAGs en font : ``.operator.task_id`` et l'opérateur ``>>``.
    """

    def __init__(self, step_name: str):
        self.step_name = step_name
        self.operator = types.SimpleNamespace(task_id=step_name)
        self.downstream = []

    def __rshift__(self, other):
        self.downstream.append(other)
        return other

    def __rrshift__(self, other):
        return self


class DateTimeSensorAsync:
    """Doublure du sensor Airflow : ne fait que mémoriser ses arguments."""

    def __init__(self, task_id: str, target_time: str, **kwargs):
        self.task_id = task_id
        self.target_time = target_time
        self.kwargs = kwargs
        self.downstream = []

    def __rshift__(self, other):
        self.downstream.append(other)
        return other

    def __rrshift__(self, other):
        return self


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
    # Branche du produit donnée à la gateway de l'orchestrateur pour cette demande.
    product_branch: str = None

    def __init__(self, **kwargs):
        for cls in reversed(type(self).__mro__):
            for name in getattr(cls, "__annotations__", {}):
                default = cls.__dict__.get(name)
                if isinstance(default, FieldInfo):
                    default = default.default
                setattr(self, name, kwargs.pop(name, default))
        for name, value in kwargs.items():
            setattr(self, name, value)


class ProductActionPayload(ProductCreatePayload):
    """Même doublure : realm, apcode, environment, region, subscription_id."""


class ProductActionConfig:
    def __init__(self, **options):
        self.options = options


class LinearCooldownPolicy:
    def __init__(self, delay, max_attempts):
        self.delay = delay
        self.max_attempts = max_attempts


class TerraformVar:
    def __init__(self, value, sensitive: bool = False):
        self.value = value
        self.sensitive = sensitive


class VCS:
    def __init__(self, repository, branch, oauth_token_id, directory):
        self.repository = repository
        self.branch = branch
        self.oauth_token_id = oauth_token_id
        self.directory = directory


class OrchestratorEnvironment(str, Enum):
    INT = "int"
    PREPROD = "preprod"
    PROD = "prod"


class SASession: ...


class SchematicsBackend: ...


class StateManager: ...


class Vault: ...


class ReaderConnector:
    """Connecteur de lecture de l'orchestrateur (multireader) : doublure vide."""


class TriggerRule(str, Enum):
    ALL_SUCCESS = "all_success"
    ALL_FAILED = "all_failed"
    ALL_DONE = "all_done"
    ONE_SUCCESS = "one_success"
    ONE_FAILED = "one_failed"
    NONE_FAILED = "none_failed"
    NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"
    ALWAYS = "always"


def build_modules() -> dict[str, types.ModuleType]:
    """Construit un jeu frais de modules doublures, indexé par nom qualifié."""
    registry: dict[str, Step] = {}

    def step(fn):
        return Step(fn, registry)

    def product_action(name=None, tags=None, payload=None, config=None, *, action_id=None, **_ignored):
        """Accepte l'ancienne forme positionnelle et la forme ``action_id=`` du vrai DAG."""

        def decorator(fn):
            def run():
                fn()
                return registry

            run.dag_name = action_id or name
            run.payload = payload
            run.config = config
            return run

        return decorator

    def passthrough_decorator(*dargs, **dkwargs):
        """``@task`` / ``@task_group`` d'Airflow : avec ou sans parenthèses, rend la fonction telle quelle."""
        if len(dargs) == 1 and callable(dargs[0]) and not dkwargs:
            return dargs[0]
        return lambda fn: fn

    def depends(dependency):
        return None

    root = types.ModuleType("bp2i_airflow_library")
    root.__path__ = []  # paquet : un sous-module inconnu donne "No module named" et non "is not a package"
    root.add_project_to_path = lambda: None

    config = types.ModuleType("bp2i_airflow_library.config")
    config.ENVIRONMENT = OrchestratorEnvironment.INT.value
    config.OrchestratorEnvironment = OrchestratorEnvironment

    # Le vrai DAG teste la version d'Airflow pour choisir ses imports ; en
    # doublure on reste sur la branche Airflow 2 (airflow.decorators, airflow.utils).
    version_compat = types.ModuleType("bp2i_airflow_library.version_compat")
    version_compat.AIRFLOW_V_3_0_PLUS = False

    connectors = types.ModuleType("bp2i_airflow_library.connectors")
    connectors.__path__ = []
    reader = types.ModuleType("bp2i_airflow_library.connectors.reader")
    reader.ReaderConnector = ReaderConnector

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
        "reader_dependency",
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
    schemas.ProductActionPayload = ProductActionPayload
    schemas.ProductActionConfig = ProductActionConfig

    terraform = types.ModuleType("bp2i_terraform")
    terraform.__path__ = []
    backends = types.ModuleType("bp2i_terraform.backends")
    backends.__path__ = []
    schematics = types.ModuleType("bp2i_terraform.backends.schematics")
    schematics.TerraformVar = TerraformVar
    schematics.VCS = VCS
    # le vrai schematics_service importe VCS depuis bp2i_terraform.schemas
    tf_schemas = types.ModuleType("bp2i_terraform.schemas")
    tf_schemas.TerraformVar = TerraformVar
    tf_schemas.VCS = VCS
    components = types.ModuleType("bp2i_terraform.components")
    components.__path__ = []
    cooldown = types.ModuleType("bp2i_terraform.components.cooldown_policies")
    cooldown.LinearCooldownPolicy = LinearCooldownPolicy

    airflow = types.ModuleType("airflow")
    airflow.__path__ = []
    sensors = types.ModuleType("airflow.sensors")
    sensors.__path__ = []
    date_time = types.ModuleType("airflow.sensors.date_time")
    date_time.DateTimeSensorAsync = DateTimeSensorAsync
    # Imports du bloc de compatibilité du vrai DAG (branche Airflow 2).
    decorators = types.ModuleType("airflow.decorators")
    decorators.task = passthrough_decorator
    decorators.task_group = passthrough_decorator
    utils = types.ModuleType("airflow.utils")
    utils.__path__ = []
    trigger_rule = types.ModuleType("airflow.utils.trigger_rule")
    trigger_rule.TriggerRule = TriggerRule
    context = types.ModuleType("airflow.utils.context")
    context.Context = dict
    # Branche Airflow 3 (airflow.sdk) : fournie aussi, au cas où AIRFLOW_V_3_0_PLUS serait forcé.
    sdk = types.ModuleType("airflow.sdk")
    sdk.Context = dict
    sdk.TriggerRule = TriggerRule
    sdk.task = passthrough_decorator
    sdk.task_group = passthrough_decorator

    return {
        "airflow": airflow,
        "airflow.sensors": sensors,
        "airflow.sensors.date_time": date_time,
        "airflow.decorators": decorators,
        "airflow.utils": utils,
        "airflow.utils.trigger_rule": trigger_rule,
        "airflow.utils.context": context,
        "airflow.sdk": sdk,
        "bp2i_airflow_library.version_compat": version_compat,
        "bp2i_airflow_library.connectors": connectors,
        "bp2i_airflow_library.connectors.reader": reader,
        "bp2i_terraform.components": components,
        "bp2i_terraform.components.cooldown_policies": cooldown,
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
        "bp2i_terraform.schemas": tf_schemas,
    }


# Modules que la fixture ``dag`` remplace même quand la vraie lib est là.
DAG_OVERRIDES = (
    "bp2i_airflow_library",
    "bp2i_airflow_library.config",
    "bp2i_airflow_library.version_compat",
    "bp2i_airflow_library.connectors",
    "bp2i_airflow_library.connectors.reader",
    "bp2i_airflow_library.dag",
    "bp2i_airflow_library.dependencies",
    "airflow.decorators",
    "airflow.utils",
    "airflow.utils.trigger_rule",
    "airflow.utils.context",
    "airflow.sdk",
    "bp2i_airflow_library.schemas",
    "airflow",
    "airflow.sensors",
    "airflow.sensors.date_time",  # le vrai sensor exige un contexte de DAG
)
# NB : airflow.sensors.base (PokeReturnValue) n'est pas remplacé : la doublure
# n'est installée que si le vrai module manque (conftest).
