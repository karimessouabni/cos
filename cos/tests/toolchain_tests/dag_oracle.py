"""Oracle des scénarios Terraform : le DAG lui-même dit ce qu'il attend.

Pour chaque payload de bucket qu'un scénario envoie à l'orchestrateur, ce
module rejoue **le vrai code du produit** (``cos_service.schemas.bucket_retention``,
``cos_service.services.immutability_service``) et renvoie l'issue attendue :
accepté (avec le bloc immutability calculé) ou refusé (avec le message exact
de ``DeclineDemandException``). Les scénarios ``.tftest.hcl`` sont générés
depuis ces issues : un message qui change dans le DAG change l'assertion, un
cas que le DAG accepte n'est jamais asserté « refusé » par erreur.

Les contrôles de ``validate_request`` des DAGs (instance COS inconnue, backup
vault inconnu…) ne sont pas importables (le module DAG charge Airflow) : ils
sont rejoués ici à l'identique, et ``test_scenario_matrix.py`` vérifie que
chaque message existe bien tel quel dans le source du DAG.

Les règles du produit ne sont pas recopiées : seules les signatures des
fonctions de service sont connues ici.
"""
from __future__ import annotations

import importlib
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # <projet>/cos
DAGS_DIR = ROOT / "cos_service" / "dags" / "bucket" / "v1"

STORAGE_CLASSES = ("standard", "vault", "cold", "smart")  # BucketCreatePayload.StorageClass
# Valeurs des surcharges de contexte d'un bucket de scénario, qui n'existent
# pas dans l'orchestrateur (voir scenario_matrix.py).
UNKNOWN_COS_INSTANCE = "co000000000000"
UNKNOWN_VAULT_NAME = "vault-inconnu-toolchain"

# Messages de validate_request (cos.bucket.v1.create / update) : rejoués ici,
# vérifiés contre le source du DAG par test_scenario_matrix.py.
DAG_MESSAGES = {
    "cos_instance_unknown": ("cos.bucket.v1.create.py", "the cos instance {name} doesn't exist"),
    "vault_name_required": ("cos.bucket.v1.create.py", "The Backup Vault name is required to enable bucket backup"),
    "vault_unknown_create": ("cos.bucket.v1.create.py", "The Backup Vault doesn't exist for the name : {name}"),
    "vault_unknown_update": ("cos.bucket.v1.update.py", "No Backup Vault exist with the name : {name}"),
    "storage_class_invalid": ("cos.bucket.v1.create.py", "Input should be 'standard', 'vault', 'cold' or 'smart'"),
}

_PAYLOAD_SCALARS = (
    "storage_class",
    "enable_versioning",
    "enable_custom_permissions",
    "immutability_choice",
    "object_lock_duration_days",
    "object_lock_duration_years",
)


@dataclass
class Outcome:
    accepted: bool
    message: str | None = None
    immutability: dict | None = None
    source: str = "service"  # service | schema | dag
    rules: list[str] = field(default_factory=list)

    @property
    def declined(self) -> bool:
        return not self.accepted


# --------------------------------------------------------------------------- #
# Chargement du produit avec les doublures d'infrastructure de tests/unit
# --------------------------------------------------------------------------- #

_loaded: dict = {}


def load_product() -> dict:
    """Importe le vrai code produit derrière les doublures de ``tests/unit/stubs``
    (framework bp2i, Airflow, SQLAlchemy). Idempotent."""
    if _loaded:
        return _loaded
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    importlib.import_module("tests.unit.conftest")  # installe les doublures
    from pydantic import ValidationError

    from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
    from cos_service.schemas.bucket_backup import BucketBackup
    from cos_service.schemas.bucket_retention import BucketRetention
    from cos_service.schemas.immutability import Immutability
    from cos_service.services import immutability_service

    _loaded.update(
        ValidationError=ValidationError,
        DeclineDemandException=DeclineDemandException,
        BucketBackup=BucketBackup,
        BucketRetention=BucketRetention,
        Immutability=Immutability,
        svc=immutability_service,
    )
    return _loaded


def _schema_messages(exc) -> str:
    """Messages d'une ValidationError pydantic, sans le préfixe « Value error, »,
    joints comme le fait le produit (« | »)."""
    messages = []
    for error in exc.errors():
        msg = error.get("msg", "")
        if msg.startswith("Value error, "):
            msg = msg[len("Value error, "):]
        messages.append(msg)
    return " | ".join(messages)


def fresh_immutability(versioning: bool) -> dict:
    """Bloc immutability vierge, tel que process_protection_configuration le construit."""
    return {
        "object_locking_enabled": False,
        "object_versioning_enabled": versioning,
        "object_lock_duration_days": None,
        "object_lock_duration_years": None,
        "retention": {"retention_enabled": False, "default": None, "minimum": None, "maximum": None},
        "backup": {"backup_enabled": False, "backup_vault_sub_id": None, "backup_retention_days": None},
    }


def backup_block(payload: dict) -> dict | None:
    """Bloc ``backup`` tel que main.tf l'envoie, ou None s'il n'y en a pas.

    Même règle que ``local.bucket_payloads`` : présent dès qu'une des clés
    backup_* est renseignée ; ``backup_enabled`` vaut true par défaut ;
    ``backup_vault_name`` absent = le vault du scénario (nom réel inconnu ici,
    représenté par ``"<vault>"``)."""
    keys = ("backup_enabled", "backup_vault_name", "backup_retention_days")
    if all(payload.get(k) is None for k in keys):
        return None
    enabled = payload.get("backup_enabled")
    return {
        "backup_enabled": True if enabled is None else enabled,
        "backup_vault_name": "<vault>" if payload.get("backup_vault_name") is None else payload["backup_vault_name"],
        "backup_retention_days": payload.get("backup_retention_days"),
    }


def _backup_object(block: dict | None, product: dict):
    if block is None:
        return None
    return product["BucketBackup"](
        backup_enabled=block["backup_enabled"],
        backup_vault_name=block["backup_vault_name"] or None,
        backup_retention_days=block["backup_retention_days"],
        backup_vault_sub_id="vault-sub" if block["backup_vault_name"] == "<vault>" else None,
    )


def _retention_object(payload: dict, product: dict):
    """BucketRetention du payload (None si absent) ; lève ValidationError comme le DAG."""
    if payload.get("retention") is None:
        return None
    return product["BucketRetention"](**{k: v for k, v in payload["retention"].items() if v is not None})


def _vault_errors(block: dict | None, stage: str) -> list[str]:
    """validate_request : backup demandé sans nom de vault, ou vault inconnu."""
    if block is None or block["backup_enabled"] is not True:
        return []
    name = block["backup_vault_name"]
    if not name:
        return [DAG_MESSAGES["vault_name_required"][1]]
    if name != "<vault>":
        key = "vault_unknown_create" if stage == "create" else "vault_unknown_update"
        return [DAG_MESSAGES[key][1].format(name=name)]
    return []


# --------------------------------------------------------------------------- #
# Création
# --------------------------------------------------------------------------- #

def expected_create(payload: dict) -> Outcome:
    """Issue attendue de cos.bucket.v1.create pour un payload de scénario
    (clés de ``var.buckets`` de main.tf)."""
    product = load_product()
    errors = []

    # validate_request (DAG) : contexte
    if payload.get("cos_instance") is not None:
        errors.append(DAG_MESSAGES["cos_instance_unknown"][1].format(name=payload["cos_instance"]))
    block = backup_block(payload)
    errors.extend(_vault_errors(block, "create"))
    if errors:
        return Outcome(False, " | ".join(errors), source="dag")

    # Construction du payload (pydantic) : BucketRetention, StorageClass, Immutability
    storage_class = payload.get("storage_class", "standard")
    if storage_class not in STORAGE_CLASSES:
        return Outcome(False, DAG_MESSAGES["storage_class_invalid"][1], source="schema")
    try:
        retention = _retention_object(payload, product)
        choice = product["Immutability"](payload.get("immutability_choice") or "none")
    except product["ValidationError"] as exc:
        return Outcome(False, _schema_messages(exc), source="schema")
    except ValueError as exc:  # choix d'immutabilité inconnu
        return Outcome(False, str(exc), source="schema")

    # process_protection_configuration (DAG) -> compute_bucket_new_immutability (service)
    versioning = payload.get("enable_versioning") if payload.get("enable_versioning") is not None else False
    backup = _backup_object(block, product) if block is not None and block["backup_enabled"] is True else None
    try:
        immutability = product["svc"].compute_bucket_new_immutability(
            choice,
            retention,
            payload.get("object_lock_duration_days"),
            payload.get("object_lock_duration_years"),
            versioning,
            fresh_immutability(versioning),
            backup,
        )
    except product["DeclineDemandException"] as exc:
        return Outcome(False, str(exc))
    return Outcome(True, immutability=immutability)


# --------------------------------------------------------------------------- #
# Mise à jour
# --------------------------------------------------------------------------- #

def bucket_row(immutability: dict) -> dict:
    """Ligne bucket (dict) telle que le DAG d'update la relit en base, depuis le
    bloc immutability d'une création ou d'une mise à jour acceptée."""
    retention = immutability["retention"]
    backup = immutability["backup"]
    return {
        "object_lock_duration_days": immutability["object_lock_duration_days"],
        "object_lock_duration_years": immutability["object_lock_duration_years"],
        "object_versioning_enabled": immutability["object_versioning_enabled"],
        "retention_enabled": bool(retention["retention_enabled"]),
        "retention_default": retention["default"],
        "retention_minimum": retention["minimum"],
        "retention_maximum": retention["maximum"],
        "backup_enabled": bool(backup["backup_enabled"]),
        "backup_vault_subscription_id": backup["backup_vault_sub_id"],
        "backup_retention_days": backup["backup_retention_days"],
    }


def expected_update(current: dict, payload: dict) -> Outcome:
    """Issue attendue de cos.bucket.v1.update : ``current`` est le bloc
    immutability en base (dernière opération acceptée), ``payload`` le payload
    complet que Terraform renvoie (le provider envoie la ressource entière,
    pas seulement les attributs modifiés)."""
    product = load_product()
    errors = []

    block = backup_block(payload)
    errors.extend(_vault_errors(block, "update"))
    try:
        retention = _retention_object(payload, product)
    except product["ValidationError"] as exc:
        return Outcome(False, _schema_messages(exc), source="schema")

    backup = _backup_object(block, product)
    try:
        immutability = product["svc"].validate_immutability_for_update_bucket(
            bucket_row(current),
            retention,
            payload.get("object_lock_duration_days"),
            payload.get("object_lock_duration_years"),
            payload.get("enable_versioning"),
            False,  # has_contents : les buckets de scénario sont vides
            backup,
            None,
        )
    except product["DeclineDemandException"] as exc:
        errors.extend(str(exc).split(" | "))
    if errors:
        return Outcome(False, " | ".join(errors), source="dag" if block and errors[0].startswith("No Backup") else "service")
    return Outcome(True, immutability=immutability)


def payload_echo(payload: dict) -> dict:
    """Ce que le provider doit relire dans ``payload`` après acceptation :
    les scalaires envoyés, les clés de retention et du bloc backup."""
    echo = {k: payload[k] for k in _PAYLOAD_SCALARS if payload.get(k) is not None}
    echo.setdefault("storage_class", "standard")
    if payload.get("retention"):
        echo["retention"] = {k: v for k, v in payload["retention"].items() if v is not None}
    block = backup_block(payload)
    if block is not None:
        echo["backup"] = {k: v for k, v in block.items() if v is not None and k != "backup_vault_name"}
    return echo
