"""Garde-fous de la matrice de scénarios Terraform (scenario_matrix.py).

* les fichiers terraform/tests/*.tftest.hcl sont à jour par rapport à la matrice ;
* chaque message de refus du produit (immutability_service, BucketRetention,
  validate_request des DAGs) est provoqué par au moins un scénario, ou
  explicitement listé comme non testable depuis Terraform ;
* les messages rejoués par l'oracle existent tels quels dans le source des DAGs ;
* les regex générées reconnaissent bien les messages de l'oracle.
"""
import ast
import json
import os
import re
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import dag_oracle  # noqa: E402
import generate_tests  # noqa: E402
from scenario_matrix import SCENARIOS  # noqa: E402

SERVICE_FILES = (
    dag_oracle.ROOT / "cos_service" / "services" / "immutability_service.py",
    dag_oracle.ROOT / "cos_service" / "schemas" / "bucket_retention.py",
)

# Messages du produit qu'aucun scénario Terraform ne peut provoquer, et pourquoi.
UNTESTABLE = {
    "Setting a retention is not possible when the bucket already contains objects.":
        "demande un bucket avec des objets : hors de portée d'un scénario tofu test (voir terraform/persistent)",
    "Retention and object Lock are not compatible.":
        "préfixe partagé : couvert par « Retention and object Lock are not compatible. We cannot activate both »",
    "Object Lock Duration Days or Years must be not empty.":
        "inatteignable : apply_choice_unit refuse avant (choix _daily/_yearly) ou le choix est déduit d'une durée présente",
    "Retention must not be empty.":
        "inatteignable : le choix RETENTION sans valeurs est refusé avant par apply_choice_unit ; "
        "le choix générique « retention » sans valeurs aussi (« must not be empty for choice »)",
    "Retention configuration is not valid. You must set default, minimum and maximum in the same unit, either in days or in years.":
        "validate_retention_update : les bornes manquantes sont relues en base, où un bucket en rétention les a toujours",
    "must be set, either in days":
        "compute_bucket_object_lock : une durée est garantie présente par _new_object_lock_immutability avant l'appel",
}


def _message_literals(path) -> set[str]:
    """Chaînes passées à errors.append(...), DeclineDemandException(...) et ValueError(...)
    (partie statique des f-strings) dans un fichier source."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    literals = set()

    def static_text(node) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            parts = [v.value for v in node.values if isinstance(v, ast.Constant)]
            return parts[0] if parts else None
        if isinstance(node, ast.BinOp):  # "a" + "b" implicite -> déjà fusionné par ast ; "a" "b" idem
            return None
        return None

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if name not in ("append", "DeclineDemandException", "ValueError"):
            continue
        text = static_text(node.args[0])
        if text and len(text.strip()) >= 20:
            literals.add(text.strip())
    return literals


@pytest.fixture(scope="module")
def planned():
    return {s.filename: generate_tests.plan_scenario(s) for s in SCENARIOS}


@pytest.fixture(scope="module")
def declined_messages(planned) -> list[str]:
    return [
        step.outcome.message
        for plan in planned.values()
        for _, _, steps in plan
        for step in steps
        if step.outcome is not None and step.outcome.declined
    ]


def test_generated_files_are_up_to_date():
    problems = generate_tests.check()
    assert not problems, "python tests/toolchain_tests/generate_tests.py\n" + "\n".join(problems)


def test_every_product_refusal_is_exercised(declined_messages):
    """Un message de refus ajouté au produit sans scénario fait échouer ce test."""
    literals = set().union(*(_message_literals(p) for p in SERVICE_FILES))
    assert literals, "aucun message trouvé : extraction cassée"
    not_covered = sorted(
        literal for literal in literals
        if not any(_static_prefix(literal) in message for message in declined_messages)
        and not any(literal.startswith(_static_prefix(u)) for u in UNTESTABLE)
    )
    assert not not_covered, "messages du produit sans scénario de refus :\n- " + "\n- ".join(not_covered)


def test_untestable_messages_still_exist():
    """Une exemption qui ne correspond plus à rien dans le produit est à retirer."""
    sources = "\n".join(p.read_text(encoding="utf-8") for p in SERVICE_FILES)
    dags = "\n".join((dag_oracle.DAGS_DIR / n).read_text(encoding="utf-8") for n in os.listdir(dag_oracle.DAGS_DIR) if n.endswith(".py"))
    for message in UNTESTABLE:
        prefix = _static_prefix(message)[:40]
        assert prefix in sources or prefix in dags, f"exemption obsolète : {message!r}"


def _static_prefix(text: str) -> str:
    """Partie d'un message avant le premier placeholder de f-string ({...})."""
    return re.split(r"\{[^}]*\}", text)[0].strip()


def test_dag_messages_match_dag_sources():
    for key, (filename, template) in dag_oracle.DAG_MESSAGES.items():
        source = (dag_oracle.DAGS_DIR / filename).read_text(encoding="utf-8")
        for fragment in re.split(r"\{[^}]*\}", template):
            fragment = fragment.strip()
            if key == "storage_class_invalid":  # message pydantic, dérivé de l'enum StorageClass
                continue
            assert fragment in source, f"{key} : « {fragment} » absent de {filename}"


def test_storage_classes_match_dag_enum():
    source = (dag_oracle.DAGS_DIR / "cos.bucket.v1.create.py").read_text(encoding="utf-8")
    enum_block = source[source.index("class StorageClass"):source.index("storage_class: StorageClass")]
    assert set(re.findall(r'= "(\w+)"', enum_block)) == set(dag_oracle.STORAGE_CLASSES)


def test_regexes_match_their_messages(declined_messages):
    """La regex du manifeste reconnaît le message, y compris tel que le provider
    l'affiche (décompte de jours d'une autre année, points de suspension échappés)."""
    for message in declined_messages:
        first = message.split(" | ")[0]
        pattern = generate_tests.failure_regex(first)
        assert re.search(pattern, first), f"regex générée ne reconnaît pas : {first!r}"
        shown = re.sub(r"\(182[5-7] days", "(1827 days", first).replace("…", "\\u2026")
        output = ('Error: Cannot create subscription: status reason \\"1 validation error for '
                  'BucketCreatePayload\\nretention\\n  Value error, ' + shown + '\\"')
        assert re.search(pattern, output), first


def test_manifest_lists_every_refusal(planned):
    files = generate_tests.rendered_files()
    manifest = json.loads(files[generate_tests.MANIFEST])
    refusals = sum(1 for plan in planned.values() for _, _, steps in plan
                   for s in steps if s.outcome is not None and s.outcome.declined)
    assert len(manifest) == refusals
    for name, entry in manifest.items():
        assert name in files, name
        assert f'run "{entry["run"]}"' in files[name]
        for prelude in entry["prelude"]:
            assert f'run "{prelude}"' in files[name]
        assert "bucket_status" not in files[name]


def test_run_names_are_unique_per_scenario():
    for scenario in SCENARIOS:
        names = [run.name for run in scenario.runs]
        assert len(names) == len(set(names)), f"{scenario.filename} : runs en double"
        for name in names:
            assert re.fullmatch(r"[a-z][a-z0-9_]*", name), f"{scenario.filename} : nom de run invalide {name!r}"


def test_each_scenario_has_both_outcomes_where_expected(planned):
    """Les fichiers *_rules portent des refus, les autres au moins une acceptation."""
    for scenario in SCENARIOS:
        steps = [s for _, _, steps in planned[scenario.filename] for s in steps if s.outcome is not None]
        if scenario.slug.endswith("_rules"):
            assert any(s.outcome.declined for s in steps), f"{scenario.filename} : aucun refus"
        else:
            assert any(s.outcome.accepted for s in steps), f"{scenario.filename} : aucune acceptation"


def test_oracle_examples():
    """Quelques issues connues, pour détecter un oracle qui dirait oui à tout."""
    ok = dag_oracle.expected_create({"retention": {"minimum_days": 1, "default_days": 2, "maximum_days": 3}})
    assert ok.accepted and ok.immutability["retention"]["default"] == 2
    ko = dag_oracle.expected_create({"enable_versioning": True, "retention": {"minimum_days": 1, "default_days": 2, "maximum_days": 3}})
    assert ko.declined and "Retention and versioning are not compatible" in ko.message
    upd = dag_oracle.expected_update(ok.immutability, {"retention": {"minimum_days": 1, "default_days": 2, "maximum_days": 3}, "object_lock_duration_days": 1})
    assert upd.declined and "object-lock is not possible when retention" in upd.message
