#!/usr/bin/env python3
"""Génère terraform/tests/*.tftest.hcl depuis scenario_matrix.py et dag_oracle.py.

    python generate_tests.py            # (ré)écrit les fichiers
    python generate_tests.py --check    # code 1 si un fichier généré n'est pas à jour
    python generate_tests.py --summary  # tableau des scénarios (pour le README)

Pour chaque run d'un scénario, l'oracle (le code des DAGs) donne l'issue de
chaque bucket ajouté ou modifié :

* accepté : le run reste dans le fichier du scénario (<nn>_<slug>.tftest.hcl)
  avec ses assertions : la souscription existe, le payload relu porte ce qui a
  été envoyé, et pour un update le `name` n'a pas changé ;
* retiré (`drop`) : la souscription n'est plus dans les outputs ;
* refusé : le provider orchestrator fait ÉCHOUER l'apply sur une demande
  refusée (« Demand create status is CANCELLED … status reason … »), ce que
  `tofu test` ne sait pas attendre (expect_failures ne couvre pas les erreurs
  de provider) et qui arrête le fichier. Chaque refus a donc SON fichier
  (<nn>_<slug>__<run>.tftest.hcl : le bucket de base s'il s'agit d'un update,
  puis le run refusé, sans assertion), et le motif attendu est écrit dans
  tests/expected_failures.json. toolchain_env.py lance ces fichiers et juge :
  run en échec ET motif du DAG dans la sortie = succès ; run passé = échec
  (le DAG accepte ce qu'il devrait refuser).
"""
from __future__ import annotations

import argparse
import difflib
import json
import logging
import os
import re
import sys
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import dag_oracle  # noqa: E402
from scenario_matrix import SCENARIOS, Run, Scenario  # noqa: E402

logging.getLogger("cos_service").setLevel(logging.ERROR)  # avertissements « legacy retention » de l'oracle

TESTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "terraform", "tests")
HEADER = "# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main."
RESOURCE = "orchestrator_subscription_cosbucket_v1.bucket"
MANIFEST = "expected_failures.json"  # dans terraform/tests/ : lu par toolchain_env.py


@dataclass
class Step:
    """Ce qu'un run fait d'un bucket, et ce que l'oracle en attend."""
    key: str
    kind: str  # create | update | drop
    payload: dict | None
    outcome: dag_oracle.Outcome | None
    created_in: str | None = None  # run qui a créé le bucket (pour vérifier `name`)
    base_payload: dict | None = None  # update : dernier payload accepté du bucket


# --------------------------------------------------------------------------- #
# Moteur : rejoue la suite de runs et calcule les attentes
# --------------------------------------------------------------------------- #

def plan_scenario(scenario: Scenario) -> list[tuple[Run, dict[str, dict], list[Step]]]:
    """Pour chaque run : la map `buckets` complète à envoyer et les étapes attendues."""
    current_payload: dict[str, dict] = {}  # dernier payload ACCEPTÉ par bucket
    current_imm: dict[str, dict] = {}
    created_in: dict[str, str] = {}
    planned = []
    for run in scenario.runs:
        steps = []
        for key in run.drop:
            if key not in current_payload:
                raise ValueError(f"{scenario.filename} / {run.name} : drop d'un bucket inconnu {key!r}")
            steps.append(Step(key, "drop", None, None, created_in[key]))
            del current_payload[key], current_imm[key], created_in[key]
        buckets = dict(current_payload)
        for key, payload in run.buckets.items():
            buckets[key] = payload
            if key in current_payload:
                if payload == current_payload[key]:
                    raise ValueError(f"{scenario.filename} / {run.name} : {key!r} inchangé, le run n'enverrait rien")
                outcome = dag_oracle.expected_update(current_imm[key], payload)
                steps.append(Step(key, "update", payload, outcome, created_in[key], current_payload[key]))
            else:
                outcome = dag_oracle.expected_create(payload)
                steps.append(Step(key, "create", payload, outcome, run.name))
            if outcome.accepted:
                current_payload[key] = payload
                current_imm[key] = outcome.immutability
                created_in.setdefault(key, run.name)
        planned.append((run, buckets, steps))
    return planned


def needs_vault(scenario: Scenario) -> bool:
    return any(dag_oracle.backup_block(p) is not None for run in scenario.runs for p in run.buckets.values())


# --------------------------------------------------------------------------- #
# Rendu HCL
# --------------------------------------------------------------------------- #

def hcl_value(value) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"').replace("${", "$${") + '"'
    if isinstance(value, dict):
        if not value:
            return "{}"
        return "{ " + ", ".join(f"{k} = {hcl_value(v)}" for k, v in value.items()) + " }"
    raise TypeError(f"valeur HCL non gérée : {value!r}")


def failure_regex(message: str) -> str:
    """Regex Python qui reconnaît le message exact dans la sortie de tofu : les
    décomptes du plafond en jours « (1826 days » sont génériques (1825 à 1827
    selon la date du jour), les points de suspension acceptent leurs écritures
    échappées."""
    escaped = re.escape(message)
    escaped = re.sub(r"\\\(182[5-7]\\ days", r"\\(\\d+ days", escaped)  # plafond 5 ans en jours
    escaped = escaped.replace("…", r"(?:…|\\u2026|\.\.\.)")
    return escaped


def expected_message(outcome: dag_oracle.Outcome) -> str:
    """Motif à retrouver : pour un refus de schéma (pydantic) seul le premier
    motif est sûr, le provider ne remonte que le texte de l'erreur."""
    return outcome.message.split(" | ")[0] if outcome.source == "schema" else outcome.message


def hcl_string(text: str) -> str:
    """Chaîne HCL d'un message d'assertion : les guillemets sont échappés hors
    des interpolations ${...}, qui restent telles quelles (HCL y interdit \")."""
    out, depth, i = [], 0, 0
    while i < len(text):
        two = text[i:i + 2]
        if two == "${" and depth == 0:
            depth, out = 1, out + ["${"]
            i += 2
            continue
        char = text[i]
        if depth:
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
            out.append(char)
        elif char == "\\":
            out.append("\\\\")
        elif char == '"':
            out.append('\\"')
        else:
            out.append(char)
        i += 1
    return '"' + "".join(out) + '"'


def _assert(condition: str, message: str) -> str:
    return f"  assert {{\n    condition     = {condition}\n    error_message = {hcl_string(message)}\n  }}\n"


def _echo_assertions(key: str, echo: dict, prefix: str) -> list[str]:
    out = []
    for attr, value in echo.items():
        if isinstance(value, dict):
            for sub, sub_value in value.items():
                out.append(_assert(
                    f'{RESOURCE}["{key}"].payload.{attr}.{sub} == {hcl_value(sub_value)}',
                    f"{prefix} : payload.{attr}.{sub} attendu {hcl_value(sub_value)}, "
                    f"relu ${{jsonencode(try({RESOURCE}[\"{key}\"].payload.{attr}, null))}}.",
                ))
        else:
            out.append(_assert(
                f'{RESOURCE}["{key}"].payload.{attr} == {hcl_value(value)}',
                f"{prefix} : payload.{attr} attendu {hcl_value(value)}, "
                f"relu ${{jsonencode(try({RESOURCE}[\"{key}\"].payload.{attr}, null))}}.",
            ))
    return out


def render_step(step: Step) -> str:
    key = step.key
    label = f"{key} ({step.kind})"
    if step.kind == "drop":
        return _assert(
            f'!contains(keys(output.bucket_names), "{key}")',
            f"{label} : le bucket aurait dû être détruit.",
        )
    if step.outcome.declined:
        return ""  # l'apply échoue avant toute assertion : jugé par toolchain_env.py (expected_failures.json)
    parts = [_assert(f'{RESOURCE}["{key}"].name != ""', f"{label} : souscription sans name.")]
    if step.kind == "update":
        parts.append(_assert(
            f'{RESOURCE}["{key}"].name == run.{step.created_in}.bucket_names["{key}"]',
            f"{label} : l'update a recréé la souscription au lieu de la modifier en place.",
        ))
    parts.extend(_echo_assertions(key, dag_oracle.payload_echo(step.payload), label))
    return "".join(parts)


def describe(step: Step) -> str:
    if step.kind == "drop":
        return f"#   {step.key} : retiré -> détruit"
    verdict = "ACCEPTÉ" if step.outcome.accepted else f"REFUSÉ ({step.outcome.source}) « {step.outcome.message} »"
    return f"#   {step.key} : {step.kind} -> {verdict}"


def _render_run(out: list[str], run: Run, buckets: dict[str, dict], steps: list[Step]) -> None:
    out.append("")
    if run.note:
        out.extend(f"# {line}" for line in _wrap(run.note))
    out.append("# Attendu :")
    out.extend(describe(step) for step in steps)
    out.append(f'run "{run.name}" {{')
    out.append("  variables {")
    if buckets:
        out.append("    buckets = {")
        width = max(len(k) for k in buckets)
        for key, payload in buckets.items():
            out.append(f"      {key.ljust(width)} = {hcl_value(payload)}")
        out.append("    }")
    else:
        out.append("    buckets = {}")
    out.append("  }")
    body = "".join(render_step(step) for step in steps).rstrip("\n")
    if body:
        out.append("")
        out.append(body)
    out.append("}")


def _render_file(scenario: Scenario, title: str, doc: str, with_vault: bool,
                 runs: list[tuple[Run, dict[str, dict], list[Step]]], label: str = "") -> str:
    out = [HEADER, f"# {title}", "#"]
    out.extend(f"# {line}" for line in _wrap(doc))
    out.append("")
    out.append("variables {")
    name = scenario.slug.replace("_", " ") + (f" {label}" if label else "")
    if with_vault:
        out.append(f'  scenario   = "{name}"')
        out.append("  with_vault = true")
    else:
        out.append(f'  scenario = "{name}"')
    out.append("}")
    for run, buckets, steps in runs:
        _render_run(out, run, buckets, steps)
    return "\n".join(out) + "\n"


def _has_backup(buckets: dict[str, dict]) -> bool:
    return any(dag_oracle.backup_block(p) is not None for p in buckets.values())


def split_scenario(scenario: Scenario) -> tuple[dict[str, str], dict[str, dict]]:
    """Fichiers d'un scénario : la chaîne des runs acceptés, puis un fichier par
    refus. Renvoie ({nom: contenu}, {nom: entrée du manifeste})."""
    files, manifest = {}, {}
    chain = []
    for run, buckets, steps in plan_scenario(scenario):
        refused = [s for s in steps if s.outcome is not None and s.outcome.declined]
        if not refused:
            if steps:
                chain.append((run, buckets, steps))
            continue
        if len(steps) != 1:
            raise ValueError(f"{scenario.filename} / {run.name} : un run refusé ne porte qu'un seul bucket")
        step = refused[0]
        name = f"{scenario.number}_{scenario.slug}__{run.name}.tftest.hcl"
        if step.kind == "create":
            own_buckets = {step.key: step.payload}
            prelude = []
        else:
            base_payload = step.base_payload
            base = dag_oracle.expected_create(base_payload)
            step = Step(step.key, "update", step.payload, dag_oracle.expected_update(base.immutability, step.payload), "create_base")
            if step.outcome.accepted:
                raise ValueError(f"{name} : refusé dans la chaîne mais accepté sur une base créée directement")
            own_buckets = {step.key: step.payload}
            prelude = [(Run("create_base", {step.key: base_payload}), {step.key: base_payload},
                        [Step(step.key, "create", base_payload, base, "create_base")])]
        doc = (f"{scenario.title} : cas de refus « {run.name} ». " + (run.note or "") +
               " Le provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie "
               "que la sortie porte le motif du DAG (tests/expected_failures.json).")
        with_vault = _has_backup(own_buckets) or any(_has_backup(b) for _, b, _ in prelude)
        files[name] = _render_file(scenario, f"{scenario.title} : refus « {run.name} »", doc, with_vault,
                                   prelude + [(run, own_buckets, [step])], label=run.name)
        message = expected_message(step.outcome)
        manifest[name] = {
            "scenario": scenario.slug, "run": run.name, "bucket": step.key, "kind": step.kind,
            "source": step.outcome.source, "message": message, "regex": failure_regex(message),
            "prelude": [r.name for r, _, _ in prelude],
        }
    if chain:
        files[scenario.filename] = _render_file(scenario, scenario.title, scenario.doc,
                                                any(_has_backup(b) for _, b, _ in chain), chain)
    return files, manifest


def _wrap(text: str, width: int = 78) -> list[str]:
    words, lines, line = text.split(), [], ""
    for word in words:
        if line and len(line) + 1 + len(word) > width:
            lines.append(line)
            line = word
        else:
            line = f"{line} {word}" if line else word
    if line:
        lines.append(line)
    return lines


# --------------------------------------------------------------------------- #
# Fichiers
# --------------------------------------------------------------------------- #

def rendered_files() -> dict[str, str]:
    """Tous les fichiers générés, manifeste compris ({nom: contenu})."""
    files, manifest = {}, {}
    for scenario in SCENARIOS:
        scenario_files, scenario_manifest = split_scenario(scenario)
        for name in scenario_files:
            if name in files:
                raise ValueError(f"deux scénarios produisent {name}")
        files.update(scenario_files)
        manifest.update(scenario_manifest)
    files[MANIFEST] = json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    return files


def stale_generated_files(tests_dir: str, keep: set[str]) -> list[str]:
    """Fichiers générés par une version précédente de la matrice."""
    stale = []
    for name in sorted(os.listdir(tests_dir)) if os.path.isdir(tests_dir) else []:
        if name.endswith(".tftest.hcl") and name not in keep:
            with open(os.path.join(tests_dir, name), encoding="utf-8") as handle:
                if handle.readline().rstrip("\n") == HEADER:
                    stale.append(name)
    return stale


def check(tests_dir: str = TESTS_DIR) -> list[str]:
    """Différences entre la matrice et les fichiers présents (vide = à jour)."""
    problems = []
    files = rendered_files()
    for name, content in files.items():
        path = os.path.join(tests_dir, name)
        if not os.path.exists(path):
            problems.append(f"{name} : absent")
            continue
        with open(path, encoding="utf-8") as handle:
            existing = handle.read()
        if existing != content:
            diff = difflib.unified_diff(existing.splitlines(), content.splitlines(), f"{name} (actuel)", f"{name} (attendu)", lineterm="", n=1)
            problems.append("\n".join(list(diff)[:40]))
    for name in stale_generated_files(tests_dir, set(files)):
        problems.append(f"{name} : généré par une ancienne matrice, à supprimer")
    return problems


def write(tests_dir: str = TESTS_DIR) -> list[str]:
    os.makedirs(tests_dir, exist_ok=True)
    files = rendered_files()
    for name in stale_generated_files(tests_dir, set(files)):
        os.remove(os.path.join(tests_dir, name))
    for name, content in files.items():
        with open(os.path.join(tests_dir, name), "w", encoding="utf-8") as handle:
            handle.write(content)
    return sorted(files)


def summary() -> str:
    """Tableau Markdown des scénarios : fichier, runs, buckets acceptés / refusés."""
    rows = ["| Scénario | Couvre | Runs acceptés | Fichiers de refus |", "|---|---|---|---|"]
    for scenario in SCENARIOS:
        files, manifest = split_scenario(scenario)
        accepted = sum(1 for _, _, steps in plan_scenario(scenario) for s in steps
                       if s.outcome is None or s.outcome.accepted)
        rows.append(f"| `{scenario.number}_{scenario.slug}` | {scenario.title} | {accepted} | {len(manifest)} |")
    return "\n".join(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="ne rien écrire, code 1 si les fichiers ne sont pas à jour")
    parser.add_argument("--summary", action="store_true", help="imprimer le tableau des scénarios")
    parser.add_argument("--dir", default=TESTS_DIR, help="dossier des .tftest.hcl (défaut: terraform/tests)")
    args = parser.parse_args(argv)
    if args.summary:
        print(summary())
        return 0
    if args.check:
        problems = check(args.dir)
        for problem in problems:
            print(problem)
        print("à jour" if not problems else f"{len(problems)} fichier(s) à régénérer : python generate_tests.py")
        return 1 if problems else 0
    for name in write(args.dir):
        print(f"écrit {os.path.join(args.dir, name)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
