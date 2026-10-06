#!/usr/bin/env python3
"""Génère terraform/tests/*.tftest.hcl depuis scenario_matrix.py et dag_oracle.py.

    python generate_tests.py            # (ré)écrit les fichiers
    python generate_tests.py --check    # code 1 si un fichier généré n'est pas à jour
    python generate_tests.py --summary  # tableau des scénarios (pour le README)

Pour chaque run d'un scénario, l'oracle (le code des DAGs) donne l'issue de
chaque bucket ajouté ou modifié, et les assertions sont écrites en
conséquence :

* accepté : la souscription existe, n'est pas refusée, le payload relu porte
  ce qui a été envoyé ; pour un update, le `name` n'a pas changé ;
* refusé : status DECLINED et motif = message exact du DAG (regex, les
  nombres de jours « (1826 days) » sont génériques car ils dépendent de la
  date : années bissextiles) ;
* retiré (`drop`) : la souscription n'est plus dans les outputs.
"""
from __future__ import annotations

import argparse
import difflib
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


@dataclass
class Step:
    """Ce qu'un run fait d'un bucket, et ce que l'oracle en attend."""
    key: str
    kind: str  # create | update | drop
    payload: dict | None
    outcome: dag_oracle.Outcome | None
    created_in: str | None = None  # run qui a créé le bucket (pour vérifier `name`)


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
                steps.append(Step(key, "update", payload, outcome, created_in[key]))
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


def hcl_regex(message: str) -> str:
    """Regex RE2 qui reconnaît le message exact, les décomptes de jours
    « (1826 days » étant génériques (ils dépendent de la date du jour)."""
    escaped = re.sub(r"([\\.^$*+?{}\[\]|()])", r"\\\1", message)
    escaped = re.sub(r"\\\(\d+ days", r"\\(\\d+ days", escaped)
    return escaped


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
    status = f'output.bucket_status["{key}"]'
    reason = f'output.bucket_status_reason["{key}"]'
    label = f"{key} ({step.kind})"
    if step.kind == "drop":
        return _assert(
            f'!contains(keys(output.bucket_names), "{key}")',
            f"{label} : le bucket aurait dû être détruit.",
        )
    outcome = step.outcome
    if outcome.declined:
        message = outcome.message
        if outcome.source == "schema":  # format du refus d'un payload invalide : seul le premier motif est sûr
            message = message.split(" | ")[0]
        return (
            _assert(f'{status} == "DECLINED"',
                    f"{label} : aurait dû être refusé ({message}) ; status = ${{jsonencode({status})}}.")
            + _assert(f"can(regex({hcl_string(hcl_regex(message))}, {reason}))",
                      f"{label} : motif inattendu : ${{{reason}}}")
        )
    parts = [
        _assert(f'{status} == null || {status} != "DECLINED"',
                f"{label} : refusé alors que le DAG l'accepte : ${{{reason}}}"),
        _assert(f'{RESOURCE}["{key}"].name != ""', f"{label} : souscription sans name."),
    ]
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


def render_scenario(scenario: Scenario) -> str:
    out = [HEADER, f"# {scenario.title}", "#"]
    out.extend(f"# {line}" for line in _wrap(scenario.doc))
    out.append("")
    out.append("variables {")
    if needs_vault(scenario):
        out.append(f'  scenario   = "{scenario.slug.replace("_", " ")}"')
        out.append("  with_vault = true")
    else:
        out.append(f'  scenario = "{scenario.slug.replace("_", " ")}"')
    out.append("}")
    for run, buckets, steps in plan_scenario(scenario):
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
        out.append("")
        out.append("".join(render_step(step) for step in steps).rstrip("\n"))
        out.append("}")
    return "\n".join(out) + "\n"


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
    files = {}
    for scenario in SCENARIOS:
        if scenario.filename in files:
            raise ValueError(f"deux scénarios produisent {scenario.filename}")
        files[scenario.filename] = render_scenario(scenario)
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
    rows = ["| Fichier | Couvre | Runs | Acceptés | Refusés |", "|---|---|---|---|---|"]
    for scenario in SCENARIOS:
        steps = [s for _, _, steps in plan_scenario(scenario) for s in steps if s.outcome is not None]
        ok = sum(s.outcome.accepted for s in steps)
        ko = len(steps) - ok
        rows.append(f"| `{scenario.filename}` | {scenario.title} | {len(scenario.runs)} | {ok} | {ko} |")
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
