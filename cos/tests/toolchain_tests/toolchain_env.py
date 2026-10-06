#!/usr/bin/env python3
r"""
Préparation de l'environnement Terraform des tests toolchain (créer des COS et
des buckets via le provider orchestrator) : tout ce qu'il fallait faire à la
main avant un `terraform plan` est enchaîné par le script.

Étapes (chacune est sautée si elle a déjà été faite et reste valide) :
1. tofu login <artifactory>          si aucun credential n'est enregistré
                                     (~/.terraform.d/credentials.tofurc.json)
2. tofu init                         dans le dossier de tests de l'environnement
                                     si .terraform/ n'existe pas encore (--reinit
                                     pour forcer)
3. proxy                             http://<user>:<mot de passe>@ncproxy.fr.net.intra:8080
                                     user et mot de passe demandés une fois,
                                     vérifiés sur iam.cloud.ibm.com (407 =
                                     refusés) puis mémorisés ; utilisés pour
                                     les appels suivants et exportés pour tofu
4. token Vault                       de l'instance Vault de l'environnement
                                     (int -> hvault-dev) : token sauvegardé s'il
                                     est encore valide, sinon
                                     GET <service token>/v1/token/<uid>?namespace=AP85135
                                     (le client_token renvoyé est un token Vault,
                                     valable 30 jours), sinon Chrome / Edge sur
                                     l'UI Vault en secours
5. API key IBM Cloud                 GET <vault>/v1/<kv_path> avec le token
                                     (X-Vault-Namespace: AP85135) ; la clé est
                                     gardée tant que son lease est valide
6. variables d'environnement         IBM_CLOUD_API_KEY, ORCHESTRATOR_IBMCLOUD_API_KEY,
                                     http_proxy / https_proxy / no_proxy en
                                     minuscules et en MAJUSCULES (+ TF_LOG)

Utilisation
    # dans le shell courant : exporte les variables puis terraform plan/apply
    eval "$(python toolchain_env.py --env int)"
    terraform -chdir=int/new_version plan

    # ou tout enchaîné : les étapes 1-6 puis tofu test / plan / apply
    python toolchain_env.py --env int --run test
    python toolchain_env.py --env int --run test -- -filter=tests/20_bucket_lifecycle.tftest.hcl
    python toolchain_env.py --env int --run plan

Logs
Avec --run, le journal de tofu (TF_LOG=debug : appels du provider à
l'orchestrateur, attente des souscriptions) est écrit dans
<dossier terraform>/logs/<horodatage>-<env>-<commande>.log, et ses lignes
info, warn et error sont recopiées en direct dans le terminal, préfixées par
« | » : `tofu test` n'affiche sinon rien tant qu'un run n'est pas fini.
    --follow debug        tout le détail en direct (trace, debug, info, warn, error, off)
    --tf-log trace        niveau du journal
    --no-log-file         pas de journal (comportement d'avant)

    # ou un sous-shell avec les variables déjà exportées
    python toolchain_env.py --env int --shell

    # sans argument dans un terminal : mode guidé (menus numérotés)
    python toolchain_env.py

Proxy
    export PROXY_USER=h12345 PROXY_PASSWORD=...   # ou --proxy-user / --proxy-password
    --no-proxy                                    # pas de proxy du tout
Si https_proxy est déjà exporté dans le shell et répond, il est réutilisé tel
quel. Sinon user et mot de passe sont demandés une fois, vérifiés sur
iam.cloud.ibm.com, puis mémorisés dans le trousseau macOS (sans trousseau, le
mot de passe est redemandé à chaque lancement ; il n'est jamais écrit sur le
disque) : plus rien n'est redemandé tant que le proxy les accepte.
--new-proxy-password pour en saisir un autre, --forget-proxy-password pour l'oublier.

Token Vault
    export VAULT_TOKEN=hvs....                # ou --vault-token
    --uid lh90871                             # uid passé au service token
                                              # (défaut: $TOOLCHAIN_UID, sinon demandé
                                              # une fois puis mémorisé)
Le token récupéré est mémorisé dans le trousseau macOS avec l'API key et
réutilisé tant qu'il est valide (vérifié par lookup-self) : le service token
n'est rappelé que s'il est expiré ou refusé. ~/.cache/cos-toolchain/state.json
ne garde que des réglages (user, uid, versions), jamais de secret. --new-token force un nouveau token, --new-key une nouvelle API key,
--forget efface tout ce qui est sauvegardé.
Sans service token pour l'instance Vault (--browser-token pour forcer), le
token est lu dans un Chrome / Edge en navigation privée ouvert sur l'UI Vault
(login SSO) ; sans Chrome / Edge, ou avec --manual-token, l'UI Vault est
ouverte dans le navigateur par défaut : "Copy token" dans le menu utilisateur,
puis Entrée dans le terminal (le token est lu dans le presse-papiers).

Les appels HTTPS (Vault, service token, test du proxy) vérifient toujours le
certificat du serveur. Les CA internes sont lues dans les trousseaux système
macOS, ou dans un bundle PEM : --ca-bundle, $COS_TOOLCHAIN_CA_BUNDLE ou
$SSL_CERT_FILE.

Codes de sortie: 0 OK, 1 erreur args/token, 2 erreur Vault, 3 erreur tofu
(avec --run, le code de sortie de tofu est renvoyé tel quel).
"""

from __future__ import annotations

import argparse
import base64
import getpass
import json
import os
import re
import shlex
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass
from typing import Any, Callable, Sequence, TypeVar

T = TypeVar("T")

# --------------------------------------------------------------------------- #
# Configuration : Vaults, environnements, terraform
# --------------------------------------------------------------------------- #

VAULTS = {
    "dev": "https://hvault-dev.fr.net.intra",
    "staging": "https://hvault.staging.echonet",
    "group": "https://hvault.group.echonet",
}

# Service qui délivre un token Vault par uid : GET <url>/v1/token/<uid>?namespace=<ns>
# (Swagger sur <url>/docs). Réponse au format Vault : auth.client_token.
# Instance Vault -> URL du service ; "" : pas de service, passage par l'UI Vault.
TOKEN_SERVICES = {
    "dev": "https://s02vl9956141:4430",
    "staging": "",  # URL à renseigner
    "group": "",  # URL à renseigner
}
TOKEN_SERVICE_PATH = "/v1/token/"


@dataclass(frozen=True)
class Environment:
    vault: str  # clé de VAULTS
    kv_path: str  # chemin KV lu dans Vault : ibm_<account>/creds/<role> (pas une valeur sensible)
    tests_dir: str = ""  # sous-dossier de tests (défaut : <env>/new_version si présent, sinon <env>)


# Environnements des tests toolchain (dossiers tests/toolchain_tests/<env>/).
# int et prod viennent de la procédure manuelle ; qual / pprod pointent sur le
# Vault staging (à vérifier, ou passer --vault / --secret-path).
ENVIRONMENTS = {
    "int": Environment("dev", "ibm_ac002i000263/creds/rl002i000138_buhub"),
    "qual": Environment("staging", "ibm_ac002i000263/creds/rl002i000077_buhub"),
    "pprod": Environment("staging", "ibm_ac002i000263/creds/rl002i000077_buhub"),
    "prod": Environment("group", "ibm_ac002i000266/creds/rl002i000102_buhub"),
}
DEFAULT_ENV = "int"
ENV_VAR = "TOOLCHAIN_ENV"
DEFAULT_NAMESPACE = "AP85135"
NAMESPACE_ENV = "VAULT_NAMESPACE"
VAULT_TOKEN_ENV = "VAULT_TOKEN"
UID_ENV = "TOOLCHAIN_UID"
VAULT_UI_PATH = "/ui/"

TERRAFORM_HOST = "repo.artifactory-dogen.group.echonet"
# OpenTofu uniquement : `tofu test` demande 1.6+, et Terraform >= 1.6 est sous
# licence BUSL. Mêmes commandes, mêmes fichiers .tftest.hcl.
TERRAFORM_BIN = "tofu"
# Source du provider : sans hôte, tofu sous-entend registry.opentofu.org ; on
# qualifie registry.terraform.io pour que le miroir / login Artifactory
# configuré pour cet hôte (celui du .terraform.lock.hcl) s'applique.
PROVIDER_SOURCE = "registry.terraform.io/bp2i/orchestrator"
NEW_VERSION_DIR = "new_version"
# Nouvelle arborescence (terraform test) : un root unique terraform/ à côté du
# script, envs/<env>.tfvars pour ce qui change par environnement, et
# versions.tf généré depuis provider_version du tfvars.
TERRAFORM_ROOT_DIR = "terraform"
ENV_TFVARS_DIR = "envs"
ENV_TFVARS_DIRS = ("envs", "env")  # les deux noms sont acceptés
TEST_FILTER_DIR = ".tftest-filter"  # émulation de -filter pour terraform < 1.7
VERSIONS_TF = "versions.tf"
VERSIONS_TF_TEMPLATE = """# Généré par toolchain_env.py depuis envs/<env>.tfvars (provider_version) :
# une contrainte de version ne peut pas être une variable, et la version du
# provider orchestrator diffère par environnement (2.3.0-int, ...).
# La source est qualifiée registry.terraform.io/... : sans hôte, OpenTofu
# irait chercher sur registry.opentofu.org.
# Ne pas éditer à la main : `python ../toolchain_env.py --env <env>` le réécrit.
terraform {
  required_version = ">= 1.6.0" # tofu test

  required_providers {
    orchestrator = {
      source  = "%s"
      version = "%s"
    }
  }
}
"""
# Journal de tofu : avec --run, TF_LOG part dans <dossier terraform>/logs/
# <horodatage>-<env>-<commande>.log (TF_LOG_PATH) au lieu de noyer la sortie
# de la commande, et les lignes d'un niveau >= --follow sont recopiées en
# direct dans le terminal. `tofu test` n'affiche rien pendant qu'un run tourne :
# c'est le seul moyen de voir ce qui se passe.
LOGS_DIR = "logs"
LOGS_KEPT = 20  # journaux gardés par dossier, les plus anciens sont supprimés
# --parallel : un processus `tofu test -filter=<fichier>` par scénario. tofu
# enchaîne les fichiers un par un ; les scénarios étant indépendants (instance
# COS partagée, buckets distincts), on les lance en parallèle. Les journaux
# sont gardés plus longtemps : un lancement en produit un par scénario.
PARALLEL_LOGS_KEPT = 60
TF_LOG_LEVELS = ("trace", "debug", "info", "warn", "error")
DEFAULT_JOURNAL_LEVEL = "debug"
DEFAULT_FOLLOW_LEVEL = "info"
FOLLOW_OFF = "off"
_TF_LOG_LINE = re.compile(r"^(?:\d{4}-\d\d-\d\dT(\d\d:\d\d:\d\d)\S*|\S+) \[(TRACE|DEBUG|INFO|WARN|ERROR)\]")

# Sous-commandes terraform qui acceptent -var-file : envs/<env>.tfvars y est ajouté.
VAR_FILE_COMMANDS = frozenset({"plan", "apply", "destroy", "test", "refresh", "console"})

# Variables exportées pour terraform (le provider orchestrator et le provider ibm).
API_KEY_VARS = ("IBM_CLOUD_API_KEY", "ORCHESTRATOR_IBMCLOUD_API_KEY")
# Proxy d'entreprise, authentifié par le compte de chaque utilisateur :
# http://<user>:<mot de passe>@ncproxy.fr.net.intra:8080 (hvault-dev.fr.net.intra
# passe par le proxy, les hôtes .echonet non).
DEFAULT_PROXY = "ncproxy.fr.net.intra:8080"
DEFAULT_NO_PROXY = "localhost,127.0.0.1,.echonet,0.0.0.0"
PROXY_USER_ENV = "PROXY_USER"
PROXY_PASSWORD_ENV = "PROXY_PASSWORD"
PROXY_ENV_VARS = ("http_proxy", "https_proxy", "no_proxy")
# URL publique appelée à travers le proxy pour vérifier les identifiants avant
# terraform (c'est l'hôte que le provider ibm joint en premier).
PROXY_CHECK_URL = "https://iam.cloud.ibm.com/identity/.well-known/openid-configuration"

DEFAULT_TIMEOUT = 30
TOKEN_MIN_VALIDITY = 120  # secondes : en dessous, token / API key considérés expirés
BROWSER_LOGIN_TIMEOUT = 300  # secondes laissées pour le login SSO

EXIT_OK, EXIT_USAGE, EXIT_VAULT_FAILED, EXIT_TERRAFORM_FAILED = 0, 1, 2, 3

_MASK_RE = re.compile(r"://([^:/@]+):([^@/]+)@")


def mask_url(url: str) -> str:
    """Cache le mot de passe d'une URL de proxy pour les logs."""
    return _MASK_RE.sub(r"://\1:***@", url)


class CliExit(Exception):
    def __init__(self, code: int, message: str = ""):
        super().__init__(message)
        self.code = code


class VaultError(Exception):
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


def _log(message: str) -> None:
    print(message, file=sys.stderr)


def _duration(seconds: float) -> str:
    return f"{int(seconds)} s" if seconds < 60 else f"{int(seconds) // 60} min"


# --------------------------------------------------------------------------- #
# Token Vault : nettoyage, presse-papiers
# --------------------------------------------------------------------------- #

# hvs.xxx (service token), s.xxx / b.xxx (anciens formats), hvb.xxx (batch)
_VAULT_TOKEN_RE = re.compile(r"\b(?:hv[sb]\.[A-Za-z0-9_-]{20,}|[sb]\.[A-Za-z0-9]{24})\b")


def clean_token(raw: str) -> str:
    """Nettoie un copier-coller : guillemets, 'X-Vault-Token:', espaces ; retourne
    le token Vault trouvé, ou la valeur nettoyée s'il n'a pas un format connu."""
    value = raw.strip().strip("\"'").strip()
    if value.lower().startswith("x-vault-token:"):
        value = value.split(":", 1)[1].strip()
    match = _VAULT_TOKEN_RE.search(value)
    return match.group(0) if match else value


def read_clipboard() -> str:
    commands = (["pbpaste"], ["wl-paste", "-n"], ["xclip", "-selection", "clipboard", "-o"],
                ["powershell", "-NoProfile", "-Command", "Get-Clipboard"])
    for cmd in commands:
        if shutil.which(cmd[0]):
            try:
                return subprocess.run(cmd, capture_output=True, text=True, timeout=5).stdout
            except (OSError, subprocess.SubprocessError):
                continue
    return ""


# --------------------------------------------------------------------------- #
# Client Vault (urllib, sans dépendance)
# --------------------------------------------------------------------------- #

CA_BUNDLE_ENV = "COS_TOOLCHAIN_CA_BUNDLE"
# Trousseaux macOS d'où sont lues les autorités de certification internes
# (poussées par le MDM) : elles ne sont pas dans le magasin OpenSSL de Python.
MACOS_CA_KEYCHAINS = ("/Library/Keychains/System.keychain",
                      "/System/Library/Keychains/SystemRootCertificates.keychain")


def ca_bundle_path(explicit: str | None = None, environ: dict[str, str] | None = None) -> str:
    """Bundle PEM des CA internes : --ca-bundle, sinon $COS_TOOLCHAIN_CA_BUNDLE,
    sinon $SSL_CERT_FILE ; "" si aucun."""
    environ = os.environ if environ is None else environ
    return explicit or environ.get(CA_BUNDLE_ENV) or environ.get("SSL_CERT_FILE") or ""


def macos_keychain_cas() -> str:
    """Certificats (PEM) des trousseaux système macOS, "" hors macOS ou si
    `security` échoue. Permet de vérifier les certificats internes sans bundle."""
    pems = []
    for keychain in MACOS_CA_KEYCHAINS:
        result = _keychain("find-certificate", "-a", "-p", keychain)
        if result is not None and result.returncode == 0 and "BEGIN CERTIFICATE" in result.stdout:
            pems.append(result.stdout)
    return "\n".join(pems)


def tls_context(ca_bundle: str | None = None) -> ssl.SSLContext:
    """Contexte TLS qui vérifie toujours le certificat du serveur : CA du
    système, plus le bundle (ca_bundle_path) et les CA des trousseaux macOS.
    La vérification n'est jamais désactivée."""
    context = ssl.create_default_context()
    bundle = ca_bundle_path(ca_bundle)
    if bundle:
        context.load_verify_locations(cafile=bundle)
    cas = macos_keychain_cas()
    if cas:
        try:
            context.load_verify_locations(cadata=cas)
        except ssl.SSLError as exc:
            _log(f"CA du trousseau macOS ignorées ({exc}) : passer --ca-bundle si Vault est refusé.")
    return context


class VaultClient:
    def __init__(self, base_url: str, namespace: str, timeout: int = DEFAULT_TIMEOUT,
                 ca_bundle: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.namespace = namespace
        self.timeout = timeout
        self._context = tls_context(ca_bundle)

    def _request(self, token: str, path: str) -> dict[str, Any]:
        url = f"{self.base_url}/v1/{path.strip('/')}"
        headers = {"X-Vault-Token": token, "Accept": "application/json"}
        if self.namespace:
            headers["X-Vault-Namespace"] = self.namespace
        request = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout, context=self._context) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:300]
            raise VaultError(f"HTTP {exc.code} sur GET {url}: {body}", exc.code) from exc
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise VaultError(f"GET {url} impossible: {exc}") from exc
        try:
            body = json.loads(raw or b"{}")
        except ValueError as exc:
            raise VaultError(f"réponse non JSON de GET {url}") from exc
        return body if isinstance(body, dict) else {}

    def token_ttl(self, token: str) -> int | None:
        """Secondes de validité restantes du token (lookup-self) ; None si le
        token n'expire pas. VaultError(403) si le token est invalide."""
        data = self._request(token, "auth/token/lookup-self").get("data") or {}
        ttl = data.get("ttl")
        if ttl in (None, 0) and not data.get("expire_time"):
            return None
        try:
            return int(ttl)
        except (TypeError, ValueError):
            return None

    def read_kv(self, token: str, path: str) -> tuple[dict[str, Any], int | None]:
        """(data, lease_duration en secondes ou None) du secret lu à `path`."""
        body = self._request(token, path)
        data = body.get("data") or {}
        if isinstance(data.get("data"), dict) and "api_key" not in data:  # KV v2
            data = data["data"]
        lease = body.get("lease_duration")
        try:
            lease = int(lease) if lease not in (None, 0) else None
        except (TypeError, ValueError):
            lease = None
        return data, lease


def _http_get_json(url: str, timeout: int, context: ssl.SSLContext,
                   headers: dict[str, str] | None = None,
                   proxies: dict[str, str] | None = None) -> dict[str, Any]:
    """GET JSON. proxies=None : proxy de l'environnement ; {} : connexion directe ;
    {"https": url} : ce proxy-là."""
    request = urllib.request.Request(url, headers={"Accept": "application/json", **(headers or {})})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler(proxies),
                                         urllib.request.HTTPSHandler(context=context))
    # Un proxy imposé doit être utilisé même si l'hôte est dans no_proxy.
    saved = {name: os.environ.pop(name) for name in ("no_proxy", "NO_PROXY") if proxies and name in os.environ}
    try:
        with opener.open(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:300]
        raise VaultError(f"HTTP {exc.code} sur GET {url}: {body}", exc.code) from exc
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise VaultError(f"GET {url} impossible: {exc}") from exc
    finally:
        os.environ.update(saved)
    try:
        body = json.loads(raw or b"{}")
    except ValueError as exc:
        raise VaultError(f"réponse non JSON de GET {url}") from exc
    return body if isinstance(body, dict) else {}


class _ipv4_only:
    """Pendant le bloc, socket.getaddrinfo ne renvoie que des adresses IPv4 :
    Python se connecte à la première adresse résolue et attend si c'est une
    IPv6 injoignable, là où curl bascule seul sur l'IPv4."""

    def __enter__(self) -> None:
        self._original = socket.getaddrinfo

        def ipv4(*args: Any, **kwargs: Any) -> list:
            results = self._original(*args, **kwargs)
            return [r for r in results if r[0] == socket.AF_INET] or results
        socket.getaddrinfo = ipv4

    def __exit__(self, *exc: Any) -> None:
        socket.getaddrinfo = self._original


def tcp_probe(host: str, port: int, timeout: float = 5) -> list[str]:
    """Une ligne par adresse résolue : 'IPv4 1.2.3.4:4430 OK' ou 'IPv6 ... timed out'."""
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        return [f"{host} : résolution DNS impossible ({exc})"]
    lines = []
    for family, _, _, _, sockaddr in infos:
        label = "IPv6" if family == socket.AF_INET6 else "IPv4"
        try:
            with socket.create_connection(sockaddr[:2], timeout=timeout):
                lines.append(f"{label} {sockaddr[0]}:{port} OK")
        except OSError as exc:
            lines.append(f"{label} {sockaddr[0]}:{port} {exc}")
    return lines


def token_service_url(base_url: str, uid: str, namespace: str) -> str:
    return (base_url.rstrip("/") + TOKEN_SERVICE_PATH + urllib.parse.quote(uid, safe="")
            + "?" + urllib.parse.urlencode({"namespace": namespace}))


def service_token(base_url: str, uid: str, namespace: str, timeout: int = DEFAULT_TIMEOUT,
                  ca_bundle: str | None = None) -> str:
    """Token Vault délivré par le service token : GET /v1/token/<uid>?namespace=<ns>,
    champ auth.client_token de la réponse (format Vault)."""
    url = token_service_url(base_url, uid, namespace)
    host = urllib.parse.urlsplit(url).hostname or ""
    context = tls_context(ca_bundle)
    # Le service token est un hôte intranet : selon le poste il se joint en
    # direct ou via le proxy. On essaie les deux (direct d'abord si le nom se
    # résout, sinon le proxy d'abord) avec un délai court chacun.
    proxy = os.environ.get("https_proxy") or os.environ.get("HTTPS_PROXY") or ""
    try:
        families = {info[0] for info in socket.getaddrinfo(host, None)}
        resolves = True
    except OSError:
        families, resolves = set(), False
        _log(f"Service token : {host} ne se résout pas en DNS depuis ce poste"
             + (" ; essai via le proxy." if proxy else " ; essayer le nom complet (FQDN) avec --token-service."))
    routes: list[tuple[str, dict[str, str] | None]] = [("direct", {})]
    if socket.AF_INET6 in families and socket.AF_INET in families:
        routes.append(("direct en IPv4 seulement", None))
    if proxy:
        routes.append((f"via le proxy {mask_url(proxy)}", {"https": proxy, "http": proxy}))
    if not resolves:
        routes.reverse()
    attempt_timeout = max(5, min(timeout, 15))
    errors: list[str] = []
    body: dict[str, Any] = {}
    for label, proxies in routes:
        _log(f"Token Vault demandé au service token ({label}, {attempt_timeout} s max) : GET {url}")
        try:
            if proxies is None:
                with _ipv4_only():
                    body = _http_get_json(url, attempt_timeout, context, proxies={})
            else:
                body = _http_get_json(url, attempt_timeout, context, proxies=proxies)
            _log(f"Service token joint {label}.")
            break
        except VaultError as exc:
            if exc.status_code is not None:  # réponse HTTP : l'hôte est joint, inutile d'insister
                raise
            errors.append(f"{label} : {exc}")
    else:
        port = urllib.parse.urlsplit(url).port or 443
        probe = "\n  ".join(tcp_probe(host, port))
        raise VaultError("service token injoignable (" + " ; ".join(errors) + f").\n  Sondage TCP :\n  {probe}"
                         "\n  Vérifier le VPN, ou passer le nom complet avec --token-service, ou --browser-token.")
    token = clean_token(str((body.get("auth") or {}).get("client_token") or ""))
    if not _VAULT_TOKEN_RE.fullmatch(token):
        raise VaultError(f"pas de auth.client_token dans la réponse du service token "
                         f"(champs: {', '.join(sorted(body)) or 'aucun'})")
    return token


API_KEY_FIELDS = ("api_key", "apikey", "apiKey", "API_KEY", "value")


def extract_api_key(data: dict[str, Any]) -> str:
    """API key IBM Cloud dans la réponse de Vault (moteur ibmcloud : data.api_key)."""
    for key in API_KEY_FIELDS:
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key, value in data.items():
        if "api" in key.lower() and "key" in key.lower() and isinstance(value, str) and value.strip():
            return value.strip()
    return ""


# --------------------------------------------------------------------------- #
# Token Vault : récupération automatique via Chrome / Edge (protocole DevTools)
# --------------------------------------------------------------------------- #

BROWSER_CANDIDATES = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "microsoft-edge",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
)

# Expression JS évaluée dans la page de l'UI Vault : l'UI garde le token de la
# session dans sessionStorage (clé "vault-<backend>☃<cluster>", valeur JSON
# avec le champ token) ; on cherche un token Vault dans sessionStorage puis
# localStorage, sinon "".
TOKEN_FINDER_JS = (
    "(()=>{const R=/(?:hv[sb]\\.[A-Za-z0-9_-]{20,}|\\b[sb]\\.[A-Za-z0-9]{24})/;"
    "for(const s of[sessionStorage,localStorage]){for(let i=0;i<s.length;i++){"
    "const k=s.key(i)||'',v=s.getItem(k)||'';"
    "try{const o=JSON.parse(v);if(o&&typeof o.token=='string'&&R.test(o.token))return o.token.match(R)[0]}catch(e){}"
    "if(k.startsWith('vault')){const m=v.match(R);if(m)return m[0]}}}"
    "for(const s of[sessionStorage,localStorage])for(let i=0;i<s.length;i++){"
    "const m=(s.getItem(s.key(i))||'').match(R);if(m)return m[0]}return ''})()"
)


def find_chromium_browser() -> str | None:
    for candidate in BROWSER_CANDIDATES:
        path = candidate if os.path.isabs(candidate) else shutil.which(candidate)
        if path and os.path.exists(path):
            return path
    return None


class _WebSocket:
    """Client WebSocket minimal (texte, non fragmenté côté envoi) pour parler au
    protocole DevTools de Chrome sans dépendance externe."""

    def __init__(self, url: str, timeout: float = 10):
        parts = urllib.parse.urlsplit(url)
        self._sock = socket.create_connection((parts.hostname, parts.port), timeout=timeout)
        self._buffer = b""
        key = base64.b64encode(os.urandom(16)).decode()
        self._sock.sendall((
            f"GET {parts.path} HTTP/1.1\r\nHost: {parts.hostname}:{parts.port}\r\n"
            "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
        ).encode())
        while b"\r\n\r\n" not in self._buffer:
            self._buffer += self._recv()
        head, self._buffer = self._buffer.split(b"\r\n\r\n", 1)
        if b" 101 " not in head.split(b"\r\n", 1)[0]:
            raise ConnectionError(f"handshake WebSocket refusé: {head[:100]!r}")

    def __enter__(self) -> "_WebSocket":
        return self

    def __exit__(self, *exc: Any) -> None:
        self._sock.close()

    def _recv(self) -> bytes:
        chunk = self._sock.recv(65536)
        if not chunk:
            raise ConnectionError("WebSocket fermé")
        return chunk

    def _read(self, size: int) -> bytes:
        while len(self._buffer) < size:
            self._buffer += self._recv()
        data, self._buffer = self._buffer[:size], self._buffer[size:]
        return data

    def send(self, text: str) -> None:
        payload = text.encode()
        size = len(payload)
        if size < 126:
            header = bytes([0x81, 0x80 | size])
        elif size < 1 << 16:
            header = bytes([0x81, 0x80 | 126]) + size.to_bytes(2, "big")
        else:
            header = bytes([0x81, 0x80 | 127]) + size.to_bytes(8, "big")
        mask = os.urandom(4)
        self._sock.sendall(header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

    def receive(self) -> str:
        message = b""
        while True:
            first, second = self._read(2)
            size = second & 0x7F
            if size >= 126:
                size = int.from_bytes(self._read(2 if size == 126 else 8), "big")
            mask = self._read(4) if second & 0x80 else b""
            data = self._read(size)
            if mask:
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
            opcode = first & 0x0F
            if opcode == 0x8:
                raise ConnectionError("WebSocket fermé")
            if opcode in (0x9, 0xA):  # ping / pong
                continue
            message += data
            if first & 0x80:
                return message.decode("utf-8", errors="replace")


def devtools_evaluate(ws_url: str, expression: str, timeout: float = 10) -> Any:
    """Runtime.evaluate d'une expression dans une page, retourne sa valeur."""
    with _WebSocket(ws_url, timeout) as ws:
        ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate",
                            "params": {"expression": expression, "returnByValue": True}}))
        while True:
            reply = json.loads(ws.receive())
            if reply.get("id") == 1:
                return ((reply.get("result") or {}).get("result") or {}).get("value")


def _devtools_port(profile_dir: str, process: subprocess.Popen, timeout: float = 30) -> int:
    """Port DevTools choisi par Chrome (--remote-debugging-port=0), lu dans DevToolsActivePort."""
    path = os.path.join(profile_dir, "DevToolsActivePort")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and process.poll() is None:
        try:
            with open(path, encoding="utf-8") as fh:
                return int(fh.readline())
        except (OSError, ValueError):
            time.sleep(0.2)
    raise RuntimeError("le navigateur n'a pas exposé le protocole DevTools "
                       "(peut être bloqué par une politique d'entreprise)")


def _find_token_in_pages(port: int, evaluate: Callable[[str, str], Any] = devtools_evaluate) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/json/list", timeout=5) as response:
        targets = json.load(response)
    for target in targets:
        ws_url = target.get("webSocketDebuggerUrl")
        if target.get("type") != "page" or not ws_url:
            continue
        try:
            token = clean_token(str(evaluate(ws_url, TOKEN_FINDER_JS) or ""))
        except (OSError, ValueError):
            continue
        if _VAULT_TOKEN_RE.fullmatch(token):
            return token
    return ""


def browser_token(
    ui_url: str,
    timeout: float = BROWSER_LOGIN_TIMEOUT,
    browser: str | None = None,
    poll_interval: float = 2,
) -> str:
    """Ouvre l'UI Vault dans un Chrome / Edge dédié (profil temporaire, navigation
    privée), attend le login SSO, lit le token
    de session dans la page via le protocole DevTools puis ferme le navigateur.
    "" si échec ou délai dépassé."""
    browser = browser or find_chromium_browser()
    if not browser:
        _log("Chrome / Edge introuvable : récupération automatique impossible.")
        return ""
    profile_dir = tempfile.mkdtemp(prefix="cos-toolchain-")
    command = [browser, f"--user-data-dir={profile_dir}", "--remote-debugging-port=0",
               "--no-first-run", "--no-default-browser-check", "--new-window",
               "--incognito", ui_url]
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        port = _devtools_port(profile_dir, process)
        _log("Navigateur ouvert en navigation privée : se connecter à Vault "
             f"(attente {_duration(timeout)} max, fermer la fenêtre pour annuler).")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and process.poll() is None:
            try:
                token = _find_token_in_pages(port)
            except (OSError, ValueError):
                token = ""
            if token:
                return token
            time.sleep(poll_interval)
        _log("Aucun token récupéré dans le navigateur.")
        return ""
    except RuntimeError as exc:
        _log(f"Récupération automatique impossible : {exc}")
        return ""
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
        shutil.rmtree(profile_dir, ignore_errors=True)


def acquire_token_interactively(
    ui_url: str,
    read_clip: Callable[[], str] = read_clipboard,
    ask: Callable[[str], str] = getpass.getpass,
    open_url: Callable[[str], Any] = webbrowser.open,
    attempts: int = 3,
    auto: bool = True,
    auto_browser: Callable[[str], str] = browser_token,
) -> str:
    """Récupère un token depuis l'UI Vault :
    - auto : via Chrome / Edge piloté (browser_token), sans copier-coller ;
    - sinon : ouvre l'UI dans le navigateur par défaut, l'utilisateur copie le
      token ("Copy token" du menu utilisateur) puis il est lu dans le
      presse-papiers (ou pris s'il est collé)."""
    if auto:
        token = auto_browser(ui_url)
        if token:
            return token
        _log("Passage à la copie manuelle du token.")
    if not sys.stdin.isatty():
        _log("Terminal non interactif : impossible de demander le token "
             "(lancer le script dans un terminal, ou passer --vault-token).")
        return ""
    _log(f"Ouverture de {ui_url}")
    open_url(ui_url)
    _log("Se connecter, puis menu utilisateur > Copy token.")
    for _ in range(attempts):
        pasted = clean_token(ask("Entrée pour lire le presse-papiers (ou coller le token) : "))
        token = pasted or clean_token(read_clip())
        if token:
            return token
        _log("Aucun token trouvé.")
    return ""


# --------------------------------------------------------------------------- #
# Cache local : réglages (state.json) et secrets (trousseau macOS)
# --------------------------------------------------------------------------- #
#
# state.json ne contient que des réglages non sensibles (user du proxy, uid,
# version du provider...) et l'index des secrets mémorisés. Les secrets
# eux-mêmes (tokens Vault, API keys, mot de passe du proxy) vont dans le
# trousseau macOS via `security` ; sans trousseau ils ne vivent que le temps
# du processus et sont redemandés au lancement suivant. Rien de sensible n'est
# jamais écrit en clair sur le disque.

KEYCHAIN_SERVICE = "cos-toolchain"
# Anciennes sections de state.json qui contenaient des secrets en clair :
# purgées à la première lecture (versions antérieures du script).
LEGACY_SECRET_SECTIONS = ("vault_tokens", "api_keys", "proxy_passwords")


def state_path() -> str:
    """~/.cache/cos-toolchain/state.json (ou $XDG_CACHE_HOME/...)."""
    root = os.environ.get("XDG_CACHE_HOME") or os.path.join(os.path.expanduser("~"), ".cache")
    return os.path.join(root, "cos-toolchain", "state.json")


def _read_state() -> dict[str, Any]:
    try:
        with open(state_path(), encoding="utf-8") as fh:
            state = json.load(fh)
    except (OSError, ValueError):
        return {}
    if not isinstance(state, dict):
        return {}
    if any(section in state for section in LEGACY_SECRET_SECTIONS):
        for section in LEGACY_SECRET_SECTIONS:
            state.pop(section, None)
        _write_state(state)
        _log(f"Secrets en clair purgés de {state_path()} (ancienne version du script).")
    return state


def _write_state(state: dict[str, Any]) -> None:
    """Écrit le cache lisible par l'utilisateur seul (dossier 0700, fichier 0600)."""
    path = state_path()
    try:
        os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2)
    except OSError as exc:
        _log(f"Cache non sauvegardé ({exc}).")


def load_setting(name: str) -> str:
    value = (_read_state().get("settings") or {}).get(name)
    return value if isinstance(value, str) else ""


def save_setting(name: str, value: str) -> None:
    state = _read_state()
    state.setdefault("settings", {})[name] = value
    _write_state(state)


def _security_quote(arg: str) -> str:
    """Argument pour le mode interactif de `security` (double quotes, \\ et \" échappés)."""
    return '"' + arg.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _keychain(*args: str) -> subprocess.CompletedProcess | None:
    """`security` (trousseau macOS) ; None si indisponible. Une commande qui
    porte un secret (-w) est envoyée sur l'entrée standard de `security -i`,
    jamais en argument : les arguments d'un processus sont visibles de tous
    (ps, logs d'audit)."""
    if sys.platform != "darwin" or not shutil.which("security"):
        return None
    try:
        if "-w" in args[1:] and args[0] == "add-generic-password":
            line = " ".join(_security_quote(a) for a in args) + "\n"
            return subprocess.run(["security", "-i"], input=line, capture_output=True, text=True, timeout=15)
        return subprocess.run(["security", *args], capture_output=True, text=True, timeout=15)
    except (OSError, subprocess.SubprocessError):
        return None


class SecretStore:
    """Secrets nommés : trousseau macOS (service KEYCHAIN_SERVICE, compte = nom),
    sinon mémoire du processus. L'index des noms mémorisés est gardé dans
    state.json pour pouvoir tout oublier (--forget)."""

    def __init__(self, keychain: Callable[..., subprocess.CompletedProcess | None] | None = None):
        self._explicit_keychain = keychain
        self._memory: dict[str, str] = {}

    def _keychain(self, *args: str) -> subprocess.CompletedProcess | None:
        # Résolu à l'appel (et non à la construction) : les tests remplacent _keychain du module.
        return (self._explicit_keychain or _keychain)(*args)

    @property
    def persistent(self) -> bool:
        """Vrai si les secrets survivent au processus (trousseau disponible)."""
        return self._keychain("help") is not None

    def in_keychain(self, name: str) -> bool:
        """Vrai si `name` est lisible dans le trousseau (donc depuis un shell, via `security`)."""
        result = self._keychain("find-generic-password", "-a", name, "-s", KEYCHAIN_SERVICE, "-w")
        return result is not None and result.returncode == 0

    def load(self, name: str) -> str:
        result = self._keychain("find-generic-password", "-a", name, "-s", KEYCHAIN_SERVICE, "-w")
        if result is not None:
            return result.stdout.strip() if result.returncode == 0 else ""
        return self._memory.get(name, "")

    def save(self, name: str, value: str) -> bool:
        """Mémorise `value` ; vrai si elle survivra au processus."""
        self._index(name, add=True)
        result = self._keychain("add-generic-password", "-a", name, "-s", KEYCHAIN_SERVICE, "-w", value, "-U")
        if result is not None and result.returncode == 0:
            self._memory.pop(name, None)
            return True
        if result is not None:
            _log(f"Trousseau macOS indisponible ({result.stderr.strip()}) : secret gardé en mémoire seulement.")
        self._memory[name] = value
        return False

    def forget(self, name: str) -> None:
        self._keychain("delete-generic-password", "-a", name, "-s", KEYCHAIN_SERVICE)
        self._memory.pop(name, None)
        self._index(name, add=False)

    def forget_all(self) -> None:
        for name in list(_read_state().get("secrets") or []) + list(self._memory):
            self.forget(name)
        self._memory.clear()

    @staticmethod
    def _index(name: str, add: bool) -> None:
        state = _read_state()
        names = [n for n in (state.get("secrets") or []) if isinstance(n, str) and n != name]
        if add:
            names.append(name)
        state["secrets"] = names
        _write_state(state)


SECRETS = SecretStore()


def _token_name(vault_url: str) -> str:
    return f"vault-token:{vault_url.rstrip('/')}"


def load_cached_token(vault_url: str) -> str | None:
    return SECRETS.load(_token_name(vault_url)) or None


def save_cached_token(vault_url: str, token: str) -> None:
    SECRETS.save(_token_name(vault_url), token)


def forget_cached_token(vault_url: str) -> None:
    SECRETS.forget(_token_name(vault_url))


def _api_key_cache_key(vault_url: str, kv_path: str) -> str:
    return f"{vault_url.rstrip('/')}/v1/{kv_path.strip('/')}"


def load_cached_api_key(vault_url: str, kv_path: str, now: float | None = None) -> str | None:
    """API key sauvegardée pour ce secret si son lease est encore valide. Le
    lease (non sensible) est dans state.json, la clé dans le trousseau."""
    key = _api_key_cache_key(vault_url, kv_path)
    entry = (_read_state().get("api_key_leases") or {}).get(key)
    if not isinstance(entry, dict):
        return None
    expires_at = entry.get("expires_at")
    if expires_at is not None and float(expires_at) - (time.time() if now is None else now) < TOKEN_MIN_VALIDITY:
        return None
    return SECRETS.load(f"api-key:{key}") or None


def save_cached_api_key(vault_url: str, kv_path: str, api_key: str, lease: int | None) -> None:
    key = _api_key_cache_key(vault_url, kv_path)
    state = _read_state()
    state.setdefault("api_key_leases", {})[key] = {
        "expires_at": None if lease is None else time.time() + lease,
        "saved_at": time.time(),
    }
    _write_state(state)
    SECRETS.save(f"api-key:{key}", api_key)


def load_proxy_password(user: str) -> str:
    """Mot de passe du proxy mémorisé pour `user` (trousseau macOS)."""
    return SECRETS.load(f"proxy:{user}")


def save_proxy_password(user: str, password: str) -> None:
    """Mémorise le mot de passe validé dans le trousseau macOS ; sans trousseau
    il est gardé en mémoire et redemandé au prochain lancement."""
    if SECRETS.save(f"proxy:{user}", password):
        _log(f"Mot de passe du proxy mémorisé dans le trousseau macOS (service {KEYCHAIN_SERVICE}).")
    else:
        _log("Pas de trousseau : le mot de passe du proxy sera redemandé au prochain lancement.")


def forget_proxy_password(user: str) -> None:
    SECRETS.forget(f"proxy:{user}")


def forget_all() -> None:
    SECRETS.forget_all()
    try:
        os.remove(state_path())
    except FileNotFoundError:
        pass


# --------------------------------------------------------------------------- #
# Terraform : login et init
# --------------------------------------------------------------------------- #

def terraform_credentials_paths() -> list[str]:
    """Fichiers de credentials lus par tofu (`tofu login`) : credentials.tofurc.json,
    puis credentials.tfrc.json (écrit par un ancien `terraform login`, encore lu par tofu)."""
    if sys.platform == "win32":
        root = os.path.join(os.environ.get("APPDATA", os.path.expanduser("~")), "terraform.d")
    else:
        root = os.path.join(os.path.expanduser("~"), ".terraform.d")
    names = ["credentials.tofurc.json", "credentials.tfrc.json"]
    return [os.path.join(root, name) for name in names]


def terraform_credentials_path() -> str:
    return terraform_credentials_paths()[-1]


def terraform_logged_in(host: str, environ: dict[str, str] | None = None) -> bool:
    """Vrai si un token est enregistré pour `host` (TF_TOKEN_<host> ou
    credentials.tfrc.json écrit par `terraform login`)."""
    environ = os.environ if environ is None else environ
    if environ.get("TF_TOKEN_" + host.replace(".", "_").replace("-", "__")):
        return True
    for path in terraform_credentials_paths():
        try:
            with open(path, encoding="utf-8") as fh:
                credentials = json.load(fh)
        except (OSError, ValueError):
            continue
        entry = (credentials.get("credentials") or {}).get(host) if isinstance(credentials, dict) else None
        if isinstance(entry, dict) and entry.get("token"):
            return True
    return False


def run_terraform(args: Sequence[str], cwd: str, env: dict[str, str] | None = None,
                  to_stderr: bool = True, terraform: str | None = None) -> int:
    """Lance terraform et retourne son code de sortie. Avec to_stderr, sa sortie
    va sur stderr pour ne pas polluer les `export` imprimés sur stdout
    (eval "$(...)")."""
    terraform = terraform or TERRAFORM_BIN
    if not shutil.which(terraform):
        raise CliExit(EXIT_TERRAFORM_FAILED, f"{terraform} introuvable dans le PATH : installer OpenTofu "
                                             "(brew install opentofu, voir README.md)")
    command = [terraform, *args]
    _log(f"$ {' '.join(shlex.quote(a) for a in command)}  (dans {cwd})")
    try:
        return subprocess.call(command, cwd=cwd, env=env,
                               stdout=sys.stderr if to_stderr else None)
    except OSError as exc:
        raise CliExit(EXIT_TERRAFORM_FAILED, f"{terraform} : {exc}") from exc


def new_journal(cwd: str, env: str, command: str, now: float | None = None) -> str:
    """Crée <cwd>/logs/<horodatage>-<env>-<commande>.log, vide et lisible par
    toi seul (TF_LOG=debug peut contenir des en-têtes HTTP), et ne garde que
    les LOGS_KEPT journaux les plus récents du dossier."""
    directory = os.path.join(cwd, LOGS_DIR)
    os.makedirs(directory, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(now))
    path = os.path.join(directory, f"{stamp}-{env}-{re.sub(r'[^A-Za-z0-9]+', '_', command)}.log")
    os.close(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600))
    for name in sorted(f for f in os.listdir(directory) if f.endswith(".log"))[:-LOGS_KEPT]:
        try:
            os.remove(os.path.join(directory, name))
        except OSError:
            pass
    return path


def journal_level(tf_log: str | None, follow: str) -> str:
    """Niveau TF_LOG du journal : --tf-log (défaut debug), abaissé au niveau
    de --follow s'il est plus détaillé, sinon il n'y aurait rien à suivre."""
    level = (tf_log or DEFAULT_JOURNAL_LEVEL).lower()
    if follow in TF_LOG_LEVELS and level in TF_LOG_LEVELS:
        return min(level, follow, key=TF_LOG_LEVELS.index)
    return level


class JournalFilter:
    """Lignes du journal d'un niveau >= seuil, raccourcies pour le terminal
    (heure seule). Une ligne sans niveau est la suite du message précédent et
    suit son sort."""

    def __init__(self, level: str):
        self.threshold = TF_LOG_LEVELS.index(level)
        self.keeping = False

    def shown(self, line: str) -> str | None:
        match = _TF_LOG_LINE.match(line)
        if match:
            self.keeping = TF_LOG_LEVELS.index(match.group(2).lower()) >= self.threshold
            if match.group(1):
                line = match.group(1) + line[match.start(2) - 2:]
        return line if self.keeping and line.strip() else None


def follow_journal(path: str, level: str, stop: threading.Event,
                   out: Callable[[str], None] | None = None, poll: float = 0.3) -> None:
    """Recopie dans le terminal les lignes du journal au fil de l'eau, jusqu'à
    ce que `stop` soit levé et que le fichier soit lu en entier."""
    out = out or _log
    journal = JournalFilter(level)

    def show(line: str) -> None:
        shown = journal.shown(line)
        if shown is not None:
            out(f"  | {shown}")

    pending = ""
    with open(path, encoding="utf-8", errors="replace") as fh:
        while True:
            chunk = fh.read()
            if chunk:
                *lines, pending = (pending + chunk).split("\n")
                for line in lines:
                    show(line)
            elif stop.is_set():
                break
            else:
                stop.wait(poll)
    if pending:
        show(pending)


def scenario_files(cwd: str, command: Sequence[str]) -> list[str]:
    """Fichiers de scénario d'un `test` : ceux des -filter= de la commande,
    sinon tous les tests/*.tftest.hcl de <cwd>."""
    filters = [a[len("-filter="):] for a in command if a.startswith("-filter=")]
    if filters:
        return filters
    tests_dir = os.path.join(cwd, "tests")
    try:
        names = sorted(f for f in os.listdir(tests_dir) if f.endswith(".tftest.hcl"))
    except OSError:
        return []
    return [os.path.join("tests", name) for name in names]


def _run_scenario(command: Sequence[str], cwd: str, env: dict[str, str], log_path: str) -> int:
    """Un `tofu test` dont toute la sortie va dans log_path."""
    if not shutil.which(TERRAFORM_BIN):
        raise CliExit(EXIT_TERRAFORM_FAILED, f"{TERRAFORM_BIN} introuvable dans le PATH : installer OpenTofu "
                                             "(brew install opentofu, voir README.md)")
    command = [TERRAFORM_BIN, *command]
    with open(log_path, "a", encoding="utf-8") as fh:
        fh.write("$ " + " ".join(shlex.quote(a) for a in command) + "\n")
        fh.flush()
        try:
            return subprocess.call(command, cwd=cwd, env=env, stdout=fh, stderr=subprocess.STDOUT)
        except OSError as exc:
            fh.write(f"{exc}\n")
            return EXIT_TERRAFORM_FAILED


EXPECTED_FAILURES = "expected_failures.json"  # tests/ : cas de refus attendus (6x_refused_*)


def load_expected_failures(cwd: str) -> dict[str, dict]:
    """Manifeste des fichiers de refus attendu ({nom de fichier: {run, regex, prelude, message...}}),
    vide s'il n'existe pas (dossier sans scénarios générés)."""
    path = os.path.join(cwd, "tests", EXPECTED_FAILURES)
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except OSError:
        return {}
    except ValueError as exc:
        raise CliExit(EXIT_USAGE, f"{path} illisible : {exc}")


_RUN_VERDICT = re.compile(r'run "([^"]+)"\.\.\. (pass|fail|skip|error)')


def judge_scenario(log_path: str, expected: dict) -> tuple[int, str]:
    """Verdict d'un fichier de refus attendu : (code, résumé).

    Le provider orchestrator fait échouer l'apply d'une demande refusée en
    citant le motif du DAG ; tofu marque le run « fail ». Succès si les runs
    préalables passent, si le run attendu échoue, et si la sortie porte le
    motif attendu. Un run attendu en refus qui passe est une régression : le
    DAG accepte ce qu'il doit refuser."""
    try:
        with open(log_path, encoding="utf-8", errors="replace") as fh:
            text = fh.read()
    except OSError:
        return EXIT_TERRAFORM_FAILED, "journal illisible"
    verdicts = dict(_RUN_VERDICT.findall(text))
    run = expected["run"]
    failed_prelude = [name for name in expected.get("prelude", []) if verdicts.get(name) != "pass"]
    if failed_prelude:
        return EXIT_TERRAFORM_FAILED, f"préalable en échec ({', '.join(failed_prelude)}), le refus n'a pas été testé"
    verdict = verdicts.get(run)
    if verdict is None:
        return EXIT_TERRAFORM_FAILED, f"run « {run} » absent de la sortie"
    if verdict == "pass":
        return EXIT_TERRAFORM_FAILED, f"ACCEPTÉ alors que le DAG doit refuser : « {expected['message']} »"
    if re.search(expected["regex"], text, re.DOTALL):
        return EXIT_OK, f"refus attendu, motif conforme : « {expected['message']} »"
    return EXIT_TERRAFORM_FAILED, f"refusé, mais sans le motif attendu « {expected['message']} » (voir le journal)"


def _scenario_summary(log_path: str) -> str:
    """Dernière ligne « Success! … » / « Failure! … » de tofu, sinon la dernière ligne."""
    try:
        lines = [line.rstrip() for line in open(log_path, encoding="utf-8", errors="replace") if line.strip()]
    except OSError:
        return ""
    verdicts = [line for line in lines if line.lstrip().startswith(("Success!", "Failure!"))]
    return (verdicts or lines or [""])[-1].strip()


def run_tests_parallel(command: Sequence[str], cwd: str, env: dict[str, str], workers: int, env_name: str,
                       tf_log: str | None = None, runner: Callable[..., int] | None = None,
                       now: float | None = None) -> int:
    """Lance `tofu test` une fois par fichier de scénario, `workers` à la fois,
    chaque sortie dans <cwd>/logs/<horodatage>-<env>-<scénario>.log, et
    résume. Code de sortie : 0 si tout passe, sinon celui du premier échec."""
    runner = runner or _run_scenario
    files = scenario_files(cwd, command)
    if not files:
        raise CliExit(EXIT_USAGE, f"aucun fichier de scénario dans {os.path.join(cwd, 'tests')}")
    expected_failures = load_expected_failures(cwd)
    base = [a for a in command if not a.startswith("-filter=")]
    if "-no-color" not in base:
        base.append("-no-color")
    workers = max(1, min(workers, len(files)))
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(now))
    directory = os.path.join(cwd, LOGS_DIR)
    os.makedirs(directory, exist_ok=True)
    _log(f"{len(files)} scénario(s), {workers} en parallèle ; journaux dans {directory}/{stamp}-{env_name}-*.log")

    def one(path: str) -> tuple[str, int, float, str]:
        name = re.sub(r"\.tftest\.hcl$", "", os.path.basename(path))
        log_path = os.path.join(directory, f"{stamp}-{env_name}-{re.sub(r'[^A-Za-z0-9]+', '_', name)}.log")
        os.close(os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600))
        scenario_env = dict(env)
        expected = expected_failures.get(os.path.basename(path))
        # Fichier de refus attendu : au moins le niveau error du provider dans le
        # journal, c'est là que le motif du DAG (status reason) est cité en entier.
        level = tf_log or ("error" if expected else None)
        if level:
            scenario_env.update(TF_LOG=level, TF_LOG_PATH=log_path)
        _log(f"  ▶ {name}")
        started = time.monotonic()
        code = runner([*base, f"-filter={path}"], cwd, scenario_env, log_path)
        elapsed = time.monotonic() - started
        if expected:  # fichier de refus attendu : c'est le motif qui compte, pas le code de tofu
            code, summary = judge_scenario(log_path, expected)
        else:
            summary = _scenario_summary(log_path) or f"code {code}"
        _log(f"  {'✔' if code == 0 else '✘'} {name} ({_duration(elapsed)}) : {summary}")
        return name, code, elapsed, log_path

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(one, files))

    for name in sorted(f for f in os.listdir(directory) if f.endswith(".log"))[:-PARALLEL_LOGS_KEPT]:
        try:
            os.remove(os.path.join(directory, name))
        except OSError:
            pass
    failed = [r for r in results if r[1] != 0]
    _log(f"\n{len(results) - len(failed)} scénario(s) OK, {len(failed)} en échec"
         + (" :\n" + "\n".join(f"  ✘ {n} -> {p}" for n, _, _, p in failed) if failed else "."))
    return failed[0][1] if failed else EXIT_OK


def run_with_journal(command: Sequence[str], cwd: str, env: dict[str, str], tf_log: str | None, follow: str,
                     env_name: str, run: Callable[..., int] | None = None) -> int:
    """Lance tofu avec son journal dans un fichier de <cwd>/logs/ et, sauf
    --follow off, les lignes d'un niveau >= follow en direct sur stderr."""
    run = run or run_terraform
    journal = new_journal(cwd, env_name, command[0])
    level = journal_level(tf_log, follow)
    env = {**env, "TF_LOG": level, "TF_LOG_PATH": journal}
    following = follow in TF_LOG_LEVELS and level in TF_LOG_LEVELS
    _log(f"Journal {TERRAFORM_BIN} (TF_LOG={level}) : {journal}\n"
         + (f"  lignes {follow} et plus affichées ici, préfixées par « | » (--follow NIVEAU pour en voir "
            f"plus ou moins, --follow {FOLLOW_OFF} pour rien)." if following
            else f"  à suivre dans un autre terminal : tail -f {shlex.quote(journal)}"))
    stop = threading.Event()
    follower = threading.Thread(target=follow_journal, args=(journal, follow, stop), daemon=True) if following else None
    if follower:
        follower.start()
    try:
        return run(command, cwd, env, to_stderr=False)
    finally:
        stop.set()
        if follower:
            follower.join(timeout=5)
        _log(f"Journal complet : {journal}")


def ensure_terraform_login(host: str, cwd: str, run: Callable[..., int] = run_terraform) -> None:
    if terraform_logged_in(host):
        _log(f"{TERRAFORM_BIN} login : credentials déjà enregistrés pour {host}.")
        return
    if not sys.stdin.isatty():
        raise CliExit(EXIT_TERRAFORM_FAILED,
                      f"aucun credential pour {host} : lancer `{TERRAFORM_BIN} login {host}` "
                      "dans un terminal (ou --skip-login).")
    code = run(["login", host], cwd)
    if code != 0:
        raise CliExit(EXIT_TERRAFORM_FAILED, f"{TERRAFORM_BIN} login {host} a échoué (code {code})")


def ensure_terraform_init(cwd: str, reinit: bool = False, run: Callable[..., int] = run_terraform,
                          version_changed: bool = False) -> None:
    if not reinit and not version_changed and os.path.isdir(os.path.join(cwd, ".terraform")):
        _log(f"{TERRAFORM_BIN} init : déjà fait dans {cwd} (--reinit pour refaire).")
        return
    code = run(["init", "-upgrade"] if version_changed else ["init"], cwd)
    if code != 0:
        raise CliExit(EXIT_TERRAFORM_FAILED, f"{TERRAFORM_BIN} init a échoué (code {code})")


def default_tests_dir(env: str, root: str | None = None) -> str:
    """<dossier du script>/terraform (root terraform test) s'il existe, sinon
    l'ancienne arborescence <env>/new_version ou <env>."""
    root = root or os.path.dirname(os.path.abspath(__file__))
    terraform_root = os.path.join(root, TERRAFORM_ROOT_DIR)
    if os.path.isdir(terraform_root):
        return terraform_root
    with_version = os.path.join(root, env, NEW_VERSION_DIR)
    return with_version if os.path.isdir(with_version) else os.path.join(root, env)


def env_tfvars_path(cwd: str, env: str) -> str | None:
    """<cwd>/envs/<env>.tfvars (ou env/) s'il existe (nouvelle arborescence), sinon None."""
    for directory in ENV_TFVARS_DIRS:
        path = os.path.join(cwd, directory, f"{env}.tfvars")
        if os.path.isfile(path):
            return path
    return None


def terraform_version(terraform: str | None = None) -> tuple[int, ...] | None:
    """(major, minor, patch) de `terraform version -json` (tofu garde la même
    clé terraform_version), None si inconnu."""
    terraform = terraform or TERRAFORM_BIN
    try:
        out = subprocess.run([terraform, "version", "-json"], capture_output=True, text=True, timeout=30).stdout
        raw = json.loads(out).get("terraform_version", "")
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", raw)
    return tuple(int(x) for x in match.groups()) if match else None


def normalize_test_filter(path: str, cwd: str) -> str:
    """Chemin d'un -filter relatif à <cwd> (là où tofu tourne) : accepte aussi
    un chemin depuis le dossier du script (terraform/tests/x), un chemin absolu,
    ou le seul nom du fichier (x -> tests/x)."""
    path = path.strip().strip("\"'").replace("\u00a0", "").replace("\u2011", "-").replace("\u2013", "-")
    candidates = [path, os.path.join("tests", os.path.basename(path))]
    parts = path.replace("\\", "/").split("/")
    candidates += ["/".join(parts[i:]) for i in range(1, len(parts))]
    for candidate in candidates:
        full = candidate if os.path.isabs(candidate) else os.path.join(cwd, candidate)
        if os.path.isfile(full):
            return os.path.relpath(full, cwd)
    tests_dir = os.path.join(cwd, "tests")
    # Nom qui ne diffère que par des espaces / caractères invisibles (fichier
    # renommé à la main) : on le prend tel quel et on conseille de le renommer.
    wanted = unicodedata.normalize("NFC", os.path.basename(path)).strip()
    try:
        entries = os.listdir(tests_dir)
    except OSError:
        entries = []
    for entry in entries:
        cleaned = "".join(c for c in unicodedata.normalize("NFC", entry) if c.isprintable() and not c.isspace())
        if cleaned == wanted and os.path.isfile(os.path.join(tests_dir, entry)):
            _log(f"Fichier trouvé sous le nom {entry!r} (caractères parasites) : à renommer en {wanted!r}.")
            return os.path.join("tests", entry)
    try:
        found = ", ".join(sorted(os.listdir(tests_dir))) or "(vide)"
    except OSError as exc:
        found = f"dossier absent ({exc.strerror})"
    raise CliExit(EXIT_USAGE, f"fichier de scénario introuvable : {path!r} (chemins relatifs à {cwd}, "
                              f"ex. -filter=tests/20_bucket_lifecycle.tftest.hcl).\n  {tests_dir} contient : {found}")


def adapt_test_filter(command: Sequence[str], cwd: str, version: tuple[int, ...] | None) -> list[str]:
    """Normalise les -filter (relatifs au dossier où tofu tourne). Puis, comme
    `test -filter=<fichier>` n'existe qu'à partir de 1.7, en 1.6 les fichiers
    demandés sont liés dans <cwd>/.tftest-filter/ et tofu reçoit
    -test-directory=.tftest-filter à la place."""
    command = [f"-filter={normalize_test_filter(a[len('-filter='):], cwd)}" if a.startswith("-filter=") else a
               for a in command] if command and command[0] == "test" else list(command)
    filters = [a[len("-filter="):] for a in command if a.startswith("-filter=")]
    if not command or command[0] != "test" or not filters or version is None or version >= (1, 7):
        return command
    link_dir = os.path.join(cwd, TEST_FILTER_DIR)
    shutil.rmtree(link_dir, ignore_errors=True)
    os.makedirs(link_dir)
    for f in filters:
        target = os.path.join(cwd, f)
        os.symlink(os.path.relpath(target, link_dir), os.path.join(link_dir, os.path.basename(f)))
    _log(f"{TERRAFORM_BIN} {'.'.join(map(str, version))} : -filter émulé via -test-directory={TEST_FILTER_DIR} "
         f"(passer en {TERRAFORM_BIN} >= 1.7 pour le vrai -filter).")
    return [a for a in command if not a.startswith("-filter=")] + [f"-test-directory={TEST_FILTER_DIR}"]


def tfvars_value(path: str, name: str) -> str | None:
    """Valeur (chaîne) de `name = "..."` dans un fichier tfvars, sans parser HCL."""
    try:
        with open(path, encoding="utf-8") as fh:
            match = re.search(rf'^\s*{re.escape(name)}\s*=\s*"([^"]*)"', fh.read(), re.MULTILINE)
    except OSError:
        return None
    return match.group(1) if match else None


def write_versions_tf(cwd: str, env: str) -> str | None:
    """Écrit <cwd>/versions.tf avec la version du provider de envs/<env>.tfvars
    (rien si l'arborescence n'a pas de tfvars). Retourne la version écrite."""
    tfvars = env_tfvars_path(cwd, env)
    if not tfvars:
        return None
    version = tfvars_value(tfvars, "provider_version")
    if not version:
        raise CliExit(EXIT_USAGE, f"provider_version manquant dans {tfvars}")
    path = os.path.join(cwd, VERSIONS_TF)
    content = VERSIONS_TF_TEMPLATE % (PROVIDER_SOURCE, version)
    try:
        with open(path, encoding="utf-8") as fh:
            unchanged = fh.read() == content
    except OSError:
        unchanged = False
    if unchanged:
        _log(f"{VERSIONS_TF} : provider orchestrator {version} (inchangé).")
        return version
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
    _log(f"{VERSIONS_TF} écrit : provider orchestrator {version} (depuis {os.path.relpath(tfvars, cwd)}).")
    return version


def with_var_file(command: Sequence[str], cwd: str, env: str) -> list[str]:
    """Ajoute -var-file=envs/<env>.tfvars aux sous-commandes qui l'acceptent,
    sauf si l'utilisateur en a déjà passé un."""
    command = list(command)
    tfvars = env_tfvars_path(cwd, env)
    if not command or command[0] not in VAR_FILE_COMMANDS or not tfvars:
        return command
    if any(a == "-var-file" or a.startswith("-var-file=") for a in command):
        return command
    return [command[0], f"-var-file={os.path.relpath(tfvars, cwd)}", *command[1:]]


# --------------------------------------------------------------------------- #
# Variables d'environnement
# --------------------------------------------------------------------------- #

def proxy_url(host_port: str, user: str, password: str) -> str:
    """http://<user>:<mot de passe>@<host:port>, identifiants encodés (ex: ! -> %21)."""
    host_port = re.sub(r"^https?://", "", host_port).rstrip("/")
    if not user:
        return f"http://{host_port}"
    return f"http://{urllib.parse.quote(user, safe='')}:{urllib.parse.quote(password, safe='')}@{host_port}"


def resolve_proxy(args: argparse.Namespace, ask: Callable[[str], str] = input,
                  ask_secret: Callable[[str], str] = getpass.getpass,
                  environ: dict[str, str] | None = None, ignore_shell: bool = False) -> dict[str, str]:
    """Variables http_proxy / https_proxy / no_proxy à utiliser : rien avec
    --no-proxy ; celles déjà exportées dans le shell si --proxy n'est pas passé
    explicitement ; sinon le proxy d'entreprise avec le user (--proxy-user,
    $PROXY_USER, mémorisé, ou demandé) et le mot de passe (--proxy-password,
    $PROXY_PASSWORD, ou demandé sans écho, jamais sauvegardé)."""
    environ = os.environ if environ is None else environ
    args.proxy_origin = "none"
    if args.no_proxy:
        return {}
    existing = environ.get("https_proxy") or environ.get("HTTPS_PROXY")
    if existing and not args.proxy_from_cli and not ignore_shell:
        _log(f"Proxy déjà exporté dans le shell, réutilisé : {mask_url(existing)} (--proxy pour l'ignorer)")
        args.proxy_origin = "shell"
        return {"http_proxy": environ.get("http_proxy") or environ.get("HTTP_PROXY") or existing,
                "https_proxy": existing,
                "no_proxy": environ.get("no_proxy") or environ.get("NO_PROXY") or args.no_proxy_hosts}
    args.proxy_origin = "asked"
    user = args.proxy_user or load_setting("proxy_user")
    password = args.proxy_password or (load_proxy_password(user) if user and not args.new_proxy_password else "")
    if password and not args.proxy_password:
        _log(f"Proxy {args.proxy} : mot de passe mémorisé pour {user} réutilisé (--new-proxy-password pour le changer).")
        args.proxy_origin = "saved"
    if not user or not password:
        if not sys.stdin.isatty():
            raise CliExit(EXIT_USAGE, f"proxy {args.proxy} : passer --proxy-user / --proxy-password "
                                      f"(ou ${PROXY_USER_ENV} / ${PROXY_PASSWORD_ENV}), ou --no-proxy")
        _log(f"Proxy {args.proxy} : identifiants de ton compte (mémorisés après vérification).")
        user = (ask(f"User du proxy{f' [Entrée = {user}]' if user else ''} : ").strip() or user)
        if not user:
            raise CliExit(EXIT_USAGE, "user du proxy manquant")
        password = args.proxy_password or (load_proxy_password(user) if not args.new_proxy_password else "")
        if password and not args.proxy_password:
            args.proxy_origin = "saved"
        else:
            password = ask_secret(f"Mot de passe du proxy pour {user} : ")
            if not password:
                raise CliExit(EXIT_USAGE, "mot de passe du proxy manquant")
    if user != load_setting("proxy_user"):
        save_setting("proxy_user", user)
    args.proxy_credentials = (user, password)
    url = proxy_url(args.proxy, user, password)
    return {"http_proxy": url, "https_proxy": url, "no_proxy": args.no_proxy_hosts}


def with_no_proxy(proxy_vars: dict[str, str], *urls: str) -> dict[str, str]:
    """Ajoute l'hôte de chaque URL à no_proxy (hôtes intranet joints en direct,
    comme le service token, que le proxy ne sait pas résoudre)."""
    if not proxy_vars:
        return proxy_vars
    hosts = [h.strip() for h in proxy_vars.get("no_proxy", "").split(",") if h.strip()]
    for url in urls:
        host = urllib.parse.urlsplit(url).hostname if url else None
        if host and host not in hosts and not any(host.endswith(h) for h in hosts if h.startswith(".")):
            hosts.append(host)
    return {**proxy_vars, "no_proxy": ",".join(hosts)}


def apply_proxy(proxy_vars: dict[str, str]) -> None:
    """Exporte le proxy dans le process courant pour les appels urllib qui suivent
    (service token, Vault) : urllib lit http_proxy / https_proxy / no_proxy."""
    for name in PROXY_ENV_VARS:
        os.environ.pop(name.upper(), None)
        if name in proxy_vars:
            os.environ[name] = proxy_vars[name]
        else:
            os.environ.pop(name, None)


def check_proxy(proxy_vars: dict[str, str], url: str = PROXY_CHECK_URL, timeout: int = 15) -> str:
    """Vérifie que le proxy laisse passer vers `url`. Retourne "" si oui, sinon
    la raison ("identifiants refusés" pour un 407, ou l'erreur réseau)."""
    if not proxy_vars:
        return ""
    request = urllib.request.Request(url, method="HEAD")
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"https": proxy_vars["https_proxy"], "http": proxy_vars["http_proxy"]}),
        urllib.request.HTTPSHandler(context=tls_context()))
    try:
        with opener.open(request, timeout=timeout):
            pass
    except urllib.error.HTTPError as exc:
        if exc.code == 407:
            return f"identifiants refusés (HTTP 407 {exc.reason})"
        _log(f"Proxy OK ({url} répond HTTP {exc.code}).")
    except (urllib.error.URLError, OSError) as exc:
        reason = getattr(exc, "reason", exc)
        if "407" in str(reason) or "authenticationrequired" in str(reason).lower():
            return f"identifiants refusés ({reason})"
        return f"{url} injoignable via ce proxy : {reason}"
    else:
        _log(f"Proxy OK ({url} joignable).")
    return ""


def resolve_and_check_proxy(args: argparse.Namespace, ask: Callable[[str], str] = input,
                            ask_secret: Callable[[str], str] = getpass.getpass,
                            check: Callable[[dict[str, str]], str] = check_proxy) -> dict[str, str]:
    """resolve_proxy + vérification. Un proxy repris du shell qui ne répond pas
    (proxy local arrêté, 503...) est abandonné au profit du proxy d'entreprise
    avec identifiants ; des identifiants refusés (407) arrêtent le script."""
    proxy_vars = resolve_proxy(args, ask, ask_secret)
    if not proxy_vars or args.skip_proxy_check:
        return proxy_vars
    problem = check(proxy_vars)
    if problem and args.proxy_origin == "shell":
        _log(f"Proxy du shell inutilisable ({problem}) : passage au proxy {args.proxy} avec identifiants.")
        proxy_vars = resolve_proxy(args, ask, ask_secret, ignore_shell=True)
        problem = check(proxy_vars)
    if problem and "identifiants refusés" in problem and args.proxy_origin == "saved" and sys.stdin.isatty():
        user = args.proxy_credentials[0]
        _log(f"Mot de passe mémorisé refusé par le proxy ({problem}) : oublié, à ressaisir.")
        forget_proxy_password(user)
        args.new_proxy_password = True
        proxy_vars = resolve_proxy(args, ask, ask_secret, ignore_shell=True)
        problem = check(proxy_vars)
    if problem:
        if "identifiants refusés" in problem:
            raise CliExit(EXIT_USAGE, f"proxy {mask_url(proxy_vars['https_proxy'])} : {problem}. Vérifier le user "
                                      "et le mot de passe (--proxy-user / --proxy-password), ou le compte bloqué.")
        _log(f"Proxy non vérifié ({problem}) ; tofu échouera si le proxy refuse.")
    elif args.proxy_origin == "asked" and getattr(args, "proxy_credentials", None):
        save_proxy_password(*args.proxy_credentials)
    return proxy_vars


def build_env_vars(api_key: str, tf_log: str | None = None,
                   proxy_vars: dict[str, str] | None = None) -> dict[str, str]:
    variables = {name: api_key for name in API_KEY_VARS}
    if tf_log:
        variables["TF_LOG"] = tf_log
    # terraform (Go) lit HTTPS_PROXY avant https_proxy, curl l'inverse : on
    # exporte les deux casses pour qu'une variable en majuscules déjà présente
    # dans le shell (souvent sans identifiants) ne prenne pas le dessus.
    for name, value in (proxy_vars or {}).items():
        variables[name] = value
        variables[name.upper()] = value
    return variables


class ShellRef(str):
    """Morceau d'une valeur exportée à insérer tel quel dans le shell (non quoté) :
    une substitution `$(...)` ou une variable `$x`, à la place d'un secret."""


URL_ENCODE_FILTER = ("python3 -c 'import sys,urllib.parse;"
                     "print(urllib.parse.quote(sys.stdin.read().rstrip(chr(10)),safe=\"\"))'")


def keychain_ref(name: str, url_encode: bool = False) -> ShellRef:
    """Lecture d'un secret du trousseau par le shell, au moment de l'eval ;
    url_encode pour un secret inséré dans une URL (mot de passe du proxy)."""
    read = f"security find-generic-password -a {shlex.quote(name)} -s {shlex.quote(KEYCHAIN_SERVICE)} -w"
    return ShellRef(f"$({read} | {URL_ENCODE_FILTER})" if url_encode else f"$({read})")


def _export_value(parts: Sequence[str]) -> str:
    return "".join(f'"{part}"' if isinstance(part, ShellRef) else shlex.quote(part) for part in parts) or "''"


def export_lines(variables: dict[str, str], parts: dict[str, Sequence[str]] | None = None) -> str:
    """Lignes `export NAME=valeur`. Pour les noms présents dans `parts`, la valeur
    est composée des morceaux donnés (les ShellRef restent des expressions shell)."""
    parts = parts or {}
    return "".join(f"export {name}={_export_value(parts.get(name) or [value])}\n" for name, value in variables.items())


SECRET_VARIABLES = (*API_KEY_VARS, *PROXY_ENV_VARS[:2], *(v.upper() for v in PROXY_ENV_VARS[:2]))


def secret_export_parts(variables: dict[str, str], api_key_ref: str, proxy_user: str,
                        proxy_from_shell: bool, store: "SecretStore | None" = None) -> dict[str, Sequence[str]]:
    """Comment exporter chaque variable secrète SANS écrire le secret : l'API key
    et le mot de passe du proxy sont lus dans le trousseau par le shell au
    moment de l'eval ; un proxy repris du shell est re-référencé tel quel.
    Les variables secrètes absentes du résultat n'ont pas de forme sûre."""
    store = store or SECRETS
    parts: dict[str, Sequence[str]] = {}
    if api_key_ref and store.in_keychain(api_key_ref):
        for name in API_KEY_VARS:
            if name in variables:
                parts[name] = [keychain_ref(api_key_ref)]
    for name in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
        url = variables.get(name)
        if not url or not _MASK_RE.search(url):
            continue  # pas d'identifiants dans l'URL : rien de secret
        if proxy_from_shell:
            parts[name] = [ShellRef("$" + name.lower())]
        elif proxy_user and store.in_keychain(f"proxy:{proxy_user}"):
            scheme, host = url.split("://", 1)[0], url.rsplit("@", 1)[-1]
            parts[name] = [f"{scheme}://{urllib.parse.quote(proxy_user, safe='')}:",
                           keychain_ref(f"proxy:{proxy_user}", url_encode=True), f"@{host}"]
    return parts


def mask_secrets(variables: dict[str, str]) -> dict[str, str]:
    """Copie des variables avec les secrets masqués (sortie --json)."""
    masked = {}
    for name, value in variables.items():
        if name in API_KEY_VARS:
            masked[name] = _mask(value)
        elif name.lower() in PROXY_ENV_VARS[:2]:
            masked[name] = mask_url(value)
        else:
            masked[name] = value
    return masked


def _mask(value: str) -> str:
    return value[:4] + "…" + value[-4:] if len(value) > 12 else "…"


# --------------------------------------------------------------------------- #
# Enchaînement : token Vault -> API key
# --------------------------------------------------------------------------- #

def _check_token(client: VaultClient, token: str, origin: str) -> int | None | bool:
    """TTL du token (None = n'expire pas), False si Vault le refuse ou s'il
    expire trop tôt."""
    try:
        ttl = client.token_ttl(token)
    except VaultError as exc:
        if exc.status_code in (401, 403):
            _log(f"Token Vault {origin} refusé ({exc.status_code}).")
            return False
        raise
    if ttl is not None and ttl < TOKEN_MIN_VALIDITY:
        _log(f"Token Vault {origin} expiré ou presque ({ttl} s).")
        return False
    return ttl


def resolve_uid(args: argparse.Namespace, ask: Callable[[str], str] = input) -> str:
    """uid envoyé au service token : --uid / $TOOLCHAIN_UID, sinon celui mémorisé,
    sinon demandé (puis mémorisé)."""
    uid = args.uid or load_setting("uid")
    if not uid:
        if not sys.stdin.isatty():
            raise CliExit(EXIT_USAGE, f"uid manquant pour le service token : --uid ou ${UID_ENV}")
        uid = ask(f"uid pour le service token (ex: lh90871) [Entrée = {getpass.getuser()}] : ").strip() \
            or getpass.getuser()
    if uid != load_setting("uid"):
        save_setting("uid", uid)
    return uid


def resolve_vault_token(args: argparse.Namespace, client: VaultClient,
                        acquire: Callable[[str], str] | None = None) -> str:
    """Token Vault utilisable : --vault-token / $VAULT_TOKEN s'il est accepté,
    sinon le token sauvegardé s'il l'est encore, sinon un nouveau via le service
    token (ou l'UI Vault), puis sauvegardé. Chaque token est vérifié par lookup-self."""
    candidates: list[tuple[str, str]] = []
    if args.vault_token:
        candidates.append((clean_token(args.vault_token), "fourni"))
    cached = None if args.new_token else load_cached_token(args.vault_url)
    if cached:
        candidates.append((cached, "sauvegardé"))
    for token, origin in candidates:
        ttl = _check_token(client, token, origin)
        if ttl is not False:
            _log(f"Token Vault {origin} valide" + ("." if ttl is None else f" encore {_duration(ttl)}."))
            if token != cached:
                save_cached_token(args.vault_url, token)
            return token
        if origin == "sauvegardé":
            forget_cached_token(args.vault_url)
        elif args.vault_token_from_cli:
            raise CliExit(EXIT_USAGE, "le token passé par --vault-token est refusé par Vault")

    token = ""
    if args.token_service and not args.browser_token and not args.manual_token:
        try:
            token = service_token(args.token_service, resolve_uid(args), args.namespace,
                                  args.timeout, args.ca_bundle)
        except VaultError as exc:
            _log(f"Service token : {exc}\nPassage par l'UI Vault.")
    if not token:
        token = (acquire or (lambda url: acquire_token_interactively(url, auto=not args.manual_token)))(args.ui_url)
    if not token:
        raise CliExit(EXIT_USAGE, f"Token Vault manquant : --vault-token, ${VAULT_TOKEN_ENV}, "
                                  f"ou connexion sur {args.ui_url}")
    token = clean_token(token)
    ttl = _check_token(client, token, "récupéré")
    if ttl is False:
        raise CliExit(EXIT_USAGE, "le token récupéré est refusé par Vault")
    _log("Token Vault récupéré, valide" + ("." if ttl is None else f" encore {_duration(ttl)}."))
    save_cached_token(args.vault_url, token)
    return token


def resolve_api_key(args: argparse.Namespace, client: VaultClient,
                    acquire: Callable[[str], str] | None = None) -> str:
    """API key IBM Cloud : celle sauvegardée si son lease est encore valide,
    sinon lue dans Vault avec un token valide (puis sauvegardée)."""
    if not args.new_key and not args.new_token:
        cached = load_cached_api_key(args.vault_url, args.kv_path)
        if cached:
            args.api_key_ref = f"api-key:{_api_key_cache_key(args.vault_url, args.kv_path)}"
            _log(f"API key sauvegardée réutilisée ({_mask(cached)}, {state_path()}).")
            return cached
    token = resolve_vault_token(args, client, acquire)
    try:
        data, lease = client.read_kv(token, args.kv_path)
    except VaultError as exc:
        if exc.status_code in (401, 403) and not args.vault_token_from_cli:
            # token sauvegardé accepté par lookup-self mais sans droit sur le
            # secret : on ne le garde pas, l'utilisateur doit se reconnecter.
            forget_cached_token(args.vault_url)
        raise CliExit(EXIT_VAULT_FAILED, f"lecture de {args.kv_path} refusée : {exc}") from exc
    api_key = extract_api_key(data)
    if not api_key:
        raise CliExit(EXIT_VAULT_FAILED, f"pas d'api_key dans la réponse de {args.kv_path} "
                                         f"(champs: {', '.join(sorted(data)) or 'aucun'})")
    _log(f"API key lue dans Vault ({_mask(api_key)}"
         + ("" if lease is None else f", lease {_duration(lease)}") + ").")
    save_cached_api_key(args.vault_url, args.kv_path, api_key, lease)
    args.api_key_ref = f"api-key:{_api_key_cache_key(args.vault_url, args.kv_path)}"
    return api_key


# --------------------------------------------------------------------------- #
# Ligne de commande
# --------------------------------------------------------------------------- #

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-i", "--interactive", action="store_true",
                        help="mode guidé : menus numérotés (défaut quand le script est lancé sans argument)")
    parser.add_argument("--env", choices=list(ENVIRONMENTS), default=os.environ.get(ENV_VAR) or DEFAULT_ENV,
                        help=f"environnement des tests toolchain (défaut: ${ENV_VAR}, sinon {DEFAULT_ENV})")
    parser.add_argument("--dir", help="dossier terraform des tests (défaut: terraform/ à côté du script, "
                                      "sinon <env>/new_version ou <env>)")
    parser.add_argument("--prefix", help="TF_VAR_prefix : préfixe des descriptions dans l'orchestrateur "
                                         "(défaut: user du proxy, sinon user système)")

    vault = parser.add_argument_group("vault")
    vault.add_argument("--vault", choices=list(VAULTS), help="instance Vault (défaut: celle de --env)")
    vault.add_argument("--vault-url", help="URL de Vault (défaut: celle de --vault)")
    vault.add_argument("--secret-path", dest="kv_path", help="chemin KV de la clé IBM dans Vault (défaut: celui de --env)")
    vault.add_argument("--namespace", default=os.environ.get(NAMESPACE_ENV) or DEFAULT_NAMESPACE,
                       help=f"X-Vault-Namespace (défaut: ${NAMESPACE_ENV}, sinon {DEFAULT_NAMESPACE})")
    vault.add_argument("--vault-token", default=os.environ.get(VAULT_TOKEN_ENV),
                       help=f"token Vault (défaut: ${VAULT_TOKEN_ENV}) ; absent ou refusé : service token / UI Vault")
    vault.add_argument("--token-service", metavar="URL",
                       help="service token (défaut: celui de --vault dans TOKEN_SERVICES ; \"\" : aucun, UI Vault)")
    vault.add_argument("--uid", default=os.environ.get(UID_ENV),
                       help=f"uid passé au service token (défaut: ${UID_ENV}, sinon mémorisé ou demandé)")
    vault.add_argument("--browser-token", action="store_true",
                       help="ne pas appeler le service token : Chrome / Edge sur l'UI Vault")
    vault.add_argument("--manual-token", action="store_true",
                       help="ni service token ni Chrome / Edge : copier le token à la main depuis l'UI Vault")
    vault.add_argument("--new-token", action="store_true",
                       help="ignorer le token et l'API key sauvegardés, se reconnecter à Vault")
    vault.add_argument("--new-key", action="store_true",
                       help="ignorer l'API key sauvegardée, la relire dans Vault")
    vault.add_argument("--probe", action="store_true",
                       help="diagnostic réseau du service token (sondage TCP + GET direct / IPv4 / proxy), puis quitter")
    vault.add_argument("--forget", action="store_true",
                       help="supprimer tout ce qui est sauvegardé (tokens, API keys, mot de passe proxy), puis quitter")
    vault.add_argument("--ca-bundle", metavar="PEM",
                       help=f"bundle PEM des CA internes pour Vault et le service token "
                            f"(défaut: ${CA_BUNDLE_ENV}, $SSL_CERT_FILE, et les trousseaux système macOS)")

    terraform = parser.add_argument_group("opentofu")
    terraform.add_argument("--terraform-host", default=TERRAFORM_HOST,
                           help=f"hôte du `tofu login` (défaut: {TERRAFORM_HOST})")
    terraform.add_argument("--skip-login", action="store_true", help="ne pas vérifier / faire tofu login")
    terraform.add_argument("--skip-init", action="store_true", help="ne pas faire tofu init")
    terraform.add_argument("--reinit", action="store_true", help="refaire tofu init même si déjà fait")
    terraform.add_argument("--tf-log", nargs="?", const="debug", default=None,
                           help="niveau TF_LOG (défaut du niveau: debug) : avec --run, celui du journal "
                                f"{LOGS_DIR}/<horodatage>-<env>-<commande>.log ; sinon TF_LOG est exporté")
    terraform.add_argument("--follow", choices=[*TF_LOG_LEVELS, FOLLOW_OFF], default=DEFAULT_FOLLOW_LEVEL,
                           metavar="NIVEAU",
                           help="avec --run : niveau des lignes du journal affichées en direct "
                                f"({', '.join(TF_LOG_LEVELS)}, {FOLLOW_OFF} ; défaut: {DEFAULT_FOLLOW_LEVEL})")
    terraform.add_argument("--no-log-file", action="store_true",
                           help="avec --run : pas de journal ; TF_LOG (--tf-log) sort alors sur le terminal")
    terraform.add_argument("--parallel", nargs="?", const=0, default=1, type=int, metavar="N",
                           help="avec --run test : un processus tofu par fichier de scénario, N à la fois "
                                "(sans N : tous en même temps) ; chaque scénario a son journal dans logs/")

    proxy = parser.add_argument_group("proxy (utilisé pour les appels Vault et exporté pour terraform)")
    proxy.add_argument("--proxy", metavar="HOST:PORT", default=None,
                       help=f"proxy d'entreprise (défaut: {DEFAULT_PROXY}, ou https_proxy déjà exporté)")
    proxy.add_argument("--proxy-user", default=os.environ.get(PROXY_USER_ENV),
                       help=f"user du proxy (défaut: ${PROXY_USER_ENV}, sinon mémorisé ou demandé)")
    proxy.add_argument("--proxy-password", default=os.environ.get(PROXY_PASSWORD_ENV),
                       help=f"mot de passe du proxy (défaut: ${PROXY_PASSWORD_ENV}, sinon demandé sans écho)")
    proxy.add_argument("--new-proxy-password", action="store_true",
                       help="ignorer le mot de passe du proxy mémorisé et le redemander")
    proxy.add_argument("--forget-proxy-password", action="store_true",
                       help="oublier le mot de passe du proxy mémorisé (trousseau / cache), puis quitter")
    proxy.add_argument("--no-proxy", action="store_true", help="aucun proxy")
    proxy.add_argument("--no-proxy-hosts", default=DEFAULT_NO_PROXY, metavar="HOSTS",
                       help=f"valeur de no_proxy (défaut: {DEFAULT_NO_PROXY} ; l'hôte du service token "
                            "y est toujours ajouté)")
    proxy.add_argument("--skip-proxy-check", action="store_true",
                       help=f"ne pas tester le proxy sur {PROXY_CHECK_URL} avant de continuer")

    output = parser.add_argument_group("sortie")
    output.add_argument("--run", metavar="COMMAND",
                        help="lancer `tofu COMMAND` dans le dossier de tests (ex: --run test, --run plan) ; "
                             "les options de terraform se passent après -- (ex: --run apply -- -auto-approve)")
    output.add_argument("terraform_args", nargs="*", metavar="TERRAFORM_ARG",
                        help="options passées à tofu avec --run (après --)")
    output.add_argument("--shell", action="store_true",
                        help="ouvrir un sous-shell avec les variables exportées")
    output.add_argument("--json", action="store_true", dest="as_json",
                        help="imprimer les variables en JSON au lieu de lignes `export` (secrets masqués)")
    output.add_argument("--print-secrets", action="store_true",
                        help="imprimer les secrets en clair dans les `export` / le JSON (par défaut : "
                             "références au trousseau, lues par le shell à l'eval)")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)

    args = parser.parse_args(argv)
    args.vault_token_from_cli = bool(args.vault_token)
    if args.env not in ENVIRONMENTS:
        parser.error(f"${ENV_VAR}={args.env} inconnu (choix: {', '.join(ENVIRONMENTS)})")
    environment = ENVIRONMENTS[args.env]
    args.vault = args.vault or environment.vault
    args.vault_url = (args.vault_url or VAULTS[args.vault]).rstrip("/")
    args.kv_path = (args.kv_path or environment.kv_path).strip("/")
    if not args.kv_path:
        parser.error(f"chemin du secret non renseigné pour {args.env} : "
                     "le compléter dans ENVIRONMENTS en tête du script, ou passer --secret-path")
    args.ui_url = args.vault_url + VAULT_UI_PATH + "?" + urllib.parse.urlencode({"namespace": args.namespace})
    if args.token_service is None:  # "" explicite : pas de service token
        args.token_service = TOKEN_SERVICES.get(args.vault, "")
    args.token_service = args.token_service.rstrip("/")
    args.proxy_from_cli = args.proxy is not None
    args.proxy = args.proxy or DEFAULT_PROXY
    if args.no_proxy and args.proxy_from_cli:
        parser.error("--no-proxy est incompatible avec --proxy")
    args.dir = os.path.abspath(args.dir or (environment.tests_dir and os.path.join(
        os.path.dirname(os.path.abspath(__file__)), environment.tests_dir)) or default_tests_dir(args.env))
    if args.terraform_args and not args.run:
        parser.error("des arguments terraform sont donnés sans --run")
    args.run = [args.run, *args.terraform_args] if args.run else None
    if args.run and args.shell:
        parser.error("--run est incompatible avec --shell")
    return args


# --------------------------------------------------------------------------- #
# Mode guidé : menus numérotés qui construisent la ligne de commande
# --------------------------------------------------------------------------- #

def _choose(question: str, options: Sequence[tuple[str, T]], ask: Callable[[str], str] = input,
            default: int = 1) -> T:
    """Affiche des options numérotées et retourne la valeur de celle choisie
    (Entrée = option par défaut)."""
    print(f"\n{question}", file=sys.stderr)
    for number, (label, _) in enumerate(options, 1):
        print(f"  {number}) {label}{'  [défaut]' if number == default else ''}", file=sys.stderr)
    while True:
        answer = ask(f"Choix [1-{len(options)}, Entrée = {default}] : ").strip()
        if not answer:
            return options[default - 1][1]
        if answer.isdigit() and 1 <= int(answer) <= len(options):
            return options[int(answer) - 1][1]
        print(f"  Taper un nombre entre 1 et {len(options)}.", file=sys.stderr)


def _ask_text(question: str, default: str = "", ask: Callable[[str], str] = input) -> str:
    suffix = f" [Entrée = {default}]" if default else ""
    return ask(f"{question}{suffix} : ").strip() or default


def interactive_argv(ask: Callable[[str], str] = input) -> list[str] | None:
    """Pose les questions une à une et retourne les arguments équivalents
    (None si l'utilisateur quitte)."""
    _log("Préparation de l'environnement Terraform des tests toolchain : mode guidé (Ctrl+C pour quitter).")
    env = _choose("Environnement ?", [
        (f"{name} (Vault {e.vault} : {VAULTS[e.vault]}, clé IBM sous {e.kv_path})", name)
        for name, e in ENVIRONMENTS.items()
    ], ask, default=list(ENVIRONMENTS).index(DEFAULT_ENV) + 1)
    argv = [] if env == DEFAULT_ENV else ["--env", env]

    action = _choose("Que veux-tu faire ?", [
        ("terraform test : tous les scénarios (login, init, token Vault, API key, puis test)", ["--run", "test"]),
        ("terraform test : un seul scénario (fichier demandé ensuite)", ["--run", "test", "--", "-filter="]),
        ("terraform plan", ["--run", "plan"]),
        ("terraform apply", ["--run", "apply"]),
        ("Ouvrir un sous-shell avec les variables exportées", ["--shell"]),
        ("Imprimer les `export` (pour eval \"$(python toolchain_env.py ...)\")", []),
        ("Quitter", None),
    ], ask)
    if action is None:
        return None

    argv += _choose("Token Vault / API key ?", [
        ("Réutiliser ce qui est sauvegardé s'il est encore valide, sinon le service token", []),
        ("Nouveau token via le service token (uid demandé s'il n'est pas mémorisé)", ["--new-token"]),
        ("Nouveau token via Chrome en navigation privée sur l'UI Vault", ["--browser-token", "--new-token"]),
        ("Copier-coller manuel depuis l'UI Vault", ["--manual-token", "--new-token"]),
        ("Coller le token maintenant", ["--vault-token", ""]),
    ], ask)
    if argv[-2:] == ["--vault-token", ""]:
        argv[-1] = clean_token(getpass.getpass("Token Vault : "))
    argv += _choose("Proxy ?", [
        (f"{DEFAULT_PROXY} avec mon user / mot de passe (demandés ensuite)", []),
        ("Aucun proxy", ["--no-proxy"]),
    ], ask)
    argv += _choose("Options terraform ?", [
        ("Aucune : étapes en direct (niveau info), journal complet dans logs/", []),
        ("Tout le détail en direct (--follow debug)", ["--follow", "debug"]),
        ("Rien en direct, seulement le journal (--follow off)", ["--follow", FOLLOW_OFF]),
    ], ask)
    if action and action[-1] == "-filter=":
        action[-1] += "tests/" + _ask_text("Fichier de scénario (dans tests/)", "20_bucket_lifecycle.tftest.hcl", ask)
    argv += action  # --run et les arguments terraform restent en dernier

    shown = ["<token>" if i and argv[i - 1] == "--vault-token" else a for i, a in enumerate(argv)]
    _log("\nCommande équivalente :\n  python toolchain_env.py " + " ".join(shlex.quote(a) for a in shown) + "\n")
    return argv


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def prepare(args: argparse.Namespace, acquire: Callable[[str], str] | None = None,
            run: Callable[..., int] | None = None) -> dict[str, str]:
    """Étapes 1 à 5 ; retourne les variables d'environnement à exporter."""
    run = run or run_terraform
    if not os.path.isdir(args.dir):
        raise CliExit(EXIT_USAGE, f"dossier de tests introuvable : {args.dir} (--dir)")
    _log(f"Environnement : {args.env}  dossier : {args.dir}")
    _log(f"Vault : {args.vault_url} (namespace {args.namespace})  clé IBM sous : {args.kv_path}")
    if not args.skip_login:
        ensure_terraform_login(args.terraform_host, args.dir, run)
    previous = tfvars_value(os.path.join(args.dir, VERSIONS_TF), "version")
    version = write_versions_tf(args.dir, args.env)
    if not args.skip_init:
        ensure_terraform_init(args.dir, args.reinit, run,
                              version_changed=bool(version and previous and version != previous))
    proxy_vars = with_no_proxy(resolve_and_check_proxy(args), args.token_service)
    apply_proxy(proxy_vars)
    if proxy_vars:
        _log(f"Proxy : {mask_url(proxy_vars['https_proxy'])}  no_proxy : {proxy_vars['no_proxy']}")
    client = VaultClient(args.vault_url, args.namespace, args.timeout, args.ca_bundle)
    try:
        api_key = resolve_api_key(args, client, acquire)
    except VaultError as exc:
        raise CliExit(EXIT_VAULT_FAILED, f"Vault : {exc}") from exc
    variables = build_env_vars(api_key, args.tf_log, proxy_vars)
    if env_tfvars_path(args.dir, args.env):
        # préfixe des descriptions dans l'orchestrateur : qui a lancé le test
        variables["TF_VAR_prefix"] = args.prefix or load_setting("proxy_user") or getpass.getuser()
    return variables


def probe(args: argparse.Namespace) -> int:
    """--probe : sondage TCP du service token puis GET du token par chaque chemin."""
    if not args.token_service:
        _log(f"Pas de service token configuré pour le Vault {args.vault}.")
        return EXIT_USAGE
    parts = urllib.parse.urlsplit(args.token_service)
    host, port = parts.hostname or "", parts.port or 443
    _log(f"Service token : {args.token_service}\nSondage TCP :\n  " + "\n  ".join(tcp_probe(host, port)))
    try:
        proxy_vars = with_no_proxy(resolve_and_check_proxy(args), args.token_service)
        apply_proxy(proxy_vars)
        token = service_token(args.token_service, resolve_uid(args), args.namespace, args.timeout, args.ca_bundle)
    except CliExit as exc:
        _log(str(exc))
        return exc.code
    except VaultError as exc:
        _log(f"Échec : {exc}")
        return EXIT_VAULT_FAILED
    _log(f"OK : token {_mask(token)} obtenu.")
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
        if not argv and sys.stdin.isatty():
            argv = ["--interactive"]
    if argv in (["-i"], ["--interactive"]):
        try:
            argv = interactive_argv()
        except (KeyboardInterrupt, EOFError):
            _log("\nAnnulé.")
            return EXIT_OK
        if argv is None:
            return EXIT_OK
    args = parse_args(argv)
    if args.forget:
        forget_all()
        _log(f"Cache supprimé ({state_path()}), secrets oubliés (trousseau {KEYCHAIN_SERVICE}).")
        return EXIT_OK
    if args.forget_proxy_password:
        user = args.proxy_user or load_setting("proxy_user")
        if user:
            forget_proxy_password(user)
        _log(f"Mot de passe du proxy oublié pour {user or '(aucun user mémorisé)'}.")
        return EXIT_OK
    if args.probe:
        return probe(args)
    try:
        variables = prepare(args)
    except CliExit as exc:
        if str(exc):
            _log(str(exc))
        return exc.code
    except KeyboardInterrupt:
        _log("\nAnnulé.")
        return EXIT_USAGE

    env = {**os.environ, **variables}
    if args.run:
        _log("Variables exportées : " + ", ".join(variables))  # valeurs non affichées
        try:
            command = with_var_file(args.run, args.dir, args.env)
            if command[0] == "test":
                version = terraform_version()
                if version is not None and version < (1, 6):
                    raise CliExit(EXIT_TERRAFORM_FAILED, f"{TERRAFORM_BIN} {'.'.join(map(str, version))} : "
                                                         "`tofu test` demande OpenTofu 1.6 au minimum.")
                command = adapt_test_filter(command, args.dir, version)
                judged = bool(load_expected_failures(args.dir))
                if args.parallel != 1 or judged:
                    if version is not None and version < (1, 7):
                        if judged and args.parallel == 1:
                            _log(f"Attention : {TERRAFORM_BIN} < 1.7, les fichiers de refus attendu "
                                 f"(tests/{EXPECTED_FAILURES}) ne seront pas jugés : ils apparaîtront en échec.")
                        else:
                            raise CliExit(EXIT_TERRAFORM_FAILED, f"--parallel demande {TERRAFORM_BIN} >= 1.7 "
                                                                 "(vrai -filter) ; en 1.6, lancer sans --parallel.")
                    else:
                        # Un processus tofu par fichier (séquentiel sans --parallel) : chaque
                        # fichier de refus attendu est jugé sur sa propre sortie.
                        workers = args.parallel if args.parallel > 0 else len(scenario_files(args.dir, command))
                        return run_tests_parallel(command, args.dir, env, max(1, workers), args.env, args.tf_log)
            if args.no_log_file:
                return run_terraform(command, args.dir, env, to_stderr=False)
            return run_with_journal(command, args.dir, env, args.tf_log, args.follow, args.env)
        except CliExit as exc:
            _log(str(exc))
            return exc.code
    if args.shell:
        shell = os.environ.get("SHELL") or os.environ.get("COMSPEC") or "/bin/sh"
        _log(f"Sous-shell {shell} dans {args.dir} avec " + ", ".join(variables)
             + " exportées (exit pour revenir).")
        return subprocess.call([shell], cwd=args.dir, env=env)
    # Sortie sur stdout (eval / --json) : jamais un secret en clair, sauf
    # --print-secrets. Les secrets sont référencés depuis le trousseau, que le
    # shell lit lui-même au moment de l'eval ; --run et --shell n'ont pas ce
    # problème (variables passées en mémoire au processus).
    if args.as_json:
        print(json.dumps(variables if args.print_secrets else mask_secrets(variables), indent=2))
    else:
        parts = {} if args.print_secrets else secret_export_parts(
            variables, getattr(args, "api_key_ref", ""),
            (getattr(args, "proxy_credentials", None) or ("", ""))[0], args.proxy_origin == "shell")
        unsafe = [n for n in variables if n in SECRET_VARIABLES and n not in parts]
        if unsafe and not args.print_secrets:
            _log(f"Pas de trousseau pour {', '.join(unsafe)} : rien n'est imprimé en clair. Utiliser --run "
                 "ou --shell (secrets passés en mémoire), ou --print-secrets en connaissance de cause.")
            return EXIT_USAGE
        sys.stdout.write(export_lines(variables, parts))
        _log("Variables prêtes : " + ", ".join(variables)
             + f"\nDans le shell courant : eval \"$(python {os.path.basename(__file__)} --env {args.env})\""
             + f"\nPuis : terraform -chdir={shlex.quote(args.dir)} plan"
             + (f" -var-file={ENV_TFVARS_DIR}/{args.env}.tfvars" if env_tfvars_path(args.dir, args.env) else ""))
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
