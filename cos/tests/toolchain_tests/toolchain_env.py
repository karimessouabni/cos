#!/usr/bin/env python3
r"""
Préparation de l'environnement Terraform des tests toolchain (créer des COS et
des buckets via le provider orchestrator) : tout ce qu'il fallait faire à la
main avant un `terraform plan` est enchaîné par le script.

Étapes (chacune est sautée si elle a déjà été faite et reste valide) :
1. terraform login <artifactory>     si aucun credential n'est enregistré
                                     (~/.terraform.d/credentials.tfrc.json)
2. terraform init                    dans le dossier de tests de l'environnement
                                     si .terraform/ n'existe pas encore (--reinit
                                     pour forcer)
3. proxy                             http://<user>:<mot de passe>@ncproxy.fr.net.intra:8080
                                     user et mot de passe demandés à chaque
                                     lancement (le mot de passe n'est jamais
                                     sauvegardé), vérifiés tout de suite sur
                                     iam.cloud.ibm.com (407 = refusés) ; utilisé
                                     pour les appels suivants et exporté pour
                                     terraform
4. token Vault                       de l'instance Vault de l'environnement
                                     (int -> hvault-dev) : token sauvegardé s'il
                                     est encore valide, sinon
                                     GET <service token>/v1/token/<uid>?namespace=AP85135
                                     (le client_token renvoyé est un token Vault,
                                     valable 30 jours), sinon Chrome / Edge sur
                                     l'UI Vault en secours
5. API key IBM Cloud                 GET <vault>/v1/<secret_path> avec le token
                                     (X-Vault-Namespace: AP85135) ; la clé est
                                     gardée tant que son lease est valide
6. variables d'environnement         IBM_CLOUD_API_KEY, ORCHESTRATOR_IBMCLOUD_API_KEY,
                                     http_proxy / https_proxy / no_proxy en
                                     minuscules et en MAJUSCULES (+ TF_LOG)

Utilisation
    # dans le shell courant : exporte les variables puis terraform plan/apply
    eval "$(python toolchain_env.py --env int)"
    terraform -chdir=int/new_version plan

    # ou tout enchaîné : les étapes 1-6 puis terraform plan / apply
    python toolchain_env.py --env int --run plan
    python toolchain_env.py --env int --run apply -- -auto-approve

    # ou un sous-shell avec les variables déjà exportées
    python toolchain_env.py --env int --shell

    # sans argument dans un terminal : mode guidé (menus numérotés)
    python toolchain_env.py

Proxy
    export PROXY_USER=h12345 PROXY_PASSWORD=...   # ou --proxy-user / --proxy-password
    --no-proxy                                    # pas de proxy du tout
Si https_proxy est déjà exporté dans le shell, il est réutilisé tel quel sans
rien demander. Le user est mémorisé (pas le mot de passe).

Token Vault
    export VAULT_TOKEN=hvs....                # ou --vault-token
    --uid la90261                             # uid passé au service token
                                              # (défaut: $TOOLCHAIN_UID, sinon demandé
                                              # une fois puis mémorisé)
Le token récupéré est sauvegardé dans ~/.cache/cos-toolchain/state.json
(lisible par toi seul) avec l'API key et réutilisé tant qu'il est valide
(vérifié par lookup-self) : le service token n'est rappelé que s'il est expiré
ou refusé. --new-token force un nouveau token, --new-key une nouvelle API key,
--forget efface tout ce qui est sauvegardé.
Sans service token pour l'instance Vault (--browser-token pour forcer), le
token est lu dans un Chrome / Edge en navigation privée ouvert sur l'UI Vault
(login SSO) ; sans Chrome / Edge, ou avec --manual-token, l'UI Vault est
ouverte dans le navigateur par défaut : "Copy token" dans le menu utilisateur,
puis Entrée dans le terminal (le token est lu dans le presse-papiers).

Les appels HTTP vers Vault ignorent la vérification TLS (certificats internes),
comme les curl de la procédure manuelle ; --verify-tls la réactive.

Codes de sortie: 0 OK, 1 erreur args/token, 2 erreur Vault, 3 erreur terraform
(avec --run, le code de sortie de terraform est renvoyé tel quel).
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
import time
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
    "dev": "https://s02vi9956141:4430",
    "staging": "",  # URL à renseigner
    "group": "",  # URL à renseigner
}
TOKEN_SERVICE_PATH = "/v1/token/"


@dataclass(frozen=True)
class Environment:
    vault: str  # clé de VAULTS
    secret_path: str  # chemin lu dans Vault : ibm_<account>/creds/<role>
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
TERRAFORM_BIN = "terraform"
NEW_VERSION_DIR = "new_version"

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

def insecure_ssl_context() -> ssl.SSLContext:
    """Contexte TLS sans aucune vérification (certificats internes auto-signés)."""
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


class VaultClient:
    def __init__(self, base_url: str, namespace: str, timeout: int = DEFAULT_TIMEOUT,
                 verify_tls: bool = False):
        self.base_url = base_url.rstrip("/")
        self.namespace = namespace
        self.timeout = timeout
        self._context = ssl.create_default_context() if verify_tls else insecure_ssl_context()

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

    def read_secret(self, token: str, path: str) -> tuple[dict[str, Any], int | None]:
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
                   headers: dict[str, str] | None = None) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
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


def token_service_url(base_url: str, uid: str, namespace: str) -> str:
    return (base_url.rstrip("/") + TOKEN_SERVICE_PATH + urllib.parse.quote(uid, safe="")
            + "?" + urllib.parse.urlencode({"namespace": namespace}))


def service_token(base_url: str, uid: str, namespace: str, timeout: int = DEFAULT_TIMEOUT,
                  verify_tls: bool = False) -> str:
    """Token Vault délivré par le service token : GET /v1/token/<uid>?namespace=<ns>,
    champ auth.client_token de la réponse (format Vault)."""
    url = token_service_url(base_url, uid, namespace)
    _log(f"Token Vault demandé au service token : GET {url}")
    body = _http_get_json(url, timeout, ssl.create_default_context() if verify_tls else insecure_ssl_context())
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
    privée, erreurs de certificat ignorées), attend le login SSO, lit le token
    de session dans la page via le protocole DevTools puis ferme le navigateur.
    "" si échec ou délai dépassé."""
    browser = browser or find_chromium_browser()
    if not browser:
        _log("Chrome / Edge introuvable : récupération automatique impossible.")
        return ""
    profile_dir = tempfile.mkdtemp(prefix="cos-toolchain-")
    command = [browser, f"--user-data-dir={profile_dir}", "--remote-debugging-port=0",
               "--no-first-run", "--no-default-browser-check", "--new-window",
               "--incognito", "--ignore-certificate-errors", ui_url]
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
# Cache local : tokens Vault et API keys (réutilisés tant qu'ils sont valides)
# --------------------------------------------------------------------------- #

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
    return state if isinstance(state, dict) else {}


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


def load_cached_token(vault_url: str) -> str | None:
    token = (_read_state().get("vault_tokens") or {}).get(vault_url.rstrip("/"))
    return token if isinstance(token, str) and token else None


def save_cached_token(vault_url: str, token: str) -> None:
    state = _read_state()
    state.setdefault("vault_tokens", {})[vault_url.rstrip("/")] = token
    _write_state(state)


def forget_cached_token(vault_url: str) -> None:
    state = _read_state()
    if (state.get("vault_tokens") or {}).pop(vault_url.rstrip("/"), None) is not None:
        _write_state(state)


def _api_key_cache_key(vault_url: str, secret_path: str) -> str:
    return f"{vault_url.rstrip('/')}/v1/{secret_path.strip('/')}"


def load_cached_api_key(vault_url: str, secret_path: str, now: float | None = None) -> str | None:
    """API key sauvegardée pour ce secret si son lease est encore valide."""
    entry = (_read_state().get("api_keys") or {}).get(_api_key_cache_key(vault_url, secret_path))
    if not isinstance(entry, dict) or not entry.get("api_key"):
        return None
    expires_at = entry.get("expires_at")
    if expires_at is not None and float(expires_at) - (time.time() if now is None else now) < TOKEN_MIN_VALIDITY:
        return None
    return str(entry["api_key"])


def save_cached_api_key(vault_url: str, secret_path: str, api_key: str, lease: int | None) -> None:
    state = _read_state()
    state.setdefault("api_keys", {})[_api_key_cache_key(vault_url, secret_path)] = {
        "api_key": api_key,
        "expires_at": None if lease is None else time.time() + lease,
        "saved_at": time.time(),
    }
    _write_state(state)


def load_setting(name: str) -> str:
    value = (_read_state().get("settings") or {}).get(name)
    return value if isinstance(value, str) else ""


def save_setting(name: str, value: str) -> None:
    state = _read_state()
    state.setdefault("settings", {})[name] = value
    _write_state(state)


def forget_all() -> None:
    try:
        os.remove(state_path())
    except FileNotFoundError:
        pass


# --------------------------------------------------------------------------- #
# Terraform : login et init
# --------------------------------------------------------------------------- #

def terraform_credentials_path() -> str:
    if sys.platform == "win32":
        return os.path.join(os.environ.get("APPDATA", os.path.expanduser("~")), "terraform.d",
                            "credentials.tfrc.json")
    return os.path.join(os.path.expanduser("~"), ".terraform.d", "credentials.tfrc.json")


def terraform_logged_in(host: str, environ: dict[str, str] | None = None) -> bool:
    """Vrai si un token est enregistré pour `host` (TF_TOKEN_<host> ou
    credentials.tfrc.json écrit par `terraform login`)."""
    environ = os.environ if environ is None else environ
    if environ.get("TF_TOKEN_" + host.replace(".", "_").replace("-", "__")):
        return True
    try:
        with open(terraform_credentials_path(), encoding="utf-8") as fh:
            credentials = json.load(fh)
    except (OSError, ValueError):
        return False
    entry = (credentials.get("credentials") or {}).get(host) if isinstance(credentials, dict) else None
    return bool(isinstance(entry, dict) and entry.get("token"))


def run_terraform(args: Sequence[str], cwd: str, env: dict[str, str] | None = None,
                  to_stderr: bool = True, terraform: str = TERRAFORM_BIN) -> int:
    """Lance terraform et retourne son code de sortie. Avec to_stderr, sa sortie
    va sur stderr pour ne pas polluer les `export` imprimés sur stdout
    (eval "$(...)")."""
    if not shutil.which(terraform):
        raise CliExit(EXIT_TERRAFORM_FAILED, f"{terraform} introuvable dans le PATH")
    command = [terraform, *args]
    _log(f"$ {' '.join(shlex.quote(a) for a in command)}  (dans {cwd})")
    try:
        return subprocess.call(command, cwd=cwd, env=env,
                               stdout=sys.stderr if to_stderr else None)
    except OSError as exc:
        raise CliExit(EXIT_TERRAFORM_FAILED, f"{terraform} : {exc}") from exc


def ensure_terraform_login(host: str, cwd: str, run: Callable[..., int] = run_terraform) -> None:
    if terraform_logged_in(host):
        _log(f"terraform login : credentials déjà enregistrés pour {host}.")
        return
    if not sys.stdin.isatty():
        raise CliExit(EXIT_TERRAFORM_FAILED,
                      f"aucun credential terraform pour {host} : lancer `terraform login {host}` "
                      "dans un terminal (ou --skip-login).")
    code = run(["login", host], cwd)
    if code != 0:
        raise CliExit(EXIT_TERRAFORM_FAILED, f"terraform login {host} a échoué (code {code})")


def ensure_terraform_init(cwd: str, reinit: bool = False, run: Callable[..., int] = run_terraform) -> None:
    if not reinit and os.path.isdir(os.path.join(cwd, ".terraform")):
        _log(f"terraform init : déjà fait dans {cwd} (--reinit pour refaire).")
        return
    code = run(["init"], cwd)
    if code != 0:
        raise CliExit(EXIT_TERRAFORM_FAILED, f"terraform init a échoué (code {code})")


def default_tests_dir(env: str, root: str | None = None) -> str:
    """<dossier du script>/<env>/new_version s'il existe, sinon <dossier du script>/<env>."""
    root = root or os.path.dirname(os.path.abspath(__file__))
    with_version = os.path.join(root, env, NEW_VERSION_DIR)
    return with_version if os.path.isdir(with_version) else os.path.join(root, env)


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
                  environ: dict[str, str] | None = None) -> dict[str, str]:
    """Variables http_proxy / https_proxy / no_proxy à utiliser : rien avec
    --no-proxy ; celles déjà exportées dans le shell si --proxy n'est pas passé
    explicitement ; sinon le proxy d'entreprise avec le user (--proxy-user,
    $PROXY_USER, mémorisé, ou demandé) et le mot de passe (--proxy-password,
    $PROXY_PASSWORD, ou demandé sans écho, jamais sauvegardé)."""
    environ = os.environ if environ is None else environ
    if args.no_proxy:
        return {}
    existing = environ.get("https_proxy") or environ.get("HTTPS_PROXY")
    if existing and not args.proxy_from_cli:
        _log(f"Proxy déjà exporté dans le shell, réutilisé : {mask_url(existing)}")
        return {"http_proxy": environ.get("http_proxy") or environ.get("HTTP_PROXY") or existing,
                "https_proxy": existing,
                "no_proxy": environ.get("no_proxy") or environ.get("NO_PROXY") or args.no_proxy_hosts}
    user = args.proxy_user or load_setting("proxy_user")
    password = args.proxy_password
    if not user or not password:
        if not sys.stdin.isatty():
            raise CliExit(EXIT_USAGE, f"proxy {args.proxy} : passer --proxy-user / --proxy-password "
                                      f"(ou ${PROXY_USER_ENV} / ${PROXY_PASSWORD_ENV}), ou --no-proxy")
        _log(f"Proxy {args.proxy} : identifiants de ton compte (le mot de passe n'est pas sauvegardé).")
        user = (ask(f"User du proxy{f' [Entrée = {user}]' if user else ''} : ").strip() or user)
        if not user:
            raise CliExit(EXIT_USAGE, "user du proxy manquant")
        password = password or ask_secret(f"Mot de passe du proxy pour {user} : ")
        if not password:
            raise CliExit(EXIT_USAGE, "mot de passe du proxy manquant")
    if user != load_setting("proxy_user"):
        save_setting("proxy_user", user)
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


def check_proxy(proxy_vars: dict[str, str], url: str = PROXY_CHECK_URL, timeout: int = 15) -> None:
    """Vérifie que le proxy laisse passer vers `url` : 407 = user / mot de passe
    refusés (CliExit) ; les autres erreurs réseau ne sont que signalées."""
    if not proxy_vars:
        return
    request = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=insecure_ssl_context()):
            pass
    except urllib.error.HTTPError as exc:
        if exc.code == 407:
            raise CliExit(EXIT_USAGE, f"proxy {mask_url(proxy_vars['https_proxy'])} : identifiants refusés "
                                      f"(HTTP 407 {exc.reason}). Vérifier le user et le mot de passe "
                                      "(--proxy-user / --proxy-password), ou le compte bloqué.")
        _log(f"Proxy OK ({url} répond HTTP {exc.code}).")
    except (urllib.error.URLError, OSError) as exc:
        reason = getattr(exc, "reason", exc)
        if "407" in str(reason) or "authenticationrequired" in str(reason).lower():
            raise CliExit(EXIT_USAGE, f"proxy {mask_url(proxy_vars['https_proxy'])} : identifiants refusés "
                                      f"({reason}). Vérifier le user et le mot de passe.")
        _log(f"Proxy non vérifié ({url} : {reason}) ; terraform échouera si le proxy refuse.")
    else:
        _log(f"Proxy OK ({url} joignable).")


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


def export_lines(variables: dict[str, str]) -> str:
    return "".join(f"export {name}={shlex.quote(value)}\n" for name, value in variables.items())


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
        uid = ask(f"uid pour le service token (ex: la90261) [Entrée = {getpass.getuser()}] : ").strip() \
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
                                  args.timeout, args.verify_tls)
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
    _log(f"Token Vault récupéré, valide" + ("." if ttl is None else f" encore {_duration(ttl)}."))
    save_cached_token(args.vault_url, token)
    return token


def resolve_api_key(args: argparse.Namespace, client: VaultClient,
                    acquire: Callable[[str], str] | None = None) -> str:
    """API key IBM Cloud : celle sauvegardée si son lease est encore valide,
    sinon lue dans Vault avec un token valide (puis sauvegardée)."""
    if not args.new_key and not args.new_token:
        cached = load_cached_api_key(args.vault_url, args.secret_path)
        if cached:
            _log(f"API key sauvegardée réutilisée ({_mask(cached)}, {state_path()}).")
            return cached
    token = resolve_vault_token(args, client, acquire)
    try:
        data, lease = client.read_secret(token, args.secret_path)
    except VaultError as exc:
        if exc.status_code in (401, 403) and not args.vault_token_from_cli:
            # token sauvegardé accepté par lookup-self mais sans droit sur le
            # secret : on ne le garde pas, l'utilisateur doit se reconnecter.
            forget_cached_token(args.vault_url)
        raise CliExit(EXIT_VAULT_FAILED, f"lecture de {args.secret_path} refusée : {exc}") from exc
    api_key = extract_api_key(data)
    if not api_key:
        raise CliExit(EXIT_VAULT_FAILED, f"pas d'api_key dans la réponse de {args.secret_path} "
                                         f"(champs: {', '.join(sorted(data)) or 'aucun'})")
    _log(f"API key lue dans Vault ({_mask(api_key)}"
         + ("" if lease is None else f", lease {_duration(lease)}") + ").")
    save_cached_api_key(args.vault_url, args.secret_path, api_key, lease)
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
    parser.add_argument("--dir", help="dossier terraform des tests (défaut: <env>/new_version ou <env>, "
                                      "à côté du script)")

    vault = parser.add_argument_group("vault")
    vault.add_argument("--vault", choices=list(VAULTS), help="instance Vault (défaut: celle de --env)")
    vault.add_argument("--vault-url", help="URL de Vault (défaut: celle de --vault)")
    vault.add_argument("--secret-path", help="chemin du secret IBM (défaut: celui de --env)")
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
    vault.add_argument("--forget", action="store_true",
                       help="supprimer tout ce qui est sauvegardé (tokens, API keys), puis quitter")
    vault.add_argument("--verify-tls", action="store_true",
                       help="vérifier le certificat de Vault (ignoré par défaut : certificats internes)")

    terraform = parser.add_argument_group("terraform")
    terraform.add_argument("--terraform-host", default=TERRAFORM_HOST,
                           help=f"hôte du `terraform login` (défaut: {TERRAFORM_HOST})")
    terraform.add_argument("--skip-login", action="store_true", help="ne pas vérifier / faire terraform login")
    terraform.add_argument("--skip-init", action="store_true", help="ne pas faire terraform init")
    terraform.add_argument("--reinit", action="store_true", help="refaire terraform init même si déjà fait")
    terraform.add_argument("--tf-log", nargs="?", const="debug", default=None,
                           help="exporter TF_LOG (défaut du niveau: debug)")

    proxy = parser.add_argument_group("proxy (utilisé pour les appels Vault et exporté pour terraform)")
    proxy.add_argument("--proxy", metavar="HOST:PORT", default=None,
                       help=f"proxy d'entreprise (défaut: {DEFAULT_PROXY}, ou https_proxy déjà exporté)")
    proxy.add_argument("--proxy-user", default=os.environ.get(PROXY_USER_ENV),
                       help=f"user du proxy (défaut: ${PROXY_USER_ENV}, sinon mémorisé ou demandé)")
    proxy.add_argument("--proxy-password", default=os.environ.get(PROXY_PASSWORD_ENV),
                       help=f"mot de passe du proxy (défaut: ${PROXY_PASSWORD_ENV}, sinon demandé sans écho)")
    proxy.add_argument("--no-proxy", action="store_true", help="aucun proxy")
    proxy.add_argument("--no-proxy-hosts", default=DEFAULT_NO_PROXY, metavar="HOSTS",
                       help=f"valeur de no_proxy (défaut: {DEFAULT_NO_PROXY} ; l'hôte du service token "
                            "y est toujours ajouté)")
    proxy.add_argument("--skip-proxy-check", action="store_true",
                       help=f"ne pas tester le proxy sur {PROXY_CHECK_URL} avant de continuer")

    output = parser.add_argument_group("sortie")
    output.add_argument("--run", metavar="COMMAND",
                        help="lancer `terraform COMMAND` dans le dossier de tests (ex: --run plan) ; "
                             "les options de terraform se passent après -- (ex: --run apply -- -auto-approve)")
    output.add_argument("terraform_args", nargs="*", metavar="TERRAFORM_ARG",
                        help="options passées à terraform avec --run (après --)")
    output.add_argument("--shell", action="store_true",
                        help="ouvrir un sous-shell avec les variables exportées")
    output.add_argument("--json", action="store_true", dest="as_json",
                        help="imprimer les variables en JSON au lieu de lignes `export`")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)

    args = parser.parse_args(argv)
    args.vault_token_from_cli = bool(args.vault_token)
    if args.env not in ENVIRONMENTS:
        parser.error(f"${ENV_VAR}={args.env} inconnu (choix: {', '.join(ENVIRONMENTS)})")
    environment = ENVIRONMENTS[args.env]
    args.vault = args.vault or environment.vault
    args.vault_url = (args.vault_url or VAULTS[args.vault]).rstrip("/")
    args.secret_path = (args.secret_path or environment.secret_path).strip("/")
    if not args.secret_path:
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


def interactive_argv(ask: Callable[[str], str] = input) -> list[str] | None:
    """Pose les questions une à une et retourne les arguments équivalents
    (None si l'utilisateur quitte)."""
    _log("Préparation de l'environnement Terraform des tests toolchain : mode guidé (Ctrl+C pour quitter).")
    env = _choose("Environnement ?", [
        (f"{name} (Vault {e.vault} : {VAULTS[e.vault]}, secret {e.secret_path})", name)
        for name, e in ENVIRONMENTS.items()
    ], ask, default=list(ENVIRONMENTS).index(DEFAULT_ENV) + 1)
    argv = [] if env == DEFAULT_ENV else ["--env", env]

    action = _choose("Que veux-tu faire ?", [
        ("terraform plan (login, init, token Vault, API key, puis plan)", ["--run", "plan"]),
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
        ("Aucune", []),
        ("TF_LOG=debug", ["--tf-log"]),
    ], ask)
    argv += action  # --run doit rester en dernier (REMAINDER)

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
    _log(f"Vault : {args.vault_url} (namespace {args.namespace})  secret : {args.secret_path}")
    if not args.skip_login:
        ensure_terraform_login(args.terraform_host, args.dir, run)
    if not args.skip_init:
        ensure_terraform_init(args.dir, args.reinit, run)
    proxy_vars = with_no_proxy(resolve_proxy(args), args.token_service)
    apply_proxy(proxy_vars)
    if proxy_vars:
        _log(f"Proxy : {mask_url(proxy_vars['https_proxy'])}  no_proxy : {proxy_vars['no_proxy']}")
        if not args.skip_proxy_check:
            check_proxy(proxy_vars)
    client = VaultClient(args.vault_url, args.namespace, args.timeout, args.verify_tls)
    try:
        api_key = resolve_api_key(args, client, acquire)
    except VaultError as exc:
        raise CliExit(EXIT_VAULT_FAILED, f"Vault : {exc}") from exc
    return build_env_vars(api_key, args.tf_log, proxy_vars)


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
        _log(f"Cache supprimé ({state_path()}).")
        return EXIT_OK
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
            return run_terraform(args.run, args.dir, env, to_stderr=False)
        except CliExit as exc:
            _log(str(exc))
            return exc.code
    if args.shell:
        shell = os.environ.get("SHELL") or os.environ.get("COMSPEC") or "/bin/sh"
        _log(f"Sous-shell {shell} dans {args.dir} avec " + ", ".join(variables)
             + " exportées (exit pour revenir).")
        return subprocess.call([shell], cwd=args.dir, env=env)
    if args.as_json:
        print(json.dumps(variables, indent=2))
    else:
        sys.stdout.write(export_lines(variables))
        _log("Variables prêtes : " + ", ".join(variables)
             + f"\nDans le shell courant : eval \"$(python {os.path.basename(__file__)} --env {args.env})\""
             + f"\nPuis : terraform -chdir={shlex.quote(args.dir)} plan")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
