#!/usr/bin/env python3
r"""
Nettoyage des souscriptions orchestrator (produit cos.bucket par défaut).

Mode delete (défaut)
1. GET  {base}/multireader/api/v1/subscriptions?page=<n>&size=100, page par page
   jusqu'à la dernière (page incomplète, vide, ou total_pages/total atteint)
   -> on ne garde que les rows dont geninfo.product == <product> et
      context.user == <user> (défaut: h90871),
      puis pour chacune on regarde geninfo.demands :
      la souscription est "éligible" si :
        - toutes les demandes force_clean / create / update sont en SUCCESS,
        - les demandes delete, s'il y en a, sont TOUTES en erreur
          (status != SUCCESS) : un delete réussi rend la souscription
          non éligible,
        - la liste est non vide et ne contient pas d'autre action.
2. Pour chaque souscription éligible :
   - sans delete en échec : DELETE {base}/apl/v1/subscriptions/<subscription_id>
     avec le payload {"product_branch": "main", "payload": {}} ;
   - avec delete(s) en échec : on relance la demande existante plutôt que
     d'en créer une nouvelle :
       GET  {base}/api/v1/demands/<demand_uuid>
       -> names des processes dont status == ERROR
       POST {base}/api/v1/demands/<demand_uuid>/retry
            {"tasks": [<names>], "retry_non_failed_tasks": false}

Mode "on-error" (--on-error) : décliner les demandes des souscriptions dont
TOUTES les demandes sont en ON_ERROR.
1. même listing paginé que le mode delete, filtré côté script sur
   geninfo.product == <product> et context.user == <user>
2. est éligible une souscription dont geninfo.demands est non vide et dont
   chaque demande a status == ON_ERROR ; les demandes à décliner sont celles-là
   (leur uuid est celui de la demande)
3. Avec --decline, pour chaque demande retenue :
   POST {base}/state_manager/api/v1/demands/<uuid>/status
        {"status": "DECLINED", "reason": "to remove"}

Token
    export ORCHESTRATOR_TOKEN=...            # ou --token
Sans token valide (absent ou JWT expiré), le script ouvre le Swagger dans un
Chrome / Edge dédié, en navigation privée (--no-private pour l'éviter) : on se
connecte en SSO, on clique Authorize, et le token est lu automatiquement dans
la page puis le navigateur se ferme. Sans Chrome / Edge, ou avec --manual-token,
le script ouvre le Swagger
(--swagger-url, $ORCHESTRATOR_SWAGGER_URL ou DEFAULT_SWAGGER_URL en tête du
script) : on s'y connecte en SSO, on copie
le token, puis Entrée : il est lu dans le presse-papiers (ou collé au prompt).
Pour copier le token en un clic depuis le Swagger, mettre en favori le
bookmarklet affiché par --print-bookmarklet.

Usage:
    python subscriptions_cleanup.py                       # dry-run: liste les souscriptions éligibles
    python subscriptions_cleanup.py --delete              # supprime / relance réellement
    python subscriptions_cleanup.py --delete --yes        # sans confirmation
    python subscriptions_cleanup.py --delete --workers 8  # 8 appels en parallèle
    python subscriptions_cleanup.py --user h12345               # autre user
    python subscriptions_cleanup.py --all-users                 # sans filtre user
    python subscriptions_cleanup.py --product cos.bucket --base-url https://...
    python subscriptions_cleanup.py --page-size 50 --first-page 0   # pagination
    python subscriptions_cleanup.py --input scratch.json  # lit un JSON local au lieu du GET
    python subscriptions_cleanup.py --on-error                  # liste les demandes à décliner (dry-run)
    python subscriptions_cleanup.py --on-error --decline        # POST DECLINED sur chacune
    python subscriptions_cleanup.py --on-error --decline --yes --reason "cleanup sprint 12"
    python subscriptions_cleanup.py --on-error --subscription-status LOCKED   # restreint aux LOCKED
    python subscriptions_cleanup.py --print-bookmarklet         # bookmarklet de copie du token

TLS (certificat interne BNPP, sinon "CERTIFICATE_VERIFY_FAILED: self-signed
certificate in certificate chain") :
    python subscriptions_cleanup.py --ca-cert ~/Root-Certificats-Internes/*.cer
    export ORCHESTRATOR_CA_CERTS=~/Root-Certificats-Internes/2014-2044\ BNPP\ Root.cer
    # par défaut : pas de vérification TLS (DEFAULT_INSECURE) ; pour vérifier :
    python subscriptions_cleanup.py --verify-tls

Codes de sortie: 0 OK, 1 erreur args/token, 2 erreur HTTP sur le GET,
3 au moins un DELETE / retry / decline en échec.
"""

from __future__ import annotations

import argparse
import base64
import getpass
import json
import os
import re
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
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Sequence, TypeVar

DEFAULT_BASE_URL = "https://orchestrator-gw.int.staging.echonet"
# Page Swagger ouverte dans le navigateur quand il faut un token : mettre ici son URL.
DEFAULT_SWAGGER_URL = ""
DEFAULT_PRODUCT = "cos.bucket"
DEFAULT_PRODUCT_BRANCH = "main"
DEFAULT_USER = "h90871"
DEFAULT_TIMEOUT = 60
DEFAULT_PAGE_SIZE = 100
DEFAULT_FIRST_PAGE = 1
DEFAULT_WORKERS = 1
MAX_PAGES = 10_000

TOKEN_ENV = "ORCHESTRATOR_TOKEN"
SWAGGER_URL_ENV = "ORCHESTRATOR_SWAGGER_URL"
CA_CERTS_ENV = "ORCHESTRATOR_CA_CERTS"  # chemins séparés par os.pathsep (":" sur macOS/Linux)
# TLS non vérifié par défaut (certificats internes) ; --verify-tls ou --ca-cert pour vérifier.
DEFAULT_INSECURE = True
TOKEN_MIN_VALIDITY = 60  # secondes : en dessous, le token est considéré expiré

STATE_MANAGER_PREFIX = "/state_manager/api/v1"
DEMAND_ON_ERROR_STATUS = "ON_ERROR"
DECLINED_STATUS = "DECLINED"
DEFAULT_DECLINE_REASON = "to remove"

ALLOWED_ACTIONS = frozenset({"force_clean", "create", "update"})
DELETE_ACTION = "delete"
SUCCESS_STATUS = "SUCCESS"
PROCESS_ERROR_STATUS = "ERROR"

EXIT_OK, EXIT_USAGE, EXIT_GET_FAILED, EXIT_ACTION_FAILED = 0, 1, 2, 3

Row = dict[str, Any]
T = TypeVar("T")


class OrchestratorApiError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None, payload: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class CliExit(Exception):
    """Arrêt du CLI avec un message sur stderr et un code de sortie."""

    def __init__(self, code: int, message: str = ""):
        super().__init__(message)
        self.code = code


@dataclass
class Subscription:
    subscription_id: str
    name: str = ""
    user: str = ""
    status: str = ""
    environment: str = ""
    region: str = ""
    actions: list[str] = field(default_factory=list)
    failed_delete_demand_ids: list[str] = field(default_factory=list)

    @property
    def needs_retry(self) -> bool:
        return bool(self.failed_delete_demand_ids)


@dataclass
class ErrorDemand:
    """Demande d'une souscription dont toutes les demandes sont en erreur (mode --on-error)."""
    subscription_id: str
    demand_id: str
    subscription_name: str = ""
    user: str = ""
    action: str = ""
    status: str = ""
    create_date: str = ""


# --------------------------------------------------------------------------- #
# Accès aux champs d'une row multireader
# --------------------------------------------------------------------------- #

def _first(mapping: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = mapping.get(key)
        if value:
            return str(value)
    return ""


def _geninfo(row: Row) -> dict[str, Any]:
    return row.get("geninfo") or {}


def _demands(row: Row) -> list[dict[str, Any]]:
    return _geninfo(row).get("demands") or []


def subscription_uuid(row: Row) -> str:
    """uuid de la souscription : geninfo.subscription_id (rows multireader),
    sinon uuid / subscription_id / id au premier niveau."""
    return _first(_geninfo(row), "subscription_id", "uuid") or _first(row, "uuid", "subscription_id", "id")


def subscription_user(row: Row) -> str:
    return _first(row.get("context") or {}, "user") or _first(row, "user", "owner", "requester")


def subscription_status(row: Row) -> str:
    return _first(row, "status") or _first(_geninfo(row), "status")


def subscription_name(row: Row) -> str:
    return _first(row, "name") or _first(_geninfo(row), "name")


def demand_uuid(demand: dict[str, Any]) -> str:
    return _first(demand, "uuid", "demand_id", "id")


def extract_rows(body: dict[str, Any]) -> list[Row]:
    result = body.get("result", body)
    rows = result.get("rows", [])
    if not isinstance(rows, list):
        raise ValueError("Format inattendu: result.rows n'est pas une liste")
    return rows


def extract_items(body: Any) -> list[dict[str, Any]]:
    """Liste d'objets depuis une réponse state_manager : liste brute, ou dict
    avec result/rows/items/data/demands/subscriptions."""
    if body is None:
        return []
    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)]
    if not isinstance(body, dict):
        raise ValueError(f"Format inattendu: {type(body).__name__}")
    for key in ("rows", "items", "data", "demands", "subscriptions"):
        value = body.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    result = body.get("result")
    if isinstance(result, (list, dict)):
        return extract_items(result)
    return []


# --------------------------------------------------------------------------- #
# Mode delete : sélection
# --------------------------------------------------------------------------- #

def is_eligible(demands: Iterable[dict[str, Any]] | None) -> bool:
    """True si toutes les demandes force_clean/create/update sont en SUCCESS
    et que les éventuelles demandes delete sont toutes en erreur (!= SUCCESS)."""
    demands = list(demands or [])

    def ok(demand: dict[str, Any]) -> bool:
        action = demand.get("action")
        succeeded = demand.get("status") == SUCCESS_STATUS
        if action in ALLOWED_ACTIONS:
            return succeeded
        return action == DELETE_ACTION and not succeeded

    return bool(demands) and all(ok(d) for d in demands)


def _demand_label(demand: dict[str, Any]) -> str:
    action = demand.get("action", "?")
    status = demand.get("status", "?")
    return action if status == SUCCESS_STATUS else f"{action}({status})"


def failed_delete_demand_ids(demands: Iterable[dict[str, Any]]) -> list[str]:
    """uuid des demandes delete non SUCCESS, dans l'ordre de create_date."""
    failed = [
        d for d in demands
        if d.get("action") == DELETE_ACTION and d.get("status") != SUCCESS_STATUS and d.get("uuid")
    ]
    failed.sort(key=lambda d: d.get("create_date") or "")
    return [d["uuid"] for d in failed]


def failed_process_names(demand: dict[str, Any]) -> list[str]:
    """names des processes en ERROR dans la réponse GET /api/v1/demands/<uuid>."""
    return [
        p["name"] for p in demand.get("processes") or []
        if p.get("status") == PROCESS_ERROR_STATUS and p.get("name")
    ]


def find_eligible_subscriptions(body: dict[str, Any], user: str | None = DEFAULT_USER) -> list[Subscription]:
    """Retourne les souscriptions dont context.user == user (None = pas de filtre)
    et dont geninfo.demands respecte is_eligible()."""
    eligible: list[Subscription] = []
    for row in extract_rows(body):
        row_user = (row.get("context") or {}).get("user", "")
        if user is not None and row_user != user:
            continue
        geninfo = _geninfo(row)
        demands = _demands(row)
        subscription_id = geninfo.get("subscription_id")
        if not subscription_id or not is_eligible(demands):
            continue
        eligible.append(Subscription(
            subscription_id=subscription_id,
            name=geninfo.get("name", ""),
            user=row_user,
            status=geninfo.get("status", ""),
            environment=geninfo.get("environment", ""),
            region=geninfo.get("region", ""),
            actions=[_demand_label(d) for d in demands],
            failed_delete_demand_ids=failed_delete_demand_ids(demands),
        ))
    return eligible


# --------------------------------------------------------------------------- #
# Mode on-error : sélection
# --------------------------------------------------------------------------- #

def all_demands_in_status(row: Row, status: str = DEMAND_ON_ERROR_STATUS) -> bool:
    """True si geninfo.demands est non vide et que chaque demande a ce status."""
    demands = _demands(row)
    return bool(demands) and all(d.get("status") == status for d in demands)


def find_error_demands(
    subscriptions: Iterable[Row],
    user: str | None = DEFAULT_USER,
    demand_status: str = DEMAND_ON_ERROR_STATUS,
    subscription_status_filter: str | None = None,
) -> list[ErrorDemand]:
    """Souscriptions du listing dont TOUTES les demandes (geninfo.demands) sont en
    demand_status, filtrées sur context.user == user (si user n'est pas None) et
    sur geninfo.status == subscription_status_filter (si fourni). Retourne leurs
    demandes, triées par souscription puis create_date."""
    found: list[ErrorDemand] = []
    for row in subscriptions:
        sub_id = subscription_uuid(row)
        row_user = subscription_user(row)
        if (not sub_id
                or (user is not None and row_user != user)
                or (subscription_status_filter and subscription_status(row) != subscription_status_filter)
                or not all_demands_in_status(row, demand_status)):
            continue
        found.extend(
            ErrorDemand(
                subscription_id=sub_id,
                demand_id=demand_uuid(demand),
                subscription_name=subscription_name(row),
                user=row_user,
                action=_first(demand, "action"),
                status=str(demand.get("status", "")),
                create_date=_first(demand, "create_date", "created_at"),
            )
            for demand in _demands(row) if demand_uuid(demand)
        )
    found.sort(key=lambda d: (d.subscription_id, d.create_date, d.demand_id))
    return found


# --------------------------------------------------------------------------- #
# Pagination
# --------------------------------------------------------------------------- #

def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def total_pages_hint(body: Any, size: int) -> int | None:
    """Nombre total de pages annoncé par la réponse (total_pages / totalPages,
    ou total / total_count / totalElements / count divisé par size), sinon None."""
    if not isinstance(body, dict):
        return None
    candidates = [body]
    if isinstance(body.get("result"), dict):
        candidates.append(body["result"])
    for key in ("page", "pagination", "meta"):
        candidates += [c[key] for c in list(candidates) if isinstance(c.get(key), dict)]

    def first_int(keys: Sequence[str]) -> int | None:
        for c in candidates:
            for key in keys:
                n = _as_int(c.get(key))
                if n is not None:
                    return n
        return None

    pages = first_int(("total_pages", "totalPages", "pages", "page_count", "pageCount"))
    if pages is not None:
        return max(pages, 0)
    total = first_int(("total", "total_count", "totalCount", "totalElements", "total_elements", "count"))
    if total is None or size <= 0:
        return None
    return max(-(-total // size), 0)


def iterate_pages(
    fetch_page: Callable[[int, int], Any],
    size: int = DEFAULT_PAGE_SIZE,
    first_page: int = DEFAULT_FIRST_PAGE,
    max_pages: int = MAX_PAGES,
    progress: Callable[[int, int], None] | None = None,
) -> list[Row]:
    """Appelle fetch_page(page, size) depuis first_page et concatène les rows.

    Arrêt : page vide, page incomplète (< size), total_pages atteint, page
    identique à la précédente (API qui ignore ?page=), ou max_pages."""
    rows: list[Row] = []
    previous_keys: list[str] | None = None
    for index in range(max_pages):
        page = first_page + index
        body = fetch_page(page, size)
        page_rows = extract_rows(body) if isinstance(body, dict) else extract_items(body)
        if progress:
            progress(page, len(page_rows))
        keys = [subscription_uuid(r) for r in page_rows]
        if not page_rows or keys == previous_keys:
            break
        rows.extend(page_rows)
        hint = total_pages_hint(body, size)
        if (hint is not None and index + 1 >= hint) or len(page_rows) < size:
            break
        previous_keys = keys
    return rows


def dedupe_rows(rows: Iterable[Row]) -> list[Row]:
    """Supprime les doublons de subscription uuid (chevauchement de pages)."""
    seen: set[str] = set()
    out: list[Row] = []
    for row in rows:
        key = subscription_uuid(row)
        if key:
            if key in seen:
                continue
            seen.add(key)
        out.append(row)
    return out


def filter_product(rows: Iterable[Row], product: str | None) -> list[Row]:
    """Garde les rows dont geninfo.product == product (rows sans product conservées)."""
    if not product:
        return list(rows)
    return [r for r in rows if _geninfo(r).get("product", product) == product]


# --------------------------------------------------------------------------- #
# Token : lecture, expiration, récupération depuis le Swagger
# --------------------------------------------------------------------------- #

_JWT_RE = re.compile(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*")

# Expression JS évaluée dans la page Swagger, dans l'ordre :
# 1. l'état Swagger UI (bouton Authorize : OAuth2 Keycloak ou bearer collé) ;
# 2. l'adaptateur keycloak-js de la page (window.keycloak / kc / _keycloak) ;
# 3. un access_token dans le fragment de l'URL (retour Keycloak en implicit) ;
# 4. le premier JWT de sessionStorage / localStorage ; sinon "".
TOKEN_FINDER_JS = (
    "(()=>{const J=/[A-Za-z0-9_-]{10,}\\.[A-Za-z0-9_-]{10,}\\.[A-Za-z0-9_-]+/;let t='';"
    "try{const a=(window.ui||ui).authSelectors.authorized().toJS();"
    "for(const k in a){const v=a[k];t=(v.token&&v.token.access_token)||v.value||'';if(t)break}}catch(e){}"
    "for(const k of['keycloak','kc','_keycloak'])if(!t&&window[k]&&typeof window[k].token=='string')t=window[k].token;"
    "if(!t)t=new URLSearchParams(location.hash.slice(1)).get('access_token')||'';"
    "if(!t)for(const s of[sessionStorage,localStorage])for(let i=0;i<s.length&&!t;i++){"
    "const m=(s.getItem(s.key(i))||'').match(J);if(m)t=m[0]}"
    "return t.replace(/^Bearer\\s+/i,'')})()"
)

# Bookmarklet à mettre en favori : sur la page Swagger (après login SSO), il
# copie le token trouvé par TOKEN_FINDER_JS dans le presse-papiers.
BOOKMARKLET = (
    f"javascript:(()=>{{const t={TOKEN_FINDER_JS};"
    "if(!t){alert('Aucun token trouvé : cliquer Authorize dans le Swagger');return}"
    "navigator.clipboard.writeText(t).then("
    "()=>alert('Token copié ('+t.length+' car.)'),()=>prompt('Copier le token :',t))})()"
)


def clean_token(raw: str) -> str:
    """Nettoie un copier-coller : guillemets, 'Authorization:', 'Bearer ', espaces."""
    value = raw.strip().strip("\"'").strip()
    if value.lower().startswith("authorization:"):
        value = value.split(":", 1)[1].strip()
    if value.lower().startswith("bearer "):
        value = value[len("bearer "):].strip()
    match = _JWT_RE.search(value)
    return match.group(0) if match else value


def jwt_expiry(token: str) -> int | None:
    """Claim exp du JWT (signature non vérifiée), None si ce n'est pas un JWT."""
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        claims = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
        return int(claims["exp"])
    except (ValueError, KeyError, TypeError):
        return None


def token_seconds_left(token: str, now: float | None = None) -> int | None:
    exp = jwt_expiry(token)
    return None if exp is None else int(exp - (time.time() if now is None else now))


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


def _log(message: str) -> None:
    print(message, file=sys.stderr)


# --------------------------------------------------------------------------- #
# Token : récupération automatique via Chrome / Edge (protocole DevTools)
# --------------------------------------------------------------------------- #

BROWSER_CANDIDATES = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "microsoft-edge",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
)
BROWSER_LOGIN_TIMEOUT = 300  # secondes laissées pour le login SSO


def _duration(seconds: float) -> str:
    return f"{int(seconds)} s" if seconds < 60 else f"{int(seconds) // 60} min"


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


def _find_token_in_pages(port: int) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/json/list", timeout=5) as response:
        targets = json.load(response)
    for target in targets:
        ws_url = target.get("webSocketDebuggerUrl")
        if target.get("type") != "page" or not ws_url:
            continue
        try:
            token = clean_token(str(devtools_evaluate(ws_url, TOKEN_FINDER_JS) or ""))
        except (OSError, ValueError):
            continue
        left = token_seconds_left(token)
        if token and (left is None or left >= TOKEN_MIN_VALIDITY):
            return token
    return ""


def browser_token(
    swagger_url: str,
    private: bool = True,
    insecure: bool = False,
    timeout: float = BROWSER_LOGIN_TIMEOUT,
    browser: str | None = None,
    poll_interval: float = 2,
) -> str:
    """Ouvre le Swagger dans un Chrome / Edge dédié (profil temporaire, navigation
    privée par défaut), attend le login SSO + Authorize, lit le token dans la page
    via le protocole DevTools puis ferme le navigateur. "" si échec ou délai dépassé."""
    browser = browser or find_chromium_browser()
    if not browser:
        _log("Chrome / Edge introuvable : récupération automatique impossible.")
        return ""
    profile_dir = tempfile.mkdtemp(prefix="subscriptions-cleanup-")
    command = [browser, f"--user-data-dir={profile_dir}", "--remote-debugging-port=0",
               "--no-first-run", "--no-default-browser-check", "--new-window"]
    if private:
        command.append("--incognito")
    if insecure:
        command.append("--ignore-certificate-errors")
    process = subprocess.Popen(command + [swagger_url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        port = _devtools_port(profile_dir, process)
        _log(f"Navigateur ouvert{' en navigation privée' if private else ''} : se connecter en SSO "
             f"puis cliquer Authorize (attente {_duration(timeout)} max, fermer la fenêtre pour annuler).")
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
    swagger_url: str | None,
    read_clip: Callable[[], str] = read_clipboard,
    ask: Callable[[str], str] = getpass.getpass,
    open_url: Callable[[str], Any] = webbrowser.open,
    ask_url: Callable[[str], str] = input,
    attempts: int = 3,
    auto: bool = True,
    auto_browser: Callable[[str], str] = browser_token,
) -> str:
    """Récupère un token depuis le Swagger (URL demandée si non configurée) :
    - auto : via Chrome / Edge piloté (browser_token), sans copier-coller ;
    - sinon, ou si ça échoue : ouvre le Swagger, attend que l'utilisateur copie
      le token puis le lit dans le presse-papiers (ou le prend s'il est collé).
    Retourne "" hors terminal interactif ou après `attempts` essais."""
    interactive = sys.stdin.isatty()
    if not swagger_url and interactive:
        swagger_url = ask_url(f"URL du Swagger à ouvrir (${SWAGGER_URL_ENV} non défini, Entrée pour passer) : ").strip()
        if swagger_url:
            _log(f"Astuce : export {SWAGGER_URL_ENV}={swagger_url}")
    if swagger_url and auto:
        token = auto_browser(swagger_url)
        if token:
            return token
        _log("Passage à la copie manuelle du token.")
    if not interactive:
        _log("Terminal non interactif : impossible de demander le token "
             "(lancer le script dans un terminal, ou passer --token).")
        return ""
    if swagger_url:
        _log(f"Ouverture du Swagger : {swagger_url}")
        if not open_url(swagger_url):
            _log(f"Navigateur non ouvert : ouvrir {swagger_url} à la main.")
    _log("Se connecter en SSO, cliquer Authorize puis copier le token (bookmarklet : --print-bookmarklet).")
    for _ in range(attempts):
        pasted = clean_token(ask("Entrée pour lire le presse-papiers (ou coller le token) : "))
        token = pasted or clean_token(read_clip())
        left = token_seconds_left(token)
        if not token:
            _log("Presse-papiers vide.")
        elif left is None and not pasted:
            _log("Le presse-papiers ne contient pas de JWT.")
        elif left is not None and left < TOKEN_MIN_VALIDITY:
            _log("Token expiré, en copier un nouveau depuis le Swagger.")
        else:
            return token
    return ""


def resolve_token(
    token: str | None,
    swagger_url: str | None,
    acquire: Callable[[str | None], str] | None = None,
) -> str:
    """Token utilisable : celui fourni s'il n'est pas expiré, sinon un nouveau
    récupéré depuis le Swagger. Lève CliExit si aucun n'est disponible."""
    if token:
        token = clean_token(token)
        left = token_seconds_left(token)
        if left is None or left >= TOKEN_MIN_VALIDITY:
            if left is not None:
                _log(f"Token valide encore {left // 60} min.")
            return token
        _log("Token fourni expiré : récupération d'un nouveau depuis le Swagger.")
    token = (acquire or acquire_token_interactively)(swagger_url)
    if not token:
        raise CliExit(EXIT_USAGE, f"Token manquant: --token, ${TOKEN_ENV}, ou copie depuis le Swagger "
                                  f"(--swagger-url / ${SWAGGER_URL_ENV})")
    left = token_seconds_left(token)
    if left is not None:
        _log(f"Token récupéré, valide encore {left // 60} min.")
    return token


# --------------------------------------------------------------------------- #
# TLS
# --------------------------------------------------------------------------- #

def _load_ca_cert(context: ssl.SSLContext, path: str) -> None:
    """Ajoute un certificat CA (fichier .cer/.crt/.pem, encodé PEM ou DER) au contexte."""
    with open(path, "rb") as fh:
        data = fh.read()
    if not data.strip():
        raise ValueError(f"certificat vide: {path}")
    try:
        context.load_verify_locations(cadata=data.decode("ascii") if b"-----BEGIN" in data else data)
    except (ssl.SSLError, UnicodeDecodeError, ValueError) as exc:
        raise ValueError(f"certificat illisible: {path} ({exc})") from exc


def build_ssl_context(ca_certs: Iterable[str] | None = None, insecure: bool = False) -> ssl.SSLContext | None:
    """Contexte TLS pour urlopen.

    - insecure : aucune vérification (check_hostname=False, CERT_NONE) ;
    - ca_certs : CA système + chaque fichier (PEM ou DER), typiquement les
      "Root-Certificats-Internes" BNPP ;
    - sinon None : comportement par défaut de urllib (CA système / $SSL_CERT_FILE).
    """
    if insecure:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context
    paths = [os.path.expanduser(p) for p in (ca_certs or []) if p]
    if not paths:
        return None
    context = ssl.create_default_context()
    for path in paths:
        _load_ca_cert(context, path)
    return context


def ca_certs_from_env(value: str | None) -> list[str]:
    """Découpe $ORCHESTRATOR_CA_CERTS (séparateur os.pathsep) en liste de chemins."""
    return [p.strip() for p in (value or "").split(os.pathsep) if p.strip()]


# --------------------------------------------------------------------------- #
# Client HTTP
# --------------------------------------------------------------------------- #

def _quote(segment: str) -> str:
    return urllib.parse.quote(segment, safe="")


def _truncate(value: Any, limit: int) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    text = (value if isinstance(value, str) else json.dumps(value)).strip()
    return text if len(text) <= limit else text[:limit] + "..."


def _try_json(raw: bytes) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return raw.decode("utf-8", errors="replace")


class OrchestratorClient:
    def __init__(
        self,
        token: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = DEFAULT_TIMEOUT,
        insecure: bool = False,
        ca_certs: Iterable[str] | None = None,
    ):
        if not token:
            raise ValueError("Bearer token manquant")
        self.token = token
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._ssl_context = build_ssl_context(ca_certs, insecure)

    def _request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout, context=self._ssl_context) as response:
                return _try_json(response.read())
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            raise OrchestratorApiError(f"{method} {url} -> HTTP {exc.code}: {_truncate(raw, 300)}",
                                       status_code=exc.code, payload=_try_json(raw)) from exc
        except urllib.error.URLError as exc:
            raise OrchestratorApiError(f"{method} {url} -> {exc.reason}") from exc

    # --- multireader / apl ---

    def get_subscriptions_page(self, page: int, size: int = DEFAULT_PAGE_SIZE) -> Any:
        """GET /multireader/api/v1/subscriptions?page=<page>&size=<size>."""
        query = urllib.parse.urlencode({"page": page, "size": size})
        return self._request("GET", f"/multireader/api/v1/subscriptions?{query}")

    def get_subscriptions(
        self,
        product: str | None = DEFAULT_PRODUCT,
        page_size: int = DEFAULT_PAGE_SIZE,
        first_page: int = DEFAULT_FIRST_PAGE,
        progress: Callable[[int, int], None] | None = None,
    ) -> dict[str, Any]:
        """Parcourt toutes les pages et retourne {"result": {"rows": [...]}} avec
        toutes les souscriptions (dédoublonnées), filtrées sur geninfo.product."""
        rows = iterate_pages(self.get_subscriptions_page, page_size, first_page, progress=progress)
        return {"result": {"rows": filter_product(dedupe_rows(rows), product)}}

    def delete_subscription(self, subscription_id: str, product_branch: str = DEFAULT_PRODUCT_BRANCH) -> Any:
        """DELETE /apl/v1/subscriptions/<id> avec {"product_branch": ..., "payload": {}}."""
        return self._request("DELETE", f"/apl/v1/subscriptions/{_quote(subscription_id)}",
                             {"product_branch": product_branch, "payload": {}})

    # --- demandes ---

    def get_demand(self, demand_id: str) -> dict[str, Any]:
        """GET /api/v1/demands/<uuid>."""
        return self._request("GET", f"/api/v1/demands/{_quote(demand_id)}")

    def retry_demand(self, demand_id: str, tasks: list[str], retry_non_failed_tasks: bool = False) -> Any:
        """POST /api/v1/demands/<uuid>/retry avec {"tasks": [...], "retry_non_failed_tasks": false}."""
        return self._request("POST", f"/api/v1/demands/{_quote(demand_id)}/retry",
                             {"tasks": tasks, "retry_non_failed_tasks": retry_non_failed_tasks})

    def retry_failed_delete(self, demand_id: str) -> list[str]:
        """GET la demande, relance ses processes en ERROR. Retourne les tasks relancées
        (liste vide si aucun process en ERROR : rien n'est envoyé)."""
        tasks = failed_process_names(self.get_demand(demand_id))
        if tasks:
            self.retry_demand(demand_id, tasks)
        return tasks

    # --- state_manager (mode --on-error) ---

    def set_demand_status(self, demand_id: str, status: str = DECLINED_STATUS,
                          reason: str = DEFAULT_DECLINE_REASON) -> Any:
        """POST /state_manager/api/v1/demands/<demand_id>/status {"status": ..., "reason": ...}."""
        return self._request("POST", f"{STATE_MANAGER_PREFIX}/demands/{_quote(demand_id)}/status",
                             {"status": status, "reason": reason})


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    auth = parser.add_argument_group("authentification")
    auth.add_argument("--token", default=os.environ.get(TOKEN_ENV),
                      help=f"bearer token (défaut: ${TOKEN_ENV}) ; absent ou expiré : copie depuis le Swagger")
    auth.add_argument("--swagger-url", default=os.environ.get(SWAGGER_URL_ENV) or DEFAULT_SWAGGER_URL,
                      help="page Swagger ouverte quand il faut un token "
                           f"(défaut: ${SWAGGER_URL_ENV}, sinon DEFAULT_SWAGGER_URL dans le script)")
    auth.add_argument("--manual-token", action="store_true",
                      help="ne pas piloter Chrome / Edge : copier le token à la main depuis le Swagger")
    auth.add_argument("--no-private", action="store_true",
                      help="ouvrir le Swagger hors navigation privée (profil temporaire quand même)")
    auth.add_argument("--print-bookmarklet", action="store_true",
                      help="affiche le bookmarklet qui copie le token depuis le Swagger, puis quitte")

    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--product", default=DEFAULT_PRODUCT,
                        help=f"filtre geninfo.product (défaut: {DEFAULT_PRODUCT})")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE,
                        help=f"taille de page du listing (défaut: {DEFAULT_PAGE_SIZE})")
    parser.add_argument("--first-page", type=int, default=DEFAULT_FIRST_PAGE,
                        help=f"numéro de la première page (défaut: {DEFAULT_FIRST_PAGE})")
    parser.add_argument("--product-branch", default=DEFAULT_PRODUCT_BRANCH,
                        help=f"product_branch du payload DELETE (défaut: {DEFAULT_PRODUCT_BRANCH})")
    parser.add_argument("--user", default=DEFAULT_USER,
                        help=f"ne garder que context.user == USER (défaut: {DEFAULT_USER})")
    parser.add_argument("--all-users", action="store_true",
                        help="pas de filtre sur context.user")
    parser.add_argument("--input", metavar="FILE",
                        help="lit le listing depuis un fichier JSON au lieu du GET")
    parser.add_argument("--delete", action="store_true",
                        help="exécute les DELETE / retry (sinon dry-run)")
    parser.add_argument("--yes", action="store_true", help="ne pas demander de confirmation")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"appels DELETE / retry / decline en parallèle (défaut: {DEFAULT_WORKERS})")

    on_error = parser.add_argument_group("mode on-error (demandes à décliner)")
    on_error.add_argument("--on-error", "--locked", action="store_true", dest="on_error",
                          help="liste les demandes des souscriptions dont toutes les demandes sont en erreur")
    on_error.add_argument("--decline", action="store_true",
                          help=f"passe ces demandes en {DECLINED_STATUS} (sinon dry-run)")
    on_error.add_argument("--reason", default=DEFAULT_DECLINE_REASON,
                          help=f"raison envoyée avec le status (défaut: {DEFAULT_DECLINE_REASON!r})")
    on_error.add_argument("--subscription-status", default=None,
                          help="ne garder que les souscriptions ayant ce geninfo.status (ex: LOCKED ; défaut: tous)")
    on_error.add_argument("--demand-status", default=DEMAND_ON_ERROR_STATUS,
                          help=f"status que doivent avoir toutes les demandes (défaut: {DEMAND_ON_ERROR_STATUS})")

    tls = parser.add_mutually_exclusive_group()
    tls.add_argument("--ca-cert", nargs="+", metavar="FILE", dest="ca_certs",
                     default=ca_certs_from_env(os.environ.get(CA_CERTS_ENV)),
                     help="certificat(s) CA interne(s) à ajouter aux CA système, .cer/.pem en PEM ou DER "
                          f"(défaut: ${CA_CERTS_ENV}, chemins séparés par '{os.pathsep}')")
    tls.add_argument("--insecure", action="store_true", default=None,
                     help="désactive la vérification TLS "
                          f"(défaut: {'oui' if DEFAULT_INSECURE else 'non'}, sauf avec --ca-cert / ${CA_CERTS_ENV})")
    tls.add_argument("--verify-tls", action="store_true",
                     help="vérifie les certificats TLS avec les CA système")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--json", action="store_true", dest="as_json",
                        help="sortie JSON (liste des éléments retenus)")

    args = parser.parse_args(argv)
    if args.insecure is None:
        args.insecure = DEFAULT_INSECURE and not args.verify_tls and not args.ca_certs
    if args.page_size < 1:
        parser.error("--page-size doit être >= 1")
    if args.workers < 1:
        parser.error("--workers doit être >= 1")
    if args.decline and not args.on_error:
        parser.error("--decline nécessite --on-error")
    if args.on_error and args.delete:
        parser.error("--on-error est incompatible avec --delete")
    return args


def _make_client(args: argparse.Namespace) -> OrchestratorClient:
    return OrchestratorClient(args.token, args.base_url, args.timeout,
                              insecure=args.insecure, ca_certs=args.ca_certs)


class Session:
    """Client HTTP créé à la demande : pas de token demandé tant qu'aucun appel
    réseau n'est nécessaire (--input en dry-run)."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self._client: OrchestratorClient | None = None

    @property
    def client(self) -> OrchestratorClient:
        if self._client is None:
            self.args.token = resolve_token(self.args.token, self.args.swagger_url, self._acquire_token)
            if self.args.insecure:
                _log("TLS non vérifié (--verify-tls ou --ca-cert pour vérifier les certificats).")
            try:
                self._client = _make_client(self.args)
            except (OSError, ValueError) as exc:
                raise CliExit(EXIT_USAGE, f"Certificat CA invalide: {exc}") from exc
        return self._client

    def _acquire_token(self, swagger_url: str | None) -> str:
        return acquire_token_interactively(
            swagger_url, auto=not self.args.manual_token,
            auto_browser=lambda url: browser_token(url, private=not self.args.no_private,
                                                   insecure=self.args.insecure))

    def load_rows(self) -> list[Row]:
        """Listing des souscriptions : fichier --input, sinon GET paginé."""
        if self.args.input:
            with open(self.args.input, encoding="utf-8") as fh:
                return extract_rows(json.load(fh))
        try:
            body = self.client.get_subscriptions(
                self.args.product, self.args.page_size, self.args.first_page,
                lambda page, count: _log(f"page {page}: {count} row(s)"))
        except OrchestratorApiError as exc:
            hint = ""
            if "CERTIFICATE_VERIFY_FAILED" in str(exc):
                hint = f"\nAstuce: passer le CA interne avec --ca-cert <fichier.cer> (ou ${CA_CERTS_ENV})."
            raise CliExit(EXIT_GET_FAILED, f"GET échoué: {exc}{hint}") from exc
        return extract_rows(body)


def _confirm(question: str) -> bool:
    return input(f"\n{question} [y/N] ").strip().lower() in ("y", "yes", "o", "oui")


def _run_all(items: Sequence[T], action: Callable[[T], bool], workers: int) -> int:
    """Exécute action sur chaque élément (en parallèle si workers > 1) et
    retourne le nombre d'échecs. Les résultats restent dans l'ordre des items."""
    if workers <= 1 or len(items) <= 1:
        results = [action(item) for item in items]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(action, items))
    return results.count(False)


def run_cleanup(args: argparse.Namespace, session: Session, rows: list[Row]) -> int:
    """Mode delete : DELETE des souscriptions éligibles, retry des deletes en échec."""
    user_filter = None if args.all_users else args.user
    eligible = find_eligible_subscriptions({"result": {"rows": rows}}, user_filter)

    if args.as_json and not args.delete:
        print(json.dumps([s.subscription_id for s in eligible], indent=2))
        return EXIT_OK

    scope = "tous users" if user_filter is None else f"user={user_filter}"
    retry_count = sum(s.needs_retry for s in eligible)
    print(f"{len(rows)} souscription(s) lue(s), {len(eligible)} éligible(s) ({scope}): "
          f"{len(eligible) - retry_count} à supprimer, {retry_count} delete à relancer")
    for sub in eligible:
        plan = f"RETRY {','.join(sub.failed_delete_demand_ids)}" if sub.needs_retry else "DELETE"
        print(f"  {sub.subscription_id}  {sub.name:<16} {sub.user:<12} {sub.environment:<5} {sub.region:<7} "
              f"{sub.status:<12} demands={','.join(sub.actions)}  -> {plan}")

    if not eligible:
        return EXIT_OK
    if not args.delete:
        print("\nDry-run: relancer avec --delete pour exécuter.")
        return EXIT_OK
    client = session.client
    if not args.yes and not _confirm(f"Exécuter {len(eligible) - retry_count} DELETE et {retry_count} retry ?"):
        print("Annulé.")
        return EXIT_OK

    def process(sub: Subscription) -> bool:
        if sub.needs_retry:
            return all([retry(sub, demand_id) for demand_id in sub.failed_delete_demand_ids])
        try:
            response = client.delete_subscription(sub.subscription_id, args.product_branch)
        except OrchestratorApiError as exc:
            _log(f"DELETE {sub.subscription_id} ({sub.name}) -> ERREUR {exc}")
            return False
        print(f"DELETE {sub.subscription_id} ({sub.name}) -> OK {_truncate(response, 120)}")
        return True

    def retry(sub: Subscription, demand_id: str) -> bool:
        try:
            tasks = client.retry_failed_delete(demand_id)
        except OrchestratorApiError as exc:
            _log(f"RETRY {demand_id} ({sub.name}) -> ERREUR {exc}")
            return False
        outcome = f"OK tasks={','.join(tasks)}" if tasks else f"ignoré, aucun process en {PROCESS_ERROR_STATUS}"
        print(f"RETRY {demand_id} ({sub.name}) -> {outcome}")
        return True

    failures = _run_all(eligible, process, args.workers)
    print(f"\n{len(eligible) - failures} traitée(s), {failures} en échec.")
    return EXIT_ACTION_FAILED if failures else EXIT_OK


def run_on_error(args: argparse.Namespace, session: Session, rows: list[Row]) -> int:
    """Mode --on-error : liste (et avec --decline, décline) les demandes des
    souscriptions dont toutes les demandes sont en ON_ERROR."""
    user_filter = None if args.all_users else args.user
    demands = find_error_demands(rows, user_filter, args.demand_status, args.subscription_status)

    if args.as_json and not args.decline:
        print(json.dumps([{"subscription_id": d.subscription_id, "demand_id": d.demand_id,
                           "action": d.action, "status": d.status} for d in demands], indent=2))
        return EXIT_OK

    scope = "tous users" if user_filter is None else f"user={user_filter}"
    if args.subscription_status:
        scope += f", status={args.subscription_status}"
    subscription_count = len({d.subscription_id for d in demands})
    print(f"{len(rows)} souscription(s) lue(s) ({scope}) : {subscription_count} avec toutes leurs "
          f"demandes en {args.demand_status}, {len(demands)} demande(s) à passer en {DECLINED_STATUS}")
    for d in demands:
        print(f"  {d.subscription_id}  {d.subscription_name:<16} {d.user:<12} "
              f"demand={d.demand_id} {d.action:<12} {d.status:<10} {d.create_date}"
              f"  -> {DECLINED_STATUS} ({args.reason})")

    if not demands:
        return EXIT_OK
    if not args.decline:
        print("\nDry-run: relancer avec --on-error --decline pour exécuter.")
        return EXIT_OK
    client = session.client
    if not args.yes and not _confirm(f"Passer {len(demands)} demande(s) en {DECLINED_STATUS} ?"):
        print("Annulé.")
        return EXIT_OK

    def decline(d: ErrorDemand) -> bool:
        label = d.subscription_name or d.subscription_id
        try:
            response = client.set_demand_status(d.demand_id, DECLINED_STATUS, args.reason)
        except OrchestratorApiError as exc:
            _log(f"DECLINE {d.demand_id} ({label}) -> ERREUR {exc}")
            return False
        print(f"DECLINE {d.demand_id} ({label}) -> OK {_truncate(response, 120)}")
        return True

    failures = _run_all(demands, decline, args.workers)
    print(f"\n{len(demands) - failures} déclinée(s), {failures} en échec.")
    return EXIT_ACTION_FAILED if failures else EXIT_OK


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.print_bookmarklet:
        print(BOOKMARKLET)
        return EXIT_OK
    session = Session(args)
    try:
        rows = session.load_rows()
        return (run_on_error if args.on_error else run_cleanup)(args, session, rows)
    except CliExit as exc:
        if str(exc):
            _log(str(exc))
        return exc.code


if __name__ == "__main__":
    sys.exit(main())
