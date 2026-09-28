"""Tests unitaires de toolchain_env (python -m pytest ou python test_toolchain_env.py).

Vault est remplacé par un petit serveur HTTP local, terraform et le navigateur
par des doublures ; le cache ne touche jamais celui de l'utilisateur."""

from __future__ import annotations

import http.server
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import toolchain_env as te  # noqa: E402

os.environ["XDG_CACHE_HOME"] = tempfile.mkdtemp(prefix="cos-toolchain-tests-")

TOKEN_OK = "hvs.CAESIEKO1gxUBXCdPSqoxyv5kr8P0bG4Mc5lgJEOsaVtqeTq"
TOKEN_BAD = "hvs.CAESIFXQc9s9BYB1K9iDoRwwugBc3iFctwrVl30RdM9Ml4au"
API_KEY = "YqGLLC6QTdV46Usp0ITQAuXGYAeKNRLRhnf9WH4G9KmK"
SECRET_PATH = "ibm_ac002i000263/creds/rl002i000138_buhub"
UID = "lh90871"


class FakeVault(http.server.BaseHTTPRequestHandler):
    """lookup-self : 200 avec ttl pour TOKEN_OK, 403 sinon ;
    <SECRET_PATH> : api_key + lease pour TOKEN_OK, 403 sinon."""
    ttl = 3600
    lease = 7200
    requests: list[tuple[str, dict[str, str]]] = []

    def do_GET(self) -> None:  # noqa: N802
        FakeVault.requests.append((self.path, {k.lower(): v for k, v in self.headers.items()}))
        if self.path.startswith("/v1/token/"):  # service token
            if self.path == f"/v1/token/{UID}?namespace=AP85135":
                return self._send(200, {"auth": {"client_token": TOKEN_OK, "policies": ["ap85135-ops"],
                                                 "lease_duration": 2592000}, "data": None})
            return self._send(404, {"detail": "Not Found"})
        token = self.headers.get("X-Vault-Token", "")
        if token != TOKEN_OK:
            return self._send(403, {"errors": ["permission denied"]})
        if self.path == "/v1/auth/token/lookup-self":
            return self._send(200, {"data": {"ttl": FakeVault.ttl, "expire_time": "2030-01-01T00:00:00Z"}})
        if self.path == f"/v1/{SECRET_PATH}":
            return self._send(200, {"lease_duration": FakeVault.lease,
                                    "data": {"api_key": API_KEY, "service_id": "ServiceId-1"}})
        self._send(404, {"errors": []})

    def _send(self, code: int, body: dict) -> None:
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args) -> None:  # silence
        pass


def _fresh_cache() -> None:
    te.forget_all()


def _args(server_url: str, *extra: str, tests_dir: str | None = None) -> "te.argparse.Namespace":
    argv = ["--env", "int", "--vault-url", server_url, "--skip-login", "--skip-init", "--no-proxy",
            "--token-service", server_url, "--uid", UID,
            "--dir", tests_dir or tempfile.mkdtemp(prefix="tfdir-"), *extra]
    return te.parse_args(argv)


class VaultServerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = http.server.HTTPServer(("127.0.0.1", 0), FakeVault)
        cls.url = f"http://127.0.0.1:{cls.server.server_port}"
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self) -> None:
        _fresh_cache()
        FakeVault.requests = []
        FakeVault.ttl = 3600
        FakeVault.lease = 7200
        os.environ.pop(te.VAULT_TOKEN_ENV, None)


# --------------------------------------------------------------------------- #
# Helpers purs
# --------------------------------------------------------------------------- #

class CleanTokenTest(unittest.TestCase):
    def test_extracts_token_from_curl_header(self):
        self.assertEqual(te.clean_token(f'"X-Vault-Token:{TOKEN_OK}"'), TOKEN_OK)
        self.assertEqual(te.clean_token(f"  X-Vault-Token: {TOKEN_OK}\n"), TOKEN_OK)
        self.assertEqual(te.clean_token(f"--header 'X-Vault-Token:{TOKEN_OK}' -H ..."), TOKEN_OK)

    def test_legacy_formats(self):
        self.assertEqual(te.clean_token("s.abcdefghijklmnopqrstuvwx"), "s.abcdefghijklmnopqrstuvwx")
        self.assertEqual(te.clean_token("hvb.AAAAAQK6UJvKUQKRvEwaO0ZcIF2tMZ2z"), "hvb.AAAAAQK6UJvKUQKRvEwaO0ZcIF2tMZ2z")

    def test_unknown_value_returned_stripped(self):
        self.assertEqual(te.clean_token("  'root'  "), "root")
        self.assertEqual(te.clean_token(""), "")


class ExtractApiKeyTest(unittest.TestCase):
    def test_ibmcloud_engine(self):
        self.assertEqual(te.extract_api_key({"api_key": API_KEY, "service_id": "x"}), API_KEY)

    def test_other_field_names(self):
        self.assertEqual(te.extract_api_key({"apikey": " k "}), "k")
        self.assertEqual(te.extract_api_key({"ibm_api_key": "k2"}), "k2")
        self.assertEqual(te.extract_api_key({"service_id": "x"}), "")
        self.assertEqual(te.extract_api_key({}), "")


class EnvVarsTest(unittest.TestCase):
    def test_build_and_export(self):
        proxy = {"http_proxy": "http://u:p@ncproxy:8080", "https_proxy": "http://u:p@ncproxy:8080",
                 "no_proxy": te.DEFAULT_NO_PROXY}
        variables = te.build_env_vars("k'1", tf_log="debug", proxy_vars=proxy)
        self.assertEqual(variables["IBM_CLOUD_API_KEY"], "k'1")
        self.assertEqual(variables["ORCHESTRATOR_IBMCLOUD_API_KEY"], "k'1")
        self.assertEqual(variables["TF_LOG"], "debug")
        self.assertEqual(variables["https_proxy"], "http://u:p@ncproxy:8080")
        self.assertEqual(variables["HTTPS_PROXY"], "http://u:p@ncproxy:8080")
        self.assertEqual(variables["no_proxy"], te.DEFAULT_NO_PROXY)
        self.assertEqual(variables["NO_PROXY"], te.DEFAULT_NO_PROXY)
        lines = te.export_lines(variables)
        self.assertIn("export IBM_CLOUD_API_KEY='k'\"'\"'1'\n", lines)
        self.assertIn("export TF_LOG=debug\n", lines)

    def test_minimal(self):
        self.assertEqual(set(te.build_env_vars("k")), set(te.API_KEY_VARS))


class TerraformLoginDetectionTest(unittest.TestCase):
    def test_env_token(self):
        env = {"TF_TOKEN_repo_artifactory__dogen_group_echonet": "x"}
        with mock.patch.object(te, "terraform_credentials_path", return_value="/nope/credentials.tfrc.json"):
            self.assertTrue(te.terraform_logged_in(te.TERRAFORM_HOST, env))
            self.assertFalse(te.terraform_logged_in(te.TERRAFORM_HOST, {}))

    def test_credentials_file(self):
        with tempfile.TemporaryDirectory() as home, mock.patch.object(
                te, "terraform_credentials_path", return_value=os.path.join(home, "credentials.tfrc.json")):
            self.assertFalse(te.terraform_logged_in(te.TERRAFORM_HOST, {}))
            with open(os.path.join(home, "credentials.tfrc.json"), "w") as fh:
                json.dump({"credentials": {te.TERRAFORM_HOST: {"token": "abc"}}}, fh)
            self.assertTrue(te.terraform_logged_in(te.TERRAFORM_HOST, {}))
            self.assertFalse(te.terraform_logged_in("other.host", {}))


class TerraformStepsTest(unittest.TestCase):
    def test_login_skipped_when_logged_in(self):
        run = mock.Mock(return_value=0)
        with mock.patch.object(te, "terraform_logged_in", return_value=True):
            te.ensure_terraform_login("h", "/tmp", run)
        run.assert_not_called()

    def test_login_runs_when_missing(self):
        run = mock.Mock(return_value=0)
        with mock.patch.object(te, "terraform_logged_in", return_value=False), \
                mock.patch.object(sys.stdin, "isatty", return_value=True):
            te.ensure_terraform_login("h", "/tmp", run)
        run.assert_called_once_with(["login", "h"], "/tmp")

    def test_login_failure(self):
        run = mock.Mock(return_value=1)
        with mock.patch.object(te, "terraform_logged_in", return_value=False), \
                mock.patch.object(sys.stdin, "isatty", return_value=True), \
                self.assertRaises(te.CliExit) as ctx:
            te.ensure_terraform_login("h", "/tmp", run)
        self.assertEqual(ctx.exception.code, te.EXIT_TERRAFORM_FAILED)

    def test_init_skipped_then_forced(self):
        run = mock.Mock(return_value=0)
        with tempfile.TemporaryDirectory() as cwd:
            te.ensure_terraform_init(cwd, False, run)
            run.assert_called_once_with(["init"], cwd)
            os.mkdir(os.path.join(cwd, ".terraform"))
            te.ensure_terraform_init(cwd, False, run)
            self.assertEqual(run.call_count, 1)
            te.ensure_terraform_init(cwd, True, run)
            self.assertEqual(run.call_count, 2)

    def test_default_tests_dir(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(te.default_tests_dir("int", root), os.path.join(root, "int"))
            os.makedirs(os.path.join(root, "int", te.NEW_VERSION_DIR))
            self.assertEqual(te.default_tests_dir("int", root), os.path.join(root, "int", te.NEW_VERSION_DIR))


class ParseArgsTest(unittest.TestCase):
    def test_env_defaults(self):
        args = te.parse_args(["--env", "prod"])
        self.assertEqual(args.vault_url, te.VAULTS["group"])
        self.assertEqual(args.secret_path, te.ENVIRONMENTS["prod"].secret_path)
        self.assertIn("namespace=AP85135", args.ui_url)
        self.assertTrue(args.dir.endswith(os.sep + "prod"))

    def test_overrides_and_run(self):
        args = te.parse_args(["--vault", "staging", "--secret-path", "/a/b/", "--run", "apply", "--", "-auto-approve"])
        self.assertEqual(args.vault_url, te.VAULTS["staging"])
        self.assertEqual(args.secret_path, "a/b")
        self.assertEqual(args.run, ["apply", "-auto-approve"])

    def test_run_without_args_is_an_error(self):
        with self.assertRaises(SystemExit):
            te.parse_args(["--run"])
        with self.assertRaises(SystemExit):
            te.parse_args(["--", "-no-color"])  # arguments terraform sans --run
        self.assertIsNone(te.parse_args([]).run)

    def test_run_and_shell_conflict(self):
        with self.assertRaises(SystemExit):
            te.parse_args(["--shell", "--run", "plan"])

    def test_proxy_defaults(self):
        args = te.parse_args([])
        self.assertEqual(args.proxy, te.DEFAULT_PROXY)
        self.assertFalse(args.proxy_from_cli)
        self.assertEqual(args.token_service, te.TOKEN_SERVICES["dev"])
        self.assertTrue(te.parse_args(["--proxy", "p:1"]).proxy_from_cli)
        with self.assertRaises(SystemExit):
            te.parse_args(["--no-proxy", "--proxy", "p:1"])


class ProxyTest(unittest.TestCase):
    def setUp(self) -> None:
        _fresh_cache()

    def test_proxy_url_encodes_credentials(self):
        self.assertEqual(te.proxy_url("ncproxy.fr.net.intra:8080", "h90871", "Ab@c!/d"),
                         "http://h90871:Ab%40c%21%2Fd@ncproxy.fr.net.intra:8080")
        self.assertEqual(te.proxy_url("http://p:1/", "", ""), "http://p:1")

    def test_mask_url(self):
        self.assertEqual(te.mask_url("http://h90871:Secret%21@ncproxy:8080"), "http://h90871:***@ncproxy:8080")
        self.assertEqual(te.mask_url("http://ncproxy:8080"), "http://ncproxy:8080")

    def test_no_proxy(self):
        self.assertEqual(te.resolve_proxy(te.parse_args(["--no-proxy"]), environ={}), {})

    def test_asks_user_and_password_and_remembers_user(self):
        args = te.parse_args([])
        asked = []
        with mock.patch.object(sys.stdin, "isatty", return_value=True):
            proxy = te.resolve_proxy(args, ask=lambda q: (asked.append(q), "h90871")[1],
                                     ask_secret=lambda q: "Pass!", environ={})
        self.assertEqual(proxy["https_proxy"], f"http://h90871:Pass%21@{te.DEFAULT_PROXY}")
        self.assertEqual(proxy["http_proxy"], proxy["https_proxy"])
        self.assertEqual(proxy["no_proxy"], te.DEFAULT_NO_PROXY)
        self.assertEqual(te.load_setting("proxy_user"), "h90871")
        self.assertNotIn("Pass!", json.dumps(te._read_state()))
        # deuxième lancement : le user est proposé par défaut, le mot de passe redemandé
        asked.clear()
        with mock.patch.object(sys.stdin, "isatty", return_value=True):
            proxy = te.resolve_proxy(te.parse_args([]), ask=lambda q: (asked.append(q), "")[1],
                                     ask_secret=lambda q: "x", environ={})
        self.assertIn("h90871", asked[0])
        self.assertEqual(proxy["https_proxy"], f"http://h90871:x@{te.DEFAULT_PROXY}")

    def test_cli_credentials_without_prompt(self):
        args = te.parse_args(["--proxy-user", "u", "--proxy-password", "p", "--proxy", "p:1"])
        proxy = te.resolve_proxy(args, ask=lambda q: self.fail("prompt"), ask_secret=lambda q: self.fail("prompt"),
                                 environ={"https_proxy": "http://ignored:1"})
        self.assertEqual(proxy["https_proxy"], "http://u:p@p:1")

    def test_existing_shell_proxy_reused(self):
        environ = {"https_proxy": "http://u:p@old:8080", "no_proxy": "a,b"}
        proxy = te.resolve_proxy(te.parse_args([]), ask=lambda q: self.fail("prompt"),
                                 ask_secret=lambda q: self.fail("prompt"), environ=environ)
        self.assertEqual(proxy, {"http_proxy": "http://u:p@old:8080", "https_proxy": "http://u:p@old:8080",
                                 "no_proxy": "a,b"})

    def test_non_interactive_without_credentials(self):
        with mock.patch.object(sys.stdin, "isatty", return_value=False), self.assertRaises(te.CliExit):
            te.resolve_proxy(te.parse_args([]), environ={})

    def test_with_no_proxy_adds_token_service_host(self):
        proxy = {"https_proxy": "http://p", "no_proxy": "localhost,.echonet"}
        self.assertEqual(te.with_no_proxy(proxy, "https://s02vl9956141:4430")["no_proxy"],
                         "localhost,.echonet,s02vl9956141")
        self.assertEqual(te.with_no_proxy(proxy, "https://x.echonet/")["no_proxy"], "localhost,.echonet")
        self.assertEqual(te.with_no_proxy(proxy, "")["no_proxy"], "localhost,.echonet")
        self.assertEqual(te.with_no_proxy({}, "https://s02:1"), {})

    def test_check_proxy(self):
        proxy = {"http_proxy": "http://u:p@px:1", "https_proxy": "http://u:p@px:1", "no_proxy": ""}
        import urllib.error
        te.check_proxy({})  # aucun proxy : rien à vérifier
        with mock.patch.object(te.urllib.request, "urlopen", side_effect=urllib.error.HTTPError(
                "u", 407, "authenticationrequired", {}, None)), self.assertRaises(te.CliExit) as ctx:
            te.check_proxy(proxy)
        self.assertIn("identifiants refusés", str(ctx.exception))
        self.assertNotIn(":p@", str(ctx.exception))
        with mock.patch.object(te.urllib.request, "urlopen", side_effect=urllib.error.HTTPError(
                "u", 405, "Method Not Allowed", {}, None)):
            te.check_proxy(proxy)  # le proxy laisse passer : pas d'erreur
        with mock.patch.object(te.urllib.request, "urlopen", side_effect=urllib.error.URLError("timed out")):
            te.check_proxy(proxy)  # injoignable : avertissement seulement
        with mock.patch.object(te.urllib.request, "urlopen", side_effect=urllib.error.URLError(
                "Tunnel connection failed: 407 authenticationrequired")), self.assertRaises(te.CliExit):
            te.check_proxy(proxy)

    def test_apply_proxy_sets_process_env(self):
        with mock.patch.dict(os.environ, {"HTTPS_PROXY": "http://x", "no_proxy": "old"}, clear=False):
            te.apply_proxy({"http_proxy": "http://p", "https_proxy": "http://p", "no_proxy": "n"})
            self.assertEqual(os.environ["https_proxy"], "http://p")
            self.assertNotIn("HTTPS_PROXY", os.environ)
            te.apply_proxy({})
            self.assertNotIn("https_proxy", os.environ)
            self.assertNotIn("no_proxy", os.environ)


# --------------------------------------------------------------------------- #
# Enchaînement token -> API key contre le faux Vault
# --------------------------------------------------------------------------- #

class ResolveApiKeyTest(VaultServerTest):
    def _client(self, args):
        return te.VaultClient(args.vault_url, args.namespace, args.timeout)

    def test_token_from_cli_reads_key_and_caches(self):
        args = _args(self.url, "--vault-token", TOKEN_OK)
        key = te.resolve_api_key(args, self._client(args), acquire=lambda url: self.fail("navigateur inattendu"))
        self.assertEqual(key, API_KEY)
        paths = [p for p, _ in FakeVault.requests]
        self.assertEqual(paths, ["/v1/auth/token/lookup-self", f"/v1/{SECRET_PATH}"])
        self.assertEqual(FakeVault.requests[1][1]["x-vault-namespace"], "AP85135")
        self.assertEqual(te.load_cached_token(self.url), TOKEN_OK)
        self.assertEqual(te.load_cached_api_key(self.url, SECRET_PATH), API_KEY)

    def test_cached_key_reused_without_any_call(self):
        te.save_cached_api_key(self.url, SECRET_PATH, API_KEY, 7200)
        args = _args(self.url)
        key = te.resolve_api_key(args, self._client(args), acquire=lambda url: self.fail("navigateur inattendu"))
        self.assertEqual(key, API_KEY)
        self.assertEqual(FakeVault.requests, [])

    def test_expired_key_is_reread_with_cached_token(self):
        te.save_cached_api_key(self.url, SECRET_PATH, "old", 10)
        te.save_cached_token(self.url, TOKEN_OK)
        args = _args(self.url)
        key = te.resolve_api_key(args, self._client(args), acquire=lambda url: self.fail("navigateur inattendu"))
        self.assertEqual(key, API_KEY)
        self.assertEqual([p for p, _ in FakeVault.requests],
                         ["/v1/auth/token/lookup-self", f"/v1/{SECRET_PATH}"])

    def test_no_token_uses_token_service(self):
        args = _args(self.url)
        acquire = mock.Mock(return_value="")
        key = te.resolve_api_key(args, self._client(args), acquire)
        self.assertEqual(key, API_KEY)
        acquire.assert_not_called()
        self.assertEqual([p for p, _ in FakeVault.requests],
                         [f"/v1/token/{UID}?namespace=AP85135", "/v1/auth/token/lookup-self", f"/v1/{SECRET_PATH}"])
        self.assertEqual(te.load_cached_token(self.url), TOKEN_OK)

    def test_rejected_cached_token_uses_token_service(self):
        te.save_cached_token(self.url, TOKEN_BAD)
        args = _args(self.url)
        key = te.resolve_api_key(args, self._client(args), acquire=mock.Mock(return_value=""))
        self.assertEqual(key, API_KEY)
        self.assertEqual(te.load_cached_token(self.url), TOKEN_OK)

    def test_token_service_failure_falls_back_to_browser(self):
        args = _args(self.url, "--uid", "unknown")
        acquire = mock.Mock(return_value=f"X-Vault-Token:{TOKEN_OK}")
        key = te.resolve_api_key(args, self._client(args), acquire)
        self.assertEqual(key, API_KEY)
        acquire.assert_called_once_with(args.ui_url)

    def test_no_token_service_configured_uses_browser(self):
        args = _args(self.url, "--token-service", "")
        acquire = mock.Mock(return_value=TOKEN_OK)
        self.assertEqual(te.resolve_api_key(args, self._client(args), acquire), API_KEY)
        acquire.assert_called_once()
        self.assertFalse(any(p.startswith("/v1/token/") for p, _ in FakeVault.requests))

    def test_browser_token_flag_skips_token_service(self):
        args = _args(self.url, "--browser-token")
        acquire = mock.Mock(return_value=TOKEN_OK)
        te.resolve_api_key(args, self._client(args), acquire)
        acquire.assert_called_once()
        self.assertFalse(any(p.startswith("/v1/token/") for p, _ in FakeVault.requests))

    def test_service_token_helpers(self):
        self.assertEqual(te.token_service_url("https://s02:4430/", "la 1", "AP85135"),
                         "https://s02:4430/v1/token/la%201?namespace=AP85135")
        self.assertEqual(te.service_token(self.url, UID, "AP85135"), TOKEN_OK)
        with self.assertRaises(te.VaultError):
            te.service_token(self.url, "nobody", "AP85135")

    def test_resolve_uid_prompt_then_remembered(self):
        args = te.parse_args(["--no-proxy"])
        with mock.patch.object(sys.stdin, "isatty", return_value=True):
            self.assertEqual(te.resolve_uid(args, ask=lambda q: " lh90871 "), "lh90871")
        self.assertEqual(te.resolve_uid(args, ask=lambda q: self.fail("prompt")), "lh90871")
        self.assertEqual(te.resolve_uid(te.parse_args(["--uid", "x"]), ask=lambda q: self.fail("prompt")), "x")

    def test_new_key_ignores_cached_key_but_keeps_token(self):
        te.save_cached_api_key(self.url, SECRET_PATH, "old", 7200)
        te.save_cached_token(self.url, TOKEN_OK)
        args = _args(self.url, "--new-key")
        key = te.resolve_api_key(args, self._client(args), acquire=lambda url: self.fail("navigateur inattendu"))
        self.assertEqual(key, API_KEY)

    def test_new_token_ignores_everything_cached(self):
        te.save_cached_api_key(self.url, SECRET_PATH, "old", 7200)
        te.save_cached_token(self.url, TOKEN_OK)
        args = _args(self.url, "--new-token", "--token-service", "")
        acquire = mock.Mock(return_value=TOKEN_OK)
        self.assertEqual(te.resolve_api_key(args, self._client(args), acquire), API_KEY)
        acquire.assert_called_once()

    def test_short_ttl_token_is_refreshed(self):
        FakeVault.ttl = 30
        te.save_cached_token(self.url, TOKEN_OK)
        args = _args(self.url, "--token-service", "")
        acquire = mock.Mock(return_value=TOKEN_OK)
        with mock.patch.object(te, "TOKEN_MIN_VALIDITY", 60):
            with self.assertRaises(te.CliExit):  # le nouveau a aussi 30 s : refusé
                te.resolve_api_key(args, self._client(args), acquire)
        acquire.assert_called_once()

    def test_rejected_cli_token_is_usage_error(self):
        args = _args(self.url, "--vault-token", TOKEN_BAD)
        with self.assertRaises(te.CliExit) as ctx:
            te.resolve_api_key(args, self._client(args), acquire=mock.Mock(return_value=TOKEN_OK))
        self.assertEqual(ctx.exception.code, te.EXIT_USAGE)

    def test_no_token_acquired(self):
        args = _args(self.url, "--token-service", "")
        with self.assertRaises(te.CliExit) as ctx:
            te.resolve_api_key(args, self._client(args), acquire=lambda url: "")
        self.assertEqual(ctx.exception.code, te.EXIT_USAGE)

    def test_vault_unreachable(self):
        args = _args("http://127.0.0.1:1", "--vault-token", TOKEN_OK)
        with self.assertRaises(te.VaultError):
            te.resolve_api_key(args, self._client(args), acquire=lambda url: "")

    def test_secret_without_api_key(self):
        args = _args(self.url, "--vault-token", TOKEN_OK, "--secret-path", "nope")
        with self.assertRaises(te.CliExit) as ctx:
            te.resolve_api_key(args, self._client(args), acquire=lambda url: "")
        self.assertEqual(ctx.exception.code, te.EXIT_VAULT_FAILED)


class CacheTest(unittest.TestCase):
    def setUp(self) -> None:
        _fresh_cache()

    def test_api_key_expiry(self):
        te.save_cached_api_key("https://v", "p", "k", 1000)
        self.assertEqual(te.load_cached_api_key("https://v", "p"), "k")
        self.assertIsNone(te.load_cached_api_key("https://v", "p", now=time.time() + 950))
        te.save_cached_api_key("https://v", "p", "k2", None)
        self.assertEqual(te.load_cached_api_key("https://v", "p", now=time.time() + 10 ** 9), "k2")

    def test_token_roundtrip_and_forget(self):
        te.save_cached_token("https://v/", TOKEN_OK)
        self.assertEqual(te.load_cached_token("https://v"), TOKEN_OK)
        te.forget_cached_token("https://v")
        self.assertIsNone(te.load_cached_token("https://v"))
        if os.name == "posix":
            te.save_cached_token("https://v", TOKEN_OK)
            self.assertEqual(os.stat(te.state_path()).st_mode & 0o777, 0o600)


# --------------------------------------------------------------------------- #
# Navigateur : lecture du token dans les pages via DevTools (mocké)
# --------------------------------------------------------------------------- #

class BrowserTokenTest(unittest.TestCase):
    def _targets(self, *args, **kwargs):
        class Response:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *exc):
                pass

            def read(self_inner):
                return json.dumps([{"type": "page", "webSocketDebuggerUrl": "ws://127.0.0.1:1/a"},
                                   {"type": "service_worker", "webSocketDebuggerUrl": "ws://127.0.0.1:1/b"}]).encode()
        return Response()

    def test_find_token_in_pages(self):
        with mock.patch.object(te.urllib.request, "urlopen", side_effect=self._targets):
            evaluate = mock.Mock(return_value=TOKEN_OK)
            self.assertEqual(te._find_token_in_pages(1234, evaluate), TOKEN_OK)
            evaluate.assert_called_once_with("ws://127.0.0.1:1/a", te.TOKEN_FINDER_JS)
            self.assertEqual(te._find_token_in_pages(1234, mock.Mock(return_value="")), "")
            self.assertEqual(te._find_token_in_pages(1234, mock.Mock(return_value="not-a-token")), "")
            self.assertEqual(te._find_token_in_pages(1234, mock.Mock(side_effect=OSError)), "")

    def test_no_browser_falls_back_to_clipboard(self):
        with mock.patch.object(sys.stdin, "isatty", return_value=True):
            token = te.acquire_token_interactively(
                "https://v/ui/", read_clip=lambda: f"X-Vault-Token: {TOKEN_OK}", ask=lambda q: "",
                open_url=lambda url: None, auto_browser=lambda url: "")
        self.assertEqual(token, TOKEN_OK)

    def test_pasted_token_wins(self):
        with mock.patch.object(sys.stdin, "isatty", return_value=True):
            token = te.acquire_token_interactively(
                "https://v/ui/", read_clip=lambda: "", ask=lambda q: TOKEN_OK,
                open_url=lambda url: None, auto=False)
        self.assertEqual(token, TOKEN_OK)

    def test_non_interactive(self):
        with mock.patch.object(sys.stdin, "isatty", return_value=False):
            self.assertEqual(te.acquire_token_interactively("https://v/ui/", auto=False), "")


# --------------------------------------------------------------------------- #
# main : export / json / run
# --------------------------------------------------------------------------- #

class MainTest(VaultServerTest):
    def _run_main(self, *extra: str, run=None):
        run = run or mock.Mock(return_value=0)
        tests_dir = tempfile.mkdtemp(prefix="tfdir-")
        argv = ["--env", "int", "--vault-url", self.url, "--skip-login", "--dir", tests_dir,
                "--vault-token", TOKEN_OK, "--no-proxy", *extra]
        import io
        out = io.StringIO()
        with mock.patch.object(te, "run_terraform", run), mock.patch.object(sys, "stdout", out):
            code = te.main(argv)
        return code, out.getvalue(), run, tests_dir

    def test_exports(self):
        code, out, run, tests_dir = self._run_main()
        self.assertEqual(code, 0)
        self.assertIn(f"export IBM_CLOUD_API_KEY={API_KEY}\n", out)
        self.assertIn(f"export ORCHESTRATOR_IBMCLOUD_API_KEY={API_KEY}\n", out)
        run.assert_called_once_with(["init"], tests_dir)  # init sur stderr, stdout = exports seuls
        self.assertTrue(all(line.startswith("export ") for line in out.splitlines()))

    def test_json_and_options(self):
        code, out, _, _ = self._run_main("--json", "--tf-log", "--skip-init")
        self.assertEqual(code, 0)
        variables = json.loads(out)
        self.assertEqual(variables["TF_LOG"], "debug")
        self.assertNotIn("http_proxy", variables)

    def test_proxy_exported_and_used_for_vault(self):
        tests_dir = tempfile.mkdtemp(prefix="tfdir-")
        argv = ["--env", "int", "--vault-url", self.url, "--skip-login", "--skip-init", "--dir", tests_dir,
                "--vault-token", TOKEN_OK, "--proxy", "127.0.0.1:9", "--proxy-user", "u", "--proxy-password", "p!",
                "--no-proxy-hosts", "127.0.0.1", "--skip-proxy-check", "--json"]
        import io
        out = io.StringIO()
        with mock.patch.object(sys, "stdout", out), mock.patch.dict(os.environ, {}, clear=False):
            code = te.main(argv)
        self.assertEqual(code, 0)  # no_proxy=127.0.0.1 : le faux Vault est joint sans passer par le proxy
        variables = json.loads(out.getvalue())
        self.assertEqual(variables["https_proxy"], "http://u:p%21@127.0.0.1:9")
        self.assertEqual(variables["HTTP_PROXY"], "http://u:p%21@127.0.0.1:9")
        self.assertEqual(variables["no_proxy"], "127.0.0.1,s02vl9956141")  # + hôte du service token

    def test_run_plan(self):
        run = mock.Mock(return_value=5)
        code, out, run, tests_dir = self._run_main("--skip-init", "--run", "plan", "--", "-no-color", run=run)
        self.assertEqual(code, 5)
        self.assertEqual(out, "")
        self.assertEqual(run.call_args.args[0], ["plan", "-no-color"])
        self.assertEqual(run.call_args.args[1], tests_dir)
        self.assertEqual(run.call_args.args[2]["IBM_CLOUD_API_KEY"], API_KEY)
        self.assertFalse(run.call_args.kwargs["to_stderr"])

    def test_missing_dir(self):
        with mock.patch.object(te, "run_terraform", mock.Mock(return_value=0)):
            code = te.main(["--env", "int", "--vault-url", self.url, "--skip-login", "--dir", "/nope/nope",
                            "--vault-token", TOKEN_OK, "--no-proxy"])
        self.assertEqual(code, te.EXIT_USAGE)

    def test_forget(self):
        te.save_cached_token(self.url, TOKEN_OK)
        self.assertEqual(te.main(["--forget"]), 0)
        self.assertIsNone(te.load_cached_token(self.url))


class InteractiveTest(unittest.TestCase):
    def test_menu_builds_argv(self):
        answers = iter(["1", "1", "2", "2", "2"])  # int, plan, nouveau token, sans proxy, TF_LOG
        argv = te.interactive_argv(ask=lambda q: next(answers))
        self.assertEqual(argv, ["--new-token", "--no-proxy", "--tf-log", "--run", "plan"])
        args = te.parse_args(argv)
        self.assertEqual(args.run, ["plan"])
        self.assertTrue(args.new_token)

    def test_quit(self):
        answers = iter(["4", "5"])  # prod, quitter
        self.assertIsNone(te.interactive_argv(ask=lambda q: next(answers)))


if __name__ == "__main__":
    unittest.main()
