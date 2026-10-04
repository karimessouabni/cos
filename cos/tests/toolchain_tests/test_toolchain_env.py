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
# Jamais le vrai trousseau macOS : sans `security`, les secrets restent en mémoire du processus.
mock.patch.object(te, "_keychain", side_effect=lambda *args: None).start()

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
        if self.path.startswith("http://"):  # requête reçue en tant que proxy HTTP
            self.path = "/" + self.path.split("/", 3)[3]
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

    def test_tofu_credentials_and_source(self):
        with tempfile.TemporaryDirectory() as home, mock.patch.object(te.os.path, "expanduser", return_value=home):
            paths = te.terraform_credentials_paths()
            self.assertEqual([os.path.basename(p) for p in paths], ["credentials.tofurc.json", "credentials.tfrc.json"])
            os.makedirs(os.path.dirname(paths[0]))
            self.assertFalse(te.terraform_logged_in(te.TERRAFORM_HOST, {}))
            with open(paths[0], "w") as fh:
                json.dump({"credentials": {te.TERRAFORM_HOST: {"token": "t"}}}, fh)
            self.assertTrue(te.terraform_logged_in(te.TERRAFORM_HOST, {}))
        root = tempfile.mkdtemp(prefix="tfroot-")
        os.makedirs(os.path.join(root, "envs"))
        open(os.path.join(root, "envs", "int.tfvars"), "w").write('provider_version = "2.3.0-int"\n')
        te.write_versions_tf(root, "int")
        self.assertIn('source  = "registry.terraform.io/bp2i/orchestrator"', open(os.path.join(root, "versions.tf")).read())

    def test_credentials_file(self):
        with tempfile.TemporaryDirectory() as home, mock.patch.object(
                te, "terraform_credentials_paths", return_value=[os.path.join(home, "credentials.tfrc.json")]):
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
        self.assertTrue(args.dir.endswith(os.sep + te.TERRAFORM_ROOT_DIR))  # root terraform test

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

    def test_password_remembered_after_check(self):
        with mock.patch.object(te, "_keychain", return_value=None), \
                mock.patch.object(sys.stdin, "isatty", return_value=True), mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("https_proxy", None)
            os.environ.pop("HTTPS_PROXY", None)
            asked = []
            proxy = te.resolve_and_check_proxy(te.parse_args([]), ask=lambda q: "h90871",
                                               ask_secret=lambda q: (asked.append(q), "pw1")[1], check=lambda pv: "")
            self.assertEqual(proxy["https_proxy"], f"http://h90871:pw1@{te.DEFAULT_PROXY}")
            self.assertEqual(len(asked), 1)
            self.assertEqual(te.load_proxy_password("h90871"), "pw1")
            # second lancement : rien n'est redemandé
            proxy = te.resolve_and_check_proxy(te.parse_args([]), ask=lambda q: self.fail("prompt"),
                                               ask_secret=lambda q: self.fail("prompt"), check=lambda pv: "")
            self.assertEqual(proxy["https_proxy"], f"http://h90871:pw1@{te.DEFAULT_PROXY}")
            # mot de passe mémorisé refusé : oublié, redemandé, re-mémorisé
            checks = iter(["identifiants refusés (HTTP 407)", ""])
            proxy = te.resolve_and_check_proxy(te.parse_args([]), ask=lambda q: "",
                                               ask_secret=lambda q: "pw2", check=lambda pv: next(checks))
            self.assertEqual(proxy["https_proxy"], f"http://h90871:pw2@{te.DEFAULT_PROXY}")
            self.assertEqual(te.load_proxy_password("h90871"), "pw2")
            # --new-proxy-password force la saisie ; --forget-proxy-password oublie
            proxy = te.resolve_and_check_proxy(te.parse_args(["--new-proxy-password"]), ask=lambda q: "",
                                               ask_secret=lambda q: "pw3", check=lambda pv: "")
            self.assertEqual(te.load_proxy_password("h90871"), "pw3")
            self.assertEqual(te.main(["--forget-proxy-password"]), 0)
            self.assertEqual(te.load_proxy_password("h90871"), "")
            # un mot de passe non vérifié (check sauté) n'est pas mémorisé
            te.resolve_and_check_proxy(te.parse_args(["--skip-proxy-check"]), ask=lambda q: "",
                                       ask_secret=lambda q: "pw4", check=lambda pv: self.fail("check"))
            self.assertEqual(te.load_proxy_password("h90871"), "")

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

    def _opener(self, side_effect):
        opener = mock.Mock()
        opener.open = mock.Mock(side_effect=side_effect)
        return mock.patch.object(te.urllib.request, "build_opener", return_value=opener)

    def test_check_proxy(self):
        proxy = {"http_proxy": "http://u:p@px:1", "https_proxy": "http://u:p@px:1", "no_proxy": ""}
        import urllib.error
        self.assertEqual(te.check_proxy({}), "")  # aucun proxy : rien à vérifier
        with self._opener(urllib.error.HTTPError("u", 407, "authenticationrequired", {}, None)):
            self.assertIn("identifiants refusés", te.check_proxy(proxy))
        with self._opener(urllib.error.HTTPError("u", 405, "Method Not Allowed", {}, None)):
            self.assertEqual(te.check_proxy(proxy), "")  # le proxy laisse passer
        with self._opener(urllib.error.URLError("Tunnel connection failed: 503 Service Unavailable")):
            self.assertIn("503", te.check_proxy(proxy))
        with self._opener(urllib.error.URLError("Tunnel connection failed: 407 authenticationrequired")):
            self.assertIn("identifiants refusés", te.check_proxy(proxy))

    def test_dead_shell_proxy_falls_back_to_corporate_proxy(self):
        environ = {"https_proxy": "http://127.0.0.1:8079"}
        checks = {"http://127.0.0.1:8079": "Tunnel connection failed: 503 Service Unavailable"}
        with mock.patch.dict(os.environ, environ, clear=False), mock.patch.object(sys.stdin, "isatty", return_value=True):
            proxy = te.resolve_and_check_proxy(te.parse_args([]), ask=lambda q: "h90871", ask_secret=lambda q: "pw",
                                               check=lambda pv: checks.get(pv["https_proxy"], ""))
        self.assertEqual(proxy["https_proxy"], f"http://h90871:pw@{te.DEFAULT_PROXY}")

    def test_shell_proxy_kept_when_it_works(self):
        with mock.patch.dict(os.environ, {"https_proxy": "http://ok:1"}, clear=False):
            proxy = te.resolve_and_check_proxy(te.parse_args([]), ask=lambda q: self.fail("prompt"),
                                               ask_secret=lambda q: self.fail("prompt"), check=lambda pv: "")
        self.assertEqual(proxy["https_proxy"], "http://ok:1")

    def test_rejected_credentials_stop(self):
        args = te.parse_args(["--proxy-user", "u", "--proxy-password", "p"])
        with mock.patch.dict(os.environ, {}, clear=False), self.assertRaises(te.CliExit) as ctx:
            os.environ.pop("https_proxy", None)
            os.environ.pop("HTTPS_PROXY", None)
            te.resolve_and_check_proxy(args, check=lambda pv: "identifiants refusés (HTTP 407)")
        self.assertIn("identifiants refusés", str(ctx.exception))
        self.assertNotIn(":p@", str(ctx.exception))

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

    def test_service_token_falls_back_to_proxy_route(self):
        # direct injoignable (port fermé), puis via le "proxy" qui est en fait le faux serveur :
        # le GET arrive bien et renvoie le token.
        with mock.patch.dict(os.environ, {"https_proxy": self.url}, clear=False):
            self.assertEqual(te.service_token("http://127.0.0.1:1", UID, "AP85135", timeout=5), TOKEN_OK)
        self.assertEqual(FakeVault.requests[-1][0], f"/v1/token/{UID}?namespace=AP85135")
        # ni direct ni proxy : message qui liste les deux essais
        with mock.patch.dict(os.environ, {"https_proxy": "http://127.0.0.1:1"}, clear=False), \
                self.assertRaises(te.VaultError) as ctx:
            te.service_token("http://127.0.0.1:1", UID, "AP85135", timeout=5)
        self.assertIn("injoignable", str(ctx.exception))
        self.assertIn("direct", str(ctx.exception))
        self.assertIn("via le proxy", str(ctx.exception))
        self.assertIn("Sondage TCP", str(ctx.exception))

    def test_ipv4_only_and_tcp_probe(self):
        import socket
        v6 = (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", 1, 0, 0))
        v4 = (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 1))
        with mock.patch.object(socket, "getaddrinfo", return_value=[v6, v4]):
            with te._ipv4_only():
                self.assertEqual(socket.getaddrinfo("h", 1), [v4])
            self.assertEqual(socket.getaddrinfo("h", 1), [v6, v4])
        port = self.server.server_port
        lines = te.tcp_probe("127.0.0.1", port, timeout=2)
        self.assertEqual(lines, [f"IPv4 127.0.0.1:{port} OK"])
        self.assertIn("refused", te.tcp_probe("127.0.0.1", 1, timeout=2)[0].lower()
                      + " connection refused")  # port fermé : erreur, pas OK
        self.assertNotIn("OK", te.tcp_probe("127.0.0.1", 1, timeout=2)[0])

    def test_service_token_ipv6_then_ipv4(self):
        # le nom résout en IPv6 (injoignable) puis IPv4 (le faux serveur) : la
        # route "IPv4 seulement" aboutit.
        import socket
        port = self.server.server_port
        original = socket.getaddrinfo

        def fake(host, *args, **kwargs):
            if host == "svc":
                return [(socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", 1, 0, 0)),
                        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))]
            return original(host, *args, **kwargs)
        with mock.patch.object(socket, "getaddrinfo", side_effect=fake), \
                mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("https_proxy", None)
            self.assertEqual(te.service_token(f"http://svc:{port}", UID, "AP85135", timeout=5), TOKEN_OK)

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

class FakeKeychain:
    """Doublure de `security` : find/add/delete-generic-password et find-certificate."""

    def __init__(self, certificates: str = "", broken: bool = False):
        self.items: dict[tuple[str, str], str] = {}
        self.certificates = certificates
        self.broken = broken

    def __call__(self, *args: str) -> te.subprocess.CompletedProcess:
        def done(code: int, out: str = "", err: str = "") -> te.subprocess.CompletedProcess:
            return te.subprocess.CompletedProcess(["security", *args], code, out, err)
        command, options = args[0], dict(zip(args[1::2], args[2::2]))
        if command == "find-certificate":
            return done(0, self.certificates) if self.certificates else done(44, "", "no certificates")
        if self.broken:
            return done(36, "", "keychain locked")
        key = (options.get("-a", ""), options.get("-s", ""))
        if command == "add-generic-password":
            self.items[key] = options["-w"]
            return done(0)
        if command == "find-generic-password":
            return done(0, self.items[key] + "\n") if key in self.items else done(44, "", "not found")
        if command == "delete-generic-password":
            return done(0 if self.items.pop(key, None) is not None else 44)
        return done(0)


class SecretStoreTest(unittest.TestCase):
    """Les secrets vont dans le trousseau, jamais en clair dans state.json."""

    def setUp(self) -> None:
        te.forget_all()

    def test_without_keychain_secrets_live_in_memory_only(self):
        store = te.SecretStore(keychain=lambda *args: None)
        self.assertFalse(store.persistent)
        self.assertFalse(store.save("vault-token:https://v", TOKEN_OK))
        self.assertEqual(store.load("vault-token:https://v"), TOKEN_OK)
        self.assertNotIn(TOKEN_OK, json.dumps(te._read_state()))
        self.assertEqual(te._read_state()["secrets"], ["vault-token:https://v"])
        store.forget("vault-token:https://v")
        self.assertEqual(store.load("vault-token:https://v"), "")
        self.assertEqual(te._read_state()["secrets"], [])

    def test_with_keychain_secrets_go_to_the_keychain(self):
        keychain = FakeKeychain()
        store = te.SecretStore(keychain=keychain)
        self.assertTrue(store.persistent)
        self.assertTrue(store.save("proxy:h90871", "Pass!"))
        self.assertEqual(keychain.items, {("proxy:h90871", te.KEYCHAIN_SERVICE): "Pass!"})
        self.assertEqual(store.load("proxy:h90871"), "Pass!")
        self.assertNotIn("Pass!", json.dumps(te._read_state()))
        store.forget_all()
        self.assertEqual(keychain.items, {})
        self.assertEqual(store.load("proxy:h90871"), "")

    def test_locked_keychain_falls_back_to_memory(self):
        keychain = FakeKeychain(broken=True)
        store = te.SecretStore(keychain=keychain)
        with mock.patch.object(te, "_log") as log:
            self.assertFalse(store.save("api-key:k", API_KEY))
        self.assertIn("mémoire seulement", log.call_args.args[0])
        self.assertEqual(store.load("api-key:k"), "")  # le trousseau cassé répond "non trouvé"
        self.assertEqual(store._memory["api-key:k"], API_KEY)

    def test_module_store_follows_the_patched_keychain(self):
        keychain = FakeKeychain()
        with mock.patch.object(te, "_keychain", side_effect=keychain):
            te.save_cached_token("https://v", TOKEN_OK)
            self.assertIn(("vault-token:https://v", te.KEYCHAIN_SERVICE), keychain.items)
            self.assertEqual(te.load_cached_token("https://v"), TOKEN_OK)
            te.save_cached_api_key("https://v", "p", API_KEY, 1000)
            self.assertEqual(te.load_cached_api_key("https://v", "p"), API_KEY)
            self.assertEqual(set(te._read_state()["api_key_leases"]), {"https://v/v1/p"})
            self.assertNotIn(API_KEY, json.dumps(te._read_state()))
            te.save_proxy_password("h90871", "pw")
            self.assertEqual(te.load_proxy_password("h90871"), "pw")
            te.forget_all()
            self.assertEqual(keychain.items, {})
            self.assertFalse(os.path.exists(te.state_path()))

    def test_legacy_plaintext_secrets_are_purged_on_read(self):
        os.makedirs(os.path.dirname(te.state_path()), exist_ok=True)
        with open(te.state_path(), "w", encoding="utf-8") as fh:
            json.dump({"settings": {"proxy_user": "h90871"}, "vault_tokens": {"https://v": TOKEN_OK},
                       "api_keys": {"k": {"api_key": API_KEY}}, "proxy_passwords": {"h90871": "pw"}}, fh)
        with mock.patch.object(te, "_log") as log:
            state = te._read_state()
        self.assertEqual(state, {"settings": {"proxy_user": "h90871"}})
        with open(te.state_path(), encoding="utf-8") as fh:
            on_disk = fh.read()
        for secret in (TOKEN_OK, API_KEY, "pw"):
            self.assertNotIn(secret, on_disk)
        self.assertIn("purgés", log.call_args.args[0])
        self.assertIsNone(te.load_cached_token("https://v"))


def _first_pem_certificate() -> str:
    """Un certificat PEM du magasin système (pour tester le chargement de CA)."""
    cafile = te.ssl.get_default_verify_paths().cafile
    if not cafile or not os.path.exists(cafile):
        return ""
    with open(cafile, encoding="utf-8", errors="ignore") as fh:
        content = fh.read()
    start = content.find("-----BEGIN CERTIFICATE-----")
    end = content.find("-----END CERTIFICATE-----", start)
    return content[start:end + len("-----END CERTIFICATE-----")] + "\n" if start >= 0 and end > 0 else ""


class TlsContextTest(unittest.TestCase):
    """La vérification TLS n'est jamais désactivée ; les CA internes s'ajoutent."""

    def assert_verifying(self, context: te.ssl.SSLContext) -> None:
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, te.ssl.CERT_REQUIRED)

    def test_default_context_verifies(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(te.CA_BUNDLE_ENV, None)
            os.environ.pop("SSL_CERT_FILE", None)
            self.assert_verifying(te.tls_context())
            self.assert_verifying(te.VaultClient("https://v", "ns")._context)

    def test_bundle_precedence(self):
        self.assertEqual(te.ca_bundle_path("explicit.pem", {te.CA_BUNDLE_ENV: "env.pem", "SSL_CERT_FILE": "ssl.pem"}),
                         "explicit.pem")
        self.assertEqual(te.ca_bundle_path(None, {te.CA_BUNDLE_ENV: "env.pem", "SSL_CERT_FILE": "ssl.pem"}), "env.pem")
        self.assertEqual(te.ca_bundle_path(None, {"SSL_CERT_FILE": "ssl.pem"}), "ssl.pem")
        self.assertEqual(te.ca_bundle_path(None, {}), "")

    def test_bundle_is_loaded_and_still_verifies(self):
        pem = _first_pem_certificate()
        if not pem:
            self.skipTest("pas de magasin de CA système lisible")
        with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as fh:
            fh.write(pem)
        with mock.patch.dict(os.environ, {te.CA_BUNDLE_ENV: fh.name}):
            context = te.tls_context()
        self.assert_verifying(context)
        self.assertGreaterEqual(context.cert_store_stats()["x509"], 1)

    def test_missing_bundle_is_an_error_not_a_bypass(self):
        with self.assertRaises(OSError):
            te.tls_context(os.path.join(tempfile.mkdtemp(), "absent.pem"))

    def test_macos_keychain_cas_are_added(self):
        pem = _first_pem_certificate()
        if not pem:
            self.skipTest("pas de magasin de CA système lisible")
        with mock.patch.object(te, "_keychain", side_effect=FakeKeychain(certificates=pem)):
            self.assertIn("BEGIN CERTIFICATE", te.macos_keychain_cas())
            context = te.tls_context()
        self.assert_verifying(context)
        self.assertGreaterEqual(context.cert_store_stats()["x509"], 1)
        with mock.patch.object(te, "_keychain", side_effect=FakeKeychain()):
            self.assertEqual(te.macos_keychain_cas(), "")

    def test_unreadable_keychain_cas_are_ignored_with_a_log(self):
        with mock.patch.object(te, "_keychain", side_effect=FakeKeychain(certificates="-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----\n")), \
                mock.patch.object(te, "_log") as log:
            self.assert_verifying(te.tls_context())
        self.assertIn("--ca-bundle", log.call_args.args[0])

    def test_ca_bundle_argument(self):
        self.assertEqual(te.parse_args(["--ca-bundle", "/tmp/ca.pem"]).ca_bundle, "/tmp/ca.pem")
        self.assertIsNone(te.parse_args([]).ca_bundle)
        with self.assertRaises(SystemExit):
            te.parse_args(["--verify-tls"])


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

    def test_browser_is_launched_without_ignoring_certificate_errors(self):
        class Process:
            def poll(self):
                return None

            def terminate(self):
                pass

            def wait(self, timeout=None):
                pass

        with mock.patch.object(te.subprocess, "Popen", return_value=Process()) as popen, \
                mock.patch.object(te, "_devtools_port", side_effect=RuntimeError("no devtools")):
            self.assertEqual(te.browser_token("https://v/ui/", browser="/usr/bin/chrome", timeout=0.1), "")
        command = popen.call_args.args[0]
        self.assertEqual(command[0], "/usr/bin/chrome")
        self.assertIn("--incognito", command)
        self.assertNotIn("--ignore-certificate-errors", command)
        self.assertEqual(command[-1], "https://v/ui/")

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
        journal = run.call_args.args[2]["TF_LOG_PATH"]  # le journal de tofu part dans un fichier de logs/
        self.assertEqual(os.path.dirname(journal), os.path.join(tests_dir, "logs"))
        self.assertTrue(journal.endswith("-int-plan.log"))
        self.assertEqual(run.call_args.args[2]["TF_LOG"], "debug")

    def test_run_without_log_file(self):
        code, _, run, tests_dir = self._run_main("--skip-init", "--no-log-file", "--tf-log", "trace", "--run", "plan")
        self.assertEqual(code, 0)
        self.assertNotIn("TF_LOG_PATH", run.call_args.args[2])
        self.assertEqual(run.call_args.args[2]["TF_LOG"], "trace")  # comme avant : sur le terminal
        self.assertFalse(os.path.exists(os.path.join(tests_dir, "logs")))

    def test_run_test_in_terraform_root(self):
        root = tempfile.mkdtemp(prefix="tfroot-")
        os.makedirs(os.path.join(root, "envs"))
        with open(os.path.join(root, "envs", "int.tfvars"), "w") as fh:
            fh.write('provider_version = "2.3.0-int"\n')
        run = mock.Mock(return_value=0)
        with mock.patch.object(te, "run_terraform", run), mock.patch.dict(os.environ, {}, clear=False), \
                mock.patch.object(te, "terraform_version", return_value=(1, 9, 8)):
            code = te.main(["--env", "int", "--vault-url", self.url, "--skip-login", "--dir", root, "--no-proxy",
                            "--vault-token", TOKEN_OK, "--prefix", "h90871", "--run", "test", "--", "-verbose"])
        self.assertEqual(code, 0)
        self.assertEqual(run.call_args_list[0].args[0], ["init"])  # versions.tf écrit puis init
        self.assertIn('version = "2.3.0-int"', open(os.path.join(root, "versions.tf")).read())
        self.assertEqual(run.call_args_list[1].args[0], ["test", "-var-file=envs/int.tfvars", "-verbose"])
        self.assertEqual(run.call_args_list[1].args[2]["TF_VAR_prefix"], "h90871")

    def test_missing_dir(self):
        with mock.patch.object(te, "run_terraform", mock.Mock(return_value=0)):
            code = te.main(["--env", "int", "--vault-url", self.url, "--skip-login", "--dir", "/nope/nope",
                            "--vault-token", TOKEN_OK, "--no-proxy"])
        self.assertEqual(code, te.EXIT_USAGE)

    def test_probe(self):
        code = te.main(["--env", "int", "--no-proxy", "--token-service", self.url, "--uid", UID, "--probe"])
        self.assertEqual(code, 0)
        self.assertEqual(te.main(["--env", "int", "--no-proxy", "--token-service", "http://127.0.0.1:1",
                                  "--uid", UID, "--probe", "--timeout", "5"]), te.EXIT_VAULT_FAILED)

    def test_forget(self):
        te.save_cached_token(self.url, TOKEN_OK)
        self.assertEqual(te.main(["--forget"]), 0)
        self.assertIsNone(te.load_cached_token(self.url))


class JournalTest(unittest.TestCase):
    LINES = [
        "2026-10-02T12:00:01.123+0200 [INFO]  Starting apply for orchestrator_subscription_cos_v1.cos",
        "2026-10-02T12:00:02.000+0200 [DEBUG] provider.terraform-provider-orchestrator: GET /subscriptions/1",
        "  suite du message debug",
        "2026-10-02T12:00:03.000+0200 [ERROR] provider.terraform-provider-orchestrator: subscription failed",
        "  détail de l'erreur",
        "",
    ]

    def test_new_journal_is_private_and_old_ones_are_pruned(self):
        cwd = tempfile.mkdtemp(prefix="tfdir-")
        paths = [te.new_journal(cwd, "int", "test", now=1_800_000_000 + i) for i in range(te.LOGS_KEPT + 2)]
        self.assertEqual(os.stat(paths[-1]).st_mode & 0o777, 0o600)
        self.assertTrue(paths[-1].endswith("-int-test.log"))
        kept = sorted(os.listdir(os.path.join(cwd, "logs")))
        self.assertEqual(kept, [os.path.basename(p) for p in paths[2:]])

    def test_journal_level(self):
        self.assertEqual(te.journal_level(None, "info"), "debug")
        self.assertEqual(te.journal_level("error", "info"), "info")  # sinon rien à suivre
        self.assertEqual(te.journal_level("trace", "info"), "trace")
        self.assertEqual(te.journal_level("error", "off"), "error")

    def test_filter_keeps_level_and_its_continuation_lines(self):
        journal = te.JournalFilter("info")
        self.assertEqual([journal.shown(line) for line in self.LINES], [
            "12:00:01 [INFO]  Starting apply for orchestrator_subscription_cos_v1.cos",
            None, None,
            "12:00:03 [ERROR] provider.terraform-provider-orchestrator: subscription failed",
            "  détail de l'erreur",
            None,
        ])
        self.assertEqual(te.JournalFilter("debug").shown(self.LINES[2]), None)  # suite sans message avant
        self.assertEqual(te.JournalFilter("warn").shown("x [WARN] sans horodatage ISO"), "x [WARN] sans horodatage ISO")

    def test_follow_reads_what_is_written_while_running_and_after_stop(self):
        path = te.new_journal(tempfile.mkdtemp(prefix="tfdir-"), "int", "test")
        stop, seen = threading.Event(), []
        follower = threading.Thread(target=te.follow_journal, args=(path, "info", stop, seen.append, 0.01))
        follower.start()
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(self.LINES[0] + "\n" + self.LINES[1][:20])  # ligne coupée au milieu d'une écriture
            fh.flush()
            deadline = time.time() + 5
            while not seen and time.time() < deadline:
                time.sleep(0.01)
            self.assertEqual(seen, ["  | 12:00:01 [INFO]  Starting apply for orchestrator_subscription_cos_v1.cos"])
            fh.write(self.LINES[1][20:] + "\n" + self.LINES[3])  # dernière ligne sans retour à la ligne
        stop.set()
        follower.join(timeout=5)
        self.assertFalse(follower.is_alive())
        self.assertEqual(seen[1:], ["  | 12:00:03 [ERROR] provider.terraform-provider-orchestrator: subscription failed"])

    def test_run_with_journal_follows_and_returns_the_exit_code(self):
        cwd = tempfile.mkdtemp(prefix="tfdir-")

        def fake_tofu(command, cwd, env, to_stderr):
            with open(env["TF_LOG_PATH"], "a", encoding="utf-8") as fh:
                fh.write("\n".join(self.LINES))
            return 7

        shown = []
        with mock.patch.object(te, "_log", shown.append):
            code = te.run_with_journal(["test"], cwd, {"A": "b"}, None, "info", "int", run=fake_tofu)
        self.assertEqual(code, 7)
        self.assertEqual([line for line in shown if line.startswith("  | ")], [
            "  | 12:00:01 [INFO]  Starting apply for orchestrator_subscription_cos_v1.cos",
            "  | 12:00:03 [ERROR] provider.terraform-provider-orchestrator: subscription failed",
            "  |   détail de l'erreur",
        ])
        self.assertIn("Journal complet : ", shown[-1])

    def test_follow_off_only_writes_the_file(self):
        shown = []
        run = mock.Mock(return_value=0)
        with mock.patch.object(te, "_log", shown.append):
            te.run_with_journal(["plan"], tempfile.mkdtemp(prefix="tfdir-"), {}, "info", "off", "int", run=run)
        self.assertEqual(run.call_args.args[2]["TF_LOG"], "info")
        self.assertIn("tail -f", shown[0])


class InteractiveTest(unittest.TestCase):
    def test_menu_builds_argv(self):
        answers = iter(["1", "3", "2", "2", "2"])  # int, plan, nouveau token, sans proxy, détail en direct
        argv = te.interactive_argv(ask=lambda q: next(answers))
        self.assertEqual(argv, ["--new-token", "--no-proxy", "--follow", "debug", "--run", "plan"])
        args = te.parse_args(argv)
        self.assertEqual(args.run, ["plan"])
        self.assertTrue(args.new_token)

    def test_quit(self):
        answers = iter(["4", "7"])  # prod, quitter
        self.assertIsNone(te.interactive_argv(ask=lambda q: next(answers)))

    def test_menu_single_scenario(self):
        answers = iter(["1", "2", "1", "2", "1", "30_bucket_retention.tftest.hcl"])
        argv = te.interactive_argv(ask=lambda q: next(answers))
        self.assertEqual(argv, ["--no-proxy", "--run", "test", "--", "-filter=tests/30_bucket_retention.tftest.hcl"])
        self.assertEqual(te.parse_args(argv).run, ["test", "-filter=tests/30_bucket_retention.tftest.hcl"])


class TerraformRootTest(unittest.TestCase):
    """Nouvelle arborescence : terraform/ + envs/<env>.tfvars + versions.tf généré."""

    def setUp(self) -> None:
        self.root = tempfile.mkdtemp(prefix="tfroot-")
        os.makedirs(os.path.join(self.root, "envs"))
        with open(os.path.join(self.root, "envs", "int.tfvars"), "w") as fh:
            fh.write('environment      = "int"\nrealm = "rl1"\nprovider_version = "2.3.0-int" # commentaire\n')

    def test_tfvars_value(self):
        path = os.path.join(self.root, "envs", "int.tfvars")
        self.assertEqual(te.tfvars_value(path, "provider_version"), "2.3.0-int")
        self.assertEqual(te.tfvars_value(path, "realm"), "rl1")
        self.assertIsNone(te.tfvars_value(path, "absent"))
        self.assertIsNone(te.tfvars_value("/nope", "x"))

    def test_write_versions_tf(self):
        self.assertEqual(te.write_versions_tf(self.root, "int"), "2.3.0-int")
        content = open(os.path.join(self.root, "versions.tf")).read()
        self.assertIn('version = "2.3.0-int"', content)
        self.assertIn('source  = "registry.terraform.io/bp2i/orchestrator"', content)
        self.assertEqual(te.tfvars_value(os.path.join(self.root, "versions.tf"), "version"), "2.3.0-int")
        self.assertEqual(te.write_versions_tf(self.root, "int"), "2.3.0-int")  # inchangé : pas de réécriture
        self.assertIsNone(te.write_versions_tf(self.root, "pprod"))  # pas de tfvars : ancienne arborescence
        with open(os.path.join(self.root, "envs", "qual.tfvars"), "w") as fh:
            fh.write('environment = "qual"\n')
        with self.assertRaises(te.CliExit):
            te.write_versions_tf(self.root, "qual")

    def test_with_var_file(self):
        self.assertEqual(te.with_var_file(["plan"], self.root, "int"), ["plan", "-var-file=envs/int.tfvars"])
        self.assertEqual(te.with_var_file(["test", "-filter=tests/a.tftest.hcl"], self.root, "int"),
                         ["test", "-var-file=envs/int.tfvars", "-filter=tests/a.tftest.hcl"])
        self.assertEqual(te.with_var_file(["plan", "-var-file=x"], self.root, "int"), ["plan", "-var-file=x"])
        self.assertEqual(te.with_var_file(["init"], self.root, "int"), ["init"])
        self.assertEqual(te.with_var_file(["plan"], self.root, "pprod"), ["plan"])  # pas de tfvars

    def test_env_dir_singular_accepted(self):
        root = tempfile.mkdtemp(prefix="tfroot-")
        os.makedirs(os.path.join(root, "env"))
        open(os.path.join(root, "env", "int.tfvars"), "w").write('provider_version = "x"\n')
        self.assertEqual(te.with_var_file(["plan"], root, "int"), ["plan", "-var-file=env/int.tfvars"])

    def test_adapt_test_filter(self):
        os.makedirs(os.path.join(self.root, "tests"))
        open(os.path.join(self.root, "tests", "a.tftest.hcl"), "w").write("")
        cmd = ["test", "-var-file=envs/int.tfvars", "-filter=tests/a.tftest.hcl", "-verbose"]
        self.assertEqual(te.adapt_test_filter(cmd, self.root, (1, 9, 8)), cmd)  # >= 1.7 : inchangé
        self.assertEqual(te.adapt_test_filter(cmd, self.root, None), cmd)
        self.assertEqual(te.adapt_test_filter(["plan", "-filter=x"], self.root, (1, 6, 0)), ["plan", "-filter=x"])
        adapted = te.adapt_test_filter(cmd, self.root, (1, 6, 6))
        self.assertEqual(adapted, ["test", "-var-file=envs/int.tfvars", "-verbose", f"-test-directory={te.TEST_FILTER_DIR}"])
        link = os.path.join(self.root, te.TEST_FILTER_DIR, "a.tftest.hcl")
        self.assertTrue(os.path.islink(link) and os.path.isfile(link))
        with self.assertRaises(te.CliExit):
            te.adapt_test_filter(["test", "-filter=tests/nope.tftest.hcl"], self.root, (1, 6, 6))

    def test_normalize_test_filter_with_stray_characters(self):
        os.makedirs(os.path.join(self.root, "tests"))
        open(os.path.join(self.root, "tests", "a.tftest.hcl "), "w").write("")  # espace finale
        self.assertEqual(te.normalize_test_filter("tests/a.tftest.hcl", self.root), "tests/a.tftest.hcl ")

    def test_normalize_test_filter(self):
        os.makedirs(os.path.join(self.root, "tests"))
        open(os.path.join(self.root, "tests", "a.tftest.hcl"), "w").write("")
        base = os.path.basename(self.root)
        for given in ("tests/a.tftest.hcl", f"{base}/tests/a.tftest.hcl", "a.tftest.hcl",
                      os.path.join(self.root, "tests", "a.tftest.hcl"), f"x/y/{base}/tests/a.tftest.hcl"):
            self.assertEqual(te.normalize_test_filter(given, self.root), "tests/a.tftest.hcl", given)
        self.assertEqual(te.adapt_test_filter(["test", f"-filter={base}/tests/a.tftest.hcl"], self.root, (1, 9, 0)),
                         ["test", "-filter=tests/a.tftest.hcl"])
        with self.assertRaises(te.CliExit):
            te.normalize_test_filter("tests/zz.tftest.hcl", self.root)

    def test_terraform_version(self):
        with mock.patch.object(te.subprocess, "run", return_value=mock.Mock(stdout='{"terraform_version": "1.6.6"}')):
            self.assertEqual(te.terraform_version(), (1, 6, 6))
        with mock.patch.object(te.subprocess, "run", side_effect=OSError):
            self.assertIsNone(te.terraform_version())

    def test_default_dir_prefers_terraform_root(self):
        base = tempfile.mkdtemp(prefix="tcroot-")
        os.makedirs(os.path.join(base, "int", "new_version"))
        self.assertEqual(te.default_tests_dir("int", base), os.path.join(base, "int", "new_version"))
        os.makedirs(os.path.join(base, te.TERRAFORM_ROOT_DIR))
        self.assertEqual(te.default_tests_dir("int", base), os.path.join(base, te.TERRAFORM_ROOT_DIR))

    def test_init_upgrade_when_provider_version_changes(self):
        run = mock.Mock(return_value=0)
        os.mkdir(os.path.join(self.root, ".terraform"))
        te.ensure_terraform_init(self.root, False, run, version_changed=False)
        run.assert_not_called()
        te.ensure_terraform_init(self.root, False, run, version_changed=True)
        run.assert_called_once_with(["init", "-upgrade"], self.root)


if __name__ == "__main__":
    unittest.main()
