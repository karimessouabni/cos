"""Tests unitaires du filtrage (python -m pytest ou python test_subscriptions_cleanup.py)."""

from __future__ import annotations

import base64
import http.server
import json
import os
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import subscriptions_cleanup as sc  # noqa: E402

# Le cache de tokens des tests ne doit jamais toucher celui de l'utilisateur.
os.environ["XDG_CACHE_HOME"] = tempfile.mkdtemp(prefix="cos-subscriptions-tests-")


def _demand(action: str, status: str = "SUCCESS", uuid: str = "", create_date: str = "") -> dict:
    return {"action": action, "status": status, "status_reason": status.lower(),
            "uuid": uuid or f"uuid-{action}-{status}", "create_date": create_date}


def _no_browser():
    return mock.patch.object(sc, "acquire_token_interactively", return_value="")


def _jwt(exp: int) -> str:
    def enc(data: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")
    return f"{enc({'alg': 'RS256'})}.{enc({'exp': exp, 'sub': 'h90871'})}.c2lnbmF0dXJl"


FAR_FUTURE = 4_000_000_000


def _row(subscription_id: str, demands: list[dict], name: str = "bu003i023571",
         user: str = "h90871") -> dict:
    return {
        "context": {"code_bu": "BP2I", "realm": "rl003i001058", "user": user,
                    "tier": "A", "env_type": "NPR"},
        "geninfo": {
            "apcode": "A100473",
            "demands": demands,
            "environment": "int",
            "region": "eu-de",
            "name": name,
            "product": "cos.bucket",
            "status": "ACTIVE",
            "subscription_id": subscription_id,
        },
        "specinfo": {"cos_instance": "co003i012219"},
    }


class IsEligibleTests(unittest.TestCase):
    def test_only_allowed_actions_in_success(self):
        self.assertTrue(sc.is_eligible([_demand("force_clean"), _demand("create"), _demand("update")]))

    def test_single_create(self):
        self.assertTrue(sc.is_eligible([_demand("create")]))

    def test_successful_delete_is_rejected(self):
        # Cas de la capture d'écran: force_clean + create + delete SUCCESS -> non éligible
        self.assertFalse(sc.is_eligible([_demand("force_clean"), _demand("create"), _demand("delete")]))

    def test_failed_delete_is_accepted(self):
        self.assertTrue(sc.is_eligible([_demand("force_clean"), _demand("create"), _demand("delete", "ERROR")]))
        self.assertTrue(sc.is_eligible([_demand("create"), _demand("delete", "FAILED"), _demand("delete", "ERROR")]))

    def test_failed_delete_with_failed_other_action_is_rejected(self):
        self.assertFalse(sc.is_eligible([_demand("create", "ERROR"), _demand("delete", "ERROR")]))

    def test_mixed_delete_statuses_is_rejected(self):
        self.assertFalse(sc.is_eligible([_demand("create"), _demand("delete", "ERROR"), _demand("delete")]))

    def test_unknown_action_is_rejected(self):
        self.assertFalse(sc.is_eligible([_demand("create"), _demand("restore")]))

    def test_failed_status_is_rejected(self):
        self.assertFalse(sc.is_eligible([_demand("create"), _demand("update", "FAILED")]))

    def test_empty_or_missing_is_rejected(self):
        self.assertFalse(sc.is_eligible([]))
        self.assertFalse(sc.is_eligible(None))


class FindEligibleTests(unittest.TestCase):
    def test_returns_only_eligible_ids(self):
        body = {
            "$schema": "https://orchestrator-gw.int.staging.echonet/schemas/SubscriptionsOutputBody.json",
            "result": {
                "product": "cos.bucket",
                "rows": [
                    _row("0d8022cd-5e47-48be-b4ac-b50d1bb54211",
                         [_demand("force_clean"), _demand("create"), _demand("delete")]),
                    _row("aaaa-1", [_demand("create"), _demand("update")], name="bu003i000001"),
                    _row("bbbb-2", [_demand("force_clean"), _demand("create")], name="bu003i000002"),
                    _row("cccc-3", [_demand("create", "FAILED")], name="bu003i000003"),
                    _row("dddd-4", [_demand("create"), _demand("delete", "ERROR")], name="bu003i000004"),
                ],
            },
        }
        found = sc.find_eligible_subscriptions(body)
        self.assertEqual([s.subscription_id for s in found], ["aaaa-1", "bbbb-2", "dddd-4"])
        self.assertEqual(found[0].actions, ["create", "update"])
        self.assertEqual(found[2].actions, ["create", "delete(ERROR)"])
        self.assertFalse(found[0].needs_retry)
        self.assertTrue(found[2].needs_retry)
        self.assertEqual(found[2].failed_delete_demand_ids, ["uuid-delete-ERROR"])
        self.assertEqual(found[0].name, "bu003i000001")

    def test_filters_on_context_user(self):
        body = {"result": {"rows": [
            _row("mine-1", [_demand("create")], user="h90871"),
            _row("other-1", [_demand("create")], user="service-account-products_cft_confidential"),
            _row("nouser-1", [_demand("create")]),
        ]}}
        body["result"]["rows"][2]["context"].pop("user")
        self.assertEqual([s.subscription_id for s in sc.find_eligible_subscriptions(body)], ["mine-1"])
        self.assertEqual(sc.find_eligible_subscriptions(body)[0].user, "h90871")
        self.assertEqual(
            [s.subscription_id for s in sc.find_eligible_subscriptions(body, "service-account-products_cft_confidential")],
            ["other-1"],
        )
        self.assertEqual(
            [s.subscription_id for s in sc.find_eligible_subscriptions(body, None)],
            ["mine-1", "other-1", "nouser-1"],
        )

    def test_cli_user_flags(self):
        self.assertEqual(sc.parse_args([]).user, "h90871")
        self.assertEqual(sc.parse_args(["--user", "h12345"]).user, "h12345")
        self.assertTrue(sc.parse_args(["--all-users"]).all_users)

    def test_row_without_subscription_id_is_skipped(self):
        row = _row("", [_demand("create")])
        self.assertEqual(sc.find_eligible_subscriptions({"result": {"rows": [row]}}), [])


class RetryHelpersTests(unittest.TestCase):
    def test_failed_delete_demand_ids_sorted_by_create_date(self):
        demands = [
            _demand("create"),
            _demand("delete", "ERROR", uuid="d2", create_date="2026-09-09T19:00:00Z"),
            _demand("delete", "ERROR", uuid="d1", create_date="2026-09-09T18:00:00Z"),
            _demand("delete", "SUCCESS", uuid="d3"),
        ]
        self.assertEqual(sc.failed_delete_demand_ids(demands), ["d1", "d2"])

    def test_failed_process_names(self):
        demand = {
            "kind": "Demand",
            "uuid": "182e47b2-cfa2-4829-bb01-c2110a96099b",
            "status": "IN_PROGRESS",
            "processes": [
                {"kind": "Process", "name": "bootstrap", "status": "SUCCESS"},
                {"kind": "Process", "name": "validate_bucket_and_workspace", "status": "ERROR"},
                {"kind": "Process", "name": "delete_bucket", "status": "ERROR"},
                {"kind": "Process", "name": "notify", "status": "PENDING"},
            ],
        }
        self.assertEqual(sc.failed_process_names(demand),
                         ["validate_bucket_and_workspace", "delete_bucket"])
        self.assertEqual(sc.failed_process_names({}), [])


class PaginationTests(unittest.TestCase):
    def _fetch(self, pages: dict, calls: list):
        def fetch(page, size):
            calls.append((page, size))
            return pages.get(page, {"result": {"rows": []}})
        return fetch

    def test_stops_on_short_page(self):
        calls = []
        pages = {1: {"result": {"rows": [_row("a", []), _row("b", [])]}},
                 2: {"result": {"rows": [_row("c", [])]}}}
        rows = sc.iterate_pages(self._fetch(pages, calls), size=2)
        self.assertEqual([sc.subscription_uuid(r) for r in rows], ["a", "b", "c"])
        self.assertEqual(calls, [(1, 2), (2, 2)])

    def test_stops_on_empty_page(self):
        calls = []
        pages = {1: {"result": {"rows": [_row("a", []), _row("b", [])]}}}
        rows = sc.iterate_pages(self._fetch(pages, calls), size=2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(calls, [(1, 2), (2, 2)])

    def test_stops_on_total_pages_hint(self):
        calls = []
        pages = {1: {"result": {"rows": [_row("a", []), _row("b", [])], "total_pages": 2}},
                 2: {"result": {"rows": [_row("c", []), _row("d", [])], "total_pages": 2}},
                 3: {"result": {"rows": [_row("zz", []), _row("zy", [])], "total_pages": 2}}}
        rows = sc.iterate_pages(self._fetch(pages, calls), size=2)
        self.assertEqual([sc.subscription_uuid(r) for r in rows], ["a", "b", "c", "d"])
        self.assertEqual(calls, [(1, 2), (2, 2)])

    def test_total_count_hint(self):
        self.assertEqual(sc.total_pages_hint({"total": 250}, 100), 3)
        self.assertEqual(sc.total_pages_hint({"result": {"totalElements": 200}}, 100), 2)
        self.assertEqual(sc.total_pages_hint({"pagination": {"totalPages": 4}}, 100), 4)
        self.assertEqual(sc.total_pages_hint({"total": 0}, 100), 0)
        self.assertIsNone(sc.total_pages_hint({"result": {"rows": []}}, 100))
        self.assertIsNone(sc.total_pages_hint([], 100))

    def test_stops_when_api_ignores_page_param(self):
        calls = []
        same = {"result": {"rows": [_row("a", []), _row("b", [])]}}
        rows = sc.iterate_pages(lambda p, s: (calls.append(p), same)[1], size=2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(calls, [1, 2])

    def test_first_page_and_max_pages(self):
        calls = []
        full = lambda p, s: (calls.append(p), {"result": {"rows": [_row(f"a{p}", []), _row(f"b{p}", [])]}})[1]
        rows = sc.iterate_pages(full, size=2, first_page=0, max_pages=3)
        self.assertEqual(calls, [0, 1, 2])
        self.assertEqual(len(rows), 6)

    def test_accepts_bare_list_pages(self):
        pages = {1: [_row("a", [])]}
        rows = sc.iterate_pages(lambda p, s: pages.get(p, []), size=2)
        self.assertEqual(len(rows), 1)

    def test_dedupe_rows(self):
        rows = sc.dedupe_rows([_row("a", []), _row("b", []), _row("a", []), {"x": 1}, {"y": 2}])
        self.assertEqual(len(rows), 4)

    def test_cli_pagination_flags(self):
        args = sc.parse_args(["--page-size", "50", "--first-page", "0"])
        self.assertEqual((args.page_size, args.first_page), (50, 0))
        self.assertEqual((sc.parse_args([]).page_size, sc.parse_args([]).first_page), (100, 1))
        with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
            sc.parse_args(["--page-size", "0"])


class ClientTests(unittest.TestCase):
    def test_get_demand_url(self):
        client = sc.OrchestratorClient("tok")
        with mock.patch.object(client, "_request", return_value={}) as req:
            client.get_demand("182e47b2-cfa2-4829-bb01-c2110a96099b")
        req.assert_called_once_with("GET", "/api/v1/demands/182e47b2-cfa2-4829-bb01-c2110a96099b")

    def test_retry_demand_payload(self):
        client = sc.OrchestratorClient("tok")
        with mock.patch.object(client, "_request", return_value={}) as req:
            client.retry_demand("182e47b2", ["delete_bucket"])
        req.assert_called_once_with(
            "POST", "/api/v1/demands/182e47b2/retry",
            {"tasks": ["delete_bucket"], "retry_non_failed_tasks": False},
        )

    def test_retry_failed_delete_end_to_end(self):
        client = sc.OrchestratorClient("tok")
        demand = {"processes": [{"name": "bootstrap", "status": "SUCCESS"},
                                {"name": "delete_bucket", "status": "ERROR"}]}
        with mock.patch.object(client, "get_demand", return_value=demand), \
             mock.patch.object(client, "retry_demand", return_value={}) as retry:
            self.assertEqual(client.retry_failed_delete("d1"), ["delete_bucket"])
        retry.assert_called_once_with("d1", ["delete_bucket"])

    def test_retry_failed_delete_skips_when_nothing_in_error(self):
        client = sc.OrchestratorClient("tok")
        demand = {"processes": [{"name": "bootstrap", "status": "SUCCESS"}]}
        with mock.patch.object(client, "get_demand", return_value=demand), \
             mock.patch.object(client, "retry_demand") as retry:
            self.assertEqual(client.retry_failed_delete("d1"), [])
        retry.assert_not_called()

    def test_delete_payload_and_url(self):
        client = sc.OrchestratorClient("tok", "https://orchestrator-gw.int.staging.echonet/")
        with mock.patch.object(client, "_request", return_value={"ok": True}) as req:
            client.delete_subscription("0d8022cd-5e47-48be-b4ac-b50d1bb54211")
        req.assert_called_once_with(
            "DELETE",
            "/api/v1/subscriptions/0d8022cd-5e47-48be-b4ac-b50d1bb54211",
            {"product_branch": "main", "payload": {}},
        )

    def test_get_url(self):
        client = sc.OrchestratorClient("tok")
        with mock.patch.object(client, "_request", return_value={"result": {"rows": []}}) as req:
            client.get_subscriptions("cos.bucket")
        req.assert_called_once_with("GET", "/multireader/api/v1/subscriptions?page=1&size=100")
        with mock.patch.object(client, "_request", return_value={"result": {"rows": []}}) as req:
            client.get_subscriptions("cos.bucket", page_size=50, first_page=0)
        req.assert_called_once_with("GET", "/multireader/api/v1/subscriptions?page=0&size=50")

    def test_get_subscriptions_walks_all_pages(self):
        client = sc.OrchestratorClient("tok")
        pages = {
            "/multireader/api/v1/subscriptions?page=1&size=2": {"result": {"rows": [_row("s1", []), _row("s2", [])]}},
            "/multireader/api/v1/subscriptions?page=2&size=2": {"result": {"rows": [_row("s3", []), _row("s4", [])]}},
            "/multireader/api/v1/subscriptions?page=3&size=2": {"result": {"rows": [_row("s5", [])]}},
        }
        seen = []
        with mock.patch.object(client, "_request", side_effect=lambda m, path: pages[path]) as req:
            body = client.get_subscriptions("cos.bucket", page_size=2, progress=lambda p, n: seen.append((p, n)))
        self.assertEqual([r["geninfo"]["subscription_id"] for r in body["result"]["rows"]],
                         ["s1", "s2", "s3", "s4", "s5"])
        self.assertEqual(req.call_count, 3)
        self.assertEqual(seen, [(1, 2), (2, 2), (3, 1)])

    def test_get_subscriptions_filters_product(self):
        client = sc.OrchestratorClient("tok")
        other = _row("s2", [])
        other["geninfo"]["product"] = "cos.instance"
        no_product = _row("s3", [])
        del no_product["geninfo"]["product"]
        body = {"result": {"rows": [_row("s1", []), other, no_product]}}
        with mock.patch.object(client, "_request", return_value=body):
            rows = client.get_subscriptions("cos.bucket")["result"]["rows"]
            self.assertEqual([r["geninfo"]["subscription_id"] for r in rows], ["s1", "s3"])
            rows = client.get_subscriptions(None)["result"]["rows"]
            self.assertEqual(len(rows), 3)

    def test_request_sets_bearer_header(self):
        client = sc.OrchestratorClient("tok")
        captured = {}

        class _Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                return json.dumps({"ok": True}).encode()

        def fake_urlopen(request, timeout=None, context=None):
            captured["request"] = request
            return _Resp()

        with mock.patch("urllib.request.urlopen", fake_urlopen):
            out = client._request("DELETE", "/x", {"product_branch": "main", "payload": {}})
        req = captured["request"]
        self.assertEqual(out, {"ok": True})
        self.assertEqual(req.get_method(), "DELETE")
        self.assertEqual(req.get_header("Authorization"), "Bearer tok")
        self.assertEqual(json.loads(req.data), {"product_branch": "main", "payload": {}})

    def test_missing_token_rejected(self):
        with self.assertRaises(ValueError):
            sc.OrchestratorClient("")


def _err_row(uuid: str, statuses: list[str] | None = None, user: str = "h90871",
             sub_status: str = "ACTIVE", name: str = "bu003i023571") -> dict:
    """Row du listing multireader dont les demandes ont les statuts donnés (ON_ERROR par défaut)."""
    statuses = ["ON_ERROR"] if statuses is None else statuses
    demands = [_demand(["create", "update", "delete"][i % 3], st, uuid=f"{uuid}-d{i}", create_date=f"2026-0{i + 1}")
               for i, st in enumerate(statuses)]
    row = _row(uuid, demands, name=name, user=user)
    row["geninfo"]["status"] = sub_status
    return row


class OnErrorModeTests(unittest.TestCase):
    def test_extract_items_shapes(self):
        rows = [{"a": 1}]
        self.assertEqual(sc.extract_items(rows), rows)
        self.assertEqual(sc.extract_items({"rows": rows}), rows)
        self.assertEqual(sc.extract_items({"result": {"rows": rows}}), rows)
        self.assertEqual(sc.extract_items({"result": rows}), rows)
        self.assertEqual(sc.extract_items({"data": [1, {"b": 2}]}), [{"b": 2}])
        self.assertEqual(sc.extract_items(None), [])
        self.assertEqual(sc.extract_items({}), [])
        with self.assertRaises(ValueError):
            sc.extract_items("oops")

    def test_id_helpers(self):
        self.assertEqual(sc.subscription_uuid({"geninfo": {"subscription_id": "g"}, "id": 7}), "g")
        self.assertEqual(sc.subscription_uuid({"uuid": "u"}), "u")
        self.assertEqual(sc.subscription_uuid({"id": 42}), "42")
        self.assertEqual(sc.subscription_uuid({}), "")
        self.assertEqual(sc.subscription_user({"context": {"user": "h1"}}), "h1")
        self.assertEqual(sc.subscription_status({"geninfo": {"status": "LOCKED"}}), "LOCKED")
        self.assertEqual(sc.demand_uuid({"uuid": "u"}), "u")
        self.assertEqual(sc.demand_uuid({"demand_id": "d"}), "d")
        self.assertEqual(sc.demand_uuid({"id": "i"}), "i")

    def test_all_demands_in_status(self):
        self.assertTrue(sc.all_demands_in_status(_err_row("s", ["ON_ERROR", "ON_ERROR"])))
        self.assertFalse(sc.all_demands_in_status(_err_row("s", ["ON_ERROR", "SUCCESS"])))
        self.assertFalse(sc.all_demands_in_status(_err_row("s", ["ON_ERROR", "ERROR"])))
        self.assertFalse(sc.all_demands_in_status(_err_row("s", [])))
        self.assertFalse(sc.all_demands_in_status({}))
        self.assertTrue(sc.all_demands_in_status(_err_row("s", ["ERROR"]), "ERROR"))

    def test_find_error_demands(self):
        subs = [
            _err_row("sub-b", ["ON_ERROR", "ON_ERROR"]),
            _err_row("sub-a", ["ON_ERROR"], sub_status="LOCKED"),
            _err_row("sub-mixed", ["ON_ERROR", "SUCCESS"]),        # une SUCCESS -> exclue
            _err_row("sub-other-user", user="h00000"),            # autre user -> exclue
            _err_row("sub-empty", []),                            # pas de demande -> exclue
            {"geninfo": {"demands": [_demand("create", "ON_ERROR")]}, "context": {"user": "h90871"}},  # pas d'uuid
        ]
        found = sc.find_error_demands(subs)
        self.assertEqual([(d.subscription_id, d.demand_id) for d in found],
                         [("sub-a", "sub-a-d0"), ("sub-b", "sub-b-d0"), ("sub-b", "sub-b-d1")])
        self.assertEqual(found[0].action, "create")
        self.assertEqual(found[0].status, "ON_ERROR")
        self.assertEqual(found[0].subscription_name, "bu003i023571")
        self.assertEqual(found[0].user, "h90871")
        self.assertEqual(found[0].create_date, "2026-01")

    def test_find_error_demands_filters(self):
        subs = [_err_row("s1", sub_status="LOCKED"), _err_row("s2", user="h00000"),
                _err_row("s3", ["FAILED", "FAILED"])]
        self.assertEqual([d.subscription_id for d in sc.find_error_demands(subs, user=None)], ["s1", "s2"])
        self.assertEqual([d.subscription_id for d in sc.find_error_demands(subs, subscription_status_filter="LOCKED")],
                         ["s1"])
        self.assertEqual([d.demand_id for d in sc.find_error_demands(subs, demand_status="FAILED")], ["s3-d0", "s3-d1"])

    def test_client_set_demand_status(self):
        client = sc.OrchestratorClient("tok")
        with mock.patch.object(client, "_request", return_value={}) as req:
            client.set_demand_status("182e47b2")
            client.set_demand_status("a/b", "DECLINED", "cleanup")
        req.assert_has_calls([
            mock.call("POST", "/state_manager/api/v1/demands/182e47b2/status",
                      {"status": "DECLINED", "reason": "to remove"}),
            mock.call("POST", "/state_manager/api/v1/demands/a%2Fb/status",
                      {"status": "DECLINED", "reason": "cleanup"}),
        ])

    def test_cli_on_error_flags(self):
        args = sc.parse_args(["--on-error", "--decline", "--yes", "--reason", "r"])
        self.assertTrue(args.on_error and args.decline and args.yes)
        self.assertEqual(args.reason, "r")
        self.assertIsNone(args.subscription_status)
        self.assertEqual(args.demand_status, "ON_ERROR")
        self.assertTrue(sc.parse_args(["--locked"]).on_error)  # alias
        self.assertEqual(sc.parse_args(["--on-error", "--subscription-status", "LOCKED"]).subscription_status,
                         "LOCKED")
        for argv in (["--decline"], ["--on-error", "--delete"]):
            with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
                sc.parse_args(argv)

    def _fake_client(self, subs, post=None):
        client = mock.Mock()
        client.get_subscriptions.return_value = {"result": {"rows": subs}}
        client.set_demand_status.side_effect = post or (lambda *a, **k: {"ok": True})
        return client

    def _run(self, argv, client):
        with mock.patch.object(sc, "_make_client", return_value=client), \
             mock.patch("sys.stdout") as out, mock.patch("sys.stderr"):
            code = sc.main(argv)
        return code, "".join(c.args[0] for c in out.write.call_args_list)

    def test_main_dry_run_lists_without_posting(self):
        client = self._fake_client([_err_row("s1", ["ON_ERROR", "ON_ERROR"]), _err_row("s2", ["ON_ERROR", "SUCCESS"])])
        code, printed = self._run(["--on-error", "--token", "t"], client)
        self.assertEqual(code, 0)
        self.assertEqual(client.get_subscriptions.call_args.args[:3], ("cos.bucket", 100, 1))
        self.assertIn("2 souscription(s) lue(s) (user=h90871) : 1 avec toutes leurs demandes en ON_ERROR, "
                      "2 demande(s) à passer en DECLINED", printed)
        self.assertIn("demand=s1-d0", printed)
        self.assertIn("demand=s1-d1", printed)
        self.assertNotIn("s2-d0", printed)
        self.assertIn("Dry-run", printed)
        client.set_demand_status.assert_not_called()

    def test_main_json_output(self):
        client = self._fake_client([_err_row("s1")])
        code, printed = self._run(["--on-error", "--token", "t", "--json"], client)
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(printed),
                         [{"subscription_id": "s1", "demand_id": "s1-d0", "action": "create", "status": "ON_ERROR"}])

    def test_main_decline_posts_each_demand(self):
        client = self._fake_client([_err_row("s1"), _err_row("s2", ["ON_ERROR", "ON_ERROR"])])
        code, _ = self._run(["--on-error", "--decline", "--yes", "--token", "t", "--reason", "bye"], client)
        self.assertEqual(code, 0)
        client.set_demand_status.assert_has_calls([
            mock.call("s1-d0", "DECLINED", "bye"), mock.call("s2-d0", "DECLINED", "bye"),
            mock.call("s2-d1", "DECLINED", "bye"),
        ])
        self.assertEqual(client.set_demand_status.call_count, 3)

    def test_main_decline_asks_confirmation(self):
        client = self._fake_client([_err_row("s1")])
        with mock.patch("builtins.input", return_value="n"):
            self.assertEqual(self._run(["--on-error", "--decline", "--token", "t"], client)[0], 0)
        client.set_demand_status.assert_not_called()
        with mock.patch("builtins.input", return_value="o"):
            self.assertEqual(self._run(["--on-error", "--decline", "--token", "t"], client)[0], 0)
        client.set_demand_status.assert_called_once_with("s1-d0", "DECLINED", "to remove")

    def test_main_decline_failure_exit_code(self):
        def post(demand_id, *a):
            if demand_id == "s1-d1":
                raise sc.OrchestratorApiError("boom", status_code=500)
            return {}
        client = self._fake_client([_err_row("s1", ["ON_ERROR", "ON_ERROR"])], post)
        code, _ = self._run(["--on-error", "--decline", "--yes", "--token", "t"], client)
        self.assertEqual(code, 3)
        self.assertEqual(client.set_demand_status.call_count, 2)

    def test_main_get_failure(self):
        client = mock.Mock()
        client.get_subscriptions.side_effect = sc.OrchestratorApiError("GET x -> HTTP 500")
        self.assertEqual(self._run(["--on-error", "--token", "t"], client)[0], 2)

    def test_main_requires_token(self):
        with mock.patch.dict(os.environ, {"ORCHESTRATOR_TOKEN": ""}), _no_browser(), mock.patch("sys.stderr"):
            self.assertEqual(sc.main(["--on-error"]), 1)


class CleanupModeTests(unittest.TestCase):
    ROWS = [
        _row("s1", [_demand("create")], name="bu1"),
        _row("s2", [_demand("create"), _demand("delete", "ERROR", uuid="d2")], name="bu2"),
        _row("s3", [_demand("create", "FAILED")], name="bu3"),
    ]

    def _client(self, delete=None):
        client = mock.Mock()
        client.get_subscriptions.return_value = {"result": {"rows": self.ROWS}}
        client.delete_subscription.side_effect = delete or (lambda *a: {"ok": True})
        client.retry_failed_delete.return_value = ["task-a"]
        return client

    def _run(self, argv, client):
        with mock.patch.object(sc, "_make_client", return_value=client), \
             mock.patch("sys.stdout") as out, mock.patch("sys.stderr"):
            code = sc.main(argv)
        return code, "".join(c.args[0] for c in out.write.call_args_list)

    def test_dry_run_lists_plan(self):
        client = self._client()
        code, printed = self._run(["--token", "t"], client)
        self.assertEqual(code, 0)
        self.assertIn("3 souscription(s) lue(s), 2 éligible(s) (user=h90871): 1 à supprimer, 1 delete à relancer",
                      printed)
        self.assertIn("-> RETRY d2", printed)
        self.assertIn("Dry-run", printed)
        client.delete_subscription.assert_not_called()

    def test_delete_and_retry(self):
        client = self._client()
        code, printed = self._run(["--token", "t", "--delete", "--yes", "--product-branch", "dev"], client)
        self.assertEqual(code, 0)
        client.delete_subscription.assert_called_once_with("s1", "dev")
        client.retry_failed_delete.assert_called_once_with("d2")
        self.assertIn("RETRY d2 (bu2) -> OK tasks=task-a", printed)
        self.assertIn("2 traitée(s), 0 en échec", printed)

    def test_failure_exit_code_in_parallel(self):
        def delete(sub_id, branch):
            raise sc.OrchestratorApiError("boom", status_code=500)
        client = self._client(delete)
        code, printed = self._run(["--token", "t", "--delete", "--yes", "--workers", "4"], client)
        self.assertEqual(code, 3)
        self.assertIn("1 traitée(s), 1 en échec", printed)

    def test_json_output(self):
        code, printed = self._run(["--token", "t", "--json"], self._client())
        self.assertEqual((code, json.loads(printed)), (0, ["s1", "s2"]))

    def test_workers_must_be_positive(self):
        with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
            sc.parse_args(["--workers", "0"])


class TokenTests(unittest.TestCase):
    def test_jwt_expiry_and_seconds_left(self):
        token = _jwt(2_000)
        self.assertEqual(sc.jwt_expiry(token), 2_000)
        self.assertEqual(sc.token_seconds_left(token, now=1_400), 600)
        self.assertIsNone(sc.jwt_expiry("pas-un-jwt"))

    def test_clean_token(self):
        token = _jwt(2_000)
        for raw in (token, f"Bearer {token}", f'  "{token}"\n', f"Authorization: Bearer {token}"):
            self.assertEqual(sc.clean_token(raw), token)
        self.assertEqual(sc.clean_token(" opaque "), "opaque")

    def test_valid_token_is_kept(self):
        acquire = mock.Mock()
        with mock.patch("sys.stderr"):
            token = sc.resolve_token(f"Bearer {_jwt(FAR_FUTURE)}", None, acquire)
        self.assertEqual(token, _jwt(FAR_FUTURE))
        acquire.assert_not_called()

    def test_opaque_token_is_kept(self):
        self.assertEqual(sc.resolve_token("opaque", None, mock.Mock()), "opaque")

    def test_expired_token_is_replaced(self):
        acquire = mock.Mock(return_value=_jwt(FAR_FUTURE))
        with mock.patch("sys.stderr"):
            token = sc.resolve_token(_jwt(1_000), "https://swagger", acquire)
        self.assertEqual(token, _jwt(FAR_FUTURE))
        acquire.assert_called_once_with("https://swagger")

    def test_missing_token_raises(self):
        with self.assertRaises(sc.CliExit) as cm:
            sc.resolve_token(None, None, lambda url: "")
        self.assertEqual(cm.exception.code, 1)

    def test_acquire_reads_clipboard_and_skips_bad_values(self):
        clips = iter([_jwt(1_000), "du texte", _jwt(FAR_FUTURE)])
        opened = []
        with mock.patch.object(sys.stdin, "isatty", return_value=True), mock.patch("sys.stderr"):
            token = sc.acquire_token_interactively(
                "https://swagger", read_clip=lambda: next(clips), ask=lambda prompt: "", auto=False, open_url=lambda url: opened.append(url) or True)
        self.assertEqual(token, _jwt(FAR_FUTURE))
        self.assertEqual(opened, ["https://swagger"])

    def test_acquire_accepts_pasted_token(self):
        with mock.patch.object(sys.stdin, "isatty", return_value=True), mock.patch("sys.stderr"):
            token = sc.acquire_token_interactively(
                None, read_clip=lambda: "", ask=lambda prompt: "Bearer opaque-token", ask_url=lambda prompt: "",
                auto_browser=self.fail)
        self.assertEqual(token, "opaque-token")

    def test_acquire_asks_swagger_url_when_not_configured(self):
        opened = []
        with mock.patch.object(sys.stdin, "isatty", return_value=True), mock.patch("sys.stderr"):
            token = sc.acquire_token_interactively(
                None, read_clip=lambda: _jwt(FAR_FUTURE), ask=lambda prompt: "",
                open_url=lambda url: opened.append(url) or True, ask_url=lambda prompt: " https://swagger ",
                auto=False)
        self.assertEqual(token, _jwt(FAR_FUTURE))
        self.assertEqual(opened, ["https://swagger"])

    def test_acquire_gives_up_outside_a_terminal(self):
        with mock.patch.object(sys.stdin, "isatty", return_value=False):
            with mock.patch("sys.stderr"):
                self.assertEqual(sc.acquire_token_interactively(
                    "https://swagger", open_url=self.fail, auto_browser=lambda url: ""), "")

    def test_acquire_uses_browser_first_even_outside_a_terminal(self):
        with mock.patch.object(sys.stdin, "isatty", return_value=False):
            token = sc.acquire_token_interactively(
                "https://swagger", open_url=self.fail, auto_browser=lambda url: "from-browser")
        self.assertEqual(token, "from-browser")

    def test_acquire_falls_back_to_clipboard_when_browser_fails(self):
        with mock.patch.object(sys.stdin, "isatty", return_value=True), mock.patch("sys.stderr"):
            token = sc.acquire_token_interactively(
                "https://swagger", read_clip=lambda: _jwt(FAR_FUTURE), ask=lambda prompt: "",
                open_url=lambda url: True, auto_browser=lambda url: "")
        self.assertEqual(token, _jwt(FAR_FUTURE))

    def test_browser_is_always_private_and_ignores_certificates(self):
        with mock.patch.object(sc.subprocess, "Popen") as popen, \
             mock.patch.object(sc, "_devtools_port", side_effect=RuntimeError("stop")), mock.patch("sys.stderr"):
            sc.browser_token("https://swagger", browser="/bin/chrome")
        command = popen.call_args.args[0]
        self.assertIn("--incognito", command)
        self.assertIn("--ignore-certificate-errors", command)
        self.assertEqual(command[-1], "https://swagger")

    def test_swagger_url_defaults_to_base_url_docs(self):
        with mock.patch.dict(os.environ, {"ORCHESTRATOR_SWAGGER_URL": ""}):
            self.assertEqual(sc.parse_args([]).swagger_url, "https://orchestrator-gw.int.staging.echonet/docs")
            self.assertEqual(sc.parse_args(["--base-url", "https://gw.example/"]).swagger_url,
                             "https://gw.example/docs")
            self.assertEqual(sc.parse_args(["--swagger-url", "https://x/ui"]).swagger_url, "https://x/ui")
        with mock.patch.dict(os.environ, {"ORCHESTRATOR_SWAGGER_URL": "https://env/docs"}):
            self.assertEqual(sc.parse_args([]).swagger_url, "https://env/docs")

    def test_session_passes_browser_flags(self):
        args = sc.parse_args(["--manual-token"])
        with mock.patch.object(sc, "acquire_token_interactively", return_value="t") as acquire:
            self.assertEqual(sc._acquire_token(args, "https://swagger"), "t")
        self.assertFalse(acquire.call_args.kwargs["auto"])

    def test_browser_token_without_browser(self):
        with mock.patch.object(sc, "find_chromium_browser", return_value=None), mock.patch("sys.stderr"):
            self.assertEqual(sc.browser_token("https://swagger"), "")


    def test_bookmarklet_option_is_gone(self):
        with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
            sc.parse_args(["--print-bookmarklet"])


class InteractiveModeTests(unittest.TestCase):
    def _wizard(self, answers):
        replies = iter(answers)
        with mock.patch("sys.stdout") as out:
            argv = sc.interactive_argv(ask=lambda prompt: next(replies))
        return argv, "".join(c.args[0] for c in out.write.call_args_list)

    def test_all_defaults_is_a_dry_run(self):
        argv, printed = self._wizard([""] * 6)
        self.assertEqual(argv, [])
        self.assertIn("1) Supprimer les souscriptions éligibles", printed)
        self.assertIn("Commande équivalente", printed)
        self.assertFalse(sc.parse_args(argv).delete)

    def test_delete_for_real_with_choices(self):
        answers = ["1", "2", "2", "h12345", "", "dev", "", "3", "4"]
        argv, _ = self._wizard(answers)
        self.assertEqual(argv, ["--delete", "--user", "h12345", "--product-branch", "dev",
                                "--manual-token", "--workers", "4"])
        args = sc.parse_args(argv)
        self.assertEqual((args.delete, args.user, args.workers), (True, "h12345", 4))

    def test_on_error_decline_locked(self):
        answers = ["2", "2", "3", "", "2", "", "", "", ""]
        argv, _ = self._wizard(answers)
        self.assertEqual(argv, ["--on-error", "--decline", "--all-users", "--subscription-status", "LOCKED"])
        self.assertTrue(sc.parse_args(argv).decline)

    def test_invalid_choice_is_asked_again(self):
        argv, printed = self._wizard(["9", "abc", "3"])
        self.assertIsNone(argv)
        self.assertIn("Taper un nombre entre 1 et 3", printed)

    def test_no_bookmarklet_choice(self):
        argv, printed = self._wizard(["3"])
        self.assertIsNone(argv)
        self.assertNotIn("bookmarklet", printed)

    def test_token_is_masked_in_the_printed_command(self):
        with mock.patch("getpass.getpass", return_value="Bearer secret-token"):
            argv, printed = self._wizard(["1", "1", "1", "", "", "4"])
        self.assertEqual(argv, ["--token", "secret-token"])
        self.assertNotIn("secret-token", printed)
        self.assertIn("--token '<token>'", printed)

    def test_main_starts_wizard_without_arguments_in_a_terminal(self):
        with mock.patch.object(sys, "argv", ["subscriptions_cleanup.py"]), \
             mock.patch.object(sys.stdin, "isatty", return_value=True), \
             mock.patch.object(sc, "interactive_argv", return_value=None) as wizard:
            self.assertEqual(sc.main(), 0)
        wizard.assert_called_once()

    def test_main_ctrl_c_in_wizard(self):
        with mock.patch.object(sc, "interactive_argv", side_effect=KeyboardInterrupt), mock.patch("sys.stdout"):
            self.assertEqual(sc.main(["-i"]), 0)

    def test_no_json_input_step(self):
        argv, printed = self._wizard([""] * 6)
        self.assertNotIn("JSON", printed)
        with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
            sc.parse_args(["--input", "scratch.json"])

    def test_dry_run_flag(self):
        self.assertTrue(sc.parse_args(["--dry-run"]).dry_run)
        with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
            sc.parse_args(["--dry-run", "--delete"])


class TokenCacheTests(unittest.TestCase):
    BASE = "https://gw.example"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = mock.patch.dict(os.environ, {"XDG_CACHE_HOME": self.tmp.name, "ORCHESTRATOR_TOKEN": ""})
        self.env.start()
        self.stderr = mock.patch("sys.stderr")
        self.stderr.start()

    def tearDown(self):
        self.stderr.stop()
        self.env.stop()
        self.tmp.cleanup()

    def _args(self, *extra):
        return sc.parse_args(["--base-url", self.BASE, *extra])

    def test_save_and_load(self):
        token = _jwt(FAR_FUTURE)
        sc.save_cached_token(self.BASE + "/", token)
        self.assertEqual(sc.load_cached_token(self.BASE), token)
        self.assertEqual(os.stat(sc.token_cache_path()).st_mode & 0o777, 0o600)
        self.assertIsNone(sc.load_cached_token("https://other"))

    def test_expired_or_opaque_tokens_are_not_reused(self):
        sc.save_cached_token(self.BASE, _jwt(1_000))
        self.assertIsNone(sc.load_cached_token(self.BASE))
        sc.forget_cached_token(self.BASE)
        sc.save_cached_token(self.BASE, "opaque")
        self.assertIsNone(sc.load_cached_token(self.BASE))

    def test_corrupted_cache_is_ignored(self):
        os.makedirs(os.path.dirname(sc.token_cache_path()))
        with open(sc.token_cache_path(), "w") as fh:
            fh.write("{pas du json")
        self.assertIsNone(sc.load_cached_token(self.BASE))

    def test_connect_reuses_saved_token_without_browser(self):
        token = _jwt(FAR_FUTURE)
        sc.save_cached_token(self.BASE, token)
        with mock.patch.object(sc, "acquire_token_interactively") as acquire:
            client = sc.connect(self._args())
        self.assertEqual(client.token, token)
        acquire.assert_not_called()

    def test_connect_saves_acquired_token(self):
        token = _jwt(FAR_FUTURE)
        with mock.patch.object(sc, "acquire_token_interactively", return_value=token):
            sc.connect(self._args())
        self.assertEqual(sc.load_cached_token(self.BASE), token)

    def test_new_token_flag_skips_the_cache(self):
        sc.save_cached_token(self.BASE, _jwt(FAR_FUTURE))
        fresh = _jwt(FAR_FUTURE + 1)
        with mock.patch.object(sc, "acquire_token_interactively", return_value=fresh):
            self.assertEqual(sc.connect(self._args("--new-token")).token, fresh)
        self.assertEqual(sc.load_cached_token(self.BASE), fresh)

    def test_rejected_saved_token_is_replaced_once(self):
        sc.save_cached_token(self.BASE, _jwt(FAR_FUTURE))
        fresh = _jwt(FAR_FUTURE + 1)
        calls = []

        def get_subscriptions(self_client, *a, **k):
            calls.append(self_client.token)
            if len(calls) == 1:
                raise sc.OrchestratorApiError("HTTP 401", status_code=401)
            return {"result": {"rows": []}}

        with mock.patch.object(sc.OrchestratorClient, "get_subscriptions", get_subscriptions), \
             mock.patch.object(sc, "acquire_token_interactively", return_value=fresh):
            client, rows = sc.connect_and_list(self._args())
        self.assertEqual((client.token, rows), (fresh, []))
        self.assertEqual(calls, [_jwt(FAR_FUTURE), fresh])
        self.assertEqual(sc.load_cached_token(self.BASE), fresh)

    def test_rejected_cli_token_is_not_retried(self):
        with mock.patch.object(sc.OrchestratorClient, "get_subscriptions",
                               side_effect=sc.OrchestratorApiError("HTTP 401", status_code=401)), \
             mock.patch.object(sc, "acquire_token_interactively") as acquire:
            with self.assertRaises(sc.CliExit) as cm:
                sc.connect_and_list(self._args("--token", "opaque"))
        self.assertEqual(cm.exception.code, 2)
        acquire.assert_not_called()

    def test_forget_token_flag(self):
        sc.save_cached_token(self.BASE, _jwt(FAR_FUTURE))
        self.assertEqual(sc.main(["--base-url", self.BASE, "--forget-token"]), 0)
        self.assertIsNone(sc.load_cached_token(self.BASE))


class EnvironmentTests(unittest.TestCase):
    ENVS = {"staging": "https://staging.example", "preprod": "https://preprod.example", "prod": ""}

    def setUp(self):
        patches = [mock.patch.dict(sc.ENVIRONMENTS, self.ENVS),
                   mock.patch.dict(os.environ, {"ORCHESTRATOR_ENV": "", "ORCHESTRATOR_SWAGGER_URL": ""})]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def test_env_selects_the_url(self):
        self.assertEqual(sc.parse_args([]).env, "staging")
        args = sc.parse_args(["--env", "preprod"])
        self.assertEqual((args.base_url, args.swagger_url), ("https://preprod.example", "https://preprod.example/docs"))
        self.assertEqual(sc.parse_args(["--env", "preprod", "--base-url", "https://x"]).base_url, "https://x")

    def test_env_from_environment_variable(self):
        with mock.patch.dict(os.environ, {"ORCHESTRATOR_ENV": "preprod"}):
            self.assertEqual(sc.parse_args([]).base_url, "https://preprod.example")

    def test_env_without_url_is_rejected(self):
        for argv in (["--env", "prod"], ["--env", "dev"]):
            with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
                sc.parse_args(argv)
        self.assertEqual(sc.parse_args(["--env", "prod", "--base-url", "https://prod.example"]).base_url,
                         "https://prod.example")

    def test_wizard_env_choice(self):
        replies = iter(["1", "1", "1", "", "2", "", ""])
        with mock.patch("sys.stdout"):
            argv = sc.interactive_argv(ask=lambda prompt: next(replies))
        self.assertEqual(argv, ["--env", "preprod"])

    def test_wizard_asks_url_when_env_has_none(self):
        replies = iter(["1", "1", "1", "", "3", "https://prod.example", "", ""])
        with mock.patch("sys.stdout"):
            argv = sc.interactive_argv(ask=lambda prompt: next(replies))
        self.assertEqual(argv, ["--env", "prod", "--base-url", "https://prod.example"])
        self.assertEqual(sc.parse_args(argv).base_url, "https://prod.example")


@unittest.skipUnless(sc.find_chromium_browser() and os.environ.get("RUN_BROWSER_TESTS"),
                     "Chrome / Edge absent ou RUN_BROWSER_TESTS non défini")
class BrowserTokenTests(unittest.TestCase):
    """Chrome headless contre un faux Swagger local qui expose le token après 1 s."""

    def test_token_read_from_swagger_ui_state(self):
        token = _jwt(FAR_FUTURE)
        self.assertEqual(self._browse("setTimeout(()=>{window.ui={authSelectors:{authorized:()=>({toJS:()=>"
                                      f"({{bearer:{{token:{{access_token:'{token}'}}}}}})}})}}}}}},1000)"), token)

    def test_token_read_from_keycloak_js(self):
        token = _jwt(FAR_FUTURE)
        self.assertEqual(self._browse(f"setTimeout(()=>{{window.keycloak={{token:'{token}'}}}},1000)"), token)

    def _browse(self, script: str) -> str:
        page = f"<html><body><script>{script}</script></body></html>".encode()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(page)))
                self.end_headers()
                self.wfile.write(page)

            def log_message(self, *a):
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        with tempfile.TemporaryDirectory() as tmp:
            wrapper = os.path.join(tmp, "browser.sh")
            with open(wrapper, "w") as fh:
                fh.write(f'#!/bin/sh\nexec "{sc.find_chromium_browser()}" --headless=new "$@"\n')
            os.chmod(wrapper, 0o755)
            try:
                with mock.patch("sys.stderr"):
                    found = sc.browser_token(f"http://127.0.0.1:{server.server_address[1]}/",
                                             browser=wrapper, timeout=30, poll_interval=0.5)
            finally:
                server.shutdown()
                server.server_close()
        return found


def _make_self_signed(tmpdir: str) -> tuple[str, str, str]:
    """Génère (key.pem, cert.pem, cert.der) auto-signés pour localhost via openssl."""
    key = os.path.join(tmpdir, "key.pem")
    pem = os.path.join(tmpdir, "cert.pem")
    der = os.path.join(tmpdir, "cert.der")
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
         "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
         "-keyout", key, "-out", pem],
        check=True, capture_output=True,
    )
    subprocess.run(["openssl", "x509", "-in", pem, "-outform", "DER", "-out", der],
                   check=True, capture_output=True)
    return key, pem, der


@unittest.skipUnless(shutil.which("openssl"), "openssl absent")
class TlsTests(unittest.TestCase):
    def test_ssl_context_never_verifies(self):
        ctx = sc.insecure_ssl_context()
        self.assertFalse(ctx.check_hostname)
        self.assertEqual(ctx.verify_mode, ssl.CERT_NONE)

    def test_end_to_end_against_self_signed_server(self):
        """Serveur HTTPS auto-signé : le client passe sans aucun certificat CA."""
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps({"result": {"rows": []}}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        with tempfile.TemporaryDirectory() as tmp:
            key, pem, _ = _make_self_signed(tmp)
            server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
            srv_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            srv_ctx.load_cert_chain(pem, key)
            server.socket = srv_ctx.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            body = sc.OrchestratorClient("tok", f"https://localhost:{server.server_address[1]}").get_subscriptions()
            self.assertEqual(body, {"result": {"rows": []}})
        finally:
            server.shutdown()
            server.server_close()

    def test_tls_options_are_gone(self):
        for flag in ("--insecure", "--verify-tls", "--ca-cert", "--no-private"):
            with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
                sc.parse_args([flag])


if __name__ == "__main__":
    unittest.main()
