"""Independent, offline regression checks for the conservative startup gates."""
from __future__ import annotations

import ast
import copy
import io
import inspect
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.collection import environment, pricing_route, pricing_route_model as prm
from scripts.collection import scenario_runner as sr, gateway_cpu_network as gw
from scripts.collection import run_campaign, run_scenario


def deployment(namespace, owner=None):
    annotations = {} if owner is None else {prm.OWNER_KEY: owner}
    return {"apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "pricing", "namespace": namespace, "uid": "deploy-1",
                         "resourceVersion": "9", "labels": {}, "annotations": annotations},
            "spec": {"selector": {"matchLabels": {"app": "pricing"}}, "replicas": 1,
                     "template": {"metadata": {"labels": {"app": "pricing"}}, "spec": {"containers": [
                         {"name": "pricing", "image": "fixture/image:v1", "env": [
                             {"name": prm.ROUTE_ENV, "value": prm.DIRECT_URL},
                             {"name": prm.NACOS_ENV, "value": "false"}]}]}}}}


class FakeKubectl:
    """Synthetic JSON transport; no command is started."""
    evidence_kind = "synthetic"

    def __init__(self, namespace, cluster_uid, namespace_uid, deploy):
        self.namespace, self.cluster_uid, self.namespace_uid = namespace, cluster_uid, namespace_uid
        self.deploy, self.calls = deploy, []

    def run(self, argv, *, timeout_s, max_output_bytes):
        args = tuple(argv); self.calls.append(args)
        at = args.index("get"); kind = args[at + 1]
        if kind == "namespace":
            name = args[at + 2]
            uid = self.cluster_uid if name == "kube-system" else self.namespace_uid
            value = {"kind": "Namespace", "metadata": {"name": name, "uid": uid}}
        elif kind == "deployment":
            value = copy.deepcopy(self.deploy)
        else:
            value = {"items": []}
        return types.SimpleNamespace(status="ok", return_code=0, workers_joined=True,
                                     stdout=json.dumps(value).encode(), stderr=b"")


class StartupReproductionTests(unittest.TestCase):
    def setUp(self):
        parent = Path(os.environ["RECSHOP_TEST_TMP"])
        parent.mkdir(parents=True, exist_ok=True)
        self._temp = tempfile.TemporaryDirectory(prefix="repro-", dir=parent)
        self.root = Path(self._temp.name)

    def tearDown(self):
        self._temp.cleanup()

    def executable(self, relative):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"placeholder; tests never execute this file")
        return path.resolve()

    def test_registered_pricing_scope_reads_public_catalog_for_real_short_contract(self):
        from test_collection_portable import PortableCollectionTests
        fixture = PortableCollectionTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        with mock.patch("subprocess.run", side_effect=AssertionError("no process")), \
             mock.patch("subprocess.Popen", side_effect=AssertionError("no process")), \
             mock.patch("socket.socket", side_effect=AssertionError("no network")):
            spec = sr.entry_view(sr.entry_spec("S01"))
            assembled, _, _ = sr.build("S01", "catalog-scope-test", sr.snapshot(spec, enabled=False),
                                       enabled=False, profile="short")
            plans = (types.SimpleNamespace(fault_instance_ids=("F1",)),)
            sr.r._registered_pricing_scope(assembled.contract, plans)
            with self.assertRaisesRegex(sr.r.RunnerError, "gateway fault legs"):
                sr.r._registered_pricing_scope(assembled.contract,
                    (types.SimpleNamespace(fault_instance_ids=("F2",)),))
            other = sr.entry_view(sr.entry_spec("S04"))
            nongateway, _, _ = sr.build("S04", "catalog-scope-negative", sr.snapshot(other, enabled=False),
                                       enabled=False, profile="short")
            with self.assertRaisesRegex(sr.r.RunnerError, "gateway fault legs"):
                sr.r._registered_pricing_scope(nongateway.contract, plans)

    def test_route_baseline_binds_new_namespace_and_rejects_uid_or_owner_drift(self):
        for namespace, ns_uid in (("collection-next", "ns-next"), ("team-a", "ns-a")):
            process = FakeKubectl(namespace, "cluster-1", ns_uid, deployment(namespace))
            adapter = pricing_route.PricingRouteAdapter("ctx-fixture", namespace, "cluster-1", ns_uid,
                process=process, policy=pricing_route.RoutePolicy(settle_timeout_s=1, poll_interval_s=.001))
            with mock.patch.object(prm, "judge_rollout", return_value={"settled": True}), \
                 mock.patch.object(prm, "validate_baseline", wraps=prm.validate_baseline) as validate:
                result = adapter.read_baseline()
            self.assertTrue(result["exists"])
            self.assertEqual(adapter.last_readback["environment"]["namespace"], namespace)
            self.assertEqual(validate.call_args.kwargs["namespace"], namespace)
            self.assertTrue(process.calls)
            self.assertTrue(all(call[call.index("--namespace") + 1] == namespace for call in process.calls))

        cases = (("kube-system", "wrong-cluster", "cluster-1", "ns-1"),
                 ("collection-next", "wrong-ns-uid", "cluster-1", "ns-1"))
        for bad_name, bad_uid, expected_cluster, expected_ns in cases:
            namespace = "collection-next"
            process = FakeKubectl(namespace, expected_cluster, expected_ns, deployment(namespace))
            original = process.run
            def mutate_first_identity(argv, *, timeout_s, max_output_bytes):
                args = tuple(argv); at = args.index("get")
                if args[at + 1] == "namespace" and args[at + 2] == bad_name:
                    process.calls.append(args)
                    obj = {"kind": "Namespace", "metadata": {"name": bad_name, "uid": bad_uid}}
                    return types.SimpleNamespace(status="ok", return_code=0, workers_joined=True,
                                                 stdout=json.dumps(obj).encode(), stderr=b"")
                return original(argv, timeout_s=timeout_s, max_output_bytes=max_output_bytes)
            process.run = mutate_first_identity
            adapter = pricing_route.PricingRouteAdapter("ctx-fixture", namespace, "cluster-1", "ns-1", process=process)
            with self.assertRaisesRegex(pricing_route.PricingRouteError, "ROUTE_NAMESPACE_IDENTITY_CHANGED"):
                adapter.read_baseline()
            self.assertEqual(len(process.calls), 1 if bad_name == "kube-system" else 2)

        namespace = "collection-next"
        adapter = pricing_route.PricingRouteAdapter("ctx", namespace, "cluster-1", "ns-1",
            process=FakeKubectl(namespace, "cluster-1", "ns-1", deployment(namespace, "foreign-owner")))
        with self.assertRaisesRegex(prm.RouteModelError, "BASELINE_OWNER_PRESENT"):
            adapter.read_baseline()

        wrong_scope = pricing_route.PricingRouteAdapter("ctx", namespace, "cluster-1", "ns-1",
            process=FakeKubectl(namespace, "cluster-1", "ns-1", deployment("another-namespace")))
        with self.assertRaisesRegex(prm.RouteModelError, "WRONG_NAMESPACE"):
            wrong_scope.read_baseline()

    def test_cli_path_with_spaces_bundle_fallback_drift_and_missing_are_safe(self):
        path_cli = self.executable("Program Files/kubectl.exe")
        with mock.patch.dict(environment.os.environ,
                             {"PATH": "unchanged", "ProgramFiles": str(self.root)}, clear=True), \
             mock.patch.object(environment.shutil, "which", return_value=str(path_cli)):
            self.assertEqual(Path(environment.resolve_cli("kubectl")), path_cli)
            self.assertEqual(environment.os.environ["PATH"], "unchanged")
            changed = self.executable("Other Tools/kubectl.exe")
            with mock.patch.object(environment.shutil, "which", return_value=str(changed)):
                with self.assertRaisesRegex(ValueError, "changed since parent resolution"):
                    environment.resolve_cli("kubectl")

        if os.name == "nt":
            bundled = self.executable("Docker/Docker/resources/bin/docker.exe")
            with mock.patch.dict(environment.os.environ,
                                 {"PATH": "still-unchanged", "ProgramFiles": str(self.root)}, clear=True), \
                 mock.patch.object(environment.shutil, "which", return_value=None):
                self.assertEqual(Path(environment.resolve_cli("docker")), bundled)
                self.assertEqual(environment.os.environ["PATH"], "still-unchanged")
        missing_program = self.root / "missing-install"
        missing_program.mkdir()
        with mock.patch.dict(environment.os.environ, {"PATH": "stable", "ProgramFiles": str(missing_program)}, clear=True), \
             mock.patch.object(environment.shutil, "which", return_value=None), \
             mock.patch("subprocess.run", side_effect=AssertionError("no command")), \
             mock.patch("subprocess.Popen", side_effect=AssertionError("no command")):
            with self.assertRaisesRegex(ValueError, "CLI missing"):
                environment.resolve_cli("docker")
            self.assertEqual(environment.os.environ["PATH"], "stable")

    def test_monitoring_allows_service_dns_but_rejects_direct_origin_without_echo(self):
        doc = {"telemetry": {"prometheus": "http://127.0.0.1:19090", "jaeger": "http://127.0.0.1:16686",
            "metrics_exporter": "http://127.0.0.1:18899",
            "metrics_collector_config_path": "/etc/m1/otel-metrics-config.yaml",
            "metrics_endpoint_for_apps": "http://otel-collector.ops.svc:4318/v1/metrics",
            "metrics_scrape_url": "http://m1-otel-metrics.svc:8889/metrics",
            "loki": "http://recweb2-loki.ops.svc:3100"}}
        environment.validate_monitoring(doc)
        bad = copy.deepcopy(doc)
        bad["telemetry"]["prometheus"] = "https://user:TOPSECRET@prom.example/path?token=HIDDEN"
        with self.assertRaises(ValueError) as caught:
            environment.validate_monitoring(bad)
        for secret in ("TOPSECRET", "HIDDEN", "user", "prom.example"):
            self.assertNotIn(secret, str(caught.exception))
        bad = copy.deepcopy(doc); bad["telemetry"]["metrics_collector_config_path"] = "/tmp/elsewhere"
        with self.assertRaisesRegex(ValueError, "container path"):
            environment.validate_monitoring(bad)
        source = self.root / "invalid-env.json"
        source.write_text(json.dumps({"telemetry": {"prometheus": "http://user:TOPSECRET@host/path"}}), encoding="utf-8")
        before = environment._active, environment._path
        with self.assertRaises(ValueError) as caught, \
             mock.patch.object(environment, "resolve_cli", side_effect=AssertionError("config only")):
            environment.load(source)
        self.assertNotIn("TOPSECRET", str(caught.exception))
        self.assertEqual((environment._active, environment._path), before)

    def test_actual_reader_sampling_and_incremental_inputs_use_resolved_tools(self):
        kubectl = str(self.executable("folder with spaces/kubectl.exe"))
        with mock.patch.object(sr.portable_env, "assert_live"), mock.patch.object(sr.portable_env, "apply"), \
             mock.patch.object(sr.portable_env, "resolve_cli", return_value=kubectl), \
             mock.patch.object(sr.live, "KubectlReadClient") as read_client:
            sr.reader(True)
        self.assertEqual(read_client.call_args.args[0], kubectl)

        tree = ast.parse(inspect.getsource(sr.build))
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and ast.unparse(n.func).endswith("kubectl_driver_factory")]
        self.assertTrue(calls)
        executable = next(k.value for n in calls for k in n.keywords if k.arg == "executable")
        self.assertIn("portable_env.resolve_cli('kubectl')", ast.unparse(executable))
        for consumer in (sr.provision_stressor_carriers, sr.delete_stressor_carriers,
                         sr.ProxyControl.start, sr.recover_pricing_aux_attempt):
            self.assertIn("portable_env.resolve_cli", inspect.getsource(consumer))
        sampling_source = inspect.getsource(sr.resolve_sampling_observer)
        self.assertIn('portable_env.resolve_cli("docker")', sampling_source)
        self.assertIn("docker_executable=docker", sampling_source)
        inc_tree = ast.parse(inspect.getsource(gw.install))
        adapter_calls = [n for n in ast.walk(inc_tree) if isinstance(n, ast.Call)
                         and ast.unparse(n.func).endswith("PricingRouteAdapter")]
        self.assertTrue(any(any(k.arg == "kubectl" and "resolve_cli('kubectl')" in ast.unparse(k.value)
                               for k in n.keywords) for n in adapter_calls))

    def test_recovery_needs_kubectl_but_not_docker_while_new_run_still_requires_docker(self):
        def unavailable_docker(calls):
            def resolve(name):
                calls.append(name)
                if name == "docker":
                    raise ValueError("synthetic docker CLI missing")
                return "C:/synthetic/kubectl.exe"
            return resolve

        # Credential-isolated wrapper: run refuses at discovery; recover reaches
        # the child with the same kubectl binding and without Docker discovery.
        for step in ("run", "recover"):
            calls = []
            with mock.patch.object(run_scenario.environment, "apply"), \
                 mock.patch.object(environment, "require_windows_execution"), \
                 mock.patch.object(environment, "resolve_cli", side_effect=unavailable_docker(calls)), \
                 mock.patch.object(environment, "credentials", return_value={}), \
                 mock.patch.object(sr, "main") as child, \
                 mock.patch.object(run_scenario.sys, "argv", ["run_scenario.py", step]):
                if step == "run":
                    with self.assertRaisesRegex(ValueError, "docker CLI missing"):
                        run_scenario.main()
                    child.assert_not_called()
                    self.assertEqual(calls, ["kubectl", "docker"])
                else:
                    run_scenario.main()
                    child.assert_called_once()
                    self.assertEqual(calls, ["kubectl"])

        # Direct shared entry uses the same distinction before environment or
        # attempt reads; the sentinel stops the accepted recover path safely.
        class StopAfterToolGate(Exception): pass
        for step in ("run", "recover"):
            calls = []
            with mock.patch.object(environment, "require_windows_execution"), \
                 mock.patch.object(environment, "resolve_cli", side_effect=unavailable_docker(calls)), \
                 mock.patch.object(environment, "assert_live"), \
                 mock.patch.object(sr, "require_prior_outer_drained", side_effect=StopAfterToolGate):
                args = types.SimpleNamespace(step=step, live=True)
                if step == "run":
                    with self.assertRaisesRegex(ValueError, "docker CLI missing"):
                        sr.execute_case(args)
                    self.assertEqual(calls, ["kubectl", "docker"])
                else:
                    with self.assertRaises(StopAfterToolGate):
                        sr.execute_case(args)
                    self.assertEqual(calls, ["kubectl"])

        # Incremental child validates only a synthetic sealed condition, then
        # proves the same missing-Docker behavior without reaching a collector.
        science = {"fixture": "incremental-science"}
        condition_file = self.root / "condition-recovery.json"
        condition_file.write_text(json.dumps([{"condition_id": "fixture-condition",
            "contract": {"scenario": {"scenario_id": "S29"}, "purpose": "formal",
                          "protocol_status": "formal_frozen"}}]), encoding="utf-8")
        for step in ("run", "recover"):
            calls = []
            with mock.patch.object(gw, "condition_rows", return_value=[{"scenario_id": "S29", "science": science}]), \
                 mock.patch("scripts.collection.review_samples.semantic_signature", return_value=science), \
                 mock.patch.object(environment, "require_windows_execution"), \
                 mock.patch.object(environment, "resolve_cli", side_effect=unavailable_docker(calls)), \
                 mock.patch.object(environment, "credentials", return_value={key: "fixture" for key in
                     ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")}) as credentials, \
                 mock.patch.object(gw.cd, "main", return_value=0) as child, \
                 mock.patch.dict(os.environ, {}, clear=False):
                argv = [step, "--condition-file", condition_file.as_posix(), "--condition-id", "fixture-condition", "--live"]
                if step == "run":
                    with self.assertRaisesRegex(ValueError, "docker CLI missing"):
                        gw.driver_main(argv)
                    self.assertEqual(calls, ["kubectl", "docker"])
                    credentials.assert_not_called(); child.assert_not_called()
                else:
                    self.assertEqual(gw.driver_main(argv), 0)
                    self.assertEqual(calls, ["kubectl"])
                    credentials.assert_called_once(); child.assert_called_once()

    def test_posix_live_guards_precede_cli_credentials_and_runtime_calls(self):
        science = {"fixture": "semantic-signature"}
        condition_file = self.root / "condition.json"
        condition_file.write_text(json.dumps([{"condition_id": "fixture-condition",
            "contract": {"scenario": {"scenario_id": "S29"}, "purpose": "formal",
                          "protocol_status": "formal_frozen"}}]), encoding="utf-8")
        with mock.patch.object(environment.os, "name", "posix"), \
             mock.patch.object(environment, "resolve_cli") as resolver, \
             mock.patch("subprocess.run") as run, mock.patch("subprocess.Popen") as popen:
            with self.assertRaisesRegex(ValueError, "Windows control host"):
                run_campaign.run_round(self.root / "not-read.json", 1, execute=True, resume=False)
            with self.assertRaisesRegex(ValueError, "Windows control host"):
                sr.execute_case(None)
            resolver.assert_not_called(); run.assert_not_called(); popen.assert_not_called()
            with mock.patch.object(environment, "apply"), mock.patch.object(environment, "credentials") as creds, \
                 mock.patch.object(sr, "main") as child, mock.patch.object(run_scenario.sys, "argv", ["child.py", "run"]):
                with self.assertRaisesRegex(ValueError, "Windows control host"):
                    run_scenario.main()
                creds.assert_not_called(); child.assert_not_called(); resolver.assert_not_called()
            with mock.patch.object(gw, "condition_rows",
                                   return_value=[{"scenario_id": "S29", "science": science}]), \
                 mock.patch("scripts.collection.review_samples.semantic_signature", return_value=science), \
                 mock.patch.object(environment, "credentials") as creds, \
                 mock.patch.object(gw.cd, "main") as driver, \
                 mock.patch.object(gw, "Path", type(self.root)):
                for step in ("run", "recover"):
                    with self.subTest(incremental_step=step):
                        with self.assertRaisesRegex(ValueError, "Windows control host"):
                            gw.driver_main([step, "--condition-file", condition_file.as_posix(),
                                "--condition-id", "fixture-condition", "--live"])
                creds.assert_not_called(); driver.assert_not_called(); resolver.assert_not_called()

    def test_offline_help_status_plan_prepare_and_preview_do_not_inherit_live_gate(self):
        with mock.patch.object(environment, "require_windows_execution", side_effect=AssertionError("offline")), \
             mock.patch.object(environment, "resolve_cli", side_effect=AssertionError("offline")), \
             mock.patch.object(environment, "apply", side_effect=AssertionError("help loads no environment")), \
             mock.patch.object(gw, "install", side_effect=AssertionError("help installs nothing")), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(gw.main(["--help"]), 0)

        campaign = {"campaign_id": "fixture", "budget": {"new_slots": 2}, "scenarios": [{}, {}]}
        with mock.patch.object(run_campaign, "load_campaign", return_value=(campaign, "sha", self.root / "plan.json")), \
             mock.patch.object(run_campaign, "_rounds", return_value=(1,)), \
             mock.patch.object(run_campaign, "_scenario_count", return_value=2), \
             mock.patch.object(run_campaign, "status", return_value={"status": "synthetic"}), \
             mock.patch.object(environment, "require_windows_execution", side_effect=AssertionError("offline")), \
             mock.patch.object(environment, "resolve_cli", side_effect=AssertionError("offline")), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(run_campaign.main(["--plan", str(self.root / "plan.json")]), 0)
            self.assertEqual(run_campaign.main(["--status", "--plan", str(self.root / "plan.json")]), 0)

        with mock.patch.object(gw, "ROOT", self.root), mock.patch.object(gw.cd.portable_env, "apply"), \
             mock.patch.object(gw, "condition_rows", return_value=[]), \
             mock.patch("scripts.collection.prepare_campaign.prepare", return_value=self.root / "plan.json"), \
             mock.patch.object(environment, "require_windows_execution", side_effect=AssertionError("offline")), \
             mock.patch.object(environment, "resolve_cli", side_effect=AssertionError("offline")), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(gw.prepare_main(["--output-dir", str(self.root / "new-plan"), "--campaign-id", "fixture",
                "--rounds", "1", "--environment", str(self.root / "env.json")]), 0)
        with mock.patch.object(run_scenario.environment, "apply"), mock.patch.object(sr, "main") as child, \
             mock.patch.object(environment, "require_windows_execution", side_effect=AssertionError("preview offline")), \
             mock.patch.object(environment, "resolve_cli", side_effect=AssertionError("preview offline")), \
             mock.patch.object(environment, "credentials", side_effect=AssertionError("preview offline")), \
             mock.patch.object(run_scenario.sys, "argv", ["child.py", "preview"]):
            run_scenario.main(); child.assert_called_once()


if __name__ == "__main__":
    unittest.main()
