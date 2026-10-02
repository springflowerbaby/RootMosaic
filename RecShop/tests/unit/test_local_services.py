"""Offline readiness and ownership tests for the local RecShop launcher."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("recshop_local_services_under_test", ROOT / "scripts/entrypoints/start_local_services.py")
APP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(APP)
ALL_SERVICES = copy.deepcopy(APP.SERVICES)


class Process:
    def __init__(self, pid=100, returncode=None):
        self.pid = pid
        self.returncode = returncode
        self.terminated = False
        self.killed = False

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = 0

    def kill(self):
        self.killed = True
        self.returncode = -9

    def wait(self, timeout=None):
        return self.returncode


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class LocalReadinessTests(unittest.TestCase):
    def setUp(self):
        self.saved = {name: getattr(APP, name) for name in (
            "load_configuration", "check_health", "wait_for_service", "start_otel_stack",
            "otel_stack_ready", "start_nacos", "_write_result")}
        self.proc = Process()
        self.reports = []
        svc = next(copy.deepcopy(s) for s in ALL_SERVICES if s["cwd"].name == "user_service")
        self.patch(APP, "SERVICES", [svc])
        self.patch(APP, "processes", [])
        self.patch(APP, "load_configuration", return_value={"NACOS_ENABLED": "false"})
        self.patch(APP, "_port_open", return_value=False)
        self.patch(APP, "check_health", return_value=True)
        self.patch(APP, "wait_for_service", return_value=True)
        self.patch(APP, "ensure_docker_running", return_value=True)
        self.patch(APP, "start_otel_stack", return_value=True)
        self.patch(APP, "docker_available", return_value=True)
        self.patch(APP, "otel_stack_ready", return_value=True)
        self.patch(APP, "start_nacos", return_value=True)
        self.patch(APP, "nacos_ready", return_value=True)
        self.patch(APP, "_write_result", side_effect=self.save_report)
        self.patch(APP.subprocess, "Popen", return_value=self.proc)
        self.patch(APP.subprocess, "run", side_effect=AssertionError("unexpected real command"))
        self.patch(APP.socket, "create_connection", side_effect=AssertionError("unexpected socket"))
        self.patch(APP.time, "sleep", side_effect=KeyboardInterrupt)
        self.patch(APP, "info")
        self.patch(APP, "warn")
        self.patch(APP, "ok")
        self.patch(APP, "err")

    def patch(self, owner, name, *args, **kwargs):
        p = mock.patch.object(owner, name, *args, **kwargs)
        obj = p.start()
        self.addCleanup(p.stop)
        return obj

    def save_report(self, result, path=None):
        self.reports.append(copy.deepcopy(result))
        return True

    def test_original_service_inventory_is_25(self):
        self.assertEqual(len(ALL_SERVICES), 25)
        self.assertEqual(len({s["port"] for s in ALL_SERVICES}), 25)

    def test_otel_start_failure_is_nonzero_and_not_active(self):
        APP.start_otel_stack.return_value = False
        self.assertEqual(APP.start_all(), 1)
        self.assertFalse(APP._otel_stack_active)
        APP.subprocess.Popen.assert_not_called()
        self.assertEqual(self.reports[-1]["dependencies"]["otel"], "FAILED")

    def test_docker_failure_does_not_attempt_compose_or_app(self):
        APP.ensure_docker_running.return_value = False
        self.assertEqual(APP.start_all(), 1)
        APP.start_otel_stack.assert_not_called()
        APP.subprocess.Popen.assert_not_called()

    def test_enabled_nacos_failure_is_nonzero(self):
        APP.load_configuration.return_value = {"NACOS_ENABLED": "true"}
        APP.start_nacos.return_value = False
        self.assertEqual(APP.start_all(no_docker=True), 1)
        APP.subprocess.Popen.assert_not_called()
        self.assertEqual(self.reports[-1]["dependencies"]["nacos"], "FAILED")

    def test_disabled_nacos_is_explicitly_skipped(self):
        APP._port_open.return_value = True
        self.assertEqual(APP.start_all(no_docker=True), 0)
        APP.start_nacos.assert_not_called()
        self.assertEqual(self.reports[-1]["dependencies"], {"otel": "SKIPPED", "nacos": "SKIPPED"})

    def test_immediate_application_exit_fails_and_cleans_owned_only(self):
        self.proc.returncode = 1
        APP.wait_for_service.return_value = False
        self.assertEqual(APP.start_all(no_docker=True), 1)
        self.assertEqual(self.reports[-1]["services"][0]["status"], "EXITED")
        self.assertEqual(APP.processes, [])
        APP.subprocess.run.assert_not_called()

    def test_health_timeout_terminates_our_process(self):
        APP.wait_for_service.return_value = False
        self.assertEqual(APP.start_all(no_docker=True), 1)
        self.assertTrue(self.proc.terminated)
        self.assertEqual(self.reports[-1]["services"][0]["status"], "HEALTH_TIMEOUT")
        APP.subprocess.run.assert_not_called()

    def test_unhealthy_occupied_port_is_not_killed_or_replaced(self):
        APP._port_open.return_value = True
        APP.check_health.return_value = False
        self.assertEqual(APP.start_all(no_docker=True), 1)
        APP.subprocess.Popen.assert_not_called()
        APP.subprocess.run.assert_not_called()
        self.assertEqual(self.reports[-1]["services"][0]["status"], "UNRECOGNIZED_OR_UNHEALTHY")

    def test_successfully_reused_service_is_not_owned(self):
        APP._port_open.return_value = True
        self.assertEqual(APP.start_all(no_docker=True), 0)
        APP.subprocess.Popen.assert_not_called()
        self.assertEqual(self.reports[-1]["status"], "READY")
        self.assertEqual(self.reports[-1]["services"][0]["ownership"], "external")

    def test_owned_ready_then_interrupt_only_cleans_our_process(self):
        external = Process(pid=200)
        APP.processes.append(external)
        self.assertEqual(APP.start_all(no_docker=True), 130)
        self.assertEqual([x["status"] for x in self.reports], ["READY", "STOPPED"])
        self.assertTrue(self.proc.terminated)
        self.assertFalse(external.terminated)
        self.assertEqual(APP.processes, [external])
        APP.subprocess.run.assert_not_called()

    def test_later_child_exit_returns_nonzero(self):
        # Startup wait passes; the supervisor then observes the unexpected exit.
        self.proc.returncode = 0
        self.assertEqual(APP.start_all(no_docker=True), 1)
        self.assertEqual([x["status"] for x in self.reports], ["READY", "FAILED"])
        self.assertEqual(self.reports[-1]["services"][0]["status"], "EXITED")

    def test_popen_exception_cleans_prior_owned_child(self):
        second = copy.deepcopy(APP.SERVICES[0])
        second["name"] = "Second"
        second["cwd"] = ROOT / "services" / "cart_service"
        second["port"] = 5006
        APP.SERVICES.append(second)
        APP.subprocess.Popen.side_effect = [self.proc, OSError("sensitive detail must not be emitted")]
        self.assertEqual(APP.start_all(no_docker=True), 1)
        self.assertTrue(self.proc.terminated)
        self.assertNotIn("sensitive detail", json.dumps(self.reports[-1]))

    def test_check_only_reads_all_apps_and_starts_nothing(self):
        APP.SERVICES[:] = copy.deepcopy(ALL_SERVICES)
        APP.load_configuration.return_value = {"NACOS_ENABLED": "true"}
        self.assertEqual(APP.start_all(check_only=True), 0)
        APP.subprocess.Popen.assert_not_called()
        APP.ensure_docker_running.assert_not_called()
        APP.start_otel_stack.assert_not_called()
        APP.start_nacos.assert_not_called()
        APP.check_health.assert_called()
        self.assertEqual(len(self.reports[-1]["services"]), 25)

    def test_check_only_reports_failed_app_without_cleanup(self):
        APP.check_health.return_value = False
        self.assertEqual(APP.start_all(no_docker=True, check_only=True), 1)
        APP.subprocess.Popen.assert_not_called()
        APP.subprocess.run.assert_not_called()
        self.assertEqual(self.reports[-1]["status"], "FAILED")

    def test_check_dependency_failure_never_starts_dependencies(self):
        APP.otel_stack_ready.return_value = False
        self.assertEqual(APP.start_all(check_only=True), 1)
        APP.start_otel_stack.assert_not_called()
        APP.start_nacos.assert_not_called()
        APP.subprocess.Popen.assert_not_called()

    def test_check_uses_actual_port_and_identity(self):
        APP.load_configuration.return_value = {"NACOS_ENABLED": "false", "USER_SERVICE_PORT": "15004"}
        self.assertEqual(APP.start_all(no_docker=True, check_only=True), 0)
        APP.check_health.assert_called_once_with("http://127.0.0.1:15004/health", expected="user_service")

    def test_configuration_environment_overrides_file_without_printing(self):
        with mock.patch("dotenv.dotenv_values", return_value={"DB_PASSWORD": "fixture-only", "NACOS_ENABLED": "true"}), \
             mock.patch.dict(APP.os.environ, {"NACOS_ENABLED": "false"}, clear=True):
            value = self.saved["load_configuration"]()
        self.assertEqual(value["NACOS_ENABLED"], "false")
        self.assertEqual(value["DB_PASSWORD"], "fixture-only")
        APP.info.assert_not_called()

    def test_wait_deadline_bounds_last_request(self):
        clock = Clock()
        APP.time.sleep.side_effect = clock.sleep
        self.patch(APP.time, "monotonic", side_effect=clock.monotonic)
        svc = APP.configured_services({"NACOS_ENABLED": "false"}, 180, 30)[0]
        svc["wait"] = 0.3

        def failed_health(url, timeout, expected):
            clock.now += timeout
            return False

        APP.check_health.side_effect = failed_health
        self.assertFalse(self.saved["wait_for_service"](svc, self.proc))
        self.assertLessEqual(clock.now, 0.3)
        self.assertEqual(APP.check_health.call_args.kwargs["timeout"], 0.3)

    def test_wait_immediate_exit_does_not_probe(self):
        self.proc.returncode = 1
        svc = APP.configured_services({}, 180, 30)[0]
        self.assertFalse(self.saved["wait_for_service"](svc, self.proc))
        APP.check_health.assert_not_called()

    def test_wait_does_not_accept_child_that_exits_during_health(self):
        proc = mock.Mock()
        proc.poll.side_effect = [None, 1, 1]
        APP.time.sleep.side_effect = None
        svc = APP.configured_services({}, 180, 30)[0]
        self.assertFalse(self.saved["wait_for_service"](svc, proc))

    def test_foreign_http200_payload_is_rejected(self):
        self.assertFalse(APP._health_payload_ok({"status": "healthy"}, "user_service"))
        self.assertFalse(APP._health_payload_ok({"status": "healthy", "service": "other"}, "user_service"))
        self.assertFalse(APP._health_payload_ok({}, "backend_api"))

    def test_degraded_backend_is_rejected(self):
        self.assertFalse(APP._health_payload_ok(
            {"status": "degraded", "database": "unhealthy", "sasrec_api": "healthy"}, "backend_api"))

    def test_unloaded_sasrec_and_nested_dependency_are_rejected(self):
        model = {"status": "model_not_loaded", "model_loaded": False, "dataset_info": None}
        self.assertFalse(APP._health_payload_ok(model, "sasrec_api"))
        self.assertFalse(APP._health_payload_ok(
            {"recommendation_system": "healthy", "sasrec_service": model}, "recommendation_agent"))

    def test_known_service_contracts_pass(self):
        model = {"status": "healthy", "model_loaded": True,
                 "dataset_info": {"user_num": 1, "item_num": 2, "interaction_num": 3}}
        self.assertTrue(APP._health_payload_ok(model, "sasrec_api"))
        self.assertTrue(APP._health_payload_ok(
            {"recommendation_system": "healthy", "sasrec_service": model}, "recommendation_agent"))
        self.assertTrue(APP._health_payload_ok(
            {"status": "healthy", "database": "healthy", "sasrec_api": "healthy"}, "backend_api"))
        self.assertTrue(APP._health_payload_ok({"status": "ok", "service": "shop_web"}, "shop_web"))

    def test_http_probe_disables_proxy_and_rejects_non_json(self):
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.status = 200
        response.read.return_value = b"<html>not our service</html>"
        opener = mock.Mock()
        opener.open.return_value = response
        with mock.patch.object(APP.urllib.request, "build_opener", return_value=opener):
            self.assertFalse(self.saved["check_health"]("http://127.0.0.1:5004/health", expected="user_service"))
        opener.open.assert_called_once()

    def test_otel_partial_stack_rejected(self):
        APP.subprocess.run.side_effect = None
        APP.subprocess.run.return_value = mock.Mock(returncode=0, stdout='[{"Service":"otel-collector","State":"running","Health":""}]')
        APP._port_open.return_value = True
        self.assertFalse(self.saved["otel_stack_ready"]())

    def test_otel_known_complete_stack_passes(self):
        APP.subprocess.run.side_effect = None
        rows = [{"Service": name, "State": "running", "Health": ""} for name in APP._OTEL_SERVICES]
        APP.subprocess.run.return_value = mock.Mock(returncode=0, stdout="\n".join(json.dumps(x) for x in rows))
        APP._port_open.return_value = True
        self.assertTrue(self.saved["otel_stack_ready"]())

    def test_running_containers_with_any_unready_backend_are_rejected(self):
        APP.subprocess.run.side_effect = None
        rows = [{"Service": name, "State": "running", "Health": ""} for name in APP._OTEL_SERVICES]
        APP.subprocess.run.return_value = mock.Mock(returncode=0, stdout=json.dumps(rows))
        APP._port_open.return_value = True
        for failing, _ in APP._OTEL_READY_ENDPOINTS:
            with self.subTest(backend=failing):
                APP.check_health.side_effect = lambda url, timeout, expected: expected != failing
                self.assertFalse(self.saved["otel_stack_ready"]())

    def test_backend_readiness_response_contracts(self):
        responses = {
            "prometheus": b"Prometheus Server is Ready.\n",
            "loki": b"ready\n",
            "grafana": b'{"database":"ok","version":"11.6.1"}',
            "jaeger": b'{"data":[],"errors":null}'
        }
        for expected, raw in responses.items():
            with self.subTest(backend=expected):
                response = mock.MagicMock()
                response.__enter__.return_value = response
                response.status = 200
                response.read.return_value = raw
                opener = mock.Mock()
                opener.open.return_value = response
                with mock.patch.object(APP.urllib.request, "build_opener", return_value=opener):
                    self.assertTrue(self.saved["check_health"]("http://127.0.0.1/health", expected=expected))
                    response.read.return_value = b'{"database":"failed","version":"11.6.1","data":[],"errors":["not ready"]}'
                    self.assertFalse(self.saved["check_health"]("http://127.0.0.1/health", expected=expected))

    def test_otel_backend_probe_respects_shared_deadline(self):
        clock = Clock()
        self.patch(APP.time, "monotonic", side_effect=clock.monotonic)
        APP.subprocess.run.side_effect = None
        rows = [{"Service": name, "State": "running", "Health": ""} for name in APP._OTEL_SERVICES]
        APP.subprocess.run.return_value = mock.Mock(returncode=0, stdout=json.dumps(rows))
        APP._port_open.return_value = True

        def slow_health(url, timeout, expected):
            clock.now += timeout
            return True

        APP.check_health.side_effect = slow_health
        self.assertFalse(self.saved["otel_stack_ready"](timeout=0.4))
        self.assertEqual(clock.now, 0.4)

    def test_compose_command_failure_returns_false(self):
        APP.subprocess.run.side_effect = None
        APP.subprocess.run.return_value = mock.Mock(returncode=1)
        self.assertFalse(self.saved["start_otel_stack"]())

    def test_remote_nacos_failure_does_not_start_local_registry(self):
        APP.nacos_ready.return_value = False
        self.assertFalse(self.saved["start_nacos"]({"NACOS_ENABLED": "true", "NACOS_SERVER_ADDRESSES": "example.invalid:8848"}))
        APP.subprocess.Popen.assert_not_called()

    def test_cleanup_kill_fallback_is_limited_to_owned_handle(self):
        own = Process(pid=321)
        external = Process(pid=654)
        own.wait = mock.Mock(side_effect=[
            APP.subprocess.TimeoutExpired("owned", 5), 0])
        APP.processes[:] = [own, external]
        APP.shutdown_all([own])
        self.assertTrue(own.terminated)
        self.assertTrue(own.killed)
        self.assertFalse(external.terminated)
        self.assertFalse(external.killed)
        self.assertEqual(APP.processes, [external])
        APP.subprocess.run.assert_not_called()

    def test_status_report_omits_configuration_secrets(self):
        APP.load_configuration.return_value = {
            "NACOS_ENABLED": "false",
            "DB_PASSWORD": "fixture-private-password",
            "DEEPSEEK_API_KEY": "fixture-private-api-key"}
        self.assertEqual(APP.start_all(no_docker=True, check_only=True), 0)
        encoded = json.dumps(self.reports[-1])
        self.assertNotIn("fixture-private-password", encoded)
        self.assertNotIn("fixture-private-api-key", encoded)
        self.assertNotIn("DB_PASSWORD", encoded)

    def test_invalid_timeouts_are_rejected_before_dependencies(self):
        self.assertEqual(APP.start_all(model_timeout=901), 1)
        self.assertEqual(APP.start_all(service_timeout=0), 1)
        APP.ensure_docker_running.assert_not_called()
        APP.subprocess.Popen.assert_not_called()

    def test_independent_stop_refuses_global_process_scan(self):
        self.assertEqual(APP.main(["--stop"]), 2)
        APP.subprocess.run.assert_not_called()
        APP.subprocess.Popen.assert_not_called()
        APP.load_configuration.assert_not_called()

    def test_optional_json_is_explicit_output_only(self):
        with tempfile.TemporaryDirectory(prefix="recshop-readiness-test-") as directory:
            base = Path(directory).resolve()
            self.assertEqual(base.parent, Path(tempfile.gettempdir()).resolve())
            target = base / "status.json"
            self.assertTrue(self.saved["_write_result"]({"status": "READY", "services": []}, target))
            self.assertEqual(json.loads(target.read_text(encoding="utf-8"))["status"], "READY")


if __name__ == "__main__":
    unittest.main()
