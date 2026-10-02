"""Offline portable-binding and fail-closed regression checks. No infrastructure required."""
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from scripts.collection import environment as env
from scripts.collection import scenario_runner as cd, campaign_runtime, run_campaign
from scripts.collection import scenario_definitions, contract, journal, prepare_campaign
from scripts.environment import start_collection_environment as startup


class PortableCollectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="recshop-portable-", dir=os.environ.get("RECSHOP_TEST_TMP"))
        self.root = Path(self.tmp.name)
        self.before = os.environ.get(env.ENV_KEY)
        self.doc = json.loads((env.ROOT / "configs/collection/environment.example.json").read_text())
        self.doc["binding_mode"] = "offline_fixture"
        self.doc.update(kube_context="offline-test", namespace="fixture-apps", node_name="fixture-node")
        for i, key in enumerate(("cluster_uid", "namespace_uid", "node_uid"), 1):
            self.doc[key] = str(uuid.UUID(int=i))
        self.doc["database"].update(name="fixture_db", server_uuid=str(uuid.UUID(int=4)), checksums={"items": "1", "inventory": "2"})
        self.doc["paths"] = {key: str(self.root / key) for key in self.doc["paths"]}
        self.path = self.root / "environment.json"
        self.save()
        env.apply(cd, self.path)

    def tearDown(self):
        env._active = env._path = None
        if self.before is None:
            os.environ.pop(env.ENV_KEY, None)
        else:
            os.environ[env.ENV_KEY] = self.before
        self.tmp.cleanup()

    def save(self):
        self.path.write_text(json.dumps(self.doc), encoding="utf-8")

    def test_missing_binding_rejected(self):
        env._active = env._path = None
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(ValueError):
            env.current()

    def test_example_is_not_a_live_binding(self):
        with self.assertRaises((ValueError, KeyError)):
            env.load(env.ROOT / "configs/collection/environment.example.json")

    def test_config_load_has_no_process_or_network_io(self):
        with patch("subprocess.run") as process, patch("urllib.request.urlopen") as network:
            loaded = env.load(self.path)
            self.assertEqual(loaded["namespace"], "fixture-apps")
            process.assert_not_called(); network.assert_not_called()

    def test_fixture_rejected_before_any_live_adapter(self):
        with patch("subprocess.run") as process, patch("subprocess.Popen") as spawn, patch("urllib.request.urlopen") as network:
            for callback in (lambda: cd.reader(True), lambda: cd.execute_case(None), campaign_runtime.route_baseline,
                             campaign_runtime.prom_baseline, env.credentials):
                with self.assertRaises(ValueError):
                    callback()
            process.assert_not_called(); spawn.assert_not_called(); network.assert_not_called()

    def test_fixture_batch_execute_rejected_even_with_environment_ref(self):
        plan = self.root / "campaign.json"
        plan.write_text(json.dumps({"runtime": {"environment": env.binding_ref()}}))
        with patch("subprocess.run") as process, patch("subprocess.Popen") as spawn:
            with self.assertRaises(ValueError):
                run_campaign.run_round(plan, 1, execute=True, resume=False)
            process.assert_not_called(); spawn.assert_not_called()

    def test_binding_file_drift_rejected(self):
        reference = env.binding_ref()
        self.doc["namespace"] = "changed"
        self.save()
        with self.assertRaises(ValueError):
            env.verify_ref(reference)

    def test_paths_cannot_overlap_source(self):
        self.doc["paths"]["output_root"] = "scripts/overlap"
        self.save()
        with self.assertRaises(ValueError):
            env.load(self.path)

    def test_protected_data_overlap_rejected(self):
        self.doc["paths"]["output_root"] = str(self.root / "protected/sub")
        self.doc["protected_roots"] = [str(self.root / "protected")]
        self.save()
        with self.assertRaises(ValueError):
            env.load(self.path)

    def test_runtime_roots_must_not_nest(self):
        self.doc["paths"]["lease_root"] = self.doc["paths"]["output_root"] + "/lease"
        self.save()
        with self.assertRaises(ValueError):
            env.load(self.path)

    def test_global_registry_not_overridden_by_output_path(self):
        from scripts.collection import runner
        original = runner.environment_registry_root()
        self.doc["paths"]["output_root"] = str(self.root / "new-output")
        self.save(); env.load(self.path)
        self.assertEqual(original, runner.environment_registry_root())

    def test_two_catalogs_have_69_unique_versioned_definitions(self):
        base = scenario_definitions.load_registry(repo_root=env.ROOT)
        extra = contract.DesignRegistry.from_json((env.ROOT / 'configs/collection/gateway_cpu_network_scenarios.json').read_bytes())
        self.assertEqual(len(base.scenarios), 66)
        self.assertEqual({s.key.scenario_id for s in extra.scenarios}, {"S29", "S30", "D34"})
        self.assertEqual(len({s.key for s in (*base.scenarios, *extra.scenarios)}), 69)

    def test_condition_binding_changes_locations_not_doses(self):
        source = json.loads(prepare_campaign.DEFAULT_CONDITIONS.read_text())["conditions"]
        rows = prepare_campaign.load_conditions(prepare_campaign.DEFAULT_CONDITIONS, self.root / "raw")
        self.assertEqual(len(rows), 66)
        for before, after in zip(source, rows):
            old, new = before["seed"]["contract"], after["seed"]["contract"]
            self.assertEqual(old["metric_interval_s"], new["metric_interval_s"])
            for a, b in zip(old["faults"], new["faults"]):
                self.assertEqual(a["parameters"], b["parameters"])
                self.assertEqual(a["normalized_root_entity"], b["normalized_root_entity"])
                self.assertEqual(a["planned_window"], b["planned_window"])
            self.assertEqual(new["context"]["namespace"], "fixture-apps")
            self.assertTrue(all(s["entrypoint"] == "http://127.0.0.1:18005" for s in new["request_profile"]["streams"]))

    def test_same_repo_output_policy_preserves_data_guard(self):
        policy = journal.OutputPolicy.define(repo_root=env.ROOT, source_repo_root=env.ROOT,
                                             approved_root=self.root / "raw", protected_roots=env.protected_roots())
        policy.check_root()
        with self.assertRaises(journal.JournalError):
            journal.OutputPolicy.define(repo_root=env.ROOT, source_repo_root=env.ROOT,
                                        approved_root=env.ROOT / "datasets/new", protected_roots=env.protected_roots())

    def container(self, name, source):
        image, project, service, ports = startup.CONTAINERS[name]
        destination, _ = startup.MOUNTS[name]
        return {"Name": "/" + name, "Id": "fixture-id", "State": {"Status": "running"},
                "Config": {"Image": image, "Labels": {"com.docker.compose.project": project,
                                                       "com.docker.compose.service": service}},
                "HostConfig": {"PortBindings": {port: [{"HostPort": host}] for port, host in ports.items()}},
                "NetworkSettings": {"Networks": {self.doc["windows_startup"]["container_network"]: {}}},
                "Mounts": [{"Destination": destination, "Source": source, "Type": "bind", "RW": False},
                           {"Destination": "/prometheus", "Type": "volume",
                            "Name": self.doc["windows_startup"]["prometheus_volume"]}]}

    def test_default_mount_layout_remains_without_mapping(self):
        self.doc["windows_startup"].pop("bind_sources", None)
        self.save(); env.load(self.path)
        for name, (_, suffix) in startup.MOUNTS.items():
            self.assertEqual(startup.container_identity(self.container(name, "C:/fixture/" + suffix), name), "running")
        name = "recshop-m1-otel-metrics"
        with self.assertRaises(startup.Blocked):
            startup.container_identity(self.container(name, "C:/fixture/ops/m1/otel-metrics-config.yaml"), name)

    def test_explicit_current_legacy_unix_and_windows_sources(self):
        name = "recshop-m1-otel-metrics"
        for source, observed in (("C:/fixture/ops/metrics/otel-metrics-config.yaml", "C:/fixture/ops/metrics/otel-metrics-config.yaml"),
                                 (r"C:\fixture\ops\m1\otel-metrics-config.yaml", "C:/fixture/ops/m1/otel-metrics-config.yaml"),
                                 ("/srv/Fixture/ops/m1/config.yaml", "/srv/Fixture/ops/m1/config.yaml"),
                                 (r"\\server\share\config.yaml", "//server/share/config.yaml")):
            with self.subTest(source=source):
                self.doc["windows_startup"]["bind_sources"] = {name: source}
                self.save(); env.load(self.path)
                self.assertEqual(startup.container_identity(self.container(name, observed), name), "running")

    def test_explicit_source_never_matches_suffix_or_linux_case(self):
        name = "recshop-m1-otel-metrics"
        self.doc["windows_startup"]["bind_sources"] = {name: "/srv/Fixture/ops/metrics/otel-metrics-config.yaml"}
        self.save(); env.load(self.path)
        for source in ("/another/ops/metrics/otel-metrics-config.yaml", "/srv/fixture/ops/metrics/otel-metrics-config.yaml"):
            with self.subTest(source=source), self.assertRaises(startup.Blocked):
                startup.container_identity(self.container(name, source), name)

    def test_explicit_source_preserves_mount_and_container_guards(self):
        name, source = "recshop-m1-prometheus", "C:/fixture/ops/m1/prometheus-container-resources.yml"
        self.doc["windows_startup"]["bind_sources"] = {name: source}
        self.save(); env.load(self.path)
        original = self.container(name, source)
        self.assertEqual(startup.container_identity(original, name), "running")
        mutations = [lambda d: d["Mounts"][0].update(RW=True),
                     lambda d: d["Mounts"][0].update(Type="volume"),
                     lambda d: d["Mounts"][0].update(Destination="/other/config"),
                     lambda d: d["Mounts"].pop(0),
                     lambda d: d["Mounts"].append(copy.deepcopy(d["Mounts"][0])),
                     lambda d: d["Config"].update(Image="other-image"),
                     lambda d: d["Config"]["Labels"].update({"com.docker.compose.project": "other"}),
                     lambda d: d["Config"]["Labels"].update({"com.docker.compose.service": "other"}),
                     lambda d: d["HostConfig"]["PortBindings"]["9090/tcp"][0].update(HostPort="19091"),
                     lambda d: d["NetworkSettings"].update(Networks={"other": {}}),
                     lambda d: d["Mounts"][1].update(Name="other-volume"),
                     lambda d: d["Mounts"].pop(1)]
        for index, mutate in enumerate(mutations):
            with self.subTest(mutation=index):
                doc = copy.deepcopy(original); mutate(doc)
                with self.assertRaises(startup.Blocked):
                    startup.container_identity(doc, name)

    def test_invalid_bind_config_rejected_before_host_construction(self):
        name = "recshop-m1-otel-metrics"
        bad = [None, [], {"unknown": "/absolute/file"}, {"recweb2-jaeger": "/absolute/file"},
               {"": "/absolute/file"}]
        bad += [{name: value} for value in ("", "relative/file", "C:relative", "C:/base/../file",
                                             "/base/../file", "/base/./file", " /absolute", 1, "C:/bad\nfile")]
        for mapping in bad:
            with self.subTest(mapping=mapping):
                self.doc["windows_startup"]["bind_sources"] = mapping
                self.save()
                with self.assertRaises(ValueError):
                    env.load(self.path)
                with patch.object(startup, "Host") as host, patch("builtins.print"):
                    self.assertEqual(startup.main(["--environment", str(self.path), "--check-only"]), 2)
                    host.assert_not_called()


if __name__ == "__main__":
    unittest.main()
