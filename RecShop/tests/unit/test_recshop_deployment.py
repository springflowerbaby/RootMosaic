"""Offline CLI integration tests; only the stateful fake-kubectl fixture is executed."""
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from scripts.deployment import recshop


class PortableDeploymentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="RecShop portable test ")
        self.root = Path(self.temp.name)
        self.config = json.loads((recshop.ROOT / "deployment.example.json").read_text())
        self.config["context"] = "offline-fixture-context"
        self.config["kubectl"] = [sys.executable, str(recshop.ROOT / "tests/fixtures/fake_kubectl_recshop.py")]
        self.config["assets"]["local_dir"] = "local assets"
        self.config["timeouts"] = {"command_seconds": 15, "rollout_seconds": 15, "asset_copy_seconds": 15}
        self.assets = self.root / "local assets"
        self.assets.mkdir()
        for role, spec in self.config["assets"]["files"].items():
            data = ("OFFLINE-FIXTURE-" + role).encode()
            (self.assets / spec["name"]).write_bytes(data)
            spec["sha256"] = hashlib.sha256(data).hexdigest()
        self.cfg = self.root / "deployment.json"
        self.cluster = self.root / "fake-cluster.json"
        self.env = dict(os.environ, FAKE_KUBE_STATE=str(self.cluster),
                        RECSHOP_DB_PASSWORD="offline-test-db-password-only",
                        RECSHOP_DB_ROOT_PASSWORD="offline-test-root-password-only",
                        RECSHOP_FLASK_SECRET="offline-test-flask-secret-only")
        for k in ("FAKE_BAD_HEALTH", "FAKE_LEAK_SECRET_ERROR", "FAKE_HELPER_TERMINAL", "FAKE_REPLACE_BEFORE_PATCH"):
            self.env.pop(k, None)
        self.write_config()

    def tearDown(self):
        self.temp.cleanup()

    def write_config(self):
        self.cfg.write_text(json.dumps(self.config), encoding="utf-8")

    def run_cli(self, action, *extra, env=None, workspace=None):
        r = subprocess.run([sys.executable, "-B", "-X", "utf8", str((workspace or recshop.ROOT) / (action + "_recshop.py")),
                            "--config", str(self.cfg), *extra], cwd=self.root, env=env or self.env,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8", timeout=90)
        self.assertEqual(r.stderr, "", r.stderr)
        data = json.loads(r.stdout)
        for k in ("RECSHOP_DB_PASSWORD", "RECSHOP_DB_ROOT_PASSWORD", "RECSHOP_FLASK_SECRET"):
            self.assertNotIn(self.env[k], r.stdout)
            self.assertNotIn(base64.b64encode(self.env[k].encode()).decode(), r.stdout)
        return r.returncode, data

    def read_cluster(self):
        return json.loads(self.cluster.read_text())

    def test_render_is_offline_allowlisted_and_secret_free(self):
        self.config["kubectl"] = ["THIS_MUST_NEVER_RUN"]
        self.write_config()
        target = self.root / "render.json"
        code, data = self.run_cli("deploy", "--render", "--out", str(target))
        self.assertEqual(code, 0, data)
        plan = json.loads(target.read_text())["plan"]
        self.assertEqual(len(plan["apps"]), 50)
        self.assertFalse(self.cluster.exists())
        self.assertNotIn("catalog-bad", target.read_text())
        self.assertNotIn('"kind": "Secret"', target.read_text())
        self.assertNotIn("NetworkChaos", target.read_text())
        recagent = next(d for d in plan["apps"] if d["kind"] == "Deployment" and d["metadata"]["name"] == "rec-agent")
        self.assertEqual(recagent["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["memory"], "4Gi")

    def test_asset_helper_hash_checks_and_atomic_finalization_on_local_fixture(self):
        remote = self.root / "isolated asset volume"
        remote.mkdir()
        manifest = [{"name": p.name, "bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                    for p in self.assets.iterdir()]
        encoded = base64.b64encode(json.dumps(manifest).encode()).decode()
        # Only redirect the helper's fixed mount path into a disposable local fixture.
        script = recshop.ASSET_SCRIPT.replace("pathlib.Path('/assets')", "pathlib.Path(sys.argv[3])")
        def run(mode):
            return subprocess.run([sys.executable, "-B", "-c", script, mode, encoded, str(remote)],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=15)
        checked = run("inspect")
        self.assertEqual(checked.returncode, 0)
        self.assertEqual(len(json.loads(checked.stdout)["missing"]), 3)
        for p in self.assets.iterdir():
            shutil.copyfile(p, remote / ".recshop-incoming" / p.name)
        final = run("finalize")
        self.assertEqual(final.returncode, 0, final.stderr)
        self.assertEqual(json.loads(run("inspect").stdout)["missing"], [])
        wrong = remote / manifest[0]["name"]
        wrong.chmod(0o644)
        wrong.write_bytes(b"wrong replacement")
        self.assertEqual(run("inspect").returncode, 4)

    def test_empty_deploy_repeat_start_and_readonly_check(self):
        code, result = self.run_cli("deploy")
        self.assertEqual(code, 0, result)
        self.assertEqual(result["status"], "BUSINESS_READY")
        first = self.read_cluster()
        self.assertEqual(sum(x["kind"] == "Deployment" for x in first["objects"].values()), 26)
        self.assertEqual(sum(c["action"] == "cp" for c in first["calls"]), 3)
        self.assertNotIn("pod/recshop-assets-loader", first["objects"])
        code, data = self.run_cli("deploy")
        self.assertEqual(code, 0, data)
        second = self.read_cluster()
        self.assertEqual(sum(c["action"] == "cp" for c in second["calls"]), 3, "Repeated deploy must reuse verified assets")
        for key, item in first["objects"].items():
            self.assertEqual(item["metadata"]["uid"], second["objects"][key]["metadata"]["uid"])
        second["objects"]["deployment/catalog"]["spec"]["replicas"] = 0
        self.cluster.write_text(json.dumps(second))
        code, data = self.run_cli("start")
        self.assertEqual(code, 0, data)
        third = self.read_cluster()
        before = len(third["calls"])
        code, data = self.run_cli("check")
        self.assertEqual(code, 0, data)
        self.assertFalse(data["external_llm_called"])
        self.assertFalse(data["collection_ready"])
        after = self.read_cluster()
        self.assertTrue(all(x["action"] == "get" for x in after["calls"][before:]))
        extra = copy.deepcopy(after["objects"]["deployment/catalog"])
        extra["metadata"]["name"] = "unexpected-extra-app"
        after["objects"]["deployment/unexpected-extra-app"] = extra
        self.cluster.write_text(json.dumps(after))
        code, data = self.run_cli("check")
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "unexpected_deployment_roster")

    def test_protected_namespaces_and_missing_assets_fail_before_kube(self):
        for ns in ("recweb-chaos", "default", "kube-system", "recweb-chaos-copy"):
            self.config["namespace"] = ns
            self.write_config()
            code, data = self.run_cli("deploy")
            self.assertEqual(code, 2)
            self.assertEqual(data["reason"], "protected_namespace")
        self.config["namespace"] = "recshop-demo"
        self.write_config()
        next(self.assets.iterdir()).unlink()
        code, data = self.run_cli("deploy")
        self.assertEqual(code, 2)
        self.assertFalse(self.cluster.exists())

    def test_unowned_namespace_uid_drift_bad_health_and_image_drift(self):
        self.cluster.write_text(json.dumps({"objects": {"namespace/recshop-demo": {
            "kind": "Namespace", "metadata": {"name": "recshop-demo", "uid": "foreign", "labels": {}}}},
            "assets": {}, "stage": {}, "calls": [], "seq": 0}))
        code, data = self.run_cli("deploy")
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "namespace_exists_without_local_ownership_state")
        self.cluster.unlink()
        code, data = self.run_cli("deploy")
        self.assertEqual(code, 0, data)
        code, data = self.run_cli("check", env=dict(self.env, FAKE_BAD_HEALTH="sasrec"))
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "sasrec_model_not_loaded")
        state = self.read_cluster()
        state["objects"]["deployment/catalog"]["spec"]["template"]["spec"]["containers"][0]["image"] = "unexpected:latest"
        self.cluster.write_text(json.dumps(state))
        code, data = self.run_cli("start")
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "deployment_configuration_drift")
        state["objects"]["namespace/recshop-demo"]["metadata"]["uid"] = "recreated"
        self.cluster.write_text(json.dumps(state))
        code, data = self.run_cli("check")
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "resource_uid_changed")

    def test_secret_echo_and_rotation_are_rejected_without_disclosure(self):
        code, data = self.run_cli("deploy", env=dict(self.env, FAKE_LEAK_SECRET_ERROR="1"))
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "kubectl_failed:create")
        code, data = self.run_cli("deploy")
        self.assertEqual(code, 0, data)
        changed = dict(self.env, RECSHOP_DB_PASSWORD="different-offline-password")
        code, data = self.run_cli("deploy", env=changed)
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "secret_changed_rotation_not_automatic")

    def test_foreign_resource_missing_record_and_asset_mismatch(self):
        code, data = self.run_cli("deploy")
        self.assertEqual(code, 0, data)
        state = self.read_cluster()
        filename = next(iter(state["assets"]))
        state["assets"][filename]["sha256"] = "0" * 64
        self.cluster.write_text(json.dumps(state))
        code, data = self.run_cli("deploy")
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "kubectl_failed:exec")
        state = self.read_cluster()
        state["objects"]["deployment/catalog"]["metadata"]["labels"][recshop.OWNER] = "foreign"
        self.cluster.write_text(json.dumps(state))
        code, data = self.run_cli("check")
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "foreign_resource")

    def test_lock_collision_does_not_remove_another_operation_lock(self):
        folder = self.root / ".recshop-state"
        folder.mkdir()
        lock = folder / "recshop-demo.lock"
        lock.write_text("another-operation")
        code, data = self.run_cli("deploy")
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "instance_operation_already_running")
        self.assertEqual(lock.read_text(), "another-operation")

    def test_cas_rejects_replacement_after_read(self):
        code, data = self.run_cli("deploy")
        self.assertEqual(code, 0, data)
        self.config["images"]["catalog"] = "recweb-catalog:new-version"
        self.write_config()
        code, data = self.run_cli("deploy", env=dict(self.env, FAKE_REPLACE_BEFORE_PATCH="catalog"))
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "kubectl_failed:patch")
        state = self.read_cluster()
        self.assertEqual(state["objects"]["deployment/catalog"]["spec"]["template"]["spec"]["containers"][0]["image"], "recweb-catalog:latest")

    def test_interrupted_uid_save_is_recovered_only_by_deploy_then_pinned(self):
        code, data = self.run_cli("deploy")
        self.assertEqual(code, 0, data)
        path = self.root / ".recshop-state" / "recshop-demo.json"
        local = json.loads(path.read_text())
        missing = ["Secret/recshop-secrets", "PersistentVolumeClaim/recshop-assets",
                   "ConfigMap/recshop-db-init", "Deployment/catalog", "Service/catalog"]
        original = {key: local["resources"].pop(key) for key in missing}
        path.write_text(json.dumps(local))
        # A read-only check must not silently repin a missing local resource record.
        code, data = self.run_cli("check")
        self.assertEqual(code, 2, data)
        self.assertEqual(data["reason"], "resource_uid_missing_run_deploy")
        checked = json.loads(path.read_text())
        self.assertTrue(all(key not in checked["resources"] for key in missing))
        code, data = self.run_cli("deploy")
        self.assertEqual(code, 0, data)
        repaired = json.loads(path.read_text())
        self.assertEqual({key: repaired["resources"][key] for key in missing}, original)
        state = self.read_cluster()
        state["objects"]["deployment/catalog"]["metadata"]["uid"] = "same-labels-different-uid"
        self.cluster.write_text(json.dumps(state))
        code, data = self.run_cli("check")
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "resource_uid_changed")

    def owned_object_fixture(self):
        """Fast, disposable object backend; no subprocess, service or cluster execution."""
        _, config = recshop.load_config(self.cfg)
        instance = recshop.Instance(self.cfg, config)
        token = "offline-owned-fixture"
        plan = recshop.build(config, token, "fake-node")
        items = [plan["namespace"], *plan["volumes"], *plan["database"], *plan["apps"],
                 recshop.obj(config, token, "Secret", "recshop-secrets", data={})]
        objects = {}
        for index, item in enumerate(items):
            item = copy.deepcopy(item)
            item["metadata"].update(uid="fixture-" + str(index), resourceVersion="1", generation=1)
            if item["kind"] == "Deployment":
                item["status"] = {"observedGeneration": 1, "readyReplicas": 1, "availableReplicas": 1, "updatedReplicas": 1}
            elif item["kind"] == "PersistentVolumeClaim":
                item["status"] = {"phase": "Bound"}
            objects[instance.key(item)] = item
        instance.state = {k: config[k] for k in ("instance_id", "context", "namespace")}
        instance.state.update(owner_token=token, asset_node="fake-node", assets=[{"fixture": True}],
                              resources={key: value["metadata"]["uid"] for key, value in objects.items()})
        instance.persist()
        class Objects:
            def get(self, kind, name):
                return copy.deepcopy(objects.get(kind + "/" + name))
            def run(self, args, **kwargs):
                if args == ["get", "deployments", "-o", "json"]:
                    return json.dumps({"items": [d for d in objects.values() if d["kind"] == "Deployment"]}).encode()
                if args[:2] == ["rollout", "status"]:
                    return b"{}"
                if args[:2] != ["get", "--raw"]:
                    raise AssertionError("unexpected mutation in lightweight fixture")
                url = args[2]
                name = url.split("/services/")[1].split(":")[0]
                if "/api/items/" in url:
                    value = {"success": True, "item": {"item_id": recshop.DEMO_ID}}
                elif name == "rec-agent":
                    value = {"recommendation_system": "healthy", "sasrec_service": {"status": "healthy", "model_loaded": True}}
                elif name == "sasrec":
                    value = {"status": "healthy", "model_loaded": True}
                elif name == "shop-web":
                    value = {"status": "ok", "service": "shop_web"}
                elif name == "backend":
                    value = {"status": "healthy", "database": "healthy", "sasrec_api": "healthy"}
                else:
                    value = {"status": "healthy", "database": "healthy", "service": name.replace("-", "_") + "_service"}
                return json.dumps(value).encode()
        instance.k = Objects()
        return instance, plan

    def test_readonly_missing_namespace_uid_is_rejected_without_state_write(self):
        instance, _ = self.owned_object_fixture()
        instance.state["resources"].pop("Namespace/recshop-demo")
        instance.persist()
        before = instance.statepath.read_bytes()
        for action in (instance.check, instance.start):
            with self.assertRaisesRegex(recshop.Failure, "namespace_uid_missing_run_deploy"):
                action()
            self.assertEqual(instance.statepath.read_bytes(), before)
        # The explicitly mutating deploy path alone repairs the interrupted registration.
        instance.namespace(create=True)
        self.assertEqual(instance.check()["status"], "BUSINESS_READY")

    def test_readonly_missing_catalog_uid_is_rejected_without_state_write(self):
        instance, plan = self.owned_object_fixture()
        instance.state["resources"].pop("Deployment/catalog")
        instance.persist()
        before = instance.statepath.read_bytes()
        for action in (instance.check, instance.start):
            with self.assertRaisesRegex(recshop.Failure, "resource_uid_missing_run_deploy"):
                action()
            self.assertEqual(instance.statepath.read_bytes(), before)
        desired = next(d for d in plan["apps"] if d["kind"] == "Deployment" and d["metadata"]["name"] == "catalog")
        instance.apply(desired)
        self.assertEqual(instance.check()["status"], "BUSINESS_READY")

    def test_render_scheduler_affinity_without_node_name(self):
        _, config = recshop.load_config(self.cfg)
        initial = recshop.build(config, "offline-owner")
        self.assertNotIn("nodeName", initial["helper"]["spec"])
        self.assertNotIn("affinity", initial["helper"]["spec"])
        pinned = recshop.build(config, "offline-owner", "recorded-node")
        affinity = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{"matchFields": [{"key": "metadata.name", "operator": "In", "values": ["recorded-node"]}]}]}}}
        self.assertNotIn("nodeName", pinned["helper"]["spec"])
        self.assertEqual(pinned["helper"]["spec"]["affinity"], affinity)
        for d in pinned["apps"]:
            if d["kind"] != "Deployment":
                continue
            pod = d["spec"]["template"]["spec"]
            self.assertNotIn("nodeName", pod)
            if d["metadata"]["name"] in {"sasrec", "rec-agent"}:
                self.assertEqual(pod["affinity"], affinity)

    def test_interrupted_helper_reuses_registered_unaffined_pod(self):
        instance, plan = self.owned_object_fixture()
        helper = recshop.build(instance.c, instance.state["owner_token"])["helper"]
        helper["metadata"].update(uid="registered-loader-uid", resourceVersion="1")
        # The API assigns nodeName after the scheduler places the original unaffined pod.
        helper["spec"]["nodeName"] = "fake-node"
        helper["status"] = {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
        instance.state["resources"]["Pod/recshop-assets-loader"] = helper["metadata"]["uid"]
        instance.persist()
        original = instance.k
        commands = []
        class ReuseBackend:
            def get(self, kind, name):
                if kind == "Pod" and name == "recshop-assets-loader":
                    return copy.deepcopy(helper)
                return original.get(kind, name)
            def run(self, args, **kwargs):
                commands.append(args[0])
                if args[0] == "wait":
                    return b"{}"
                if args[0] == "exec":
                    return b'{"missing": []}'
                raise AssertionError("Existing loader must not be mutated or recreated")
        instance.k = ReuseBackend()
        instance.transfer(plan["helper"], self.assets, [{"name": "verified-fixture", "bytes": 1, "sha256": "0" * 64}])
        self.assertNotIn("affinity", helper["spec"])
        self.assertEqual(instance.state["resources"]["Pod/recshop-assets-loader"], "registered-loader-uid")
        self.assertEqual(commands, ["wait", "exec", "exec"])
        helper["metadata"]["uid"] = "replaced-loader-uid"
        before = instance.statepath.read_bytes()
        with self.assertRaisesRegex(recshop.Failure, "resource_uid_changed"):
            instance.transfer(plan["helper"], self.assets, [])
        self.assertEqual(instance.statepath.read_bytes(), before)

    def test_terminal_helper_and_non_catalog_degraded_are_not_ready(self):
        code, data = self.run_cli("deploy", env=dict(self.env, FAKE_HELPER_TERMINAL="1"))
        self.assertEqual(code, 2)
        self.assertEqual(data["reason"], "kubectl_failed:wait")
        # A failed import keeps the helper for inspection; explicitly repair the fixture only.
        state = self.read_cluster()
        state["objects"]["pod/recshop-assets-loader"]["status"] = {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
        self.cluster.write_text(json.dumps(state))
        code, data = self.run_cli("deploy")
        self.assertEqual(code, 0, data)
        for name in ("search", "backend", "rec-agent"):
            code, data = self.run_cli("check", env=dict(self.env, FAKE_BAD_HEALTH=name))
            self.assertEqual(code, 2, (name, data))

    def test_clean_copy_without_git_or_original_workspace(self):
        clean = self.root / "clean source copy"
        files = ["scripts/database_schema.sql", "deployment.example.json", "tests/fixtures/fake_kubectl_recshop.py"]
        files += ["scripts/deployment/__init__.py", "scripts/deployment/recshop.py"]
        files += [x + "_recshop.py" for x in ("deploy", "start", "check")]
        files += ["k8s/services/" + name for name in recshop.FILES.values()]
        for name in files:
            dest = clean / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(recshop.ROOT / name, dest)
        self.config["kubectl"] = [sys.executable, str(clean / "tests/fixtures/fake_kubectl_recshop.py")]
        self.write_config()
        self.assertFalse((clean / ".git").exists())
        for action, extra in (("deploy", ("--render", "--out", str(self.root / "clean-plan.json"))),
                              ("deploy", ()), ("deploy", ()), ("start", ()), ("check", ())):
            code, data = self.run_cli(action, *extra, workspace=clean)
            self.assertEqual(code, 0, (action, data))


if __name__ == "__main__":
    unittest.main()
