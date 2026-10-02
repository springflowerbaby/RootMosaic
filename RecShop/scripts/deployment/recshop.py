"""Deploy an isolated business instance; never operate the protected collection namespace."""
from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

import yaml

ROOT = Path(__file__).resolve().parents[2]
MANAGED = "recshop-portable"
OWNER = "recshop.dev/owner"
INSTANCE = "recshop.dev/instance"
FILES = {
    "rec-agent": "rec-agent.yaml", "llm-rerank": "llm-rerank.yaml",
    "user": "user.yaml", "address": "address.yaml", "ai-memory": "ai-memory.yaml",
    "announcement": "announcement.yaml", "catalog": "catalog.yaml", "search": "search.yaml",
    "review-query": "review-query.yaml", "merchant": "merchant.yaml",
    "interaction": "interaction.yaml", "sasrec": "sasrec.yaml",
    "notification": "notification.yaml", "admin-audit": "admin-audit.yaml",
    "backend": "backend.yaml", "review": "review.yaml", "shop-web": "shop-web.yaml",
    "cart": "cart.yaml", "order": "order.yaml", "checkout": "checkout.yaml",
    "payment": "payment.yaml", "inventory": "inventory.yaml", "pricing": "pricing.yaml",
    "promotion": "promotion.yaml", "shipping": "shipping.yaml",
}
DEMO_ID = "RECSHOP_DEMO_001"
DEMO_SQL = """USE shopify2;
-- Synthetic deployment check only; not recommendation training or research data.
INSERT INTO items (item_id,title,category,price,description,status)
SELECT 'RECSHOP_DEMO_001','Synthetic RecShop deployment check item','deployment-demo',1.00,
       'Synthetic demonstration record; not research data','active'
WHERE NOT EXISTS (SELECT 1 FROM items WHERE item_id='RECSHOP_DEMO_001');
"""


class Failure(Exception):
    """A bounded, safe-to-print failure code."""


def need(condition, code):
    if not condition:
        raise Failure(code)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def load_config(path):
    path = Path(path).resolve()
    c = json.loads(path.read_text(encoding="utf-8-sig"))
    need(c.get("schema_version") == 1, "unsupported_config_schema")
    for field in ("instance_id", "namespace"):
        need(isinstance(c.get(field), str) and re.fullmatch(r"[a-z][a-z0-9-]{0,39}", c[field]), "invalid_" + field)
    ns = c["namespace"]
    need(ns not in {"default", "recweb-chaos"} and not ns.startswith(("kube-", "recweb-chaos-")), "protected_namespace")
    need(isinstance(c.get("context"), str) and bool(c["context"].strip()) and not c["context"].startswith("-"), "explicit_context_required")
    need(set(c.get("images", {})) == set(FILES), "exact_25_application_images_required")
    for value in list(c["images"].values()) + [c["database"]["image"], c["assets"]["helper_image"]]:
        need(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/@:+-]*", value), "invalid_image")
    a = c["assets"]
    need(set(a.get("files", {})) == {"model", "cache", "items"}, "three_assets_required")
    names = []
    for spec in a["files"].values():
        name = spec.get("name", "")
        need(isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,159}", name) and ".." not in name, "invalid_asset_basename")
        names.append(name)
        need(spec.get("sha256") is None or re.fullmatch(r"[a-fA-F0-9]{64}", spec["sha256"]), "invalid_asset_hash")
    need(len(set(names)) == 3, "duplicate_asset_names")
    need(isinstance(a.get("local_dir"), str) and bool(a["local_dir"]), "local_asset_directory_required")
    for field in ("assets", "database"):
        need(re.fullmatch(r"[1-9][0-9]*(Mi|Gi|Ti)", str(c[field].get("storage", ""))), "invalid_storage_size")
        sc = c[field].get("storage_class")
        need(sc is None or (isinstance(sc, str) and bool(re.fullmatch(r"[a-z0-9][a-z0-9.-]*", sc))), "invalid_storage_class")
    for key in ("db_password_env", "db_root_password_env", "flask_secret_env"):
        need(re.fullmatch(r"[A-Z][A-Z0-9_]*", str(c.get("secrets", {}).get(key, ""))), "secret_environment_name_required")
    llm = c["secrets"].get("llm_key_env")
    need(llm is None or re.fullmatch(r"[A-Z][A-Z0-9_]*", llm), "invalid_llm_environment_name")
    c.setdefault("observability", {"enabled": False})
    need(type(c["observability"].get("enabled")) is bool, "invalid_observability_switch")
    if c["observability"]["enabled"]:
        need(re.fullmatch(r"https?://[A-Za-z0-9.-]+:[0-9]+", c["observability"].get("otlp_endpoint", "")), "explicit_otlp_endpoint_required")
    c.setdefault("timeouts", {})
    for k, default in (("command_seconds", 60), ("rollout_seconds", 900), ("asset_copy_seconds", 1800)):
        v = c["timeouts"].setdefault(k, default)
        need(type(v) is int and 1 <= v <= 7200, "invalid_timeout")
    c.setdefault("kubectl", ["kubectl"])
    need(isinstance(c["kubectl"], list) and c["kubectl"] and all(isinstance(x, str) and x for x in c["kubectl"]), "invalid_kubectl_argv")
    c.setdefault("resources", {})
    need(isinstance(c["resources"], dict) and set(c["resources"]) <= set(FILES), "invalid_resource_overrides")
    for resource in c["resources"].values():
        need(isinstance(resource, dict) and set(resource) <= {"requests", "limits"}, "invalid_resource_overrides")
        for values in resource.values():
            need(isinstance(values, dict) and set(values) <= {"cpu", "memory"}, "invalid_resource_overrides")
            for key, value in values.items():
                pattern = r"[0-9]+(?:\.[0-9]+)?m?" if key == "cpu" else r"[1-9][0-9]*(Mi|Gi)"
                need(isinstance(value, str) and re.fullmatch(pattern, value), "invalid_resource_quantity")
    return path, c


def labels(c, token):
    return {"app.kubernetes.io/managed-by": MANAGED, OWNER: token, INSTANCE: c["instance_id"]}


def obj(c, token, kind, name, **fields):
    m = {"name": name, "labels": labels(c, token)}
    if kind != "Namespace":
        m["namespace"] = c["namespace"]
    return {"apiVersion": "apps/v1" if kind == "Deployment" else "v1", "kind": kind, "metadata": m, **fields}


def set_env(container, name, value=None, secret=None):
    rows = [r for r in container.get("env", []) if r["name"] != name]
    row = {"name": name}
    if secret:
        row["valueFrom"] = {"secretKeyRef": {"name": "recshop-secrets", "key": secret}}
    else:
        row["value"] = str(value)
    container["env"] = rows + [row]


def pvc(c, token, name, spec):
    volume = {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": spec["storage"]}}}
    if spec.get("storage_class") is not None:
        volume["storageClassName"] = spec["storage_class"]
    return obj(c, token, "PersistentVolumeClaim", name, spec=volume)


def required_node_affinity(node):
    """Keep shared RWO assets on one node without bypassing the scheduler."""
    return {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
        "nodeSelectorTerms": [{"matchFields": [{"key": "metadata.name", "operator": "In", "values": [node]}]}]}}}


def build(c, token, asset_node=None):
    """Offline, explicit allowlist; no credentials are loaded here."""
    ns = obj(c, token, "Namespace", c["namespace"])
    volumes = [pvc(c, token, "recshop-assets", c["assets"]), pvc(c, token, "recshop-mysql-data", c["database"])]
    init = obj(c, token, "ConfigMap", "recshop-db-init", data={
        "01-schema.sql": (ROOT / "scripts/database_schema.sql").read_text(encoding="utf-8-sig"),
        "02-demo.sql": DEMO_SQL})
    db_container = {"name": "mysql", "image": c["database"]["image"], "imagePullPolicy": "IfNotPresent",
                    "ports": [{"containerPort": 3306}], "env": [
                        {"name": "MYSQL_DATABASE", "value": "shopify2"}, {"name": "MYSQL_USER", "value": "recshop"}],
                    "volumeMounts": [{"name": "data", "mountPath": "/var/lib/mysql"},
                                     {"name": "init", "mountPath": "/docker-entrypoint-initdb.d", "readOnly": True}],
                    "readinessProbe": {"exec": {"command": ["sh", "-c", 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -urecshop -h127.0.0.1 -N -e "SELECT 1 FROM shopify2.items LIMIT 1"']}, "periodSeconds": 5, "timeoutSeconds": 5},
                    "resources": {"requests": {"cpu": "100m", "memory": "256Mi"}}}
    set_env(db_container, "MYSQL_PASSWORD", secret="db-password")
    set_env(db_container, "MYSQL_ROOT_PASSWORD", secret="db-root-password")
    db_labels = {**labels(c, token), "app": "recshop-mysql"}
    db = obj(c, token, "Deployment", "recshop-mysql", spec={"replicas": 1, "strategy": {"type": "Recreate"},
        "selector": {"matchLabels": db_labels}, "template": {"metadata": {"labels": db_labels}, "spec": {
            "containers": [db_container], "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "recshop-mysql-data"}},
                                                     {"name": "init", "configMap": {"name": "recshop-db-init"}}]}}})
    db_svc = obj(c, token, "Service", "recshop-mysql", spec={"type": "ClusterIP", "selector": db_labels,
        "ports": [{"name": "mysql", "port": 3306, "targetPort": 3306}]})
    helper = obj(c, token, "Pod", "recshop-assets-loader", spec={"restartPolicy": "Always",
        "containers": [{"name": "loader", "image": c["assets"]["helper_image"], "imagePullPolicy": "IfNotPresent",
                        "command": ["python", "-c", "import time\nwhile True: time.sleep(3600)"],
                        "resources": {"requests": {"cpu": "10m", "memory": "32Mi"}, "limits": {"memory": "256Mi"}},
                        "volumeMounts": [{"name": "assets", "mountPath": "/assets"}]}],
        "volumes": [{"name": "assets", "persistentVolumeClaim": {"claimName": "recshop-assets"}}]})
    if asset_node:
        helper["spec"]["affinity"] = required_node_affinity(asset_node)
    apps = []
    for name, filename in FILES.items():
        source = list(yaml.safe_load_all((ROOT / "k8s/services" / filename).read_text(encoding="utf-8-sig")))
        need(len(source) == 2 and {x["kind"] for x in source} == {"Deployment", "Service"}, "unexpected_application_manifest")
        for original in source:
            d = copy.deepcopy(original)
            need(d["metadata"]["name"] == name, "manifest_name_mismatch")
            d["metadata"]["namespace"] = c["namespace"]
            d["metadata"].setdefault("labels", {}).update(labels(c, token))
            if d["kind"] == "Service":
                d["spec"]["type"] = "ClusterIP"
                d["spec"]["selector"].update(labels(c, token))
            else:
                d["spec"]["replicas"] = 1
                d["spec"]["selector"]["matchLabels"].update(labels(c, token))
                template = d["spec"]["template"]
                template["metadata"]["labels"].update(labels(c, token))
                pod = template["spec"]
                need(len(pod["containers"]) == 1, "unexpected_container_count")
                container = pod["containers"][0]
                container["image"] = c["images"][name]
                container["imagePullPolicy"] = "IfNotPresent"
                if name in c["resources"]:
                    container["resources"] = deep_merge(container.get("resources", {}), c["resources"][name])
                for k, v in (("NACOS_ENABLED", "false"), ("OTEL_ENABLED", str(c["observability"]["enabled"]).lower())):
                    set_env(container, k, v)
                if c["observability"]["enabled"]:
                    set_env(container, "OTEL_EXPORTER_OTLP_ENDPOINT", c["observability"]["otlp_endpoint"])
                else:
                    container["env"] = [e for e in container["env"] if not e["name"].startswith("OTEL_EXPORTER_")]
                if name not in {"sasrec", "rec-agent", "llm-rerank"}:
                    for k, v in (("DB_HOST", "recshop-mysql"), ("DB_PORT", "3306"), ("DB_NAME", "shopify2"), ("DB_USER", "recshop")):
                        set_env(container, k, v)
                    set_env(container, "DB_PASSWORD", secret="db-password")
                set_env(container, "SECRET_KEY", secret="flask-secret")
                if c["secrets"].get("llm_key_env"):
                    set_env(container, "DEEPSEEK_API_KEY", secret="llm-key")
                if name in {"sasrec", "rec-agent"}:
                    pod["volumes"] = [{"name": "recshop-assets", "persistentVolumeClaim": {"claimName": "recshop-assets", "readOnly": True}}]
                    mount_roles = ["model", "cache", "items"] if name == "sasrec" else ["items"]
                    container["volumeMounts"] = [{"name": "recshop-assets", "mountPath": "/opt/recshop-assets/" + c["assets"]["files"][role]["name"],
                        "subPath": c["assets"]["files"][role]["name"], "readOnly": True} for role in mount_roles]
                    for role, envname in (("model", "SASREC_MODEL_PATH"), ("cache", "SASREC_CACHE_PATH"), ("items", "SASREC_ITEM_FILE")):
                        if name == "sasrec":
                            set_env(container, envname, "/opt/recshop-assets/" + c["assets"]["files"][role]["name"])
                    if name == "rec-agent":
                        set_env(container, "ITEM_FILE_PATH", "/opt/recshop-assets/" + c["assets"]["files"]["items"]["name"])
                    if asset_node:
                        pod["affinity"] = required_node_affinity(asset_node)
            apps.append(d)
    return {"namespace": ns, "volumes": volumes, "database": [init, db_svc, db], "helper": helper, "apps": apps}


def subset(actual, desired):
    if isinstance(desired, dict):
        return isinstance(actual, dict) and all(k in actual and subset(actual[k], v) for k, v in desired.items())
    if isinstance(desired, list):
        return isinstance(actual, list) and len(actual) == len(desired) and all(subset(a, b) for a, b in zip(actual, desired))
    return actual == desired


def deep_merge(actual, desired):
    """Preserve API-assigned fields (notably Service clusterIP) when updating owned resources."""
    if isinstance(actual, dict) and isinstance(desired, dict):
        result = copy.deepcopy(actual)
        for key, value in desired.items():
            result[key] = deep_merge(result.get(key), value)
        return result
    return copy.deepcopy(desired)


class Kube:
    def __init__(self, c):
        self.c = c

    def run(self, args, data=None, timeout=None, cwd=None):
        argv = self.c["kubectl"] + ["--context", self.c["context"], "--namespace", self.c["namespace"], "--request-timeout=45s"] + args
        try:
            r = subprocess.run(argv, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               timeout=timeout or self.c["timeouts"]["command_seconds"], cwd=cwd, check=False)
        except (OSError, subprocess.TimeoutExpired):
            raise Failure("kubectl_unavailable_or_timeout:" + args[0]) from None
        # Never include child stdout/stderr or submitted Secret data in a public error.
        need(r.returncode == 0, "kubectl_failed:" + args[0])
        return r.stdout

    def get(self, kind, name):
        raw = self.run(["get", kind, name, "--ignore-not-found", "-o", "json"])
        return json.loads(raw) if raw.strip() else None


ASSET_SCRIPT = '''import base64,hashlib,json,os,pathlib,sys
m=json.loads(base64.b64decode(sys.argv[2])); root=pathlib.Path('/assets'); mode=sys.argv[1]
def h(p):
 q=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(4194304),b''):q.update(b)
 return q.hexdigest()
missing=[]
for s in m:
 p=root/s['name']; stage=root/'.recshop-incoming'/s['name']
 if p.exists():
  if p.is_symlink() or not p.is_file() or p.stat().st_size!=s['bytes'] or h(p)!=s['sha256']:raise SystemExit(4)
 elif mode=='finalize':
  if stage.is_symlink() or not stage.is_file() or stage.stat().st_size!=s['bytes'] or h(stage)!=s['sha256']:raise SystemExit(5)
  stage.chmod(0o444)
  os.replace(stage,p)
 else: missing.append(s['name'])
(root/'.recshop-incoming').mkdir(exist_ok=True)
print(json.dumps({'missing':missing}))
'''


class Instance:
    def __init__(self, path, c):
        self.path, self.c, self.k = path, c, Kube(c)
        self.statepath = path.parent / ".recshop-state" / (c["instance_id"] + ".json")
        self.state = json.loads(self.statepath.read_text(encoding="utf-8")) if self.statepath.exists() else None
        if self.state:
            need(all(self.state[k] == c[k] for k in ("instance_id", "context", "namespace")), "state_identity_mismatch")

    def persist(self):
        save(self.statepath, self.state)

    def key(self, d):
        return d["kind"] + "/" + d["metadata"]["name"]

    def owned(self, d, require_recorded=True):
        need(d is not None, "managed_resource_missing")
        m = d["metadata"]
        need(all(m.get("labels", {}).get(k) == v for k, v in labels(self.c, self.state["owner_token"]).items()), "foreign_resource")
        key = self.key(d)
        prior = self.state["resources"].get(key)
        need(not require_recorded or prior is not None, "resource_uid_missing_run_deploy")
        need(prior is None or prior == m["uid"], "resource_uid_changed")
        return d

    def namespace(self, create=False):
        existing = self.k.get("Namespace", self.c["namespace"])
        if not create:
            need(self.state is not None and "Namespace/" + self.c["namespace"] in self.state["resources"],
                 "namespace_uid_missing_run_deploy")
            self.owned(existing)
            return
        if not self.state:
            need(create and existing is None, "namespace_exists_without_local_ownership_state")
            self.state = {k: self.c[k] for k in ("instance_id", "namespace", "context")}
            self.state.update(owner_token=uuid.uuid4().hex, resources={}, asset_node=None)
            self.persist()
        if existing:
            self.owned(existing, require_recorded=False)
        else:
            need(create and "Namespace/" + self.c["namespace"] not in self.state["resources"], "recorded_namespace_missing")
            d = obj(self.c, self.state["owner_token"], "Namespace", self.c["namespace"])
            self.k.run(["create", "-f", "-"], json.dumps(d).encode())
            existing = self.owned(self.k.get("Namespace", self.c["namespace"]), require_recorded=False)
        self.state["resources"][self.key(existing)] = existing["metadata"]["uid"]
        self.persist()

    def apply(self, d):
        self.namespace()
        key = self.key(d)
        live = self.k.get(d["kind"], d["metadata"]["name"])
        if live:
            self.owned(live, require_recorded=False)
            # Recover a create-before-local-save interruption only for this owner's object.
            # check() remains read-only and never creates resource UID bindings.
            if key not in self.state["resources"]:
                self.state["resources"][key] = live["metadata"]["uid"]
                self.persist()
            if d["kind"] == "Secret":
                need(live.get("data") == d["data"], "secret_changed_rotation_not_automatic")
                return
            if d["kind"] in {"PersistentVolumeClaim", "ConfigMap", "Pod"}:
                fields = ("data",) if d["kind"] == "ConfigMap" else ("spec",)
                need(all(subset(live.get(k), d.get(k)) for k in fields), "immutable_managed_resource_differs")
                return
            if subset(live.get("spec"), d.get("spec")):
                return
            patch = [{"op": "test", "path": "/metadata/uid", "value": live["metadata"]["uid"]},
                     {"op": "test", "path": "/metadata/resourceVersion", "value": live["metadata"]["resourceVersion"]},
                     {"op": "replace", "path": "/spec", "value": deep_merge(live["spec"], d["spec"])}]
            self.k.run(["patch", d["kind"], d["metadata"]["name"], "--type=json", "--patch", json.dumps(patch)])
        else:
            need(key not in self.state["resources"], "recorded_resource_missing")
            self.k.run(["create", "-f", "-"], json.dumps(d).encode())
        fresh = self.owned(self.k.get(d["kind"], d["metadata"]["name"]), require_recorded=False)
        self.state["resources"][key] = fresh["metadata"]["uid"]
        self.persist()

    def wait(self, name):
        self.owned(self.k.get("Deployment", name))
        secs = self.c["timeouts"]["rollout_seconds"]
        self.k.run(["rollout", "status", "deployment/" + name, "--timeout=" + str(secs) + "s"], timeout=secs + 10)

    def asset_manifest(self):
        local = Path(self.c["assets"]["local_dir"])
        if not local.is_absolute():
            local = self.path.parent / local
        local = local.resolve()
        result = []
        for spec in self.c["assets"]["files"].values():
            p = (local / spec["name"]).resolve()
            need(p.parent == local and p.is_file() and p.stat().st_size > 0, "asset_missing_or_invalid")
            h = file_digest(p)
            need(not spec.get("sha256") or h == spec["sha256"].lower(), "asset_declared_hash_mismatch")
            result.append({"name": spec["name"], "bytes": p.stat().st_size, "sha256": h})
        if self.state and self.state.get("assets"):
            need(self.state["assets"] == result, "asset_manifest_changed_use_new_instance")
        return local, result

    def transfer(self, helper, local, manifest):
        existing = self.k.get("Pod", "recshop-assets-loader")
        if existing and self.state.get("asset_node"):
            self.owned(existing)
            spec, status = existing["spec"], existing.get("status", {})
            if "affinity" not in spec:
                # The first loader was scheduled before its node could be recorded.
                # Reuse that registered live pod without trying to mutate its immutable spec.
                need(spec.get("nodeName") == self.state["asset_node"] and status.get("phase") == "Running"
                     and any(x.get("type") == "Ready" and x.get("status") == "True" for x in status.get("conditions", [])),
                     "existing_unaffined_loader_not_ready_on_recorded_node")
                helper = copy.deepcopy(helper)
                helper["spec"].pop("affinity", None)
        self.apply(helper)
        seconds = self.c["timeouts"]["rollout_seconds"]
        self.k.run(["wait", "--for=condition=Ready", "pod/recshop-assets-loader", "--timeout=" + str(seconds) + "s"], timeout=seconds + 10)
        pod = self.owned(self.k.get("Pod", "recshop-assets-loader"))
        need(pod.get("status", {}).get("phase") == "Running" and
             any(x.get("type") == "Ready" and x.get("status") == "True" for x in pod.get("status", {}).get("conditions", [])), "asset_loader_not_ready")
        node = pod["spec"].get("nodeName")
        need(bool(node), "asset_loader_not_scheduled")
        prior_node = self.state.get("asset_node")
        need(not prior_node or prior_node == node, "asset_node_changed")
        self.state["asset_node"] = node
        self.persist()
        encoded = base64.b64encode(json.dumps(manifest).encode()).decode()
        def inspect(mode):
            self.owned(self.k.get("Pod", "recshop-assets-loader"))
            return json.loads(self.k.run(["exec", "recshop-assets-loader", "--", "python", "-c", ASSET_SCRIPT, mode, encoded], timeout=self.c["timeouts"]["asset_copy_seconds"]))
        missing = inspect("inspect")["missing"]
        for name in missing:
            self.owned(self.k.get("Pod", "recshop-assets-loader"))
            # A basename plus cwd avoids kubectl cp interpreting Windows C: as a remote separator.
            self.k.run(["cp", name, self.c["namespace"] + "/recshop-assets-loader:/assets/.recshop-incoming/" + name],
                       cwd=local, timeout=self.c["timeouts"]["asset_copy_seconds"])
        need(inspect("finalize")["missing"] == [], "asset_finalization_failed")
        self.state["assets"] = manifest
        self.persist()

    def secret(self):
        data = {}
        for key, env in (("db-password", "db_password_env"), ("db-root-password", "db_root_password_env"), ("flask-secret", "flask_secret_env")):
            value = os.environ.get(self.c["secrets"][env], "")
            need(len(value) >= 16, "required_secret_missing_or_too_short")
            data[key] = base64.b64encode(value.encode()).decode()
        if self.c["secrets"].get("llm_key_env"):
            value = os.environ.get(self.c["secrets"]["llm_key_env"], "")
            need(bool(value), "llm_secret_missing")
            data["llm-key"] = base64.b64encode(value.encode()).decode()
        return obj(self.c, self.state["owner_token"], "Secret", "recshop-secrets", type="Opaque", data=data)

    def deploy(self):
        local, manifest = self.asset_manifest()  # Fail before creating resources if local assets are absent.
        for key in ("db_password_env", "db_root_password_env", "flask_secret_env"):
            need(len(os.environ.get(self.c["secrets"][key], "")) >= 16, "required_secret_missing_or_too_short")
        self.namespace(create=True)
        plan = build(self.c, self.state["owner_token"], self.state.get("asset_node"))
        self.apply(self.secret())
        for d in plan["volumes"]:
            self.apply(d)
        self.transfer(plan["helper"], local, manifest)
        plan = build(self.c, self.state["owner_token"], self.state["asset_node"])
        for d in plan["database"]:
            self.apply(d)
        self.wait("recshop-mysql")
        for d in plan["apps"]:
            self.apply(d)
        for name in FILES:
            self.wait(name)
        result = self.check()
        helper = self.owned(self.k.get("Pod", "recshop-assets-loader"))
        # Delete only this instance's registered temporary pod; volumes and namespace remain.
        body = {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": helper["metadata"]["uid"]}}
        url = "/api/v1/namespaces/" + self.c["namespace"] + "/pods/recshop-assets-loader"
        self.k.run(["delete", "--raw", url, "-f", "-"], json.dumps(body).encode())
        self.k.run(["wait", "--for=delete", "pod/recshop-assets-loader", "--timeout=60s"], timeout=70)
        self.state["resources"].pop("Pod/recshop-assets-loader", None)
        self.persist()
        return result

    def expected(self):
        need(self.state and self.state.get("assets") and self.state.get("asset_node"), "instance_not_deployed")
        return build(self.c, self.state["owner_token"], self.state["asset_node"])

    def start(self):
        self.namespace()
        plan = self.expected()
        for d in plan["database"] + plan["apps"]:
            if d["kind"] != "Deployment":
                continue
            current = self.owned(self.k.get("Deployment", d["metadata"]["name"]))
            want = copy.deepcopy(d["spec"])
            want.pop("replicas")
            need(subset(current["spec"], want), "deployment_configuration_drift")
            replicas = current["spec"].get("replicas", 1)
            need(replicas in (0, 1), "unexpected_replica_count")
            if replicas == 0:
                patch = [{"op": "test", "path": "/metadata/uid", "value": current["metadata"]["uid"]},
                         {"op": "test", "path": "/metadata/resourceVersion", "value": current["metadata"]["resourceVersion"]},
                         {"op": "replace", "path": "/spec/replicas", "value": 1}]
                self.k.run(["patch", "Deployment", d["metadata"]["name"], "--type=json", "--patch", json.dumps(patch)])
            self.wait(d["metadata"]["name"])
        return self.check()

    def check(self):
        self.namespace()
        plan = self.expected()
        roster = json.loads(self.k.run(["get", "deployments", "-o", "json"]))["items"]
        need({d["metadata"]["name"] for d in roster} == set(FILES) | {"recshop-mysql"}
             and len(roster) == 26, "unexpected_deployment_roster")
        for d in roster:
            self.owned(d)
        for d in plan["volumes"] + plan["database"] + plan["apps"]:
            current = self.owned(self.k.get(d["kind"], d["metadata"]["name"]))
            if d["kind"] == "PersistentVolumeClaim":
                need(current.get("status", {}).get("phase") == "Bound", "volume_not_bound")
            else:
                for field in ("spec", "data"):
                    if field in d:
                        need(subset(current.get(field), d[field]), "resource_configuration_drift")
            if d["kind"] == "Deployment":
                status = current.get("status", {})
                need(status.get("observedGeneration", 0) >= current["metadata"].get("generation", 1)
                     and all(status.get(k) == 1 for k in ("readyReplicas", "updatedReplicas", "availableReplicas")), "deployment_not_ready")
        self.owned(self.k.get("Secret", "recshop-secrets"))
        def get(service, port, path):
            url = "/api/v1/namespaces/" + self.c["namespace"] + "/services/" + service + ":" + str(port) + "/proxy" + path
            return json.loads(self.k.run(["get", "--raw", url]))
        health_checked = []
        for d in plan["apps"]:
            if d["kind"] != "Deployment":
                continue
            name = d["metadata"]["name"]
            container = d["spec"]["template"]["spec"]["containers"][0]
            probe = container["readinessProbe"]["httpGet"]
            port = probe["port"]
            if isinstance(port, str):
                port = next(p["containerPort"] for p in container["ports"] if p.get("name") == port)
            value = get(name, port, probe["path"])
            if name == "rec-agent":
                nested = value.get("sasrec_service", {})
                need(value.get("recommendation_system") == "healthy" and nested.get("status") == "healthy"
                     and nested.get("model_loaded") is True, "rec_agent_dependency_unhealthy")
            elif name == "sasrec":
                need(value.get("status") == "healthy" and value.get("model_loaded") is True, "sasrec_model_not_loaded")
            elif name == "shop-web":
                need(value.get("status") == "ok" and value.get("service") == "shop_web", "shop_web_process_unhealthy")
            elif name == "backend":
                need(value.get("status") == "healthy" and value.get("database") == "healthy"
                     and value.get("sasrec_api") == "healthy", "backend_dependency_unhealthy")
            else:
                need(value.get("status") == "healthy" and value.get("service") == name.replace("-", "_") + "_service",
                     "application_health_failed:" + name)
                if name != "llm-rerank":
                    need(value.get("database") == "healthy", "application_database_unhealthy:" + name)
            health_checked.append(name)
        item = get("catalog", 5005, "/api/items/" + DEMO_ID)
        need(item.get("success") is True and item.get("item", {}).get("item_id") == DEMO_ID, "synthetic_catalog_read_failed")
        return {"status": "BUSINESS_READY", "namespace": self.c["namespace"], "applications": 25,
                "database_deployments": 1, "synthetic_demo_item": DEMO_ID, "model_loaded": True,
                "application_http_health_checked": health_checked,
                "rec_agent_nested_probe_scope": "configured_service_health_verified",
                "external_llm_called": False, "observability_enabled": self.c["observability"]["enabled"],
                "collection_ready": False,
                "frontend_command": ["kubectl", "--context", self.c["context"], "-n", self.c["namespace"], "port-forward", "svc/shop-web", "3000:3000"]}


def main(action=None, argv=None):
    purpose = {
        "deploy": "Deploy an isolated RecShop business instance",
        "start": "Start an existing RecShop business instance",
        "check": "Check readiness of an existing RecShop business instance",
        "render": "Render an offline RecShop deployment plan",
    }.get(action, "Manage an isolated RecShop business instance")
    parser = argparse.ArgumentParser(
        description=purpose + "; never operate the protected collection namespace.")
    if action is None:
        parser.add_argument("action", choices=("render", "deploy", "start", "check"))
    parser.add_argument("--config", required=True)
    parser.add_argument("--render", action="store_true", help="Offline deployment plan only")
    parser.add_argument("--out", help="Output path for an offline plan (never contains Secret values)")
    args = parser.parse_args(argv)
    lock = None
    lock_acquired = False
    try:
        path, c = load_config(args.config)
        selected = "render" if args.render else (action or args.action)
        if selected == "render":
            plan = build(c, "offline-render")
            local = Path(c["assets"]["local_dir"])
            if not local.is_absolute():
                local = path.parent / local
            missing = [s["name"] for s in c["assets"]["files"].values() if not (local / s["name"]).is_file()]
            data = {"status": "OFFLINE_RENDERED_NOT_DEPLOYED", "applications": 25, "namespace": c["namespace"],
                    "secret_values_included": False, "secret_environment_names": list(c["secrets"].values()),
                    "missing_local_assets": missing, "collection_ready": False, "plan": plan}
            if args.out:
                save(Path(args.out).resolve(), data)
                print(json.dumps({k: v for k, v in data.items() if k != "plan"}, ensure_ascii=False))
            else:
                print(json.dumps(data, ensure_ascii=False))
            return 0
        # Only a local exclusive lock is written by check; it never mutates Kubernetes.
        lock = (path.parent / ".recshop-state" / (c["instance_id"] + ".json")).with_suffix(".lock")
        lock.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            raise Failure("instance_operation_already_running") from None
        os.close(fd)
        lock_acquired = True
        inst = Instance(path, c)  # Read persistent UIDs only after acquiring this operation's lock.
        result = getattr(inst, selected)()
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except Failure as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}))
        return 2
    except Exception:
        # Configuration and backend responses can contain sensitive values; do not echo them.
        print(json.dumps({"status": "FAILED", "reason": "invalid_input_or_unexpected_backend_response"}))
        return 2
    finally:
        if lock_acquired and lock is not None and lock.exists():
            lock.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
