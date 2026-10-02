"""Single-Deployment sampling maintenance; no fault/sample qualification.

Planning reads only. apply/restore require explicit execute=True and an owned
write-ahead receipt. Real command transport is disabled unless explicitly set.
Failed receipts are closed only by evidence-backed reconcile_receipt, never by
manual wiping; reconciliation retains the failure history (qualify_recovery
semantics: recovery eligibility is not a success claim).
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import time
import uuid

from scripts.collection import journal as j
from scripts.collection import runner as r
from scripts.collection.live_runtime import BoundedProcess
from ops.metrics.maintenance_receipt import (PASS_STATES, RECONCILE_ELIGIBLE_STATES,
                                        RECONCILED_PASS_STATES, verify_maintenance_marker)

SCHEMA = "recshop-m1-sampling-maintenance-v1"
OWNER = "recshop.dev/m1-sampling-maintenance"
FAULT_OWNER = "recshop.dev/m1-owner"
ENV_KEYS = ("OTEL_METRIC_EXPORT_INTERVAL", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
HELPER = "/app/shared/otel_metric_config.py"


class MaintenanceError(RuntimeError):
    """Reason codes only; resource/env/command payloads never enter errors."""


def need(ok, reason):
    if not ok:
        raise MaintenanceError(reason)


def raw(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


def clone(value):
    return json.loads(raw(value))


def positive(value):
    need(type(value) in (int, float) and math.isfinite(value) and value > 0, "positive_finite_budget_required")


def checked_path(value):
    path = Path(value)
    need(path.is_absolute() and ".." not in path.parts and path.resolve() == path, "absolute_unredirected_path_required")
    return path


def read_pinned(path, expected_sha):
    path = checked_path(path)
    data = path.read_bytes()
    need(sha(data) == expected_sha, "input_hash_mismatch")
    return json.loads(data)


def _one(rows, key, value):
    found = [row for row in rows if row.get(key) == value]
    need(len(found) == 1, "unique_row_required")
    return found[0]


def load_release(*, build_plan, build_plan_sha256, build_results, build_results_sha256,
                 config_verification, config_verification_sha256, deployment):
    """Bind current release to explicitly reviewed local artifact bytes."""
    plan = read_pinned(build_plan, build_plan_sha256)
    built = read_pinned(build_results, build_results_sha256)
    config = read_pinned(config_verification, config_verification_sha256)
    need(plan.get("schema_version") == "recshop-m1-sampling-plan-v1"
         and built.get("plan_sha256") == build_plan_sha256, "build_plan_identity_mismatch")
    source = _one(plan["services"], "deployment", deployment)
    image = _one(built["images"], "deployment", deployment)
    verification = _one(config["rows"], "deployment", deployment)
    need(image.get("status") == "BUILT_AND_ISOLATED_CONTENT_VERIFIED" and image.get("build_exit_code") == 0
         and image["base_image_id"] == source["base_image_id"]
         and image["new_image_tag"] == source["new_image_tag"], "unverified_build")
    need(image["content_check"] == {"source": source["modified_source_sha256"], "helper": source["helper_sha256"],
                                   "default": 15000, "m1": 2000}, "built_content_differs")
    need(verification.get("original_tag_current_id") == source["base_image_id"]
         and verification.get("new_image_id") == image["image_id"]
         and verification.get("original_config_sha256") == verification.get("new_config_sha256")
         and bool(verification.get("original_config_sha256"))
         and verification.get("config_fields_equal") == dict.fromkeys(("Cmd", "Entrypoint", "User", "WorkingDir", "Env"), True),
         "inherited_config_not_verified")
    destinations = [v.split("=", 1)[1] for v in source["build_command"] if v.startswith("SERVICE_DESTINATION=")]
    need(len(destinations) == 1 and destinations[0].startswith("/app/") and ".." not in PurePosixPath(destinations[0]).parts,
         "source_path_not_pinned")
    return {"deployment": deployment, "deployment_uid": source["deployment_uid_at_snapshot"], "container": source["container"],
            "original_image": source["original_image"], "base_image_id": source["base_image_id"],
            "target_image": image["new_image_tag"], "target_image_id": image["image_id"],
            "source_path": destinations[0], "source_before_sha256": source["base_source_sha256"],
            "source_after_sha256": source["modified_source_sha256"], "helper_sha256": source["helper_sha256"],
            "release_refs": {"build_plan": build_plan_sha256, "build_results": build_results_sha256,
                             "config_verification": config_verification_sha256}}


class KubectlClient:
    """Bounded transport; only this tool's fixed read/patch operations."""
    evidence_kind = "observed"

    def __init__(self, executable, context, namespace, *, execute=False, process=None,
                 timeout_s=30, rollout_timeout_s=300, max_output_bytes=4_000_000):
        positive(timeout_s); positive(rollout_timeout_s)
        need(type(max_output_bytes) is int and max_output_bytes > 0, "response_limit_required")
        need(all(type(v) is str and v and not any(x in v for x in "\r\n\0") for v in (executable, context, namespace)), "explicit_route_required")
        self.executable, self.context, self.namespace = executable, context, namespace
        self.process = process if process is not None else BoundedProcess(execute=execute)
        self.execute, self.timeout, self.rollout_timeout, self.limit = execute, timeout_s, rollout_timeout_s, max_output_bytes
        self.evidence = []

    def run(self, args, *, wait=False, stdin=None):
        need(self.execute, "runtime_commands_disabled")
        result = self.process.run((self.executable, "--context", self.context, "--namespace", self.namespace, *args),
                                  timeout_s=self.rollout_timeout + self.timeout if wait else self.timeout,
                                  max_output_bytes=self.limit, stdin=stdin)
        self.evidence.append({"operation": args[0], "status": result.status, "return_code": result.return_code,
                              "stdout_sha256": sha(result.stdout), "stderr_sha256": sha(result.stderr),
                              "started_monotonic_s": result.started_monotonic_s, "ended_monotonic_s": result.ended_monotonic_s})
        need(result.status == "ok" and result.return_code == 0 and result.workers_joined, "bounded_command_failed")
        return result.stdout

    def get(self, kind, name=None, *, selector=None):
        need(kind in {"namespace", "deployment", "replicaset", "pod", "node"}, "unapproved_read_kind")
        args = ["get", kind]
        if name is not None:
            args.append(name)
        if selector is not None:
            args += ["-l", ",".join(k + "=" + v for k, v in sorted(selector.items()))]
        return json.loads(self.run(tuple(args + ["-o", "json"])))

    def rollout(self, name):
        self.run(("rollout", "status", "deployment/" + name, "--timeout=" + str(int(self.rollout_timeout)) + "s"), wait=True)

    def wait_pod_deleted(self, name):
        self.run(("wait", "pod/" + name, "--for=delete", "--timeout=" + str(int(self.rollout_timeout)) + "s"), wait=True)

    def patch(self, name, operations):
        self.run(("patch", "deployment", name, "--type=json", "-p", raw(operations).decode(), "-o", "json"))

    def runtime(self, pod, container, source_path):
        # Only source/hash and two selected PID1 env values are emitted.
        script = "\n".join(("import hashlib,json,pathlib,sys", "source=pathlib.Path(sys.argv[1])", "helper=pathlib.Path(sys.argv[2])",
                            "env=dict(x.split(b'=',1) for x in pathlib.Path('/proc/1/environ').read_bytes().split(b'\\0') if b'=' in x)",
                            "keys=" + repr(ENV_KEYS),
                            "print(json.dumps({'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'helper_sha256':hashlib.sha256(helper.read_bytes()).hexdigest() if helper.exists() else None,'env':{k:env[k.encode()].decode() if k.encode() in env else None for k in keys}}))"))
        return json.loads(self.run(("exec", "pod/" + pod, "--container", container, "--", "python", "-c", script, source_path, HELPER)))


class PinnedCRIImages:
    """Read a caller-produced, hash-pinned per-node CRI inspect receipt.

    This is time-bounded prior runtime evidence, not a claim of atomic presence
    at scheduling. Deployment uses Never; live Pod image/content readback follows.
    """
    def __init__(self, path, expected_sha256, *, max_age_s, clock=time.time):
        positive(max_age_s)
        self.path, self.expected, self.max_age, self.clock = path, expected_sha256, max_age_s, clock

    def inspect(self, node, tag, expected_config_id):
        value = read_pinned(self.path, self.expected)
        root_format = value.get("schema_version") == "root-node-image-identity-evidence-v1"
        if root_format:
            value = self._root_format(value)
        need(value.get("schema_version") == "recshop-m1-node-cri-images-v1", "node_receipt_schema")
        need(value.get("evidence_kind") in {"observed", "synthetic"}, "node_receipt_evidence_kind_required")
        age = self.clock() - value["observed_at_epoch_s"]
        need(0 <= age <= self.max_age, "stale_node_image_evidence")
        meta, info = node["metadata"], node["status"]["nodeInfo"]
        need(value.get("node") == {"name": meta["name"], "uid": meta["uid"], "os": info["operatingSystem"], "architecture": info["architecture"]},
             "node_image_identity_mismatch")
        if value.get("boot_id") is not None:
            need(value["boot_id"] == info.get("bootID"), "node_rebooted_since_image_evidence")
        row = _one(value["images"], "requested_tag", tag)
        if root_format:
            inspect, inspect_hash = {"status": row["verified_cri_status"]}, value["cri_list_sha256"]
        elif "inspect_raw" in row:
            inspect = self._raw_json(row["inspect_raw"])
            inspect_hash = row["inspect_raw"]["sha256"]
        else:
            need(value["evidence_kind"] == "synthetic", "observed_inspect_requires_original_bytes")
            inspect = row["inspect"]
            need(row.get("inspect_sha256") == sha(raw(inspect)), "cri_projection_hash_mismatch")
            inspect_hash = row["inspect_sha256"]
        status = inspect["status"]
        # Runtime image IDs and Docker build IDs can refer to different OCI
        # objects. The reviewed raw CRI repoDigest binds this runtime ID to the
        # built image; never equate the two identifier kinds implicitly.
        canonical_tag = tag if "/" in tag else "docker.io/library/" + tag
        need(tag in status.get("repoTags", []) or canonical_tag in status.get("repoTags", []), "cri_requested_tag_missing")
        chain = row.get("oci_chain")
        manifest_digest = None
        if chain is not None:
            ctr_bytes = self._raw_bytes(chain["ctr_images_raw"])
            lines = [line.split() for line in ctr_bytes.decode().splitlines()]
            refs = [line for line in lines if line and line[0] in {tag, canonical_tag}]
            need(len(refs) == 1 and len(refs[0]) >= 3 and refs[0][2] == expected_config_id, "ctr_tag_target_differs_from_build")
            index_bytes = self._raw_bytes(chain["index_raw"])
            need("sha256:" + sha(index_bytes) == expected_config_id, "oci_index_digest_mismatch")
            index = json.loads(index_bytes)
            descriptors = [v for v in index.get("manifests", []) if v.get("platform", {}).get("os") == info["operatingSystem"]
                           and v.get("platform", {}).get("architecture") == info["architecture"]]
            need(len(descriptors) == 1, "unique_node_platform_manifest_required")
            manifest_bytes = self._raw_bytes(chain["manifest_raw"])
            manifest_digest = "sha256:" + sha(manifest_bytes)
            need(descriptors[0].get("digest") == manifest_digest
                 and json.loads(manifest_bytes).get("config", {}).get("digest") == status.get("id"), "oci_manifest_to_cri_config_mismatch")
        else:
            need(any(v.endswith("@" + expected_config_id) for v in status.get("repoDigests", [])), "cri_tag_to_built_digest_mismatch")
        aliases = [status["id"], *status.get("repoDigests", [])]
        if manifest_digest:
            repository = tag.rsplit(":", 1)[0]
            aliases += [manifest_digest, expected_config_id, repository + "@" + manifest_digest,
                        "docker.io/library/" + repository + "@" + manifest_digest]
        need(all(type(v) is str and v for v in aliases), "runtime_image_alias_missing")
        return {"node_uid": meta["uid"], "tag": tag, "built_image_id": expected_config_id, "cri_config_id": status["id"], "runtime_ids": aliases,
                "evidence_kind": value["evidence_kind"], "receipt_sha256": self.expected, "inspect_sha256": inspect_hash,
                "observed_at_epoch_s": value["observed_at_epoch_s"]}

    @staticmethod
    def _raw_bytes(reference):
        path = checked_path(reference["path"])
        need(path.stat().st_size <= 4_000_000, "node_evidence_file_too_large")
        data = path.read_bytes()
        need(sha(data) == reference["sha256"], "node_raw_artifact_hash_mismatch")
        return data

    @classmethod
    def _raw_json(cls, reference):
        return json.loads(cls._raw_bytes(reference))

    @classmethod
    def _root_format(cls, evidence):
        """Adapt root's actual 50-image receipt, recomputing source relations."""
        inventory = cls._raw_json(evidence["cri_list_raw"])
        images = inventory["images"]
        converted = []
        for service in evidence["images"]:
            need(service.get("node_uid") == evidence["node_uid"] and service.get("os") == evidence["node_info"]["operatingSystem"]
                 and service.get("architecture") == evidence["node_info"]["architecture"], "service_node_image_binding_mismatch")
            for role in ("original", "new"):
                row = service[role]
                status = _one(images, "id", row["config_digest"])
                need(status == row["cri_status"] and row["ctr_target_digest"] == row["build_image_id"]
                     and row["config_digest"] == status["id"]
                     and "sha256:" + row["manifest_raw"]["sha256"] == row["platform_manifest_digest"], "root_cri_projection_mismatch")
                name = row["tag"]
                need(name.startswith("docker.io/library/"), "unsupported_root_image_repository")
                converted.append({"requested_tag": name.removeprefix("docker.io/library/"), "verified_cri_status": status,
                                  "oci_chain": {"ctr_images_raw": evidence["ctr_list_raw"], "index_raw": row["target_raw"], "manifest_raw": row["manifest_raw"]}})
        timestamp = datetime.fromisoformat(evidence["time"])
        need(timestamp.tzinfo is not None, "node_evidence_timezone_required")
        return {"schema_version": "recshop-m1-node-cri-images-v1", "evidence_kind": "observed", "observed_at_epoch_s": timestamp.timestamp(),
                "boot_id": evidence["node_info"].get("bootID"), "cri_list_sha256": evidence["cri_list_raw"]["sha256"],
                "node": {"name": evidence["node_name"], "uid": evidence["node_uid"], "os": evidence["node_info"]["operatingSystem"],
                         "architecture": evidence["node_info"]["architecture"]}, "images": converted}


def _uid(obj, kind, name=None, namespace=None):
    meta = obj.get("metadata", {})
    need(obj.get("kind") == kind and bool(meta.get("uid")) and bool(meta.get("resourceVersion"))
         and not meta.get("deletionTimestamp") and (name is None or meta.get("name") == name)
         and (namespace is None or meta.get("namespace") == namespace), "resource_identity_invalid")
    return meta["uid"]


def _selector(value, reason):
    """Selector pin contract: matchLabels carrying exactly the single key "app".

    The pinned value may historically differ from the Deployment name (the
    cluster-side selector is immutable); the shape may not. Anything broader
    (multi-key labels, matchExpressions, non-string value) is an anomalous
    deployment and is refused loudly.
    """
    need(type(value) is dict and set(value) == {"matchLabels"}
         and type(value["matchLabels"]) is dict and set(value["matchLabels"]) == {"app"}
         and type(value["matchLabels"]["app"]) is str and value["matchLabels"]["app"] != "", reason)
    return {"matchLabels": {"app": value["matchLabels"]["app"]}}


def _controller(obj, kind):
    rows = [v for v in obj["metadata"].get("ownerReferences", []) if v.get("controller") is True]
    need(len(rows) == 1 and rows[0].get("kind") == kind, "controller_chain_invalid")
    return rows[0]


def _entry(mapping, key):
    return {"present": key in mapping, "value": mapping.get(key)}


def _container(obj, name):
    return _one(obj["spec"]["template"]["spec"]["containers"], "name", name)


def _fields(obj, container):
    item = _container(obj, container)
    values = {}
    rows = item.get("env", [])
    need(type(rows) is list and len({v["name"] for v in rows}) == len(rows), "duplicate_or_invalid_env")
    for key in ENV_KEYS:
        found = [v for v in rows if v["name"] == key]
        if found:
            need(set(found[0]) == {"name", "value"} and type(found[0]["value"]) is str, "selected_env_valueFrom_or_null_unsupported")
        values[key] = {"present": bool(found), "value": found[0]["value"] if found else None}
    policy = _entry(item, "imagePullPolicy")
    need(not policy["present"] or policy["value"] in {"Always", "Never", "IfNotPresent"}, "invalid_pull_policy")
    return {"image": item["image"], "image_pull_policy": policy, "env": values,
            "owner": _entry(obj["metadata"].get("annotations", {}), OWNER),
            "template_owner": _entry(obj["spec"]["template"]["metadata"].get("annotations", {}), OWNER)}


def _guard(obj, container):
    spec = clone(obj["spec"])
    item = _one(spec["template"]["spec"]["containers"], "name", container)
    item.pop("image", None); item.pop("imagePullPolicy", None)
    remainder = [v for v in item.get("env", []) if v["name"] not in ENV_KEYS]
    item.pop("env", None)
    if remainder:
        item["env"] = remainder
    annotations = spec["template"]["metadata"].get("annotations", {})
    annotations.pop(OWNER, None)
    if not annotations:
        spec["template"]["metadata"].pop("annotations", None)
    return sha(raw(spec))


def _patch(obj, container, desired):
    index = next(i for i, v in enumerate(obj["spec"]["template"]["spec"]["containers"]) if v["name"] == container)
    item, base = _container(obj, container), "/spec/template/spec/containers/" + str(index)
    ops = [{"op": "test", "path": "/metadata/uid", "value": obj["metadata"]["uid"]},
           {"op": "test", "path": "/metadata/resourceVersion", "value": obj["metadata"]["resourceVersion"]},
           {"op": "replace", "path": base + "/image", "value": desired["image"]}]
    entry = desired["image_pull_policy"]
    if entry["present"]:
        ops.append({"op": "add", "path": base + "/imagePullPolicy", "value": entry["value"]})
    elif "imagePullPolicy" in item:
        ops.append({"op": "remove", "path": base + "/imagePullPolicy"})
    # Never serialize unrelated environment values into the patch/receipt.
    rows = clone(item.get("env", []))
    for key in ENV_KEYS:
        existing = [i for i, row in enumerate(rows) if row["name"] == key]
        entry = desired["env"][key]
        if existing:
            i = existing[0]
            if entry["present"]:
                rows[i] = {"name": key, "value": entry["value"]}
                ops.append({"op": "replace", "path": base + "/env/" + str(i), "value": rows[i]})
            else:
                rows.pop(i)
                ops.append({"op": "remove", "path": base + "/env/" + str(i)})
        elif entry["present"]:
            row = {"name": key, "value": entry["value"]}
            if "env" not in item and not rows:
                ops.append({"op": "add", "path": base + "/env", "value": [row]})
            else:
                ops.append({"op": "add", "path": base + "/env/-", "value": row})
            rows.append(row)
    for path, meta, key in (("/metadata", obj["metadata"], "owner"), ("/spec/template/metadata", obj["spec"]["template"]["metadata"], "template_owner")):
        entry, annotations = desired[key], meta.get("annotations")
        escaped = OWNER.replace("/", "~1")
        if entry["present"]:
            ops.append({"op": "add", "path": path + "/annotations" + ("/" + escaped if annotations is not None else ""),
                        "value": entry["value"] if annotations is not None else {OWNER: entry["value"]}})
        elif annotations is not None and OWNER in annotations:
            ops.append({"op": "remove", "path": path + "/annotations/" + escaped})
    return ops


def _runtime_id(value):
    return value.split("://", 1)[-1]


class SamplingRollout:
    def __init__(self, release, client, image_provider, *, cluster_uid, namespace_uid, endpoint,
                 receipt_root, lease_root, max_plan_age_s, registry_root=None, clock=time.time):
        positive(max_plan_age_s)
        need(re.fullmatch(r"http://host\.docker\.internal:[0-9]{1,5}", endpoint) is not None
             and 1 <= int(endpoint.rsplit(":", 1)[1]) <= 65535, "explicit_metrics_only_endpoint_required")
        self.release, self.client, self.images = clone(release), client, image_provider
        self.identity = r.EnvironmentIdentity(cluster_uid, namespace_uid)
        self.root, self.lease_root = checked_path(receipt_root), checked_path(lease_root)
        self.registry = checked_path(registry_root) if registry_root else r.environment_registry_root()
        self.clock, self.max_age, self.endpoint = clock, max_plan_age_s, endpoint
        need(client.evidence_kind in {"observed", "synthetic"}, "explicit_evidence_kind_required")
        need(registry_root is None or client.evidence_kind == "synthetic", "live_registry_override_forbidden")
        need(self.root != self.lease_root and self.root != self.registry, "distinct_maintenance_receipt_root_required")

    def _environment(self):
        need(_uid(self.client.get("namespace", "kube-system"), "Namespace", "kube-system") == self.identity.cluster_uid
             and _uid(self.client.get("namespace", self.client.namespace), "Namespace", self.client.namespace) == self.identity.namespace_uid,
             "cluster_namespace_changed")
        nodes = self.client.get("node")["items"]
        need(len(nodes) == 1, "only_single_node_cluster_supported")
        node = nodes[0]
        _uid(node, "Node")
        need(not node.get("spec", {}).get("unschedulable") and any(v.get("type") == "Ready" and v.get("status") == "True" for v in node["status"]["conditions"]), "node_not_ready")
        return node

    def _deployment(self, expected_selector=None):
        data = self.release
        obj = self.client.get("deployment", data["deployment"])
        need(_uid(obj, "Deployment", data["deployment"], self.client.namespace) == data["deployment_uid"], "deployment_uid_changed")
        selector = _selector(obj["spec"].get("selector"), "selector_shape_invalid")
        if expected_selector is not None:
            need(selector == expected_selector, "selector_changed")
        need(not obj["metadata"].get("annotations", {}).get(FAULT_OWNER)
             and not obj["spec"]["template"]["metadata"].get("annotations", {}).get(FAULT_OWNER), "active_fault_owner")
        _fields(obj, data["container"])
        return obj

    def _runtime(self, obj, expected_fields, source_hash, helper_hash, expected_env=None, *, wait, selector):
        data, container = self.release, self.release["container"]
        signature = (_fields(obj, container), _guard(obj, container), obj["metadata"]["generation"])
        if wait:
            self.client.rollout(data["deployment"])
        obj = self._deployment(selector)
        need((_fields(obj, container), _guard(obj, container), obj["metadata"]["generation"]) == signature, "deployment_changed_during_wait")
        need(_fields(obj, container) == expected_fields, "controlled_fields_mismatch")
        replicas = obj["spec"].get("replicas", 1)
        status = obj["status"]
        need(type(replicas) is int and replicas > 0 and status.get("observedGeneration", -1) >= obj["metadata"]["generation"]
             and all(status.get(key, 0) == replicas for key in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas")), "deployment_not_coherently_ready")
        node = self._environment()
        wanted_id = data["target_image_id"] if expected_fields["image"] == data["target_image"] else data["base_image_id"]
        image = self.images.inspect(node, expected_fields["image"], wanted_id)
        need(image.get("evidence_kind") == self.client.evidence_kind, "node_runtime_evidence_kind_mismatch")
        pods = self.client.get("pod", selector=selector["matchLabels"])["items"]
        for pod in pods:
            if pod["metadata"].get("deletionTimestamp"):
                owner = _controller(pod, "ReplicaSet")
                rs = self.client.get("replicaset", owner["name"])
                need(rs["metadata"]["uid"] == owner["uid"] and _controller(rs, "Deployment")["uid"] == data["deployment_uid"], "terminating_pod_owner_changed")
                need(wait, "terminating_pod_at_plan")
                self.client.wait_pod_deleted(pod["metadata"]["name"])
        if any(p["metadata"].get("deletionTimestamp") for p in pods):
            pods = self.client.get("pod", selector=selector["matchLabels"])["items"]
        need(len(pods) == replicas, "pod_count_mismatch")
        facts = []
        for pod in pods:
            uid = _uid(pod, "Pod", namespace=self.client.namespace)
            need(pod["spec"].get("nodeName") == node["metadata"]["name"], "pod_node_mismatch")
            owner = _controller(pod, "ReplicaSet")
            rs = self.client.get("replicaset", owner["name"])
            need(_uid(rs, "ReplicaSet", owner["name"], self.client.namespace) == owner["uid"]
                 and _controller(rs, "Deployment")["uid"] == data["deployment_uid"], "pod_owner_changed")
            template = clone(rs["spec"]["template"])
            template["metadata"].get("labels", {}).pop("pod-template-hash", None)
            need(template == obj["spec"]["template"], "replicaset_template_mismatch")
            cs = _one(pod["status"]["containerStatuses"], "name", container)
            need(cs.get("ready") is True and cs.get("containerID") and type(cs.get("restartCount")) is int
                 and cs.get("state", {}).get("running", {}).get("startedAt")
                 and _runtime_id(cs.get("imageID", "")) in image["runtime_ids"], "container_image_or_state_mismatch")
            need(_one(pod["spec"]["containers"], "name", container)["image"] == expected_fields["image"], "pod_image_tag_mismatch")
            runtime = self.client.runtime(pod["metadata"]["name"], container, data["source_path"])
            need(runtime.get("source_sha256") == source_hash and runtime.get("helper_sha256") == helper_hash, "runtime_code_mismatch")
            need(type(runtime.get("env")) is dict and set(runtime["env"]) == set(ENV_KEYS)
                 and all(v is None or type(v) is str for v in runtime["env"].values()), "selected_runtime_env_missing")
            if expected_env is not None:
                need(runtime["env"] == expected_env, "runtime_env_mismatch")
            else:
                for key, entry in expected_fields["env"].items():
                    if entry["present"]:
                        need(runtime["env"][key] == entry["value"], "literal_runtime_env_mismatch")
            after = self.client.get("pod", pod["metadata"]["name"])
            need(_uid(after, "Pod", pod["metadata"]["name"], self.client.namespace) == uid
                 and after["metadata"].get("ownerReferences") == pod["metadata"].get("ownerReferences")
                 and _one(after["status"]["containerStatuses"], "name", container) == cs, "pod_changed_during_exec")
            facts.append({"pod_name": pod["metadata"]["name"], "pod_uid": uid, "container_id": cs["containerID"],
                          "image_id": cs["imageID"], "restart_count": cs["restartCount"], "runtime": runtime})
        fresh = self._deployment(selector)
        need((_fields(fresh, container), _guard(fresh, container), fresh["metadata"]["generation"]) == signature, "deployment_changed_during_runtime_read")
        need(self._environment()["metadata"]["uid"] == node["metadata"]["uid"], "node_changed_during_runtime_read")
        return fresh, {"pods": facts, "image_visibility": image, "observed_at_epoch_s": self.clock(),
                       "deployment": {"uid": fresh["metadata"]["uid"], "resource_version": fresh["metadata"]["resourceVersion"],
                                      "generation": fresh["metadata"]["generation"], "fields": _fields(fresh, container),
                                      "guard_sha256": _guard(fresh, container)}}

    def plan(self):
        node, obj = self._environment(), self._deployment()
        # Pin the observed immutable Deployment selector; every later read must
        # match this pinned value instead of re-deriving it from the name.
        selector = _selector(obj["spec"].get("selector"), "selector_shape_invalid")
        original = _fields(obj, self.release["container"])
        need(not original["owner"]["present"] and not original["template_owner"]["present"], "deployment_already_maintained")
        need(original["image"] == self.release["original_image"], "baseline_image_tag_changed")
        self.images.inspect(node, self.release["target_image"], self.release["target_image_id"])
        obj, runtime = self._runtime(obj, original, self.release["source_before_sha256"], None, wait=False, selector=selector)
        envs = [p["runtime"]["env"] for p in runtime["pods"]]
        need(all(v == envs[0] for v in envs), "baseline_env_varies_by_pod")
        owner = "m1-sampling-" + uuid.uuid4().hex
        desired = clone(original)
        desired.update(image=self.release["target_image"], image_pull_policy={"present": True, "value": "Never"},
                       owner={"present": True, "value": owner}, template_owner={"present": True, "value": owner})
        desired["env"] = {ENV_KEYS[0]: {"present": True, "value": "2000"}, ENV_KEYS[1]: {"present": True, "value": self.endpoint}}
        return {"schema_version": SCHEMA, "purpose": "sampling_configuration_maintenance_not_fault_sample", "status": "PLANNED",
                "created_at_epoch_s": self.clock(), "owner": owner, "release": clone(self.release),
                "environment": {"cluster_uid": self.identity.cluster_uid, "namespace_uid": self.identity.namespace_uid,
                                "context": self.client.context, "namespace": self.client.namespace},
                "selector": selector, "original": original, "desired": desired,
                "guard_sha256": _guard(obj, self.release["container"]),
                "original_env_key_present": "env" in _container(obj, self.release["container"]),
                "original_runtime_env": envs[0], "baseline_runtime": runtime, "evidence_kind": self.client.evidence_kind}

    def _validate_plan(self, plan):
        need(plan.get("schema_version") == SCHEMA and plan.get("release") == self.release
             and plan.get("evidence_kind") == self.client.evidence_kind
             and plan.get("environment") == {"cluster_uid": self.identity.cluster_uid, "namespace_uid": self.identity.namespace_uid,
                                             "context": self.client.context, "namespace": self.client.namespace}, "plan_identity_mismatch")
        need(re.fullmatch(r"m1-sampling-[0-9a-f]{32}", plan.get("owner", "")), "plan_owner_invalid")
        _selector(plan.get("selector"), "plan_selector_invalid")
        expected = clone(plan["original"])
        expected.update(image=self.release["target_image"], image_pull_policy={"present": True, "value": "Never"},
                        owner={"present": True, "value": plan["owner"]}, template_owner={"present": True, "value": plan["owner"]})
        expected["env"] = {ENV_KEYS[0]: {"present": True, "value": "2000"}, ENV_KEYS[1]: {"present": True, "value": self.endpoint}}
        need(plan.get("desired") == expected and plan["original"]["image"] == self.release["original_image"]
             and plan["original"]["owner"] == {"present": False, "value": None}
             and plan["original"]["template_owner"] == {"present": False, "value": None}, "plan_controlled_field_tampering")
        # original/desired are container-field projections; the pinned selector
        # lives only at plan level and the mutation contract never touches
        # spec.selector, so neither projection may carry a selector entry.
        need("selector" not in plan["original"] and "selector" not in plan["desired"]
             and set(plan["desired"]) == set(plan["original"]), "desired_must_not_touch_selector")

    def _receipt(self, owner):
        path = self.root / owner
        need(path.resolve() == path, "receipt_path_redirected")
        return path

    def _event(self, path, event, payload):
        need(path.resolve() == path, "receipt_redirected")
        filename = str(time.time_ns()) + "-" + uuid.uuid4().hex + "-" + event + ".json"
        with (path / filename).open("xb") as f:
            f.write(raw({"schema_version": SCHEMA, "event": event, "time_epoch_s": self.clock(), "payload": payload})); f.flush(); os.fsync(f.fileno())
        return filename

    def _state(self, path, status, *, failed, **extra):
        event = self._event(path, "state", {"status": status, "failed": failed, **extra})
        r._atomic_json(path / "state.json", {"schema_version": SCHEMA, "status": status, "failed": failed, **extra})
        marker = {"schema_version": SCHEMA, "environment_key": self.identity.key, "owner": path.name,
                  "receipt": str(path), "status": status, "plan_sha256": sha((path / "plan.json").read_bytes()),
                  "state_sha256": sha((path / "state.json").read_bytes()), "completed_event": event,
                  "completed_event_sha256": sha((path / event).read_bytes())}
        r._atomic_json(self.registry / (self.identity.key + ".maintenance.json"), marker)

    @contextmanager
    def _exclusive(self, owner):
        # Same physical lock as R; a fault writer cannot overlap maintenance.
        self.registry.mkdir(parents=True, exist_ok=True)
        need(self.registry.resolve() == self.registry, "registry_redirected")
        lock = self.registry / (self.identity.key + ".lock")
        need(lock.resolve() == lock, "environment_lock_redirected")
        fd = j._lock_file(lock)
        try:
            marker_path = self.registry / (self.identity.key + ".maintenance.json")
            need(not marker_path.is_symlink() and marker_path.resolve() == marker_path, "maintenance_marker_redirected")
            if marker_path.exists():
                need(marker_path.is_file(), "maintenance_marker_not_regular_file")
                marker = json.loads(marker_path.read_bytes())
                need(marker.get("environment_key") == self.identity.key, "maintenance_marker_identity_mismatch")
                if marker.get("owner") != owner:
                    verify_maintenance_marker(self.registry, self.identity.key)
            binding_path = self.registry / (self.identity.key + ".binding.json")
            need(not binding_path.is_symlink() and binding_path.resolve() == binding_path, "lease_binding_redirected")
            if binding_path.exists():
                need(binding_path.is_file(), "lease_binding_not_regular_file")
                binding = json.loads(binding_path.read_bytes())
                need(binding.get("cluster_uid") == self.identity.cluster_uid and binding.get("namespace_uid") == self.identity.namespace_uid
                     and binding.get("lease_root") == str(self.lease_root), "lease_binding_mismatch")
                state_path = self.lease_root / (self.identity.key + ".state.json")
                need(state_path.resolve() == state_path and state_path.is_file(), "lease_state_missing")
                state = json.loads(state_path.read_bytes())
                need(state.get("schema_version") == r.RUNNER_SCHEMA and state.get("status") == "clean"
                     and state.get("environment_key") == self.identity.key and state.get("workers") == "drained", "active_or_dirty_fault_lease")
            self.root.mkdir(parents=True, exist_ok=True)
            need(self.root.resolve() == self.root, "receipt_root_redirected")
            for path in self.root.iterdir():
                if path.is_dir() and path.name != owner:
                    need(path.resolve() == path and (path / "state.json").is_file(), "unknown_maintenance_receipt")
                    state = json.loads((path / "state.json").read_bytes())
                    # Reconciled receipts pass with failed=true (failure history
                    # retained); every pending or plain-failed receipt still blocks.
                    need(state.get("status") in PASS_STATES
                         and state.get("failed") is (state.get("status") in RECONCILED_PASS_STATES), "unfinished_other_maintenance")
            yield
        finally:
            j._unlock_file(fd)

    def apply(self, plan, *, execute=False, checkpoint=None):
        self._validate_plan(plan)
        if not execute:
            return {"status": "PLANNED_NOT_EXECUTED", "plan_sha256": sha(raw(plan))}
        need(type(execute) is bool and 0 <= self.clock() - plan["created_at_epoch_s"] <= self.max_age, "stale_plan_or_execute_invalid")
        with self._exclusive(plan["owner"]):
            path = self._receipt(plan["owner"])
            need(not path.exists(), "receipt_already_exists_use_restore")
            # Recheck runtime before opening the mutation attempt.
            obj = self._deployment(plan["selector"])
            need(_guard(obj, self.release["container"]) == plan["guard_sha256"] and _fields(obj, self.release["container"]) == plan["original"], "baseline_drift_since_plan")
            obj, baseline = self._runtime(obj, plan["original"], self.release["source_before_sha256"], None, plan["original_runtime_env"],
                                          wait=False, selector=plan["selector"])
            self.images.inspect(self._environment(), self.release["target_image"], self.release["target_image_id"])
            path.mkdir()
            with (path / "plan.json").open("xb") as f:
                f.write(raw(plan)); f.flush(); os.fsync(f.fileno())
            self._state(path, "APPLYING", failed=False, plan_sha256=sha(raw(plan)))
            try:
                self._change(path, plan, obj, plan["desired"], "apply", checkpoint)
                final, runtime = self._runtime(self._deployment(plan["selector"]), plan["desired"], self.release["source_after_sha256"], self.release["helper_sha256"],
                                               {ENV_KEYS[0]: "2000", ENV_KEYS[1]: self.endpoint}, wait=True, selector=plan["selector"])
                need(_guard(final, self.release["container"]) == plan["guard_sha256"], "uncontrolled_fields_changed")
                self._state(path, "INSTALLED_VERIFIED", failed=False, plan_sha256=sha(raw(plan)), runtime=runtime)
                return {"status": "INSTALLED_VERIFIED", "receipt": str(path), "sampling_cadence": "not_assessed"}
            except Exception as exc:
                self._state(path, "FAILED", failed=True, error_type=type(exc).__name__, plan_sha256=sha(raw(plan)))
                try:
                    self._restore(path, plan, failed=True, checkpoint=checkpoint)
                except Exception as recovery:
                    self._state(path, "BLOCKED", failed=True, error_type=type(recovery).__name__, plan_sha256=sha(raw(plan)))
                raise MaintenanceError("sampling_apply_failed_inspect_receipt") from None

    def _change(self, path, plan, obj, desired, operation, checkpoint):
        need(_guard(obj, self.release["container"]) == plan["guard_sha256"], "uncontrolled_fields_changed")
        if operation == "apply":
            need(0 <= self.clock() - plan["created_at_epoch_s"] <= self.max_age, "plan_expired_before_patch")
        operations = _patch(obj, self.release["container"], desired)
        if operation == "restore" and not plan["original_env_key_present"]:
            # Remove an originally absent env list only after selected additions are gone.
            remaining = [v for v in _container(obj, self.release["container"]).get("env", []) if v["name"] not in ENV_KEYS]
            need(not remaining, "originally_absent_env_has_foreign_entries")
            if "env" in _container(obj, self.release["container"]):
                index = next(i for i, v in enumerate(obj["spec"]["template"]["spec"]["containers"]) if v["name"] == self.release["container"])
                operations.append({"op": "remove", "path": "/spec/template/spec/containers/" + str(index) + "/env"})
        self._event(path, operation + "_intent", {"plan_sha256": sha(raw(plan)), "uid": obj["metadata"]["uid"],
                                                  "resource_version": obj["metadata"]["resourceVersion"], "patch": operations,
                                                  "original": plan["original"], "desired": desired})
        if checkpoint:
            checkpoint("after_" + operation + "_intent")
        self.client.patch(self.release["deployment"], operations)
        if checkpoint:
            checkpoint("after_" + operation + "_patch")
        self._event(path, operation + "_command_returned", {})

    def _restore(self, path, plan, *, failed, checkpoint=None, reconciled=None):
        self._environment()
        obj = self._deployment(plan["selector"])
        fields = _fields(obj, self.release["container"])
        need(_guard(obj, self.release["container"]) == plan["guard_sha256"], "uncontrolled_fields_changed")
        # A mutable original tag must still resolve to its reviewed original image.
        self.images.inspect(self._environment(), self.release["original_image"], self.release["base_image_id"])
        if fields == plan["desired"]:
            self._state(path, "RESTORING", failed=failed, plan_sha256=sha(raw(plan)))
            self._change(path, plan, obj, plan["original"], "restore", checkpoint)
        else:
            need(fields == plan["original"], "restore_owner_or_fields_changed")
        final, runtime = self._runtime(self._deployment(plan["selector"]), plan["original"], self.release["source_before_sha256"], None,
                                       plan["original_runtime_env"], wait=True, selector=plan["selector"])
        need(_guard(final, self.release["container"]) == plan["guard_sha256"], "restored_guard_changed")
        status, extra = ("FAILED_RESTORED" if failed else "RESTORED_VERIFIED"), {}
        if reconciled is not None:
            # Reconcile-completed restore: failed stays true so this is never
            # mistaken for a one-shot RESTORED_VERIFIED success; the dedicated
            # reconcile evidence section is stored alongside the runtime readback.
            need(failed, "reconciled_terminal_requires_retained_failure")
            status, extra = "RECONCILED_VERIFIED_RESTORED", {"reconcile": reconciled}
        self._state(path, status, failed=failed, plan_sha256=sha(raw(plan)), runtime=runtime, **extra)
        return {"status": status, "receipt": str(path), "sampling_cadence": "not_assessed"}

    def restore(self, owner, *, execute=False, checkpoint=None):
        need(re.fullmatch(r"m1-sampling-[0-9a-f]{32}", owner), "invalid_owner")
        path = self._receipt(owner)
        need(all((path / name).resolve() == path / name for name in ("plan.json", "state.json")), "receipt_file_redirected")
        plan = json.loads((path / "plan.json").read_bytes())
        self._validate_plan(plan)
        state = json.loads((path / "state.json").read_bytes())
        need(state.get("plan_sha256") == sha(raw(plan)), "receipt_plan_hash_mismatch")
        if not execute:
            return {"status": "RESTORE_PLANNED_NOT_EXECUTED", "owner": owner}
        need(type(execute) is bool, "explicit_execute_boolean_required")
        with self._exclusive(owner):
            try:
                need((path / "plan.json").read_bytes() == raw(plan) and json.loads((path / "state.json").read_bytes()) == state,
                     "receipt_changed_before_lock")
                return self._restore(path, plan, failed=state.get("failed", False) or state.get("status") in {"APPLYING", "RESTORING", "BLOCKED", "FAILED"}, checkpoint=checkpoint)
            except Exception as exc:
                self._state(path, "BLOCKED", failed=True, error_type=type(exc).__name__, plan_sha256=sha(raw(plan)))
                raise MaintenanceError("sampling_restore_blocked_inspect_receipt") from None

    def reconcile_receipt(self, owner, *, fresh_image_evidence, execute=False, checkpoint=None):
        """Close one failed maintenance receipt from fresh evidence.

        Eligibility is not success (qualify_recovery semantics): only
        BLOCKED/FAILED/FAILED_RESTORED receipts with failed=true enter, the
        prior failure history stays append-only, and the terminal state keeps
        failed=true so a reconciled receipt is never mistaken for a one-shot
        INSTALLED/RESTORED_VERIFIED success. Determination refusals leave the
        receipt untouched; only a chosen branch's execution failures re-block.
        """
        need(re.fullmatch(r"m1-sampling-[0-9a-f]{32}", owner), "invalid_owner")
        path = self._receipt(owner)
        need((path / "plan.json").is_file() and (path / "state.json").is_file(), "reconcile_receipt_missing")
        need(all((path / name).resolve() == path / name for name in ("plan.json", "state.json")), "receipt_file_redirected")
        plan = json.loads((path / "plan.json").read_bytes())
        self._validate_plan(plan)
        state = json.loads((path / "state.json").read_bytes())
        need(state.get("plan_sha256") == sha(raw(plan)), "receipt_plan_hash_mismatch")
        need(type(fresh_image_evidence) is PinnedCRIImages, "fresh_image_evidence_provider_required")
        need(state.get("schema_version") == SCHEMA and state.get("status") in RECONCILE_ELIGIBLE_STATES
             and state.get("failed") is True, "receipt_not_reconcilable")
        if not execute:
            return {"status": "RECONCILE_PLANNED_NOT_EXECUTED", "owner": owner, "prior_status": state["status"]}
        need(type(execute) is bool, "explicit_execute_boolean_required")
        with self._exclusive(owner):
            need((path / "plan.json").read_bytes() == raw(plan) and json.loads((path / "state.json").read_bytes()) == state,
                 "receipt_changed_before_lock")
            return self._reconcile(path, plan, state, fresh_image_evidence, checkpoint)

    def _reconcile(self, path, plan, prior_state, images, checkpoint):
        saved, self.images = self.images, images
        try:
            # Determination (read-only; any refusal leaves the receipt untouched):
            # fresh node-image evidence plus a live readback must place the
            # controlled fields exactly at desired or exactly at original.
            self._environment()
            obj = self._deployment(plan["selector"])
            need(_guard(obj, self.release["container"]) == plan["guard_sha256"], "uncontrolled_fields_changed")
            fields = _fields(obj, self.release["container"])
            if fields == plan["desired"]:
                branch = "completed_restore"
                obj, readback = self._runtime(obj, plan["desired"], self.release["source_after_sha256"],
                                              self.release["helper_sha256"], {ENV_KEYS[0]: "2000", ENV_KEYS[1]: self.endpoint},
                                              wait=False, selector=plan["selector"])
            elif fields == plan["original"]:
                branch = "verified_prior_restore"
                obj, readback = self._runtime(obj, plan["original"], self.release["source_before_sha256"], None,
                                              plan["original_runtime_env"], wait=False, selector=plan["selector"])
            else:
                need(False, "reconcile_foreign_or_intermediate_state")
            record = {"branch": branch, "prior_status": prior_state["status"],
                      "determination": {"deployment_uid": readback["deployment"]["uid"],
                                        "resource_version": readback["deployment"]["resource_version"],
                                        "generation": readback["deployment"]["generation"],
                                        "node_uid": readback["image_visibility"]["node_uid"],
                                        "fields": readback["deployment"]["fields"]},
                      "fresh_image_evidence": {key: readback["image_visibility"][key]
                                               for key in ("receipt_sha256", "observed_at_epoch_s", "evidence_kind",
                                                           "tag", "built_image_id", "cri_config_id", "node_uid")},
                      "readback_pods": [{"pod_uid": pod["pod_uid"], "container_id": pod["container_id"],
                                         "image_id": pod["image_id"]} for pod in readback["pods"]]}
            self._event(path, "reconcile_intent", {"plan_sha256": sha(raw(plan)), **record})
            try:
                if branch == "completed_restore":
                    result = self._restore(path, plan, failed=True, checkpoint=checkpoint, reconciled=record)
                    return {**result, "reconcile_branch": branch, "prior_status": prior_state["status"]}
                final = self._deployment(plan["selector"])
                need(_fields(final, self.release["container"]) == plan["original"]
                     and _guard(final, self.release["container"]) == plan["guard_sha256"], "reconcile_drift_after_readback")
                self._state(path, "RECONCILED_VERIFIED", failed=True, plan_sha256=sha(raw(plan)), runtime=readback, reconcile=record)
                return {"status": "RECONCILED_VERIFIED", "receipt": str(path), "sampling_cadence": "not_assessed",
                        "reconcile_branch": branch, "prior_status": prior_state["status"]}
            except Exception as exc:
                self._state(path, "BLOCKED", failed=True, error_type=type(exc).__name__, plan_sha256=sha(raw(plan)))
                raise MaintenanceError("sampling_reconcile_blocked_inspect_receipt") from None
        finally:
            self.images = saved
