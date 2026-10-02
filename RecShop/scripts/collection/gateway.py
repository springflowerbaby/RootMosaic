"""Apply gateway faults through owned configuration and deployment rollouts.

Each fault records resource intents and verifies fresh Pod, init-container,
process and parsed-configuration identity. Writable baselines require file
timestamps before the main process started; nginx configuration output alone
is not proof of what the running master loaded. New main-container mounts are
read-only, with a writable initialization copy.

Rollout transitions record write, completion and settling times together with
resource versions and Pod generations. Explicit execution is required for
mutations. Preview performs read-only preflight and server dry-run checks, and
writes no journal, plan, transition or command-digest evidence."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import PurePosixPath
import re
import uuid

from . import contract as c
from . import journal as j
from . import primitives as p

MECHANISM = "nginx_configmap_rollout"
MECHANISM_VERSION = "m1-gateway-configmap-v1"
PLAN_SCHEMA = "rq4-collect/gateway-plan-v1"
TRANSITION_SCHEMA = "rq4-collect/gateway-transition-v1"
ACTIVE_PATH = "/etc/nginx/conf.d/default.conf"
PLAN_ANNOTATION = "recshop.dev/m1-gateway-plan"


# GATEWAY-STARTUP-READONLY-20260917.json): the seed-conf init copies the
# ConfigMap baseline and then chmods 444, keeping the 0444 baseline semantics.
# Strict equality against the live initContainer command; any drift must fail
# preflight instead of silently passing. The value is pinned into every plan.
_SEED_COMMAND = ["sh", "-c", "cp /confs/baseline.conf /etc/nginx/conf.d/default.conf && chmod 444 /etc/nginx/conf.d/default.conf"]

# Settled determination basis: the checks _runtime must pass before a switch
# band counts as settled. This basis is the observation bucketing boundary the
# gate side consumes (review condition "settled bucketing"); a declared window
# never substitutes for it (Source-bound).
SETTLED_BASIS = (
    "rollout_status_exit_zero",
    "observed_generation_not_below_generation",
    "replicas_updated_ready_available_equal_spec_replicas",
    "terminating_pods_waited_for_delete",
    "pod_set_size_equals_replicas_and_none_deleting",
    "pod_owner_chain_pinned_deployment",
    "replicaset_template_matches_current_generation",
    "main_ready_and_seed_init_exit_zero",
    "seed_init_finished_not_after_main_start",
    "active_bytes_equal_expected_config",
    "outside_closure_hash_unchanged",
    "config_file_times_precede_main_start",
    "pod_and_container_identity_stable_during_readback",
    "deployment_control_fields_stable_during_readback",
)


def _now_utc():
    return datetime.now(timezone.utc)


def _parse_utc(value):
    _check(isinstance(value, str) and bool(value), "UTC timestamp string required")
    moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _check(moment.tzinfo is not None, "timestamp must carry a timezone")
    return moment


_DIRECTIVE_RE = re.compile(r"\b(proxy_read_timeout|proxy_next_upstream)\s+([^;\r\n]+);")


def _directive_inventory(text):
    return {name: value.strip() for name, value in _DIRECTIVE_RE.findall(text)}


def _knob_isolation(baseline_text, modified_text):
    """C1 section 3.4 evidence: exactly the one fault directive may differ.

    A timeout fault must not add retry directives, a retry fault must not move
    the timeout, and no slow-route or other compound edit may ride along (the
    old D01 compound state stays dead).
    """
    before, after = _directive_inventory(baseline_text), _directive_inventory(modified_text)
    return {"baseline": before, "modified": after,
            "changed_directives": sorted(key for key in set(before) | set(after) if before.get(key) != after.get(key))}


def effectiveness_boundary(transition, *, buffer_s):
    """C1 section 2.4 observation boundary for one recorded gateway switch band.

    Probes inside [switch_initiated, settled_confirmed] are in the switching
    band and are excluded. The fault-state boundary is the settled
    confirmation plus the calibration buffer (C1 initial value 5s; pilot
    recalibrates). For a restore-phase transition the same boundary marks when
    the baseline is effective again, i.e. when the fault has cleared.
    """
    _check(isinstance(transition, dict) and transition.get("schema_version") == TRANSITION_SCHEMA,
           "recorded gateway transition required")
    _check(type(buffer_s) in (int, float) and not isinstance(buffer_s, bool) and buffer_s >= 0,
           "non-negative numeric buffer required")
    initiated = _parse_utc(transition["switch_initiated_at_utc"])
    settled = _parse_utc(transition["settled_confirmed_at_utc"])
    _check(settled >= initiated, "settled confirmation precedes switch initiation")
    return {"schema_version": TRANSITION_SCHEMA, "fault_instance_id": transition["fault_instance_id"],
            "phase": transition["phase"],
            "semantics": "fault_effective_from" if transition["phase"] == "inject" else "fault_cleared_from",
            "probe_exclusion_window": {"from_utc": transition["switch_initiated_at_utc"],
                                       "to_utc": transition["settled_confirmed_at_utc"]},
            "rollout_status_completed_at_utc": transition["rollout_status_completed_at_utc"],
            "effective_from_utc": (settled + timedelta(seconds=buffer_s)).isoformat(),
            "buffer_s": buffer_s}


def classify_probe(timestamp, transition, *, buffer_s):
    """Bucket one probe timestamp against a recorded switch band.

    Buckets: pre_switch / switching_band (excluded) / settle_buffer (excluded,
    C1 buffer) / post_switch_steady. post_switch_steady means fault-effective
    for an inject transition and baseline-restored for a restore transition.
    Boundary instants belong to the later bucket (settled instant -> buffer,
    settled+buffer -> steady), the conservative reading for pairing.
    """
    boundary = effectiveness_boundary(transition, buffer_s=buffer_s)
    if isinstance(timestamp, datetime):
        _check(timestamp.tzinfo is not None, "probe timestamp must carry a timezone")
        moment = timestamp
    else:
        moment = _parse_utc(timestamp)
    if moment < _parse_utc(boundary["probe_exclusion_window"]["from_utc"]):
        return "pre_switch"
    if moment < _parse_utc(boundary["probe_exclusion_window"]["to_utc"]):
        return "switching_band"
    if moment < _parse_utc(boundary["effective_from_utc"]):
        return "settle_buffer"
    return "post_switch_steady"


class GatewayError(p.PrimitiveError):
    pass


def _check(condition, reason):
    if not condition:
        raise GatewayError(reason)


def _copy(value):
    return json.loads(c.canonical_json(value))


def _hash(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _opaque_hash(value):
    # Hash-only opaque template guard; never persist the full Deployment or env.
    return _hash(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))


def _entry(mapping, key):
    return {"present": key in mapping, "value": mapping.get(key)}


def _target_key(target):
    return target.kind, target.scope, target.name, target.uid


def _binding_identity(binding):
    return {"context": binding.context, "namespace": binding.namespace, "entity": binding.entity,
            "deployment": binding.deployment, "deployment_uid": binding.deployment_uid,
            "container": binding.container, "selector": dict(binding.selector)}


def _name(value):
    _check(type(value) is str and re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", value) is not None, "invalid gateway resource name")


@dataclass(frozen=True)
class GatewayRequest:
    spec: c.FaultInstanceSpec
    binding: p.PinnedTarget
    directive: str
    value: str


def prepare_gateway(spec: c.FaultInstanceSpec, binding: p.PinnedTarget) -> GatewayRequest:
    """Pure preparation: this distinct rollout mechanism never masquerades as P1."""
    data = spec.to_dict()
    _check(data["mechanism"] == MECHANISM and data["mechanism_version"] == MECHANISM_VERSION, "explicit gateway rollout mechanism/version required")
    _check(binding.entity == data["normalized_root_entity"] == "catalog-gw" and binding.container == "nginx", "gateway binding mismatch")
    _check(data["raw_target"] == {"kind": "Deployment", "name": binding.deployment,
                                  "scope": binding.namespace, "selector": dict(binding.selector)}, "raw gateway target mismatch")
    if data["fault_type"] == "timeout_misconfiguration":
        value = data["parameters"].get("read_timeout_ms")
        _check(set(data["parameters"]) == {"read_timeout_ms"} and type(value) is int and value > 0, "positive explicit timeout ms required")
        return GatewayRequest(spec, binding, "proxy_read_timeout", str(value) + "ms")
    _check(data["fault_type"] == "retry_policy_misconfiguration" and data["parameters"] == {"proxy_next_upstream": "off"}, "only isolated retry-off is supported")
    return GatewayRequest(spec, binding, "proxy_next_upstream", "off")


@dataclass(frozen=True, init=False, repr=False)
class GatewayPlan:
    _json: str

    def __init__(self):
        raise TypeError("Use GatewayAdapter.preflight or load_plan")

    def to_dict(self):
        return json.loads(self._json)

    @property
    def sha256(self):
        return _hash(self._json)


def _seal_plan(value):
    result = object.__new__(GatewayPlan)
    object.__setattr__(result, "_json", c.canonical_json(value))
    return result


def _dump_closure(dump: str, active: str):
    headers = list(re.finditer(r"^# configuration file ([^\r\n:]+):\r?$", dump, re.M))
    _check(headers, "nginx -T configuration closure missing")
    blocks, order = {}, []
    for index, header in enumerate(headers):
        name = header[1]
        path = PurePosixPath(name)
        _check(name.startswith("/etc/nginx/") and ".." not in path.parts and name not in blocks, "nginx closure path ambiguous or outside supported tree")
        body = dump[header.end():headers[index + 1].start() if index + 1 < len(headers) else len(dump)]
        blocks[name] = body.strip("\r\n")
        order.append(name)
    _check(ACTIVE_PATH in blocks and blocks[ACTIVE_PATH] == active.strip("\r\n"), "nginx -T active block differs from actual file")
    _check(all(not name.startswith("/etc/nginx/conf.d/") or name == ACTIVE_PATH for name in blocks), "additional conf.d routes are not supported")
    outside = {name: body for name, body in blocks.items() if name != ACTIVE_PATH}
    _check("/etc/nginx/nginx.conf" in outside, "main nginx config absent")
    text = re.sub(r"#[^\r\n]*", "", "\n".join(outside.values()))
    _check(re.search(r"\bproxy_(?:read_timeout|next_upstream)\b", text) is None, "inherited gateway fault directive outside controlled file")
    return _opaque_hash(outside), tuple(order)


class GatewayAdapter:
    def __init__(self, journal: j.AttemptJournal, client: p.CommandClient, requests: tuple[GatewayRequest, ...], *,
                 kubectl: str, command_timeout_s: float, wait_timeout_s: int):
        _check(client.evidence_kind in {"fake", "live"} and client.supports_delete_preconditions is True
               and client.supports_server_dry_run is True, "verified dry-run/conditional-delete client required; no implicit real client")
        _check(type(kubectl) is str and bool(kubectl) and type(wait_timeout_s) is int and wait_timeout_s > 0
               and type(command_timeout_s) in (int, float) and command_timeout_s > wait_timeout_s, "bounded command/wait configuration required")
        self.journal, self.client, self.kubectl = journal, client, kubectl
        self.command_timeout_s, self.wait_timeout_s = command_timeout_s, wait_timeout_s
        self.evidence_kind = client.evidence_kind
        self.requests, self.plans, self._targets = {}, {}, {}
        self.evidence = []
        
        # paths (non-durable preflight, dry-run switch) suppress them so a
        # preview leaves no file that could be mistaken for executed evidence.
        self._command_digests_enabled = True
        # Transition-band evidence state (in-memory; durable copies are the
        # gateway-transition-* journal artifacts written per phase).
        self._op_log = {}          # fid -> [{"operation", "issued_at_utc", "completed_at_utc"}]
        self._runtime_span = {}    # fid -> op-log slice of the latest _runtime readback
        self._patch_contexts = {}  # (fid, phase) -> switch patch timing + pre-patch generation/RV
        self.transitions = {}      # (fid, phase) -> recorded transition record
        context = journal.contract.context.to_dict()
        for request in requests:
            _check(request == prepare_gateway(request.spec, request.binding), "gateway request changed")
            _check(any(f.to_dict() == request.spec.to_dict() for f in journal.contract.faults), "gateway request not in contract")
            _check((request.binding.context, request.binding.namespace) == (context["kube_context"], context["namespace"]), "gateway context/namespace mismatch")
            fid = request.spec.fault_instance_id
            _check(fid not in self.requests, "duplicate gateway fault instance")
            self.requests[fid] = request
            name = "m1-gw-" + _hash(journal.metadata["ownership"] + ":" + fid)[:24]
            targets = (j.Target("ConfigMap", name, request.binding.namespace, None),
                       j.Target("Deployment", request.binding.deployment, request.binding.namespace, request.binding.deployment_uid))
            for target in targets:
                key = _target_key(target)
                _check(key not in self._targets, "same gateway resource assigned twice")
                self._targets[key] = fid

    @property
    def targets(self):
        return tuple(j.Target(kind=key[0], scope=key[1], name=key[2], uid=key[3]) for key in self._targets)

    def targets_for(self, fid):
        return tuple(target for target in self.targets if self._targets[_target_key(target)] == fid)

    def _fid(self, target):
        _check(_target_key(target) in self._targets, "unapproved gateway resource target")
        return self._targets[_target_key(target)]

    def _plan(self, fid):
        _check(fid in self.plans, "gateway plan must be durably prepared or loaded")
        return self.plans[fid].to_dict()

    def _run(self, fid, operation, args, *, stdin=None, target=None):
        request = self.requests[fid]
        if args[0] in {"create", "patch", "delete"} and "--dry-run=server" not in args:
            _check(target is not None and self._fid(target) == fid, "mutation target missing")
            self._intent_guard(target, self.desired_fields(target), restoring=True)
            _check(operation in {"create_owned_configmap", "switch_gateway_volume", "delete_owned_configmap"}, "unapproved mutation operation")
        argv = (self.kubectl, "--context", request.binding.context, "--namespace", request.binding.namespace, *args)
        issued = _now_utc()
        result = self.client.run(argv, stdin=stdin, timeout_s=self.command_timeout_s)
        completed = _now_utc()
        _check(isinstance(result, p.CommandResult) and type(result.returncode) is int, "invalid command result")
        event = {"fault_instance_id": fid, "operation": operation, "return_code": result.returncode,
                 "recorded_at_utc": completed.isoformat(), "evidence_kind": self.evidence_kind,
                 "stdout_sha256": _hash(result.stdout), "stderr_sha256": _hash(result.stderr)}
        if self._command_digests_enabled:
            artifact_id = "gateway-command-" + uuid.uuid4().hex
            # Same durable digest-file pattern as accepted P1: read/wait evidence
            # does not create two extra journal events per GET/exec. Resource intents
            # and the recovery plan still use J's anchored write-ahead mechanisms.
            payload = c.canonical_json(event).encode()
            filename = artifact_id + ".json"
            self.journal._write_new(filename, payload)
            reference = {"artifact_id": artifact_id, "relative_path": filename, "sha256": hashlib.sha256(payload).hexdigest(),
                         "bytes": len(payload), "media_type": "application/json", "status": "written",
                         "journal_artifact_registration": False, "contract_sha256": self.journal.contract.sha256}
            self.evidence.append({**event, "artifact_ref": reference})
        # In-memory transition timing only; the durable per-command digest
        # format above stays byte-compatible with the accepted L1-P2 evidence.
        self._op_log.setdefault(fid, []).append({"operation": operation,
                                                "issued_at_utc": issued.isoformat(),
                                                "completed_at_utc": completed.isoformat()})
        _check(result.returncode == 0, "gateway command failed; raw output is not logged")
        return result

    def _get(self, fid, kind, name, *, allow_deleting=False):
        resource = {"Deployment": "deployments.apps", "ConfigMap": "configmaps", "ReplicaSet": "replicasets.apps", "Pod": "pods"}[kind]
        result = self._run(fid, "get_" + kind, ("get", resource, name, "-o", "json", "--ignore-not-found"))
        if not result.stdout.strip():
            return None
        obj = json.loads(result.stdout)
        meta = obj.get("metadata", {})
        _check(obj.get("kind") == kind and meta.get("name") == name and meta.get("namespace") == self.requests[fid].binding.namespace
               and type(meta.get("uid")) is str and bool(meta["uid"]) and type(meta.get("resourceVersion")) is str and bool(meta["resourceVersion"]), "resource identity or version missing")
        _check(obj.get("apiVersion") == ("v1" if kind in {"ConfigMap", "Pod"} else "apps/v1"), "resource API version mismatch")
        _check(allow_deleting or not meta.get("deletionTimestamp"), "required gateway resource is deleting")
        return obj

    def _deployment(self, fid):
        request = self.requests[fid]
        obj = self._get(fid, "Deployment", request.binding.deployment)
        _check(obj is not None and obj["metadata"]["uid"] == request.binding.deployment_uid, "gateway Deployment UID changed")
        spec = obj["spec"]
        _check(spec["selector"] == {"matchLabels": dict(request.binding.selector)} and
               all(spec["template"]["metadata"]["labels"].get(k) == v for k, v in request.binding.selector), "gateway selector changed")
        podspec = spec["template"]["spec"]
        _check(len(podspec.get("containers", [])) == 1 and podspec["containers"][0]["name"] == "nginx"
               and len(podspec.get("initContainers", [])) == 1 and podspec["initContainers"][0]["name"] == "seed-conf"
               and podspec["initContainers"][0].get("command") == _SEED_COMMAND
               and podspec["initContainers"][0].get("restartPolicy") is None, "unsupported gateway container/init layout")
        security = podspec["containers"][0].get("securityContext", {})
        _check(security.get("privileged", False) is False and "SYS_ADMIN" not in security.get("capabilities", {}).get("add", []), "gateway config protection unsupported for privileged main container")
        volumes = {v["name"]: v for v in podspec.get("volumes", [])}
        _check(len(volumes) == len(podspec.get("volumes", [])) and set(volumes) == {"confs", "live"}
               and set(volumes["confs"]) == {"name", "configMap"} and volumes["live"] == {"name": "live", "emptyDir": {}}, "gateway volume layout changed")
        source = volumes["confs"]["configMap"]
        _check(set(source) <= {"name", "defaultMode", "optional"} and source.get("optional", False) is False, "unsupported ConfigMap projection mapping")
        _name(source.get("name"))
        for container in (*podspec["containers"], *podspec["initContainers"]):
            mounts = {mount["name"]: mount for mount in container.get("volumeMounts", [])}
            _check(len(mounts) == len(container.get("volumeMounts", [])) and set(mounts) == {"confs", "live"}
                   and mounts["confs"]["mountPath"] == "/confs" and mounts["live"]["mountPath"] == "/etc/nginx/conf.d"
                   and not any("subPath" in mount or "subPathExpr" in mount for mount in mounts.values()), "gateway mount mapping changed")
            if container["name"] == "seed-conf":
                _check(mounts["live"].get("readOnly", False) is False, "seed init must retain write access")
            _check("readOnly" not in mounts["live"] or type(mounts["live"]["readOnly"]) is bool, "live mount readOnly type invalid")
        return obj

    @staticmethod
    def _fields(obj):
        template = obj["spec"]["template"]
        volume = next(v for v in template["spec"]["volumes"] if v["name"] == "confs")
        mount = next(v for v in template["spec"]["containers"][0]["volumeMounts"] if v["name"] == "live")
        annotations = template["metadata"].get("annotations", {})
        return {"volume_source": _copy(volume["configMap"]), "main_live_read_only": _entry(mount, "readOnly"),
                "template_owner": _entry(annotations, p.OWNER_ANNOTATION)}

    @staticmethod
    def _template_guard(obj):
        template = json.loads(json.dumps(obj["spec"]["template"]))
        next(v for v in template["spec"]["volumes"] if v["name"] == "confs")["configMap"] = "CONTROLLED"
        next(v for v in template["spec"]["containers"][0]["volumeMounts"] if v["name"] == "live").pop("readOnly", None)
        annotations = template["metadata"].get("annotations", {})
        annotations.pop(p.OWNER_ANNOTATION, None)
        if not annotations:
            template["metadata"].pop("annotations", None)
        return _opaque_hash({"replicas": obj["spec"]["replicas"], "selector": obj["spec"]["selector"], "template": template})

    def _original_configmap(self, fid, plan):
        original = plan["original_configmap"]
        obj = self._get(fid, "ConfigMap", original["name"])
        _check(obj is not None and obj["metadata"]["uid"] == original["uid"] and _opaque_hash(obj.get("data", {})) == original["data_sha256"]
               and _opaque_hash(obj.get("binaryData", {})) == original["binary_sha256"], "original ConfigMap drifted; cannot assume restart restores baseline")
        _check(obj["metadata"].get("annotations", {}).get(p.OWNER_ANNOTATION) is None, "original ConfigMap claimed by another task")
        _check(obj.get("data", {}).get("baseline.conf") == plan["baseline_config"], "original baseline bytes changed")

    def _runtime(self, fid, obj, expected_config, *, expected_outside=None, require_readonly=False):
        op_span_start = len(self._op_log.get(fid, ()))
        request = self.requests[fid]
        expected_fields, guard = self._fields(obj), self._template_guard(obj)
        expected_owner = obj["metadata"].get("annotations", {}).get(p.OWNER_ANNOTATION)
        self._run(fid, "rollout_status", ("rollout", "status", "deployment/" + request.binding.deployment, "--timeout=" + str(self.wait_timeout_s) + "s"))
        obj = self._deployment(fid)
        _check(self._fields(obj) == expected_fields and self._template_guard(obj) == guard
               and obj["metadata"].get("annotations", {}).get(p.OWNER_ANNOTATION) == expected_owner,
               "gateway control state changed while waiting for rollout")
        status, replicas = obj.get("status", {}), obj["spec"].get("replicas")
        _check(type(replicas) is int and replicas > 0 and status.get("observedGeneration", 0) >= obj["metadata"].get("generation", 1)
               and all(status.get(key) == replicas for key in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas")), "gateway rollout generation/readiness unconfirmed")
        if require_readonly:
            _check(self._fields(obj)["main_live_read_only"] == {"present": True, "value": True}, "main gateway config mount is not read-only")
        selector = ",".join(k + "=" + v for k, v in request.binding.selector)
        result = self._run(fid, "list_gateway_pods", ("get", "pods", "--selector", selector, "-o", "json"))
        items = json.loads(result.stdout).get("items", [])
        terminating = [pod for pod in items if pod.get("metadata", {}).get("deletionTimestamp")]
        for pod in terminating:
            meta = pod["metadata"]
            _name(meta["name"])
            owners = [owner for owner in meta.get("ownerReferences", []) if owner.get("controller") is True and owner.get("kind") == "ReplicaSet"]
            _check(meta.get("namespace") == request.binding.namespace and len(owners) == 1, "unrelated terminating Pod in gateway selector")
            rs = self._get(fid, "ReplicaSet", owners[0]["name"], allow_deleting=True)
            _check(rs is not None and rs["metadata"]["uid"] == owners[0]["uid"] and any(
                owner.get("kind") == "Deployment" and owner.get("controller") is True and owner.get("uid") == request.binding.deployment_uid
                for owner in rs["metadata"].get("ownerReferences", [])), "terminating Pod does not belong to gateway")
            self._run(fid, "wait_old_gateway_pod_deleted", ("wait", "pods/" + meta["name"], "--for=delete", "--timeout=" + str(self.wait_timeout_s) + "s"))
        if terminating:
            result = self._run(fid, "list_settled_gateway_pods", ("get", "pods", "--selector", selector, "-o", "json"))
            items = json.loads(result.stdout).get("items", [])
        _check(len(items) == replicas and all(not pod.get("metadata", {}).get("deletionTimestamp") for pod in items), "gateway Pod set not settled")
        evidence, outside_hashes = [], set()
        for pod in items:
            meta = pod["metadata"]
            _name(meta["name"])
            _check(meta.get("namespace") == request.binding.namespace and type(meta.get("uid")) is str and bool(meta["uid"])
                   and all(meta.get("labels", {}).get(k) == v for k, v in request.binding.selector), "gateway Pod identity mismatch")
            owners = [owner for owner in meta.get("ownerReferences", []) if owner.get("controller") is True and owner.get("kind") == "ReplicaSet"]
            _check(len(owners) == 1, "gateway Pod ReplicaSet owner ambiguous")
            owner = owners[0]
            rs = self._get(fid, "ReplicaSet", owner["name"])
            _check(rs is not None and rs["metadata"]["uid"] == owner["uid"] and any(
                parent.get("controller") is True and parent.get("kind") == "Deployment" and parent.get("uid") == request.binding.deployment_uid
                for parent in rs["metadata"].get("ownerReferences", [])), "Pod does not belong to pinned Deployment")
            _check(rs["spec"]["template"] == obj["spec"]["template"] or
                   self._pod_template_matches(rs["spec"]["template"], obj["spec"]["template"]), "ReplicaSet template differs from current gateway generation")
            containers = pod.get("status", {}).get("containerStatuses", [])
            init = pod.get("status", {}).get("initContainerStatuses", [])
            main = [item for item in containers if item.get("name") == "nginx"]
            seeded = [item for item in init if item.get("name") == "seed-conf"]
            _check(len(main) == len(seeded) == 1 and main[0].get("ready") is True
                   and seeded[0].get("state", {}).get("terminated", {}).get("exitCode") == 0, "gateway main/init startup not verified")
            _check(type(main[0].get("containerID")) is str and bool(main[0]["containerID"])
                   and type(main[0].get("restartCount")) is int and main[0]["restartCount"] >= 0, "main container identity/restart counter missing")
            pod_main = [item for item in pod["spec"]["containers"] if item.get("name") == "nginx"]
            pod_init = [item for item in pod["spec"].get("initContainers", []) if item.get("name") == "seed-conf"]
            _check(len(pod_main) == len(pod_init) == 1 and pod_main[0].get("image") == obj["spec"]["template"]["spec"]["containers"][0]["image"]
                   and pod_init[0].get("image") == obj["spec"]["template"]["spec"]["initContainers"][0]["image"]
                   and type(main[0].get("imageID")) is str and bool(main[0]["imageID"])
                   and type(seeded[0].get("imageID")) is str and bool(seeded[0]["imageID"]), "gateway Pod image identity missing or changed")
            if fid in self.plans:
                prior = self._plan(fid)["baseline_runtime"]["pods"]
                _check({item["main_image_id"] for item in prior} == {main[0]["imageID"]}
                       and {item["init_image_id"] for item in prior} == {seeded[0]["imageID"]}, "gateway image digest changed across rollout")
            started = datetime.fromisoformat(main[0]["state"]["running"]["startedAt"].replace("Z", "+00:00"))
            _check(started.tzinfo is not None, "container start lacks timezone")
            finished = datetime.fromisoformat(seeded[0]["state"]["terminated"]["finishedAt"].replace("Z", "+00:00"))
            _check(finished.tzinfo is not None and finished <= started, "init did not complete before main start")
            pod_mount = next(m for item in pod["spec"]["containers"] if item["name"] == "nginx" for m in item["volumeMounts"] if m["name"] == "live")
            _check(not require_readonly or pod_mount.get("readOnly") is True, "new Pod did not inherit read-only config")
            prefix = ("exec", meta["name"], "--container", "nginx", "--")
            command_line = self._run(fid, "nginx_master_argv", (*prefix, "cat", "/proc/1/cmdline")).stdout.replace("\x00", " ").strip()
            _check(command_line in {"nginx -g daemon off;", "nginx: master process nginx -g daemon off;"}, "PID1 is not the expected nginx master invocation")
            active = self._run(fid, "read_active_config", (*prefix, "cat", ACTIVE_PATH)).stdout
            _check(active == expected_config, "active nginx file differs from approved exact bytes")
            dump = self._run(fid, "nginx_T", (*prefix, "nginx", "-T")).stdout
            outside, files = _dump_closure(dump, active)
            outside_hashes.add(outside)
            _check(expected_outside is None or outside == expected_outside, "uncontrolled nginx configuration closure changed")
            times = {}
            for filename in files:
                output = self._run(fid, "config_file_times", (*prefix, "stat", "-c", "%Y %Z", filename)).stdout.split()
                _check(len(output) == 2 and all(re.fullmatch(r"[0-9]+", value) for value in output), "configuration file times unavailable")
                times[filename] = {"mtime_s": int(output[0]), "ctime_s": int(output[1])}
                # Whole-second file times must precede the lower-bound container
                # start; equality is ambiguous and is refused, never guessed.
                if require_readonly and filename == ACTIVE_PATH:
                    _check(max(int(value) for value in output) <= started.timestamp(), "read-only startup file has contradictory late modification evidence")
                else:
                    _check(max(int(value) for value in output) + 1 <= started.timestamp(), "whole-second file time cannot prove unchanged pre-start configuration")
            fresh_pod = self._get(fid, "Pod", meta["name"])
            fresh_main = [] if fresh_pod is None else [item for item in fresh_pod.get("status", {}).get("containerStatuses", []) if item.get("name") == "nginx"]
            _check(fresh_pod is not None and fresh_pod["metadata"]["uid"] == meta["uid"]
                   and not fresh_pod["metadata"].get("deletionTimestamp") and fresh_pod["metadata"].get("ownerReferences") == meta.get("ownerReferences")
                   and len(fresh_main) == 1 and fresh_main[0].get("ready") is True
                   and all(fresh_main[0].get(key) == main[0].get(key) for key in ("containerID", "restartCount", "imageID"))
                   and fresh_main[0].get("state", {}).get("running", {}).get("startedAt") == main[0]["state"]["running"]["startedAt"],
                   "Pod/container changed during gateway runtime readback")
            evidence.append({"pod_name": meta["name"], "pod_uid": meta["uid"], "replicaset_uid": owner["uid"],
                             "main_started_at": main[0]["state"]["running"]["startedAt"], "seed_exit_code": 0,
                             "container_id": main[0]["containerID"], "restart_count": main[0]["restartCount"],
                             "main_image_id": main[0].get("imageID"), "init_image_id": seeded[0].get("imageID"),
                             "active_sha256": _hash(active), "nginx_outside_sha256": outside, "file_times": times,
                             "active_readonly": pod_mount.get("readOnly", False), "loaded_config_basis": "new_readonly_startup" if require_readonly else "unchanged_files_before_main_start"})
        _check(len(outside_hashes) == 1, "gateway Pods have different nginx closures")
        fresh = self._deployment(fid)
        _check(self._fields(fresh) == expected_fields and self._template_guard(fresh) == guard
               and fresh["metadata"].get("annotations", {}).get(p.OWNER_ANNOTATION) == expected_owner, "gateway changed during runtime readback")
        self._runtime_span[fid] = (op_span_start, len(self._op_log.get(fid, ())))
        return fresh, {"pods": evidence, "outside_sha256": next(iter(outside_hashes))}

    @staticmethod
    def _pod_template_matches(actual, expected):
        normalized = json.loads(json.dumps(actual))
        normalized.get("metadata", {}).get("labels", {}).pop("pod-template-hash", None)
        return normalized == expected

    def preflight(self, request: GatewayRequest, *, durable: bool = True) -> GatewayPlan:
        """Read-only validation chain; durable=False keeps the plan in-memory only.

        The durable path registers the plan (write-ahead artifact) as before.
        The preview path (executor apply execute=False) runs the identical
        checks and server dry-runs but writes nothing at all: no journal rows,
        no plan artifact and, per collection/F1, no per-command digest files either.
        """
        _check(type(durable) is bool, "durable must be an explicit bool")
        self._command_digests_enabled = durable
        try:
            return self._preflight(request, register=durable)
        finally:
            self._command_digests_enabled = True

    def _preflight(self, request: GatewayRequest, *, register: bool) -> GatewayPlan:
        fid = request.spec.fault_instance_id
        _check(self.requests.get(fid) == request and fid not in self.plans, "unknown or already prepared gateway request")
        cm_target, _ = self.targets_for(fid)
        obj = self._deployment(fid)
        _check(obj["metadata"].get("annotations", {}).get(p.OWNER_ANNOTATION) is None, "gateway already has an owner")
        fields = self._fields(obj)
        _check(fields["template_owner"]["present"] is False, "gateway template already owned")
        cm = self._get(fid, "ConfigMap", fields["volume_source"]["name"])
        _check(cm is not None and type(cm.get("data", {}).get("baseline.conf")) is str, "original baseline ConfigMap missing")
        _check(cm["metadata"].get("annotations", {}).get(p.OWNER_ANNOTATION) is None, "original ConfigMap has a task owner")
        _check(self._get(fid, "ConfigMap", cm_target.name) is None, "attempt ConfigMap name collision; do not overwrite")
        baseline = cm["data"]["baseline.conf"]
        _check(re.search(r"\b(?:ssl_certificate_key|auth_basic_user_file)\b|proxy_set_header\s+(?:Authorization|Cookie)\b", baseline, re.I) is None, "credential-bearing gateway baseline unsupported")
        changed = p.rewrite_gateway_config(baseline, directive=request.directive, value=request.value, expected_sha256=_hash(baseline))
        
        # mutation; verify keeps the same hard check after rollout.
        isolation = _knob_isolation(baseline, changed["text"])
        _check(isolation["changed_directives"] == [request.directive], "gateway edit is not an isolated single-knob change")
        if request.directive == "proxy_read_timeout":
            old_value = baseline[slice(*changed["source_changed_span"])].strip("\"'")
            match = re.fullmatch(r"([0-9]+)(ms|s|m|h)", old_value)
            _check(match is not None, "unsupported original timeout unit")
            original_ms = int(match[1]) * {"ms": 1, "s": 1000, "m": 60000, "h": 3600000}[match[2]]
            _check(original_ms != int(request.value[:-2]), "semantically unchanged timeout is not an injection")
        current, runtime = self._runtime(fid, obj, baseline, require_readonly=fields["main_live_read_only"] == {"present": True, "value": True})
        if request.binding.pods:
            _check({(pin.name, pin.uid) for pin in request.binding.pods} == {(pod["pod_name"], pod["pod_uid"]) for pod in runtime["pods"]}, "original pinned gateway Pod changed")
        _check(self._fields(current) == fields and self._template_guard(current) == self._template_guard(obj), "gateway changed during baseline inspection")
        cm_after = self._get(fid, "ConfigMap", cm["metadata"]["name"])
        _check(cm_after is not None and cm_after["metadata"]["uid"] == cm["metadata"]["uid"] and cm_after.get("data") == cm.get("data"), "baseline ConfigMap changed during preflight")
        plan = _seal_plan({"schema_version": PLAN_SCHEMA, "contract_sha256": self.journal.contract.sha256,
                           "ownership": self.journal.metadata["ownership"], "fault_instance_id": fid, "mechanism_version": MECHANISM_VERSION,
                           "seed_command": list(_SEED_COMMAND),
                           "request_spec": request.spec.to_dict(), "binding": _binding_identity(request.binding), "original_fields": fields,
                           "original_configmap": {"name": cm["metadata"]["name"], "uid": cm["metadata"]["uid"],
                                                  "data_sha256": _opaque_hash(cm.get("data", {})), "binary_sha256": _opaque_hash(cm.get("binaryData", {}))},
                           "owned_configmap": cm_target.name, "baseline_config": baseline, "modified_config": changed["text"],
                           "knob_isolation": isolation,
                           "template_guard_sha256": self._template_guard(current), "outside_config_sha256": runtime["outside_sha256"],
                           "baseline_runtime": runtime, "directive": request.directive, "value": request.value})
        if register:
            self.journal.write_artifact(artifact_id="gateway-plan-" + fid, relative_path="artifacts/gateway/plan-" + fid + ".json",
                                        raw=plan._json.encode(), media_type="application/json")
            self.plans[fid] = plan
        # ConfigMap server dry-run is read-only; unknown defaulting is refused.
        manifest = self._manifest(fid, plan)
        result = self._run(fid, "dry_run_owned_configmap", ("create", "--dry-run=server", "-f", "-", "-o", "json"), stdin=c.canonical_json(manifest))
        projected = json.loads(result.stdout)
        _check(projected.get("apiVersion") == "v1" and projected.get("kind") == "ConfigMap"
               and projected.get("metadata", {}).get("name") == cm_target.name and projected["metadata"].get("namespace") == cm_target.scope
               and not projected["metadata"].get("ownerReferences") and not projected["metadata"].get("deletionTimestamp")
               and projected.get("data") == manifest["data"] and projected.get("immutable") is True
               and projected.get("binaryData", {}) == {} and projected.get("metadata", {}).get("annotations") == manifest["metadata"]["annotations"], "ConfigMap admission changes approved content")
        return plan

    def load_plan(self, fid):
        _check(fid in self.requests and fid not in self.plans, "gateway plan already loaded or unknown")
        rows = [row for row in self.journal.records() if row["event"] == "artifact_complete" and row["payload"]["artifact_id"] == "gateway-plan-" + fid]
        _check(len(rows) == 1, "durable gateway plan missing")
        reference = rows[0]["payload"]
        raw = self.journal._file(reference["relative_path"]).read_bytes()
        _check(hashlib.sha256(raw).hexdigest() == reference["sha256"], "gateway plan hash mismatch")
        data = json.loads(raw)
        _check(data.get("schema_version") == PLAN_SCHEMA and data.get("contract_sha256") == self.journal.contract.sha256
               and data.get("ownership") == self.journal.metadata["ownership"] and data.get("request_spec") == self.requests[fid].spec.to_dict()
               and data.get("binding") == _binding_identity(self.requests[fid].binding), "gateway plan identity drift")
        _check(data.get("seed_command") == list(_SEED_COMMAND), "gateway plan seed command drifted from the pinned RO baseline")
        self.plans[fid] = _seal_plan(data)
        return self.plans[fid]

    def desired_fields(self, target):
        return self.desired_fields_for(target, self._plan(self._fid(target)))

    def desired_fields_for(self, target, plan):
        """Desired controlled fields from an explicit plan (registry-free)."""
        fid = self._fid(target)
        data = plan.to_dict() if isinstance(plan, GatewayPlan) else plan
        _check(isinstance(data, dict) and data.get("fault_instance_id") == fid, "plan does not belong to this gateway target")
        if target.kind == "ConfigMap":
            return {"data": {"baseline.conf": data["modified_config"]}, "binaryData": {}, "immutable": True}
        original = data["original_fields"]
        return {"volume_source": {**original["volume_source"], "name": data["owned_configmap"]},
                "main_live_read_only": {"present": True, "value": True},
                "template_owner": {"present": True, "value": self.journal.metadata["ownership"]}}

    def _manifest(self, fid, plan):
        target = self.targets_for(fid)[0]
        data = plan.to_dict() if isinstance(plan, GatewayPlan) else plan
        return {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": target.name, "namespace": target.scope,
                "annotations": {p.OWNER_ANNOTATION: self.journal.metadata["ownership"], PLAN_ANNOTATION: _hash(c.canonical_json(data))}},
                **self.desired_fields_for(target, data)}

    def observe(self, target, field_names):
        fid, plan = self._fid(target), self._plan(self._fid(target))
        _check(set(field_names) == set(self.desired_fields(target)), "unexpected gateway controlled fields")
        self._original_configmap(fid, plan)
        if target.kind == "ConfigMap":
            obj = self._get(fid, "ConfigMap", target.name, allow_deleting=True)
            if obj is not None and obj["metadata"].get("deletionTimestamp"):
                _check(obj["metadata"].get("annotations", {}).get(p.OWNER_ANNOTATION) == self.journal.metadata["ownership"]
                       and obj["metadata"].get("annotations", {}).get(PLAN_ANNOTATION) == self.plans[fid].sha256,
                       "deleting ConfigMap identity is not owned by this plan")
                deployment = self._deployment(fid)
                _check(self._fields(deployment) == plan["original_fields"], "deleting ConfigMap cannot support an active volume switch")
                self._run(fid, "wait_owned_configmap_deleted", ("wait", "configmap/" + target.name, "--for=delete", "--timeout=" + str(self.wait_timeout_s) + "s"))
                obj = self._get(fid, "ConfigMap", target.name, allow_deleting=True)
                _check(obj is None, "owned ConfigMap deletion remains unconfirmed")
            if obj is None:
                deployment = self._deployment(fid)
                _check(self._fields(deployment) == plan["original_fields"], "owned ConfigMap absent while Deployment still points away from baseline")
                fresh, _ = self._runtime(fid, deployment, plan["baseline_config"], expected_outside=plan["outside_config_sha256"],
                                         require_readonly=plan["original_fields"]["main_live_read_only"] == {"present": True, "value": True})
                _check(self._fields(fresh) == plan["original_fields"] and self._template_guard(fresh) == plan["template_guard_sha256"]
                       and fresh["metadata"].get("annotations", {}).get(p.OWNER_ANNOTATION) is None, "gateway baseline changed while verifying absent ConfigMap")
                return j.ResourceState(False, None, None, {})
            _check(obj["metadata"].get("annotations", {}).get(PLAN_ANNOTATION) == self.plans[fid].sha256, "owned ConfigMap plan identity missing")
            fields = {"data": obj.get("data", {}), "binaryData": obj.get("binaryData", {}), "immutable": obj.get("immutable", False)}
        else:
            obj = self._deployment(fid)
            _check(self._template_guard(obj) == plan["template_guard_sha256"], "uncontrolled gateway template fields changed")
            fields = self._fields(obj)
            if fields == plan["original_fields"]:
                obj, _ = self._runtime(fid, obj, plan["baseline_config"], expected_outside=plan["outside_config_sha256"],
                                       require_readonly=fields["main_live_read_only"] == {"present": True, "value": True})
                fields = self._fields(obj)
                _check(fields == plan["original_fields"] and self._template_guard(obj) == plan["template_guard_sha256"], "gateway original readback changed")
            # When owned changed fields exist, control-state observation remains
            # possible even if rollout failed. Executor verifies runtime before
            # success; recovery may safely CAS the known fields back meanwhile.
        return j.ResourceState(True, obj["metadata"]["uid"], obj["metadata"].get("annotations", {}).get(p.OWNER_ANNOTATION), _copy(fields))

    def _intent_guard(self, target, desired, *, restoring):
        states = self.journal._replay(self.journal.records())
        for state in reversed(tuple(states.values())):
            intent = state["intent"]
            if intent["target"] == target.to_dict() and state["status"] != "restored":
                _check(intent["ownership"] == self.journal.metadata["ownership"] and intent["controlled_fields"] == desired,
                       "gateway mutation does not match durable intent")
                _check(restoring or state["status"] == "pending", "apply intent is not pending")
                return
        raise GatewayError("gateway mutation lacks durable intent")

    def _patch(self, target, expected, desired, ownership, *, dry_run=False, phase=None, plan=None):
        fid = self._fid(target)
        obj = self._deployment(fid)
        current = j.ResourceState(True, obj["metadata"]["uid"], obj["metadata"].get("annotations", {}).get(p.OWNER_ANNOTATION), self._fields(obj))
        guard_plan = plan.to_dict() if isinstance(plan, GatewayPlan) else (plan if plan is not None else self._plan(fid))
        _check(current.to_dict() == expected.to_dict() and self._template_guard(obj) == guard_plan["template_guard_sha256"], "gateway CAS state changed")
        template = obj["spec"]["template"]
        vi = next(i for i, volume in enumerate(template["spec"]["volumes"]) if volume["name"] == "confs")
        mi = next(i for i, mount in enumerate(template["spec"]["containers"][0]["volumeMounts"]) if mount["name"] == "live")
        patch = [{"op": "test", "path": "/metadata/uid", "value": target.uid},
                 {"op": "test", "path": "/metadata/resourceVersion", "value": obj["metadata"]["resourceVersion"]},
                 {"op": "replace", "path": f"/spec/template/spec/volumes/{vi}/configMap", "value": desired["volume_source"]}]
        ro = desired["main_live_read_only"]
        mount_path = f"/spec/template/spec/containers/0/volumeMounts/{mi}/readOnly"
        if ro["present"]:
            patch.append({"op": "add", "path": mount_path, "value": ro["value"]})
        elif "readOnly" in template["spec"]["containers"][0]["volumeMounts"][mi]:
            patch.append({"op": "remove", "path": mount_path})
        for base, metadata, entry in (("/metadata", obj["metadata"], {"present": ownership is not None, "value": ownership}),
                                       ("/spec/template/metadata", template["metadata"], desired["template_owner"])):
            annotations = metadata.get("annotations")
            escaped = p.OWNER_ANNOTATION.replace("/", "~1")
            if entry["present"]:
                patch.append({"op": "add", "path": base + "/annotations" + ("/" + escaped if annotations is not None else ""),
                              "value": entry["value"] if annotations is not None else {p.OWNER_ANNOTATION: entry["value"]}})
            elif annotations is not None and p.OWNER_ANNOTATION in annotations:
                patch.append({"op": "remove", "path": base + "/annotations/" + escaped})
        args = ("patch", "deployments.apps", target.name, "--type=json", "-p", c.canonical_json(patch), "-o", "json")
        if dry_run:
            result = self._run(fid, "switch_gateway_volume", ("patch", "--dry-run=server", *args[1:]))
            projected = json.loads(result.stdout)
            projected_template = projected["spec"]["template"]
            volume = next(v for v in projected_template["spec"]["volumes"] if v["name"] == "confs")
            mount = next(m for m in projected_template["spec"]["containers"][0]["volumeMounts"] if m["name"] == "live")
            template_owner = desired["template_owner"]
            template_annotations = projected_template["metadata"].get("annotations", {})
            _check(projected.get("metadata", {}).get("uid") == target.uid
                   and projected.get("metadata", {}).get("annotations", {}).get(p.OWNER_ANNOTATION) == ownership
                   and volume.get("configMap") == desired["volume_source"]
                   and mount.get("readOnly") == desired["main_live_read_only"]["value"]
                   and (p.OWNER_ANNOTATION in template_annotations) == template_owner["present"]
                   and (not template_owner["present"] or template_annotations[p.OWNER_ANNOTATION] == template_owner["value"]),
                   "dry-run volume switch projection mismatch")
            return projected
        result = self._run(fid, "switch_gateway_volume", args, target=target)
        _check(phase in {"inject", "restore"}, "gateway switch phase required for transition evidence")
        entry = self._op_log[fid][-1]
        _check(entry["operation"] == "switch_gateway_volume", "gateway switch oplog mismatch")
        self._patch_contexts[(fid, phase)] = {"issued_at_utc": entry["issued_at_utc"], "completed_at_utc": entry["completed_at_utc"],
                                             "generation_before": obj["metadata"].get("generation"),
                                             "resource_version_before": obj["metadata"]["resourceVersion"]}
        return json.loads(result.stdout)

    def compare_and_apply(self, target, expected, desired_fields, ownership):
        fid = self._fid(target)
        _check(ownership == self.journal.metadata["ownership"] and desired_fields == self.desired_fields(target), "gateway apply parameters differ")
        self._intent_guard(target, desired_fields, restoring=False)
        if target.kind == "ConfigMap":
            _check(not expected.exists and self._get(fid, "ConfigMap", target.name) is None, "owned ConfigMap create collision")
            self._run(fid, "create_owned_configmap", ("create", "-f", "-", "-o", "json"), stdin=c.canonical_json(self._manifest(fid, self.plans[fid])), target=target)
        else:
            owned = self.targets_for(fid)[0]
            state = self.observe(owned, tuple(self.desired_fields(owned)))
            _check(state.exists and state.ownership == ownership and state.fields == self.desired_fields(owned), "owned ConfigMap not verified before volume switch")
            self._patch(target, expected, desired_fields, ownership, phase="inject")

    def compare_and_restore(self, target, expected, original):
        fid, plan = self._fid(target), self._plan(self._fid(target))
        self._intent_guard(target, self.desired_fields(target), restoring=True)
        self._original_configmap(fid, plan)
        if target.kind == "Deployment":
            _check(original.exists and original.uid == expected.uid and original.fields == plan["original_fields"], "original gateway state mismatch")
            self._patch(target, expected, original.fields, original.ownership, phase="restore")
            fresh, runtime = self._runtime(fid, self._deployment(fid), plan["baseline_config"], expected_outside=plan["outside_config_sha256"],
                                          require_readonly=original.fields["main_live_read_only"] == {"present": True, "value": True})
            self._record_transition(fid, "restore", fresh, runtime, plan)
        else:
            _check(not original.exists, "refuse to delete a pre-existing ConfigMap")
            deployment = self._deployment(fid)
            _check(self._fields(deployment) == plan["original_fields"], "restore Deployment before deleting its owned ConfigMap")
            self._runtime(fid, deployment, plan["baseline_config"], expected_outside=plan["outside_config_sha256"],
                          require_readonly=plan["original_fields"]["main_live_read_only"] == {"present": True, "value": True})
            current = self._get(fid, "ConfigMap", target.name)
            _check(current is not None and current["metadata"]["uid"] == expected.uid
                   and self.observe(target, tuple(expected.fields)).to_dict() == expected.to_dict(), "owned ConfigMap changed before conditional delete")
            options = {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": expected.uid, "resourceVersion": current["metadata"]["resourceVersion"]}}
            self._run(fid, "delete_owned_configmap", ("delete", "--raw", "/api/v1/namespaces/" + target.scope + "/configmaps/" + target.name, "-f", "-"), stdin=c.canonical_json(options), target=target)
            self._run(fid, "wait_owned_configmap_deleted", ("wait", "configmap/" + target.name, "--for=delete", "--timeout=" + str(self.wait_timeout_s) + "s"))

    def _record_transition(self, fid, phase, deployment_after, runtime, plan):
        """Compose and durably record one switch band (C1 section 2.4 input).

        Timestamps: switch patch issued/completed, rollout-status completion,
        settled confirmation (the last readback command of the verifying
        _runtime). Identity: deployment generation/resourceVersion across the
        switch and the Pod generation change. The settled basis is the fixed
        SETTLED_BASIS tuple; _runtime returning without refusal is what proves
        every listed check passed.
        """
        context = self._patch_contexts.get((fid, phase))
        span = self._runtime_span.get(fid)
        _check(context is not None and span is not None and span[0] < span[1], "gateway transition evidence incomplete")
        ops = self._op_log.get(fid, ())[span[0]:span[1]]
        rollout = [op for op in ops if op["operation"] == "rollout_status"]
        _check(bool(rollout), "rollout completion missing from gateway transition evidence")
        initiated, patch_done = _parse_utc(context["issued_at_utc"]), _parse_utc(context["completed_at_utc"])
        rollout_done, settled_done = _parse_utc(rollout[0]["completed_at_utc"]), _parse_utc(ops[-1]["completed_at_utc"])
        _check(patch_done >= initiated and rollout_done >= patch_done and settled_done >= rollout_done,
               "gateway transition timestamps unordered")
        if phase == "inject":
            previous = [pod["pod_uid"] for pod in plan["baseline_runtime"]["pods"]]
        else:
            
            # recover it from the durable artifact, so the restore band keeps
            # its previous-Pod generation instead of degrading to null. A
            # corrupted durable record still refuses here.
            earlier = self.transitions.get((fid, "inject")) or self.load_transition(fid, "inject", missing_ok=True)
            previous = None if earlier is None else earlier["pod_generation_change"]["serving_pod_uids"]
        serving = [pod["pod_uid"] for pod in runtime["pods"]]
        generation_after = deployment_after["metadata"].get("generation")
        record = {"schema_version": TRANSITION_SCHEMA, "fault_instance_id": fid, "phase": phase,
                  "mechanism_version": MECHANISM_VERSION, "evidence_kind": self.evidence_kind,
                  "switch_initiated_at_utc": context["issued_at_utc"],
                  "switch_patch_completed_at_utc": context["completed_at_utc"],
                  "rollout_status_completed_at_utc": rollout[0]["completed_at_utc"],
                  "settled_confirmed_at_utc": ops[-1]["completed_at_utc"],
                  "deployment_generation": {"before": context["generation_before"], "after": generation_after},
                  "deployment_resource_version": {"before": context["resource_version_before"],
                                                  "after": deployment_after["metadata"]["resourceVersion"]},
                  "pod_generation_change": {"previous_pod_uids": previous, "serving_pod_uids": serving,
                                            "previous_pod_uids_absent": None if previous is None else not set(previous) & set(serving)},
                  "settled": {"basis": list(SETTLED_BASIS), "pod_count": len(serving),
                              "replicas": deployment_after["spec"].get("replicas"),
                              "observed_generation": deployment_after.get("status", {}).get("observedGeneration"),
                              "generation": generation_after},
                  "durations_s": {"switch_patch_s": (patch_done - initiated).total_seconds(),
                                  "rollout_band_s": (rollout_done - initiated).total_seconds(),
                                  "settled_readback_s": (settled_done - rollout_done).total_seconds()}}
        self.transitions[(fid, phase)] = record
        self.journal.write_artifact(artifact_id="gateway-transition-" + fid + "-" + phase,
                                    relative_path="artifacts/gateway/transition-" + fid + "-" + phase + ".json",
                                    raw=c.canonical_json(record).encode(), media_type="application/json")
        return record

    def load_transition(self, fid, phase, *, missing_ok=False):
        """Hash-verified reload of a durable transition record after a reopen.

        missing_ok=True returns None when the record was never written; a
        written-but-corrupted record still refuses instead of degrading.
        """
        _check(fid in self.requests and phase in {"inject", "restore"}, "unknown gateway transition identity")
        rows = [row for row in self.journal.records() if row["event"] == "artifact_complete"
                and row["payload"]["artifact_id"] == "gateway-transition-" + fid + "-" + phase]
        if missing_ok and not rows:
            return None
        _check(len(rows) == 1, "durable gateway transition missing")
        reference = rows[0]["payload"]
        raw = self.journal._file(reference["relative_path"]).read_bytes()
        _check(hashlib.sha256(raw).hexdigest() == reference["sha256"], "gateway transition hash mismatch")
        data = json.loads(raw)
        _check(data.get("schema_version") == TRANSITION_SCHEMA and data.get("fault_instance_id") == fid
               and data.get("phase") == phase and data.get("mechanism_version") == MECHANISM_VERSION,
               "gateway transition identity drift")
        return data

    def transition(self, fid):
        """In-memory transition records for one fault: {'inject': ..., 'restore': ...}."""
        _check(fid in self.requests, "unknown gateway fault instance")
        return {phase[1]: record for phase, record in self.transitions.items() if phase[0] == fid}

    def dry_run_switch(self, request, plan):
        """Server dry-run of the Deployment seed-source switch; nothing persists.

        collection/F1: no per-command digest evidence is written -- a dry-run is not
        executed-attempt evidence.
        """
        fid = request.spec.fault_instance_id
        _, deployment = self.targets_for(fid)
        self._command_digests_enabled = False
        try:
            obj = self._deployment(fid)
            current = j.ResourceState(True, obj["metadata"]["uid"], obj["metadata"].get("annotations", {}).get(p.OWNER_ANNOTATION), self._fields(obj))
            return self._patch(deployment, current, self.desired_fields_for(deployment, plan),
                               self.journal.metadata["ownership"], dry_run=True, plan=plan)
        finally:
            self._command_digests_enabled = True

    def verify(self, request):
        fid, plan = request.spec.fault_instance_id, self._plan(request.spec.fault_instance_id)
        cm, deployment = self.targets_for(fid)
        for target in (cm, deployment):
            state = self.observe(target, tuple(self.desired_fields(target)))
            _check(state.exists and state.ownership == self.journal.metadata["ownership"] and state.fields == self.desired_fields(target), "gateway desired control state not verified")
        obj, runtime = self._runtime(fid, self._deployment(fid), plan["modified_config"], expected_outside=plan["outside_config_sha256"], require_readonly=True)
        final = self.observe(deployment, tuple(self.desired_fields(deployment)))
        _check(final.fields == self.desired_fields(deployment) and final.ownership == self.journal.metadata["ownership"], "gateway control state drifted during runtime verification")
        old_pods = {pod["pod_uid"] for pod in plan["baseline_runtime"]["pods"]}
        _check(not old_pods.intersection(pod["pod_uid"] for pod in runtime["pods"]), "volume switch did not produce new Pods")
        isolation = _knob_isolation(plan["baseline_config"], plan["modified_config"])
        _check(isolation["changed_directives"] == [request.directive], "gateway edit is not an isolated single-knob change")
        transition = self._record_transition(fid, "inject", obj, runtime, plan)
        return {"fault_instance_id": fid, "mechanism_version": MECHANISM_VERSION, "controlled_state_matches": True,
                "evidence_kind": self.evidence_kind, "runtime": runtime, "injection_effectiveness": "not_assessed",
                "knob_isolation": isolation, "transition": transition,
                "rollout_transition_is_not_timeout_signal": True}


class GatewayExecutor:
    def __init__(self, journal, adapter):
        _check(adapter.journal is journal, "gateway adapter belongs to another attempt")
        self.journal, self.adapter = journal, adapter

    def apply(self, request, *, intent_prefix, execute, checkpoint=None):
        """Execute or preview one gateway fault.

        execute is a mandatory explicit bool. False runs the full chain as a
        zero-change preview: read-only preflight (layout/ownership/rewrite/
        runtime baseline), server dry-run of the owned-ConfigMap admission and
        of the Deployment seed-source switch, plus the simulated two-intent
        plan -- no journal intent/artifact writes, no cluster mutation, no
        failure marking and (collection/F1) no per-command digest files; the attempt
        evidence directory stays exactly as it was. True is the accepted
        write-ahead apply path; any failure marks the attempt failed for
        recovery via the durable intents.
        """
        _check(type(execute) is bool, "execute must be an explicit bool")
        _check(type(intent_prefix) is str and intent_prefix.isidentifier(), "valid intent prefix required")
        if execute is False:
            return self._preview(request, intent_prefix)
        fid = request.spec.fault_instance_id
        try:
            self.adapter.preflight(request)
            cm, deployment = self.adapter.targets_for(fid)
            ids = (intent_prefix + "_configmap", intent_prefix + "_volume")
            for target, intent_id in zip((cm, deployment), ids):
                self.journal.execute_change(intent_id=intent_id, fault_instance_id=fid, target=target,
                                            desired_fields=self.adapter.desired_fields(target), adapter=self.adapter, checkpoint=checkpoint)
                if checkpoint:
                    checkpoint("after_configmap_intent" if target.kind == "ConfigMap" else "after_volume_intent")
            result = self.adapter.verify(request)
            return {**result, "sub_intent_ids": list(ids), "normalized_root_entity": request.spec.normalized_root_entity}
        except Exception as exc:
            
            # already failed upstream (the during_fault log-archive poisoning
            # family), execute_change rejects this leg at its guard BEFORE any
            # observation, durable intent or cluster mutation -- no snapshot-*
            # artifact exists for it. Marking that rejection as a gateway
            # apply/verify failure mislabels the upstream cause in the failure
            # chain; the attempt is already failed and keeps its true terminal
            # reason code. The exception still propagates unchanged. Every
            # other failure (including OperationFailed after a durable
            # gateway intent) keeps the honest marking.
            if type(exc) is j.JournalError and str(exc) == "failed/blocked attempt cannot apply new changes":
                raise
            self.journal.mark_failed("gateway_apply_or_verify_failed")
            raise

    def _preview(self, request, intent_prefix):
        """Full-chain zero-change preview; raises without marking the journal."""
        fid = request.spec.fault_instance_id
        plan = self.adapter.preflight(request, durable=False)
        cm_target, deployment = self.adapter.targets_for(fid)
        ids = (intent_prefix + "_configmap", intent_prefix + "_volume")
        self.adapter.dry_run_switch(request, plan)
        data = plan.to_dict()
        isolation = _knob_isolation(data["baseline_config"], data["modified_config"])
        _check(isolation["changed_directives"] == [request.directive], "gateway edit is not an isolated single-knob change")
        return {"execute": False, "fault_instance_id": fid, "mechanism": MECHANISM, "mechanism_version": MECHANISM_VERSION,
                "evidence_kind": self.adapter.evidence_kind, "plan_sha256": plan.sha256,
                "directive": request.directive, "value": request.value, "knob_isolation": isolation,
                "simulated_intents": [
                    {"intent_id": ids[0], "target": cm_target.to_dict(),
                     "controlled_fields": self.adapter.desired_fields_for(cm_target, plan)},
                    {"intent_id": ids[1], "target": deployment.to_dict(),
                     "controlled_fields": self.adapter.desired_fields_for(deployment, plan)}],
                "admission_dry_run": {"owned_configmap": "passed", "deployment_volume_switch": "passed"},
                "seed_command": list(_SEED_COMMAND), "transition": None,
                "transition_note": "no rollout occurs in preview; execute=True records the switch band",
                "cluster_mutations": [], "journal_intents_written": 0,
                "per_command_digest_files_written": 0,
                "injection_effectiveness": "not_assessed",
                "preview_only_no_cluster_or_journal_change": True}

    def recover(self, *, fault_instance_ids=None, checkpoint=None):
        return self.journal.reconcile(self.adapter, checkpoint=checkpoint, fault_instance_ids=fault_instance_ids)


class CompositeResourceAdapter:
    """Explicit exact-target routing only; never discovers or mutates extra roots."""
    def __init__(self, routes):
        self.routes = {}
        kinds = set()
        for target, adapter in routes:
            key = _target_key(target)
            _check(key not in self.routes, "ambiguous composite resource route")
            self.routes[key] = adapter
            kinds.add(adapter.evidence_kind)
        _check(bool(self.routes) and len(kinds) == 1 and kinds <= {"fake", "live"}, "composite adapter evidence kind mismatch")
        self.evidence_kind = next(iter(kinds))

    def _adapter(self, target):
        _check(_target_key(target) in self.routes, "unapproved composite target")
        return self.routes[_target_key(target)]

    def observe(self, target, field_names):
        return self._adapter(target).observe(target, field_names)

    def compare_and_apply(self, target, expected, desired_fields, ownership):
        return self._adapter(target).compare_and_apply(target, expected, desired_fields, ownership)

    def compare_and_restore(self, target, expected, original):
        return self._adapter(target).compare_and_restore(target, expected, original)
