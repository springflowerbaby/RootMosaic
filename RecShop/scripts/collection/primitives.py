"""collection source-backed primitive preparation and journal-guarded K8s adapters.

Preparation is pure. Real commands require an explicitly enabled client; tests
use a fake client only. Control-plane readback never certifies symptom/QC
effectiveness. Direct gateway file mutation is deliberately unsupported.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import math
import re
import subprocess
from typing import Any, Protocol
import uuid

from .contract import FaultInstanceSpec, canonical_json, canonical_sha256
from .journal import AttemptJournal, JournalError, ResourceState, Target

MECHANISM_VERSION = "m1-p1-v1"
OWNER_ANNOTATION = "recshop.dev/m1-owner"
SELECTION_ANNOTATION = "recshop.dev/m1-selection"
# Off-graph fault roots (CATALOG scope_note "not ordinary service"): they never
# bind an ordinary service Deployment; "host" binds the dedicated CPU stressor
# carrier, "mysql_items_lock" is served by scripts.collection.primitives_db instead.
OFF_GRAPH_ENTITIES = frozenset({"host", "mysql_items_lock"})
_CRD_PLURALS = {"NetworkChaos": "networkchaos", "StressChaos": "stresschaos", "PodChaos": "podchaos"}


def _cpu_cores(value: Any) -> float | None:
    """Parse a Kubernetes CPU quantity ("2", "1.5", "500m"); None when invalid."""
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?(?:m)?", value):
        return None
    return float(value[:-1]) / 1000 if value.endswith("m") else float(value)


def _resource(kind: str) -> str:
    return {"Deployment": "deployments.apps", "Pod": "pods", **{k: v + ".chaos-mesh.org" for k, v in _CRD_PLURALS.items()}}[kind]


class PrimitiveError(JournalError):
    pass


class UnsupportedPrimitive(PrimitiveError):
    pass


def _check(ok: bool, code: str) -> None:
    if not ok:
        raise PrimitiveError(code)


def _name(value: Any) -> None:
    _check(isinstance(value, str) and re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", value) is not None,
           "invalid Kubernetes name")


def _positive(value: Any, name: str, *, integer: bool = False, zero: bool = False) -> None:
    _check(type(value) in ((int,) if integer else (int, float)) and math.isfinite(value)
           and (value >= 0 if zero else value > 0), "invalid " + name)


def _fields(value: Any, expected: set[str]) -> None:
    _check(isinstance(value, dict) and set(value) == expected, "mechanism parameters missing or unknown")


def _copy(value: Any) -> Any:
    try:
        return json.loads(canonical_json(value))
    except (ValueError, TypeError):
        raise PrimitiveError("invalid or credential-bearing primitive data") from None


@dataclass(frozen=True)
class PodPin:
    name: str
    uid: str
    images: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        _name(self.name)
        _check(isinstance(self.uid, str) and bool(self.uid), "pod UID missing")
        _check(type(self.images) is tuple and bool(self.images), "original pod container images required")
        _check(len(dict(self.images)) == len(self.images), "duplicate container image pin")
        for name, image in self.images:
            _name(name)
            _check(isinstance(image, str) and bool(image), "container image missing")

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "uid": self.uid, "images": dict(self.images)}


@dataclass(frozen=True)
class PinnedTarget:
    """Explicitly pinned Kubernetes fault target with two distinct identities.

    ``deployment`` is the Deployment OBJECT name used for every API call; the
    ``selector`` value is that deployment's canonical LIVE ``app`` pod label.
    Ordinary services keep the two identical (app label == deployment name);
    two rollout-era deviations keep K8s-immutable labels that differ from the
    object name (deployment "backend" -> app=backend_api, deployment
    "rec-agent" -> app=recommendation_agent; SDEPLOY-SELECTOR-PIN precedent
    pins the observed live label, never the object name). The selector must
    still be exactly the single canonical ``app`` key with a non-empty string
    value; live binding fail-loud re-verifies the deployment matchLabels and
    every pinned pod label against it, so a wrong value can never silently
    select nothing.
    """

    context: str
    namespace: str
    entity: str
    deployment: str
    deployment_uid: str
    container: str
    selector: tuple[tuple[str, str], ...]
    pods: tuple[PodPin, ...] = ()

    def __post_init__(self) -> None:
        _check(isinstance(self.context, str) and bool(self.context.strip()) and not any(c in self.context for c in "\r\n\0"),
               "explicit Kubernetes context required")
        for value in (self.namespace, self.deployment, self.container):
            _name(value)
        _check(isinstance(self.deployment_uid, str) and bool(self.deployment_uid), "deployment UID missing")
        # The canonical app label pins the bound Deployment itself. Ordinary
        
        # dedicated stressor carrier Deployment instead (never a victim service).
        _check(self.deployment == self.entity or self.entity == "host", "off-graph entity binds only the host stressor carrier")
        # The selector value is the deployment's LIVE canonical app label, which
        # for the two rollout-era deviations differs from the object name
        # (SDEPLOY-SELECTOR-PIN deviant, F-S1-3: backend -> backend_api,
        # rec-agent -> recommendation_agent) -- so it is no longer required to
        # equal self.deployment. The shape discipline is unchanged: exactly the
        # single "app" key, no duplicate keys, non-empty string value.
        selector_map = dict(self.selector) if type(self.selector) is tuple else None
        _check(selector_map is not None and len(selector_map) == len(self.selector) and set(selector_map) == {"app"}
               and isinstance(selector_map["app"], str) and bool(selector_map["app"]),
               "explicit canonical app selector required")
        _check(type(self.pods) is tuple and len({p.name for p in self.pods}) == len(self.pods), "duplicate/missing pod pins")


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str = field(repr=False)
    stderr: str = field(repr=False)


class CommandClient(Protocol):
    evidence_kind: str
    supports_delete_preconditions: bool
    supports_server_dry_run: bool

    def run(self, argv: tuple[str, ...], *, stdin: str | None, timeout_s: float) -> CommandResult: ...


@dataclass(frozen=True)
class SubprocessClient:
    """No command is run unless the caller explicitly enables this instance.

    supports_delete_preconditions must be confirmed for the installed kubectl
    before enabling CRD mutations. No kubeconfig/credential contents are logged.
    """
    enabled: bool = False
    supports_delete_preconditions: bool = False
    supports_server_dry_run: bool = False
    evidence_kind: str = field(default="live", init=False)

    def run(self, argv: tuple[str, ...], *, stdin: str | None, timeout_s: float) -> CommandResult:
        _check(self.enabled is True, "real command client disabled")
        _check(type(argv) is tuple and bool(argv) and all(isinstance(a, str) and "\0" not in a for a in argv), "invalid command argv")
        _positive(timeout_s, "command timeout")
        try:
            result = subprocess.run(list(argv), input=stdin, capture_output=True, text=True, encoding="utf-8",
                                    errors="replace", timeout=timeout_s, shell=False)
        except (OSError, subprocess.TimeoutExpired):
            raise PrimitiveError("command launch or deadline failure") from None
        return CommandResult(result.returncode, result.stdout, result.stderr)


def _controlled_projection(actual: Any, submitted: Any) -> Any:
    """Keep every submitted field/type/list position; ignore only extra keys.

    Full-spec equivalence is checked separately, so extra admission fields can
    disqualify an attempt without making its owned newly-created CRD undeletable.
    """
    if isinstance(submitted, dict):
        _check(isinstance(actual, dict) and set(submitted).issubset(actual), "CRD controlled field absent")
        return {key: _controlled_projection(actual[key], value) for key, value in submitted.items()}
    if isinstance(submitted, list):
        _check(type(actual) is list and len(actual) == len(submitted), "CRD controlled list changed")
        return [_controlled_projection(a, b) for a, b in zip(actual, submitted)]
    _check(type(actual) is type(submitted), "CRD controlled field type changed")
    return actual


@dataclass(frozen=True, repr=False)
class PreparedFault:
    spec: FaultInstanceSpec
    binding: PinnedTarget
    target: Target
    desired_json: str
    mode: str

    @property
    def desired_fields(self) -> dict[str, Any]:
        return json.loads(self.desired_json)


def prepare_fault(spec: FaultInstanceSpec, binding: PinnedTarget, *, ownership: str) -> PreparedFault:
    """Pure mechanism/schema preparation; does not query a cluster or read env."""
    data = spec.to_dict()
    if data["fault_type"] not in {"dependency_latency", "runtime_exception", "timeout_misconfiguration",
                                  "retry_policy_misconfiguration", "network_delay", "network_loss",
                                  "service_cpu_saturation", "service_unavailable", "host_cpu_saturation"}:
        raise UnsupportedPrimitive('db_table_lock uses the scripts.collection.primitives_db adapter; other pending mechanisms are not implemented')
    raw = data["raw_target"]
    _check(data["mechanism_version"] == MECHANISM_VERSION, "unsupported primitive mechanism version")
    _check(binding.entity == data["normalized_root_entity"] and raw["kind"] == "Deployment"
           and raw["name"] == binding.deployment and raw["scope"] == binding.namespace
           and raw["selector"] == dict(binding.selector), "fault/binding target mismatch")
    _check(isinstance(ownership, str) and re.fullmatch(r"[a-zA-Z0-9-]+", ownership) is not None, "ownership marker invalid")
    fault_type, params = data["fault_type"], data["parameters"]
    if fault_type in {"dependency_latency", "runtime_exception"}:
        _check(data["mechanism"] == "app_env_hook" and binding.entity in {"catalog", "inventory"}, "unsupported env-hook target/mechanism")
        if fault_type == "dependency_latency":
            _fields(params, {"delay_ms"})
            _positive(params["delay_ms"], "delay_ms", integer=True)
            variable, value = "FAULT_DELAY_MS", str(params["delay_ms"])
        else:
            _fields(params, {"enabled"})
            _check(params["enabled"] is True, "runtime exception injection must enable FAULT_RAISE")
            variable, value = "FAULT_RAISE", "1"
        target = Target("Deployment", binding.deployment, binding.namespace, binding.deployment_uid)
        return PreparedFault(spec, binding, target, canonical_json({variable: {"present": True, "value": value}}), "deployment_env")
    if fault_type in {"timeout_misconfiguration", "retry_policy_misconfiguration"}:
        _check(data["mechanism"] == "nginx_directive" and binding.entity == "catalog-gw", "unsupported gateway mechanism")
        if fault_type == "timeout_misconfiguration":
            _fields(params, {"read_timeout_ms"})
            _positive(params["read_timeout_ms"], "read_timeout_ms", integer=True)
            desired = {"proxy_read_timeout": str(params["read_timeout_ms"]) + "ms"}
        else:
            _fields(params, {"proxy_next_upstream"})
            _check(params["proxy_next_upstream"] == "off", "only isolated retry-disable supported")
            desired = {"proxy_next_upstream": "off"}
        return PreparedFault(spec, binding, Target("GatewayFile", binding.deployment, binding.namespace, binding.deployment_uid),
                             canonical_json(desired), "gateway_prepare_only")
    if fault_type not in {"network_delay", "network_loss", "service_cpu_saturation", "service_unavailable", "host_cpu_saturation"}:
        raise UnsupportedPrimitive('db_table_lock uses the scripts.collection.primitives_db adapter; other pending mechanisms are not implemented')
    _check(data["mechanism"] == "chaos_mesh" and bool(binding.pods), "Chaos Mesh requires explicit pinned pods")
    if fault_type == "network_delay":
        _check(binding.entity not in OFF_GRAPH_ENTITIES, "network chaos targets an ordinary service deployment")
        _fields(params, {"latency_ms", "jitter_ms", "correlation_percent", "duration_s"})
        _positive(params["latency_ms"], "latency_ms", integer=True)
        _positive(params["jitter_ms"], "jitter_ms", integer=True, zero=True)
        _positive(params["correlation_percent"], "correlation_percent", zero=True)
        _check(params["correlation_percent"] <= 100, "correlation above 100 percent")
        kind = "NetworkChaos"
        chaos = {"action": "delay", "direction": "to", "delay": {"latency": str(params["latency_ms"]) + "ms",
                 "jitter": str(params["jitter_ms"]) + "ms", "correlation": str(params["correlation_percent"])}}
    elif fault_type == "network_loss":
        _check(binding.entity not in OFF_GRAPH_ENTITIES, "network chaos targets an ordinary service deployment")
        _fields(params, {"loss_percent", "correlation_percent", "duration_s"})
        _positive(params["loss_percent"], "loss_percent")
        _positive(params["correlation_percent"], "correlation_percent", zero=True)
        _check(params["loss_percent"] <= 100 and params["correlation_percent"] <= 100, "network percentage above 100")
        kind, chaos = "NetworkChaos", {"action": "loss", "direction": "to",
            "loss": {"loss": str(params["loss_percent"]), "correlation": str(params["correlation_percent"])}}
    elif fault_type == "service_cpu_saturation":
        # Container-scope CPU stress inside the pinned service pods; the pinned
        # cgroup limit keeps the effect service-local (old probe2 lesson).
        _check(binding.entity not in OFF_GRAPH_ENTITIES, "service CPU saturation targets an ordinary service deployment")
        _fields(params, {"workers", "load_percent", "duration_s"})
        _positive(params["workers"], "workers", integer=True)
        _positive(params["load_percent"], "load_percent", integer=True)
        _check(params["load_percent"] <= 100, "CPU load above 100 percent")
        kind, chaos = "StressChaos", {"stressors": {"cpu": {"workers": params["workers"], "load": params["load_percent"]}}}
    elif fault_type == "host_cpu_saturation":
        # Node-scope CPU saturation: the same StressChaos engine selects the
        # dedicated no-CPU-limit stressor carrier pods pinned by the binding,
        # never victim service pods. CPU-only stressors; vm/memory stress is
        # rejected by the parameter schema (never --vm, prevents OOMKill).
        _check(binding.entity == "host", "host CPU saturation binds only the off-graph host stressor carrier")
        _fields(params, {"workers", "load_percent", "duration_s"})
        _positive(params["workers"], "workers", integer=True)
        _positive(params["load_percent"], "load_percent", integer=True)
        _check(params["load_percent"] <= 100, "CPU load above 100 percent")
        kind, chaos = "StressChaos", {"stressors": {"cpu": {"workers": params["workers"], "load": params["load_percent"]}}}
    else:
        _check(binding.entity not in OFF_GRAPH_ENTITIES, "pod-failure targets an ordinary service deployment")
        _fields(params, {"action", "duration_s"})
        _check(params["action"] == "pod-failure", "pod-kill is not service_unavailable pod-failure")
        kind, chaos = "PodChaos", {"action": "pod-failure"}
    _positive(params["duration_s"], "duration_s", integer=True)
    _check(params["duration_s"] >= spec.planned_window.end - spec.planned_window.start, "CRD duration shorter than planned fault exposure")
    chaos.update(mode="all", duration=str(params["duration_s"]) + "s",
                 # namespaces must be submitted explicitly: the Chaos Mesh admission
                 # webhook defaults selector.namespaces from the pods map, and the
                 # server dry-run spec equality (preflight_crd) fail-loud refuses any
                 # admission-authored field. Matches the verified old runner yamls
                 # (e.g. PodChaos service-unavailability configuration). Live evidence:
                 
                 selector={"pods": {binding.namespace: [p.name for p in binding.pods]},
                           "namespaces": [binding.namespace]})
    selection = {"deployment_uid": binding.deployment_uid, "selector": dict(binding.selector),
                 "pods": [p.to_dict() for p in binding.pods]}
    name = "m1-" + canonical_sha256({"ownership": ownership, "instance": data["fault_instance_id"], "spec": data})[:32]
    return PreparedFault(spec, binding, Target(kind, name, binding.namespace, None),
                         canonical_json({"spec": chaos, "selection": selection}), "chaos_crd")


def render_chaos_manifest(prepared: PreparedFault, *, ownership: str) -> dict[str, Any]:
    _check(prepared.mode == "chaos_crd", "not a Chaos Mesh primitive")
    _check(prepared == prepare_fault(prepared.spec, prepared.binding, ownership=ownership), "manifest owner differs from prepared identity")
    desired = prepared.desired_fields
    return {"apiVersion": "chaos-mesh.org/v1alpha1", "kind": prepared.target.kind,
            "metadata": {"name": prepared.target.name, "namespace": prepared.target.scope,
                         "annotations": {OWNER_ANNOTATION: ownership, SELECTION_ANNOTATION: canonical_json(desired["selection"])}},
            "spec": desired["spec"]}


def render_stressor_manifest(prepared: PreparedFault, *, image: str, ownership: str,
                             requests_cpu: str = "2", requests_memory: str = "64Mi") -> dict[str, Any]:
    """Pure rendering of the required off-graph CPU stressor carrier Deployment.

    Mirrors the verified old shape (chaos-stressor-deploy.yaml): no CPU limits so
    stress-ng can saturate the whole node, explicit CPU requests so the carrier is
    not BestEffort, and a bare sleep shell (stress-ng itself is injected by the
    StressChaos CRD). This function never applies anything; provisioning and
    scale-down backstops belong to the run infrastructure outside the fault CAS
    window. Strength values live in the fault contract (NOT_FROZEN), not here.
    """
    _check(prepared.mode == "chaos_crd", "not a Chaos Mesh primitive")
    _check(prepared == prepare_fault(prepared.spec, prepared.binding, ownership=ownership), "manifest owner differs from prepared identity")
    data = prepared.spec.to_dict()
    _check(data["fault_type"] == "host_cpu_saturation" and prepared.binding.entity == "host",
           "stressor manifest is specific to host CPU saturation")
    _check(isinstance(image, str) and bool(image.strip()) and not any(c in image for c in " \t\r\n\0"),
           "stressor carrier image required")
    _check(_cpu_cores(requests_cpu) is not None and _cpu_cores(requests_cpu) > 0, "stressor CPU requests invalid")
    _check(isinstance(requests_memory, str)
           and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?(?:E|P|T|G|M|k|m|Ki|Mi|Gi|Ti|Pi|Ei)?", requests_memory) is not None,
           "stressor memory requests invalid")
    name = prepared.binding.deployment
    return {"apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": name, "namespace": prepared.binding.namespace, "labels": {"app": name},
                         "annotations": {OWNER_ANNOTATION: ownership}},
            "spec": {"replicas": 1, "selector": {"matchLabels": {"app": name}},
                     "template": {"metadata": {"labels": {"app": name}},
                                  "spec": {"enableServiceLinks": False, "containers": [{
                                      "name": prepared.binding.container, "image": image,
                                      "imagePullPolicy": "IfNotPresent", "command": ["sleep", "infinity"],
                                      "resources": {"requests": {"cpu": requests_cpu, "memory": requests_memory}}}]}}}}


class KubernetesAdapter:
    """Journal-bound CAS adapter for Deployment env and owned Chaos resources."""

    def __init__(self, journal: AttemptJournal, client: CommandClient, prepared: tuple[PreparedFault, ...], *,
                 kubectl: str, command_timeout_s: float, wait_timeout_s: int) -> None:
        _check(isinstance(kubectl, str) and bool(kubectl), "explicit kubectl executable required")
        _positive(command_timeout_s, "command timeout")
        _positive(wait_timeout_s, "wait timeout", integer=True)
        _check(command_timeout_s > wait_timeout_s, "command timeout must exceed wait timeout")
        _check(client.evidence_kind in {"fake", "live"}, "client evidence kind required")
        self.journal, self.client = journal, client
        self.evidence_kind = client.evidence_kind
        self.kubectl, self.command_timeout_s, self.wait_timeout_s = kubectl, command_timeout_s, wait_timeout_s
        context = journal.contract.context.to_dict()
        self._prepared = {}
        self._dry_run_ready: set[Target] = set()
        for item in prepared:
            rebound = prepare_fault(item.spec, item.binding, ownership=journal.metadata["ownership"])
            _check(item == rebound, "prepared primitive differs from validated mechanism/owner")
            _check(item.binding.context == context["kube_context"] and item.binding.namespace == context["namespace"], "client context/namespace differs from contract")
            _check(any(f.to_dict() == item.spec.to_dict() for f in journal.contract.faults), "primitive not bound to this contract")
            key = (item.target.kind, item.target.name, item.target.scope)
            _check(key not in self._prepared, "duplicate prepared target")
            self._prepared[key] = item
            _check(item.mode != "chaos_crd" or (client.supports_delete_preconditions is True
                   and getattr(client, "supports_server_dry_run", False) is True),
                   "CRD execution disabled without verified dry-run and conditional-delete support")
        self.evidence: list[dict[str, Any]] = []

    def _item(self, target: Target) -> PreparedFault:
        item = self._prepared.get((target.kind, target.name, target.scope))
        _check(item is not None and item.target == target, "unapproved resource target/UID")
        return item

    def _run(self, item: PreparedFault, operation: str, args: tuple[str, ...], *, stdin: str | None = None) -> CommandResult:
        if args[0] in {"patch", "create", "delete"} and "--dry-run=server" not in args:
            _check(operation in {"patch_env", "create_crd", "delete_crd_preconditioned"}, "unapproved mutation operation")
            self._intent_guard(item.target, item.desired_fields, restoring=True)
        argv = (self.kubectl, "--context", item.binding.context, "--namespace", item.binding.namespace, *args)
        result = self.client.run(argv, stdin=stdin, timeout_s=self.command_timeout_s)
        _check(isinstance(result, CommandResult) and type(result.returncode) is int
               and isinstance(result.stdout, str) and isinstance(result.stderr, str), "command result type invalid")
        evidence = {"operation": operation, "target": item.target.to_dict(), "returncode": result.returncode,
                    "recorded_at_utc": datetime.now(timezone.utc).isoformat(), "evidence_kind": self.evidence_kind,
                    "stdout_sha256": hashlib.sha256(result.stdout.encode("utf-8")).hexdigest(),
                    "stderr_sha256": hashlib.sha256(result.stderr.encode("utf-8")).hexdigest()}
        self.evidence.append(evidence)
        # Separate durable command/wait evidence; controlled snapshots remain
        # in journal storage, never a full credential-bearing resource dump.
        self.journal._write_new("primitive-evidence-" + uuid.uuid4().hex + ".json", canonical_json(evidence).encode("utf-8"))
        _check(result.returncode == 0, "command failed; output retained only as digest")
        return result

    def _get(self, item: PreparedFault, kind: str, name: str) -> dict[str, Any] | None:
        result = self._run(item, "get_" + kind, ("get", _resource(kind), name, "-o", "json", "--ignore-not-found"))
        if not result.stdout.strip():
            return None
        try:
            obj = json.loads(result.stdout)
        except (ValueError, TypeError):
            raise PrimitiveError("resource JSON invalid") from None
        _check(isinstance(obj, dict) and obj.get("kind") == kind and obj.get("metadata", {}).get("name") == name
               and obj["metadata"].get("namespace") == item.binding.namespace, "resource identity/namespace mismatch")
        _check(obj.get("apiVersion") == ("apps/v1" if kind == "Deployment" else "v1" if kind == "Pod" else "chaos-mesh.org/v1alpha1"),
               "resource API version mismatch")
        for key in ("uid", "resourceVersion"):
            _check(isinstance(obj["metadata"].get(key), str) and bool(obj["metadata"][key]), "resource UID/version missing")
        return obj

    def _deployment(self, item: PreparedFault) -> dict[str, Any]:
        obj = self._get(item, "Deployment", item.binding.deployment)
        _check(obj is not None and obj["metadata"]["uid"] == item.binding.deployment_uid, "deployment UID changed/missing")
        wanted = dict(item.binding.selector)
        _check(obj.get("spec", {}).get("selector", {}).get("matchLabels") == wanted,
               "deployment selector mismatch")
        labels = obj["spec"].get("template", {}).get("metadata", {}).get("labels", {})
        _check(all(labels.get(k) == v for k, v in wanted.items()), "deployment pod labels mismatch")
        return obj

    def _pods(self, item: PreparedFault, *, require_original_images: bool) -> None:
        obj = self._deployment(item)
        if item.binding.entity == "host":
            self._verify_host_carrier(item, obj)
        for pin in item.binding.pods:
            if require_original_images:
                self._run(item, "wait_pod_ready", ("wait", "pods/" + pin.name,
                          "--for=condition=Ready", "--timeout=" + str(self.wait_timeout_s) + "s"))
            obj = self._get(item, "Pod", pin.name)
            _check(obj is not None and obj["metadata"]["uid"] == pin.uid, "selected Pod UID changed/missing")
            _check(not obj["metadata"].get("deletionTimestamp"), "selected Pod is deleting")
            _check(all(obj["metadata"].get("labels", {}).get(k) == v for k, v in item.binding.selector), "selected Pod label mismatch")
            if require_original_images:
                images = {c["name"]: c["image"] for c in obj.get("spec", {}).get("containers", [])}
                _check(images == dict(pin.images), "selected Pod original images not verified")
                _check(any(c.get("type") == "Ready" and c.get("status") == "True" for c in obj.get("status", {}).get("conditions", [])),
                       "selected Pod readiness not verified")

    @staticmethod
    def _container(obj: dict[str, Any], name: str) -> tuple[int, dict[str, Any]]:
        containers = obj.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        matches = [(i, c) for i, c in enumerate(containers) if c.get("name") == name]
        _check(len(matches) == 1, "container missing or ambiguous")
        return matches[0]

    @staticmethod
    def _verify_host_carrier(item: PreparedFault, obj: dict[str, Any]) -> None:
        """Mechanism-shape gate for the host CPU carrier (old probe2/probe4 lessons).

        A cpu-limited carrier would confine stress-ng to its own cgroup and turn
        node-scope saturation into a service-scope no-op; a BestEffort carrier
        (no requests) gets reverse-throttled by victim requests instead.
        """
        _, container = KubernetesAdapter._container(obj, item.binding.container)
        resources = container.get("resources") or {}
        requests, limits = resources.get("requests") or {}, resources.get("limits") or {}
        cores = _cpu_cores(requests.get("cpu"))
        _check(cores is not None and cores > 0, "host CPU carrier requires positive CPU requests (BestEffort would no-op)")
        _check("cpu" not in limits, "host CPU carrier must not set a CPU limit (node-scope saturation required)")

    @staticmethod
    def _env(container: dict[str, Any], variable: str) -> dict[str, Any]:
        matches = [e for e in container.get("env", []) if e.get("name") == variable]
        _check(len(matches) <= 1, "duplicate fault env variable")
        if not matches:
            return {"present": False, "value": None}
        _check(set(matches[0]) == {"name", "value"} and isinstance(matches[0]["value"], str),
               "valueFrom/nonliteral fault env cannot be safely overridden")
        return {"present": True, "value": matches[0]["value"]}

    def observe(self, target: Target, field_names: tuple[str, ...]) -> ResourceState:
        item = self._item(target)
        _check(set(field_names) == set(item.desired_fields), "unexpected controlled fields")
        if item.mode == "gateway_prepare_only":
            raise UnsupportedPrimitive("gateway file mutation lacks proven file CAS/ownership/lock; use future owned-ConfigMap rollout adapter")
        if item.mode == "deployment_env":
            obj = self._deployment(item)
            self._run(item, "observe_rollout_status", ("rollout", "status", "deployment/" + item.binding.deployment,
                      "--timeout=" + str(self.wait_timeout_s) + "s"))
            obj = self._deployment(item)  # Do not return the pre-wait snapshot.
            generation = obj["metadata"].get("generation")
            status, replicas = obj.get("status", {}), obj.get("spec", {}).get("replicas")
            _check(type(generation) is int and generation > 0 and type(replicas) is int and replicas > 0
                   and type(status.get("observedGeneration")) is int and status["observedGeneration"] >= generation
                   and all(status.get(k) == replicas for k in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas"))
                   and not obj["metadata"].get("deletionTimestamp"), "fresh deployment generation/readiness not coherent")
            _, container = self._container(obj, item.binding.container)
            fields = {name: self._env(container, name) for name in field_names}
        else:
            obj = self._get(item, target.kind, target.name)
            if obj is None:
                self._pods(item, require_original_images=True)
                return ResourceState(False, None, None, {})
            try:
                selection = json.loads(obj["metadata"].get("annotations", {})[SELECTION_ANNOTATION])
            except (ValueError, KeyError, TypeError):
                raise PrimitiveError("CRD selection identity absent/invalid") from None
            fields = {"spec": _controlled_projection(obj["spec"], item.desired_fields["spec"]), "selection": selection}
        owner = obj["metadata"].get("annotations", {}).get(OWNER_ANNOTATION)
        return ResourceState(True, obj["metadata"]["uid"], owner, _copy(fields))

    def preflight_crd(self, prepared: PreparedFault) -> None:
        """Explicit server dry-run before any persisted resource mutation."""
        self._item(prepared.target)
        manifest = render_chaos_manifest(prepared, ownership=self.journal.metadata["ownership"])
        result = self._run(prepared, "server_dry_run_crd", ("create", "--dry-run=server", "-f", "-", "-o", "json"),
                           stdin=canonical_json(manifest))
        try:
            obj = json.loads(result.stdout)
            meta = obj["metadata"]
            _check(obj["kind"] == manifest["kind"] and obj["apiVersion"] == manifest["apiVersion"]
                   and meta["name"] == manifest["metadata"]["name"] and meta["namespace"] == prepared.target.scope
                   and all(meta.get("annotations", {}).get(k) == v for k, v in manifest["metadata"]["annotations"].items())
                   and canonical_json(obj["spec"]) == canonical_json(manifest["spec"]),
                   "CRD admission changes submitted specification; execution blocked before create")
        except (ValueError, TypeError, KeyError):
            raise PrimitiveError("CRD server dry-run output invalid") from None
        self._dry_run_ready.add(prepared.target)

    def admission_matches(self, prepared: PreparedFault) -> bool:
        obj = self._get(prepared, prepared.target.kind, prepared.target.name)
        _check(obj is not None, "created CRD missing at admission verification")
        # In-memory comparison only; raw admission payload is not logged.
        encoded = json.dumps(obj["spec"], sort_keys=True, separators=(",", ":"), allow_nan=False)
        submitted = json.dumps(prepared.desired_fields["spec"], sort_keys=True, separators=(",", ":"), allow_nan=False)
        return encoded == submitted

    def _intent_guard(self, target: Target, desired: dict[str, Any], *, restoring: bool) -> None:
        states = self.journal._replay(self.journal.records())
        for item in reversed(tuple(states.values())):
            intent = item["intent"]
            if intent["target"] != target.to_dict() or item["status"] == "restored":
                continue
            _check(intent["ownership"] == self.journal.metadata["ownership"], "journal ownership mismatch")
            if restoring:
                _check(desired == intent["controlled_fields"], "recovery expected state differs from durable intent")
            else:
                _check(item["status"] == "pending" and desired == intent["controlled_fields"], "mutation lacks current durable intent")
            return
        raise PrimitiveError("mutation blocked without durable write-ahead intent")

    def _patch_env(self, item: PreparedFault, expected: ResourceState, desired: dict[str, Any], ownership: str | None) -> None:
        obj = self._deployment(item)
        index, container = self._container(obj, item.binding.container)
        current = ResourceState(True, obj["metadata"]["uid"], obj["metadata"].get("annotations", {}).get(OWNER_ANNOTATION),
                                {name: self._env(container, name) for name in desired})
        _check(current.to_dict() == expected.to_dict(), "deployment CAS state mismatch")
        patch = [{"op": "test", "path": "/metadata/uid", "value": expected.uid},
                 {"op": "test", "path": "/metadata/resourceVersion", "value": obj["metadata"]["resourceVersion"]}]
        # P1 env primitives intentionally control exactly one variable.
        _check(len(desired) == 1, "only one env knob per operation supported")
        variable, entry = next(iter(desired.items()))
        _fields(entry, {"present", "value"})
        _check(type(entry["present"]) is bool and (isinstance(entry["value"], str) if entry["present"] else entry["value"] is None),
               "env presence/value invalid")
        env = container.get("env", [])
        found = [i for i, e in enumerate(env) if e.get("name") == variable]
        base = f"/spec/template/spec/containers/{index}/env"
        if entry["present"]:
            value = {"name": variable, "value": entry["value"]}
            if found:
                patch.append({"op": "replace", "path": base + "/" + str(found[0]), "value": value})
            elif "env" in container:
                patch.append({"op": "add", "path": base + "/-", "value": value})
            else:
                patch.append({"op": "add", "path": base, "value": [value]})
        elif found:
            patch.append({"op": "remove", "path": base + "/" + str(found[0])})
        escaped = OWNER_ANNOTATION.replace("~", "~0").replace("/", "~1")
        annotations = obj["metadata"].get("annotations")
        if ownership is None:
            if annotations and OWNER_ANNOTATION in annotations:
                patch.append({"op": "remove", "path": "/metadata/annotations/" + escaped})
        elif annotations is None:
            patch.append({"op": "add", "path": "/metadata/annotations", "value": {OWNER_ANNOTATION: ownership}})
        else:
            patch.append({"op": "add", "path": "/metadata/annotations/" + escaped, "value": ownership})
        self._run(item, "patch_env", ("patch", _resource("Deployment"), item.binding.deployment, "--type=json", "-p", canonical_json(patch), "-o", "json"))
        self._run(item, "rollout_status", ("rollout", "status", "deployment/" + item.binding.deployment,
                                           "--timeout=" + str(self.wait_timeout_s) + "s"))

    def compare_and_apply(self, target: Target, expected: ResourceState, desired_fields: dict[str, Any], ownership: str) -> None:
        item = self._item(target)
        _check(ownership == self.journal.metadata["ownership"] and desired_fields == item.desired_fields, "apply ownership/spec mismatch")
        self._intent_guard(target, desired_fields, restoring=False)
        if item.mode == "deployment_env":
            self._patch_env(item, expected, desired_fields, ownership)
            return
        _check(item.mode == "chaos_crd" and self.client.supports_delete_preconditions is True,
               "CRD execution requires verified conditional-delete capability")
        _check(target in self._dry_run_ready, "CRD requires fresh successful server dry-run before apply")
        self._dry_run_ready.remove(target)
        _check(not expected.exists and not self.observe(target, tuple(desired_fields)).exists, "CRD create cannot overwrite an existing object")
        self._pods(item, require_original_images=True)
        self._run(item, "create_crd", ("create", "-f", "-", "-o", "json"), stdin=canonical_json(render_chaos_manifest(item, ownership=ownership)))
        self._run(item, "wait_AllInjected", ("wait", _resource(target.kind) + "/" + target.name,
                  "--for=condition=AllInjected", "--timeout=" + str(self.wait_timeout_s) + "s"))
        self._pods(item, require_original_images=False)

    def compare_and_restore(self, target: Target, expected: ResourceState, original: ResourceState) -> None:
        item = self._item(target)
        self._intent_guard(target, expected.fields, restoring=True)
        if item.mode == "deployment_env":
            _check(original.exists and original.uid == expected.uid, "original deployment UID mismatch")
            self._patch_env(item, expected, original.fields, original.ownership)
            return
        _check(item.mode == "chaos_crd" and not original.exists and self.client.supports_delete_preconditions is True,
               "unsupported CRD restore or missing deletion preconditions")
        current = self.observe(target, tuple(expected.fields))
        _check(current.to_dict() == expected.to_dict(), "CRD recovery CAS state mismatch")
        obj = self._get(item, target.kind, target.name)
        _check(obj is not None and obj["metadata"]["uid"] == expected.uid, "CRD UID changed before delete")
        # resourceVersion protects fields/ownership checked immediately above.
        checked = self.observe(target, tuple(expected.fields))
        _check(checked.to_dict() == expected.to_dict(), "CRD changed before conditional delete")
        options = {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions":
                   {"uid": expected.uid, "resourceVersion": obj["metadata"]["resourceVersion"]}}
        path = "/apis/chaos-mesh.org/v1alpha1/namespaces/" + target.scope + "/" + _CRD_PLURALS[target.kind] + "/" + target.name
        self._run(item, "delete_crd_preconditioned", ("delete", "--raw", path, "-f", "-"), stdin=canonical_json(options))
        self._run(item, "wait_crd_deleted", ("wait", _resource(target.kind) + "/" + target.name,
                  "--for=delete", "--timeout=" + str(self.wait_timeout_s) + "s"))
        self._pods(item, require_original_images=True)


class PrimitiveExecutor:
    def __init__(self, journal: AttemptJournal, adapter: KubernetesAdapter) -> None:
        _check(adapter.journal is journal, "adapter belongs to another journal")
        self.journal, self.adapter = journal, adapter

    def apply(self, prepared: PreparedFault, *, intent_id: str) -> dict[str, Any]:
        if prepared.mode == "gateway_prepare_only":
            raise UnsupportedPrimitive("gateway execution blocked: direct file copy/reload has no proven atomic CAS or exclusive owner lock")
        if prepared.mode == "chaos_crd":
            try:
                self.adapter.preflight_crd(prepared)
            except Exception:
                self.journal.mark_failed("primitive_preflight_failed")
                raise
        self.journal.execute_change(intent_id=intent_id, fault_instance_id=prepared.spec.fault_instance_id,
            target=prepared.target, desired_fields=prepared.desired_fields, adapter=self.adapter)
        result = self.verify(prepared)
        if result["admission_spec_matches"] is False:
            self.journal.mark_failed("unexpected_crd_admission")
            raise PrimitiveError("actual CRD admission changed full spec; attempt failed, owned cleanup required")
        return result

    def verify(self, prepared: PreparedFault) -> dict[str, Any]:
        try:
            state = self.adapter.observe(prepared.target, tuple(prepared.desired_fields))
            if prepared.mode == "chaos_crd":
                self.adapter._pods(prepared, require_original_images=False)
            _check(state.exists and state.fields == prepared.desired_fields
                   and state.ownership == self.journal.metadata["ownership"], "primitive controlled state no longer matches")
            return {"fault_instance_id": prepared.spec.fault_instance_id,
                    "controlled_state_matches": state.exists and state.fields == prepared.desired_fields
                        and state.ownership == self.journal.metadata["ownership"],
                    "state_sha256": canonical_sha256(state.to_dict()), "evidence_kind": self.adapter.evidence_kind,
                    "admission_spec_matches": self.adapter.admission_matches(prepared) if prepared.mode == "chaos_crd" else None,
                    "injection_effectiveness": "not_assessed", "physical_recovery": "not_assessed"}
        except Exception:
            self.journal.mark_failed("primitive_verify_failed")
            raise

    def recover(self):
        return self.journal.reconcile(self.adapter)


def rewrite_gateway_config(text: str, *, directive: str, value: str, expected_sha256: str) -> dict[str, Any]:
    """Pure exact-byte edit of one directive in one simple location / block.

    Reject includes, ambiguous inherited directives and old catalog-bad routes.
    This does not execute nginx, acquire a live file lock or certify live CAS.
    """
    _check(isinstance(text, str) and hashlib.sha256(text.encode("utf-8")).hexdigest() == expected_sha256, "gateway source hash mismatch")
    _check(directive in {"proxy_read_timeout", "proxy_next_upstream"}, "gateway directive not permitted")
    if directive == "proxy_read_timeout":
        _check(isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*ms", value) is not None, "timeout requires positive integer ms")
    else:
        _check(value == "off", "retry edit permits only off")
    pattern = re.compile(r'#[^\r\n]*|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|[{};]|[^\s{};#]+')
    tokens = [(m.group(), m.start(), m.end()) for m in pattern.finditer(text) if not m.group().startswith("#")]
    _check(not any("catalog-bad" in token.lower() for token, _, _ in tokens), "legacy slow catalog-bad route forbidden")
    stack = []
    statement = []
    locations = []
    candidates = []
    all_occurrences = 0
    for token, start, end in tokens:
        if token == "{":
            header = tuple(t[0] for t in statement)
            stack.append(header)
            if header == ("location", "/"):
                locations.append({"open_end": end, "path": tuple(stack)})
            statement = []
        elif token == "}":
            _check(bool(stack) and not statement, "gateway brace/statement malformed")
            stack.pop()
        elif token == ";":
            _check(bool(statement), "empty gateway directive")
            _check(statement[0][0] != "include", "gateway includes require separate frozen configuration closure")
            if statement[0][0] == directive:
                all_occurrences += 1
                if stack and stack[-1] == ("location", "/"):
                    _check(len(statement) >= 2, "gateway directive has no value")
                    candidates.append((statement[1][1], statement[-1][2]))
            statement = []
        else:
            _check(not any(q in token for q in ("\"", "'")) or (token[0] == token[-1] and token[0] in "\"'"), "gateway quoting malformed")
            statement.append((token, start, end))
    _check(not stack and not statement and len(locations) == 1, "gateway location scope missing/ambiguous")
    if candidates:
        _check(len(candidates) == 1 and all_occurrences == 1, "gateway directive scope ambiguous")
        start, end = candidates[0]
        rewritten = text[:start] + value + text[end:]
    else:
        _check(directive == "proxy_next_upstream" and all_occurrences == 0, "required timeout directive missing/inherited")
        start = locations[0]["open_end"]
        end = start
        rewritten = text[:start] + "\n        proxy_next_upstream off;" + text[start:]
    _check(rewritten != text, "gateway directive already equals injection value")
    return {"directive": directive, "source_sha256": expected_sha256,
            "result_sha256": hashlib.sha256(rewritten.encode("utf-8")).hexdigest(),
            "source_changed_span": [start, end], "text": rewritten, "execution_supported": False,
            "pending": "attempt-owned ConfigMap plus UID/RV volume CAS and actual nginx -T readback"}
