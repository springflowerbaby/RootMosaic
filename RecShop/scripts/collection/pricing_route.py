"""Concrete bounded pricing route auxiliary; no lease acquisition or release.

Only the reviewed DIRECT/replicas=1/NACOS=false baseline is supported. Raw
Kubernetes documents remain in memory; persisted evidence is projected/redacted.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import inspect
import json
import math
import os
import re
from pathlib import Path
import time

from . import journal as j, pricing_route_model as model
from .live_runtime import BoundedProcess

ADAPTER_KIND = "pricing_route_cas_v1"
SCHEMA = "rq4-collect/pricing-route-adapter-v1"


class PricingRouteError(RuntimeError):
    pass


def need(condition, code):
    if not condition:
        raise PricingRouteError(code)


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def sources():
    return {"pricing_route": sha(Path(__file__).read_bytes()), "pricing_route_model": sha(Path(model.__file__).read_bytes()),
            "live_runtime": sha(Path(inspect.getfile(BoundedProcess)).read_bytes())}


@dataclass(frozen=True)
class RoutePolicy:
    command_timeout_s: float = 15.0
    settle_timeout_s: float = 120.0
    poll_interval_s: float = 1.0
    max_output_bytes: int = 2_000_000
    cleanup_cas_attempts: int = 3

    def __post_init__(self):
        need(all(type(v) in (int, float) and math.isfinite(v) and v > 0
                 for v in (self.command_timeout_s, self.settle_timeout_s, self.poll_interval_s)), "INVALID_ROUTE_TIME_BUDGET")
        need(type(self.max_output_bytes) is int and 0 < self.max_output_bytes <= 4_000_000
             and type(self.cleanup_cas_attempts) is int and 1 <= self.cleanup_cas_attempts <= 3, "INVALID_ROUTE_BOUND")


class PricingSessionAuthority:
    """Concrete current-lease binding, checked again immediately before patch."""
    def __init__(self, lease, journal):
        self.lease, self.journal = lease, journal
        self.lock_identity = (os.fstat(lease._fd).st_dev, os.fstat(lease._fd).st_ino)
        self.contract_sha256 = journal.contract.sha256

    def verify(self, declaration):
        lease, journal = self.lease, self.journal
        need(not lease._closed and not journal._closed and lease.state.get("status") == "dirty", "ROUTE_SESSION_CLOSED_OR_CLEAN")
        need(journal.contract.sha256 == self.contract_sha256 == lease.contract.sha256
             and json.loads(lease.state_path.read_bytes()) == lease.state
             and lease.state.get("auxiliary") == declaration, "ROUTE_LEASE_BINDING_CHANGED")
        current = os.fstat(lease._fd)
        on_disk = lease.lock_path.stat()
        need((current.st_dev, current.st_ino) == self.lock_identity == (on_disk.st_dev, on_disk.st_ino), "ROUTE_PHYSICAL_LOCK_CHANGED")
        need(declaration.get("activation") == "READY_FOR_SETUP", "ROUTE_SETUP_NOT_AUTHORIZED")


class PricingRouteAdapter:
    def __init__(self, context, namespace, cluster_uid, namespace_uid, *, process,
                 evidence_kind="synthetic", policy=None, kubectl="kubectl", cleanup_only=False):
        need(all(type(v) is str and v and not any(x in v for x in "\r\n\0")
                 for v in (context, namespace, cluster_uid, namespace_uid, kubectl)), "INVALID_ROUTE_IDENTITY")
        need(len(namespace) <= 63 and re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", namespace)
             and evidence_kind in {"synthetic", "observed"}, "UNSUPPORTED_ROUTE_SCOPE")
        if evidence_kind == "observed":
            need(type(process) is BoundedProcess and process.execute is True, "OBSERVED_ROUTE_REQUIRES_ENABLED_BOUNDED_PROCESS")
        else:
            need(getattr(process, "evidence_kind", None) == "synthetic" and callable(getattr(process, "run", None)), "SYNTHETIC_ROUTE_TRANSPORT_REQUIRED")
        
        # RECOVERY-DRIFT-AUDIT): binds under the current tree without the
        # source-fingerprint clause. Contract/mode/policy/environment/terminal
        # verifications are unchanged.
        need(type(cleanup_only) is bool, "ROUTE_CLEANUP_FLAG_TYPED_BOOL_REQUIRED")
        self.cleanup_only = cleanup_only
        self.context, self.namespace, self.cluster_uid, self.namespace_uid = context, namespace, cluster_uid, namespace_uid
        self.process, self.evidence_kind, self.policy, self.kubectl = process, evidence_kind, policy or RoutePolicy(), kubectl
        need(type(self.policy) is RoutePolicy, "TYPED_ROUTE_POLICY_REQUIRED")
        self.journal = self.declaration = self.aux_id = self.authority = None
        self.last_readback, self._document, self._commands = None, None, []

    def bind(self, journal, declaration, aux_id, authority):
        need(isinstance(journal, j.AttemptJournal) and type(authority) is PricingSessionAuthority
             and authority.journal is journal, "TYPED_ROUTE_SESSION_REQUIRED")
        self.journal, self.declaration, self.aux_id, self.authority = journal, declaration, aux_id, authority
        self.authority.verify(declaration)
        manifest = self._artifact(declaration)[0]
        need(manifest["contract_sha256"] == journal.contract.sha256
             and manifest["mode"] == ("controlled_pilot" if self.evidence_kind == "observed" else "isolated_test"),
             "ROUTE_MANIFEST_SOURCE_MODE_MISMATCH")
        if not self.cleanup_only:
            need(all(manifest["source_fingerprints"].get(k) == v for k, v in sources().items()),
                 "ROUTE_MANIFEST_SOURCE_MODE_MISMATCH")
        need(manifest.get("adapter_policies", {}).get(aux_id) == asdict(self.policy), "ROUTE_POLICY_DIFFERS_FROM_MANIFEST")
        context = journal.contract.context.to_dict()
        need(context["namespace"] == self.namespace and context["kube_context"] == self.context
             and manifest["cluster_uid"] == self.cluster_uid and manifest["namespace_uid"] == self.namespace_uid
             and authority.lease.identity.cluster_uid == self.cluster_uid and authority.lease.identity.namespace_uid == self.namespace_uid
             and authority.lease.mode == manifest["mode"],
             "ROUTE_ENVIRONMENT_BINDING_MISMATCH")

    def validate_transport(self):
        if self.evidence_kind == "observed":
            need(type(self.process) is BoundedProcess and self.process.execute is True, "OBSERVED_ROUTE_REQUIRES_ENABLED_BOUNDED_PROCESS")
        else:
            need(self.evidence_kind == "synthetic" and getattr(self.process, "evidence_kind", None) == "synthetic", "SYNTHETIC_ROUTE_TRANSPORT_REQUIRED")

    def _artifact(self, reference):
        need(self.journal is not None, "ROUTE_JOURNAL_NOT_BOUND")
        candidates = []
        for row in self.journal.records():
            if row["payload"].get("artifact_id") != reference["artifact_id"]:
                continue
            if row["event"] == "artifact_complete":
                candidates.append(row["payload"])
            elif row["event"] == "artifact_recovered_complete":
                candidates.append(row["payload"]["expected"])
        need(len(candidates) == 1, "ROUTE_INTENT_NOT_COMPLETE")
        receipt = candidates[0]
        need(all(receipt[k] == reference[k] for k in ("artifact_id", "relative_path", "sha256", "bytes")), "ROUTE_RECEIPT_MISMATCH")
        path = self.journal.path / receipt["relative_path"]
        need(path.resolve() == path and path.is_file() and receipt["bytes"] <= self.policy.max_output_bytes, "ROUTE_ARTIFACT_PATH_INVALID")
        with path.open("rb") as stream:
            raw = stream.read(receipt["bytes"] + 1)
        need(len(raw) == receipt["bytes"] and sha(raw) == receipt["sha256"], "ROUTE_ARTIFACT_BYTES_CHANGED")
        return json.loads(raw), receipt

    def _run(self, args, deadline):
        self.validate_transport()
        remaining = deadline - time.monotonic()
        need(remaining > 0, "ROUTE_READBACK_DEADLINE")
        argv = (self.kubectl, "--context", self.context, "--namespace", self.namespace, *args)
        began = time.time()
        try:
            result = self.process.run(argv, timeout_s=min(self.policy.command_timeout_s, remaining), max_output_bytes=self.policy.max_output_bytes)
        except BaseException:
            raise PricingRouteError("ROUTE_TRANSPORT_FAILURE") from None
        self._commands.append({"argv": list(argv), "started_at_epoch_s": began, "ended_at_epoch_s": time.time(),
                               "status": result.status, "return_code": result.return_code, "workers_joined": result.workers_joined,
                               "stdout_sha256": sha(result.stdout), "stderr_sha256": sha(result.stderr)})
        need(result.status == "ok" and result.return_code == 0 and result.workers_joined is True
             and time.monotonic() <= deadline, "ROUTE_COMMAND_FAILED_OR_UNKNOWN")
        return result.stdout

    def _get(self, kind, deadline, *, name=None, selector=False):
        need(kind in {"namespace", "deployment", "replicaset", "pod", "hpa"}, "ROUTE_READ_NOT_ALLOWED")
        args = ["get", kind]
        if name:
            args.append(name)
        if selector:
            args.extend(("-l", "app=pricing"))
        try:
            return json.loads(self._run((*args, "-o", "json"), deadline))
        except (ValueError, UnicodeError):
            raise PricingRouteError("ROUTE_JSON_INVALID") from None

    def _identity(self, deadline):
        for name, expected in (("kube-system", self.cluster_uid), (self.namespace, self.namespace_uid)):
            obj = self._get("namespace", deadline, name=name)
            metadata = obj.get("metadata", {})
            need(obj.get("kind") == "Namespace" and metadata.get("name") == name and metadata.get("uid") == expected
                 and not metadata.get("deletionTimestamp"), "ROUTE_NAMESPACE_IDENTITY_CHANGED")

    def _read(self, plan=None):
        deadline, first = time.monotonic() + self.policy.settle_timeout_s, len(self._commands)
        self._identity(deadline)
        while True:
            before = self._get("deployment", deadline, name=model.DEPLOYMENT)
            hpas = self._get("hpa", deadline).get("items")
            need(type(hpas) is list and len(hpas) <= 100, "ROUTE_SCALER_INVENTORY_INVALID")
            targets = [row.get("spec", {}).get("scaleTargetRef", {}).get("name") for row in hpas
                       if row.get("spec", {}).get("scaleTargetRef", {}).get("kind") == "Deployment"]
            need(model.DEPLOYMENT not in targets, "PRICING_SCALER_PRESENT")
            projected = model.deployment_projection(before, namespace=self.namespace)
            if plan is None:
                model.validate_baseline(before, namespace=self.namespace, hpa_target_names=targets)
            else:
                owner = self.journal.metadata["ownership"] + ":" + self.aux_id
                classification = model.classify_route_state(before, original=plan.expected_original, aux_owner=owner, namespace=self.namespace)
                need(classification["state"] in {"ORIGINAL", "OWNED_DESIRED"}, "ROUTE_SEMANTIC_CONFLICT")
            need(not projected["conflicts"], "ROUTE_CONFIGURATION_CONFLICT")
            route = projected["fields"]["route_env"]["entry"]["value"]
            replica_sets = self._get("replicaset", deadline, selector=True).get("items")
            pods = self._get("pod", deadline, selector=True).get("items")
            need(type(replica_sets) is list and len(replica_sets) <= 100 and type(pods) is list and len(pods) <= 100, "ROUTE_CHILD_INVENTORY_INVALID")
            runtime = {}
            statuses = pods[0].get("status", {}).get("containerStatuses", []) if len(pods) == 1 else []
            running = [row for row in statuses if row.get("name") == model.CONTAINER and row.get("ready") is True
                       and row.get("containerID") and row.get("imageID")]
            if len(pods) == 1 and not pods[0].get("metadata", {}).get("deletionTimestamp") and pods[0].get("status", {}).get("phase") == "Running" and len(running) == 1:
                name, uid = pods[0]["metadata"]["name"], pods[0]["metadata"]["uid"]
                try:
                    exact_before = self._get("pod", deadline, name=name)
                    if exact_before.get("metadata", {}).get("uid") == uid:
                        route_value = self._run(("exec", "pod/" + name, "--container", model.CONTAINER, "--", "printenv", model.ROUTE_ENV), deadline).decode("utf-8").strip()
                        nacos_value = self._run(("exec", "pod/" + name, "--container", model.CONTAINER, "--", "printenv", model.NACOS_ENV), deadline).decode("utf-8").strip()
                        exact_after = self._get("pod", deadline, name=name)
                        runtime = {"pod_name": name, "pod_uid_before": uid, "pod_uid_after": exact_after.get("metadata", {}).get("uid"),
                                   "container": model.CONTAINER, "return_codes": {"route": 0, "nacos": 0},
                                   model.ROUTE_ENV: route_value, model.NACOS_ENV: nacos_value}
                except PricingRouteError:
                    # Retry only a freshly demonstrated child replacement or
                    # non-running transition; an unexplained API failure stays
                    # fail-closed. The next loop rechecks Deployment semantics.
                    fresh = self._get("pod", deadline, selector=True).get("items")
                    need(type(fresh) is list and len(fresh) <= 100, "ROUTE_CHILD_INVENTORY_INVALID")
                    same = [row for row in fresh if row.get("metadata", {}).get("uid") == uid]
                    transitional = not same or any(row.get("metadata", {}).get("deletionTimestamp")
                                   or row.get("status", {}).get("phase") != "Running" for row in same)
                    need(transitional, "ROUTE_RUNTIME_READ_FAILED_WITHOUT_CHILD_TRANSITION")
                    time.sleep(min(self.policy.poll_interval_s, max(0, deadline - time.monotonic())))
                    continue
            after = self._get("deployment", deadline, name=model.DEPLOYMENT)
            decision = model.judge_rollout(before, after, replica_sets, pods, runtime,
                                           expected_uid=projected["uid"], expected_route=route, expected_owner=projected["owner"],
                                           expected_guard=projected["fields"]["outside_guard_sha256"], namespace=self.namespace)
            if decision["settled"]:
                self._identity(deadline)
                self._document = after
                self.last_readback = {"schema_version": "rq4-collect/pricing-route-readback-v1", "evidence_kind": self.evidence_kind,
                                      "environment": {"context": self.context, "namespace": self.namespace, "cluster_uid": self.cluster_uid, "namespace_uid": self.namespace_uid},
                                      "observed_at_epoch_s": time.time(), "projection": model.deployment_projection(after, namespace=self.namespace),
                                      "decision": decision, "runtime_env": runtime,
                                      "replicasets": [{"name": row["metadata"]["name"], "uid": row["metadata"]["uid"], "replicas": row["spec"]["replicas"]} for row in replica_sets],
                                      "commands": self._commands[first:]}
                return self.last_readback["projection"]
            need(time.monotonic() < deadline, "ROUTE_ROLLOUT_NOT_SETTLED")
            time.sleep(min(self.policy.poll_interval_s, max(0, deadline - time.monotonic())))

    def read_baseline(self):
        return self._read()

    def read(self, plan):
        if self.journal is None:
            current = self.read_baseline()
            need(model._semantic(current) == model._semantic(plan.expected_original), "UNBOUND_ROUTE_ORIGINAL_MISMATCH")
            return current
        manifest, _ = self._artifact(self.declaration)
        need(manifest.get("adapter_policies", {}).get(self.aux_id) == asdict(self.policy), "ROUTE_PLAN_OR_SOURCE_CHANGED")
        return self._read(plan)

    def active_inventory(self, rows, observed, owner):
        return model.match_active_inventory(rows, uid=observed["uid"], aux_owner=owner, namespace=self.namespace)

    def _logical(self, plan, reference, action):
        need(type(self.authority) is PricingSessionAuthority, "ROUTE_SESSION_NOT_BOUND")
        self.authority.verify(self.declaration)
        logical, _ = self._artifact(reference)
        need(logical.get("action") == action + "-intent" and logical.get("aux_id") == self.aux_id == plan.aux_id
             and logical.get("manifest_sha256") == self.declaration["sha256"]
             and logical.get("owner") == self.journal.metadata["ownership"] + ":" + self.aux_id
             and logical.get("evidence_kind") == self.evidence_kind, "ROUTE_LOGICAL_INTENT_MISMATCH")
        manifest, _ = self._artifact(self.declaration)
        need([row for row in manifest["plans"] if row["aux_id"] == plan.aux_id] == [plan.to_dict()]
             and all(manifest["source_fingerprints"].get(k) == v for k, v in sources().items())
             and manifest.get("adapter_policies", {}).get(plan.aux_id) == asdict(self.policy), "ROUTE_PLAN_OR_SOURCE_CHANGED")
        return logical

    def _cas(self, plan, action, logical_ref, expected, patch):
        logical = self._logical(plan, logical_ref, action)
        prefix = "pricing-cas-" + self.aux_id + "-" + action + "-"
        prior = [row["payload"]["artifact_id"] for row in self.journal.records() if row["event"] == "artifact_intent"
                 and row["payload"].get("artifact_id", "").startswith(prefix) and row["payload"]["artifact_id"].endswith("-intent")]
        need(action != "setup" or not prior, "ROUTE_SETUP_CANNOT_REPLAY")
        aid = prefix + "%06d" % (len(prior) + 1)
        value = {"schema_version": SCHEMA, "evidence_kind": self.evidence_kind, "action": action, "aux_id": plan.aux_id,
                 "contract_sha256": self.journal.contract.sha256, "manifest_sha256": self.declaration["sha256"],
                 "logical_intent_ref": logical_ref, "logical_intent_sha256": sha(encoded(logical)),
                 "expected": expected, "patch": patch, "patch_sha256": sha(encoded(patch)), "sources": sources(),
                 "policy": asdict(self.policy), "environment": {"context": self.context, "namespace": self.namespace,
                 "cluster_uid": self.cluster_uid, "namespace_uid": self.namespace_uid}}
        receipt = self.journal.write_artifact(artifact_id=aid + "-intent", relative_path="artifacts/auxiliary/" + aid + "-intent.json",
                                              raw=encoded(value), media_type="application/json")
        self._artifact(receipt)
        self.authority.verify(self.declaration)
        first = len(self._commands)
        failure = None
        try:
            self._run(("patch", "deployments.apps", model.DEPLOYMENT, "--type=json", "-p", encoded(patch).decode(), "-o", "json"),
                      time.monotonic() + self.policy.command_timeout_s)
        except PricingRouteError as exc:
            failure = str(exc)
        self.journal.write_artifact(artifact_id=aid + "-result", relative_path="artifacts/auxiliary/" + aid + "-result.json",
                                    raw=encoded({"intent_sha256": receipt["sha256"], "evidence_kind": self.evidence_kind,
                                                 "api_result": "UNKNOWN_OR_FAILED" if failure else "RETURNED_SUCCESS",
                                                 "commands": self._commands[first:]}), media_type="application/json")
        if failure:
            raise PricingRouteError(failure)

    def setup(self, plan, expected, owner, *, logical_intent_ref):
        logical = self._logical(plan, logical_intent_ref, "setup")
        need(owner == logical["owner"] and logical.get("desired_fields") == plan.desired_fields
             and logical.get("target") == plan.target, "ROUTE_SETUP_ARGUMENT_BINDING_MISMATCH")
        current = self.read(plan)
        need(model._semantic(current) == model._semantic(expected) == model._semantic(logical["original"]), "ROUTE_SETUP_SEMANTICS_CHANGED")
        patch = model.render_set_patch(self._document, expected=current, aux_owner=owner, namespace=self.namespace)
        self._cas(plan, "setup", logical_intent_ref, current, patch)
        verified = self.read(plan)
        need(verified["owner"] == owner and verified["fields"] == plan.desired_fields, "ROUTE_SETUP_READBACK_MISMATCH")

    def cleanup(self, plan, expected, original, *, logical_intent_ref):
        owner = self.journal.metadata["ownership"] + ":" + self.aux_id
        logical = self._logical(plan, logical_intent_ref, "cleanup")
        need(model._semantic(logical["original"]) == model._semantic(original), "ROUTE_RESTORE_ORIGINAL_CHANGED")
        previous = None
        for index in range(self.policy.cleanup_cas_attempts):
            current = self.read(plan)
            if model._semantic(current) == model._semantic(original):
                return
            need(current["owner"] == owner and current["fields"] == plan.desired_fields
                 and current["uid"] == original["uid"] and model._semantic(current) == model._semantic(expected), "ROUTE_RESTORE_SEMANTICS_CHANGED")
            if previous is not None:
                need(current["resource_version"] != previous["resource_version"], "ROUTE_RESTORE_UNCHANGED_RV_AFTER_FAILURE")
            patch = model.render_restore_patch(self._document, expected=current, original=original, aux_owner=owner, namespace=self.namespace)
            try:
                self._cas(plan, "cleanup", logical_intent_ref, current, patch)
            except PricingRouteError:
                previous = current
                if index + 1 == self.policy.cleanup_cas_attempts:
                    # A lost last reply may still have restored the original.
                    final = self.read(plan)
                    need(model._semantic(final) == model._semantic(original), "ROUTE_RESTORE_CAS_BUDGET_EXHAUSTED")
                    return
                continue
            verified = self.read(plan)
            need(model._semantic(verified) == model._semantic(original), "ROUTE_RESTORE_READBACK_MISMATCH")
            return
