"""Journal-bound auxiliary lifetimes, initially restricted to typed fake adapters.

This module deliberately has no runner dependency. It is not a live Kubernetes
adapter. The runner owns the physical lease and supplies its durable identity.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any
import uuid

from . import journal as j, primitives as p, pricing_route as pr

SCHEMA = "rq4-collect/auxiliary-session-v1"
STEP_SCHEMA = "rq4-collect/auxiliary-step-v1"


class AuxiliaryError(RuntimeError):
    pass


def require(ok, message):
    if not ok:
        raise AuxiliaryError(message)


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def clone(value):
    return json.loads(encoded(value))


@dataclass(frozen=True)
class AuxiliaryPlan:
    aux_id: str
    adapter_kind: str
    fault_instance_ids: tuple[str, ...]
    target: dict[str, str]
    expected_original: dict[str, Any]
    desired_fields: dict[str, Any]
    terminal_host_fault_ids: tuple[str, ...] = ()
    
    # the aux target Deployment but control a DISJOINT face (StressChaos CRD
    # pod CPU vs the aux's Deployment env write). NOT a subset of
    # fault_instance_ids -- coexisting legs are unaffected by the aux; the
    # runner overlap gate owns the eligibility check against the contract.
    coexisting_fault_ids: tuple[str, ...] = ()

    def __post_init__(self):
        require(isinstance(self.aux_id, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,39}", self.aux_id), "invalid aux_id")
        require(self.adapter_kind in {"fake_create", "fake_patch", pr.ADAPTER_KIND}, "unsupported auxiliary adapter kind")
        require(bool(self.fault_instance_ids) and len(set(self.fault_instance_ids)) == len(self.fault_instance_ids), "unique affected faults required")
        require(type(self.target) is dict and set(self.target) == {"kind", "name", "scope"}
                and all(type(v) is str and v for v in self.target.values()), "explicit auxiliary target required")
        require(type(self.desired_fields) is dict and bool(self.desired_fields), "controlled projection required")
        validate_observation(self.expected_original)
        require(self.expected_original["exists"] == (self.adapter_kind in {"fake_patch", pr.ADAPTER_KIND}), "create/patch precondition mismatch")
        require(not self.expected_original.get("owner"), "foreign auxiliary ownership")
        require(set(self.terminal_host_fault_ids) <= set(self.fault_instance_ids), "terminal fault outside plan")
        require(all(isinstance(v, str) and v for v in self.coexisting_fault_ids)
                and len(set(self.coexisting_fault_ids)) == len(self.coexisting_fault_ids),
                "coexisting fault ids must be unique non-empty strings")
        if self.adapter_kind == pr.ADAPTER_KIND:
            pr.model._check_original(self.expected_original)
            expected_desired = clone(self.expected_original["fields"])
            expected_desired["route_env"] = pr.model._route(pr.model.GATEWAY_URL)
            require(self.target == {"kind": "Deployment", "name": "pricing", "scope": pr.model.NAMESPACE}
                    and self.desired_fields == expected_desired and not self.terminal_host_fault_ids, "pricing route plan exceeds supported scope")
        # Freeze nested caller inputs; later mutation is additionally detected by
        # the manifest hash at every transaction operation.
        object.__setattr__(self, "target", clone(self.target))
        object.__setattr__(self, "expected_original", clone(self.expected_original))
        object.__setattr__(self, "desired_fields", clone(self.desired_fields))

    def to_dict(self):
        # coexisting_fault_ids is emitted only when non-empty so pre-ln persisted
        # AUX-PLAN dicts rebuild byte-for-byte equal under plan.to_dict()
        
        value = {"aux_id": self.aux_id, "adapter_kind": self.adapter_kind,
                 "fault_instance_ids": list(self.fault_instance_ids), "target": clone(self.target),
                 "expected_original": clone(self.expected_original), "desired_fields": clone(self.desired_fields),
                 "desired_shape_sha256": digest(self.desired_fields),
                 "terminal_host_fault_ids": list(self.terminal_host_fault_ids)}
        if self.coexisting_fault_ids:
            value["coexisting_fault_ids"] = list(self.coexisting_fault_ids)
        return value


def absent():
    return {"exists": False, "uid": None, "resource_version": None, "owner": None,
            "fields": {}, "descendants": [], "conflicts": []}


def validate_observation(value):
    require(type(value) is dict and set(value) == set(absent()), "auxiliary observation fields invalid")
    require(type(value["exists"]) is bool and type(value["fields"]) is dict
            and type(value["descendants"]) is list and type(value["conflicts"]) is list, "auxiliary observation invalid")
    if value["exists"]:
        require(all(type(value[k]) is str and value[k] for k in ("uid", "resource_version")), "resource identity missing")
        require(value["owner"] is None or type(value["owner"]) is str, "owner invalid")
    else:
        require(value == absent(), "absent auxiliary has residual descendants/conflicts")
    encoded(value)


class FakeAuxiliaryAdapter:
    """Concrete, synthetic, in-memory CAS adapter; never runs external commands.

    Separate object and descendant inventories permit replacement/orphan tests.
    `fault_objects` is the fake Kubernetes client's actual resource dictionary,
    so terminal CRD observations do not rely on success flags or old receipts.
    """
    evidence_kind = "synthetic"

    def __init__(self, resources=None, *, fault_objects=None):
        self.resources = {} if resources is None else resources
        self.fault_objects = {} if fault_objects is None else fault_objects
        self.mutations = []
        self.fail_after_setup = False
        self.fail_after_cleanup = False

    def key(self, plan):
        return (plan.target["kind"], plan.target["name"], plan.target["scope"])

    def read(self, plan):
        value = clone(self.resources.get(self.key(plan), absent()))
        validate_observation(value)
        require(not value["conflicts"], "auxiliary replacement/conflict inventory nonempty")
        require(not value["descendants"], "fake auxiliary slice does not support descendants")
        return value

    def setup(self, plan, expected, owner):
        require(self.read(plan) == expected, "setup CAS failed")
        value = {"exists": True, "uid": expected["uid"] or uuid.uuid4().hex,
                 "resource_version": uuid.uuid4().hex, "owner": owner,
                 "fields": clone(plan.desired_fields), "descendants": [], "conflicts": []}
        self.resources[self.key(plan)] = value
        self.mutations.append(("setup", plan.aux_id))
        if self.fail_after_setup:
            raise AuxiliaryError("synthetic setup reply lost")

    def cleanup(self, plan, expected, original):
        require(self.read(plan) == expected, "cleanup CAS failed")
        if original["exists"]:
            restored = clone(original)
            restored["resource_version"] = uuid.uuid4().hex
            self.resources[self.key(plan)] = restored
        else:
            self.resources.pop(self.key(plan), None)
        self.mutations.append(("cleanup", plan.aux_id))
        if self.fail_after_cleanup:
            raise AuxiliaryError("synthetic cleanup reply lost")


def original_matches(actual, original):
    return all(actual[k] == original[k] for k in actual if k != "resource_version")


def read_artifact(journal, artifact_id):
    rows = [row["payload"] if row["event"] == "artifact_complete" else row["payload"]["expected"]
            for row in journal.records() if row["event"] in {"artifact_complete", "artifact_recovered_complete"}
            and row["payload"].get("artifact_id") == artifact_id]
    require(len(rows) == 1, "auxiliary artifact missing or duplicated: " + artifact_id)
    receipt = rows[0]
    path = journal.path / receipt["relative_path"]
    require(path.resolve() == path and path.is_file(), "auxiliary artifact redirected/missing")
    raw = path.read_bytes()
    require(len(raw) == receipt["bytes"] and hashlib.sha256(raw).hexdigest() == receipt["sha256"], "auxiliary artifact bytes mismatch")
    return json.loads(raw), receipt


def write_artifact(journal, artifact_id, value):
    return journal.write_artifact(artifact_id=artifact_id, relative_path="artifacts/auxiliary/" + artifact_id + ".json",
                                  raw=encoded(value), media_type="application/json")


def validate_adapters(plans, adapters, mode):
    require(mode in {"isolated_test", "controlled_pilot"} and set(adapters) == {plan.aux_id for plan in plans}, "auxiliary mode/adapter set invalid")
    kind = "observed" if mode == "controlled_pilot" else "synthetic"
    for plan in plans:
        adapter = adapters[plan.aux_id]
        if plan.adapter_kind == pr.ADAPTER_KIND:
            require(type(adapter) is pr.PricingRouteAdapter and adapter.evidence_kind == kind, "pricing route mode/provenance mismatch")
            adapter.validate_transport()
        else:
            require(mode == "isolated_test" and type(adapter) is FakeAuxiliaryAdapter, "only concrete fake auxiliary adapters supported in isolated mode")


class AuxiliaryTransaction:
    """Verified manifest and append-only setup/cleanup proof chain."""
    def __init__(self, journal, declaration, plans, adapters):
        require(type(declaration) is dict and declaration.get("required") is True, "persistent auxiliary declaration required")
        require(all(type(plan) is AuxiliaryPlan for plan in plans) and bool(plans), "typed nonempty auxiliary plans required")
        self.journal, self.declaration, self.plans, self.adapters = journal, clone(declaration), tuple(plans), dict(adapters)
        self.manifest, receipt = read_artifact(journal, declaration.get("artifact_id"))
        validate_adapters(plans, adapters, self.manifest.get("mode"))
        self.evidence_kind = "observed" if self.manifest["mode"] == "controlled_pilot" else "synthetic"
        require(all(receipt.get(key) == declaration.get(key) for key in ("artifact_id", "relative_path", "sha256", "bytes")), "auxiliary manifest declaration mismatch")
        context = journal.contract.context.to_dict()
        require(self.manifest.get("schema_version") == SCHEMA
                and self.manifest.get("contract_sha256") == journal.contract.sha256
                and all(self.manifest.get(k) == context[k] for k in ("run_id", "attempt_id", "output_root", "evidence_root"))
                and self.manifest.get("mode") in {"isolated_test", "controlled_pilot"}, "auxiliary manifest identity/mode mismatch")
        require(self.manifest.get("plans") == [p.to_dict() for p in plans], "auxiliary plans differ from durable manifest")

    def verify_manifest(self, *, allow_pending=False):
        validate_adapters(self.plans, self.adapters, self.manifest["mode"])
        manifest, receipt = read_artifact(self.journal, self.declaration["artifact_id"])
        require(receipt["sha256"] == self.declaration["sha256"] and manifest == self.manifest
                and manifest["plans"] == [p.to_dict() for p in self.plans], "auxiliary manifest changed")
        if not allow_pending:
            require(not any(aid.startswith("aux-") for aid in self.journal.pending_artifact_ids()), "auxiliary pending artifact blocks mutation")

    def owner(self, plan):
        return self.journal.metadata["ownership"] + ":" + plan.aux_id

    def step(self, plan, action):
        prefix = "aux-" + plan.aux_id + "-" + action
        ids = [row["payload"].get("artifact_id") for row in self.journal.records()
               if row["event"] in {"artifact_complete", "artifact_recovered_complete"}
               and (row["payload"].get("artifact_id") == prefix or re.fullmatch(re.escape(prefix) + r"-[0-9]{6}", row["payload"].get("artifact_id", "")))]
        if not ids:
            return None
        require(len(ids) == 1, "competing completed auxiliary proof chains")
        value, _ = read_artifact(self.journal, ids[0])
        require(value.get("schema_version") == STEP_SCHEMA and value.get("aux_id") == plan.aux_id
                and value.get("action") == action and value.get("manifest_sha256") == self.declaration["sha256"]
                and value.get("owner") == self.owner(plan) and value.get("evidence_kind") == self.evidence_kind, "auxiliary step identity mismatch")
        if ids[0] != prefix:
            rows = self.journal.records()
            lineage = [row["payload"]["artifact_id"] for row in rows if row["event"] == "artifact_intent"
                       and (row["payload"]["artifact_id"] == prefix or re.fullmatch(re.escape(prefix) + r"-[0-9]{6}", row["payload"]["artifact_id"]))]
            index = lineage.index(ids[0])
            prior = lineage[:index]
            states = self.journal._artifact_states(rows)
            require(ids[0] == prefix + "-%06d" % (index + 1)
                    and value.get("step_sequence") == index + 1 and value.get("superseded_artifact_ids") == prior
                    and all(states[aid]["abandoned"] and not states[aid]["complete"] for aid in prior),
                    "auxiliary step sequence/superseded disposition mismatch")
        else:
            require("step_sequence" not in value and "superseded_artifact_ids" not in value, "legacy auxiliary step schema mismatch")
        return value

    def record(self, plan, action, **fields):
        self.verify_manifest()
        require(not set(fields) & {"schema_version", "aux_id", "action", "manifest_sha256", "owner", "evidence_kind", "step_sequence", "superseded_artifact_ids"},
                "auxiliary step reserved field override")
        prefix = "aux-" + plan.aux_id + "-" + action
        prior = [row["payload"]["artifact_id"] for row in self.journal.records() if row["event"] == "artifact_intent"
                 and (row["payload"]["artifact_id"] == prefix or re.fullmatch(re.escape(prefix) + r"-[0-9]{6}", row["payload"]["artifact_id"]))]
        artifact_id = prefix + "-%06d" % (len(prior) + 1)
        value = {"schema_version": STEP_SCHEMA, "aux_id": plan.aux_id, "action": action,
                 "manifest_sha256": self.declaration["sha256"], "owner": self.owner(plan),
                 "evidence_kind": self.evidence_kind, "step_sequence": len(prior) + 1,
                 "superseded_artifact_ids": prior, **fields}
        write_artifact(self.journal, artifact_id, value)
        return value

    def step_receipt(self, plan, action):
        selected = self.step(plan, action)
        require(selected is not None, "auxiliary logical intent missing")
        prefix = "aux-" + plan.aux_id + "-" + action
        for row in self.journal.records():
            if row["event"] not in {"artifact_complete", "artifact_recovered_complete"}:
                continue
            aid = row["payload"].get("artifact_id", "")
            if aid == prefix or re.fullmatch(re.escape(prefix) + r"-[0-9]{6}", aid):
                value, receipt = read_artifact(self.journal, aid)
                if value == selected:
                    return receipt
        raise AuxiliaryError("auxiliary logical receipt missing")

    def verify_owned(self, plan, current, intent):
        require(current["exists"] and current["owner"] == self.owner(plan)
                and current["fields"] == plan.desired_fields and not current["conflicts"], "auxiliary current owner/shape mismatch")
        original = intent["original"]
        if original["exists"]:
            require(current["uid"] == original["uid"], "auxiliary original UID replaced")
        complete = self.step(plan, "setup-complete")
        if complete:
            require(complete["intent_sha256"] == digest(intent) and current["uid"] == complete["observed"]["uid"], "auxiliary setup receipt/UID mismatch")

    def setup(self):
        self.verify_manifest()
        require(not any(self.step(plan, "setup-intent") for plan in self.plans), "auxiliary setup cannot be repeated")
        for plan in self.plans:
            adapter = self.adapters[plan.aux_id]
            original = adapter.read(plan)
            route = type(adapter) is pr.PricingRouteAdapter
            require(original_matches(original, plan.expected_original) if route else original == plan.expected_original, "auxiliary original precondition mismatch")
            intent = self.record(plan, "setup-intent", original=original, original_sha256=digest(original),
                                 desired_fields=plan.desired_fields, target=plan.target, restore_policy="exact_original")
            if route:
                adapter.setup(plan, original, self.owner(plan), logical_intent_ref=self.step_receipt(plan, "setup-intent"))
            else:
                adapter.setup(plan, original, self.owner(plan))
            observed = adapter.read(plan)
            self.verify_owned(plan, observed, intent)
            self.record(plan, "setup-complete", intent_sha256=digest(intent), observed=observed,
                        **({"readback": adapter.last_readback} if route else {}))

    def active_environment(self, raw):
        self.verify_manifest()
        owners, route_rows = [], []
        for plan in self.plans:
            intent, complete = self.step(plan, "setup-intent"), self.step(plan, "setup-complete")
            require(intent is not None and complete is not None and self.step(plan, "cleanup-complete") is None,
                    "auxiliary is not a verified active resource")
            adapter = self.adapters[plan.aux_id]
            observed = adapter.read(plan)
            self.verify_owned(plan, observed, intent)
            if type(adapter) is pr.PricingRouteAdapter:
                route_rows.append((adapter, observed, self.owner(plan)))
            owners.append(self.owner(plan))
        if route_rows:
            require(len(route_rows) == 1 and len(self.plans) == 1, "pricing residual admission supports one route auxiliary")
            adapter, observed, owner = route_rows[0]
            require(adapter.active_inventory(raw["residual_owners"], observed, owner)["ok"], "pricing residual object identity mismatch")
        else:
            require(set(raw["residual_owners"]) == set(owners), "auxiliary/raw residual owner inventory mismatch")
        return clone(raw)  # Raw inventory remains visible; it is never filtered.

    def cleanup(self):
        self.verify_manifest()
        for plan in reversed(self.plans):
            adapter = self.adapters[plan.aux_id]
            route = type(adapter) is pr.PricingRouteAdapter
            setup = self.step(plan, "setup-intent")
            if setup is None:
                current = adapter.read(plan)
                require(original_matches(current, plan.expected_original) if route else current == plan.expected_original, "unattempted auxiliary precondition changed")
                setup = self.record(plan, "setup-intent", original=current, original_sha256=digest(current),
                                    desired_fields=plan.desired_fields, target=plan.target, restore_policy="exact_original", never_applied=True)
            original = setup["original"]
            require(digest(original) == setup["original_sha256"], "auxiliary original snapshot mismatch")
            current = adapter.read(plan)
            completed = self.step(plan, "cleanup-complete")
            if completed:
                require(original_matches(current, original), "cleaned auxiliary changed/reappeared")
                continue
            intent = self.step(plan, "cleanup-intent")
            if intent is None:
                if not original_matches(current, original):
                    self.verify_owned(plan, current, setup)
                intent = self.record(plan, "cleanup-intent", original=original, original_sha256=digest(original),
                                     setup_intent_sha256=digest(setup), expected=current, target=plan.target)
            require(intent["setup_intent_sha256"] == digest(setup), "cleanup intent chain mismatch")
            if not original_matches(current, original):
                self.verify_owned(plan, current, setup)
                require(original_matches(current, intent["expected"]) if route else current == intent["expected"], "cleanup current CAS projection changed")
                if route:
                    adapter.cleanup(plan, current, original, logical_intent_ref=self.step_receipt(plan, "cleanup-intent"))
                else:
                    adapter.cleanup(plan, current, original)
            observed = adapter.read(plan)
            require(original_matches(observed, original), "auxiliary restoration unverified")
            self.record(plan, "cleanup-complete", intent_sha256=digest(intent), observed=observed,
                        **({"readback": adapter.last_readback} if route else {}))

    def verify_terminal(self):
        self.verify_manifest()
        evidence = []
        for plan in self.plans:
            setup, intent, complete = (self.step(plan, name) for name in ("setup-intent", "cleanup-intent", "cleanup-complete"))
            require(all(value is not None for value in (setup, intent, complete)), "auxiliary cleanup proof incomplete")
            require(intent["setup_intent_sha256"] == digest(setup) and complete["intent_sha256"] == digest(intent), "auxiliary cleanup proof chain mismatch")
            observed = self.adapters[plan.aux_id].read(plan)
            require(original_matches(observed, setup["original"])
                    and original_matches(complete["observed"], setup["original"]), "auxiliary terminal state mismatches original")
            evidence.append({"aux_id": plan.aux_id, "observed": observed, "cleanup_sha256": digest(complete),
                             **({"readback": self.adapters[plan.aux_id].last_readback} if type(self.adapters[plan.aux_id]) is pr.PricingRouteAdapter else {})})
        return evidence


class TerminalHostObservationAdapter:
    """Read-only terminal proof for one explicitly declared host CPU CRD.

    Activated by the runner's explicit dispatcher after auxiliary cleanup.
    No synthetic Pod/image/readiness state is returned.
    """
    evidence_kind = "fake"

    def __init__(self, transaction, plan, prepared):
        require(type(transaction) is AuxiliaryTransaction and type(plan) is AuxiliaryPlan, "typed terminal proof required")
        require(type(prepared) is p.PreparedFault, "concrete prepared host primitive required")
        spec = prepared.spec
        require(spec.fault_instance_id in plan.terminal_host_fault_ids
                and spec.to_dict()["normalized_root_entity"] == "host"
                and spec.to_dict()["fault_type"] == "host_cpu_saturation"
                and prepared.mode == "chaos_crd" and not plan.expected_original["exists"]
                and prepared.binding.deployment == plan.target["name"]
                and any(f.to_dict() == spec.to_dict() for f in transaction.journal.contract.faults),
                "terminal target is not declared host CPU carrier")
        self.transaction, self.plan, self.prepared = transaction, plan, prepared

    def observe(self, target, field_names):
        require(target == self.prepared.target, "foreign terminal target")
        self.transaction.verify_terminal()
        adapter = self.transaction.adapters[self.plan.aux_id]
        require((target.kind, target.name) not in adapter.fault_objects, "terminal fault CRD reappeared")
        require(not adapter.read(self.plan)["exists"], "terminal carrier reappeared")
        pin = self.prepared.binding
        require(("Deployment", pin.deployment) not in adapter.fault_objects, "terminal actual carrier Deployment remains")
        for pod in pin.pods:
            require(("Pod", pod.name) not in adapter.fault_objects, "terminal original Pod remains/replaced")
        for (kind, _), obj in adapter.fault_objects.items():
            if kind not in {"Pod", "ReplicaSet"}:
                continue
            metadata = obj.get("metadata", {})
            selected = all(metadata.get("labels", {}).get(k) == v for k, v in pin.selector)
            descendant = any(row.get("uid") == pin.deployment_uid for row in metadata.get("ownerReferences", []))
            require(not selected and not descendant, "terminal matching/orphan descendant remains")
        return j.ResourceState(False, None, None, {})

    def compare_and_apply(self, *args, **kwargs):
        raise AuxiliaryError("terminal host adapter cannot apply")

    def compare_and_restore(self, *args, **kwargs):
        raise AuxiliaryError("terminal host adapter cannot restore")
