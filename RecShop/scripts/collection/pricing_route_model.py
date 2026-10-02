"""Pure, bounded pricing-route model; no transport, persistence, or live claims.

Inputs are caller-supplied Kubernetes JSON. A future adapter must bind them to
fresh reads, durable intents and the canonical lease before using a rendered
patch. This module cannot acquire a lease, execute a patch, or certify recovery.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re

DEPLOYMENT = CONTAINER = "pricing"
ROUTE_ENV = "CATALOG_SERVICE_URL"
NACOS_ENV = "NACOS_ENABLED"
DIRECT_URL = "http://catalog:5005"
GATEWAY_URL = "http://catalog-gw"
OWNER_KEY = "recshop.dev/m1-owner"
# Deployment-controller bookkeeping, not a business/template configuration.
REVISION_KEY = "deployment.kubernetes.io/revision"
NAMESPACE = "recweb-chaos"
SCHEMA = "rq4-collect/pricing-route-model-v1"


class RouteModelError(ValueError):
    """Only a fixed reason code, never arbitrary resource/env contents."""


def _need(condition, code):
    if not condition:
        raise RouteModelError(code)


def _digest(value):
    try:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise RouteModelError("INVALID_JSON_VALUE") from None
    return hashlib.sha256(raw).hexdigest()


def _mapping(value, code):
    _need(type(value) is dict, code)
    return value


def _text(value, code):
    _need(type(value) is str and bool(value) and not any(x in value for x in "\r\n\0"), code)
    return value


def _metadata(obj, kind, namespace):
    _mapping(obj, "INVALID_RESOURCE")
    _need(obj.get("kind") == kind and obj.get("apiVersion") == ("v1" if kind == "Pod" else "apps/v1"),
          "WRONG_RESOURCE_KIND")
    meta = _mapping(obj.get("metadata"), "INVALID_METADATA")
    _need(meta.get("namespace") == namespace, "WRONG_NAMESPACE")
    for key in ("name", "uid", "resourceVersion"):
        _text(meta.get(key), "MISSING_RESOURCE_IDENTITY")
    _need(not meta.get("deletionTimestamp"), "RESOURCE_DELETING")
    return meta


def _annotations(metadata):
    value = metadata.get("annotations", {})
    _mapping(value, "INVALID_ANNOTATIONS")
    _need(all(type(k) is str and type(v) is str for k, v in value.items()), "INVALID_ANNOTATIONS")
    return value


def _container(spec):
    containers = _mapping(spec, "INVALID_POD_SPEC").get("containers")
    _need(type(containers) is list and bool(containers), "INVALID_CONTAINERS")
    _need(all(type(c) is dict and type(c.get("name")) is str for c in containers), "INVALID_CONTAINERS")
    _need(len({c["name"] for c in containers}) == len(containers), "DUPLICATE_CONTAINER")
    matches = [(i, c) for i, c in enumerate(containers) if c["name"] == CONTAINER]
    _need(len(matches) == 1, "PRICING_CONTAINER_MISSING")
    return matches[0]


def _env_entry(container, name):
    entries = container.get("env", [])
    _need(type(entries) is list and all(type(e) is dict and type(e.get("name")) is str for e in entries), "INVALID_ENV")
    _need(len({e["name"] for e in entries}) == len(entries), "DUPLICATE_ENV")
    matches = [(i, e) for i, e in enumerate(entries) if e["name"] == name]
    return matches[0] if matches else (None, None)


def _safe_env(entry, name):
    if entry is None:
        return {"present": False, "kind": "absent"}
    if set(entry) == {"name", "valueFrom"} and type(entry["valueFrom"]) is dict:
        return {"present": True, "kind": "value_from", "reference_sha256": _digest(entry["valueFrom"])}
    if set(entry) == {"name"} or (set(entry) == {"name", "value"} and entry["value"] == ""):
        return {"present": True, "kind": "empty_literal", "entry_keys": sorted(entry)}
    _need(set(entry) == {"name", "value"} and type(entry["value"]) is str, "MALFORMED_CONTROLLED_ENV")
    allowed = {DIRECT_URL, GATEWAY_URL} if name == ROUTE_ENV else {"false", "true"}
    if entry["value"] not in allowed:
        return {"present": True, "kind": "other_literal", "value_sha256": _digest(entry["value"])}
    return {"present": True, "kind": "literal", "entry": {"name": name, "value": entry["value"]}}


def _route(value):
    return {"present": True, "kind": "literal", "entry": {"name": ROUTE_ENV, "value": value}}


def _parsed(deployment, namespace):
    meta = _metadata(deployment, "Deployment", namespace)
    _need(meta["name"] == DEPLOYMENT, "WRONG_DEPLOYMENT")
    spec = _mapping(deployment.get("spec"), "INVALID_DEPLOYMENT_SPEC")
    _need(spec.get("selector") == {"matchLabels": {"app": DEPLOYMENT}}, "SELECTOR_MISMATCH")
    template = _mapping(spec.get("template"), "INVALID_TEMPLATE")
    tmeta = _mapping(template.get("metadata"), "INVALID_TEMPLATE_METADATA")
    _need(type(tmeta.get("labels")) is dict and tmeta["labels"].get("app") == DEPLOYMENT, "TEMPLATE_LABEL_MISMATCH")
    ci, container = _container(template.get("spec"))
    _text(container.get("image"), "CONTAINER_IMAGE_MISSING")
    ei, entry = _env_entry(container, ROUTE_ENV)
    _, nacos = _env_entry(container, NACOS_ENV)
    annotations, template_annotations = _annotations(meta), _annotations(tmeta)
    owner = annotations.get(OWNER_KEY)
    if OWNER_KEY in annotations:
        _text(owner, "INVALID_OWNER")
    replicas = spec.get("replicas")
    _need(replicas is None or type(replicas) is int, "INVALID_REPLICAS")
    return meta, spec, template, container, ci, ei, entry, nacos, owner, template_annotations


def outside_guard_digest(deployment, *, namespace=NAMESPACE):
    """Hash only; never returns non-route env/config values to the caller."""
    meta, spec, _, _, ci, ei, _, _, _, _ = _parsed(deployment, namespace)
    protected = copy.deepcopy(spec)
    if ei is not None:
        entry = protected["template"]["spec"]["containers"][ci]["env"][ei]
        if "value" in entry:
            entry["value"] = "__CONTROLLED_ROUTE_VALUE__"
    # This adapter never sets template owner. Preserve all template metadata.
    tmeta = protected["template"]["metadata"]
    if not tmeta.get("annotations"):
        tmeta.pop("annotations", None)
    annotations = {k: v for k, v in _annotations(meta).items() if k not in {OWNER_KEY, REVISION_KEY}}
    return _digest({"spec": protected, "labels": meta.get("labels", {}), "annotations": annotations})


def deployment_projection(deployment, *, namespace=NAMESPACE):
    """Redacted auxiliary envelope; children are separately verified, not absent.

    descendants denotes independently-created auxiliary resources. This patch
    creates none. Real RS/Pod inventory must go through judge_rollout().
    Unsupported route literals/references are represented only by kind + hash.
    """
    meta, spec, _, _, _, _, entry, nacos, owner, tanno = _parsed(deployment, namespace)
    conflicts = []
    if OWNER_KEY in tanno:
        conflicts.append("TEMPLATE_OWNER_PRESENT")
    if _safe_env(nacos, NACOS_ENV) != {"present": True, "kind": "literal", "entry": {"name": NACOS_ENV, "value": "false"}}:
        conflicts.append("NACOS_NOT_EXPLICITLY_DISABLED")
    return {"exists": True, "uid": meta["uid"], "resource_version": meta["resourceVersion"], "owner": owner,
            "fields": {"container": CONTAINER, "route_env": _safe_env(entry, ROUTE_ENV),
                       "replicas": spec.get("replicas"), "outside_guard_sha256": outside_guard_digest(deployment, namespace=namespace)},
            "descendants": [], "conflicts": conflicts}


def _check_original(original):
    _need(type(original) is dict and set(original) == {"exists", "uid", "resource_version", "owner", "fields", "descendants", "conflicts"},
          "INVALID_ORIGINAL_SNAPSHOT")
    _need(original["exists"] is True and original["owner"] is None and original["descendants"] == [] and original["conflicts"] == [],
          "INVALID_ORIGINAL_SNAPSHOT")
    _text(original["uid"], "INVALID_ORIGINAL_IDENTITY")
    _text(original["resource_version"], "INVALID_ORIGINAL_IDENTITY")
    fields = original["fields"]
    _need(type(fields) is dict and set(fields) == {"container", "route_env", "replicas", "outside_guard_sha256"}, "INVALID_ORIGINAL_FIELDS")
    _need(fields["container"] == CONTAINER and fields["route_env"] == _route(DIRECT_URL)
          and type(fields["replicas"]) is int and fields["replicas"] == 1, "ORIGINAL_NOT_SUPPORTED_BASELINE")
    _need(type(fields["outside_guard_sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", fields["outside_guard_sha256"]), "INVALID_ORIGINAL_GUARD")


def validate_baseline(deployment, *, namespace=NAMESPACE, hpa_target_names=()):
    """Validate structural baseline only; full runtime readiness is separate."""
    _need(type(hpa_target_names) in {tuple, list} and all(type(x) is str for x in hpa_target_names), "INVALID_SCALER_INVENTORY")
    _need(DEPLOYMENT not in hpa_target_names, "PRICING_SCALER_PRESENT")
    value = deployment_projection(deployment, namespace=namespace)
    _need(not value["conflicts"], value["conflicts"][0] if value["conflicts"] else "BASELINE_CONFLICT")
    _need(value["owner"] is None, "BASELINE_OWNER_PRESENT")
    _need(value["fields"]["route_env"] == _route(DIRECT_URL), "BASELINE_ROUTE_NOT_DIRECT_LITERAL")
    _need(type(value["fields"]["replicas"]) is int and value["fields"]["replicas"] == 1, "BASELINE_REPLICAS_NOT_ONE")
    _check_original(value)
    return value


def _aux_owner(value):
    _need(type(value) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,159}", value), "INVALID_AUXILIARY_OWNER")


def json_pointer_token(value):
    _need(type(value) is str, "INVALID_POINTER_TOKEN")
    return value.replace("~", "~0").replace("/", "~1")


def _tests(deployment, expected, namespace):
    actual = deployment_projection(deployment, namespace=namespace)
    _need(actual == expected, "STALE_CAS_OBSERVATION")
    meta, spec, _, _, ci, ei, _, _, _, _ = _parsed(deployment, namespace)
    _need(ei is not None and actual["fields"]["route_env"]["kind"] == "literal", "PATCH_ROUTE_NOT_LITERAL")
    base = f"/spec/template/spec/containers/{ci}"
    path = base + f"/env/{ei}"
    operations = [
        {"op": "test", "path": "/metadata/uid", "value": meta["uid"]},
        {"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]},
        {"op": "test", "path": "/spec/replicas", "value": spec["replicas"]},
        {"op": "test", "path": base + "/name", "value": CONTAINER},
        {"op": "test", "path": path, "value": copy.deepcopy(actual["fields"]["route_env"]["entry"])},
    ]
    return operations, path, meta


def render_set_patch(deployment, *, expected, aux_owner, namespace=NAMESPACE):
    """Render only. Caller must durably authorize the exact fresh expectation."""
    _aux_owner(aux_owner)
    current = validate_baseline(deployment, namespace=namespace)
    _need(current == expected, "STALE_CAS_OBSERVATION")
    operations, path, meta = _tests(deployment, expected, namespace)
    operations.append({"op": "replace", "path": path + "/value", "value": GATEWAY_URL})
    if "annotations" in meta:
        operations.append({"op": "add", "path": "/metadata/annotations/" + json_pointer_token(OWNER_KEY), "value": aux_owner})
    else:
        operations.append({"op": "add", "path": "/metadata/annotations", "value": {OWNER_KEY: aux_owner}})
    return operations


def render_restore_patch(deployment, *, expected, original, aux_owner, namespace=NAMESPACE):
    _aux_owner(aux_owner)
    _check_original(original)
    current = deployment_projection(deployment, namespace=namespace)
    state = classify_route_state(deployment, original=original, aux_owner=aux_owner, namespace=namespace)
    _need(state["state"] == "OWNED_DESIRED", "RESTORE_STATE_NOT_OWNED_DESIRED")
    _need(current == expected, "STALE_CAS_OBSERVATION")
    operations, path, _ = _tests(deployment, expected, namespace)
    operations.append({"op": "test", "path": "/metadata/annotations/" + json_pointer_token(OWNER_KEY), "value": aux_owner})
    operations.append({"op": "replace", "path": path + "/value", "value": original["fields"]["route_env"]["entry"]["value"]})
    operations.append({"op": "remove", "path": "/metadata/annotations/" + json_pointer_token(OWNER_KEY)})
    return operations


def _semantic(value):
    return {k: v for k, v in value.items() if k != "resource_version"}


def classify_route_state(deployment, *, original, aux_owner, previous=None, namespace=NAMESPACE):
    """Classify supplied state only; cannot infer whether an API was sent."""
    _check_original(original)
    _aux_owner(aux_owner)
    try:
        current = deployment_projection(deployment, namespace=namespace)
    except RouteModelError as exc:
        return {"state": "CONFLICT", "change": "UNKNOWN", "reason": str(exc)}
    if current["uid"] != original["uid"]:
        reason = "DEPLOYMENT_UID_CHANGED"
    elif current["conflicts"]:
        reason = current["conflicts"][0]
    elif current["fields"]["outside_guard_sha256"] != original["fields"]["outside_guard_sha256"]:
        reason = "OUTSIDE_CONFIGURATION_CHANGED"
    elif current["fields"]["replicas"] != 1:
        reason = "REPLICAS_CHANGED"
    elif current["owner"] is None and current["fields"]["route_env"] == original["fields"]["route_env"]:
        reason = "ORIGINAL"
    elif current["owner"] == aux_owner and current["fields"]["route_env"] == _route(GATEWAY_URL):
        reason = "OWNED_DESIRED"
    else:
        reason = "ROUTE_OWNER_CONFLICT"
    if reason not in {"ORIGINAL", "OWNED_DESIRED"}:
        return {"state": "CONFLICT", "change": "SEMANTIC", "reason": reason}
    prior = original if previous is None else previous
    _need(type(prior) is dict and set(prior) == set(current), "INVALID_PREVIOUS_OBSERVATION")
    change = "SAME" if prior == current else "RESOURCE_VERSION_ONLY" if _semantic(prior) == _semantic(current) else "SEMANTIC"
    return {"state": reason, "change": change, "reason": "SUPPLIED_STATE_ONLY_NOT_RECOVERY_PROOF"}


def match_active_inventory(rows, *, uid, aux_owner, namespace=NAMESPACE):
    """Compare actual object identity, never a broad owner-string allowlist."""
    _aux_owner(aux_owner)
    _text(uid, "MISSING_DEPLOYMENT_UID")
    expected = {"resource": "deployment", "name": DEPLOYMENT, "uid": uid, "owner": aux_owner, "unknown_ownership": False}
    if type(rows) is not list or len(rows) != 1 or type(rows[0]) is not dict:
        return {"ok": False, "reason": "RESIDUAL_INVENTORY_NOT_EXACT"}
    row = rows[0]
    allowed = set(expected) | {"namespace"}
    ok = (set(row) <= allowed and row.get("unknown_ownership") is False
          and all(row.get(k) == v for k, v in expected.items()) and row.get("namespace", namespace) == namespace)
    return {"ok": ok, "reason": "EXACT_AUXILIARY_RESOURCE" if ok else "RESIDUAL_IDENTITY_MISMATCH"}


def _controller(meta, kind, uid):
    refs = meta.get("ownerReferences", [])
    _need(type(refs) is list and all(type(row) is dict for row in refs), "INVALID_OWNER_CHAIN")
    controllers = [row for row in refs if row.get("controller") is True]
    _need(len(controllers) == 1 and controllers[0].get("kind") == kind and controllers[0].get("uid") == uid, "OWNER_CHAIN_MISMATCH")


def _runtime_env(container, desired):
    _, route = _env_entry(container, ROUTE_ENV)
    _, nacos = _env_entry(container, NACOS_ENV)
    _need(_safe_env(route, ROUTE_ENV) == _route(desired), "CHILD_ROUTE_MISMATCH")
    _need(_safe_env(nacos, NACOS_ENV) == {"present": True, "kind": "literal", "entry": {"name": NACOS_ENV, "value": "false"}},
          "CHILD_NACOS_NOT_DISABLED")


def judge_rollout(deployment_before, deployment_after, replica_sets, pods, runtime_env, *,
                  expected_uid, expected_route, expected_owner, expected_guard, namespace=NAMESPACE):
    """Pure coherent-readback judgment, NOT freshness or API authenticity proof.

    Caller supplies bounded complete selector inventories and actual before/after
    reads. New RS/Pod UIDs are allowed under the same Deployment. No raw env/spec
    contents are returned, including on failure.
    """
    try:
        _need(type(expected_route) is str and expected_route in {DIRECT_URL, GATEWAY_URL}, "UNSUPPORTED_EXPECTED_ROUTE")
        if expected_owner is not None:
            _aux_owner(expected_owner)
        before = deployment_projection(deployment_before, namespace=namespace)
        after = deployment_projection(deployment_after, namespace=namespace)
        for projected in (before, after):
            _need(projected["uid"] == expected_uid and projected["owner"] == expected_owner, "DEPLOYMENT_IDENTITY_OWNER_CHANGED")
            _need(not projected["conflicts"] and projected["fields"]["route_env"] == _route(expected_route)
                  and projected["fields"]["replicas"] == 1 and projected["fields"]["outside_guard_sha256"] == expected_guard,
                  "DEPLOYMENT_PROJECTION_CHANGED")
        generation = deployment_before["metadata"].get("generation")
        _need(type(generation) is int and generation > 0 and deployment_after["metadata"].get("generation") == generation,
              "DEPLOYMENT_GENERATION_CHANGED")
        for deployment in (deployment_before, deployment_after):
            status = _mapping(deployment.get("status"), "DEPLOYMENT_STATUS_MISSING")
            _need(type(status.get("observedGeneration")) is int and status["observedGeneration"] >= generation,
                  "DEPLOYMENT_GENERATION_UNOBSERVED")
            _need(all(type(status.get(k)) is int and status[k] == 1 for k in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas")),
                  "DEPLOYMENT_NOT_SETTLED")
            _need(type(status.get("unavailableReplicas", 0)) is int and status.get("unavailableReplicas", 0) == 0, "DEPLOYMENT_UNAVAILABLE")
        _need(type(replica_sets) is list and bool(replica_sets) and type(pods) is list and len(pods) == 1, "CHILD_INVENTORY_NOT_QUIESCENT")
        rs_by_uid, active = {}, []
        for rs in replica_sets:
            meta = _metadata(rs, "ReplicaSet", namespace)
            _need(meta["uid"] not in rs_by_uid and all(meta["name"] != x["metadata"]["name"] for x in rs_by_uid.values()), "DUPLICATE_REPLICASET")
            rs_labels = _mapping(meta.get("labels"), "REPLICASET_LABELS_MISSING")
            _need(rs_labels.get("app") == DEPLOYMENT, "REPLICASET_SELECTOR_MISMATCH")
            _controller(meta, "Deployment", expected_uid)
            replicas = _mapping(rs.get("spec"), "INVALID_REPLICASET").get("replicas")
            _need(type(replicas) is int and replicas in {0, 1}, "REPLICASET_REPLICAS_INVALID")
            status = _mapping(rs.get("status", {}), "INVALID_REPLICASET_STATUS")
            _need(type(status.get("replicas", 0)) is int and status.get("replicas", 0) == replicas, "REPLICASET_NOT_SETTLED")
            if replicas == 1:
                active.append(rs)
                _need(all(type(status.get(k)) is int and status[k] == 1 for k in ("readyReplicas", "availableReplicas")), "REPLICASET_NOT_READY")
            rs_by_uid[meta["uid"]] = rs
        _need(len(active) == 1, "ACTIVE_REPLICASET_AMBIGUOUS")
        rs = active[0]
        rs_meta = rs["metadata"]
        # The active RS must be for this exact desired template, not merely an
        # old owned RS whose route/image happen to match. The Deployment
        # controller adds only the pod-template-hash label at this boundary.
        rs_template = copy.deepcopy(_mapping(rs["spec"].get("template"), "REPLICASET_TEMPLATE_MISSING"))
        rs_template_meta = _mapping(rs_template.get("metadata"), "REPLICASET_TEMPLATE_METADATA_MISSING")
        rs_template_labels = _mapping(rs_template_meta.get("labels"), "REPLICASET_TEMPLATE_LABELS_MISSING")
        rs_template_labels.pop("pod-template-hash", None)
        expected_template = copy.deepcopy(deployment_after["spec"]["template"])
        expected_template["metadata"]["labels"].pop("pod-template-hash", None)
        for template in (rs_template, expected_template):
            if not template["metadata"].get("annotations"):
                template["metadata"].pop("annotations", None)
        _need(rs_template == expected_template, "REPLICASET_TEMPLATE_MISMATCH")
        _, rs_container = _container(rs["spec"].get("template", {}).get("spec"))
        _runtime_env(rs_container, expected_route)
        _, deployment_container = _container(deployment_after["spec"]["template"]["spec"])
        _need(rs_container.get("image") == deployment_container["image"], "REPLICASET_IMAGE_MISMATCH")
        pod = pods[0]
        pod_meta = _metadata(pod, "Pod", namespace)
        _controller(pod_meta, "ReplicaSet", rs_meta["uid"])
        labels = _mapping(pod_meta.get("labels"), "POD_LABELS_MISSING")
        rs_hash = rs_meta.get("labels", {}).get("pod-template-hash")
        _need(type(rs_hash) is str and bool(rs_hash) and labels.get("app") == DEPLOYMENT
              and labels.get("pod-template-hash") == rs_hash, "POD_TEMPLATE_HASH_MISMATCH")
        _, container = _container(pod.get("spec"))
        _runtime_env(container, expected_route)
        _need(container.get("image") == deployment_container["image"], "POD_IMAGE_MISMATCH")
        node = _text(pod["spec"].get("nodeName"), "POD_NODE_MISSING")
        status = _mapping(pod.get("status"), "POD_STATUS_MISSING")
        _need(status.get("phase") == "Running", "POD_NOT_RUNNING")
        conditions = status.get("conditions", [])
        _need(type(conditions) is list and all(type(x) is dict for x in conditions), "INVALID_POD_CONDITIONS")
        ready = [x for x in conditions if x.get("type") == "Ready"]
        _need(len(ready) == 1 and ready[0].get("status") == "True", "POD_NOT_READY")
        states = status.get("containerStatuses", [])
        _need(type(states) is list and all(type(x) is dict for x in states), "INVALID_CONTAINER_STATUS")
        matches = [x for x in states if x.get("name") == CONTAINER]
        _need(len(matches) == 1 and matches[0].get("ready") is True, "CONTAINER_NOT_READY")
        ct = matches[0]
        _need(type(ct.get("restartCount")) is int and ct["restartCount"] >= 0, "RESTART_COUNT_INVALID")
        image_id = _text(ct.get("imageID"), "IMAGE_ID_MISSING")
        _text(ct.get("containerID"), "CONTAINER_ID_MISSING")
        _mapping(runtime_env, "RUNTIME_ENV_READ_MISSING")
        _need(runtime_env.get("pod_name") == pod_meta["name"] and runtime_env.get("pod_uid_before") == pod_meta["uid"]
              and runtime_env.get("pod_uid_after") == pod_meta["uid"] and runtime_env.get("container") == CONTAINER,
              "RUNTIME_READ_POD_IDENTITY_CHANGED")
        codes = runtime_env.get("return_codes")
        _need(type(codes) is dict and set(codes) == {"route", "nacos"}
              and all(type(code) is int and code == 0 for code in codes.values()), "RUNTIME_ENV_READ_FAILED")
        _need(runtime_env.get(ROUTE_ENV) == expected_route and runtime_env.get(NACOS_ENV) == "false", "RUNTIME_ENV_MISMATCH")
        return {"settled": True, "reason_codes": [], "proof_scope": "SUPPLIED_READBACK_ONLY",
                "deployment_uid": expected_uid, "generation": generation,
                "resource_version_before": before["resource_version"], "resource_version_after": after["resource_version"],
                "serving_pod": {"name": pod_meta["name"], "uid": pod_meta["uid"], "replicaset_uid": rs_meta["uid"],
                                "node_name": node, "image_id": image_id, "restart_count": ct["restartCount"]}}
    except RouteModelError as exc:
        return {"settled": False, "reason_codes": [str(exc)], "proof_scope": "SUPPLIED_READBACK_ONLY"}
