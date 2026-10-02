"""Read-only authentication of maintenance markers for the runner lease gate."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

SCHEMA = "recshop-m1-sampling-maintenance-v1"
PASS_STATES = {"INSTALLED_VERIFIED", "RESTORED_VERIFIED", "RECONCILED_VERIFIED_RESTORED", "RECONCILED_VERIFIED"}
# Reconciled pass states keep failed=true: the attempt did fail and only a later
# evidence-backed reconciliation (SamplingRollout.reconcile_receipt) closed the
# receipt; they never claim the one-shot INSTALLED/RESTORED_VERIFIED success.
RECONCILED_PASS_STATES = {"RECONCILED_VERIFIED_RESTORED", "RECONCILED_VERIFIED"}
RECONCILE_ELIGIBLE_STATES = {"BLOCKED", "FAILED", "FAILED_RESTORED"}


def verify_maintenance_marker(registry, environment_key, *, evidence_kind=None):
    """Caller must hold the canonical physical-environment OS lock.

    No marker is the backwards-compatible pre-maintenance case. Existing marker
    must bind exact persisted plan/state and the matching completed state event.
    This verifies internally coherent provenance, not hostile-writer signatures.
    """
    marker_path = Path(registry) / (environment_key + ".maintenance.json")
    def require(ok):
        if not ok:
            raise RuntimeError("sampling maintenance incomplete or receipt invalid")
    # exists() follows links: a dangling link must not mean "never maintained".
    require(not marker_path.is_symlink() and marker_path.resolve() == marker_path)
    if not marker_path.exists():
        return None
    require(marker_path.is_file())
    marker = json.loads(marker_path.read_bytes())
    require(marker.get("schema_version") == SCHEMA and marker.get("environment_key") == environment_key
            and marker.get("status") in PASS_STATES)
    path = Path(marker["receipt"])
    require(path.is_absolute() and ".." not in path.parts and path.resolve() == path and path.name == marker.get("owner"))
    values = {}
    for key, filename in (("plan", "plan.json"), ("state", "state.json"), ("completed_event", marker["completed_event"])):
        target = path / filename
        require(target.parent == path and target.resolve() == target and target.is_file())
        data = target.read_bytes()
        require(hashlib.sha256(data).hexdigest() == marker.get(key + "_sha256"))
        values[key] = json.loads(data)
    plan, state, event = values["plan"], values["state"], values["completed_event"]
    env = plan.get("environment", {})
    encoded = json.dumps({"cluster_uid": env.get("cluster_uid"), "namespace_uid": env.get("namespace_uid")},
                         sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    reconciled = marker["status"] in RECONCILED_PASS_STATES
    require(hashlib.sha256(encoded).hexdigest() == environment_key and plan.get("schema_version") == SCHEMA
            and (evidence_kind is None or plan.get("evidence_kind") == evidence_kind)
            and plan.get("owner") == marker["owner"] and state.get("schema_version") == SCHEMA
            and state.get("status") == marker["status"] and state.get("failed") is reconciled
            and state.get("plan_sha256") == marker["plan_sha256"]
            and event.get("schema_version") == SCHEMA and event.get("event") == "state"
            and event.get("payload") == {k: v for k, v in state.items() if k != "schema_version"})
    runtime = state.get("runtime", {})
    require(type(runtime.get("pods")) is list and bool(runtime["pods"]) and runtime.get("image_visibility"))
    if reconciled:
        # The reconcile section is the independent evidence that this pass state
        # was reached by re-coordination of a genuinely failed receipt, not by a
        # forged one-shot success: it binds the fresh node-image receipt hash,
        # the live readback UIDs (deployment + node), the prior failure state
        # and the exact fields observed at determination time.
        record = state.get("reconcile")
        determination = record.get("determination") if type(record) is dict else None
        evidence = record.get("fresh_image_evidence") if type(record) is dict else None
        require(type(record) is dict and record.get("prior_status") in RECONCILE_ELIGIBLE_STATES
                and record.get("branch") in {"completed_restore", "verified_prior_restore"}
                and type(determination) is dict and type(evidence) is dict
                and determination.get("deployment_uid") == plan["release"]["deployment_uid"]
                and determination.get("node_uid") == evidence.get("node_uid") == runtime["image_visibility"].get("node_uid")
                and determination.get("fields") == (plan["desired"] if record["branch"] == "completed_restore" else plan["original"])
                and type(evidence.get("receipt_sha256")) is str and bool(evidence["receipt_sha256"])
                and type(evidence.get("observed_at_epoch_s")) in (int, float)
                and evidence.get("evidence_kind") == plan.get("evidence_kind"))
    expected_source = plan["release"]["source_after_sha256"] if marker["status"] == "INSTALLED_VERIFIED" else plan["release"]["source_before_sha256"]
    expected_helper = plan["release"]["helper_sha256"] if marker["status"] == "INSTALLED_VERIFIED" else None
    expected_env = {key: entry["value"] for key, entry in plan["desired"]["env"].items()} if marker["status"] == "INSTALLED_VERIFIED" else plan["original_runtime_env"]
    deployment = runtime.get("deployment", {})
    require(deployment.get("uid") == plan["release"]["deployment_uid"] and deployment.get("resource_version")
            and deployment.get("guard_sha256") == plan["guard_sha256"]
            and deployment.get("fields") == (plan["desired"] if marker["status"] == "INSTALLED_VERIFIED" else plan["original"]))
    for pod in runtime["pods"]:
        require(pod.get("pod_uid") and pod.get("container_id") and pod.get("image_id")
                and pod.get("runtime", {}).get("source_sha256") == expected_source
                and pod["runtime"].get("helper_sha256") == expected_helper
                and pod["image_id"].split("://", 1)[-1] in runtime["image_visibility"].get("runtime_ids", [])
                and pod["runtime"].get("env") == expected_env)
    return marker
