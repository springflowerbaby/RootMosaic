"""Finite source-timestamp preservation and retained-update evidence (collection, closed collection).

Original gzip/plain exporter bodies remain intact. Two actual source/config
snapshots bracket a phase; a separate Prometheus range reports timestamps of
retained source updates. This is not a witness of every upstream export.

collection closure: retained cadence is a necessary condition for production PASS
(ruling 2.1), and the full-window range queries must retain their execution
clock on both bases, ended not before the window end (ruling 2.4), while both
snapshots re-derive the container start from the raw component readback
(ruling 2.5). Boundary-undecided inputs stay NOT_ASSESSED, never a quiet PASS.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import zlib

from . import contract as c

SCHEMA = "rq4-collect/sampling-readback-r12-v3"
METHOD = "bounded_source_update_stream_v1"
RULE_FIELDS = {"method", "rule_ref", "max_configured_interval_s", "nominal_interval_s", "cadence_tolerance_s",
               "max_missing_fraction", "max_source_age_s", "max_clock_uncertainty_s", "timestamp_resolution_s",
               "max_snapshot_edge_gap_s", "max_decoded_bytes"}


class SamplingError(ValueError):
    pass


def need(ok, reason):
    if not ok:
        raise SamplingError(reason)


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def validate_policy(policy):
    need(type(policy) is dict and set(policy) == RULE_FIELDS and policy["method"] == METHOD,
         "explicit_finite_sampling_policy_required")
    need(type(policy["rule_ref"]) is str and bool(policy["rule_ref"]), "sampling_rule_ref_required")
    for key in RULE_FIELDS - {"method", "rule_ref"}:
        need(finite(policy[key]) and policy[key] >= 0, "sampling_numeric_policy_invalid")
    for key in ("max_configured_interval_s", "nominal_interval_s", "timestamp_resolution_s"):
        need(policy[key] > 0, "sampling_positive_policy_required")
    need(policy["cadence_tolerance_s"] < policy["nominal_interval_s"] / 2 and policy["max_missing_fraction"] <= 1,
         "sampling_tolerance_or_fraction_invalid")
    need(type(policy["max_decoded_bytes"]) is int and policy["max_decoded_bytes"] > 0, "sampling_decoded_byte_budget_required")


def retained_cadence(source_values, window, policy, *, right_boundary_value=None):
    """Count interior nominal missing slots once; unknown boundary gaps are NA.

    This never emits interpolated observations. Grid repeats are diagnostic,
    not another missing penalty. Adjacent gaps avoid accumulated phase drift.
    """
    validate_policy(policy)
    need(all(finite(value) for value in source_values) and source_values, "source_points_empty_or_invalid")
    need(all(b >= a for a,b in zip(source_values,source_values[1:])), "source_timestamps_reversed")
    values = list(source_values)
    if right_boundary_value is not None:
        need(finite(right_boundary_value) and right_boundary_value >= values[-1], "right_boundary_source_reversed")
        values.append(right_boundary_value)
    distinct = sorted(set(values))
    inside = [value for value in distinct if window.start <= value < window.end]
    need(len(inside) >= 2, "source_distinct_points_insufficient")
    carry = [value for value in distinct if value < window.start]
    normal = policy["nominal_interval_s"] + policy["cadence_tolerance_s"] + policy["timestamp_resolution_s"]
    if carry:
        need(inside[0] - carry[-1] <= normal, "left_cross_boundary_gap_unresolved")
    else:
        need(inside[0] - window.start <= normal, "left_boundary_incomplete")
    if right_boundary_value is not None and right_boundary_value >= window.end:
        need(right_boundary_value - inside[-1] <= normal, "right_cross_boundary_gap_unresolved")
    else:
        need(window.end - inside[-1] <= normal, "right_boundary_incomplete")
    gaps, missing = [], 0
    for left,right in zip(inside,inside[1:]):
        gap = right-left
        intervals = max(1, math.floor(gap / policy["nominal_interval_s"] + .5))
        absent = 0
        if gap > normal:
            need(intervals >= 2 and abs(gap - intervals * policy["nominal_interval_s"])
                 <= intervals * policy["cadence_tolerance_s"] + policy["timestamp_resolution_s"],
                 "non_nominal_cadence_not_explained_by_missing_slots")
            absent = intervals - 1
        missing += absent
        gaps.append({"gap_s":gap,"estimated_missing_nominal_slots":absent})
    expected = math.ceil((window.end-window.start)/policy["nominal_interval_s"])
    need(expected > 0, "nominal_phase_slots_empty")
    fraction = missing/expected
    return {"status":"PASS" if fraction <= policy["max_missing_fraction"] else "FAIL",
            "expected_nominal_slots":expected,"estimated_missing_nominal_slots":missing,"missing_fraction":fraction,
            "distinct_source_updates_in_phase":len(inside),"raw_source_gaps":gaps,
            "repeated_range_values":len(source_values)-len(set(source_values)),
            "carry_in_timestamp":carry[-1] if carry else None,"boundary_complete":True}


def decode_body(record, *, max_decoded_bytes):
    need(type(record) is dict and type(max_decoded_bytes) is int and max_decoded_bytes > 0, "bounded_response_required")
    raw = base64.b64decode(record["raw_base64"], validate=True)
    need(len(raw) == record["raw_bytes"] and sha(raw) == record["raw_sha256"], "response_raw_hash_mismatch")
    headers = record["response_headers"]
    need(type(headers) is list and all(type(row) in (list, tuple) and len(row) == 2 for row in headers), "response_headers_required")
    encodings = [value.lower().strip() for key, value in headers if key.lower() == "content-encoding"]
    need(len(encodings) <= 1, "ambiguous_content_encoding")
    encoding = encodings[0] if encodings else "identity"
    if encoding == "gzip":
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
        decoded = decoder.decompress(raw, max_decoded_bytes + 1)
        need(len(decoded) <= max_decoded_bytes and not decoder.unconsumed_tail and decoder.eof
             and not decoder.unused_data, "gzip_truncated_concatenated_or_decoded_limit")
    else:
        need(encoding == "identity", "unsupported_content_encoding")
        decoded = raw
        need(len(decoded) <= max_decoded_bytes, "decoded_response_limit")
    return decoded


_SAMPLE = re.compile(r'^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{(.*)\})?\s+(\S+)\s+([+-]?[0-9]+)\s*$')
_LABEL = re.compile(r'\s*([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\["\\n])*)"\s*(?:,|$)')


def exposition_points(raw, metric):
    """Prometheus text 0.0.4 explicit timestamps: integer milliseconds.

    Parse exact complete label maps; do not select an arbitrary first series or
    throw away http_host to merge old proxy traffic into the current source.
    """
    need(type(raw) is bytes and type(metric) is str, "exposition_bytes_and_metric_required")
    output = []
    for line in raw.decode("utf-8").splitlines():
        if not line.startswith(metric + "{") and not line.startswith(metric + " "):
            continue
        match = _SAMPLE.fullmatch(line)
        need(match is not None and match.group(1) == metric, "metric_explicit_timestamp_missing_or_invalid")
        labels, cursor, blob = {}, 0, match.group(2) or ""
        while cursor < len(blob):
            item = _LABEL.match(blob, cursor)
            need(item is not None and item.group(1) not in labels, "metric_labels_invalid_or_duplicate")
            labels[item.group(1)] = json.loads('"' + item.group(2) + '"')
            cursor = item.end()
        value = float(match.group(3))
        need(math.isfinite(value), "metric_value_nonfinite")
        milliseconds = int(match.group(4))
        need(milliseconds >= 0, "source_timestamp_negative")
        output.append({"labels": labels, "value": value, "source_timestamp_s": milliseconds / 1000,
                       "source_timestamp_ms": milliseconds, "timestamp_resolution_s": .001})
    return output


def prometheus_labels(labels, target_labels, honor_labels):
    """Prometheus documented honor_labels mapping; reject ambiguous collisions."""
    need(type(labels) is dict and type(target_labels) is dict and type(honor_labels) is bool,
         "explicit_target_label_mapping_required")
    result = dict(labels)
    for key, value in target_labels.items():
        if key in result and result[key] != value:
            if honor_labels:
                continue
            exported = "exported_" + key
            need(exported not in result, "ambiguous_exported_label_collision")
            result[exported] = result[key]
        result[key] = value
    return result


def _snapshot_proof(snapshot, contract):
    """Recompute the public timestamp-path projection from retained read replies."""
    import yaml
    binding, path, proof = snapshot["source_binding"], snapshot["timestamp_path"], snapshot["timestamp_path_evidence"]
    inspect = proof["collector_inspect"]
    need(inspect.get("running") is True and inspect.get("id") == path["collector_id"]
         and inspect.get("image_id") == path["collector_image_id"] and inspect.get("started_at") == path["collector_started_at"],
         "collector_projection_disagrees_with_readback")
    config_raw = base64.b64decode(proof["collector_config_base64"], validate=True)
    config = yaml.safe_load(config_raw)
    need(sha(config_raw) == path["collector_config_sha256"]
         and config.get("exporters",{}).get("prometheus",{}).get("send_timestamps") is True,
         "collector_timestamp_config_unbound")
    config_reply = json.loads(base64.b64decode(proof["prom_config_raw_base64"], validate=True))
    active_text = config_reply["data"]["yaml"]
    need(config_reply.get("status") == "success" and sha(active_text.encode()) == path["prom_config_sha256"],
         "prometheus_active_config_unbound")
    active = yaml.safe_load(active_text)
    jobs = [job for job in active.get("scrape_configs",[]) if job.get("job_name") == proof["prom_job"]]
    need(len(jobs) == 1 and jobs[0].get("honor_timestamps") is True
         and jobs[0].get("honor_labels",False) == path["honor_labels"]
         and not jobs[0].get("relabel_configs") and not jobs[0].get("metric_relabel_configs"),
         "prometheus_label_or_timestamp_policy_unbound")
    targets = json.loads(base64.b64decode(proof["prom_targets_raw_base64"],validate=True))
    matches = [row for row in targets.get("data",{}).get("activeTargets",[]) if row.get("scrapeUrl") == proof["prom_scrape_url"]]
    need(targets.get("status") == "success" and len(matches) == 1 and matches[0].get("health") == "up"
         and not matches[0].get("lastError") and matches[0].get("labels") == path["target_labels"],
         "prometheus_target_identity_unbound")
    from .live_runtime import _duration_seconds
    need(_duration_seconds(matches[0]["scrapeInterval"]) == path["configured_scrape_interval_s"], "scrape_interval_unbound")
    runtime, component = snapshot["runtime_readback"], snapshot["component_readback"]
    need(runtime.get("process_id") == 1 and runtime.get("source_sha256") == binding["runtime_source_sha256"]
         and runtime.get("helper_sha256") == binding["runtime_helper_sha256"]
         and runtime.get("process_cmd_sha256") == binding["process_cmd_sha256"]
         and float(runtime.get("env",{}).get("OTEL_METRIC_EXPORT_INTERVAL"))/1000 == path["configured_export_interval_s"],
         "runtime_export_configuration_unbound")
    context = contract.context.to_dict()
    need(component.get("contract_sha256") == contract.sha256 and component.get("run_id") == context["run_id"]
         and component.get("attempt_id") == context["attempt_id"] and component.get("entity") == binding["entity"]
         and component.get("deployment_uid") == binding["deployment_uid"] and component.get("evidence_kind") == "observed"
         and type(component.get("return_code")) is int and component["return_code"] == 0,
         "source_component_identity_unbound")
    pods = [row for row in component.get("pods",[]) if row.get("pod_uid") == binding["pod_uid"]]
    need(len(pods) == 1 and pods[0].get("container_id") == binding["container_id"]
         and pods[0].get("image_id") == binding["image_id"] and pods[0].get("pod_name") == binding["pod_name"],
         "source_pod_container_projection_unbound")


def _derived_container_started_epoch_s(snapshot):
    """Re-derive the container start from the raw component readback pods row.

    Ruling 2.5: the binding field alone proves nothing; the timestamp must be
    re-derivable from the retained readback bytes each snapshot actually got.
    """
    from datetime import datetime
    binding = snapshot["source_binding"]
    rows = [row for row in snapshot["component_readback"].get("pods", []) if row.get("pod_uid") == binding["pod_uid"]]
    need(len(rows) == 1 and type(rows[0].get("container_started_at")) is str and rows[0]["container_started_at"],
         "container_started_at_not_readable_from_readback")
    started = datetime.fromisoformat(rows[0]["container_started_at"].replace("Z", "+00:00"))
    need(started.tzinfo is not None, "container_started_at_timezone_missing")
    return started.timestamp()


def _query_execution_clock(query, clock, window, label):
    """Ruling 2.4: retained dual-basis execution clock for full-window queries.

    A range query covering the whole phase cannot have ended before the window
    end; the anchor consistency of both bases is judged with the declared
    clock uncertainty (same form as the archive metadata checks in quality).
    """
    values = {key: query.get(key) for key in ("started_monotonic_s", "ended_monotonic_s",
                                              "started_unix_epoch_s", "ended_unix_epoch_s")}
    need(all(finite(value) for value in values.values())
         and values["ended_monotonic_s"] >= values["started_monotonic_s"]
         and values["ended_unix_epoch_s"] >= values["started_unix_epoch_s"],
         label + "_execution_clock_missing_or_reversed")
    for basis in ("started", "ended"):
        anchored = clock["unix_epoch_s"] + values[basis + "_monotonic_s"] - clock["monotonic_s"]
        need(abs(values[basis + "_unix_epoch_s"] - anchored) <= clock["uncertainty_s"],
             label + "_execution_clock_anchor_inconsistent")
    need(values["ended_unix_epoch_s"] >= window.end, label + "_ended_before_full_window")
    return values


def _declared_replacement_band_s(contract, snapshot, phase):
    """P04 (collection.1): declared rollout switch band (seconds) for THIS source.

    Same declaration channel the collection/collection UID-lifecycle chain reads (log_archive
    ``_declared_switch_band_s`` -> the assembly S07 env-rollout / gateway
    ConfigMap-rollout decision records): a during-phase Pod replacement is a
    DECLARED mechanism outcome only when a banded rollout mechanism in THIS
    run contract targets this exact Deployment with a window overlapping the
    phase. The overlap compares run_offset against run_offset -- the planned
    window and the contract's own phase windows share one basis, while the
    recorded actual windows are unix-epoch and can never be compared to a
    planned offset directly (D07 fix-1: the mixed-basis comparison made the
    band 0 on every real contract, so the segmentation below never fired in
    the field). No band, a basis disagreement, unbanded mechanisms, foreign
    deployments or lookup failure return 0.0 and the caller refuses to guess.
    """
    if phase != "during_fault":
        return 0.0
    try:
        from . import scenario_definitions as asm
        bands = {"app_env_hook": float(asm.S07_ROLLOUT_PROFILES[asm.S07_DEFAULT_ROLLOUT_PROFILE].rollout_band_s),
                 "nginx_configmap_rollout": float(asm.GATEWAY_ROLLOUT_PROFILES[asm.GATEWAY_DEFAULT_ROLLOUT_PROFILE].rollout_band_s)}
    except BaseException:
        return 0.0
    deployment = (snapshot.get("component_readback") or {}).get("deployment")
    namespace = snapshot.get("source_binding", {}).get("namespace")
    if type(deployment) is not str or not deployment or type(namespace) is not str:
        return 0.0
    planned_phase = (contract.to_dict().get("phases") or {}).get(phase) or {}
    if planned_phase.get("time_basis") != "run_offset":
        return 0.0
    band = 0.0
    for fault in contract.faults:
        spec = fault.to_dict()
        if spec.get("mechanism") not in bands:
            continue
        target = spec.get("raw_target") or {}
        planned = spec.get("planned_window") or {}
        if (target.get("name") == deployment and target.get("scope") == namespace
                and planned.get("time_basis") == "run_offset"
                and finite(planned.get("start")) and finite(planned.get("end"))
                and planned["start"] < planned_phase["end"] and planned["end"] > planned_phase["start"]):
            band = max(band, bands[spec["mechanism"]])
    return band


def _pod_replacement_facts(before, after, band):
    head, tail = before["source_binding"], after["source_binding"]
    keys = ("pod_name", "pod_uid", "container_id", "image_id", "container_started_at_epoch_s")
    return {"head_pod": {key: head.get(key) for key in keys},
            "tail_pod": {key: tail.get(key) for key in keys},
            "deployment_uid": head.get("deployment_uid"),
            "declared_switch_band_s": band,
            "segmentation": "head_binding_pins_window_start_tail_binding_pins_window_end"}



# with the DECLARED rollout itself. The pilot template fires the first fault
# at the during boundary, so the env/ConfigMap spec write lands inside the
# head chain's own reads: the bracket tears (generation advances between the
# two deployment reads), the surge window shows two current pods, the
# replacement pod has no containerStatuses yet, or the pod swaps inside one
# chain. These are the mechanism's own boundary, the same evidential class
# as the P04 segmentation -- deferring them requires the DECLARED band for
# this exact Deployment (read off the surviving snapshot) AND that every
# failed side's reason is a collision signature. Budget misses, payload
# errors, or any mixed failure keep the ordinary missing-snapshot verdict.
_BOUNDARY_COLLISION_REASONS = frozenset({
    "component_deployment_changed_during_read",
    "sampling_requires_one_unambiguous_source_container",
    "component_container_status_missing_or_ambiguous",
    "sampling_source_changed_during_runtime_read"})


def _boundary_capture_collided_with_declared_rollout(record, contract, phase):
    if phase != "during_fault":
        return None
    issues = record.get("snapshot_issues") or {}
    reasons, failed_sides = set(), []
    for side, key in (("head", "before"), ("tail", "after")):
        if type(record.get(key)) is dict:
            continue
        failed_sides.append(side)
        receipt = issues.get(side + "_source_failure")
        if isinstance(receipt, dict):
            reasons.add(str(receipt.get("reason")))
        elif isinstance(issues.get(side), str):
            reasons.add(issues[side])
    if not reasons or not reasons <= _BOUNDARY_COLLISION_REASONS:
        return None
    survivor = record.get("after") if type(record.get("after")) is dict else record.get("before")
    if not isinstance(survivor, dict):
        return None
    band = _declared_replacement_band_s(contract, survivor, phase)
    if band <= 0:
        return None
    return {"failed_sides": failed_sides,
            "collision_reasons": sorted(reasons), "declared_band_s": band,
            "survivor_pod": survivor.get("source_binding", {}).get("pod_name"),
            "band_source_deployment": (survivor.get("component_readback") or {}).get("deployment")}



def evaluate(record, *, contract, phase, query_id, source_id, actual_window, policy, max_decoded_bytes,
             expected_clock, expected_labels):
    """Caller must bind record bytes with VerifiedArtifacts before evaluating.

    collection three-state verdict: PASS requires the whole collection chain plus the collection
    closure gates (retained cadence PASS, dual-basis query execution clocks
    ended not before the window end, container start re-derived from both raw
    readbacks). Any need failure stays NOT_ASSESSED with its reason; a cadence
    FAIL on otherwise verified evidence is a FAIL. Boundary-undecided inputs
    (left/right boundary incomplete or unresolved) remain NOT_ASSESSED.
    """
    result = {"method": METHOD, "accepted_scope": "retained_source_updates_and_age_not_all_upstream_exports"}
    try:
        validate_policy(policy)
        context = contract.context.to_dict()
        need(record.get("schema_version") == SCHEMA and record.get("method") == METHOD
             and record.get("contract_sha256") == contract.sha256 and record.get("run_id") == context["run_id"]
             and record.get("attempt_id") == context["attempt_id"] and record.get("phase") == phase
             and record.get("source_id") == source_id and record.get("query_id") == query_id
             and record.get("evidence_kind") == "observed" and record.get("actual_window") == actual_window.to_dict(),
             "sampling_record_identity_mismatch")
        clock = record["clock_mapping"]
        need(record.get("run_clock_anchor_ref") == "run-record"
             and clock == {**expected_clock, "clock_id":context["clock_id"]}
             and clock.get("clock_id") == context["clock_id"]
             and all(finite(clock.get(key)) for key in ("monotonic_s", "unix_epoch_s", "uncertainty_s"))
             and 0 <= clock["uncertainty_s"] <= policy["max_clock_uncertainty_s"], "sampling_clock_unverified")
        before, after = record["before"], record["after"]
        if type(before) is not dict or type(after) is not dict:
            
            # check whether the failed side collided with the DECLARED rollout
            # band itself (machine-gated; see
            # _boundary_capture_collided_with_declared_rollout). The deferral
            # keeps the collision facts and never converts into a PASS.
            collision = _boundary_capture_collided_with_declared_rollout(record, contract, phase)
            if collision is not None:
                return {**result, "status": "NOT_ASSESSED",
                        "reason": "sampling_boundary_capture_collided_with_declared_rollout_band",
                        "details": collision}
        need(type(before) is dict and type(after) is dict,
             "sampling_snapshot_missing:" + json.dumps(record.get("snapshot_issues", {}), sort_keys=True))
        derived_starts = {}
        for name, snapshot in (("head", before), ("tail", after)):
            need(snapshot.get("evidence_kind") == "observed" and snapshot.get("workers_joined") is True,
                 "sampling_snapshot_not_observed_or_not_drained")
            need(finite(snapshot.get("started_at_epoch_s")) and finite(snapshot.get("ended_at_epoch_s"))
                 and snapshot["started_at_epoch_s"] <= snapshot["ended_at_epoch_s"], "sampling_snapshot_clock_missing")
            _snapshot_proof(snapshot, contract)
            derived_start = _derived_container_started_epoch_s(snapshot)
            need(finite(snapshot["source_binding"].get("container_started_at_epoch_s"))
                 and derived_start == snapshot["source_binding"]["container_started_at_epoch_s"],
                 "container_started_at_disagrees_with_readback_derivation")
            derived_starts[name] = derived_start
        # P04: the head pod must cover the window start. Whether the TAIL pod
        # must also cover the window start depends on the segmentation below
        # (same-source vs a declared replacement), so that check moved here
        # from the per-snapshot loop.
        need(derived_starts["head"] <= actual_window.start, "derived_container_started_after_phase_start")
        need(actual_window.start <= before["started_at_epoch_s"] <= before["ended_at_epoch_s"]
             <= actual_window.start + policy["max_snapshot_edge_gap_s"]
             and actual_window.end-policy["max_snapshot_edge_gap_s"] <= after["ended_at_epoch_s"] <= actual_window.end
             and after["started_at_epoch_s"] >= actual_window.end-policy["max_snapshot_edge_gap_s"]
             and not any(record.get("snapshot_issues", {}).values()),
             "sampling_snapshot_stage_mismatch")
        binding = before["source_binding"]
        if binding != after["source_binding"]:
            
            # into the blanket identity-change rejection, so every legal
            # rollout/rebuild scenario was misread as a data defect. Segmented
            
            # accepted as a DECLARED replacement only when it is exactly a
            # pod_uid change under the SAME deployment_uid (a same-pod
            # container restart is not a replacement; a Deployment change is a
            # different physical source) with the declared rollout band for
            # this exact Deployment in the run contract and the new pod
            # already covering the tail snapshot. This never converts into a
            # PASS: the fixed exporter-label source model cannot assess a
            # replaced source, so the honest verdict is a NOT_ASSESSED that
            # names the segmentation with its facts instead of a defect.
            tail_binding = after["source_binding"]
            need(all(tail_binding.get(key) == binding.get(key)
                     for key in ("entity", "namespace", "namespace_uid", "deployment_uid")),
                 "source_identity_or_lifecycle_changed_without_proof")
            need(tail_binding.get("pod_uid") and tail_binding["pod_uid"] != binding.get("pod_uid"),
                 "source_identity_or_lifecycle_changed_without_proof")
            band = _declared_replacement_band_s(contract, before, phase)
            if band <= 0:
                return {**result, "status": "NOT_ASSESSED",
                        "reason": "sampling_pod_replacement_band_undeclared",
                        "details": _pod_replacement_facts(before, after, None)}
            need(derived_starts["tail"] <= actual_window.end,
                 "sampling_replacement_tail_pod_started_after_window_end")
            return {**result, "status": "NOT_ASSESSED",
                    "reason": "sampling_pod_replacement_segmented_fixed_source_model_cannot_assess",
                    "details": _pod_replacement_facts(before, after, band)}
        need(derived_starts["tail"] <= actual_window.start, "derived_container_started_after_phase_start")
        required = ("entity", "namespace", "namespace_uid", "deployment_uid", "pod_name", "pod_uid", "container_id", "image_id", "runtime_source_sha256")
        need(all(type(binding.get(key)) is str and binding[key] for key in required), "physical_source_pins_missing")
        need(finite(binding.get("container_started_at_epoch_s")) and binding["container_started_at_epoch_s"] <= actual_window.start,
             "source_container_does_not_cover_phase_start")
        path = before["timestamp_path"]
        need(path == after["timestamp_path"], "active_timestamp_path_changed")
        need(path.get("send_timestamps") is True and path.get("honor_timestamps") is True,
             "source_timestamp_preservation_not_enabled")
        need(all(type(path.get(key)) is str and path[key] for key in
                 ("collector_id", "collector_image_id", "collector_config_sha256", "prom_config_sha256", "exporter_origin")),
             "active_timestamp_path_identity_missing")
        for key in ("configured_export_interval_s", "configured_scrape_interval_s"):
            need(finite(path.get(key)) and 0 < path[key] <= policy["max_configured_interval_s"], "configured_interval_outside_policy")
        labels = record["exporter_labels"]
        need(labels.get("k8s_pod_name") == binding["pod_name"] and labels.get("k8s_namespace_name") == binding["namespace"],
             "metric_labels_do_not_match_physical_source")
        need(binding["namespace"] == context["namespace"] and type(expected_labels) is dict and expected_labels
             and all(labels.get(key) == value for key,value in expected_labels.items()), "metric_observer_labels_mismatch")
        mapped = prometheus_labels(labels, path["target_labels"], path["honor_labels"])
        query = record["source_query"]
        need(query.get("status") == "ok" and query.get("http_status") == 200
             and query.get("origin") == path["prom_origin"] and query.get("path") == "/api/v1/query_range",
             "source_query_unavailable")
        source_clock = _query_execution_clock(query, clock, actual_window, "source_query")
        params = query["parameters"]
        expression = "timestamp(" + record["metric"] + "{" + ",".join(key + "=" + json.dumps(value) for key, value in sorted(mapped.items())) + "})"
        need(params.get("query") == expression and params.get("step") == contract.to_dict()["metric_interval_s"]
             and params.get("start") == actual_window.start and params.get("end") == actual_window.end,
             "source_query_not_exact_full_phase_or_declared_labels")
        raw = decode_body(query, max_decoded_bytes=max_decoded_bytes)
        payload = json.loads(raw)
        need(payload.get("status") == "success" and payload.get("data", {}).get("resultType") == "matrix", "source_query_payload_invalid")
        series = payload["data"].get("result", [])
        need(len(series) == 1 and series[0].get("metric") == mapped, "source_query_series_ambiguous_or_unmatched")
        points, previous = [], None
        quantum = policy["timestamp_resolution_s"]
        for stamp, value in series[0].get("values", []):
            source_stamp = float(value)
            need(finite(stamp) and finite(source_stamp) and (previous is None or stamp > previous), "source_query_points_invalid")
            previous = stamp
            if actual_window.start-quantum <= stamp < actual_window.end:
                points.append((stamp, source_stamp))
        need(points, "source_points_empty")
        need(points[0][0] <= actual_window.start + quantum
             and points[-1][0] + params["step"] >= actual_window.end - quantum
             and all(b[0] - a[0] <= params["step"] + quantum for a,b in zip(points,points[1:])),
             "source_query_grid_has_unexplained_gaps")
        distinct = sorted({value for _, value in points})
        need(len(distinct) >= 2, "source_distinct_points_insufficient")
        need(all(b[1] >= a[1] for a,b in zip(points,points[1:])), "source_timestamps_reversed")
        gaps = [b-a for a,b in zip(distinct,distinct[1:])]
        ages = [stamp-value for stamp,value in points]
        need(all(-quantum - clock["uncertainty_s"] <= age
                 and age + clock["uncertainty_s"] <= policy["max_source_age_s"] for age in ages), "source_age_or_future_time_outside_policy")
        tail = record["right_boundary_query"]
        need(tail.get("status") == "ok" and tail.get("http_status") == 200
             and tail.get("parameters") == {**params,"start":actual_window.end}, "right_boundary_query_missing")
        tail_clock = _query_execution_clock(tail, clock, actual_window, "right_boundary_query")
        tail_payload=json.loads(decode_body(tail,max_decoded_bytes=max_decoded_bytes))
        tail_series=tail_payload.get("data",{}).get("result",[])
        need(tail_payload.get("status")=="success" and len(tail_series)==1 and tail_series[0].get("metric")==mapped
             and len(tail_series[0].get("values",[]))==1, "right_boundary_query_invalid")
        tail_at,tail_value=tail_series[0]["values"][0];tail_value=float(tail_value)
        need(finite(tail_at) and finite(tail_value) and abs(tail_at-actual_window.end)<=quantum
             and -quantum-clock["uncertainty_s"] <= tail_at-tail_value
             and tail_at-tail_value+clock["uncertainty_s"]<=policy["max_source_age_s"],"right_boundary_age_invalid")
        cadence=retained_cadence([value for _,value in points],actual_window,policy,right_boundary_value=tail_value)
        witnesses, matched = [], 0
        for snapshot in (before, after):
            witness = snapshot["exposition"]
            need(witness.get("origin") == path["exporter_origin"] and witness.get("path") == "/metrics"
                 and witness.get("status") == "ok" and witness.get("http_status") == 200, "independent_exporter_witness_missing")
            candidates = [row for row in exposition_points(decode_body(witness, max_decoded_bytes=max_decoded_bytes), record["metric"])
                          if row["labels"] == labels]
            need(len(candidates) == 1, "exporter_labels_not_unique_or_missing")
            point = candidates[0]
            need(point["timestamp_resolution_s"] == policy["timestamp_resolution_s"], "timestamp_resolution_not_declared")
            age = snapshot["ended_at_epoch_s"] - point["source_timestamp_s"]
            need(-quantum - clock["uncertainty_s"] <= age
                 and age + clock["uncertainty_s"] <= policy["max_source_age_s"], "exporter_witness_too_old_or_future")
            did_match = any(abs(point["source_timestamp_s"] - value) <= quantum for _,value in points)
            matched += did_match
            witnesses.append({"source_timestamp_s": point["source_timestamp_s"], "range_match": did_match,
                              "actual_capture_end_epoch_s": snapshot["ended_at_epoch_s"], "age_s": age})
        need(matched >= 1, "source_timestamp_preservation_not_crosschecked")
        
        # deltas stay separate facts; a FAIL on otherwise verified evidence is
        # an overall FAIL, never a quiet PASS.
        details = {"configured_export_interval_s":path["configured_export_interval_s"],
            "configured_scrape_interval_s":path["configured_scrape_interval_s"],
            "query_grid_step_s":params["step"],"timestamp_resolution_s":policy["timestamp_resolution_s"],
            "distinct_source_points":len(distinct),"retained_source_gaps_s":gaps,"source_ages_s":ages,
            "repeated_query_source_values":len(points)-len(distinct),"witnesses":witnesses,"rule_ref":policy["rule_ref"],
            "cadence":cadence,
            "cadence_gate":"retained_cadence_pass_is_a_necessary_condition_for_production_pass_r12_v1",
            "query_execution_clocks":{"source_query":source_clock,"right_boundary_query":tail_clock},
            "container_started_derivation_s":derived_starts,
            "snapshot_edge_gaps_s":{"head":before["ended_at_epoch_s"]-actual_window.start,"tail":actual_window.end-after["ended_at_epoch_s"]}}
        if cadence["status"] != "PASS":
            return {**result,"status":"FAIL","cadence_status":cadence["status"],
                "reason":"retained_cadence_gate_failed","details":details}
        return {**result,"status":"PASS","cadence_status":"PASS",
            "reason":"sampling_proof_chain_closed_retained_updates_within_declared_policy","details":details}
    except (ValueError,KeyError,TypeError,IndexError,AttributeError,OverflowError,zlib.error) as exc:
        return {**result,"status":"NOT_ASSESSED","reason":str(exc) if isinstance(exc,SamplingError) else "sampling_evidence_invalid"}
