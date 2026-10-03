"""Fixed-design labels, distinct from empirical interaction certification."""
from __future__ import annotations
import copy
import hashlib
import itertools
import json
import math
from datetime import datetime
from pathlib import Path


def require(ok, reason):
    if not ok:
        raise ValueError("design labels: " + reason)


def mapping_path():
    local = Path(__file__).with_name("design_labels.v1.json")
    return local if local.is_file() else Path(__file__).resolve().parents[2] / "configs/dataset/design_labels.v1.json"


def load_mapping(path=None):
    raw = (Path(path) if path is not None else mapping_path()).read_bytes()
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


def assign_roles(value, roles):
    if isinstance(value, dict):
        if "fault_instance_id" in value and "role" in value:
            fid = value["fault_instance_id"]
            require(fid in roles and not isinstance(value["role"], dict), "unknown instance or role shape")
            value["role"] = roles[fid]
        for child in value.values():
            assign_roles(child, roles)
    elif isinstance(value, list):
        for child in value:
            assign_roles(child, roles)


def planned_composition(faults):
    windows = [f["planned_window"] for f in faults]
    require(bool(windows), "empty planned faults")
    for w in windows:
        require(all(type(w[k]) in (int, float) and math.isfinite(w[k]) for k in ("start", "end"))
                and w["start"] < w["end"], "invalid planned window")
    require(len({(w["time_basis"], w["clock_id"]) for w in windows}) == 1, "incompatible planned clocks")
    bounds = [(w["start"], w["end"]) for w in windows]
    if len(bounds) == 1:
        return "none"
    if len(set(bounds)) == 1:
        return "simultaneous"
    if any(all(a <= c and b >= d for c, d in bounds) for a, b in bounds):
        return "nested"
    if any(max(a, c) < min(b, d) for (a, b), (c, d) in itertools.combinations(bounds, 2)):
        return "partial_overlap"
    return "staggered"


def overlap_windows(component_windows):
    result = {}
    for size in range(2, len(component_windows) + 1):
        for ids in itertools.combinations(sorted(component_windows), size):
            rows = [component_windows[fid] for fid in ids]
            key = "".join(ids)
            if any(not r.get("start_time") or not r.get("end_time") for r in rows):
                result[key] = None
                continue
            starts = [(datetime.fromisoformat(r["start_time"].replace("Z", "+00:00")), r["start_time"]) for r in rows]
            ends = [(datetime.fromisoformat(r["end_time"].replace("Z", "+00:00")), r["end_time"]) for r in rows]
            start, end = max(starts), min(ends)
            result[key] = None if start[0] > end[0] else {"start": start[1], "end": end[1],
                "duration_seconds": round((end[0] - start[0]).total_seconds(), 2)}
    return result


def apply_labels(metadata, groundtruth, annotation, mapping, mapping_sha256,
                 mapping_ref="../../../adapter/design_labels.v1.json"):
    """Return copies; match design+scenario+complete instance signatures strictly.

    Unknown designs and incomplete schemas fail explicitly rather than borrowing
    same-number historical labels. Pair times retain their original evidence/status.
    """
    indexed = {}
    for row in mapping["rows"]:
        key = (row["design_version"], row["scenario_id"])
        require(key not in indexed, "duplicate design key")
        indexed[key] = row
    identity = metadata["migration"]
    key = (identity["design_version"], identity["scenario_id"])
    require(key in indexed, "unmapped design version/scenario")
    row = indexed[key]
    require(metadata["ground_truth"] == groundtruth, "embedded/external GT mismatch")
    faults = metadata["faults"]
    fids = [f["fault_instance_id"] for f in faults]
    require(len(fids) == len(set(fids)) and set(fids) == set(row["roles"]) == set(row["fault_signatures"]), "incomplete instance mapping")
    for fault in faults:
        fid = fault["fault_instance_id"]
        require({k: fault[k] for k in ("fault_type", "target_component")} == row["fault_signatures"][fid], "instance signature mismatch")
        require(isinstance(row["roles"][fid], str) and bool(row["roles"][fid]), "empty role")
    require(all(isinstance(row[k], str) and row[k] for k in ("path_relation", "interaction_pattern")), "empty design label")
    require(annotation["attempt_id"] == identity["attempt_id"] and annotation["run_id"] == metadata["run_id"], "time evidence belongs to another attempt")
    pairs = copy.deepcopy(annotation.get("pair_relations") or [])
    actual = [tuple(sorted(p["instance_ids"])) for p in pairs]
    require(len(actual) == len(set(actual)) and all(len(ids) == 2 and len(set(ids)) == 2
            and set(ids) <= set(fids) for ids in actual), "foreign or duplicated pair instances")
    temporal = planned_composition(faults)
    provenance = {"annotation_basis": "design_assigned", "empirical_interaction_certification": False,
                  "design_key": {"design_version": key[0], "scenario_id": key[1]},
                  "mapping_ref": mapping_ref, "mapping_sha256": mapping_sha256,
                  "composition_source": "faults[].planned_window",
                  "composition_rule": "all_equal_simultaneous;one_contains_all_nested;any_positive_overlap_partial_overlap;otherwise_staggered;single_none",
                  "pair_time_source": "audit/annotation-draft-original.json",
                  "pair_time_semantics": "existing_same_attempt_operation_return_window_derived_not_physical_boundaries",
                  "time_certification": "original_status_and_evidence_retained_not_new_C1_certification",
                  "acceptance_and_QC_unchanged": True}
    meta, gt = copy.deepcopy(metadata), copy.deepcopy(groundtruth)
    for obj in (meta, gt):
        obj["category_relation"] = obj.get("category_relation", obj["composition_type"])
        obj["composition_type"] = temporal
        obj["path_relation"] = row["path_relation"]
        obj["interaction_pattern"] = row["interaction_pattern"]
        obj["pair_time_relations"] = [{k: copy.deepcopy(p[k]) for k in ("instance_ids", "time") if k in p} for p in pairs]
        obj["annotation_provenance"] = copy.deepcopy(provenance)
        obj["overlap_windows"] = overlap_windows(obj["component_fault_windows"])
        assign_roles(obj, row["roles"])
    meta["ground_truth"] = copy.deepcopy(gt)
    return meta, gt
