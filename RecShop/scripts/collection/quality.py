"""Evidence-bound Q1/Q2/Q3 evaluation, separate from collection and release.

No empirical thresholds are supplied by default. Pure evaluation can diagnose
synthetic/in-memory inputs, but live safety/sample claims require matching local
artifact contents. Labels, frozen plans and successful commands do not by
themselves prove physical faults, recovery, coverage or a formal sample.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
from typing import Any

from . import contract as c
from . import journal as j
from . import telemetry as t
from . import observability as obs

RULE_SCHEMA = "rq4-collect/quality-rules-v1"
EVIDENCE_SCHEMA = "rq4-collect/quality-evidence-v1"
P0_ASSESSMENT_SCHEMA = "rq4-collect/p0-assessment-m1p0-v1"
_PHASES = ("pre_fault", "during_fault", "post_recovery")
_MODALITIES = ("metrics", "traces", "logs")


def p0_classify(row):
    """minimum-use checks (acceptance v0.7 section 0) mapping of one quality check row.

    Returns ``(p0, blocking)`` where ``p0`` names the P0 a row belongs to
    ("experiment_semantics", "process_recoverable", "data_real", or
    "auxiliary") and ``blocking`` says whether this row's current status
    blocks that P0. The P0 aggregator may waive only named completeness and
    coverage rows after confirming usable observations in every phase. This
    classification never changes the row, group statuses, or any Q verdict.
    """
    name, group, status = row["check"], row["group"], row["status"]
    reason = str(row.get("reason", ""))
    if name == "final_annotation_and_review":
        return "auxiliary", False          # designed post-collection deferral (v0.7 section 0.3)
    if group == "PROTOCOL":
        return "data_real", status != "PASS"
    if group == "Q1":
        # Unassessed signal rows remain visible; without same-attempt alternate
        # mechanism evidence this admission layer cannot treat them as semantic PASS.
        return "experiment_semantics", status != "PASS"
    if group == "Q2":
        return "process_recoverable", status != "PASS"
    # Q3 data faces
    if name == "attempt_not_failed":
        return "process_recoverable", status != "PASS"
    if name == "actual_phase_windows":
        return "data_real", status != "PASS"
    if name == "three_phase_trace_presence":
        return "data_real", status == "FAIL"
    if ".metrics.sampling." in name:
        # Cadence/completeness gaps may be assessed against the real points
        # retained in this phase. Missing snapshots and unresolved identity or
        # lifecycle changes remain hard failures.
        return "data_real", (status in ("FAIL", "BLOCKED")
                              or (status == "NOT_ASSESSED"
                                  and reason not in ("sampling_pod_replacement_segmented_fixed_source_model_cannot_assess",
                                                     "sampling_boundary_capture_collided_with_declared_rollout_band")))
    if ".metrics.missing." in name:
        return "data_real", status == "FAIL"   # no row-level causal proof is consumed here
    if name.endswith(".coverage_file") or name.endswith(".run_clock_anchor") or name.endswith(".inventory_record_set"):
        return "data_real", status != "PASS"
    if name.endswith(".coverage"):
        return "data_real", status != "PASS"
    if any(name.endswith("." + suffix) for suffix in
           ("identity", "file", "declared_storage_files", "projection", "raw",
            "projection_raw_binding", "sources", "queries", "errors_and_truncation")):
        return "data_real", status != "PASS"
    if name.endswith(".finite_points") or ".metrics.series." in name or ".metrics.required." in name:
        return "data_real", status == "FAIL"
    return "data_real", status in ("FAIL", "BLOCKED")   # unknown Q3 data row: conservative


_OBSERVATION_CORE = ("identity", "file", "declared_storage_files", "projection", "raw",
                     "projection_raw_binding", "sources", "queries", "errors_and_truncation")
_UNPROVEN_SERIES_REASON = "source_identity_or_lifecycle_changed_without_proof"
_SAMPLING_GAP_REASONS = {"source_sampling_slower_than_declared_bound", "retained_cadence_gate_failed",
                         "sampling_pod_replacement_segmented_fixed_source_model_cannot_assess",
                         "sampling_boundary_capture_collided_with_declared_rollout_band"}
_PARTIAL_COVERAGE_REASONS = {"archive_attachment_gap_exceeds_policy",
                             "query_started_before_declared_ingestion_wait",
                             "metric_query_does_not_cover_declared_window_or_step",
                             "trace_limit_or_lookback_policy_not_met", "trace_search_slice_gap",
                             "trace_search_does_not_reach_phase_end"}
_PARTIAL_COVERAGE_NA_REASONS = {"independent_coverage_evidence_missing", "archive_gap_policy_not_predeclared",
                                "archive_coverage_identity_or_policy_unbound", "clock_mapping_uncertainty_exceeds_policy",
                                "missing_explicit_utc_monotonic_mapping", "query_timing_not_observed",
                                "trace_slice_provenance_missing", "trace_query_time_parameters_missing",
                                "required_old_pod_evidence_unavailable"}


def _phase_observation_scope(checks, bundles, phases):
    """List verified nonempty modalities and observed bounds in each phase."""
    status = {row["check"]: row["status"] for row in checks}
    result = {"phases": {}, "missing_phases": []}
    for phase in _PHASES:
        usable, missing, observed, excluded_metric_channels = [], [], {}, []
        for modality in _MODALITIES:
            bundle = bundles.get(phase, {}).get(modality)
            if bundle is None:
                missing.append(modality)
                continue
            if type(bundle) is not dict:
                continue
            prefix = phase + "." + modality
            if (not phases.get(phase) or status.get("actual_phase_windows") != "PASS"
                    or any(status.get(prefix + "." + item) != "PASS" for item in _OBSERVATION_CORE)):
                continue  # the corresponding Q3 identity/file/source failure remains blocking
            records = bundle.get("records")
            if type(records) is not list:
                continue
            queries = bundle.get("queries")
            if type(queries) is not list:
                continue
            unproven_channels = {
                _metric_query_index(row["check"]): row for row in checks
                if row.get("check", "").startswith(phase + ".metrics.sampling.")
                and row.get("status") == "NOT_ASSESSED"
                and row.get("reason") == _UNPROVEN_SERIES_REASON
                and _metric_query_index(row["check"]) is not None
            }
            valid_queries = []
            for query_index, query in enumerate(queries):
                if modality == "metrics" and query_index in unproven_channels:
                    sampling_row = unproven_channels[query_index]
                    excluded_metric_channels.append({"query_index": query_index,
                                                     "query_id": query.get("query_id") if type(query) is dict else None,
                                                     "check": sampling_row["check"],
                                                     "reason": sampling_row["reason"]})
                    continue
                if type(query) is not dict or query.get("status") != "ok":
                    continue
                source = query.get("source") if type(query.get("source")) is dict else {}
                source_id = query.get("source_id", source.get("source_id"))
                provenance = query.get("backend_provenance")
                if type(source_id) is str and type(provenance) is dict and provenance.get("source_id") == source_id:
                    valid_queries.append((query, source_id))
            query_sources = {source_id for _, source_id in valid_queries}
            stamps, observation_count = [], 0
            if modality == "metrics":
                stamps = [r["timestamp_epoch_s"] for r in records if type(r) is dict
                          and r.get("value_state") == "finite" and type(r.get("value")) in (int, float)
                          and math.isfinite(r["value"])
                          and any(q.get("query_id") == r.get("query_id") and source_id == r.get("source_id")
                                  for q, source_id in valid_queries)
                          and _within_stamp(r.get("timestamp_epoch_s"), phases[phase])]
            elif modality == "logs":
                for record in records:
                    if type(record) is not dict or record.get("source_id") not in query_sources:
                        continue
                    try:
                        stamp = float(record["timestamp_epoch_decimal_s"])
                    except (KeyError, ValueError, TypeError, OverflowError):
                        continue
                    if _within_stamp(stamp, phases[phase]):
                        stamps.append(stamp)
                observation_count = len(stamps)
            else:
                try:
                    presence = t.trace_phase_presence({phase: bundle})["phases"][phase]
                    if presence["nonempty"]:
                        observation_count = presence["in_window_structurally_valid_span_count"]
                        stamps = [max(phases[phase].start, r["start_time_us"] / 1e6)
                                  for r in records if type(r) is dict and r.get("in_window") is True]
                        stamps += [min(phases[phase].end, r["end_time_us"] / 1e6)
                                   for r in records if type(r) is dict and r.get("in_window") is True]
                except (ValueError, KeyError, TypeError, OverflowError):
                    stamps, observation_count = [], 0
            if modality != "traces":
                observation_count = len(stamps)
            if stamps:
                usable.append(modality)
                observed[modality] = {"count": observation_count,
                                      "epoch_range": [min(stamps), max(stamps)],
                                      "source_ids": sorted(query_sources)}
            else:
                missing.append(modality)
        result["phases"][phase] = {"available_modalities": usable,
                                    "missing_modalities": sorted(missing), "observed": observed,
                                    "excluded_metric_channels": excluded_metric_channels}
        if not usable:
            result["missing_phases"].append(phase)
    result["complete"] = not result["missing_phases"]
    return result


def _metric_query_index(name):
    marker = ".metrics.sampling."
    if marker not in name:
        marker = ".metrics.missing."
    if marker not in name:
        return None
    value = name.split(marker, 1)[1].split(".", 1)[0]
    return int(value) if value.isdigit() else None


def _partial_q3_gap_is_supported(row, scope):
    """Allow only explicit completeness/coverage gaps when a phase has data."""
    if type(scope) is not dict or type(scope.get("phases")) is not dict or row.get("status") == "PASS":
        return False
    name, reason = row.get("check", ""), row.get("reason", "")
    if name == "three_phase_trace_presence":
        return scope.get("complete") is True
    phase = next((p for p in _PHASES if name.startswith(p + ".")), None)
    if phase is None:
        return False
    phase_scope = scope["phases"].get(phase, {})
    available = phase_scope.get("available_modalities", [])
    if name.endswith((".metrics.identity", ".traces.identity", ".logs.identity")):
        modality = name.split(".")[-2]
        return modality in phase_scope.get("missing_modalities", []) and bool(available)
    if ".metrics.missing." in name:
        index = _metric_query_index(name)
        excluded = {item.get("query_index") for item in phase_scope.get("excluded_metric_channels", [])}
        if index in excluded:
            return reason == "metric_missing_fraction_exceeded" and {"traces", "logs"} <= set(available)
        return reason == "metric_missing_fraction_exceeded" and "metrics" in available
    if ".metrics.sampling." in name:
        index = _metric_query_index(name)
        excluded = {item.get("query_index") for item in phase_scope.get("excluded_metric_channels", [])}
        if reason == _UNPROVEN_SERIES_REASON and index in excluded:
            return {"traces", "logs"} <= set(available)
        return reason in _SAMPLING_GAP_REASONS and "metrics" in available
    if name.endswith((".coverage", ".coverage_file", ".run_clock_anchor", ".inventory_record_set")):
        if not available:
            return False
        if row.get("status") == "NOT_ASSESSED":
            return (reason in _PARTIAL_COVERAGE_NA_REASONS
                    or reason.startswith("archive_pod_replacement_gap_not_assessed:"))
        return reason in _PARTIAL_COVERAGE_REASONS
    return False


def _p0_assessment(checks, faults, phase_observations=None):
    """Derive the minimum-use checks admission verdict from the recorded checks only.

    v0.7 section 0.1: three P0s (experiment semantics correct; process
    complete and recoverable; data real, sufficient, unpolluted). Completeness
    and auxiliary-coverage failures are waived only when validated bundles
    still provide a real, source- and time-identifiable observation in every
    phase. Identity, raw binding, source, actual-window, and cross-phase
    conflict failures stay blocking. Original Q rows are untouched.
    """
    rows = [row for row in checks]
    # Keep faults for caller compatibility; declarations cannot replace phase
    # observations. Only the bundle-derived scope supports narrow waivers.
    p0s, exemptions = {}, []
    for p0 in ("experiment_semantics", "process_recoverable", "data_real"):
        p0s[p0] = {"status": "PASS", "blocking": [], "reason": "no_blocking_failures"}
    for row in rows:
        p0, blocking = p0_classify(row)
        if (phase_observations is None and row.get("group") == "Q3"
                and row.get("status") != "PASS" and p0 != "auxiliary"):
            blocking = True  # no bundle scope means no completeness waiver
        if p0 == "auxiliary" or not blocking:
            continue
        if p0 == "data_real" and _partial_q3_gap_is_supported(row, phase_observations):
            continue
        p0s[p0]["blocking"].append(row["check"])
    if type(phase_observations) is dict:
        for phase in phase_observations.get("missing_phases", []):
            p0s["data_real"]["blocking"].append("phase_observation_scope." + phase + ".no_usable_observation")
        exemptions.append({
            "kind": "verified_phase_observation_scope",
            "policy": "m1-p0-v0.7-phase-observability-v1",
            "phases": phase_observations.get("phases", {}),
            "tolerated_q3_rows": [
                {key: row.get(key) for key in ("check", "status", "reason", "details") if key in row}
                for row in rows if row.get("group") == "Q3"
                and _partial_q3_gap_is_supported(row, phase_observations)
            ],
        })
    for p0 in p0s.values():
        if p0["blocking"]:
            p0["status"], p0["reason"] = "FAIL", "blocking_failures_recorded"
    return {"schema_version": P0_ASSESSMENT_SCHEMA,
            "status": "PASS" if all(p["status"] == "PASS" for p in p0s.values()) else "FAIL",
            "p0s": p0s, "exemptions": exemptions,
            "policy": "m1-p0-v0.7-phase-observability-v1 section 0.1: verified phase ranges; Q1/Q2/Q3 statuses unchanged",
            "collection_release_eligible": False, "formal_release_eligible": False}


class QualityError(ValueError):
    pass


def _require(ok, message):
    if not ok:
        raise QualityError(message)


def _canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _equal(left, right):
    return _canonical(left) == _canonical(right)


def _number(value, *, minimum=0, maximum=None):
    _require(type(value) in (int, float) and math.isfinite(value) and value >= minimum
             and (maximum is None or value <= maximum), "invalid explicit numeric threshold")


def _positive_int(value):
    _require(type(value) is int and value > 0, "positive count required")


def _text(value):
    _require(type(value) is str and bool(value.strip()) and value == value.strip(), "explicit nonempty text required")


def _unresolved(value):
    if isinstance(value, dict):
        return any(_unresolved(v) for v in value.values())
    if isinstance(value, list):
        return any(_unresolved(v) for v in value)
    return isinstance(value, str) and any(k in value.upper() for k in ("UNRESOLVED", "NOT_FROZEN", "PILOT_TO_FREEZE", "TBD"))


def _known_non_observed(value):
    if type(value) is dict:
        if any(value.get(key) is True for key in ("synthetic", "fake_command", "mock")):
            return True
        kind, mode = value.get("evidence_kind"), value.get("execution_mode")
        if (type(kind) is str and kind in {"synthetic", "fake", "mock", "planned"}) or (type(mode) is str and mode in {"mock", "synthetic", "simulation", "virtual_clock_mock", "validation_only"}):
            return True
        return any(_known_non_observed(item) for item in value.values())
    return type(value) is list and any(_known_non_observed(item) for item in value)


@dataclass(frozen=True, init=False, repr=False)
class QualityRules:
    _json: str

    def __init__(self):
        raise TypeError("Use QualityRules.from_dict")

    @classmethod
    def from_dict(cls, value):
        fields = {"schema_version", "version", "status", "rule_refs", "injection", "recovery", "data"}
        _require(type(value) is dict and set(value) == fields and value["schema_version"] == RULE_SCHEMA, "invalid quality rule envelope")
        _require(not _unresolved(value), "unresolved rule values are not executable")
        _text(value["version"])
        _require(value["status"] in {"pilot_fixed", "formal_frozen"}, "explicit rule freezing status required")
        _require(set(value["rule_refs"]) == {"injection", "recovery", "completeness", "annotation"}, "rule references required")
        for ref in value["rule_refs"].values():
            _text(ref)
        _require(type(value["injection"]) is list and bool(value["injection"]), "per-instance injection rules required")
        for rule in value["injection"]:
            envelope = {"instance_id", "mechanism", "mechanism_version", "operation_source_id", "signal_kind", "signal"}
            if "evidence_mode" in rule:
                
                
                
                # six-field envelope -- their own carrier stream is unmasked
                # by construction.
                _require(rule["signal_kind"] in ("carrier_error_fraction", "carrier_p95_ratio")
                         and rule["evidence_mode"] in ("carrier_signal", "mechanism_or_bypass"),
                         "invalid injection rule evidence mode")
                envelope.add("evidence_mode")
            _require(set(rule) == envelope, "invalid injection rule")
            for name in ("instance_id", "mechanism", "mechanism_version", "operation_source_id", "signal_kind"):
                _text(rule[name])
            if rule["signal_kind"] == "metric_threshold":
                _validate_signal(rule["signal"], injection=True)
            else:
                _require(type(rule["signal"]) is dict, "unsupported signal must remain an explicit object")
                if rule["signal"].get("schema_version") == obs.CARRIER_RULE_SCHEMA:
                    obs.validate_carrier_rule(rule["signal"], rule["signal_kind"])
        _require(len({r["instance_id"] for r in value["injection"]}) == len(value["injection"]), "duplicate instance rule")
        recovery = value["recovery"]
        _require(set(recovery) == {"metric_rules", "component_sources", "checksum_source", "lock_source"}, "invalid recovery rules")
        _require(type(recovery["metric_rules"]) is list and bool(recovery["metric_rules"]), "recovery metric rules required")
        for rule in recovery["metric_rules"]:
            _validate_signal(rule, injection=False)
        _require(type(recovery["component_sources"]) is dict, "explicit component source map required")
        for entity, source in recovery["component_sources"].items():
            _text(entity)
            _text(source)
        _text(recovery["checksum_source"])
        _text(recovery["lock_source"])
        data = value["data"]
        data_fields = {"duration_tolerance_s", "max_missing_fraction", "max_nonfinite_fraction", "max_source_interval_s", "required_metric_queries", "required_sources", "require_human_review", "coverage_policy"}
        _require(data_fields <= set(data) and set(data) - data_fields <= {"phase_evidence_policy", "source_sampling_policy"}, "invalid data rules")
        if "phase_evidence_policy" in data:
            _require(data["phase_evidence_policy"] == "required_phase_observations_r10_v1", "unsupported_phase_evidence_policy")
        if "source_sampling_policy" in data:
            from . import sampling as source_sampling
            source_sampling.validate_policy(data["source_sampling_policy"])
            _require(data["source_sampling_policy"]["max_configured_interval_s"] <= data["max_source_interval_s"]
                     and data["source_sampling_policy"]["max_missing_fraction"] <= data["max_missing_fraction"],
                     "sampling_policy_cannot_relax_original_source_or_missing_bound")
        _number(data["duration_tolerance_s"])
        _number(data["max_missing_fraction"], maximum=1)
        _number(data["max_nonfinite_fraction"], maximum=1)
        _number(data["max_source_interval_s"], minimum=1e-12)
        _require(type(data["required_metric_queries"]) is list and bool(data["required_metric_queries"]), "explicit required metric query list required")
        for query in data["required_metric_queries"]:
            _require(type(query) is dict and set(query) == {"query_id", "source_id", "entity", "unit"}, "invalid required metric query")
            for field_value in query.values():
                _text(field_value)
        _require(type(data["require_human_review"]) is bool and type(data["required_sources"]) is dict
                 and set(data["required_sources"]) == set(_MODALITIES), "explicit source/review requirements required")
        for sources in data["required_sources"].values():
            _require(type(sources) is list and bool(sources) and len(set(sources)) == len(sources), "unique required sources required")
            for source in sources:
                _text(source)
        coverage = data["coverage_policy"]
        _require(type(coverage) is dict and set(coverage) in (
                     {"method", "min_ingestion_wait_s", "jaeger_min_lookback_s", "max_clock_uncertainty_s"},
                     {"method", "min_ingestion_wait_s", "jaeger_min_lookback_s", "max_clock_uncertainty_s", "archive_policy"})
                 and coverage["method"] == "bounded_query_window_v1" and set(coverage["min_ingestion_wait_s"]) == set(_MODALITIES), "explicit bounded coverage policy required")
        for field_value in coverage["min_ingestion_wait_s"].values():
            _number(field_value)
        _number(coverage["jaeger_min_lookback_s"])
        _number(coverage["max_clock_uncertainty_s"])
        if "archive_policy" in coverage:
            archive = coverage["archive_policy"]
            _require(type(archive) is dict and set(archive) == {"method", "rule_ref", "max_capture_start_gap_s", "max_attachment_gap_s"}
                     and archive["method"] == "bounded_log_archive_v1", "explicit_archive_coverage_policy_required")
            _text(archive["rule_ref"])
            _number(archive["max_capture_start_gap_s"])
            _number(archive["max_attachment_gap_s"])
        result = object.__new__(cls)
        object.__setattr__(result, "_json", _canonical(value))
        return result

    def to_dict(self):
        return json.loads(self._json)

    @property
    def sha256(self):
        return _sha(self._json.encode("utf-8"))


def _validate_signal(rule, *, injection):
    common = {"entity", "query_id", "source_id", "unit", "labels", "min_points"}
    extra = {"mode", "operator", "threshold", "required_fraction"} if injection else {"absolute_tolerance", "relative_tolerance"}
    _require(type(rule) is dict and set(rule) == common | extra, "invalid metric signal rule")
    for key in ("entity", "query_id", "source_id", "unit"):
        _text(rule[key])
    _require(type(rule["labels"]) is dict and all(type(k) is str and type(v) is str for k, v in rule["labels"].items()), "explicit metric label filter required")
    _positive_int(rule["min_points"])
    if injection:
        _require(rule["mode"] in {"absolute", "delta_from_pre"} and rule["operator"] in {"ge", "le"}, "unsupported metric criterion")
        _number(rule["threshold"], minimum=-1e300)
        _number(rule["required_fraction"], minimum=0, maximum=1)
        _require(rule["required_fraction"] > 0, "positive signal fraction required")
    else:
        _number(rule["absolute_tolerance"])
        _number(rule["relative_tolerance"])


@dataclass(frozen=True, init=False, repr=False)
class VerifiedArtifacts:
    _json: str

    def __init__(self):
        raise TypeError("Use verify_artifacts")

    def to_dict(self):
        return json.loads(self._json)


def verify_artifacts(contract: c.RunContract, output_policy: j.OutputPolicy, descriptors: tuple[dict, ...], *,
                     max_file_bytes: int, max_total_bytes: int) -> VerifiedArtifacts:
    """Read-only JSON verification under J's explicit resolved attempt boundary.

    Local protected-workspace assumption matches J; not an adversarial filesystem
    sandbox. Every evaluated object must also match its verified parsed content.
    """
    _positive_int(max_file_bytes)
    _positive_int(max_total_bytes)
    _require(isinstance(contract, c.RunContract) and isinstance(output_policy, j.OutputPolicy), "validated contract/output policy required")
    _require(type(descriptors) is tuple and bool(descriptors), "explicit artifact descriptors required")
    attempt = output_policy.attempt_path(contract).resolve(strict=True)
    _require(attempt.is_dir(), "attempt directory missing")
    identity = (attempt.stat().st_dev, attempt.stat().st_ino)
    entries, total = {}, 0
    for descriptor in descriptors:
        _require(type(descriptor) is dict and set(descriptor) == {"artifact_id", "relative_path", "sha256"}, "invalid artifact descriptor")
        aid = descriptor["artifact_id"]
        _text(aid)
        _require(aid not in entries, "duplicate artifact ID")
        _text(descriptor["relative_path"])
        relative = Path(descriptor["relative_path"])
        _require(not relative.is_absolute() and ".." not in relative.parts and ":" not in descriptor["relative_path"]
                 and relative.suffix.lower() in {".json", ".bin"}, "unsafe or unsupported artifact path")
        path = (attempt / relative).resolve(strict=False)
        _require(path.is_relative_to(attempt) and path != attempt, "artifact escaped attempt root")
        row = {**descriptor, "status": "FAIL", "reason": None, "data": None}
        entries[aid] = row
        try:
            with path.open("rb") as handle:
                before = os.fstat(handle.fileno())
                if before.st_size > max_file_bytes or total + before.st_size > max_total_bytes:
                    row["reason"] = "size_limit"
                    continue
                raw = handle.read(max_file_bytes + 1)
                after = os.fstat(handle.fileno())
            total += len(raw)
            if len(raw) > max_file_bytes or total > max_total_bytes:
                row["reason"] = "size_limit"
                continue
            if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                row["reason"] = "file_changed_during_read"
                continue
            row["actual_sha256"] = _sha(raw)
            row["bytes"] = len(raw)
            if row["actual_sha256"] != descriptor["sha256"]:
                row["reason"] = "hash_mismatch"
                continue
            if relative.suffix.lower() == ".bin":
                row.update(status="PASS", reason=None, format="bytes", bytes_base64=base64.b64encode(raw).decode("ascii"))
                _require((attempt / relative).resolve(strict=False) == path, "artifact alias changed")
                continue
            def pairs(items):
                result = {}
                for key, value in items:
                    _require(key not in result, "duplicate JSON key")
                    result[key] = value
                return result
            parsed = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                                parse_constant=lambda _: (_ for _ in ()).throw(QualityError("nonfinite JSON")))
            _require(type(parsed) in (dict, list), "artifact requires structured JSON")
            row.update(status="PASS", reason=None, data=parsed, format="json")
        except (OSError, ValueError, UnicodeError):
            row["reason"] = "missing_unreadable_or_invalid_json"
        _require((attempt / relative).resolve(strict=False) == path, "artifact alias changed")
    output_policy.check_root()
    _require(output_policy.attempt_path(contract).resolve() == attempt and
             (attempt.stat().st_dev, attempt.stat().st_ino) == identity, "attempt identity changed")
    result = object.__new__(VerifiedArtifacts)
    object.__setattr__(result, "_json", _canonical({"contract_sha256": contract.sha256, "attempt_root": str(attempt), "artifacts": entries}))
    return result


def _artifact_matches(inventory, artifact_id, value):
    if inventory is None:
        return "NOT_ASSESSED", "artifact_verification_not_supplied"
    row = inventory["artifacts"].get(artifact_id)
    if row is None or row["status"] != "PASS":
        return "BLOCKED", "artifact_missing_or_not_verified"
    if _canonical(row["data"]) != _canonical(value):
        return "FAIL", "evaluated_object_differs_from_verified_file"
    return "PASS", "verified_file_content_matches"


def _stored_files_match(bundle, inventory):
    if inventory is None or type(bundle) is not dict:
        return False
    entries = list(inventory["artifacts"].values())
    def find(path, digest):
        if type(path) is not str:
            return None
        matches = [row for row in entries if row["status"] == "PASS" and Path(row["relative_path"]) == Path(path) and row.get("actual_sha256") == digest]
        return matches[0] if len(matches) == 1 else None
    manifest = bundle.get("manifest", {})
    if manifest.get("persistence") == "written":
        row = find(manifest.get("file_path"), manifest.get("file_sha256"))
        if row is None or not _equal(row.get("data"), bundle.get("records")):
            return False
    for raw in bundle.get("raw_responses", []):
        if raw.get("persistence") == "written":
            row = find(raw.get("file_path"), raw.get("file_sha256"))
            if row is None or row.get("bytes_base64") != raw.get("base64"):
                return False
    return True


def _identity(record, contract, *, phase=None, entity=None, source=None):
    if type(record) is not dict:
        return False
    context = contract.context.to_dict()
    expected = {"contract_sha256": contract.sha256, "run_id": context["run_id"], "attempt_id": context["attempt_id"]}
    if phase is not None:
        expected["phase"] = phase
    if entity is not None:
        expected["entity"] = entity
    if source is not None:
        expected["source_id"] = source
    return all(record.get(k) == v for k, v in expected.items())


def _window(value, contract):
    try:
        result = c.TimeWindow.from_dict(value)
        if result.time_basis != "unix_epoch" or result.clock_id != contract.context.to_dict()["clock_id"]:
            return None
        return result
    except (ValueError, TypeError):
        return None


def _within_stamp(stamp, window):
    return window is not None and type(stamp) in (int, float) and math.isfinite(stamp) and window.start <= stamp < window.end


def _status(checks):
    states = {check["status"] for check in checks}
    return "FAIL" if "FAIL" in states else "BLOCKED" if "BLOCKED" in states else "NOT_ASSESSED" if "NOT_ASSESSED" in states else "PASS"


def evaluate_quality(contract: c.RunContract, rules: QualityRules, evidence: dict, *,
                     artifacts: VerifiedArtifacts | None = None) -> dict:
    """Compute mechanism/recovery/data checks; never performs network or writes.

    Evidence flags are a producer provenance contract, not a signature. Optional
    file verification binds bytes/objects, not the physical truth of a producer.
    Synthetic evidence is diagnostically evaluated but cannot certify cleanliness
    or sample eligibility. Final release is outside this module.
    """
    _require(isinstance(contract, c.RunContract) and isinstance(rules, QualityRules) and type(evidence) is dict, "validated rules/contract/evidence required")
    evidence = json.loads(_canonical(evidence))
    policy, plan = rules.to_dict(), contract.to_dict()
    inventory = artifacts.to_dict() if isinstance(artifacts, VerifiedArtifacts) else None
    checks = []

    def add(group, name, status, reason, *, refs=(), **details):
        checks.append({"group": group, "check": name, "status": status, "reason": reason,
                       "evidence_refs": list(refs), "details": details})

    def test(group, name, condition, reason, **details):
        add(group, name, "PASS" if condition else "FAIL", "verified" if condition else reason, **details)
        return bool(condition)

    def bound(group, name, aid, value):
        status, reason = _artifact_matches(inventory, aid, value)
        add(group, name, status, reason, refs=(aid,) if isinstance(aid, str) else ())
        return status == "PASS"

    matching_rules = (rules.sha256 == plan["context"]["fingerprints"]["quality_rules"] and
                      all(policy["rule_refs"][key] == plan["rules"][key] for key in policy["rule_refs"]))
    test("PROTOCOL", "rule_binding", matching_rules, "rule_hash_or_reference_mismatch")
    test("PROTOCOL", "freeze_state", plan["purpose"] != "formal" or policy["status"] == "formal_frozen", "formal_rules_not_frozen")
    identity_ok = _identity(evidence, contract) and evidence.get("schema_version") == EVIDENCE_SCHEMA
    test("PROTOCOL", "evidence_identity", identity_ok, "evidence_attempt_or_contract_mismatch")
    if inventory is not None:
        test("PROTOCOL", "artifact_attempt", inventory["contract_sha256"] == contract.sha256, "artifact_attempt_mismatch")
    provenance_observed = evidence.get("evidence_kind") == "observed"
    add("PROTOCOL", "observed_provenance", "PASS" if provenance_observed else "BLOCKED", "observed_producer_claim" if provenance_observed else "synthetic_or_planned_evidence_not_live")
    known_fake = _known_non_observed(evidence)
    add("PROTOCOL", "nested_provenance", "BLOCKED" if known_fake else "PASS", "explicit_fake_or_planned_marker_present" if known_fake else "no_contradictory_provenance_marker")
    evidence_bound = bound("PROTOCOL", "evidence_file", evidence.get("artifact_id"), evidence)
    for field, shape in (("actual_phases", dict), ("bundles", dict), ("instance_windows", dict),
                         ("operations", list), ("components", list), ("checksums", dict), ("annotation", dict),
                         ("bundle_artifact_ids", dict), ("coverage", dict)):
        if type(evidence.get(field)) is not shape:
            add("PROTOCOL", field + ".shape", "FAIL", "missing_or_invalid_evidence_structure")
            evidence[field] = shape()
    phases = {phase: _window(evidence.get("actual_phases", {}).get(phase), contract) for phase in _PHASES}
    phase_ok = all(phases.values()) and all(phases[a].end <= phases[b].start for a, b in zip(_PHASES, _PHASES[1:]))
    test("Q3", "actual_phase_windows", phase_ok, "missing_invalid_or_overlapping_actual_phases")
    if phase_ok:
        for phase, window in phases.items():
            desired = plan["phases"][phase]["end"] - plan["phases"][phase]["start"]
            test("Q3", phase + ".duration", abs(window.end - window.start - desired) <= policy["data"]["duration_tolerance_s"], "phase_duration_outside_tolerance")
    bundles = evidence.get("bundles", {})

    def metric_values(phase, rule, subset=None):
        bundle = bundles.get(phase, {}).get("metrics")
        if type(bundle) is not dict or not _bundle_identity(bundle, contract, phase, "metrics", phases[phase]):
            return [], "missing_or_foreign_metric_bundle"
        query = [q for q in bundle.get("queries", []) if q.get("query_id") == rule["query_id"] and q.get("source_id") == rule["source_id"]]
        if len(query) != 1 or query[0].get("status") != "ok":
            return [], "metric_query_missing_or_failed"
        if query[0].get("backend_provenance", {}).get("source_id") != rule["source_id"] or _known_non_observed(query[0]):
            return [], "metric_source_provenance_unverified_or_fake"
        if bundle.get("manifest", {}).get("projection_sha256") != _sha(_canonical(bundle.get("records", [])).encode("utf-8")):
            return [], "metric_projection_hash_mismatch"
        values, seen = [], set()
        raw_by_ref = {}
        try:
            for item in bundle.get("raw_responses", []):
                raw = base64.b64decode(item["base64"], validate=True)
                if (_sha(raw) != item["retained_sha256"] or _sha(raw) != item["received_sha256"]
                        or len(raw) != item["retained_bytes"] or len(raw) != item["received_bytes"] or item["retained_truncated"]):
                    return [], "metric_raw_integrity_failed"
                raw_by_ref[item["ref"]] = raw
        except (ValueError, KeyError, TypeError):
            return [], "metric_raw_integrity_failed"
        for record in bundle.get("records", []):
            if any(record.get(key) != rule[key] for key in ("query_id", "source_id", "entity", "unit")):
                continue
            if any(record.get("labels", {}).get(k) != v for k, v in rule["labels"].items()):
                continue
            stamp = record.get("timestamp_epoch_s")
            if not _within_stamp(stamp, subset or phases[phase]):
                continue
            if not _record_matches_raw("metrics", record, raw_by_ref):
                return [], "metric_projection_disagrees_with_raw"
            if stamp in seen:
                return [], "ambiguous_duplicate_metric_timestamp"
            seen.add(stamp)
            value = record.get("value")
            if record.get("value_state") == "finite" and type(value) in (int, float) and math.isfinite(value):
                values.append(value)
        return values, None

    faults = {f.fault_instance_id: f.to_dict() for f in contract.faults}
    injection_rules = {r["instance_id"]: r for r in policy["injection"]}
    test("Q1", "per_instance_rules", set(injection_rules) == set(faults), "missing_or_extra_instance_rule")
    operations = [op for op in evidence.get("operations", []) if type(op) is dict]
    observed_windows = evidence.get("instance_windows", {})
    def operation_time(op, action):
        stamp, phase = op.get("timestamp_epoch_s"), op.get("phase")
        if type(stamp) not in (int, float) or not math.isfinite(stamp) or not phase_ok:
            return False
        if phase == "during_fault":
            return _within_stamp(stamp, phases["during_fault"])
        if action == "inject" and phase == "injection_transition":
            return phases["pre_fault"].end <= stamp <= phases["during_fault"].start
        if action == "recover" and phase == "recovery_transition":
            return phases["during_fault"].end <= stamp <= phases["post_recovery"].start
        return action == "recover" and phase == "post_recovery" and stamp == phases["post_recovery"].start
    for fid, fault in faults.items():
        rule = injection_rules.get(fid)
        rows = [op for op in operations if op.get("instance_id") == fid and op.get("action") == "inject"]
        op = rows[0] if len(rows) == 1 else None
        op_ok = (op is not None and rule is not None and _identity(op, contract, entity=fault["normalized_root_entity"], source=rule["operation_source_id"])
                 and op.get("evidence_kind") == "observed" and type(op.get("return_code")) is int and op["return_code"] == 0
                 and op.get("mechanism") == fault["mechanism"] and op.get("mechanism_version") == fault["mechanism_version"]
                 and _equal(op.get("raw_target"), fault["raw_target"]) and _equal(op.get("effective_parameters"), fault["parameters"])
                 and type(op.get("resource_uid")) is str and bool(op["resource_uid"])
                 and type(op.get("baseline_fields")) is dict and bool(op["baseline_fields"])
                 and type(op.get("desired_fields")) is dict and bool(op["desired_fields"]) and _equal(op.get("observed_fields"), op["desired_fields"])
                 and operation_time(op, "inject"))
        test("Q1", fid + ".operation", op_ok, "missing_failed_mismatched_or_unobserved_injection")
        actual = _window(observed_windows.get(fid), contract)
        contained = (actual is not None and phase_ok and phases["pre_fault"].end <= actual.start < phases["during_fault"].end
                     and phases["during_fault"].start < actual.end <= phases["post_recovery"].start)
        signal_window = c.TimeWindow(max(actual.start, phases["during_fault"].start), min(actual.end, phases["during_fault"].end),
                                     actual.time_basis, actual.clock_id) if contained else None
        test("Q1", fid + ".window", contained, "missing_or_outside_actual_fault_window")
        if rule is None:
            continue
        if not test("Q1", fid + ".mechanism_rule", (rule["mechanism"], rule["mechanism_version"]) == (fault["mechanism"], fault["mechanism_version"]), "rule_mechanism_mismatch"):
            continue
        if rule["signal_kind"] != "metric_threshold":
            if rule["signal_kind"] in {"carrier_error_fraction", "carrier_p95_ratio"}:
                status, reason, details = obs.evaluate_carrier_signal(contract, rule, evidence, inventory,
                                                                     signal_window, _known_non_observed)
                mode = rule.get("evidence_mode")
                if mode == "mechanism_or_bypass" and status == "FAIL":
                    
                    # potentially masked (assembly COMBO_LEG_STREAM_BINDINGS
                    # rationale); the FAIL is the expected masked shape and
                    
                    # checks (Q1.<fid>.operation/.window) plus bypass or
                    # mechanism-specific evidence -- degraded and explicitly
                    # distinguishable, never silently read as "injection
                    # broken".
                    reason = "masked_root_carrier_statistic_below_threshold_mechanism_or_bypass_evidence_applies"
                add("Q1", fid + ".signal", status, reason, evidence_mode=mode, **details)
                continue
            if rule["signal_kind"] in {"pod_restart_delta", "config_state_marker"}:
                status, reason, details = obs.evaluate_state_signal(contract, rule, evidence, inventory,
                                                                   signal_window, _known_non_observed)
                refs = details.pop("evidence_refs", [])
                add("Q1", fid + ".signal", status, reason, refs=refs, **details)
                continue
            add("Q1", fid + ".signal", "NOT_ASSESSED", "unsupported_signal_kind", signal_kind=rule["signal_kind"])
            continue
        signal = rule["signal"]
        if not test("Q1", fid + ".signal_entity", signal["entity"] == fault["normalized_root_entity"], "signal_belongs_to_another_root"):
            continue
        values, error = metric_values("during_fault", signal, signal_window)
        baseline, baseline_error = metric_values("pre_fault", signal) if signal["mode"] == "delta_from_pre" else ([], None)
        enough = contained and not error and len(values) >= signal["min_points"] and (signal["mode"] != "delta_from_pre" or (not baseline_error and len(baseline) >= signal["min_points"]))
        if not enough:
            add("Q1", fid + ".signal", "NOT_ASSESSED", error or baseline_error or "insufficient_real_signal_points", observed_points=len(values))
            continue
        offset = statistics.median(baseline) if baseline else 0
        hits = sum(v - offset >= signal["threshold"] if signal["operator"] == "ge" else v - offset <= signal["threshold"] for v in values)
        test("Q1", fid + ".signal", hits / len(values) >= signal["required_fraction"], "predeclared_physical_signal_not_met", measured_fraction=hits / len(values), points=len(values), pre_median=offset if baseline else None)

    components = [row for row in evidence.get("components", []) if type(row) is dict]
    def phase_clock_bound(snapshot):
        row = {} if inventory is None else inventory["artifacts"].get("run-record", {})
        run = row.get("data", {})
        return (row.get("status") == "PASS" and _identity(run, contract)
                and snapshot.get("clock_mapping") == {**run.get("clock_mapping", {}), "clock_id": plan["context"]["clock_id"]})
    service_entities = set(contract.root_entities) - {"host", "mysql_items_lock"}
    test("Q2", "component_rule_coverage", set(policy["recovery"]["component_sources"]) >= service_entities, "component_rules_missing_for_service_roots")
    for fid, fault in faults.items():
        rows = [op for op in operations if op.get("instance_id") == fid and op.get("action") == "recover"]
        op = rows[0] if len(rows) == 1 else None
        applied = [row for row in operations if row.get("instance_id") == fid and row.get("action") == "inject"]
        initial = applied[0] if len(applied) == 1 else {}
        rule = injection_rules.get(fid)
        recovery_time_ok = op is not None and operation_time(op, "recover")
        ok = (op is not None and rule is not None and _identity(op, contract, entity=fault["normalized_root_entity"], source=rule["operation_source_id"])
              and op.get("evidence_kind") == "observed" and type(op.get("return_code")) is int and op["return_code"] == 0
              and op.get("mechanism") == fault["mechanism"] and op.get("mechanism_version") == fault["mechanism_version"]
              and _equal(op.get("raw_target"), fault["raw_target"]) and op.get("resource_uid") == initial.get("resource_uid")
              and type(op.get("baseline_fields")) is dict and bool(op["baseline_fields"]) and _equal(op["baseline_fields"], initial.get("baseline_fields"))
              and _equal(op.get("observed_fields"), op["baseline_fields"]) and recovery_time_ok)
        test("Q2", fid + ".restored_fields", ok, "restore_operation_or_readback_not_verified")
    for entity, source in policy["recovery"]["component_sources"].items():
        rows = [row for row in components if row.get("entity") == entity]
        item = rows[0] if len(rows) == 1 else {}
        fields = item.get("fields", {})
        counts_valid = all(type(fields.get(k)) is int and fields[k] >= 0 for k in
                           ("desired_replicas", "ready_replicas", "available_replicas", "generation", "observed_generation"))
        healthy = (counts_valid and fields["desired_replicas"] > 0 and fields["ready_replicas"] >= fields["desired_replicas"]
                   and fields["available_replicas"] >= fields["desired_replicas"] and fields["observed_generation"] >= fields["generation"])
        phase_receipt_ok = ("phase_evidence_policy" not in policy["data"] or "phase_observation_ref" in item)
        if "phase_observation_ref" in item:
            receipt = {} if inventory is None else inventory["artifacts"].get(item["phase_observation_ref"], {})
            snapshot = receipt.get("data", {})
            raw_item = {key: value for key, value in item.items() if key != "phase_observation_ref"}
            phase_receipt_ok = (receipt.get("status") == "PASS" and _identity(snapshot, contract, phase="post_recovery")
                                and snapshot.get("schema_version") == obs.SCHEMA
                                and snapshot.get("actual_window") == evidence.get("actual_phases", {}).get("post_recovery")
                                and snapshot.get("workers_joined") is True
                                and snapshot.get("readback", {}).get("workers_joined") is True
                                and phase_clock_bound(snapshot)
                                and any(_equal(row, raw_item) for row in snapshot.get("readback", {}).get("components", [])))
        test("Q2", entity + ".component", phase_receipt_ok and _identity(item, contract, phase="post_recovery", entity=entity, source=source)
             and item.get("evidence_kind") == "observed" and type(item.get("return_code")) is int and item["return_code"] == 0
             and _within_stamp(item.get("timestamp_epoch_s"), phases["post_recovery"]) and healthy, "component_not_ready_or_readback_unverified")
    recovery_entities = {rule["entity"] for rule in policy["recovery"]["metric_rules"]}
    test("Q2", "metric_rule_coverage", recovery_entities >= set(contract.root_entities), "recovery_metric_rules_missing_root")
    for index, rule in enumerate(policy["recovery"]["metric_rules"]):
        pre, e1 = metric_values("pre_fault", rule)
        post, e2 = metric_values("post_recovery", rule)
        if e1 or e2 or min(len(pre), len(post)) < rule["min_points"]:
            add("Q2", "metric." + str(index), "NOT_ASSESSED", e1 or e2 or "insufficient_pre_post_points")
            continue
        baseline, recovered = statistics.median(pre), statistics.median(post)
        tolerance = rule["absolute_tolerance"] + rule["relative_tolerance"] * abs(baseline)
        test("Q2", "metric." + str(index), abs(recovered - baseline) <= tolerance, "recovery_metric_outside_predeclared_tolerance", pre_median=baseline, post_median=recovered, allowed_delta=tolerance)
    checksum = evidence.get("checksums", {})
    checksum_ok = (_identity(checksum, contract, source=policy["recovery"]["checksum_source"])
                   and checksum.get("evidence_kind") == "observed" and type(checksum.get("return_code")) is int and checksum["return_code"] == 0
                   and _within_stamp(checksum.get("pre_at_s"), phases["pre_fault"])
                   and _within_stamp(checksum.get("post_at_s"), phases["post_recovery"]))
    checksum_ok = checksum_ok and ("phase_evidence_policy" not in policy["data"] or "phase_observation_refs" in checksum)
    if "phase_observation_refs" in checksum:
        refs = checksum["phase_observation_refs"]
        expected = ["pre_fault.phase-observations", "post_recovery.phase-observations"]
        checksum_ok = checksum_ok and refs == expected
        for phase, aid, suffix in zip(("pre_fault", "post_recovery"), expected, ("pre", "post")):
            receipt = {} if inventory is None else inventory["artifacts"].get(aid, {})
            snapshot = receipt.get("data", {})
            database = snapshot.get("readback", {}).get("database") or {}
            checksum_ok = (checksum_ok and receipt.get("status") == "PASS" and _identity(snapshot, contract, phase=phase)
                           and snapshot.get("schema_version") == obs.SCHEMA
                           and snapshot.get("actual_window") == evidence.get("actual_phases", {}).get(phase)
                           and snapshot.get("workers_joined") is True and snapshot.get("readback", {}).get("workers_joined") is True
                           and database.get("workers_joined") is True
                           and database.get("evidence_kind") == "observed" and type(database.get("return_code")) is int
                           and database["return_code"] == 0
                           
                           # check of observability.py consumer face -- the
                           # producer records benign shared/intention-shared
                           # business locks it observed (empty only when the
                           # workload held none); exclusive-family entries
                           # fail here. Kept byte-equal in predicate to the
                           # observability face so the two consumers cannot
                           # diverge.
                           and all((entry.get("lock_type") in ("SHARED_READ", "SHARED_WRITE"))
                                   if entry.get("kind") == "metadata"
                                   else entry.get("lock_mode") in ("IS", "S", "S,REC_NOT_GAP", "REC_NOT_GAP")
                                   for entry in (database.get("target_metadata_locks") or []) + (database.get("target_data_locks") or []))
                           and type(database.get("server_uuid")) is str and bool(database["server_uuid"])
                           and _within_stamp(database.get("started_at_epoch_s"), phases[phase])
                           and database.get("started_at_epoch_s") <= database.get("timestamp_epoch_s", -1)
                           and _identity(database, contract, phase=phase,
                               source=policy["recovery"]["checksum_source"])
                           and phase_clock_bound(snapshot)
                           and database.get("server_uuid") == checksum.get("server_uuid")
                           and database.get("timestamp_epoch_s") == checksum.get(suffix + "_at_s")
                           and all(database.get("checksums", {}).get(table) == checksum.get("tables", {}).get(table, {}).get(suffix)
                                   for table in ("items", "inventory")))
    tables = checksum.get("tables", {}) if type(checksum.get("tables")) is dict else {}
    for table in ("items", "inventory"):
        values = tables.get(table, {}) if type(tables.get(table)) is dict else {}
        test("Q2", table + ".checksum", checksum_ok and type(values.get("pre")) is str and bool(values["pre"])
             and values.get("post") == values["pre"], "checksum_missing_unverified_or_changed")
    lock_faults = [fid for fid, f in faults.items() if f["fault_type"] == "db_table_lock"]
    if lock_faults:
        locks = evidence.get("locks", {}) if type(evidence.get("locks")) is dict else {}
        expected_connections = {connection for op in operations if op.get("action") == "inject" and op.get("instance_id") in lock_faults
                                and type(op.get("lock_connection_ids")) is list
                                for connection in op.get("lock_connection_ids", [])}
        ok = (_identity(locks, contract, phase="post_recovery", source=policy["recovery"]["lock_source"])
              and locks.get("evidence_kind") == "observed" and _within_stamp(locks.get("timestamp_epoch_s"), phases["post_recovery"])
              and type(locks.get("return_code")) is int and locks["return_code"] == 0 and locks.get("owner_id") == contract.context.to_dict()["owner_id"]
              and bool(expected_connections) and all(type(v) is int and v > 0 for v in expected_connections)
              and type(locks.get("examined_connection_ids")) is list and expected_connections <= set(locks["examined_connection_ids"])
              and type(locks.get("active_owned_locks")) is list and not locks["active_owned_locks"])
        test("Q2", "owned_database_locks", ok, "lock_release_unknown_or_lock_still_held")
    else:
        add("Q2", "owned_database_locks", "PASS", "not_applicable_no_db_lock_instance")

    _evaluate_data(contract, policy, evidence, phases, bundles, inventory, add, test, bound)
    groups = {group: {"status": _status([x for x in checks if x["group"] == group]),
                      "checks": [x for x in checks if x["group"] == group]} for group in ("PROTOCOL", "Q1", "Q2", "Q3")}
    recovery_artifacts_ok = all(_artifact_matches(inventory, evidence.get("bundle_artifact_ids", {}).get(phase, {}).get("metrics"),
                                               bundles.get(phase, {}).get("metrics"))[0] == "PASS" and _stored_files_match(bundles.get(phase, {}).get("metrics"), inventory)
                               for phase in ("pre_fault", "post_recovery"))
    live_bound = (identity_ok and matching_rules and provenance_observed and not known_fake and evidence_bound and recovery_artifacts_ok
                  and inventory is not None and inventory["contract_sha256"] == contract.sha256)
    system_clean = "CLEAN" if live_bound and groups["Q2"]["status"] == "PASS" else "NOT_CONFIRMED"
    sample_status = "ELIGIBLE_CANDIDATE" if all(group["status"] == "PASS" for group in groups.values()) else "REJECTED" if any(group["status"] == "FAIL" for group in groups.values()) else "BLOCKED"
    
    # review. Keep the original Q3, sample eligibility and human review gate
    # unchanged. A release consumer must explicitly opt into this new scope.
    technical_checks = [row for row in checks if row["check"] != "final_annotation_and_review"]
    technical_groups = {name: _status([row for row in technical_checks if row["group"] == name])
                        for name in ("PROTOCOL", "Q1", "Q2", "Q3")}
    technical = {"schema_version": "rq4-collect/technical-assessment-r10-v1",
                 "status": _status(technical_checks), "groups": technical_groups,
                 "phase_evidence_policy": policy["data"].get("phase_evidence_policy", "legacy_optional_phase_refs"),
                 "deferred_checks": ["final_annotation_and_review"],
                 "collection_release_eligible": False, "formal_release_eligible": False,
                 "final_annotation_review_status": next(row["status"] for row in checks
                                                         if row["check"] == "final_annotation_and_review"),
                 "scope": "technical_evidence_only_not_collection_authorization_or_final_human_labels"}
    phase_observations = _phase_observation_scope(checks, bundles, phases)
    p0 = _p0_assessment(checks, list(faults.values()), phase_observations)
    return {"schema_version": "rq4-collect/quality-report-v1", "contract_sha256": contract.sha256, "rules_sha256": rules.sha256,
            "run_id": plan["context"]["run_id"], "attempt_id": plan["context"]["attempt_id"], "groups": groups,
            "system_clean": system_clean, "sample_eligibility": sample_status, "sample_purpose": plan["purpose"],
            "formal_release_eligible": False, "release_gate": "NOT_EVALUATED",
            "evidence_kind": evidence.get("evidence_kind"), "technical_assessment": technical,
            "p0_assessment": p0,
            "report_semantics": "producer_claims_and_verified_bytes_not_authenticated_physical_truth"}


def _bundle_identity(bundle, contract, phase, modality, window):
    if type(bundle) is not dict or window is None:
        return False
    scope = bundle.get("scope", {})
    ctx = contract.context.to_dict()
    return (bundle.get("schema_version") == t.SCHEMA_VERSION and bundle.get("modality") == modality
            and _identity(scope, contract, phase=phase) and scope.get("kube_context") == ctx["kube_context"]
            and scope.get("namespace") == ctx["namespace"] and scope.get("actual_window") == window.to_dict())


def _record_matches_raw(modality, record, raw_by_ref):
    try:
        if modality == "metrics":
            raw = json.loads(raw_by_ref[record["raw_ref"]])
            series = raw["data"]["result"][record["series_index"]]
            point = series["values"][record["point_index"]]
            if raw.get("status") != "success" or not _equal(series["metric"], record["labels"]) or not _equal(point[0], record["timestamp_epoch_s"]) or not _equal(point[1], record["raw_value"]):
                return False
            try:
                value = float(point[1]) if type(point[1]) in (str, int, float) else None
            except (ValueError, OverflowError):
                value = None
            if value is not None and math.isfinite(value):
                return record.get("value_state") == "finite" and type(record.get("value")) in (int, float) and record["value"] == value
            return record.get("value") is None and record.get("value_state") in {"non_finite", "invalid_value"}
        if modality == "logs":
            line = raw_by_ref[record["raw_ref"]].decode("utf-8").splitlines()[record["line_ordinal"]]
            stamp, _, message = line.partition(" ")
            return stamp == record["timestamp_original"] and message == record["message"] and str(t.parse_rfc3339(stamp)) == record["timestamp_epoch_decimal_s"]
        if modality == "traces":
            for location in record["observed_in"]:
                raw = json.loads(raw_by_ref[location["raw_ref"]])
                for trace in raw["data"]:
                    if trace["traceID"] != record["trace_id"]:
                        continue
                    for span in trace["spans"]:
                        if span["spanID"] == record["span_id"]:
                            process = trace["processes"][span["processID"]]
                            if (span["startTime"] == record["start_time_us"] and span["duration"] == record["duration_us"]
                                    and span["operationName"] == record["operation"] and process["serviceName"] == record["service"]
                                    and span["processID"] == record["process_id"] and _equal(span.get("tags", []), record["tags"])
                                    and _equal(process.get("tags", []), record["process_tags"]) and _equal(span.get("references", []), record["references"])):
                                return True
            return False
    except (ValueError, KeyError, TypeError, IndexError, OverflowError):
        return False
    return False


def _record_key(modality, record):
    if modality == "traces":
        fields = [record["trace_id"], record["span_id"]]
    elif modality == "metrics":
        fields = [record["query_id"], record["labels"], record["timestamp_epoch_s"]]
    else:
        fields = [record["pod_uid"], record["container"], record["timestamp_original"], record["line_ordinal"]]
    return _sha(_canonical(fields).encode("utf-8"))


def _evaluate_data(contract, policy, evidence, phases, bundles, inventory, add, test, bound):
    annotation = evidence.get("annotation", {})
    faults = {f.fault_instance_id: f.to_dict() for f in contract.faults}
    instances = annotation.get("fault_instances", []) if type(annotation.get("fault_instances")) is list else []
    ids = [row.get("fault_instance_id") for row in instances if type(row) is dict]
    annotation_identity = (_identity(annotation, contract) and annotation.get("scenario") == contract.to_dict()["scenario"])
    test("Q3", "annotation_identity", annotation_identity, "annotation_contract_or_scenario_mismatch")
    instances_ok = (len(ids) == len(instances) == len(faults) and len(set(ids)) == len(ids) and set(ids) == set(faults)
                    and all(_equal(row.get("planned"), faults[row["fault_instance_id"]]) and row.get("normalized_root_entity") == faults[row["fault_instance_id"]]["normalized_root_entity"] for row in instances))
    test("Q3", "instance_ground_truth", instances_ok, "instance_metadata_missing_or_drifted")
    test("Q3", "entity_ground_truth", annotation.get("root_entities") == list(contract.root_entities) and type(annotation.get("G")) is int and annotation["G"] == len(contract.root_entities), "entity_set_or_G_mismatch")
    labels_ready = (annotation.get("status") == "reviewed" and not _unresolved(annotation)
                    and annotation.get("annotation_rules_ref") == policy["rule_refs"]["annotation"]
                    and type(annotation.get("dictionary_version")) is str and bool(annotation["dictionary_version"]))
    role_labels = {"primary", "secondary", "amplifier", "co_primary"}
    def refs_verified(refs):
        return (type(refs) is list and bool(refs) and inventory is not None and
                all(type(ref) is str and ref != evidence.get("artifact_id") and inventory["artifacts"].get(ref, {}).get("status") == "PASS" for ref in refs))
    def scope_verified(item):
        window = _window(item.get("scope_window"), contract)
        return window is not None and phases["during_fault"] is not None and phases["during_fault"].contains_window(window)
    labels_ready = labels_ready and all(row.get("role", {}).get("label") in role_labels
                                       and row["role"].get("rule_ref") == policy["rule_refs"]["annotation"]
                                       and refs_verified(row["role"].get("evidence_refs")) and scope_verified(row["role"]) for row in instances)
    expected_pairs = {frozenset((a, b)) for i, a in enumerate(ids) for b in ids[i + 1:]}
    pairs = annotation.get("pair_relations", [])
    observed_pairs = [frozenset(row.get("instance_ids", [])) for row in pairs if type(row) is dict]
    labels_ready = labels_ready and len(observed_pairs) == len(expected_pairs) and set(observed_pairs) == expected_pairs
    enums = {"category": {"intra_class", "cross_class"}, "time": {"simultaneous", "staggered", "partial_overlap", "nested"},
             "request_path": {"same_request_path", "different_request_path", "partially_shared_path"},
             "interaction": {"independent_parallel", "trigger_amplifier", "fault_masking"}}
    for pair in pairs:
        labels_ready = labels_ready and scope_verified(pair) and pair.get("rule_ref") == policy["rule_refs"]["annotation"]
        for dimension, valid in enums.items():
            item = pair.get(dimension, {})
            labels_ready = labels_ready and item.get("label") in valid and refs_verified(item.get("evidence_refs"))
        if labels_ready:
            members = pair["instance_ids"]
            expected_class = "intra_class" if faults[members[0]]["fault_class"] == faults[members[1]]["fault_class"] else "cross_class"
            labels_ready = pair["category"]["label"] == expected_class
            interaction = pair["interaction"]
            if interaction["label"] in {"trigger_amplifier", "fault_masking"}:
                labels_ready = labels_ready and {interaction.get("source_instance_id"), interaction.get("target_instance_id")} == set(members)
    reviews = annotation.get("reviews", [])
    reviewer_ids = [row.get("reviewer_id") for row in reviews if type(row) is dict and _identity(row, contract)
                    and row.get("decision") == "approved" and (not policy["data"]["require_human_review"] or row.get("kind") == "human")
                    and _artifact_matches(inventory, row.get("artifact_id"), row)[0] == "PASS"]
    reviews_ok = len(set(reviewer_ids) - {None, ""}) >= 2 and annotation.get("adjudication_status") in {"agreement", "resolved"}
    add("Q3", "final_annotation_and_review", "PASS" if labels_ready and reviews_ok else "BLOCKED", "final_labels_and_review_present" if labels_ready and reviews_ok else "labels_roles_or_independent_review_not_complete")
    test("Q3", "attempt_not_failed", evidence.get("attempt_state") == "COMPLETED_CLEAN", "failed_or_unfinished_attempt_is_not_an_accepted_sample")
    trace_bundles = {}
    for phase in _PHASES:
        for modality in _MODALITIES:
            name = phase + "." + modality
            bundle = bundles.get(phase, {}).get(modality)
            if not test("Q3", name + ".identity", _bundle_identity(bundle, contract, phase, modality, phases[phase]), "missing_or_foreign_modality_phase"):
                continue
            bound("Q3", name + ".file", evidence.get("bundle_artifact_ids", {}).get(phase, {}).get(modality), bundle)
            add("Q3", name + ".declared_storage_files", "NOT_ASSESSED" if inventory is None else "PASS" if _stored_files_match(bundle, inventory) else "FAIL",
                "storage_checked" if inventory is not None and _stored_files_match(bundle, inventory) else "declared_projection_or_raw_file_unverified")
            manifest, records, queries, raws = bundle.get("manifest", {}), bundle.get("records", []), bundle.get("queries", []), bundle.get("raw_responses", [])
            projection_ok = manifest.get("projection_sha256") == _sha(_canonical(records).encode("utf-8")) and manifest.get("record_count") == len(records)
            test("Q3", name + ".projection", projection_ok, "projection_hash_or_count_mismatch")
            raw_by_ref, raw_ok = {}, bool(raws)
            for item in raws:
                try:
                    raw = base64.b64decode(item["base64"], validate=True)
                    correct = (item["ref"] not in raw_by_ref and len(raw) == item["retained_bytes"] == item["received_bytes"]
                               and _sha(raw) == item["retained_sha256"] == item["received_sha256"] and item["retained_truncated"] is False)
                    raw_by_ref[item["ref"]] = raw
                    raw_ok = raw_ok and correct
                except (ValueError, KeyError, TypeError):
                    raw_ok = False
            test("Q3", name + ".raw", raw_ok, "raw_missing_truncated_or_hash_mismatch")
            test("Q3", name + ".projection_raw_binding", raw_ok and all(_record_matches_raw(modality, record, raw_by_ref) for record in records), "normalized_record_disagrees_with_raw")
            source_ids = {query.get("source_id", query.get("source", {}).get("source_id")) for query in queries}
            test("Q3", name + ".sources", set(policy["data"]["required_sources"][modality]) <= source_ids, "required_source_not_collected")
            query_ok = bool(queries) and all(q.get("status") == "ok" and q.get("raw_ref") in raw_by_ref
                                            and q.get("backend_provenance", {}).get("source_id") == q.get("source_id", q.get("source", {}).get("source_id"))
                                            and not _known_non_observed(q) for q in queries)
            test("Q3", name + ".queries", query_ok, "query_failed_or_raw_reference_missing")
            harmful = []
            for query in queries:
                harmful.extend(issue for issue in query.get("issues", []) if issue not in {"point_outside_half_open_window", "line_outside_half_open_window", "span_on_search_boundary"})
            harmful.extend(manifest.get("issues", []))
            test("Q3", name + ".errors_and_truncation", not harmful, "unresolved_collection_issue_or_truncation", issues=harmful)
            if modality == "metrics":
                nonfinite = sum(record.get("value_state") != "finite" for record in records)
                fraction = nonfinite / len(records) if records else 1
                test("Q3", name + ".finite_points", bool(records) and fraction <= policy["data"]["max_nonfinite_fraction"], "missing_or_nonfinite_metric_points")
                for required in policy["data"]["required_metric_queries"]:
                    matching = [q for q in queries if all(q.get(key) == required[key] for key in ("query_id", "source_id", "entity"))]
                    test("Q3", name + ".required." + required["query_id"], len(matching) == 1 and any(all(r.get(k) == v for k, v in required.items()) for r in records), "required_metric_query_or_series_missing")
                for index, query in enumerate(queries):
                    step = query.get("query_step_s")
                    valid_step = type(step) in (int, float) and math.isfinite(step) and step > 0
                    expected = math.ceil((phases[phase].end - phases[phase].start) / step) if valid_step else 0
                    selected = [r for r in records if r.get("query_id") == query.get("query_id") and r.get("source_id") == query.get("source_id")]
                    grouped = {}
                    for record in selected:
                        grouped.setdefault(_canonical(record.get("labels")), []).append(record.get("timestamp_epoch_s"))
                    test("Q3", name + ".series." + str(index), bool(grouped) and expected > 0, "metric_query_has_no_series_or_grid")
                    for series, stamps in grouped.items():
                        expected_grid = [round(phases[phase].start + k * step, 9) for k in range(expected)] if valid_step else []
                        known = {round(v, 9) for v in stamps if type(v) in (int, float) and math.isfinite(v)}
                        # Real Prometheus query_range rounds evaluation timestamps
                        # to whole milliseconds, so a returned point can sit ~1 ms
                        # beside the requested grid point. A grid point therefore
                        # counts as present only when a known timestamp lies
                        # within this 2 ms alignment tolerance; the missing
                        # fraction semantics itself stay unchanged.
                        missing = sum(1 for stamp in expected_grid if not any(abs(stamp - v) <= 0.002 for v in known))
                        test("Q3", name + ".missing." + str(index) + "." + _sha(series.encode())[:8], expected > 0 and missing / expected <= policy["data"]["max_missing_fraction"], "metric_missing_fraction_exceeded", missing_count=missing, expected_count=expected)
                    sampling = query.get("source_sampling", {})
                    sampling_row = {} if inventory is None else inventory["artifacts"].get(sampling.get("evidence_ref"), {})
                    readback = sampling_row.get("data", {})
                    if "source_sampling_policy" in policy["data"]:
                        from . import sampling as source_sampling
                        run_row = {} if inventory is None else inventory["artifacts"].get("run-record", {})
                        if sampling_row.get("status") != "PASS" or run_row.get("status") != "PASS" or _known_non_observed(readback):
                            add("Q3", name + ".sampling." + str(index), "NOT_ASSESSED", "sampling_or_original_run_artifact_unverified")
                            continue
                        assessed = source_sampling.evaluate(readback, contract=contract, phase=phase, query_id=query["query_id"],
                            source_id=query["source_id"], actual_window=phases[phase], policy=policy["data"]["source_sampling_policy"],
                            max_decoded_bytes=policy["data"]["source_sampling_policy"]["max_decoded_bytes"],
                            expected_clock=run_row["data"]["clock_mapping"], expected_labels=query.get("signal_labels", {}))
                        add("Q3", name + ".sampling." + str(index), assessed["status"], assessed["reason"],
                            accepted_scope=assessed.get("accepted_scope"), **assessed.get("details", {}))
                        continue
                    sampling_ok = (sampling_row.get("status") == "PASS" and type(readback) is dict
                                   and _identity(readback, contract, phase=phase, source=query.get("source_id"))
                                   and readback.get("schema_version") == "rq4-collect/sampling-readback-v1"
                                   and readback.get("evidence_kind") == "observed"
                                   and type(readback.get("return_code")) is int and readback["return_code"] == 0
                                   and not _known_non_observed(readback)
                                   and _within_stamp(readback.get("timestamp_epoch_s"), phases[phase])
                                   and readback.get("scrape_interval_s") == sampling.get("scrape_interval_s")
                                   and readback.get("export_interval_s") == sampling.get("export_interval_s"))
                    too_slow = False
                    for key in ("scrape_interval_s", "export_interval_s"):
                        value = sampling.get(key)
                        if key == "export_interval_s" and value is None and readback.get("export_applicability") == "not_applicable":
                            continue
                        too_slow = too_slow or (sampling_ok and type(value) in (int, float) and math.isfinite(value) and value > policy["data"]["max_source_interval_s"])
                        sampling_ok = sampling_ok and type(value) in (int, float) and math.isfinite(value) and 0 < value <= policy["data"]["max_source_interval_s"]
                    add("Q3", name + ".sampling." + str(index), "FAIL" if too_slow else "PASS" if sampling_ok else "NOT_ASSESSED", "source_sampling_slower_than_declared_bound" if too_slow else "source_readback_within_declared_bound" if sampling_ok else "source_sampling_missing_or_unverified")
            if modality == "traces":
                trace_bundles[phase] = bundle
            # Inventory equality is a useful cross-check, NOT an exhaustive
            # backend/pagination/ingestion/archive coverage certificate.
            coverage = evidence.get("coverage", {}).get(phase, {}).get(modality)
            if type(coverage) is not dict:
                add("Q3", name + ".coverage", "NOT_ASSESSED", "independent_coverage_evidence_missing")
                continue
            coverage_bound = bound("Q3", name + ".coverage_file", coverage.get("artifact_id"), coverage)
            if "phase_evidence_policy" in policy["data"]:
                anchor_row = {} if inventory is None else inventory["artifacts"].get("run-record", {})
                anchor_run = anchor_row.get("data", {})
                anchor_bound = (coverage.get("run_clock_anchor_ref") == "run-record" and anchor_row.get("status") == "PASS"
                                and _identity(anchor_run, contract) and coverage.get("clock_mapping") == anchor_run.get("clock_mapping"))
                test("Q3", name + ".run_clock_anchor", anchor_bound, "coverage_clock_not_bound_to_original_run")
                coverage_bound = coverage_bound and anchor_bound
            if modality == "logs" and coverage.get("method") == "bounded_log_archive_v1":
                status, reason, details = _archive_coverage(contract, phase, bundle, coverage, inventory,
                                                          policy["data"]["coverage_policy"], phases[phase], coverage_bound)
                add("Q3", name + ".coverage", status, reason, **details)
                continue
            if coverage.get("method") == "bounded_query_window_v1":
                status, reason, details = _bounded_coverage(contract, phase, modality, bundle, coverage, inventory,
                                                          policy["data"]["coverage_policy"], phases[phase], coverage_bound)
                add("Q3", name + ".coverage", status, reason, **details)
                continue
            coverage_ok = (_identity(coverage, contract, phase=phase) and coverage.get("evidence_kind") == "observed"
                           and coverage.get("method") == "independent_inventory" and coverage.get("window") == phases[phase].to_dict()
                           and coverage.get("projection_sha256") == manifest.get("projection_sha256")
                           and type(coverage.get("sources")) is dict and set(coverage["sources"]) == source_ids)
            if coverage_ok:
                for source, item in coverage["sources"].items():
                    if type(item) is not dict or inventory is None:
                        coverage_ok = False
                        continue
                    inventory_row = inventory["artifacts"].get(item.get("inventory_artifact_id"), {})
                    snapshot = inventory_row.get("data", {})
                    selected = [r for r in records if r.get("source_id") == source]
                    try:
                        observed_keys = [_record_key(modality, row) for row in selected]
                        valid = (inventory_row.get("status") == "PASS" and inventory_row.get("actual_sha256") == item.get("inventory_snapshot_sha256")
                                 and _identity(snapshot, contract, phase=phase, source=source)
                                 and snapshot.get("schema_version") == "rq4-collect/coverage-inventory-v1"
                                 and snapshot.get("evidence_kind") == "observed" and snapshot.get("window") == phases[phase].to_dict()
                                 and type(snapshot.get("record_keys")) is list and len(set(snapshot["record_keys"])) == len(snapshot["record_keys"])
                                 and sorted(snapshot["record_keys"]) == sorted(observed_keys))
                    except (ValueError, KeyError, TypeError):
                        valid = False
                    coverage_ok = coverage_ok and valid
            test("Q3", name + ".inventory_record_set", coverage_ok and coverage_bound, "coverage_inventory_missing_mismatched_or_unverified")
            add("Q3", name + ".coverage", "NOT_ASSESSED", "exhaustive_backend_or_archive_coverage_verifier_not_implemented")
    # A span can legitimately intersect several observation windows. Its
    # phase membership/query provenance changes, but its underlying completed
    # span facts cannot become a different event merely by rehashing each file.
    span_facts, span_conflicts, repeated_spans = {}, [], 0
    immutable_fields = ("start_time_us", "duration_us", "end_time_us", "service", "operation")
    for phase, bundle in trace_bundles.items():
        for record in bundle.get("records", []):
            if type(record) is not dict or type(record.get("trace_id")) is not str or type(record.get("span_id")) is not str:
                continue  # Structural rejection is handled by trace_phase_presence.
            identity = (record["trace_id"], record["span_id"])
            facts = {field: record.get(field) for field in immutable_fields}
            if identity in span_facts:
                previous_phase, previous = span_facts[identity]
                repeated_spans += 1
                if not _equal(previous, facts):
                    span_conflicts.append({"trace_id": identity[0], "span_id": identity[1],
                                           "first_phase": previous_phase, "conflicting_phase": phase,
                                           "fields": [field for field in immutable_fields if not _equal(previous[field], facts[field])]})
            else:
                span_facts[identity] = (phase, facts)
    test("Q3", "cross_phase_span_identity", not span_conflicts, "same_span_identity_has_conflicting_immutable_facts",
         unique_spans=len(span_facts), repeated_observations=repeated_spans,
         conflict_count=len(span_conflicts), conflicts=span_conflicts[:20])
    try:
        presence = t.trace_phase_presence(trace_bundles)
        test("Q3", "three_phase_trace_presence", presence["three_phases_nonempty"], "at_least_one_phase_has_no_valid_trace")
    except (ValueError, KeyError, TypeError):
        add("Q3", "three_phase_trace_presence", "FAIL", "trace_identity_window_or_projection_invalid")


def _validated_pod_gaps(manifest, scope):
    """collection: re-validate manifest ``pod_gaps`` proof rows before any exemption.

    collection lets a during_fault archive record a mechanism-replacement coverage
    gap (GAP_POD_REPLACED) with a four-leg proof instead of poisoning the
    journal. The rows are producer claims like every other manifest field,
    so this consumer re-validates each leg independently of the producer
    before any reconciliation exemption can exist; a row missing any leg is
    not accepted and the affected pod keeps the old hard reconciliation
    (fail-closed):

    * gap_code GAP_POD_REPLACED, living in the during_fault archive (row
      phase and manifest phase both during_fault);
    * a declared mechanism switch band > 0;
    * the ReplicaSet->Deployment ancestry pinned to THIS archive's
      deployment_uid (ReplicaSet uid/name present);
    * complete replaced-Pod identity (uid/name/creation, resource version,
      six-element physical identity), every successor carrying uid/name,
      and live replacement evidence: a non-empty successor list or a
      received deletion/deletion-request for the replaced Pod.

    Two rows naming the same replaced uid are ambiguous and exempt nothing.
    Returns {uid: row} of the accepted rows only; a valid row whose uid is
    not in the manifest pod map exempts nothing and is simply carried.
    """
    accepted, ambiguous = {}, set()
    rows = manifest.get("pod_gaps")
    if manifest.get("phase") != "during_fault" or type(rows) is not list:
        return accepted
    for row in rows:
        if type(row) is not dict:
            continue
        replaced, replicaset = row.get("replaced_pod"), row.get("replicaset")
        successors = row.get("successor_pods")
        valid = (row.get("gap_code") == "GAP_POD_REPLACED" and row.get("phase") == "during_fault"
                 and type(row.get("declared_switch_band_s")) in (int, float)
                 and math.isfinite(row["declared_switch_band_s"]) and row["declared_switch_band_s"] > 0
                 and type(replicaset) is dict and replicaset.get("deployment_uid") == scope.get("deployment_uid")
                 and type(replicaset.get("uid")) is str and bool(replicaset["uid"])
                 and type(replicaset.get("name")) is str and bool(replicaset["name"])
                 and type(replaced) is dict
                 and type(replaced.get("uid")) is str and bool(replaced["uid"])
                 and type(replaced.get("name")) is str and bool(replaced["name"])
                 and type(replaced.get("creation_epoch_s")) in (int, float) and math.isfinite(replaced["creation_epoch_s"])
                 and type(replaced.get("last_resource_version")) is str and bool(replaced["last_resource_version"])
                 and type(replaced.get("physical_identity")) is list and len(replaced["physical_identity"]) == 6
                 and type(successors) is list
                 and all(type(item) is dict and type(item.get("uid")) is str and bool(item["uid"])
                         and type(item.get("name")) is str and bool(item["name"]) for item in successors)
                 and (bool(successors) or replaced.get("deleted_received_epoch_s") is not None
                      or replaced.get("deletion_requested_received_epoch_s") is not None))
        if not valid or replaced["uid"] in ambiguous:
            continue
        if replaced["uid"] in accepted:
            del accepted[replaced["uid"]]
            ambiguous.add(replaced["uid"])
            continue
        accepted[replaced["uid"]] = row
    return accepted


def _archive_coverage(contract, phase, bundle, coverage, inventory, policy, window, bound):
    """Verify retained archive bytes, declared capture-gap limits and the
    complete discovered identity set re-derived from the retained raw evidence.

    Per retained stream the receipt/ancestry/view chain is verified as before.
    On top of that (collection), the watch chunk chain is re-parsed from its verified
    bytes and every Pod's physical timeline is rebuilt independently of
    ``manifest["pods"]``/``manifest["events"]``; the rebuilt set is reconciled
    with the manifest pod map, the per-uid attachment streams and the view
    stream identities before any PASS can be returned.

    This remains bounded capture evidence. It never certifies pre-attachment
    bytes, unobserved history, native application timestamps or backend
    exhaustiveness. No capture-gap tolerance is inferred from this run's
    observed gap.
    """
    archive_policy = policy.get("archive_policy")
    if archive_policy is None:
        return "NOT_ASSESSED", "archive_gap_policy_not_predeclared", {}
    if (not bound or inventory is None or not _identity(coverage, contract, phase=phase)
            or coverage.get("evidence_kind") != "observed" or _known_non_observed(coverage)
            or coverage.get("window") != window.to_dict()
            or coverage.get("archive_rule_ref") != archive_policy["rule_ref"]):
        return "NOT_ASSESSED", "archive_coverage_identity_or_policy_unbound", {}
    entries = inventory["artifacts"]

    def receipt(ref):
        _require(type(ref) is dict and type(ref.get("artifact_id")) is str, "archive_receipt_missing")
        row = entries.get(ref["artifact_id"], {})
        _require(row.get("status") == "PASS" and row.get("actual_sha256") == ref.get("sha256")
                 and row.get("relative_path") == ref.get("relative_path"), "archive_receipt_unverified")
        return row

    gaps, summaries, pod_gap_notes = [], [], []
    try:
        queries = bundle.get("queries")
        _require(type(queries) is list and bool(queries), "archive_queries_missing")
        for query in queries:
            provenance, source = query.get("backend_provenance", {}), query.get("source", {})
            capture = provenance.get("capture", {})
            _require(query.get("status") == "ok" and set(query.get("issues", [])) <= {"line_outside_half_open_window"}
                     and provenance.get("uid_verification") == "deployment_pinned_ancestry_per_stream"
                     and provenance.get("timestamp_semantics") == "archive_chunk_receipt_upper_bound_not_container_native"
                     and provenance.get("source_id") == source.get("source_id")
                     and provenance.get("pod_uid") == source.get("pod_uid"), "archive_source_or_view_unverified")
            _require(capture.get("archive_status") == "CAPTURED_BOUNDED_NOT_EXHAUSTIVE"
                     and capture.get("archive_failures") == [] and capture.get("transport_failures") == [],
                     "archive_capture_failed")
            manifest = receipt(capture.get("manifest_artifact"))["data"]
            _require(_identity(manifest, contract) and manifest.get("schema_version") == "rq4-collect/log-archive-v1"
                     and manifest.get("evidence_kind") == "observed" and not _known_non_observed(manifest)
                     and manifest.get("archive_id") == capture.get("archive_id")
                     and manifest.get("status") == "CAPTURED_BOUNDED_NOT_EXHAUSTIVE"
                     and manifest.get("failures") == [] and manifest.get("writer_drained_before_manifest") is True,
                     "archive_manifest_not_clean_or_same_attempt")
            scope, binding = manifest.get("scope", {}), provenance.get("identity", {})
            _require(all(scope.get(key) == binding.get(key) for key in ("context", "namespace", "deployment_uid", "app", "container"))
                     and scope.get("namespace") == contract.context.to_dict()["namespace"]
                     and scope.get("context") == contract.context.to_dict()["kube_context"]
                     and scope.get("deployment_uid") == source.get("pod_uid"), "archive_scope_pin_mismatch")
            uncertainty = manifest.get("clock_uncertainty_s")
            _number(uncertainty)
            _require(uncertainty <= policy["max_clock_uncertainty_s"], "archive_clock_uncertainty_exceeds_policy")
            start, stop = manifest["started_at"]["epoch_s"], manifest["stop_requested_at"]["epoch_s"]
            _number(start); _number(stop)
            _require(stop >= start and stop - uncertainty >= window.end, "archive_capture_ended_before_phase")
            start_gap = max(0., start + uncertainty - window.start)
            _require(start_gap <= archive_policy["max_capture_start_gap_s"], "archive_start_gap_exceeds_policy")
            _require(capture.get("capture_started_epoch_s") == start and capture.get("stop_requested_epoch_s") == stop,
                     "archive_capture_clock_projection_mismatch")
            from . import log_archive as la
            typed_scope = la.ArchiveScope(**scope)
            
            # manifest's pinned scope before they can soften reconciliation.
            pod_gaps = _validated_pod_gaps(manifest, scope)
            pod_gap_notes.extend({"archive_id": manifest.get("archive_id"), "gap_code": row["gap_code"],
                                  "trigger": row.get("trigger"), "uid": uid, "name": row["replaced_pod"]["name"]}
                                 for uid, row in pod_gaps.items())

            def projection(ref, kind):
                metadata = receipt(ref)["data"]
                _require(_identity(metadata, contract) and metadata.get("projection_format") == "identity_projection_v1"
                         and metadata.get("kind") == kind, "archive_projection_metadata_mismatch")
                raw = receipt(metadata["raw"])
                _require(raw.get("format") == "bytes", "archive_projection_raw_missing")
                payload = json.loads(base64.b64decode(raw["bytes_base64"], validate=True))
                for key in ("started_epoch_s", "ended_epoch_s", "started_monotonic_s", "ended_monotonic_s"):
                    _number(metadata[key])
                _require(metadata["ended_epoch_s"] >= metadata["started_epoch_s"]
                         and metadata["ended_monotonic_s"] >= metadata["started_monotonic_s"], "archive_projection_clock_reversed")
                for epoch_key, mono_key in (("started_epoch_s", "started_monotonic_s"), ("ended_epoch_s", "ended_monotonic_s")):
                    _require(abs(metadata[epoch_key] - (start + metadata[mono_key] - manifest["started_at"]["monotonic_s"])) <= uncertainty,
                             "archive_projection_clock_anchor_mismatch")
                return payload

            initial = projection(manifest["initial_read"], "initial")
            _require(initial.get("cluster_uid") == scope["cluster_uid"]
                     and initial.get("namespace_uid") == scope["namespace_uid"] and type(initial.get("pods")) is list,
                     "archive_initial_inventory_scope_mismatch")
            for pod in initial["pods"]:
                la._pod(pod, typed_scope)
            for read in manifest.get("reads", []):
                metadata = receipt(read["artifact"])["data"]
                _require(_equal(metadata, {key: value for key, value in read.items() if key != "artifact"}),
                         "archive_read_projection_differs_from_file")
                receipt(metadata["raw"])
            accepted, persisted, failed = (manifest.get(key) for key in ("accepted_bytes", "persisted_bytes", "failed_bytes"))
            _require(type(accepted) is dict and accepted == persisted and type(failed) is dict
                     and all(type(value) is int and value == 0 for value in failed.values()), "archive_bytes_not_fully_persisted")
            view_streams = provenance.get("streams", [])
            _require(len(view_streams) == capture.get("stream_count") == len(manifest.get("streams", {})),
                     "archive_stream_inventory_mismatch")
            reconstructed, watch_raw, gap_exempted = {}, None, set()
            for stream_id, chunks in manifest["chunks"].items():
                cursor, pieces = 0, []
                for index, chunk in enumerate(chunks):
                    _require(chunk["sequence"] == index and chunk["byte_start"] == cursor
                             and type(chunk["byte_end"]) is int and chunk["byte_end"] >= cursor, "archive_chunk_gap")
                    row = receipt(chunk["raw"])
                    _require(row.get("format") == "bytes", "archive_chunk_not_binary")
                    raw = base64.b64decode(row["bytes_base64"], validate=True)
                    offset = chunk["raw"].get("byte_offset", 0)
                    length = chunk["raw"].get("bytes", len(raw))
                    _require(type(offset) is int and type(length) is int and 0 <= offset <= offset + length <= len(raw)
                             and length == chunk["byte_end"] - cursor, "archive_chunk_byte_range_mismatch")
                    pieces.append(raw[offset:offset + length]); cursor = chunk["byte_end"]
                _require(cursor == accepted.get(stream_id), "archive_chunk_total_mismatch")
                joined = manifest.get("stream_joins", {}).get(stream_id, {})
                _require(joined.get("joined") is True and type(joined.get("return_code")) is int,
                         "archive_stream_join_unconfirmed")
                if stream_id == "watch":
                    watch_raw = b"".join(pieces)
                    continue
                pod = manifest["pods"][manifest["streams"][stream_id]]
                attachments = pod["attachments"]
                gap_row = pod_gaps.get(manifest["streams"][stream_id])
                attachment = None
                if gap_row is None or stream_id in attachments:
                    attachment = attachments[stream_id]
                    before = projection(attachment["before_read"], "pod")
                    after = projection(attachment["after_read"], "pod")
                    ancestor = projection(attachment["ancestor_read"], "replicaset")
                    la._pod(before, typed_scope); la._pod(after, typed_scope)
                    owners = [row for row in before["owners"] if row["controller"] is True]
                    _require(la._physical(before) == la._physical(after)
                             and before["owners"] == after["owners"] and before["uid"] == pod["first"]["uid"]
                             and before["container"] == attachment["container_projection"]
                             and len(owners) == 1 and ancestor.get("uid") == owners[0]["uid"]
                             and ancestor.get("name") == owners[0]["name"]
                             and ancestor.get("namespace") == scope["namespace"]
                             and ancestor.get("deployment_uid") == scope["deployment_uid"]
                             and ancestor.get("controller") is True, "archive_pod_ancestry_or_container_identity_mismatch")
                    matched = [row for row in view_streams if row.get("pod_uid") == pod["first"]["uid"]
                               and row.get("container_id") == attachment["container_projection"]["container_id"]
                               and row.get("restart_count") == attachment["container_projection"]["restart_count"]]
                else:
                    
                    # join confirmation verified above) but its attach
                    # confirmation read raced the mechanism rollout, so the
                    # attachment chain is honestly missing and covered by the
                    # pod's validated gap row. Bind the view row by the
                    # retained bytes themselves (pod uid + stream hash +
                    # length, exactly one); the ancestry/container identity
                    # leg stays NOT_ASSESSED and the coverage check below can
                    # no longer be PASS.
                    matched = [row for row in view_streams if row.get("pod_uid") == pod["first"]["uid"]
                               and row.get("stream_sha256") == _sha(b"".join(pieces))
                               and row.get("stream_bytes") == cursor]
                _require(len(matched) == 1, "archive_view_stream_identity_mismatch")
                view = matched[0]
                _require(view.get("stream_sha256") == _sha(b"".join(pieces)) and view.get("stream_bytes") == cursor
                         and view.get("trailing_partial_line") is False
                         and view.get("excluded_by_view_truncation") is False, "archive_view_bytes_incomplete")
                from . import log_transport as lt
                original = b"".join(pieces)
                
                # stream (silent-by-design carrier) has zero lines; splitting
                # b"" would fabricate one empty line whose receipt lookup then
                # fails on the empty chunk list (caught below as a bogus
                # archive_evidence_structure_invalid FAIL).
                lines = original.split(b"\n") if original else []
                if original.endswith(b"\n"):
                    lines = lines[:-1]
                positions = tuple((chunk["byte_start"], chunk["byte_end"], chunk["received_epoch_s"]) for chunk in chunks)
                transformed, offset = [], 0
                for line in lines:
                    end = offset + len(line)
                    at = lt._chunk_receipt(positions, max(0, end - 1))
                    transformed.append(lt._rfc3339(at).encode("ascii") + b" " + line + b"\n")
                    offset = end + 1
                key = (view["pod_uid"], view["container_id"], view["restart_count"])
                _require(key not in reconstructed, "archive_duplicate_physical_stream")
                reconstructed[key] = b"".join(transformed)
                if attachment is None:
                    
                    # chain is not counted as coverage anywhere.
                    gap_exempted.add(key)
                    gaps.append({"source_id": source["source_id"], "pod_uid": pod["first"]["uid"],
                                 "capture_start_gap_s": start_gap,
                                 "pod_replacement_gap": {"gap_code": gap_row["gap_code"], "trigger": gap_row.get("trigger"),
                                                         "declared_switch_band_s": gap_row["declared_switch_band_s"]}})
                else:
                    attachment_gap = max(0., attachment["completed_epoch_s"] + uncertainty
                                         - max(window.start, pod["first"]["creation_epoch_s"]))
                    _require(attachment_gap <= archive_policy["max_attachment_gap_s"], "archive_attachment_gap_exceeds_policy")
                    gaps.append({"source_id": source["source_id"], "pod_uid": pod["first"]["uid"],
                                 "capture_start_gap_s": start_gap, "attachment_gap_s": attachment_gap})
            expected_view = b"".join(reconstructed[(view["pod_uid"], view["container_id"], view["restart_count"])] for view in view_streams)
            raws = [row for row in bundle.get("raw_responses", []) if row.get("ref") == query.get("raw_ref")]
            _require(len(raws) == 1 and base64.b64decode(raws[0]["base64"], validate=True) == expected_view,
                     "archive_query_view_disagrees_with_retained_chunks")
            
            # only cover streams the manifest chose to record. Before any PASS,
            # re-derive the complete discovered identity set from the verified
            # watch chunk chain plus the initial projection, reconcile it with
            # manifest pods/attachments/view streams and cross-check every
            # declared event against the raw frame it claims to project.
            _require(watch_raw is not None, "archive_watch_stream_missing")
            summaries.append(_archive_discovered_identity_inventory(
                receipt, manifest, initial, typed_scope, watch_raw, manifest["chunks"]["watch"], view_streams,
                pod_gaps, gap_exempted))
        
        # identity inventory re-derived from raw bytes reconciled for every
        # archive in this bundle, and declared events cross-checked against the
        # raw frames. The residual scope stays explicit: this certifies the
        # retained evidence's internal completeness, never backend
        # exhaustiveness or unobserved pre-attachment history.
        if pod_gap_notes:
            
            # NOT_ASSESSED, never counted as coverage and never PASS. A real
            # failure on any other pod already returned FAIL above; the gap
            # face must not upgrade or erase it.
            return "NOT_ASSESSED", "archive_pod_replacement_gap_not_assessed:" + ";".join(
                "%s:uid=%s:name=%s" % (row["gap_code"], row["uid"], row["name"]) for row in pod_gap_notes), {
                "accepted_scope": "retained_stream_checks_and_discovered_identity_inventory_reconciled_except_proven_replacement_gaps_not_backend_exhaustive",
                "residual_limits": "backend_exhaustiveness_and_unobserved_history_not_claimed;replaced_pod_logs_missing_by_proven_mechanism_replacement",
                "rule_ref": archive_policy["rule_ref"], "gaps": gaps, "pod_gaps": pod_gap_notes,
                "discovered": summaries}
        return "PASS", "archive_discovered_identity_inventory_reconciled", {
            "accepted_scope": "retained_stream_checks_and_discovered_identity_inventory_reconciled_not_backend_exhaustive",
            "residual_limits": "backend_exhaustiveness_and_unobserved_history_not_claimed",
            "rule_ref": archive_policy["rule_ref"], "gaps": gaps,
            "discovered": summaries}
    except (ValueError, KeyError, TypeError, IndexError, OverflowError, RuntimeError, AttributeError) as exc:
        
        # access (e.g. a forged string view-stream row) used to escape this
        # function instead of failing loudly; it now takes the same FAIL path.
        return "FAIL", str(exc) if isinstance(exc, QualityError) else "archive_evidence_structure_invalid", {}


def _archive_discovered_identity_inventory(receipt, manifest, initial, typed_scope, watch_raw, watch_chunks, view_streams,
                                           pod_gaps, gap_exempted):
    """Re-derive the complete discovered Pod identity set from retained raw bytes.

    collection consumer closing the archive coverage gap: ``manifest["pods"]`` and
    ``manifest["events"]`` are producer claims. A Pod lost before entering the
    producer's pod map (dropped initial projection, dropped watch event) never
    trips the producer's own unattached-pod failure, so this consumer trusts
    only the receipt-verified watch chunk chain and the initial projection:

    - every frame is re-parsed from the raw watch bytes (producer frame
      semantics: whitelisted fields, ``la._pod`` projection validation, rv
      agreement, control-frame shape) and cross-checked against the declared
      ``manifest["events"]`` (count, sequence, byte range inside the chunk
      chain, projection equality, receipt equality, per-event artifact
      receipt, chunk binding);
    - every uid's physical timeline (first identity, each in-place
      replacement boundary, DELETED/deletion-requested receipt moments, per-uid
      rv monotonicity) is rebuilt from the initial projection plus the frames;
    - three-way reconciliation, all required: rebuilt uid set == manifest pod
      keys; per uid, the manifest boundary list must be the contiguous suffix
      of rebuilt transitions beginning at the first attached physical identity
      and the attachment set must equal every physical identity from that point
      (first attached instance and each boundary ``to_physical`` own exactly
      one stream; identities superseded before the first attachment correctly
      have none in the producer design); view stream identities == attached
      identities in both directions, and stream source ids must re-derive from
      the raw physical identities.

    collection root adjudication (2026-09-20, evidence: 2 of 45 clean field manifests,
    S19 a37d4c59): an "incomplete identity waypoint" -- a physical identity
    whose container_id, image_id or started_epoch_s element is None -- mirrors
    producer ``_attach``'s own attachability gate and is exempt from the
    must-own-a-stream set (the producer legally never attaches it; observed in
    the wild as a mid-chain pause/sandbox container reporting its containerd id
    before its start time). The exemption is exactly this attachability
    semantics and nothing else: waypoints stay in the rebuilt timeline and
    physical chain, boundary alignment still includes their transitions, any
    attachment whose own declared physical identity is incomplete is a forgery
    (the producer never attaches one), and every complete identity after the
    first attachment still requires exactly one stream.

    collection (2026-09-21, #79 open_condition_R49): a Pod with a re-validated
    GAP_POD_REPLACED row in ``pod_gaps`` lost its logs to the injected
    mechanism's Deployment rollout -- the bytes physically do not exist. For
    exactly that pod the attachment-presence and stream-coverage assertions
    relax to subset semantics (every attachment/source it does claim must
    still re-derive from the retained raw timeline); the missing tail is the
    declared gap and never counts as coverage. ``gap_exempted`` carries the
    view identities the caller bound by retained bytes for streams whose
    attach confirmation was lost to the same rollout. Every other pod keeps
    the full hard reconciliation; any failure anywhere still raises.

    Any mismatch raises QualityError with a locatable reason (uid, frame index
    or physical identity included). This is still retained-evidence
    reconciliation, never a backend-exhaustiveness certificate.
    """
    from . import log_archive as la
    from . import log_transport as lt

    def attachable(physical):
        """Producer `_attach` gate: an identity without a fully formed container
        instance (container id / image id / start time, i.e. elements [2], [4]
        and [5]) is an incomplete waypoint the producer legally never attaches."""
        return physical[2] is not None and physical[4] is not None and physical[5] is not None

    def frames_from_raw():
        if not watch_raw:
            return []
        _require(watch_raw.endswith(b"\n"), "archive_watch_partial_frame_in_retained_bytes")
        positions = tuple((chunk["byte_start"], chunk["byte_end"], chunk["received_epoch_s"]) for chunk in watch_chunks)

        def pairs(items):
            row = {}
            for key, value in items:
                _require(key not in row, "duplicate_watch_frame_field")
                row[key] = value
            return row

        parsed, offset = [], 0
        for index, line in enumerate(watch_raw.split(b"\n")[:-1]):
            _require(bool(line.strip()), "archive_watch_frame_empty:index=%d" % index)
            try:
                event = json.loads(line.decode("utf-8"), object_pairs_hook=pairs,
                                   parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite")))
            except (ValueError, UnicodeError):
                _require(False, "archive_watch_frame_invalid_json:index=%d" % index)
            _require(type(event) is dict and set(event) == {"type", "resource_version", "pod", "code"},
                     "archive_watch_frame_fields_outside_whitelist:index=%d" % index)
            la._text(event["resource_version"])
            _require(event["type"] in {"ADDED", "MODIFIED", "DELETED", "BOOKMARK", "ERROR"},
                     "archive_watch_frame_type_unknown:index=%d" % index)
            if event["type"] in {"ADDED", "MODIFIED", "DELETED"}:
                la._pod(event["pod"], typed_scope)
                _require(event["code"] is None and event["pod"]["resource_version"] == event["resource_version"],
                         "archive_watch_frame_identity_mismatch:index=%d" % index)
            else:
                _require(event["pod"] is None and (type(event["code"]) is int if event["type"] == "ERROR" else event["code"] is None),
                         "archive_watch_frame_control_invalid:index=%d" % index)
            end = offset + len(line) + 1
            parsed.append({"index": index, "byte_start": offset, "byte_end": end, "event": event,
                           "receipt": lt._chunk_receipt(positions, end - 1)})
            offset = end
        _require(offset == len(watch_raw), "archive_watch_frame_byte_accounting_mismatch")
        return parsed

    frames = frames_from_raw()

    # Independent physical timeline per discovered uid: initial projections
    # first (in initial order), then watch frames in retained byte order.
    timelines = {}
    for pod in initial["pods"]:
        uid = pod["uid"]
        _require(uid not in timelines, "archive_initial_pod_uid_repeated:uid=%s" % uid)
        timelines[uid] = {"first": pod, "latest": pod, "physicals": [la._physical(pod)], "transitions": [],
                          "last_rv": pod["resource_version"], "first_receipt": None, "deleted_receipt": None,
                          "deletion_requested_receipt": None,
                          "deletion_requested_seen": pod["deletion_requested_epoch_s"] is not None}
    for frame in frames:
        event = frame["event"]
        if event["pod"] is None:
            continue
        pod, uid = event["pod"], event["pod"]["uid"]
        timeline = timelines.get(uid)
        if timeline is None:
            timeline = timelines[uid] = {"first": pod, "latest": pod, "physicals": [la._physical(pod)],
                                         "transitions": [], "last_rv": None, "first_receipt": frame["receipt"],
                                         "deleted_receipt": None, "deletion_requested_receipt": None,
                                         "deletion_requested_seen": pod["deletion_requested_epoch_s"] is not None}
        previous_rv, rv = timeline["last_rv"], pod["resource_version"]
        _require(not (previous_rv is not None and previous_rv.isdigit() and rv.isdigit() and int(rv) < int(previous_rv)),
                 "archive_watch_resource_version_regressed:uid=%s:rv=%s:after=%s" % (uid, rv, previous_rv))
        timeline["last_rv"] = rv
        first = timeline["first"]
        _require(all(pod[key] == first[key] for key in ("creation_epoch_s", "namespace", "name", "owners")),
                 "archive_pod_creation_or_owner_identity_changed:uid=%s" % uid)
        current, previous = la._physical(pod), timeline["physicals"][-1]
        if current != previous:
            # Producer semantics: a physical change must be a new container
            # instance (container id and/or start time), not a bare counter move.
            _require(previous[2] != current[2] or previous[5] != current[5],
                     "archive_container_restarted_or_replaced:uid=%s" % uid)
            timeline["transitions"].append({"from": previous, "to": current, "rv": rv, "receipt": frame["receipt"]})
            timeline["physicals"].append(current)
        if pod["deletion_requested_epoch_s"] is not None and not timeline["deletion_requested_seen"]:
            timeline["deletion_requested_seen"] = True
            timeline["deletion_requested_receipt"] = frame["receipt"]
        if event["type"] == "DELETED":
            timeline["deleted_receipt"] = frame["receipt"]
        timeline["latest"] = pod

    # Declared events must project exactly the raw frames they claim to.
    events = manifest.get("events")
    _require(type(events) is list, "archive_events_not_a_list")
    _require(len(events) == len(frames),
             "archive_event_count_mismatch:manifest=%d:raw_frames=%d" % (len(events), len(frames)))
    for record, frame in zip(events, frames):
        index = frame["index"]
        _require(type(record) is dict, "archive_event_record_invalid:index=%d" % index)
        _require(record.get("sequence") == index, "archive_event_sequence_mismatch:index=%d" % index)
        _require(record.get("byte_start") == frame["byte_start"] and record.get("byte_end") == frame["byte_end"],
                 "archive_event_byte_range_mismatch:index=%d" % index)
        _require(0 <= record.get("byte_start", -1) < record.get("byte_end", -1) <= len(watch_raw),
                 "archive_event_byte_range_outside_watch_chain:index=%d" % index)
        _require(_equal(record.get("projection"), frame["event"]),
                 "archive_event_projection_mismatch:index=%d" % index)
        _require(record.get("received_epoch_s") == frame["receipt"],
                 "archive_event_receipt_mismatch:index=%d" % index)
        _require(_equal(receipt(record.get("artifact"))["data"],
                        {key: value for key, value in record.items() if key != "artifact"}),
                 "archive_event_projection_differs_from_file:index=%d" % index)
        overlapping = [chunk["raw"]["artifact_id"] for chunk in watch_chunks
                       if chunk["byte_start"] < frame["byte_end"] and chunk["byte_end"] > frame["byte_start"]]
        _require(record.get("chunk_artifact_ids") == overlapping,
                 "archive_event_chunk_binding_mismatch:index=%d" % index)

    # Three-way reconciliation: uid set, per-uid physical identity coverage,
    # view stream identity coverage.
    pods = manifest.get("pods")
    _require(type(pods) is dict, "archive_manifest_pods_not_a_map")
    discovered, declared = set(timelines), set(pods)
    _require(discovered == declared,
             "archive_discovered_uid_set_mismatch:missing_from_manifest=%s:not_in_retained_raw=%s"
             % (sorted(discovered - declared), sorted(declared - discovered)))
    streams_map = manifest.get("streams")
    _require(type(streams_map) is dict, "archive_manifest_streams_not_a_map")
    rebuilt_streams, attached_identities, restart_total, waypoint_count = {}, [], 0, 0
    for uid, timeline in timelines.items():
        entry = pods[uid]
        physicals = timeline["physicals"]
        _require(len(physicals) == len(timeline["transitions"]) + 1, "archive_timeline_shape_invalid:uid=%s" % uid)
        _require(_equal(entry.get("first"), timeline["first"]), "archive_pod_first_projection_mismatch:uid=%s" % uid)
        _require(_equal(entry.get("latest"), timeline["latest"]), "archive_pod_latest_projection_mismatch:uid=%s" % uid)
        _require(entry.get("last_resource_version") == timeline["last_rv"],
                 "archive_pod_resource_version_mismatch:uid=%s" % uid)
        _require(entry.get("first_received_epoch_s") is not None, "archive_pod_first_receipt_missing:uid=%s" % uid)
        if timeline["first_receipt"] is not None:  # watch-discovered pods carry the ADDED frame receipt
            _require(entry.get("first_received_epoch_s") == timeline["first_receipt"],
                     "archive_pod_first_receipt_mismatch:uid=%s" % uid)
        _require(entry.get("deleted_received_epoch_s") == timeline["deleted_receipt"],
                 "archive_pod_deleted_receipt_mismatch:uid=%s" % uid)
        _require((entry.get("deletion_requested_received_epoch_s") is not None) == timeline["deletion_requested_seen"],
                 "archive_pod_deletion_requested_presence_mismatch:uid=%s" % uid)
        if timeline["deletion_requested_receipt"] is not None:
            _require(entry.get("deletion_requested_received_epoch_s") == timeline["deletion_requested_receipt"],
                     "archive_pod_deletion_requested_receipt_mismatch:uid=%s" % uid)
        attachments = entry.get("attachments")
        gap_row = pod_gaps.get(uid)
        _require(type(attachments) is dict and (gap_row is not None or bool(attachments)),
                 "archive_pod_without_attachment:uid=%s" % uid)
        attached = []
        for attachment in attachments.values():
            _require(type(attachment) is dict and type(attachment.get("physical_identity")) is list
                     and len(attachment["physical_identity"]) == 6,
                     "archive_attachment_physical_invalid:uid=%s" % uid)
            _require(attachable(tuple(attachment["physical_identity"])),
                     "archive_attachment_physical_identity_incomplete:uid=%s" % uid)
            attached.append(tuple(attachment["physical_identity"]))
        _require(len(attached) == len(set(attached)), "archive_duplicate_attachment_physical:uid=%s" % uid)
        attached_set = set(attached)
        boundaries = entry.get("container_restarts")
        _require(type(boundaries) is list, "archive_container_restarts_not_a_list:uid=%s" % uid)
        restart_total += len(boundaries)
        if boundaries:
            first_from = boundaries[0].get("from_physical")
            _require(type(first_from) is list and tuple(first_from) in physicals,
                     "archive_restart_boundary_not_in_retained_raw:uid=%s" % uid)
            start_index = physicals.index(tuple(first_from))
            _require(len(boundaries) == len(physicals) - 1 - start_index,
                     "archive_restart_boundary_missing_after_first_attachment:uid=%s" % uid)
            for offset, boundary in enumerate(boundaries):
                position = start_index + offset
                _require(boundary.get("from_physical") == list(physicals[position])
                         and boundary.get("to_physical") == list(physicals[position + 1]),
                         "archive_restart_boundary_unaligned:uid=%s:index=%d" % (uid, offset))
                transition = timeline["transitions"][position]
                _require(boundary.get("resource_version") == transition["rv"],
                         "archive_restart_boundary_rv_mismatch:uid=%s:index=%d" % (uid, offset))
                _require(boundary.get("observed_epoch_s") == transition["receipt"],
                         "archive_restart_boundary_receipt_mismatch:uid=%s:index=%d" % (uid, offset))
        else:
            start_index = len(physicals) - 1
        
        # start time missing) mirror the producer's own attachability gate and
        # cannot own a stream; every COMPLETE identity from the first attachment
        # onward still requires exactly one.
        expected = set(physical for physical in physicals[start_index:] if attachable(physical))
        waypoint_count += sum(1 for physical in physicals[start_index:] if not attachable(physical))
        if gap_row is None:
            _require(attached_set == expected,
                     "archive_attachment_physical_set_mismatch:uid=%s:not_discovered_in_raw=%s:discovered_without_stream=%s"
                     % (uid, [list(p) for p in attached_set - expected], [list(p) for p in expected - attached_set]))
        else:
            
            # every attachment it does claim must still re-derive from raw.
            _require(attached_set <= expected, "archive_attachment_physical_set_mismatch:uid=%s:not_discovered_in_raw=%s"
                     % (uid, [list(p) for p in attached_set - expected]))
        for attachment in attachments.values():
            projection = attachment.get("container_projection")
            _require(type(projection) is dict, "archive_attachment_container_projection_missing:uid=%s" % uid)
            identity = tuple(attachment["physical_identity"])
            _require(identity[1] == projection.get("name") and identity[2] == projection.get("container_id")
                     and identity[3] == projection.get("restart_count"),
                     "archive_attachment_identity_disagrees_with_container_projection:uid=%s" % uid)
            attached_identities.append((uid, projection.get("container_id"), projection.get("restart_count")))
        # Stream source ids are derived from the physical identity; the manifest
        # stream map must re-derive from raw bytes, in physical order. The
        # producer never creates a source for an incomplete waypoint.
        expected_sources = ["pod-" + _sha(_canonical(list(physical)).encode("utf-8"))[:24]
                            for physical in physicals[start_index:] if attachable(physical)]
        if gap_row is None:
            _require(entry.get("source_ids") == expected_sources, "archive_pod_source_ids_not_derived_from_raw:uid=%s" % uid)
            _require(entry.get("source_id") == expected_sources[-1], "archive_pod_current_source_mismatch:uid=%s" % uid)
            for source, _ in zip(expected_sources, physicals[start_index:]):
                rebuilt_streams[source] = uid
        else:
            
            # from the raw physical timeline (subset); its unregistered tail
            # is exactly the declared replacement gap.
            _require(type(entry.get("source_ids")) is list and set(entry["source_ids"]) <= set(expected_sources),
                     "archive_pod_source_ids_not_derived_from_raw:uid=%s" % uid)
            _require(entry.get("source_id") is None or entry["source_id"] in expected_sources,
                     "archive_pod_current_source_mismatch:uid=%s" % uid)
            for source in entry["source_ids"]:
                rebuilt_streams[source] = uid
    _require(streams_map == rebuilt_streams, "archive_stream_map_not_derived_from_discovered_identities")
    view_identities = [(row.get("pod_uid"), row.get("container_id"), row.get("restart_count")) for row in view_streams]
    _require(len(view_identities) == len(set(view_identities)), "archive_duplicate_view_stream_identity")
    _require(len(attached_identities) == len(set(attached_identities)), "archive_duplicate_attached_identity")
    _require(set(view_identities) == set(attached_identities) | set(gap_exempted),
             "archive_view_identity_set_mismatch:view_without_attachment=%s:attachment_without_view=%s"
             % ([list(key) for key in set(view_identities) - set(attached_identities) - set(gap_exempted)],
                [list(key) for key in set(attached_identities) - set(view_identities)]))
    return {"archive_id": manifest.get("archive_id"), "pods": len(timelines),
            "watch_frames": len(frames), "restart_boundaries": restart_total,
            "incomplete_identity_waypoints": waypoint_count,
            "proven_replacement_gaps": sorted(pod_gaps),
            "attachability": "incomplete_identity_waypoints_exempt_from_stream_coverage_per_producer_attach_gate",
            "streams_per_pod": {uid: len(pods[uid].get("attachments", {})) for uid in timelines}}


def _bounded_coverage(contract, phase, modality, bundle, coverage, inventory, policy, window, bound):
    """Finite predeclared query/identity/ingestion checks, NOT omniscient coverage."""
    if not bound or not _identity(coverage, contract, phase=phase) or coverage.get("evidence_kind") != "observed" or _known_non_observed(coverage) or coverage.get("window") != window.to_dict():
        return "NOT_ASSESSED", "coverage_window_identity_or_file_unverified", {}
    anchor = coverage.get("clock_mapping", {})
    try:
        for key in ("monotonic_s", "unix_epoch_s", "uncertainty_s"):
            _number(anchor[key])
        if anchor["uncertainty_s"] > policy["max_clock_uncertainty_s"]:
            return "NOT_ASSESSED", "clock_mapping_uncertainty_exceeds_policy", {}
    except (ValueError, KeyError, TypeError):
        return "NOT_ASSESSED", "missing_explicit_utc_monotonic_mapping", {}
    slices = {}
    for query in bundle.get("queries", []):
        if query.get("status") != "ok" or any(k in query.get("issues", []) for k in
                                                ("truncation_suspected_limit_hit", "truncation_suspected_byte_limit", "aggregate_raw_budget_exhausted")):
            return "FAIL", "query_failed_or_capacity_boundary_hit", {}
        provenance = query.get("backend_provenance", {})
        clocks = provenance.get("log_reply", {}) if modality == "logs" else provenance
        started, ended = clocks.get("started_monotonic_s"), clocks.get("ended_monotonic_s")
        try:
            _number(started)
            _number(ended)
            _require(ended >= started, "query clock reversed")
        except (ValueError, TypeError):
            return "NOT_ASSESSED", "query_timing_not_observed", {}
        start_utc_lower = anchor["unix_epoch_s"] + started - anchor["monotonic_s"] - anchor["uncertainty_s"]
        if not math.isfinite(start_utc_lower):
            return "FAIL", "clock_mapping_overflow", {}
        if start_utc_lower < window.end + policy["min_ingestion_wait_s"][modality]:
            return "FAIL", "query_started_before_declared_ingestion_wait", {}
        if modality == "metrics":
            params = query.get("parameters", {})
            if not (type(params.get("start")) in (int, float) and type(params.get("end")) in (int, float)
                    and params["start"] <= window.start and params["end"] >= window.end
                    and type(params.get("step")) in (int, float) and params["step"] == contract.to_dict()["metric_interval_s"]):
                return "FAIL", "metric_query_does_not_cover_declared_window_or_step", {}
        elif modality == "traces":
            params, segment = query.get("parameters", {}), query.get("slice_us")
            if type(segment) is not list or len(segment) != 2 or not all(type(v) is int for v in segment):
                return "NOT_ASSESSED", "trace_slice_provenance_missing", {}
            if type(params.get("start")) is not int or type(params.get("end")) is not int:
                return "NOT_ASSESSED", "trace_query_time_parameters_missing", {}
            if (query.get("limit_hit") is not False or type(query.get("lookback_s")) not in (int, float)
                    or query["lookback_s"] < policy["jaeger_min_lookback_s"]
                    or params.get("start") > max(0, segment[0] - int(policy["jaeger_min_lookback_s"] * 1e6))
                    or params.get("end") < segment[1]):
                return "FAIL", "trace_limit_or_lookback_policy_not_met", {}
            slices.setdefault(query.get("query_id"), []).append(segment)
        else:
            source = query.get("source", {})
            if provenance.get("uid_verification") != "before_and_after_match" or provenance.get("pod_uid") != source.get("pod_uid"):
                return "NOT_ASSESSED", "log_source_identity_not_confirmed", {}
            for ref in source.get("old_pod_evidence_refs", []):
                if inventory is None or inventory["artifacts"].get(ref, {}).get("status") != "PASS":
                    return "NOT_ASSESSED", "required_old_pod_evidence_unavailable", {}
    if modality == "traces":
        for segments in slices.values():
            cursor = int(window.start * 1e6)
            for left, right in sorted(segments):
                if left > cursor or right <= left:
                    return "FAIL", "trace_search_slice_gap", {}
                cursor = max(cursor, right)
            if cursor < int(window.end * 1e6):
                return "FAIL", "trace_search_does_not_reach_phase_end", {}
    return "PASS", "predeclared_bounded_query_checks_satisfied", {"accepted_scope": "bounded_window_query_evidence_not_all_requests_or_all_spans",
                                                                   "residual_limits": "backend_exhaustiveness_and_unobserved_history_not_claimed"}
