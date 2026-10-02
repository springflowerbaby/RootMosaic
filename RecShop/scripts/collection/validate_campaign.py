"""Source-bound admission for versioned collection campaigns.

Campaign admission binds each selected replicate to its scientific condition,
collector source, runtime template, and unique attempt identity. Optional
historical samples are evidence references only; they never qualify a new run.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path, PurePosixPath
from typing import Any

from . import contract as c
from . import review_samples as dp
from . import quality as q

CAMPAIGN_SCHEMA = "m1-repeat-campaign-v2"
MARKER_SCHEMA = "m1-repeat-admission-v2"
MARKER_NAME = "repeat-admission.json"
CAMPAIGN_NAME = "campaign.json"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ATTEMPT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_ROOT = Path(__file__).resolve().parents[2]
_CATALOG_PATH = _ROOT / 'configs/collection/scenarios.json'
_COLLECTOR_PATHS = {
    'scenario_runner': 'scripts/collection/scenario_runner.py',
    'scenario_definitions': 'scripts/collection/scenario_definitions.py',
    "contract": 'scripts/collection/contract.py',
    "runner": 'scripts/collection/runner.py',
    "journal": 'scripts/collection/journal.py',
    "annotations": 'scripts/collection/annotations.py',
    "primitives": 'scripts/collection/primitives.py',
    "quality": 'scripts/collection/quality.py',
    "telemetry": 'scripts/collection/telemetry.py',
    "primitives_db": 'scripts/collection/primitives_db.py',
    "gateway": 'scripts/collection/gateway.py',
    "auxiliary": 'scripts/collection/auxiliary.py',
    "pricing_route": 'scripts/collection/pricing_route.py',
    "pricing_route_model": 'scripts/collection/pricing_route_model.py',
    "release_gate": 'scripts/collection/release_gate.py',
    "observability": 'scripts/collection/observability.py',
    "sampling": 'scripts/collection/sampling.py',
    "live_runtime": 'scripts/collection/live_runtime.py',
    "log_archive": 'scripts/collection/log_archive.py',
    "log_transport": 'scripts/collection/log_transport.py',
    "log_driver_kubectl": 'scripts/collection/log_driver_kubectl.py',
    "workload": 'scripts/collection/workload.py',
    "environment": 'scripts/collection/environment.py',
    'campaign_runtime': 'scripts/collection/campaign_runtime.py',
    'run_scenario': 'scripts/collection/run_scenario.py',
    "ops.maintenance_receipt": 'ops/metrics/maintenance_receipt.py',
}
_REQUIRED_SOURCE_PATHS = {
    *_COLLECTOR_PATHS.values(),
    'scripts/collection/prepare_campaign.py',
    'scripts/collection/validate_campaign.py',
    'scripts/collection/run_campaign.py',
}
_TEMPLATE_KEYS = {"contract_rules", "context_fingerprints", "repo_root", "output_root", "quality_rules_sha256"}
_FINGERPRINT_KEYS = ("quality_rules", "generator", "annotation_rules")
_PHASES = {"pre_fault": (0, 300), "during_fault": (300, 600), "post_recovery": (600, 900)}
_ACCEPTED_HISTORY_STATES = {"P0_COUNTED", "P0_COUNTED_REVIEWED", "P0_COUNTED_LEGACY"}


class RepeatAdmissionError(ValueError):
    """A campaign or one of its selected slots is not admitted."""


def _sha_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return c.canonical_json(value).encode("utf-8")


def _read_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_bytes().decode("utf-8-sig"))
    except FileNotFoundError:
        raise RepeatAdmissionError(label + " missing") from None
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise RepeatAdmissionError(label + " is not valid UTF-8 JSON") from None


def _safe_relative_file(root: Path, relative: str) -> Path:
    if type(relative) is not str or not relative or "\\" in relative:
        raise RepeatAdmissionError("source path must be a repo-relative POSIX path")
    parsed = PurePosixPath(relative)
    if parsed.is_absolute() or any(part in {"", ".", ".."} for part in parsed.parts):
        raise RepeatAdmissionError("source path escaped repository root")
    path = root.joinpath(*parsed.parts)
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
    except (OSError, ValueError):
        raise RepeatAdmissionError("source file is missing or outside repository") from None
    if not resolved.is_file():
        raise RepeatAdmissionError("source path is not a file")
    return resolved


def _verify_source_files(root: Path, expected: dict[str, str]) -> list[str]:
    """Return named source drift errors; exposed for focused local tests."""
    errors: list[str] = []
    if type(expected) is not dict:
        return ["source_files_must_be_object"]
    missing = sorted(_REQUIRED_SOURCE_PATHS - set(expected))
    if missing:
        errors.append("source_files_missing_required:" + ",".join(missing))
    for relative, expected_sha in sorted(expected.items()):
        if not isinstance(expected_sha, str) or not _SHA256.fullmatch(expected_sha):
            errors.append("source_sha256_invalid:" + str(relative))
            continue
        try:
            path = _safe_relative_file(root, relative)
        except RepeatAdmissionError:
            errors.append("source_file_missing_or_unsafe:" + str(relative))
            continue
        if _sha_bytes(path.read_bytes()) != expected_sha:
            errors.append("source_drift:" + relative)
    return errors


def _current_collector_fingerprint(root: Path) -> str:
    current = {name: _sha_bytes(_safe_relative_file(root, relative).read_bytes())
               for name, relative in _COLLECTOR_PATHS.items()}
    return c.canonical_sha256(current)


def _campaign_rounds(campaign: dict[str, Any]) -> list[int]:
    budget = campaign.get("budget") if type(campaign.get("budget")) is dict else {}
    rounds = budget.get("rounds")
    if type(rounds) is not list or not rounds or any(type(value) is not int or value < 1 for value in rounds):
        return []
    if rounds != sorted(set(rounds)):
        return []
    return rounds


def _validate_budget(budget: Any, scenario_count: int | None = None) -> list[str]:
    if type(budget) is not dict:
        return ["budget_must_be_object"]
    rounds = budget.get("rounds")
    if type(rounds) is not list or not rounds or any(type(value) is not int or value < 1 for value in rounds):
        return ["budget_rounds_must_be_positive_integers"]
    if rounds != sorted(set(rounds)):
        return ["budget_rounds_must_be_unique_ascending"]
    cases = budget.get("cases_per_round")
    count = scenario_count if scenario_count is not None else cases
    if type(cases) is not int or cases < 1 or count != cases:
        return ["budget_cases_per_round_must_match_scenarios"]
    firstpass = budget.get("firstpass_count")
    new_slots = budget.get("new_slots")
    target = budget.get("target_total")
    if type(firstpass) is not int or firstpass < 0:
        return ["budget_firstpass_count_must_be_nonnegative_integer"]
    expected_new = cases * len(rounds)
    if type(new_slots) is not int or new_slots != expected_new:
        return ["budget_new_slots_must_match_rounds_and_cases"]
    if type(target) is not int or target != firstpass + expected_new:
        return ["budget_target_total_must_equal_history_plus_new_slots"]
    if set(budget) != {"firstpass_count", "new_slots", "target_total", "rounds", "cases_per_round"}:
        return ["budget_fields_unsupported"]
    return []


def _template_errors(template: Any) -> list[str]:
    if type(template) is not dict or set(template) != _TEMPLATE_KEYS:
        return ["scenario_template_shape_invalid"]
    errors = []
    if type(template.get("contract_rules")) is not dict or not template["contract_rules"]:
        errors.append("scenario_template_contract_rules_missing")
    fingerprints = template.get("context_fingerprints")
    if type(fingerprints) is not dict:
        errors.append("scenario_template_fingerprints_missing")
    else:
        for key in _FINGERPRINT_KEYS:
            if not isinstance(fingerprints.get(key), str) or not _SHA256.fullmatch(fingerprints.get(key, "")):
                errors.append("scenario_template_fingerprint_invalid:" + key)
    for key in ("repo_root", "output_root"):
        value = template.get(key)
        if not isinstance(value, str) or not Path(value).is_absolute():
            errors.append("scenario_template_path_invalid:" + key)
    quality_sha = template.get("quality_rules_sha256")
    if not isinstance(quality_sha, str) or not _SHA256.fullmatch(quality_sha):
        errors.append("scenario_template_quality_sha_invalid")
    elif type(fingerprints) is dict and fingerprints.get("quality_rules") != quality_sha:
        errors.append("scenario_template_quality_sha_mismatch")
    return errors


def _row_shape_errors(row: Any, rounds: list[int]) -> list[str]:
    if type(row) is not dict:
        return ["scenario_row_not_object"]
    errors = []
    for field in ("design_version", "scenario_id", "condition_id", "condition_path", "source_sha256", "gate_sha256"):
        if not isinstance(row.get(field), str) or not row[field]:
            errors.append("scenario_field_invalid:" + field)
    for field in ("condition_sha256", "condition_signature_sha256", "source_sha256", "gate_sha256"):
        value = row.get(field)
        if not isinstance(value, str) or not _SHA256.fullmatch(value):
            errors.append("scenario_sha256_invalid:" + field)
    condition = row.get("condition")
    expected_fields = {"scenario", "faults", "request_profile", "phases", "metric_interval_s"}
    if type(condition) is not dict or set(condition) != expected_fields:
        errors.append("condition_signature_shape_invalid")
    else:
        if row.get("condition_signature_sha256") != c.canonical_sha256(condition):
            errors.append("condition_signature_hash_mismatch")
        if condition.get("scenario") != {"design_version": row.get("design_version"), "scenario_id": row.get("scenario_id")}:
            errors.append("condition_scenario_identity_mismatch")
        if condition.get("metric_interval_s") != 2.0 or type(condition.get("metric_interval_s")) is not float:
            errors.append("condition_metric_interval_must_be_2s")
        expected_phases = {name: {"start": start, "end": end, "time_basis": "run_offset"}
                           for name, (start, end) in _PHASES.items()}
        if condition.get("phases") != expected_phases:
            errors.append("condition_phase_geometry_must_be_300_300_300")
        faults = condition.get("faults")
        if type(faults) is not list or not faults or any(type(item) is not dict or not item.get("normalized_root_entity") for item in faults):
            errors.append("condition_fault_identity_invalid")
    errors.extend(_template_errors(row.get("template")))
    attempts = row.get("repeat_attempts")
    expected_keys = {str(value) for value in rounds}
    if type(attempts) is not dict or set(attempts) != expected_keys:
        errors.append("repeat_attempts_must_match_selected_rounds")
    elif any(not isinstance(value, str) or not _ATTEMPT_ID.fullmatch(value) for value in attempts.values()):
        errors.append("repeat_attempt_id_invalid")
    history = row.get("firstpass_history")
    if history is not None:
        errors.extend(_history_shape_errors(history, row))
    return errors


def _history_shape_errors(history: Any, row: dict[str, Any] | None = None) -> list[str]:
    if type(history) is not dict:
        return ["firstpass_history_not_object"]
    errors = []
    for key in ("attempt_id", "scenario_id", "condition_id", "design_version", "count_kind", "raw_p0_status",
                "repo_root", "output_root", "queue_state"):
        if not isinstance(history.get(key), str) or not history[key]:
            errors.append("firstpass_history_field_invalid:" + key)
    if type(history.get("replicate")) is not int or history["replicate"] < 1:
        errors.append("firstpass_history_replicate_invalid")
    if history.get("queue_state") not in _ACCEPTED_HISTORY_STATES:
        errors.append("firstpass_history_state_unaccepted")
    record = history.get("count_record")
    if type(record) is not dict or not isinstance(record.get("path"), str) or not _SHA256.fullmatch(str(record.get("sha256", ""))):
        errors.append("firstpass_history_count_record_invalid")
    refs = history.get("refs")
    if type(refs) is not dict:
        errors.append("firstpass_history_refs_missing")
    else:
        for key in ("source_manifest", "summary", "contract", "quality"):
            ref = refs.get(key)
            if type(ref) is not dict or not isinstance(ref.get("path"), str) or not _SHA256.fullmatch(str(ref.get("sha256", ""))):
                errors.append("firstpass_history_ref_invalid:" + key)
    errors.extend(_template_errors({
        "contract_rules": history.get("contract_rules"),
        "context_fingerprints": history.get("context_fingerprints"),
        "repo_root": history.get("repo_root"), "output_root": history.get("output_root"),
        "quality_rules_sha256": history.get("quality_rules_sha256"),
    }))
    if row is not None:
        for key in ("scenario_id", "condition_id", "design_version"):
            if history.get(key) != row.get(key):
                errors.append("firstpass_history_condition_mismatch:" + key)
        if history.get("contract_rules") != row.get("template", {}).get("contract_rules"):
            errors.append("firstpass_history_contract_rules_mismatch")
        expected = row.get("template", {}).get("context_fingerprints", {})
        observed = history.get("context_fingerprints", {})
        if any(observed.get(key) != expected.get(key) for key in _FINGERPRINT_KEYS):
            errors.append("firstpass_history_fingerprints_mismatch")
        if history.get("quality_rules_sha256") != row.get("template", {}).get("quality_rules_sha256"):
            errors.append("firstpass_history_quality_rules_mismatch")
    return errors


def _resolve_any_file(raw_path: Any, label: str) -> Path:
    if not isinstance(raw_path, str) or not raw_path:
        raise RepeatAdmissionError(label + " path missing")
    path = Path(raw_path)
    if not path.is_absolute():
        path = _ROOT / path
    try:
        resolved = path.resolve(strict=True)
    except OSError:
        raise RepeatAdmissionError(label + " file missing") from None
    if not resolved.is_file():
        raise RepeatAdmissionError(label + " path is not a file")
    return resolved


def _verify_input_sources(campaign: dict[str, Any]) -> list[str]:
    inputs = campaign.get("input_sources")
    if type(inputs) is not dict or "conditions" not in inputs:
        return ["campaign_condition_source_missing"]
    errors = []
    if set(inputs) - {"conditions", "history"}:
        errors.append("campaign_input_source_fields_unsupported")
    for label, ref in inputs.items():
        if type(ref) is not dict or set(ref) != {"path", "sha256"}:
            errors.append("campaign_input_source_invalid:" + label)
            continue
        if not isinstance(ref.get("path"), str) or not isinstance(ref.get("sha256"), str) or not _SHA256.fullmatch(ref["sha256"]):
            errors.append("campaign_input_source_invalid:" + label)
            continue
        if label == "history":
            # The importer verified these raw references once before freezing
            # the campaign. Runtime admission consumes the sealed identity,
            # not the old v58/history ledger as a template dependency.
            continue
        try:
            raw = _resolve_any_file(ref["path"], label).read_bytes()
        except RepeatAdmissionError:
            errors.append("campaign_input_source_missing:" + label)
            continue
        if _sha_bytes(raw) != ref["sha256"]:
            errors.append("campaign_input_source_drift:" + label)
    return errors


def _verify_condition_file(row: dict[str, Any]) -> list[str]:
    try:
        path = _safe_relative_file(_ROOT, row.get("condition_path"))
        raw = path.read_bytes()
    except RepeatAdmissionError:
        return ["condition_file_missing_or_unsafe:" + str(row.get("scenario_id"))]
    if _sha_bytes(raw) != row.get("condition_sha256"):
        return ["condition_file_sha_mismatch:" + str(row.get("scenario_id"))]
    try:
        value = json.loads(raw.decode("utf-8-sig"))
        candidates = [item for item in value if type(item) is dict and item.get("condition_id") == row["condition_id"]] if type(value) is list else [value]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError):
        return ["condition_file_invalid:" + str(row.get("scenario_id"))]
    if len(candidates) != 1:
        return ["condition_file_identity_mismatch:" + str(row.get("scenario_id"))]
    seed = candidates[0]
    try:
        data = seed["contract"]
        if seed.get("scenario_id") != row["scenario_id"] or seed.get("condition_id") != row["condition_id"]:
            raise ValueError("identity")
        if data.get("scenario") != {"design_version": row["design_version"], "scenario_id": row["scenario_id"]}:
            raise ValueError("design")
        if dp.semantic_signature(data) != row["condition"]:
            raise ValueError("science")
        if data.get("rules") != row["template"]["contract_rules"]:
            raise ValueError("rules")
        context = data.get("context", {})
        if context.get("repo_root") != row["template"]["repo_root"] or context.get("output_root") != row["template"]["output_root"]:
            raise ValueError("roots")
        fingerprints = context.get("fingerprints", {})
        for key in _FINGERPRINT_KEYS:
            if fingerprints.get(key) != row["template"]["context_fingerprints"].get(key):
                raise ValueError("fingerprints")
        rules = q.QualityRules.from_dict(seed["quality_rules_payload"])
        if rules.sha256 != row["template"]["quality_rules_sha256"]:
            raise ValueError("quality")
        registry = c.DesignRegistry.from_json(_CATALOG_PATH.read_bytes())
        c.RunContract.from_dict(data, registry)
    except (KeyError, TypeError, ValueError, c.ContractError) as exc:
        return ["condition_file_template_mismatch:" + str(row.get("scenario_id")) + ":" + str(exc)]
    return []


def verify_history_entry(history: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    """Verify one imported historical count against its actual evidence bytes.

    This is used by the one-time history importer. Runtime slot admission uses
    the sealed identity only and never treats this result as a new PASS.
    """
    shape = _history_shape_errors(history, row)
    if shape:
        raise RepeatAdmissionError("history shape invalid: " + ",".join(sorted(set(shape))))
    refs = history["refs"]
    resolved = {name: _resolve_any_file(refs[name]["path"], "history " + name) for name in ("source_manifest", "summary", "contract", "quality")}
    record_path = _resolve_any_file(history["count_record"]["path"], "history count record")
    raw = {name: resolved[name].read_bytes() for name in resolved}
    raw["count_record"] = record_path.read_bytes()
    for name, value in raw.items():
        expected = history["count_record"]["sha256"] if name == "count_record" else refs[name]["sha256"]
        if _sha_bytes(value) != expected:
            raise RepeatAdmissionError("history " + name + " bytes differ from imported reference")
    try:
        source = json.loads(raw["source_manifest"].decode("utf-8-sig"))
        summary = json.loads(raw["summary"].decode("utf-8"))
        actual = json.loads(raw["contract"].decode("utf-8"))
        quality_artifact = json.loads(raw["quality"].decode("utf-8"))
        quality = summary["inner_result"]["quality"]
        p0 = quality["p0_assessment"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError):
        raise RepeatAdmissionError("history evidence JSON or SUMMARY structure invalid") from None
    context = actual.get("context", {})
    replicate = history["replicate"]
    if type(replicate) is not int or context.get("replicate") != replicate:
        raise RepeatAdmissionError("history replicate identity differs from actual contract")
    attempt = history["attempt_id"]
    if (summary.get("condition_id") != row["condition_id"] or quality.get("attempt_id") != attempt
            or context.get("attempt_id") != attempt or actual.get("scenario") != {"design_version": row["design_version"], "scenario_id": row["scenario_id"]}):
        raise RepeatAdmissionError("history SUMMARY/contract identity differs from condition")
    if quality.get("sample_purpose") != "formal" or actual.get("purpose") != "formal" or actual.get("protocol_status") != "formal_frozen":
        raise RepeatAdmissionError("history sample is not a frozen formal run")
    if actual.get("metric_interval_s") != 2.0 or any(
            actual.get("phases", {}).get(name, {}).get("end") - actual.get("phases", {}).get(name, {}).get("start") != 300
            for name in _PHASES):
        raise RepeatAdmissionError("history does not retain the 2s and 300/300/300 protocol")
    if dp.semantic_signature(actual) != row["condition"]:
        raise RepeatAdmissionError("history scientific condition differs from template")
    if type(source) is list:
        source_rows = [item for item in source if type(item) is dict and item.get("condition_id") == row["condition_id"]]
    elif type(source) is dict and type(source.get("condition")) is dict:
        source_rows = [source["condition"]]
    else:
        source_rows = []
    if (len(source_rows) != 1 or source_rows[0].get("scenario_id") != row["scenario_id"]
            or source_rows[0].get("condition_id") != row["condition_id"]
            or dp.semantic_signature(source_rows[0].get("contract", {})) != row["condition"]):
        raise RepeatAdmissionError("history source manifest identity/science differs from condition")
    if quality_artifact != quality:
        raise RepeatAdmissionError("history quality artifact differs from SUMMARY")
    if p0.get("status") != history.get("raw_p0_status") or p0.get("p0s", {}).get("process_recoverable", {}).get("status") != "PASS":
        raise RepeatAdmissionError("history raw P0 or recovery evidence differs")
    if (quality.get("contract_sha256") != c.canonical_sha256(actual)
            or summary.get("inner_result", {}).get("contract_sha256") != c.canonical_sha256(actual)):
        raise RepeatAdmissionError("history SUMMARY contract hash does not match actual contract")
    old_hashes = summary.get("source_hashes")
    if type(old_hashes) is not dict or actual.get("context", {}).get("fingerprints", {}).get("collector") != c.canonical_sha256(old_hashes):
        raise RepeatAdmissionError("history collector fingerprint differs from SUMMARY source hashes")
    if actual.get("rules") != history["contract_rules"]:
        raise RepeatAdmissionError("history contract rules differ from imported record")
    if context.get("repo_root") != history["repo_root"] or context.get("output_root") != history["output_root"]:
        raise RepeatAdmissionError("history repo/output roots differ from actual contract")
    fingerprints = context.get("fingerprints", {})
    for key in _FINGERPRINT_KEYS:
        if fingerprints.get(key) != history["context_fingerprints"].get(key):
            raise RepeatAdmissionError("history fingerprint differs: " + key)
    if history["quality_rules_sha256"] != history["context_fingerprints"].get("quality_rules"):
        raise RepeatAdmissionError("history quality-rules SHA differs")
    if history["count_kind"] == "raw_p0_pass" and history["raw_p0_status"] != "PASS":
        raise RepeatAdmissionError("raw-P0 history was not a machine PASS")
    if history["count_kind"] == "v08_same_attempt_reviewed" and history["raw_p0_status"] != "FAIL":
        raise RepeatAdmissionError("reviewed history must preserve raw machine FAIL")
    if history["count_kind"] == "s06_historical_legacy" and (row["scenario_id"] != "S06" or history["raw_p0_status"] != "PASS"):
        raise RepeatAdmissionError("legacy history identity/status is invalid")
    if history["count_kind"] not in {"raw_p0_pass", "v08_same_attempt_reviewed", "s06_historical_legacy"}:
        raise RepeatAdmissionError("history count kind is unsupported")
    try:
        rows = list(csv.DictReader(raw["count_record"].decode("utf-8-sig").splitlines()))
    except (UnicodeDecodeError, csv.Error):
        raise RepeatAdmissionError("history count record is not a supported CSV ledger") from None
    matches = [item for item in rows if item.get("scenario_id") == row["scenario_id"]]
    if len(matches) != 1:
        raise RepeatAdmissionError("history count ledger scenario identity is not unique")
    record = matches[0]
    expected_state = {"raw_p0_pass": "P0_COUNTED", "v08_same_attempt_reviewed": "P0_COUNTED_REVIEWED",
                      "s06_historical_legacy": "P0_COUNTED_LEGACY"}[history["count_kind"]]
    if (record.get("condition_id") != row["condition_id"] or record.get("attempt_id") != attempt
            or record.get("state") != expected_state or record.get("state") != history["queue_state"]):
        raise RepeatAdmissionError("history count ledger identity/state differs")
    current_hashes = {name: _sha_bytes(_safe_relative_file(_ROOT, relative).read_bytes())
                      for name, relative in _COLLECTOR_PATHS.items()}
    source_compatibility = {name: {"historical_sha256": old_hashes.get(name), "current_sha256": value}
                            for name, value in current_hashes.items() if old_hashes.get(name) != value}
    compatible_changes = {'scenario_runner', "runner", "release_gate", "log_archive", "observability"}
    if set(source_compatibility) - compatible_changes:
        raise RepeatAdmissionError("history source drift outside reviewed compatibility set: "
                                   + ",".join(sorted(set(source_compatibility) - compatible_changes)))
    return {"replicate": replicate, "attempt_id": attempt, "count_kind": history["count_kind"],
            "raw_p0_status": history["raw_p0_status"], "count_record_sha256": history["count_record"]["sha256"],
            "refs": {name: refs[name]["sha256"] for name in refs},
            "source_compatibility": source_compatibility}


def _history_summary(history: Any) -> Any:
    if type(history) is not dict:
        return None
    return {key: history.get(key) for key in ("replicate", "attempt_id", "count_kind", "raw_p0_status", "queue_state")}


def _history_schedule_errors(rows: list[dict[str, Any]], rounds: list[int]) -> list[str]:
    """Reject duplicate history identities and history/new-slot overlap."""
    seen_slots, errors = set(), []
    for row in rows:
        if type(row) is not dict or type(row.get("firstpass_history")) is not dict:
            continue
        history = row["firstpass_history"]
        slot = (row.get("scenario_id"), history.get("replicate"))
        if slot in seen_slots:
            errors.append("firstpass_history_slot_duplicate")
        seen_slots.add(slot)
        if type(slot[1]) is int and slot[1] in rounds:
            errors.append("firstpass_history_overlaps_selected_round:" + str(row.get("scenario_id")))
    return errors


def _campaign_content_errors(campaign: Any) -> list[str]:
    if type(campaign) is not dict:
        return ["campaign_not_object"]
    errors = []
    if campaign.get("schema_version") != CAMPAIGN_SCHEMA:
        errors.append("campaign_schema_unsupported")
    for key in ("campaign_id", "source_fingerprint", "collector_source_fingerprint", "release_gate_sha256"):
        value = campaign.get(key)
        if not isinstance(value, str) or not value:
            errors.append("campaign_field_invalid:" + key)
    for key in ("source_fingerprint", "collector_source_fingerprint", "release_gate_sha256"):
        if not isinstance(campaign.get(key), str) or not _SHA256.fullmatch(campaign.get(key, "")):
            errors.append("campaign_sha256_invalid:" + key)
    source_files = campaign.get("source_files")
    if type(source_files) is not dict:
        errors.append("source_files_missing")
    else:
        if campaign.get("source_fingerprint") != c.canonical_sha256(source_files):
            errors.append("source_fingerprint_mismatch")
        if source_files.get(_COLLECTOR_PATHS["release_gate"]) != campaign.get("release_gate_sha256"):
            errors.append("release_gate_fingerprint_mismatch")
        try:
            if _current_collector_fingerprint(_ROOT) != campaign.get("collector_source_fingerprint"):
                errors.append("collector_source_fingerprint_drift")
        except RepeatAdmissionError:
            errors.append("collector_source_fingerprint_unavailable")
    runtime = campaign.get("runtime")
    try:
        from . import environment
        environment.verify_ref(runtime["environment"])
    except (ValueError, OSError, KeyError, TypeError):
        errors.append("campaign_environment_binding_invalid")
    if type(runtime) is not dict or set(runtime) != {"python_executable", "output_root", "batch_root", "environment"}:
        errors.append("campaign_runtime_shape_invalid")
    else:
        for key in ("python_executable", "output_root", "batch_root"):
            if not isinstance(runtime.get(key), str) or not Path(runtime[key]).is_absolute():
                errors.append("campaign_runtime_path_invalid:" + key)
    rows = campaign.get("scenarios")
    if type(rows) is not list or not rows:
        errors.append("campaign_scenarios_required")
        return errors
    if type(campaign.get("scenario_count")) is not int or campaign.get("scenario_count") != len(rows):
        errors.append("campaign_scenario_count_mismatch")
    errors.extend(_validate_budget(campaign.get("budget"), len(rows)))
    rounds = _campaign_rounds(campaign)
    if not rounds:
        errors.append("campaign_rounds_invalid")
    scenario_ids = [row.get("scenario_id") if type(row) is dict else None for row in rows]
    condition_ids = [row.get("condition_id") if type(row) is dict else None for row in rows]
    if None in scenario_ids or len(set(scenario_ids)) != len(rows):
        errors.append("scenario_ids_must_be_unique")
    if None in condition_ids or len(set(condition_ids)) != len(rows):
        errors.append("condition_ids_must_be_unique")
    attempts, history_attempts, history_count = [], set(), 0
    for row in rows:
        errors.extend(_row_shape_errors(row, rounds))
        if type(row) is not dict:
            continue
        if row.get("source_sha256") != campaign.get("collector_source_fingerprint"):
            errors.append("scenario_source_fingerprint_mismatch:" + str(row.get("scenario_id")))
        if row.get("gate_sha256") != campaign.get("release_gate_sha256"):
            errors.append("scenario_gate_fingerprint_mismatch:" + str(row.get("scenario_id")))
        if row.get("template", {}).get("repo_root") != str(_ROOT.resolve()):
            errors.append("scenario_template_repo_root_mismatch:" + str(row.get("scenario_id")))
        if type(runtime) is dict and row.get("template", {}).get("output_root") != runtime.get("output_root"):
            errors.append("scenario_template_output_root_mismatch:" + str(row.get("scenario_id")))
        errors.extend(_verify_condition_file(row))
        if type(row.get("repeat_attempts")) is dict:
            attempts.extend(row["repeat_attempts"].values())
        history = row.get("firstpass_history")
        if history is not None:
            history_count += 1
            if type(history) is dict:
                history_attempts.add(history.get("attempt_id"))
    errors.extend(_history_schedule_errors(rows, rounds))
    if len(attempts) != len(set(attempts)):
        errors.append("repeat_attempt_ids_must_be_unique")
    if set(attempts) & history_attempts:
        errors.append("repeat_attempt_conflicts_with_history")
    budget = campaign.get("budget", {})
    if type(budget) is dict and budget.get("firstpass_count") != history_count:
        errors.append("budget_history_count_mismatch")
    errors.extend(_verify_input_sources(campaign))
    return errors


def _marker_entry(row: dict[str, Any]) -> dict[str, Any]:
    return {key: row.get(key) for key in ("design_version", "scenario_id", "condition_id",
            "condition_signature_sha256", "source_sha256", "gate_sha256", "repeat_attempts")} | {
                "firstpass_history": _history_summary(row.get("firstpass_history")),
                "template_sha256": c.canonical_sha256(row.get("template"))}


def _verify_marker(campaign_path: Path, campaign: dict[str, Any], campaign_sha256: str) -> tuple[dict[str, Any] | None, list[str]]:
    marker_path = campaign_path.parent / MARKER_NAME
    try:
        raw = marker_path.read_bytes()
    except OSError:
        return None, ["repeat_admission_marker_missing"]
    try:
        marker = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, ["repeat_admission_marker_invalid_json"]
    if type(marker) is not dict:
        return None, ["repeat_admission_marker_not_object"]
    errors = []
    if marker.get("schema_version") != MARKER_SCHEMA:
        errors.append("repeat_admission_marker_schema_unsupported")
    if marker.get("campaign_path") != CAMPAIGN_NAME:
        errors.append("repeat_admission_marker_campaign_path_invalid")
    if marker.get("campaign_sha256") != campaign_sha256:
        errors.append("repeat_admission_marker_campaign_sha_mismatch")
    for key in ("campaign_id", "source_fingerprint", "release_gate_sha256", "source_files"):
        if marker.get(key) != campaign.get(key):
            errors.append("repeat_admission_marker_binding_mismatch:" + key)
    entries = marker.get("entries")
    rows = campaign.get("scenarios")
    if type(entries) is not list or type(rows) is not list or len(entries) != len(rows):
        errors.append("repeat_admission_marker_entry_count_mismatch")
    elif entries != [_marker_entry(row) for row in rows if type(row) is dict]:
        errors.append("repeat_admission_marker_entries_do_not_match_campaign")
    return marker, errors


def validate_campaign(path: str | Path, expected_sha256: str | None = None) -> dict[str, Any]:
    """Validate the sealed campaign, marker, inputs, and pinned source files."""
    campaign_path = Path(path)
    try:
        campaign_path = campaign_path.resolve(strict=True)
        campaign_path.relative_to(_ROOT.resolve(strict=True))
    except (OSError, ValueError):
        return {"ok": False, "status": "BLOCKED", "errors": ["campaign_path_missing_or_outside_repository"]}
    try:
        raw = campaign_path.read_bytes()
        campaign = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {"ok": False, "status": "BLOCKED", "errors": ["campaign_json_unreadable"]}
    if type(campaign) is not dict:
        return {"ok": False, "status": "BLOCKED", "errors": ["campaign_not_object"]}
    digest = _sha_bytes(raw)
    errors = []
    if expected_sha256 is not None:
        if not isinstance(expected_sha256, str) or not _SHA256.fullmatch(expected_sha256):
            errors.append("expected_campaign_sha256_invalid")
        elif digest != expected_sha256:
            errors.append("campaign_sha256_mismatch")
    errors.extend(_campaign_content_errors(campaign))
    marker, marker_errors = _verify_marker(campaign_path, campaign, digest)
    errors.extend(marker_errors)
    if type(campaign.get("source_files")) is dict:
        errors.extend(_verify_source_files(_ROOT, campaign["source_files"]))
    marker_sha = None
    if marker is not None and not marker_errors:
        try:
            marker_sha = _sha_bytes((campaign_path.parent / MARKER_NAME).read_bytes())
        except OSError:
            pass
    budget = campaign.get("budget", {})
    return {"ok": not errors, "status": "READY" if not errors else "BLOCKED",
            "campaign_id": campaign.get("campaign_id"), "campaign_sha256": digest,
            "source_fingerprint": campaign.get("source_fingerprint"),
            "release_gate_sha256": campaign.get("release_gate_sha256"),
            "scenario_count": campaign.get("scenario_count"),
            "rounds": budget.get("rounds") if type(budget) is dict else None,
            "new_slots": budget.get("new_slots") if type(budget) is dict else None,
            "firstpass_count": budget.get("firstpass_count") if type(budget) is dict else None,
            "marker_sha256": marker_sha, "errors": sorted(set(errors))}


def _as_contract_dict(contract: Any) -> dict[str, Any]:
    if isinstance(contract, c.RunContract):
        return contract.to_dict()
    if type(contract) is dict:
        return contract
    raise RepeatAdmissionError("contract must be RunContract or object")


def _validate_attempt_identity(row: dict[str, Any], repeat_number: int, contract_data: dict[str, Any], attempt: Any) -> dict[str, Any]:
    if type(repeat_number) is not int or repeat_number < 1:
        raise RepeatAdmissionError("repeat_number must be a positive integer")
    planned = row.get("repeat_attempts", {}).get(str(repeat_number))
    if not isinstance(planned, str):
        raise RepeatAdmissionError("planned attempt missing for selected round")
    context = contract_data.get("context")
    if type(context) is not dict or type(context.get("replicate")) is not int or context["replicate"] != repeat_number:
        raise RepeatAdmissionError("contract replicate does not match selected slot")
    if isinstance(attempt, str):
        attempt_id = attempt
        retry_match = re.fullmatch(re.escape(planned) + r"-retry-([1-9][0-9]*)", attempt_id)
        if attempt_id != planned and retry_match is None:
            raise RepeatAdmissionError("attempt is not the unique planned slot or its same-slot retry")
        retry = retry_match is not None
        retry_no = int(retry_match.group(1)) if retry_match else None
    elif type(attempt) is dict:
        attempt_id, retry_no = attempt.get("attempt_id"), attempt.get("retry_number")
        if (attempt.get("retry_of") != planned or type(retry_no) is not int or retry_no < 1
                or attempt_id != f"{planned}-retry-{retry_no}"):
            raise RepeatAdmissionError("retry must name its same slot and unique retry attempt ID")
        retry = True
    else:
        raise RepeatAdmissionError("attempt must be an ID or typed retry record")
    if not isinstance(attempt_id, str) or not _ATTEMPT_ID.fullmatch(attempt_id):
        raise RepeatAdmissionError("attempt ID syntax invalid")
    if context.get("attempt_id") != attempt_id:
        raise RepeatAdmissionError("contract attempt ID differs from selected slot attempt")
    return {"attempt_id": attempt_id, "planned_attempt_id": planned, "repeat_number": repeat_number,
            "retry": retry, "retry_number": retry_no, "retry_of": planned if retry else None}


def _condition_signature(contract_data: dict[str, Any], registry: c.DesignRegistry) -> dict[str, Any]:
    typed = c.RunContract.from_dict(contract_data, registry)
    return dp.semantic_signature(typed.to_dict())


def validate_slot(campaign: dict[str, Any], scenario_id: str, repeat_number: int,
                  contract: Any, attempt: Any) -> dict[str, Any]:
    """Admit an execution identity; this does not qualify its resulting sample."""
    errors = _campaign_content_errors(campaign)
    if errors:
        return {"ok": False, "status": "BLOCKED", "errors": sorted(set(errors))}
    if type(repeat_number) is not int or repeat_number not in _campaign_rounds(campaign):
        return {"ok": False, "status": "BLOCKED", "errors": ["round_not_selected_in_campaign"]}
    rows = [row for row in campaign["scenarios"] if row["scenario_id"] == scenario_id]
    if len(rows) != 1:
        return {"ok": False, "status": "BLOCKED", "errors": ["scenario_not_uniquely_in_campaign"]}
    row = rows[0]
    try:
        data = _as_contract_dict(contract)
        if data.get("scenario") != {"design_version": row["design_version"], "scenario_id": scenario_id}:
            raise RepeatAdmissionError("contract scenario/design identity differs from campaign")
        if data.get("purpose") != "formal" or data.get("protocol_status") != "formal_frozen":
            raise RepeatAdmissionError("contract must retain formal frozen protocol")
        if data.get("qualification") != "not_assessed":
            raise RepeatAdmissionError("new contract cannot carry a qualification claim")
        registry = c.DesignRegistry.from_json(_CATALOG_PATH.read_bytes())
        normalized = _condition_signature(data, registry)
        if normalized != row["condition"] or c.canonical_sha256(normalized) != row["condition_signature_sha256"]:
            raise RepeatAdmissionError("contract scientific condition differs from campaign")
        if data.get("metric_interval_s") != 2.0 or type(data.get("metric_interval_s")) is not float:
            raise RepeatAdmissionError("metric interval must remain 2 seconds")
        template = row["template"]
        if data.get("rules") != template["contract_rules"]:
            raise RepeatAdmissionError("contract rule references differ from row template")
        context = data.get("context", {})
        if context.get("repo_root") != template["repo_root"] or context.get("output_root") != template["output_root"]:
            raise RepeatAdmissionError("contract runtime roots differ from campaign template")
        fingerprints = context.get("fingerprints", {})
        for key in _FINGERPRINT_KEYS:
            if fingerprints.get(key) != template["context_fingerprints"].get(key):
                raise RepeatAdmissionError("contract template fingerprint differs: " + key)
        if template["quality_rules_sha256"] != template["context_fingerprints"]["quality_rules"]:
            raise RepeatAdmissionError("quality rules SHA differs from template")
        if fingerprints.get("collector") != row["source_sha256"]:
            raise RepeatAdmissionError("contract collector fingerprint differs from campaign")
        attempt_info = _validate_attempt_identity(row, repeat_number, data, attempt)
        history = row.get("firstpass_history")
        return {"ok": True, "status": "ADMITTED", "campaign_id": campaign["campaign_id"],
                "scenario_id": scenario_id, "condition_id": row["condition_id"],
                "condition_signature_sha256": row["condition_signature_sha256"],
                "historical_count_reference": _history_summary(history), "attempt": attempt_info,
                "qualification": "NOT_ASSESSED"}
    except (RepeatAdmissionError, c.ContractError, KeyError, TypeError, ValueError) as exc:
        return {"ok": False, "status": "BLOCKED", "scenario_id": scenario_id, "errors": [str(exc)]}


def validate_contract(contract: Any, directory: str | Path | None = None, *,
                      services: Any = None, expected_campaign_sha256: str | None = None) -> dict[str, Any]:
    """Formal-runner hook. Marker SHA is pinned by contract.release_gate_ref."""
    try:
        data = _as_contract_dict(contract)
        reference = data.get("release_gate_ref")
        if type(reference) is not dict or set(reference) != {"uri", "sha256"}:
            raise RepeatAdmissionError("repeat release_gate_ref required")
        uri = reference["uri"]
        gate_dir = Path(directory) if directory is not None else Path(uri)
        gate_dir = gate_dir.resolve(strict=True)
        if not gate_dir.is_absolute() or ".." in gate_dir.parts:
            raise RepeatAdmissionError("repeat gate directory must be absolute and traversal-free")
        marker_path = gate_dir / MARKER_NAME
        if marker_path.resolve(strict=True) != marker_path or _sha_bytes(marker_path.read_bytes()) != reference["sha256"]:
            raise RepeatAdmissionError("repeat admission marker bytes differ from release-gate SHA")
        marker = _read_json(marker_path, "repeat admission marker")
        if marker.get("schema_version") != MARKER_SCHEMA or marker.get("campaign_path") != CAMPAIGN_NAME:
            raise RepeatAdmissionError("repeat admission marker schema/path invalid")
        campaign_path = gate_dir / CAMPAIGN_NAME
        if campaign_path.resolve(strict=True) != campaign_path or _sha_bytes(campaign_path.read_bytes()) != marker.get("campaign_sha256"):
            raise RepeatAdmissionError("campaign bytes differ from marker SHA")
        validation = validate_campaign(campaign_path, expected_campaign_sha256 or marker.get("campaign_sha256"))
        if not validation.get("ok"):
            raise RepeatAdmissionError("campaign validation failed: " + ",".join(validation.get("errors", [])))
        campaign = campaign_report_campaign(campaign_path)
        result = validate_slot(campaign, data.get("scenario", {}).get("scenario_id"),
                               data.get("context", {}).get("replicate"), data,
                               data.get("context", {}).get("attempt_id"))
        if not result.get("ok"):
            raise RepeatAdmissionError("repeat slot validation failed: " + ",".join(result.get("errors", [])))
        return {"ok": True, "status": "ADMITTED", "campaign_id": campaign["campaign_id"],
                "campaign_sha256": validation["campaign_sha256"], "marker_sha256": validation["marker_sha256"],
                "slot": result, "qualification": "NOT_ASSESSED"}
    except (OSError, RepeatAdmissionError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {"ok": False, "status": "BLOCKED", "errors": [str(exc)]}


def campaign_report_campaign(path: Path) -> dict[str, Any]:
    """Read the campaign after its caller has validated the pinned bytes."""
    value = _read_json(path, "campaign")
    if type(value) is not dict:
        raise RepeatAdmissionError("campaign is not an object")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    validate = sub.add_parser("validate", help="validate a sealed v2 campaign")
    validate.add_argument("campaign")
    validate.add_argument("--expected-sha256")
    args = parser.parse_args(argv)
    result = validate_campaign(args.campaign, args.expected_sha256)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
