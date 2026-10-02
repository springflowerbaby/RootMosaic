"""Compile and offline-preview a versioned collection campaign.

This module reads condition templates and optional history evidence. It never
injects faults, loads database credentials, or contacts the cluster.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import sys
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import scenario_definitions as asm
from . import scenario_runner as cd
from . import contract as c
from . import review_samples as dp
from . import quality as q
from . import runner
from . import release_gate as rg
from . import workload as w
from . import environment

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONDITIONS = ROOT / 'configs/collection/campaign_conditions.json'
DEFAULT_PYTHON = Path(sys.executable)
PHASE_NAMES = ("pre_fault", "during_fault", "post_recovery")


def require(ok: bool, message: str) -> None:
    if not ok:
        raise ValueError("repeat preparation: " + message)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def file_ref(path: Path) -> dict[str, Any]:
    return {"path": str(path.resolve()), "sha256": digest(path), "bytes": path.stat().st_size}


def write_new(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")


def _rounds(values: Any) -> list[int]:
    require(type(values) is list and bool(values), "at least one round is required")
    require(all(type(value) is int and value > 0 for value in values), "rounds must be positive integers")
    require(values == sorted(set(values)), "rounds must be unique and ascending")
    return list(values)


def load_conditions(path: Path, runtime_output_root: Path) -> list[dict[str, Any]]:
    """Load science and contract templates, rebinding only local path fields."""
    source = read_json(path)
    require(type(source) is dict and source.get("schema_version") == "m1-repeat-conditions-v1",
            "conditions schema must be m1-repeat-conditions-v1")
    rows = source.get("conditions")
    require(type(rows) is list and bool(rows), "conditions must be a non-empty array")
    registry = asm.load_registry(repo_root=ROOT)
    result, seen_scenarios, seen_conditions = [], set(), set()
    for source_row in rows:
        require(type(source_row) is dict, "condition row must be an object")
        seed = copy.deepcopy(source_row.get("seed"))
        require(type(seed) is dict and type(seed.get("contract")) is dict, "condition seed contract missing")
        sid, cid = source_row.get("scenario_id"), source_row.get("condition_id")
        require(isinstance(sid, str) and sid and isinstance(cid, str) and cid, "condition identity missing")
        require(sid not in seen_scenarios and cid not in seen_conditions, "duplicate scenario or condition identity")
        seen_scenarios.add(sid)
        seen_conditions.add(cid)
        science = copy.deepcopy(source_row.get("science"))
        contract_data = seed["contract"]
        environment.bind_contract(contract_data)
        contract_data["catalog_sha256"] = registry.catalog_sha256
        contract_data["catalog_source"] = {"kind": registry.source_kind, "sha256": registry.source_sha256}
        science = dp.semantic_signature(contract_data)
        source_row = dict(source_row, condition_signature_sha256=c.canonical_sha256(science))
        require(seed.get("scenario_id") == sid and seed.get("condition_id") == cid, sid + ": seed identity mismatch")
        design_version = source_row.get("design_version")
        require(contract_data.get("scenario") == {"design_version": design_version, "scenario_id": sid}, sid + ": design identity mismatch")
        require(dp.semantic_signature(contract_data) == science, sid + ": seed scientific signature mismatch")
        science_sha = c.canonical_sha256(science)
        require(source_row.get("condition_signature_sha256", science_sha) == science_sha, sid + ": science SHA mismatch")
        template = copy.deepcopy(source_row.get("template"))
        fingerprints = contract_data["context"]["fingerprints"]
        for field, module in (("generator", w), ("annotation_rules", cd.a)):
            current_sha = digest(Path(module.__file__))
            fingerprints[field] = current_sha
            template["context_fingerprints"][field] = current_sha
        require(type(template) is dict, sid + ": contract template missing")
        require(contract_data.get("rules") == template.get("contract_rules"), sid + ": contract rules differ from template")
        fingerprints = contract_data.get("context", {}).get("fingerprints", {})
        for key in ("quality_rules", "generator", "annotation_rules"):
            require(fingerprints.get(key) == template.get("context_fingerprints", {}).get(key), sid + ": template fingerprint differs: " + key)
        rules = q.QualityRules.from_dict(seed["quality_rules_payload"])
        require(rules.sha256 == template.get("quality_rules_sha256") == fingerprints.get("quality_rules"), sid + ": quality rules content hash mismatch")
        context = contract_data["context"]
        # Only machine-local roots move. The condition signature excludes these fields.
        template["repo_root"] = str(ROOT.resolve())
        template["output_root"] = str(runtime_output_root.resolve())
        context["repo_root"] = template["repo_root"]
        context["output_root"] = template["output_root"]
        context["evidence_root"] = str(runtime_output_root.resolve() / contract_data["purpose"] / context["run_id"] / context["attempt_id"])
        c.RunContract.from_dict(contract_data, registry)
        seed.update(design_version=design_version, scenario_id=sid, condition_id=cid, template=template)
        result.append({"sequence": int(source_row.get("sequence", len(result) + 1)),
                       "design_version": design_version, "scenario_id": sid, "condition_id": cid,
                       "source_combo": source_row.get("source_combo", seed.get("source_combo")),
                       "seed": seed, "runtime": copy.deepcopy(source_row.get("runtime", {})),
                       "template": template, "science": science, "condition_signature_sha256": science_sha})
    return result


def import_history(path: Path | None, conditions: list[dict[str, Any]], rounds: list[int]) -> dict[str, dict[str, Any]]:
    """One-time migration check for real historical evidence and count identity."""
    if path is None:
        return {}
    from . import validate_campaign as admission
    source = read_json(path)
    require(type(source) is dict and source.get("schema_version") == "m1-repeat-history-v1",
            "history schema must be m1-repeat-history-v1")
    entries = source.get("entries")
    require(type(entries) is list, "history entries must be an array")
    by_sid = {row["scenario_id"]: row for row in conditions}
    imported: dict[str, dict[str, Any]] = {}
    seen_slots, seen_attempts = set(), set()
    for entry_value in entries:
        require(type(entry_value) is dict, "history row must be an object")
        entry = copy.deepcopy(entry_value)
        sid = entry.get("scenario_id")
        require(sid in by_sid, "history scenario is not in conditions: " + str(sid))
        row = by_sid[sid]
        require(sid not in imported, "more than one historical firstpass for scenario " + sid)
        slot = (sid, entry.get("replicate"))
        require(slot not in seen_slots, "duplicate historical scenario/replicate")
        require(type(slot[1]) is int and slot[1] > 0, "history replicate must be positive integer")
        require(slot[1] not in rounds, sid + ": imported history overlaps selected round")
        require(entry.get("attempt_id") not in seen_attempts, "duplicate historical attempt ID")
        verified = admission.verify_history_entry(entry, {"scenario_id": sid, "condition_id": row["condition_id"],
            "design_version": row["design_version"], "condition": row["science"], "template": row["template"]})
        # Keep a transparent record of expected, narrowly permitted source drift.
        entry["source_compatibility"] = verified.get("source_compatibility", {})
        entry["history_identity_verified"] = True
        imported[sid] = entry
        seen_slots.add(slot)
        seen_attempts.add(entry["attempt_id"])
    return imported


def bind_condition(seed: dict[str, Any], campaign: dict[str, Any], gate_directory: Path,
                   repeat_number: int, attempt_id: str) -> dict[str, Any]:
    """Bind one selected run identity while preserving the science signature."""
    require(type(repeat_number) is int and repeat_number > 0, "round must be a positive integer")
    result = copy.deepcopy(seed)
    data = result["contract"]
    row = next((item for item in campaign["scenarios"] if item["condition_id"] == result["condition_id"]), None)
    require(row is not None, "condition is not present in campaign")
    template = row["template"]
    context = data["context"]
    run_id = "r10c-" + c.canonical_sha256({"condition_id": result["condition_id"]})[:20]
    clock_id = "clock-" + run_id
    context.update(run_id=run_id, attempt_id=attempt_id, owner_id=run_id + "/" + attempt_id,
                   replicate=repeat_number, clock_id=clock_id,
                   contract_ref="repeat-plan:" + campaign["campaign_id"] + ":" + result["condition_id"],
                   repo_root=template["repo_root"], output_root=template["output_root"],
                   evidence_root=str(Path(template["output_root"]) / "formal" / run_id / attempt_id))
    context["fingerprints"]["collector"] = campaign["collector_source_fingerprint"]
    for key in ("quality_rules", "generator", "annotation_rules"):
        context["fingerprints"][key] = template["context_fingerprints"][key]
    data["release_gate_ref"] = {"uri": str(gate_directory.resolve()),
                                "sha256": digest(gate_directory / "repeat-admission.json")}
    for phase in data["phases"].values():
        phase["clock_id"] = clock_id
    for fault in data["faults"]:
        fault["planned_window"]["clock_id"] = clock_id
    result["repeat_campaign_id"] = campaign["campaign_id"]
    result["repeat_number"] = repeat_number
    result["repeat_attempt_id"] = attempt_id
    require(dp.semantic_signature(data) == row["condition"], "binding changed scientific content")
    c.RunContract.from_dict(data, asm.load_registry(repo_root=ROOT))
    return result


@contextmanager
def offline_order_receipt(source: dict[str, Any]):
    """Replay only saved order substitutions during offline preview."""
    saved = {row["stream_id"]: row for row in source["contract"]["request_profile"]["streams"]}
    original_build = cd.build

    def build_with_saved_order(*args, **kwargs):
        require(not kwargs.get("enabled", False), "saved order replay is offline-only")
        assembled, face, metadata = original_build(*args, **kwargs)
        data = assembled.contract.to_dict()
        replacements = {}
        for stream in data["request_profile"]["streams"]:
            if "/api/orders/" not in stream["endpoint"]:
                continue
            wanted = saved.get(stream["stream_id"])
            require(wanted is not None, "saved order stream missing")
            prefix = stream["endpoint"].split("/api/orders/", 1)[0] + "/api/orders/"
            endpoint = wanted["endpoint"]
            require(endpoint.startswith(prefix) and re.fullmatch(r"ORD[A-Za-z0-9]+", endpoint[len(prefix):]) is not None,
                    "saved order endpoint is not an exact M04 order ID")
            stream["endpoint"] = endpoint
            replacements[stream["stream_id"]] = endpoint
        if replacements:
            typed = c.RunContract.from_dict(data, asm.load_registry(repo_root=ROOT))
            assembled = replace(assembled, contract=typed, contract_dict=typed.to_dict())
            policy = face.settings.workload_policy
            allowlist = tuple(w.EndpointRule(rule.stream_id, rule.method, rule.origin,
                                            replacements.get(rule.stream_id, rule.endpoint)) for rule in policy.allowlist)
            face = replace(face, settings=replace(face.settings, workload_policy=replace(policy, allowlist=allowlist)))
        return assembled, face, metadata

    cd.build = build_with_saved_order
    try:
        yield
    finally:
        cd.build = original_build


def _read_condition(path: Path, condition_id: str) -> dict[str, Any]:
    value = read_json(path)
    matches = [row for row in value if type(row) is dict and row.get("condition_id") == condition_id] if type(value) is list else [value]
    require(len(matches) == 1, "condition file does not contain exactly one matching seed")
    return matches[0]


def preview_campaign(campaign_path: Path) -> dict[str, Any]:
    from . import validate_campaign as admission
    campaign = read_json(campaign_path)
    validation = admission.validate_campaign(campaign_path, expected_sha256=digest(campaign_path))
    require(validation.get("ok") is True, "campaign invalid: " + str(validation.get("errors")))
    rounds = campaign["budget"]["rounds"]
    registry = asm.load_registry(repo_root=ROOT)
    results = []
    for row in campaign["scenarios"]:
        seed = _read_condition(ROOT / row["condition_path"], row["condition_id"])
        for number in rounds:
            bound = bind_condition(seed, campaign, campaign_path.parent, number, row["repeat_attempts"][str(number)])
            decision = runner._validate_release_entry(c.RunContract.from_dict(bound["contract"], registry))
            require(decision.get("repeat_admission", {}).get("ok") is True,
                    row["scenario_id"] + ": selected round " + str(number) + " refused")
        number = rounds[0]
        attempt = row["repeat_attempts"][str(number)]
        bound = bind_condition(seed, campaign, campaign_path.parent, number, attempt)
        spec = cd.entry_view(cd.entry_spec(seed.get("source_combo") or row["scenario_id"]))
        with offline_order_receipt(bound):
            built, face, metadata = cd.build_condition(
                bound, attempt, cd.snapshot(spec, enabled=False), enabled=False,
                action_budget=row["action_budget"], lateness_budget=row["lateness_budget"],
                first_injection_lateness_budget=row["first_injection_lateness_budget"])
        require(dp.semantic_signature(built.contract.to_dict()) == row["condition"], row["scenario_id"] + ": production build changed science")
        require(built.contract.context.to_dict()["replicate"] == number, row["scenario_id"] + ": production replicate changed")
        value = runner.run_attempt(built.contract, face.settings, face.services, execute=False)
        require(value.get("status") == "validated_not_executed", row["scenario_id"] + ": production preview failed")
        results.append({"scenario_id": row["scenario_id"], "condition_id": row["condition_id"],
                        "production_preview": value["status"], "admitted_replicates": rounds,
                        "source_hashes": metadata["source_hashes"], "actual_contract_sha256": built.contract.sha256,
                        "live_runtime_identity": "NOT_ASSESSED",
                        "saved_order_receipt_replayed_offline": any("/api/orders/" in stream["endpoint"]
                            for stream in seed["contract"]["request_profile"]["streams"])})
        if len(results) % 12 == 0 or len(results) == len(campaign["scenarios"]):
            print(f"Offline production preview: {len(results)}/{len(campaign['scenarios'])} scenarios; "
                  f"{len(results) * len(rounds)}/{campaign['budget']['new_slots']} slots", flush=True)
    return {"schema_version": "m1-repeat-preview-v2", "campaign_sha256": digest(campaign_path),
            "status": "OFFLINE_VALIDATED_NOT_EXECUTED", "scenario_count": len(results),
            "repeat_slots_validated": sum(len(row["admitted_replicates"]) for row in results), "rows": results,
            "no_live_execution": True}


def prepare(output_dir: Path, campaign_id: str, conditions_file: Path = DEFAULT_CONDITIONS,
            rounds: list[int] | tuple[int, ...] = (1, 2, 3, 4, 5), history_file: Path | None = None,
            python_executable: Path | None = None, output_root: Path | None = None,
            batch_root: Path | None = None) -> Path:
    from . import validate_campaign as admission
    environment.apply(cd)
    require(re.fullmatch(r"[a-z][a-z0-9-]{0,29}", campaign_id) is not None,
            "campaign ID must be 1..30 lowercase safe characters")
    rounds = _rounds(list(rounds))
    output_dir = output_dir.resolve()
    try:
        output_dir.relative_to(ROOT.resolve())
    except ValueError:
        raise ValueError("repeat preparation: output must stay inside this workspace") from None
    require(not output_dir.exists(), "output exists; preserve it and select a new version")
    conditions_file = conditions_file.resolve(strict=True)
    condition_config = read_json(conditions_file)
    configured_runtime = condition_config.get("runtime", {}) if type(condition_config) is dict else {}
    runtime_output = (output_root or Path(configured_runtime.get("output_root", str(environment.resolve_path(environment.current()["paths"]["output_root"]))))).resolve()
    runtime_batch = (batch_root or Path(configured_runtime.get("batch_root", str(environment.resolve_path(environment.current()["paths"]["batch_root"]))))).resolve()
    runtime_python = (python_executable or Path(configured_runtime.get("python_executable", str(DEFAULT_PYTHON)))).resolve()
    conditions = load_conditions(conditions_file, runtime_output)
    imported_history = import_history(history_file.resolve(strict=True) if history_file is not None else None, conditions, rounds)
    source_paths = set(admission._REQUIRED_SOURCE_PATHS)
    source_paths.update(Path(spec.path).relative_to(ROOT).as_posix() for spec in runner._formal_asset_specs(runner._formal_asset_root()))
    source_paths.add("configs/collection/collection-policy.json")
    wrapper = ROOT / 'scripts/entrypoints/collect_dataset.ps1'
    require(wrapper.is_file(), "Windows batch entry must exist before freezing preparation")
    source_paths.add(wrapper.relative_to(ROOT).as_posix())
    source_files = {rel: digest(ROOT / rel) for rel in sorted(source_paths)}
    collector = c.canonical_sha256(cd.source_hashes())
    gate_source_sha = digest(Path(rg.__file__))
    output_dir.mkdir(parents=True)
    scenarios = []
    for item in conditions:
        seed = copy.deepcopy(item["seed"])
        seed["status"] = "REPEAT_TEMPLATE_REQUIRES_ATTEMPT_BINDING"
        seed["blockers"] = ["fresh_runtime_identity_required"]
        seed["template"] = copy.deepcopy(item["template"])
        seed["contract"]["context"]["fingerprints"]["collector"] = collector
        seed_path = output_dir / "conditions" / (item["scenario_id"] + ".json")
        write_new(seed_path, [seed])
        row = {"sequence": item["sequence"], "design_version": item["design_version"],
               "scenario_id": item["scenario_id"], "condition_id": item["condition_id"],
               "source_combo": item["source_combo"], "condition_path": seed_path.relative_to(ROOT).as_posix(),
               "condition_sha256": digest(seed_path), "condition": item["science"],
               "condition_signature_sha256": item["condition_signature_sha256"],
               "source_sha256": collector, "gate_sha256": gate_source_sha,
               "template": copy.deepcopy(item["template"]),
               "firstpass_lateness_budget": item["runtime"].get("first_injection_lateness_budget"),
               "first_injection_lateness_budget": item["runtime"].get("first_injection_lateness_budget"),
               "action_budget": item["runtime"].get("action_budget", 17.0),
               "lateness_budget": item["runtime"].get("lateness_budget", 2.0),
               "budget_mode": item["runtime"].get("budget_mode", "default"),
               "execution_family": item["runtime"].get("execution_family", ""),
               "repeat_attempts": {str(number): f"{campaign_id}-{item['scenario_id'].lower()}-r{number}" for number in rounds}}
        if item["scenario_id"] in imported_history:
            row["firstpass_history"] = imported_history[item["scenario_id"]]
        scenarios.append(row)
    history_count = len(imported_history)
    inputs = {"conditions": {"path": str(conditions_file), "sha256": digest(conditions_file)}}
    if history_file is not None:
        resolved_history = history_file.resolve(strict=True)
        inputs["history"] = {"path": str(resolved_history), "sha256": digest(resolved_history)}
    budget = {"rounds": rounds, "cases_per_round": len(scenarios),
              "new_slots": len(scenarios) * len(rounds), "firstpass_count": history_count,
              "target_total": history_count + len(scenarios) * len(rounds)}
    campaign = {"schema_version": admission.CAMPAIGN_SCHEMA, "campaign_id": campaign_id,
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_fingerprint": c.canonical_sha256(source_files),
                "collector_source_fingerprint": collector, "release_gate_sha256": gate_source_sha,
                "source_files": source_files, "scenario_count": len(scenarios), "budget": budget,
                "runtime": {"python_executable": str(runtime_python), "output_root": str(runtime_output), "batch_root": str(runtime_batch), "environment": environment.binding_ref()},
                "input_sources": inputs, "scenarios": scenarios}
    campaign_path = output_dir / admission.CAMPAIGN_NAME
    write_new(campaign_path, campaign)
    marker = {"schema_version": admission.MARKER_SCHEMA, "campaign_path": admission.CAMPAIGN_NAME,
              "campaign_sha256": digest(campaign_path), "campaign_id": campaign_id,
              "source_fingerprint": campaign["source_fingerprint"], "release_gate_sha256": gate_source_sha,
              "source_files": source_files, "entries": [admission._marker_entry(row) for row in scenarios]}
    write_new(output_dir / admission.MARKER_NAME, marker)
    validation = admission.validate_campaign(campaign_path, expected_sha256=digest(campaign_path))
    write_new(output_dir / "ADMISSION-VALIDATION.json", validation)
    require(validation.get("ok") is True, "campaign validation failed: " + str(validation.get("errors")))
    preview = preview_campaign(campaign_path)
    write_new(output_dir / "PRODUCTION-PREVIEWS.json", preview)
    write_new(output_dir / "FREEZE.json", {"schema_version": "m1-repeat-preparation-freeze-v2",
        "campaign_id": campaign_id, "campaign_sha256": digest(campaign_path),
        "marker_sha256": digest(output_dir / admission.MARKER_NAME), "source_fingerprint": campaign["source_fingerprint"],
        "scenario_count": len(scenarios), "new_slots": budget["new_slots"],
        "firstpass_count": history_count, "rounds": rounds,
        "status": "OFFLINE_VALIDATED_NOT_EXECUTED", "preview_sha256": digest(output_dir / "PRODUCTION-PREVIEWS.json")})
    return campaign_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--conditions-file", type=Path, default=DEFAULT_CONDITIONS)
    parser.add_argument("--rounds", nargs="+", type=int, default=[1, 2, 3, 4, 5])
    parser.add_argument("--history-file", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--python-executable", type=Path)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--batch-root", type=Path)
    parser.add_argument("--environment", type=Path, required=True)
    args = parser.parse_args(argv)
    environment.load(args.environment)
    try:
        path = prepare(args.output_dir, args.campaign_id, args.conditions_file, args.rounds,
                       args.history_file, args.python_executable, args.output_root, args.batch_root)
    except (ValueError, OSError, KeyError, TypeError, StopIteration, RuntimeError) as exc:
        print("REPEAT PREPARATION STOP: " + str(exc))
        return 2
    campaign = read_json(path)
    print(json.dumps({"status": "OFFLINE_VALIDATED_NOT_EXECUTED", "campaign": str(path),
        "sha256": digest(path), "scenarios": campaign["scenario_count"],
        "rounds": campaign["budget"]["rounds"], "new_repeat_slots": campaign["budget"]["new_slots"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
