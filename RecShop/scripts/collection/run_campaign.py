"""Serial collection repeat collector. Planning/status are read-only; live collection needs --execute."""
from __future__ import annotations

import argparse
import csv
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid
from typing import Any, Callable, Iterable


ROOT = Path(__file__).resolve().parents[2]
OLD_ROOT = ROOT
BATCH_ROOT = ROOT / "runs/collection/batches"
EXECUTOR_LOCK = ROOT / ".recshop-collection/.executor.lock"
DEFAULT_PLAN = ROOT / "runs/collection/prepared/campaign.json"
DB_ENV_WRAPPER = ROOT / 'scripts/collection/run_scenario.py'
PYTHON = Path(sys.executable)
SCHEMA = "m1-repeat-batch-v1"
PHASES = {"pre_fault": (0, 300), "during_fault": (300, 600), "post_recovery": (600, 900)}
SAFE_RECOVERY = {"RECOVERY_QUALIFIED", "ALREADY_CLEAN"}
BUDGETS = {"default": (17.0, 2.0), "r48_fixed": (4.0, 0.8), "t07_r51": (5.0, 0.8)}
ATTEMPT_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
CAMPAIGN_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")


class BatchError(RuntimeError):
    """Fail-closed planning, admission, state, or evidence error."""


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _pairs_no_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise BatchError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_bytes().decode("utf-8"), object_pairs_hook=_pairs_no_duplicates,
                          parse_constant=lambda value: (_ for _ in ()).throw(BatchError("non-finite JSON value")))
    except BatchError:
        raise
    except Exception as exc:
        raise BatchError(f"cannot read JSON {path}: {type(exc).__name__}") from None


def resolve_repo_path(value: str | Path, *, allow_old_root: bool = True) -> Path:
    raw = Path(value)
    path = raw if raw.is_absolute() else ROOT / raw
    path = path.resolve()
    roots = (ROOT.resolve(), OLD_ROOT.resolve()) if allow_old_root else (ROOT.resolve(),)
    if not any(_is_relative_to(path, parent) for parent in roots):
        raise BatchError(f"path escapes approved source roots: {value}")
    if any(part.lower() == ".env" or part.lower().startswith(".env.") for part in path.parts):
        raise BatchError("campaign source manifest must not read or hash credential files")
    return path


def _rounds(campaign: dict[str, Any]) -> tuple[int, ...]:
    budget = campaign.get("budget")
    if type(budget) is not dict:
        raise BatchError("campaign budget is required")
    rounds = budget.get("rounds")
    if (type(rounds) is not list or not rounds
            or any(type(value) is not int or value < 1 for value in rounds)
            or rounds != sorted(set(rounds))):
        raise BatchError("campaign budget.rounds must be a sorted, unique list of positive integers")
    if rounds != list(range(rounds[0], rounds[-1] + 1)):
        raise BatchError("campaign budget.rounds must be a contiguous sequence")
    return tuple(rounds)


def _require_round(campaign: dict[str, Any], round_number: int) -> None:
    if type(round_number) is not int or round_number not in _rounds(campaign):
        raise BatchError("requested round is not listed in campaign budget.rounds")


def _scenario_count(campaign: dict[str, Any]) -> int:
    rows = campaign.get("scenarios")
    if type(rows) is not list or not rows:
        raise BatchError("campaign must contain at least one scenario")
    return len(rows)


def _runtime_path(campaign: dict[str, Any], key: str) -> Path:
    runtime = campaign.get("runtime")
    value = runtime.get(key) if type(runtime) is dict else None
    if type(value) is not str or not value.strip():
        raise BatchError(f"campaign runtime.{key} is required")
    raw = Path(value)
    return (raw if raw.is_absolute() else ROOT / raw).resolve()


def _batch_root(campaign: dict[str, Any]) -> Path:
    runtime = campaign.get("runtime")
    if type(runtime) is not dict or not runtime.get("batch_root"):
        return BATCH_ROOT.resolve()
    return _runtime_path(campaign, "batch_root")


def _output_root(campaign: dict[str, Any]) -> Path:
    return _runtime_path(campaign, "output_root")


def _python_path(campaign: dict[str, Any]) -> Path:
    runtime = campaign.get("runtime")
    if type(runtime) is not dict or not runtime.get("python_executable"):
        return PYTHON.resolve()
    return _runtime_path(campaign, "python_executable")


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _slot_rows(campaign: dict[str, Any]) -> list[dict[str, Any]]:
    rows = campaign.get("scenarios")
    if type(rows) is not list or not rows:
        raise BatchError("campaign must contain at least one scenario")
    rounds = _rounds(campaign)
    seen_scenarios, seen_conditions, seen_attempts = set(), set(), set()
    for row in rows:
        if type(row) is not dict:
            raise BatchError("campaign scenario row must be an object")
        for field in ("scenario_id", "design_version", "condition_id", "condition_path", "condition_sha256", "repeat_attempts"):
            if not row.get(field):
                raise BatchError(f"campaign scenario missing {field}")
        template = row.get("template")
        if type(template) is not dict or any(key not in template for key in
                ("contract_rules", "context_fingerprints", "repo_root", "output_root", "quality_rules_sha256")):
            raise BatchError(f"{row['scenario_id']}: campaign scenario template is incomplete")
        template_repo = Path(template["repo_root"])
        template_repo = (template_repo if template_repo.is_absolute() else ROOT / template_repo).resolve()
        template_output = Path(template["output_root"])
        template_output = (template_output if template_output.is_absolute() else ROOT / template_output).resolve()
        if template_repo != ROOT.resolve() or template_output != _output_root(campaign):
            raise BatchError(f"{row['scenario_id']}: template repo/output roots differ from campaign runtime")
        scenario_key = (row["design_version"], row["scenario_id"])
        if scenario_key in seen_scenarios or row["condition_id"] in seen_conditions:
            raise BatchError("duplicate scenario or condition in campaign")
        seen_scenarios.add(scenario_key)
        seen_conditions.add(row["condition_id"])
        attempts = row["repeat_attempts"]
        if type(attempts) is not dict or set(attempts) != {str(round_number) for round_number in rounds}:
            raise BatchError(f"{row['scenario_id']}: repeat_attempts must match campaign budget.rounds")
        for round_number in rounds:
            attempt = attempts[str(round_number)]
            if type(attempt) is not str or not ATTEMPT_ID_RE.fullmatch(attempt) or attempt in seen_attempts:
                raise BatchError("planned attempt IDs must be safe and globally unique")
            seen_attempts.add(attempt)
    return rows


def load_campaign(path: Path, *, verify_sources: bool = True) -> tuple[dict[str, Any], str, Path]:
    path = path.resolve()
    if not path.is_file():
        raise BatchError(f"campaign file is missing: {path}")
    raw = path.read_bytes()
    campaign = read_json(path)
    from . import environment, scenario_runner
    environment.verify_ref(campaign["runtime"]["environment"])
    environment.apply(scenario_runner)
    if type(campaign) is not dict or campaign.get("schema_version") != "m1-repeat-campaign-v2":
        raise BatchError("unsupported campaign schema")
    if not CAMPAIGN_ID_RE.fullmatch(str(campaign.get("campaign_id", ""))):
        raise BatchError("campaign_id is missing or unsafe")
    if type(campaign.get("runtime")) is not dict:
        raise BatchError("campaign runtime configuration is required")
    for key in ("python_executable", "output_root", "batch_root"):
        _runtime_path(campaign, key)
    budget = campaign.get("budget") or {}
    if type(budget) is not dict:
        raise BatchError("campaign budget is required")
    rows = _slot_rows(campaign)
    scenario_count = len(rows)
    firstpass_count = budget.get("firstpass_count", 0)
    expected_new_slots = scenario_count * len(_rounds(campaign))
    if (type(campaign.get("scenario_count")) is not int
            or campaign.get("scenario_count") != scenario_count
            or type(budget.get("cases_per_round")) is not int
            or budget.get("cases_per_round") != scenario_count
            or type(budget.get("new_slots")) is not int
            or budget.get("new_slots") != expected_new_slots
            or type(firstpass_count) is not int or firstpass_count < 0
            or type(budget.get("target_total")) is not int
            or budget.get("target_total") != firstpass_count + expected_new_slots):
        raise BatchError("campaign scenario count and budget totals disagree")
    if type(campaign.get("source_files")) is not dict or not campaign["source_files"]:
        raise BatchError("campaign source_files fingerprint map is required")
    if verify_sources:
        for source_name, expected in campaign["source_files"].items():
            if type(expected) is not str or not re.fullmatch(r"[a-f0-9]{64}", expected):
                raise BatchError(f"invalid source SHA256: {source_name}")
            source_path = resolve_repo_path(source_name)
            if not source_path.is_file() or sha256_file(source_path) != expected:
                raise BatchError(f"campaign source fingerprint changed: {source_name}")
        for row in rows:
            condition_path = resolve_repo_path(row["condition_path"], allow_old_root=False)
            if not condition_path.is_file() or sha256_file(condition_path) != row["condition_sha256"]:
                raise BatchError(f"{row['scenario_id']}: prepared condition file/hash changed")
    return campaign, _sha(raw), path


def campaign_dir(campaign_id: str, batch_root: Path | None = None) -> Path:
    if not CAMPAIGN_ID_RE.fullmatch(campaign_id):
        raise BatchError("unsafe campaign_id")
    return (batch_root or BATCH_ROOT) / campaign_id


def _write_once(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        raise BatchError(f"refusing to overwrite existing artifact: {path}") from None


def _write_once_or_match(path: Path, data: bytes) -> None:
    if path.exists():
        if path.read_bytes() != data:
            raise BatchError(f"existing prepared artifact differs; preserve it: {path}")
        return
    _write_once(path, data)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = _canonical(payload) + b"\n"
    descriptor = os.open(path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    try:
        written = os.write(descriptor, line)
        if written != len(line):
            raise BatchError("short append to event log")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def append_event(directory: Path, campaign_id: str, event: str, **fields) -> dict[str, Any]:
    payload = {"schema_version": SCHEMA, "campaign_id": campaign_id,
               "at_utc": datetime.now(timezone.utc).isoformat(), "event": event, **fields}
    _append_jsonl(directory / "events.jsonl", payload)
    return payload


def load_events(directory: Path, campaign_id: str | None = None) -> list[dict[str, Any]]:
    path = directory / "events.jsonl"
    if not path.exists():
        return []
    raw = path.read_bytes()
    if raw and not raw.endswith(b"\n"):
        raise BatchError("event log has an incomplete final record; preserve and review before resuming")
    events = []
    for index, line in enumerate(raw.splitlines(), 1):
        try:
            row = json.loads(line.decode("utf-8"), object_pairs_hook=_pairs_no_duplicates)
        except Exception:
            raise BatchError(f"event log record {index} is invalid; preserve and review") from None
        if type(row) is not dict or row.get("schema_version") != SCHEMA:
            raise BatchError(f"event log record {index} has an unexpected schema")
        if campaign_id is not None and row.get("campaign_id") != campaign_id:
            raise BatchError("event log campaign identity mismatch")
        events.append(row)
    return events


@contextmanager
def executor_lock(path: Path):
    """Nonblocking process lock. Windows releases the byte lock when a process exits."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    acquired = False
    try:
        if path.stat().st_size == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                raise BatchError("another repeat executor holds the OS lock") from None
            acquired = True
        else:
            import fcntl
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise BatchError("another repeat executor holds the OS lock") from None
            acquired = True
        yield
    finally:
        if acquired:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


def index_state(events: list[dict[str, Any]]) -> dict[str, Any]:
    starts, outcomes, decisions, retries, settled, reviews = {}, {}, {}, {}, set(), {}
    for event in events:
        kind = event["event"]
        round_number = event.get("round")
        key = (round_number, event.get("scenario_id"), event.get("repeat_number"), event.get("attempt_id"))
        if kind == "attempt_started":
            starts[key] = event
        elif kind in {"attempt_finished", "attempt_stopped"}:
            outcomes[key] = event
        elif kind == "review_decision_recorded":
            decisions[key] = event
        elif kind == "retry_queued":
            retries[(round_number, event.get("scenario_id"), event.get("repeat_number"))] = event
        elif kind == "round_settled":
            settled.add(round_number)
        elif kind == "round_review_required":
            reviews[round_number] = event
    in_flight = [key for key in starts if key not in outcomes]
    return {"starts": starts, "outcomes": outcomes, "decisions": decisions,
            "retries": retries, "settled": settled, "reviews": reviews, "in_flight": in_flight}


def current_attempt(index: dict[str, Any], round_number: int, scenario_id: str,
                    planned_attempt: str) -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None]:
    retry = index["retries"].get((round_number, scenario_id, round_number))
    attempt_id = retry.get("attempt_id") if retry else planned_attempt
    key = (round_number, scenario_id, round_number, attempt_id)
    return attempt_id, index["outcomes"].get(key), index["decisions"].get(key)


def _round_manifest_path(directory: Path, round_number: int, digest: str) -> Path:
    return directory / f"review-round-{round_number}-{digest[:12]}.json"


def classify_completion(summary: dict[str, Any], quality: dict[str, Any], result: dict[str, Any],
                        *, safe_recovery: bool, source_safe: bool, site_safe: bool,
                        injection_complete: bool, phases_complete: bool) -> tuple[str, str]:
    """Classify only this attempt's evidence. No first-pass QC state is inherited."""
    if not (safe_recovery and source_safe and site_safe):
        return "STOP", "recovery, source, or site safety is unknown or failed"
    groups = quality.get("groups") or {}
    if any(check.get("status") != "PASS" for check in (groups.get("PROTOCOL") or {}).get("checks", [])):
        return "STOP", "protocol identity/source binding failed"
    if any(check.get("check") == "cross_phase_span_identity" and check.get("status") != "PASS"
           for check in (groups.get("Q3") or {}).get("checks", [])):
        return "STOP", "cross-phase source identity conflict detected"
    p0s = result.get("p0s") or {}
    required = ("experiment_semantics", "process_recoverable", "data_real")
    if summary.get("outer_drained") is not True:
        return "STOP", "same-attempt outer workers are not proven drained"
    if not injection_complete or not phases_complete:
        return "FAILED", "same-attempt injection or formal observation was incomplete; clean attempt retained"
    if any(p0s.get(name) not in {"PASS", "FAIL"} for name in required):
        return "FAILED", "current-attempt P0 evidence is incomplete; clean attempt retained"
    old_q = result.get("old_q_states") or {}
    if any(p0s[name] != "PASS" for name in required):
        return "REVIEW_PENDING", "complete attempt is safe; current-attempt P0 weakness requires bound batch review"
    if any(old_q.get(name) not in {"PASS", "NOT_ASSESSED"} for name in ("Q1", "Q3")):
        return "REVIEW_PENDING", "current-attempt Q1/Q3 issue requires bound batch review"
    return "CANDIDATE", "same-attempt three P0 checks passed; batch review still required"


def _attempt_id_for_retry(planned_attempt: str, retry_number: int) -> str:
    value = f"{planned_attempt}-retry-{retry_number}"
    if not ATTEMPT_ID_RE.fullmatch(value):
        raise BatchError("prepared retry attempt ID exceeds admission limits")
    return value


def _condition_row(condition_path: Path, condition_id: str) -> dict[str, Any]:
    data = read_json(condition_path)
    if type(data) is list:
        matches = [row for row in data if type(row) is dict and row.get("condition_id") == condition_id]
    elif type(data) is dict and data.get("condition_id") == condition_id:
        matches = [data]
    else:
        matches = []
    if len(matches) != 1:
        raise BatchError(f"{condition_id}: condition file does not contain one matching row")
    return matches[0]


def _load_helpers(campaign=None):
    from . import campaign_runtime, environment
    if campaign is not None:
        environment.verify_ref(campaign["runtime"]["environment"])
    environment.apply(campaign_runtime.cd)
    return campaign_runtime


def _load_admission():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    try:
        from scripts.collection import validate_campaign
    except Exception as exc:
        raise BatchError(f"cannot load validate_campaign.py: {type(exc).__name__}") from None
    return validate_campaign


def _require_admitted(value: Any, label: str) -> None:
    if value is False or value is None:
        raise BatchError(f"{label} was not admitted")
    if type(value) is dict and value.get("ok") is False:
        raise BatchError(f"{label} was not admitted: {value.get('reasons') or value.get('errors') or 'unspecified'}")


def _resolve_condition(campaign: dict[str, Any], campaign_path: Path, campaign_sha: str,
                       admission: Any, row: dict[str, Any], round_number: int,
                       attempt_id: str, retry_of: str | None, retry_number: int | None,
                       directory: Path) -> tuple[Path, dict[str, Any], dict[str, Any], Any]:
    source_path = resolve_repo_path(row["condition_path"], allow_old_root=False)
    source_row = _condition_row(source_path, row["condition_id"])
    if source_row.get("scenario_id") != row["scenario_id"] or source_row.get("design_version") != row["design_version"]:
        raise BatchError(f"{row['scenario_id']}: condition scenario/design identity changed")
    if sha256_file(source_path) != row["condition_sha256"]:
        raise BatchError(f"{row['scenario_id']}: frozen seed condition SHA changed")
    from scripts.collection import prepare_campaign
    actual_row = prepare_campaign.bind_condition(source_row, campaign, campaign_path.parent, round_number, attempt_id)
    retry_spec: Any = attempt_id if retry_of is None else {
        "attempt_id": attempt_id, "retry_of": row["repeat_attempts"][str(round_number)],
        "retry_number": retry_number,
    }
    _require_admitted(admission.validate_campaign(str(campaign_path), expected_sha256=campaign_sha), "campaign admission")
    _require_admitted(admission.validate_slot(campaign, row["scenario_id"], round_number,
                                               actual_row["contract"], retry_spec), "repeat slot admission")
    attempt_dir = directory / "attempts" / f"round-{round_number}" / row["scenario_id"] / attempt_id
    attempt_condition = attempt_dir / "condition.json"
    return attempt_condition, actual_row, actual_row["contract"], retry_spec


def _persist_attempt_binding(directory: Path, campaign: dict[str, Any], campaign_path: Path,
                             row: dict[str, Any], round_number: int, attempt_id: str,
                             retry_of: str | None, source_path: Path, attempt_condition: Path,
                             actual_row: dict[str, Any], contract: dict[str, Any],
                             actual_contract_sha256: str) -> None:
    attempt_dir = attempt_condition.parent
    condition_raw = _canonical([actual_row]) + b"\n"
    _write_once_or_match(attempt_condition, condition_raw)
    _write_once_or_match(attempt_dir / "attempt-manifest.json", _canonical({
        "schema_version": "m1-repeat-attempt-manifest-v1", "campaign_id": campaign["campaign_id"],
        "campaign_sha256": sha256_file(campaign_path), "round": round_number,
        "repeat_number": round_number, "scenario_id": row["scenario_id"],
        "design_version": row["design_version"], "condition_id": row["condition_id"],
        "source_condition_path": str(source_path), "source_condition_sha256": row["condition_sha256"],
        "condition_path": str(attempt_condition), "condition_sha256": _sha(condition_raw),
        "attempt_id": attempt_id, "retry_of": retry_of,
        "actual_contract_sha256": actual_contract_sha256,
        "release_gate_sha256": campaign.get("release_gate_sha256"),
        "marker_sha256": contract["release_gate_ref"]["sha256"],
    }) + b"\n")


def _build_item(shared: Any, row: dict[str, Any], source_path: Path, actual_row: dict[str, Any],
                attempt_id: str) -> dict[str, Any]:
    from scripts.collection import scenario_definitions as asm, contract as c
    data = actual_row["contract"]
    run_contract = c.RunContract.from_dict(data, asm.load_registry(repo_root=ROOT))
    source_contract_sha = run_contract.sha256
    expected_actual = json.loads(json.dumps(data))
    expected_actual["context"]["contract_ref"] = (
        "condition:" + row["condition_id"] + ":" + source_contract_sha)
    run_contract = c.RunContract.from_dict(expected_actual, asm.load_registry(repo_root=ROOT))
    key = c.canonical_sha256(c._normalized_conditions(run_contract))
    coverage = c.canonical_sha256({"design_version": data["scenario"]["design_version"],
                                   "scenario_id": row["scenario_id"], "condition_key": key})
    run_id = "r10c-" + c.canonical_sha256({"condition_id": row["condition_id"]})[:20]
    output_root = Path(data["context"]["output_root"])
    expected_output = _runtime_path({"runtime": {"output_root": row["template"]["output_root"]}}, "output_root")
    if output_root.resolve() != expected_output:
        raise BatchError(f"{row['scenario_id']}: attempt contract output root differs from frozen template")
    mode = actual_row.get("budget_mode", "default")
    if mode not in BUDGETS:
        raise BatchError(f"{row['scenario_id']}: unknown frozen budget mode")
    return {"scenario": row["scenario_id"], "condition_id": row["condition_id"],
            "condition_file": source_path, "condition": actual_row, "contract": run_contract,
            "attempt": attempt_id, "condition_key": key, "coverage_key": coverage,
            "evidence_root": output_root / "formal" / run_id / attempt_id,
            "report_root": shared.cd.ARTIFACTS / f"{row['condition_id']}-{attempt_id}",
            "source_contract_sha256": source_contract_sha,
            "action_budget": float(row.get("action_budget", BUDGETS[mode][0])),
            "lateness_budget": float(row.get("lateness_budget", BUDGETS[mode][1])),
            "first_injection_lateness_budget": row.get("first_injection_lateness_budget")}


def _admission_module_and_campaign(directory: Path, plan_path: Path, campaign_sha: str):
    admission = _load_admission()
    _require_admitted(admission.validate_campaign(str(plan_path), expected_sha256=campaign_sha), "campaign admission")
    return admission


def _fresh_preflight(shared: Any, admission: Any, campaign: dict[str, Any], campaign_path: Path,
                     row: dict[str, Any], round_number: int, attempt_id: str,
                     contract: dict[str, Any], retry_spec: Any, campaign_sha: str,
                     item: dict[str, Any]) -> dict[str, Any]:
    _require_admitted(admission.validate_campaign(str(campaign_path), expected_sha256=campaign_sha), "campaign source/gate validation")
    _require_admitted(admission.validate_slot(campaign, row["scenario_id"], round_number, contract, retry_spec),
                       "repeat slot validation")
    if item["report_root"].exists() or item["evidence_root"].exists():
        raise BatchError("attempt output already exists; preserve it and choose a new attempt ID")
    lease = shared.require_clean_lease()
    route = shared.route_baseline()
    prom = shared.prom_baseline()
    db = _database_baseline(shared, item)
    return {"lease": lease, "route": route, "prom": prom, "db": db}


def _database_baseline(shared, item):
    return shared.database_baseline(item)


def _verify_actual_contract(campaign: dict[str, Any], campaign_path: Path, row: dict[str, Any],
                            round_number: int, attempt_id: str, summary: dict[str, Any],
                            saved: dict[str, Any], quality: dict[str, Any], evidence: dict[str, Any],
                            result: dict[str, Any], item: dict[str, Any], actual_row: dict[str, Any],
                            shared: Any) -> tuple[bool, bool]:
    from scripts.collection import scenario_definitions as asm, contract as c
    contract = saved.get("contract") or {}
    context = contract.get("context") or {}
    scenario = contract.get("scenario") or {}
    if (scenario.get("scenario_id"), scenario.get("design_version")) != (row["scenario_id"], row["design_version"]):
        raise BatchError("actual contract scenario/design does not match campaign")
    if context.get("replicate") != round_number or context.get("attempt_id") != attempt_id:
        raise BatchError("actual contract does not contain this repeat number and attempt ID")
    if context.get("owner_id") != context.get("run_id", "") + "/" + attempt_id:
        raise BatchError("actual contract owner is not attempt-specific")
    if context.get("evidence_root") != str(Path(context.get("output_root", "")) / "formal" / context.get("run_id", "") / attempt_id):
        raise BatchError("actual contract evidence path is not attempt-specific")
    marker_path = campaign_path.parent / "repeat-admission.json"
    gate_ref = contract.get("release_gate_ref") or {}
    if (gate_ref.get("uri") != str(campaign_path.parent.resolve()) or not marker_path.is_file()
            or gate_ref.get("sha256") != sha256_file(marker_path)):
        raise BatchError("actual contract does not bind the current repeat admission marker")
    expected = item["contract"].to_dict()
    if c.canonical_json(contract) != c.canonical_json(expected):
        raise BatchError("persisted actual contract differs from the attempt-specific bound contract")
    typed = c.RunContract.from_dict(contract, asm.load_registry(repo_root=ROOT))
    if saved.get("actual_contract_sha256") != typed.sha256 or summary.get("contract_sha256") != typed.sha256:
        raise BatchError("SUMMARY/recovery-input contract hash does not bind this attempt")
    if (summary.get("actual_contract_sha256") not in (None, typed.sha256)
            or result.get("attempt") not in (None, attempt_id)
            or result.get("condition_id") not in (None, row["condition_id"])):
        raise BatchError("raw result identity differs from this attempt")
    if saved.get("condition") is None or saved["condition"].get("repeat_number") != round_number:
        raise BatchError("recovery input lacks this attempt's repeat metadata")
    if c.canonical_json(saved["condition"]) != c.canonical_json(actual_row):
        raise BatchError("recovery input condition differs from the persisted attempt-specific seed")
    current_sources = shared.cd.source_hashes()
    if (type(saved.get("source_hashes")) is not dict or saved["source_hashes"] != current_sources
            or summary.get("source_hashes") != current_sources
            or c.canonical_sha256(current_sources) != campaign.get("collector_source_fingerprint")):
        raise BatchError("same-attempt source fingerprint differs from the frozen campaign")
    if quality:
        if (quality.get("sample_purpose") != "formal" or quality.get("attempt_id") != attempt_id
                or quality.get("contract_sha256") != typed.sha256):
            raise BatchError("quality report does not bind this formal attempt")
    if evidence:
        if evidence.get("attempt_id") != attempt_id or evidence.get("contract_sha256") != typed.sha256:
            raise BatchError("raw DB evidence source is not bound to this attempt")
    if result.get("formal") not in (None, "formal"):
        raise BatchError("parsed result is not formal")
    if result.get("lease", {}).get("status") not in (None, "clean") or result.get("lease", {}).get("workers") not in (None, "drained"):
        raise BatchError("same-attempt lease/worker receipt is dirty or unknown")
    recovery_status = (summary.get("recovery") or {}).get("status")
    if recovery_status not in SAFE_RECOVERY:
        raise BatchError("same-attempt recovery receipt is unsafe or unknown")
    if summary.get("outer_drained") is not True:
        raise BatchError("same-attempt outer worker drain is not proven")
    inner = summary.get("inner_result") or {}
    phase_facts = inner.get("actual_phase_facts") or []
    phase_names = [fact.get("phase") for fact in phase_facts]
    if len(phase_names) != len(set(phase_names)) or any(name not in PHASES for name in phase_names):
        raise BatchError("same-attempt phase receipts contain duplicate or unknown phases")
    expected_order = [name for name in PHASES if name in phase_names]
    if phase_names != expected_order:
        raise BatchError("same-attempt phase receipts are out of order")
    if any(fact.get("duration_s") != 300 or fact.get("workers_started") is not True for fact in phase_facts):
        raise BatchError("observed phase receipt conflicts with frozen formal protocol")
    phases_complete = phase_names == list(PHASES)
    actual_faults = {fault.get("fault_instance_id") for fault in contract.get("faults", [])}
    inject_receipts = [action for action in inner.get("action_timing", []) if action.get("action") == "inject"]
    injection_ids = [action.get("instance_id") for action in inject_receipts]
    allowed_action_status = {"operation_confirmed_not_physical", "operation_failed", "started"}
    if (not actual_faults or len(injection_ids) != len(set(injection_ids)) or not set(injection_ids) <= actual_faults
            or any(action.get("status") not in allowed_action_status for action in inject_receipts)):
        raise BatchError("same-attempt injection receipts are ambiguous or name another fault")
    injection_complete = (set(injection_ids) == actual_faults and
                          all(action.get("status") == "operation_confirmed_not_physical" for action in inject_receipts))
    if summary.get("fault_execution_confirmed") is True and not injection_complete:
        raise BatchError("SUMMARY injection claim conflicts with per-leg action receipts")
    if summary.get("sample_three_phases_attempted") is True and not phases_complete:
        raise BatchError("SUMMARY phase claim conflicts with phase receipts")
    return injection_complete, phases_complete


def _raw_manifest(report_root: Path, evidence_root: Path, campaign: dict[str, Any], row: dict[str, Any],
                  round_number: int, attempt_id: str, summary: dict[str, Any], saved: dict[str, Any],
                  quality: dict[str, Any], evidence: dict[str, Any], classification: str, reason: str,
                  runner_rc: int, post_site_checks: dict[str, Any], injection_complete: bool,
                  phases_complete: bool, result: dict[str, Any]) -> dict[str, Any]:
    paths = [report_root / "SUMMARY.json", report_root / "recovery-input.json",
             evidence_root / "contract.json", evidence_root / "artifacts/quality/result.json",
             evidence_root / "artifacts/quality/evidence.json"]
    files = [{"path": str(path.resolve()), "sha256": sha256_file(path)} for path in paths if path.is_file()]
    if len(files) < 2:
        raise BatchError("same-attempt summary or recovery-input is missing")
    descriptors = []
    for artifact in ((summary.get("inner_result") or {}).get("artifacts") or []):
        if type(artifact) is dict and all(type(artifact.get(key)) is str for key in ("artifact_id", "relative_path", "sha256")):
            descriptors.append({key: artifact[key] for key in ("artifact_id", "relative_path", "sha256")})
    raw_identity = {"files": files, "runner_artifact_sha256": descriptors,
                    "actual_contract_sha256": saved.get("actual_contract_sha256"),
                    "quality_contract_sha256": quality.get("contract_sha256") if quality else None,
                    "evidence_contract_sha256": evidence.get("contract_sha256") if evidence else None,
                    "classification": classification, "reason": reason, "runner_return_code": runner_rc,
                    "injection_complete": injection_complete, "phases_complete": phases_complete,
                    "p0s": result.get("p0s"), "old_q_states": result.get("old_q_states"),
                    "post_site_checks": post_site_checks}
    return {"schema_version": "m1-repeat-raw-manifest-v1", "campaign_id": campaign["campaign_id"],
            "round": round_number, "repeat_number": round_number, "scenario_id": row["scenario_id"],
            "condition_id": row["condition_id"], "attempt_id": attempt_id, **raw_identity,
            "raw_identity": raw_identity, "raw_sha256": _sha(_canonical(raw_identity))}


def _decision_template(directory: Path, campaign: dict[str, Any], round_number: int,
                       rows: list[dict[str, Any]], index: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    cases, pending_cases = [], []
    for row in rows:
        attempt_id, outcome, decision = current_attempt(index, round_number, row["scenario_id"],
                                                        row["repeat_attempts"][str(round_number)])
        if not outcome or outcome.get("classification") not in {"CANDIDATE", "REVIEW_PENDING", "FAILED"}:
            raise BatchError(f"{row['scenario_id']}: no completed attempt ready for batch review")
        manifest_path = Path(outcome["raw_manifest_path"])
        raw_manifest = read_json(manifest_path)
        case = {"scenario_id": row["scenario_id"], "design_version": row["design_version"],
                      "condition_id": row["condition_id"], "repeat_number": round_number,
                      "attempt_id": attempt_id, "classification": outcome["classification"],
                      "raw_sha256": raw_manifest["raw_sha256"],
                      "decision": decision.get("decision", "") if decision else "",
                      "reason": decision.get("reason", "") if decision else "",
                      "evidence_refs": decision.get("evidence_refs", []) if decision else []}
        cases.append(case)
        if decision is None:
            pending_cases.append(case)
    manifest_core = {"schema_version": "m1-repeat-review-manifest-v1", "campaign_id": campaign["campaign_id"],
                     "round": round_number, "cases": cases}
    digest = _sha(_canonical(manifest_core))
    manifest = {**manifest_core, "manifest_sha256": digest}
    manifest_path = _round_manifest_path(directory, round_number, digest)
    _write_once_or_match(manifest_path, _canonical(manifest) + b"\n")
    template = {**manifest, "cases": pending_cases,
                "instructions": "Decide only the unresolved current attempts. Set decision to QUALIFY or RECOLLECT, give an evidence-based reason, cite current-attempt path/SHA references, and preserve raw_sha256."}
    _write_once_or_match(directory / f"review-decisions-round-{round_number}-{digest[:12]}.template.json",
                         _canonical(template) + b"\n")
    return manifest_path, manifest


def _rebuild_and_maybe_review(directory: Path, campaign: dict[str, Any], round_number: int,
                              rows: list[dict[str, Any]], events: list[dict[str, Any]]) -> dict[str, Any]:
    index = index_state(events)
    target_count = len(rows)
    completed = []
    for row in rows:
        attempt_id, outcome, decision = current_attempt(index, round_number, row["scenario_id"],
                                                        row["repeat_attempts"][str(round_number)])
        if not outcome:
            return {"status": "IN_PROGRESS", "completed": len(completed), "total": target_count}
        if outcome.get("event") == "attempt_stopped":
            return {"status": "STOPPED", "completed": len(completed), "total": target_count,
                    "reason": outcome.get("reason")}
        completed.append((row, attempt_id, outcome, decision))
    if len(completed) != target_count:
        return {"status": "IN_PROGRESS", "completed": len(completed), "total": target_count}
    if round_number in index["settled"]:
        return {"status": "SETTLED", "completed": target_count, "qualified": target_count}
    if any(decision is None for _, _, _, decision in completed):
        manifest_path, manifest = _decision_template(directory, campaign, round_number, rows, index)
        existing_review = index["reviews"].get(round_number)
        if (existing_review is None or existing_review.get("manifest_sha256") != manifest["manifest_sha256"]):
            append_event(directory, campaign["campaign_id"], "round_review_required", round=round_number,
                         manifest_path=str(manifest_path), manifest_sha256=manifest["manifest_sha256"],
                         qualified_count=sum(1 for _, _, _, decision in completed if decision and decision.get("decision") == "QUALIFY"))
        return {"status": "REVIEW_PENDING", "completed": target_count, "manifest": str(manifest_path)}
    qualified = sum(1 for _, _, _, decision in completed if decision.get("decision") == "QUALIFY")
    recollect = sum(1 for _, _, _, decision in completed if decision.get("decision") == "RECOLLECT")
    if qualified == target_count:
        append_event(directory, campaign["campaign_id"], "round_settled", round=round_number,
                     qualified_count=target_count, manifest_sha256=index["reviews"].get(round_number, {}).get("manifest_sha256"))
        return {"status": "SETTLED", "completed": target_count, "qualified": target_count}
    return {"status": "RECOLLECT_REQUIRED", "completed": target_count, "qualified": qualified, "recollect": recollect}


def _raw_sha_for_outcome(outcome: dict[str, Any]) -> str:
    manifest = read_json(Path(outcome["raw_manifest_path"]))
    return str(manifest.get("raw_sha256", ""))


def _verify_review_raw(campaign: dict[str, Any], row: dict[str, Any], round_number: int,
                       attempt_id: str, outcome: dict[str, Any]) -> dict[str, Any]:
    directory = campaign_dir(campaign["campaign_id"], _batch_root(campaign))
    expected_manifest = directory / "attempts" / f"round-{round_number}" / row["scenario_id"] / attempt_id / "raw-manifest.json"
    manifest_path = Path(outcome.get("raw_manifest_path", "")).resolve()
    if manifest_path != expected_manifest.resolve() or not manifest_path.is_file():
        raise BatchError(f"{row['scenario_id']}: raw manifest path is not the current attempt")
    manifest = read_json(manifest_path)
    identity = manifest.get("raw_identity")
    if type(identity) is not dict or _sha(_canonical(identity)) != manifest.get("raw_sha256"):
        raise BatchError(f"{row['scenario_id']}: raw manifest SHA is invalid")
    if any(manifest.get(key) != value for key, value in identity.items()):
        raise BatchError(f"{row['scenario_id']}: raw manifest top-level fields disagree with its SHA-bound identity")
    for key, expected in (("campaign_id", campaign["campaign_id"]), ("round", round_number),
                          ("repeat_number", round_number), ("scenario_id", row["scenario_id"]),
                          ("condition_id", row["condition_id"]), ("attempt_id", attempt_id)):
        if manifest.get(key) != expected:
            raise BatchError(f"{row['scenario_id']}: raw manifest {key} identity mismatch")
    condition_path = resolve_repo_path(row["condition_path"], allow_old_root=False)
    seed = _condition_row(condition_path, row["condition_id"])
    context = seed["contract"]["context"]
    run_id = "r10c-" + hashlib.sha256(_canonical({"condition_id": row["condition_id"]})).hexdigest()[:20]
    report_root = __import__('scripts.collection.scenario_runner', fromlist=["ARTIFACTS"]).ARTIFACTS / f"{row['condition_id']}-{attempt_id}"
    evidence_root = Path(context["output_root"]) / "formal" / run_id / attempt_id
    permitted = {
        (report_root / "SUMMARY.json").resolve(),
        (report_root / "recovery-input.json").resolve(),
        (evidence_root / "contract.json").resolve(),
        (evidence_root / "artifacts/quality/result.json").resolve(),
        (evidence_root / "artifacts/quality/evidence.json").resolve(),
    }
    files = identity.get("files")
    if type(files) is not list or len(files) < 2:
        raise BatchError(f"{row['scenario_id']}: raw manifest critical file list is incomplete")
    available_refs = {}
    for file_row in files:
        path = Path(file_row.get("path", "")).resolve()
        if path not in permitted or not path.is_file() or sha256_file(path) != file_row.get("sha256"):
            raise BatchError(f"{row['scenario_id']}: current raw file changed or escaped attempt roots")
        available_refs[str(path)] = file_row["sha256"]
    if str((report_root / "SUMMARY.json").resolve()) not in available_refs or str((report_root / "recovery-input.json").resolve()) not in available_refs:
        raise BatchError(f"{row['scenario_id']}: required summary/recovery raw references are missing")
    for descriptor in identity.get("runner_artifact_sha256", []):
        available_refs[descriptor.get("relative_path", "")] = descriptor.get("sha256", "")
    return manifest | {"available_evidence_refs": available_refs}


def import_decisions(plan_path: Path, round_number: int, decision_path: Path) -> dict[str, Any]:
    campaign, plan_sha, _ = load_campaign(plan_path)
    _require_round(campaign, round_number)
    batch_root = _batch_root(campaign)
    directory = campaign_dir(campaign["campaign_id"], batch_root)
    with executor_lock(EXECUTOR_LOCK):
        events = load_events(directory, campaign["campaign_id"])
        _require_same_plan(events, plan_sha)
        index = index_state(events)
        if index["in_flight"]:
            raise BatchError("in-flight attempt exists; decisions cannot be imported until it is closed")
        state = _rebuild_and_maybe_review(directory, campaign, round_number, campaign["scenarios"], events)
        if state["status"] not in {"REVIEW_PENDING", "RECOLLECT_REQUIRED"}:
            raise BatchError(f"round is not awaiting decisions: {state['status']}")
        review_event = index_state(load_events(directory, campaign["campaign_id"]))["reviews"].get(round_number)
        doc = read_json(decision_path)
        if type(doc) is not dict or doc.get("campaign_id") != campaign["campaign_id"] or doc.get("round") != round_number:
            raise BatchError("decision file campaign/round mismatch")
        if doc.get("manifest_sha256") != (review_event or {}).get("manifest_sha256"):
            raise BatchError("decision file is not bound to the current round review manifest")
        cases = doc.get("cases")
        if type(cases) is not list or not cases:
            raise BatchError("decision file must contain case decisions")
        by_scenario = {row["scenario_id"]: row for row in campaign["scenarios"]}
        for case in cases:
            if type(case) is not dict:
                raise BatchError("decision case must be an object")
            scenario_id = case.get("scenario_id")
            if scenario_id not in by_scenario:
                raise BatchError("decision names a scenario outside the frozen campaign")
            row = by_scenario[scenario_id]
            attempt_id, outcome, old_decision = current_attempt(index, round_number, scenario_id,
                                                                row["repeat_attempts"][str(round_number)])
            if not outcome or case.get("attempt_id") != attempt_id or case.get("repeat_number") != round_number:
                raise BatchError(f"{scenario_id}: decision is stale or names another attempt")
            raw_manifest = _verify_review_raw(campaign, row, round_number, attempt_id, outcome)
            if case.get("raw_sha256") != raw_manifest["raw_sha256"]:
                raise BatchError(f"{scenario_id}: raw SHA does not match the current attempt")
            if old_decision is not None:
                if (old_decision.get("decision") == case.get("decision")
                        and old_decision.get("reason") == case.get("reason")
                        and old_decision.get("evidence_refs") == case.get("evidence_refs")):
                    continue
                raise BatchError(f"{scenario_id}: attempt already has a different decision")
            decision = case.get("decision")
            reason = case.get("reason")
            if decision not in {"QUALIFY", "RECOLLECT"} or type(reason) is not str or len(reason.strip()) < 16:
                raise BatchError(f"{scenario_id}: decision must be QUALIFY/RECOLLECT with an evidence-based reason")
            refs = case.get("evidence_refs")
            if type(refs) is not list or not refs:
                raise BatchError(f"{scenario_id}: decision must cite current-attempt raw evidence")
            for ref in refs:
                if type(ref) is not dict or raw_manifest["available_evidence_refs"].get(ref.get("path")) != ref.get("sha256"):
                    raise BatchError(f"{scenario_id}: evidence reference is outside or differs from the current raw manifest")
            if decision == "QUALIFY" and outcome.get("classification") == "FAILED":
                raise BatchError(f"{scenario_id}: a failed P0 attempt cannot be counted by manual decision")
            append_event(directory, campaign["campaign_id"], "review_decision_recorded", round=round_number,
                         scenario_id=scenario_id, design_version=row["design_version"], condition_id=row["condition_id"],
                         repeat_number=round_number, attempt_id=attempt_id, raw_sha256=case["raw_sha256"],
                         decision=decision, reason=reason.strip(), evidence_refs=case["evidence_refs"],
                         decision_file_sha256=sha256_file(decision_path))
            if decision == "RECOLLECT":
                retry_number = sum(1 for event in load_events(directory, campaign["campaign_id"])
                                   if event.get("event") == "retry_queued" and event.get("scenario_id") == scenario_id
                                   and event.get("round") == round_number) + 1
                planned_attempt = row["repeat_attempts"][str(round_number)]
                retry_id = _attempt_id_for_retry(planned_attempt, retry_number)
                append_event(directory, campaign["campaign_id"], "retry_queued", round=round_number,
                             scenario_id=scenario_id, design_version=row["design_version"], condition_id=row["condition_id"],
                             repeat_number=round_number, attempt_id=retry_id, retry_of=planned_attempt,
                             previous_attempt_id=attempt_id, retry_number=retry_number,
                             decision_raw_sha256=case["raw_sha256"])
        events = load_events(directory, campaign["campaign_id"])
        final = _rebuild_and_maybe_review(directory, campaign, round_number, campaign["scenarios"], events)
        return {"imported": len(cases), **final}


def _require_same_plan(events: list[dict[str, Any]], plan_sha: str) -> None:
    for event in events:
        if event.get("event") == "campaign_bound" and event.get("campaign_sha256") != plan_sha:
            raise BatchError("campaign changed after batch journal creation; preserve prior batch")


def _bind_campaign(directory: Path, campaign: dict[str, Any], campaign_sha: str, plan_path: Path,
                   events: list[dict[str, Any]]) -> None:
    rounds = list(_rounds(campaign))
    scenario_count = _scenario_count(campaign)
    budget = campaign["budget"]
    if not events:
        append_event(directory, campaign["campaign_id"], "campaign_bound", campaign_sha256=campaign_sha,
                     campaign_path=str(plan_path), rounds=rounds, cases_per_round=scenario_count,
                     firstpass_count=budget.get("firstpass_count", 0),
                     additional_repeats=budget["new_slots"],
                     target_total=budget["target_total"])
    else:
        first = next((event for event in events if event.get("event") == "campaign_bound"), None)
        if first is None or first.get("campaign_sha256") != campaign_sha or first.get("campaign_path") != str(plan_path):
            raise BatchError("existing batch log is not bound to this campaign bytes/path")


def _assert_round_order(index: dict[str, Any], campaign: dict[str, Any], round_number: int) -> None:
    rounds = _rounds(campaign)
    _require_round(campaign, round_number)
    missing = [prior for prior in rounds if prior < round_number and prior not in index["settled"]]
    if missing:
        raise BatchError(f"prior round(s) are not batch-reviewed and fully qualified: {missing}")
    if round_number in index["settled"]:
        raise BatchError(f"round {round_number} is already settled")


def _check_current_raw(report_root: Path, evidence_root: Path, item: dict[str, Any],
                       shared: Any, admission: Any, campaign: dict[str, Any], campaign_path: Path,
                       campaign_sha: str, row: dict[str, Any], round_number: int, attempt_id: str,
                       retry_spec: Any, actual_row: dict[str, Any], runner_rc: int,
                       site_safe: bool, post_site_checks: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    summary_path, input_path = report_root / "SUMMARY.json", report_root / "recovery-input.json"
    if not summary_path.is_file() or not input_path.is_file():
        raise BatchError("same-attempt SUMMARY or recovery-input is missing; recovery cannot be proven")
    summary, saved = read_json(summary_path), read_json(input_path)
    quality_path, evidence_path = (evidence_root / "artifacts/quality/result.json",
                                   evidence_root / "artifacts/quality/evidence.json")
    quality, evidence = {}, {}
    if quality_path.is_file() and evidence_path.is_file():
        summary, quality, evidence, result = shared.read_completion(item, runner_rc)
        if summary.get("status") == "ENGINEERING_EXECUTED_RECOVERED":
            shared.verify_database_evidence(quality, evidence)
    else:
        try:
            lease = shared.read_lease()
        except Exception:
            lease = {"status": "UNKNOWN", "workers": "UNKNOWN"}
        result = {"condition_id": row["condition_id"], "attempt": attempt_id, "formal": "formal",
                  "recovery_status": (summary.get("recovery") or {}).get("status"), "lease": lease,
                  "p0s": {}, "old_q_states": {}, "runner_return_code": runner_rc}
    injection_complete, phases_complete = _verify_actual_contract(
        campaign, campaign_path, row, round_number, attempt_id, summary, saved,
        quality, evidence, result, item, actual_row, shared)
    _require_admitted(admission.validate_campaign(str(campaign_path), expected_sha256=campaign_sha),
                      "post-attempt source/gate validation")
    _require_admitted(admission.validate_slot(campaign, row["scenario_id"], round_number,
                                               saved["contract"], retry_spec), "post-attempt slot validation")
    post_lease = post_site_checks.get("lease") if type(post_site_checks.get("lease")) is dict else {}
    post_db = post_site_checks.get("db") if type(post_site_checks.get("db")) is dict else {}
    safe_recovery = ((summary.get("recovery") or {}).get("status") in SAFE_RECOVERY
                     and result.get("lease", {}).get("status") == "clean"
                     and result.get("lease", {}).get("workers") == "drained"
                     and post_lease.get("status") == "clean"
                     and post_lease.get("workers") == "drained"
                     and bool(post_db))
    status, reason = classify_completion(summary, quality, result, safe_recovery=safe_recovery,
        source_safe=True, site_safe=site_safe, injection_complete=injection_complete,
        phases_complete=phases_complete)
    manifest = _raw_manifest(report_root, evidence_root, campaign, row, round_number, attempt_id,
        summary, saved, quality, evidence, status, reason, runner_rc, post_site_checks,
        injection_complete, phases_complete, result)
    return {"summary": summary, "quality": quality, "evidence": evidence, "result": result,
            "classification": status, "reason": reason, "injection_complete": injection_complete,
            "phases_complete": phases_complete}, manifest


def _execution_error(summary: dict[str, Any], row: dict[str, Any]) -> dict[str, str] | None:
    inner = summary.get("inner_result") or {}
    error = summary.get("error") or inner.get("run_error")
    if not isinstance(error, str) or not error.strip():
        return None
    signature = re.sub(r"\b[0-9a-fA-F-]{12,}\b", "<id>", error.split(" <-", 1)[0].strip())
    signature = re.sub(r"\s+", " ", signature)[:180]
    family = str(row.get("execution_family") or "shared_execution")
    return {"signature": signature, "family": family}


def _pause_repeated_public_error(directory: Path, campaign: dict[str, Any], round_number: int,
                                 scenario_id: str) -> None:
    events = load_events(directory, campaign["campaign_id"])
    recent = [event for event in events if event.get("round") == round_number
              and event.get("event") == "attempt_finished"]
    recent = recent[-2:]
    if len(recent) != 2:
        return
    first, second = (event.get("execution_error") for event in recent)
    if not first or not second or first.get("signature") != second.get("signature"):
        return
    left = set(str(first.get("family", "")).split("|"))
    right = set(str(second.get("family", "")).split("|"))
    affected = sorted((left & right) - {""}) or ["shared_execution"]
    append_event(directory, campaign["campaign_id"], "execution_family_paused", round=round_number,
                 scenario_id=scenario_id, affected_families=affected,
                 error_signature=first["signature"], consecutive_attempts=2)
    raise BatchError(f"same public execution error repeated; paused affected family: {', '.join(affected)}")


def run_round(plan_path: Path, round_number: int, *, execute: bool, resume: bool) -> dict[str, Any]:
    if execute:
        from . import environment
        environment.require_windows_execution()
        environment.resolve_cli("kubectl")
        environment.resolve_cli("docker")
        document = read_json(plan_path)
        environment.verify_ref(document["runtime"]["environment"])
        environment.assert_live()
    campaign, campaign_sha, plan_path = load_campaign(plan_path)
    _require_round(campaign, round_number)
    batch_root = _batch_root(campaign)
    directory = campaign_dir(campaign["campaign_id"], batch_root)
    if not execute:
        return {"mode": "plan", "campaign_id": campaign["campaign_id"], "campaign_sha256": campaign_sha,
                "round": round_number, "slots": [{"scenario_id": row["scenario_id"], "condition_id": row["condition_id"],
                    "repeat_number": round_number, "attempt_id": row["repeat_attempts"][str(round_number)]}
                    for row in campaign["scenarios"]]}
    python_path = _python_path(campaign)
    if Path(sys.executable).resolve() != python_path:
        raise BatchError(f"live run must use campaign runtime.python_executable: {python_path}")
    with executor_lock(EXECUTOR_LOCK):
        events = load_events(directory, campaign["campaign_id"])
        _require_same_plan(events, campaign_sha)
        _bind_campaign(directory, campaign, campaign_sha, plan_path, events)
        events = load_events(directory, campaign["campaign_id"])
        index = index_state(events)
        _assert_round_order(index, campaign, round_number)
        if index["in_flight"]:
            raise BatchError("an attempt is in-flight or has unknown completion; do not take it over")
        stopped = [event for event in events if event.get("round") == round_number
                   and event.get("event") in {"attempt_stopped", "execution_family_paused"}]
        if stopped:
            raise BatchError("round has a persistent STOP latch; preserve evidence and require explicit root closure")
        existing_round = any(event.get("round") == round_number and event.get("event") in
                              {"preflight_failed", "attempt_prepared", "attempt_started", "attempt_finished",
                               "attempt_stopped", "round_review_required"} for event in events)
        if existing_round and not resume:
            raise BatchError("round already has journaled work; explicit --resume is required")
        if not existing_round and resume:
            raise BatchError("--resume was requested but this round has no journaled work")
        admission = _admission_module_and_campaign(directory, plan_path, campaign_sha)
        shared = _load_helpers(campaign)
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
        append_event(directory, campaign["campaign_id"], "round_started", round=round_number,
                     resume=resume, source_fingerprint=campaign.get("source_fingerprint"),
                     release_gate_sha256=campaign.get("release_gate_sha256"))
        for row in campaign["scenarios"]:
            events = load_events(directory, campaign["campaign_id"])
            index = index_state(events)
            if index["in_flight"]:
                raise BatchError("attempt is in-flight or worker state is unknown; stopping without takeover")
            retry = index["retries"].get((round_number, row["scenario_id"], round_number))
            attempt_id = retry["attempt_id"] if retry else row["repeat_attempts"][str(round_number)]
            retry_of = retry.get("retry_of") if retry else None
            retry_number = retry.get("retry_number") if retry else None
            key = (round_number, row["scenario_id"], round_number, attempt_id)
            if key in index["outcomes"]:
                continue
            if key in index["starts"] and key not in index["outcomes"]:
                raise BatchError(f"{row['scenario_id']}: started attempt has no completion; refusing replay")
            slot_decisions = [event for event in events if event.get("event") == "review_decision_recorded"
                              and event.get("round") == round_number and event.get("scenario_id") == row["scenario_id"]]
            if slot_decisions and slot_decisions[-1].get("decision") != "RECOLLECT":
                continue
            condition_path = resolve_repo_path(row["condition_path"], allow_old_root=False)
            attempt_condition, actual_row, contract, retry_spec = _resolve_condition(
                campaign, plan_path, campaign_sha, admission, row, round_number,
                attempt_id, retry_of, retry_number, directory)
            item = _build_item(shared, row, attempt_condition, actual_row, attempt_id)
            try:
                preflight = _fresh_preflight(shared, admission, campaign, plan_path, row, round_number,
                    attempt_id, contract, retry_spec, campaign_sha, item)
            except Exception as exc:
                append_event(directory, campaign["campaign_id"], "preflight_failed", round=round_number,
                    scenario_id=row["scenario_id"], condition_id=row["condition_id"],
                    repeat_number=round_number, attempt_id=attempt_id, retry_of=retry_of,
                    reason=f"{type(exc).__name__}: {str(exc)[:300]}")
                raise
            _persist_attempt_binding(directory, campaign, plan_path, row, round_number, attempt_id,
                retry_of, condition_path, attempt_condition, actual_row, contract, item["contract"].sha256)
            append_event(directory, campaign["campaign_id"], "attempt_prepared", round=round_number,
                scenario_id=row["scenario_id"], condition_id=row["condition_id"], repeat_number=round_number,
                attempt_id=attempt_id, retry_of=retry_of, condition_sha256=sha256_file(attempt_condition),
                contract_sha256=item["contract"].sha256)
            append_event(directory, campaign["campaign_id"], "attempt_started", round=round_number,
                         scenario_id=row["scenario_id"], design_version=row["design_version"],
                         condition_id=row["condition_id"], repeat_number=round_number,
                         attempt_id=attempt_id, retry_of=retry_of, source_condition_sha256=row["condition_sha256"],
                         attempt_condition_sha256=sha256_file(attempt_condition),
                         contract_sha256=item["contract"].sha256, preflight=preflight,
                         started_pid=os.getpid())
            command = [str(python_path), "-B", "-X", "utf8", str(DB_ENV_WRAPPER), "run",
                       "--condition-file", str(attempt_condition), "--condition-id", row["condition_id"],
                       "--attempt", attempt_id, "--action-budget", f"{item['action_budget']:g}",
                       "--lateness-budget", f"{item['lateness_budget']:g}", "--live"]
            if item.get("first_injection_lateness_budget") is not None:
                command += ["--first-injection-lateness-budget", f"{float(item['first_injection_lateness_budget']):g}"]
            completed = subprocess.run(command, cwd=ROOT, check=False)
            # Do not invoke recover here. The run entry owns its single recovery attempt.
            try:
                post_lease = shared.require_clean_lease()
                post_route = shared.route_baseline()
                post_prom = shared.prom_baseline()
                post_db = _database_baseline(shared, item)
                site_safe = True
            except Exception as exc:
                site_safe = False
                site_error = f"{type(exc).__name__}: {str(exc)[:200]}"
                post_lease = post_route = post_prom = post_db = None
            try:
                post_checks = {"lease": post_lease, "route": post_route, "prom": post_prom, "db": post_db}
                parsed, raw_manifest = _check_current_raw(
                    item["report_root"], item["evidence_root"], item, shared, admission, campaign,
                    plan_path, campaign_sha, row, round_number, attempt_id, retry_spec, actual_row,
                    completed.returncode, site_safe, post_checks)
                classification = parsed["classification"]
                raw_result = parsed["result"]
                safe_to_continue = classification in {"CANDIDATE", "REVIEW_PENDING", "FAILED"}
                attempt_dir = directory / "attempts" / f"round-{round_number}" / row["scenario_id"] / attempt_id
                raw_manifest_path = attempt_dir / "raw-manifest.json"
                _write_once(raw_manifest_path, _canonical(raw_manifest) + b"\n")
                append_event(directory, campaign["campaign_id"], "attempt_finished" if safe_to_continue else "attempt_stopped",
                    round=round_number, scenario_id=row["scenario_id"], design_version=row["design_version"],
                    condition_id=row["condition_id"], repeat_number=round_number, attempt_id=attempt_id,
                    retry_of=retry_of, classification=classification, reason=parsed["reason"],
                    raw_manifest_path=str(raw_manifest_path), raw_sha256=raw_manifest["raw_sha256"],
                    runner_return_code=completed.returncode,
                    post_site_checks=post_checks,
                    site_error=None if site_safe else site_error,
                    p0s=raw_result.get("p0s"), old_q_states=raw_result.get("old_q_states"),
                    execution_error=_execution_error(parsed["summary"], row))
            except Exception as exc:
                append_event(directory, campaign["campaign_id"], "attempt_stopped", round=round_number,
                    scenario_id=row["scenario_id"], design_version=row["design_version"],
                    condition_id=row["condition_id"], repeat_number=round_number, attempt_id=attempt_id,
                    retry_of=retry_of, classification="STOP", reason=f"{type(exc).__name__}: {str(exc)[:300]}",
                    runner_return_code=completed.returncode,
                    report_root=str(item["report_root"]), evidence_root=str(item["evidence_root"]))
                raise BatchError(f"{row['scenario_id']}: completion evidence unsafe/incomplete; stopped without recovery retry") from None
            if not safe_to_continue:
                raise BatchError(f"{row['scenario_id']}: {parsed['reason']}; preserving raw and stopping the batch")
            _pause_repeated_public_error(directory, campaign, round_number, row["scenario_id"])
        events = load_events(directory, campaign["campaign_id"])
        result = _rebuild_and_maybe_review(directory, campaign, round_number, campaign["scenarios"], events)
        return {"campaign_id": campaign["campaign_id"], "round": round_number, **result}


def status(plan_path: Path, round_number: int | None = None) -> dict[str, Any]:
    campaign, plan_sha, _ = load_campaign(plan_path)
    if round_number is not None:
        _require_round(campaign, round_number)
    planned_rounds = _rounds(campaign)
    scenario_count = _scenario_count(campaign)
    directory = campaign_dir(campaign["campaign_id"], _batch_root(campaign))
    events = load_events(directory, campaign["campaign_id"])
    _require_same_plan(events, plan_sha)
    index = index_state(events)
    stopped = [event for event in events if (round_number is None or event.get("round") == round_number)
               and event.get("event") in {"attempt_stopped", "execution_family_paused"}]
    rounds = {}
    selected = (round_number,) if round_number is not None else planned_rounds
    for number in selected:
        completed = qualified = pending_review = failed = recollect = 0
        for row in campaign["scenarios"]:
            attempt_id, outcome, decision = current_attempt(index, number, row["scenario_id"],
                                                             row["repeat_attempts"][str(number)])
            if outcome:
                completed += 1
                if outcome.get("classification") == "FAILED":
                    failed += 1
                if decision is None:
                    pending_review += 1
                elif decision.get("decision") == "QUALIFY":
                    qualified += 1
                elif decision.get("decision") == "RECOLLECT":
                    recollect += 1
        rounds[number] = {"completed": completed, "qualified": qualified,
                          "review_pending": pending_review, "failed": failed,
                          "recollect_queued": recollect, "total": scenario_count,
                          "settled": number in index["settled"]}
    return {"campaign_id": campaign["campaign_id"], "campaign_sha256": plan_sha,
            "in_flight": index["in_flight"], "halted": bool(stopped), "stop_events": len(stopped), "rounds": rounds,
            "live_site_checked": False, "credentials_read": False}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--round", type=int)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--status", action="store_true", help="show journal status without site/credential reads")
    mode.add_argument("--decision-file", type=Path, help="import batch decisions bound to raw SHA256 evidence")
    parser.add_argument("--resume", action="store_true", help="continue only completed/queued slots; never take over in-flight work")
    parser.add_argument("--execute", action="store_true", help="explicitly run live collection")
    args = parser.parse_args(argv)
    if args.execute and args.round is None:
        raise BatchError("--execute requires an explicit round from the selected campaign")
    if args.resume and not args.execute:
        raise BatchError("--resume is only valid with --execute")
    if args.status and (args.execute or args.resume or args.decision_file):
        raise BatchError("--status cannot be combined with execution or review writes")
    if args.decision_file and (args.execute or args.resume):
        raise BatchError("decision import cannot be combined with live execution")
    if args.status:
        print(json.dumps(status(args.plan, args.round), ensure_ascii=False, indent=2))
        return 0
    if args.decision_file:
        if args.round is None:
            raise BatchError("--round is required when importing decisions")
        result = import_decisions(args.plan, args.round, args.decision_file)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if args.round is None:
        campaign, digest, _ = load_campaign(args.plan)
        rounds = _rounds(campaign)
        scenario_count = _scenario_count(campaign)
        budget = campaign["budget"]
        print(json.dumps({"mode": "plan", "campaign_id": campaign["campaign_id"], "campaign_sha256": digest,
                          "rounds": list(rounds), "slots_per_round": scenario_count,
                          "additional_repeats": budget["new_slots"],
                          "live_site_checked": False, "credentials_read": False}, ensure_ascii=False, indent=2))
        return 0
    result = run_round(args.plan, args.round, execute=args.execute, resume=args.resume)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") in {"SETTLED", "REVIEW_PENDING", "IN_PROGRESS"} or result.get("mode") == "plan" else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BatchError as exc:
        print(f"REPEAT BATCH STOP: {exc}", file=sys.stderr)
        raise SystemExit(2)
