"""Offline, versioned collection run configuration; never a live execution/release gate.

The registry describes composition, including designs whose parameters are not
yet frozen. A RunContract instead requires concrete parameters for one attempt.
It records planned windows only. Observation, recovery, output reservation,
credential resolution and final sample qualification belong to later modules.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from itertools import combinations
import json
import math
from pathlib import Path
import re
from typing import Any
from urllib.parse import parse_qsl, urlsplit

SCHEMA_VERSION = "rq4-collect/run-contract-v1"
_UNRESOLVED = ("NOT_FROZEN", "UNRESOLVED", "PILOT_TO_FREEZE", "TBD")
_SECRET = re.compile(r"(?:password|passwd|secret|token|api[_-]?key|authorization|cookie)", re.I)
_PHASES = ("pre_fault", "during_fault", "post_recovery")


class ContractError(ValueError):
    """Invalid public configuration. Messages never include supplied values."""


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise ContractError(message)


def _text(value: Any, field: str) -> str:
    _require(isinstance(value, str) and bool(value.strip()), field + ": nonempty text required")
    _require(value == value.strip(), field + ": surrounding whitespace forbidden")
    return value


def _identifier(value: Any, field: str) -> str:
    _text(value, field)
    _require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value) is not None,
             field + ": unsafe identifier")
    _require(value.upper().split(".")[0] not in
             {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
              *(f"LPT{i}" for i in range(1, 10))}, field + ": reserved identifier")
    return value


def _number(value: Any, field: str, *, positive: bool = False) -> None:
    _require(type(value) in (int, float) and math.isfinite(value)
             and (value > 0 if positive else value >= 0), field + ": invalid finite number")


def _integer(value: Any, field: str, minimum: int = 1) -> None:
    _require(type(value) is int and value >= minimum, field + ": invalid integer")


def _fields(value: Any, expected: set[str], field: str) -> None:
    _require(isinstance(value, dict), field + ": object required")
    _require(set(value) == expected, field + ": missing or unknown fields")


def _sha(value: Any, field: str) -> None:
    _require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None,
             field + ": lowercase SHA256 required")


def _json_values(value: Any, *, concrete: bool, location: str = "configuration") -> None:
    """Accept exact JSON types; reject credentials before serializing/repr/errors.

    collection: ``location`` now tracks the exact leaf path (``.key`` / ``[index]``),
    so a rejected leaf is named instead of reporting the bare root. Diagnostic
    only -- which values are accepted or rejected is unchanged.
    """
    if isinstance(value, dict):
        for key, item in value.items():
            _require(type(key) is str, location + ": string keys required")
            if key == "credential_refs":
                _credential_refs(item)
                continue
            _require(_SECRET.search(key) is None or key.endswith("_ref") or key == "credential_refs",
                     location + ": inline credential field forbidden")
            if _SECRET.search(key):
                _credential_reference(item)
            _json_values(item, concrete=concrete, location=location + "." + key)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _json_values(item, concrete=concrete, location=location + "[" + str(index) + "]")
    elif type(value) is str:
        if concrete:
            _require(not any(marker in value.upper() for marker in _UNRESOLVED),
                     location + ": unresolved runtime value")
        if value.lower().startswith(("http://", "https://", "//")) or value.startswith("/"):
            try:
                url = urlsplit(value)
                _require(url.username is None and url.password is None,
                         location + ": URL credentials forbidden")
                _require(not any(_SECRET.search(k) for k, _ in parse_qsl(url.query)),
                         location + ": URL credential query forbidden")
            except ValueError:
                raise ContractError(location + ": invalid URL") from None
    elif type(value) in (int, float):
        _require(math.isfinite(value), location + ": nonfinite JSON number")
    else:
        _require(value is None or type(value) is bool, location + ": unsupported JSON value")


def _concrete_parameters(value: Any) -> None:
    """Reject missing knobs; semantic knob names/ranges need a mechanism schema."""
    if isinstance(value, (dict, list)):
        _require(bool(value), "fault parameters: empty container")
        for item in (value.values() if isinstance(value, dict) else value):
            _concrete_parameters(item)
    elif isinstance(value, str):
        _text(value, "fault parameter")
    else:
        _require(value is not None, "fault parameters: null value")


def canonical_json(value: Any) -> str:
    _json_values(value, concrete=False)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _credential_reference(value: Any) -> None:
    _require(isinstance(value, str) and
             re.fullmatch(r"(?:env|secret):[A-Za-z0-9][A-Za-z0-9_./-]*", value) is not None,
             "credential reference required, never a literal secret")


def _credential_refs(refs: Any) -> None:
    _require(isinstance(refs, dict), "credential_refs: object required")
    for alias, reference in refs.items():
        _identifier(alias, "credential alias")
        _credential_reference(reference)


def _seal(cls: Any, payload: dict[str, Any]) -> Any:
    result = object.__new__(cls)
    object.__setattr__(result, "_json", canonical_json(payload))
    return result


def _strict_json(payload: str) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            _require(key not in result, "JSON: duplicate object key")
            result[key] = value
        return result

    def reject_constant(_: str) -> None:
        raise ContractError("JSON: nonfinite constant")

    try:
        return json.loads(payload, object_pairs_hook=pairs, parse_constant=reject_constant)
    except (json.JSONDecodeError, TypeError, UnicodeError):
        raise ContractError("JSON: invalid document") from None


@dataclass(frozen=True)
class ScenarioKey:
    design_version: str
    scenario_id: str

    def __post_init__(self) -> None:
        _identifier(self.design_version, "design_version")
        _identifier(self.scenario_id, "scenario_id")


@dataclass(frozen=True)
class TimeWindow:
    """Positive half-open [start, end) window in one explicitly named clock.

    Equal numeric values from different clock IDs or time bases cannot be
    compared. No default tolerance or physical-time inference is supplied.
    """
    start: float
    end: float
    time_basis: str
    clock_id: str

    def __post_init__(self) -> None:
        _number(self.start, "window.start")
        _number(self.end, "window.end")
        _require(self.end > self.start, "window: end must exceed start")
        _require(self.time_basis in {"run_offset", "monotonic", "unix_epoch"}, "window: unsupported time basis")
        _text(self.clock_id, "window.clock_id")

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> TimeWindow:
        _fields(value, {"start", "end", "time_basis", "clock_id"}, "window")
        return cls(**value)

    def to_dict(self) -> dict[str, Any]:
        return {"start": self.start, "end": self.end, "time_basis": self.time_basis, "clock_id": self.clock_id}

    def contains_window(self, other: TimeWindow) -> bool:
        _require((self.time_basis, self.clock_id) == (other.time_basis, other.clock_id),
                 "window: incompatible clocks")
        return self.start <= other.start and other.end <= self.end


@dataclass(frozen=True)
class ScenarioDefinition:
    key: ScenarioKey
    atoms: tuple[str, ...]


@dataclass(frozen=True)
class DesignRegistry:
    """Composition-only binding; importing NOT_FROZEN designs is intentional."""
    design_version: str
    scenarios: tuple[ScenarioDefinition, ...]
    catalog_sha256: str
    source_kind: str
    source_sha256: str

    @classmethod
    def from_json(cls, payload: bytes) -> DesignRegistry:
        """Parse caller-supplied bytes and retain their exact (including BOM/EOL) hash."""
        _require(type(payload) is bytes, "catalog JSON: original bytes required")
        try:
            parsed = _strict_json(payload.decode("utf-8-sig"))
        except UnicodeError:
            raise ContractError("catalog JSON: invalid UTF-8") from None
        registry = cls.from_catalog(parsed)
        return cls(registry.design_version, registry.scenarios, registry.catalog_sha256,
                   "raw_json_bytes", hashlib.sha256(payload).hexdigest())

    @classmethod
    def from_catalog(cls, catalog: dict[str, Any]) -> DesignRegistry:
        _require(isinstance(catalog, dict), "catalog: object required")
        version = _identifier(catalog.get("design_version"), "catalog.design_version")
        rows = catalog.get("scenarios")
        _require(isinstance(rows, list) and bool(rows), "catalog: scenarios required")
        scenarios = []
        seen = set()
        for row in rows:
            _require(isinstance(row, dict), "catalog scenario: object required")
            key = ScenarioKey(row.get("design_version"), row.get("scenario_id"))
            _require(key.design_version == version and key not in seen, "catalog: duplicate or mismatched key")
            seen.add(key)
            atoms = row.get("atoms")
            _require(isinstance(atoms, list) and bool(atoms), "catalog: atoms required")
            for atom in atoms:
                _atom_parts(atom)
            _require(len(atoms) == len(set(atoms)), "catalog: duplicate atom")
            _integer(row.get("G"), "catalog.G")
            _integer(row.get("fault_count"), "catalog.fault_count")
            _require(row["G"] == len({_atom_parts(a)[1] for a in atoms})
                     and row["fault_count"] == len(atoms), "catalog: incorrect entity/instance count")
            scenarios.append(ScenarioDefinition(key, tuple(sorted(atoms))))
        content_hash = canonical_sha256(catalog)
        return cls(version, tuple(scenarios), content_hash, "in_memory_canonical_json", content_hash)

    def resolve(self, key: ScenarioKey) -> ScenarioDefinition:
        _require(key.design_version == self.design_version, "scenario: wrong design version")
        for scenario in self.scenarios:
            if scenario.key == key:
                return scenario
        raise ContractError("scenario: unknown or retired ID")


def _atom_parts(atom: Any) -> tuple[str, str]:
    _text(atom, "atom_id")
    parts = atom.split("@")
    _require(len(parts) == 2 and all(parts), "atom_id: expected fault_type@entity")
    return parts[0], parts[1]


@dataclass(frozen=True, repr=False, init=False)
class FaultInstanceSpec:
    """Validated immutable planned instance; parameters/raw target stay JSON."""
    _json: str

    def __init__(self) -> None:
        raise TypeError("Use FaultInstanceSpec.from_dict")

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> FaultInstanceSpec:
        _fields(value, {"fault_instance_id", "atom_id", "normalized_root_entity", "fault_class",
                        "fault_type", "mechanism", "mechanism_version", "normalization_version",
                        "raw_target", "parameters", "planned_window"}, "fault instance")
        _json_values(value, concrete=True)
        _identifier(value["fault_instance_id"], "fault_instance_id")
        for field in ("normalized_root_entity", "fault_class", "fault_type", "mechanism", "mechanism_version", "normalization_version"):
            _text(value[field], "fault." + field)
        _require(_atom_parts(value["atom_id"]) == (value["fault_type"], value["normalized_root_entity"]),
                 "fault: atom type/entity mismatch")
        _fields(value["raw_target"], {"kind", "name", "scope", "selector"}, "raw_target")
        for field in ("kind", "name", "scope"):
            _text(value["raw_target"][field], "raw_target." + field)
        _require(isinstance(value["raw_target"]["selector"], dict), "raw_target.selector: object required")
        _require(isinstance(value["parameters"], dict) and bool(value["parameters"]), "fault: explicit parameters required")
        _concrete_parameters(value["parameters"])
        window = TimeWindow.from_dict(value["planned_window"])
        _require(window.time_basis == "run_offset", "fault: planned window requires run_offset")
        return _seal(cls, value)

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._json)

    @property
    def fault_instance_id(self) -> str:
        return self.to_dict()["fault_instance_id"]

    @property
    def normalized_root_entity(self) -> str:
        return self.to_dict()["normalized_root_entity"]

    @property
    def planned_window(self) -> TimeWindow:
        return TimeWindow.from_dict(self.to_dict()["planned_window"])


def root_entities(instances: tuple[FaultInstanceSpec, ...]) -> tuple[str, ...]:
    _require(bool(instances), "instances: nonempty collection required")
    ids = [instance.fault_instance_id for instance in instances]
    _require(len(ids) == len(set(ids)), "instances: duplicate instance ID")
    return tuple(sorted({instance.normalized_root_entity for instance in instances}))


def validate_main_set(instances: tuple[FaultInstanceSpec, ...]) -> None:
    _require(len(root_entities(instances)) == len(instances), 'collection main set: one instance per entity required')


@dataclass(frozen=True, repr=False, init=False)
class RunContext:
    """Explicit immutable identity. Paths bind intended locations, not ownership proof."""
    _json: str

    def __init__(self) -> None:
        raise TypeError("Use RunContext.from_dict")

    @classmethod
    def from_dict(cls, value: dict[str, Any], *, purpose: str) -> RunContext:
        _fields(value, {"run_id", "attempt_id", "profile_id", "block", "replicate", "repo_root",
                        "output_root", "evidence_root", "contract_ref", "owner_id", "clock_id",
                        "kube_context", "namespace", "fingerprints", "credential_refs"}, "context")
        _json_values(value, concrete=True)
        for field in ("run_id", "attempt_id", "profile_id", "namespace"):
            _identifier(value[field], "context." + field)
        for field in ("contract_ref", "clock_id", "kube_context"):
            _text(value[field], "context." + field)
        for field in ("block", "replicate"):
            _integer(value[field], "context." + field)
        _require(value["owner_id"] == value["run_id"] + "/" + value["attempt_id"], "context: owner identity mismatch")
        paths = {}
        for field in ("repo_root", "output_root", "evidence_root"):
            _text(value[field], "context." + field)
            path = Path(value[field])
            _require(path.is_absolute(), "context: explicit absolute paths required")
            # Pure lexical binding here; real-path/junction checks + exclusive
            # reservation MUST be performed by the later output gate (G15).
            _require(".." not in path.parts, "context: path traversal forbidden")
            paths[field] = path
        expected = paths["output_root"] / purpose / value["run_id"] / value["attempt_id"]
        _require(paths["evidence_root"] == expected, "context: evidence path not owned by this attempt/purpose")
        forbidden = {"k8s_pilot", "_delivery", "_archive", "traditional_v2_lite"}
        _require(not forbidden.intersection(part.lower() for part in paths["output_root"].parts),
                 "context: legacy output location forbidden")
        _fields(value["fingerprints"], {"collector", "generator", "annotation_rules", "quality_rules", "environment"}, "fingerprints")
        for fingerprint in value["fingerprints"].values():
            _sha(fingerprint, "fingerprint")
        _credential_refs(value["credential_refs"])
        return _seal(cls, value)

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._json)


def _validate_profile(profile: dict[str, Any], profile_id: str) -> None:
    _fields(profile, {"profile_id", "generator_version", "random_seed", "arrival_model", "max_concurrency",
                      "overload_policy", "stop_policy", "streams"}, "request_profile")
    _require(profile["profile_id"] == profile_id, "request_profile: identity mismatch")
    _text(profile["generator_version"], "generator_version")
    _integer(profile["random_seed"], "random_seed", 0)
    _integer(profile["max_concurrency"], "max_concurrency")
    _require(profile["arrival_model"] == "fixed_rate_open_loop", "request_profile: unsupported arrival model")
    _require(profile["overload_policy"] == "drop", "request_profile: explicit drop policy required")
    _require(profile["stop_policy"] in {"drain", "cancel"}, "request_profile: invalid stop policy")
    _require(isinstance(profile["streams"], list) and bool(profile["streams"]), "request_profile: streams required")
    seen = set()
    for stream in profile["streams"]:
        _fields(stream, {"stream_id", "entrypoint", "endpoint", "method", "parameters", "carrier", "direct_api",
                         "rate_rps", "max_concurrency", "timeout_s", "client_retry_limit"}, "request stream")
        sid = _identifier(stream["stream_id"], "stream_id")
        _require(sid not in seen, "request_profile: duplicate stream_id")
        seen.add(sid)
        for field in ("entrypoint", "endpoint", "carrier"):
            _text(stream[field], "stream." + field)
        try:
            entrypoint = urlsplit(stream["entrypoint"])
            endpoint = urlsplit(stream["endpoint"])
            _require(entrypoint.scheme.lower() in {"http", "https"} and bool(entrypoint.hostname)
                     and entrypoint.username is None and entrypoint.password is None
                     and not entrypoint.fragment, "stream: invalid or credential-bearing entrypoint")
            _require(not endpoint.scheme and not endpoint.netloc and endpoint.path.startswith("/")
                     and not endpoint.fragment, "stream: endpoint must be an absolute path, not a URL")
            _require(not any(_SECRET.search(k) for url in (entrypoint, endpoint) for k, _ in parse_qsl(url.query)),
                     "stream: credential query forbidden")
        except ValueError:
            raise ContractError("stream: invalid URL/path") from None
        _require(stream["method"] in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}, "stream: invalid HTTP method")
        _require(type(stream["direct_api"]) is bool, "stream: direct_api must be boolean")
        _require(isinstance(stream["parameters"], dict), "stream: explicit parameters object required")
        _integer(stream["max_concurrency"], "stream.max_concurrency")
        _require(stream["max_concurrency"] <= profile["max_concurrency"], "stream: concurrency exceeds global cap")
        _integer(stream["client_retry_limit"], "client_retry_limit", 0)
        for field in ("rate_rps", "timeout_s"):
            _number(stream[field], "stream." + field, positive=True)


@dataclass(frozen=True, repr=False, init=False)
class RunContract:
    """Concrete plan only. Validation does not run or authorize anything.

    qualification remains not_assessed for smoke, pilot AND formal plans.
    Formal plans additionally bind a release-gate reference; this module cannot
    attest its existence, freshness, approval or final sample qualification.
    """
    _json: str

    def __init__(self) -> None:
        raise TypeError("Use RunContract.from_dict or from_json")

    @classmethod
    def from_json(cls, payload: str, registry: DesignRegistry) -> RunContract:
        return cls.from_dict(_strict_json(payload), registry)

    @classmethod
    def from_dict(cls, value: dict[str, Any], registry: DesignRegistry) -> RunContract:
        _fields(value, {"schema_version", "scenario", "catalog_sha256", "catalog_source", "purpose", "protocol_status",
                        "qualification", "scope", "context", "request_profile", "phases", "faults",
                        "metric_interval_s", "rules", "release_gate_ref"}, "run contract")
        _json_values(value, concrete=True)
        _require(value["schema_version"] == SCHEMA_VERSION, "run contract: unsupported schema")
        _fields(value["scenario"], {"design_version", "scenario_id"}, "scenario")
        definition = registry.resolve(ScenarioKey(**value["scenario"]))
        _sha(value["catalog_sha256"], "catalog_sha256")
        _require(value["catalog_sha256"] == registry.catalog_sha256, "run contract: catalog content mismatch")
        _fields(value["catalog_source"], {"kind", "sha256"}, "catalog_source")
        _require(value["catalog_source"] == {"kind": registry.source_kind, "sha256": registry.source_sha256},
                 "run contract: catalog source identity mismatch")
        purpose = value["purpose"]
        _require(purpose in {"smoke", "pilot", "formal"}, "run contract: invalid purpose")
        _require(value["protocol_status"] == ("formal_frozen" if purpose == "formal" else "pilot_fixed"),
                 "run contract: purpose/protocol mismatch")
        _require(value["qualification"] == "not_assessed", "run contract: configuration cannot claim sample qualification")
        _require(value["scope"] in {"m1_main", "extension"}, "run contract: invalid scope")
        context = RunContext.from_dict(value["context"], purpose=purpose).to_dict()
        _validate_profile(value["request_profile"], context["profile_id"])
        _fields(value["phases"], set(_PHASES), "phases")
        phases = [TimeWindow.from_dict(value["phases"][p]) for p in _PHASES]
        _require(all(p.time_basis == "run_offset" and p.clock_id == context["clock_id"] for p in phases),
                 "phases: clock mismatch")
        _require(phases[0].start == 0 and phases[0].end <= phases[1].start and phases[1].end <= phases[2].start,
                 "phases: must be ordered without overlap from zero")
        _require(isinstance(value["faults"], list) and bool(value["faults"]), "run contract: faults required")
        faults = tuple(FaultInstanceSpec.from_dict(fault) for fault in value["faults"])
        root_entities(faults)
        _require(tuple(sorted(fault.to_dict()["atom_id"] for fault in faults)) == definition.atoms,
                 "run contract: composition differs from versioned scenario")
        for fault in faults:
            _require(phases[1].contains_window(fault.planned_window), "fault: planned window outside during_fault")
        if value["scope"] == "m1_main":
            validate_main_set(faults)
        _number(value["metric_interval_s"], "metric_interval_s", positive=True)
        _fields(value["rules"], {"timing", "injection", "recovery", "completeness", "annotation"}, "rules")
        for rule in value["rules"].values():
            _text(rule, "rule reference")
        if purpose == "formal":
            _require(all(p.end - p.start == 300 for p in phases) and value["metric_interval_s"] == 2,
                     'formal: current collection requires three 300-second observations and 2-second metric interval')
            _fields(value["release_gate_ref"], {"uri", "sha256"}, "release_gate_ref")
            _text(value["release_gate_ref"]["uri"], "release gate URI")
            _sha(value["release_gate_ref"]["sha256"], "release gate SHA256")
        else:
            _require(value["release_gate_ref"] is None, "pilot/smoke: formal release gate must be null")
        return _seal(cls, value)

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._json)

    def to_json(self) -> str:
        return self._json

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self._json.encode("utf-8")).hexdigest()

    @property
    def context(self) -> RunContext:
        value = self.to_dict()
        return RunContext.from_dict(value["context"], purpose=value["purpose"])

    @property
    def faults(self) -> tuple[FaultInstanceSpec, ...]:
        return tuple(FaultInstanceSpec.from_dict(f) for f in self.to_dict()["faults"])

    @property
    def root_entities(self) -> tuple[str, ...]:
        return root_entities(self.faults)

    @property
    def phase_transition_windows(self) -> dict[str, TimeWindow | None]:
        """Planned conversion/recovery gaps, excluded from observation durations."""
        phases = [TimeWindow.from_dict(self.to_dict()["phases"][p]) for p in _PHASES]
        return {name: (TimeWindow(a.end, b.start, a.time_basis, a.clock_id) if b.start > a.end else None)
                for name, a, b in (("pre_to_during", phases[0], phases[1]),
                                   ("during_to_post", phases[1], phases[2]))}


PLAN_SCHEMA_VERSION = "rq4-collect/execution-plan-v1"
ORDERING_PROTOCOL = "replicate_blocks_sha256_v1"
CONTROL_COMPARISON_V1 = "exact_contract_conditions_v1"
CONTROL_COMPARISON_V2 = "target_rule_semantics_v2"


@dataclass(frozen=True)
class ConditionTemplate:
    """A concrete validated v1 contract plus declarative family references.

    The template's run/attempt/replicate/block/evidence/owner/clock identities
    are replaced during compilation. Physical conditions are not calibrated,
    inferred, reordered or promoted to empirically qualified samples.
    """
    condition_id: str
    contract: RunContract
    family_ids: tuple[str, ...] = ()
    quality_rules_payload: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        _identifier(self.condition_id, "condition_id")
        _require(isinstance(self.contract, RunContract), "condition: validated RunContract required")
        _require(type(self.family_ids) is tuple and len(set(self.family_ids)) == len(self.family_ids),
                 "condition: unique family tuple required")
        for family_id in self.family_ids:
            _identifier(family_id, "family_id")
        if self.quality_rules_payload is not None:
            _validated_quality_payload(self.contract, self.quality_rules_payload)
            object.__setattr__(self, "quality_rules_payload", json.loads(canonical_json(self.quality_rules_payload)))


def _validated_quality_payload(contract: RunContract, payload: Any) -> dict[str, Any]:
    # Late import avoids a contract/quality module import cycle. This is only
    # used by the explicit v2 compiler after both schemas are initialized.
    from .quality import QualityRules
    _require(type(payload) is dict, "v2 pairing requires each full quality-rules payload")
    rules = QualityRules.from_dict(payload)
    value = contract.to_dict()
    _require(rules.sha256 == value["context"]["fingerprints"]["quality_rules"],
             "v2 quality-rules payload hash differs from its contract")
    _require(payload["rule_refs"] == {key: value["rules"][key] for key in ("injection", "recovery", "completeness", "annotation")},
             "v2 quality-rules references differ from their contract")
    faults = {fault["fault_instance_id"]: fault for fault in value["faults"]}
    _require({rule["instance_id"] for rule in payload["injection"]} == set(faults),
             "v2 injection rules must cover exactly this contract's fault instances")
    for rule in payload["injection"]:
        fault = faults[rule["instance_id"]]
        _require(rule["mechanism"] == fault["mechanism"] and rule["mechanism_version"] == fault["mechanism_version"]
                 and rule["signal"]["entity"] == fault["normalized_root_entity"], "v2 target rule identity mismatch")
    _require(set(contract.root_entities) <= {rule["entity"] for rule in payload["recovery"]["metric_rules"]},
             "v2 recovery rules missing a target entity")
    return rules.to_dict()


def _target_quality_projection(contract: RunContract, payload: dict[str, Any], target_atoms: tuple[str, ...]) -> dict[str, Any]:
    rules = _validated_quality_payload(contract, payload)
    faults = {fault["atom_id"]: fault for fault in contract.to_dict()["faults"]}
    _require(set(target_atoms) <= set(faults), "quality projection target missing")
    by_instance = {rule["instance_id"]: rule for rule in rules["injection"]}
    injections = []
    entities = {faults[atom]["normalized_root_entity"] for atom in target_atoms}
    for atom in target_atoms:
        row = dict(by_instance[faults[atom]["fault_instance_id"]])
        del row["instance_id"]  # Proven local key, resolved through the same atom.
        injections.append({"atom_id": atom, "rule": row})
    recovery = dict(rules["recovery"])
    recovery["metric_rules"] = [rule for rule in recovery["metric_rules"] if rule["entity"] in entities]
    # Every source, query id, threshold, label, data/coverage policy and all
    # component readback obligations remain substantive, byte-exact fields.
    return {"injection": injections, "recovery": recovery, "data": rules["data"], "status": rules["status"]}


@dataclass(frozen=True)
class RepetitionSlot:
    replicate: int
    block: int

    def __post_init__(self) -> None:
        _integer(self.replicate, "repetition.replicate")
        _integer(self.block, "repetition.block")


def _normalized_conditions(contract: RunContract) -> dict[str, Any]:
    """Exact normalization whitelist. Preserve list order and all conditions."""
    value = contract.to_dict()
    for name in ("run_id", "attempt_id", "block", "replicate", "evidence_root", "owner_id", "clock_id", "contract_ref"):
        del value["context"][name]
    for window in value["phases"].values():
        window["clock_id"] = "plan-relative-clock"
    for fault in value["faults"]:
        del fault["fault_instance_id"]
        fault["planned_window"]["clock_id"] = "plan-relative-clock"
    return value


def _control_mismatches(combo: dict[str, Any], control: dict[str, Any],
                        target_atoms: tuple[str, ...]) -> list[str]:
    """Compare shared conditions while retaining observed-unit differences."""
    reasons = []
    def differs(left: Any, right: Any) -> bool:
        return canonical_json(left) != canonical_json(right)

    if differs(combo["request_profile"], control["request_profile"]):
        reasons.append("request_profile_mismatch")
    if differs(combo["phases"], control["phases"]) or differs(combo["metric_interval_s"], control["metric_interval_s"]):
        reasons.append("observation_protocol_mismatch")
    if differs(combo["rules"], control["rules"]):
        reasons.append("rules_mismatch")
    if differs(combo["context"], control["context"]):
        reasons.append("context_or_fingerprint_mismatch")
    for name in ("schema_version", "purpose", "protocol_status", "scope", "catalog_sha256", "catalog_source", "release_gate_ref"):
        if differs(combo[name], control[name]):
            reasons.append("protocol_or_source_mismatch")
            break
    if combo["scenario"]["design_version"] != control["scenario"]["design_version"]:
        reasons.append("design_version_mismatch")
    retained = [fault for fault in combo["faults"] if fault["atom_id"] in target_atoms]
    controls = control["faults"]
    if [f["atom_id"] for f in retained] != [f["atom_id"] for f in controls]:
        reasons.append("shared_fault_order_mismatch")
    control_by_atom = {f["atom_id"]: f for f in controls}
    for fault in retained:
        candidate = control_by_atom.get(fault["atom_id"])
        if candidate is None:
            reasons.append("target_atom_missing")
            continue
        dimensions = {
            "parameters": "strength_mismatch", "raw_target": "raw_target_mismatch",
            "planned_window": "target_window_mismatch", "mechanism": "mechanism_mismatch",
            "mechanism_version": "mechanism_mismatch", "normalization_version": "normalization_mismatch",
            "normalized_root_entity": "normalization_mismatch", "fault_class": "fault_class_mismatch",
            "fault_type": "fault_type_mismatch",
        }
        for field, reason in dimensions.items():
            if differs(fault[field], candidate[field]):
                reasons.append(reason)
    return sorted(set(reasons))


def _find_control(combo: dict[str, Any], target_atoms: tuple[str, ...],
                  units: list[dict[str, Any]], normalized: dict[str, dict[str, Any]],
                  missing_reason: str, *, quality_by_key=None) -> dict[str, Any]:
    candidates = []
    matched = []
    combo_context = combo["run_contract"]["context"]
    wanted = set(target_atoms)
    for candidate in units:
        data = candidate["run_contract"]
        if {fault["atom_id"] for fault in data["faults"]} != wanted:
            continue
        context = data["context"]
        if (context["replicate"], context["block"]) != (combo_context["replicate"], combo_context["block"]):
            continue
        left, right = normalized[combo["condition_key"]], normalized[candidate["condition_key"]]
        if quality_by_key is None:
            reasons = _control_mismatches(left, right, target_atoms)
        else:
            left, right = json.loads(canonical_json(left)), json.loads(canonical_json(right))
            for item in (left, right):
                del item["context"]["fingerprints"]["quality_rules"]
                for key in ("injection", "recovery", "completeness"):
                    item["rules"][key] = CONTROL_COMPARISON_V2
            reasons = _control_mismatches(left, right, target_atoms)
            source = quality_by_key[combo["condition_key"]]
            control = quality_by_key[candidate["condition_key"]]
            if canonical_json(_target_quality_projection(source["contract"], source["rules"], target_atoms)) !=\
                    canonical_json(_target_quality_projection(control["contract"], control["rules"], target_atoms)):
                reasons.append("target_quality_semantics_mismatch")
        if not reasons:
            matched.append(candidate["unit_id"])
        else:
            candidates.append({"unit_id": candidate["unit_id"], "reasons": reasons})
    if len(matched) == 1:
        return {"status": "planned_match", "control_unit_id": matched[0], "reasons": [], "rejected_candidates": candidates}
    if len(matched) > 1:
        return {"status": "unpaired", "control_unit_id": None,
                "reasons": ["multiple_compatible_controls"], "compatible_candidates": sorted(matched),
                "rejected_candidates": candidates}
    reasons = sorted({reason for candidate in candidates for reason in candidate["reasons"]})
    return {"status": "unpaired", "control_unit_id": None, "reasons": reasons or [missing_reason],
            "rejected_candidates": candidates}


@dataclass(frozen=True, repr=False, init=False)
class CompiledPlan:
    """Immutable planned manifest and RunContracts; no empirical qualification."""
    _json: str
    _contracts: tuple[RunContract, ...]

    def __init__(self) -> None:
        raise TypeError("Use compile_execution_plan")

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._json)

    def to_json(self) -> str:
        return self._json

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self._json.encode("utf-8")).hexdigest()

    @property
    def contracts(self) -> tuple[RunContract, ...]:
        return self._contracts


def compile_execution_plan(registry: DesignRegistry, *, plan_id: str,
                           conditions: tuple[ConditionTemplate, ...], repetitions: tuple[RepetitionSlot, ...],
                           ordering_seed: int, ordering_protocol: str, first_attempt_id: str,
                           comparison_protocol: str = CONTROL_COMPARISON_V1) -> CompiledPlan:
    """Compile explicit conditions; exact duplicate conditions share one unit.

    Ordering seed changes scheduling only, never request_profile.random_seed.
    Hash ordering is versioned and independent of Python's random library.
    Condition family labels are provenance/ref groupings, not proof of matching.
    Single controls are selected only through exact planned compatibility;
    missing/incompatible controls remain in the target-unit denominator.
    """
    _identifier(plan_id, "plan_id")
    _identifier(first_attempt_id, "first_attempt_id")
    _integer(ordering_seed, "ordering_seed", 0)
    _require(ordering_protocol == ORDERING_PROTOCOL, "plan: unsupported ordering protocol")
    _require(comparison_protocol in {CONTROL_COMPARISON_V1, CONTROL_COMPARISON_V2}, "plan: unsupported comparison protocol")
    _require(type(conditions) is tuple and bool(conditions) and all(isinstance(c, ConditionTemplate) for c in conditions),
             "plan: explicit condition tuple required")
    _require(type(repetitions) is tuple and bool(repetitions) and all(isinstance(r, RepetitionSlot) for r in repetitions),
             "plan: explicit repetition tuple required")
    _require(len({c.condition_id for c in conditions}) == len(conditions), "plan: duplicate condition ID")
    _require(len({r.replicate for r in repetitions}) == len(repetitions), "plan: duplicate replicate number")
    ordered_slots = sorted(repetitions, key=lambda r: (r.block, r.replicate))
    groups: dict[str, dict[str, Any]] = {}
    common_scope = None
    sources = []
    quality_by_key = {} if comparison_protocol == CONTROL_COMPARISON_V2 else None
    for condition in sorted(conditions, key=lambda c: c.condition_id):
        # Rebind even an already valid template against this exact registry.
        template = RunContract.from_json(condition.contract.to_json(), registry)
        data = template.to_dict()
        _require(data["scope"] == "m1_main", 'compiler v1: collection main-set conditions only')
        scope = (data["purpose"], data["context"]["repo_root"], data["context"]["output_root"])
        _require(common_scope is None or scope == common_scope, "plan: mixed purpose/repository/output root")
        common_scope = scope
        if data["purpose"] == "formal":
            _require(len(repetitions) == 5, "formal plan: exactly five distinct repetitions required")
        normalized = _normalized_conditions(template)
        condition_key = canonical_sha256(normalized)
        if quality_by_key is not None:
            quality_by_key[condition_key] = {"contract": template,
                "rules": _validated_quality_payload(template, condition.quality_rules_payload)}
        if condition_key not in groups:
            groups[condition_key] = {"template": template, "normalized": normalized,
                                     "source_ids": [], "family_ids": set()}
        group = groups[condition_key]
        _require(group["normalized"] == normalized, "plan: condition hash collision")
        group["source_ids"].append(condition.condition_id)
        group["family_ids"].update(condition.family_ids)
        sources.append({"condition_id": condition.condition_id, "template_sha256": template.sha256,
                        "condition_key": condition_key, "family_ids": sorted(condition.family_ids)})
        if quality_by_key is not None:
            sources[-1]["quality_rules_payload"] = quality_by_key[condition_key]["rules"]
            sources[-1]["quality_rules_sha256"] = template.context.to_dict()["fingerprints"]["quality_rules"]
    plan_definition = {"schema_version": PLAN_SCHEMA_VERSION, "plan_id": plan_id,
                       "catalog_source": {"kind": registry.source_kind, "sha256": registry.source_sha256},
                       "condition_sources": sources,
                       "repetitions": [{"replicate": s.replicate, "block": s.block} for s in ordered_slots],
                       "ordering": {"protocol": ordering_protocol, "seed": ordering_seed},
                       "first_attempt_id": first_attempt_id}
    if quality_by_key is not None:
        plan_definition["comparison_protocol"] = comparison_protocol
    definition_sha = canonical_sha256(plan_definition)
    normalized_by_key = {key: group["normalized"] for key, group in groups.items()}
    units = []
    contracts = []
    run_ids = set()
    for slot in ordered_slots:
        ordered_keys = sorted(groups, key=lambda key: (canonical_sha256({"protocol": ordering_protocol,
            "seed": ordering_seed, "replicate": slot.replicate, "block": slot.block, "condition_key": key}), key))
        for condition_key in ordered_keys:
            group = groups[condition_key]
            value = group["template"].to_dict()
            suffix = canonical_sha256({"definition_sha256": definition_sha, "condition_key": condition_key,
                                       "replicate": slot.replicate, "block": slot.block})[:24]
            run_id = plan_id + "-" + suffix
            _require(run_id not in run_ids, "plan: generated run ID collision")
            run_ids.add(run_id)
            clock_id = "clock-" + run_id
            context = value["context"]
            context.update(run_id=run_id, attempt_id=first_attempt_id, replicate=slot.replicate, block=slot.block,
                           clock_id=clock_id, owner_id=run_id + "/" + first_attempt_id,
                           contract_ref="plan:" + plan_id + ":" + definition_sha,
                           evidence_root=str(Path(context["output_root"]) / value["purpose"] / run_id / first_attempt_id))
            for window in value["phases"].values():
                window["clock_id"] = clock_id
            for index, fault in enumerate(value["faults"], 1):
                fault["fault_instance_id"] = "F" + str(index)
                fault["planned_window"]["clock_id"] = clock_id
            contract = RunContract.from_dict(value, registry)
            contracts.append(contract)
            units.append({"unit_id": run_id, "condition_key": condition_key,
                          "source_condition_ids": list(group["source_ids"]), "family_ids": sorted(group["family_ids"]),
                          "execution_ordinal": len(units) + 1, "run_contract_sha256": contract.sha256,
                          "run_contract": contract.to_dict()})
    target_pairs = []
    subset_pairs = []
    catalog_compositions = {frozenset(s.atoms) for s in registry.scenarios}
    for combo in units:
        faults = combo["run_contract"]["faults"]
        if len(faults) < 2:
            continue
        for fault in faults:
            target_entity = fault["normalized_root_entity"]
            pair = {"comparison_unit_id": canonical_sha256({"combination_unit_id": combo["unit_id"], "target_entity": target_entity}),
                    "combination_unit_id": combo["unit_id"], "target_entity": target_entity, "target_atom": fault["atom_id"]}
            pair.update(_find_control(combo, (fault["atom_id"],), units, normalized_by_key, "single_control_missing",
                                      quality_by_key=quality_by_key))
            target_pairs.append(pair)
        for size in range(2, len(faults)):
            for subset in combinations([fault["atom_id"] for fault in faults], size):
                pair = {"combination_unit_id": combo["unit_id"], "subset_atoms": list(subset)}
                pair.update(_find_control(combo, subset, units, normalized_by_key, "subset_control_missing",
                                          quality_by_key=quality_by_key))
                if frozenset(subset) not in catalog_compositions:
                    pair["reasons"] = sorted(set(pair["reasons"] + ["subset_not_in_catalog"]))
                subset_pairs.append(pair)
    paired = sum(pair["status"] == "planned_match" for pair in target_pairs)
    observations = sum(sum(w["end"] - w["start"] for w in unit["run_contract"]["phases"].values()) for unit in units)
    gaps = sum(sum(w.end - w.start for w in c.phase_transition_windows.values() if w is not None) for c in contracts)
    manifest = {**plan_definition, "definition_sha256": definition_sha, "status": "planned_not_empirical",
                "qualification": "not_assessed", "execution_units": units, "target_pairs": target_pairs,
                "subset_controls": subset_pairs,
                "budget": {"submitted_condition_templates": len(conditions), "unique_execution_conditions": len(groups),
                           "repetitions_per_condition": len(repetitions), "planned_sample_count": len(units),
                           "observation_seconds": observations, "planned_transition_seconds": gaps},
                "planned_pairing_coverage": {"numerator": paired, "denominator": len(target_pairs),
                                             "percent": paired / len(target_pairs) * 100 if target_pairs else None},
                "subset_control_coverage": {"numerator": sum(p["status"] == "planned_match" for p in subset_pairs),
                                            "denominator": len(subset_pairs)},
                "family_execution_units": {family: [unit["unit_id"] for unit in units if family in unit["family_ids"]]
                    for family in sorted({family for group in groups.values() for family in group["family_ids"]})}}
    result = _seal(CompiledPlan, manifest)
    object.__setattr__(result, "_contracts", tuple(contracts))
    return result
