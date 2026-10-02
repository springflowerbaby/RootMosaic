"""Validate collection coverage, source fingerprints and recorded evidence.

This offline module registers sealed evidence and verifies it against the
current execution plan, scenario catalog, rules and source assets. Missing,
stale, contradictory or unexecuted evidence cannot become a passing record.
Re-evaluation remains bound to the original attempt and the applicable rules;
it cannot reconstruct missing observations or silently carry old judgments
into a different source revision.

Legacy serialized record types and staged evidence remain supported for
compatibility. This module neither starts runtime clients nor collects data.
It preserves the required coverage, identity and evidence checks used by the
collection entry point."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any
import uuid

from scripts.collection import contract as c

RELEASE_SCHEMA = "rq4-collect/release-gate-v1"
FINGERPRINT_SCHEMA = "rq4-collect/release-fingerprint-v1"
EXPECTED_SCHEMA = "rq4-collect/release-coverage-v1"
EVIDENCE_SCHEMA = "rq4-collect/release-evidence-v1"
RELEASE_RECORD_SCHEMA = "rq4-collect/release-record-v1"
INVALIDATION_EVENT_SCHEMA = "rq4-collect/release-invalidation-event-v1"
QUALITY_REPORT_SCHEMA = "rq4-collect/quality-report-v1"
PLAN_SCHEMA = "rq4-collect/execution-plan-v1"
QUALITY_REPORT_RELATIVE_PATH = "artifacts/quality/result.json"
MAX_ASSET_BYTES = 64 * 1024 * 1024

SHARED_CORE = "SHARED_CORE"
SCENARIO_LOCAL = "SCENARIO_LOCAL"
CRITERION = "CRITERION"
ENVIRONMENT = "ENVIRONMENT"
DOCUMENT = "DOCUMENT"
ROLES = (SHARED_CORE, SCENARIO_LOCAL, CRITERION, ENVIRONMENT, DOCUMENT)

# Explicit invalidation rules: which asset change invalidates which evidence.
# scope: "all" (every smoke/pilot item) or "scenario" (only items whose
# scenario_id is bound to the asset). re_evaluation_allowed marks the single
# path where a PASS judgment may be re-derived without rerunning the field:
# pure criterion/annotation interpretation, unchanged field behaviour and
# sufficient retained raw evidence (enforced at re-evaluation registration).
INVALIDATION_RULES = {
    SHARED_CORE: {"scope": "all", "requires": "full_smoke_and_full_pilot_rerun",
                  "re_evaluation_allowed": False},
    ENVIRONMENT: {"scope": "all", "requires": "full_smoke_and_full_pilot_rerun",
                  "re_evaluation_allowed": False},
    DOCUMENT: {"scope": "all", "requires": "rederive_expected_sets_and_rerun_affected",
               "re_evaluation_allowed": False},
    SCENARIO_LOCAL: {"scope": "scenario", "requires": "scenario_smoke_and_affected_pilot_rerun",
                     "re_evaluation_allowed": False},
    CRITERION: {"scope": "criteria", "requires": "re_evaluation_or_rerun",
                "re_evaluation_allowed": True},
}

# Machine-checkable minimum rules for a prescribed full-pilot selection.
PILOT_COVERAGE_RULES = (
    "pilot_items_are_plan_items",
    "mechanism_coverage",
    "observation_protocol_coverage",
    "fault_count_tier_coverage",
    "family_combo_coverage",
)

_EVIDENCE_KINDS = ("smoke", "pilot")
_EVIDENCE_MODES = ("direct", "re_evaluation")
_EVIDENCE_STATUSES = ("PASS", "FAIL", "SKIP", "NOT_RUN", "BLOCKED")
_FAILING = ("FAIL", "BLOCKED")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_ENV_SECTIONS = ("cluster_uid", "namespace_uid", "kube_context", "nodes", "image_manifest")
_QUALITY_GROUPS = ("PROTOCOL", "Q1", "Q2", "Q3")


class GateError(ValueError):
    """Invalid release-gate input. Messages never include supplied values."""


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise GateError(message)


def _text(value: Any, field: str) -> str:
    _require(isinstance(value, str) and bool(value.strip()), field + ": nonempty text required")
    return value


def _sha(value: Any, field: str) -> None:
    _require(isinstance(value, str) and _SHA256.fullmatch(value) is not None,
             field + ": lowercase SHA256 required")


def _timestamp(value: Any, field: str) -> Any:
    from datetime import datetime
    _text(value, field)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise GateError(field + ": invalid ISO-8601 timestamp") from None
    _require(parsed.tzinfo is not None and parsed.utcoffset() is not None, field + ": timezone required")
    return parsed


def _identifier(value: Any, field: str) -> None:
    _text(value, field)
    _require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value) is not None,
             field + ": unsafe identifier")


def _hash_file(path: Path) -> tuple[str, int]:
    _require(path.is_file(), "fingerprint asset: missing file")
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            _require(total <= MAX_ASSET_BYTES, "fingerprint asset: exceeds size limit")
            digest.update(chunk)
    return digest.hexdigest(), total


def _sealed_hash(payload: dict[str, Any], hash_field: str) -> str:
    return c.canonical_sha256({key: value for key, value in payload.items() if key != hash_field})


# ---------------------------------------------------------------------------
# Fingerprint bundle
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AssetSpec:
    """One frozen asset file plus its invalidation role/scope."""
    label: str
    path: Path
    role: str
    scenario_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.label, "asset label")
        _require(self.role in ROLES, "asset role: unknown role")
        _require(isinstance(self.path, Path) and self.path.is_absolute(), "asset path: absolute Path required")
        if self.role != SCENARIO_LOCAL:
            _require(not self.scenario_ids, "asset scope: scenario_ids only valid for SCENARIO_LOCAL")
        for scenario_id in self.scenario_ids:
            _identifier(scenario_id, "asset scenario_id")
        _require(len(set(self.scenario_ids)) == len(self.scenario_ids), "asset scope: duplicate scenario")


def default_asset_specs(repo_root: Path) -> tuple[AssetSpec, ...]:
    """Real-workspace frozen asset list (modules, ops configs, documents).

    Reference helper for the root integration phase; offline tests build
    their own fake asset trees. Roles follow INVALIDATION_RULES above.
    """
    root = Path(repo_root)
    _require(root.is_absolute() and root.is_dir(), "default assets: repo root required")
    modules = ("__init__", "contract", "primitives", "workload", "telemetry", "annotations",
               "quality", "journal", "runner", "gateway", "live_runtime", "log_archive",
               "review_samples",
               "log_driver_kubectl", "log_transport", "release_gate", "auxiliary", "pricing_route_model", "pricing_route",
               "observability", "sampling", 'scenario_runner')
    assets = [AssetSpec("module." + name, (root / 'scripts/collection' / name).with_suffix(".py"),
                        SHARED_CORE) for name in modules]
    for label, relative in (
        ("ops.prometheus", 'ops/metrics/prometheus.yml'),
        ("ops.prometheus-cri", 'ops/metrics/prometheus-container-resources.yml'),
        ("ops.metrics-compose", 'ops/metrics/docker-compose.metrics.yml'),
        ("ops.otel-metrics-config", 'ops/metrics/otel-metrics-config.yaml'),
        ("ops.maintenance-receipt", 'ops/metrics/maintenance_receipt.py'),
        ("ops.rollout-sampling", 'ops/metrics/rollout_sampling.py'),
        ("ops.cri-exporter", 'ops/metrics/cri_resource_exporter/exporter.py'),
        ('config.scenarios', 'configs/collection/scenarios.json'),
        ('config.gateway-cpu-network-scenarios', 'configs/collection/gateway_cpu_network_scenarios.json'),
        ("module.environment", 'scripts/collection/environment.py'),
        ('module.campaign-runtime', 'scripts/collection/campaign_runtime.py'),
        ('module.run-scenario', 'scripts/collection/run_scenario.py'),
    ):
        assets.append(AssetSpec(label, root / relative, DOCUMENT if label.startswith("doc.") else SHARED_CORE))
    return tuple(assets)


def validate_environment_fingerprint(value: dict[str, Any]) -> dict[str, Any]:
    """Validate externally captured environment identity (cluster/namespace UID,
    nodes, image manifest reference). No cluster access happens here."""
    _require(type(value) is dict and set(value) == set(_ENV_SECTIONS),
             "environment: exact identity fields required")
    for field in ("cluster_uid", "namespace_uid"):
        _text(value[field], "environment." + field)
        try:
            _require(str(uuid.UUID(value[field])) == value[field], "environment: canonical UUID required")
        except (ValueError, AttributeError):
            raise GateError("environment: canonical UUID required") from None
    _text(value["kube_context"], "environment.kube_context")
    _require(type(value["nodes"]) is list and bool(value["nodes"]) and len(set(value["nodes"])) == len(value["nodes"]),
             "environment: unique nonempty node list required")
    for node in value["nodes"]:
        _text(node, "environment node")
    manifest = value["image_manifest"]
    _require(type(manifest) is dict and set(manifest) == {"uri", "sha256"},
             "environment: image manifest reference required")
    _text(manifest["uri"], "image manifest uri")
    _sha(manifest["sha256"], "image manifest sha256")
    return json.loads(c.canonical_json(value))


@dataclass(frozen=True, init=False, repr=False)
class FingerprintBundle:
    """Immutable fingerprint bundle; the bundle hash seals content + roles."""
    _json: str

    def __init__(self) -> None:
        raise TypeError("Use compute_fingerprint_bundle or FingerprintBundle.from_dict")

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._json)

    @property
    def bundle_sha256(self) -> str:
        return self.to_dict()["bundle_sha256"]

    @property
    def collected_at_utc(self) -> str:
        return self.to_dict()["collected_at_utc"]

    @property
    def assets(self) -> dict[str, dict[str, Any]]:
        return self.to_dict()["assets"]

    @property
    def environment(self) -> dict[str, Any]:
        return self.to_dict()["environment"]

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> FingerprintBundle:
        _require(type(value) is dict and set(value) ==
                 {"schema_version", "collected_at_utc", "assets", "environment", "bundle_sha256"},
                 "fingerprint bundle: exact fields required")
        _require(value["schema_version"] == FINGERPRINT_SCHEMA, "fingerprint bundle: unsupported schema")
        _timestamp(value["collected_at_utc"], "fingerprint bundle.collected_at_utc")
        _require(type(value["assets"]) is dict and bool(value["assets"]), "fingerprint bundle: assets required")
        for label, entry in value["assets"].items():
            _identifier(label, "fingerprint asset label")
            _require(type(entry) is dict and set(entry) == {"sha256", "role", "scenario_ids", "bytes"},
                     "fingerprint asset: exact fields required")
            _sha(entry["sha256"], "fingerprint asset sha256")
            _require(entry["role"] in ROLES, "fingerprint asset: unknown role")
            _require(type(entry["bytes"]) is int and entry["bytes"] >= 0,
                     "fingerprint asset: byte count required")
            scenario_ids = entry["scenario_ids"]
            _require(type(scenario_ids) is list and len(set(scenario_ids)) == len(scenario_ids),
                     "fingerprint asset: unique scenario scope required")
            for scenario_id in scenario_ids:
                _identifier(scenario_id, "fingerprint asset scenario_id")
            _require(bool(scenario_ids) == (entry["role"] == SCENARIO_LOCAL),
                     "fingerprint asset: scenario scope must match role")
        validate_environment_fingerprint(value["environment"])
        _sha(value["bundle_sha256"], "fingerprint bundle sha256")
        _require(value["bundle_sha256"] == _sealed_hash(value, "bundle_sha256"),
                 "fingerprint bundle: hash does not seal content")
        result = object.__new__(cls)
        object.__setattr__(result, "_json", c.canonical_json(value))
        return result

    @classmethod
    def from_json(cls, payload: str) -> FingerprintBundle:
        return cls.from_dict(c._strict_json(payload))


def compute_fingerprint_bundle(assets: tuple[AssetSpec, ...], environment: dict[str, Any],
                               *, collected_at_utc: str) -> FingerprintBundle:
    """Hash every frozen asset file and seal it with roles and environment identity."""
    _require(type(assets) is tuple and bool(assets), "fingerprint bundle: asset tuple required")
    _timestamp(collected_at_utc, "fingerprint bundle.collected_at_utc")
    entries: dict[str, dict[str, Any]] = {}
    for asset in assets:
        _require(isinstance(asset, AssetSpec), "fingerprint bundle: AssetSpec entries required")
        _require(asset.label not in entries, "fingerprint bundle: duplicate asset label")
        digest, size = _hash_file(asset.path)
        entries[asset.label] = {"sha256": digest, "role": asset.role,
                                "scenario_ids": list(asset.scenario_ids), "bytes": size}
    payload = {"schema_version": FINGERPRINT_SCHEMA, "collected_at_utc": collected_at_utc,
               "assets": entries, "environment": validate_environment_fingerprint(environment)}
    payload["bundle_sha256"] = _sealed_hash(payload, "bundle_sha256")
    return FingerprintBundle.from_dict(payload)


@dataclass(frozen=True, init=False, repr=False)
class InvalidationDiff:
    """Machine-computed asset/environment change classification."""
    _json: str

    def __init__(self) -> None:
        raise TypeError("Use diff_fingerprints")

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._json)

    @property
    def changed(self) -> tuple[dict[str, Any], ...]:
        return tuple(self.to_dict()["changed"])

    @property
    def empty(self) -> bool:
        return not self.to_dict()["changed"]

    @property
    def allows_re_evaluation(self) -> bool:
        return self.to_dict()["allows_re_evaluation"]

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> InvalidationDiff:
        _require(type(value) is dict and set(value) ==
                 {"changed", "affected_scenario_ids", "worst_scope", "allows_re_evaluation"},
                 "invalidation diff: exact fields required")
        result = object.__new__(cls)
        object.__setattr__(result, "_json", c.canonical_json(value))
        return result


def diff_fingerprints(old: FingerprintBundle, new: FingerprintBundle) -> InvalidationDiff:
    """Classify every difference between two bundles through the rule table.

    Environment identity drift is always ENVIRONMENT-scoped (all evidence
    invalid). Asset additions/removals and role changes are treated as
    SHARED_CORE (conservative, cannot be scoped by the caller).
    """
    changed: list[dict[str, Any]] = []
    affected_scenarios: set[str] = set()
    scopes: list[str] = []
    for label in sorted(set(old.assets) | set(new.assets)):
        before, after = old.assets.get(label), new.assets.get(label)
        if before == after:
            continue
        if before is None or after is None:
            role = (after or before)["role"]
            reason = "asset_added" if before is None else "asset_removed"
        elif before["role"] != after["role"]:
            role, reason = SHARED_CORE, "role_changed"
        else:
            role, reason = after["role"], "content_changed"
        entry = after or before
        changed.append({"label": label, "role": role, "reason": reason,
                        "from_sha256": before["sha256"] if before else None,
                        "to_sha256": after["sha256"] if after else None,
                        "scenario_ids": list(entry["scenario_ids"])})
        scopes.append(INVALIDATION_RULES[role]["scope"])
        affected_scenarios.update(entry["scenario_ids"])
    for section in _ENV_SECTIONS:
        if old.environment.get(section) != new.environment.get(section):
            changed.append({"label": "environment." + section, "role": ENVIRONMENT,
                            "reason": "environment_drift", "from_sha256": None, "to_sha256": None,
                            "scenario_ids": []})
            scopes.append(INVALIDATION_RULES[ENVIRONMENT]["scope"])
    worst = "all" if "all" in scopes else "scenario" if "scenario" in scopes else\
        "criteria" if "criteria" in scopes else "none"
    allows = bool(changed) and all(INVALIDATION_RULES[entry["role"]]["re_evaluation_allowed"]
                                   for entry in changed)
    return InvalidationDiff.from_dict({"changed": changed, "affected_scenario_ids": sorted(affected_scenarios),
                                       "worst_scope": worst, "allows_re_evaluation": allows})


def criteria_sha256(fingerprints: FingerprintBundle) -> str | None:
    """Identity of the judgment rules bound into a fingerprint bundle.

    This is the value a re-evaluation record must carry as rules_sha256, both
    at registration (the judgment is derived under exactly these rules) and at
    verification (a RELEASED record may only rest on re-evaluations judged
    under the CURRENT rules). Convention: with a single CRITERION-role asset
    the rules identity is that asset's sha256 (the established registration
    contract); with several criterion assets it is the canonical hash of the
    label->sha256 map. None means the bundle binds no judgment rules, in which
    case no re-evaluation can be registered or verified against it (a
    criterion-only diff is impossible without criterion assets).
    """
    _require(isinstance(fingerprints, FingerprintBundle), "criteria: FingerprintBundle required")
    criteria = {label: entry["sha256"] for label, entry in fingerprints.assets.items()
                if entry["role"] == CRITERION}
    if not criteria:
        return None
    if len(criteria) == 1:
        return next(iter(criteria.values()))
    return c.canonical_sha256(criteria)


# ---------------------------------------------------------------------------
# Expected coverage sets (derived from the compiled execution plan)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PilotSelection:
    """One prescribed full-pilot item, resolved through plan condition IDs."""
    scenario_id: str
    condition_id: str

    def __post_init__(self) -> None:
        _identifier(self.scenario_id, "pilot selection scenario")
        _identifier(self.condition_id, "pilot selection condition")


_ITEM_FIELDS = {"key", "design_version", "scenario_id", "condition_key", "mechanisms",
                "fault_count", "protocol_sha256", "family_ids", "run_contract_sha256"}


@dataclass(frozen=True, init=False, repr=False)
class ExpectedCoverage:
    """Expected smoke set + prescribed full-pilot set, bound to plan and catalog."""
    _json: str

    def __init__(self) -> None:
        raise TypeError("Use derive_expected_sets or ExpectedCoverage.from_dict")

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._json)

    @property
    def smoke(self) -> tuple[dict[str, Any], ...]:
        return tuple(self.to_dict()["smoke"])

    @property
    def pilot(self) -> tuple[dict[str, Any], ...]:
        return tuple(self.to_dict()["pilot"])

    @property
    def coverage_sha256(self) -> str:
        return self.to_dict()["coverage_sha256"]

    @property
    def smoke_keys(self) -> tuple[str, ...]:
        return tuple(item["key"] for item in self.smoke)

    @property
    def pilot_keys(self) -> tuple[str, ...]:
        return tuple(item["key"] for item in self.pilot)

    @property
    def plan_sha256(self) -> str:
        return self.to_dict()["plan_sha256"]

    @property
    def catalog_source(self) -> dict[str, Any]:
        return self.to_dict()["catalog_source"]

    @property
    def design_version(self) -> str:
        return self.to_dict()["design_version"]

    def item_by_key(self) -> dict[str, dict[str, Any]]:
        return {item["key"]: item for item in self.smoke + self.pilot}

    def run_contracts_by_key(self) -> dict[str, set[str]]:
        return {item["key"]: set(item["run_contract_sha256"]) for item in self.smoke}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ExpectedCoverage:
        _require(type(value) is dict and set(value) ==
                 {"schema_version", "plan_id", "plan_sha256", "catalog_source", "design_version",
                  "smoke", "pilot", "coverage_sha256"}, "expected coverage: exact fields required")
        _require(value["schema_version"] == EXPECTED_SCHEMA, "expected coverage: unsupported schema")
        _identifier(value["plan_id"], "expected coverage plan id")
        _sha(value["plan_sha256"], "expected coverage plan sha256")
        _require(type(value["catalog_source"]) is dict and set(value["catalog_source"]) == {"kind", "sha256"},
                 "expected coverage: catalog source required")
        _text(value["catalog_source"]["kind"], "expected coverage catalog kind")
        _sha(value["catalog_source"]["sha256"], "expected coverage catalog sha256")
        _identifier(value["design_version"], "expected coverage design version")
        for section in ("smoke", "pilot"):
            _require(type(value[section]) is list and bool(value[section]),
                     "expected coverage: items required")
            for item in value[section]:
                _require(type(item) is dict and set(item) == _ITEM_FIELDS,
                         "coverage item: exact fields required")
                for field in ("key", "protocol_sha256"):
                    _sha(item[field], "coverage item " + field)
                _sha(item["condition_key"], "coverage item condition_key")
                _require(type(item["mechanisms"]) is list and type(item["run_contract_sha256"]) is list
                         and item["run_contract_sha256"], "coverage item: mechanism/contract lists required")
                for digest in item["run_contract_sha256"]:
                    _sha(digest, "coverage item run contract sha256")
        smoke_keys = [item["key"] for item in value["smoke"]]
        pilot_keys = [item["key"] for item in value["pilot"]]
        _require(len(set(smoke_keys)) == len(smoke_keys) and len(set(pilot_keys)) == len(pilot_keys),
                 "expected coverage: duplicate coverage key")
        _require(set(pilot_keys) <= set(smoke_keys), "expected coverage: pilot items must be smoke items")
        _sha(value["coverage_sha256"], "expected coverage sha256")
        _require(value["coverage_sha256"] == _sealed_hash(value, "coverage_sha256"),
                 "expected coverage: hash does not seal content")
        result = object.__new__(cls)
        object.__setattr__(result, "_json", c.canonical_json(value))
        return result


def _protocol_signature(unit: dict[str, Any]) -> str:
    """Observation-protocol tier: phase lengths/order + metric interval.

    Follows the compiler's normalization whitelist: assigned clock names are
    replaced by the plan-relative name, so replicate units of one condition
    share a signature while different durations/intervals do not.
    """
    contract_value = unit["run_contract"]
    phases = {name: {**window, "clock_id": "plan-relative-clock"}
              for name, window in contract_value["phases"].items()}
    return c.canonical_sha256({"phases": phases, "metric_interval_s": contract_value["metric_interval_s"]})


def derive_expected_sets(plan_manifest: dict[str, Any], catalog_payload: bytes,
                         pilot_selection: tuple[PilotSelection, ...]) -> ExpectedCoverage:
    """Derive expected smoke items and validate the prescribed pilot set.

    The plan must be a rq4-collect/execution-plan-v1 manifest compiled from the
    exact catalog bytes supplied here. The plan scenario set must equal the
    catalog scenario set; any silent scenario deletion/mutation is rejected.
    A revised scenario scope therefore requires a new explicit catalog/plan
    version with recomputed pairing closure, never an edited expected set.
    """
    _require(type(plan_manifest) is dict, "expected coverage: plan manifest required")
    _require(plan_manifest.get("schema_version") == PLAN_SCHEMA, "expected coverage: unsupported plan schema")
    _require(type(pilot_selection) is tuple, "expected coverage: pilot selection tuple required")
    for entry in pilot_selection:
        _require(isinstance(entry, PilotSelection), "expected coverage: PilotSelection entries required")
    registry = c.DesignRegistry.from_json(catalog_payload)
    catalog_source = {"kind": registry.source_kind, "sha256": registry.source_sha256}
    _require(plan_manifest.get("catalog_source") == catalog_source,
             "expected coverage: plan was not compiled from this catalog (kind/sha mismatch)")
    condition_key_by_id = {}
    for source in plan_manifest["condition_sources"]:
        _require(source["condition_id"] not in condition_key_by_id,
                 "expected coverage: duplicate condition source")
        condition_key_by_id[source["condition_id"]] = source["condition_key"]
    units: dict[tuple[str, str], list[dict[str, Any]]] = {}
    plan_scenarios: set[str] = set()
    for unit in plan_manifest["execution_units"]:
        scenario = unit["run_contract"]["scenario"]
        _require(scenario["design_version"] == registry.design_version,
                 "expected coverage: unit design version differs from catalog")
        plan_scenarios.add(scenario["scenario_id"])
        units.setdefault((scenario["scenario_id"], unit["condition_key"]), []).append(unit)
    catalog_scenarios = {scenario.key.scenario_id for scenario in registry.scenarios}
    _require(not (catalog_scenarios - plan_scenarios) and not (plan_scenarios - catalog_scenarios),
             "expected coverage: plan scenario set differs from catalog (silent scenario change forbidden)")
    smoke: dict[str, dict[str, Any]] = {}
    for (scenario_id, condition_key), group in sorted(units.items()):
        representative = group[0]
        contract_value = representative["run_contract"]
        key = c.canonical_sha256({"design_version": registry.design_version,
                                  "scenario_id": scenario_id, "condition_key": condition_key})
        smoke[key] = {"key": key, "design_version": registry.design_version, "scenario_id": scenario_id,
                      "condition_key": condition_key,
                      "mechanisms": sorted({fault["mechanism"] + "/" + fault["mechanism_version"]
                                            for fault in contract_value["faults"]}),
                      "fault_count": len(contract_value["faults"]),
                      "protocol_sha256": _protocol_signature(representative),
                      "family_ids": sorted(representative["family_ids"]),
                      "run_contract_sha256": sorted({u["run_contract_sha256"] for u in group})}
    pilot_keys: list[str] = []
    for entry in pilot_selection:
        condition_key = condition_key_by_id.get(entry.condition_id)
        _require(condition_key is not None, "expected coverage: pilot condition not in plan")
        _require((entry.scenario_id, condition_key) in units,
                 "expected coverage: pilot scenario/condition pair not in plan")
        key = c.canonical_sha256({"design_version": registry.design_version,
                                  "scenario_id": entry.scenario_id, "condition_key": condition_key})
        _require(key not in pilot_keys, "expected coverage: duplicate pilot item")
        pilot_keys.append(key)
    _check_pilot_rules(smoke, pilot_keys)
    payload = {"schema_version": EXPECTED_SCHEMA, "plan_id": plan_manifest["plan_id"],
               "plan_sha256": c.canonical_sha256(plan_manifest), "catalog_source": catalog_source,
               "design_version": registry.design_version,
               "smoke": [smoke[key] for key in sorted(smoke)], "pilot": [smoke[key] for key in pilot_keys]}
    payload["coverage_sha256"] = _sealed_hash(payload, "coverage_sha256")
    return ExpectedCoverage.from_dict(payload)


def _check_pilot_rules(smoke: dict[str, dict[str, Any]], pilot_keys: list[str]) -> None:
    """Enforce the explicit minimum machine rules for the prescribed pilot set."""
    pilot = [smoke[key] for key in pilot_keys]
    _require({m for item in smoke.values() for m in item["mechanisms"]}
             <= {m for item in pilot for m in item["mechanisms"]},
             "pilot coverage rule mechanism_coverage: every plan mechanism needs a full pilot item")
    _require({item["protocol_sha256"] for item in smoke.values()}
             <= {item["protocol_sha256"] for item in pilot},
             "pilot coverage rule observation_protocol_coverage: every timing/interval tier needs a pilot item")
    _require({item["fault_count"] for item in smoke.values()}
             <= {item["fault_count"] for item in pilot},
             "pilot coverage rule fault_count_tier_coverage: every single/dual/triple tier needs a pilot item")
    
    # the pilot set must embed at least one COMPLETE control family - every arm
    
    
    # items carrying that family id, so completeness cannot shrink to a
    
    
    pilot_key_set = set(pilot_keys)
    complete_family = False
    for family in {family for item in smoke.values() for family in item["family_ids"]}:
        arms = [item for item in smoke.values() if family in item["family_ids"]]
        if (all(item["key"] in pilot_key_set for item in arms)
                and any(item["fault_count"] == 1 for item in arms)
                and any(item["fault_count"] >= 2 for item in arms)):
            complete_family = True
            break
    _require(complete_family,
             "pilot coverage rule family_combo_coverage: representative coverage "
             "needs at least one complete control family (all arms of one family: "
             "its single-root arms plus its combo arm)")


# ---------------------------------------------------------------------------
# Evidence ledger
# ---------------------------------------------------------------------------

_EVIDENCE_FIELDS = {"schema_version", "evidence_id", "kind", "mode", "status", "coverage_keys",
                    "run_contract_sha256", "attempt", "quality_report", "fingerprint_bundle_sha256",
                    "plan_sha256", "catalog_source", "rules_sha256", "original_evidence_id",
                    "independent_confirmation", "recorded_at_utc", "record_sha256"}
_ATTEMPT_FIELDS = {"purpose", "run_id", "attempt_id", "evidence_root"}


class EvidenceLedger:
    """Append-only evidence registry bound to one ExpectedCoverage definition."""

    def __init__(self, expected: ExpectedCoverage) -> None:
        _require(isinstance(expected, ExpectedCoverage), "ledger: ExpectedCoverage required")
        self.expected = expected
        self._records: dict[str, dict[str, Any]] = {}
        self._bundles: dict[str, FingerprintBundle] = {}
        self._events: list[dict[str, Any]] = []

    @property
    def records(self) -> tuple[dict[str, Any], ...]:
        return tuple(self._records.values())

    @property
    def invalidation_events(self) -> tuple[dict[str, Any], ...]:
        return tuple(self._events)

    def bundle(self, bundle_sha256: str) -> FingerprintBundle | None:
        return self._bundles.get(bundle_sha256)

    def record(self, evidence_id: str) -> dict[str, Any] | None:
        return self._records.get(evidence_id)

    def register(self, record: dict[str, Any], fingerprints: FingerprintBundle,
                 *, attempt_root: Path | None = None) -> str:
        """Validate and append one evidence record; returns its evidence_id.

        For PASS (and every re-evaluation) the quality report is re-read from
        the attempt directory and its hash verified, so a registered PASS is
        always bound to intact on-disk bytes, never to a bare claim.
        """
        _require(isinstance(fingerprints, FingerprintBundle), "register: FingerprintBundle required")
        _require(type(record) is dict and set(record) == _EVIDENCE_FIELDS,
                 "evidence record: exact fields required")
        _require(record["schema_version"] == EVIDENCE_SCHEMA, "evidence record: unsupported schema")
        _identifier(record["evidence_id"], "evidence_id")
        _require(record["evidence_id"] not in self._records, "evidence record: duplicate evidence ID")
        _require(record["kind"] in _EVIDENCE_KINDS, "evidence record: kind must be smoke or pilot")
        _require(record["mode"] in _EVIDENCE_MODES, "evidence record: unknown mode")
        _require(record["status"] in _EVIDENCE_STATUSES, "evidence record: unknown status")
        _timestamp(record["recorded_at_utc"], "evidence record.recorded_at_utc")
        _sha(record["record_sha256"], "evidence record sha256")
        _require(record["record_sha256"] == _sealed_hash(record, "record_sha256"),
                 "evidence record: hash does not seal content")
        _sha(record["fingerprint_bundle_sha256"], "evidence fingerprint sha256")
        _require(record["fingerprint_bundle_sha256"] == fingerprints.bundle_sha256,
                 "evidence record: bound fingerprint bundle mismatch")
        _require(self.expected.plan_sha256 == record["plan_sha256"],
                 "evidence record: bound execution plan mismatch")
        _require(record["catalog_source"] == self.expected.catalog_source,
                 "evidence record: bound catalog mismatch")
        keys = record["coverage_keys"]
        _require(type(keys) is list and bool(keys) and len(set(keys)) == len(keys),
                 "evidence record: unique coverage keys required")
        expected_keys = set(self.expected.smoke_keys if record["kind"] == "smoke" else self.expected.pilot_keys)
        _require(set(keys) <= expected_keys, "evidence record: coverage key outside expected set")
        contracts = self.expected.run_contracts_by_key()
        if record["status"] == "NOT_RUN":
            _require(record["run_contract_sha256"] is None, "evidence record: NOT_RUN has no run contract")
        elif record["mode"] == "direct":
            # One attempt executes exactly one scenario/condition tier.
            _require(len(keys) == 1, "evidence record: direct evidence covers exactly one coverage item")
            _sha(record["run_contract_sha256"], "evidence run contract sha256")
            _require(all(record["run_contract_sha256"] in contracts[key] for key in keys),
                     "evidence record: run contract does not belong to the covered coverage items")
        if record["attempt"] is not None:
            self._check_attempt_shape(record["attempt"])
            _require(record["attempt"]["purpose"] == record["kind"],
                     "evidence record: attempt purpose differs from evidence kind")
        if record["status"] == "PASS" and record["mode"] == "direct":
            self._verify_attempt_and_quality(record, attempt_root)
        elif record["status"] != "PASS":
            _require(record["quality_report"] is None or self._valid_quality_reference(record),
                     "evidence record: malformed quality report reference")
        if record["mode"] == "re_evaluation":
            _require(record["status"] == "PASS", "re-evaluation: only a PASS judgment may be re-derived")
            original = self._records.get(_text(record["original_evidence_id"], "evidence original id"))
            _require(original is not None, "re-evaluation: original evidence must already be registered")
            _require(original["status"] == "PASS" and original["mode"] == "direct",
                     "re-evaluation: original must be a direct PASS record")
            _require(original["kind"] == record["kind"],
                     "re-evaluation: must re-judge the same coverage class as the original")
            _require(set(keys) <= set(original["coverage_keys"]),
                     "re-evaluation: cannot cover items beyond the original raw evidence (no backfilling)")
            _require(record["attempt"] == original["attempt"]
                     and record["run_contract_sha256"] == original["run_contract_sha256"],
                     "re-evaluation: must reuse the original attempt identity (no fabricated run)")
            _require(record["independent_confirmation"] is True,
                     "re-evaluation: independent confirmation is mandatory")
            _sha(record["rules_sha256"], "re-evaluation rules sha256")
            # F3: the re-derived judgment must be bound to the criteria it was
            # actually judged under; after a further criteria change the new
            # judgment must be re-registered under the new rules hash.
            current_rules = criteria_sha256(fingerprints)
            _require(current_rules is not None and record["rules_sha256"] == current_rules,
                     "re-evaluation: rules_sha256 must equal the current criteria hash "
                     "(re-register under the new rules after a criteria change)")
            original_bundle = self._bundles.get(original["fingerprint_bundle_sha256"])
            _require(original_bundle is not None, "re-evaluation: original fingerprint bundle unavailable")
            diff = diff_fingerprints(original_bundle, fingerprints)
            _require(bool(diff.changed) and diff.allows_re_evaluation,
                     "re-evaluation: only pure-criterion changes qualify; field behaviour changed")
            self._verify_attempt_and_quality(record, attempt_root, original=original)
        else:
            _require(record["original_evidence_id"] is None and record["rules_sha256"] is None,
                     "evidence record: original/rules fields are re-evaluation only")
            _require(record["independent_confirmation"] is None or type(record["independent_confirmation"]) is bool,
                     "evidence record: independent_confirmation must be boolean or null")
        self._records[record["evidence_id"]] = json.loads(c.canonical_json(record))
        self._bundles.setdefault(fingerprints.bundle_sha256, fingerprints)
        return record["evidence_id"]

    def _check_attempt_shape(self, attempt: Any) -> None:
        _require(type(attempt) is dict and set(attempt) == _ATTEMPT_FIELDS,
                 "evidence attempt: exact fields required")
        _require(attempt["purpose"] in _EVIDENCE_KINDS, "evidence attempt: purpose must be smoke or pilot")
        for field in ("run_id", "attempt_id"):
            _identifier(attempt[field], "evidence attempt." + field)
        root = Path(_text(attempt["evidence_root"], "evidence attempt.evidence_root"))
        _require(root.is_absolute() and ".." not in root.parts,
                 "evidence attempt: absolute safe path required")

    def _valid_quality_reference(self, record: dict[str, Any]) -> bool:
        value = record["quality_report"]
        if record["mode"] == "re_evaluation":
            expected_path = "artifacts/quality/reeval-" + record["rules_sha256"][:12] + ".json"
        else:
            expected_path = QUALITY_REPORT_RELATIVE_PATH
        return (type(value) is dict and set(value) == {"relative_path", "sha256"}
                and value["relative_path"] == expected_path
                and _SHA256.fullmatch(value.get("sha256") or "") is not None)

    def _verify_attempt_and_quality(self, record: dict[str, Any], attempt_root: Path | None,
                                    *, original: dict[str, Any] | None = None) -> None:
        _require(record["attempt"] is not None, "evidence record: PASS requires an attempt reference")
        _require(self._valid_quality_reference(record),
                 "evidence record: quality report reference required")
        _require(isinstance(attempt_root, Path) and attempt_root.is_absolute(),
                 "evidence loader: absolute attempt root required")
        _require(attempt_root.resolve() == Path(record["attempt"]["evidence_root"]).resolve(),
                 "evidence loader: attempt root differs from the record's evidence root")
        path = attempt_root / record["quality_report"]["relative_path"]
        _require(path.is_file() and path.resolve().is_relative_to(attempt_root.resolve()),
                 "evidence loader: quality report missing or outside the attempt")
        raw = path.read_bytes()
        _require(hashlib.sha256(raw).hexdigest() == record["quality_report"]["sha256"],
                 "evidence loader: quality report bytes do not match the recorded hash")
        report = json.loads(raw.decode("utf-8"))
        _require(type(report) is dict, "evidence loader: quality report must be an object")
        _require(report.get("schema_version") == QUALITY_REPORT_SCHEMA,
                 "evidence loader: unsupported quality report schema")
        _require(type(report.get("groups")) is dict and set(report["groups"]) == set(_QUALITY_GROUPS),
                 "evidence loader: quality groups missing")
        _require(all(type(group) is dict and group.get("status") == "PASS"
                     for group in report["groups"].values()),
                 "evidence loader: quality report is not all-PASS; cannot register as PASS evidence")
        _require(report.get("sample_eligibility") == "ELIGIBLE_CANDIDATE",
                 "evidence loader: quality report is not an eligible candidate")
        _require(report.get("sample_purpose") == record["kind"],
                 "evidence loader: quality report purpose differs from the evidence kind")
        if original is not None:
            # The retained raw evidence must still be exactly what the original
            # PASS was registered against; changed bytes mean re-evaluation
            # would judge data that no longer exists (no backfilling).
            original_path = attempt_root / original["quality_report"]["relative_path"]
            _require(original_path.is_file() and original_path.resolve().is_relative_to(attempt_root.resolve()),
                     "re-evaluation: original quality report missing from the attempt")
            original_raw = original_path.read_bytes()
            _require(hashlib.sha256(original_raw).hexdigest() == original["quality_report"]["sha256"],
                     "re-evaluation: original attempt evidence changed on disk (raw evidence not intact)")
            original_report = json.loads(original_raw.decode("utf-8"))
            _require(type(original_report) is dict
                     and original_report.get("schema_version") == QUALITY_REPORT_SCHEMA,
                     "re-evaluation: original quality report schema unusable")

    def register_invalidation_event(self, event: dict[str, Any]) -> None:
        """Append one explicit invalidation/rerun/re-evaluation history event."""
        _require(type(event) is dict and set(event) ==
                 {"schema_version", "event_id", "occurred_at_utc", "reason", "from_bundle_sha256",
                  "to_bundle_sha256", "action", "event_sha256"},
                 "invalidation event: exact fields required")
        _require(event["schema_version"] == INVALIDATION_EVENT_SCHEMA,
                 "invalidation event: unsupported schema")
        _identifier(event["event_id"], "invalidation event id")
        _timestamp(event["occurred_at_utc"], "invalidation event.occurred_at_utc")
        _text(event["reason"], "invalidation event.reason")
        _sha(event["from_bundle_sha256"], "invalidation event from sha256")
        _sha(event["to_bundle_sha256"], "invalidation event to sha256")
        _require(type(event["action"]) is dict and set(event["action"]) == {"kind", "details"}
                 and event["action"]["kind"] in {"rerun_smoke", "rerun_full_pilot", "re_evaluation",
                                                 "rederive_expected_sets"}
                 and type(event["action"]["details"]) is dict,
                 "invalidation event: explicit action required")
        _sha(event["event_sha256"], "invalidation event sha256")
        _require(event["event_sha256"] == _sealed_hash(event, "event_sha256"),
                 "invalidation event: hash does not seal content")
        self._events.append(json.loads(c.canonical_json(event)))


# ---------------------------------------------------------------------------
# Validity computation and release record
# ---------------------------------------------------------------------------

def evidence_validity(record: dict[str, Any], record_bundle: FingerprintBundle | None,
                      current: FingerprintBundle,
                      items: dict[str, dict[str, Any]] | None = None) -> dict[str, str]:
    """Per-coverage-key validity of one record under the CURRENT bundle.

    Only PASS records can be valid. A record bound to the current bundle is
    valid everywhere it covers. Otherwise the rule table decides: shared-core,
    document or environment drift invalidates everything the record covered;
    scenario-local changes invalidate only that scenario's items; criterion-only
    changes invalidate direct records (re-evaluation records stay valid ONLY
    when judged under the current criteria hash - a chained criterion change
    v2->v3 stales every v2-bound re-evaluation, which must be re-registered).
    `items` (key -> coverage item) is required whenever the diff is not
    bundle-equal, to resolve each key's scenario binding.
    """
    _require(record["status"] in _EVIDENCE_STATUSES, "validity: unknown status")
    if record["status"] != "PASS":
        return {key: "status_" + record["status"].lower() + "_not_pass" for key in record["coverage_keys"]}
    if record_bundle is None:
        return {key: "bound_fingerprint_bundle_unavailable" for key in record["coverage_keys"]}
    if record_bundle.bundle_sha256 == current.bundle_sha256:
        # defense in depth: registration already binds rules_sha256 to the
        # bundle's criteria; a mismatch here means a hand-crafted record.
        if record.get("mode") != "re_evaluation" or record.get("rules_sha256") == criteria_sha256(current):
            return {key: "valid_current_fingerprint" for key in record["coverage_keys"]}
        return {key: "criteria_rules_changed_re_registration_required" for key in record["coverage_keys"]}
    diff = diff_fingerprints(record_bundle, current)
    if diff.empty:
        return {key: "valid_current_fingerprint" for key in record["coverage_keys"]}
    if diff.allows_re_evaluation:
        if record["mode"] == "re_evaluation":
            # F3: the re-derived judgment counts only under the rules it was
            # re-registered for; older rules hashes require re-registration.
            if record.get("rules_sha256") == criteria_sha256(current):
                return {key: "valid_re_evaluation_under_new_criteria" for key in record["coverage_keys"]}
            return {key: "criteria_rules_changed_re_registration_required" for key in record["coverage_keys"]}
        return {key: "criteria_changed_re_evaluation_required" for key in record["coverage_keys"]}
    _require(items is not None, "validity: coverage items required for scoped invalidation")
    affected = set(diff.to_dict()["affected_scenario_ids"])
    worst = diff.to_dict()["worst_scope"]
    result: dict[str, str] = {}
    for key in record["coverage_keys"]:
        item = items.get(key)
        _require(item is not None, "validity: coverage key outside expected items")
        if worst == "all":
            result[key] = "invalidated_by_" + worst + "_scope_change"
        elif item["scenario_id"] in affected:
            result[key] = "invalidated_by_scenario_local_change"
        else:
            result[key] = "valid_unchanged_scenario_scope"
    return result


def evaluate_release(ledger: EvidenceLedger, fingerprints: FingerprintBundle, *,
                     review: dict[str, Any]) -> dict[str, Any]:
    """Compute the machine release record: RELEASED only with full, currently
    valid PASS coverage, closed failures and an approved independent review."""
    _require(isinstance(ledger, EvidenceLedger) and isinstance(fingerprints, FingerprintBundle),
             "release evaluation: ledger and FingerprintBundle required")
    expected = ledger.expected
    _require(type(review) is dict and set(review) == {"reviewer", "reviewed_at_utc", "conclusion"},
             "release evaluation: review envelope required")
    _text(review["reviewer"], "release review.reviewer")
    _timestamp(review["reviewed_at_utc"], "release review.reviewed_at_utc")
    _require(review["conclusion"] in {"APPROVED", "REJECTED"}, "release review: explicit conclusion required")
    reasons: list[str] = []
    catalog_bound = fingerprints.assets.get("doc.design-catalog")
    if catalog_bound is None:
        reasons.append("catalog_not_bound_in_fingerprints")
    elif catalog_bound["sha256"] != expected.catalog_source["sha256"]:
        reasons.append("catalog_fingerprint_mismatch")
    if "doc.acceptance-standard" not in fingerprints.assets:
        reasons.append("acceptance_standard_not_bound_in_fingerprints")
    smoke_scenarios = {item["scenario_id"] for item in expected.smoke}
    for label, entry in sorted(fingerprints.assets.items()):
        if entry["role"] == SCENARIO_LOCAL and set(entry["scenario_ids"]) - smoke_scenarios:
            reasons.append("asset_scope_unknown_scenario:" + label)

    items = expected.item_by_key()
    per_key: dict[str, list[tuple[Any, dict[str, Any], dict[str, str]]]] = {}
    for record in ledger.records:
        bundle = ledger.bundle(record["fingerprint_bundle_sha256"])
        validity = evidence_validity(record, bundle, fingerprints, items)
        for key in record["coverage_keys"]:
            per_key.setdefault(key, []).append(
                (_timestamp(record["recorded_at_utc"], "record time"), record, validity))
    valid_smoke: dict[str, dict[str, Any]] = {}
    valid_pilot: dict[str, dict[str, Any]] = {}
    closures: dict[str, dict[str, Any]] = {}
    unclosed: list[str] = []
    for kind, target in (("smoke", valid_smoke), ("pilot", valid_pilot)):
        for key in getattr(expected, kind + "_keys"):
            events = sorted(per_key.get(key, []), key=lambda item: item[0])
            failures = [item for item in events if item[1]["status"] in _FAILING and item[1]["kind"] == kind]
            passes = [item for item in events if item[1]["status"] == "PASS" and item[1]["kind"] == kind
                      and item[2].get(key, "").startswith("valid_")]
            if failures:
                latest_failure = failures[-1]
                closing = [item for item in passes if item[0] >= latest_failure[0]]
                closures[key] = {"latest_failure": {"evidence_id": latest_failure[1]["evidence_id"],
                                                    "status": latest_failure[1]["status"],
                                                    "recorded_at_utc": latest_failure[1]["recorded_at_utc"]},
                                 "closed_by": closing[-1][1]["evidence_id"] if closing else None}
                if not closing:
                    unclosed.append(key)
            if passes:
                latest = passes[-1][1]
                # rules_sha256 is embedded so verify_release can check the
                # re-evaluation rules binding against the CURRENT criteria
                # (None for direct evidence, which carries no re-judgment).
                target[key] = {"evidence_id": latest["evidence_id"], "kind": kind, "mode": latest["mode"],
                               "rules_sha256": latest.get("rules_sha256"),
                               "recorded_at_utc": latest["recorded_at_utc"]}
    missing_smoke = [key for key in expected.smoke_keys if key not in valid_smoke]
    missing_pilot = [key for key in expected.pilot_keys if key not in valid_pilot]
    if missing_smoke:
        reasons.append("smoke_coverage_incomplete:" + ",".join(
            items[key]["scenario_id"] + "/" + key[:12] for key in missing_smoke))
    if missing_pilot:
        reasons.append("full_pilot_coverage_incomplete:" + ",".join(
            items[key]["scenario_id"] + "/" + key[:12] for key in missing_pilot))
    if unclosed:
        reasons.append("unclosed_failures:" + ",".join(sorted(unclosed)))
    if review["conclusion"] != "APPROVED":
        reasons.append("independent_review_not_approved")
    decision = "RELEASED" if not reasons else "BLOCKED"
    payload = {"schema_version": RELEASE_RECORD_SCHEMA,
               "created_at_utc": fingerprints.collected_at_utc,
               "fingerprint_bundle_sha256": fingerprints.bundle_sha256,
               "fingerprints": fingerprints.to_dict(),
               "expected": expected.to_dict(),
               "valid_smoke": {key: valid_smoke[key] for key in sorted(valid_smoke)},
               "valid_pilot": {key: valid_pilot[key] for key in sorted(valid_pilot)},
               "missing": {"smoke": missing_smoke, "full_pilot": missing_pilot},
               "failure_closures": {key: closures[key] for key in sorted(closures)},
               "unclosed_failures": sorted(unclosed),
               "invalidation_history": [dict(event) for event in ledger.invalidation_events],
               "review": json.loads(c.canonical_json(review)),
               "decision": decision,
               "reasons": reasons}
    payload["record_sha256"] = _sealed_hash(payload, "record_sha256")
    return payload


_VALID_ENTRY_FIELDS = {"evidence_id", "kind", "mode", "rules_sha256", "recorded_at_utc"}
_RELEASE_REVIEW_FIELDS = {"reviewer", "reviewed_at_utc", "conclusion"}
_CLOSURE_FIELDS = {"latest_failure", "closed_by"}
_CLOSURE_FAILURE_FIELDS = {"evidence_id", "status", "recorded_at_utc"}


def _interior_timestamp(value: Any) -> Any:
    """Parse a timestamp inside a forged record; None when unusable (never raises)."""
    try:
        return _timestamp(value, "interior timestamp")
    except GateError:
        return None


def _embedded_expected_keys(stored: dict[str, Any]) -> dict[str, list[str]] | None:
    """Parse the embedded expected-set key lists; None when malformed."""
    keys: dict[str, list[str]] = {}
    for section in ("smoke", "pilot"):
        items = stored.get(section)
        if (type(items) is not list
                or any(type(item) is not dict or not isinstance(item.get("key"), str) for item in items)):
            return None
        keys[section] = [item["key"] for item in items]
    return keys


def _release_interior_reasons(record: dict[str, Any]) -> tuple[list[str], dict[str, list[str]] | None]:
    """F2: re-derive every structural relation the registrar guarantees.

    A record whose content contradicts its own claimed decision (non-empty
    missing/unclosed/reasons under RELEASED, valid maps outside or
    inconsistent with the embedded expected sets, embedded bundle/expected
    summaries that do not seal or bind, malformed valid entries, unsealed
    invalidation events, closure entries that are malformed or whose
    closed_by reference/timing contradicts the record's own valid entries)
    is rejected even when record_sha256 was recomputed after the edit.
    Returns the interior reasons plus the embedded expected key lists (None
    when the embedded expected sets are malformed).
    """
    reasons: list[str] = []
    if record.get("decision") not in ("RELEASED", "BLOCKED"):
        reasons.append("record_interior_inconsistent")
    embedded_keys: dict[str, list[str]] | None = None
    stored = record.get("expected")
    if type(stored) is dict:
        embedded_keys = _embedded_expected_keys(stored)
        coverage = stored.get("coverage_sha256")
        if not (isinstance(coverage, str) and _SHA256.fullmatch(coverage)
                and coverage == _sealed_hash(stored, "coverage_sha256")):
            reasons.append("embedded_expected_not_self_sealed")
    else:
        reasons.append("record_interior_inconsistent")
    embedded_fp = record.get("fingerprints")
    if type(embedded_fp) is not dict:
        reasons.append("record_interior_inconsistent")
    else:
        sealed = embedded_fp.get("bundle_sha256")
        if not (isinstance(sealed, str) and _SHA256.fullmatch(sealed))\
                or sealed != record.get("fingerprint_bundle_sha256"):
            reasons.append("embedded_fingerprint_mismatch")
        elif sealed != _sealed_hash(embedded_fp, "bundle_sha256"):
            reasons.append("embedded_fingerprint_not_self_sealed")
        if record.get("created_at_utc") != embedded_fp.get("collected_at_utc"):
            reasons.append("record_creation_time_inconsistent")
    listed_reasons = record.get("reasons")
    if type(listed_reasons) is not list or any(not isinstance(r, str) for r in listed_reasons):
        reasons.append("record_interior_inconsistent")
    review = record.get("review")
    if type(review) is not dict or set(review) != _RELEASE_REVIEW_FIELDS\
            or review.get("conclusion") not in ("APPROVED", "REJECTED"):
        reasons.append("record_interior_inconsistent")
    missing = record.get("missing")
    missing_shape = (type(missing) is dict and set(missing) == {"smoke", "full_pilot"}
                     and all(type(missing[field]) is list for field in ("smoke", "full_pilot")))
    if not missing_shape:
        reasons.append("record_interior_inconsistent")
    for section, valid_field, missing_field in (("smoke", "valid_smoke", "smoke"),
                                                ("pilot", "valid_pilot", "full_pilot")):
        valid = record.get(valid_field)
        if type(valid) is not dict:
            reasons.append("record_interior_inconsistent")
            continue
        for entry in valid.values():
            if (type(entry) is not dict or set(entry) != _VALID_ENTRY_FIELDS
                    or entry.get("kind") != section or entry.get("mode") not in _EVIDENCE_MODES
                    or not isinstance(entry.get("evidence_id"), str) or not entry["evidence_id"]
                    or (entry.get("mode") == "direct") != (entry.get("rules_sha256") is None)):
                reasons.append("valid_entry_inconsistent")
                break
        if embedded_keys is not None:
            section_keys = set(embedded_keys[section])
            if not set(valid) <= section_keys:
                reasons.append("valid_" + section + "_keys_outside_expected")
            if missing_shape and set(missing[missing_field]) != section_keys - set(valid):
                reasons.append("missing_" + missing_field + "_inconsistent")
    closures = record.get("failure_closures")
    unclosed = record.get("unclosed_failures")
    valid_sections = tuple(record.get(section) for section in ("valid_smoke", "valid_pilot"))
    if (type(unclosed) is not list or any(not isinstance(key, str) for key in unclosed)
            or type(closures) is not dict or any(not isinstance(key, str) for key in closures)):
        reasons.append("record_interior_inconsistent")
        closures = None
    if closures is not None:
        derived = sorted(key for key, entry in closures.items()
                         if type(entry) is dict and entry.get("closed_by") is None)
        if sorted(unclosed) != derived:
            reasons.append("unclosed_failures_inconsistent")
        for key, entry in closures.items():
            failure = entry.get("latest_failure") if type(entry) is dict else None
            closed_by = entry.get("closed_by") if type(entry) is dict else None
            failure_time = _interior_timestamp(
                failure.get("recorded_at_utc")) if type(failure) is dict else None
            if (type(entry) is not dict or set(entry) != _CLOSURE_FIELDS
                    or type(failure) is not dict or set(failure) != _CLOSURE_FAILURE_FIELDS
                    or not isinstance(failure.get("evidence_id"), str) or not failure["evidence_id"]
                    or failure.get("status") not in _FAILING or failure_time is None
                    or not (closed_by is None or (isinstance(closed_by, str) and bool(closed_by)))):
                reasons.append("closure_entry_inconsistent")
                continue
            if closed_by is None:
                continue
            # (a) reference: closed_by must be the evidence the record itself
            # presents as the current valid PASS for that key; (b) timing:
            # that PASS must not predate the failure it claims to close. The
            # closure entry carries no kind, so the claim may resolve against
            # the key's valid entry in either coverage class.
            candidates = [valid for valid in valid_sections
                          if type(valid) is dict and type(valid.get(key)) is dict
                          and valid[key].get("evidence_id") == closed_by]
            if not candidates:
                reasons.append("closure_reference_inconsistent")
                continue
            ordered = False
            for valid in candidates:
                pass_time = _interior_timestamp(valid[key].get("recorded_at_utc"))
                if pass_time is not None and pass_time >= failure_time:
                    ordered = True
                    break
            if not ordered:
                reasons.append("closure_timing_inconsistent")
    history = record.get("invalidation_history")
    if type(history) is not list:
        reasons.append("record_interior_inconsistent")
    else:
        for event in history:
            if (type(event) is not dict or type(event.get("action")) is not dict
                    or not isinstance(event.get("event_sha256"), str)
                    or event["event_sha256"] != _sealed_hash(event, "event_sha256")):
                reasons.append("invalidation_event_not_self_sealed")
                break
    if record.get("decision") == "RELEASED":
        if listed_reasons != []:
            reasons.append("released_record_reasons_not_empty")
        if record.get("missing") != {"smoke": [], "full_pilot": []}:
            reasons.append("released_record_missing_not_empty")
        if record.get("unclosed_failures") != []:
            reasons.append("released_record_unclosed_failures_not_empty")
        if type(review) is not dict or review.get("conclusion") != "APPROVED":
            reasons.append("released_record_review_not_approved")
    return reasons, embedded_keys


def verify_release(record: dict[str, Any], fingerprints: FingerprintBundle,
                   expected: ExpectedCoverage) -> dict[str, Any]:
    """R formal-entry check: a RELEASED record must be intact, current and complete.

    This is the integration contract consumed by the R runner (purpose=formal):
    before any formal attempt starts or resumes, R must hold a record whose
    self-hash, fingerprint bundle and expected sets all match the current
    state, whose content is self-consistent (F2: a record rehashed after
    editing is still rejected when its content contradicts its own claimed
    decision, its embedded summaries do not seal, or its closure entries
    contradict the record's own valid entries by reference or timing) and
    whose re-evaluation entries are bound to the criteria hash current AT
    VERIFY TIME (F3: after a criteria change the release must be re-registered
    under the new rules hash, never silently carried over). There is no
    parameter that can bypass a failed verification.
    """
    _require(isinstance(fingerprints, FingerprintBundle) and isinstance(expected, ExpectedCoverage),
             "verify: FingerprintBundle and ExpectedCoverage required")
    _require(type(record) is dict and set(record) ==
             {"schema_version", "created_at_utc", "fingerprint_bundle_sha256", "fingerprints", "expected",
              "valid_smoke", "valid_pilot", "missing", "failure_closures", "unclosed_failures",
              "invalidation_history", "review", "decision", "reasons", "record_sha256"},
             "verify: release record fields required")
    if record.get("schema_version") != RELEASE_RECORD_SCHEMA:
        return {"ok": False, "reasons": ["unsupported_record_schema"], "decision": record.get("decision")}
    if record.get("record_sha256") != _sealed_hash(record, "record_sha256"):
        return {"ok": False, "reasons": ["record_hash_mismatch_tampered"], "decision": record.get("decision")}
    interior, embedded_keys = _release_interior_reasons(record)
    reasons = list(interior)
    if record.get("decision") != "RELEASED":
        listed = record.get("reasons")
        detail = ",".join(listed) if type(listed) is list\
            and all(isinstance(r, str) for r in listed) else "unavailable"
        reasons.append("record_not_released:" + detail)
    if record.get("fingerprint_bundle_sha256") != fingerprints.bundle_sha256:
        reasons.append("fingerprint_drift_record_is_stale")
    elif record.get("fingerprints") != fingerprints.to_dict():
        # not stale: the embedded bundle must be exactly the current bundle
        reasons.append("embedded_fingerprint_mismatch")
    stored = record.get("expected")
    if type(stored) is dict:
        if stored.get("coverage_sha256") != expected.coverage_sha256:
            reasons.append("expected_sets_changed_record_is_stale")
        else:
            if stored != expected.to_dict():
                reasons.append("embedded_expected_mismatch")
            if embedded_keys is not None:
                if set(embedded_keys["smoke"]) != set(expected.smoke_keys):
                    reasons.append("smoke_expected_set_mismatch")
                if set(embedded_keys["pilot"]) != set(expected.pilot_keys):
                    reasons.append("pilot_expected_set_mismatch")
        if stored.get("catalog_source") != expected.catalog_source:
            reasons.append("catalog_binding_mismatch")
    if record.get("decision") == "RELEASED":
        # F3: a RELEASED record may only rest on re-evaluations judged under
        # the rules that are current at verify time; anything else must be
        # re-registered under the new criteria hash.
        current_rules = criteria_sha256(fingerprints)
        stale_rules = any(
            type(entry) is dict and entry.get("mode") == "re_evaluation"
            and entry.get("rules_sha256") != current_rules
            for section in ("valid_smoke", "valid_pilot")
            if type(record.get(section)) is dict
            for entry in record[section].values())
        if stale_rules:
            reasons.append("re_evaluation_rules_stale_re_registration_required")
    reasons = list(dict.fromkeys(reasons))
    return {"ok": not reasons, "reasons": reasons, "decision": record.get("decision")}


# ---------------------------------------------------------------------------

# -> FIRST_CANDIDATE -> CONDITION_VERIFIED -> BLOCK_EXPANSION
# ---------------------------------------------------------------------------

FAST16_STAGE_RECORD_SCHEMA = "rq4-collect/fast16-stage-record-v1"
FAST16_STATE_SCHEMA = "rq4-collect/fast16-state-v1"
FAST16_LEDGER_SCHEMA = "rq4-collect/fast16-ledger-v1"
TECHNICAL_ASSESSMENT_SCHEMA = "rq4-collect/technical-assessment-r10-v1"
P0_ASSESSMENT_SCHEMA = "rq4-collect/p0-assessment-m1p0-v1"

FAST16_STAGES = ("OFFLINE_COMPLETE", "REPRESENTATIVE_VERIFIED", "FIRST_CANDIDATE",
                 "CONDITION_VERIFIED", "BLOCK_EXPANSION")
FAST16_OFFLINE_DIMENSIONS = ("entities", "parameters", "timing", "pairing",
                             "recovery", "output", "budget")
FAST16_KIND_STAGE = {"offline_check": "OFFLINE_COMPLETE",
                     "representative": "REPRESENTATIVE_VERIFIED",
                     "first_candidate": "FIRST_CANDIDATE",
                     "condition_verified": "CONDITION_VERIFIED",
                     "repetition": "CONDITION_VERIFIED",
                     "block_expansion": "BLOCK_EXPANSION"}
FAST16_PURPOSE = {"representative": "pilot", "first_candidate": "formal",
                  "condition_verified": "formal", "repetition": "formal"}

_FAST16_STATUSES = ("PASS", "FAIL")
_FAST16_FIELDS = {"schema_version", "record_id", "kind", "stage", "status", "coverage_keys",
                  "run_contract_sha256", "attempt", "quality_report", "checklist",
                  "fingerprint_bundle_sha256", "plan_sha256", "catalog_source",
                  "recorded_at_utc", "record_sha256"}
_STATE_FIELDS = {"schema_version", "computed_at_utc", "fingerprint_bundle_sha256", "plan_sha256",
                 "catalog_source", "design_version", "coverage_sha256", "stages",
                 "ladder_position", "record_sha256"}


def _check_offline_checklist(checklist: Any, expected: ExpectedCoverage) -> None:
    """The offline record must carry the FULL expected condition checklist.

    Every scenario/condition of the compiled plan appears with a verdict for
    every FAST16 offline dimension; unresolved/missing/contradictory items are
    explicit BLOCKED verdicts with a reason, never silently absent (FAST16
    policy clause 1: 不能边开采边隐藏未完成设计).

    Verdict semantics (three-state since collection; root authorization = user
    instruction 6 "孤立单根按不适用处理", semantic precision only):
    - PASS: the dimension is verified; carries no excuse text.
    - BLOCKED: a real blocking gap; requires an explicit reason; blocks the
      stage completion below.
    - NOT_APPLICABLE: the dimension has no denominator for THIS condition
      (e.g. an isolated single root has no pairing partner); requires an
      explicit reason naming the inapplicability; does NOT block stage
      completion and is NOT a PASS - it must never be read downstream as
      satisfying the dimension or as eligibility.

    Abuse surface (防护面): for the pairing dimension the plan's own
    family_ids decide applicability, so the shape layer refuses pairing=NA
    on any condition that belongs to a contrast family (it has a pairing
    denominator). The other six dimensions carry no denominator test inside
    the gate's frozen contract inputs - their NA admissibility is a
    registration-review surface, never a gate decision.
    """
    _require(type(checklist) is dict and set(checklist) == set(expected.smoke_keys),
             "fast16 offline record: full smoke checklist required (missing conditions cannot be hidden)")
    items = expected.item_by_key()
    for key, entry in checklist.items():
        _require(type(entry) is dict
                 and (set(entry) == set(FAST16_OFFLINE_DIMENSIONS)
                      or set(entry) == set(FAST16_OFFLINE_DIMENSIONS) | {"admission_hold"}
                      or set(entry) == set(FAST16_OFFLINE_DIMENSIONS) | {"risk_binding"}),
                 "fast16 offline record: checklist entry must cover every offline dimension")
        for dimension, row in entry.items():
            if dimension == "admission_hold":
                
                # REGISTERED-pending (machine-evidenced BLOCKED dimension with a
                # named resolution lane) is recorded as held out of the current
                # admission scope instead of blocking every other condition's
                # first batch. The hold never drops the key from coverage, never
                # converts a BLOCKED verdict, and the key cannot represent or
                # candidate until a later checklist re-includes it without the
                # hold (which re-exposes its BLOCKED rows to completion).
                hold = entry["admission_hold"]
                _require(type(hold) is dict and set(hold) == {"reason", "resolution_lane"},
                         "fast16 offline record: admission hold shape required")
                _text(hold["reason"], "fast16 offline record: admission hold reason required")
                _text(hold["resolution_lane"],
                      "fast16 offline record: admission hold resolution lane required")
                _require(any(row2["status"] == "BLOCKED" for row2 in entry.values()
                             if isinstance(row2, dict) and "status" in row2),
                         "fast16 offline record: admission hold requires a real BLOCKED dimension")
                continue
            if dimension == "risk_binding":
                # validated by _check_risk_binding below (shape) and the
                # representative stage (semantics); carries no verdict.
                continue
            _require(type(row) is dict and set(row) == {"status", "reason"},
                     "fast16 offline record: dimension verdict shape required")
            _require(row["status"] in ("PASS", "BLOCKED", "NOT_APPLICABLE"),
                     "fast16 offline record: dimension status must be PASS, BLOCKED or NOT_APPLICABLE")
            if row["status"] == "PASS":
                _require(row["reason"] is None,
                         "fast16 offline record: PASS dimension must carry no excuse text")
            else:
                # BLOCKED and NOT_APPLICABLE share the same discipline: the
                # verdict is only admissible with an explicit nonempty reason.
                _text(row["reason"],
                      "fast16 offline record: " + row["status"]
                      + " dimension requires an explicit reason")
            if row["status"] == "NOT_APPLICABLE" and dimension == "pairing":
                
                # plan's own family_ids ARE the pairing denominator contract.
                # A condition inside at least one contrast family has pairing
                # partners and may never carry pairing=NOT_APPLICABLE - only
                # family-less conditions (isolated single roots) may. The
                # other six dimensions have no denominator test inside the
                # gate's frozen contract inputs; their NA admissibility stays
                # a registration-review surface, never a gate PASS.
                _require(not items[key]["family_ids"],
                         "fast16 offline record: pairing NOT_APPLICABLE requires a condition "
                         "with no contrast family (a family condition has a pairing denominator)")
    _check_risk_binding(checklist, items)


def _check_risk_binding(checklist, items):
    """minimum-use checks (v0.7 section 0.2, merged coverage) shape check.

    Optional per-key ``risk_binding`` entries (alongside the seven dimension
    verdicts, like ``admission_hold``): representatives cover RISK CLASSES,
    not one long pilot per key. The binding is declared per non-held key with
    a machine-derived risk class and its derivation basis; the representative
    stage accepts one currently-valid PASS anchor per class as covering the
    class. A held key carries no binding (it is never a candidate); the
    binding never changes a checklist verdict.
    """
    for key, entry in checklist.items():
        if "risk_binding" not in entry:
            continue
        _require("admission_hold" not in entry,
                 "fast16 offline record: held key cannot carry a risk binding")
        binding = entry["risk_binding"]
        _require(type(binding) is dict and set(binding) == {"risk_class", "derived_from"},
                 "fast16 offline record: risk binding entry shape required")
        _text(binding["risk_class"], "fast16 offline record: risk class required")
        _text(binding["derived_from"], "fast16 offline record: risk class derivation required")


def _fast16_quality_reference(record: dict[str, Any]) -> bool:
    value = record["quality_report"]
    return (type(value) is dict and set(value) == {"relative_path", "sha256"}
            and value["relative_path"] == QUALITY_REPORT_RELATIVE_PATH
            and _SHA256.fullmatch(value.get("sha256") or "") is not None)


def _check_fast16_attempt(attempt: Any, purpose: str) -> None:
    _require(type(attempt) is dict and set(attempt) == _ATTEMPT_FIELDS,
             "fast16 record: attempt exact fields required")
    _require(attempt["purpose"] == purpose, "fast16 record: attempt purpose mismatch")
    for field in ("run_id", "attempt_id"):
        _identifier(attempt[field], "fast16 attempt." + field)
    root = Path(_text(attempt["evidence_root"], "fast16 attempt.evidence_root"))
    _require(root.is_absolute() and ".." not in root.parts,
             "fast16 attempt: absolute safe path required")


def _fast16_structural_check(record: Any, expected: ExpectedCoverage,
                             fingerprints: FingerprintBundle | None = None) -> None:
    """Field/enum/seal/membership validation of one FAST16 stage record.

    Used both at registration (with the current bundle) and when reloading a
    persisted ledger (integrity re-derivation without touching attempt disks).
    """
    _require(type(record) is dict and
             (set(record) == _FAST16_FIELDS or
              (record.get("kind") == "representative" and record.get("status") == "PASS"
               and set(record) == _FAST16_FIELDS | {"derived_p0_review"})),
             "fast16 record: exact fields required")
    if "derived_p0_review" in record:
        ref = record["derived_p0_review"]
        _require(type(ref) is dict and set(ref) == {"path", "sha256"},
                 "fast16 record: derived review reference shape required")
        _sha(ref["sha256"], "fast16 record: derived review sha256")
    _require(record["schema_version"] == FAST16_STAGE_RECORD_SCHEMA,
             "fast16 record: unsupported schema")
    _require(record["kind"] in FAST16_KIND_STAGE, "fast16 record: unknown kind")
    _require(record["stage"] == FAST16_KIND_STAGE[record["kind"]],
             "fast16 record: kind/stage mismatch")
    _identifier(record["record_id"], "fast16 record id")
    _timestamp(record["recorded_at_utc"], "fast16 record.recorded_at_utc")
    _sha(record["record_sha256"], "fast16 record sha256")
    _require(record["record_sha256"] == _sealed_hash(record, "record_sha256"),
             "fast16 record: hash does not seal content")
    _sha(record["fingerprint_bundle_sha256"], "fast16 fingerprint sha256")
    if fingerprints is not None:
        _require(record["fingerprint_bundle_sha256"] == fingerprints.bundle_sha256,
                 "fast16 record: bound fingerprint bundle mismatch")
    _require(expected.plan_sha256 == record["plan_sha256"],
             "fast16 record: bound execution plan mismatch")
    _require(record["catalog_source"] == expected.catalog_source,
             "fast16 record: bound catalog mismatch")
    keys = record["coverage_keys"]
    _require(type(keys) is list and bool(keys) and len(set(keys)) == len(keys),
             "fast16 record: unique coverage keys required")
    kind = record["kind"]
    if kind == "offline_check":
        _require(set(keys) == set(expected.smoke_keys),
                 "fast16 record: offline checklist must cover the full expected condition set")
        _require(record["status"] is None and record["run_contract_sha256"] is None
                 and record["attempt"] is None and record["quality_report"] is None,
                 "fast16 record: offline checklist carries no run artifacts")
        _check_offline_checklist(record["checklist"], expected)
    elif kind == "block_expansion":
        _require(set(keys) <= set(expected.smoke_keys),
                 "fast16 record: first block must be a subset of expected conditions")
        _require(record["status"] is None and record["run_contract_sha256"] is None
                 and record["attempt"] is None and record["quality_report"] is None
                 and record["checklist"] is None,
                 "fast16 record: block expansion carries no run artifacts")
    else:
        _require(record["status"] in _FAST16_STATUSES,
                 "fast16 record: status must be PASS or FAIL")
        _require(record["checklist"] is None, "fast16 record: checklist is offline-only")
        _require(len(keys) == 1, "fast16 record: run evidence covers exactly one condition")
        pool = set(expected.pilot_keys if kind == "representative" else expected.smoke_keys)
        _require(keys[0] in pool, "fast16 record: coverage key outside the kind's expected set")


def _verify_fast16_attempt_and_quality(record: dict[str, Any], attempt_root: Path | None,
                                       purpose: str) -> None:
    """Bind a PASS stage record to intact on-disk quality evidence.

    minimum-use checks (acceptance v0.7 section 0.4): the FAST16 admission criterion is the
    P0 assessment layer (experiment semantics / process complete and
    recoverable / data real, sufficient, unpolluted) derived from the recorded
    checks; the technical layer stays bound and byte-verified as the recorded
    state, but a non-P0 technical failure no longer blocks admission by
    itself. This is NOT release eligibility and creates no PASS for any old
    check: the all-PASS quality contract for EvidenceLedger/verify_release is
    untouched.
    """
    _require(isinstance(attempt_root, Path) and attempt_root.is_absolute(),
             "fast16 loader: absolute attempt root required")
    _require(attempt_root.resolve() == Path(record["attempt"]["evidence_root"]).resolve(),
             "fast16 loader: attempt root differs from the record's evidence root")
    path = attempt_root / record["quality_report"]["relative_path"]
    _require(path.is_file() and path.resolve().is_relative_to(attempt_root.resolve()),
             "fast16 loader: quality report missing or outside the attempt")
    raw = path.read_bytes()
    _require(hashlib.sha256(raw).hexdigest() == record["quality_report"]["sha256"],
             "fast16 loader: quality report bytes do not match the recorded hash")
    report = json.loads(raw.decode("utf-8"))
    _require(type(report) is dict, "fast16 loader: quality report must be an object")
    _require(report.get("schema_version") == QUALITY_REPORT_SCHEMA,
             "fast16 loader: unsupported quality report schema")
    _require(report.get("sample_purpose") == purpose,
             "fast16 loader: quality report purpose differs from the record kind")
    technical = report.get("technical_assessment")
    _require(type(technical) is dict
             and technical.get("schema_version") == TECHNICAL_ASSESSMENT_SCHEMA,
             "fast16 loader: technical assessment missing or unsupported schema")
    _require(technical.get("deferred_checks") == ["final_annotation_and_review"],
             "fast16 loader: technical assessment must keep final annotation deferred")
    _require(isinstance(technical.get("final_annotation_review_status"), str)
             and bool(technical["final_annotation_review_status"]),
             "fast16 loader: final annotation review status must be surfaced")
    _require(technical.get("collection_release_eligible") is False
             and technical.get("formal_release_eligible") is False,
             "fast16 loader: technical assessment must not claim any release eligibility")
    p0 = report.get("p0_assessment")
    _require(type(p0) is dict and p0.get("schema_version") == P0_ASSESSMENT_SCHEMA,
             'fast16 loader: minimum-use checks assessment missing or unsupported schema')
    if "derived_p0_review" not in record:
        _require(p0.get("status") == "PASS",
                 'fast16 loader: minimum-use checks assessment is not PASS (semantics/process-recovery/data P0 not verified)')
        _require(type(p0.get("p0s")) is dict
                 and set(p0["p0s"]) == {"experiment_semantics", "process_recoverable", "data_real"}
                 and all(block.get("status") == "PASS" and type(block.get("reason")) is str
                         for block in p0["p0s"].values()),
                 'fast16 loader: minimum-use checks blocks incomplete or not all PASS')
    else:
        # Representative-only v0.8 re-review. The raw quality report stays
        # byte-identical and machine FAIL remains visible in the ledger.
        from scripts.collection import review_samples as derived
        derived.verify(derived.load_review_ref(record["derived_p0_review"]), record, report, attempt_root,
                       Path(__file__).with_name("runner.py"))
    _require(p0.get("collection_release_eligible") is False
             and p0.get("formal_release_eligible") is False,
             'fast16 loader: minimum-use checks assessment must not claim any release eligibility')


def _fast16_record_validity(record: dict[str, Any], record_bundle: FingerprintBundle | None,
                            current: FingerprintBundle,
                            items: dict[str, dict[str, Any]]) -> dict[str, str]:
    """Per-key currency of one FAST16 record under the CURRENT bundle.

    Same rule table as evidence_validity, with one documented distinction: a
    criteria-only change leaves the plan-bound offline checklist valid (it
    checks plan facts, not judgments), but stales every QC-bearing record and
    the block-expansion claim - their PASS verdicts were derived under the old
    judgment rules and must be re-verified.
    """
    keys = record["coverage_keys"]
    if record_bundle is None:
        return {key: "bound_fingerprint_bundle_unavailable" for key in keys}
    if record_bundle.bundle_sha256 == current.bundle_sha256:
        return {key: "valid_current_fingerprint" for key in keys}
    diff = diff_fingerprints(record_bundle, current)
    if diff.empty:
        return {key: "valid_current_fingerprint" for key in keys}
    if diff.allows_re_evaluation:
        if record["kind"] == "offline_check":
            return {key: "valid_plan_bound_checklist" for key in keys}
        return {key: "invalidated_by_criteria_change" for key in keys}
    _require(items is not None, "fast16 validity: coverage items required")
    affected = set(diff.to_dict()["affected_scenario_ids"])
    worst = diff.to_dict()["worst_scope"]
    result: dict[str, str] = {}
    for key in keys:
        item = items.get(key)
        _require(item is not None, "fast16 validity: coverage key outside expected items")
        if worst == "all":
            result[key] = "invalidated_by_" + worst + "_scope_change"
        elif item["scenario_id"] in affected:
            result[key] = "invalidated_by_scenario_local_change"
        else:
            result[key] = "valid_unchanged_scenario_scope"
    return result


def _fast16_kind_events(admission: "Fast16Admission", kind: str, key: str,
                        fingerprints: FingerprintBundle,
                        items: dict[str, dict[str, Any]]) -> list[tuple[Any, dict[str, Any], Any]]:
    events: list[tuple[Any, dict[str, Any], Any]] = []
    for record in admission.records:
        if record["kind"] != kind or key not in record["coverage_keys"]:
            continue
        bundle = admission.bundle(record["fingerprint_bundle_sha256"])
        validity = _fast16_record_validity(record, bundle, fingerprints, items)
        events.append((_timestamp(record["recorded_at_utc"], "fast16 record time"),
                       record, validity.get(key)))
    return sorted(events, key=lambda event: event[0])


def _fast16_latest_valid_pass(admission: "Fast16Admission", kind: str, key: str,
                              fingerprints: FingerprintBundle,
                              items: dict[str, dict[str, Any]], *,
                              at_or_before: Any = None) -> dict[str, Any] | None:
    events = _fast16_kind_events(admission, kind, key, fingerprints, items)
    passes = [event for event in events
              if event[1]["status"] == "PASS" and str(event[2]).startswith("valid_")
              and (at_or_before is None or event[0] <= at_or_before)]
    return passes[-1][1] if passes else None


def _fast16_composite_reasons(admission: "Fast16Admission", fingerprints: FingerprintBundle,
                              block_keys: list[str] | tuple[str, ...],
                              items: dict[str, dict[str, Any]],
                              outside_boundary: Any) -> list[str]:
    """First-block composite check (FAST16 policy clause 5).

    全预期条件集合: block keys are a subset of the expected condition set (the
    state snapshot lists the remaining conditions). 失败闭合: every failing
    first candidate inside the block is closed by a later currently-valid PASS
    (checked over the whole history, so a post-expansion failure of a block
    condition also stops further expansion - quarantine semantics). 恢复:
    every block condition carries a currently-valid condition-verified record,
    whose technical QC (including the Q2 recovery group) was bound to intact
    attempt bytes at registration. 版本一致性: all counted evidence is
    currently valid under the very bundle this check runs against - stale
    (old-fingerprint) records cannot participate. Formal runs recorded before
    the ORIGINAL expansion claim (outside_boundary) but outside the declared
    block contradict the claim; runs after it are the expansion itself.
    """
    reasons: list[str] = []
    block = set(block_keys)
    if not (block <= set(admission.expected.smoke_keys)):
        reasons.append("block_keys_outside_expected_set")
    for key in sorted(block):
        scenario = items[key]["scenario_id"]
        if _fast16_latest_valid_pass(admission, "condition_verified", key, fingerprints, items) is None:
            reasons.append("block_condition_not_verified:" + scenario)
        events = _fast16_kind_events(admission, "first_candidate", key, fingerprints, items)
        failures = [event for event in events if event[1]["status"] == "FAIL"]
        passes = [event for event in events
                  if event[1]["status"] == "PASS" and str(event[2]).startswith("valid_")]
        if failures and not [event for event in passes if event[0] >= failures[-1][0]]:
            reasons.append("unclosed_first_candidate_failure:" + scenario)
    for record in admission.records:
        if record["kind"] != "first_candidate":
            continue
        if _timestamp(record["recorded_at_utc"], "fast16 record time") > outside_boundary:
            continue
        for key in record["coverage_keys"]:
            if key not in block:
                reasons.append("formal_run_outside_declared_first_block:" + items[key]["scenario_id"])
    return list(dict.fromkeys(reasons))


def _fast16_claims(admission: "Fast16Admission") -> list[tuple[Any, dict[str, Any]]]:
    """All block-expansion claims, oldest first."""
    claims = [(_timestamp(record["recorded_at_utc"], "fast16 record time"), record)
              for record in admission.records if record["kind"] == "block_expansion"]
    return sorted(claims, key=lambda claim: claim[0])


class Fast16Admission:
    """FAST16 staged-admission registrar bound to one ExpectedCoverage.

    Complementary to EvidenceLedger (see module docstring): this ledger gates
    the staged collection progression only. Registration enforces the ladder
    prerequisites fail-closed - a record whose stage prerequisites are not
    currently reached is rejected, never downgraded; empty or placeholder
    evidence is rejected; nothing here can produce a RELEASED decision or
    relax verify_release.
    """

    def __init__(self, expected: ExpectedCoverage) -> None:
        _require(isinstance(expected, ExpectedCoverage), "fast16 ledger: ExpectedCoverage required")
        self.expected = expected
        self._records: dict[str, dict[str, Any]] = {}
        self._bundles: dict[str, FingerprintBundle] = {}

    @property
    def records(self) -> tuple[dict[str, Any], ...]:
        return tuple(self._records.values())

    def record(self, record_id: str) -> dict[str, Any] | None:
        return self._records.get(record_id)

    def bundle(self, bundle_sha256: str) -> FingerprintBundle | None:
        return self._bundles.get(bundle_sha256)

    def register(self, record: dict[str, Any], fingerprints: FingerprintBundle,
                 *, attempt_root: Path | None = None) -> str:
        """Validate and append one FAST16 stage record; returns its record_id."""
        _require(isinstance(fingerprints, FingerprintBundle),
                 "fast16 register: FingerprintBundle required")
        _fast16_structural_check(record, self.expected, fingerprints)
        _require(record["record_id"] not in self._records,
                 "fast16 record: duplicate record ID")
        kind = record["kind"]
        items = self.expected.item_by_key()
        now = _timestamp(record["recorded_at_utc"], "fast16 record.recorded_at_utc")
        if kind == "offline_check":
            pass  # first stage: no prerequisite
        elif kind == "block_expansion":
            claims = _fast16_claims(self)
            if claims:
                # a still-valid claim makes a new one a duplicate; a fully
                # invalidated one (fingerprint drift) makes re-claiming the
                # REQUIRED re-verification after re-verifying the evidence.
                stale = True
                for _, prior in claims:
                    bundle = self.bundle(prior["fingerprint_bundle_sha256"])
                    validity = _fast16_record_validity(prior, bundle, fingerprints, items)
                    if all(str(value).startswith("valid_") for value in validity.values()):
                        stale = False
                _require(stale,
                         "fast16 record: block expansion already claimed and currently valid")
                boundary = claims[0][0]  # original claim disciplines pre-expansion runs
            else:
                boundary = now
            composite = _fast16_composite_reasons(self, fingerprints, record["coverage_keys"],
                                                  items, boundary)
            _require(not composite,
                     "fast16 record: first-block composite check failed ("
                     + ",".join(composite[:20]) + ")")
        else:
            key = record["coverage_keys"][0]
            contracts = self.expected.run_contracts_by_key()
            _sha(record["run_contract_sha256"], "fast16 run contract sha256")
            _require(record["run_contract_sha256"] in contracts[key],
                     "fast16 record: run contract does not belong to the covered condition")
            _check_fast16_attempt(record["attempt"], FAST16_PURPOSE[kind])
            if kind == "representative":
                state = fast16_stage_state(self, fingerprints)
                _require(state["stages"]["OFFLINE_COMPLETE"]["reached"],
                         "fast16 record: representative evidence requires the completed offline checklist")
            elif kind == "first_candidate":
                state = fast16_stage_state(self, fingerprints)
                _require(state["stages"]["REPRESENTATIVE_VERIFIED"]["reached"],
                         "fast16 record: first formal candidate requires verified representatives")
                claims = _fast16_claims(self)
                if claims and key not in claims[-1][1]["coverage_keys"]:
                    _require(state["stages"]["BLOCK_EXPANSION"]["reached"],
                             "fast16 record: formal run outside the declared first block "
                             "requires completed block expansion")
            elif kind == "condition_verified":
                first = _fast16_latest_valid_pass(self, "first_candidate", key, fingerprints,
                                                  items, at_or_before=now)
                _require(first is not None,
                         "fast16 record: condition verification requires a currently-valid "
                         "first-candidate PASS for the condition")
            else:  # repetition
                verified = _fast16_latest_valid_pass(self, "condition_verified", key,
                                                     fingerprints, items)
                _require(verified is not None,
                         "fast16 record: repetition requires the condition to be verified first")
            if record["status"] == "PASS":
                _require(_fast16_quality_reference(record),
                         "fast16 record: quality report reference required")
                _verify_fast16_attempt_and_quality(record, attempt_root, FAST16_PURPOSE[kind])
            else:
                _require(record["quality_report"] is None or _fast16_quality_reference(record),
                         "fast16 record: malformed quality report reference")
        self._records[record["record_id"]] = json.loads(c.canonical_json(record))
        self._bundles.setdefault(fingerprints.bundle_sha256, fingerprints)
        return record["record_id"]

    def save(self, path: Any) -> None:
        """Persist the registrar as a self-sealed ledger file."""
        payload = {"schema_version": FAST16_LEDGER_SCHEMA,
                   "expected": self.expected.to_dict(),
                   "bundles": [bundle.to_dict() for bundle in self._bundles.values()],
                   "records": [dict(record) for record in self._records.values()],
                   "ledger_sha256": None}
        payload["ledger_sha256"] = _sealed_hash(payload, "ledger_sha256")
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(c.canonical_json(payload) + "\n", encoding="utf-8")

    @classmethod
    def load(cls, path: Any) -> "Fast16Admission":
        """Reload a sealed ledger file; any edit breaks the ledger seal."""
        payload = c._strict_json(Path(path).read_text(encoding="utf-8"))
        _require(type(payload) is dict and set(payload) ==
                 {"schema_version", "expected", "bundles", "records", "ledger_sha256"},
                 "fast16 ledger: exact fields required")
        _require(payload["schema_version"] == FAST16_LEDGER_SCHEMA,
                 "fast16 ledger: unsupported schema")
        _require(payload["ledger_sha256"] == _sealed_hash(payload, "ledger_sha256"),
                 "fast16 ledger: hash does not seal content (tampered)")
        admission = cls(ExpectedCoverage.from_dict(payload["expected"]))
        for bundle_value in payload["bundles"]:
            bundle = FingerprintBundle.from_dict(bundle_value)
            admission._bundles.setdefault(bundle.bundle_sha256, bundle)
        for record in payload["records"]:
            _fast16_structural_check(record, admission.expected)
            _require(record["record_id"] not in admission._records,
                     "fast16 ledger: duplicate record ID")
            admission._records[record["record_id"]] = json.loads(c.canonical_json(record))
        return admission


def fast16_stage_state(admission: Fast16Admission,
                       fingerprints: FingerprintBundle) -> dict[str, Any]:
    """Compute the sealed FAST16 admission state under the CURRENT bundle.

    Every stage is re-derived from the registered records through the
    invalidation rule table, so fingerprint/code/criteria drift drops stages
    explicitly instead of silently carrying old evidence forward. A stage is
    reached only when every prerequisite stage is reached and its own evidence
    is complete and currently valid; blocked offline items are listed
    explicitly, never hidden. NOT_APPLICABLE offline verdicts are neither
    blockers nor passes for OFFLINE_COMPLETE: completion counts only BLOCKED
    rows, and NA never upgrades a stage (NA != PASS, eligibility semantics
    live downstream and are unchanged by this).
    """
    _require(isinstance(admission, Fast16Admission) and isinstance(fingerprints, FingerprintBundle),
             "fast16 state: Fast16Admission and FingerprintBundle required")
    expected = admission.expected
    items = expected.item_by_key()

    def label(key: str) -> str:
        return items[key]["scenario_id"] + "/" + key[:12]

    stages: dict[str, dict[str, Any]] = {}
    reasons: list[str] = []
    held_keys: list[str] = []
    offline_events: list[tuple[Any, dict[str, Any], dict[str, str]]] = []
    for record in admission.records:
        if record["kind"] != "offline_check":
            continue
        bundle = admission.bundle(record["fingerprint_bundle_sha256"])
        validity = _fast16_record_validity(record, bundle, fingerprints, items)
        offline_events.append((_timestamp(record["recorded_at_utc"], "fast16 record time"),
                               record, validity))
    if not offline_events:
        reasons.append("offline_checklist_not_registered")
    else:
        _, latest, validity = max(offline_events, key=lambda event: event[0])
        if not all(str(value).startswith("valid_") for value in validity.values()):
            reasons.append("offline_checklist_invalidated_by_fingerprint_change")
        else:
            
            # deliberately excluded here - NA is a verdict about a missing
            # denominator for that condition/dimension, not a gap to close -
            # and equally deliberately never counted as satisfying anything.
            # minimum-use checks (v0.7 section 0.4): rows under an admission_hold belong to
            # REGISTERED-pending families: they are surfaced as held (never
            # hidden), do not block the constructible set's completion, and
            # their keys stay ineligible for representation/candidacy.
            held_keys = sorted(key for key, entry in latest["checklist"].items()
                               if "admission_hold" in entry)
            blocked = sorted(items[key]["scenario_id"] + "." + dimension
                             for key, entry in latest["checklist"].items()
                             if "admission_hold" not in entry
                             for dimension, row in entry.items()
                             if isinstance(row, dict) and row.get("status") == "BLOCKED")
            if blocked:
                reasons.append("offline_checklist_has_blocked_items")
                reasons.extend("blocked:" + item for item in blocked)
    stages["OFFLINE_COMPLETE"] = {"reached": not reasons, "reasons": reasons,
                                  "held_pending_keys": held_keys}

    reasons = []
    verified_pilot: dict[str, str] = {}
    latest_offline = next((record for record in reversed(admission.records)
                           if record["kind"] == "offline_check"), None)
    risk_of = {}
    if latest_offline is not None:
        risk_of = {key: entry["risk_binding"]["risk_class"]
                   for key, entry in latest_offline["checklist"].items() if "risk_binding" in entry}
    held_keys = stages["OFFLINE_COMPLETE"].get("held_pending_keys", [])
    if not stages["OFFLINE_COMPLETE"]["reached"]:
        reasons.append("prerequisite_offline_complete_not_reached")
    # minimum-use checks (v0.7 section 0.2): merged representative coverage -- per-key PASS
    # records always satisfy a key; additionally ONE currently-valid PASS
    # anchor satisfies every other key of the same declared risk class.
    class_anchors: dict[str, tuple] = {}
    key_passes: dict[str, list] = {}
    for key in expected.pilot_keys:
        events = _fast16_kind_events(admission, "representative", key, fingerprints, items)
        passes = [event for event in events
                  if event[1]["status"] == "PASS" and str(event[2]).startswith("valid_")]
        failures = [event for event in events if event[1]["status"] == "FAIL"]
        key_passes[key] = (passes, failures)
        if passes and key not in held_keys and key in risk_of:
            cls = risk_of[key]
            if cls not in class_anchors or passes[-1][0] > class_anchors[cls][0]:
                class_anchors[cls] = passes[-1]
    for key in expected.pilot_keys:
        if key in held_keys:
            continue  # minimum-use checks v0.7: a held pending-design key demands no representative
        passes, failures = key_passes[key]
        if passes:
            verified_pilot[key] = passes[-1][1]["record_id"]
            if failures and not [event for event in passes if event[0] >= failures[-1][0]]:
                reasons.append("unclosed_representative_failure:" + label(key))
            continue
        cls = risk_of.get(key)
        if cls is not None and cls in class_anchors:
            verified_pilot[key] = class_anchors[cls][1]["record_id"]
            continue
        reasons.append("representative_missing:" + label(key))
    stages["REPRESENTATIVE_VERIFIED"] = {
        "reached": not reasons, "reasons": reasons,
        "verified_pilot_keys": sorted(verified_pilot)}

    reasons = []
    first_by_key: dict[str, str] = {}
    if not stages["REPRESENTATIVE_VERIFIED"]["reached"]:
        reasons.append("prerequisite_representative_verified_not_reached")
    for record in admission.records:
        if record["kind"] != "first_candidate":
            continue
        key = record["coverage_keys"][0]
        if key in held_keys:
            continue  # minimum-use checks v0.7: a held key cannot be a first candidate
        current = _fast16_latest_valid_pass(admission, "first_candidate", key, fingerprints, items)
        if current is not None:
            first_by_key[key] = current["record_id"]
    if not first_by_key:
        reasons.append("no_valid_first_candidate")
    stages["FIRST_CANDIDATE"] = {"reached": not reasons, "reasons": reasons,
                                 "first_candidate_keys": sorted(first_by_key)}

    reasons = []
    pool: dict[str, dict[str, Any]] = {}
    if not stages["FIRST_CANDIDATE"]["reached"]:
        reasons.append("prerequisite_first_candidate_not_reached")
    for record in admission.records:
        if record["kind"] != "condition_verified":
            continue
        key = record["coverage_keys"][0]
        verified = _fast16_latest_valid_pass(admission, "condition_verified", key,
                                             fingerprints, items)
        if verified is None:
            continue
        if key not in first_by_key:
            reasons.append("condition_verified_without_valid_first_candidate:" + label(key))
            continue
        repetitions = sum(
            1 for event in _fast16_kind_events(admission, "repetition", key, fingerprints, items)
            if event[1]["status"] == "PASS" and str(event[2]).startswith("valid_"))
        pool[key] = {"condition_verified_record": verified["record_id"],
                     "first_candidate_record": first_by_key[key],
                     "valid_repetitions": repetitions}
    if not pool:
        reasons.append("no_verified_condition")
    stages["CONDITION_VERIFIED"] = {"reached": not reasons, "reasons": reasons,
                                    "condition_pool": {key: pool[key] for key in sorted(pool)}}

    reasons = []
    detail: dict[str, Any] = {}
    if not stages["CONDITION_VERIFIED"]["reached"]:
        reasons.append("prerequisite_condition_verified_not_reached")
    claims = _fast16_claims(admission)
    if not claims:
        reasons.append("block_expansion_not_registered")
    else:
        _, claim = claims[-1]  # the latest claim governs the current block
        bundle = admission.bundle(claim["fingerprint_bundle_sha256"])
        validity = _fast16_record_validity(claim, bundle, fingerprints, items)
        if not all(str(value).startswith("valid_") for value in validity.values()):
            reasons.append("block_expansion_invalidated_by_fingerprint_change")
        else:
            reasons.extend(_fast16_composite_reasons(
                admission, fingerprints, claim["coverage_keys"], items, claims[0][0]))
            detail = {"first_block_keys": sorted(claim["coverage_keys"]),
                      "remaining_conditions": sorted(set(expected.smoke_keys)
                                                     - set(claim["coverage_keys"]))}
    stages["BLOCK_EXPANSION"] = {"reached": not reasons, "reasons": reasons, **detail}

    ladder = None
    for stage in FAST16_STAGES:
        if stages[stage]["reached"]:
            ladder = stage
        else:
            break
    payload = {"schema_version": FAST16_STATE_SCHEMA,
               "computed_at_utc": fingerprints.collected_at_utc,
               "fingerprint_bundle_sha256": fingerprints.bundle_sha256,
               "plan_sha256": expected.plan_sha256,
               "catalog_source": expected.catalog_source,
               "design_version": expected.design_version,
               "coverage_sha256": expected.coverage_sha256,
               "stages": stages,
               "ladder_position": ladder}
    payload["record_sha256"] = _sealed_hash(payload, "record_sha256")
    return payload


def verify_fast16_stage(state: Any, fingerprints: FingerprintBundle,
                        expected: ExpectedCoverage, stage: str) -> dict[str, Any]:
    """FAST16 machine entry check for admission consumers.

    ok=True only when the sealed state binds the CURRENT fingerprint bundle
    and expected coverage, its interior is self-consistent (reached stages
    form a ladder prefix, ladder_position matches, every stage carries a
    reasons list) and the required stage is reached. No parameter can bypass
    a failed verification; a stage below the ladder is always refused.
    """
    _require(isinstance(fingerprints, FingerprintBundle)
             and isinstance(expected, ExpectedCoverage),
             "fast16 verify: FingerprintBundle and ExpectedCoverage required")
    _require(stage in FAST16_STAGES, "fast16 verify: unknown stage")
    _require(type(state) is dict and set(state) == _STATE_FIELDS,
             "fast16 verify: state fields required")
    if state.get("schema_version") != FAST16_STATE_SCHEMA:
        return {"ok": False, "reasons": ["unsupported_state_schema"],
                "ladder_position": state.get("ladder_position")}
    if state.get("record_sha256") != _sealed_hash(state, "record_sha256"):
        return {"ok": False, "reasons": ["state_hash_mismatch_tampered"],
                "ladder_position": state.get("ladder_position")}
    reasons: list[str] = []
    stages = state.get("stages")
    if type(stages) is not dict or set(stages) != set(FAST16_STAGES):
        reasons.append("state_interior_inconsistent")
    else:
        for name, entry in stages.items():
            if (type(entry) is not dict or type(entry.get("reasons")) is not list
                    or any(not isinstance(reason, str) for reason in entry["reasons"])
                    or type(entry.get("reached")) is not bool):
                reasons.append("state_interior_inconsistent")
                break
        if "state_interior_inconsistent" not in reasons:
            reached = [name for name in FAST16_STAGES if stages[name]["reached"]]
            if reached != [name for name in FAST16_STAGES[:len(reached)]]:
                reasons.append("reached_stages_not_a_ladder_prefix")
            if state.get("ladder_position") != (reached[-1] if reached else None):
                reasons.append("ladder_position_inconsistent")
            if stage not in reached:
                reasons.append("stage_not_reached:" + stage)
    if state.get("fingerprint_bundle_sha256") != fingerprints.bundle_sha256:
        reasons.append("fingerprint_drift_state_is_stale")
    if (state.get("plan_sha256") != expected.plan_sha256
            or state.get("catalog_source") != expected.catalog_source):
        reasons.append("state_plan_or_catalog_binding_mismatch")
    if state.get("coverage_sha256") != expected.coverage_sha256:
        reasons.append("expected_sets_changed_state_is_stale")
    reasons = list(dict.fromkeys(reasons))
    return {"ok": not reasons, "reasons": reasons,
            "ladder_position": state.get("ladder_position")}


def main(argv: list[str] | None = None) -> int:
    """Narrow CLI surface for the FAST16 admission registrar.

    Run from the repository root as:
      python -m scripts.collection.release_gate <command> ...
    Commands:
      fast16-register --ledger P --record P --fingerprints P
                      [--attempt-root P] [--expected P]
        Register one sealed FAST16 stage record (JSON) and persist the ledger.
        --expected (ExpectedCoverage JSON) is mandatory when creating the
        ledger file; --attempt-root is mandatory for PASS run records.
      fast16-state --ledger P --fingerprints P
        Print the sealed FAST16 stage state under the supplied bundle.
      fast16-verify-stage --ledger P --fingerprints P --stage STAGE
        Exit 0 only when STAGE (and its whole prerequisite ladder) is reached
        under the current fingerprints; exit 1 with reasons otherwise; exit 2
        on invalid registrar input.
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(prog="release-gate",
                                     description="offline FAST16 admission registrar")
    commands = parser.add_subparsers(dest="command", required=True)
    register_command = commands.add_parser("fast16-register",
                                           help="register one FAST16 stage record")
    register_command.add_argument("--ledger", required=True)
    register_command.add_argument("--record", required=True)
    register_command.add_argument("--fingerprints", required=True)
    register_command.add_argument("--attempt-root")
    register_command.add_argument("--expected",
                                  help="expected-coverage JSON (creates a new ledger)")
    state_command = commands.add_parser("fast16-state",
                                        help="print the sealed FAST16 stage state")
    state_command.add_argument("--ledger", required=True)
    state_command.add_argument("--fingerprints", required=True)
    verify_command = commands.add_parser("fast16-verify-stage",
                                         help="exit 0 only when STAGE is reached")
    verify_command.add_argument("--ledger", required=True)
    verify_command.add_argument("--fingerprints", required=True)
    verify_command.add_argument("--stage", required=True)
    args = parser.parse_args(argv)
    try:
        fingerprints = FingerprintBundle.from_json(
            Path(args.fingerprints).read_text(encoding="utf-8"))
        ledger_path = Path(args.ledger)
        if args.command == "fast16-register":
            if ledger_path.is_file():
                admission = Fast16Admission.load(ledger_path)
            else:
                _require(args.expected is not None,
                         "fast16-register: --expected is required to create a new ledger")
                admission = Fast16Admission(ExpectedCoverage.from_dict(
                    c._strict_json(Path(args.expected).read_text(encoding="utf-8"))))
            record = c._strict_json(Path(args.record).read_text(encoding="utf-8"))
            admission.register(record, fingerprints,
                               attempt_root=Path(args.attempt_root) if args.attempt_root else None)
            admission.save(ledger_path)
            print(c.canonical_json({"registered": record["record_id"]}))
            return 0
        admission = Fast16Admission.load(ledger_path)
        state = fast16_stage_state(admission, fingerprints)
        if args.command == "fast16-state":
            print(c.canonical_json(state))
            return 0
        result = verify_fast16_stage(state, fingerprints, admission.expected, args.stage)
        print(c.canonical_json(result))
        return 0 if result["ok"] else 1
    except GateError as exc:
        print("release-gate: " + str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
