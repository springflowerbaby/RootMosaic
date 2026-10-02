"""Explicit serial smoke/pilot harness; formal entry is release-gated (collection).

Validation is the default and does not create files, invoke clients or start
workers. Actual execution preserves separate operation/observation facts and
never upgrades command success or synthetic input to empirical quality. A
formal purpose additionally passes the release-gate entry check inside
validate_run (frozen release record, ledger re-derivation, on-site asset
fingerprints); smoke/pilot paths never touch the release gate.
"""
from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import inspect
import json
import math
import os
import re
from pathlib import Path
import threading
import time
from typing import Any, Callable, Mapping
import uuid

from . import annotations as a, auxiliary as aux, contract as c, gateway as g, journal as j, observability as obs, pricing_route as pr, primitives as p, primitives_db as d, quality as q, release_gate as rg, telemetry as t, workload as w

RUNNER_SCHEMA = "rq4-collect/runner-v1"


# footprint = rollout band (15 s) + control-write margin (2 s) = 17 s -- the
# exact reservation D07's default-tier same-end pair passed with. The
# same-offset carry floors env reservations against this number.
_ENV_ROLLOUT_WRITE_MARGIN_S = 2.0
RECOVERY_SCHEMA = "rq4-collect/recovery-evidence-v1"
# Exact complete check set of the recovery-qualification scope; a subset or any
# extra name must be rejected, mirroring the sample path's exact Q2 set.
RECOVERY_CHECKS = ("recovery_reconcile", "environment_empty", "components_healthy", "checksums_unchanged")
_PHASES = ("pre_fault", "during_fault", "post_recovery")
_MODALITIES = ("metrics", "traces", "logs")

# collection formal-entry release gate: the release_gate_ref directory layout
# (record + ledger sidecar + expected coverage, all co-stored) and the R-side
# serialization schema of the EvidenceLedger sidecar (G-16: the ledger has no
# persistence face of its own; records/events/bundles are already
# register-validated plain dictionaries, so JSON round-trips add no semantics).
_FORMAL_RELEASE_RECORD_FILE = "release-record.json"
_FORMAL_LEDGER_FILE = "evidence-ledger.json"
_FORMAL_COVERAGE_FILE = "expected-coverage.json"
_FORMAL_LEDGER_SIDECAR_SCHEMA = "rq4-collect/evidence-ledger-sidecar-v1"

# Formal source manifests include the required collection executor modules.
# Offline analysis modules are not part of collection admission.
_FORMAL_REQUIRED_ASSET_LABELS = ("module.primitives_db", "module.auxiliary", "module.observability", "module.sampling", 'module.scenario_runner')
_FORMAL_SUGGESTED_ASSET_LABELS = ()


# this side only LOADS the sealed disk ledger and the on-site fingerprint
# bundle; it never registers, forces or rewrites ladder stages (N1: formal
# consumption goes through the disk-ledger path only).
_FORMAL_FAST16_LEDGER_FILE = "fast16-ledger.json"
_FORMAL_FAST16_FINGERPRINTS_FILE = "fingerprints.json"


class RunnerError(RuntimeError):
    pass


class LeaseBlocked(RunnerError):
    pass


class TimingViolation(RunnerError):
    pass


def _record_timing_budget_warning(event: dict[str, Any], metric: str,
                                 actual_s: float, budget_s: float) -> None:
    """Record an execution-budget overrun without changing semantic validity."""
    if actual_s > budget_s:
        event.setdefault("timing_warnings", []).append({
            "metric": metric,
            "actual_s": actual_s,
            "budget_s": budget_s,
        })
        event["timing_budget_status"] = "warning"
    else:
        event.setdefault("timing_budget_status", "within_budget")


def _validate_injection_bounds(started: float, ended: float | None,
                               phase_start: float, phase_end: float,
                               recovery_deadline: float) -> None:
    """Keep injection inside its observed phase and own recovery window."""
    if not phase_start <= started < phase_end:
        raise TimingViolation("fault injection started outside during_fault phase")
    if started >= recovery_deadline:
        raise TimingViolation("fault injection started at or after its own recovery deadline")
    if ended is None:
        return
    if ended < started:
        raise TimingViolation("fault injection end precedes its start")
    if not phase_start <= ended < phase_end:
        raise TimingViolation("fault injection completed outside during_fault phase")
    if ended > recovery_deadline:
        raise TimingViolation("injection confirmation missed its own recovery deadline")


class BaselineViolation(RunnerError):
    pass


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise RunnerError(message)


def _error_summary(exc: BaseException) -> str:
    """Exception class plus leading message text; class name alone when empty.

    collection (D19 diagnosis): a wrapped failure keeps one causal hop in the
    summary (caused-by class + message) so run journals name the real
    origin; deeper hops stay on the exception itself, never fabricated.
    """
    message = str(exc).strip()
    summary = type(exc).__name__ if not message else type(exc).__name__ + ": " + message[:200]
    cause = exc.__cause__
    if cause is not None:
        inner = str(cause).strip()[:160]
        summary += " <- " + type(cause).__name__ + (": " + inner if inner else "")
    return summary


def _number(value: Any, *, positive: bool = False) -> None:
    _require(type(value) in (int, float) and math.isfinite(value) and (value > 0 if positive else value >= 0), "invalid finite timing/budget")


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _hash(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class EnvironmentIdentity:
    cluster_uid: str
    namespace_uid: str

    def __post_init__(self) -> None:
        try:
            _require(str(uuid.UUID(self.cluster_uid)) == self.cluster_uid and str(uuid.UUID(self.namespace_uid)) == self.namespace_uid,
                     "canonical cluster/namespace UUIDs required")
        except (ValueError, TypeError, AttributeError):
            raise RunnerError("canonical cluster/namespace UUIDs required") from None

    @property
    def key(self) -> str:
        return _hash(_json_bytes({"cluster_uid": self.cluster_uid, "namespace_uid": self.namespace_uid}))


def environment_registry_root() -> Path:
    """Machine-user-local fixed registration root; queried only on execution."""
    base = Path(os.environ["LOCALAPPDATA"]) if os.name == "nt" and os.environ.get("LOCALAPPDATA") else Path.home() / ".local" / "state"
    return base.resolve() / "RecShop" / "M1" / "environment-registry-v1"


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    _require(path.resolve() == path and path.parent.is_dir(), "state path redirected or missing")
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    with temporary.open("xb") as stream:
        stream.write(_json_bytes(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


class EnvironmentLease:
    """One physical environment across output roots/context aliases.

    The fixed registry binds a physical identity to its first explicit lease
    root. The global OS lock and persistent dirty state survive output changes;
    OS lock release is never evidence of successful resource recovery.
    """
    def __init__(self, identity: EnvironmentIdentity, lease_root: Path, contract: c.RunContract, *, mode: str, resume: bool,
                 prepared_manifest=None) -> None:
        self.identity, self.contract, self.mode = identity, contract, mode
        self.root = Path(lease_root)
        _require(self.root.is_absolute() and ".." not in self.root.parts and self.root.resolve() == self.root,
                 "explicit stable lease root required")
        registry = environment_registry_root()
        _require(registry.resolve() == registry, "global lease registry redirected")
        registry.mkdir(parents=True, exist_ok=True)
        self.binding_path = registry / (identity.key + ".binding.json")
        self.lock_path = registry / (identity.key + ".lock")
        _require(self.lock_path.resolve() == self.lock_path, "environment lock redirected")
        self._fd = j._lock_file(self.lock_path)
        self._closed = False
        try:
            # Sampling deployment uses this same physical lock. An interrupted
            # maintenance rollout must block even before the first fault lease.
            from ops.metrics.maintenance_receipt import verify_maintenance_marker
            verify_maintenance_marker(registry, identity.key, evidence_kind="observed" if mode == "controlled_pilot" else "synthetic")
            self.root.mkdir(parents=True, exist_ok=True)
            _require(self.root.resolve() == self.root, "lease root redirected")
            self.state_path = self.root / (identity.key + ".state.json")
            binding = {"schema_version": RUNNER_SCHEMA, "cluster_uid": identity.cluster_uid,
                       "namespace_uid": identity.namespace_uid, "lease_root": str(self.root)}
            if self.binding_path.exists():
                _require(self.binding_path.resolve() == self.binding_path, "lease binding redirected")
                _require(json.loads(self.binding_path.read_bytes()) == binding, "physical environment is bound to another lease root")
                _require(self.state_path.is_file() and self.state_path.resolve() == self.state_path, "registered environment state missing")
                previous = json.loads(self.state_path.read_bytes())
                _require(previous.get("schema_version") == RUNNER_SCHEMA and previous.get("environment_key") == identity.key,
                         "persistent lease state identity/schema mismatch")
            else:
                _require(prepared_manifest is None, "prepared entry cannot register an environment")
                _require(not self.state_path.exists() and not resume, "cannot infer missing lease registration")
                with self.binding_path.open("xb") as stream:
                    stream.write(_json_bytes(binding)); stream.flush(); os.fsync(stream.fileno())
                previous = None
            context = contract.context.to_dict()
            self.reference = {"contract_sha256": contract.sha256, "run_id": context["run_id"], "attempt_id": context["attempt_id"],
                              "evidence_root": context["evidence_root"], "output_root": context["output_root"]}
            if resume:
                _require(previous is not None and previous.get("status") == "dirty"
                         and previous.get("attempt") == self.reference and previous.get("mode") == mode,
                         "resume must name the exact dirty attempt")
                _require(previous.get("workers") in {"not_started", "drained"},
                         "prior worker/controller state needs a separate drain audit; lock or PID is insufficient")
                self.state = previous
            else:
                if previous is not None:
                    _require(previous.get("status") == "clean" and previous.get("mode") == mode,
                             "environment has unresolved residual attempt; explicit reconcile required")
                self.state = {"schema_version": RUNNER_SCHEMA, "environment_key": identity.key, "status": "dirty",
                              "mode": mode, "attempt": self.reference, "workers": "not_started", "pid": os.getpid()}
                if prepared_manifest is not None:
                    _require(previous is not None and previous.get("workers") == "drained" and mode in {"isolated_test", "controlled_pilot"},
                             "prepared entry requires registered clean/drained environment")
                    reservation = {"schema_version": BOOTSTRAP_SCHEMA, "phase": "INITIALIZING", "mutations_authorized": False,
                                   "evidence_kind": _prepared_kind(mode),
                                   "contract": contract.to_dict(), "manifest": prepared_manifest,
                                   "environment_key": identity.key, "lease_root": str(self.root), "mode": mode,
                                   "previous_clean_state_sha256": _hash(_json_bytes(previous))}
                    path = self.root / "prepared-bootstrap" / uuid.uuid4().hex / "reservation.json"
                    reference = _write_exclusive_record(path, reservation)
                    self.state["auxiliary"] = {"required": True, "initialization": "INITIALIZING", "reservation": reference}
                self.save()
        except BaseException:
            self.close()
            raise

    def save(self) -> None:
        _require(not self._closed and self.root.resolve() == self.root, "lease closed or moved")
        self.state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        try:
            _atomic_json(self.state_path, self.state)
        except BaseException:
            self.state.update(status="dirty", persistence="unknown")
            raise

    def workers(self, state: str) -> None:
        _require(state in {"running", "drained", "unconfirmed"}, "unknown worker state")
        self.state["workers"] = state
        self.save()

    def mark_clean(self, journal: j.AttemptJournal, quality: dict[str, Any], *, quality_rules: q.QualityRules | None = None,
                   scope: str = "sample", auxiliary_verifier=None, recovery_ref=None, bootstrap_verifier=None,
                   cleanup_only: bool = False) -> None:
        _require(scope in {"sample", "recovery", "bootstrap_recovery"}, "unknown clean scope")
        _require(type(cleanup_only) is bool, "cleanup_only must be a typed bool")
        if scope == "bootstrap_recovery":
            _require(journal is None and type(bootstrap_verifier) is BootstrapRecoveryVerifier, "typed bootstrap-only verifier required")
            bootstrap_verifier.verify(self, quality)
            self.state.update(status="clean", cleanup_scope=("live" if self.mode == "controlled_pilot" else "offline") + "_bootstrap_abort_recovered", workers="drained",
                              bootstrap_recovery=bootstrap_verifier.report_ref)
            self.save()
            return
        _require(journal.contract.sha256 == self.contract.sha256 and self.state["workers"] == "drained", "lease attempt/workers mismatch")
        durable = json.loads(self.state_path.read_bytes())
        _require(durable == self.state, "persistent lease changed before clean")
        if "auxiliary" in durable:
            _require(scope == "recovery" and type(auxiliary_verifier) is aux.AuxiliaryTransaction and type(recovery_ref) is RecoveryRoundRef,
                     "auxiliary lease requires dedicated recovery and concrete terminal verifier")
            _require(auxiliary_verifier.journal is journal and auxiliary_verifier.declaration == durable["auxiliary"],
                     "auxiliary verifier does not bind this lease/journal")
            manifest = auxiliary_verifier.manifest
            _require(manifest["environment_key"] == self.identity.key
                     and manifest["lease_root"] == str(self.root)
                     and manifest["mode"] == self.mode
                     and (cleanup_only or manifest["source_fingerprints"] == _prepared_sources()),
                     "auxiliary clean environment/root/source mismatch")
            auxiliary_verifier.verify_terminal()
        journal.assert_queue_safe(require_live=self.mode == "controlled_pilot")
        if scope == "recovery":
            self._mark_clean_recovery(journal, quality, recovery_ref=recovery_ref)
            return
        if self.mode == "controlled_pilot":
            context = self.contract.context.to_dict()
            _require(isinstance(quality_rules, q.QualityRules) and quality_rules.sha256 == context["fingerprints"]["quality_rules"],
                     "live recovery requires the contract's quality rules")
            policy = quality_rules.to_dict()
            _require(all(value == self.contract.to_dict()["rules"][key] for key, value in policy["rule_refs"].items()),
                     "live recovery quality rule references differ")
            # D19 fix-5 seq1705: the old single bundled message attributed a
            # system_clean failure to "identity/rules/provenance" and sent the
            # repair arc chasing rule-version drift while every identity field
            # matched byte-for-byte. Split the surfaces so the journal names the
            # actual failing predicate; every predicate is unchanged.
            _require(type(quality) is dict, "live recovery report is not an object")
            _require(quality.get("schema_version") == "rq4-collect/quality-report-v1"
                     and all(quality.get(key) == self.reference[key] for key in ("contract_sha256", "run_id", "attempt_id")),
                     "live recovery report identity mismatch")
            _require(quality.get("rules_sha256") == context["fingerprints"]["quality_rules"]
                     and quality.get("evidence_kind") == "observed",
                     "live recovery report rules/provenance mismatch")
            _require(quality.get("system_clean") == "CLEAN",
                     "live recovery report system cleanliness not confirmed")
            groups = quality.get("groups", {})
            _require(type(groups) is dict and type(groups.get("PROTOCOL")) is dict and type(groups.get("Q2")) is dict,
                     "live recovery report groups missing")
            protocol, recovery = groups["PROTOCOL"].get("checks"), groups["Q2"].get("checks")
            _require(type(protocol) is list and type(recovery) is list and bool(recovery)
                     and all(type(item) is dict for item in protocol + recovery), "live recovery checks missing")
            required = {"rule_binding", "evidence_identity", "artifact_attempt", "observed_provenance", "nested_provenance", "evidence_file"}
            for name in required:
                matches = [item for item in protocol if item.get("check") == name]
                _require(len(matches) == 1 and matches[0].get("group") == "PROTOCOL" and matches[0].get("status") == "PASS",
                         "live recovery protocol binding not verified")
            expected_recovery = {"component_rule_coverage", "metric_rule_coverage", "items.checksum", "inventory.checksum", "owned_database_locks"}
            expected_recovery.update(f.fault_instance_id + ".restored_fields" for f in self.contract.faults)
            expected_recovery.update(entity + ".component" for entity in policy["recovery"]["component_sources"])
            expected_recovery.update("metric." + str(index) for index in range(len(policy["recovery"]["metric_rules"])))
            names = [item.get("check") for item in recovery]
            _require(len(names) == len(expected_recovery) and all(type(name) is str for name in names)
                     and set(names) == expected_recovery and groups["Q2"].get("status") == "PASS"
                     and all(item.get("group") == "Q2" and item.get("status") == "PASS" for item in recovery),
                     "live recovery Q2 check set incomplete or not passed")
            raw = _json_bytes(quality)
            receipts = [row["payload"] for row in journal.records() if row["event"] == "artifact_complete"
                        and row["payload"].get("artifact_id") == "quality-result"]
            _require(len(receipts) == 1 and receipts[0]["relative_path"] == "artifacts/quality/result.json"
                     and receipts[0]["sha256"] == _hash(raw) and receipts[0]["bytes"] == len(raw),
                     "live recovery report lacks this attempt's completed artifact")
            report_path = journal.path / receipts[0]["relative_path"]
            _require(report_path.resolve() == report_path and report_path.read_bytes() == raw,
                     "live recovery report persisted bytes mismatch")
            self.state["quality_report_sha256"] = _hash(raw)
        self.state.update(status="clean", cleanup_scope="live_qualified" if self.mode == "controlled_pilot" else "offline_simulation_only")
        self.save()

    def _mark_clean_recovery(self, journal: j.AttemptJournal, quality: dict[str, Any], *, recovery_ref=None) -> None:
        """Release gate of the recovery-qualification workflow only.

        A recovery report can never satisfy or bypass the sample-quality gate
        above: it carries exactly one RECOVERY group, its sample eligibility
        stays NOT_ASSESSED, and the lease records a separate recovery hash and
        cleanup scope, so the live sampling path keeps its exact complete Q2 set.
        """
        expected_kind = "observed" if self.mode == "controlled_pilot" else "synthetic"
        _require(type(quality) is dict and quality.get("schema_version") == "rq4-collect/quality-report-v1"
                 and all(quality.get(key) == self.reference[key] for key in ("contract_sha256", "run_id", "attempt_id"))
                 and quality.get("rules_sha256") == self.contract.context.to_dict()["fingerprints"]["quality_rules"]
                 and quality.get("evidence_kind") == expected_kind and quality.get("system_clean") == "CLEAN"
                 and quality.get("sample_eligibility") == "NOT_ASSESSED" and quality.get("formal_release_eligible") is False,
                 "recovery qualification report identity/rules/provenance mismatch")
        groups = quality.get("groups", {})
        _require(type(groups) is dict and set(groups) == {"RECOVERY"} and type(groups["RECOVERY"]) is dict,
                 "recovery qualification must carry only the RECOVERY group")
        checks = groups["RECOVERY"].get("checks")
        _require(type(checks) is list and bool(checks) and all(type(item) is dict for item in checks),
                 "recovery qualification checks missing")
        names = [item.get("check") for item in checks]
        _require(len(names) == len(RECOVERY_CHECKS) and all(type(name) is str for name in names)
                 and set(names) == set(RECOVERY_CHECKS) and groups["RECOVERY"].get("status") == "PASS"
                 and all(item.get("group") == "RECOVERY" and item.get("status") == "PASS" for item in checks),
                 "recovery qualification check set incomplete or not passed")
        raw = _json_bytes(quality)
        if recovery_ref is None:
            report_id, report_path = "recovery-qualification", "artifacts/recovery/qualification.json"
        else:
            _require(type(recovery_ref) is RecoveryRoundRef and "auxiliary" in self.state, "prepared recovery round required")
            recovery_ref.verify(self, journal, quality)
            report_id, report_path = recovery_ref.report_receipt["artifact_id"], recovery_ref.report_receipt["relative_path"]
        receipts = [row["payload"] for row in journal.records() if row["event"] == "artifact_complete"
                    and row["payload"].get("artifact_id") == report_id]
        _require(len(receipts) == 1 and receipts[0]["relative_path"] == report_path
                 and receipts[0]["sha256"] == _hash(raw) and receipts[0]["bytes"] == len(raw),
                 "recovery qualification lacks this attempt's completed artifact")
        report_path = journal.path / receipts[0]["relative_path"]
        _require(report_path.resolve() == report_path and report_path.read_bytes() == raw,
                 "recovery qualification report persisted bytes mismatch")
        self.state["recovery_qualification_sha256"] = _hash(raw)
        self.state.update(status="clean",
                          cleanup_scope="live_recovery_qualified" if self.mode == "controlled_pilot" else "offline_recovery_qualified")
        self.save()

    def close(self) -> None:
        if not getattr(self, "_closed", True):
            self._closed = True
            j._unlock_file(self._fd)


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def epoch(self) -> float:
        return time.time()

    def wait_until(self, deadline: float, *, stop_event: threading.Event | None = None, passive: bool = False) -> bool:
        while True:
            if stop_event is not None and stop_event.is_set():
                return False
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                return True
            if stop_event is not None:
                stop_event.wait(min(remaining, .05))
            else:
                time.sleep(min(remaining, .05))


@dataclass(frozen=True)
class ClockAnchor:
    monotonic_s: float
    unix_epoch_s: float
    uncertainty_s: float

    def epoch_at(self, monotonic_s: float) -> float:
        return round(self.unix_epoch_s + monotonic_s - self.monotonic_s, 6)


@dataclass(frozen=True)
class DeadlinePolicy:
    warmup_s: float
    phase_start_lateness_s: float
    action_start_lateness_s: float
    action_duration_s: float
    worker_join_timeout_s: float
    telemetry_ingestion_wait_s: float
    clock_uncertainty_s: float
    environment_max_age_s: float
    first_injection_start_lateness_s: float | None = None

    def __post_init__(self) -> None:
        for name, value in self.__dict__.items():
            if name == "first_injection_start_lateness_s" and value is None:
                continue
            _number(value, positive=name in {"action_duration_s", "worker_join_timeout_s", "environment_max_age_s"})
        if self.first_injection_start_lateness_s is not None:
            _number(self.first_injection_start_lateness_s)
            _require(self.first_injection_start_lateness_s >= self.action_start_lateness_s,
                     "first-injection start allowance cannot be below the ordinary action-start allowance")
            _require(self.first_injection_start_lateness_s <= 2 * self.action_start_lateness_s,
                     "first-injection start allowance cannot exceed 2x the ordinary action-start allowance")


@dataclass(frozen=True)
class RunSettings:
    environment: EnvironmentIdentity
    lease_root: Path
    output_policy: j.OutputPolicy
    deadlines: DeadlinePolicy
    workload_policy: w.HttpPolicy
    annotation_policy: a.TimingPolicy
    mode: str
    max_artifact_bytes: int
    max_total_artifact_bytes: int


@dataclass(frozen=True)
class TelemetryInputs:
    metric_queries: tuple[t.MetricQuery, ...]
    trace_queries: tuple[t.TraceQuery, ...]
    log_sources: tuple[t.PodLogSource, ...]
    metric_backend: Any
    trace_backend: Any
    log_backend: Any
    limits: t.CollectionLimits
    # Optional rollout-surviving live log capture controller (L2 transport).
    # None keeps the historical post-hoc log backend behavior unchanged.
    log_capture: Any = None

    def collect(self, scope: t.PhaseScope) -> dict[str, dict[str, Any]]:
        return t.collect_independent({
            "metrics": lambda: t.collect_prom(scope, self.metric_queries, self.metric_backend, self.limits),
            "traces": lambda: t.collect_jaeger(scope, self.trace_queries, self.trace_backend, self.limits),
            "logs": lambda: t.collect_logs(scope, self.log_sources, self.log_backend, self.limits)}, max_workers=3)


@dataclass(frozen=True)
class RunServices:
    primitive_client: p.CommandClient
    pinned_targets: tuple[tuple[str, p.PinnedTarget], ...]
    kubectl: str
    command_timeout_s: float
    command_wait_timeout_s: int
    telemetry: TelemetryInputs
    environment_reader: Callable[[], dict[str, Any]]
    quality_rules: q.QualityRules
    operation_source_id: str
    supplement: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    # Optional database-leg services (collection): db_table_lock faults validate
    # through db_bindings and will execute through db_client in later slices;
    # the defaults keep every pre-existing construction unchanged.
    db_bindings: tuple[tuple[str, d.DbBinding], ...] = ()
    db_client: d.DbCommandClient | None = None
    
    # the extended executor modules). None resolves to this runner's own
    # repository; offline tests point it at a mirrored fake asset tree. Only
    # the formal entry path reads it; smoke/pilot never hash any assets.
    formal_asset_root: Path | None = None
    
    phase_observer: Any = None


def _first_injection_start_exception(events, fault_specs, deadlines: DeadlinePolicy) -> dict[str, Any] | None:
    """Describe the legacy one-event timing allowance without inventing a failure.

    The identity restriction remains; a negative estimated successor margin is
    an engineering warning. Actual injection/phase/recovery bounds govern safety.
    """
    allowance = deadlines.first_injection_start_lateness_s
    if allowance is None:
        return None
    ordered = sorted(events)
    _require(len(ordered) >= 2, "first-injection exception requires a following scheduled action")
    first = ordered[0]
    first_offset_injects = [event for event in ordered
                            if event[0] == first[0] and event[3] == "inject"]
    _require(first[3] == "inject" and len(first_offset_injects) == 1
             and first_offset_injects[0] == first,
             "first-injection exception requires a unique earliest event that is an inject")
    fault = fault_specs.get(first[4])
    _require(type(fault) is dict and fault.get("mechanism") == "chaos_mesh"
             and fault.get("fault_type") == "service_cpu_saturation",
             "first-injection exception currently requires a chaos-mesh service_cpu_saturation action")
    successor = ordered[1]
    gap = successor[0] - first[0]
    _require(gap > 0, "first-injection exception requires a positive gap to the next action")
    margin = (gap + deadlines.action_start_lateness_s
              - (allowance + deadlines.action_duration_s))
    result = {"instance_id": first[4], "offset_s": first[0], "allowance_s": allowance,
            "ordinary_allowance_s": deadlines.action_start_lateness_s,
            "action_duration_budget_s": deadlines.action_duration_s,
            "next_event": {"action": successor[3], "instance_id": successor[4], "offset_s": successor[0]},
            "gap_to_next_s": gap, "downstream_margin_s": margin}
    _record_timing_budget_warning(result, "first_injection_serial_reservation_s",
                                  allowance + deadlines.action_duration_s,
                                  gap + deadlines.action_start_lateness_s)
    return result


def validate_run(contract: c.RunContract, settings: RunSettings, services: RunServices) -> dict[str, Any]:
    """No attempts/leases/threads/commands/HTTP are created by this function."""
    plan, context = contract.to_dict(), contract.context.to_dict()
    action_events = []
    fault_specs = {}
    for order, fault in enumerate(contract.faults):
        data = fault.to_dict()
        fault_specs[fault.fault_instance_id] = data
        action_events.extend([(fault.planned_window.start, 1, order, "inject", fault.fault_instance_id),
                              (fault.planned_window.end, 0, order, "recover", fault.fault_instance_id)])
    first_injection_exception = _first_injection_start_exception(action_events, fault_specs, settings.deadlines)
    _require(plan["purpose"] in {"smoke", "pilot", "formal"}, "unsupported execution purpose")
    if plan["purpose"] == "formal":
        # G16-G18 hard conditions at the formal entry (preview and execute
        # both pass validate_run first, so all three entry points inherit).
        _validate_release_entry(contract, services)
    _require(settings.mode in {"isolated_test", "controlled_pilot"}, "explicit execution mode required")
    expected_kind = "fake" if settings.mode == "isolated_test" else "live"
    _require(services.primitive_client.evidence_kind == expected_kind, "primitive provenance differs from execution mode")
    _require(settings.workload_policy.traffic_purpose == ("isolated_test" if settings.mode == "isolated_test" else "read_only_business"),
             "HTTP traffic policy differs from execution mode")
    _require(callable(services.environment_reader) and isinstance(services.quality_rules, q.QualityRules), "explicit environment reader and quality rules required")
    _require(services.quality_rules.sha256 == context["fingerprints"]["quality_rules"], "quality rule fingerprint mismatch")
    _require(isinstance(services.telemetry, TelemetryInputs) and bool(services.telemetry.metric_queries)
             and bool(services.telemetry.trace_queries) and bool(services.telemetry.log_sources), "all telemetry modalities require explicit sources")
    _require(all(isinstance(query, t.MetricQuery) and query.source_id == getattr(services.telemetry.metric_backend, "source_id", None)
                 and query.step_s == plan["metric_interval_s"] for query in services.telemetry.metric_queries), "metric source/interval mismatch")
    _require(all(isinstance(query, t.TraceQuery) and query.source_id == getattr(services.telemetry.trace_backend, "source_id", None)
                 for query in services.telemetry.trace_queries), "trace source mismatch")
    _require(all(isinstance(source, t.PodLogSource) for source in services.telemetry.log_sources)
             and callable(getattr(services.telemetry.log_backend, "fetch_logs", None)), "log source/backend mismatch")
    _require(services.telemetry.log_capture is None
             or all(callable(getattr(services.telemetry.log_capture, name, None)) for name in ("start_phase", "bind_journal")),
             "live log capture controller protocol invalid")
    _require(services.phase_observer is None or callable(getattr(services.phase_observer, "start_phase", None)),
             "phase observer start protocol invalid")
    _require(isinstance(services.operation_source_id, str) and bool(services.operation_source_id), "operation source required")
    for limit in (settings.max_artifact_bytes, settings.max_total_artifact_bytes):
        _require(type(limit) is int and limit > 0, "artifact budgets required")
    settings.output_policy.attempt_path(contract)
    lease_root = Path(settings.lease_root)
    _require(lease_root.is_absolute() and ".." not in lease_root.parts and lease_root.resolve() == lease_root,
             "explicit unredirected lease root required")
    _require(not j._within(lease_root, settings.output_policy.approved_real)
             and not j._within(settings.output_policy.approved_real, lease_root), "lease state must be separate from data output")
    _require(all(not j._within(lease_root, root.resolve()) and not j._within(root.resolve(), lease_root)
                 for root in settings.output_policy.protected_roots), "lease root overlaps protected data")
    pins = dict(services.pinned_targets)
    db_pins = dict(services.db_bindings)
    _require(len(pins) == len(services.pinned_targets) and len(db_pins) == len(services.db_bindings),
             "duplicate instance pins are not allowed")
    _require(not set(pins) & set(db_pins), "one instance must not carry both a Kubernetes pin and a database binding")
    _require(set(pins) | set(db_pins) == {f.fault_instance_id for f in contract.faults}, "all instance pins required")
    # Mechanism dispatch happens here because neither the contract layer nor
    # primitives.py whitelists mechanism strings: a gateway/db spec reaching
    # p.prepare_fault below would be refused as an unsupported primitive.
    fault_legs: dict[str, str] = {}
    for spec in contract.faults:
        fid = spec.fault_instance_id
        data = spec.to_dict()
        if data["mechanism"] == g.MECHANISM:
            _require(fid in pins, "gateway rollout mechanism requires a pinned Kubernetes target")
            pin = pins[fid]
            _require(pin.context == context["kube_context"] and pin.namespace == context["namespace"], "pin context mismatch")
            g.prepare_gateway(spec, pin)
            _require(services.primitive_client.supports_delete_preconditions is True
                     and getattr(services.primitive_client, "supports_server_dry_run", False) is True,
                     "gateway rollout capabilities unavailable")
            fault_legs[fid] = "gateway"
        elif data["fault_type"] == "db_table_lock":
            _require(fid in db_pins, "db_table_lock requires an explicit database binding, not a Kubernetes pin")
            d.prepare_db_lock(spec, db_pins[fid], ownership="validation-only")
            _require(services.db_client is not None, "db_table_lock requires an explicit database client")
            _require(services.db_client.evidence_kind == expected_kind, "database client provenance differs from execution mode")
            fault_legs[fid] = "db"
        else:
            _require(fid in pins, "Kubernetes fault mechanism requires a pinned target, not a database binding")
            binding = pins[fid]
            _require(binding.context == context["kube_context"] and binding.namespace == context["namespace"], "pin context mismatch")
            prepared = p.prepare_fault(spec, binding, ownership="validation-only")
            _require(prepared.mode != "gateway_prepare_only",
                     "gateway primitive is not executable; use the nginx_configmap_rollout mechanism")
            if prepared.mode == "chaos_crd":
                _require(services.primitive_client.supports_delete_preconditions is True
                         and getattr(services.primitive_client, "supports_server_dry_run", False) is True, "CRD capabilities unavailable")
            fault_legs[fid] = "k8s"
    for phase in _PHASES:
        w.prepare_workload(contract, phase, settings.workload_policy)
    _require(settings.deadlines.worker_join_timeout_s > max(s["timeout_s"] for s in plan["request_profile"]["streams"]),
             "worker join budget must exceed request timeout")
    _require(settings.deadlines.worker_join_timeout_s > settings.deadlines.telemetry_ingestion_wait_s,
             "join budget must exceed ingestion wait")
    preview = {"schema_version": RUNNER_SCHEMA, "status": "validated_not_executed", "contract_sha256": contract.sha256,
               "qualification": "not_assessed", "formal_release_eligible": False,
               "fault_instance_ids": sorted(set(pins) | set(db_pins)), "fault_legs": fault_legs}
    if first_injection_exception is not None:
        preview["first_injection_start_exception"] = first_injection_exception
    return preview


def _formal_asset_root() -> Path:
    """Repository root hosting this runner (scripts/collection/..)."""
    return Path(__file__).resolve().parents[2]


def _formal_asset_specs(root: Path) -> tuple[rg.AssetSpec, ...]:
    """Build the formal source manifest, including every required executor module."""
    package = Path(root) / 'scripts/collection'
    defaults = rg.default_asset_specs(Path(root))
    labels = {spec.label for spec in defaults}
    return defaults + tuple(
        rg.AssetSpec(label, package / (label.split(".", 1)[1] + ".py"), rg.SHARED_CORE)
        for label in _FORMAL_REQUIRED_ASSET_LABELS + _FORMAL_SUGGESTED_ASSET_LABELS if label not in labels)


def _read_release_json(path: Path, what: str) -> dict[str, Any]:
    _require(path.is_file(), what + " missing from the release-gate directory")
    try:
        value = json.loads(path.read_bytes().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise RunnerError(what + " is not valid UTF-8 JSON") from None
    _require(type(value) is dict, what + " must be a JSON object")
    return value


def _validate_release_entry(contract: c.RunContract, services: RunServices | None = None) -> dict[str, Any] | None:
    """Validate formal entry against its source-bound release evidence.

    Four elements, in the order enforced:
    (i)   ONE on-site per-asset ``_hash_file`` collection -- "the same point
          in time" is exactly that single collection, never a re-stamped
          bundle;
    (ii)  the bundle consumed by re-derivation and verify_release is pinned
          to the stored record: ``FingerprintBundle.from_dict(record["fingerprints"])``
          with its timestamp pinned by the stored record and its seal
          self-verified by from_dict. A freshly stamped
          ``compute_fingerprint_bundle(collected_at_utc=now)`` must NEVER be
          fed to the comparison: collected_at_utc participates in the bundle
          seal, so a fresh timestamp would reject every honest entry;
    (iii) honest drift detection: the on-site per-asset hash/bytes are
          compared for equality against the stored bundle, asset by asset --
          real staleness (changed asset bytes) is rejected here, timestamps
          never enter the comparison;
    (iv)  asset-label set assertion: the stored asset-label set must cover
          the mandatory extension set and equal the on-site manifest label
          set (added or removed assets require a newly prepared source binding).

    The ledger sidecar is replayed record by record through register() (each
    record bound to its stored bundle) and evaluate_release() must reproduce
    the stored release record as full dictionaries -- explicitly including
    invalidation_history event by event -- before verify_release() must
    report ok. Class-2 residuals (deleted closures / hostile rewrites that
    only the ledger reveals) are NOT exemptable in code (O-collection: default no
    exemption; any written exemption is a root-recorded decision outside
    this module). The uri is interpreted as a local directory path, optionally
    ``file:``-prefixed; the record bytes must match release_gate_ref.sha256.

    collection: the same ``release_gate_ref`` directory may instead carry the
    FAST16 staged-admission layout (fast16-ledger.json + fingerprints.json).
    The two layouts are mutually exclusive (an ambiguous directory is
    refused); the FAST16 branch is :func:`_validate_fast16_entry` and returns
    the admission decision instead of None.
    """
    reference = contract.to_dict()["release_gate_ref"]
    _require(reference is not None, "formal execution requires a release_gate_ref")
    location = reference["uri"][5:] if reference["uri"].startswith("file:") else reference["uri"]
    directory = Path(location)
    _require(directory.is_absolute() and ".." not in directory.parts and directory.resolve() == directory,
             "release gate URI must be an explicit absolute local directory")
    repeat_marker = directory / "repeat-admission.json"
    if repeat_marker.is_file():
        _require(not any((directory / name).exists() for name in
                         (_FORMAL_FAST16_LEDGER_FILE, _FORMAL_FAST16_FINGERPRINTS_FILE,
                          _FORMAL_RELEASE_RECORD_FILE)),
                 "release gate directory mixes repeat-admission and legacy gate layouts")
        from . import validate_campaign
        verdict = validate_campaign.validate_contract(contract, directory, services=services)
        _require(verdict.get("ok") is True,
                 "repeat admission rejected the entry: "
                 + ",".join(verdict.get("errors", [])))
        return {"repeat_admission": verdict}
    if ((directory / _FORMAL_FAST16_LEDGER_FILE).is_file()
            or (directory / _FORMAL_FAST16_FINGERPRINTS_FILE).is_file()):
        _require(not (directory / _FORMAL_RELEASE_RECORD_FILE).is_file(),
                 "release gate directory mixes the FAST16 and released layouts")
        return _validate_fast16_entry(contract, services, directory)
    record_path = directory / _FORMAL_RELEASE_RECORD_FILE
    _require(record_path.is_file(), "release record missing from the release-gate directory")
    raw = record_path.read_bytes()
    _require(_hash(raw) == reference["sha256"], "release record bytes do not match release_gate_ref sha256")
    record = _read_release_json(record_path, "release record")
    asset_root = getattr(services, "formal_asset_root", None)
    root = Path(asset_root) if asset_root is not None else _formal_asset_root()
    _require(root.is_absolute() and root.is_dir(), "formal asset root must be an absolute existing directory")
    specs = _formal_asset_specs(root)
    try:
        # Element (ii): consume the bundle pinned to the stored record only.
        bundle = rg.FingerprintBundle.from_dict(record.get("fingerprints"))
        stored_assets = bundle.assets
        # Element (iv): label-set assertion (mandatory extension first, then
        
        missing = [label for label in _FORMAL_REQUIRED_ASSET_LABELS if label not in stored_assets]
        _require(not missing, "release fingerprint bundle lacks a mandatory extended asset (module.primitives_db)")
        _require(set(stored_assets) == {spec.label for spec in specs},
                 "release fingerprint asset set differs from the on-site manifest")
        # Elements (i)+(iii): one on-site collection, per-asset equality.
        for spec in specs:
            digest, size = rg._hash_file(spec.path)
            entry = stored_assets[spec.label]
            _require(digest == entry["sha256"] and size == entry["bytes"],
                     "formal entry asset drift: " + spec.label)
        coverage = rg.ExpectedCoverage.from_dict(
            _read_release_json(directory / _FORMAL_COVERAGE_FILE, "expected coverage"))
        sidecar = _read_release_json(directory / _FORMAL_LEDGER_FILE, "evidence ledger sidecar")
        _require(type(sidecar) is dict and set(sidecar) ==
                 {"schema_version", "records", "invalidation_events", "bundles"}
                 and sidecar["schema_version"] == _FORMAL_LEDGER_SIDECAR_SCHEMA
                 and type(sidecar["records"]) is list and type(sidecar["invalidation_events"]) is list
                 and type(sidecar["bundles"]) is list, "evidence ledger sidecar: unexpected shape")
        bundles: dict[str, rg.FingerprintBundle] = {}
        for value in sidecar["bundles"]:
            pinned = rg.FingerprintBundle.from_dict(value)
            _require(pinned.bundle_sha256 not in bundles, "evidence ledger sidecar: duplicate bundle")
            bundles[pinned.bundle_sha256] = pinned
        ledger = rg.EvidenceLedger(coverage)
        for stored in sidecar["records"]:
            _require(type(stored) is dict, "evidence ledger sidecar: records must be objects")
            attempt = stored.get("attempt")
            if attempt is not None:
                _require(type(attempt) is dict and isinstance(attempt.get("evidence_root"), str),
                         "evidence ledger sidecar: record attempt shape invalid")
            bound = bundles.get(stored.get("fingerprint_bundle_sha256"))
            _require(bound is not None, "evidence ledger sidecar: record without its bound bundle")
            attempt_root = Path(attempt["evidence_root"]) if attempt is not None else None
            ledger.register(stored, bound, attempt_root=attempt_root)
        for event in sidecar["invalidation_events"]:
            ledger.register_invalidation_event(event)
        # Re-derivation with the stored review envelope; full-dictionary
        
        rederived = rg.evaluate_release(ledger, bundle, review=record.get("review"))
        _require(rederived == record,
                 "release record does not equal its ledger re-derivation (invalidation_history included)")
        verdict = rg.verify_release(record, bundle, coverage)
        _require(verdict.get("ok") is True,
                 "release gate verification failed: " + ",".join(verdict.get("reasons") or ("unknown",)))
    except (rg.GateError, ValueError, TypeError, KeyError) as exc:
        raise RunnerError("formal release gate rejected the entry: " + _error_summary(exc)) from None


def _validate_fast16_entry(contract: c.RunContract, services: RunServices | None,
                           directory: Path) -> dict[str, Any]:
    """collection FAST16 staged-admission entry for the formal purpose (user ruling
    2026-09-21T01:24 item 5): connect the FAST16 staged admission to real
    execution with three distinct stages and no circular first entry.

    First admission -- the ladder (OFFLINE_COMPLETE + REPRESENTATIVE_VERIFIED)
    admits ONE first formal execution of a condition that has NO own-history
    requirement (the first case never demands its own completion results);
    a condition whose first-candidate PASS is already registered but not yet
    verified gets no second admission until its post-run acceptance is
    registered. Post-run acceptance -- the executed attempt's technical QC
    (including the Q2 recovery group) is bound to intact attempt bytes at
    registration time by Fast16Admission.register; a passing first-candidate
    plus condition_verified registration moves the condition into the formal
    repetition pool. Repetition/expansion -- a further formal execution of a
    condition requires its CONDITION_VERIFIED registration, and before the
    BLOCK_EXPANSION stage is reached no formal run may target a condition
    outside the declared first block.

    Consumption contract (N1): the sealed ledger is loaded from disk via
    Fast16Admission.load (any edit breaks the ledger seal), the fingerprint
    bundle is pinned to the fingerprints.json bytes bound by
    release_gate_ref.sha256, and the on-site asset state is compared per
    asset (hash + bytes) against that bundle -- fingerprint drift is refused
    naming the drifted asset, never by a timestamp. There is no force/skip
    parameter; nothing here registers or rewrites ladder stages.
    """
    reference = contract.to_dict()["release_gate_ref"]
    prints_path = directory / _FORMAL_FAST16_FINGERPRINTS_FILE
    ledger_path = directory / _FORMAL_FAST16_LEDGER_FILE
    _require(prints_path.is_file(), "FAST16 fingerprints missing from the admission directory")
    _require(ledger_path.is_file(), "FAST16 ledger missing from the admission directory")
    raw = prints_path.read_bytes()
    _require(_hash(raw) == reference["sha256"],
             "FAST16 fingerprint bytes do not match release_gate_ref sha256")
    asset_root = getattr(services, "formal_asset_root", None)
    root = Path(asset_root) if asset_root is not None else _formal_asset_root()
    _require(root.is_absolute() and root.is_dir(), "formal asset root must be an absolute existing directory")
    specs = _formal_asset_specs(root)
    try:
        bundle = rg.FingerprintBundle.from_json(raw.decode("utf-8"))
        stored_assets = bundle.assets
        _require(set(stored_assets) == {spec.label for spec in specs},
                 "FAST16 fingerprint asset set differs from the on-site manifest")
        for spec in specs:
            digest, size = rg._hash_file(spec.path)
            entry = stored_assets[spec.label]
            _require(digest == entry["sha256"] and size == entry["bytes"],
                     "FAST16 admission asset drift: " + spec.label)
        admission = rg.Fast16Admission.load(ledger_path)
        
        # record structure and the ledger seal only; every run-kind PASS
        # record's technical quality bytes are re-read from its recorded
        
        # self-resealed) ledger without intact on-disk evidence cannot reach
        # any stage at consumption time. This mirrors, never replaces, the
        # registrar's own registration-time binding.
        for record in admission.records:
            if record["status"] != "PASS" or record["kind"] not in rg.FAST16_PURPOSE:
                continue
            rg._verify_fast16_attempt_and_quality(
                record, Path(record["attempt"]["evidence_root"]), rg.FAST16_PURPOSE[record["kind"]])
        expected = admission.expected
        data = contract.to_dict()
        _require(expected.design_version == data["scenario"]["design_version"]
                 and expected.catalog_source == data["catalog_source"],
                 "FAST16 expected coverage binds a different catalog/design version")
        condition_key = c.canonical_sha256(c._normalized_conditions(contract))
        key = c.canonical_sha256({"design_version": data["scenario"]["design_version"],
                                  "scenario_id": data["scenario"]["scenario_id"],
                                  "condition_key": condition_key})
        items = expected.item_by_key()
        _require(key in items, "formal contract does not bind an expected FAST16 coverage condition "
                               "(scenario=" + data["scenario"]["scenario_id"] + ")")
        state = rg.fast16_stage_state(admission, bundle)
        verdict = rg.verify_fast16_stage(state, bundle, expected, "REPRESENTATIVE_VERIFIED")
        if not verdict["ok"]:
            detail = list(verdict["reasons"])
            for stage in ("OFFLINE_COMPLETE", "REPRESENTATIVE_VERIFIED"):
                detail.extend(stage + ":" + reason
                              for reason in state["stages"][stage]["reasons"][:8])
            _require(False, "FAST16 admission ladder not reached: " + ",".join(detail[:24]))
        return _fast16_entry_decision(admission, bundle, expected, key, state)
    except RunnerError:
        raise
    except (rg.GateError, ValueError, TypeError, KeyError) as exc:
        raise RunnerError("FAST16 admission rejected the entry: " + _error_summary(exc)) from None


def _fast16_entry_decision(admission: rg.Fast16Admission, fingerprints: rg.FingerprintBundle,
                           expected: rg.ExpectedCoverage, key: str,
                           state: dict[str, Any]) -> dict[str, Any]:
    """Classify one formal execution against the staged admission ladder.

    Repetition wins over first-admission bookkeeping: a currently-valid
    condition_verified PASS admits further repetitions. A condition with a
    currently-valid first-candidate PASS but no verification is refused -- its
    post-run acceptance (the condition_verified registration over the SAME
    attempt evidence) must complete first; an unclosed first-candidate FAIL
    keeps the policy-retry door open exactly like the registrar's closure
    semantics. The declared first block disciplines every fresh admission
    before BLOCK_EXPANSION is reached. Decisions reuse the registrar's own
    validity computation under the CURRENT bundle; no stage is re-derived
    differently here.
    """
    items = expected.item_by_key()
    _require(key not in state["stages"]["OFFLINE_COMPLETE"]["held_pending_keys"],
             "FAST16 admission refused: condition remains under admission_hold")
    scenario = items[key]["scenario_id"]
    base = {"fingerprint_bundle_sha256": fingerprints.bundle_sha256,
            "coverage_key": key, "scenario_id": scenario,
            "ladder_position": state["ladder_position"]}
    if rg._fast16_latest_valid_pass(admission, "condition_verified", key, fingerprints, items) is not None:
        return {**base, "fast16_admission": "repetition"}
    events = rg._fast16_kind_events(admission, "first_candidate", key, fingerprints, items)
    passes = [event for event in events
              if event[1]["status"] == "PASS" and str(event[2]).startswith("valid_")]
    failures = [event for event in events if event[1]["status"] == "FAIL"]
    if passes and (not failures or passes[-1][0] >= failures[-1][0]):
        _require(False, "FAST16 admission refused: first candidate for " + scenario + " already executed; "
                         "register its post-run acceptance (CONDITION_VERIFIED over the same attempt "
                         "evidence) before further formal runs of this condition")
    if failures:
        return {**base, "fast16_admission": "first_candidate_retry",
                "unclosed_failure_record": failures[-1][1]["record_id"]}
    claims = rg._fast16_claims(admission)
    if claims:
        _, claim = claims[-1]
        validity = rg._fast16_record_validity(
            claim, admission.bundle(claim["fingerprint_bundle_sha256"]), fingerprints, items)
        if (all(str(value).startswith("valid_") for value in validity.values())
                and key not in claim["coverage_keys"]):
            verdict = rg.verify_fast16_stage(state, fingerprints, expected, "BLOCK_EXPANSION")
            detail = list(verdict["reasons"])
            detail.extend("BLOCK_EXPANSION:" + reason
                          for reason in state["stages"]["BLOCK_EXPANSION"]["reasons"][:8])
            _require(verdict["ok"], "FAST16 block discipline: formal run outside the declared first "
                     "block requires completed block expansion (" + scenario + "): "
                     + ",".join(detail[:16]))
    return {**base, "fast16_admission": "first_candidate"}


def _owner_aware_environment_read(reader: Callable[..., dict[str, Any]], owner_id: str) -> dict[str, Any]:
    """collection: hand the contract owner_id to readers that accept it.

    The reader call chain stays explicit (no global state): the runner owns the
    contract, so it is the layer that knows owner_id. Readers whose signature
    still declares no ``expected_owner`` parameter keep their exact HEAD call
    shape, which keeps every pre-collection callable byte-identical in behavior.
    """
    _require(callable(reader), "environment reader must be callable")
    try:
        parameters = inspect.signature(reader).parameters
    except (TypeError, ValueError):
        parameters = {}
    owner_aware = any(name == "expected_owner" or parameter.kind is inspect.Parameter.VAR_KEYWORD
                      for name, parameter in parameters.items())
    return reader(expected_owner=owner_id) if owner_aware else reader()


def _environment_check(contract: c.RunContract, settings: RunSettings, services: RunServices, clock, *, require_empty: bool) -> dict[str, Any]:
    context = contract.context.to_dict()
    value = _owner_aware_environment_read(services.environment_reader, context["owner_id"])
    _require(isinstance(value, dict) and value.get("cluster_uid") == settings.environment.cluster_uid
             and value.get("namespace_uid") == settings.environment.namespace_uid
             and value.get("namespace") == context["namespace"] and value.get("context") == context["kube_context"],
             "actual environment identity mismatch")
    _require(value.get("evidence_kind") == ("synthetic" if settings.mode == "isolated_test" else "observed"), "environment provenance mismatch")
    _number(value.get("observed_at_epoch_s"))
    _require(0 <= clock.epoch() - value["observed_at_epoch_s"] <= settings.deadlines.environment_max_age_s, "environment identity observation stale")
    _require(isinstance(value.get("residual_owners"), list), "explicit residual owner inventory required")
    
    # carriers into ``examined_own_carriers`` (never residuals); ``residual_owners``
    # keeps its unchanged meaning and require_empty still demands it be empty.
    _require(isinstance(value.get("examined_own_carriers", []), list), "examined own carrier inventory must be a list")
    if require_empty:
        _require(not value["residual_owners"], "environment has unexamined residual owners")
    return value


class _Artifacts:
    def __init__(self, journal: j.AttemptJournal, settings: RunSettings) -> None:
        self.journal, self.settings = journal, settings
        self.descriptors: list[dict[str, Any]] = []
        self.total = 0

    def write(self, artifact_id: str, path: str, raw: bytes, media_type: str = "application/json") -> dict[str, Any]:
        _require(len(raw) <= self.settings.max_artifact_bytes and self.total + len(raw) <= self.settings.max_total_artifact_bytes,
                 "artifact byte budget exceeded")
        receipt = self.journal.write_artifact(artifact_id=artifact_id, relative_path="artifacts/" + path, raw=raw, media_type=media_type)
        self.total += len(raw)
        descriptor = {key: receipt[key] for key in ("artifact_id", "relative_path", "sha256")}
        self.descriptors.append(descriptor)
        return descriptor

    def json(self, artifact_id: str, path: str, value: Any) -> dict[str, Any]:
        return self.write(artifact_id, path, _json_bytes(value))

    def bundle(self, phase: str, modality: str, bundle: dict[str, Any]) -> tuple[dict[str, Any], str]:
        value = json.loads(_json_bytes(bundle))
        _require(value.get("schema_version") == t.SCHEMA_VERSION and value.get("modality") == modality,
                 "invalid telemetry bundle schema/modality")
        prefix = phase + "." + modality
        base = phase + "/" + modality
        records = _json_bytes(value["records"])
        _require(_hash(records) == value["manifest"]["projection_sha256"], "telemetry projection hash mismatch")
        refs = set()
        mapping = {}
        for index, raw in enumerate(value["raw_responses"]):
            ref = raw["ref"]
            _require(ref == "memory:raw:" + str(index) and ref not in refs, "bundle-local raw reference mismatch")
            refs.add(ref)
            data = base64.b64decode(raw["base64"], validate=True)
            _require(len(data) == raw["retained_bytes"] and _hash(data) == raw["retained_sha256"], "telemetry raw byte/hash mismatch")
            receipt = self.write(prefix + ".raw-" + str(index), base + "/raw-" + str(index) + ".bin", data, "application/octet-stream")
            raw.update(persistence="written", file_path=receipt["relative_path"], file_sha256=receipt["sha256"])
            mapping[ref] = receipt
        projection = self.write(prefix + ".projection", base + "/projection.json", records)
        value["manifest"].update(persistence="written", file_path=projection["relative_path"], file_sha256=projection["sha256"])
        bundle_id = prefix + ".bundle"
        receipt = self.json(bundle_id, base + "/bundle.json", value)
        self.json(prefix + ".storage", base + "/storage.json", {"bundle": receipt, "projection": projection, "raw_refs": mapping})
        return value, bundle_id


class _Phase:
    def __init__(self, contract, phase, start, duration, anchor, settings, services, clock, workload_driver, on_failure=None):
        self.phase, self.start, self.end = phase, start, start + duration
        self.anchor, self.settings, self.clock = anchor, settings, clock
        self.cancel_at = None
        self.stop = threading.Event()
        self.pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="m1-phase-" + phase)
        self.plan = w.prepare_workload(contract, phase, settings.workload_policy)
        self.workload = self.telemetry = None
        self.contract, self.services, self.driver = contract, services, workload_driver
        self.on_failure = on_failure
        self.observer_drained = services.phase_observer is None
        self.logs_drained = services.telemetry.log_capture is None
        self.phase_observations = None
        self.phase_observation_failure = None

    def start_workers(self) -> None:
        # The owner registers this phase before calling start_workers, so a
        # second-submit exception cannot orphan the first future.
        self.workload = self.pool.submit(self.driver, self.plan, execute=True, stop_event=self.stop, t0_monotonic=self.start)
        self.workload.add_done_callback(lambda future: self._worker_done("workload", future))
        self.telemetry = self.pool.submit(self._collect, self.contract, self.services)
        self.telemetry.add_done_callback(lambda future: self._worker_done("telemetry", future))

    def _worker_done(self, kind, future) -> None:
        try:
            if kind == "workload":
                result = self.finish_workload()
                _require(result.get("summary", {}).get("workers_joined") is True
                         and result["summary"].get("driver_healthy") is True, "workload worker failed or is not drained")
            else:
                capture = future.result()
                _check_capture(capture)
        except BaseException:
            if self.on_failure is not None:
                self.on_failure()

    def _collect(self, contract, services):
        capture = services.telemetry.log_capture
        session = observer = None
        # This is separate from the two occupied phase workers (workload and
        # _collect). Sampling must start without waiting for log enumeration.
        startup_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="m1-log-start-" + self.phase) if capture is not None else None
        log_start = None
        try:
            if startup_pool is not None:
                log_start = startup_pool.submit(capture.start_phase, contract, self.phase, self.anchor.epoch_at(self.start))
            if services.phase_observer is not None:
                try:
                    observer = services.phase_observer.start_phase(
                        contract, self.phase, self.anchor.epoch_at(self.start), self.anchor.epoch_at(self.end),
                        {**asdict(self.anchor), "clock_id": contract.context.to_dict()["clock_id"]})
                except obs.PhaseObservationError as exc:
                    self.observer_drained = exc.workers_joined is True
                    self.phase_observation_failure = (exc.context if exc.context is not None else {
                        "phase": self.phase, "reason": str(exc)[:256],
                        "workers_joined": exc.workers_joined is True})
                    raise
                _require(all(callable(getattr(observer, name, None)) for name in ("close", "abort")),
                         "phase observer session protocol invalid")
                if log_start is not None:
                    session = log_start.result()
                boundary_budget = getattr(observer, "boundary_budget_s", 0)
                _require(type(boundary_budget) in (int, float) and math.isfinite(boundary_budget)
                         and boundary_budget >= 0 and boundary_budget < self.end - self.start,
                         "phase boundary snapshot budget invalid")
                if boundary_budget:
                    _require(callable(getattr(observer, "observe_boundary", None)), "boundary snapshot method missing")
                    boundary_at = self.end - boundary_budget
                    # Never delay the main fault scheduler. An observer that
                    # consumed this reserved interval simply lacks a tail proof.
                    if self.clock.monotonic() <= boundary_at and not self.stop.is_set():
                        self.clock.wait_until(boundary_at, stop_event=self.stop, passive=True)
                        if not self.stop.is_set() and self.cancel_at is None and self.clock.monotonic() < self.end:
                            boundary_window = c.TimeWindow(self.anchor.epoch_at(self.start), self.anchor.epoch_at(self.end),
                                "unix_epoch", contract.context.to_dict()["clock_id"])
                            observer.observe_boundary(boundary_window, deadline_epoch_s=boundary_window.end)
            if log_start is not None and session is None:
                session = log_start.result()
            self.clock.wait_until(self.end, stop_event=self.stop, passive=True)
            end = min(self.end, self.cancel_at) if self.cancel_at is not None else self.end
            _require(end > self.start, "zero-duration failed phase has no observation window")
            window = c.TimeWindow(self.anchor.epoch_at(self.start), self.anchor.epoch_at(end), "unix_epoch", contract.context.to_dict()["clock_id"])
            if session is not None:
                # Logs are already streamed: backend ingestion/readback waits
                # must not extend their phase membership into the next fault.
                # Retain the archive's existing end+uncertainty coverage guard,
                # plus the run anchor's mapping uncertainty; no new tolerance.
                uncertainty = getattr(session, "stop_clock_uncertainty_s", 0)
                _require(type(uncertainty) in (int, float) and math.isfinite(uncertainty)
                         and uncertainty >= 0, "phase log stop uncertainty invalid")
                self.clock.wait_until(end + self.anchor.uncertainty_s + uncertainty, passive=True)
                session.close(window)
            self.clock.wait_until(end + self.settings.deadlines.telemetry_ingestion_wait_s, passive=True)
            if observer is not None:
                facts = observer.close(window)
                _require(type(facts) is dict and facts.get("workers_joined") is True,
                         "phase observation workers unconfirmed")
                self.phase_observations = facts
                self.observer_drained = True
            bundles = services.telemetry.collect(t.PhaseScope(contract, self.phase, window))
            return {"actual_window": window.to_dict(), "bundles": bundles, "partial": end < self.end,
                    **({"phase_observations": self.phase_observations} if self.phase_observations is not None else {})}
        finally:
            try:
                # Keep this telemetry future pending until startup really exits.
                # Even an observer failure must retrieve the concurrent log side.
                if log_start is not None and session is None:
                    session = log_start.result()
            finally:
                if startup_pool is not None:
                    startup_pool.shutdown(wait=True)
                try:
                    if observer is not None:
                        self.observer_drained = False
                        result = observer.abort()
                        self.observer_drained = type(result) is dict and result.get("workers_joined") is True
                        _require(self.observer_drained, "phase observer abort not drained")
                finally:
                    if session is None and capture is not None:
                        session = getattr(capture, "sessions", {}).get(self.phase)
                    if session is not None:
                        self.logs_drained = False
                        session.abort()
                        entries = getattr(session, "entries", None)
                        self.logs_drained = type(entries) is dict and all(
                            entry.get("archive") is None or
                            (type(entry.get("stop_result")) is dict and entry["stop_result"].get("workers_joined") is True)
                            for entry in entries.values())
                        _require(self.logs_drained, "phase log archive workers unconfirmed")

    def cancel(self, at: float) -> None:
        if self.cancel_at is None:
            self.cancel_at = at
        self.stop.set()

    def finish_workload(self) -> dict[str, Any]:
        _require(self.workload is not None, "workload worker was not started")
        result = self.workload.result(timeout=self.settings.deadlines.worker_join_timeout_s)
        plan = self.plan.to_dict()
        _require(isinstance(result, dict) and result.get("schema_version") == w.SCHEMA_VERSION
                 and result.get("plan") == plan, "workload returned wrong plan/attempt/phase")
        _require(result.get("execution_mode") == "direct_http" or
                 (self.settings.mode == "isolated_test" and result.get("execution_mode") == "synthetic_no_http"), "workload execution provenance invalid")
        _require(result.get("clock", {}).get("phase_epoch_s") == self.start
                 and result["clock"].get("phase_end_s") == self.end, "workload phase epoch shifted or double-offset")
        rows = result.get("requests")
        _require(isinstance(rows, list) and len(rows) == len(plan["arrivals"]), "workload ledger row count differs from plan")
        expected_ids = {f"{plan['run_id']}/{plan['attempt_id']}/{self.phase}/{arrival['stream_id']}/{arrival['ordinal']:08d}"
                        for arrival in plan["arrivals"]}
        _require(len({row.get("request_id") for row in rows}) == len(rows)
                 and {row.get("request_id") for row in rows} == expected_ids
                 and all(row.get("run_id") == plan["run_id"] and row.get("attempt_id") == plan["attempt_id"]
                         and row.get("phase") == self.phase for row in rows), "workload request identity mismatch")
        return result

    def finish(self) -> tuple[dict[str, Any], dict[str, Any]]:
        workload = self.finish_workload()
        telemetry = self.telemetry.result(timeout=self.settings.deadlines.worker_join_timeout_s)
        self.pool.shutdown(wait=True)
        return workload, telemetry


def _check_capture(capture) -> None:
    for modality, bundle in capture["bundles"].items():
        status = bundle.get("manifest", {}).get("collection_status")
        if bundle.get("schema_version") != t.SCHEMA_VERSION or status in {"failed", "not_run"}:
            query_statuses = sorted({str(row.get("status"))[:32] for row in bundle.get("queries", ())
                                     if type(row) is dict})
            raise RunnerError("independent telemetry worker failed:"
                              + str(modality)[:24] + ":" + str(status)[:24]
                              + ":" + ",".join(query_statuses)[:100])


def _check_telemetry_workers(sessions: list[_Phase]) -> None:
    for session in sessions:
        if session.workload is not None and session.workload.done():
            result = session.finish_workload()
            _require(result.get("summary", {}).get("workers_joined") is True
                     and result["summary"].get("driver_healthy") is True, "workload worker failed or is not drained")
        if session.telemetry is None or not session.telemetry.done():
            continue
        _check_capture(session.telemetry.result())


@dataclass(frozen=True)
class _FaultExecutors:
    """Per-leg executors plus one composite adapter for every reconcile site.

    Leg membership repeats validate_run's mechanism dispatch (gateway rollout
    mechanism -> gateway, db_table_lock -> database, otherwise Kubernetes), so
    an attempt never re-decides a leg after validation. Legs without faults
    keep None executors and empty prepared maps.
    """
    k8s: p.PrimitiveExecutor | None
    gateway: g.GatewayExecutor | None
    db: d.DatabaseLockExecutor | None
    prepared_k8s: dict[str, p.PreparedFault]
    prepared_gateway: dict[str, g.GatewayRequest]
    prepared_db: dict[str, d.PreparedDbLock]
    adapter: j.ResourceAdapter

    def spec(self, fid: str):
        for prepared in (self.prepared_k8s, self.prepared_gateway, self.prepared_db):
            if fid in prepared:
                return prepared[fid].spec
        raise RunnerError("unknown fault instance: " + fid)

    def __iter__(self):
        """Legacy two-tuple face, defined only for Kubernetes-only contracts.

        The guard suite consumes ``executor, prepared = _primitive_executor(..)``
        and predates the multi-leg structure; every contract that face ever
        accepted (including legacy gateway_prepare_only members, refused later
        by validate_run) lands in prepared_k8s, so yielding the Kubernetes
        executor and its map reproduces the historical behavior exactly. Mixed
        or non-Kubernetes contracts must use the structure fields instead.
        """
        _require(not self.prepared_gateway and not self.prepared_db,
                 "legacy executor unpacking supports Kubernetes-only contracts; use the executor structure fields")
        yield self.k8s
        yield self.prepared_k8s


class _RemovedOwnCarrierAdapter:
    """collection/F2: an own carrier's intended absence is not an identity anomaly.

    Wraps exactly one prepared host-carrier chaos-CRD fault route, and is only
    constructed by the explicit reconcile/release entries (qualify_recovery,
    resume_reconcile) -- never by in-run executors, where a vanished carrier
    stays an identity anomaly. The wrapped KubernetesAdapter.observe proves
    CRD absence by re-verifying the pinned carrier Deployment and its pods in
    their original state, which assumes the deployment always exists; the S05
    host carrier is provisioned before the attempt and deleted after the
    journal's cleanup, so a release-time reconcile structurally died on
    ``deployment UID changed/missing`` (collection F2; the raw adapter keeps raising
    it by design). Exactly one shape is tolerated here: the controlled CRD,
    the pinned carrier Deployment and every pinned Pod are all positively
    absent (bounded empty reads, the same evidence class as the residual scan)
    AND every durable intent the journal holds for this target is already
    ``intent`` -> ``restored`` -- the existing cleanup-evidence shape the
    primitives' intent guard already replays. The observe then reports the
    carrier as removed as intended (the absent state a create-intent restores
    to); every other shape (live or replaced carrier, leftover pod, any
    unrestored intent on the target -- including a re-injection round whose
    disappearance was never journaled) re-raises the inner identity error
    untouched so true drift is never swallowed. Removal reports are per-target
    first-observation only: later tolerant observations of the same route never
    mint additional reports.
    """

    def __init__(self, journal: j.AttemptJournal, inner: p.KubernetesAdapter, prepared: p.PreparedFault):
        _require(isinstance(inner, p.KubernetesAdapter) and type(prepared) is p.PreparedFault
                 and prepared.mode == "chaos_crd" and prepared.binding.entity == "host",
                 "own-carrier removal tolerance covers exactly host chaos-CRD fault routes")
        self.journal, self.inner, self.prepared = journal, inner, prepared
        self.evidence_kind = inner.evidence_kind
        self.removal_reports: list[dict[str, Any]] = []

    def _target_fully_restored(self) -> bool:
        """F-1 (collection): every durable intent on this exact target must be restored.

        Reviewer terminal-rule fix: a single restored intent can no longer
        certify the tolerance -- any applied-but-unrestored intent on the same
        target (e.g. a re-injection round that vanished unjournaled) keeps the
        tolerance off and the inner identity error fires. A target with no
        journal chain at all is equally intolerable (no evidence).
        """
        states = self.journal._replay(self.journal.records())
        target = self.prepared.target.to_dict()
        selected = [item for item in states.values() if item["intent"]["target"] == target]
        return bool(selected) and all(item["status"] == "restored" for item in selected)

    def _confirmed_absent(self) -> bool:
        item = self.prepared
        try:
            if self.inner._get(item, item.target.kind, item.target.name) is not None:
                return False  # The controlled CRD still exists (or reappeared).
            if self.inner._get(item, "Deployment", item.binding.deployment) is not None:
                return False  # Carrier present or replaced; a replacement is drift.
            return all(self.inner._get(item, "Pod", pin.name) is None for pin in item.binding.pods)
        except Exception:
            return False  # An unreadable environment never certifies absence.

    def observe(self, target, field_names):
        try:
            return self.inner.observe(target, field_names)
        except p.PrimitiveError:
            if not (self._confirmed_absent() and self._target_fully_restored()):
                raise
            if not self.removal_reports:
                
                # route repeatedly (final baseline loop, repeated release
                # reconciles); each later tolerant observation confirms the same
                # already-reported fact and must not accumulate duplicates.
                self.removal_reports.append({"fault_instance_id": self.prepared.spec.fault_instance_id,
                                             "carrier_deployment": self.prepared.binding.deployment,
                                             "target": self.prepared.target.to_dict(),
                                             "disposition": "owned_carrier_removed_as_intended"})
            return j.ResourceState(False, None, None, {})

    def compare_and_apply(self, target, *args, **kwargs):
        return self.inner.compare_and_apply(target, *args, **kwargs)

    def compare_and_restore(self, target, *args, **kwargs):
        return self.inner.compare_and_restore(target, *args, **kwargs)


def _owned_carrier_removal_reports(executors: _FaultExecutors) -> list[dict[str, Any]]:
    """Collect the as-intended removal observations of this session's routes."""
    reports: list[dict[str, Any]] = []
    for adapter in getattr(executors.adapter, "routes", {}).values():
        if type(adapter) is _RemovedOwnCarrierAdapter:
            reports.extend(adapter.removal_reports)
    return reports


class _CoexistingRolloutAdapter:
    """ln (collection.1 EXEC-4/D22): an aux-attributed rollout is not pod drift.

    Wraps exactly one prepared chaos-CRD fault route whose fault instance is
    DECLARED coexisting with an auxiliary plan (plan.coexisting_fault_ids --
    the restricted lane whose StressChaos pod selection shares the aux target
    Deployment). The wrapped KubernetesAdapter's deleted-CRD observe path
    proves intended absence by re-verifying the PINNED pods' identity; the
    auxiliary's ordered cleanup (direct-route restore) legitimately rolls
    that Deployment, so the pinned pod is replaced by a same-image,
    same-selector, ready sibling AFTER the fault's own restore. Exactly one
    shape is tolerated here: the controlled CRD is absent AND every durable
    journal intent on this target is already restored AND the selector's
    current pods match the pinned pods' (labels, images, count) with a Ready
    condition -- rollout evidence, never a silent pass. Every other shape
    (CRD alive, wrong image, extra/missing pod, unrestored intent, unreadable
    environment) re-raises the inner identity error untouched so true drift
    is never swallowed. Churn reports are first-observation only, mirroring
    the own-carrier removal precedent.
    """

    def __init__(self, journal: j.AttemptJournal, inner: p.KubernetesAdapter, prepared: p.PreparedFault, plan):
        _require(isinstance(inner, p.KubernetesAdapter) and type(prepared) is p.PreparedFault
                 and prepared.mode == "chaos_crd"
                 and prepared.spec.fault_instance_id in plan.coexisting_fault_ids,
                 "coexisting rollout tolerance covers exactly declared coexisting chaos-CRD fault routes")
        self.journal, self.inner, self.prepared, self.plan = journal, inner, prepared, plan
        self.evidence_kind = inner.evidence_kind
        self.rollout_reports: list[dict[str, Any]] = []

    def _target_fully_restored(self) -> bool:
        states = self.journal._replay(self.journal.records())
        target = self.prepared.target.to_dict()
        selected = [item for item in states.values() if item["intent"]["target"] == target]
        return bool(selected) and all(item["status"] == "restored" for item in selected)

    def _selector_semantic_confirmed(self) -> bool:
        item = self.prepared
        try:
            if self.inner._get(item, item.target.kind, item.target.name) is not None:
                return False  # the controlled CRD still exists (or reappeared)
            selector = dict(item.binding.selector)
            match = ",".join(k + "=" + v for k, v in selector.items())
            result = self.inner._run(item, "list_selector_pods", ("get", "pods", "--selector", match, "-o", "json"))
            items = json.loads(result.stdout).get("items", [])
            pins = list(item.binding.pods)
            if len(items) != len(pins):
                return False
            for pod in items:
                meta = pod.get("metadata", {})
                if meta.get("deletionTimestamp") or meta.get("namespace") != item.target.scope:
                    return False
                labels = meta.get("labels", {})
                if any(labels.get(k) != v for k, v in selector.items()):
                    return False
                if not any(c.get("type") == "Ready" and c.get("status") == "True"
                           for c in pod.get("status", {}).get("conditions", [])):
                    return False
                images = {c.get("name"): c.get("image") for c in pod.get("spec", {}).get("containers", [])}
                if not any(images == dict(pin.images) for pin in pins):
                    return False
            return True
        except Exception:
            return False  # an unreadable environment never certifies the rollout

    def observe(self, target, field_names):
        try:
            return self.inner.observe(target, field_names)
        except p.PrimitiveError:
            if not (self._selector_semantic_confirmed() and self._target_fully_restored()):
                raise
            if not self.rollout_reports:
                self.rollout_reports.append({"fault_instance_id": self.prepared.spec.fault_instance_id,
                                             "aux_id": self.plan.aux_id,
                                             "target": self.prepared.target.to_dict(),
                                             "pinned_pods": [pin.name for pin in self.prepared.binding.pods],
                                             "disposition": "pinned_pod_replaced_by_declared_aux_rollout"})
            return j.ResourceState(False, None, None, {})

    def compare_and_apply(self, target, *args, **kwargs):
        return self.inner.compare_and_apply(target, *args, **kwargs)

    def compare_and_restore(self, target, *args, **kwargs):
        return self.inner.compare_and_restore(target, *args, **kwargs)


def _coexisting_rollout_reports(executors: _FaultExecutors) -> list[dict[str, Any]]:
    """Collect the aux-attributed rollout observations of this session's routes."""
    reports: list[dict[str, Any]] = []
    for adapter in getattr(executors.adapter, "routes", {}).values():
        if type(adapter) is _CoexistingRolloutAdapter:
            reports.extend(adapter.rollout_reports)
    return reports


def _primitive_executor(journal: j.AttemptJournal, services: RunServices, *, auxiliary_transaction=None,
                        own_carrier_removal_tolerant: bool = False) -> _FaultExecutors:
    pins = dict(services.pinned_targets)
    db_pins = dict(services.db_bindings)
    ownership = journal.metadata["ownership"]
    prepared_k8s: dict[str, p.PreparedFault] = {}
    prepared_gateway: dict[str, g.GatewayRequest] = {}
    prepared_db: dict[str, d.PreparedDbLock] = {}
    for fault in journal.contract.faults:
        fid = fault.fault_instance_id
        data = fault.to_dict()
        if data["mechanism"] == g.MECHANISM:
            prepared_gateway[fid] = g.prepare_gateway(fault, pins[fid])
        elif data["fault_type"] == "db_table_lock":
            prepared_db[fid] = d.prepare_db_lock(fault, db_pins[fid], ownership=ownership)
        else:
            prepared_k8s[fid] = p.prepare_fault(fault, pins[fid], ownership=ownership)
    routes: list[tuple[j.Target, j.ResourceAdapter]] = []
    k8s_executor = gateway_executor = db_executor = None
    if prepared_k8s:
        k8s_adapter = p.KubernetesAdapter(journal, services.primitive_client, tuple(prepared_k8s.values()), kubectl=services.kubectl,
                                          command_timeout_s=services.command_timeout_s, wait_timeout_s=services.command_wait_timeout_s)
        for fid, item in prepared_k8s.items():
            selected_adapter = k8s_adapter
            if auxiliary_transaction is not None:
                for plan in auxiliary_transaction.plans:
                    if fid in plan.terminal_host_fault_ids and auxiliary_transaction.step(plan, "cleanup-complete"):
                        selected_adapter = aux.TerminalHostObservationAdapter(auxiliary_transaction, plan, item)
                    elif fid in plan.coexisting_fault_ids:
                        # ln restricted coexistence: the aux's ordered cleanup
                        # rolls the shared Deployment after the fault's own
                        # restore; the pinned-pod identity re-check tolerates
                        # exactly that rollout (selector-semantic evidence).
                        selected_adapter = _CoexistingRolloutAdapter(journal, k8s_adapter, item, plan)
            if (own_carrier_removal_tolerant and type(selected_adapter) is p.KubernetesAdapter
                    and item.mode == "chaos_crd" and item.binding.entity == "host"):
                
                # journal; the explicit reconcile/release entries tolerate its
                # confirmed post-cleanup absence instead of raising the
                # pinned-carrier UID error. In-run executors and terminal
                # auxiliary proofs keep their own stricter discipline.
                selected_adapter = _RemovedOwnCarrierAdapter(journal, k8s_adapter, item)
            routes.append((item.target, selected_adapter))
        k8s_executor = p.PrimitiveExecutor(journal, k8s_adapter)
    if prepared_gateway:
        gateway_adapter = g.GatewayAdapter(journal, services.primitive_client, tuple(prepared_gateway.values()),
                                           kubectl=services.kubectl, command_timeout_s=services.command_timeout_s,
                                           wait_timeout_s=services.command_wait_timeout_s)
        routes.extend((target, gateway_adapter) for target in gateway_adapter.targets)
        gateway_executor = g.GatewayExecutor(journal, gateway_adapter)
        # Crash-reopen support: a reopened session reconciles through a fresh
        # adapter instance whose plans start empty, and reconcile observes every
        # gateway target (including already-restored ones) through _plan(). Load
        # each durable plan that already exists; a fresh attempt has none yet.
        completed = {row["payload"].get("artifact_id") for row in journal.records() if row["event"] == "artifact_complete"}
        for fid in prepared_gateway:
            if "gateway-plan-" + fid in completed:
                gateway_adapter.load_plan(fid)
    if prepared_db:
        _require(services.db_client is not None, "database faults require the validated database client")
        db_adapter = d.DatabaseLockAdapter(journal, services.db_client, tuple(prepared_db.values()))
        routes.extend((item.target, db_adapter) for item in prepared_db.values())
        db_executor = d.DatabaseLockExecutor(journal, db_adapter)
    return _FaultExecutors(k8s=k8s_executor, gateway=gateway_executor, db=db_executor, prepared_k8s=prepared_k8s,
                          prepared_gateway=prepared_gateway, prepared_db=prepared_db,
                          adapter=g.CompositeResourceAdapter(routes))


def _record_normal_baseline(executors, artifacts, clock, mode):
    facts, failures = [], []
    for fid, item in executors.prepared_k8s.items():
        fact = {"fault_instance_id": fid, "target": item.target.to_dict(), "observed_at_epoch_s": clock.epoch(),
                "evidence_kind": "synthetic" if mode == "isolated_test" else "observed"}
        try:
            current = executors.k8s.adapter.observe(item.target, tuple(item.desired_fields))
            fact["controlled_state"] = current.to_dict()
            normal = current.ownership is None and (current.exists if item.mode == "deployment_env" else not current.exists)
            if item.mode == "deployment_env":
                normal = normal and current.fields != item.desired_fields
            if item.binding.entity in {"catalog", "inventory"}:
                deployment = executors.k8s.adapter._deployment(item)
                _, container = executors.k8s.adapter._container(deployment, item.binding.container)
                hooks = {name: executors.k8s.adapter._env(container, name) for name in ("FAULT_DELAY_MS", "FAULT_RAISE")}
                fact["baseline_hooks"] = hooks
                delay = hooks["FAULT_DELAY_MS"]
                delay_value = int((delay["value"] or "0") if delay["present"] else "0")
                raised = hooks["FAULT_RAISE"]
                raise_value = raised["present"] and raised["value"].strip().lower() in {"1", "true", "yes", "on"}
                normal = normal and delay_value == 0 and not raise_value
            fact["normal_for_declared_primitive"] = bool(normal)
            if not normal:
                failures.append(fid)
        except Exception as exc:
            fact.update(normal_for_declared_primitive=False, error_type=type(exc).__name__)
            failures.append(fid)
        facts.append(fact)
    for fid, item in executors.prepared_db.items():
        fact = {"fault_instance_id": fid, "target": item.target.to_dict(), "observed_at_epoch_s": clock.epoch(),
                "evidence_kind": "synthetic" if mode == "isolated_test" else "observed"}
        try:
            # The absent empty state is the only normal baseline: a foreign
            # holder observes as exists without this attempt's ownership.
            current = executors.db.adapter.observe(item.target, tuple(item.desired_fields))
            fact["controlled_state"] = current.to_dict()
            normal = current.ownership is None and not current.exists
            fact["normal_for_declared_primitive"] = bool(normal)
            if not normal:
                failures.append(fid)
        except Exception as exc:
            fact.update(normal_for_declared_primitive=False, error_type=type(exc).__name__)
            failures.append(fid)
        facts.append(fact)
    for fid, request in executors.prepared_gateway.items():
        fact = {"fault_instance_id": fid, "target": request.spec.to_dict()["raw_target"],
                "observed_at_epoch_s": clock.epoch(),
                "evidence_kind": "synthetic" if mode == "isolated_test" else "observed"}
        try:
            # Zero-write preview (durable=False): the durable plan is produced
            # later inside GatewayExecutor.apply's own preflight, which refuses
            # duplicate registration, so the preview must stay in-memory only.
            plan = executors.gateway.adapter.preflight(request, durable=False)
            data = plan.to_dict()
            fact["controlled_state"] = data["original_fields"]
            fact["baseline_runtime"] = {"pods": len(data["baseline_runtime"]["pods"]),
                                        "outside_config_sha256": data["outside_config_sha256"]}
            # Preflight itself refuses an owned/foreign-marked deployment or
            # ConfigMap, so surviving it is exactly the normal baseline.
            fact["normal_for_declared_primitive"] = True
        except Exception as exc:
            fact.update(normal_for_declared_primitive=False, error_type=type(exc).__name__)
            failures.append(fid)
        facts.append(fact)
    artifacts.json("preflight-baseline", "preflight/baseline.json", {"facts": facts, "failures": failures,
                     "claim_scope": "controlled_fault_knobs_and_identity_only_not_overall_system_health"})
    if failures:
        raise BaselineViolation("pre-existing fault, no-op injection, or uncertain baseline refused")


def _journal_section(journal: j.AttemptJournal):
    """Serialize unlocked journal reads with a live log-capture background writer.

    J's mutating methods take the journal mutex, but records()-style readers in
    this module previously ran only on the main thread. An L2 live archive writes
    exclusive artifacts from its own worker during phases, so main-thread reads
    (phase advance, operation records, lease cleanup) must hold the same RLock
    to never observe an artifact intent without its completion event.
    """
    return journal._mutex


def _enter_phase(journal: j.AttemptJournal, phase: str) -> None:
    with _journal_section(journal):
        journal.enter_phase(phase)


# collection switch-band summary: exactly the six fields a durable gateway transition
# record really carries (gateway _record_transition). Derived quantities (probe
# exclusion windows, effectiveness boundaries) stay consumer-side pure functions
# over the durable record and never enter the summary.
_SWITCH_BAND_STAMPS = ("switch_initiated_at_utc", "switch_patch_completed_at_utc",
                       "rollout_status_completed_at_utc", "settled_confirmed_at_utc")


def _gateway_switch_band(gateway_adapter, fid: str, phase: str) -> dict[str, Any]:
    """Six durable transition fields for one gateway switch band.

    The composite adapter has no transition face; the gateway sub-adapter is
    the only source. In-session records come from ``transition(fid)``; after a
    reopen the in-memory map is empty, so the hash-verified durable record is
    reloaded through ``load_transition``.
    """
    record = gateway_adapter.transition(fid).get(phase)
    if record is None:
        record = gateway_adapter.load_transition(fid, phase)
    durations = record.get("durations_s") if type(record) is dict else None
    _require(type(record) is dict and all(name in record for name in _SWITCH_BAND_STAMPS)
             and type(durations) is dict
             and type(durations.get("rollout_band_s")) in (int, float)
             and type(durations.get("settled_readback_s")) in (int, float),
             "gateway switch band evidence incomplete")
    band = {name: record[name] for name in _SWITCH_BAND_STAMPS}
    band.update(rollout_band_s=durations["rollout_band_s"], settled_readback_s=durations["settled_readback_s"])
    return band


def _db_lock_operation_fields(db_executor, fid: str, action: str) -> dict[str, Any]:
    """Merge one database adapter evidence record into the operation row.

    Source is the session-scoped in-memory evidence list of the leg's adapter;
    exactly one record must exist for this (fault, action) pair, so a missing
    or duplicated evidence row refuses instead of degrading to a blank.
    """
    operation = "db_lock_inject" if action == "inject" else "db_lock_recover"
    matches = [row for row in db_executor.adapter.evidence
               if row.get("operation") == operation and row.get("instance_id") == fid]
    _require(len(matches) == 1, "database lock operation evidence missing or duplicated")
    row = matches[0]
    if action == "inject":
        return {"lock_connection_ids": list(row["lock_connection_ids"]),
                "db_client_protocol": row["db_client_protocol"]}
    return {"examined_connection_ids": list(row["examined_connection_ids"]),
            "active_owned_locks": [dict(lock) for lock in row["active_owned_locks"]],
            "unlock": dict(row["unlock"])}


def _operation_record(journal, fid, action, started, ended, anchor, source_id, evidence_kind, phase, executors):
    with _journal_section(journal):
        rows = journal.records()
        items = journal._replay(rows)
        candidates = [item for item in items.values() if item["intent"]["fault_instance_id"] == fid]
        if fid in executors.prepared_gateway:
            # Gateway legs own two intents per fault (owned ConfigMap create
            
            # assertion relaxes to an exact dual-intent set. Evidence anchors on
            # the Deployment/volume intent: its applied uid is the stable
            # deployment identity and its snapshot/desired triples are the
            # controlled state Q1/Q2 consume; ConfigMap-leg fields never mix in.
            _require(len(candidates) == 2
                     and {item["intent"]["target"]["kind"] for item in candidates} == {"ConfigMap", "Deployment"},
                     "gateway fault requires exactly its configmap and volume intents")
            item = next(item for item in candidates if item["intent"]["target"]["kind"] == "Deployment")
        else:
            _require(len(candidates) == 1, "P1 runner requires one intent per root; composite summaries need explicit integration")
            item = candidates[-1]
        intent = item["intent"]
        baseline = json.loads(journal._file(intent["snapshot"]["path"]).read_bytes())
    applied = item["applied"]
    _require(applied is not None, "operation lacks confirmed applied readback")
    spec = next(f.to_dict() for f in journal.contract.faults if f.fault_instance_id == fid)
    context = journal.contract.context.to_dict()
    record = {"contract_sha256": journal.contract.sha256, "run_id": context["run_id"], "attempt_id": context["attempt_id"],
              "evidence_kind": evidence_kind, "instance_id": fid, "action": action, "phase": phase,
              "entity": spec["normalized_root_entity"], "source_id": source_id, "mechanism": spec["mechanism"],
              "mechanism_version": spec["mechanism_version"], "raw_target": spec["raw_target"], "resource_uid": applied["uid"],
              "effective_parameters": spec["parameters"], "return_code": 0, "timestamp_epoch_s": anchor.epoch_at(ended),
              "operation_started_monotonic_s": started, "operation_completed_monotonic_s": ended,
              "baseline_fields": {"exists": baseline["exists"], "controlled_fields": baseline["fields"]},
              "desired_fields": {"exists": True, "controlled_fields": intent["controlled_fields"]},
              "observed_fields": {"exists": applied["exists"] if action == "inject" else baseline["exists"],
                                  "controlled_fields": applied["fields"] if action == "inject" else baseline["fields"]},
              "effectiveness": "not_assessed"}
    if fid in executors.prepared_gateway:
        # Transition summary for consumers that must not glob the attempt
        # directory; the durable record stays the authoritative full copy.
        record["switch_band"] = _gateway_switch_band(executors.gateway.adapter, fid,
                                                     "inject" if action == "inject" else "restore")
    elif fid in executors.prepared_db:
        record.update(_db_lock_operation_fields(executors.db, fid, action))
    return record


def _merge_completed_artifacts(artifacts):
    """Include same-journal archive/primitive proofs; never infer from files."""
    journal = artifacts.journal
    with _journal_section(journal):
        states = journal._artifact_states(journal.records())
    completed = {aid: item["spec"] for aid, item in states.items()
                 if item["complete"] and not item["abandoned"] and aid not in {"quality-result", "artifact-index"}}
    by_id, paths = {}, {}
    for descriptor in artifacts.descriptors:
        aid = descriptor["artifact_id"]
        _require(aid in completed, "artifact descriptor lacks a completed journal receipt")
        wanted = {key: completed[aid][key] for key in ("artifact_id", "relative_path", "sha256")}
        _require(descriptor == wanted, "artifact descriptor differs from its journal receipt")
        _require(aid not in by_id or by_id[aid] == wanted, "artifact id conflict")
        by_id[aid] = wanted
    total = 0
    for aid, receipt in completed.items():
        descriptor = {key: receipt[key] for key in ("artifact_id", "relative_path", "sha256")}
        path = descriptor["relative_path"]
        _require(path not in paths or paths[path] == aid, "artifact path conflict")
        _require(receipt["bytes"] <= artifacts.settings.max_artifact_bytes, "completed artifact byte budget exceeded")
        total += receipt["bytes"]
        paths[path] = aid
        by_id[aid] = descriptor
    _require(total <= artifacts.settings.max_total_artifact_bytes, "completed artifact total budget exceeded")
    artifacts.descriptors[:] = list(by_id.values())
    artifacts.total = total


def _persist_result(contract, settings, services, journal, sessions, workload_results, telemetry_results,
                    operations, action_times, phase_facts, anchor, run_error, artifacts):
    context = contract.context.to_dict()
    identity = {"contract_sha256": contract.sha256, "run_id": context["run_id"], "attempt_id": context["attempt_id"]}
    bundles, bundle_ids, actual_phases = {}, {}, {}
    workload_ids, phase_observations = {}, {}
    errors = []
    for session in sessions:
        phase = session.phase
        if phase in workload_results:
            value = workload_results[phase]
            suffix = "workload" if value.get("schema_version") == w.SCHEMA_VERSION else "workload-error"
            artifacts.json(phase + "." + suffix, phase + "/" + suffix + ".json", value)
            workload_ids[phase] = phase + "." + suffix
        if session.phase_observations is not None:
            facts_id = phase + ".phase-observations"
            artifacts.json(facts_id, phase + "/phase-observations.json", session.phase_observations)
            phase_observations[phase] = {"artifact_id": facts_id, "facts": session.phase_observations}
        result = telemetry_results.get(phase)
        if not result or "actual_window" not in result:
            errors.append(phase + ":telemetry_unavailable")
            error_record = dict(result) if type(result) is dict else {"status": "not_collected"}
            observation_failure = getattr(session, "phase_observation_failure", None)
            if observation_failure is not None:
                error_record["phase_observation_failure"] = observation_failure
            artifacts.json(phase + ".telemetry-error", phase + "/telemetry-error.json", error_record)
            continue
        actual_phases[phase] = result["actual_window"]
        bundles[phase], bundle_ids[phase] = {}, {}
        for modality in _MODALITIES:
            bundle = result["bundles"].get(modality, {"collection_status": "collector_error"})
            if bundle.get("schema_version") != t.SCHEMA_VERSION:
                errors.append(phase + ":" + modality + ":collector_error")
                artifact_id = phase + "." + modality + ".error"
                artifacts.json(artifact_id, phase + "/" + modality + "/error.json", bundle)
                bundles[phase][modality], bundle_ids[phase][modality] = bundle, artifact_id
                continue
            _require(bundle["scope"]["contract_sha256"] == contract.sha256 and bundle["scope"]["phase"] == phase
                     and bundle["scope"]["run_id"] == context["run_id"] and bundle["scope"]["attempt_id"] == context["attempt_id"]
                     and bundle["scope"]["actual_window"] == result["actual_window"], "telemetry bundle crosses attempt/phase")
            persisted, artifact_id = artifacts.bundle(phase, modality, bundle)
            bundles[phase][modality], bundle_ids[phase][modality] = persisted, artifact_id
            if bundle["manifest"]["collection_status"] in {"failed", "not_run"}:
                errors.append(phase + ":" + modality + ":collection_failed")
    if errors and not run_error:
        journal.mark_failed("telemetry_collection_failed")
        run_error = "TelemetryCollectionFailed"
    artifacts.json("operations", "operations.json", {**identity, "operations": operations, "action_timing": action_times})
    timings = []
    
    # face (>=2 contract faults) each leg additionally carries
    # observed_effective as an OPERATION-WINDOW PROXY, not a mechanism-level
    # effectiveness boundary. The proxy reuses the real event-loop operation
    # bounds already recorded for operation_confirmed (inject/recover
    # completion timestamps from artifacts/operations.json); the declared
    # planned window is never substituted. It exists so the E1 combination
    # loader's observed_overlap_lattice contract is satisfiable; mechanism-
    # level effective boundaries (rollout settle + buffer etc.) stay gate-side
    # (E1 reports buffer_applied_by_loader: False). Single-fault faces keep
    
    combo_face = len(contract.faults) >= 2
    for fid in (fault.fault_instance_id for fault in contract.faults):
        inject = next((row for row in operations if row["instance_id"] == fid and row["action"] == "inject"), None)
        recover = next((row for row in reversed(operations) if row["instance_id"] == fid and row["action"] == "recover"), None)
        operation_window = None
        effective_window = None
        if inject and recover and recover["timestamp_epoch_s"] > inject["timestamp_epoch_s"]:
            window = c.TimeWindow(inject["timestamp_epoch_s"], recover["timestamp_epoch_s"], "unix_epoch", context["clock_id"])
            operation_window = a.WindowEvidence(window, "operation_return_bounds_not_physical_effectiveness",
                (a.EvidenceReference(context["run_id"], context["attempt_id"], fid, "artifact:operations"),))
            if combo_face:
                effective_window = a.WindowEvidence(window,
                    "combo_leg_effective_operation_window_proxy_not_mechanism_level_boundary",
                    (a.EvidenceReference(context["run_id"], context["attempt_id"], fid, "artifact:operations"),))
        timings.append(a.FaultTiming(fid, operation_window, effective_window))
    annotation = a.build_annotation_draft(contract, tuple(timings), timing_policy=settings.annotation_policy)
    artifacts.json("annotation-draft", "annotation-draft.json", annotation)
    record = {**identity, "schema_version": RUNNER_SCHEMA, "mode": settings.mode, "planned_phases": contract.to_dict()["phases"],
              "actual_phases": actual_phases, "phase_facts": phase_facts, "action_timing": action_times,
              "clock_mapping": anchor.__dict__, "run_error": run_error, "collection_errors": errors,
              "qualification": "not_assessed", "fault_effectiveness": "not_assessed"}
    if services.phase_observer is not None:
        record.update(phase_observations=phase_observations, workload_results=workload_results,
                      workload_artifact_ids=workload_ids, bundles=bundles, bundle_artifact_ids=bundle_ids,
                      operations=operations)
    artifacts.json("run-record", "run-record.json", record)
    supplement = services.supplement(record) if services.supplement is not None else {}
    _require(isinstance(supplement, dict) and set(supplement) <= {"instance_windows", "components", "checksums", "locks", "coverage", "artifacts"},
             "supplement contains unsupported fields; it cannot override runtime identity/operations/annotation")
    for artifact_id, value in supplement.get("artifacts", {}).items():
        artifacts.json(artifact_id, "supplement/" + artifact_id + ".json", value)
    evidence = {**identity, "schema_version": q.EVIDENCE_SCHEMA, "evidence_kind": "synthetic" if settings.mode == "isolated_test" else "observed",
                "artifact_id": "quality-evidence", "attempt_state": "FAILED_CLEAN" if run_error else "COMPLETED_CLEAN",
                "actual_phases": actual_phases, "instance_windows": supplement.get("instance_windows", {}),
                "operations": operations, "components": supplement.get("components", []), "checksums": supplement.get("checksums", {}),
                "locks": supplement.get("locks"), "annotation": annotation, "bundles": bundles,
                "bundle_artifact_ids": bundle_ids, "coverage": supplement.get("coverage", {})}
    artifacts.json("quality-evidence", "quality/evidence.json", evidence)
    _merge_completed_artifacts(artifacts)
    receipt = q.verify_artifacts(contract, settings.output_policy, tuple(artifacts.descriptors),
                                 max_file_bytes=settings.max_artifact_bytes, max_total_bytes=settings.max_total_artifact_bytes)
    quality = q.evaluate_quality(contract, services.quality_rules, evidence, artifacts=receipt)
    artifacts.json("quality-result", "quality/result.json", quality)
    artifacts.json("artifact-index", "artifact-index.json", {**identity, "artifacts": list(artifacts.descriptors),
                                                             "qualification": "not_assessed"})
    return quality, list(artifacts.descriptors), run_error


def _persist_worker_salvage(artifacts, workloads, telemetry):
    """Retain completed worker outputs even when another worker is unjoined."""
    for phase, value in workloads.items():
        suffix = "workload" if value.get("schema_version") == w.SCHEMA_VERSION else "workload-error"
        artifacts.json(phase + "." + suffix, phase + "/" + suffix + ".json", value)
    for phase, capture in telemetry.items():
        if "bundles" not in capture:
            artifacts.json(phase + ".telemetry-error", phase + "/telemetry-error.json", capture)
            continue
        for modality, bundle in capture["bundles"].items():
            if bundle.get("schema_version") == t.SCHEMA_VERSION:
                artifacts.bundle(phase, modality, bundle)
            else:
                artifacts.json(phase + "." + modality + ".error", phase + "/" + modality + "/error.json", bundle)
    artifacts.json("worker-salvage", "worker-salvage.json", {"status": "BLOCKED_WORKERS", "qualification": "not_assessed",
        "resource_cleanup": "not_attempted_before_worker_drain", "artifacts": list(artifacts.descriptors)})


def _prepared_settings_identity(settings):
    return _hash(json.dumps(asdict(settings), sort_keys=True, default=str, separators=(",", ":")).encode())


def _prepared_sources():
    return {"runner": _hash(Path(__file__).read_bytes()), "auxiliary": _hash(Path(aux.__file__).read_bytes()),
            "journal": _hash(Path(j.__file__).read_bytes()), "observability": _hash(Path(obs.__file__).read_bytes()),
            "sampling": _hash(Path(__file__).with_name("sampling.py").read_bytes()),
            'scenario_runner': _hash(Path(__file__).with_name('scenario_runner.py').read_bytes()), **pr.sources()}


def _prepared_kind(mode):
    _require(mode in {"isolated_test", "controlled_pilot"}, "prepared mode invalid")
    return "observed" if mode == "controlled_pilot" else "synthetic"


def _registered_pricing_scope(contract, plans):
    """Bind a supported route auxiliary to the actual catalog and request leg.

    Composition membership comes from the registry, not a scenario-ID allowlist.
    This admits no host/DB auxiliary adapter and does not change fault parameters.
    """
    registry = c.DesignRegistry.from_json((_formal_asset_root() /
        'configs/collection/scenarios.json').read_bytes())
    c.RunContract.from_json(contract.to_json(), registry)
    gateway_fids = {fault.fault_instance_id for fault in contract.faults
                   if fault.normalized_root_entity == "catalog-gw"
                   and (fault.to_dict()["fault_type"], fault.to_dict()["mechanism"]) in {
                       ("network_delay", "chaos_mesh"), ("network_loss", "chaos_mesh"),
                       ("timeout_misconfiguration", g.MECHANISM), ("retry_policy_misconfiguration", g.MECHANISM)}}
    _require(gateway_fids and set(plans[0].fault_instance_ids) == gateway_fids,
             "pricing auxiliary must bind exactly the registered gateway fault legs")
    namespace = contract.context.to_dict()["namespace"]
    prefix = "/api/v1/namespaces/" + namespace + "/services/pricing:5014/proxy/api/pricing/"
    _require(any(stream["method"] == "GET" and stream["endpoint"].startswith(prefix)
                 for stream in contract.to_dict()["request_profile"]["streams"]),
             "pricing auxiliary requires an explicit pricing request carrier")


def _validate_preparation(contract, settings, environment_reader, plans, adapters, quality_rules):
    _require(isinstance(contract, c.RunContract) and isinstance(settings, RunSettings), "typed preparation contract/settings required")
    _require(settings.mode in {"isolated_test", "controlled_pilot"} and contract.to_dict()["purpose"] in {"smoke", "pilot", "formal"},
             "unsupported prepared mode or purpose")
    if contract.to_dict()["purpose"] == "formal":
        # Before a lease, setup intent, external SET, or worker can be created.
        # run_attempt will independently enforce the same gate again at consume.
        _validate_release_entry(contract)
    _require(callable(environment_reader) and isinstance(quality_rules, q.QualityRules), "preparation environment/rules required")
    _require(quality_rules.sha256 == contract.context.to_dict()["fingerprints"]["quality_rules"], "preparation quality rules mismatch")
    _require(type(plans) is tuple and bool(plans) and all(type(plan) is aux.AuxiliaryPlan for plan in plans), "typed auxiliary plans required")
    _require(len({plan.aux_id for plan in plans}) == len(plans)
             and len({tuple(sorted(plan.target.items())) for plan in plans}) == len(plans), "duplicate auxiliary ID/target")
    _require(type(adapters) is dict, "typed auxiliary adapters required")
    if settings.mode == "controlled_pilot":
        _require(len(plans) == 1 and plans[0].adapter_kind == pr.ADAPTER_KIND,
                 "prepared auxiliary live/formal execution is not implemented for these adapters")
        _registered_pricing_scope(contract, plans)
    aux.validate_adapters(plans, adapters, settings.mode)
    faults = {fault.fault_instance_id: fault.to_dict() for fault in contract.faults}
    for plan in plans:
        _require(set(plan.fault_instance_ids) <= set(faults) and plan.target["scope"] == contract.context.to_dict()["namespace"],
                 "auxiliary fault/namespace binding mismatch")
        
        # fault may share the aux target Deployment only when its own control
        # face is the chaos_mesh StressChaos pod selector (never a Deployment
        # spec write) -- mirror of scenario_runner._RESTRICTED_COEXISTENCE_KINDS;
        # undeclared or differently-shaped collisions stay refused.
        _require(set(plan.coexisting_fault_ids) <= set(faults), "coexisting fault outside contract")
        for fid, fault in faults.items():
            target = fault["raw_target"]
            collision = all(target.get(k) == plan.target[k] for k in ("kind", "name", "scope"))
            restricted_lane = (fid in plan.coexisting_fault_ids
                               and fault["mechanism"] == "chaos_mesh"
                               and fault["fault_type"] == "service_cpu_saturation"
                               and target.get("kind") == "Deployment")
            _require(not collision or restricted_lane
                     or (fid in plan.terminal_host_fault_ids and fault["normalized_root_entity"] == "host"
                         and fault["fault_type"] == "host_cpu_saturation"), "auxiliary/fault controlled resource overlap")
    settings.output_policy.attempt_path(contract)
    lease_root = Path(settings.lease_root)
    _require(lease_root.is_absolute() and lease_root.resolve() == lease_root and ".." not in lease_root.parts, "preparation lease root invalid")
    _require(not j._within(lease_root, settings.output_policy.approved_real)
             and not j._within(settings.output_policy.approved_real, lease_root), "lease/output overlap")
    _require(all(not j._within(lease_root, root.resolve()) and not j._within(root.resolve(), lease_root)
                 for root in settings.output_policy.protected_roots), "lease/protected data overlap")
    _require(settings.workload_policy.traffic_purpose == ("isolated_test" if settings.mode == "isolated_test" else "read_only_business"), "preparation HTTP purpose mismatch")
    _require(isinstance(settings.deadlines, DeadlinePolicy), "preparation deadlines required")
    return {"schema_version": RUNNER_SCHEMA, "status": "preparation_validated_not_executed",
            "contract_sha256": contract.sha256, "auxiliary_ids": [plan.aux_id for plan in plans],
            "formal_release_eligible": False}


_PREPARED_TOKEN = object()
BOOTSTRAP_SCHEMA = "rq4-collect/prepared-bootstrap-v1"


def _write_exclusive_record(path, value):
    path = Path(path)
    _require(path.is_absolute() and path.resolve() == path and ".." not in path.parts, "recovery record path redirected")
    path.parent.mkdir(parents=True, exist_ok=True)
    _require(path.parent.resolve() == path.parent, "recovery record parent redirected")
    raw = _json_bytes(value)
    with path.open("xb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    _require(path.read_bytes() == raw, "recovery record persistence mismatch")
    return {"path": str(path), "sha256": _hash(raw), "bytes": len(raw)}


def _read_bound_record(reference, *, parse_json=True):
    _require(type(reference) is dict and set(reference) == {"path", "sha256", "bytes"}, "recovery record reference invalid")
    path = Path(reference["path"])
    _require(path.is_absolute() and path.resolve() == path and path.is_file()
             and type(reference["bytes"]) is int and 0 <= reference["bytes"] <= 16_000_000,
             "recovery record path/size invalid")
    with path.open("rb") as stream:
        raw = stream.read(reference["bytes"] + 1)
    _require(len(raw) == reference["bytes"] and _hash(raw) == reference["sha256"], "recovery record bytes mismatch")
    return json.loads(raw) if parse_json else raw


def _require_registered_preparation(settings):
    registry, key = environment_registry_root(), settings.environment.key
    binding_path, state_path = registry / (key + ".binding.json"), Path(settings.lease_root) / (key + ".state.json")
    _require(registry.is_dir() and registry.resolve() == registry and binding_path.is_file() and state_path.is_file()
             and binding_path.resolve() == binding_path and state_path.resolve() == state_path,
             "prepared entry requires an already registered environment")
    expected = {"schema_version": RUNNER_SCHEMA, "cluster_uid": settings.environment.cluster_uid,
                "namespace_uid": settings.environment.namespace_uid, "lease_root": str(settings.lease_root)}
    _require(json.loads(binding_path.read_bytes()) == expected, "prepared registry binding differs")
    state = json.loads(state_path.read_bytes())
    _require(state.get("schema_version") == RUNNER_SCHEMA and state.get("environment_key") == key
             and state.get("status") == "clean" and state.get("workers") == "drained" and state.get("mode") == settings.mode,
             "prepared entry requires registered clean/drained environment")


def _verified_completed_prepared_recovery(contract, settings, *, kind):
    """Inspect a previous clean receipt under the canonical lock; write nothing."""
    registry, key = environment_registry_root(), settings.environment.key
    lock_path, binding_path = registry / (key + ".lock"), registry / (key + ".binding.json")
    state_path = Path(settings.lease_root) / (key + ".state.json")
    _require(all(path.is_file() and path.resolve() == path for path in (lock_path, binding_path, state_path)),
             "recovery inspection requires complete canonical registration")
    fd = j._lock_file(lock_path)
    try:
        expected_binding = {"schema_version": RUNNER_SCHEMA, "cluster_uid": settings.environment.cluster_uid,
                            "namespace_uid": settings.environment.namespace_uid, "lease_root": str(settings.lease_root)}
        _require(json.loads(binding_path.read_bytes()) == expected_binding, "recovery inspection binding mismatch")
        from ops.metrics.maintenance_receipt import verify_maintenance_marker
        verify_maintenance_marker(registry, key, evidence_kind=_prepared_kind(settings.mode))
        state = json.loads(state_path.read_bytes())
        if state.get("status") != "clean":
            return None
        context = contract.context.to_dict()
        expected_attempt = {"contract_sha256": contract.sha256, **{k: context[k] for k in ("run_id", "attempt_id", "evidence_root", "output_root")}}
        _require(state.get("attempt") == expected_attempt and state.get("environment_key") == key
                 and state.get("mode") == settings.mode and state.get("workers") == "drained", "completed recovery names another attempt")
        declaration = state.get("auxiliary", {})
        reservation = _read_bound_record(declaration.get("reservation"))
        _require(reservation["contract"] == contract.to_dict()
                 and reservation["manifest"]["source_fingerprints"] == _prepared_sources()
                 and reservation["manifest"]["settings_sha256"] == _prepared_settings_identity(settings), "completed recovery source/reservation mismatch")
        if kind == "bootstrap":
            _require(state.get("cleanup_scope") == ("live" if settings.mode == "controlled_pilot" else "offline") + "_bootstrap_abort_recovered", "completed recovery is not bootstrap abort")
            report = _read_bound_record(state["bootstrap_recovery"])
            tombstone = _read_bound_record(report["tombstone_ref"])
            evidence = _read_bound_record(report["evidence_ref"])
            _require(report.get("schema_version") == "rq4-collect/bootstrap-recovery-v1"
                     and report.get("contract_sha256") == contract.sha256
                     and report.get("round") == state.get("bootstrap_round")
                     and tombstone.get("round") == report["round"]
                     and tombstone.get("reservation_ref") == declaration["reservation"]
                     and evidence.get("contract_sha256") == contract.sha256, "completed bootstrap receipt mismatch")
            return {"status": "BOOTSTRAP_ALREADY_RECOVERED", "sample_eligibility": "NOT_ASSESSED", "formal_release_eligible": False}
        _require(state.get("cleanup_scope") == ("live" if settings.mode == "controlled_pilot" else "offline") + "_recovery_qualified", "completed recovery has another scope")
        inspected = j.AttemptJournal.inspect_records(contract, settings.output_policy)
        selected = state.get("recovery_round", {})
        rid = selected.get("round_id")
        begins = [row for row in inspected["records"] if row["event"] == "recovery_round_begin" and row["event_hash"] == selected.get("begin_hash")]
        _require(len(begins) == 1 and begins[0]["payload"]["round_id"] == rid
                 and begins[0]["payload"]["source_fingerprints"] == _prepared_sources(), "completed recovery round mismatch")
        aid = "recovery-" + rid + "-qualification"
        item = inspected["artifacts"].get(aid, {})
        _require(item.get("complete") is True and item["spec"]["sha256"] == state.get("recovery_qualification_sha256"), "completed recovery report not bound")
        receipt = item["spec"]
        report = _read_bound_record({"path": str(settings.output_policy.attempt_path(contract) / receipt["relative_path"]),
                                     "sha256": receipt["sha256"], "bytes": receipt["bytes"]})
        evidence_item = inspected["artifacts"].get("recovery-" + rid + "-evidence", {})
        _require(evidence_item.get("complete") is True and evidence_item["spec"]["sha256"] == report.get("recovery_evidence_sha256")
                 and report.get("recovery_round_id") == rid and report.get("recovery_round_begin_hash") == selected["begin_hash"]
                 and report.get("sample_eligibility") == "NOT_ASSESSED" and report.get("formal_release_eligible") is False,
                 "completed recovery evidence mismatch")
        evidence_spec = evidence_item["spec"]
        _read_bound_record({"path": str(settings.output_policy.attempt_path(contract) / evidence_spec["relative_path"]),
                            "sha256": evidence_spec["sha256"], "bytes": evidence_spec["bytes"]})
        return {"status": "PREPARED_ALREADY_RECOVERED", "sample_eligibility": "NOT_ASSESSED", "formal_release_eligible": False,
                "recovery_round_id": rid}
    finally:
        j._unlock_file(fd)


@dataclass(frozen=True)
class RecoveryRoundRef:
    round_id: str
    begin_hash: str
    evidence_receipt: dict[str, Any]
    report_receipt: dict[str, Any]

    def verify(self, lease, journal, report):
        selected = lease.state.get("recovery_round")
        _require(type(selected) is dict and selected.get("round_id") == self.round_id
                 and selected.get("begin_hash") == self.begin_hash, "recovery round is not lease-selected")
        begins = [row for row in journal.records() if row["event"] == "recovery_round_begin" and row["event_hash"] == self.begin_hash]
        _require(len(begins) == 1 and begins[0]["payload"]["round_id"] == self.round_id
                 and begins[0]["payload"]["contract_sha256"] == journal.contract.sha256
                 and begins[0]["payload"]["source_fingerprints"] == _prepared_sources()
                 and begins[0]["payload"]["rules_sha256"] == report["rules_sha256"]
                 and report.get("recovery_round_id") == self.round_id and report.get("recovery_round_begin_hash") == self.begin_hash,
                 'configs/collection/scenarios.json: runner definition')
        for suffix, expected in (("evidence", self.evidence_receipt), ("qualification", self.report_receipt)):
            aid = "recovery-" + self.round_id + "-" + suffix
            value, receipt = aux.read_artifact(journal, aid)
            _require(all(receipt[k] == expected[k] for k in ("artifact_id", "relative_path", "sha256", "bytes"))
                     and receipt["relative_path"] == "artifacts/recovery/" + self.round_id + "/" + suffix + ".json",
                     "recovery round receipt/path mismatch")
            if suffix == "evidence":
                _require(value.get("recovery_round_id") == self.round_id and value.get("recovery_round_begin_hash") == self.begin_hash
                         and report.get("recovery_evidence_sha256") == receipt["sha256"], "recovery evidence identity mismatch")
            else:
                _require(value == report, "recovery round report differs")
        journal.verify_recovery_residue()


def _initializer_prefix(raw, template, *, hex_markers=(), time_marker=None):
    """Recognize an actual canonical initializer prefix without filling bytes."""
    encoded_template = _json_bytes(template)
    tokens = [(marker.encode(), "hex", size) for marker, size in hex_markers]
    if time_marker:
        tokens.append((time_marker.encode(), "time", 40))
    positions = sorted((encoded_template.index(token), token, kind, size) for token, kind, size in tokens)
    cursor, offset = 0, 0
    for index, token, kind, size in positions:
        literal = encoded_template[cursor:index]
        remainder = raw[offset:]
        if len(remainder) <= len(literal):
            return literal.startswith(remainder)
        if not remainder.startswith(literal):
            return False
        offset += len(literal)
        remainder = raw[offset:]
        if kind == "hex":
            fragment = remainder[:size]
            if not re.fullmatch(rb"[0-9a-f]*", fragment):
                return False
            if len(remainder) <= size:
                return True
            offset += size
        else:
            end = remainder.find(b'"')
            fragment = remainder if end < 0 else remainder[:end]
            if len(fragment) > size or not re.fullmatch(rb"[0-9T:+.\-]*", fragment):
                return False
            if end < 0:
                return True
            try:
                datetime.fromisoformat(fragment.decode())
            except ValueError:
                return False
            offset += end
        cursor = index + len(token)
    return encoded_template[cursor:].startswith(raw[offset:])


def _bootstrap_residue(contract, policy, manifest):
    """Bounded inventory only: never repairs/moves initializer bytes."""
    path = policy.attempt_path(contract)
    if not path.exists():
        return []
    _require(path.is_dir() and path.resolve() == path, "bootstrap attempt directory invalid")
    result = []
    context = contract.context.to_dict()
    owner_raw, owner_value, event_rows = None, None, []
    files = {}
    for entry in sorted(path.rglob("*")):
        _require(entry.resolve() == entry, "bootstrap residue redirected")
        if entry.is_dir():
            _require(entry.relative_to(path).as_posix() in {"artifacts", "artifacts/auxiliary"}, "unknown bootstrap directory")
            continue
        relative = entry.relative_to(path).as_posix()
        allowed = relative in {"writer.lock", "owner.json", "contract.json", "events.jsonl", "head.json", "artifacts/auxiliary/aux-manifest.json"}
        allowed = allowed or (entry.parent == path and (re.fullmatch(r"head-[0-9a-f]{32}\.tmp", entry.name)
                                                       or re.fullmatch(r"INITIALIZATION-ABORTED-round-[0-9]{6}\.json", entry.name)))
        _require(allowed and entry.stat().st_size <= 2_000_000, "unknown/oversized bootstrap residue")
        if relative == "writer.lock":
            # Windows byte-range locks prohibit a second handle reading this
            # coordination byte while we own the lock. It is not an evidence
            # payload; retain the file, verify shape, and never hash by reopening.
            _require(entry.stat().st_size == 1, "bootstrap writer lock shape invalid")
            continue
        with entry.open("rb") as stream:
            raw = stream.read(2_000_001)
        _require(len(raw) <= 2_000_000, "bootstrap residue exceeds bound")
        files[relative] = raw
        if relative == "contract.json":
            _require(contract.to_json().encode().startswith(raw), "bootstrap contract residue conflicts")
        if relative == "owner.json":
            template = {"schema": j.JOURNAL_SCHEMA, "run_id": context["run_id"], "attempt_id": context["attempt_id"],
                        "owner_id": context["owner_id"], "ownership": "OWNERSHIPHEX", "purpose": contract.to_dict()["purpose"],
                        "contract_sha256": contract.sha256, "scenario": contract.to_dict()["scenario"], "profile_id": context["profile_id"]}
            _require(_initializer_prefix(raw, template, hex_markers=(("OWNERSHIPHEX", 32),)), "unrecognized bootstrap owner prefix")
            try:
                owner = json.loads(raw)
            except (ValueError, UnicodeError):
                owner = None
            if owner is not None:
                _require(type(owner) is dict and owner.get("contract_sha256") == contract.sha256
                         and all(owner.get(k) == context[k] for k in ("run_id", "attempt_id", "owner_id", "profile_id")),
                         "bootstrap owner identity conflicts")
                owner_raw, owner_value = raw, owner
        if relative == "events.jsonl":
            previous = "0" * 64
            for index, line in enumerate(raw.splitlines(), 1):
                try:
                    row = json.loads(line)
                except (ValueError, UnicodeError):
                    _require(index == len(raw.splitlines()) and not raw.endswith(b"\n"), "corrupt bootstrap event")
                    break
                body = {k: v for k, v in row.items() if k != "event_hash"}
                _require(row.get("event") in {"created", "artifact_intent", "artifact_complete"}
                         and row.get("sequence") == index and row.get("previous_hash") == previous
                         and row.get("event_hash") == c.canonical_sha256(body)
                         and row.get("run_id") == context["run_id"] and row.get("attempt_id") == context["attempt_id"],
                         "bootstrap events indicate activation or corruption")
                if row["event"] != "created":
                    _require(row["payload"].get("artifact_id") == "aux-manifest", "bootstrap unexpected artifact history")
                previous = row["event_hash"]
                event_rows.append(row)
        result.append({"path": str(entry), "sha256": _hash(raw), "bytes": len(raw)})
    # Actual create ordering: contract -> owner -> empty events -> head -> created.
    if any(name in files for name in ("events.jsonl", "head.json")) or any(name.startswith("head-") for name in files):
        _require(owner_value is not None and files.get("contract.json") == contract.to_json().encode(), "initializer files precede valid identity")
    if "artifacts/auxiliary/aux-manifest.json" in files:
        _require(_json_bytes(manifest).startswith(files["artifacts/auxiliary/aux-manifest.json"]), "bootstrap manifest residue conflicts")
    if owner_value is not None:
        owner_hash = _hash(owner_raw)
        artifact = {"artifact_id": "aux-manifest", "relative_path": "artifacts/auxiliary/aux-manifest.json",
                    "sha256": _hash(_json_bytes(manifest)), "bytes": len(_json_bytes(manifest)), "media_type": "application/json"}
        _require(len(event_rows) <= 3, "unexpected initializer event count")
        for index, row in enumerate(event_rows):
            expected_event = ("created", "artifact_intent", "artifact_complete")[index]
            expected_payload = {"metadata_sha256": owner_hash} if index == 0 else artifact
            _require(row["event"] == expected_event and row["payload"] == expected_payload
                     and row.get("ownership") == owner_value["ownership"] and row.get("schema") == j.JOURNAL_SCHEMA,
                     "bootstrap event is not the exact initialization chain")
        expected_head = [{"sequence": i, "event_hash": "0" * 64 if i == 0 else event_rows[i - 1]["event_hash"], "metadata_sha256": owner_hash}
                         for i in range(len(event_rows) + 1)]
        for name, raw in files.items():
            if name == "head.json" or re.fullmatch(r"head-[0-9a-f]{32}\.tmp", name):
                _require(any(_json_bytes(value).startswith(raw) for value in expected_head), "unrecognized bootstrap head prefix")
        events_raw = files.get("events.jsonl", b"")
        lines = events_raw.splitlines()
        if lines and not events_raw.endswith(b"\n") and len(lines) > len(event_rows):
            next_index = len(event_rows)
            _require(next_index < 3, "unexpected bootstrap event tail")
            template = {"schema": j.JOURNAL_SCHEMA, "sequence": next_index + 1,
                        "previous_hash": "0" * 64 if not event_rows else event_rows[-1]["event_hash"],
                        "run_id": context["run_id"], "attempt_id": context["attempt_id"], "ownership": owner_value["ownership"],
                        "recorded_at_utc": "TIMESTAMP", "event": ("created", "artifact_intent", "artifact_complete")[next_index],
                        "payload": {"metadata_sha256": owner_hash} if next_index == 0 else artifact, "event_hash": "EVENTHEX"}
            _require(_initializer_prefix(lines[-1], template, hex_markers=(("EVENTHEX", 64),), time_marker="TIMESTAMP"), "unrecognized bootstrap event prefix")
    return result


class BootstrapRecoveryVerifier:
    def __init__(self, token, contract, settings, environment_reader, plans, adapters, quality_rules, clock,
                 reservation_ref, evidence_ref, report_ref, tombstone_ref):
        _require(token is _PREPARED_TOKEN, "bootstrap verifier must be built by the runner")
        self.contract, self.settings, self.reader, self.plans, self.adapters, self.rules, self.clock = contract, settings, environment_reader, plans, adapters, quality_rules, clock
        self.reservation_ref, self.evidence_ref, self.report_ref, self.tombstone_ref = reservation_ref, evidence_ref, report_ref, tombstone_ref

    def verify(self, lease, report):
        _require(not lease._closed and lease.contract.sha256 == self.contract.sha256 and lease.mode == self.settings.mode
                 and json.loads(lease.state_path.read_bytes()) == lease.state, "bootstrap lease binding invalid")
        aux.validate_adapters(self.plans, self.adapters, lease.mode)
        declaration = lease.state.get("auxiliary", {})
        _require(lease.state["status"] == "dirty" and lease.state["workers"] == "not_started"
                 and declaration.get("initialization") == "INITIALIZING"
                 and declaration.get("reservation") == self.reservation_ref, "bootstrap mutation authorization already issued")
        reservation = _read_bound_record(self.reservation_ref)
        _require(reservation["phase"] == "INITIALIZING" and reservation["mutations_authorized"] is False
                 and reservation["contract"] == self.contract.to_dict()
                 and reservation["manifest"]["settings_sha256"] == _prepared_settings_identity(self.settings)
                 and reservation["manifest"]["source_fingerprints"] == _prepared_sources()
                 and reservation["manifest"]["plans"] == [plan.to_dict() for plan in self.plans]
                 and reservation["manifest"]["environment_key"] == lease.identity.key
                 and reservation["lease_root"] == str(lease.root)
                 and self.rules.sha256 == self.contract.context.to_dict()["fingerprints"]["quality_rules"], "bootstrap reservation/source mismatch")
        evidence = _read_bound_record(self.evidence_ref)
        _require(_read_bound_record(self.report_ref) == report and report.get("schema_version") == "rq4-collect/bootstrap-recovery-v1"
                 and report.get("sample_eligibility") == "NOT_ASSESSED" and report.get("formal_release_eligible") is False
                 and report.get("evidence_ref") == self.evidence_ref and report.get("tombstone_ref") == self.tombstone_ref
                 and lease.state.get("bootstrap_round") == report["round"], 'configs/collection/scenarios.json: runner definition')
        tombstone = _read_bound_record(self.tombstone_ref)
        _require(tombstone.get("reservation_ref") == self.reservation_ref and tombstone.get("round") == report["round"], "bootstrap tombstone mismatch")
        for residue in evidence["residue"]:
            _read_bound_record(residue, parse_json=False)
        for record in evidence.get("previous_records", []):
            _read_bound_record(record, parse_json=False)
        current = _bootstrap_residue(self.contract, self.settings.output_policy, reservation["manifest"])
        expected_paths = {v["path"] for v in evidence["residue"]} | {self.tombstone_ref["path"]}
        _require({v["path"] for v in current} == expected_paths, "bootstrap residue inventory changed")
        for plan in self.plans:
            adapter = self.adapters[plan.aux_id]
            current = adapter.read(plan)
            _require(aux.original_matches(current, plan.expected_original) if type(adapter) is pr.PricingRouteAdapter else current == plan.expected_original,
                     "bootstrap original state changed")
        read_services = type("BootstrapEnvironment", (), {"environment_reader": staticmethod(self.reader)})()
        _environment_check(self.contract, self.settings, read_services, self.clock, require_empty=True)
        _verify_recovery_evidence(self.contract, self.settings, self.rules.to_dict(),
                                  {"components": evidence["components"], "checksums": evidence["checksums"]}, _prepared_kind(self.settings.mode), self.clock)


def abort_prepared_initialization(contract, settings, environment_reader, plans, adapters, quality_rules,
                                  evidence_supplier, *, execute=False, clock=None):
    """Abort positively reserved, unactivated preparation; never repairs journal."""
    preview = _validate_preparation(contract, settings, environment_reader, plans, adapters, quality_rules)
    _require(type(execute) is bool and callable(evidence_supplier), "explicit bootstrap execution and fresh supplier required")
    if not execute:
        return {**preview, "status": "bootstrap_abort_validated_not_executed"}
    clock = clock if clock is not None else SystemClock()
    _require(settings.mode == "isolated_test" or type(clock) is SystemClock, "observed prepared recovery requires SystemClock")
    completed = _verified_completed_prepared_recovery(contract, settings, kind="bootstrap")
    if completed is not None:
        return completed
    lease = EnvironmentLease(settings.environment, Path(settings.lease_root), contract, mode=settings.mode, resume=True)
    writer = None
    try:
        declaration = lease.state.get("auxiliary", {})
        _require(declaration.get("initialization") == "INITIALIZING" and lease.state["workers"] == "not_started",
                 "bootstrap abort requires unactivated positive reservation")
        reservation_ref = declaration.get("reservation")
        reservation = _read_bound_record(reservation_ref)
        _require(reservation["schema_version"] == BOOTSTRAP_SCHEMA and reservation["mutations_authorized"] is False
                 and reservation["phase"] == "INITIALIZING" and reservation["contract"] == contract.to_dict()
                 and reservation["manifest"]["plans"] == [plan.to_dict() for plan in plans]
                 and reservation["manifest"]["source_fingerprints"] == _prepared_sources()
                 and reservation["manifest"]["settings_sha256"] == _prepared_settings_identity(settings), "bootstrap reservation mismatch")
        path = settings.output_policy.attempt_path(contract)
        if path.exists():
            _require(path.is_dir() and path.resolve() == path, "bootstrap directory redirected")
            # Existing lock only: an absent writer file is itself an earlier
            # initialization stage, not permission to manufacture journal files.
            if (path / "writer.lock").exists():
                _require((path / "writer.lock").resolve() == path / "writer.lock", "bootstrap writer redirected")
                writer = j._lock_file(path / "writer.lock")
        residue = _bootstrap_residue(contract, settings.output_policy, reservation["manifest"])
        read_services = type("BootstrapEnvironment", (), {"environment_reader": staticmethod(environment_reader)})()
        environment = _environment_check(contract, settings, read_services, clock, require_empty=True)
        original = []
        for plan in plans:
            adapter = adapters[plan.aux_id]
            value = adapter.read(plan)
            _require(aux.original_matches(value, plan.expected_original) if type(adapter) is pr.PricingRouteAdapter else value == plan.expected_original,
                     "bootstrap actual resource differs from original")
            original.append({"aux_id": plan.aux_id, "observed": value})
        directory = Path(reservation_ref["path"]).parent
        old_files, rounds = [], []
        for candidate in sorted(directory.iterdir()):
            if candidate.name == "reservation.json":
                continue
            match = re.fullmatch(r"round-([0-9]{6})-(intent|evidence|report)\.json", candidate.name)
            _require(match is not None and candidate.is_file() and candidate.resolve() == candidate and candidate.stat().st_size <= 16_000_000,
                     "unknown bootstrap recovery record")
            rounds.append(int(match.group(1)))
            raw = candidate.read_bytes()
            old_files.append({"path": str(candidate), "sha256": _hash(raw), "bytes": len(raw)})
        round_id = "round-%06d" % (max(rounds, default=0) + 1)
        intent_ref = _write_exclusive_record(directory / (round_id + "-intent.json"), {
            "schema_version": BOOTSTRAP_SCHEMA, "round_id": round_id, "reservation_ref": reservation_ref,
            "previous_records": old_files, "residue": residue, "claim": "abort_without_mutation_authorization"})
        selected = {"round_id": round_id, "intent_ref": intent_ref}
        lease.state["bootstrap_round"] = selected
        lease.save()
        context, policy = contract.context.to_dict(), quality_rules.to_dict()
        identity = {"contract_sha256": contract.sha256, "run_id": context["run_id"], "attempt_id": context["attempt_id"]}
        supplied = evidence_supplier({**identity, "phase": "recovery_qualification", "evidence_kind": _prepared_kind(settings.mode),
                                      "component_sources": policy["recovery"]["component_sources"], "checksum_source": policy["recovery"]["checksum_source"]})
        components, checksum = _verify_recovery_evidence(contract, settings, policy, supplied, _prepared_kind(settings.mode), clock)
        evidence_ref = _write_exclusive_record(directory / (round_id + "-evidence.json"), {
            **identity, "round": selected, "evidence_kind": _prepared_kind(settings.mode), "environment": environment, "original_resources": original,
            "components": components, "checksums": checksum, "residue": residue, "previous_records": old_files})
        # A tombstone reserves the original attempt ID without inventing a
        # contract/owner/event chain. All incomplete initializer bytes stay put.
        settings.output_policy.check_root()
        path.mkdir(parents=True, exist_ok=True)
        _require(path.resolve() == path, "bootstrap attempt path changed")
        tombstone_ref = _write_exclusive_record(path / ("INITIALIZATION-ABORTED-" + round_id + ".json"), {
            **identity, "reservation_ref": reservation_ref, "round": selected, "sample_eligibility": "NOT_ASSESSED"})
        report = {**identity, "schema_version": "rq4-collect/bootstrap-recovery-v1", "round": selected,
                  "evidence_kind": _prepared_kind(settings.mode),
                  "evidence_ref": evidence_ref, "tombstone_ref": tombstone_ref, "rules_sha256": quality_rules.sha256,
                  "sample_eligibility": "NOT_ASSESSED", "formal_release_eligible": False,
                  "checks": {"never_activated": "PASS", "original_resources": "PASS", "environment_empty": "PASS",
                             "components_healthy": "PASS", "checksums_unchanged": "PASS", "residue_preserved": "PASS"}}
        report_ref = _write_exclusive_record(directory / (round_id + "-report.json"), report)
        verifier = BootstrapRecoveryVerifier(_PREPARED_TOKEN, contract, settings, environment_reader, plans, adapters,
                                             quality_rules, clock, reservation_ref, evidence_ref, report_ref, tombstone_ref)
        lease.mark_clean(None, report, scope="bootstrap_recovery", bootstrap_verifier=verifier)
        return {"status": "BOOTSTRAP_ABORT_RECOVERED", "environment_lease_status": lease.state["status"],
                "sample_eligibility": "NOT_ASSESSED", "formal_release_eligible": False, "report_ref": report_ref}
    except BaseException as exc:
        return {"status": "BLOCKED_INITIALIZATION_INCOMPLETE", "error": _error_summary(exc), "environment_lease_status": "dirty",
                "sample_eligibility": "NOT_ASSESSED", "formal_release_eligible": False}
    finally:
        if writer is not None:
            j._unlock_file(writer)
        lease.close()


class PreparedAttemptSession:
    """Runner-owned, single-consumption holder of one physical lease and journal.

    Closing releases handles only; it never implies recovery. The first slice
    supports isolated fake adapters and rejects live entry at validation time.
    """
    def __init__(self, token, contract, settings, environment_reader, plans, adapters, quality_rules, clock, lease, journal,
                 cleanup_only=False):
        _require(token is _PREPARED_TOKEN, "prepared sessions must be reserved by the runner")
        _require(type(cleanup_only) is bool, "cleanup_only must be a typed bool")
        self.contract, self.settings, self.environment_reader = contract, settings, environment_reader
        self.plans, self.adapters, self.quality_rules = plans, dict(adapters), quality_rules
        self.clock, self.lease, self.journal = clock, lease, journal
        self.cleanup_only = cleanup_only
        self.settings_sha256, self.sources = _prepared_settings_identity(settings), _prepared_sources()
        self.closed, self.consumed, self.services = False, False, None
        self.rebind_journal(journal)

    def rebind_journal(self, journal):
        self.journal = journal
        self.transaction = aux.AuxiliaryTransaction(journal, self.lease.state["auxiliary"], self.plans, self.adapters)
        manifest = self.transaction.manifest
        if not self.cleanup_only:
            
            # (the driver records a RECOVERY-DRIFT-AUDIT for the BLOCKED
            # attempt); environment binding still binds below.
            _require(manifest["settings_sha256"] == self.settings_sha256 and manifest["source_fingerprints"] == self.sources,
                     "prepared source/settings fingerprint changed")
        _require(manifest["environment_key"] == self.settings.environment.key
                 and manifest["lease_root"] == str(self.settings.lease_root), "prepared environment/root mismatch")
        for plan in self.plans:
            adapter = self.adapters[plan.aux_id]
            if type(adapter) is pr.PricingRouteAdapter:
                adapter.bind(journal, self.lease.state["auxiliary"], plan.aux_id, pr.PricingSessionAuthority(self.lease, journal))

    def _open(self, *, allow_pending=False):
        _require(not self.closed and not self.lease._closed and not self.journal._closed, "prepared session closed")
        _require(self.lease.state["status"] == "dirty", "prepared session no longer dirty")
        _require(self.lease.state["auxiliary"].get("activation") == "READY_FOR_SETUP", "prepared mutation authorization absent")
        _require(json.loads(self.lease.state_path.read_bytes()) == self.lease.state, "prepared persistent lease changed")
        if not self.cleanup_only:
            _require(_prepared_settings_identity(self.settings) == self.settings_sha256 and _prepared_sources() == self.sources,
                     "prepared source/settings changed")
        self.transaction.verify_manifest(allow_pending=allow_pending)

    def prepare_auxiliaries(self):
        self._open()
        _require(not self.consumed, "prepared session already consumed")
        self.transaction.setup()
        return {"status": "AUXILIARIES_PREPARED", "sample_eligibility": "NOT_ASSESSED"}

    def check_active(self, services, clock):
        self._open()
        value = _environment_check(self.contract, self.settings, services, clock, require_empty=False)
        return self.transaction.active_environment(value)

    def consume(self, contract, settings, services, clock):
        self._open()
        _require(self.transaction.manifest.get("execution_kind") != "auxiliary_validation", "observed auxiliary validation cannot start sample collection")
        _require(not self.consumed and contract.sha256 == self.contract.sha256
                 and contract.to_dict() == self.contract.to_dict()
                 and _prepared_settings_identity(settings) == self.settings_sha256, "prepared session reuse/contract/settings mismatch")
        _require(clock is self.clock, "prepared session clock changed")
        self.validate_runtime_auxiliary(services)
        raw = self.check_active(services, clock)
        binding = {"contract_sha256": contract.sha256, "manifest_sha256": self.lease.state["auxiliary"]["sha256"],
                   "pins": [(fid, asdict(pin)) for fid, pin in services.pinned_targets],
                   "db_bindings": [(fid, asdict(pin)) for fid, pin in services.db_bindings],
                   "environment": raw, "evidence_kind": _prepared_kind(settings.mode)}
        aux.write_artifact(self.journal, "aux-runtime-binding", binding)
        self.consumed, self.services = True, services
        if "prepared_outer_workers" in self.lease.state:
            self.lease.state["prepared_inner_workers"] = "running"
            self.lease.workers("running")

    def begin_outer_workers(self):
        """One driver-owned proxy group; never a second environment lease."""
        self._open()
        _require(not self.consumed and "prepared_outer_workers" not in self.lease.state, "outer workers already declared")
        self.lease.state.update(prepared_outer_workers="running", prepared_inner_workers="not_started")
        self.lease.workers("running")

    def record_inner_workers(self, drained):
        _require(type(drained) is bool and not self.lease._closed, "inner worker result invalid")
        self.lease.state["prepared_inner_workers"] = "drained" if drained else "unconfirmed"
        outer = self.lease.state.get("prepared_outer_workers", "drained")
        aggregate = "unconfirmed" if not drained or outer == "unconfirmed" else "running" if outer == "running" else "drained"
        self.lease.workers(aggregate)

    def end_outer_workers(self, *, drained):
        _require(type(drained) is bool and self.lease.state.get("prepared_outer_workers") == "running", "outer worker result invalid")
        self.lease.state["prepared_outer_workers"] = "drained" if drained else "unconfirmed"
        inner = self.lease.state.get("prepared_inner_workers", "not_started")
        self.lease.workers("drained" if drained and inner in {"not_started", "drained"} else "unconfirmed")

    def validate_runtime_auxiliary(self, services):
        pins = dict(services.pinned_targets)
        for plan in self.plans:
            for fid in plan.terminal_host_fault_ids:
                _require(fid in pins and pins[fid].entity == "host"
                         and pins[fid].deployment == plan.target["name"]
                         and self.adapters[plan.aux_id].fault_objects is getattr(services.primitive_client, "objects", None),
                         "terminal auxiliary requires the exact runtime carrier/fault inventory")

    def recover(self, evidence_supplier, *, services=None):
        self._open(allow_pending=True)
        selected = services if services is not None else self.services
        return _recover_prepared_session(self, selected, evidence_supplier)

    def close(self):
        if not self.closed:
            self.closed = True
            self.journal.close()
            # Do not close an OS lock owned by still-running inner workers.
            # run_attempt's existing drain callback closes it once drained.
            if self.lease.state.get("workers") in {"not_started", "drained"}:
                self.lease.close()

    def __enter__(self):
        self._open()
        return self

    def __exit__(self, *args):
        self.close()


def prepare_attempt(contract, settings, environment_reader, plans, adapters, quality_rules, *, execute=False, clock=None,
                    execution_kind=None):
    preview = _validate_preparation(contract, settings, environment_reader, plans, adapters, quality_rules)
    kind = execution_kind or ("auxiliary_validation" if settings.mode == "controlled_pilot" else "prepared_sample")
    _require(execution_kind is None or (kind == "registered_sample" and settings.mode == "controlled_pilot")
             or (kind == "engineering_smoke" and contract.to_dict()["purpose"] == "smoke"
             and len(contract.faults) == 1 and contract.faults[0].normalized_root_entity == "catalog-gw"
             and (contract.to_dict()["scenario"]["scenario_id"], contract.faults[0].to_dict()["fault_type"],
                  contract.faults[0].to_dict()["mechanism"]) in {
                      ("S01", "network_delay", "chaos_mesh"),
                      ("S23", "retry_policy_misconfiguration", g.MECHANISM),
                      ("S24", "timeout_misconfiguration", g.MECHANISM),
                      ("S02", "network_loss", "chaos_mesh")}),
             "engineering smoke opt-in is limited to registered gateway-family singles and their original mechanisms")
    _require(type(execute) is bool, "execute must be explicit boolean")
    if not execute:
        return preview
    _require(not settings.output_policy.attempt_path(contract).exists(), "prepared attempt already exists")
    _require_registered_preparation(settings)
    clock = clock if clock is not None else SystemClock()
    _require(settings.mode == "isolated_test" or type(clock) is SystemClock, "observed prepared execution requires SystemClock")
    read_services = type("PreparationEnvironment", (), {"environment_reader": staticmethod(environment_reader)})()
    _environment_check(contract, settings, read_services, clock, require_empty=True)
    context = contract.context.to_dict()
    manifest = {"schema_version": aux.SCHEMA, "contract_sha256": contract.sha256,
                "evidence_kind": _prepared_kind(settings.mode),
                "execution_kind": kind,
                **{k: context[k] for k in ("run_id", "attempt_id", "evidence_root", "output_root")},
                "environment_key": settings.environment.key, "cluster_uid": settings.environment.cluster_uid,
                "namespace_uid": settings.environment.namespace_uid, "mode": settings.mode,
                "lease_root": str(settings.lease_root), "settings_sha256": _prepared_settings_identity(settings),
                "source_fingerprints": _prepared_sources(), "plans": [plan.to_dict() for plan in plans],
                "adapter_policies": {plan.aux_id: asdict(adapters[plan.aux_id].policy) for plan in plans if type(adapters[plan.aux_id]) is pr.PricingRouteAdapter}}
    lease = EnvironmentLease(settings.environment, Path(settings.lease_root), contract, mode=settings.mode, resume=False,
                             prepared_manifest=manifest)
    journal = None
    try:
        reservation_ref = lease.state["auxiliary"]["reservation"]
        _environment_check(contract, settings, read_services, clock, require_empty=True)
        journal = j.AttemptJournal.create(contract, settings.output_policy)
        receipt = aux.write_artifact(journal, "aux-manifest", manifest)
        lease.state["auxiliary"] = {"required": True, "activation": "READY_FOR_SETUP", "reservation": reservation_ref,
                                    **{k: receipt[k] for k in ("artifact_id", "relative_path", "sha256", "bytes")}}
        lease.save()
        return PreparedAttemptSession(_PREPARED_TOKEN, contract, settings, environment_reader, plans, adapters, quality_rules, clock, lease, journal)
    except BaseException as exc:
        if journal is not None:
            journal.close()
        lease.close()
        raise RunnerError("BLOCKED_INITIALIZATION_INCOMPLETE: " + _error_summary(exc)) from exc


class _NoFaultsAdapter:
    evidence_kind = "fake"
    def __init__(self, mode="isolated_test"):
        _prepared_kind(mode)
        self.evidence_kind = "live" if mode == "controlled_pilot" else "fake"
    def observe(self, *args):
        raise RunnerError("setup-only recovery encountered fault intent")
    compare_and_apply = observe
    compare_and_restore = observe


def _recover_prepared_session(session, services, evidence_supplier):
    lease, journal = session.lease, session.journal
    contract, settings, clock = session.contract, session.settings, session.clock
    try:
        _require(callable(evidence_supplier), "fresh prepared recovery supplier required")
        _require(lease.state.get("workers") in {"not_started", "drained"}, "prepared workers not drained")
        journal.mark_failed("prepared_recovery_only")
        begin = journal.begin_recovery_round(source_fingerprints=_prepared_sources(), rules_sha256=session.quality_rules.sha256)
        round_id, begin_hash = begin["payload"]["round_id"], begin["event_hash"]
        lease.state["recovery_round"] = {"round_id": round_id, "begin_hash": begin_hash}
        lease.save()
        for artifact_id in journal.pending_artifact_ids():
            state = journal._artifact_states(journal.records())[artifact_id]
            target = journal.path / state["spec"]["relative_path"]
            if target.exists():
                journal.audit_incomplete_artifact(artifact_id, round_id=round_id, round_begin_hash=begin_hash,
                                                 reason_code="prepared_recovery_audit", max_bytes=settings.max_artifact_bytes)
            else:
                journal.abandon_incomplete_artifact(artifact_id, reason_code="prepared_recovery_absent_artifact")
        session.transaction.verify_manifest()
        rows = journal.records()
        has_faults = any(row["event"] == "intent" for row in rows)
        has_binding = any(row["event"] in {"artifact_complete", "artifact_recovered_complete"}
                          and row["payload"].get("artifact_id") == "aux-runtime-binding" for row in rows)
        if services is None:
            _require(not has_faults and not has_binding, "bound/fault attempt requires original services")
            fault_adapter = _NoFaultsAdapter(settings.mode)
        else:
            validate_run(contract, settings, services)
            session.validate_runtime_auxiliary(services)
            if has_binding:
                binding, _ = aux.read_artifact(journal, "aux-runtime-binding")
                _require(binding["pins"] == aux.clone([(fid, asdict(pin)) for fid, pin in services.pinned_targets])
                         and binding["db_bindings"] == aux.clone([(fid, asdict(pin)) for fid, pin in services.db_bindings]), "prepared recovery runtime pins differ")
            else:
                _require(not has_faults, "fault intent lacks prepared runtime binding")
            fault_adapter = _primitive_executor(journal, services, auxiliary_transaction=session.transaction).adapter
        recovery = journal.reconcile(fault_adapter)
        _require(recovery.status == ("CLEAN_LIVE" if settings.mode == "controlled_pilot" else "CLEAN_OFFLINE"), "prepared fault recovery blocked")
        session.transaction.cleanup()
        if services is not None:
            fault_adapter = _primitive_executor(journal, services, auxiliary_transaction=session.transaction).adapter
        recovery = journal.reconcile(fault_adapter)
        _require(recovery.status == ("CLEAN_LIVE" if settings.mode == "controlled_pilot" else "CLEAN_OFFLINE"), "prepared terminal fault recovery blocked")
        read_services = type("PreparationEnvironment", (), {"environment_reader": staticmethod(session.environment_reader)})()
        environment = _environment_check(contract, settings, read_services, clock, require_empty=True)
        lease.workers("drained")
        context, policy = contract.context.to_dict(), session.quality_rules.to_dict()
        identity = {"contract_sha256": contract.sha256, "run_id": context["run_id"], "attempt_id": context["attempt_id"]}
        supplied = evidence_supplier({**identity, "phase": "recovery_qualification", "evidence_kind": _prepared_kind(settings.mode),
                                      "component_sources": policy["recovery"]["component_sources"],
                                      "checksum_source": policy["recovery"]["checksum_source"]})
        components, checksum = _verify_recovery_evidence(contract, settings, policy, supplied, _prepared_kind(settings.mode), clock)
        evidence = {**identity, "schema_version": RECOVERY_SCHEMA, "evidence_kind": _prepared_kind(settings.mode),
                    "recovery_round_id": round_id, "recovery_round_begin_hash": begin_hash,
                    "attempt_state": journal.state, "recovery": recovery.__dict__, "environment": environment,
                    "components": components, "checksums": checksum,
                    "auxiliary_terminal": session.transaction.verify_terminal(),
                    "claim_scope": "prepared_recovery_only_not_sample_quality"}
        report = {**identity, "schema_version": "rq4-collect/quality-report-v1", "rules_sha256": session.quality_rules.sha256,
                  "recovery_round_id": round_id, "recovery_round_begin_hash": begin_hash,
                  "groups": {"RECOVERY": {"status": "PASS", "checks": [{"group": "RECOVERY", "check": name, "status": "PASS",
                             "reason": "verified_fresh_recovery_evidence"} for name in RECOVERY_CHECKS]}},
                  "system_clean": "CLEAN", "sample_eligibility": "NOT_ASSESSED", "sample_purpose": contract.to_dict()["purpose"],
                  "formal_release_eligible": False, "release_gate": "NOT_EVALUATED", "evidence_kind": _prepared_kind(settings.mode),
                  "report_semantics": "recovery_qualification_only_not_sample_quality"}
        artifacts = _Artifacts(journal, settings)
        evidence_id = "recovery-" + round_id + "-evidence"
        artifacts.json(evidence_id, "recovery/" + round_id + "/evidence.json", evidence)
        _, evidence_receipt = aux.read_artifact(journal, evidence_id)
        report["recovery_evidence_sha256"] = evidence_receipt["sha256"]
        report_id = "recovery-" + round_id + "-qualification"
        artifacts.json(report_id, "recovery/" + round_id + "/qualification.json", report)
        _, report_receipt = aux.read_artifact(journal, report_id)
        round_ref = RecoveryRoundRef(round_id, begin_hash, evidence_receipt, report_receipt)
        with _journal_section(journal):
            _environment_check(contract, settings, read_services, clock, require_empty=True)
            _verify_recovery_evidence(contract, settings, policy, supplied, _prepared_kind(settings.mode), clock)
            lease.mark_clean(journal, report, scope="recovery", auxiliary_verifier=session.transaction, recovery_ref=round_ref,
                             cleanup_only=session.cleanup_only)
        return {"schema_version": RUNNER_SCHEMA, "status": "RECOVERY_QUALIFIED", "sample_eligibility": "NOT_ASSESSED",
                "formal_release_eligible": False, "environment_lease_status": lease.state["status"],
                "report": report, "artifacts": list(artifacts.descriptors)}
    except BaseException as exc:
        return {"schema_version": RUNNER_SCHEMA, "status": "BLOCKED", "error": _error_summary(exc),
                "sample_eligibility": "NOT_ASSESSED", "formal_release_eligible": False, "environment_lease_status": "dirty"}


def recover_prepared_attempt(contract, settings, environment_reader, plans, adapters, quality_rules,
                             evidence_supplier, *, services=None, execute=False, clock=None, cleanup_only=False):
    preview = _validate_preparation(contract, settings, environment_reader, plans, adapters, quality_rules)
    _require(type(execute) is bool, "execute must be explicit boolean")
    if not execute:
        return {**preview, "status": "prepared_recovery_validated_not_executed"}
    completed = _verified_completed_prepared_recovery(contract, settings, kind="prepared")
    if completed is not None:
        return completed
    clock = clock if clock is not None else SystemClock()
    _require(settings.mode == "isolated_test" or type(clock) is SystemClock, "observed prepared recovery requires SystemClock")
    lease = EnvironmentLease(settings.environment, Path(settings.lease_root), contract, mode=settings.mode, resume=True)
    journal, session = None, None
    try:
        _require("auxiliary" in lease.state, "attempt has no persistent auxiliary declaration")
        journal = j.AttemptJournal.open_for_reconcile(contract, settings.output_policy)
        session = PreparedAttemptSession(_PREPARED_TOKEN, contract, settings, environment_reader, plans, adapters, quality_rules, clock, lease, journal,
                                         cleanup_only=cleanup_only)
    except BaseException as exc:
        if journal is not None:
            journal.close()
        lease.close()
        return {"status": "BLOCKED_INITIALIZATION_INCOMPLETE", "error": _error_summary(exc),
                "environment_lease_status": "dirty", "sample_eligibility": "NOT_ASSESSED", "formal_release_eligible": False}
    try:
        return session.recover(evidence_supplier, services=services)
    finally:
        session.close()


def run_attempt(contract: c.RunContract, settings: RunSettings, services: RunServices, *, execute: bool = False,
                clock=None, workload_driver=None, prepared_session=None) -> dict[str, Any]:
    """Run one serial attempt, never an implicit batch/retry/release loop."""
    preview = validate_run(contract, settings, services)
    first_injection_exception = preview.get("first_injection_start_exception")
    _require(type(execute) is bool, "execute must be explicit boolean")
    if not execute:
        return preview
    if prepared_session is None:
        _require(not settings.output_policy.attempt_path(contract).exists(),
                 "attempt already exists; use explicit reconcile or a new attempt identity")
    clock = clock if clock is not None else SystemClock()
    _require(settings.mode == "isolated_test" or type(clock) is SystemClock,
             "controlled pilot must use the host SystemClock shared by the workload driver")
    driver = workload_driver if workload_driver is not None else w.run_workload
    _require(driver is w.run_workload or settings.mode == "isolated_test", "live workload must use the accepted driver")
    if prepared_session is None:
        _environment_check(contract, settings, services, clock, require_empty=True)
        lease = EnvironmentLease(settings.environment, Path(settings.lease_root), contract, mode=settings.mode, resume=False)
        journal = None
    else:
        _require(type(prepared_session) is PreparedAttemptSession, "concrete prepared session required")
        prepared_session.consume(contract, settings, services, clock)
        lease, journal = prepared_session.lease, prepared_session.journal
    sessions, operations, actions, phase_facts = [], [], [], []
    workloads, telemetry = {}, {}
    run_error, recovery, quality, descriptors = None, None, None, []
    workers_drained, retain_lease = True, False
    anchor = None
    executors = None
    artifact_failed = False
    artifacts = None
    worker_failed = threading.Event()
    def stop_on_worker_failure():
        worker_failed.set()
        for active in tuple(sessions):
            active.cancel(clock.monotonic())

    def wait_for_schedule(deadline):
        clock.wait_until(deadline, stop_event=worker_failed)
        _check_telemetry_workers(sessions)
        _require(not worker_failed.is_set(), "independent worker failure stopped the run")
    try:
        if journal is None:
            journal = j.AttemptJournal.create(contract, settings.output_policy)
        if services.telemetry.log_capture is not None:
            services.telemetry.log_capture.bind_journal(journal)
        artifacts = _Artifacts(journal, settings)
        descriptors = artifacts.descriptors
        executors = _primitive_executor(journal, services)
        _enter_phase(journal, "PREFLIGHT")
        if prepared_session is None:
            _environment_check(contract, settings, services, clock, require_empty=True)
        else:
            prepared_session.check_active(services, clock)
        _record_normal_baseline(executors, artifacts, clock, settings.mode)
        _enter_phase(journal, "WARMUP")
        warmup_start = clock.monotonic()
        clock.wait_until(warmup_start + settings.deadlines.warmup_s)
        before = clock.monotonic()
        epoch = clock.epoch()
        after = clock.monotonic()
        anchor = ClockAnchor((before + after) / 2, epoch, settings.deadlines.clock_uncertainty_s + (after - before) / 2 + .000001)
        origin = after
        plan = contract.to_dict()
        events = []
        for order, spec in enumerate(contract.faults):
            events.extend([(spec.planned_window.start, 1, order, "inject", spec.fault_instance_id),
                           (spec.planned_window.end, 0, order, "recover", spec.fault_instance_id)])
        events.sort()
        for phase in _PHASES:
            _check_telemetry_workers(sessions)
            if phase == "during_fault":
                _enter_phase(journal, "INJECTION_TRANSITION")
            elif phase == "post_recovery":
                _enter_phase(journal, "RECOVERY_TRANSITION")
                recovery = journal.reconcile(executors.adapter)
                _require(recovery.status in {"CLEAN_OFFLINE", "CLEAN_LIVE"}, "full recovery is not confirmed")
            window = plan["phases"][phase]
            scheduled = origin + window["start"]
            wait_for_schedule(scheduled)
            actual_start = clock.monotonic()
            _enter_phase(journal, {"pre_fault": "PRE", "during_fault": "DURING", "post_recovery": "POST"}[phase])
            session = _Phase(contract, phase, actual_start, window["end"] - window["start"], anchor,
                             settings, services, clock, driver, stop_on_worker_failure)
            sessions.append(session)
            phase_facts.append({"phase": phase, "planned_start_offset_s": window["start"], "actual_start_monotonic_s": actual_start,
                                "fixed_observation_end_monotonic_s": session.end, "start_lateness_s": actual_start - scheduled,
                                "duration_s": window["end"] - window["start"], "workload_epoch_is_phase_start": True,
                                "workers_started": False})
            _record_timing_budget_warning(phase_facts[-1], "phase_start_lateness_s",
                                          actual_start - scheduled,
                                          settings.deadlines.phase_start_lateness_s)
            workers_drained = False
            lease.workers("running")
            session.start_workers()
            phase_facts[-1]["workers_started"] = True
            _check_telemetry_workers(sessions)
            if phase == "during_fault":
                # Same-offset (same-end / simultaneous-tier) action pairs run
                # SERIALLY by declared budget (O-D3/O-D7 adjudications); the
                # later action's start deadline carries the predecessors'
                # declared action bounds, never just the raw schedule instant.
                same_offset_carry = 0.0
                previous_offset = None
                for event_index, (offset, _, _, action, fid) in enumerate(events):
                    if offset != previous_offset:
                        same_offset_carry = 0.0
                        previous_offset = offset
                    deadline = origin + offset + same_offset_carry
                    wait_for_schedule(deadline)
                    started = clock.monotonic()
                    event = {"instance_id": fid, "action": action, "scheduled_run_offset_s": offset,
                             "scheduled_monotonic_s": deadline, "actual_start_monotonic_s": started, "status": "started"}
                    actions.append(event)
                    start_lateness = started - deadline
                    start_lateness_budget = settings.deadlines.action_start_lateness_s
                    start_lateness_policy = "configured_budget"
                    if first_injection_exception is not None and event_index == 0:
                        _require(action == "inject" and fid == first_injection_exception["instance_id"],
                                 "first-injection exception no longer matches the first runtime event")
                        start_lateness_budget = first_injection_exception["allowance_s"]
                        start_lateness_policy = "unique_earliest_injection_exception"
                    event.update(start_lateness_s=start_lateness,
                                 action_start_lateness_budget_s=start_lateness_budget,
                                 start_lateness_policy=start_lateness_policy)
                    if first_injection_exception is not None and event_index == 0:
                        event["first_injection_downstream_margin_s"] = first_injection_exception["downstream_margin_s"]
                    _record_timing_budget_warning(event, "action_start_lateness_s",
                                                  start_lateness, start_lateness_budget)
                    if action == "inject":
                        _validate_injection_bounds(started, None, session.start, session.end,
                                                   origin + executors.spec(fid).planned_window.end)
                    try:
                        if action == "inject":
                            if fid in executors.prepared_k8s:
                                result = executors.k8s.apply(executors.prepared_k8s[fid], intent_id="fault_" + fid)
                            elif fid in executors.prepared_gateway:
                                # The gateway result has no admission-spec face;
                                # its admission chain already ran inside the
                                # preflight/server-dry-run before any write.
                                raw = executors.gateway.apply(executors.prepared_gateway[fid],
                                                              intent_prefix="fault_" + fid, execute=True)
                                result = {**raw, "admission_spec_matches": None}
                            else:
                                result = executors.db.apply(executors.prepared_db[fid], intent_id="fault_" + fid)
                            _require(result["controlled_state_matches"] is True and result["admission_spec_matches"] is not False,
                                     "primitive verification is unsuccessful")
                        else:
                            result = journal.reconcile(executors.adapter, fault_instance_ids=(fid,))
                            _require(result.status in {"SUBSET_CLEAN_OFFLINE", "SUBSET_CLEAN_LIVE"}, "selected fault recovery is uncertain")
                    except BaseException as exc:
                        ended = clock.monotonic()
                        event.update(actual_end_monotonic_s=ended, duration_s=ended - started, status="operation_failed", error_type=type(exc).__name__)
                        raise
                    ended = clock.monotonic()
                    event.update(actual_end_monotonic_s=ended, duration_s=ended - started, status="operation_confirmed_not_physical")
                    operation_phase = "during_fault" if session.start <= ended < session.end else "recovery_transition" if action == "recover" else "outside_observation"
                    operations.append(_operation_record(journal, fid, action, started, ended, anchor, services.operation_source_id,
                        "synthetic" if settings.mode == "isolated_test" else "observed", operation_phase, executors))
                    # ln GW-config transfer function: the nginx_configmap_rollout
                    # apply/reconcile includes the mechanism's DECLARED rollout
                    # settle wait (bounded by the adapter's own wait_timeout_s,
                    # after which kubectl itself fails) -- the action budget
                    # covers the CONTROL WRITES; the rollout settle is the
                    # declared transfer-function head, not a runaway write.
                    
                    
                    # deployment_env patch/restore ends with the same shape of
                    # declared rollout-settle wait (primitives._patch_env runs
                    # `kubectl rollout status --timeout=wait_timeout_s`), so the
                    # env leg's DURATION budget carries the assembly-declared
                    # env-rollout band (S07 single source; D07-a2 measured the
                    # restore settle at 12.92 s against a bare 17 s budget).
                    # The band is the DECLARED head, not the adapter's
                    # structural bound: a settle past writes+band breaches the
                    # declared transfer function and must fail honestly, so
                    # env legs take the band (15 s) here while gateway legs
                    # keep their adapter-wait split above. The serial CARRY
                    # below stays the declared schedule reservation (the
                    # generic action budget the same_offset_budgets notes'
                    # "rollout <=~15s -> successor" arithmetic reserves); a
                    # predecessor settle inside writes+band but past that
                    # reservation bills the successor's lateness gate exactly
                    # as the declaration breach it is.
                    declared_env_rollout_band_s = 0.0
                    if (fid not in executors.prepared_gateway
                            and getattr(executors.prepared_k8s.get(fid), "mode", None) == "deployment_env"):
                        try:
                            from . import scenario_definitions as _asm
                            declared_env_rollout_band_s = float(
                                _asm.S07_ROLLOUT_PROFILES[_asm.S07_DEFAULT_ROLLOUT_PROFILE].rollout_band_s)
                        except BaseException:
                            declared_env_rollout_band_s = 0.0  # stricter gate, never looser
                    
                    
                    # service_unavailable RECOVER action ends with the
                    # declared restore wait (wait_pod_ready; restore+Ready
                    # measured 11-13 s on D04-F1/F2 + D19fix6-F1) -- the tier
                    # budget (4 s at r48_fixed) covers the CR removal only.
                    # The bound is the assembly-declared restore band, never
                    # the adapter's structural wait timeout; injects and every
                    # other action keep the bare budget. The serial CARRY
                    # below stays the generic reservation (the C1 grids
                    # reserved the generic tier budget against min gaps).
                    declared_pod_restore_band_s = 0.0
                    if action == "recover":
                        leg_spec = executors.spec(fid).to_dict()
                        if (leg_spec.get("mechanism") == "chaos_mesh"
                                and leg_spec.get("fault_type") == "service_unavailable"):
                            try:
                                from . import scenario_definitions as _asm_pod
                                declared_pod_restore_band_s = float(_asm_pod.POD_FAILURE_RESTORE_BOUND_S)
                            except BaseException:
                                declared_pod_restore_band_s = 0.0  # stricter gate, never looser
                    action_bound = settings.deadlines.action_duration_s + (services.command_wait_timeout_s
                                                                          if fid in executors.prepared_gateway
                                                                          else declared_env_rollout_band_s) + declared_pod_restore_band_s
                    event["action_duration_budget_s"] = action_bound
                    _record_timing_budget_warning(event, "action_duration_s", ended - started, action_bound)
                    # The carry is the DECLARED serial schedule reservation for
                    # the same-offset successor. Gateway legs carry their
                    # adapter wait; env legs carry their declared rollout
                    # FOOTPRINT (band + write margin) -- floored at the generic
                    # tier budget, so the default tier keeps its exact previous
                    # deadlines (D07's passed same-end pair reserved 17 =
                    # band 15 + write 2 = the generic budget; byte-identical).
                    
                    # generic 4 s reservation against a ~17 s rollout footprint
                    # made the same-end successor structurally late; the
                    # footprint reservation fixes that tier without touching
                    # the default one.
                    if declared_env_rollout_band_s:
                        env_reservation = max(settings.deadlines.action_duration_s,
                                              declared_env_rollout_band_s + _ENV_ROLLOUT_WRITE_MARGIN_S)
                    else:
                        env_reservation = settings.deadlines.action_duration_s
                    same_offset_carry += (services.command_wait_timeout_s
                                          if fid in executors.prepared_gateway else env_reservation)
                    if action == "inject":
                        _validate_injection_bounds(started, ended, session.start, session.end,
                                                   origin + executors.spec(fid).planned_window.end)
            wait_for_schedule(session.end)
            workloads[phase] = session.finish_workload()
            _require(workloads[phase].get("summary", {}).get("workers_joined") is True
                     and workloads[phase]["summary"].get("driver_healthy") is True, "workload did not finish healthy and drained")
            _check_telemetry_workers(sessions)
            phase_facts[-1]["controller_finished_monotonic_s"] = clock.monotonic()
        _enter_phase(journal, "VERIFYING")
    except BaseException as exc:
        run_error = _error_summary(exc)
        for session in sessions:
            session.cancel(clock.monotonic())
    finally:
        # Stop/drain before changing any remaining fault. Telemetry readback
        # can finish its fixed historical window while workers drain.
        if sessions:
            latest = max(min(s.end, s.cancel_at) if s.cancel_at is not None else s.end for s in sessions)
            clock.wait_until(latest + settings.deadlines.telemetry_ingestion_wait_s)
        pending = []
        for session in sessions:
            # Each successful side survives a failure on the other side.
            for kind, future, destination in (("workload", session.workload, workloads), ("telemetry", session.telemetry, telemetry)):
                try:
                    _require(future is not None, kind + " worker was not started")
                    value = session.finish_workload() if kind == "workload" else future.result(timeout=settings.deadlines.worker_join_timeout_s)
                    destination[session.phase] = value
                except FutureTimeout:
                    session.cancel(clock.monotonic())
                    run_error = "WorkersUnconfirmed"
                    destination[session.phase] = {"status": "worker_unconfirmed", "worker": kind}
                except BaseException as exc:
                    run_error = run_error or _error_summary(exc)
                    destination.setdefault(session.phase, {"status": "worker_error", "worker": kind, "error_type": type(exc).__name__})
            unfinished = [f for f in (session.workload, session.telemetry) if f is not None and not f.done()]
            pending.extend(unfinished)
            session.pool.shutdown(wait=not unfinished, cancel_futures=bool(unfinished))
        workers_drained = not pending and all(session.observer_drained and session.logs_drained for session in sessions)
        if not workers_drained:
            run_error = run_error or "WorkersUnconfirmed"
        try:
            if prepared_session is not None and "prepared_outer_workers" in lease.state:
                prepared_session.record_inner_workers(workers_drained)
            else:
                lease.workers("drained" if workers_drained else "unconfirmed")
        except BaseException:
            run_error, artifact_failed = run_error or "LeasePersistenceFailed", True
        if pending:
            retain_lease = True
            if artifacts is not None:
                try:
                    _persist_worker_salvage(artifacts, workloads, telemetry)
                except BaseException:
                    artifact_failed = True
            callback_lock = threading.Lock()
            def release_only_after_drain(_):
                with callback_lock:
                    if all(f.done() for f in pending) and not lease._closed:
                        observer_drained = all(session.observer_drained and session.logs_drained for session in sessions)
                        if prepared_session is not None and "prepared_outer_workers" in lease.state:
                            prepared_session.record_inner_workers(observer_drained)
                            # The outer driver still owns the same physical lock;
                            # its confirmed shutdown/session close releases it.
                            if observer_drained and prepared_session.closed and lease.state.get("prepared_outer_workers") == "drained":
                                lease.close()  # Requested close, all groups drained; still dirty.
                        else:
                            try:
                                lease.workers("drained" if observer_drained else "unconfirmed")
                            finally:
                                lease.close()  # Persistent state remains dirty.
            for future in pending:
                future.add_done_callback(release_only_after_drain)
        if journal is not None and workers_drained:
            try:
                if run_error:
                    # mark_failed admits only a stable identifier code; the
                    # free-form run_error text rides as an extra field of the
                    # same durable row so the cause survives even when artifact
                    # persistence later fails. Effects otherwise equal mark_failed.
                    journal._append("failed", {"reason_code": "runner_exception", "run_error": run_error})
                    journal._reconciled_in_session = False
                if executors is None:
                    executors = _primitive_executor(journal, services)
                recovery = journal.reconcile(executors.adapter)
                _require(recovery.status in {"CLEAN_OFFLINE", "CLEAN_LIVE"}, "recovery remains blocked")
                if prepared_session is None:
                    _environment_check(contract, settings, services, clock, require_empty=True)
                else:
                    prepared_session.check_active(services, clock)
                if anchor is not None:
                    quality, descriptors, run_error = _persist_result(contract, settings, services, journal, sessions, workloads,
                        telemetry, operations, actions, phase_facts, anchor, run_error, artifacts)
                    # Persistence can record a telemetry failure. Reconfirm after
                    # all workers/data handling instead of treating an old CLEAN
                    # line as a release proof.
                    recovery = journal.reconcile(executors.adapter)
                    if not artifact_failed and prepared_session is None:
                        with _journal_section(journal):
                            lease.mark_clean(journal, quality, quality_rules=services.quality_rules)
            except BaseException as exc:
                run_error = run_error or _error_summary(exc)
                artifact_failed = True
                # Best effort after writer poisoning: reopen only a verified
                # existing journal, preserve failed qualification, restore owned
                # intents, and retain the dirty lease on any artifact problem.
                try:
                    journal.close()
                    journal = j.AttemptJournal.open_for_reconcile(contract, settings.output_policy)
                    if prepared_session is not None:
                        prepared_session.rebind_journal(journal)
                    # Same durable row pattern as the runner_exception site.
                    journal._append("failed", {"reason_code": "runner_artifact_or_finalization_failed", "run_error": run_error})
                    journal._reconciled_in_session = False
                    executors = _primitive_executor(journal, services)
                    recovery = journal.reconcile(executors.adapter)
                except BaseException:
                    recovery = None
        if journal is not None and prepared_session is None:
            journal.close()
        if not retain_lease and prepared_session is None:
            lease.close()
    status = ("BLOCKED" if artifact_failed or not workers_drained or recovery is None or recovery.status == "BLOCKED_DIRTY"
              or lease.state.get("status") != "clean" else "FAILED_CLEAN" if run_error else "COMPLETED_UNQUALIFIED")
    if prepared_session is not None and not artifact_failed and workers_drained and recovery is not None and recovery.status != "BLOCKED_DIRTY":
        status = "AWAITING_AUXILIARY_RECOVERY"
    return {"schema_version": RUNNER_SCHEMA, "status": status, "contract_sha256": contract.sha256,
            "run_error": run_error, "workers_drained": workers_drained, "recovery": recovery.__dict__ if recovery else None,
            "quality": quality, "artifacts": descriptors, "qualification": "not_assessed", "formal_release_eligible": False,
            "environment_lease_status": lease.state.get("status"), "actual_phase_facts": phase_facts, "action_timing": actions}


def resume_reconcile(contract: c.RunContract, settings: RunSettings, services: RunServices, *, execute: bool = False,
                     clock=None) -> dict[str, Any]:
    """Reconcile one exact dirty attempt; never resume its experiment or skip it."""
    preview = validate_run(contract, settings, services)
    _require(type(execute) is bool, "execute must be explicit boolean")
    if not execute:
        return {**preview, "status": "reconcile_validated_not_executed"}
    clock = clock if clock is not None else SystemClock()
    _require(settings.mode == "isolated_test" or type(clock) is SystemClock,
             "controlled reconciliation must use the host SystemClock")
    lease = EnvironmentLease(settings.environment, Path(settings.lease_root), contract, mode=settings.mode, resume=True)
    journal = None
    try:
        _require("auxiliary" not in lease.state, "auxiliary attempt requires recover_prepared_attempt")
        journal = j.AttemptJournal.open_for_reconcile(contract, settings.output_policy)
        journal.mark_failed("runner_resume_reconcile_only")
        _environment_check(contract, settings, services, clock, require_empty=False)
        executors = _primitive_executor(journal, services, own_carrier_removal_tolerant=True)
        recovery = journal.reconcile(executors.adapter)
        _require(recovery.status in {"CLEAN_OFFLINE", "CLEAN_LIVE"}, "residual recovery is blocked")
        _environment_check(contract, settings, services, clock, require_empty=True)
        lease.workers("drained")
        # Controlled-live resume lacks fresh Q2 telemetry here and therefore
        
        # qualification workflow, not inherit the prior sample's old result.
        quality = {"system_clean": "NOT_CONFIRMED", "sample_eligibility": "NOT_ASSESSED"}
        if settings.mode == "isolated_test":
            with _journal_section(journal):
                lease.mark_clean(journal, quality)
        removals = _owned_carrier_removal_reports(executors)
        rollouts = _coexisting_rollout_reports(executors)
        result = {"schema_version": RUNNER_SCHEMA, "status": "RECONCILED_FAILED_ATTEMPT" if lease.state["status"] == "clean" else "BLOCKED",
                  "recovery": recovery.__dict__, "quality": quality, "experiment_resumed": False,
                  "qualification": "not_assessed", "environment_lease_status": lease.state["status"]}
        if removals:
            result["owned_carriers_removed_as_intended"] = removals
        if rollouts:
            result["coexisting_pinned_pods_rolled_by_aux_as_intended"] = rollouts
        return result
    except BaseException as exc:
        return {"schema_version": RUNNER_SCHEMA, "status": "BLOCKED", "error_type": type(exc).__name__,
                "experiment_resumed": False, "qualification": "not_assessed", "environment_lease_status": "dirty"}
    finally:
        if journal is not None:
            journal.close()
        lease.close()


def _verify_recovery_evidence(contract: c.RunContract, settings: RunSettings, policy: dict[str, Any],
                              supplied: Any, expected_kind: str, clock) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Shape/health/freshness verification of freshly supplied recovery readbacks.

    Component rows mirror quality.py's Q2 component shape (with the explicit
    recovery_qualification phase); checksums mirror its checksum row shape plus
    the caller-supplied baseline. Provenance flags stay producer claims.
    """
    context = contract.context.to_dict()
    identity = {"contract_sha256": contract.sha256, "run_id": context["run_id"], "attempt_id": context["attempt_id"]}
    _require(type(supplied) is dict and set(supplied) == {"components", "checksums"},
             "recovery evidence supplier must return exactly components and checksums")
    sources = policy["recovery"]["component_sources"]
    rows = supplied["components"]
    _require(type(rows) is list and len(rows) == len(sources)
             and all(type(row) is dict for row in rows)
             and {row.get("entity") for row in rows} == set(sources),
             "one component readback per recovery component source required")
    counts = ("desired_replicas", "ready_replicas", "available_replicas", "generation", "observed_generation")
    now, max_age = clock.epoch(), settings.deadlines.environment_max_age_s
    for row in rows:
        fields = row.get("fields", {}) if type(row.get("fields")) is dict else {}
        healthy = (all(type(fields.get(name)) is int and fields[name] >= 0 for name in counts)
                   and fields["desired_replicas"] > 0 and fields["ready_replicas"] >= fields["desired_replicas"]
                   and fields["available_replicas"] >= fields["desired_replicas"]
                   and fields["observed_generation"] >= fields["generation"])
        stamp = row.get("timestamp_epoch_s")
        fresh = type(stamp) in (int, float) and math.isfinite(stamp) and 0 <= now - stamp <= max_age
        _require(all(row.get(key) == value for key, value in identity.items())
                 and row.get("phase") == "recovery_qualification" and row.get("source_id") == sources.get(row.get("entity"))
                 and row.get("evidence_kind") == expected_kind and type(row.get("return_code")) is int
                 and row["return_code"] == 0 and healthy and fresh,
                 "component readback unverified, unhealthy or stale")
    checksum = supplied["checksums"]
    _require(type(checksum) is dict, "checksum readback row invalid")
    tables = checksum.get("tables") if type(checksum.get("tables")) is dict else {}
    baseline = checksum.get("baseline") if type(checksum.get("baseline")) is dict else {}
    stamps = (checksum.get("pre_at_s"), checksum.get("post_at_s"))
    _require(all(type(stamp) in (int, float) and math.isfinite(stamp) and 0 <= now - stamp <= max_age for stamp in stamps)
             and stamps[0] <= stamps[1], "checksum readback timestamps stale or reversed")
    _require(all(checksum.get(key) == value for key, value in identity.items())
             and checksum.get("source_id") == policy["recovery"]["checksum_source"]
             and checksum.get("evidence_kind") == expected_kind and type(checksum.get("return_code")) is int
             and checksum["return_code"] == 0, "checksum readback identity/provenance unverified")
    for table in ("items", "inventory"):
        values = tables.get(table) if type(tables.get(table)) is dict else {}
        _require(type(values.get("pre")) is str and bool(values["pre"]) and values.get("post") == values["pre"]
                 and baseline.get(table) == values["pre"],
                 "checksum readback drifts from its baseline or is missing: " + table)
    return rows, checksum


def _persist_recovery_qualification(journal: j.AttemptJournal, artifacts: _Artifacts,
                                    evidence: dict[str, Any], report: dict[str, Any]) -> list[str]:
    """Write the byte-bound recovery artifacts, unlocking only interrupted writes.

    The single tolerated refusal is the incomplete-artifact guard of an attempt
    whose artifact write was interrupted -- a failed sample that by design can
    never re-finish that write. Exactly those still-pending artifacts receive
    the append-only terminal ``artifact_abandoned`` disposition (nothing is
    deleted, completed or retried; the attempt stays failed), and then both
    qualification artifacts go through the ordinary byte-bound artifact path.
    Every other refusal propagates unchanged and keeps the lease dirty.
    """
    try:
        artifacts.json("recovery-qualification-evidence", "recovery/evidence.json", evidence)
        artifacts.json("recovery-qualification", "recovery/qualification.json", report)
        return []
    except j.IncompleteArtifact:
        pass
    with _journal_section(journal):
        pending = journal.pending_artifact_ids()
        _require(pending, "recovery artifact write refused without pending artifacts")
        for artifact_id in pending:
            journal.abandon_incomplete_artifact(artifact_id, reason_code="recovery_qualification_interrupted_write")
        abandoned = list(pending)
        artifacts.json("recovery-qualification-evidence", "recovery/evidence.json", evidence)
        artifacts.json("recovery-qualification", "recovery/qualification.json", report)
    return abandoned


def qualify_recovery(contract: c.RunContract, settings: RunSettings, services: RunServices,
                     evidence_supplier: Callable[[dict[str, Any]], dict[str, Any]], *,
                     execute: bool = False, clock=None) -> dict[str, Any]:
    """Release one exact dirty attempt's lease from fresh physical-clean evidence.

    Recovery qualification is deliberately not sample qualification: the failed
    attempt stays failed, its telemetry is never re-evaluated and no sample
    eligibility is granted. The supplier must read components/checksums now;
    the lease turns clean only when this session's journal reconciliation, a
    fresh empty environment read and every supplied fact pass, and the resulting
    report is persisted and byte-bound in the attempt's journal. An artifact
    write interrupted before this session keeps the attempt failed; its pending
    artifact only receives the explicit terminal abandonment that lets this
    evidence itself be persisted (see _persist_recovery_qualification).
    """
    preview = validate_run(contract, settings, services)
    _require(type(execute) is bool, "execute must be explicit boolean")
    _require(callable(evidence_supplier), "recovery evidence supplier must be callable")
    if not execute:
        return {**preview, "status": "recovery_qualification_validated_not_executed"}
    clock = clock if clock is not None else SystemClock()
    _require(settings.mode == "isolated_test" or type(clock) is SystemClock,
             "controlled recovery qualification must use the host SystemClock")
    context = contract.context.to_dict()
    policy = services.quality_rules.to_dict()
    expected_kind = "synthetic" if settings.mode == "isolated_test" else "observed"
    identity = {"contract_sha256": contract.sha256, "run_id": context["run_id"], "attempt_id": context["attempt_id"]}
    lease = EnvironmentLease(settings.environment, Path(settings.lease_root), contract, mode=settings.mode, resume=True)
    journal = None
    try:
        _require("auxiliary" not in lease.state, "auxiliary attempt requires recover_prepared_attempt")
        journal = j.AttemptJournal.open_for_reconcile(contract, settings.output_policy)
        journal.mark_failed("recovery_qualification_only")
        
        # (reconcile CLEAN -> DELETE carrier -> qualify), so the executor must
        # tolerate the own carrier's confirmed intended absence.
        executors = _primitive_executor(journal, services, own_carrier_removal_tolerant=True)
        recovery = journal.reconcile(executors.adapter)
        _require(recovery.status in {"CLEAN_OFFLINE", "CLEAN_LIVE"}, "residual recovery is blocked")
        removals = _owned_carrier_removal_reports(executors)
        environment = _environment_check(contract, settings, services, clock, require_empty=True)
        lease.workers("drained")
        supplied = evidence_supplier({**identity, "phase": "recovery_qualification",
                                      "component_sources": dict(policy["recovery"]["component_sources"]),
                                      "checksum_source": policy["recovery"]["checksum_source"],
                                      "evidence_kind": expected_kind})
        components, checksum = _verify_recovery_evidence(contract, settings, policy, supplied, expected_kind, clock)
        evidence = {**identity, "schema_version": RECOVERY_SCHEMA, "evidence_kind": expected_kind,
                    "attempt_state": journal.state, "recovery": recovery.__dict__, "environment": environment,
                    "components": components, "checksums": checksum,
                    "claim_scope": "fresh_physical_recovery_readback_not_sample_quality"}
        if removals:
            
            # confirmed intended absence; absent flows keep the HEAD shape.
            evidence["owned_carriers_removed_as_intended"] = removals
        rollouts = _coexisting_rollout_reports(executors)
        if rollouts:
            # ln honest reporting: reconciled-clean through the declared aux
            # rollout replacing the coexisting leg's pinned pod.
            evidence["coexisting_pinned_pods_rolled_by_aux_as_intended"] = rollouts
        report = {**identity, "schema_version": "rq4-collect/quality-report-v1", "rules_sha256": services.quality_rules.sha256,
                  "groups": {"RECOVERY": {"status": "PASS", "checks": [{"group": "RECOVERY", "check": name, "status": "PASS",
                                      "reason": "verified_fresh_recovery_evidence"} for name in RECOVERY_CHECKS]}},
                  "system_clean": "CLEAN", "sample_eligibility": "NOT_ASSESSED", "sample_purpose": contract.to_dict()["purpose"],
                  "formal_release_eligible": False, "release_gate": "NOT_EVALUATED", "evidence_kind": expected_kind,
                  "report_semantics": "recovery_qualification_only_not_sample_quality"}
        artifacts = _Artifacts(journal, settings)
        abandoned = _persist_recovery_qualification(journal, artifacts, evidence, report)
        with _journal_section(journal):
            lease.mark_clean(journal, report, scope="recovery")
        return {"schema_version": RUNNER_SCHEMA, "status": "RECOVERY_QUALIFIED" if lease.state["status"] == "clean" else "BLOCKED",
                "recovery": recovery.__dict__, "report": report, "artifacts": list(artifacts.descriptors),
                "abandoned_artifacts": abandoned,
                "experiment_resumed": False, "qualification": "not_assessed", "sample_eligibility": "NOT_ASSESSED",
                "formal_release_eligible": False, "environment_lease_status": lease.state["status"],
                "cleanup_scope": lease.state.get("cleanup_scope")}
    except BaseException as exc:
        # Never release on any failure; leave a durable qualification-failure
        # trace without masking the original error.
        if journal is not None and not journal._closed:
            try:
                journal.mark_failed("recovery_qualification_failed")
            except BaseException:
                pass
        return {"schema_version": RUNNER_SCHEMA, "status": "BLOCKED", "error": _error_summary(exc),
                "error_type": type(exc).__name__, "experiment_resumed": False, "qualification": "not_assessed",
                "sample_eligibility": "NOT_ASSESSED", "formal_release_eligible": False,
                "environment_lease_status": lease.state.get("status", "dirty")}
    finally:
        if journal is not None:
            journal.close()
        lease.close()


# --------------------------------------------------------------------------- #
# CD-2 (combo-driver-design G-D1/G-D5): the COMBINATION DRIVER CONSTRUCTION
# FACE. Pure functions only -- no new execution entry exists here. The products
# are ordinary RunSettings/RunServices for the unchanged validate_run /
# run_attempt faces; the during-fault event loop keeps its single
# window-derived sort, which is order-identical with ComboTimingProfile by the
# same sorting law (DESIGN sections 0 and 2.1 -- the driver's only duty is to
# NOT reorder, enforced by _combo_payload_purity below). Placement O-D6(A)

# module.runner asset, so the 27-asset release-gate list is unchanged and no
# new in-package unregistered execution module appears. runner.py gains no
# import-graph edge: assemble_combo's AssembledRun arrives duck-typed
# (.spec ComboSpec / .binding RunBinding / .contract RunContract /
# .rules QualityRules / .contract_dict) and every structural check below
# speaks the runner's own data vocabulary (mechanism dispatch reuses the
# validate_run vocabulary: gateway mechanism, db_table_lock, else Kubernetes).
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ComboHttpBudgets:
    """Caller-owned HttpPolicy engineering budgets for the combo face (G-D5).

    The per-stream allowlist is derived from the assembled carriers; byte,
    lateness and request budgets stay explicit engineering inputs exactly like
    the single-root driver (workload.HttpPolicy is caller-owned permission,
    never inferred from a profile).
    """
    policy_id: str
    max_lateness_s: float
    max_request_bytes: int
    max_response_header_bytes: int
    max_response_body_bytes: int
    max_planned_requests: int


@dataclass(frozen=True)
class ComboTraceBudgets:
    """Per-leg trace query budgets (engineering inputs, not spec-derivable)."""
    slice_s: float
    lookback_s: float
    limit: int


@dataclass(frozen=True)
class ComboEnvironmentFacts:
    """Environment/execution facts a ComboSpec cannot derive (O-D2 input).

    Everything derivable from the assembled spec+binding (per-stream
    allowlist, per-leg metric/trace queries, per-leg binding discipline,
    timing assertions) is constructed and checked by build_combo_run_services;
    the caller owns the cluster identity facts, backends, budgets and policies
    listed here. The optional fields carry the CD-2 entry obligations: the
    per-fid supplement suppliers, the same-offset budget declarations
    (O-D3 corrected form) and the single-root reference attempt directories
    (O-D4 soft check).
    """
    mode: str
    environment: EnvironmentIdentity
    lease_root: Path
    output_policy: j.OutputPolicy
    deadlines: DeadlinePolicy
    annotation_policy: a.TimingPolicy
    http: ComboHttpBudgets
    max_artifact_bytes: int
    max_total_artifact_bytes: int
    primitive_client: p.CommandClient
    kubectl: str
    command_timeout_s: float
    command_wait_timeout_s: int
    metric_sampling: t.SourceSampling
    trace: ComboTraceBudgets
    log_sources: tuple[t.PodLogSource, ...]
    metric_backend: Any
    trace_backend: Any
    log_backend: Any
    telemetry_limits: t.CollectionLimits
    environment_reader: Callable[[], dict[str, Any]]
    pins: tuple[tuple[str, p.PinnedTarget], ...]
    db_bindings: tuple[tuple[str, d.DbBinding], ...] = ()
    db_client: Any = None
    leg_supplements: Mapping[str, Callable[[dict[str, Any]], dict[str, Any]]] | None = None
    same_offset_budgets: Mapping[frozenset[str], str] | None = None
    reference_attempts: tuple[Path, ...] | None = None
    log_capture: Any = None
    trace_relations: tuple[tuple[str, str], ...] | None = None
    phase_observer: Any = None

    def __post_init__(self) -> None:
        _require(self.mode in {"isolated_test", "controlled_pilot"}, "combo facts: explicit execution mode required")
        _require(isinstance(self.environment, EnvironmentIdentity) and isinstance(self.output_policy, j.OutputPolicy)
                 and isinstance(self.deadlines, DeadlinePolicy) and isinstance(self.annotation_policy, a.TimingPolicy)
                 and isinstance(self.http, ComboHttpBudgets) and isinstance(self.trace, ComboTraceBudgets)
                 and isinstance(self.metric_sampling, t.SourceSampling)
                 and isinstance(self.telemetry_limits, t.CollectionLimits),
                 "combo facts: validated policy/budget objects required")
        _require(type(self.log_sources) is tuple and bool(self.log_sources)
                 and all(isinstance(source, t.PodLogSource) for source in self.log_sources),
                 "combo facts: explicit pod log sources required")
        for limit in (self.max_artifact_bytes, self.max_total_artifact_bytes):
            _require(type(limit) is int and limit > 0, "combo facts: artifact budgets required")
        _require(type(self.pins) is tuple and all(type(pair) is tuple and len(pair) == 2
                 and type(pair[0]) is str and isinstance(pair[1], p.PinnedTarget) for pair in self.pins)
                 and len(dict(self.pins)) == len(self.pins),
                 "combo facts: pinned targets must be unique (fid, PinnedTarget) pairs")
        _require(type(self.db_bindings) is tuple and all(type(pair) is tuple and len(pair) == 2
                 and type(pair[0]) is str and isinstance(pair[1], d.DbBinding) for pair in self.db_bindings)
                 and len(dict(self.db_bindings)) == len(self.db_bindings),
                 "combo facts: db bindings must be unique (fid, DbBinding) pairs")
        _require(not (set(dict(self.pins)) & set(dict(self.db_bindings))),
                 "combo facts: one instance must not carry both a pin and a db binding")
        _require(self.leg_supplements is None or (callable(getattr(self.leg_supplements, "items", None))
                 and all(type(fid) is str and callable(supplier)
                         for fid, supplier in self.leg_supplements.items())),
                 "combo facts: leg supplements must map fault ids to callables")
        _require(self.same_offset_budgets is None or all(
                 type(key) is frozenset and bool(key) and all(type(fid) is str for fid in key)
                 and type(note) is str and bool(note.strip())
                 for key, note in self.same_offset_budgets.items()),
                 "combo facts: same-offset budgets must map frozenset(fid pairs) to non-empty declarations")
        _require(self.reference_attempts is None or (type(self.reference_attempts) is tuple
                 and all(isinstance(path, Path) and path.is_absolute() for path in self.reference_attempts)),
                 "combo facts: reference attempts must be absolute attempt directories")


@dataclass(frozen=True)
class ComboDriverFace:
    """Products of the combo construction face (never an execution entry).

    settings/services feed the unchanged validate_run / run_attempt faces;
    timing_audit carries the O-D3/O-D7/must-inject-first/payload-purity
    results, reference_check the O-D4 soft result (it never refuses), and
    warnings the non-refusing notices (missing references, un-supplied legs).
    """
    settings: RunSettings
    services: RunServices
    timing_audit: dict[str, Any]
    reference_check: dict[str, Any]
    warnings: tuple[str, ...]


def _combo_payload_purity(spec, contract, fids: tuple[str, ...]) -> dict[str, Any]:
    """DESIGN 2.1/3 driver duties: no reorder, no re-parameterization.

    The contract faults must stay in leg order with verbatim identity fields,
    parameters and windows. Re-parameterizing a leg at the driver face would
    mint a new params_id and silently break the pooled-reference identity
    chain (the binding fact is the contract's parameter bytes; the driver
    layer must never re-parameterize).
    """
    faults = contract.to_dict()["faults"]
    legs = tuple(spec.legs)
    _require(type(faults) is list and len(faults) == len(legs),
             "combo driver: contract faults must match the spec legs")
    rows = []
    for fid, leg, fault in zip(fids, legs, faults):
        _require(type(fault) is dict and fault.get("fault_instance_id") == fid,
                 "combo driver: contract faults must keep the spec leg order (no reorder)")
        _require(fault.get("fault_type") == leg.fault_type and fault.get("mechanism") == leg.mechanism
                 and fault.get("mechanism_version") == leg.mechanism_version
                 and fault.get("normalized_root_entity") == leg.entity,
                 "combo driver: leg identity fields must be verbatim (" + fid + ")")
        _require(fault.get("parameters") == dict(leg.parameters),
                 "combo driver: leg parameters must be inherited verbatim (" + fid
                 + "); re-parameterization breaks the params_id reference chain")
        window = fault.get("planned_window")
        _require(type(window) is dict and window.get("start") == leg.window[0]
                 and window.get("end") == leg.window[1],
                 "combo driver: leg planned windows must be the spec values verbatim (" + fid + ")")
        rows.append({"fid": fid, "entity": leg.entity, "window": (leg.window[0], leg.window[1])})
    return {"leg_order_and_parameters_verbatim": rows}


def _combo_timing_audit(spec, deadlines: DeadlinePolicy, same_offset_budgets, fids: tuple[str, ...]) -> dict[str, Any]:
    """Record budget pressure and validate the declared action structure.

    - O-D7 as adjudicated (ROOT-ADJUDICATIONS): inject-inject same-offset ties
      are REFUSED; recover-recover same-end pairs are legal (D11 precedent)
      and instead owe an explicit budget declaration.
    - A configured duration-plus-lateness reservation reaching the next action
      is a warning, not proof that the actual fault schedule will fail. Runtime
      records actual times and enforces injection/phase/recovery boundaries.
    - DESIGN 2.1: must_inject_first_entity is re-asserted with the assembly
      guard's own formula -- defensive redundancy against a future edit that
      consistently flips windows, orders and fixtures together.
    """
    legs = tuple(spec.legs)
    actions = []
    scheduled_events = []
    fault_specs = {}
    for fid, leg in zip(fids, legs):
        actions.append((leg.window[0], "inject", fid))
        actions.append((leg.window[1], "recover", fid))
    for order, (fid, leg) in enumerate(zip(fids, legs)):
        scheduled_events.extend([(leg.window[0], 1, order, "inject", fid),
                                 (leg.window[1], 0, order, "recover", fid)])
        fault_specs[fid] = {"fault_type": leg.fault_type, "mechanism": leg.mechanism}
    scheduled_events.sort()
    injects = sorted((offset, fid) for offset, action, fid in actions if action == "inject")
    for index in range(len(injects) - 1):
        if injects[index][0] == injects[index + 1][0]:
            raise RunnerError("combo driver: inject-inject same-offset tie at offset "
                              + str(injects[index][0]) + " (" + injects[index][1] + "/"
                              + injects[index + 1][1] + ") -- O-D7 narrowed refusal; a tie "
                              "must be an explicit design, never inherited semantics")
    declared_first = getattr(spec, "must_inject_first_entity", None)
    if declared_first is not None:
        first_leg = legs[int(spec.timing.inject_order[0][1:]) - 1]
        _require(first_leg.entity == declared_first,
                 "combo driver: must_inject_first_entity disagrees with the earliest-start leg (DESIGN 2.1 gate)")
    declarations: dict[str, Any] = {}
    for index in range(len(actions)):
        for other in range(index + 1, len(actions)):
            first, second = actions[index], actions[other]
            if first[0] != second[0] or first[2] == second[2]:
                continue
            note = (same_offset_budgets or {}).get(frozenset((first[2], second[2])))
            _require(type(note) is str and bool(note.strip()),
                     "combo driver: same-offset action pair " + first[2] + "/" + second[2]
                     + " at offset " + str(first[0]) + " requires an explicit same-offset budget "
                     "declaration (O-D3 corrected form; D11 recover-recover precedent)")
            key = first[2] + "/" + second[2]
            _require(key not in declarations, "combo driver: duplicate same-offset declaration pair")
            declarations[key] = {"offset_s": first[0], "actions": [first[1], second[1]], "declaration": note}
    ordered = sorted(actions)
    positive_gaps = [ordered[index + 1][0] - ordered[index][0] for index in range(len(ordered) - 1)]
    positive_gaps = [gap for gap in positive_gaps if gap > 0]
    _require(bool(positive_gaps), "combo driver: a combination without any positive action gap is not drivable")
    min_gap = min(positive_gaps)
    first_injection_exception = _first_injection_start_exception(scheduled_events, fault_specs, deadlines)
    audit = {"min_positive_action_gap_s": min_gap,
             "inject_offsets": {fid: leg.window[0] for fid, leg in zip(fids, legs)},
             "recover_offsets": {fid: leg.window[1] for fid, leg in zip(fids, legs)},
             "same_offset_declarations": declarations,
             "must_inject_first_entity": declared_first}
    reservation = deadlines.action_duration_s + deadlines.action_start_lateness_s
    _record_timing_budget_warning(audit, "serial_action_reservation_s", reservation, min_gap)
    if reservation == min_gap:
        audit.setdefault("timing_warnings", []).append({"metric": "serial_action_reservation_s",
                                                       "actual_s": reservation, "budget_s": min_gap})
        audit["timing_budget_status"] = "warning"
    if first_injection_exception is not None:
        audit["first_injection_start_exception"] = first_injection_exception
    return audit


def _combo_triple_subpairs(inject_offsets: dict[str, Any], fids: tuple[str, ...]) -> dict[str, Any] | None:
    """The three-leg sub-pair fid selection duty, made explicit (REVIEW 8-C).

    A triple's three leg pairs reduce to the named double-root combos
    (assembly TRIPLE_PAIR_CLOSURE, the O-E5 per-amplifier sub-case
    reduction), but E1's load_combo_run takes EXPLICIT amplified_fid /
    amplifier_fid arguments whose F1/F2 defaults are NOT a mapping onto the
    triple's legs -- every sub-pair consumption must select its fids
    deliberately. The construction face registers the three pairs here with
    their inject-first ordering so the duty is machine-visible per face; the
    role assignment itself stays consumer-side (eval knowledge, never driver
    knowledge). Two-leg faces carry no sub-pair reduction and return None.
    """
    if len(fids) != 3:
        return None
    pairs = []
    for index in range(len(fids)):
        for other in range(index + 1, len(fids)):
            first, second = sorted((fids[index], fids[other]), key=lambda fid: inject_offsets[fid])
            pairs.append({"instance_ids": [first, second], "inject_first": first})
    return {"leg_count": 3, "subpairs": pairs,
            "obligation": "E1 load_combo_run needs an explicit amplified_fid/amplifier_fid selection per "
                          "sub-pair (its F1/F2 defaults are not a mapping; TRIPLE_PAIR_CLOSURE sub-case "
                          "reduction, REVIEW 8-C)"}


def _combo_binding_discipline(spec, binding, contract, facts: ComboEnvironmentFacts, fids: tuple[str, ...]) -> None:
    """Per-leg binding discipline (G-D1): gateway/Kubernetes legs pin their own
    Deployment under the binding context; database legs bind the table lock;
    coverage is exactly the leg set (validate_run re-checks the union -- the
    constructor guarantees the per-leg correctness it can see)."""
    plan = contract.to_dict()
    _require(plan["context"]["kube_context"] == binding.kube_context
             and plan["context"]["namespace"] == binding.namespace,
             "combo driver: binding context/namespace must equal the contract")
    pins, db_pins = dict(facts.pins), dict(facts.db_bindings)
    for fid, leg, fault in zip(fids, tuple(spec.legs), plan["faults"]):
        pin = pins.get(fid)
        row = db_pins.get(fid)
        if fault["mechanism"] == g.MECHANISM:
            _require(pin is not None and row is None,
                     "combo driver: gateway leg " + fid + " requires a pinned target, never a db binding")
        elif fault["fault_type"] == "db_table_lock":
            _require(row is not None and pin is None,
                     "combo driver: database leg " + fid + " requires a db binding, not a Kubernetes pin")
            _require(row.database == leg.database and row.table == leg.table,
                     "combo driver: db binding must match the leg's table identity (" + fid + ")")
        else:
            _require(pin is not None and row is None,
                     "combo driver: Kubernetes leg " + fid + " requires a pinned target, not a db binding")
        if pin is not None:
            _require(pin.context == binding.kube_context and pin.namespace == binding.namespace,
                     "combo driver: pin context/namespace mismatch (" + fid + ")")
            _require(pin.entity == leg.entity and pin.deployment == leg.deployment,
                     "combo driver: the pin must bind the leg's own entity/deployment (" + fid + ")")
    _require(set(pins) | set(db_pins) == set(fids),
             "combo driver: every leg needs exactly its one binding")
    if db_pins:
        _require(facts.db_client is not None, "combo driver: database legs require the validated database client")


def _combo_http_policy(contract_dict: dict[str, Any], budgets: ComboHttpBudgets, mode: str) -> w.HttpPolicy:
    """G-D5: the per-stream allowlist derived from the assembled carriers.

    workload.prepare_workload hard-requires the allowlist to exactly cover the
    physical streams with matching method/origin/endpoint, so the rules are
    derived stream-by-stream from the contract (single construction truth).
    """
    streams = contract_dict["request_profile"]["streams"]
    _require(type(streams) is list and len(streams) >= 2,
             "combo driver: the dual-stream carrier profile is required")
    rules = tuple(w.EndpointRule(stream["stream_id"], stream["method"], stream["entrypoint"], stream["endpoint"])
                  for stream in streams)
    _require(len({rule.stream_id for rule in rules}) == len(rules),
             "combo driver: duplicate carrier stream rule")
    return w.HttpPolicy(budgets.policy_id,
                        "isolated_test" if mode == "isolated_test" else "read_only_business",
                        rules, budgets.max_lateness_s, budgets.max_request_bytes,
                        budgets.max_response_header_bytes, budgets.max_response_body_bytes,
                        budgets.max_planned_requests,
                        read_only_post_allowlist=w.sasrec_inference_post_allowlist(streams))


def _combo_telemetry_inputs(spec, contract, facts: ComboEnvironmentFacts, fids: tuple[str, ...]) -> TelemetryInputs:
    """G-D1/G-D5: per-leg metric queries (one per leg recovery rule -- the
    quality data's required_metric_queries) plus per-leg trace queries on the
    legs' carrier services. Source ids must equal spec.telemetry so both
    validate_run's source/interval checks and the quality required_sources
    hold on the same identifiers."""
    plan = contract.to_dict()
    signals = tuple(spec.signals)
    telemetry_ids = spec.telemetry
    legs = tuple(spec.legs)
    _require(len(signals) == len(legs),
             "combo driver: one signal per leg required")
    queries = tuple(t.MetricQuery(fid, telemetry_ids.metrics, signal.entity, signal.expression(),
                                  signal.unit, tuple(signal.signal(fid)["labels"].items()),
                                  plan["metric_interval_s"], facts.metric_sampling)
                    for fid, signal in zip(fids, signals))
    traces = tuple(t.TraceQuery("trace-" + fid, telemetry_ids.traces, signal.service_name,
                                facts.trace_relations or ((signal.service_name, signal.entity),), facts.trace.slice_s,
                                facts.trace.lookback_s, facts.trace.limit)
                   for fid, signal in zip(fids, signals))
    _require(getattr(facts.metric_backend, "source_id", None) == telemetry_ids.metrics,
             "combo driver: metric backend source id must equal spec.telemetry.metrics")
    _require(getattr(facts.trace_backend, "source_id", None) == telemetry_ids.traces,
             "combo driver: trace backend source id must equal spec.telemetry.traces")
    log_ids = [source.source_id for source in facts.log_sources]
    _require(len(set(log_ids)) == len(log_ids) and telemetry_ids.logs in log_ids,
             "combo driver: unique log source ids including the declared primary required")
    return TelemetryInputs(queries, traces, facts.log_sources, facts.metric_backend,
                           facts.trace_backend, facts.log_backend, facts.telemetry_limits, log_capture=facts.log_capture)


_SUPPLEMENT_FIELDS = ("instance_windows", "components", "checksums", "locks", "coverage", "artifacts")


def build_combo_supplement(fids: tuple[str, ...], leg_suppliers):
    """The per-fid supplement obligation, made explicit (REVIEW 8-B).

    Each leg's supplier may return only the supported supplement fields; an
    instance_windows payload must carry exactly that leg's fault id. The
    merged callable keeps the runner's own field-set contract -- a violation
    fails loud at persist time exactly like a monolithic supplier would.
    Legs without suppliers contribute nothing and keep their honest
    downstream quality state (an isolated_test Q1 instance window FAILs on
    synthetic evidence; nothing is invented). Returns None when no leg has a
    supplier, preserving the historical supplement=None behavior.
    """
    suppliers = {fid: leg_suppliers[fid] for fid in fids
                 if leg_suppliers is not None and fid in leg_suppliers}
    if not suppliers:
        return None

    def supplement(record: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for fid, supplier in suppliers.items():
            part = supplier(record)
            _require(type(part) is dict and set(part) <= set(_SUPPLEMENT_FIELDS),
                     "combo supplement: leg " + fid + " returned unsupported fields")
            windows = part.get("instance_windows")
            if windows is not None:
                _require(type(windows) is dict and set(windows) == {fid},
                         "combo supplement: leg " + fid + " must supply exactly its own instance window")
            components = part.get("components")
            if components is not None:
                _require(type(components) is list, "combo supplement: leg " + fid + " components must be a list")
                merged["components"] = merged.get("components", []) + components
            for field in ("instance_windows", "checksums", "coverage", "artifacts"):
                row = part.get(field)
                if row is None:
                    continue
                _require(type(row) is dict, "combo supplement: leg " + fid + " " + field + " must be an object")
                current = merged.setdefault(field, {})
                _require(type(current) is dict, "combo supplement: " + field + " shape conflict")
                for key, value in row.items():
                    _require(key not in current or current[key] == value,
                             "combo supplement: conflicting " + field + " key " + str(key))
                    current[key] = value
            locks = part.get("locks")
            if locks is not None:
                _require("locks" not in merged or merged["locks"] == locks,
                         "combo supplement: conflicting locks payloads")
                merged["locks"] = locks
        return merged

    return supplement


def combo_reference_check(spec, contract, reference_attempts) -> dict[str, Any]:
    """O-D4 layer 2, the driver SOFT check: expand reference_dependencies and,
    when single-root reference attempts are supplied, verify the params_id
    equality chain (probe_channel.leg_identity formula: mechanism_version ':'
    sha12(canonical(mechanism+parameters))). Never refuses: a missing or
    mismatched reference keeps the combo row in the pairing denominator
    (not_estimable) -- it is never silently dropped, and a data gap must not
    be disguised as a driver failure."""
    plan = contract.to_dict()
    leg_params = {fault["fault_instance_id"]:
                  fault["mechanism_version"] + ":" + c.canonical_sha256(
                      {"mechanism": fault["mechanism"], "parameters": fault["parameters"]})[:12]
                  for fault in plan["faults"]}
    pool: dict[str, list[str]] = {}
    warnings: list[str] = []
    supplied = tuple(reference_attempts or ())
    for path in supplied:
        try:
            doc = json.loads((Path(path) / "contract.json").read_bytes().decode("utf-8"))
            _require(type(doc) is dict, "reference contract must be an object")
            faults = doc.get("faults")
            _require(type(faults) is list and len(faults) == 1,
                     "reference candidate requires exactly one contract fault")
            fault = faults[0]
            scenario = doc.get("scenario")
            scenario_id = scenario.get("scenario_id") if type(scenario) is dict else None
            pool.setdefault(scenario_id, []).append(
                fault.get("mechanism_version") + ":" + c.canonical_sha256(
                    {"mechanism": fault.get("mechanism"), "parameters": fault.get("parameters")})[:12])
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            warnings.append("reference attempt " + str(path) + " unreadable: " + _error_summary(exc))
        except RunnerError as exc:
            warnings.append("reference attempt " + str(path) + " not a parsable single-fault run: " + str(exc))
    rows = []
    for fid, refs in spec.reference_dependencies.items():
        for ref in refs:
            if not supplied:
                status = "attempts_not_supplied"
            elif ref not in pool:
                status = "scenario_not_supplied"
                warnings.append("reference " + ref + " for leg " + fid
                                + " not found in the supplied attempts; the pairing row stays in the denominator (not_estimable)")
            elif leg_params[fid] in pool[ref]:
                status = "params_match"
            else:
                status = "params_mismatch"
                warnings.append("reference " + ref + " for leg " + fid + " carries a different params_id ("
                                + leg_params[fid] + " expected); the pairing row stays in the denominator (not_estimable)")
            rows.append({"fid": fid, "reference_scenario": ref,
                         "leg_params_id": leg_params[fid], "status": status})
    if not supplied:
        warnings.insert(0, "reference attempts not supplied: the combo proceeds and its pairing row stays in the "
                           "denominator (not_estimable) per O-D4 -- never a refusal")
    return {"rows": rows, "pool_sizes": {key: len(value) for key, value in pool.items()}, "warnings": warnings}


def build_combo_run_services(assembled, facts: ComboEnvironmentFacts) -> ComboDriverFace:
    """O-D2 pure constructor (DESIGN 6 CD-2 form build_combo_run_services):
    an assemble_combo product plus environment facts -> RunSettings +
    RunServices for the unchanged execution faces. No state is created and no
    entry is added: call run_attempt(assembled.contract, face.settings,
    face.services) for preview (execute=False) or execution exactly like any
    single-root run. All combo-specific gates live HERE, never inside
    validate_run/run_attempt: payload purity (no reorder / no
    re-parameterization), the O-D3 corrected-form deadline assertion with the
    same-offset declaration register, the O-D7 narrowed inject-tie refusal,
    the DESIGN 2.1 must-inject-first redundancy, per-leg binding discipline,
    the per-stream allowlist, per-leg telemetry queries, the per-fid
    supplement merge and the O-D4 soft reference check."""
    spec = getattr(assembled, "spec", None)
    binding = getattr(assembled, "binding", None)
    contract = getattr(assembled, "contract", None)
    rules = getattr(assembled, "rules", None)
    contract_dict = getattr(assembled, "contract_dict", None)
    _require(spec is not None and binding is not None and isinstance(contract, c.RunContract)
             and isinstance(rules, q.QualityRules) and type(contract_dict) is dict,
             "combo driver: an assemble_combo product (AssembledRun) is required")
    _require(isinstance(facts, ComboEnvironmentFacts), "combo driver: explicit ComboEnvironmentFacts required")
    legs = tuple(getattr(spec, "legs", ()) or ())
    _require(len(legs) >= 2,
             "combo driver: the combination face requires at least two legs (single-root runs keep their existing paths)")
    fids = tuple("F" + str(index + 1) for index in range(len(legs)))
    purity = _combo_payload_purity(spec, contract, fids)
    timing = _combo_timing_audit(spec, facts.deadlines, facts.same_offset_budgets, fids)
    subpairs = _combo_triple_subpairs(timing["inject_offsets"], fids)
    _combo_binding_discipline(spec, binding, contract, facts, fids)
    http_policy = _combo_http_policy(contract_dict, facts.http, facts.mode)
    telemetry = _combo_telemetry_inputs(spec, contract, facts, fids)
    _require(set(rules.to_dict()["data"]["required_sources"]["logs"])
             == {source.source_id for source in facts.log_sources},
             "combo driver: actual log sources must equal the versioned quality source set")
    operation_source = getattr(getattr(spec, "telemetry", None), "operations", None)
    _require(type(operation_source) is str and bool(operation_source),
             "combo driver: spec.telemetry.operations required")
    _require({row.get("operation_source_id") for row in rules.to_dict()["injection"]} == {operation_source},
             "combo driver: the quality rules' operation source must equal spec.telemetry.operations")
    supplement = build_combo_supplement(fids, facts.leg_supplements)
    warnings: list[str] = []
    unsupplied = [fid for fid in fids if facts.leg_supplements is None or fid not in facts.leg_supplements]
    if unsupplied:
        warnings.append("combo supplement: no per-fid supplier for " + ",".join(unsupplied)
                        + "; those legs keep their honest downstream quality state (an isolated_test Q1 instance "
                        "window FAILs on synthetic evidence rather than degrading to an invented row)")
    reference = combo_reference_check(spec, contract, facts.reference_attempts)
    warnings.extend(reference.pop("warnings"))
    settings = RunSettings(facts.environment, facts.lease_root, facts.output_policy, facts.deadlines,
                           http_policy, facts.annotation_policy, facts.mode,
                           facts.max_artifact_bytes, facts.max_total_artifact_bytes)
    services = RunServices(facts.primitive_client, facts.pins, facts.kubectl, facts.command_timeout_s,
                           facts.command_wait_timeout_s, telemetry, facts.environment_reader, rules,
                           operation_source, supplement, facts.db_bindings, facts.db_client,
                           phase_observer=facts.phase_observer)
    return ComboDriverFace(settings=settings, services=services,
                           timing_audit={**timing, "triple_subpairs": subpairs,
                                         "payload_purity": purity},
                           reference_check=reference, warnings=tuple(warnings))
