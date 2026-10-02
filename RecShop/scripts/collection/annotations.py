"""Pure, evidence-aware annotation drafts; no collection or qualification gate.

Windows use contract.TimeWindow's half-open convention. Supplied observations
are caller assertions with provenance, not verified telemetry. Planned windows
never substitute for missing operation/observed windows. Empirical path,
interaction and role labels are deliberately not inferred here.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
import math
from typing import Mapping

from . import contract as c

SCHEMA_VERSION = "rq4-collect/annotation-draft-v1"


class AnnotationError(ValueError):
    """Invalid annotation input; messages do not echo supplied values."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AnnotationError(message)


def _text(value: object) -> None:
    _require(type(value) is str and bool(value.strip()) and value == value.strip(),
             "nonempty trimmed text required")
    _require(not any(marker in value.upper() for marker in
                     ("UNRESOLVED", "NOT_FROZEN", "PILOT_TO_FREEZE", "TBD")),
             "unresolved annotation reference")


def _number(value: object) -> None:
    _require(type(value) in (int, float) and math.isfinite(value) and value >= 0,
             "finite nonnegative number required")


def _clock(windows: tuple[c.TimeWindow, ...]) -> None:
    _require(all(isinstance(w, c.TimeWindow) for w in windows), "c.TimeWindow required")
    _require(len({(w.time_basis, w.clock_id) for w in windows}) <= 1,
             "incompatible window clocks; explicit conversion required upstream")


def _window_map(windows: Mapping[str, c.TimeWindow | None]) -> dict[str, c.TimeWindow | None]:
    _require(isinstance(windows, Mapping) and bool(windows), "nonempty instance/window mapping required")
    result = dict(windows)
    for instance_id in result:
        _text(instance_id)
    _clock(tuple(w for w in result.values() if w is not None))
    return result


@dataclass(frozen=True)
class Membership:
    active_instance_ids: tuple[str, ...]
    unknown_instance_ids: tuple[str, ...]
    state: str


def membership_at(timestamp: float, windows: Mapping[str, c.TimeWindow | None], *,
                  time_basis: str, clock_id: str) -> Membership:
    """Return known active instances at [start, end), never drop a missing axis.

    Unknown windows force state=unknown even when another known instance is
    active. 'post_fault'/'between_faults' are temporal positions, not recovery
    certification. Arbitrary instance IDs replace legacy positional F1 labels.
    """
    _number(timestamp)
    _require(time_basis in {"run_offset", "monotonic", "unix_epoch"}, "unsupported time basis")
    _text(clock_id)
    values = _window_map(windows)
    known = tuple(w for w in values.values() if w is not None)
    _require(all((w.time_basis, w.clock_id) == (time_basis, clock_id) for w in known),
             "timestamp/window clock mismatch")
    active = tuple(sorted(i for i, w in values.items() if w is not None and w.start <= timestamp < w.end))
    unknown = tuple(sorted(i for i, w in values.items() if w is None))
    if unknown:
        state = "unknown"
    elif active:
        state = "active"
    elif timestamp < min(w.start for w in known):
        state = "pre_fault"
    elif timestamp >= max(w.end for w in known):
        state = "post_fault"
    else:
        state = "between_faults"
    return Membership(active, unknown, state)


def intersection(*windows: c.TimeWindow | None) -> c.TimeWindow | None:
    """Positive-duration intersection; missing input and empty overlap give None.

    Use overlap_lattice when those two reasons must be distinguished. Known
    clocks are checked even if another input is missing. No rounding or clock
    conversion is performed.
    """
    _require(len(windows) >= 2, "intersection requires at least two windows")
    known = tuple(w for w in windows if w is not None)
    _clock(known)
    if len(known) != len(windows):
        return None
    start, end = max(w.start for w in known), min(w.end for w in known)
    return c.TimeWindow(start, end, known[0].time_basis, known[0].clock_id) if start < end else None


def overlap_lattice(windows: Mapping[str, c.TimeWindow | None]) -> list[dict]:
    """All pairs plus the whole-set intersection for 3+ instances (not powerset)."""
    values = _window_map(windows)
    ids = tuple(sorted(values))
    groups = list(combinations(ids, 2)) + ([ids] if len(ids) > 2 else [])
    result = []
    for group in groups:
        members = tuple(values[i] for i in group)
        overlap = intersection(*members)
        missing = any(w is None for w in members)
        result.append({"instance_ids": list(group),
                       "status": "missing_window" if missing else "overlap" if overlap else "no_overlap",
                       "window": overlap.to_dict() if overlap else None,
                       "duration_s": None if missing else overlap.end - overlap.start if overlap else 0})
    return result


@dataclass(frozen=True)
class TimingPolicy:
    """No default policy. Only the documented priority is currently supported.

    Simultaneous requires positive overlap and both endpoints within tolerance;
    it precedes containment, which precedes partial overlap, then staggered.
    Touching is either explicitly staggered or rejected as undecided. The
    tolerance is never used to invent overlap or fill a real gap.
    """
    rule_version: str
    simultaneous_tolerance_s: float
    touching_policy: str
    precedence: tuple[str, ...]

    def __post_init__(self) -> None:
        _text(self.rule_version)
        _number(self.simultaneous_tolerance_s)
        _require(self.touching_policy in {"staggered", "reject"}, "unsupported touching policy")
        _require(type(self.precedence) is tuple and self.precedence ==
                 ("simultaneous", "nested", "partial_overlap", "staggered"),
                 "unsupported timing precedence; policy must be explicit")

    def to_dict(self) -> dict:
        return {"rule_version": self.rule_version,
                "simultaneous_tolerance_s": self.simultaneous_tolerance_s,
                "touching_policy": self.touching_policy, "precedence": list(self.precedence)}


def pair_timing_relation(left_id: str, left: c.TimeWindow, right_id: str,
                         right: c.TimeWindow, *, policy: TimingPolicy) -> dict:
    """Classify supplied real windows mathematically, not physical interaction."""
    _text(left_id)
    _text(right_id)
    _require(left_id != right_id, "two distinct instance IDs required")
    _require(isinstance(policy, TimingPolicy), "explicit TimingPolicy required")
    _clock((left, right))
    overlap = intersection(left, right)
    touching = left.end == right.start or right.end == left.start
    if touching and policy.touching_policy == "reject":
        raise AnnotationError("touching windows require a resolved policy")
    contains, contained = None, None
    if overlap and abs(left.start - right.start) <= policy.simultaneous_tolerance_s\
            and abs(left.end - right.end) <= policy.simultaneous_tolerance_s:
        label = "simultaneous"
    elif overlap and (left.contains_window(right) or right.contains_window(left)):
        label = "nested"
        contains, contained = ((left_id, right_id) if left.contains_window(right) else (right_id, left_id))
    elif overlap:
        label = "partial_overlap"
    else:
        label = "staggered"
    return {"label": label, "instance_ids": [left_id, right_id],
            "windows": {left_id: left.to_dict(), right_id: right.to_dict()},
            "overlap_window": overlap.to_dict() if overlap else None,
            "overlap_duration_s": overlap.end - overlap.start if overlap else 0,
            "touching": touching, "containing_instance_id": contains,
            "contained_instance_id": contained, "policy": policy.to_dict()}


@dataclass(frozen=True)
class EvidenceReference:
    """Caller-supplied same-attempt evidence pointer, not a verified file."""
    run_id: str
    attempt_id: str
    fault_instance_id: str
    uri: str

    def __post_init__(self) -> None:
        for value in (self.run_id, self.attempt_id, self.fault_instance_id, self.uri):
            _text(value)
        c.canonical_json(self.to_dict())  # Contract guard rejects inline URL credentials.

    def to_dict(self) -> dict:
        return {"run_id": self.run_id, "attempt_id": self.attempt_id,
                "fault_instance_id": self.fault_instance_id, "uri": self.uri}


@dataclass(frozen=True)
class WindowEvidence:
    window: c.TimeWindow
    determination_rule: str
    evidence_refs: tuple[EvidenceReference, ...]

    def __post_init__(self) -> None:
        _clock((self.window,))
        _text(self.determination_rule)
        _require(type(self.evidence_refs) is tuple and bool(self.evidence_refs)
                 and all(isinstance(e, EvidenceReference) for e in self.evidence_refs),
                 "window requires immutable nonempty evidence references")
        _require(len(set(self.evidence_refs)) == len(self.evidence_refs), "duplicate evidence reference")
        c.canonical_json(self.to_dict())

    def to_dict(self) -> dict:
        return {"window": self.window.to_dict(), "determination_rule": self.determination_rule,
                "evidence_refs": [e.to_dict() for e in self.evidence_refs], "verification": "not_assessed"}


@dataclass(frozen=True)
class FaultTiming:
    fault_instance_id: str
    operation_confirmed: WindowEvidence | None
    observed_effective: WindowEvidence | None

    def __post_init__(self) -> None:
        _text(self.fault_instance_id)
        _require(all(value is None or isinstance(value, WindowEvidence) for value in
                     (self.operation_confirmed, self.observed_effective)), "invalid window evidence")
        _require(all(e.fault_instance_id == self.fault_instance_id
                     for item in (self.operation_confirmed, self.observed_effective) if item is not None
                     for e in item.evidence_refs), "evidence belongs to another fault instance")


def build_annotation_draft(contract: c.RunContract, timings: tuple[FaultTiming, ...], *,
                           timing_policy: TimingPolicy) -> dict:
    """Construct a detached, version/attempt-bound draft from a validated plan.

    Missing empirical windows/labels stay explicitly unassessed. This output
    cannot pass final annotation/QC by itself; raw evidence and independent
    annotation review remain necessary. Operation and effective clocks may
    differ by basis, but each kind must have one named clock. Recovery success,
    actual target/readback/parameters and causal roles are later evidence work.
    """
    _require(isinstance(contract, c.RunContract), "validated c.RunContract required")
    _require(isinstance(timing_policy, TimingPolicy), "explicit TimingPolicy required")
    _require(type(timings) is tuple and all(isinstance(t, FaultTiming) for t in timings),
             "immutable FaultTiming sequence required")
    plan, context = contract.to_dict(), contract.context.to_dict()
    ids = {f.fault_instance_id for f in contract.faults}
    by_id = {t.fault_instance_id: t for t in timings}
    _require(len(by_id) == len(timings), "duplicate observed instance ID")
    _require(set(by_id) <= ids, "observed instance is absent from versioned contract")
    for kind in ("operation_confirmed", "observed_effective"):
        supplied = tuple(getattr(t, kind) for t in timings if getattr(t, kind) is not None)
        _clock(tuple(item.window for item in supplied))
        for item in supplied:
            _require(item.window.clock_id == context["clock_id"], "evidence clock differs from run clock")
            _require(all(e.run_id == context["run_id"] and e.attempt_id == context["attempt_id"]
                         for e in item.evidence_refs), "evidence belongs to another run/attempt")
    instances, effective = [], {}
    for spec in contract.faults:
        timing = by_id.get(spec.fault_instance_id)
        operation = timing.operation_confirmed if timing else None
        observed = timing.observed_effective if timing else None
        effective[spec.fault_instance_id] = observed.window if observed else None
        instances.append({"fault_instance_id": spec.fault_instance_id, "planned": spec.to_dict(),
                          "normalized_root_entity": spec.normalized_root_entity,
                          "operation_confirmed": operation.to_dict() if operation else None,
                          "observed_effective": observed.to_dict() if observed else None,
                          "role": {"status": "not_assessed", "label": None}})
    pairs = []
    for a, b in combinations(instances, 2):
        aid, bid = a["fault_instance_id"], b["fault_instance_id"]
        timing = {"status": "not_assessed", "reason": "missing_observed_effective_window", "label": None}
        if effective[aid] is not None and effective[bid] is not None:
            timing = pair_timing_relation(aid, effective[aid], bid, effective[bid], policy=timing_policy)
            timing["status"] = "derived_from_supplied_windows_pending_review"
            timing["evidence_refs"] = [e for member in (a, b) for e in member["observed_effective"]["evidence_refs"]]
        pairs.append({"instance_ids": [aid, bid],
                      "category": {"status": "configuration_derived_pending_review",
                                   "label": "intra_class" if a["planned"]["fault_class"] == b["planned"]["fault_class"] else "cross_class"},
                      "time": timing, "request_path": {"status": "not_assessed", "label": None},
                      "interaction": {"status": "not_assessed", "label": None}})
    result = {"schema_version": SCHEMA_VERSION, "status": "annotation_draft", "qualification": "not_assessed",
              "scenario": plan["scenario"], "contract_sha256": contract.sha256,
              "run_id": context["run_id"], "attempt_id": context["attempt_id"],
              "profile_id": context["profile_id"], "scope": plan["scope"],
              "root_entities": list(contract.root_entities), "G": len(contract.root_entities),
              "entity_truth_status": "planned_entities_pending_injection_validation",
              "fault_instances": instances, "pair_relations": pairs,
              "pair_relations_applicability": "applicable" if pairs else "not_applicable_single_instance",
              "observed_overlap_lattice": overlap_lattice(effective), "timing_policy": timing_policy.to_dict()}
    c.canonical_json(result)  # Validate finite JSON values/credential handling without I/O.
    return result
