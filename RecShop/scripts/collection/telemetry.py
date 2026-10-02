"""Independent, bounded telemetry readback and normalization; no business probes.

Queries use explicit actual UTC windows; Prometheus owns source sampling. This
module does not implement active config/DB polling, workload scheduling, sample
qualification or persistence. Raw bytes and normalized projections have separate
hashes. Jaeger/log completeness is never inferred from a nonempty response.
"""
from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import http.client
import json
import math
import re
import socket
import time
from typing import Callable
from urllib.parse import urlencode

from . import contract as c
from . import workload as w

SCHEMA_VERSION = "rq4-collect/telemetry-memory-v1"
WORKLOAD_TRANSPORT_SHA256 = "5e7e46321b0313733ee07a7e1275b0add38d89b24b00cf380a28f69e5d90d050"
_PHASES = {"pre_fault", "during_fault", "post_recovery"}
_PHASE_ORDER = ("pre_fault", "during_fault", "post_recovery")
_API_PATHS = {"/api/v1/query_range", "/api/traces"}
# Normalized-record fields excluded from the duplicate-span CONFLICT fingerprint.
# Each excluded field describes the collection view, not the span itself, so two
# occurrences of the same (trace_id, span_id) may legitimately differ there while
# the span's substantive content agrees:
#   process_id: opaque key into the trace's per-RESPONSE Jaeger "processes" map.
#       Jaeger does not guarantee stable key allocation across queries for the

#       swapped between the two pre_fault slice responses while the resolved
#       serviceName and process tags were identical). The substantive process
#       content is carried by "service" and "process_tags", which REMAIN in the
#       fingerprint, so a genuine process disagreement still conflicts.
#   in_window, window_membership: derived from the phase collection window and
#       the span bounds, not from span content. They are constant across the
#       slices of one phase today; excluding them keeps the conflict basis
#       independent of window arithmetic should slicing ever become per-slice
#       windowed.
# Stored per-slice records keep ALL fields (including process_id and the
# window fields); only the conflict comparison basis is span-substantive.
_CONFLICT_FINGERPRINT_EXCLUDED = ("in_window", "process_id", "window_membership")


class TelemetryError(ValueError):
    pass


def _require(ok, message):
    if not ok:
        raise TelemetryError(message)


def _text(value):
    _require(type(value) is str and bool(value.strip()) and value == value.strip(), "nonempty trimmed text required")


def _number(value, positive=False):
    _require(type(value) in (int, float) and math.isfinite(value) and (value > 0 if positive else value >= 0), "invalid finite number")


def _int(value):
    _require(type(value) is int and value > 0, "positive integer required")


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _hash(raw):
    return hashlib.sha256(raw).hexdigest()


def _epoch(window):
    _require(isinstance(window, c.TimeWindow) and window.time_basis == "unix_epoch", "explicit actual unix_epoch window required")


@dataclass(frozen=True)
class PhaseScope:
    contract: c.RunContract
    phase: str
    actual_window: c.TimeWindow

    def __post_init__(self):
        _require(isinstance(self.contract, c.RunContract) and self.phase in _PHASES, "validated contract/phase required")
        _epoch(self.actual_window)
        _require(self.actual_window.clock_id == self.contract.context.to_dict()["clock_id"], "run clock identity mismatch")

    def to_dict(self):
        source = self.contract.to_dict()
        context = source["context"]
        return {"contract_sha256": self.contract.sha256, "run_id": context["run_id"], "attempt_id": context["attempt_id"],
                "phase": self.phase, "kube_context": context["kube_context"], "namespace": context["namespace"],
                "planned_window": source["phases"][self.phase], "actual_window": self.actual_window.to_dict()}


@dataclass(frozen=True)
class CollectionLimits:
    max_queries: int
    max_records: int
    max_raw_bytes: int

    def __post_init__(self):
        for value in (self.max_queries, self.max_records, self.max_raw_bytes):
            _int(value)


@dataclass(frozen=True)
class SourceSampling:
    scrape_interval_s: float | None
    export_interval_s: float | None
    evidence_ref: str | None

    def __post_init__(self):
        for value in (self.scrape_interval_s, self.export_interval_s):
            if value is not None:
                _number(value, positive=True)
        if self.evidence_ref is not None:
            _text(self.evidence_ref)
        _require(self.evidence_ref is not None or (self.scrape_interval_s is None and self.export_interval_s is None),
                 "sampling claims require a configuration evidence reference")


@dataclass(frozen=True)
class MetricQuery:
    query_id: str
    source_id: str
    entity: str
    expression: str
    unit: str
    expected_labels: tuple[tuple[str, str], ...]
    step_s: float
    sampling: SourceSampling

    def __post_init__(self):
        for value in (self.query_id, self.source_id, self.entity, self.expression, self.unit):
            _text(value)
        _number(self.step_s, positive=True)
        _require(isinstance(self.sampling, SourceSampling), "explicit sampling knowledge required")
        _pairs(self.expected_labels)
        _require(re.search(r"\bor\s+vector\s*\(\s*0(?:\.0*)?\s*\)", self.expression, re.I) is None,
                 "legacy zero-filling query is forbidden")


def _pairs(value):
    _require(type(value) is tuple and bool(value) and all(type(p) is tuple and len(p) == 2 for p in value), "immutable label/entity pairs required")
    for key, item in value:
        _text(key)
        _text(item)
    _require(len(dict(value)) == len(value), "duplicate mapping key")


@dataclass(frozen=True)
class TraceQuery:
    query_id: str
    source_id: str
    queried_service: str
    entity_by_service: tuple[tuple[str, str], ...]
    slice_s: float
    lookback_s: float
    limit: int

    def __post_init__(self):
        for value in (self.query_id, self.source_id, self.queried_service):
            _text(value)
        _pairs(self.entity_by_service)
        _require(self.queried_service in dict(self.entity_by_service), "queried service has no entity mapping")
        _number(self.slice_s, positive=True)
        _number(self.lookback_s)
        _int(self.limit)


@dataclass(frozen=True)
class PodLogSource:
    source_id: str
    entity: str
    pod_name: str
    pod_uid: str
    container: str
    previous_container: bool
    old_pod_evidence_refs: tuple[str, ...]

    def __post_init__(self):
        for value in (self.source_id, self.entity, self.pod_name, self.pod_uid, self.container):
            _text(value)
        for value in (self.pod_name, self.container):
            _require(re.fullmatch(r"[a-z0-9][a-z0-9.-]*", value) is not None, "invalid Kubernetes resource name")
        _require(type(self.previous_container) is bool and type(self.old_pod_evidence_refs) is tuple, "explicit log identity required")
        for ref in self.old_pod_evidence_refs:
            _text(ref)


@dataclass(frozen=True)
class FetchReply:
    status: str
    raw: bytes
    provenance: dict

    def __post_init__(self):
        _require(self.status in {"ok", "not_run", "timeout", "response_limit", "transport_error", "http_error", "identity_mismatch"}, "invalid backend reply status")
        _require(type(self.raw) is bytes and type(self.provenance) is dict, "raw bytes and provenance required")
        _json_bytes(self.provenance)


@dataclass(frozen=True)
class HttpReadPolicy:
    source_id: str
    origin: str
    allowed_paths: tuple[str, ...]
    timeout_s: float
    max_response_header_bytes: int
    max_response_body_bytes: int

    def __post_init__(self):
        _text(self.source_id)
        w._origin(self.origin)  # Frozen literal-IP HTTP parser, no DNS or I/O.
        _require(type(self.allowed_paths) is tuple and bool(self.allowed_paths) and set(self.allowed_paths) <= _API_PATHS,
                 "explicit telemetry-only API allowlist required")
        _number(self.timeout_s, positive=True)
        _int(self.max_response_header_bytes)
        _int(self.max_response_body_bytes)


class HttpJsonClient:
    """Thin read-only adapter over pinned W deadline transport, not its scheduler."""
    def __init__(self, policy: HttpReadPolicy, *, execute=False):
        _require(isinstance(policy, HttpReadPolicy) and type(execute) is bool, "explicit HTTP policy/execute flag required")
        self.policy, self.execute, self.source_id = policy, execute, policy.source_id

    def fetch_json(self, path, parameters):
        _require(path in self.policy.allowed_paths and type(parameters) is dict, "API request outside allowlist")
        host, port, origin = w._origin(self.policy.origin)
        target = path + "?" + urlencode(parameters)
        provenance = {"source_id": self.source_id, "origin": origin, "path": path, "parameters": parameters,
                      "method": "GET", "retry_count": 0, "redirects_followed": 0, "transport": "direct_literal_ip_http",
                      "started_monotonic_s": None, "ended_monotonic_s": None, "http_status": None}
        if not self.execute:
            return FetchReply("not_run", b"", provenance)
        start = time.monotonic()
        provenance["started_monotonic_s"] = start
        raw = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM)
        owner = w._DeadlineSocket(raw, start + self.policy.timeout_s, self.policy)
        response, body, status = None, bytearray(), "ok"
        try:
            raw.settimeout(owner.remaining())
            raw.connect((host, port))
            authority = ("[" + host + "]" if ":" in host else host) + ":" + str(port)
            encoding = "Accept-Encoding: gzip\r\n" if getattr(self.policy, "accept_gzip", False) is True else ""
            owner.sendall(("GET " + target + " HTTP/1.1\r\nHost: " + authority + "\r\nConnection: close\r\n" + encoding + "\r\n").encode("ascii"))
            response = http.client.HTTPResponse(owner, method="GET")
            response.begin()
            owner.file.header_mode = False
            provenance["http_status"] = response.status
            if getattr(self.policy, "retain_response_headers", False) is True:
                provenance["response_headers"] = list(response.getheaders())
            while True:
                chunk = response.read(min(4096, self.policy.max_response_body_bytes - len(body) + 1))
                if not chunk:
                    break
                body.extend(chunk)
                if len(body) > self.policy.max_response_body_bytes:
                    raise w._ResponseLimit()
            if response.length is not None and response.length > 0:
                raise http.client.IncompleteRead(b"")
            owner.remaining()
            if not 200 <= response.status < 300:
                status = "http_error"
        except (TimeoutError, socket.timeout):
            status = "timeout"
        except w._ResponseLimit:
            status = "response_limit"
        except (OSError, http.client.HTTPException):
            status = "transport_error"
        finally:
            try:
                if response is not None:
                    response.close()
            finally:
                owner.close()
                provenance["ended_monotonic_s"] = time.monotonic()
                provenance["response_wire_bytes"] = owner.received
        return FetchReply(status, bytes(body[:self.policy.max_response_body_bytes]), provenance)


class _Bundle:
    def __init__(self, modality, scope, limits):
        _require(isinstance(scope, PhaseScope) and isinstance(limits, CollectionLimits), "scope/limits required")
        self.modality, self.scope, self.limits = modality, scope, limits
        self.raw, self.queries, self.records, self.issues = [], [], [], []
        self.raw_size = 0

    def reply(self, reply, query):
        _require(isinstance(reply, FetchReply), "backend must return FetchReply")
        _require(len(self.queries) < self.limits.max_queries, "query budget exceeded")
        available = self.limits.max_raw_bytes - self.raw_size
        retained = reply.raw[:available]
        self.raw_size += len(retained)
        truncated = len(retained) != len(reply.raw)
        ref = "memory:raw:" + str(len(self.raw))
        self.raw.append({"ref": ref, "received_bytes": len(reply.raw), "received_sha256": _hash(reply.raw),
                         "retained_bytes": len(retained), "retained_sha256": _hash(retained),
                         "retained_truncated": truncated, "base64": base64.b64encode(retained).decode("ascii"),
                         "persistence": "not_written"})
        row = {**query, "raw_ref": ref, "status": "response_limit" if truncated else reply.status,
               "backend_provenance": json.loads(_json_bytes(reply.provenance)), "issues": []}
        if row["status"] == "ok" and "source_id" in query and reply.provenance.get("source_id") != query["source_id"]:
            row["status"] = "identity_mismatch"
            row["issues"].append("backend_source_identity_mismatch")
        self.queries.append(row)
        if truncated:
            row["issues"].append("aggregate_raw_budget_exhausted")
        return row

    def add(self, record):
        if len(self.records) >= self.limits.max_records:
            if "record_budget_exhausted" not in self.issues:
                self.issues.append("record_budget_exhausted")
            return False
        self.records.append(record)
        return True

    def finish(self):
        failed = sum(q["status"] != "ok" for q in self.queries)
        any_issue = bool(self.issues or any(q["issues"] for q in self.queries))
        state = "not_run" if all(q["status"] == "not_run" for q in self.queries) else "failed" if failed == len(self.queries) else "partial" if failed or any_issue else "ok" if self.records else "empty"
        normalized = _json_bytes(self.records)
        return {"schema_version": SCHEMA_VERSION, "modality": self.modality, "scope": self.scope.to_dict(),
                "records": self.records, "queries": self.queries, "raw_responses": self.raw,
                "manifest": {"persistence": "not_written", "file_path": None, "file_sha256": None,
                             "projection_sha256": _hash(normalized), "projection_encoding": "canonical_utf8_json",
                             "record_count": len(self.records), "in_window_valid_count": sum(r.get("in_window", True) for r in self.records),
                             "collection_status": state, "completeness": "unknown", "issues": self.issues,
                             "qualification": "not_assessed"}}


def _payload(reply, query):
    if query["status"] != "ok":
        return None
    try:
        value = json.loads(reply.raw.decode("utf-8"), parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        _require(type(value) is dict, "backend payload must be object")
        return value
    except (ValueError, UnicodeError):
        query["status"] = "invalid_payload"
        return None


def _safe_fetch(call, *args):
    """A bounded backend failure must not discard other already-collected data."""
    try:
        reply = call(*args)
        _require(isinstance(reply, FetchReply), "invalid backend reply")
        return reply
    except Exception as exc:
        return FetchReply("transport_error", b"", {"backend_exception_type": type(exc).__name__})


def _warnings(value, query):
    for key in ("warnings", "infos"):
        if value.get(key):
            query["issues"].append("backend_" + key)
            query["backend_" + key] = value[key]


def collect_prom(scope: PhaseScope, queries: tuple[MetricQuery, ...], backend, limits: CollectionLimits):
    """Post-hoc range readback. Evaluation grid is NOT underlying scrape cadence."""
    _require(type(queries) is tuple and bool(queries) and len(queries) <= limits.max_queries, "bounded metric query list required")
    _require(all(isinstance(q, MetricQuery) and q.source_id == backend.source_id for q in queries), "metric backend source mismatch")
    _require(len({q.query_id for q in queries}) == len(queries), "duplicate metric query ID")
    bundle = _Bundle("metrics", scope, limits)
    window = scope.actual_window
    for spec in queries:
        expected = math.ceil((Decimal(str(window.end)) - Decimal(str(window.start))) / Decimal(str(spec.step_s)))
        _require(expected <= limits.max_records, "metric grid exceeds record budget")
    for spec in queries:
        parameters = {"query": spec.expression, "start": window.start, "end": window.end, "step": spec.step_s}
        reply = _safe_fetch(backend.fetch_json, "/api/v1/query_range", parameters)
        sampling_reference = vars(spec.sampling).copy()
        if type(sampling_reference["evidence_ref"]) is str and "{phase}" in sampling_reference["evidence_ref"]:
            sampling_reference["evidence_ref"] = sampling_reference["evidence_ref"].replace("{phase}", scope.phase)
        query = bundle.reply(reply, {"query_id": spec.query_id, "source_id": spec.source_id, "entity": spec.entity,
                                     "parameters": parameters, "query_step_s": spec.step_s,
                                     "signal_labels": dict(spec.expected_labels),
                                     "source_sampling": {**sampling_reference, "verification": "caller_supplied_reference_not_verified"},
                                     "timestamp_kind": "prometheus_evaluation_time"})
        value = _payload(reply, query)
        if value is None:
            continue
        _warnings(value, query)
        if value.get("status") != "success" or type(value.get("data")) is not dict or value["data"].get("resultType") != "matrix":
            query["status"] = "backend_error"
            continue
        series = value["data"].get("result")
        if type(series) is not list:
            query["status"] = "invalid_payload"
            continue
        query["returned_series"] = len(series)
        query["missing_grid_by_series"] = []
        for si, item in enumerate(series):
            labels = item.get("metric") if type(item) is dict else None
            if type(labels) is not dict or not all(type(k) is str and type(v) is str for k, v in labels.items()):
                query["issues"].append("malformed_series_labels")
                continue
            if any(labels.get(k) != v for k, v in spec.expected_labels):
                query["issues"].append("source_labels_mismatch")
                continue
            if any(labels[k] != scope.to_dict()["namespace"] for k in ("namespace", "k8s.namespace.name") if k in labels):
                query["issues"].append("source_namespace_mismatch")
                continue
            samples = item.get("values")
            if type(samples) is not list:
                query["issues"].append("missing_values")
                continue
            timestamps = set()
            for pi, point in enumerate(samples):
                if type(point) is not list or len(point) != 2 or type(point[0]) not in (int, float) or not math.isfinite(point[0]):
                    query["issues"].append("invalid_point")
                    continue
                stamp, raw_value = point
                if not window.start <= stamp < window.end:
                    query["issues"].append("point_outside_half_open_window")
                    continue
                timestamps.add(Decimal(str(stamp)))
                try:
                    if type(raw_value) not in (int, float, str):
                        raise ValueError()
                    number = float(raw_value)
                    state = "finite" if math.isfinite(number) else "non_finite"
                except (ValueError, OverflowError):
                    number, state = None, "invalid_value"
                bundle.add({"query_id": spec.query_id, "source_id": spec.source_id, "entity": spec.entity,
                            "unit": spec.unit, "labels": labels, "series_index": si, "point_index": pi,
                            "timestamp_epoch_s": stamp, "timestamp_kind": "prometheus_evaluation_time",
                            "value": number if state == "finite" else None, "value_state": state,
                            "raw_value": raw_value, "raw_ref": query["raw_ref"]})
            expected = math.ceil((Decimal(str(window.end)) - Decimal(str(window.start))) / Decimal(str(spec.step_s)))
            grid = [Decimal(str(window.start)) + k * Decimal(str(spec.step_s)) for k in range(expected)]
            query["missing_grid_by_series"].append({"series_index": si, "missing_epoch_s": [str(t) for t in grid if t not in timestamps],
                                                     "interpretation": "missing_evaluation_points_not_inferred_scrape_loss"})
    return bundle.finish()


def _microseconds(value):
    result = Decimal(str(value)) * 1000000
    _require(result == result.to_integral_value(), "Jaeger boundaries require explicit microsecond precision")
    return int(result)


def _hex_id(value, lengths):
    return type(value) is str and len(value) in lengths and re.fullmatch(r"[0-9a-fA-F]+", value) is not None and int(value, 16) > 0


def collect_jaeger(scope: PhaseScope, queries: tuple[TraceQuery, ...], backend, limits: CollectionLimits):
    _require(type(queries) is tuple and bool(queries) and all(isinstance(q, TraceQuery) and q.source_id == backend.source_id for q in queries), "trace backend source mismatch")
    _require(len({q.query_id for q in queries}) == len(queries), "duplicate trace query ID")
    bundle, tasks, seen = _Bundle("traces", scope, limits), [], {}
    start, end = _microseconds(scope.actual_window.start), _microseconds(scope.actual_window.end)
    for spec in queries:
        width, lookback = _microseconds(spec.slice_s), _microseconds(spec.lookback_s)
        _require(width > 0, "trace slice must be at least one microsecond")
        count = math.ceil((end - start) / width)
        _require(len(tasks) + count <= limits.max_queries, "trace slice query budget exceeded")
        for k in range(count):
            left, right = start + k * width, min(end, start + (k + 1) * width)
            tasks.append((spec, left, right, max(0, left - lookback)))
    service_entities = {}
    for spec in queries:
        for service, entity in spec.entity_by_service:
            _require(service not in service_entities or service_entities[service] == entity, "conflicting service/entity mapping")
            service_entities[service] = entity
    for spec, left, right, search_start in tasks:
        parameters = {"service": spec.queried_service, "start": search_start, "end": right, "limit": spec.limit}
        reply = _safe_fetch(backend.fetch_json, "/api/traces", parameters)
        query = bundle.reply(reply, {"query_id": spec.query_id, "source_id": spec.source_id,
                                     "slice_us": [left, right], "parameters": parameters,
                                     "lookback_s": spec.lookback_s, "pagination": "not_supported",
                                     "boundary_coverage": "unknown_trace_start_search_may_miss_earlier_traces",
                                     "ingestion_coverage": "unknown_requires_later_readback_or_backend_evidence"})
        value = _payload(reply, query)
        if value is None:
            continue
        _warnings(value, query)
        traces = value.get("data")
        if type(traces) is not list or value.get("errors"):
            query["status"] = "backend_error"
            continue
        query["returned_trace_count"] = len(traces)
        query["limit_hit"] = len(traces) >= spec.limit
        if query["limit_hit"]:
            query["issues"].append("truncation_suspected_limit_hit")
        for trace in traces:
            if type(trace) is not dict or not _hex_id(trace.get("traceID"), {16, 32}) or type(trace.get("processes")) is not dict or type(trace.get("spans")) is not list:
                query["issues"].append("malformed_trace")
                continue
            tid = trace["traceID"]
            for span in trace["spans"]:
                if type(span) is not dict or not _hex_id(span.get("spanID"), {16}) or span.get("traceID", tid) != tid or type(span.get("processID")) is not str or type(span.get("startTime")) is not int or type(span.get("duration")) is not int or span["startTime"] < 0 or span["duration"] < 0:
                    query["issues"].append("malformed_span")
                    continue
                process = trace["processes"].get(span.get("processID"))
                service = process.get("serviceName") if type(process) is dict else None
                if type(service) is not str or service not in service_entities:
                    query["issues"].append("unmapped_process_service")
                    continue
                tags = process.get("tags", [])
                references = span.get("references", [])
                if type(tags) is not list or type(span.get("tags", [])) is not list or type(references) is not list or type(span.get("operationName")) is not str or not span["operationName"].strip() or any(type(ref) is not dict or not _hex_id(ref.get("spanID"), {16}) or type(ref.get("refType")) is not str or ref["refType"] not in {"CHILD_OF", "FOLLOWS_FROM"} for ref in references):
                    query["issues"].append("malformed_process_or_references")
                    continue
                namespaces = [tag.get("value") for tag in tags if type(tag) is dict and tag.get("key") in {"k8s.namespace.name", "k8s.namespace"}]
                if any(ns != scope.to_dict()["namespace"] for ns in namespaces):
                    query["issues"].append("source_namespace_mismatch")
                    continue
                a, b = span["startTime"], span["startTime"] + span["duration"]
                inside = start <= a < end if a == b else a < end and b > start
                if a in {search_start, right}:
                    query["issues"].append("span_on_search_boundary")
                normalized = {"source_id": spec.source_id, "trace_id": tid, "span_id": span["spanID"], "process_id": span.get("processID"),
                              "service": service, "entity": service_entities[service], "start_time_us": a,
                              "duration_us": span["duration"], "end_time_us": b, "operation": span.get("operationName"),
                              "tags": span.get("tags", []), "process_tags": tags, "references": span.get("references", []),
                              "in_window": inside, "window_membership": "outside" if not inside else "crosses_boundary" if a < start or b > end else "inside",
                              "namespace_verification": "observed_match" if namespaces else "not_observed"}
                identity = (tid, span["spanID"])
                # Conflict basis: span-substantive projection only. The same
                # span re-observed through another slice of the same phase must
                # not conflict merely because a collection-view field differs.
                fingerprint = _hash(_json_bytes({key: value for key, value in normalized.items()
                                                  if key not in _CONFLICT_FINGERPRINT_EXCLUDED}))
                provenance = {"raw_ref": query["raw_ref"], "query_id": spec.query_id, "queried_service": spec.queried_service, "slice_us": [left, right]}
                if identity in seen:
                    previous = seen[identity]
                    if previous["projection_sha256"] != fingerprint:
                        query["issues"].append("duplicate_span_conflict")
                    previous["observed_in"].append(provenance)
                else:
                    record = {**normalized, "projection_sha256": fingerprint, "observed_in": [provenance]}
                    if bundle.add(record):
                        seen[identity] = record
    return bundle.finish()


def parse_rfc3339(value: str) -> Decimal:
    """Require an explicit offset; preserve up to nanosecond fraction as Decimal."""
    _require(type(value) is str, "RFC3339 text required")
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)", value)
    _require(match is not None, "explicit RFC3339 timezone required")
    try:
        base = datetime.fromisoformat(match[1] + ("+00:00" if match[3] == "Z" else match[3]))
        return Decimal(int(base.timestamp())) + Decimal("0." + (match[2] or "0"))
    except ValueError:
        raise TelemetryError("invalid RFC3339 timestamp") from None


class KubectlLogClient:
    """Read-only command adapter; explicit bounded runner is required to execute.

    runner(argv, timeout_s=..., max_output_bytes=...) must kill/join its command
    on timeout and return FetchReply. No subprocess default is provided in this
    batch. Unit tests inject a fake runner; actual CLI transport is not certified.
    """
    def __init__(self, executable: str, *, timeout_s: float, max_output_bytes: int, execute=False, runner: Callable | None = None):
        _text(executable)
        _number(timeout_s, positive=True)
        _int(max_output_bytes)
        _require(type(execute) is bool and (not execute or callable(runner)), "execution requires an explicit bounded command runner")
        self.executable, self.timeout_s, self.max_output_bytes, self.execute, self.runner = executable, timeout_s, max_output_bytes, execute, runner

    def fetch_logs(self, scope: PhaseScope, source: PodLogSource):
        ctx = scope.to_dict()
        prefix = [self.executable, "--context", ctx["kube_context"], "--namespace", ctx["namespace"]]
        identity = prefix + ["get", "pod", source.pod_name, "-o", "jsonpath={.metadata.uid}"]
        since = datetime.fromtimestamp(scope.actual_window.start, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        logs = prefix + ["logs", "pod/" + source.pod_name, "--container", source.container, "--timestamps=true", "--since-time=" + since,
                         "--tail=-1", "--limit-bytes=" + str(self.max_output_bytes)]
        if source.previous_container:
            logs.append("--previous=true")
        provenance = {"commands": [identity, logs, identity], "pod_uid": source.pod_uid, "source_id": source.source_id,
                      "uid_verification": "not_run", "output_byte_limit": self.max_output_bytes}
        if not self.execute:
            return FetchReply("not_run", b"", provenance)
        before = self.runner(identity, timeout_s=self.timeout_s, max_output_bytes=self.max_output_bytes)
        provenance["uid_before_status"] = before.status
        provenance["uid_before_sha256"] = _hash(before.raw)
        if before.status != "ok" or before.raw.decode("utf-8", errors="replace").strip() != source.pod_uid:
            provenance["uid_verification"] = "read_failed" if before.status != "ok" else "mismatch"
            return FetchReply(before.status if before.status != "ok" else "identity_mismatch", b"", provenance)
        reply = self.runner(logs, timeout_s=self.timeout_s, max_output_bytes=self.max_output_bytes)
        after = self.runner(identity, timeout_s=self.timeout_s, max_output_bytes=self.max_output_bytes)
        matched = after.status == "ok" and after.raw.decode("utf-8", errors="replace").strip() == source.pod_uid
        provenance["uid_after_status"] = after.status
        provenance["uid_after_sha256"] = _hash(after.raw)
        provenance["uid_verification"] = "before_and_after_match" if matched else "changed_or_unverified"
        provenance["log_reply"] = reply.provenance
        oversized = len(reply.raw) > self.max_output_bytes
        provenance["log_output_truncated"] = oversized
        status = "identity_mismatch" if not matched else "response_limit" if oversized else reply.status
        return FetchReply(status, reply.raw[:self.max_output_bytes], provenance)


def collect_logs(scope: PhaseScope, sources: tuple[PodLogSource, ...], backend, limits: CollectionLimits):
    _require(type(sources) is tuple and bool(sources) and len(sources) <= limits.max_queries
             and all(isinstance(s, PodLogSource) for s in sources), "bounded log source list required")
    _require(len({s.source_id for s in sources}) == len(sources), "duplicate log source ID")
    bundle = _Bundle("logs", scope, limits)
    for source in sources:
        reply = _safe_fetch(backend.fetch_logs, scope, source)
        query = bundle.reply(reply, {"source": vars(source), "actual_window": scope.actual_window.to_dict(),
                                     "window_filter": "timestamp_half_open_client_side", "rotation_coverage": "unknown"})
        if query["status"] != "ok":
            continue
        if reply.provenance.get("pod_uid") != source.pod_uid or reply.provenance.get("source_id") != source.source_id:
            query["status"] = "identity_mismatch"
            continue
        limit = reply.provenance.get("output_byte_limit")
        if type(limit) is int and len(reply.raw) >= limit:
            query["issues"].append("truncation_suspected_byte_limit")
        try:
            lines = reply.raw.decode("utf-8").splitlines()
        except UnicodeError:
            query["status"] = "invalid_payload"
            continue
        query["returned_lines"] = len(lines)
        query["empty_successful_query"] = len(lines) == 0
        for ordinal, line in enumerate(lines):
            stamp, separator, message = line.partition(" ")
            try:
                at = parse_rfc3339(stamp)
                _require(bool(separator), "log line needs timestamp separator")
            except TelemetryError:
                query["issues"].append("invalid_log_timestamp")
                continue
            if not Decimal(str(scope.actual_window.start)) <= at < Decimal(str(scope.actual_window.end)):
                query["issues"].append("line_outside_half_open_window")
                continue
            bundle.add({"source_id": source.source_id, "entity": source.entity, "pod_name": source.pod_name,
                        "pod_uid": source.pod_uid, "container": source.container, "previous_container": source.previous_container,
                        "timestamp_original": stamp, "timestamp_epoch_decimal_s": str(at), "message": message,
                        "line_ordinal": ordinal, "raw_ref": query["raw_ref"]})
    return bundle.finish()


def collect_independent(jobs: dict[str, Callable], *, max_workers: int):
    """Independent readback jobs only; caller owns the explicit window in each job.

    This helper does not start/schedule business requests or active metric source
    sampling. Built-in HTTP clients are bounded; custom jobs must also be bounded.
    """
    _int(max_workers)
    _require(type(jobs) is dict and bool(jobs) and set(jobs) <= {"metrics", "traces", "logs"}
             and all(callable(fn) for fn in jobs.values()), "explicit collector jobs required")
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="m1-telemetry") as pool:
        futures = {name: pool.submit(fn) for name, fn in jobs.items()}
        results = {}
        for name, future in futures.items():
            try:
                results[name] = future.result()
            except Exception as exc:
                results[name] = {"collection_status": "collector_error", "error_type": type(exc).__name__}
        return results


def trace_phase_presence(bundles: dict[str, dict]):
    """Structural nonempty evidence only, not Q3 completeness or sample acceptance."""
    _require(type(bundles) is dict and set(bundles) <= _PHASES, "phase-keyed trace bundles required")
    identities = {(b["scope"]["run_id"], b["scope"]["attempt_id"], b["scope"]["contract_sha256"],
                   b["scope"]["kube_context"], b["scope"]["namespace"], b["scope"]["actual_window"]["clock_id"])
                  for b in bundles.values()}
    _require(len(identities) <= 1, "cannot combine traces from different attempt/contract/source clocks")
    windows = [c.TimeWindow.from_dict(bundles[p]["scope"]["actual_window"]) for p in _PHASE_ORDER if p in bundles]
    for window in windows:
        _epoch(window)
    _require(all(a.end <= b.start for a, b in zip(windows, windows[1:])), "phase windows must be ordered without overlap")
    result = {}
    for phase in sorted(_PHASES):
        bundle = bundles.get(phase)
        if bundle is not None:
            _require(bundle["modality"] == "traces" and bundle["scope"]["phase"] == phase, "trace phase identity mismatch")
            _require(bundle["manifest"]["projection_sha256"] == _hash(_json_bytes(bundle["records"])), "trace projection fingerprint mismatch")
            window = bundle["scope"]["actual_window"]
            start, end = _microseconds(window["start"]), _microseconds(window["end"])
            for record in bundle["records"]:
                _require(_hex_id(record.get("trace_id"), {16, 32}) and _hex_id(record.get("span_id"), {16}), "invalid normalized trace/span identity")
                a, b, duration = record.get("start_time_us"), record.get("end_time_us"), record.get("duration_us")
                _require(type(a) is int and type(b) is int and type(duration) is int and a >= 0 and duration >= 0 and b == a + duration,
                         "invalid normalized span timestamps")
                inside = start <= a < end if a == b else a < end and b > start
                _require(type(record.get("in_window")) is bool and record["in_window"] == inside, "span window membership mismatch")
        count = 0 if bundle is None else sum(bool(r["in_window"]) for r in bundle["records"])
        result[phase] = {"in_window_structurally_valid_span_count": count, "nonempty": count > 0,
                         "projection_sha256": None if bundle is None else bundle["manifest"]["projection_sha256"]}
    return {"phases": result, "three_phases_nonempty": all(r["nonempty"] for r in result.values()),
            "quality_gate": "not_evaluated", "persistence": "not_written"}
