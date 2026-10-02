"""Bounded HTTP/1.1 fixed-arrival workload prototype, separate from telemetry.

prepare_workload is pure and run_workload defaults to validation only. Explicit
execution uses direct literal-IP HTTP, no proxies, redirects, retries or hidden
probes. The caller supplies an independent allowlist. Only drain is supported.
This is not a collector, release gate, persistence layer or server-side drain
certificate. Responses (including injected HTTP failures) become ledger rows.
"""
from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from fractions import Fraction
import hashlib
import http.client
import io
import ipaddress
import json
import math
from queue import Empty, SimpleQueue
import re
import socket
import threading
import time
from typing import Mapping
from urllib.parse import urlencode, urlsplit

from . import contract as c

SCHEMA_VERSION = "rq4-collect/http-workload-v1"
_PHASES = {"pre_fault", "during_fault", "post_recovery"}
_TERMINAL = {"succeeded", "http_error", "timed_out", "transport_error", "response_limit",
             "driver_error", "dropped", "cancelled_before_offer", "cancelled_before_send"}
_TOKEN = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+")
_RESERVED_HEADERS = {"host", "connection", "content-length", "transfer-encoding", "expect",
                     "proxy-authorization", "proxy-connection", "upgrade", "te", "trailer"}

# A business POST is not generally read-only. This one versioned permission is
# limited to the existing SASRec inference endpoint, fixed payload, and the
# explicit S27/projection stream identities below.
SASREC_INFERENCE_PROFILE_VERSION = "sasrec-inference-post-v1"
SASREC_INFERENCE_ENDPOINT = "/recommend"
SASREC_INFERENCE_ITEM_SEQUENCE = ("015600206X", "6300215695", "0446673145")
_SASREC_INFERENCE_PROXY_ENDPOINT = re.compile(
    r"^/api/v1/namespaces/[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?/services/sasrec:8200/proxy/recommend$")
SASREC_INFERENCE_STREAM_CARRIERS = {
    "sasrec-inference-post-v1": "panel-sasrec-inference-post-v1",
    "d09-main-sasrec-inference-post-v1": "combo-main-panel-sasrec-inference-post-v1",
    "d27-bypass-sasrec-inference-post-v1": "combo-bypass-panel-sasrec-inference-post-v1",
    "d28-main-sasrec-inference-post-v1": "combo-main-panel-sasrec-inference-post-v1",
    "d30-bypass-sasrec-inference-post-v1": "combo-bypass-panel-sasrec-inference-post-v1",
    "t06-third-sasrec-inference-post-v1": "combo-third-panel-sasrec-inference-post-v1",
    "t07-third-sasrec-inference-post-v1": "combo-third-panel-sasrec-inference-post-v1",
}


def sasrec_inference_parameters() -> dict[str, object]:
    """The fixed, bounded JSON request used by the versioned S27 carrier."""
    return {"query": {}, "headers": {},
            "json": {"item_sequence": list(SASREC_INFERENCE_ITEM_SEQUENCE),
                     "top_k": 5, "exclude_history": True}}


def _sasrec_inference_body_sha256() -> str:
    body = c.canonical_json(sasrec_inference_parameters()["json"]).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


SASREC_INFERENCE_BODY_SHA256 = _sasrec_inference_body_sha256()


def _is_sasrec_inference_proxy_endpoint(endpoint: object) -> bool:
    return type(endpoint) is str and _SASREC_INFERENCE_PROXY_ENDPOINT.fullmatch(endpoint) is not None


class WorkloadError(ValueError):
    """Invalid plan/policy; errors intentionally omit supplied content."""


class _ResponseLimit(Exception):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise WorkloadError(message)


def _text(value: object) -> None:
    _require(type(value) is str and bool(value.strip()) and value == value.strip(), "trimmed nonempty text required")


def _positive_int(value: object) -> None:
    _require(type(value) is int and value > 0, "positive integer required")


def _number(value: object) -> None:
    _require(type(value) in (int, float) and math.isfinite(value) and value >= 0, "finite nonnegative number required")


def _origin(value: str) -> tuple[str, int, str]:
    _text(value)
    try:
        url = urlsplit(value)
        _require(url.scheme == "http" and url.path in ("", "/") and not url.query and not url.fragment
                 and url.username is None and url.password is None, "literal-IP HTTP origin required")
        address = ipaddress.ip_address(url.hostname)
        port = 80 if url.port is None else url.port
        _require(0 < port < 65536, "invalid origin port")
    except (ValueError, TypeError):
        raise WorkloadError("invalid literal-IP HTTP origin") from None
    host = str(address)
    authority = ("[" + host + "]" if address.version == 6 else host) + ":" + str(port)
    return host, port, "http://" + authority


@dataclass(frozen=True)
class EndpointRule:
    stream_id: str
    method: str
    origin: str
    endpoint: str

    def __post_init__(self) -> None:
        for value in (self.stream_id, self.method, self.endpoint):
            _text(value)
        _origin(self.origin)


@dataclass(frozen=True)
class ReadOnlyPostRule:
    """Exact permission for the versioned SASRec inference POST only."""
    stream_id: str
    method: str
    origin: str
    endpoint: str
    profile_version: str
    payload_sha256: str

    def __post_init__(self) -> None:
        _text(self.stream_id)
        _text(self.method)
        _text(self.endpoint)
        _text(self.profile_version)
        _text(self.payload_sha256)
        _origin(self.origin)
        _require(self.method == "POST" and _is_sasrec_inference_proxy_endpoint(self.endpoint),
                 "read-only POST permission must pin the exact SASRec Service-proxy /recommend route")
        _require(self.profile_version == SASREC_INFERENCE_PROFILE_VERSION,
                 "unsupported read-only POST profile version")
        _require(SASREC_INFERENCE_STREAM_CARRIERS.get(self.stream_id) is not None,
                 "read-only POST stream is not in the versioned SASRec allowlist")
        _require(self.payload_sha256 == SASREC_INFERENCE_BODY_SHA256,
                 "read-only POST payload fingerprint is not the fixed inference request")


@dataclass(frozen=True)
class HttpPolicy:
    """Caller-owned permissions, not automatically inferred from a profile.

    Lateness and resource bounds are explicit engineering inputs, not frozen
    scientific workload/QC thresholds. max_lateness_s must be less than every
    stream interval: old arrivals cannot be replayed in an unbounded catch-up.
    """
    policy_id: str
    traffic_purpose: str
    allowlist: tuple[EndpointRule, ...]
    max_lateness_s: float
    max_request_bytes: int
    max_response_header_bytes: int
    max_response_body_bytes: int
    max_planned_requests: int
    read_only_post_allowlist: tuple[ReadOnlyPostRule, ...] = ()

    def __post_init__(self) -> None:
        _text(self.policy_id)
        _require(self.traffic_purpose in {"isolated_test", "read_only_business"}, "unsupported traffic purpose")
        _require(type(self.allowlist) is tuple and bool(self.allowlist)
                 and all(isinstance(r, EndpointRule) for r in self.allowlist), "explicit immutable allowlist required")
        _require(len({r.stream_id for r in self.allowlist}) == len(self.allowlist), "duplicate permission stream")
        _require(type(self.read_only_post_allowlist) is tuple
                 and all(isinstance(r, ReadOnlyPostRule) for r in self.read_only_post_allowlist),
                 "read-only POST permissions must be an immutable tuple")
        post_grants = {r.stream_id: r for r in self.read_only_post_allowlist}
        _require(len(post_grants) == len(self.read_only_post_allowlist), "duplicate read-only POST permission")
        _number(self.max_lateness_s)
        for value in (self.max_request_bytes, self.max_response_header_bytes,
                      self.max_response_body_bytes, self.max_planned_requests):
            _positive_int(value)
        for rule in self.allowlist:
            address = ipaddress.ip_address(_origin(rule.origin)[0])
            if self.traffic_purpose == "isolated_test":
                _require(address.is_loopback, "isolated test policy requires loopback")
            else:
                if rule.method in {"GET", "HEAD", "OPTIONS"}:
                    _require(rule.stream_id not in post_grants,
                             "read-only POST permission cannot be attached to a read-method stream")
                else:
                    grant = post_grants.get(rule.stream_id)
                    _require(rule.method == "POST" and grant is not None,
                             "business policy permits read methods or an explicit versioned inference POST")
                    _require((_origin(rule.origin)[2], rule.endpoint)
                             == (_origin(grant.origin)[2], grant.endpoint),
                             "read-only POST permission does not match the exact endpoint rule")
        if self.traffic_purpose == "read_only_business":
            post_streams = {r.stream_id for r in self.allowlist if r.method == "POST"}
            _require(set(post_grants) == post_streams,
                     "read-only POST permissions must exactly cover business POST streams")
        else:
            for stream_id, grant in post_grants.items():
                rule = next((r for r in self.allowlist if r.stream_id == stream_id), None)
                _require(rule is not None and rule.method == "POST"
                         and (_origin(rule.origin)[2], rule.endpoint) == (_origin(grant.origin)[2], grant.endpoint),
                         "read-only POST permission does not match an endpoint rule")


def sasrec_inference_post_allowlist(streams: list[dict]) -> tuple[ReadOnlyPostRule, ...]:
    """Build exact caller-owned grants only for the frozen S27 carrier shape."""
    _require(type(streams) is list, "request streams must be a list")
    grants = []
    known_labels = set(SASREC_INFERENCE_STREAM_CARRIERS.values())
    for stream in streams:
        _require(type(stream) is dict, "request stream must be an object")
        stream_id = stream.get("stream_id")
        carrier = stream.get("carrier")
        expected_carrier = SASREC_INFERENCE_STREAM_CARRIERS.get(stream_id)
        if expected_carrier is None:
            _require(carrier not in known_labels and stream.get("method") != "POST",
                     "unrecognized or unversioned business POST carrier")
            continue
        _require(carrier == expected_carrier
                 and stream.get("method") == "POST"
                 and _is_sasrec_inference_proxy_endpoint(stream.get("endpoint"))
                 and stream.get("parameters") == sasrec_inference_parameters()
                 and stream.get("direct_api") is True
                 and stream.get("rate_rps") == 1.0
                 and stream.get("max_concurrency") == 1
                 and stream.get("client_retry_limit") == 0,
                 "SASRec inference stream differs from its versioned fixed request profile")
        origin = stream.get("entrypoint")
        _origin(origin)
        grants.append(ReadOnlyPostRule(stream_id, "POST", origin, stream["endpoint"],
                                       SASREC_INFERENCE_PROFILE_VERSION, SASREC_INFERENCE_BODY_SHA256))
    return tuple(grants)


@dataclass(frozen=True, repr=False)
class _Stream:
    stream_id: str
    host: str
    port: int
    origin: str
    endpoint: str
    target: str
    method: str
    packet: bytes
    fingerprint: str
    rate: Fraction
    cap: int
    timeout_s: float


@dataclass(frozen=True)
class _Arrival:
    stream_id: str
    ordinal: int
    phase_offset_s: float


@dataclass(frozen=True, init=False, repr=False)
class WorkloadPlan:
    _contract: c.RunContract
    _phase: str
    _policy: HttpPolicy
    _streams: tuple[_Stream, ...]
    _arrivals: tuple[_Arrival, ...]

    def __init__(self) -> None:
        raise TypeError("Use prepare_workload")

    def to_dict(self) -> dict:
        source = self._contract.to_dict()
        policy = self._policy
        policy_record = {"policy_id": policy.policy_id, "traffic_purpose": policy.traffic_purpose,
                         "max_lateness_s": policy.max_lateness_s, "max_request_bytes": policy.max_request_bytes,
                         "max_response_header_bytes": policy.max_response_header_bytes,
                         "max_response_body_bytes": policy.max_response_body_bytes,
                         "max_planned_requests": policy.max_planned_requests,
                         "allowlist": [vars(r).copy() for r in policy.allowlist]}
        if policy.read_only_post_allowlist:
            policy_record["read_only_post_allowlist"] = [vars(r).copy()
                                                         for r in policy.read_only_post_allowlist]
        return {"schema_version": SCHEMA_VERSION, "contract_sha256": self._contract.sha256,
                "scenario": source["scenario"], "run_id": source["context"]["run_id"],
                "attempt_id": source["context"]["attempt_id"], "phase": self._phase,
                "phase_window": source["phases"][self._phase], "profile_id": source["context"]["profile_id"],
                "generator_version": source["request_profile"]["generator_version"],
                "random_seed": source["request_profile"]["random_seed"],
                "global_cap": source["request_profile"]["max_concurrency"], "stop_policy": "drain",
                "policy": policy_record,
                "streams": [{"stream_id": s.stream_id, "method": s.method, "origin": s.origin,
                             "endpoint": s.endpoint, "request_target": s.target,
                             "request_fingerprint": s.fingerprint, "rate_rps": float(s.rate),
                             "max_concurrency": s.cap, "timeout_s": s.timeout_s} for s in self._streams],
                "arrivals": [vars(r).copy() for r in self._arrivals],
                "schedule_kind": "fixed_phase_t0_plus_ordinal_over_rate_no_random_selection"}


def _packet(stream: dict, host: str, port: int, policy: HttpPolicy) -> tuple[str, bytes]:
    params = stream["parameters"]
    _require(set(params) == {"query", "headers", "json"}, "explicit query/headers/json parameter shape required")
    _require(type(params["query"]) is dict and type(params["headers"]) is dict, "query and headers must be objects")
    for key, value in params["query"].items():
        _text(key)
        values = value if type(value) is list else [value]
        _require(bool(values) and all(type(v) in (str, int, float, bool) for v in values), "unsupported query value")
    target = stream["endpoint"]
    parts = urlsplit(target)
    _require(not parts.scheme and not parts.netloc and parts.path.startswith("/") and not parts.fragment,
             "origin-relative endpoint required")
    query = urlencode(params["query"], doseq=True)
    if query:
        target += ("&" if "?" in target else "?") + query
    _require(not any(ord(ch) <= 32 or ord(ch) == 127 for ch in target), "unsafe HTTP request target")
    headers = []
    seen = set()
    for key, value in params["headers"].items():
        _require(type(key) is str and _TOKEN.fullmatch(key) is not None and type(value) is str,
                 "invalid HTTP header")
        low = key.lower()
        _require(low not in _RESERVED_HEADERS and low not in seen and "\r" not in value and "\n" not in value
                 and not any((ord(ch) < 32 and ch != "\t") or ord(ch) == 127 for ch in value), "unsafe or duplicate HTTP header")
        seen.add(low)
        headers.append((key, value))
    body = b"" if params["json"] is None else c.canonical_json(params["json"]).encode("utf-8")
    if params["json"] is not None and "content-type" not in seen:
        headers.append(("Content-Type", "application/json"))
    if policy.traffic_purpose == "read_only_business" and stream["method"] == "POST":
        grants = {grant.stream_id: grant for grant in policy.read_only_post_allowlist}
        grant = grants.get(stream["stream_id"])
        _require(grant is not None and params["query"] == {} and params["headers"] == {},
                 "business POST requires the exact versioned inference route with no query or custom headers")
        _require(target == grant.endpoint and hashlib.sha256(body).hexdigest() == grant.payload_sha256,
                 "business POST route or payload does not match its explicit read-only permission")
    authority = ("[" + host + "]" if ":" in host else host) + ":" + str(port)
    headers += [("Host", authority), ("Connection", "close"), ("Content-Length", str(len(body)))]
    try:
        packet = (stream["method"] + " " + target + " HTTP/1.1\r\n" +
                  "".join(k + ": " + v + "\r\n" for k, v in headers) + "\r\n").encode("latin-1") + body
        target.encode("ascii")
    except UnicodeEncodeError:
        raise WorkloadError("request target must be escaped ASCII; header values must be Latin-1") from None
    _require(len(packet) <= policy.max_request_bytes, "request exceeds explicit byte limit")
    return target, packet


def prepare_workload(contract: c.RunContract, phase: str, policy: HttpPolicy) -> WorkloadPlan:
    """Pure preparation. No sockets, DNS, threads, files or endpoint probes."""
    _require(isinstance(contract, c.RunContract) and isinstance(policy, HttpPolicy), "validated contract and explicit policy required")
    _require(phase in _PHASES, "unknown observation phase")
    source = contract.to_dict()
    profile = source["request_profile"]
    _require(profile["stop_policy"] == "drain", "cancel is not implemented; select drain before execution")
    rules = {r.stream_id: r for r in policy.allowlist}
    _require(set(rules) == {s["stream_id"] for s in profile["streams"]}, "allowlist must exactly cover physical streams")
    phase_window = source["phases"][phase]
    duration = Fraction(str(phase_window["end"])) - Fraction(str(phase_window["start"]))
    streams, arrivals = [], []
    for spec in profile["streams"]:
        _require(spec["client_retry_limit"] == 0, "client retries are not implemented")
        host, port, origin = _origin(spec["entrypoint"])
        rule = rules[spec["stream_id"]]
        _require((rule.method, _origin(rule.origin)[2], rule.endpoint) == (spec["method"], origin, spec["endpoint"]),
                 "request does not match independent endpoint permission")
        rate = Fraction(str(spec["rate_rps"]))
        _require(Fraction(str(policy.max_lateness_s)) < 1 / rate, "lateness bound must be smaller than each arrival interval")
        count = math.ceil(duration * rate)
        _require(len(arrivals) + count <= policy.max_planned_requests, "planned request limit exceeded")
        target, packet = _packet(spec, host, port, policy)
        streams.append(_Stream(spec["stream_id"], host, port, origin, spec["endpoint"], target, spec["method"],
                               packet, hashlib.sha256(packet).hexdigest(), rate, spec["max_concurrency"], spec["timeout_s"]))
        arrivals.extend(_Arrival(spec["stream_id"], k, float(k / rate)) for k in range(count))
    arrivals.sort(key=lambda item: (item.phase_offset_s, item.stream_id, item.ordinal))
    plan = object.__new__(WorkloadPlan)
    for key, value in (("_contract", contract), ("_phase", phase), ("_policy", policy),
                       ("_streams", tuple(streams)), ("_arrivals", tuple(arrivals))):
        object.__setattr__(plan, key, value)
    c.canonical_json(plan.to_dict())
    return plan


class _DeadlineRaw(io.RawIOBase):
    def __init__(self, owner: _DeadlineSocket):
        self.owner = owner

    def readable(self):
        return True

    def readinto(self, buffer):
        owner = self.owner
        remaining = owner.max_wire_bytes - owner.received
        owner.sock.settimeout(owner.remaining())
        count = owner.sock.recv_into(buffer, min(len(buffer), remaining + 1))
        owner.received += count
        if owner.received > owner.max_wire_bytes:
            raise _ResponseLimit()
        return count


class _DeadlineFile:
    def __init__(self, owner: _DeadlineSocket):
        self.owner = owner
        self.reader = io.BufferedReader(_DeadlineRaw(owner))
        self.header_mode = True
        self.header_bytes = 0
        self.header_limit = owner.max_header_bytes

    def readline(self, limit=-1):
        self.owner.remaining()
        if self.header_mode:
            cap = self.header_limit - self.header_bytes + 1
            limit = cap if limit < 0 else min(limit, cap)
        data = self.reader.readline(limit)
        self.owner.remaining()
        if self.header_mode:
            self.header_bytes += len(data)
            if self.header_bytes > self.header_limit:
                raise _ResponseLimit()
        return data

    def read(self, size=-1):
        self.owner.remaining()
        data = self.reader.read(size)
        self.owner.remaining()
        return data

    def close(self):
        self.reader.close()

    def flush(self):
        # HTTPResponse.close calls flush even on a read-only file after an
        # exceptional/incomplete response. There is no buffered output here.
        return None


class _DeadlineSocket:
    def __init__(self, sock: socket.socket, deadline: float, policy: HttpPolicy):
        self.sock, self.deadline = sock, deadline
        self.max_header_bytes = policy.max_response_header_bytes
        # Framing bytes also count. This is a conservative hard wire bound;
        # body bytes separately have their own bound.
        self.max_wire_bytes = policy.max_response_header_bytes + policy.max_response_body_bytes
        self.received = 0
        self.file = None

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError()
        return remaining

    def sendall(self, payload):
        pending = memoryview(payload)
        while pending:
            self.sock.settimeout(self.remaining())
            sent = self.sock.send(pending)
            if sent <= 0:
                raise ConnectionError()
            pending = pending[sent:]

    def makefile(self, mode):
        if mode != "rb" or self.file is not None:
            raise RuntimeError("unsupported HTTP file access")
        self.file = _DeadlineFile(self)
        return self.file

    def close(self):
        try:
            if self.file is not None:
                self.file.close()
        finally:
            self.sock.close()


def _http_once(stream: _Stream, policy: HttpPolicy, admitted: float,
               phase_end: float, stop: threading.Event) -> dict:
    start = time.monotonic()
    row = {"worker_started_at_s": start, "network_started_at_s": None, "http_send_started_at_s": None,
           "terminal_at_s": None, "queue_wait_s": start - admitted, "http_status": None,
           "error_kind": None, "response_body_bytes": 0, "response_wire_bytes": 0,
           "response_body_sha256": None, "connection_closed": True}
    raw, owner, response = None, None, None
    deadline = admitted + stream.timeout_s
    digest = hashlib.sha256()
    try:
        if stop.is_set() or start >= phase_end:
            row["status"] = "cancelled_before_send"
            return row
        if start >= deadline:
            raise TimeoutError()
        raw = socket.socket(socket.AF_INET6 if ":" in stream.host else socket.AF_INET, socket.SOCK_STREAM)
        owner = _DeadlineSocket(raw, deadline, policy)
        raw.settimeout(owner.remaining())
        row["network_started_at_s"] = time.monotonic()
        raw.connect((stream.host, stream.port))
        if stop.is_set() or time.monotonic() >= phase_end:
            row["status"] = "cancelled_before_send"
            return row
        owner.remaining()
        send_start = time.monotonic()
        if stop.is_set() or send_start >= phase_end:
            row["status"] = "cancelled_before_send"
            return row
        row["http_send_started_at_s"] = send_start
        owner.sendall(stream.packet)
        response = http.client.HTTPResponse(owner, method=stream.method)
        response.begin()
        owner.file.header_mode = False
        row["http_status"] = response.status
        while True:
            owner.remaining()
            chunk = response.read(min(4096, policy.max_response_body_bytes - row["response_body_bytes"] + 1))
            if not chunk:
                break
            row["response_body_bytes"] += len(chunk)
            if row["response_body_bytes"] > policy.max_response_body_bytes:
                raise _ResponseLimit()
            digest.update(chunk)
        if response.length is not None and response.length > 0:
            raise http.client.IncompleteRead(b"")
        owner.remaining()
        row["response_body_sha256"] = digest.hexdigest()
        row["status"] = "succeeded" if 200 <= response.status < 300 else "http_error"
    except (TimeoutError, socket.timeout):
        row.update(status="timed_out", error_kind="overall_deadline")
    except _ResponseLimit:
        row.update(status="response_limit", error_kind="response_byte_limit")
    except (OSError, http.client.HTTPException):
        row.update(status="transport_error", error_kind="http_transport")
    except Exception:
        row.update(status="driver_error", error_kind="unexpected_driver_exception")
    finally:
        cleanup_error = False
        try:
            if response is not None:
                response.close()
        except Exception:
            cleanup_error = True
        try:
            if owner is not None:
                row["response_wire_bytes"] = owner.received
                owner.close()
            elif raw is not None:
                raw.close()
        except Exception:
            cleanup_error = True
        finally:
            if raw is not None and raw.fileno() != -1:
                try:
                    raw.close()
                except OSError:
                    cleanup_error = True
            if cleanup_error:
                row.update(status="driver_error", error_kind="connection_cleanup")
            row["connection_closed"] = raw is None or raw.fileno() == -1
            row["terminal_at_s"] = time.monotonic()
    return row


def run_workload(plan: WorkloadPlan, *, execute: bool = False,
                 stop_event: threading.Event | None = None, t0_monotonic: float | None = None) -> dict:
    """Run one observation phase and join every worker before returning.

    t0_monotonic is the *phase epoch*, not the whole-run epoch. Planned run
    offsets are separately phase_window.start + ordinal/rate. No phase offset
    is added to this epoch again. External stop prevents future offers/sends;
    requests already sending/reading drain to their actual result/deadline.
    """
    _require(isinstance(plan, WorkloadPlan) and type(execute) is bool, "prepared plan and boolean execute required")
    description = plan.to_dict()
    if not execute:
        return {"schema_version": SCHEMA_VERSION, "execution_mode": "validation_only", "plan": description,
                "http_requests_started": 0, "qualification": "not_assessed"}
    _require(stop_event is None or isinstance(stop_event, threading.Event), "invalid stop event")
    if t0_monotonic is not None:
        _number(t0_monotonic)
    stop = stop_event if stop_event is not None else threading.Event()
    streams = {s.stream_id: s for s in plan._streams}
    cap = description["global_cap"]
    epoch = time.monotonic() if t0_monotonic is None else t0_monotonic
    phase_start = description["phase_window"]["start"]
    phase_end = epoch + float(Fraction(str(description["phase_window"]["end"])) - Fraction(str(phase_start)))
    _number(phase_end)
    pool = ThreadPoolExecutor(max_workers=cap, thread_name_prefix="m1-http")
    records = [{"request_id": f"{description['run_id']}/{description['attempt_id']}/{plan._phase}/{r.stream_id}/{r.ordinal:08d}",
                "run_id": description["run_id"], "attempt_id": description["attempt_id"], "phase": plan._phase,
                "stream_id": r.stream_id, "ordinal": r.ordinal,
                "planned_run_offset_s": phase_start + r.phase_offset_s,
                "scheduled_at_s": epoch + r.phase_offset_s, "offered_at_s": None, "admitted_at_s": None,
                "worker_started_at_s": None, "network_started_at_s": None, "http_send_started_at_s": None,
                "terminal_at_s": None, "queue_wait_s": None, "schedule_lateness_s": None,
                "status": "planned", "drop_reason": None, "http_status": None, "error_kind": None,
                "response_body_bytes": 0, "response_wire_bytes": 0, "response_body_sha256": None,
                "inflight_at_admission": None, "stream_inflight_at_admission": None,
                "connection_closed": True, "request_fingerprint": streams[r.stream_id].fingerprint}
               for r in plan._arrivals]
    done, active = SimpleQueue(), {}
    inflight = Counter()
    peak, peaks = 0, Counter()

    def collect():
        while True:
            try:
                future = done.get_nowait()
            except Empty:
                break
            index = active.pop(future)
            row = records[index]
            row.update(future.result())
            inflight[row["stream_id"]] -= 1

    stopped = None
    try:
        for index, row in enumerate(records):
            target = row["scheduled_at_s"]
            if stop.wait(max(0, target - time.monotonic())):
                break
            now = time.monotonic()
            if now >= phase_end or stop.is_set():
                break
            collect()
            now = time.monotonic()
            if now >= phase_end or stop.is_set():
                break
            row.update(offered_at_s=now, schedule_lateness_s=now - target)
            stream = streams[row["stream_id"]]
            reason = ("scheduler_late" if now - target > plan._policy.max_lateness_s else
                      "global_cap" if len(active) >= cap else "stream_cap" if inflight[stream.stream_id] >= stream.cap else None)
            if reason:
                row.update(status="dropped", drop_reason=reason, terminal_at_s=now)
                continue
            row.update(admitted_at_s=now, inflight_at_admission=len(active) + 1,
                       stream_inflight_at_admission=inflight[stream.stream_id] + 1)
            future = pool.submit(_http_once, stream, plan._policy, now, phase_end, stop)
            active[future] = index
            inflight[stream.stream_id] += 1
            peak = max(peak, len(active))
            peaks[stream.stream_id] = max(peaks[stream.stream_id], inflight[stream.stream_id])
            future.add_done_callback(done.put)
        stop.wait(max(0, phase_end - time.monotonic()))
        stopped = time.monotonic()
    finally:
        pool.shutdown(wait=True, cancel_futures=False)
    collect()
    for row in records:
        if row["status"] == "planned":
            row.update(status="cancelled_before_offer", terminal_at_s=stopped)
    counts = Counter(row["status"] for row in records)
    offered = sum(row["offered_at_s"] is not None for row in records)
    admitted = sum(row["admitted_at_s"] is not None for row in records)
    _require(set(counts) <= _TERMINAL and not active and not any(inflight.values()), "unfinished workload ledger")
    _require(len(records) == offered + counts["cancelled_before_offer"] and
             offered == admitted + counts["dropped"], "request count conservation failed")
    return {"schema_version": SCHEMA_VERSION, "execution_mode": "direct_http", "qualification": "not_assessed",
            "plan": description, "clock": {"time_basis": "monotonic", "clock_id": plan._contract.context.to_dict()["clock_id"],
                                          "phase_epoch_s": epoch, "phase_end_s": phase_end},
            "stop_at_s": stopped, "stop_reason": "external_stop" if stop.is_set() else "phase_horizon",
            "returned_at_s": time.monotonic(), "requests": records,
            "summary": {"planned": len(records), "offered": offered, "admitted": admitted,
                        "http_send_started": sum(r["http_send_started_at_s"] is not None for r in records),
                        "terminal_counts": {key: counts[key] for key in sorted(_TERMINAL)},
                        "peak_inflight": peak, "peak_stream_inflight": dict(peaks),
                        "workers_joined": True, "open_connections": sum(not r["connection_closed"] for r in records),
                        "driver_healthy": counts["driver_error"] == 0,
                        "server_side_drain": "not_verified"}}


def project_logical_views(result: dict, view_to_stream: Mapping[str, str]) -> dict[str, list[str]]:
    """Pure references to one physical ledger; never sends additional requests."""
    _require(result.get("execution_mode") == "direct_http" and isinstance(view_to_stream, Mapping),
             "actual request ledger and explicit view mapping required")
    streams = {s["stream_id"] for s in result["plan"]["streams"]}
    for view, stream in view_to_stream.items():
        _text(view)
        _require(stream in streams, "logical view refers to unknown physical stream")
    return {view: [r["request_id"] for r in result["requests"] if r["stream_id"] == stream]
            for view, stream in view_to_stream.items()}
