"""Collection log driver kubectl implementation. Runtime identity and source checks remain explicit."""
from __future__ import annotations

import calendar
import hashlib
import json
import math
import os
import re
import signal
import subprocess
import threading
import time
from urllib.parse import quote

from . import live_runtime as lr
from . import log_archive as la

DRIVER_SCHEMA = "rq4-collect/log-driver-kubectl-v1"
_READ_SIZE = 65536
_REAP_TIMEOUT_S = 5.
_EVENT_KINDS = {"ADDED", "MODIFIED", "DELETED", "BOOKMARK", "ERROR"}
_RFC3339 = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})")


class DriverError(RuntimeError):
    """Reason codes only; never command output, payloads or secrets."""


def _require(value, reason):
    if not value:
        raise DriverError(reason)
    return value


def _text(value, reason):
    _require(type(value) is str and bool(value) and not any(ch in value for ch in "\r\n\0"), reason)


def _positive(value, reason):
    _require(type(value) in (int, float) and math.isfinite(value) and value > 0, reason)


def _json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _epoch_seconds(value, reason):
    """Parse a Kubernetes RFC3339 timestamp into epoch seconds; None stays None."""
    if value is None:
        return None
    match = _RFC3339.fullmatch(value) if type(value) is str else None
    _require(match is not None, reason)
    year, month, day, hour, minute, second, fraction, zone = match.groups()
    try:
        result = calendar.timegm((int(year), int(month), int(day), int(hour), int(minute), int(second), 0, 0, 0))
    except (ValueError, OverflowError):
        raise DriverError(reason) from None
    result += int(fraction) / 10 ** len(fraction) if fraction else 0.
    if zone != "Z":
        offset = int(zone[1:3]) * 3600 + int(zone[4:6]) * 60
        result += -offset if zone[0] == "+" else offset
    return result


def _rfc3339_micros(epoch_s, reason):
    """Format an epoch as RFC3339 with microseconds (floor), accepted by kubectl --since-time."""
    _require(type(epoch_s) in (int, float) and math.isfinite(epoch_s) and epoch_s >= 0, reason)
    seconds, micro = divmod(int(epoch_s * 1_000_000), 1_000_000)
    moment = time.gmtime(seconds)
    return "%04d-%02d-%02dT%02d:%02d:%02d.%06dZ" % (moment.tm_year, moment.tm_mon, moment.tm_mday,
                                                    moment.tm_hour, moment.tm_min, moment.tm_sec, micro)


def project_pod(pod_object, scope):
    """Whitelist projection of one kubectl Pod JSON object; nothing outside the schema survives.

    Live-measured source shapes (review F1 follow-up): raw-endpoint PodList items carry
    NO kind field, while watch event objects and single-object gets carry kind "Pod".
    An absent kind is therefore accepted; an explicitly non-Pod kind is refused.
    """
    _require(type(pod_object) is dict, "pod_object_invalid")
    _require(pod_object.get("kind") in (None, "Pod"), "pod_object_invalid")
    meta = pod_object.get("metadata")
    _require(type(meta) is dict, "pod_metadata_invalid")
    for key in ("name", "namespace", "uid", "resourceVersion"):
        _require(type(meta.get(key)) is str and bool(meta[key]), "pod_metadata_field_missing")
    labels = meta.get("labels")
    app = labels.get("app") if type(labels) is dict else None
    _require(type(app) is str and bool(app), "pod_app_label_missing")
    owners = []
    for ref in meta.get("ownerReferences") or []:
        _require(type(ref) is dict, "pod_owner_reference_invalid")
        for key in ("kind", "name", "uid"):
            _require(type(ref.get(key)) is str and bool(ref[key]), "pod_owner_field_missing")
        owners.append({"kind": ref["kind"], "name": ref["name"], "uid": ref["uid"],
                       "controller": ref.get("controller") is True})
    status = pod_object.get("status")
    statuses = status.get("containerStatuses") if type(status) is dict else None
    row = next((row for row in statuses or [] if type(row) is dict and row.get("name") == scope.container), None)
    if row is None:
        # Not yet observed by the kubelet: pending containers carry null IDs/start fields.
        container = {"name": scope.container, "container_id": None, "image_id": None,
                     "restart_count": 0, "started_epoch_s": None, "finished_epoch_s": None}
    else:
        state = row.get("state") if type(row.get("state")) is dict else {}
        running = state.get("running") if type(state.get("running")) is dict else {}
        terminated = state.get("terminated") if type(state.get("terminated")) is dict else {}
        restart = row.get("restartCount")
        _require(type(restart) is int and restart >= 0, "pod_container_restart_count_invalid")
        started = running.get("startedAt") if running else terminated.get("startedAt")
        container = {"name": scope.container,
                     "container_id": row.get("containerID") or None,
                     "image_id": row.get("imageID") or None,
                     "restart_count": restart,
                     "started_epoch_s": _epoch_seconds(started, "pod_container_started_invalid"),
                     "finished_epoch_s": _epoch_seconds(terminated.get("finishedAt"), "pod_container_finished_invalid")}
    creation = _epoch_seconds(meta.get("creationTimestamp"), "pod_creation_timestamp_invalid")
    _require(creation is not None, "pod_creation_timestamp_missing")
    return {"namespace": meta["namespace"], "name": meta["name"], "uid": meta["uid"],
            "resource_version": meta["resourceVersion"], "app": app, "creation_epoch_s": creation,
            "deletion_requested_epoch_s": _epoch_seconds(meta.get("deletionTimestamp"), "pod_deletion_timestamp_invalid"),
            "owners": owners, "container": container}


def project_replicaset(rs_object, scope):
    """Whitelist projection of one kubectl ReplicaSet JSON object with its Deployment owner."""
    _require(type(rs_object) is dict and rs_object.get("kind") == "ReplicaSet", "replicaset_object_invalid")
    meta = rs_object.get("metadata")
    _require(type(meta) is dict, "replicaset_metadata_invalid")
    for key in ("name", "namespace", "uid", "resourceVersion"):
        _require(type(meta.get(key)) is str and bool(meta[key]), "replicaset_metadata_field_missing")
    owner = next((ref for ref in meta.get("ownerReferences") or []
                  if type(ref) is dict and ref.get("kind") == "Deployment" and ref.get("controller") is True), None)
    _require(owner is not None and type(owner.get("uid")) is str and bool(owner["uid"]),
             "replicaset_deployment_owner_missing")
    return {"namespace": meta["namespace"], "name": meta["name"], "uid": meta["uid"],
            "resource_version": meta["resourceVersion"], "deployment_uid": owner["uid"], "controller": True}


def _namespace_uid(obj):
    _require(type(obj) is dict and obj.get("kind") == "Namespace", "namespace_object_invalid")
    meta = obj.get("metadata")
    uid = meta.get("uid") if type(meta) is dict else None
    _require(type(uid) is str and bool(uid), "namespace_uid_missing")
    return uid


class _StreamProcess:
    """One owned read-only subprocess tree for a watch/follow stream.

    Mirrors BoundedProcess ownership (Windows job kill-on-close over a suspended
    start, POSIX session kill), no shell and NO_PROXY in the child environment.
    stderr is drained and only counted; its content is never retained.
    """
    def __init__(self, argv):
        _require(type(argv) is tuple and bool(argv)
                 and all(type(v) is str and v and "\0" not in v and "\n" not in v for v in argv), "stream_argv_invalid")
        self.argv = argv
        self.stderr_bytes = 0
        self.job = lr._WindowsJob() if os.name == "nt" else None
        flags = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | 0x00000004} if self.job is not None else {"start_new_session": True}
        environment = os.environ.copy()
        environment.update(NO_PROXY="*", no_proxy="*")
        self.process = None
        try:
            self.process = subprocess.Popen(list(argv), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE, shell=False, env=environment, **flags)
            if self.job is not None:
                self.job.attach_and_resume(self.process)
        except BaseException:
            if self.job is not None:
                self.job.close()
            if self.process is not None:
                self.process.kill()
                try:
                    self.process.wait(timeout=_REAP_TIMEOUT_S)
                except subprocess.TimeoutExpired:
                    pass
            raise DriverError("stream_process_start_or_ownership_failed") from None
        threading.Thread(target=self._drain_stderr, name="m1-log-driver-stderr", daemon=True).start()

    def _drain_stderr(self):
        try:
            while True:
                chunk = self.process.stderr.read1(4096)
                if not chunk:
                    return
                self.stderr_bytes += len(chunk)
        except (OSError, ValueError):
            return
        finally:
            try:
                self.process.stderr.close()
            except (OSError, ValueError):
                pass

    def read_output(self):
        return self.process.stdout.read1(_READ_SIZE)

    def close_output(self):
        try:
            self.process.stdout.close()
        except (OSError, ValueError):
            pass

    def terminate(self):
        try:
            if self.job is not None:
                self.job.close()
            else:
                os.killpg(self.process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except BaseException:
            try:
                self.process.kill()
            except BaseException:
                pass

    def reap(self, timeout_s=_REAP_TIMEOUT_S):
        try:
            return self.process.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            return None


class KubectlStreamHandle:
    """stop()/join(timeout_s) contract for one bounded stream.

    All on_bytes/on_end callbacks come from a single reader thread, in order, and
    stop after on_end. Termination causes are never upgraded: budget overrun is
    response_limit, deadline expiry timeout, non-zero natural exit or framing
    failure error, archive stop cancelled. The one evidence-based exception is a
    follow stream whose confirmed non-zero exit coincides with fresh Pod
    lifecycle evidence (see termination_recheck): the ended log source is then
    reported as a natural end with return code 0, while the raw exit code and
    the evidence code stay in diagnostics for audit.
    """
    def __init__(self, *, label, stream, timeout_s, max_total_bytes, ingest, on_bytes, on_end, diagnostics,
                 termination_recheck=None):
        _require(type(label) is str and bool(label), "stream_label_required")
        _positive(timeout_s, "stream_timeout_required")
        _require(type(max_total_bytes) is int and max_total_bytes > 0, "stream_budget_required")
        _require(callable(ingest) and callable(on_bytes) and callable(on_end), "stream_callbacks_required")
        _require(termination_recheck is None or callable(termination_recheck), "stream_recheck_invalid")
        self.label, self.stream, self.ingest = label, stream, ingest
        self.max_total_bytes, self.on_bytes, self.on_end = max_total_bytes, on_bytes, on_end
        self.diagnostics = diagnostics
        self.termination_recheck = termination_recheck
        self.termination_basis, self.raw_return_code = None, None
        self.lock = threading.Lock()
        self.stop_requested, self.stopped_at = False, None
        self.stop_event, self.timeout_event = threading.Event(), threading.Event()
        self.delivered, self.budget_exceeded, self.end_report = 0, False, None
        self.timer = threading.Timer(timeout_s, self._expire)
        self.timer.daemon = True
        self.reader = threading.Thread(target=self._read_loop, name="m1-log-driver-stream", daemon=True)
        self.reader.start()
        self.timer.start()

    def _expire(self):
        self.timeout_event.set()
        self.stream.terminate()

    def stop(self):
        with self.lock:
            if self.stop_requested:
                return
            self.stop_requested = True
            self.stopped_at = (time.time(), time.monotonic())
        self.timer.cancel()
        self.stop_event.set()
        self.stream.terminate()

    def _deliver(self, payload):
        _require(type(payload) is bytes and bool(payload), "stream_delivery_invalid")
        if self.delivered + len(payload) > self.max_total_bytes:
            self.budget_exceeded = True
            return
        self.delivered += len(payload)
        self.on_bytes(payload, time.time(), time.monotonic())

    def _safe_recheck(self):
        try:
            return self.termination_recheck()
        except BaseException:
            self.diagnostics.append(self.label + ":end_recheck_failed")
            return None

    def _read_loop(self):
        cause = "natural"
        try:
            while True:
                if self.stop_event.is_set():
                    break
                chunk = self.stream.read_output()
                if not chunk:
                    break
                if self.stop_event.is_set():
                    break
                self.ingest(chunk, self._deliver)
                if self.budget_exceeded:
                    cause = "response_limit"
                    break
        except (OSError, ValueError):
            cause = "pipe_error"
        except DriverError as exc:
            self.diagnostics.append(self.label + ":" + str(exc))
            cause = "protocol_error"
        finally:
            try:
                self.stream.close_output()
            except BaseException:
                pass
        if cause == "natural":
            cause = "cancelled" if self.stop_event.is_set() else "timeout" if self.timeout_event.is_set() else "natural"
        if cause != "natural":
            self.stream.terminate()
        return_code = self.stream.reap()
        if return_code is None:
            return_code = -1
            self.diagnostics.append(self.label + ":stream_reap_unconfirmed")
        if cause == "natural" and return_code != 0:
            cause = "error"
        if cause == "pipe_error":
            cause = "cancelled" if self.stop_event.is_set() else "timeout" if self.timeout_event.is_set() else "error"
        elif cause == "protocol_error":
            cause = "error"
        if (cause == "error" and return_code not in (0, -1) and self.termination_recheck is not None
                and not self.stop_event.is_set() and not self.timeout_event.is_set()):
            # Confirmed nonzero exit on a follow stream: decide by fresh Pod
            # lifecycle evidence, never by stderr text. No evidence keeps error.
            basis = self._safe_recheck()
            if basis is not None:
                self.termination_basis, self.raw_return_code = basis, return_code
                self.diagnostics.append(self.label + ":end_reclassified_natural_evidence=" + basis
                                        + "_raw_rc=" + str(return_code))
                cause, return_code = "natural", 0
        self.end_report = {"termination": cause, "return_code": return_code}
        self.on_end(cause, return_code, time.time(), time.monotonic())

    def join(self, timeout_s):
        _positive(timeout_s, "stream_join_timeout_required")
        self.timer.cancel()
        if self.reader.ident is not None:
            self.reader.join(timeout_s)
        report = self.end_report
        if self.reader.is_alive() or report is None:
            return {"joined": False, "return_code": report["return_code"] if report is not None else -1,
                    "termination": report["termination"] if report is not None else "timeout"}
        return {"joined": True, **report}


class _WatchProjector:
    """Project native kubectl watch envelope lines to identity_projection_v1 NDJSON.

    Frames are reassembled across arbitrary chunk boundaries; a complete source
    line becomes exactly one delivered projection line. ERROR events without a
    usable object resourceVersion fall back to the watch's last confirmed RV.
    """
    def __init__(self, scope, start_resource_version, max_frame_bytes):
        self.scope, self.last_rv = scope, start_resource_version
        self.max_frame_bytes, self.buffer = max_frame_bytes, bytearray()

    def ingest(self, chunk, deliver):
        self.buffer.extend(chunk)
        _require(len(self.buffer) <= self.max_frame_bytes, "watch_frame_exceeds_budget")
        while True:
            line, separator, rest = self.buffer.partition(b"\n")
            if not separator:
                return
            self.buffer = bytearray(rest)
            deliver(self._project(line))

    def _project(self, line):
        try:
            event = json.loads(line)
        except (ValueError, UnicodeError, TypeError):
            raise DriverError("watch_event_json_invalid") from None
        _require(type(event) is dict, "watch_event_invalid")
        kind = event.get("type")
        _require(kind in _EVENT_KINDS, "watch_event_type_unknown")
        obj = event.get("object")
        _require(type(obj) is dict, "watch_event_object_missing")
        meta = obj.get("metadata")
        meta = meta if type(meta) is dict else {}
        if kind == "ERROR":
            rv = meta.get("resourceVersion")
            if not (type(rv) is str and rv):
                rv = self.last_rv
            code = obj.get("code")
            _require(type(code) is int, "watch_error_code_invalid")
            pod = None
        elif kind == "BOOKMARK":
            rv = meta.get("resourceVersion")
            _require(type(rv) is str and bool(rv), "watch_bookmark_resource_version_missing")
            pod, code = None, None
        else:
            pod = project_pod(obj, self.scope)
            rv, code = pod["resource_version"], None
            self.last_rv = rv
        return _json_bytes({"type": kind, "resource_version": rv, "pod": pod, "code": code}) + b"\n"


def _raw_ingest(chunk, deliver, *, max_piece_bytes):
    for start in range(0, len(chunk), max_piece_bytes):
        deliver(chunk[start:start + max_piece_bytes])


class KubectlProjectionDriver:
    """Driver for UidLogArchive: bounded kubectl reads plus owned watch/follow streams.

    projection_format and scope satisfy the archive's start() checks; every read
    returns a ProjectionRead whose clocks bracket the actual subprocess calls.
    """
    projection_format = la.PROJECTION

    def __init__(self, scope, limits, *, executable="kubectl", process=None, stream_factory=None,
                 execute=False, max_source_response_bytes=None):
        _require(isinstance(scope, la.ArchiveScope) and isinstance(limits, la.ArchiveLimits),
                 "driver_scope_and_limits_required")
        _text(executable, "driver_executable_required")
        _require(type(execute) is bool, "driver_execute_boolean_required")
        if max_source_response_bytes is None:
            max_source_response_bytes = max(262_144, limits.max_read_bytes)
        _require(type(max_source_response_bytes) is int and max_source_response_bytes > 0,
                 "driver_source_budget_required")
        self.scope, self.limits, self.executable, self.execute = scope, limits, executable, execute
        self.max_source_response_bytes = max_source_response_bytes
        self.process = process if process is not None else lr.BoundedProcess(execute=execute)
        self._injected_stream_factory = stream_factory
        self.read_log, self.stream_log, self.diagnostics = [], [], []

    def _client(self, deadline):
        remaining = deadline - time.monotonic()
        _require(remaining > 0, "driver_read_deadline")
        return lr.KubectlReadClient(self.executable, self.scope.context, self.scope.namespace,
                                    process=self.process, execute=self.execute, timeout_s=remaining,
                                    max_output_bytes=self.max_source_response_bytes)

    def _get(self, deadline, *args, **kwargs):
        client = self._client(deadline)
        try:
            return client.get(*args, **kwargs)
        except lr.RuntimeReadError as exc:
            raise DriverError(str(exc)) from None
        finally:
            self.read_log.extend(client.reads)

    def _raw_json_read(self, argv, deadline):
        """Finite bounded read of a raw-endpoint JSON document, evidence kept in read_log.

        Used where kubectl printers are unfaithful (review F1: `get -o json` clears the
        collection resourceVersion); the raw face returns the apiserver body unchanged.
        """
        remaining = deadline - time.monotonic()
        _require(remaining > 0, "driver_read_deadline")
        _require(self.execute, "runtime_reads_disabled")
        result = self.process.run(argv, timeout_s=remaining, max_output_bytes=self.max_source_response_bytes)
        self.read_log.append({"argv": list(argv), "status": result.status, "return_code": result.return_code,
                              "started_monotonic_s": result.started_monotonic_s,
                              "ended_monotonic_s": result.ended_monotonic_s,
                              "stdout_sha256": _sha(result.stdout), "workers_joined": result.workers_joined})
        _require(result.status == "ok" and result.return_code == 0 and result.workers_joined, "kubectl_read_failed")
        try:
            value = json.loads(result.stdout)
        except (ValueError, UnicodeError, TypeError):
            raise DriverError("raw_read_json_invalid") from None
        _require(type(value) is dict, "raw_read_json_invalid")
        return value

    def _pods_list_uri(self):
        return "/api/v1/namespaces/%s/pods?labelSelector=%s" % (quote(self.scope.namespace, safe=""),
                                                                quote("app=" + self.scope.app, safe=""))

    def _read_projection(self, build, timeout_s):
        _positive(timeout_s, "driver_read_timeout_required")
        started_epoch, started_mono = time.time(), time.monotonic()
        value = build(started_mono + timeout_s)
        raw = _json_bytes(value)
        ended_epoch, ended_mono = time.time(), time.monotonic()
        source = self.read_log[-1]["stdout_sha256"] if self.read_log else None
        return la.ProjectionRead(raw, started_epoch, ended_epoch, started_mono, ended_mono, source)

    def initial(self, timeout_s):
        def build(deadline):
            cluster = self._get(deadline, "namespace", "kube-system")
            namespace = self._get(deadline, "namespace", self.scope.namespace)
            # Raw-endpoint list: kubectl collection printers clear the List
            # resourceVersion (review F1), the raw face preserves it (review-measured).
            listing = self._raw_json_read((self.executable, "--context", self.scope.context,
                                           "get", "--raw", self._pods_list_uri()), deadline)
            items = listing.get("items") if type(listing) is dict else None
            _require(type(items) is list, "pod_list_invalid")
            meta = listing.get("metadata")
            rv = meta.get("resourceVersion") if type(meta) is dict else None
            _require(type(rv) is str and bool(rv), "pod_list_resource_version_missing")
            pods = [project_pod(pod, self.scope) for pod in items]
            # Deterministic oldest-Pod-first attach order: during a rollout the
            # oldest Pod is the one most likely to be deleted mid-attachment, and
            # the archive attaches initial Pods in list order (S08 attempt-5 post
            # shape: the apiserver happened to list the dying Pod last). Carries
            # no archive meaning beyond attach order.
            pods.sort(key=lambda pod: (pod["creation_epoch_s"], pod["name"]))
            return {"cluster_uid": _namespace_uid(cluster), "namespace_uid": _namespace_uid(namespace),
                    "resource_version": rv, "pods": pods}
        return self._read_projection(build, timeout_s)

    def pod(self, name, timeout_s):
        _text(name, "driver_pod_name_required")

        def build(deadline):
            projection = project_pod(self._get(deadline, "pod", name), self.scope)
            _require(projection["name"] == name, "driver_pod_name_mismatch")
            return projection
        return self._read_projection(build, timeout_s)

    def replicaset(self, name, timeout_s):
        _text(name, "driver_replicaset_name_required")

        def build(deadline):
            return project_replicaset(self._get(deadline, "replicaset", name), self.scope)
        return self._read_projection(build, timeout_s)

    def watch(self, resource_version, *, on_bytes, on_end, timeout_s):
        _text(resource_version, "watch_resource_version_invalid")
        uri = "%s&watch=true&resourceVersion=%s" % (self._pods_list_uri(), quote(resource_version, safe=""))
        argv = (self.executable, "--context", self.scope.context, "get", "--raw", uri)
        projector = _WatchProjector(self.scope, resource_version, self.limits.max_read_bytes)
        return self._start_stream(argv, label="watch", timeout_s=timeout_s, ingest=projector.ingest,
                                  on_bytes=on_bytes, on_end=on_end)

    def follow(self, identity, *, since_epoch_s, on_bytes, on_end, timeout_s):
        _require(type(identity) is dict, "follow_identity_invalid")
        name = identity.get("name")
        uid = identity.get("uid")
        container = identity.get("container")
        container = container.get("name") if type(container) is dict else None
        _text(name, "follow_pod_name_required")
        _require(uid is None or type(uid) is str, "follow_pod_uid_invalid")
        _text(container, "follow_container_required")
        since = _rfc3339_micros(since_epoch_s, "follow_since_time_invalid")
        argv = (self.executable, "--context", self.scope.context, "--namespace", self.scope.namespace,
                "logs", name, "--container", container, "--follow", "--since-time", since)

        def ingest(chunk, deliver):
            _raw_ingest(chunk, deliver, max_piece_bytes=self.limits.max_chunk_bytes)
        return self._start_stream(argv, label="follow:" + name, timeout_s=timeout_s, ingest=ingest,
                                  on_bytes=on_bytes, on_end=on_end,
                                  termination_recheck=lambda: self._follow_termination_evidence(name, uid))

    def _follow_termination_evidence(self, name, uid):
        """Classify a confirmed nonzero follow exit by fresh selector-list evidence.

        Never stderr text: one bounded raw pod-list read decides. Pod absent, or
        its name reused by a different UID, means the followed Pod object is gone;
        a present Pod with a finished container or a requested deletion means the
        log source ended. A present still-running Pod, an unreadable list or any
        recheck failure returns None and the honest "error" stands. The archive
        independently demands its own lifecycle evidence before accepting the
        reclassified natural end, so a wrong basis cannot launder a real failure.
        """
        deadline = time.monotonic() + min(5., self.limits.attach_timeout_s)
        try:
            listing = self._raw_json_read((self.executable, "--context", self.scope.context,
                                           "get", "--raw", self._pods_list_uri()), deadline)
        except DriverError:
            return None
        items = listing.get("items") if type(listing) is dict else None
        if type(items) is not list:
            return None
        for pod_object in items:
            meta = pod_object.get("metadata") if type(pod_object) is dict else None
            if type(meta) is not dict or meta.get("name") != name:
                continue
            if uid is not None and meta.get("uid") != uid:
                return "pod_absent"  # Same name now belongs to another Pod object.
            try:
                pod = project_pod(pod_object, self.scope)
            except DriverError:
                return None
            if pod["container"]["finished_epoch_s"] is not None:
                return "container_finished"
            if pod["deletion_requested_epoch_s"] is not None:
                return "deletion_requested"
            return None
        return "pod_absent"

    def _start_stream(self, argv, *, label, timeout_s, ingest, on_bytes, on_end, termination_recheck=None):
        _positive(timeout_s, "stream_timeout_required")
        factory = self._injected_stream_factory if self._injected_stream_factory is not None else self._default_stream
        stream = factory(argv)
        handle = KubectlStreamHandle(label=label, stream=stream, timeout_s=timeout_s,
                                     max_total_bytes=self.limits.max_total_bytes, ingest=ingest,
                                     on_bytes=on_bytes, on_end=on_end, diagnostics=self.diagnostics,
                                     termination_recheck=termination_recheck)
        self.stream_log.append({"label": label, "argv": list(argv), "timeout_s": timeout_s,
                                "max_total_bytes": self.limits.max_total_bytes,
                                "started_monotonic_s": time.monotonic()})
        return handle

    def _default_stream(self, argv):
        _require(self.execute, "driver_streams_disabled")
        return _StreamProcess(argv)


def kubectl_driver_factory(transport_scope, *, executable="kubectl", process=None, stream_factory=None,
                           execute=False, max_source_response_bytes=None):
    """Bind one TransportScope's pinned identity/budgets to its ArchiveScope reads.

    Duck-typed against log_transport.TransportScope (callable archive_scope plus a real
    ArchiveLimits) so this module does not import the still-independently-reviewed
    transport. Any scope whose pinned UIDs/app/container differ from the incoming
    ArchiveScope is refused explicitly, never reinterpreted.
    """
    _require(callable(getattr(transport_scope, "archive_scope", None))
             and isinstance(getattr(transport_scope, "archive_limits", None), la.ArchiveLimits),
             "driver_factory_transport_scope_required")

    def factory(archive_scope):
        _require(isinstance(archive_scope, la.ArchiveScope), "driver_factory_archive_scope_required")
        expected = transport_scope.archive_scope(archive_scope.context, archive_scope.namespace)
        _require(expected == archive_scope, "driver_factory_scope_mismatch")
        return KubectlProjectionDriver(archive_scope, transport_scope.archive_limits, executable=executable,
                                       process=process, stream_factory=stream_factory, execute=execute,
                                       max_source_response_bytes=max_source_response_bytes)
    return factory
