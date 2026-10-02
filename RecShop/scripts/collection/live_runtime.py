"""Explicit bounded runtime reads for the first catalog-latency smoke.

Construction/validation is inert. These providers do not authorize mutations,
invent physical windows, or promote old snapshots to current observations.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
import base64
import hashlib
import json
import io
import math
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tarfile
import threading
import time
import zlib
from typing import Any, Callable

from . import contract as c, primitives as p, telemetry as t, workload as w

SCHEMA = "rq4-collect/live-read-v1"
OWNER = p.OWNER_ANNOTATION


class RuntimeReadError(RuntimeError):
    """Messages are reason codes, never command/database payloads or secrets.

    U02 (collection.1): an optional ``context`` mapping carries safe structured
    readback facts (failed_command truncated, return_code, timeout_s,
    duration_s, object) so every consumer that stringifies the exception --
    including wrappers outside this module -- retains the underlying command
    identity instead of only the exception type. Context is a bounded JSON-safe
    tree built from whitelisted command metadata; this class never formats
    stdout, stderr text or credentials into it. ``str()`` stays byte-identical
    to the bare reason whenever no context is attached (every existing
    reason-code comparison,
    including exact set-membership tests, is unaffected).
    """

    def __init__(self, reason, *, workers_joined=None, context=None):
        super().__init__(reason)
        self.workers_joined = workers_joined
        if context is not None:
            self.context = _bounded_read_error_context(context)
        else:
            self.context = None

    def __str__(self):
        if self.context is None:
            return super().__str__()
        return super().__str__() + " context=" + json.dumps(self.context, sort_keys=True)


def _require(ok, reason):
    if not ok:
        raise RuntimeReadError(reason)


def _positive(value):
    _require(type(value) in (int, float) and math.isfinite(value) and value > 0, "invalid_positive_budget")


def _name(value):
    _require(type(value) is str and re.fullmatch(r"[a-z0-9][a-z0-9.-]*", value) is not None, "invalid_resource_name")


def _json(raw):
    try:
        value = json.loads(raw)
        _require(type(value) is dict, "json_object_required")
        return value
    except (ValueError, TypeError, UnicodeError):
        raise RuntimeReadError("invalid_json_reply") from None


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _bounded_read_error_context(value, *, depth=0):
    """Copy a small JSON-safe diagnostic tree; never accept process payloads."""
    _require(depth <= 4, "read_error_context_too_deep")
    if type(value) is dict:
        _require(len(value) <= 32 and all(type(key) is str and 0 < len(key) <= 80 for key in value),
                 "read_error_context_mapping_invalid")
        result = {key: _bounded_read_error_context(item, depth=depth + 1)
                  for key, item in value.items()}
    elif type(value) in (list, tuple):
        _require(len(value) <= 64, "read_error_context_list_too_long")
        result = [_bounded_read_error_context(item, depth=depth + 1) for item in value]
    elif type(value) is str:
        result = value[:512]
    elif value is None or type(value) in (int, bool):
        result = value
    elif type(value) is float and math.isfinite(value):
        result = value
    else:
        raise RuntimeReadError("read_error_context_value_invalid")
    if depth == 0:
        _require(type(result) is dict
                 and len(json.dumps(result, sort_keys=True, allow_nan=False).encode("utf-8")) <= 65536,
                 "read_error_context_too_large")
    return result


def _stderr_summary(raw):
    """Return only bounded classification/size/hash metadata, never stderr text."""
    value = raw if type(raw) is bytes else b""
    lowered = value[:65536].lower()
    if b"forbidden" in lowered:
        category = "forbidden"
    elif b"notfound" in lowered or b"not found" in lowered:
        category = "not_found"
    elif b"i/o timeout" in lowered or b"timed out" in lowered:
        category = "timeout"
    elif b"connection refused" in lowered or b"unable to connect" in lowered:
        category = "connection_failure"
    elif b"no matches for kind" in lowered or b"doesn't have a resource type" in lowered:
        category = "resource_type_missing"
    else:
        category = "unclassified"
    return {"bytes": len(value), "sha256": _sha(value), "category": category}


@dataclass(frozen=True)
class ProcessResult:
    status: str
    return_code: int | None
    stdout: bytes = field(repr=False)
    stderr: bytes = field(repr=False)
    started_monotonic_s: float
    ended_monotonic_s: float
    workers_joined: bool


class _WindowsJob:
    """Own the new suspended process tree before allowing its first instruction."""
    def __init__(self):
        import ctypes
        from ctypes import wintypes as wt
        self.ctypes, self.wt = ctypes, wt
        self.api = ctypes.WinDLL("kernel32", use_last_error=True)
        class Basic(ctypes.Structure):
            _fields_ = [("process_time", ctypes.c_int64), ("job_time", ctypes.c_int64), ("flags", wt.DWORD),
                        ("min_working_set", ctypes.c_size_t), ("max_working_set", ctypes.c_size_t), ("active_process_limit", wt.DWORD),
                        ("affinity", ctypes.c_size_t), ("priority_class", wt.DWORD), ("scheduling_class", wt.DWORD)]
        class Extended(ctypes.Structure):
            _fields_ = [("basic", Basic), ("io", ctypes.c_uint64 * 6), ("process_memory", ctypes.c_size_t),
                        ("job_memory", ctypes.c_size_t), ("peak_process_memory", ctypes.c_size_t), ("peak_job_memory", ctypes.c_size_t)]
        signatures = {"CreateJobObjectW": ([ctypes.c_void_p, wt.LPCWSTR], wt.HANDLE),
                      "SetInformationJobObject": ([wt.HANDLE, ctypes.c_int, ctypes.c_void_p, wt.DWORD], wt.BOOL),
                      "AssignProcessToJobObject": ([wt.HANDLE, wt.HANDLE], wt.BOOL),
                      "CloseHandle": ([wt.HANDLE], wt.BOOL),
                      "CreateToolhelp32Snapshot": ([wt.DWORD, wt.DWORD], wt.HANDLE),
                      "OpenThread": ([wt.DWORD, wt.BOOL, wt.DWORD], wt.HANDLE),
                      "ResumeThread": ([wt.HANDLE], wt.DWORD)}
        for name, (args, result) in signatures.items():
            method = getattr(self.api, name); method.argtypes, method.restype = args, result
        self.handle = self.api.CreateJobObjectW(None, None)
        _require(bool(self.handle), "process_job_create_failed")
        limits = Extended(); limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.api.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
            self.close()
            raise RuntimeReadError("process_job_limits_failed")

    def attach_and_resume(self, process):
        api, ctypes, wt = self.api, self.ctypes, self.wt
        _require(bool(api.AssignProcessToJobObject(self.handle, wt.HANDLE(int(process._handle)))), "process_job_assignment_failed")
        class Entry(ctypes.Structure):
            _fields_ = [("size", wt.DWORD), ("usage", wt.DWORD), ("tid", wt.DWORD), ("pid", wt.DWORD),
                        ("base_priority", wt.LONG), ("delta_priority", wt.LONG), ("flags", wt.DWORD)]
        for name in ("Thread32First", "Thread32Next"):
            getattr(api, name).argtypes = [wt.HANDLE, ctypes.POINTER(Entry)]
            getattr(api, name).restype = wt.BOOL
        snapshot = api.CreateToolhelp32Snapshot(4, 0)
        _require(snapshot not in (None, ctypes.c_void_p(-1).value), "process_thread_inventory_failed")
        resumed = False
        try:
            entry = Entry(); entry.size = ctypes.sizeof(entry)
            available = api.Thread32First(snapshot, ctypes.byref(entry))
            while available:
                if entry.pid == process.pid:
                    thread = api.OpenThread(0x0002, False, entry.tid)
                    _require(bool(thread), "process_thread_open_failed")
                    try:
                        _require(api.ResumeThread(thread) != 0xFFFFFFFF, "process_resume_failed")
                        resumed = True
                    finally:
                        api.CloseHandle(thread)
                available = api.Thread32Next(snapshot, ctypes.byref(entry))
        finally:
            api.CloseHandle(snapshot)
        _require(resumed, "process_thread_missing")

    def close(self):
        if self.handle is not None:
            handle, self.handle = self.handle, None
            _require(bool(self.api.CloseHandle(handle)), "process_job_close_failed")


class BoundedProcess:
    """No shell; bounded pipe readers, process-tree kill and joined readers.

    The same executable must own its spawned descendants. No process outside
    that newly started tree is targeted. Output values are never in exceptions.
    """
    def __init__(self, *, execute=False):
        _require(type(execute) is bool, "explicit_execute_boolean_required")
        self.execute = execute

    def run(self, argv, *, timeout_s, max_output_bytes, stdin=None):
        _require(type(argv) is tuple and bool(argv) and all(type(v) is str and v and "\0" not in v for v in argv), "invalid_argv")
        _positive(timeout_s)
        _require(type(max_output_bytes) is int and max_output_bytes > 0, "invalid_output_budget")
        _require(stdin is None or type(stdin) is bytes, "stdin_bytes_required")
        start = time.monotonic()
        if not self.execute:
            return ProcessResult("not_run", None, b"", b"", start, start, True)
        job = _WindowsJob() if os.name == "nt" else None
        kwargs = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | 0x00000004} if job is not None else {"start_new_session": True}
        process = None
        try:
            environment = os.environ.copy()
            environment.update(NO_PROXY="*", no_proxy="*")
            process = subprocess.Popen(list(argv), stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False, env=environment, **kwargs)
            if job is not None:
                job.attach_and_resume(process)
        except BaseException:
            if job is not None:
                job.close()
            if process is not None:
                process.kill(); process.wait(timeout=5)
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream is not None:
                        stream.close()
            raise RuntimeReadError("process_start_or_ownership_failed") from None
        buffers, lock, exceeded, io_failed = [bytearray(), bytearray()], threading.Lock(), threading.Event(), threading.Event()
        def read_pipe(stream, index):
            try:
                while True:
                    chunk = stream.read1(4096)
                    if not chunk:
                        return
                    with lock:
                        remaining = max_output_bytes - sum(map(len, buffers))
                        buffers[index].extend(chunk[:max(0, remaining)])
                        if len(chunk) > remaining:
                            exceeded.set()
            except (OSError, ValueError):
                io_failed.set()
            finally:
                stream.close()
        def write_stdin():
            try:
                process.stdin.write(stdin)
                process.stdin.flush()
            except (OSError, ValueError):
                io_failed.set()
            finally:
                process.stdin.close()
        threads, status, worker_error, cleanup_error = [], "ok", False, False
        try:
            for index, stream in enumerate((process.stdout, process.stderr)):
                threads.append(threading.Thread(target=read_pipe, args=(stream, index), daemon=True))
            if stdin is not None:
                threads.append(threading.Thread(target=write_stdin, daemon=True))
            for thread in threads:
                thread.start()
            while process.poll() is None:
                if exceeded.is_set() or io_failed.is_set() or time.monotonic() >= start + timeout_s:
                    status = "response_limit" if exceeded.is_set() else "pipe_error" if io_failed.is_set() else "timeout"
                    break
                time.sleep(min(.01, max(0, start + timeout_s - time.monotonic())))
        except BaseException:
            worker_error = True
        finally:
            try:
                if job is not None:
                    job.close()  # Also terminates descendants after an early parent exit.
                else:
                    os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except BaseException:
                cleanup_error = True
                if process.poll() is None:
                    process.kill()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                cleanup_error = True
            for thread in threads:
                if thread.ident is not None:
                    thread.join(timeout=5)
            if not any(thread.is_alive() for thread in threads):
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream is not None and not stream.closed:
                        stream.close()
        joined = not any(thread.is_alive() for thread in threads)
        _require(not cleanup_error, "process_cleanup_unconfirmed")
        _require(joined, "process_pipe_cleanup_unconfirmed")
        _require(not worker_error, "process_worker_start_or_monitor_failed")
        status = "response_limit" if exceeded.is_set() else "pipe_error" if io_failed.is_set() else status
        if status == "ok" and process.returncode != 0:
            status = "command_error"
        return ProcessResult(status, process.returncode, bytes(buffers[0]), bytes(buffers[1]), start, time.monotonic(), joined)


def _read_object(args):
    """Safe resource identity of a kubectl read argv (no payload, no secrets).

    U02: the object a failing read targeted, so an upstream that stringifies
    the exception keeps WHICH object failed, not only the exception type.
    """
    if len(args) >= 2 and args[0] == "get":
        return args[1] + (("/" + args[2]) if len(args) >= 3 and args[2] not in ("-l", "-o") else "")
    if args and args[0] == "exec":
        return "exec " + (args[1] if len(args) > 1 else "")
    return "kubectl"


def _safe_argv(argv):
    """Argv for error context: element-wise truncation, kubectl carries no
    credentials in argv (context/namespace/resource words only)."""
    return " ".join(str(part)[:80] for part in list(argv)[:16])[:512]


class KubectlReadClient:
    """Explicit context/namespace, narrow GET/known read-only EXEC methods."""
    def __init__(self, executable, context, namespace, *, process=None, execute=False, timeout_s=15,
                 max_output_bytes=2_000_000, capture_read_diagnostics=False):
        _require(type(executable) is str and bool(executable), "explicit_kubectl_required")
        _require(type(context) is str and bool(context) and not any(v in context for v in "\r\n\0"), "explicit_context_required")
        _name(namespace); _positive(timeout_s)
        _require(type(max_output_bytes) is int and max_output_bytes > 0 and type(execute) is bool
                 and type(capture_read_diagnostics) is bool, "invalid_read_policy")
        self.executable, self.context, self.namespace = executable, context, namespace
        self.process = process if process is not None else BoundedProcess(execute=execute)
        self.execute, self.timeout_s, self.max_output_bytes = execute, timeout_s, max_output_bytes
        self.reads = []
        self.read_diagnostics = [] if capture_read_diagnostics else None

    def _read(self, args, *, namespace=None):
        _require(self.execute, "runtime_reads_disabled")
        ns = self.namespace if namespace is None else namespace
        _name(ns)
        argv = (self.executable, "--context", self.context, "--namespace", ns, *args)
        call_started = time.monotonic()
        try:
            result = self.process.run(argv, timeout_s=self.timeout_s, max_output_bytes=self.max_output_bytes)
        except BaseException:
            
            # object ride the exception itself, so every wrapper that
            # stringifies it (or narrows it to a type name) can still keep
            # the first-cause command identity.
            call_ended = time.monotonic()
            if self.read_diagnostics is not None:
                self.read_diagnostics.append({
                    "command": _safe_argv(argv), "object": _read_object(args),
                    "status": "transport_raised", "return_code": None,
                    "timeout_s": self.timeout_s, "started_monotonic_s": call_started,
                    "ended_monotonic_s": call_ended,
                    "duration_s": round(call_ended - call_started, 6),
                    "workers_joined": False, "stdout_bytes": 0,
                    "stdout_sha256": _sha(b""),
                    "stderr_summary": {"bytes": 0, "sha256": _sha(b""), "category": "unavailable"}})
            raise RuntimeReadError("kubectl_read_transport_raised", workers_joined=False, context={
                "failed_command": _safe_argv(argv), "timeout_s": self.timeout_s,
                "read_status": "transport_raised", "return_code": None,
                "object": _read_object(args)}) from None
        self.reads.append({"argv": list(argv), "status": result.status, "return_code": result.return_code,
                           "started_monotonic_s": result.started_monotonic_s, "ended_monotonic_s": result.ended_monotonic_s,
                           "stdout_sha256": _sha(result.stdout), "workers_joined": result.workers_joined})
        if self.read_diagnostics is not None:
            self.read_diagnostics.append({
                "command": _safe_argv(argv), "object": _read_object(args),
                "status": result.status, "return_code": result.return_code,
                "timeout_s": self.timeout_s,
                "started_monotonic_s": result.started_monotonic_s,
                "ended_monotonic_s": result.ended_monotonic_s,
                "duration_s": round(result.ended_monotonic_s - result.started_monotonic_s, 6),
                "workers_joined": result.workers_joined,
                "stdout_bytes": len(result.stdout), "stdout_sha256": _sha(result.stdout),
                "stderr_summary": _stderr_summary(result.stderr)})
        if not (result.status == "ok" and result.return_code == 0 and result.workers_joined):
            raise RuntimeReadError("kubectl_read_failed", workers_joined=result.workers_joined is True, context={
                "failed_command": _safe_argv(argv), "return_code": result.return_code,
                "read_status": result.status, "timeout_s": self.timeout_s,
                "duration_s": round(result.ended_monotonic_s - result.started_monotonic_s, 6),
                "object": _read_object(args), "stderr_summary": _stderr_summary(result.stderr)})
        return _json(result.stdout)

    def get(self, resource, name=None, *, selector=None):
        _require(resource in {"node", "namespace", "deployment", "replicaset", "pod", "configmap", "networkchaos", "stresschaos", "podchaos"}, "read_resource_not_supported")
        args = ["get", resource]
        if name is not None:
            _name(name); args.append(name)
        if selector is not None:
            _require(name is None and type(selector) is dict and set(selector) == {"app"}, "explicit_app_selector_required")
            value = selector["app"]
            _require(type(value) is str and 0 < len(value) <= 63
                     and re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?", value),
                     "invalid_app_label_value")
            args += ["-l", "app=" + value]
        return self._read((*args, "-o", "json"))

    def get_labeled_kinds(self, kinds, selector):
        """P02b-c (collection.1, D02-a7 evidence): ONE spawn for several labeled kinds.

        ``kubectl get replicaset,pod -l app=<x>`` returns a single List whose
        items carry their own kind; the caller splits client-side. Same data,
        same receipts discipline, one process spawn instead of one per kind
        (each spawn costs ~1 s under phase-start contention on this host).
        """
        _require(type(kinds) is tuple and 2 <= len(kinds) <= 4
                 and set(kinds) <= {"replicaset", "pod", "deployment", "configmap"}, "labeled_kinds_not_supported")
        _require(type(selector) is dict and set(selector) == {"app"}, "explicit_app_selector_required")
        value = selector["app"]
        _require(type(value) is str and 0 < len(value) <= 63
                 and re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?", value),
                 "invalid_app_label_value")
        reply = self._read(("get", ",".join(kinds), "-l", "app=" + value, "-o", "json"))
        _require(type(reply) is dict and reply.get("kind") == "List" and type(reply.get("items")) is list,
                 "labeled_kinds_reply_invalid")
        return reply

    def catalog_runtime(self, pod_name, container):
        _name(pod_name); _name(container)
        return self._read(("exec", "pod/" + pod_name, "--container", container, "--", "python", "-c", _CATALOG_READ_SCRIPT))

    def service_runtime(self, pod_name, container, source_path):
        _name(pod_name); _name(container)
        _require(type(source_path) is str and (re.fullmatch(r"/app/services/[A-Za-z_][A-Za-z0-9_]*/(?:app|api_server|workflow)\.py", source_path)
                 or source_path == "/app/services/shop_web/app/__init__.py"), "runtime_source_path_not_approved")
        script = _CATALOG_READ_SCRIPT.replace("'/app/services/catalog_service/app.py'", repr(source_path)).replace(
            "'FAULT_DELAY_MS','FAULT_RAISE'", "'OTEL_SERVICE_NAME','OTEL_RESOURCE_ATTRIBUTES'")
        return self._read(("exec", "pod/" + pod_name, "--container", container, "--", "python", "-c", script))


_CATALOG_READ_SCRIPT = "\n".join((
    "import hashlib,json,pathlib,importlib.metadata",
    "keys=('OTEL_METRIC_EXPORT_INTERVAL','OTEL_EXPORTER_OTLP_METRICS_ENDPOINT','NACOS_ENABLED','FAULT_DELAY_MS','FAULT_RAISE')",
    "env=dict(x.split(b'=',1) for x in pathlib.Path('/proc/1/environ').read_bytes().split(b'\\0') if b'=' in x)",
    "source=pathlib.Path('/app/services/catalog_service/app.py').read_bytes()",
    "helper=pathlib.Path('/app/shared/otel_metric_config.py').read_bytes()",
    "print(json.dumps({'process_id':1,'process_cmd_sha256':hashlib.sha256(pathlib.Path('/proc/1/cmdline').read_bytes()).hexdigest(),'env':{k:env.get(k.encode(),None).decode() if env.get(k.encode(),None) is not None else None for k in keys},'source_sha256':hashlib.sha256(source).hexdigest(),'helper_sha256':hashlib.sha256(helper).hexdigest(),'otel_sdk_version':importlib.metadata.version('opentelemetry-sdk')}))",
))


def _uid(obj, kind, *, name=None, namespace=None):
    _require(type(obj) is dict and obj.get("kind") == kind and type(obj.get("metadata")) is dict, "resource_kind_missing")
    meta = obj["metadata"]
    _require(type(meta.get("uid")) is str and bool(meta["uid"]) and type(meta.get("resourceVersion")) is str
             and bool(meta["resourceVersion"]) and not meta.get("deletionTimestamp"), "resource_identity_invalid")
    if name is not None:
        _require(meta.get("name") == name, "resource_name_mismatch")
    if namespace is not None:
        _require(meta.get("namespace") == namespace, "resource_namespace_mismatch")
    return meta["uid"]


def _owner_refs(obj, kind, uid):
    return any(ref.get("kind") == kind and ref.get("uid") == uid and ref.get("controller") is True
               for ref in obj["metadata"].get("ownerReferences", []))


class CatalogEnvironmentReader:
    def __init__(self, kube: KubectlReadClient, *, expected_cluster_uid, expected_namespace_uid, expected_deployment_uid,
                 container="catalog", evidence_kind="observed"):
        _require(isinstance(kube, KubectlReadClient), "explicit_kube_read_client_required")
        _name(container)
        _require(all(type(v) is str and v for v in (expected_cluster_uid, expected_namespace_uid, expected_deployment_uid)), "expected_environment_identity_required")
        _require(evidence_kind in {"observed", "synthetic"}, "explicit_evidence_kind_required")
        self.kube, self.container, self.evidence_kind = kube, container, evidence_kind
        self.expected = (expected_cluster_uid, expected_namespace_uid, expected_deployment_uid)

    def __call__(self, *, expected_owner=None):
        """Read the catalog environment once; expected_owner classifies own carriers.

        collection: with expected_owner set to the current contract's owner_id, a
        deployment/pod/configmap whose metadata OWNER annotation equals it is an
        examined own carrier (recorded in ``examined_own_carriers``), never an
        unexamined residual. The exemption boundary is exact: any other owner
        (including a previous attempt's), template-only annotations and every
        chaos CRD keep the unchanged residual semantics. With expected_owner
        unset (the default) the scan is byte-identical to the pre-collection read and
        the output carries no ``examined_own_carriers`` key at all.
        """
        _require(expected_owner is None or (type(expected_owner) is str and bool(expected_owner)),
                 "expected owner must be None or a non-empty string")
        started, first_read = time.time(), len(self.kube.reads)
        cluster = _uid(self.kube.get("namespace", "kube-system"), "Namespace", name="kube-system")
        namespace = _uid(self.kube.get("namespace", self.kube.namespace), "Namespace", name=self.kube.namespace)
        deployment = self.kube.get("deployment", "catalog")
        dep_uid = _uid(deployment, "Deployment", name="catalog", namespace=self.kube.namespace)
        _require((cluster, namespace, dep_uid) == self.expected, "environment_identity_changed")
        _require(deployment.get("spec", {}).get("selector", {}).get("matchLabels") == {"app": "catalog"}, "catalog_selector_changed")
        sets = self.kube.get("replicaset", selector={"app": "catalog"})
        pods = self.kube.get("pod", selector={"app": "catalog"})
        _require(type(sets.get("items")) is list and type(pods.get("items")) is list and bool(pods["items"]), "catalog_inventory_missing")
        rs_uids = {_uid(obj, "ReplicaSet", namespace=self.kube.namespace) for obj in sets["items"] if _owner_refs(obj, "Deployment", dep_uid)}
        pod_facts = []
        for pod in pods["items"]:
            uid = _uid(pod, "Pod", namespace=self.kube.namespace)
            _require(pod["metadata"].get("labels", {}).get("app") == "catalog"
                     and any(_owner_refs(pod, "ReplicaSet", rid) for rid in rs_uids), "catalog_pod_owner_or_selector_changed")
            containers = [row for row in pod.get("status", {}).get("containerStatuses", []) if row.get("name") == self.container]
            _require(len(containers) == 1, "catalog_container_identity_missing")
            row = containers[0]
            _require(row.get("ready") is True and type(row.get("restartCount")) is int
                     and all(type(row.get(key)) is str and row[key] for key in ("imageID", "containerID")), "catalog_container_not_ready_or_unidentified")
            pod_facts.append({"name": pod["metadata"]["name"], "uid": uid, "resource_version": pod["metadata"]["resourceVersion"],
                              "container": self.container, "image_id": row.get("imageID"), "container_id": row.get("containerID"),
                              "restart_count": row.get("restartCount"), "started_at": row.get("state", {}).get("running", {}).get("startedAt"),
                              "ready": row.get("ready") is True})
        residuals = []
        examined_own_carriers = []
        for resource in ("deployment", "pod", "configmap", "networkchaos", "stresschaos", "podchaos"):
            inventory = self.kube.get(resource)
            _require(type(inventory.get("items")) is list, "residual_inventory_unreadable")
            for obj in inventory["items"]:
                owner = obj.get("metadata", {}).get("annotations", {}).get(OWNER)
                template_owner = obj.get("spec", {}).get("template", {}).get("metadata", {}).get("annotations", {}).get(OWNER)
                if (expected_owner is not None and owner == expected_owner
                        and resource not in {"networkchaos", "stresschaos", "podchaos"}):
                    examined_own_carriers.append({"resource": resource, "name": obj["metadata"].get("name"),
                                                  "uid": obj["metadata"].get("uid"), "owner": owner})
                    continue
                if owner or template_owner or resource in {"networkchaos", "stresschaos", "podchaos"}:
                    residuals.append({"resource": resource, "name": obj["metadata"].get("name"), "uid": obj["metadata"].get("uid"),
                                      "owner": owner or template_owner, "unknown_ownership": not bool(owner or template_owner)})
        _require(_uid(self.kube.get("namespace", "kube-system"), "Namespace", name="kube-system") == cluster
                 and _uid(self.kube.get("namespace", self.kube.namespace), "Namespace", name=self.kube.namespace) == namespace,
                 "environment_identity_changed_during_read")
        final_deployment = self.kube.get("deployment", "catalog")
        _require(_uid(final_deployment, "Deployment", name="catalog", namespace=self.kube.namespace) == dep_uid
                 and final_deployment.get("spec") == deployment.get("spec")
                 and final_deployment["metadata"].get("generation") == deployment["metadata"].get("generation")
                 and final_deployment["metadata"].get("annotations", {}) == deployment["metadata"].get("annotations", {}),
                 "deployment_changed_during_environment_read")
        final_pods = self.kube.get("pod", selector={"app": "catalog"}).get("items")
        _require(type(final_pods) is list and {obj["metadata"]["uid"] for obj in final_pods} == {fact["uid"] for fact in pod_facts},
                 "catalog_pod_set_changed_during_read")
        for fact in pod_facts:
            obj = next(obj for obj in final_pods if obj["metadata"]["uid"] == fact["uid"])
            _uid(obj, "Pod", name=fact["name"], namespace=self.kube.namespace)
            initial = next(pod for pod in pods["items"] if pod["metadata"]["uid"] == fact["uid"])
            _require(obj["metadata"].get("labels") == initial["metadata"].get("labels")
                     and obj["metadata"].get("ownerReferences") == initial["metadata"].get("ownerReferences")
                     and obj.get("status", {}).get("containerStatuses") == initial.get("status", {}).get("containerStatuses"),
                     "catalog_container_generation_changed_during_read")
        result = {"schema_version": SCHEMA, "evidence_kind": self.evidence_kind, "return_code": 0,
                  "cluster_uid": cluster, "namespace_uid": namespace, "namespace": self.kube.namespace, "context": self.kube.context,
                  "started_at_epoch_s": started, "observed_at_epoch_s": time.time(), "deployment_uid": dep_uid,
                  "deployment_resource_version": deployment["metadata"]["resourceVersion"], "pods": pod_facts,
                  "residual_owners": residuals, "read_evidence": self.kube.reads[first_read:]}
        if expected_owner is not None:
            # Only owner-aware reads grow the output shape; the default read
            
            result["examined_own_carriers"] = examined_own_carriers
        return result


@dataclass(frozen=True)
class DbEnvironmentRefs:
    host: str = "DB_HOST"
    port: str = "DB_PORT"
    user: str = "DB_USER"
    password: str = "DB_PASSWORD"
    database: str = "DB_NAME"

    def __post_init__(self):
        _require(all(re.fullmatch(r"[A-Z][A-Z0-9_]*", value) for value in self.__dict__.values()), "invalid_db_environment_reference")


@dataclass(frozen=True)
class ComponentReadTarget:
    entity: str
    source_id: str
    deployment: str
    deployment_uid: str
    app: str
    container: str

    def __post_init__(self):
        for value in (self.entity, self.source_id, self.deployment_uid):
            _require(type(value) is str and bool(value) and not any(x in value for x in "\r\n\0"),
                     "component_identity_required")
        _name(self.deployment); _name(self.container)
        _require(type(self.app) is str and 0 < len(self.app) <= 63
                 and re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?", self.app),
                 "component_app_label_invalid")


class PhaseReadbackReader:
    """Fixed-range K8s component evidence and pre/post read-only DB captures.

    The target list is supplied by the collection plan; this class does not
    select its scope from the case ground truth. API replies are projected to
    identity/state fields so Deployment env/credentials never enter artifacts.
    """

    def __init__(self, kube, targets, *, db_probe=None, locks_probe=None, sampling_readers=(), evidence_kind="observed"):
        _require(isinstance(kube, KubectlReadClient) and type(targets) is tuple and targets
                 and all(isinstance(target, ComponentReadTarget) for target in targets)
                 and len({target.entity for target in targets}) == len(targets), "explicit_unique_component_targets_required")
        _require(db_probe is None or callable(getattr(db_probe, "capture", None)), "bounded_db_capture_required")
        
        # fresh lock-release readback; absent for every lane without db legs.
        _require(locks_probe is None or callable(getattr(locks_probe, "capture_bounded", None)), "bounded_locks_capture_required")
        _require(type(sampling_readers) is tuple and all(callable(getattr(reader, "capture_bounded", None)) for reader in sampling_readers),
                 "explicit_sampling_readers_required")
        _require(evidence_kind in {"observed", "synthetic"}, "component_provenance_required")
        self.kube, self.targets, self.db, self.sampling = kube, targets, db_probe, sampling_readers
        self.locks = locks_probe
        self.evidence_kind = evidence_kind

    def _component(self, contract, phase, target, kube=None):
        kube = self.kube if kube is None else kube
        context = contract.context.to_dict()
        _require((context["kube_context"], context["namespace"]) == (self.kube.context, self.kube.namespace),
                 "component_kube_identity_mismatch")
        started, first_read = time.time(), len(kube.reads)
        before = kube.get("deployment", target.deployment)
        _require(_uid(before, "Deployment", name=target.deployment, namespace=self.kube.namespace)
                 == target.deployment_uid, "component_deployment_uid_mismatch")
        _require(before.get("spec", {}).get("selector", {}).get("matchLabels", {}).get("app") == target.app,
                 "component_app_selector_mismatch")
        
        # ONE kubectl spawn (multi-kind List); every spawn costs ~1 s under
        # phase-start contention on this host, so the per-source chain drops
        # 4 -> 3 spawns. Downstream validation is byte-identical.
        bundle = kube.get_labeled_kinds(("replicaset", "pod"), {"app": target.app})
        replicasets = {"items": [row for row in bundle["items"] if row.get("kind") == "ReplicaSet"]}
        pods = {"items": [row for row in bundle["items"] if row.get("kind") == "Pod"]}
        rs_uids = {_uid(row, "ReplicaSet", namespace=self.kube.namespace)
                   for row in replicasets.get("items", []) if _owner_refs(row, "Deployment", target.deployment_uid)}
        projected = []
        for pod in pods.get("items", []):
            # A replacement Pod is legitimate only under this Deployment's RS.
            _require(any(_owner_refs(pod, "ReplicaSet", uid) for uid in rs_uids),
                     "component_foreign_or_unproven_pod_ancestry")
            metadata = pod.get("metadata", {})
            _require(type(metadata.get("uid")) is str and metadata["uid"]
                     and metadata.get("namespace") == self.kube.namespace, "component_pod_identity_missing")
            states = [row for row in pod.get("status", {}).get("containerStatuses", [])
                      if row.get("name") == target.container]
            _require(len(states) == 1, "component_container_status_missing_or_ambiguous")
            status = states[0]
            _require(type(status.get("restartCount")) is int and status["restartCount"] >= 0
                     and type(status.get("ready")) is bool, "component_container_state_invalid")
            projected.append({"pod_name": metadata.get("name"), "pod_uid": metadata["uid"],
                              "deletion_timestamp": metadata.get("deletionTimestamp"),
                              "container": target.container, "container_id": status.get("containerID"),
                              "restart_count": status["restartCount"], "ready": status["ready"],
                              "image_id": status.get("imageID"), "node_name": pod.get("spec", {}).get("nodeName"),
                              "pod_ip": pod.get("status", {}).get("podIP"),
                              "container_started_at": status.get("state", {}).get("running", {}).get("startedAt"),
                              "phase": pod.get("status", {}).get("phase")})
        after = kube.get("deployment", target.deployment)
        _require(_uid(after, "Deployment", name=target.deployment, namespace=self.kube.namespace)
                 == target.deployment_uid and after["metadata"].get("generation") == before["metadata"].get("generation"),
                 "component_deployment_changed_during_read")
        fields = {"desired_replicas": after.get("spec", {}).get("replicas"),
                  "ready_replicas": after.get("status", {}).get("readyReplicas", 0),
                  "available_replicas": after.get("status", {}).get("availableReplicas", 0),
                  "generation": after["metadata"].get("generation"),
                  "observed_generation": after.get("status", {}).get("observedGeneration")}
        _require(all(type(value) is int and value >= 0 for value in fields.values()), "component_deployment_counts_missing")
        # No containerStatuses => no readiness proof, even if stale deployment
        # status still says ready; terminating Pods cannot prove healthy state.
        ready_count = sum(row["ready"] and row["phase"] == "Running" and row["deletion_timestamp"] is None
                          for row in projected)
        fields["ready_replicas"] = min(fields["ready_replicas"], ready_count)
        return {"schema_version": "rq4-collect/component-readback-r10-v1", "contract_sha256": contract.sha256,
                "run_id": context["run_id"], "attempt_id": context["attempt_id"], "phase": phase,
                "entity": target.entity, "source_id": target.source_id, "evidence_kind": self.evidence_kind,
                "return_code": 0, "started_at_epoch_s": started, "timestamp_epoch_s": time.time(),
                "deployment": target.deployment, "deployment_uid": target.deployment_uid,
                "resource_version": after["metadata"]["resourceVersion"], "app": target.app,
                "fields": fields, "pods": projected, "read_receipts": kube.reads[first_read:]}

    
    # guard inside _component fires when a declared rollout (app_env inject
    # or recover) writes the Deployment spec between the bracketing
    # deployment reads of ONE snapshot chain -- a torn read, not drift. The
    # spec write lands once and generation stays stable afterwards (rollout
    # progress never bumps it again), so retrying the whole chain bounded
    # times under the same deadline converges; any other reason surfaces
    # exactly as before. The min-left floor keeps a hopeless retry from
    # burning into the deadline clamp and masking the torn-read reason.
    TORN_READ_RETRY_ATTEMPTS = 3
    TORN_READ_RETRY_MIN_LEFT_S = 3.0

    def _component_stable(self, contract, phase, target, kube, deadline):
        attempts = 0
        while True:
            attempts += 1
            try:
                return self._component(contract, phase, target, kube)
            except RuntimeReadError as exc:
                if (attempts < self.TORN_READ_RETRY_ATTEMPTS
                        and exc.args and exc.args[0] == "component_deployment_changed_during_read"
                        and deadline - time.monotonic() > self.TORN_READ_RETRY_MIN_LEFT_S):
                    continue
                raise

    def capture_phase(self, contract, phase, *, deadline_epoch_s):
        """Every child call consumes the same phase deadline, never a fresh one."""
        _require(phase in {"pre_fault", "during_fault", "post_recovery"}, "component_phase_required")
        remaining = deadline_epoch_s - time.time()
        _positive(remaining)
        # Keep wall time for evidence and a monotonic deadline for enforcement.
        deadline = time.monotonic() + remaining
        capture_started_monotonic_s = deadline - remaining
        original = self.kube

        class PhaseKube(KubectlReadClient):
            def _read(local, args, *, namespace=None):
                left = deadline - time.monotonic()
                _require(left > 0, "phase_component_deadline")
                local.timeout_s = min(original.timeout_s, left)
                result = super()._read(args, namespace=namespace)
                _require(time.monotonic() <= deadline, "phase_component_deadline")
                return result

        kube = PhaseKube(original.executable, original.context, original.namespace, process=original.process,
                         execute=original.execute, timeout_s=original.timeout_s, max_output_bytes=original.max_output_bytes,
                         capture_read_diagnostics=True)
        components, issues = [], []
        for target in self.targets:
            try:
                components.append(self._component_stable(contract, phase, target, kube, deadline))
            except RuntimeReadError as exc:
                # Empty status during a fault is missing state, not an active
                # worker or a zero restart counter. Keep a truthful gap record.
                drained = exc.workers_joined if type(exc.workers_joined) is bool else all(
                    row.get("workers_joined") is True for row in kube.reads)
                if phase == "during_fault" and drained and str(exc) in {
                    "component_container_status_missing_or_ambiguous", "component_container_state_invalid",
                    "component_deployment_counts_missing"}:
                    issues.append({"entity": target.entity, "source_id": target.source_id,
                                   "reason": str(exc), "status": "NOT_ASSESSED", "workers_joined": True})
                    continue
                # U02: keep the underlying command context (failed command/rc/
                # timeout/duration/object) when re-wrapping, so the narrowed
                # type upstream still stringifies the first cause. Retain the
                # bounded read sequence and remaining shared deadline so a
                # truncated outer run_error does not erase whether this was a
                # nonzero command or the last deadline slice.
                now_mono = time.monotonic()
                read_receipts = kube.read_diagnostics
                context = {
                    "phase": phase,
                    "target": {"entity": target.entity, "source_id": target.source_id,
                               "deployment": target.deployment, "deployment_uid": target.deployment_uid,
                               "app": target.app, "container": target.container},
                    "capture_started_monotonic_s": capture_started_monotonic_s,
                    "capture_deadline_monotonic_s": deadline,
                    "capture_elapsed_s": round(now_mono - capture_started_monotonic_s, 6),
                    "capture_remaining_s": round(deadline - now_mono, 6),
                    "read_error": str(exc.args[0])[:256],
                    "failure": dict(exc.context or {}),
                    "read_receipts": list(read_receipts[-32:]),
                    "read_receipts_total": len(read_receipts),
                    "read_receipts_truncated": len(read_receipts) > 32}
                raise RuntimeReadError(exc.args[0], workers_joined=drained,
                                       context=context) from None
        database = None
        if self.db is not None and phase != "during_fault":
            left = deadline - time.monotonic()
            _require(left >= 1, "phase_database_budget_exhausted")
            if isinstance(self.db, ReadOnlyDbProbe):
                database = self.db.capture(contract, phase, total_timeout_s=left)
            else:
                _require(callable(getattr(self.db, "capture_bounded", None)), "phase_db_adapter_requires_shared_deadline")
                database = self.db.capture_bounded(contract, phase, total_timeout_s=left)
            _require(database.get("workers_joined") is True, "database_capture_drain_unconfirmed")
        locks = None
        if phase == "post_recovery" and self.locks is not None:
            
            # deadline (never a fresh one), exactly like the database face.
            left = deadline - time.monotonic()
            _require(left >= 1, "phase_locks_budget_exhausted")
            locks = self.locks.capture_bounded(contract, phase, total_timeout_s=left)
            _require(locks is None or (type(locks) is dict and locks.get("workers_joined") is True),
                     "locks_capture_drain_unconfirmed")
        sampling = []
        for reader in self.sampling:
            left = deadline - time.monotonic()
            _require(left > 0 and callable(getattr(reader, "capture_bounded", None)), "sampling_reader_requires_shared_deadline")
            sampling.append(reader.capture_bounded(contract, phase, total_timeout_s=left))
        _require(time.monotonic() <= deadline, "phase_readback_deadline_exceeded")
        return {"workers_joined": True, "components": components, "database": database, "locks": locks, "sampling": sampling, "issues": issues}

class ReadOnlyDbProbe:
    """Connector 8.4 pure socket timeout; fixed SQL only, no SQL from callers.

    Timeout closes the connection and returns no success evidence. This slice
    does not certify server-side query cancellation; a failed probe blocks the
    run pending a fresh connection/lock audit.
    """
    def __init__(self, *, refs=DbEnvironmentRefs(), execute=False, total_timeout_s=30, socket_timeout_s=15,
                 connector_factory=None, environ=None, source_id="mysql-readonly-baseline", evidence_kind="observed"):
        _require(isinstance(refs, DbEnvironmentRefs) and type(execute) is bool, "explicit_db_policy_required")
        _positive(total_timeout_s); _positive(socket_timeout_s)
        _require(total_timeout_s >= 1, "connector_connect_budget_requires_one_second")
        _require(type(source_id) is str and bool(source_id) and evidence_kind in {"observed", "synthetic"}, "invalid_db_source")
        self.refs, self.execute, self.timeout, self.socket_timeout = refs, execute, total_timeout_s, socket_timeout_s
        self.factory, self.environ, self.source_id, self.evidence_kind = connector_factory, environ, source_id, evidence_kind

    def capture(self, contract, phase, *, total_timeout_s=None):
        _require(isinstance(contract, c.RunContract) and phase in {"pre_fault", "post_recovery"}, "checksum_only_in_pre_or_post")
        _require(self.execute, "database_reads_disabled")
        budget = self.timeout if total_timeout_s is None else min(self.timeout, total_timeout_s)
        _require(type(budget) in (int, float) and math.isfinite(budget) and budget >= 1, "database_capture_budget_invalid")
        environment = os.environ if self.environ is None else self.environ
        try:
            values = {key: environment[ref] for key, ref in self.refs.__dict__.items()}
            _require(all(type(value) is str and value for value in values.values()), "database_environment_missing")
            _require(re.fullmatch(r"[A-Za-z0-9_]+", values["database"]) is not None, "invalid_database_identifier")
            port = int(values["port"])
            _require(0 < port < 65536, "invalid_database_port")
        except (KeyError, ValueError, TypeError):
            raise RuntimeReadError("database_environment_missing_or_invalid") from None
        factory = self.factory
        if factory is None:
            import mysql.connector
            factory = mysql.connector.connect
        start, mono = time.time(), time.monotonic()
        deadline = mono + budget
        connection, cursor, operations = None, None, []
        caught_error = None
        abort, timer = threading.Event(), None
        def abort_socket():
            abort.set()
            sock = getattr(getattr(connection, "_socket", None), "sock", None)
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    sock.close()
                except Exception:
                    pass
        def query(label, sql, parameters=None):
            remaining = deadline - time.monotonic()
            _require(remaining > 0 and not abort.is_set(), "database_total_deadline")
            sock = getattr(getattr(connection, "_socket", None), "sock", None)
            _require(sock is not None and callable(getattr(sock, "settimeout", None)), "pure_connector_socket_timeout_unavailable")
            sock.settimeout(min(self.socket_timeout, remaining))
            before = time.time()
            cursor.execute(sql, parameters)
            rows = cursor.fetchmany(257)
            _require(len(rows) <= 256 and time.monotonic() <= deadline, "database_query_budget_exceeded")
            operations.append({"operation": label, "started_at_epoch_s": before, "finished_at_epoch_s": time.time(), "row_count": len(rows), "return_code": 0})
            return rows
        try:
            connection = factory(host=values["host"], port=port, user=values["user"], password=values["password"],
                                 database=values["database"], use_pure=True, connection_timeout=min(5, max(1, int(budget))))
            _require(time.monotonic() < deadline, "database_connect_deadline")
            timer = threading.Timer(deadline - time.monotonic(), abort_socket)
            timer.start()
            cursor = connection.cursor()
            server = query("server_identity", "SELECT @@server_uuid, @@version, @@performance_schema, CONNECTION_ID()")
            _require(len(server) == 1 and server[0][2] == 1, "performance_schema_unavailable")
            instrument = query("metadata_lock_instrumentation", "SELECT ENABLED,TIMED FROM performance_schema.setup_instruments WHERE NAME='wait/lock/metadata/sql/mdl'")
            _require(instrument == [("YES", "YES")], "metadata_lock_instrumentation_disabled")
            
            # stream keeps a recweb_k8s business session in back-to-back
            # LIKE-count reads on items -- the table carries a nearly
            # CONTINUOUS SHARED_READ metadata lock during the workload (owner
            # captured live: "SELECT COUNT(id) ... title LIKE '%..%'"). Those
            # are concurrent-read locks every business transaction takes; the
            # fault-residual intent of this check targets WRITE-INTENT locks
            # (the db_table_lock fault holds SHARED_NO_READ_WRITE / EXCLUSIVE
            # for its whole window). Classify: shared/intention-shared locks
            # pass and are recorded in the receipt; any exclusive-family lock
            # on the targets, after the bounded re-read below, still fails.
            blocking, benign_locks = [], []
            for attempt in range(3):
                metadata_locks = query("metadata_locks", "SELECT OBJECT_SCHEMA,OBJECT_NAME,LOCK_TYPE,LOCK_STATUS FROM performance_schema.metadata_locks WHERE OBJECT_TYPE='TABLE' AND OBJECT_SCHEMA=%s AND OBJECT_NAME IN ('items','inventory')", (values["database"],))
                data_locks = query("data_locks", "SELECT OBJECT_SCHEMA,OBJECT_NAME,LOCK_TYPE,LOCK_STATUS,LOCK_MODE FROM performance_schema.data_locks WHERE OBJECT_SCHEMA=%s AND OBJECT_NAME IN ('items','inventory')", (values["database"],))
                benign_locks = ([{"kind": "metadata", "table": row[1], "lock_type": row[2], "status": row[3]}
                                 for row in metadata_locks if row[2] in ("SHARED_READ", "SHARED_WRITE")]
                                + [{"kind": "data", "table": row[1], "lock_type": row[2], "status": row[3], "lock_mode": row[4]}
                                   for row in data_locks if row[4] in ("IS", "S", "S,REC_NOT_GAP", "REC_NOT_GAP")])
                blocking = ([{"kind": "metadata", "table": row[1], "lock_type": row[2], "status": row[3]}
                             for row in metadata_locks if row[2] not in ("SHARED_READ", "SHARED_WRITE")]
                            + [{"kind": "data", "table": row[1], "lock_type": row[2], "status": row[3], "lock_mode": row[4]}
                               for row in data_locks if row[4] not in ("IS", "S", "S,REC_NOT_GAP", "REC_NOT_GAP")])
                if not blocking:
                    break
                if attempt < 2 and deadline - time.monotonic() > 0.6:
                    time.sleep(0.5)
            _require(not blocking, "target_database_lock_present")
            database = values["database"]
            checks = query("checksums", "CHECKSUM TABLE `" + database + "`.`items`, `" + database + "`.`inventory`")
            _require(len(checks) == 2 and {row[0] for row in checks} == {database + ".items", database + ".inventory"}
                     and all(type(row[1]) is int and row[1] >= 0 for row in checks), "checksum_reply_invalid")
            context = contract.context.to_dict()
            return {"schema_version": SCHEMA, "contract_sha256": contract.sha256, "run_id": context["run_id"], "attempt_id": context["attempt_id"],
                    "phase": phase, "source_id": self.source_id, "evidence_kind": self.evidence_kind, "return_code": 0,
                    "workers_joined": True,
                    "started_at_epoch_s": start, "timestamp_epoch_s": time.time(), "server_uuid": server[0][0], "server_version": server[0][1],
                    "connection_id": server[0][3], "metadata_lock_instrumentation": "enabled", "target_metadata_locks": [b for b in benign_locks if b["kind"] == "metadata"], "target_data_locks": [b for b in benign_locks if b["kind"] == "data"], "target_benign_locks_note": "shared/intention-shared business locks observed and recorded; exclusive-family would fail",
                    "checksums": {row[0].rsplit(".", 1)[1]: str(row[1]) for row in checks}, "operations": operations,
                    "lock_check_semantics": "two_fresh_queries_before_checksum_not_a_transactional_lock_exclusion"}
        except RuntimeReadError as exc:
            caught_error = exc
            raise
        except Exception:
            caught_error = RuntimeReadError("database_read_failed_connection_closed_reaudit_required")
            raise caught_error from None
        finally:
            cleanup_error = False
            try:
                if cursor is not None:
                    cursor.close()
            except Exception:
                cleanup_error = True
            finally:
                try:
                    if connection is not None:
                        connection.close()
                except Exception:
                    cleanup_error = True
                if timer is not None:
                    timer.cancel(); timer.join(timeout=1)
                    cleanup_error = cleanup_error or timer.is_alive()
            if cleanup_error:
                raise RuntimeReadError("database_cleanup_unconfirmed_reaudit_required", workers_joined=False)
            if caught_error is not None:
                caught_error.workers_joined = True


class ReadonlyOrderNoProbe:
    """M04 (collection.1): resolve one existing stable order_no read-only.

    The order carrier's contract stream is the read-only single-order GET
    ``/api/orders/<order_no>`` (assembly precondition: the driver binding
    supplies and verifies the probe order_no). The panel list endpoint cannot
    serve as the resolution channel -- its ``user_token`` query key is a
    credential and never enters a driver artifact -- so the established DB_*
    environment channel is used with FIXED SQL only: the deterministic oldest
    order_no (ORDER BY order_no ASC LIMIT 1), which is stable across re-reads
    while the data is unchanged and therefore de facto consistent across the
    single/dual/triple arms of a family (each arm re-resolves the same row).
    Server identity anchors the known physical DB. Any failure is fail-closed:
    no order is created, no endpoint is switched, no fallback number is
    invented, and the response evidence is retained for the declaration.
    """

    ORDER_NO_PATTERN = r"[A-Za-z0-9_-]{1,64}"

    def __init__(self, *, execute=False, environ=None, refs=None, expected_server_uuid="",
                 server_identity_query="SELECT @@server_uuid", order_query="SELECT order_no FROM orders ORDER BY order_no ASC LIMIT 1",
                 connect_timeout_s=5.0, query_timeout_s=5.0, source_id="mysql-readonly-order-no",
                 evidence_kind="observed", connector_factory=None):
        _require(type(execute) is bool, "explicit_db_policy_required")
        _require(refs is None or isinstance(refs, DbEnvironmentRefs), "invalid_db_environment_refs")
        _require(type(expected_server_uuid) is str and bool(expected_server_uuid), "expected_server_uuid_required")
        _require(type(server_identity_query) is str and type(order_query) is str
                 and "order_no" in order_query and ";" not in order_query + server_identity_query,
                 "fixed_readonly_sql_required")
        _positive(connect_timeout_s); _positive(query_timeout_s)
        _require(type(source_id) is str and bool(source_id) and evidence_kind in {"observed", "synthetic"},
                 "invalid_order_source")
        self.refs = refs if refs is not None else DbEnvironmentRefs()
        self.execute, self.environ = execute, environ
        self.factory, self.expected_server_uuid = connector_factory, expected_server_uuid
        self.server_identity_query, self.order_query = server_identity_query, order_query
        self.connect_timeout_s, self.query_timeout_s = connect_timeout_s, query_timeout_s
        self.source_id, self.evidence_kind = source_id, evidence_kind

    def resolve(self):
        _require(self.execute, "database_reads_disabled")
        environment = os.environ if self.environ is None else self.environ
        try:
            values = {key: environment[ref] for key, ref in self.refs.__dict__.items()}
            _require(all(type(value) is str and value for value in values.values()), "database_environment_missing")
            port = int(values["port"])
            _require(0 < port < 65536, "invalid_database_port")
        except (KeyError, ValueError, TypeError):
            raise RuntimeReadError("database_environment_missing_or_invalid") from None
        import mysql.connector
        factory = self.factory if self.factory is not None else mysql.connector.connect
        started = time.time()
        connection, cursor = None, None
        try:
            connection = factory(host=values["host"], port=port, user=values["user"], password=values["password"],
                                 database=values["database"], use_pure=True,
                                 connection_timeout=min(5, max(1, int(self.connect_timeout_s))))
            cursor = connection.cursor()
            cursor.execute(self.server_identity_query)
            server = cursor.fetchall()
            _require(len(server) == 1 and server[0][0] == self.expected_server_uuid,
                     "order_probe_server_identity_mismatch")
            cursor.execute(self.order_query)
            rows = cursor.fetchall()
            _require(len(rows) == 1 and type(rows[0][0]) is str
                     and re.fullmatch(self.ORDER_NO_PATTERN, rows[0][0]) is not None,
                     "order_no_row_missing_or_invalid")
            return {"schema_version": SCHEMA, "source_id": self.source_id, "evidence_kind": self.evidence_kind,
                    "return_code": 0, "workers_joined": True, "server_uuid": server[0][0],
                    "order_no": rows[0][0], "selection": "ORDER BY order_no ASC LIMIT 1 (deterministic oldest)",
                    "started_at_epoch_s": started, "timestamp_epoch_s": time.time(),
                    "queries": [self.server_identity_query, self.order_query]}
        except RuntimeReadError:
            raise
        except Exception:
            raise RuntimeReadError("order_no_resolution_failed_fail_closed") from None
        finally:
            cleanup_error = False
            try:
                if cursor is not None:
                    cursor.close()
            except Exception:
                cleanup_error = True
            finally:
                try:
                    if connection is not None:
                        connection.close()
                except Exception:
                    cleanup_error = True
            if cleanup_error:
                raise RuntimeReadError("order_probe_cleanup_unconfirmed") from None


class OwnedLocksProbe:
    """collection: fresh post_recovery confirm-release readback over owned lock sessions.

    The examined ids come only from the caller's ``session_ids`` registry --
    a closure over the db client's own lock-session registration for this
    attempt; fabricated or zero-filled ids are rejected here, never
    produced, and a registry that never registered a session yields no
    packet at all (a condition arm may drop the database leg while reusing
    the parent observation face). The probe reports what the fresh bounded
    confirm_release query saw: the semantic judgments (examined ids
    covering every inject lock_connection_id, active_owned_locks empty)
    belong to the quality consumer, so a still-held lock stays a truthful
    non-empty row instead of a crash. A probe failure itself is fail-closed
    and blocks the run, the same semantics as a failed ReadOnlyDbProbe.
    """

    def __init__(self, client, *, session_ids, tables, source_id,
                 confirm_timeout_s=8.0, evidence_kind="observed", clock=time.time):
        _require(callable(getattr(client, "confirm_release", None)), "locks_client_protocol_required")
        _require(callable(session_ids), "locks_session_registry_required")
        _require(type(tables) is tuple and tables
                 and all(type(name) is str and bool(name) for name in tables)
                 and len(set(tables)) == len(tables), "locks_table_whitelist_required")
        _require(type(source_id) is str and bool(source_id)
                 and evidence_kind in {"observed", "synthetic"}, "invalid_locks_source")
        _require(callable(clock), "locks_clock_required")
        _positive(confirm_timeout_s)
        self.client, self.session_ids = client, session_ids
        self.tables, self.source_id = tables, source_id
        self.confirm_timeout_s, self.evidence_kind, self.clock = confirm_timeout_s, evidence_kind, clock

    def capture_bounded(self, contract, phase, *, total_timeout_s):
        _require(isinstance(contract, c.RunContract) and phase == "post_recovery",
                 "locks_readback_post_recovery_only")
        budget = min(self.confirm_timeout_s, total_timeout_s)
        _require(type(budget) in (int, float) and math.isfinite(budget) and budget >= 1,
                 "locks_capture_budget_invalid")
        registered = self.session_ids()
        _require(type(registered) in (tuple, list)
                 and all(type(value) is int and value > 0 for value in registered),
                 "locks_session_registry_invalid")
        if not registered:
            # No lock session was ever registered through this client (a
            # condition arm that dropped the database leg reuses the parent
            # observation face). Absence is the truthful answer: no packet,
            # nothing fabricated; the quality consumer keeps enforcing the
            # row for every scope that actually injected a lock.
            return None
        ids = tuple(sorted(set(registered)))
        context = contract.context.to_dict()
        started, examined, active = self.clock(), set(), []
        for table in self.tables:
            try:
                confirmation = self.client.confirm_release(table, connection_ids=ids, timeout_s=budget)
            except RuntimeReadError:
                raise
            except BaseException:
                # Reason code only; client payloads/secrets never enter evidence.
                raise RuntimeReadError("locks_confirm_release_failed") from None
            rows = getattr(confirmation, "examined_connection_ids", None)
            holders = getattr(confirmation, "active_owned_locks", None)
            _require(type(rows) in (tuple, list) and all(type(value) is int and value > 0 for value in rows),
                     "locks_confirmation_shape_invalid")
            _require(type(holders) in (tuple, list), "locks_confirmation_shape_invalid")
            examined.update(rows)
            for row in holders:
                _require(type(row) is dict and type(row.get("connection_id")) is int and row["connection_id"] > 0
                         and type(row.get("table")) is str and bool(row["table"])
                         and type(row.get("lock_type")) is str, "locks_holder_shape_invalid")
                active.append(dict(row))
        return {"schema_version": SCHEMA, "contract_sha256": contract.sha256,
                "run_id": context["run_id"], "attempt_id": context["attempt_id"], "phase": "post_recovery",
                "source_id": self.source_id, "evidence_kind": self.evidence_kind, "return_code": 0,
                "workers_joined": True, "owner_id": context["owner_id"],
                "started_at_epoch_s": started, "timestamp_epoch_s": self.clock(),
                "examined_connection_ids": sorted(examined), "active_owned_locks": active}


@dataclass(frozen=True)
class PromTargetReadPolicy(t.HttpReadPolicy):
    """Only the additional read-only target endpoint; reuse T's bounded HTTP."""
    def __post_init__(self):
        _require(type(self.source_id) is str and bool(self.source_id), "explicit_prom_source_required")
        w._origin(self.origin)
        _require(self.allowed_paths == ("/api/v1/targets",), "only_prom_targets_read_supported")
        _positive(self.timeout_s)
        for value in (self.max_response_header_bytes, self.max_response_body_bytes):
            _require(type(value) is int and value > 0, "invalid_http_read_budget")


def _duration_seconds(value):
    _require(type(value) is str, "prom_scrape_interval_missing")
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(ms|s|m)", value)
    _require(match is not None, "prom_scrape_interval_unsupported")
    result = float(match[1]) * {"ms": .001, "s": 1, "m": 60}[match[2]]
    _positive(result)
    return result


@dataclass(frozen=True)
class ExpositionReadPolicy(t.HttpReadPolicy):
    accept_gzip: bool = True
    retain_response_headers: bool = True

    def __post_init__(self):
        _require(type(self.source_id) is str and bool(self.source_id), "exposition_source_required")
        w._origin(self.origin)
        _require(self.allowed_paths == ("/metrics",) and self.accept_gzip is True
                 and self.retain_response_headers is True, "exposition_exact_path_and_headers_required")
        _positive(self.timeout_s)
        _require(type(self.max_response_header_bytes) is int and self.max_response_header_bytes > 0
                 and type(self.max_response_body_bytes) is int and self.max_response_body_bytes > 0,
                 "exposition_response_bounds_required")


class SourceExpositionCapture:
    """Read full independent exporter exposition; retain original compressed bytes."""

    def __init__(self, policy, *, execute=False, max_decoded_bytes, client_factory=t.HttpJsonClient):
        _require(isinstance(policy, ExpositionReadPolicy) and type(execute) is bool
                 and type(max_decoded_bytes) is int and max_decoded_bytes > 0, "explicit_exposition_policy_required")
        self.policy, self.execute, self.max_decoded, self.factory = policy, execute, max_decoded_bytes, client_factory

    def capture_bounded(self, *, total_timeout_s):
        from . import sampling as source_sampling
        _positive(total_timeout_s)
        client = self.factory(replace(self.policy, timeout_s=min(self.policy.timeout_s, total_timeout_s)), execute=self.execute)
        reply = client.fetch_json("/metrics", {})
        _require(isinstance(reply, t.FetchReply), "exposition_reply_required")
        record = {"schema_version": "rq4-collect/source-exposition-r10-v1", "source_id": self.policy.source_id,
                  "origin": self.policy.origin, "path": "/metrics", "status": reply.status,
                  "http_status": reply.provenance.get("http_status"),
                  "started_monotonic_s": reply.provenance.get("started_monotonic_s"),
                  "ended_monotonic_s": reply.provenance.get("ended_monotonic_s"),
                  "response_headers": reply.provenance.get("response_headers", []),
                  "raw_base64": base64.b64encode(reply.raw).decode("ascii"), "raw_bytes": len(reply.raw),
                  "raw_sha256": _sha(reply.raw), "provenance": reply.provenance}
        if reply.status == "ok":
            decoded = source_sampling.decode_body(record, max_decoded_bytes=self.max_decoded)
            record.update(decoded_bytes=len(decoded), decoded_sha256=_sha(decoded), max_decoded_bytes=self.max_decoded)
        return record


@dataclass(frozen=True)
class SamplingPromReadPolicy(t.HttpReadPolicy):
    retain_response_headers: bool = True

    def __post_init__(self):
        _require(type(self.source_id) is str and bool(self.source_id), "sampling_prom_source_required")
        w._origin(self.origin)
        _require(type(self.allowed_paths) is tuple and bool(self.allowed_paths)
                 and set(self.allowed_paths) <= {"/api/v1/query_range", "/api/v1/targets", "/api/v1/status/config"},
                 "sampling_prom_paths_invalid")
        _positive(self.timeout_s)
        _require(type(self.max_response_header_bytes) is int and self.max_response_header_bytes > 0
                 and type(self.max_response_body_bytes) is int and self.max_response_body_bytes > 0,
                 "sampling_prom_bounds_required")


def _sampling_http(client, path, parameters, deadline):
    left = deadline - time.monotonic()
    _require(left > 0 and isinstance(client, t.HttpJsonClient), "sampling_http_budget_or_client_invalid")
    bounded = t.HttpJsonClient(replace(client.policy, timeout_s=min(client.policy.timeout_s, left)), execute=client.execute)
    try:
        reply = bounded.fetch_json(path, parameters)
    except BaseException:
        raise RuntimeReadError("sampling_http_read_raised", workers_joined=False, context={
            "failed_command": "GET " + path, "timeout_s": left}) from None
    if reply.status != "ok":
        raise RuntimeReadError("sampling_http_" + reply.status, workers_joined=True, context={
            "failed_command": "GET " + path, "http_status": reply.provenance.get("http_status"),
            "timeout_s": left})
    return reply



# construction; a DECLARED rollout (app_env inject/recover, gateway ConfigMap
# rollout) replaces the physical source pod mid-run and the pin then names a
# dead pod -- the post_recovery verify honestly fails labels-vs-binding and
# the pinned-label range query targets a dead series. The head snapshot's own
# exposition body is the per-phase fresh view of the same exporter: when it
# carries exactly one series for the bound pod on the pinned label face (all
# labels stable except the pod-identity set -- name and host address, which a
# replacement pod necessarily changes), rebind the record's exporter labels
# to it. Zero/several candidates or any decode failure keeps the pin and the
# honest downstream verdicts; callers record the rebind, never silently.
_POD_IDENTITY_EXPORT_LABELS = frozenset({"k8s_pod_name", "http_host", "net_host_name"})


def _rebound_exporter_labels(head, metric, pinned, max_decoded_bytes):
    from . import sampling as source_sampling
    if type(pinned.get("k8s_pod_name")) is not str or not pinned["k8s_pod_name"]:
        return None
    pod_name = head["source_binding"]["pod_name"]
    if pod_name == pinned["k8s_pod_name"]:
        return None
    try:
        body = source_sampling.decode_body(head["exposition"], max_decoded_bytes=max_decoded_bytes)
        points = source_sampling.exposition_points(body, metric)
    except (ValueError, KeyError, TypeError, IndexError, AttributeError, OverflowError, zlib.error):
        return None
    stable = {key: value for key, value in pinned.items() if key not in _POD_IDENTITY_EXPORT_LABELS}
    candidates = [row["labels"] for row in points
                  if row["labels"].get("k8s_pod_name") == pod_name
                  and set(row["labels"]) == set(pinned)
                  and all(row["labels"].get(key) == value for key, value in stable.items())]
    if len(candidates) != 1:
        return None
    return candidates[0]


class FiniteSourceSnapshotReader:
    """Two bounded real snapshots for the fixed declared business-source scope.

    Docker reads are limited to the metrics Collector identity/config; K8s
    reads use existing pinned target/ancestry readers and a whitelisted process
    environment. No GT determines the input source list.
    """
    def __init__(self, *, kube, targets, source_paths, prom, exposition, docker_executable,
                 collector_name, collector_config_path, prom_job, prom_scrape_url, process=None):
        _require(isinstance(kube, KubectlReadClient) and isinstance(prom, t.HttpJsonClient)
                 and isinstance(prom.policy, SamplingPromReadPolicy) and isinstance(exposition, SourceExpositionCapture),
                 "finite_snapshot_readers_required")
        _require(type(targets) is dict and targets and all(isinstance(target, ComponentReadTarget) for target in targets.values())
                 and set(source_paths) == set(targets), "finite_snapshot_query_targets_required")
        _require(type(collector_name) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", collector_name)
                 and collector_config_path == "/etc/m1/otel-metrics-config.yaml", "collector_scope_not_approved")
        self.kube, self.targets, self.paths, self.prom, self.exposition = kube, targets, source_paths, prom, exposition
        self.docker, self.collector, self.config_path = docker_executable, collector_name, collector_config_path
        self.job, self.scrape_url = prom_job, prom_scrape_url
        self.process = process if process is not None else BoundedProcess(execute=kube.execute)
        self._prelude_cache = None            # P02b: collector identity proof, reader-lifetime
        self._namespace_uid_cache = None      # P02b: namespace uid, reader-lifetime

    def _process(self, argv, deadline):
        left = deadline - time.monotonic()
        if left <= 0:
            raise RuntimeReadError("sampling_snapshot_deadline", workers_joined=True)
        try:
            result = self.process.run(tuple(argv), timeout_s=left, max_output_bytes=2_000_000)
        except BaseException:
            raise RuntimeReadError("sampling_snapshot_process_raised", workers_joined=False, context={
                "failed_command": _safe_argv(argv), "timeout_s": left}) from None
        if result.status != "ok" or result.return_code != 0 or result.workers_joined is not True:
            raise RuntimeReadError("sampling_snapshot_process_failed", workers_joined=result.workers_joined is True,
                                   context={"failed_command": _safe_argv(argv), "return_code": result.return_code,
                                            "read_status": result.status, "timeout_s": left,
                                            "duration_s": round(result.ended_monotonic_s - result.started_monotonic_s, 6)})
        return result

    def _source_kube(self, deadline):
        """Deadline-enforcing per-source read client (P02: one per source).

        Each instance owns its ``reads`` ledger and its mutable ``timeout_s``
        bookkeeping, so concurrent source threads never share mutating state;
        the underlying ``BoundedProcess.run`` is stateless per call (process,
        pipes and reader threads are call-local), which is the stated
        thread-safety basis for the parallel fan-out.
        """
        original = self.kube

        class SnapshotKube(KubectlReadClient):
            def _read(local, args, *, namespace=None):
                left = deadline - time.monotonic()
                if left <= 0:
                    raise RuntimeReadError("sampling_snapshot_deadline", workers_joined=True)
                local.timeout_s = min(original.timeout_s, left)
                return super()._read(args, namespace=namespace)

        return SnapshotKube(original.executable, original.context, original.namespace, process=original.process,
                            execute=original.execute, timeout_s=original.timeout_s, max_output_bytes=original.max_output_bytes)

    @staticmethod
    def _failure_receipt(exc, reads, started):
        """Per-source honest failure receipt; never zero-filled, never a stand-in.

        ``failed_command`` is the argv of the last kubectl read this source
        actually issued (kubectl argv carries no credentials: context,
        namespace and resource args only), truncated element-wise; a failure
        before any read keeps None instead of inventing a command.
        """
        receipt = {"reason": ((exc.args[0] if isinstance(exc, RuntimeReadError) and exc.args else str(exc)) or "")[:200],
                   "workers_joined": exc.workers_joined if isinstance(exc, RuntimeReadError) else True,
                   "duration_s": round(time.time() - started, 6),
                   "failed_command": None, "return_code": None}
        if reads:
            last = reads[-1]
            argv = last.get("argv")
            receipt["failed_command"] = " ".join(str(part)[:80] for part in argv[:16])[:512]\
                if type(argv) is list else None
            receipt["return_code"] = last.get("return_code")
            receipt["read_status"] = last.get("status")
        if isinstance(exc, RuntimeReadError) and exc.context:
            receipt["context"] = dict(exc.context)
        return receipt

    def _capture_one(self, contract, phase, query_id, target, kube, namespace_uid, path, proof, begin, deadline):
        """Serial dep->RS->Pod chain, then runtime-exec ∥ fresh-Pod for ONE source.

        Byte-identical row shape and key order to the pre-P02 loop body; any
        failure raises exactly as the single-source path always did.  P02b
        (collection.1): the runtime exec (docker-desktop spawn + in-container
        python, seconds-level under phase-start contention) and the fresh-pod
        pin are independent once the pod name is known -- they run on two
        bounded clients so the slowest leg no longer eats the other's budget;
        the second client's read receipts merge back for the per-source
        failure receipt.
        """
        from datetime import datetime
        component = PhaseReadbackReader(kube, (target,))._component_stable(contract, phase, target, kube, deadline)
        current = [pod for pod in component["pods"] if pod["deletion_timestamp"] is None]
        _require(len(current) == 1, "sampling_requires_one_unambiguous_source_container")
        pod = current[0]
        kube_b = self._source_kube(deadline)
        holder = {}

        def _runtime_leg():
            try:
                holder["runtime"] = kube.service_runtime(pod["pod_name"], target.container, self.paths[query_id])
            except BaseException as exc:
                holder["runtime_error"] = exc

        def _fresh_leg():
            try:
                holder["fresh"] = kube_b.get("pod", pod["pod_name"])
            except BaseException as exc:
                holder["fresh_error"] = exc

        legs = (threading.Thread(target=_runtime_leg), threading.Thread(target=_fresh_leg))
        for leg in legs:
            leg.start()
        for leg in legs:
            leg.join()
        kube.reads.extend(kube_b.reads)
        if "runtime_error" in holder:
            raise holder["runtime_error"]
        if "fresh_error" in holder:
            raise holder["fresh_error"]
        runtime, fresh = holder["runtime"], holder["fresh"]
        export = runtime.get("env", {}).get("OTEL_METRIC_EXPORT_INTERVAL")
        _require(type(export) is str and export.strip(), "source_runtime_export_interval_not_observed")
        interval = float(export) / 1000
        _positive(interval)
        fresh = kube.get("pod", pod["pod_name"])
        states = [state for state in fresh.get("status", {}).get("containerStatuses", []) if state.get("name") == target.container]
        _require(fresh.get("metadata", {}).get("uid") == pod["pod_uid"] and len(states) == 1
                 and states[0].get("containerID") == pod["container_id"] and states[0].get("imageID") == pod["image_id"],
                 "sampling_source_changed_during_runtime_read")
        started = datetime.fromisoformat(pod["container_started_at"].replace("Z", "+00:00"))
        _require(started.tzinfo is not None, "sampling_container_start_timezone_missing")
        binding = {"entity": target.entity, "namespace": self.kube.namespace, "namespace_uid": namespace_uid,
                   "deployment_uid": target.deployment_uid, "pod_name": pod["pod_name"], "pod_uid": pod["pod_uid"],
                   "container_id": pod["container_id"], "image_id": pod["image_id"],
                   "container_started_at_epoch_s": started.timestamp(), "runtime_source_sha256": runtime["source_sha256"],
                   "runtime_helper_sha256": runtime["helper_sha256"], "process_cmd_sha256": runtime["process_cmd_sha256"]}
        return {"evidence_kind": "observed", "workers_joined": True, "source_binding": binding,
                "timestamp_path": {**path, "configured_export_interval_s": interval},
                "timestamp_path_evidence": proof, "runtime_readback": runtime,
                "component_readback": component, "started_at_epoch_s": begin}

    def _load_prelude(self, deadline):
        """Collector identity proof: docker inspect + config cp (P02b helper)."""
        format_string = '{"id":{{json .Id}},"image_id":{{json .Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}}}'
        inspected = self._process((self.docker, "inspect", "--format", format_string, self.collector), deadline)
        collector = _json(inspected.stdout)
        _require(collector.get("running") is True, "metrics_collector_not_running")
        copied = self._process((self.docker, "cp", self.collector + ":" + self.config_path, "-"), deadline)
        with tarfile.open(fileobj=io.BytesIO(copied.stdout), mode="r:*") as archive:
            files = archive.getmembers()
            _require(len(files) == 1 and files[0].isfile() and files[0].name.rsplit("/", 1)[-1] == "otel-metrics-config.yaml"
                     and files[0].size <= 200_000, "collector_config_archive_not_exact")
            config_raw = archive.extractfile(files[0]).read(200_001)
        return {"collector": collector, "config_raw": config_raw, "tar_sha256": _sha(copied.stdout)}

    def warm(self, *, timeout_s=30.0):
        """Pre-fill the reader-level caches BEFORE any phase clock runs (P02b).

        Pays the collector identity proof and the namespace uid read once at
        observer-construction time, so the first head snapshot spends its
        declared 5 s budget only on the source chains under phase-start
        contention; the prometheus freshness cross-checks still run inside
        every capture, so a changed config refuses there as always.
        """
        _positive(timeout_s)
        deadline = time.monotonic() + timeout_s
        if self._namespace_uid_cache is None:
            namespace = self._source_kube(deadline).get("namespace", self.kube.namespace)
            self._namespace_uid_cache = _uid(namespace, "Namespace", name=self.kube.namespace)
        if self._prelude_cache is None:
            self._prelude_cache = self._load_prelude(deadline)

    def capture_bounded(self, contract, phase, *, total_timeout_s):
        import yaml
        _positive(total_timeout_s)
        begin, deadline = time.time(), time.monotonic() + total_timeout_s
        
        # proof (docker inspect + config cp, seconds-level under phase-start
        # contention) is reader-cached; the two prometheus reads below stay
        # fresh per capture and cross-check the cached config sha against the
        # LIVE active config, so a collector/config change between captures
        # still refuses here.  sampling.evaluate already requires head and
        # tail to carry the SAME timestamp_path, so a per-reader prelude is
        # the consumer-expected semantics, not a freshness relaxation.
        cached = self._prelude_cache
        if cached is None:
            cached = self._prelude_cache = self._load_prelude(deadline)
        collector, config_raw = cached["collector"], cached["config_raw"]
        config = yaml.safe_load(config_raw)
        _require(config.get("exporters", {}).get("prometheus", {}).get("send_timestamps") is True,
                 "collector_explicit_timestamps_disabled")
        config_reply = _sampling_http(self.prom, "/api/v1/status/config", {}, deadline)
        config_payload = _json(config_reply.raw)
        active_text = config_payload["data"]["yaml"]
        active = yaml.safe_load(active_text)
        jobs = [job for job in active.get("scrape_configs", []) if job.get("job_name") == self.job]
        _require(len(jobs) == 1 and not jobs[0].get("metric_relabel_configs") and not jobs[0].get("relabel_configs"),
                 "sampling_job_missing_or_unreviewed_relabeling")
        job = jobs[0]
        _require(job.get("honor_timestamps") is True, "prometheus_timestamp_preservation_disabled")
        targets_reply = _sampling_http(self.prom, "/api/v1/targets", {"state": "active"}, deadline)
        targets_payload = _json(targets_reply.raw)
        matches = [row for row in targets_payload.get("data", {}).get("activeTargets", []) if row.get("scrapeUrl") == self.scrape_url]
        _require(len(matches) == 1 and matches[0].get("health") == "up" and not matches[0].get("lastError"),
                 "sampling_prometheus_target_not_uniquely_healthy")
        path = {"collector_id": collector["id"], "collector_image_id": collector["image_id"],
                "collector_started_at": collector["started_at"], "collector_config_sha256": _sha(config_raw),
                "prom_config_sha256": _sha(active_text.encode()), "send_timestamps": True, "honor_timestamps": True,
                "honor_labels": job.get("honor_labels", False), "target_labels": matches[0]["labels"],
                "configured_scrape_interval_s": _duration_seconds(matches[0]["scrapeInterval"]),
                "exporter_origin": self.exposition.policy.origin, "prom_origin": self.prom.policy.origin}
        proof = {"collector_inspect": collector, "collector_config_base64": base64.b64encode(config_raw).decode(),
                 "collector_config_tar_sha256": cached["tar_sha256"],
                 "prom_config_raw_base64": base64.b64encode(config_reply.raw).decode(),
                 "prom_targets_raw_base64": base64.b64encode(targets_reply.raw).decode(),
                 "prom_job": self.job, "prom_scrape_url": self.scrape_url}
        # Shared cluster identity read: once per reader (P02b: the namespace
        # uid cannot change inside a session; cached after the first capture),
        # outside every source's component receipts exactly as before.
        if self._namespace_uid_cache is None:
            namespace = self._source_kube(deadline).get("namespace", self.kube.namespace)
            self._namespace_uid_cache = _uid(namespace, "Namespace", name=self.kube.namespace)
        namespace_uid = self._namespace_uid_cache
        
        # the SAME absolute deadline; each source keeps the serial
        # dep->RS->Pod->runtime->fresh-Pod validation chain (_capture_one).
        # Single-source runs stay inline and raise on failure -- byte-compatible
        # with the previous behavior. Multi-source runs keep every completed
        # source's snapshot and record a per-source failure receipt (failed
        # command/rc/duration) instead of discarding the whole packet.
        snapshots, failures = {}, {}
        multi = len(self.targets) > 1
        # Different fault queries can bind to the exact same live component
        # and source file (for example T01 F1/F3 both bind to pricing). Their
        # metric labels/range queries remain query-specific in the observer,
        # but this phase-local runtime proof is identical. Capture that proof
        # once under the shared absolute deadline, then bind it to each alias;
        # never reuse it across phase captures or across a different target or
        # source path.
        source_groups = {}
        for query_id, target in self.targets.items():
            source_groups.setdefault((target, self.paths[query_id]), []).append(query_id)

        def run_source(query_ids, target):
            query_id = query_ids[0]
            started = time.time()
            kube = self._source_kube(deadline)
            first = len(kube.reads)
            try:
                snapshot = self._capture_one(contract, phase, query_id, target, kube,
                                             namespace_uid, path, proof, begin, deadline)
                for alias in query_ids:
                    snapshots[alias] = dict(snapshot)
            except RuntimeReadError as exc:
                if not multi:
                    raise
                receipt = self._failure_receipt(exc, kube.reads[first:], started)
                for alias in query_ids:
                    failures[alias] = dict(receipt)
            except (ValueError, KeyError, TypeError) as exc:
                if not multi:
                    raise
                receipt = {"reason": "source_payload_invalid_" + type(exc).__name__,
                           "workers_joined": True, "duration_s": round(time.time() - started, 6),
                           "failed_command": None, "return_code": None}
                for alias in query_ids:
                    failures[alias] = dict(receipt)

        if multi:
            threads = [threading.Thread(target=run_source, args=(tuple(query_ids), self.targets[query_ids[0]]), daemon=True)
                       for query_ids in source_groups.values()]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        else:
            for query_ids in source_groups.values():
                run_source(tuple(query_ids), self.targets[query_ids[0]])
        left = deadline - time.monotonic()
        if not multi:
            if left <= 0:
                raise RuntimeReadError("sampling_snapshot_deadline", workers_joined=True)
            exposition = self.exposition.capture_bounded(total_timeout_s=left)
            end = time.time()
            _require(time.monotonic() <= deadline, "sampling_snapshot_deadline")
            for row in snapshots.values():
                row.update(exposition=exposition, ended_at_epoch_s=end)
            return {"workers_joined": True, "snapshots": snapshots}
        # Multi-source tail: never raise away captured sources. A deadline or
        # exposition failure after the fan-out keeps the successful snapshots
        # (rows without the exposition tail honestly fail downstream evidence
        # checks -- they are never silently completed) and reports the reason.
        if left <= 0:
            result = {"workers_joined": True, "snapshots": snapshots,
                      "reason": "sampling_snapshot_deadline" if snapshots or not failures
                                else "all_snapshot_sources_failed"}
            if failures:
                result["source_failures"] = failures
            return result
        try:
            exposition = self.exposition.capture_bounded(total_timeout_s=left)
            end = time.time()
            _require(time.monotonic() <= deadline, "sampling_snapshot_deadline")
        except RuntimeReadError as exc:
            result = {"workers_joined": True, "snapshots": snapshots, "reason": str(exc)}
            if failures:
                result["source_failures"] = failures
            return result
        for row in snapshots.values():
            row.update(exposition=exposition, ended_at_epoch_s=end)
        result = {"workers_joined": True, "snapshots": snapshots}
        if failures:
            result["source_failures"] = failures
            if not snapshots:
                result["reason"] = "all_snapshot_sources_failed"
        return result


class FiniteSamplingObserver:
    """Synchronous head/tail snapshots and bounded post-phase historical queries.

    Policy numbers are supplied by the pilot plan. This producer never grants
    cadence qualification and starts no independent background workers.
    """
    def __init__(self, snapshot_reader, sources, *, boundary_budget_s, close_budget_s, max_decoded_bytes):
        _require(isinstance(snapshot_reader, FiniteSourceSnapshotReader) and type(sources) is dict and sources
                 and set(sources) == set(snapshot_reader.targets), "finite_sampling_source_set_required")
        for source in sources.values():
            _require(type(source) is dict and set(source) == {"metric", "exporter_labels", "source_id"}
                     and type(source["metric"]) is str and type(source["exporter_labels"]) is dict
                     and source["source_id"] == snapshot_reader.prom.source_id, "finite_sampling_source_definition_invalid")
        _positive(boundary_budget_s); _positive(close_budget_s)
        self.reader, self.sources = snapshot_reader, sources
        self.boundary_budget_s, self.close_budget_s = boundary_budget_s, close_budget_s
        self.max_decoded_bytes = max_decoded_bytes

    def start_phase(self, contract, phase, start_epoch_s, expected_end_epoch_s, clock_mapping):
        session = _FiniteSamplingSession(self, contract, phase, start_epoch_s, expected_end_epoch_s, clock_mapping)
        session.before = session.capture(min(expected_end_epoch_s, start_epoch_s + self.boundary_budget_s),
                                         earliest_epoch_s=start_epoch_s)
        return session


class _FiniteSamplingSession:
    def __init__(self, parent, contract, phase, start, end, clock):
        self.parent, self.contract, self.phase, self.start, self.end, self.clock = parent, contract, phase, start, end, clock
        self.boundary_budget_s = parent.boundary_budget_s
        self.before = self.after = None
        self.drained = True

    def capture(self, deadline_epoch_s, earliest_epoch_s=None):
        
        # anchor while this snapshot stamps wall-clock epochs; a mapped
        # boundary a few milliseconds ahead of time.time() started snapshots
        # before the declared window edge (sampling stage mismatch). Wait for
        # the real epoch boundary instead -- capped, because a skew beyond
        # the cap is an anchor problem to fail honestly, not one to burn the
        # whole budget sleeping through.
        if earliest_epoch_s is not None:
            for _ in range(10):
                skew = earliest_epoch_s - time.time()
                if skew <= 0 or deadline_epoch_s - time.time() <= 0:
                    break
                time.sleep(min(skew, 0.005))
        left = deadline_epoch_s - time.time()
        if left <= 0:
            return {"workers_joined": True, "snapshots": {}, "reason": "snapshot_budget_missed"}
        try:
            result = self.parent.reader.capture_bounded(self.contract, self.phase, total_timeout_s=left)
            _require(result.get("workers_joined") is True, "snapshot_drain_unconfirmed")
            if time.time() > deadline_epoch_s:
                result["reason"] = "snapshot_budget_exceeded"
            return result
        except RuntimeReadError as exc:
            # This concrete reader wraps process/HTTP exceptions with False;
            # other RuntimeReadError values are local validation after returns.
            self.drained = exc.workers_joined is not False
            if not self.drained:
                from .observability import PhaseObservationError
                raise PhaseObservationError("sampling_snapshot_workers_unconfirmed", workers_joined=False) from None
            return {"workers_joined": True, "snapshots": {}, "reason": str(exc)}
        except (ValueError, KeyError, TypeError) as exc:
            return {"workers_joined": True, "snapshots": {}, "reason": "snapshot_payload_invalid_" + type(exc).__name__}

    def observe_boundary(self, actual_window, *, deadline_epoch_s):
        _require(isinstance(actual_window, c.TimeWindow) and actual_window.start == self.start
                 and actual_window.end == deadline_epoch_s <= self.end, "snapshot_boundary_window_mismatch")
        self.after = self.capture(deadline_epoch_s, earliest_epoch_s=deadline_epoch_s - self.boundary_budget_s)

    @staticmethod
    def _http_record(reply, parameters, origin, clock):
        
        # unix-epoch leg is derived from the same run clock anchor the record
        # carries, so the consumer can re-check it within declared uncertainty.
        started, ended = reply.provenance.get("started_monotonic_s"), reply.provenance.get("ended_monotonic_s")
        _require(all(type(value) in (int, float) and math.isfinite(value) for value in (started, ended))
                 and ended >= started, "sampling_query_execution_clock_missing")
        _require(type(clock) is dict and all(type(clock.get(key)) in (int, float) and math.isfinite(clock[key])
                 for key in ("monotonic_s", "unix_epoch_s")), "sampling_query_clock_anchor_missing")
        offset = clock["unix_epoch_s"] - clock["monotonic_s"]
        return {"status": reply.status, "http_status": reply.provenance.get("http_status"), "origin": origin,
                "path": "/api/v1/query_range", "parameters": parameters,
                "started_monotonic_s": started, "ended_monotonic_s": ended,
                "started_unix_epoch_s": offset + started, "ended_unix_epoch_s": offset + ended,
                "response_headers": reply.provenance.get("response_headers", []),
                "raw_base64": base64.b64encode(reply.raw).decode(), "raw_bytes": len(reply.raw), "raw_sha256": _sha(reply.raw)}

    def close(self, actual_window):
        from . import sampling as s
        context, records = self.contract.context.to_dict(), []
        deadline = time.monotonic() + self.parent.close_budget_s
        for query_id, source in self.parent.sources.items():
            # P02: a multi-source packet may carry per-source failure receipts
            # (failed command/rc/duration) beside the surviving snapshots. They
            # ride the record's snapshot_issues so the consumer's honest
            # NOT_ASSESSED keeps the concrete failure detail; keys appear only
            # when a receipt exists, so every failure-free record (all
            # single-source runs included) keeps its exact previous bytes.
            issues = {"head": (self.before or {}).get("reason"),
                      "tail": "boundary_not_observed" if self.after is None else self.after.get("reason")}
            head_failure = (self.before or {}).get("source_failures", {}).get(query_id)
            tail_failure = (self.after or {}).get("source_failures", {}).get(query_id)
            if head_failure is not None:
                issues["head_source_failure"] = head_failure
            if tail_failure is not None:
                issues["tail_source_failure"] = tail_failure
            record = {"schema_version": s.SCHEMA, "method": s.METHOD, "contract_sha256": self.contract.sha256,
                      "run_id": context["run_id"], "attempt_id": context["attempt_id"], "phase": self.phase,
                      "query_id": query_id, "source_id": source["source_id"], "artifact_id": "sampling-" + self.phase + "-" + query_id,
                      "evidence_kind": "observed", "actual_window": actual_window.to_dict(), "clock_mapping": self.clock,
                      "run_clock_anchor_ref": "run-record", "metric": source["metric"], "exporter_labels": source["exporter_labels"],
                      "max_decoded_bytes": self.parent.max_decoded_bytes,
                      "before": (self.before or {}).get("snapshots", {}).get(query_id),
                      "after": (self.after or {}).get("snapshots", {}).get(query_id),
                      "snapshot_issues": issues}
            if record["before"] is not None and record["after"] is not None and not any(record["snapshot_issues"].values()):
                path = record["before"]["timestamp_path"]
                
                # head snapshot's own exporter view when the session pin went
                # stale under a declared replacement; the range query then
                # targets the live series of the bound pod.
                rebound = _rebound_exporter_labels(record["before"], source["metric"],
                                                   source["exporter_labels"], self.parent.max_decoded_bytes)
                if rebound is not None:
                    record["exporter_labels"] = rebound
                    record["exporter_label_rebind"] = {
                        "reason": "session_pinned_source_replaced_by_declared_rollout",
                        "session_pinned_exporter_labels": source["exporter_labels"],
                        "stable_label_keys": sorted(set(source["exporter_labels"]) - _POD_IDENTITY_EXPORT_LABELS)}
                label_face = record["exporter_labels"]
                labels = s.prometheus_labels(label_face, path["target_labels"], path["honor_labels"])
                expression = "timestamp(" + source["metric"] + "{" + ",".join(key + "=" + json.dumps(value) for key, value in sorted(labels.items())) + "})"
                params = {"query": expression, "start": actual_window.start, "end": actual_window.end,
                          "step": self.contract.to_dict()["metric_interval_s"]}
                try:
                    reply = _sampling_http(self.parent.reader.prom, "/api/v1/query_range", params, deadline)
                    record["source_query"] = self._http_record(reply, params, self.parent.reader.prom.policy.origin, self.clock)
                    tail = {**params, "start": actual_window.end}
                    tail_reply = _sampling_http(self.parent.reader.prom, "/api/v1/query_range", tail, deadline)
                    record["right_boundary_query"] = self._http_record(tail_reply, tail, self.parent.reader.prom.policy.origin, self.clock)
                except RuntimeReadError as exc:
                    self.drained = exc.workers_joined is not False
                    if not self.drained:
                        from .observability import PhaseObservationError
                        raise PhaseObservationError("sampling_query_workers_unconfirmed", workers_joined=False) from None
                    record["query_issue"] = str(exc)
            records.append(record)
        return {"workers_joined": self.drained, "records": records}

    def abort(self):
        return {"workers_joined": self.drained, "background_workers_created": False}


class CatalogSamplingCapture:
    """Read actual Pod process/config identity, Prom target and source timestamps.

    Configured export interval and measured point deltas remain separate facts.
    The caller supplies the predeclared timestamp(metric) query, never generated
    interpolation or an evaluation-grid timestamp masquerading as a source.
    """
    def __init__(self, kube, target_backend, point_backend, *, scrape_url, point_expression,
                 source_sha256, helper_sha256, source_id, evidence_kind="observed"):
        _require(isinstance(kube, KubectlReadClient) and callable(getattr(target_backend, "fetch_json", None))
                 and callable(getattr(point_backend, "fetch_json", None)), "explicit_sampling_readers_required")
        _require(type(scrape_url) is str and bool(scrape_url) and type(source_id) is str and bool(source_id), "sampling_identity_required")
        _require(getattr(target_backend, "source_id", None) == source_id and getattr(point_backend, "source_id", None) == source_id,
                 "sampling_backend_source_identity_mismatch")
        _require(type(point_expression) is str and re.fullmatch(r"timestamp\([A-Za-z_:][A-Za-z0-9_:]*(?:\{[^\r\n]*\})?\)", point_expression),
                 "explicit_source_timestamp_expression_required")
        _require(all(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) for value in (source_sha256, helper_sha256)), "reviewed_runtime_hashes_required")
        _require(evidence_kind in {"observed", "synthetic"}, "explicit_sampling_provenance_required")
        self.kube, self.targets, self.points = kube, target_backend, point_backend
        self.scrape_url, self.expression = scrape_url, point_expression
        self.source_sha, self.helper_sha, self.source_id, self.evidence_kind = source_sha256, helper_sha256, source_id, evidence_kind

    def capture(self, contract, phase, pod_name, pod_uid, *, point_window, step_s):
        _require(isinstance(contract, c.RunContract) and phase in {"pre_fault", "during_fault", "post_recovery"}, "sampling_contract_phase_required")
        _require(isinstance(point_window, c.TimeWindow) and point_window.time_basis == "unix_epoch"
                 and point_window.clock_id == contract.context.to_dict()["clock_id"] and point_window.end <= time.time(), "sampling_historical_window_required")
        _positive(step_s)
        context = contract.context.to_dict()
        _require(context["kube_context"] == self.kube.context and context["namespace"] == self.kube.namespace, "sampling_kube_identity_mismatch")
        before = self.kube.get("pod", pod_name)
        _require(_uid(before, "Pod", name=pod_name, namespace=self.kube.namespace) == pod_uid, "sampling_pod_uid_mismatch")
        runtime = self.kube.catalog_runtime(pod_name, "catalog")
        _require(runtime.get("source_sha256") == self.source_sha and runtime.get("helper_sha256") == self.helper_sha
                 and runtime.get("process_id") == 1 and type(runtime.get("otel_sdk_version")) is str, "sampling_runtime_code_unreviewed")
        interval = runtime.get("env", {}).get("OTEL_METRIC_EXPORT_INTERVAL")
        try:
            export_interval = 15. if interval is None else float(interval) / 1000
        except (TypeError, ValueError, OverflowError):
            raise RuntimeReadError("runtime_export_interval_invalid") from None
        _positive(export_interval)
        targets = self.targets.fetch_json("/api/v1/targets", {"state": "active"})
        _require(isinstance(targets, t.FetchReply) and targets.status == "ok", "prom_target_read_failed")
        _require(targets.provenance.get("source_id") == self.source_id, "prom_target_reply_source_mismatch")
        target_data = _json(targets.raw)
        _require(target_data.get("status") == "success", "prom_target_status_failed")
        matches = [item for item in target_data.get("data", {}).get("activeTargets", []) if item.get("scrapeUrl") == self.scrape_url]
        _require(len(matches) == 1 and matches[0].get("health") == "up" and not matches[0].get("lastError"), "prom_target_not_uniquely_healthy")
        scrape_interval = _duration_seconds(matches[0].get("scrapeInterval"))
        parameters = {"query": self.expression, "start": point_window.start, "end": point_window.end, "step": step_s}
        points = self.points.fetch_json("/api/v1/query_range", parameters)
        _require(isinstance(points, t.FetchReply) and points.status == "ok", "source_timestamp_query_failed")
        _require(points.provenance.get("source_id") == self.source_id, "source_timestamp_reply_source_mismatch")
        point_data = _json(points.raw)
        _require(point_data.get("status") == "success" and point_data.get("data", {}).get("resultType") == "matrix", "source_timestamp_reply_invalid")
        measured = []
        for series in point_data["data"].get("result", []):
            values = [float(pair[1]) for pair in series.get("values", [])]
            _require(all(math.isfinite(value) and value >= 0 for value in values), "source_timestamp_nonfinite")
            distinct = sorted(set(values))
            deltas = [round(b - a, 9) for a, b in zip(distinct, distinct[1:])]
            measured.append({"labels": series.get("metric", {}), "distinct_point_timestamps_s": distinct, "source_deltas_s": deltas,
                             "cadence_status": "observed_deltas" if len(deltas) >= 2 else "not_assessed_insufficient_distinct_points"})
        after = self.kube.get("pod", pod_name)
        _require(_uid(after, "Pod", name=pod_name, namespace=self.kube.namespace) == pod_uid
                 and after.get("status", {}).get("containerStatuses") == before.get("status", {}).get("containerStatuses"), "sampling_container_changed_during_read")
        return {"schema_version": "rq4-collect/sampling-readback-v1", "contract_sha256": contract.sha256,
                "run_id": context["run_id"], "attempt_id": context["attempt_id"], "phase": phase, "source_id": self.source_id,
                "evidence_kind": self.evidence_kind, "return_code": 0, "timestamp_epoch_s": time.time(),
                "pod_name": pod_name, "pod_uid": pod_uid, "container_status": after.get("status", {}).get("containerStatuses"),
                "scrape_interval_s": scrape_interval, "export_interval_s": export_interval, "runtime": runtime,
                "prom_target": {key: matches[0].get(key) for key in ("scrapeUrl", "scrapeInterval", "lastScrape", "lastScrapeDuration", "health")},
                "point_query": parameters, "measured_source_series": measured,
                "target_reply_projection": {"received_sha256": _sha(targets.raw), "provenance": targets.provenance,
                                            "projection_fields": "scrapeUrl/scrapeInterval/lastScrape/lastScrapeDuration/health only; unrelated discovery metadata omitted"},
                "raw_point_reply": {"sha256": _sha(points.raw), "json": point_data, "provenance": points.provenance},
                "sampling_semantics": "read_configuration_plus_observed_point_deltas_not_query_grid_freshness",
                "cadence_qualification": "not_assessed_requires_predeclared_delta_and_lag_rule"}


def validate_s08_request(contract: c.RunContract, *, item_id: str, catalog_origin: str) -> dict:
    """Restrict this runtime slice to one explicit direct catalog GET."""
    _require(isinstance(contract, c.RunContract), "validated_run_contract_required")
    _require(type(item_id) is str and re.fullmatch(r"[A-Za-z0-9_-]+", item_id) is not None, "explicit_fixed_item_required")
    w._origin(catalog_origin)
    plan = contract.to_dict()
    _require(plan["purpose"] in {"smoke", "pilot"} and contract.root_entities == ("catalog",) and len(contract.faults) == 1,
             "only_catalog_single_root_smoke_supported")
    fault = contract.faults[0].to_dict()
    _require(fault["atom_id"] == "dependency_latency@catalog" and fault["mechanism"] == "app_env_hook"
             and fault["fault_type"] == "dependency_latency", "only_catalog_latency_supported")
    streams = plan["request_profile"]["streams"]
    _require(len(streams) == 1 and streams[0]["entrypoint"] == catalog_origin and streams[0]["endpoint"] == "/api/items/" + item_id
             and streams[0]["method"] == "GET" and streams[0]["direct_api"] is True
             and streams[0]["parameters"] == {"query": {}, "headers": {}, "json": None}, "only_approved_catalog_item_get_supported")
    return {"status": "validated_not_executed", "contract_sha256": contract.sha256,
            "catalog_origin": catalog_origin, "endpoint": streams[0]["endpoint"], "extra_business_probes": False,
            "runtime_ready": False, "remaining_gates": ["phase_capture_integration", "continuous_uid_log_archive", "recovery_audit", "field_verification"]}
