"""Collection log transport implementation. Runtime identity and source checks remain explicit."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import math
import re
import threading
import time
from typing import Any, Callable

from . import contract as c, journal as j, log_archive as la, telemetry as t

TRANSPORT_SCHEMA = "rq4-collect/log-transport-v1"
VIEW_CONSTRUCTION = "per_line_receipt_prefix_v1"
TIMESTAMP_SEMANTICS = "archive_chunk_receipt_upper_bound_not_container_native"
RECEIPT_ROUNDING = "ceil_to_1us"
_PHASES = ("pre_fault", "during_fault", "post_recovery")
_UNSET = object()  # Snapshot marker: no per-row lifetime field supplied by the archive row


class TransportError(RuntimeError):
    pass


def _require(value, reason):
    if not value:
        raise TransportError(reason)


def _text(value):
    _require(type(value) is str and bool(value) and not any(ch in value for ch in "\r\n\0"), "invalid_transport_text")


def _number(value, *, positive=False):
    _require(type(value) in (int, float) and math.isfinite(value) and (value > 0 if positive else value >= 0),
             "invalid_transport_number")


def _rfc3339(epoch_s: float) -> str:
    """Ceil the receipt to whole microseconds so the stated time stays an upper bound."""
    micros = math.ceil(epoch_s * 1_000_000)
    moment = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=micros)
    return moment.isoformat().replace("+00:00", "Z")


def _chunk_receipt(chunks, index):
    """Receipt epoch of the persisted chunk containing one exact byte offset."""
    for byte_start, byte_end, receipt in chunks:
        if byte_start <= index < byte_end:
            return receipt
    raise TransportError("chunk_receipt_outside_stream")


@dataclass(frozen=True)
class DeploymentLogSource(t.PodLogSource):
    """Deployment-level live log source; rollout replaces Pods, not the Deployment.

    No field is added and no inherited validation is relaxed. Field semantics are
    fixed: pod_name = Deployment name, pod_uid = Deployment UID, container = watched
    container, previous_container = False (a same-UID in-place container restart is
    captured live as an honest per-container-instance stream boundary -- the L2
    archive closes the replaced container's stream and follows the new container
    instance; there is still no post-hoc "--previous" stream in live capture), and
    old_pod_evidence_refs may stay empty because old Pod evidence is captured live
    rather than referenced after deletion. Passing a plain PodLogSource to the live
    backend is refused instead of silently reinterpreted.
    """


@dataclass(frozen=True)
class TransportScope:
    """Pinned identity and budgets for one watched Deployment."""
    source_id: str
    deployment_name: str
    deployment_uid: str
    app: str
    container: str
    cluster_uid: str
    namespace_uid: str
    archive_limits: la.ArchiveLimits
    clock_uncertainty_s: float
    max_view_bytes: int
    evidence_kind: str = "synthetic"

    def __post_init__(self):
        for value in (self.source_id, self.deployment_uid, self.cluster_uid, self.namespace_uid):
            _text(value)
        for value in (self.deployment_name, self.container):
            _require(type(value) is str and re.fullmatch(r"[a-z0-9][a-z0-9.-]*", value) is not None,
                     "invalid_transport_resource_name")
        _require(la.valid_app_label_value(self.app), "invalid_transport_app_label")
        _require(isinstance(self.archive_limits, la.ArchiveLimits), "explicit archive limits required")
        _number(self.clock_uncertainty_s, positive=True)
        _require(type(self.max_view_bytes) is int and self.max_view_bytes > 0, "view byte budget required")
        _require(self.evidence_kind in {"observed", "synthetic"}, "transport evidence kind required")

    def archive_scope(self, context: str, namespace: str) -> la.ArchiveScope:
        return la.ArchiveScope(context, namespace, self.cluster_uid, self.namespace_uid,
                               self.deployment_uid, self.app, self.container)

    def source(self, entity: str) -> DeploymentLogSource:
        return DeploymentLogSource(self.source_id, entity, self.deployment_name, self.deployment_uid,
                                   self.container, False, ())


class _StreamSnapshot:
    """Immutable per-stream bytes plus the chunk receipt map needed for views."""

    def __init__(self, pod_uid, pod_name, container, trailing_partial_line, raw, chunks, pod_entry,
                 lifetime_upper_epoch_s=None, lifetime_upper_basis=None, attachment=None):
        self.pod_uid, self.pod_name, self.container = pod_uid, pod_name, container
        self.trailing_partial_line, self.raw, self.chunks = trailing_partial_line, raw, chunks
        self.pod_entry = pod_entry
        # Per-source lifetime/ancestry (same-UID restarts: one pod, many streams).
        self.lifetime_upper_epoch_s = (pod_entry.get("lifetime_upper_epoch_s")
                                       if lifetime_upper_epoch_s is _UNSET else lifetime_upper_epoch_s)
        self.lifetime_upper_basis = (pod_entry.get("lifetime_upper_basis")
                                     if lifetime_upper_basis is _UNSET else lifetime_upper_basis)
        self.attachment = attachment if attachment is not None else pod_entry.get("attachment")



# sampling head chain; 0.6 s + observed 0.1-0.5 s natural start latency
# stays inside the declared 2.0 s max_capture_start_gap_s policy.
ARCHIVE_START_STAGGER_S = 0.6


class PhaseCaptureSession:
    """One phase's live archives; closed at the phase boundary before backend reads."""

    def __init__(self, owner: "LiveLogCapture", contract: c.RunContract, phase: str, planned_start_epoch_s: float):
        _require(isinstance(contract, c.RunContract) and phase in _PHASES, "validated contract/phase required")
        _number(planned_start_epoch_s)
        self.owner, self.contract, self.phase = owner, contract, phase
        self.planned_start_epoch_s = planned_start_epoch_s
        self.context = contract.context.to_dict()
        self.lock = threading.RLock()
        self.closed = False
        self.finalized = False
        self.window = None
        self.entries = {}  # source_id -> {scope, archive, stop_result, failures[], coverage}
        self.snapshots = {}  # source_id -> tuple[_StreamSnapshot]
        self.stop_clock_uncertainty_s = max(scope.clock_uncertainty_s for scope in owner.scopes.values())

    def _record(self, source_id, reason):
        entry = self.entries.get(source_id)
        if entry is not None and reason not in entry["failures"]:
            entry["failures"].append(reason)

    def start(self):
        """Start every configured archive concurrently; a failure is recorded, never raised into R.

        P02b-logs (collection.1, D02-a3 field evidence): the per-source
        ``archive.start`` (kubectl watch spawn + initial read, seconds-level)
        used to run serially, so source #2's capture began 4.7-6.8 s after the
        phase edge -- physically past any honest declared start-gap policy
        and a real data loss for that source's early phase lines.  One thread
        per source under the same phase-start instant; ``UidLogArchive.start``
        only touches the journal on its failure path (mark_failed), so the
        concurrent starts race nowhere else.  P02b-c stagger: each archive
        start waits ARCHIVE_START_STAGGER_S so the sampling head snapshots
        (whose kubectl chain is the most budget-sensitive consumer of the
        phase-start instant) get the first spawn window; 0.6 s + the observed
        0.1-0.5 s natural start latency stays well inside the declared 2.0 s
        max_capture_start_gap_s policy (D02-a7 field evidence: every extra
        concurrent kubectl spawn at t=0 costs the head chain ~1 s).
        """
        def _start_one(source_id, scope):
            entry = {"scope": scope, "archive": None, "stop_result": None, "failures": [], "coverage": None}
            with self.lock:
                self.entries[source_id] = entry
            time.sleep(ARCHIVE_START_STAGGER_S)
            try:
                archive_scope = scope.archive_scope(self.context["kube_context"], self.context["namespace"])
                archive = la.UidLogArchive(self.owner.journal, archive_scope, scope.archive_limits,
                                           archive_id="logs-" + self.phase + "." + source_id,
                                           clock_uncertainty_s=scope.clock_uncertainty_s,
                                           evidence_kind=scope.evidence_kind)
                entry["start_result"] = archive.start(self.owner.driver_factory(archive_scope), execute=True)
                entry["archive"] = archive
            except BaseException as exc:
                self._record(source_id, "archive_start_failed" if isinstance(exc, la.ArchiveError)
                             else "archive_start_error_" + type(exc).__name__)
        threads = [threading.Thread(target=_start_one, args=(source_id, scope))
                   for source_id, scope in self.owner.scopes.items()]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return self.describe()

    def close(self, window: c.TimeWindow):
        """Stop/join every archive, then snapshot conservatively intersecting streams."""
        _require(isinstance(window, c.TimeWindow) and window.time_basis == "unix_epoch"
                 and window.clock_id == self.context["clock_id"], "explicit contract-clock unix epoch window required")
        with self.lock:
            _require(not self.closed, "phase capture session already closed")
            self.closed = True
            self.window = window
        # Cut every source off before any writer join can block. Otherwise a
        # slow first source lets later sources discover the next phase's Pods.
        for source_id, entry in self.entries.items():
            archive = entry["archive"]
            if archive is not None and archive.started:
                try:
                    archive._request_stop()
                except BaseException as exc:
                    self._record(source_id, "archive_stop_request_error_" + type(exc).__name__)
        for source_id, entry in self.entries.items():
            archive = entry["archive"]
            if archive is None or not archive.started:
                if not entry["failures"]:
                    self._record(source_id, "archive_never_started")
                continue
            try:
                entry["stop_result"] = archive.stop()
            except BaseException as exc:
                self._record(source_id, "archive_stop_failed" if isinstance(exc, la.ArchiveError)
                             else "archive_stop_error_" + type(exc).__name__)
        self._snapshot(window)
        with self.lock:
            self.finalized = True
        return self.describe()

    def abort(self):
        """Idempotent bounded stop for any exit path of R's telemetry worker."""
        with self.lock:
            if self.closed:
                return
            self.closed = True
        for source_id, entry in self.entries.items():
            archive = entry["archive"]
            if archive is None or not archive.started:
                continue
            try:
                entry["stop_result"] = archive.stop()
                self._record(source_id, "archive_aborted_by_worker_exit")
            except BaseException:
                self._record(source_id, "archive_abort_failed")
        with self.lock:
            self.finalized = True

    def _snapshot(self, window: c.TimeWindow):
        for source_id, entry in self.entries.items():
            archive = entry["archive"]
            if archive is None or not archive.started:
                continue
            stop = entry["stop_result"] or {}
            if (stop.get("status") != "CAPTURED_BOUNDED_NOT_EXHAUSTIVE" or archive.failures
                    or archive.persistence_failed or archive.manifest is None):
                self._record(source_id, "live_archive_failed")
                continue
            try:
                lower = archive.started_at["epoch_s"] + archive.uncertainty
                upper = min(window.end, archive.stop_request["epoch_s"] - archive.uncertainty)
                start = max(window.start, lower)
                _require(start < upper, "empty_capture_window")
                rows = archive.phase_sources(c.TimeWindow(start, upper, "unix_epoch", window.clock_id))
                entry["coverage"] = {"window": window.to_dict(), "capture_lower_bound_epoch_s": lower,
                                     "capture_upper_bound_epoch_s": upper,
                                     "pre_phase_capture_gap_s": max(0., archive.started_at["epoch_s"] - window.start),
                                     "phase_planned_start_epoch_s": self.planned_start_epoch_s}
                self.snapshots[source_id] = tuple(
                    _StreamSnapshot(row["pod_uid"], archive.pods[row["pod_uid"]]["first"]["name"],
                                    row["container"], row["trailing_partial_line"], row["raw"],
                                    tuple((chunk["byte_start"], chunk["byte_end"], chunk["received_epoch_s"])
                                          for chunk in archive.chunks[row["source_id"]]),
                                    archive.pods[row["pod_uid"]],
                                    lifetime_upper_epoch_s=row.get("lifetime_upper_epoch_s", _UNSET),
                                    lifetime_upper_basis=row.get("lifetime_upper_basis", _UNSET),
                                    attachment=archive.pods[row["pod_uid"]]["attachments"].get(row["source_id"]))
                    for row in rows)
            except BaseException as exc:
                self._record(source_id, "phase_source_failed" if isinstance(exc, la.ArchiveError)
                             else "phase_source_error_" + type(exc).__name__)

    def build_view(self, source_id: str, max_view_bytes: int):
        """Deterministic derived T-facing raw: receipt-prefixed lines, whole streams.

        The stored L2 chunk artifacts keep the exact original bytes; this view only
        adds an explicit receive-time upper-bound prefix per line (see DESIGN §4).
        """
        streams = self.snapshots.get(source_id) or ()
        pieces, rows, total = [], [], 0
        truncated = False
        for stream in streams:
            # A 0-byte stream (silent-by-design carrier, e.g. the S05 stressor's
            # ``sleep infinity``) has zero lines; b"".split(b"\n") would
            # fabricate one empty line whose receipt lookup then fails on the
            # empty chunk list (S05 attempt-1: view_assembly_error_
            # TransportError escalated to "independent telemetry worker
            # failed" and blocked the whole attempt).
            raw_lines = stream.raw.split(b"\n") if stream.raw else []
            if stream.raw.endswith(b"\n"):
                raw_lines = raw_lines[:-1]
            included, byte_start_view, line_count = b"", None, 0
            cursor = 0
            for content in raw_lines:
                content_end = cursor + len(content)
                receipt = _chunk_receipt(stream.chunks, max(0, content_end - 1))
                line = _rfc3339(receipt).encode("ascii") + b" " + content + b"\n"
                if total + len(line) > max_view_bytes:
                    truncated = True
                    break
                pieces.append(line)
                if byte_start_view is None:
                    byte_start_view = total
                total += len(line)
                line_count += 1
                cursor = content_end + 1
            rows.append({"pod_uid": stream.pod_uid, "pod_name": stream.pod_name,
                         "container_name": stream.container["name"], "container_id": stream.container["container_id"],
                         "restart_count": stream.container["restart_count"],
                         "stream_sha256": hashlib.sha256(stream.raw).hexdigest(),
                         "stream_bytes": len(stream.raw), "line_count": line_count,
                         "trailing_partial_line": bool(stream.trailing_partial_line),
                         "view_byte_start": byte_start_view, "view_byte_end": None if byte_start_view is None else total,
                         "excluded_by_view_truncation": truncated and byte_start_view is None,
                         "lifetime_lower_epoch_s": stream.pod_entry["first"]["creation_epoch_s"],
                         "lifetime_upper_epoch_s": stream.lifetime_upper_epoch_s,
                         "lifetime_upper_basis": stream.lifetime_upper_basis,
                         "attachment_completed_epoch_s": (stream.attachment or {}).get("completed_epoch_s"),
                         "bytes_before_follow_not_retained": True,
                         "ancestry_read_artifacts": _ancestry_refs(stream.attachment)})
            if truncated:
                break
        return b"".join(pieces), rows, truncated

    def archive_provenance(self, source_id: str):
        entry = self.entries[source_id]
        archive, stop = entry["archive"], entry["stop_result"] or {}
        result = {"backend": TRANSPORT_SCHEMA, "phase": self.phase,
                  "archive_status": stop.get("status"), "archive_failures": list(stop.get("failures") or []),
                  "transport_failures": list(entry["failures"]),
                  "coverage": entry["coverage"], "stream_count": len(self.snapshots.get(source_id) or ()),
                  "backend_exhaustiveness": "not_claimed"}
        if archive is not None and archive.started:
            result.update(archive_id=archive.archive_id, clock_uncertainty_s=archive.uncertainty,
                          capture_started_epoch_s=archive.started_at["epoch_s"],
                          stop_requested_epoch_s=(archive.stop_request or {}).get("epoch_s"),
                          artifact_count=len(stop.get("artifacts") or ()),
                          manifest_artifact=next((d for d in stop.get("artifacts") or ()
                                                  if d["artifact_id"] == archive.archive_id + ".manifest"), None))
        return result

    def describe(self):
        with self.lock:
            return {"schema_version": TRANSPORT_SCHEMA, "phase": self.phase, "closed": self.closed,
                    "finalized": self.finalized,
                    "window": None if self.window is None else self.window.to_dict(),
                    "sources": {source_id: {"failures": list(entry["failures"]),
                                            "streams": len(self.snapshots.get(source_id) or ())}
                                for source_id, entry in self.entries.items()}}


def _ancestry_refs(attachment):
    attachment = attachment or {}
    refs = {}
    for key in ("before_read", "ancestor_read", "after_read"):
        ref = attachment.get(key)
        if ref is not None:
            refs[key] = ref["artifact_id"]
    return refs


class LiveLogCapture:
    """Single-attempt controller: R phase lifecycle driver plus T log backend.

    Wire one instance as both TelemetryInputs.log_backend and TelemetryInputs.log_capture.
    It is bound to the actual AttemptJournal by run_attempt, starts one L2 archive per
    configured Deployment at each phase start, and serves fetch_logs only from fully
    closed per-phase snapshots. No Kubernetes transport is implemented or defaulted.
    """

    def __init__(self, scopes: dict[str, TransportScope], driver_factory: Callable[[la.ArchiveScope], Any]):
        _require(type(scopes) is dict and bool(scopes)
                 and all(type(key) is str and isinstance(value, TransportScope) for key, value in scopes.items()),
                 "explicit transport scopes required")
        _require(len({value.deployment_uid for value in scopes.values()}) == len(scopes)
                 and all(value.source_id == key for key, value in scopes.items()), "scope identity mismatch")
        _require(callable(driver_factory), "explicit driver factory required")
        self.scopes = dict(scopes)
        self.driver_factory = driver_factory
        self.lock = threading.RLock()
        self.journal = None
        self.sessions: dict[str, PhaseCaptureSession] = {}

    def bind_journal(self, journal: j.AttemptJournal):
        _require(isinstance(journal, j.AttemptJournal), "actual attempt journal required")
        with self.lock:
            _require(self.journal is None and not self.sessions, "capture controller already bound to an attempt")
            self.journal = journal
            context = journal.contract.context.to_dict()
            for scope in self.scopes.values():
                archive_scope = scope.archive_scope(context["kube_context"], context["namespace"])
                _require(archive_scope.namespace == context["namespace"], "transport namespace mismatch")

    def start_phase(self, contract: c.RunContract, phase: str, planned_start_epoch_s: float) -> PhaseCaptureSession:
        with self.lock:
            _require(self.journal is not None, "capture controller has no bound attempt journal")
            _require(self.journal.contract.sha256 == contract.sha256, "capture contract differs from bound attempt")
            _require(phase not in self.sessions, "phase capture session already exists")
            session = PhaseCaptureSession(self, contract, phase, planned_start_epoch_s)
            self.sessions[phase] = session
        session.start()
        return session

    def fetch_logs(self, scope: t.PhaseScope, source: t.PodLogSource) -> t.FetchReply:
        provenance = {"backend": TRANSPORT_SCHEMA, "timestamp_semantics": TIMESTAMP_SEMANTICS,
                      "receipt_epoch_rounding": RECEIPT_ROUNDING,
                      "view_construction": VIEW_CONSTRUCTION, "source_id": source.source_id,
                      "pod_uid": None, "phase": scope.phase, "issues": [], "capture": None, "streams": []}

        def reply(status, raw=b""):
            return t.FetchReply(status, raw, provenance)

        if not isinstance(source, DeploymentLogSource):
            provenance["issues"].append("fixed_pod_source_unsupported_by_live_backend")
            return reply("transport_error")
        if source.previous_container:
            provenance["issues"].append("previous_container_unsupported_by_live_capture")
            return reply("transport_error")
        with self.lock:
            session = self.sessions.get(scope.phase)
            journal = self.journal
        context = scope.to_dict()
        if session is None or not session.closed:
            provenance["issues"].append("capture_session_missing_or_open")
            return reply("not_run")
        if not session.finalized:
            provenance["issues"].append("capture_session_still_closing")
            return reply("not_run")
        if session.window is None:
            provenance["issues"].append("capture_session_aborted_without_window")
            return reply("transport_error")
        if journal is None or session.context["run_id"] != context["run_id"] or session.context["attempt_id"] != context["attempt_id"]:
            provenance["issues"].append("capture_session_attempt_mismatch")
            return reply("identity_mismatch")
        if session.window is None or session.window.to_dict() != scope.actual_window.to_dict():
            provenance["issues"].append("capture_window_mismatch")
            return reply("identity_mismatch")
        transport_scope = self.scopes.get(source.source_id)
        if transport_scope is None or source.source_id != transport_scope.source_id:
            provenance["issues"].append("source_not_configured_for_live_capture")
            return reply("identity_mismatch")
        if (source.pod_name, source.pod_uid, source.container) != (transport_scope.deployment_name,
                                                                   transport_scope.deployment_uid, transport_scope.container):
            provenance["issues"].append("deployment_identity_mismatch")
            return reply("identity_mismatch")
        if (context["kube_context"], context["namespace"]) != (session.context["kube_context"], session.context["namespace"]):
            provenance["issues"].append("scope_context_mismatch")
            return reply("identity_mismatch")
        provenance["pod_uid"] = source.pod_uid  # Echoed only after the archive pinned the same Deployment UID.
        provenance["identity"] = {"deployment_uid": transport_scope.deployment_uid,
                                  "deployment_name": transport_scope.deployment_name,
                                  "container": transport_scope.container, "app": transport_scope.app,
                                  "context": session.context["kube_context"], "namespace": session.context["namespace"]}
        provenance["capture"] = session.archive_provenance(source.source_id)
        if session.entries[source.source_id]["failures"] or source.source_id not in session.snapshots:
            provenance["issues"].append("live_archive_failed")
            return reply("transport_error")
        provenance["uid_verification"] = "deployment_pinned_ancestry_per_stream"
        provenance["output_byte_limit"] = transport_scope.max_view_bytes
        try:
            raw, rows, truncated = session.build_view(source.source_id, transport_scope.max_view_bytes)
        except BaseException as exc:
            provenance["issues"].append("view_assembly_error_" + type(exc).__name__)
            return reply("transport_error")
        provenance["streams"] = rows
        if truncated:
            provenance["issues"].append("view_truncated_byte_limit")
            return reply("response_limit", raw)
        return reply("ok", raw)
