"""Durable UID/container-bound log archive with an explicit bounded driver.

No default driver, Kubernetes transport, or Q3 approval is supplied here.
Identity bytes are whitelisted projections; application log bytes are exact.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
import queue
import re
import threading
import time
import traceback
import uuid

from . import contract as c, journal as j

SCHEMA = "rq4-collect/log-archive-v1"
PROJECTION = "identity_projection_v1"
_STALL_DEADLINE_OWNERS = {
    "archive_coordinator": {"driver_read_blocked", "driver_follow_blocked", "driver_stop_blocked", "driver_join_blocked"},
    "persistence_writer": {"journal_write_blocked"},
    "archive_finalizer": set(),
    "unclassified": set(),
}


class ArchiveError(RuntimeError):
    pass


def _require(value, reason):
    if not value:
        raise ArchiveError(reason)


def _number(value, *, positive=False):
    _require(type(value) in (int, float) and math.isfinite(value) and (value > 0 if positive else value >= 0), "invalid_number")


def _text(value):
    _require(type(value) is str and bool(value) and not any(ch in value for ch in "\r\n\0"), "invalid_identity_text")


def _json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _parse(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, "duplicate_projection_field")
            result[key] = value
        return result
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda _: (_ for _ in ()).throw(ArchiveError("nonfinite_projection")))
    except (ValueError, UnicodeError, TypeError):
        raise ArchiveError("invalid_projection_json") from None


def _fields(value, keys):
    _require(type(value) is dict and set(value) == set(keys.split()), "projection_fields_outside_whitelist")


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def valid_app_label_value(value):
    """Kubernetes label-value syntax, retaining this scope's nonempty app pin."""
    return (type(value) is str and 1 <= len(value) <= 63
            and re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?", value) is not None)


@dataclass(frozen=True)
class ArchiveScope:
    context: str
    namespace: str
    cluster_uid: str
    namespace_uid: str
    deployment_uid: str
    app: str
    container: str

    def __post_init__(self):
        for value in self.__dict__.values():
            _text(value)
        for value in (self.namespace, self.container):
            _require(re.fullmatch(r"[a-z0-9][a-z0-9.-]*", value) is not None, "invalid_scope_name")
        _require(valid_app_label_value(self.app), "invalid_scope_app_label")


@dataclass(frozen=True)
class ArchiveLimits:
    max_total_bytes: int
    max_read_bytes: int
    max_chunk_bytes: int
    max_buffered_bytes: int
    max_events: int
    max_streams: int
    attach_timeout_s: float
    eof_grace_s: float
    flush_timeout_s: float
    capture_timeout_s: float
    join_timeout_s: float
    durable_flush_timeout_s: float | None = None

    def __post_init__(self):
        if self.durable_flush_timeout_s is None:
            object.__setattr__(self, "durable_flush_timeout_s", self.flush_timeout_s)
        for key, value in self.__dict__.items():
            _number(value, positive=True)
            if key.startswith("max_"):
                _require(type(value) is int, "integer_archive_budget_required")
        _require(self.max_chunk_bytes <= self.max_buffered_bytes <= self.max_total_bytes
                 and self.max_read_bytes <= self.max_total_bytes, "archive_budget_order_invalid")


@dataclass(frozen=True)
class ProjectionRead:
    raw: bytes
    started_epoch_s: float
    ended_epoch_s: float
    started_monotonic_s: float
    ended_monotonic_s: float
    source_response_sha256: str | None = None

    def __post_init__(self):
        _require(type(self.raw) is bytes, "projection_bytes_required")
        for key in ("started_epoch_s", "ended_epoch_s", "started_monotonic_s", "ended_monotonic_s"):
            _number(getattr(self, key))
        _require(self.ended_epoch_s >= self.started_epoch_s and self.ended_monotonic_s >= self.started_monotonic_s, "projection_clock_reversed")
        _require(self.source_response_sha256 is None or re.fullmatch(r"[0-9a-f]{64}", self.source_response_sha256), "invalid_source_hash")


def _pod(value, scope):
    _fields(value, "namespace name uid resource_version app creation_epoch_s deletion_requested_epoch_s owners container")
    for key in ("namespace", "name", "uid", "resource_version", "app"):
        _text(value[key])
    _require(value["namespace"] == scope.namespace and value["app"] == scope.app, "pod_scope_mismatch")
    _number(value["creation_epoch_s"])
    if value["deletion_requested_epoch_s"] is not None:
        _number(value["deletion_requested_epoch_s"])
    _require(type(value["owners"]) is list, "pod_owner_projection_missing")
    for owner in value["owners"]:
        _fields(owner, "kind name uid controller")
        for key in ("kind", "name", "uid"):
            _text(owner[key])
        _require(type(owner["controller"]) is bool, "owner_controller_flag_invalid")
    controllers = [owner for owner in value["owners"] if owner["controller"]]
    _require(len(controllers) == 1 and controllers[0]["kind"] == "ReplicaSet", "pod_controller_not_replicaset")
    container = value["container"]
    _fields(container, "name container_id image_id restart_count started_epoch_s finished_epoch_s")
    _require(container["name"] == scope.container and type(container["restart_count"]) is int and container["restart_count"] >= 0,
             "container_identity_invalid")
    for key in ("container_id", "image_id"):
        if container[key] is not None:
            _text(container[key])
    for key in ("started_epoch_s", "finished_epoch_s"):
        if container[key] is not None:
            _number(container[key])
    _require(container["started_epoch_s"] is None or container["started_epoch_s"] >= value["creation_epoch_s"], "container_start_before_pod_creation")
    _require(container["finished_epoch_s"] is None or (container["started_epoch_s"] is not None
             and container["finished_epoch_s"] >= container["started_epoch_s"]), "container_finish_before_start")
    _require(value["deletion_requested_epoch_s"] is None or value["deletion_requested_epoch_s"] >= value["creation_epoch_s"], "pod_deletion_before_creation")
    return value


def _physical(pod):
    row = pod["container"]
    return (pod["uid"], row["name"], row["container_id"], row["restart_count"], row["image_id"], row["started_epoch_s"])


def _copy_container(container):
    return {key: value for key, value in container.items()}


class UidLogArchive:
    """One exclusive archive namespace in an existing actual AttemptJournal.

    Driver calls must honor their time budgets. If a custom driver or a disk
    write does not return, stop reports unconfirmed workers rather than claiming
    cleanup or releasing a surrounding environment lease.

    Throughput (2026-09-19 S07 lesson): pod-stream pieces received in bursts are
    coalesced into batch artifacts before persistence. Every emitted driver
    piece still becomes exactly one chunk record with its own byte bounds and
    receipt times; consecutive pieces of one flush share a single journal
    artifact, and each record's ``raw`` descriptor carries ``byte_offset``/
    ``bytes`` to slice its bytes out of that shared artifact. A batch is forced
    out when it reaches max_chunk_bytes, 16 pieces, an eighth of the flush
    budget in age, or when the worker has caught up with the queue; pieces held
    longer than the whole flush budget still fail the archive honestly.

    Same-UID container restarts (2026-09-19 S03 lesson): an in-place container
    replacement observed through the watch is recorded as an honest stream
    boundary. The replaced container's stream closes with that boundary as its
    lifecycle evidence, a new stream is attached for the new physical container
    identity (own chunk chain, own ancestry reads), and the boundary evidence
    (restart_count/container_id pair and observation times) is kept in the pod
    entry. Missing evidence for a stream end still fails the archive.

    Attempt-7 hardening (2026-09-19 collection): watch resource_versions are monotone
    per Pod (a regressed rv is a forged/stale replay and fails loudly); a
    live-observed deletion request closes stream lifecycle questions without
    forcing a stop-read of an already-gone Pod; non-ArchiveError failure codes
    keep the exception type name; under a loaded queue the coalescer defers to
    count/byte caps (up to a hard age bound inside the flush budget) so queue
    consumption is never throttled to write speed and watch items cannot starve.

    S16 hardening (2026-09-19 collection): a Windows transient file-create failure used
    to poison the journal after the artifact intent landed and desync the
    byte-offset ledger. Every artifact write is now preceded by a create/delete
    probe with exactly one bounded retry (FileNotFoundError/PermissionError
    only, evidence counted; persistently failing trees still fail honestly and
    the journal stays usable); a batch whose artifact write failed stays
    accounted in ``failed_bytes`` so later pieces never trip a bogus offset gap
    and finalize still reports accepted > persisted; full tracebacks of
    non-ArchiveError failures are kept in ``failure_details``.

    S18 stall-deadline decoupling (2026-09-19 collection): the attempt journal's writer
    mutex serializes this archive against the fault control plane -- runner
    ``execute_change``/``reconcile`` hold it across chaos apply/restore RPCs
    and readiness waits (field-measured 37-54s during pod-failure recover).
    The persistence worker can wait inside ``journal.write_artifact`` while the
    coordinator continues consuming watch events and attaching Pods. A
    probe-confirmed journal-mutex wait pauses only that writer's flush clock;
    it cannot extend coordinator attachment, watch-partial-frame, or EOF
    deadlines. Coordinator driver read/follow/stop/join waits pause only those
    coordinator clocks. Uncontended slow journal writes remain genuine
    write-path delay and still breach honestly. Every window lands in
    ``stall_windows`` with an explicit deadline owner and both clock bases,
    so real elapsed time stays fully reconstructible.

    collection rollout-collision semantics (2026-09-21): a during_fault archive starts
    concurrently with the injected mechanism's Deployment rollout
    (app_env_hook / nginx_configmap_rollout), which replaces the watched Pods
    inside the assembly-declared switch band (GW 14-28s live, env same
    magnitude). Two fixes, both declared-source driven and fail-closed:

    * Budgets: the initialization wait adds the declared switch band
      (assembly rollout-band decision records) during_fault and post_recovery.
      Per-Pod attachment deadlines add the band only during_fault, when the
      rollout can replace a watched Pod. T01 anchor-3 evidence (2026-09-23)
      showed three concurrent archives' post-phase initial reads at ~20 s
      against the bare 15 s budget after a three-rollout during phase; that
      buys initialization headroom only. The post-recovery per-Pod deadline
      stays calibrated at 15 s so the extra startup wait cannot silently
      extend log coverage. The band comes from assembly profiles of mechanisms
      in the contract, never a magic number; all per-call deadlines and
      scientific thresholds stay unchanged.
    * Gap, not poison: a during_fault Pod whose stream is missing because it
      was mechanism-replaced -- provable by the ReplicaSet->Deployment
      ancestry read plus live successor/deletion evidence -- is recorded as
      an honest ``GAP_POD_REPLACED`` row (old and new Pod identities, the
      ancestry read artifacts) in ``pod_gaps`` and the manifest, and the run
      continues; coverage judges the missing stream. Anything unprovable
      (no declaration, other phases, no successor and no deletion evidence,
      validation ArchiveErrors, persistence failures) keeps the
      fail-closed ``mark_failed`` poisoning exactly as before. The journal
      guard itself is untouched.
    """
    COALESCE_MAX_PIECES = 16

    def __init__(self, journal: j.AttemptJournal, scope: ArchiveScope, limits: ArchiveLimits, *, archive_id: str,
                 clock_uncertainty_s: float, evidence_kind="synthetic"):
        _require(isinstance(journal, j.AttemptJournal) and isinstance(scope, ArchiveScope) and isinstance(limits, ArchiveLimits), "explicit_journal_scope_limits_required")
        _require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", archive_id or "") is not None, "invalid_archive_id")
        context = journal.contract.context.to_dict()
        _require(context["kube_context"] == scope.context and context["namespace"] == scope.namespace, "archive_contract_scope_mismatch")
        _number(clock_uncertainty_s, positive=True)
        clock_capabilities = {name: vars(time.get_clock_info(name)) for name in ("monotonic", "time")}
        _require(clock_uncertainty_s >= sum(row["resolution"] for row in clock_capabilities.values()),
                 "declared_uncertainty_below_host_clock_resolution")
        _require(evidence_kind in {"observed", "synthetic"}, "archive_evidence_kind_required")
        self.journal, self.scope, self.limits = journal, scope, limits
        self.archive_id, self.uncertainty, self.evidence_kind = archive_id, clock_uncertainty_s, evidence_kind
        self.clock_capabilities = clock_capabilities
        self.identity = {"contract_sha256": journal.contract.sha256, "run_id": context["run_id"], "attempt_id": context["attempt_id"]}
        self.lock, self.ready = threading.RLock(), threading.Event()
        self.queue = queue.Queue(maxsize=limits.max_events + limits.max_streams * 4 + 8)
        # Keep API/watch processing independent from the shared journal mutex.
        # Include the maximum pending byte count because watch output may arrive
        # as many small callback chunks while one journal write is blocked.
        self.persist_queue = queue.Queue(maxsize=limits.max_events * 2 + limits.max_buffered_bytes
                                         + limits.max_streams * 16 + 64)
        self.persist_worker = None
        self.persist_stop_enqueued = False
        self.persist_drained = False
        self.persist_expected, self.persist_results, self.persist_receipt_times = {}, {}, {}
        self.persist_enqueued_monotonic = {}
        self.failures, self.descriptors, self.streams, self.pods, self.events, self.reads = [], [], {}, {}, [], []
        self.handles, self.chunks = {}, {"watch": []}
        self.accepted, self.persisted, self.pending, self.total = {"watch": 0}, {"watch": 0}, 0, 0
        self.scheduled = {"watch": 0}
        self.accepting, self.closing, self.started = False, False, False
        self.worker, self.driver, self.manifest = None, None, None
        self.watch_buffer, self.watch_offset = bytearray(), 0
        self.watch_pending_chunks, self.watch_pending_events = [], []
        self.stop_request = self.cutoff = None
        self.joins, self.ends, self.end_received = {}, {}, {}
        self.initial_ref, self.initial_rv, self.started_at = None, None, None
        self.persistence_failed = False
        self.callback_clocks = {}
        self.piece_buffers = {}  # source -> [pending pieces], flushed into batch artifacts
        self.coalesce_window_s = min(1.0, limits.flush_timeout_s / 8.)
        self.read_counter = 0  # monotonic read numbering: a failed read must not reissue its artifact ids
        self.failed_bytes = {}  # source -> bytes of batches whose artifact write failed (honest terminal state)
        self.failure_details = []  # full tracebacks behind non-ArchiveError failure codes
        self.write_probe_retries = 0  # pre-intent transient-create retries actually taken
        
        # call (journal writer mutex held by the fault control plane, or an
        # over-long driver RPC). Below stall_floor_s a call is normal jitter.
        self.stall_windows = []
        self.stall_floor_s = min(1.0, limits.flush_timeout_s / 8.)
        
        # log_transport archive_id convention "logs-<phase>.<source_id>";
        # None for every other archive_id, which keeps the fail-closed
        # behavior), the assembly-declared switch band of the mechanisms
        # targeting this Deployment, and the honest replacement-gap ledger.
        self.phase = self._phase_from_archive_id(archive_id)
        self.switch_band_s = self._declared_switch_band_s()
        self.pod_gaps = []

    def _phase_from_archive_id(self, archive_id):
        """Phase label carried by the log_transport archive_id, else None.

        The transport names per-phase archives ``logs-<phase>.<source_id>``;
        any other shape (every unit-test archive_id) yields None and none of
        the collection semantics apply.
        """
        if not archive_id.startswith("logs-"):
            return None
        phase = archive_id[len("logs-"):].split(".", 1)[0]
        return phase if phase in ("pre_fault", "during_fault", "post_recovery") else None

    def _declared_switch_band_s(self):
        """Declared mechanism switch band (seconds) for this attempt.

        Single source of the numbers: the assembly rollout-band decision
        records. The band is declared for during_fault and post_recovery
        archives when the contract targets a banded rollout mechanism; other
        phases and unbanded attempts return 0.0. It extends initialization in
        both phases, but only extends per-Pod attachment during during_fault.

        Scoping history: collection originally matched the rollout's own Deployment
        (watched-app selector). D07-a1 field evidence (2026-09-21,
        journal seq 141, ``archive_initialization_deadline``) contradicted the
        locality assumption on this single-node cluster: the inventory env
        rollout's API churn stretched a NON-targeted archive's (catalog-gw)
        initial-read pod delivery to 12.9 s against a steady 15 s budget --
        deployment-pod replacement is local, API-server churn is cluster-wide.
        The ready-wait is a maximum, so an uncontended archive still completes
        init in seconds; only genuinely stretched initializations consume the
        declared band.
        """
        # during_fault: the active rollout can stretch initialization and
        # per-Pod attachment. post_recovery: only the INITIALIZATION wait
        # rides the band because the completed rollouts can leave API churn.
        if self.phase not in ("during_fault", "post_recovery"):
            return 0.
        try:
            from . import scenario_definitions as asm
            bands = {
                # env-rollout family (S07 decision record)
                "app_env_hook": asm.S07_ROLLOUT_PROFILES[asm.S07_DEFAULT_ROLLOUT_PROFILE].rollout_band_s,
                # gateway ConfigMap-rollout family (S23/S24 decision record)
                "nginx_configmap_rollout": asm.GATEWAY_ROLLOUT_PROFILES[asm.GATEWAY_DEFAULT_ROLLOUT_PROFILE].rollout_band_s,
            }
        except BaseException:
            return 0.
        band = 0.
        for fault in self.journal.contract.faults:
            spec = fault.to_dict()
            mechanism = spec.get("mechanism")
            if mechanism in bands:
                band = max(band, float(bands[mechanism]))
        return band

    @property
    def _attach_budget_s(self):
        """Per-Pod attachment deadline, with a band only during active rollout.

        During_fault rollouts may delay attaching a replacement Pod. In
        post_recovery, the rollout band extends only the archive initialization
        wait; this per-Pod coverage deadline remains at its calibrated value.
        """
        band = self.switch_band_s if self.phase == "during_fault" else 0.
        return self.limits.attach_timeout_s + band

    @property
    def _initialization_wait_budget_s(self):
        """Initial read/watch/first-Pod startup envelope, including aftermath."""
        return self.limits.attach_timeout_s + self.switch_band_s

    def _clock(self, epoch, mono, *, source=None, lower_mono=None, upper_mono=None):
        _number(epoch); _number(mono)
        now_mono, now_epoch = time.monotonic(), time.time()
        anchor = self.started_at
        _require(anchor is not None, "archive_clock_anchor_missing")
        low = anchor["monotonic_s"] if lower_mono is None else lower_mono
        high = now_mono if upper_mono is None else upper_mono
        _require(low - self.uncertainty <= mono <= high + self.uncertainty, "observation_monotonic_outside_actual_call")
        _require(abs(epoch - (anchor["epoch_s"] + mono - anchor["monotonic_s"])) <= self.uncertainty
                 and abs(now_epoch - (anchor["epoch_s"] + now_mono - anchor["monotonic_s"])) <= self.uncertainty,
                 "observation_epoch_monotonic_mapping_mismatch")
        if source is not None:
            previous = self.callback_clocks.get(source)
            _require(previous is None or (mono >= previous[0] and epoch >= previous[1]), "stream_receipt_clock_reversed")
            self.callback_clocks[source] = (mono, epoch)

    def _fail(self, reason):
        with self.lock:
            if reason not in self.failures:
                self.failures.append(reason)
            self._request_stop()
        if self.manifest is not None and self.worker is not None and not self.worker.is_alive():
            try:
                self.journal.mark_failed("log_archive_late_protocol_failure")
            except BaseException:
                self.persistence_failed = True

    def _record_failure_detail(self, prefix, exc):
        """Keep the full traceback of a non-ArchiveError failure (S16 lesson:
        the typed twin code alone still left the diagnosis guessing)."""
        detail = {"code": self._failure_code(prefix, exc), "exception_type": type(exc).__name__,
                  "traceback": traceback.format_exc(), "recorded_at_utc": datetime.now(timezone.utc).isoformat()}
        with self.lock:
            self.failure_details.append(detail)
        return detail["code"]

    @staticmethod
    def _failure_code(prefix, exc):
        # Preserve the exception type name: a generalized code loses the detail
        
        code = prefix + "_" + type(exc).__name__
        return code if code.isidentifier() else prefix

    def _epoch_of(self, mono):
        return self.started_at["epoch_s"] + mono - self.started_at["monotonic_s"]

    def _record_stall(self, kind, began_mono, ended_mono, *, deadline_owner=None, **detail):
        """Ledger one interval the worker spent blocked inside an external call.

        collection (S18 lesson): rows are evidence, not failures -- the manifest keeps
        every piece's real receipt/flush clocks, so a reviewer can always
        reconstruct both real elapsed and deadline-counted time. Intervals at or
        below ``stall_floor_s`` are normal jitter and are never ledgered.
        """
        if ended_mono - began_mono <= self.stall_floor_s:
            return None
        owner = deadline_owner or next((name for name, kinds in _STALL_DEADLINE_OWNERS.items() if kind in kinds), "unclassified")
        _require(owner in _STALL_DEADLINE_OWNERS, "stall_deadline_owner_invalid")
        row = {**detail, "kind": kind, "deadline_owner": owner,
               "began_monotonic_s": began_mono, "ended_monotonic_s": ended_mono,
               "duration_s": ended_mono - began_mono,
               "began_epoch_s": self._epoch_of(began_mono), "ended_epoch_s": self._epoch_of(ended_mono)}
        with self.lock:
            self.stall_windows.append(row)
        return row

    def _suspended_s(self, since_mono, now_mono, *, deadline_owner="archive_coordinator"):
        """Elapsed stall time excluded for the named worker's deadline clock.

        Coordinator waits and asynchronous journal-writer waits are disjoint:
        one must never pause the other's deadlines.
        """
        _require(deadline_owner in _STALL_DEADLINE_OWNERS, "deadline_clock_owner_invalid")
        total = 0.
        for row in self.stall_windows:
            if row.get("deadline_owner") != deadline_owner:
                continue
            overlap = min(row["ended_monotonic_s"], now_mono) - max(row["began_monotonic_s"], since_mono)
            if overlap > 0.:
                total += overlap
        return min(total, max(0., now_mono - since_mono))

    def _deadline_age_s(self, since_mono, now_mono=None, *, deadline_owner="archive_coordinator"):
        now_mono = time.monotonic() if now_mono is None else now_mono
        return max(0., (now_mono - since_mono)
                   - self._suspended_s(since_mono, now_mono, deadline_owner=deadline_owner))

    def _flush_deadline_age_s(self, received_mono, enqueued_mono, committed_mono):
        """Measure receipt-to-durable-flush age with owner-specific clock segments."""
        _require(type(received_mono) in (int, float) and type(enqueued_mono) in (int, float)
                 and type(committed_mono) in (int, float)
                 and received_mono <= enqueued_mono <= committed_mono,
                 "archive_flush_clock_order_invalid")
        before_enqueue = self._deadline_age_s(received_mono, enqueued_mono,
                                              deadline_owner="archive_coordinator")
        after_enqueue = self._deadline_age_s(enqueued_mono, committed_mono,
                                             deadline_owner="persistence_writer")
        return before_enqueue + after_enqueue

    def _journal_mutex_contended(self):
        """True iff another thread currently holds the journal writer mutex.

        The runner's execute_change/reconcile hold that mutex across chaos
        apply/restore RPCs and readiness waits; a write blocked behind such a
        holder is not this archive's write-path delay. journal.py is a frozen
        anchor this campaign -- if its internal mutex is ever renamed or the
        attribute disappears, the probe reports False and slow writes keep
        failing honestly (never a silent waiver).
        """
        mutex = getattr(self.journal, "_mutex", None)
        acquire = getattr(mutex, "acquire", None)
        if mutex is None or acquire is None:
            return False
        try:
            taken = acquire(blocking=False)
        except TypeError:
            return False
        if taken:
            mutex.release()
            return False
        return True

    def _request_stop(self):
        with self.lock:
            if not self.closing:
                self.closing = True
                self.stop_request = {"epoch_s": time.time(), "monotonic_s": time.monotonic()}

    def _reserve(self, size, *, queued=False):
        with self.lock:
            _require(size >= 0 and self.total + size <= self.limits.max_total_bytes, "archive_total_byte_limit")
            if queued:
                _require(self.pending + size <= self.limits.max_buffered_bytes, "archive_buffer_full")
                self.pending += size
            self.total += size

    def _probe_artifact_tree(self):
        """Pre-intent create/delete probe with exactly one bounded retry.

        S16 lesson (2026-09-19): a Windows transient (filter-driver/Defender
        create race) inside journal.write_artifact's file creation poisons the
        whole journal AFTER the artifact intent was appended, killing the run's
        remaining evidence writes. Probing the same directory BEFORE any journal
        intent absorbs the transient class where a retry is still harmless:
        only FileNotFoundError/PermissionError, at most one retry, evidence
        recorded, and a persistently failing tree still fails honestly without
        masking. This never retries a write whose journal intent already
        landed -- after journal.write_artifact is entered there is no retry.
        """
        directory = self.journal.path / "artifacts/log-archive" / self.archive_id
        directory.mkdir(parents=True, exist_ok=True)  # same parents write_artifact would create
        probe = directory / (".write-probe-" + uuid.uuid4().hex[:12])
        for attempt in (0, 1):
            try:
                fd = os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
                os.close(fd)
                try:
                    os.unlink(probe)
                except OSError:
                    pass  # Best effort; a leftover probe dot-file carries no data.
                return
            except (FileNotFoundError, PermissionError):
                if attempt == 0:
                    with self.lock:
                        self.write_probe_retries += 1
                    time.sleep(.02)
        raise FileNotFoundError(str(probe))

    def _persistence_job_failed(self, job, exc=None):
        with self.lock:
            self.persistence_failed = True
            stream_id, byte_count = job.get("stream_id"), job.get("queued_bytes", 0)
            if stream_id is not None and byte_count:
                self.failed_bytes[stream_id] = self.failed_bytes.get(stream_id, 0) + byte_count
                self.pending -= byte_count
                _require(self.pending >= 0, "archive_pending_bytes_underflow")
        if exc is not None:
            self._record_failure_detail("archive_artifact_write_failed", exc)
        self._fail("archive_artifact_write_failed")

    def _persist_run(self):
        """Serialize journal writes away from the watch/attach coordinator.

        Artifact bytes remain bounded by max_total_bytes and queued stream bytes
        remain charged to max_buffered_bytes until their journal receipt has
        been verified. A poisoned or unconfirmed write fails the archive and
        causes the remaining queue to be drained without further journal calls.
        """
        while True:
            job = self.persist_queue.get()
            try:
                if job is None:
                    return
                if self.persistence_failed:
                    self._persistence_job_failed(job)
                    continue
                contended, began = False, time.monotonic()
                try:
                    
                    # journal mutex. Archive capture no longer waits here.
                    contended = self._journal_mutex_contended()
                    began = time.monotonic()
                    receipt = self.journal.write_artifact(
                        artifact_id=job["artifact_id"], relative_path=job["relative_path"], raw=job["raw"],
                        media_type=job["media_type"])
                    ended = time.monotonic()
                    if contended:
                        self._record_stall("journal_write_blocked", began, ended,
                                           artifact_id=job["artifact_id"])
                    descriptor = {key: receipt[key] for key in ("artifact_id", "relative_path", "sha256")}
                    _require(descriptor == job["expected"], "archive_artifact_receipt_mismatch")
                    stream_id, end_offset = job.get("stream_id"), job.get("stream_end_offset")
                    byte_count = job.get("queued_bytes", 0)
                    with self.lock:
                        if stream_id is not None:
                            _require(type(end_offset) is int and byte_count > 0
                                     and end_offset - self.persisted.get(stream_id, 0) == byte_count,
                                     "archive_persisted_stream_offset_mismatch")
                            self.persisted[stream_id] = end_offset
                            self.pending -= byte_count
                            _require(self.pending >= 0, "archive_pending_bytes_underflow")
                        self.persist_results[job["artifact_id"]] = descriptor
                        self.persist_receipt_times[job["artifact_id"]] = ended
                        self.descriptors.append(descriptor)
                    if (job.get("received_monotonic_s") is not None
                            and self._flush_deadline_age_s(job["received_monotonic_s"],
                                                           job["writer_enqueued_monotonic_s"], ended)
                            > self.limits.durable_flush_timeout_s):
                        self._fail("archive_flush_deadline_exceeded")
                except BaseException as exc:
                    if contended and began <= time.monotonic():
                        self._record_stall("journal_write_blocked", began, time.monotonic(),
                                           artifact_id=job["artifact_id"])
                    self._persistence_job_failed(job, exc)
            finally:
                self.persist_queue.task_done()

    def _write(self, suffix, raw, *, binary=False, received_mono=None,
               stream_id=None, stream_end_offset=None, queued_bytes=0):
        aid = self.archive_id + "." + suffix
        path = "artifacts/log-archive/" + self.archive_id + "/" + suffix + (".bin" if binary else ".json")
        try:
            _require(type(raw) is bytes and type(queued_bytes) is int and queued_bytes >= 0,
                     "archive_artifact_write_input_invalid")
            expected = {"artifact_id": aid, "relative_path": path, "sha256": _sha(raw)}
            _require((stream_id is None and stream_end_offset is None and queued_bytes == 0)
                     or (type(stream_id) is str and bool(stream_id) and type(stream_end_offset) is int
                         and queued_bytes > 0 and queued_bytes <= len(raw)), "archive_stream_write_binding_invalid")
            _require(self.persist_worker is not None and self.persist_worker.is_alive()
                     and not self.persist_stop_enqueued and not self.persistence_failed,
                     "archive_artifact_writer_unavailable")
            self._probe_artifact_tree()
            job = {"artifact_id": aid, "relative_path": path, "raw": raw,
                   "media_type": "application/octet-stream" if binary else "application/json",
                   "expected": expected, "stream_id": stream_id,
                   "stream_end_offset": stream_end_offset, "queued_bytes": queued_bytes,
                   "received_monotonic_s": received_mono,
                   "writer_enqueued_monotonic_s": None}
            with self.lock:
                _require(aid not in self.persist_expected, "archive_artifact_id_duplicate")
                self.persist_expected[aid] = expected
                try:
                    enqueued = time.monotonic()
                    job["writer_enqueued_monotonic_s"] = enqueued
                    self.persist_enqueued_monotonic[aid] = enqueued
                    self.persist_queue.put_nowait(job)
                except queue.Full:
                    self.persist_expected.pop(aid, None)
                    self.persist_enqueued_monotonic.pop(aid, None)
                    raise ArchiveError("archive_artifact_writer_queue_full") from None
            return expected
        except BaseException as exc:
            self.persistence_failed = True
            self._record_failure_detail("archive_artifact_write_failed", exc)
            self._fail("archive_artifact_write_failed")
            raise

    def _stop_persistence_writer(self):
        worker = self.persist_worker
        if worker is None or worker.ident is None:
            return False
        deadline = time.monotonic() + self.limits.join_timeout_s
        if not self.persist_stop_enqueued:
            try:
                self.persist_queue.put(None, timeout=max(.001, deadline - time.monotonic()))
                self.persist_stop_enqueued = True
            except queue.Full:
                self._fail("archive_artifact_writer_queue_close_timeout")
                return False
        worker.join(timeout=max(.001, deadline - time.monotonic()))
        if worker.is_alive():
            self._fail("archive_artifact_writer_join_unconfirmed")
            return False
        self.persist_drained = (self.persist_queue.unfinished_tasks == 0
                                and set(self.persist_expected) == set(self.persist_results)
                                and all(self.persist_results[key] == value
                                        for key, value in self.persist_expected.items()))
        if not self.persist_drained:
            self._fail("archive_artifact_receipts_unreconciled")
        return self.persist_drained

    def _wait_persistence_queue(self):
        deadline = time.monotonic() + self.limits.join_timeout_s
        while self.persist_queue.unfinished_tasks and time.monotonic() < deadline:
            if self.persist_worker is None or not self.persist_worker.is_alive():
                self._fail("archive_artifact_writer_stopped_with_pending_jobs")
                return False
            time.sleep(.01)
        if self.persist_queue.unfinished_tasks:
            self._fail("archive_artifact_writer_drain_timeout")
            return False
        return True

    def _write_manifest(self, raw):
        aid = self.archive_id + ".manifest"
        path = "artifacts/log-archive/" + self.archive_id + "/manifest.json"
        try:
            self._probe_artifact_tree()
            contended = self._journal_mutex_contended()
            began = time.monotonic()
            receipt = self.journal.write_artifact(artifact_id=aid, relative_path=path, raw=raw,
                media_type="application/json")
            ended = time.monotonic()
            if contended:
                self._record_stall("journal_write_blocked", began, ended,
                                   deadline_owner="archive_finalizer", artifact_id=aid)
            descriptor = {key: receipt[key] for key in ("artifact_id", "relative_path", "sha256")}
            _require(descriptor == {"artifact_id": aid, "relative_path": path, "sha256": _sha(raw)},
                     "archive_manifest_receipt_mismatch")
            self.descriptors.append(descriptor)
        except BaseException as exc:
            self.persistence_failed = True
            self._record_failure_detail("archive_artifact_write_failed", exc)
            self._fail("archive_artifact_write_failed")
            raise

    def _read(self, kind, *args):
        began = time.monotonic()
        reply = getattr(self.driver, kind)(*args, timeout_s=self.limits.attach_timeout_s)
        returned = time.monotonic()
        
        # RPC (kubectl stall) starves the queue exactly like a journal block.
        # The projection_read_deadline check below still uses raw wall time: a
        # driver that outruns its own budget is a genuine driver failure.
        self._record_stall("driver_read_blocked", began, returned, read_kind=kind)
        _require(isinstance(reply, ProjectionRead) and len(reply.raw) <= self.limits.max_read_bytes, "projection_read_invalid_or_oversized")
        _require(time.monotonic() - began <= self.limits.attach_timeout_s
                 and reply.ended_monotonic_s - reply.started_monotonic_s <= self.limits.attach_timeout_s, "projection_read_deadline")
        self._clock(reply.started_epoch_s, reply.started_monotonic_s, lower_mono=began, upper_mono=returned)
        self._clock(reply.ended_epoch_s, reply.ended_monotonic_s, lower_mono=began, upper_mono=returned)
        value = _parse(reply.raw)
        # Validate the whitelist before persisting bytes which could otherwise
        # contain full Pod environment/secret data.
        if kind == "initial":
            _fields(value, "cluster_uid namespace_uid resource_version pods")
            _require(value["cluster_uid"] == self.scope.cluster_uid and value["namespace_uid"] == self.scope.namespace_uid, "initial_scope_uid_mismatch")
            _text(value["resource_version"])
            _require(type(value["pods"]) is list and len(value["pods"]) <= self.limits.max_streams, "initial_pod_inventory_limit")
            _require(len({pod.get("uid") for pod in value["pods"]}) == len(value["pods"]), "initial_pod_uid_repeated")
            for pod in value["pods"]:
                _pod(pod, self.scope)
        elif kind == "pod":
            _pod(value, self.scope)
            _require(value["name"] == args[0], "pod_read_name_mismatch")
        elif kind == "replicaset":
            _fields(value, "namespace name uid resource_version deployment_uid controller")
            _require(value["namespace"] == self.scope.namespace and value["name"] == args[0]
                     and value["deployment_uid"] == self.scope.deployment_uid and value["controller"] is True, "replicaset_ancestry_mismatch")
            for key in ("uid", "resource_version"):
                _text(value[key])
        self._reserve(len(reply.raw))
        number = self.read_counter
        self.read_counter += 1
        raw_ref = self._write("read-%06d.raw" % number, reply.raw, binary=True)
        metadata = {**self.identity, "projection_format": PROJECTION, "kind": kind, "raw": raw_ref,
                    **{key: value for key, value in vars(reply).items() if key != "raw"}}
        ref = self._write("read-%06d" % number, _json_bytes(metadata))
        self.reads.append({"artifact": ref, **metadata})
        return value, ref

    def _bytes(self, source, raw, received_epoch_s, received_monotonic_s):
        try:
            _require(type(raw) is bytes and len(raw) <= self.limits.max_chunk_bytes, "archive_chunk_limit")
            _number(received_epoch_s); _number(received_monotonic_s)
            if not raw:
                return
            with self.lock:
                _require(self.accepting and source not in self.joins and source not in self.end_received, "callback_after_end_join_or_cutoff")
                self._clock(received_epoch_s, received_monotonic_s, source=source)
                self._reserve(len(raw), queued=True)
                start = self.accepted.get(source, 0)
                self.accepted[source] = start + len(raw)
                self.queue.put_nowait(("bytes", source, raw, received_epoch_s, received_monotonic_s, start))
        except BaseException as exc:
            self._fail(str(exc) if isinstance(exc, ArchiveError) else "archive_callback_queue_failed")

    def _end(self, source, termination, return_code, received_epoch_s, received_monotonic_s):
        try:
            _require(termination in {"natural", "cancelled", "error", "timeout", "response_limit"} and type(return_code) is int, "stream_end_invalid")
            _number(received_epoch_s); _number(received_monotonic_s)
            with self.lock:
                _require(self.accepting and source not in self.joins and source not in self.end_received, "end_after_join_or_duplicate")
                self._clock(received_epoch_s, received_monotonic_s, source=source)
                self.end_received[source] = received_monotonic_s
                self.queue.put_nowait(("end", source, termination, return_code, received_epoch_s, received_monotonic_s, self.closing))
        except BaseException as exc:
            self._fail(str(exc) if isinstance(exc, ArchiveError) else "archive_end_queue_failed")

    def _callbacks(self, source):
        return (lambda raw, epoch, mono: self._bytes(source, raw, epoch, mono),
                lambda termination, rc, epoch, mono: self._end(source, termination, rc, epoch, mono))

    def start(self, driver, *, execute=False):
        _require(type(execute) is bool and not self.started, "archive_start_once_explicit_boolean")
        _require(getattr(driver, "projection_format", None) == PROJECTION
                 and getattr(driver, "scope", None) == self.scope and all(callable(getattr(driver, name, None)) for name in
                     ("initial", "pod", "replicaset", "watch", "follow")), "explicit_scoped_projection_driver_required")
        if not execute:
            return {"schema_version": SCHEMA, "status": "validated_not_executed", **self.identity}
        self.driver, self.started, self.accepting = driver, True, True
        before, epoch = time.monotonic(), time.time()
        after = time.monotonic()
        self.started_at = {"epoch_s": epoch, "monotonic_s": (before + after) / 2, "anchor_read_span_s": after - before}
        try:
            _require((after - before) / 2 <= self.uncertainty, "host_clock_anchor_budget_exceeded")
            self.persist_worker = threading.Thread(target=self._persist_run, name="m1-log-archive-persist", daemon=True)
            self.persist_worker.start()
            self.worker = threading.Thread(target=self._run, name="m1-log-archive", daemon=True)
            self.worker.start()
        except BaseException:
            self.accepting = False; self._fail("archive_worker_start_failed")
            try:
                self._stop_persistence_writer()
            except BaseException:
                self.persistence_failed = True
            try:
                self.journal.mark_failed("log_archive_worker_start_failed")
            except BaseException:
                self.persistence_failed = True
            raise ArchiveError("archive_worker_start_failed") from None
        
        # declared band, including post-recovery API contention. The band does
        # not alter the per-Pod attachment deadline outside during_fault.
        if not self.ready.wait(self._initialization_wait_budget_s):
            self._fail("archive_initialization_deadline")
            return self.stop()
        return {"schema_version": SCHEMA, "status": "BLOCKED" if self.failures else "RUNNING", **self.identity}

    def _observe(self, pod, received_epoch, received_mono, *, deleted=False):
        _pod(pod, self.scope)
        uid = pod["uid"]
        if uid not in self.pods:
            _require(len(self.pods) < self.limits.max_streams, "archive_stream_inventory_limit")
            self.pods[uid] = {"first": pod, "latest": pod, "first_received_epoch_s": received_epoch,
                              "first_received_monotonic_s": received_mono, "deleted_received_epoch_s": None,
                              "deletion_requested_received_epoch_s": None,
                              "source_id": None, "attachment": None,
                              "source_ids": [], "attachments": {}, "container_restarts": [],
                              "attach_anchor_monotonic_s": received_mono,
                              "last_resource_version": pod["resource_version"]}
        entry = self.pods[uid]
        _require(all(pod[key] == entry["first"][key] for key in ("creation_epoch_s", "namespace", "name", "owners")), "pod_creation_or_owner_identity_changed")
        previous_rv, current_rv = entry["last_resource_version"], pod["resource_version"]
        # Watch events for one object are ordered by etcd revision: a regressed
        # rv is a forged/stale replay that must not walk the state backwards
        # (review MINOR-1: A->B->A replay otherwise records a second boundary).
        _require(not (previous_rv is not None and previous_rv.isdigit() and current_rv.isdigit()
                      and int(current_rv) < int(previous_rv)), "pod_resource_version_regressed")
        entry["last_resource_version"] = current_rv
        if pod["deletion_requested_epoch_s"] is not None and entry["deletion_requested_received_epoch_s"] is None:
            
            # attempt-7 pre_fault lesson: MODIFIED(deleting) without a DELETED
            # event before close must not force a stop-read of a gone Pod).
            entry["deletion_requested_received_epoch_s"] = received_epoch
        if entry["source_id"] is not None and _physical(pod) != _physical(entry["latest"]):
            previous, current = _physical(entry["latest"]), _physical(pod)
            # A real in-place replacement always changes the container instance
            # (container_id and/or started time). A restart_count or image_id
            # change without a new container instance is contradictory data.
            _require(previous[2] != current[2] or previous[5] != current[5], "container_restarted_or_replaced")
            # Same-UID in-place container replacement (PodChaos pod-failure shape):
            # an honest stream boundary, not a continuity violation. The replaced
            # container's stream closes with this boundary as lifecycle evidence
            # and a new stream attaches to the new physical identity.
            entry["container_restarts"].append({
                "from_physical": list(previous), "to_physical": list(current),
                "observed_epoch_s": received_epoch, "observed_monotonic_s": received_mono,
                "resource_version": pod["resource_version"]})
            entry["attach_anchor_monotonic_s"] = received_mono
        entry["latest"] = pod
        if deleted:
            entry["deleted_received_epoch_s"] = received_epoch
            if entry["source_id"] is None and entry.get("gap") is None:
                
                # ever attached it is a provable replacement gap during
                # during_fault; without proof it stays the honest poison.
                if not self._record_replacement_gap(uid, "pod_deleted_before_attachment"):
                    self._fail("pod_deleted_before_attachment")

    def _attach(self, uid):
        entry = self.pods[uid]
        pod = entry["latest"]
        if entry["deleted_received_epoch_s"] is not None or self.closing:
            return
        if entry.get("gap") is not None:
            return  
        if not all(pod["container"][key] is not None for key in ("container_id", "image_id", "started_epoch_s")):
            return
        current = list(_physical(pod))
        if any(attachment.get("physical_identity") == current for attachment in entry["attachments"].values()):
            return  # This physical container instance already has its live stream.
        _require(self._deadline_age_s(entry["attach_anchor_monotonic_s"], deadline_owner="archive_coordinator")
                 <= self._attach_budget_s, "pod_attachment_deadline")
        
        # this Pod. A driver-level read failure (Pod object gone) is gap-or-
        # poison: with a provable replacement the gap ledger records it and
        # the run continues; our own validation ArchiveErrors and persistence
        # failures re-raise unchanged (fail-closed).
        try:
            before, before_ref = self._read("pod", pod["name"])
            _require(_physical(before) == _physical(pod), "pod_changed_before_attachment")
            owner = next(row for row in before["owners"] if row["controller"])
            ancestor, ancestor_ref = self._read("replicaset", owner["name"])
            _require(ancestor["uid"] == owner["uid"], "replicaset_owner_uid_mismatch")
        except BaseException as exc:
            if isinstance(exc, ArchiveError) or self.persistence_failed:
                raise
            if self._record_replacement_gap(uid, "pod_attach_read_failed", exception_type=type(exc).__name__):
                return
            raise
        source = "pod-" + _sha(_json_bytes(_physical(before)))[:24]
        if source in self.streams:
            return  # This physical container instance already has its stream.
        _require(len(self.streams) < self.limits.max_streams, "archive_stream_inventory_limit")
        # A replacement container's bytes cannot predate its own start; the
        # first attach still follows from the archive start as before.
        replacement = bool(entry["source_ids"])
        since = self.started_at["epoch_s"]
        if replacement and pod["container"]["started_epoch_s"] is not None:
            since = max(since, pod["container"]["started_epoch_s"])
        entry["source_id"] = source
        entry["source_ids"].append(source)
        self.streams[source] = uid
        self.chunks[source], self.accepted[source], self.persisted[source] = [], 0, 0
        self.scheduled[source] = 0
        on_bytes, on_end = self._callbacks(source)
        began_follow = time.monotonic()
        handle = self.driver.follow(before, since_epoch_s=since, on_bytes=on_bytes,
                                    on_end=on_end, timeout_s=self.limits.capture_timeout_s)
        self._record_stall("driver_follow_blocked", began_follow, time.monotonic(), source_id=source)
        self.handles[source] = handle
        _require(callable(getattr(handle, "stop", None)) and callable(getattr(handle, "join", None)), "invalid_log_stream_handle")
        
        # stream itself is live and keeps its normal end/join evidence; a
        # provable replacement gap covers the missing confirmation read.
        try:
            after, after_ref = self._read("pod", pod["name"])
        except BaseException as exc:
            if isinstance(exc, ArchiveError) or self.persistence_failed:
                raise
            if self._record_replacement_gap(uid, "pod_attach_confirmation_read_failed", exception_type=type(exc).__name__):
                return
            raise
        _require(_physical(after) == _physical(before) and after["owners"] == before["owners"], "pod_changed_during_attachment")
        _require(self._deadline_age_s(entry["attach_anchor_monotonic_s"], deadline_owner="archive_coordinator")
                 <= self._attach_budget_s, "pod_attachment_deadline")
        entry["attachment"] = {"before_read": before_ref, "after_read": after_ref, "ancestor_read": ancestor_ref,
                               "since_epoch_s": since, "completed_epoch_s": time.time(),
                               "completed_monotonic_s": time.monotonic(), "physical_identity": list(_physical(before)),
                               "container_projection": _copy_container(before["container"])}
        entry["attachments"][source] = entry["attachment"]

    def _record_replacement_gap(self, uid, trigger, *, exception_type=None):
        """Honest GAP_POD_REPLACED row, or False when replacement is unprovable.

        collection ruling: during_fault, a Pod stream missing because the injected
        mechanism's Deployment rollout replaced the Pod is a coverage gap the
        run records and survives -- never a journal poisoning. The gap needs
        checkable proof, assembled here from durable evidence:

        * the Pod's controlling ReplicaSet, read fresh and proven to belong to
          this archive's pinned Deployment (deployment_uid), the same
          ancestry proof an attachment carries; and
        * live replacement evidence: a re-resolved current Pod enumeration
          (fresh ``initial`` read; successors under the same ReplicaSet and
          younger than the replaced Pod are observed into the archive so the
          watch/attach machinery covers them) or an observed deletion
          request/deletion of the replaced Pod itself.

        Both old and new Pod identities land in the gap row (pod transfer
        record). Any missing leg -- other phase, no declared switch band, no
        ReplicaSet ancestry, no successor and no deletion, any read failure
        while assembling the proof -- returns False and the caller keeps the
        fail-closed failure exactly as before. This method never masks a
        failure it cannot prove through.
        """
        if self.phase != "during_fault" or self.switch_band_s <= 0:
            return False
        entry = self.pods[uid]
        owner = next((row for row in entry["latest"]["owners"] if row.get("controller")), None)
        if owner is None or owner.get("kind") != "ReplicaSet":
            return False
        try:
            ancestor, ancestor_ref = self._read("replicaset", owner["name"])
            if ancestor["uid"] != owner["uid"] or ancestor["deployment_uid"] != self.scope.deployment_uid:
                return False
            # Re-resolve the current live Pods of this Deployment and observe
            # them (idempotent for already-tracked Pods).
            current, _ = self._read("initial")
            for pod_row in current["pods"]:
                self._observe(pod_row, time.time(), time.monotonic())
            # Successor proof at Deployment level: a rollout creates a NEW
            # ReplicaSet for the replacement generation (D01 field shape:
            # inventory-6567c8fc57 -> inventory-75d6544ff9), so the successor
            # cannot be matched by the replaced Pod's RS uid -- its own
            # controlling ReplicaSet must be read and proven to belong to the
            # same pinned Deployment (the live_runtime _component ancestry
            # pattern). A successor sharing the replaced Pod's ReplicaSet
            # inherits that already-proven ancestry.
            successors = []
            for other_uid, other in self.pods.items():
                if other_uid == uid or other["first"]["creation_epoch_s"] <= entry["first"]["creation_epoch_s"]:
                    continue
                other_owner = next((row for row in other["first"]["owners"] if row.get("controller")), None)
                if other_owner is None or other_owner.get("kind") != "ReplicaSet":
                    continue
                if other_owner["uid"] == owner["uid"]:
                    successors.append((other_uid, other_owner))
                    continue
                other_ancestor, _ = self._read("replicaset", other_owner["name"])
                if other_ancestor["uid"] == other_owner["uid"] and other_ancestor["deployment_uid"] == self.scope.deployment_uid:
                    successors.append((other_uid, other_owner))
            deleted_evidence = (entry["deleted_received_epoch_s"] is not None
                                or entry["deletion_requested_received_epoch_s"] is not None)
            if not successors and not deleted_evidence:
                return False
        except BaseException:
            return False
        gap = {"gap_code": "GAP_POD_REPLACED", "trigger": trigger,
               "phase": self.phase, "declared_switch_band_s": self.switch_band_s,
               "exception_type": exception_type,
               "replaced_pod": {"uid": uid, "name": entry["first"]["name"],
                                "creation_epoch_s": entry["first"]["creation_epoch_s"],
                                "last_resource_version": entry["last_resource_version"],
                                "physical_identity": list(_physical(entry["latest"])),
                                "deleted_received_epoch_s": entry["deleted_received_epoch_s"],
                                "deletion_requested_received_epoch_s": entry["deletion_requested_received_epoch_s"],
                                "first_received_monotonic_s": entry["first_received_monotonic_s"],
                                "attach_attempted_stream": entry["source_id"]},
               "replicaset": {"name": owner["name"], "uid": owner["uid"],
                              "deployment_uid": self.scope.deployment_uid, "read": ancestor_ref},
               "successor_pods": [{"uid": other_uid, "name": self.pods[other_uid]["first"]["name"],
                                   "creation_epoch_s": self.pods[other_uid]["first"]["creation_epoch_s"],
                                   "first_received_monotonic_s": self.pods[other_uid]["first_received_monotonic_s"],
                                   "replicaset": {"name": other_owner["name"], "uid": other_owner["uid"]}}
                                  for other_uid, other_owner in successors],
               "recorded_epoch_s": time.time(),
               "coverage_statement": "logs of the replaced Pod from its attach anchor to the replacement are not captured; "
                                     "successor streams are captured from their own attach; no exhaustiveness is claimed"}
        with self.lock:
            entry["gap"] = gap
            self.pod_gaps.append(gap)
        return True

    def _flush_due(self, source):
        pieces = self.piece_buffers.get(source)
        if not pieces:
            return False
        buffered_bytes = sum(len(piece["raw"]) for piece in pieces)
        if buffered_bytes >= self.limits.max_chunk_bytes or len(pieces) >= self.COALESCE_MAX_PIECES:
            return True
        now = time.monotonic()
        # Attempt-7 during_fault lesson, two failure modes of an age-only rule:
        # (a) oldest-age windows flush one small batch per window under load;
        # (b) receipt ages of BACKLOGGED pieces look "quiet" (their receipts are
        # old even while new pieces keep arriving behind them in the queue), also
        # collapsing batches to one piece per write. The stream is quiet only
        # when the worker has caught up (queue empty) AND no recent piece is
        # buffered; while a backlog exists the batch grows to the count/byte
        # caps, bounded by a hard oldest-age limit at a quarter of the budget.
        if self.queue.empty() and now - pieces[-1]["received_monotonic_s"] >= self.coalesce_window_s:
            return True
        return now - pieces[0]["received_monotonic_s"] >= self.limits.flush_timeout_s / 4.

    def _flush_source(self, source, *, force=False):
        pieces = self.piece_buffers.get(source)
        if not pieces:
            return
        if not force and not self._flush_due(source):
            return
        self.piece_buffers[source] = []
        total = sum(len(piece["raw"]) for piece in pieces)
        queued = False
        try:
            batch = b"".join(piece["raw"] for piece in pieces)
            # Artifact id keeps the ".chunk-" prefix of the first piece's sequence,
            # which strictly increases across batches of this source.
            suffix = "%s.chunk-%06d" % (source, len(self.chunks[source]))
            end_offset = pieces[-1]["byte_start"] + len(pieces[-1]["raw"])
            ref = self._write(suffix, batch, binary=True, received_mono=pieces[0]["received_monotonic_s"],
                              stream_id=source, stream_end_offset=end_offset, queued_bytes=total)
            queued = True
            enqueued_mono = self.persist_enqueued_monotonic[ref["artifact_id"]]
            offset_in_artifact = 0
            for piece in pieces:
                sequence = len(self.chunks[source])
                self.chunks[source].append({
                    "sequence": sequence, "byte_start": piece["byte_start"],
                    "byte_end": piece["byte_start"] + len(piece["raw"]),
                    "raw": {**ref, "byte_offset": offset_in_artifact, "bytes": len(piece["raw"])},
                    "received_epoch_s": piece["received_epoch_s"],
                    "received_monotonic_s": piece["received_monotonic_s"],
                    "writer_enqueued_monotonic_s": enqueued_mono,
                    "flushed_monotonic_s": time.monotonic()})
                offset_in_artifact += len(piece["raw"])
            _require(pieces[0]["byte_start"] == self.scheduled.get(source, 0), "chunk_scheduled_offset_gap")
            self.scheduled[source] = end_offset
        except BaseException:
            if not queued:
                # The bytes never entered the persistence queue, so they remain
                # an explicit unpersisted failure and release their buffer charge.
                self.failed_bytes[source] = self.failed_bytes.get(source, 0) + total
                with self.lock:
                    self.pending -= total
            raise

    def _process(self, item):
        if item[0] == "end":
            _, source, termination, rc, epoch, mono, stop_already_requested = item
            self.ends[source] = {"termination": termination, "return_code": rc, "received_epoch_s": epoch, "received_monotonic_s": mono,
                                 "stop_requested_before_callback": stop_already_requested}
            if termination not in {"natural", "cancelled"} or (termination == "natural" and rc != 0):
                self._fail("stream_failed_" + termination)
            if termination == "cancelled" and not stop_already_requested:
                self._fail("unrequested_stream_cancellation")
            if source == "watch" and not stop_already_requested:
                self._fail("unrequested_watch_eof")
            return
        _, source, raw, epoch, mono, offset = item
        if self.persistence_failed:
            with self.lock:
                self.failed_bytes[source] = self.failed_bytes.get(source, 0) + len(raw)
                self.pending -= len(raw)
                _require(self.pending >= 0, "archive_pending_bytes_underflow")
            return
        if source == "watch":
            self._watch_bytes(item)
            return
        try:
            buffered = self.piece_buffers.get(source, [])
            _require(offset == self.scheduled.get(source, 0)
                     + sum(len(piece["raw"]) for piece in buffered), "chunk_byte_offset_gap")
            buffered.append({"raw": raw, "byte_start": offset,
                             "received_epoch_s": epoch, "received_monotonic_s": mono})
            self.piece_buffers[source] = buffered
        except BaseException:
            with self.lock:
                self.pending -= len(raw)
            raise
        # Flush evaluation is batch-granular (_check_pending after each dequeue
        # batch): a per-piece evaluation would fire the hard age bound on the
        # first stalled piece and collapse batches back to one piece per write.

    def _watch_bytes(self, item):
        _, _, raw, epoch, mono, offset = item
        self.watch_pending_chunks.append(item)
        self.watch_buffer.extend(raw)
        _require(len(self.watch_buffer) <= self.limits.max_read_bytes, "watch_event_frame_limit")
        while b"\n" in self.watch_buffer:
            line, rest = self.watch_buffer.split(b"\n", 1)
            start, end = self.watch_offset, self.watch_offset + len(line) + 1
            self.watch_buffer, self.watch_offset = bytearray(rest), end
            _require(bool(line.strip()), "empty_watch_frame")
            event = _parse(line)
            _fields(event, "type resource_version pod code")
            _text(event["resource_version"])
            _require(event["type"] in {"ADDED", "MODIFIED", "DELETED", "BOOKMARK", "ERROR"}, "unknown_watch_event")
            _require(len(self.events) + len(self.watch_pending_events) < self.limits.max_events, "watch_event_limit")
            if event["type"] in {"ADDED", "MODIFIED", "DELETED"}:
                _pod(event["pod"], self.scope)
                _require(event["code"] is None and event["pod"]["resource_version"] == event["resource_version"], "watch_event_identity_mismatch")
            else:
                _require(event["pod"] is None and (type(event["code"]) is int if event["type"] == "ERROR" else event["code"] is None), "watch_control_event_invalid")
            self.watch_pending_events.append({"byte_start": start, "byte_end": end, "projection": event,
                                               "received_epoch_s": epoch, "received_monotonic_s": mono})
        if self.watch_buffer:
            return
        # Validate whole frames before persisting their wire chunks. A full Pod
        # or unknown credential-bearing field is rejected without writing it.
        for _, _, chunk_raw, received_epoch, received_mono, byte_start in self.watch_pending_chunks:
            _require(byte_start == self.scheduled["watch"], "watch_chunk_offset_gap")
            sequence = len(self.chunks["watch"])
            end_offset = byte_start + len(chunk_raw)
            ref = self._write("watch.chunk-%06d" % sequence, chunk_raw, binary=True, received_mono=received_mono,
                              stream_id="watch", stream_end_offset=end_offset, queued_bytes=len(chunk_raw))
            enqueued_mono = self.persist_enqueued_monotonic[ref["artifact_id"]]
            self.chunks["watch"].append({"sequence": sequence, "byte_start": byte_start, "byte_end": end_offset,
                "raw": ref, "received_epoch_s": received_epoch, "received_monotonic_s": received_mono,
                "writer_enqueued_monotonic_s": enqueued_mono, "flushed_monotonic_s": time.monotonic()})
            self.scheduled["watch"] = end_offset
        self.watch_pending_chunks = []
        pending, self.watch_pending_events = self.watch_pending_events, []
        for record in pending:
            event = record["projection"]
            record.update(sequence=len(self.events), chunk_artifact_ids=[chunk["raw"]["artifact_id"] for chunk in self.chunks["watch"]
                          if chunk["byte_start"] < record["byte_end"] and chunk["byte_end"] > record["byte_start"]])
            record["artifact"] = self._write("event-%06d" % len(self.events), _json_bytes(record))
            self.events.append(record)
            if event["type"] == "ERROR":
                self._fail("watch_expired" if event["code"] == 410 else "watch_error")
            elif event["pod"] is not None:
                self._observe(event["pod"], record["received_epoch_s"], record["received_monotonic_s"], deleted=event["type"] == "DELETED")
                self._attach(event["pod"]["uid"])

    def _drain(self):
        while True:
            try:
                item = self.queue.get_nowait()
            except queue.Empty:
                return
            self._process(item)

    def _replacement_boundary(self, uid, source):
        """Recorded restart boundary that closes this source's container, if any."""
        entry = self.pods[uid]
        physical = (entry["attachments"].get(source) or {}).get("physical_identity")
        if physical is None:
            return None
        return next((row for row in entry["container_restarts"]
                     if row["from_physical"] == physical), None)

    def _check_pending(self):
        now = time.monotonic()
        for source in tuple(self.piece_buffers):
            self._flush_source(source)
        # Coordinator deadlines subtract only coordinator-owned driver waits;
        # writer-owned journal-lock intervals cannot mask attach or watch age.
        # Coalescing rules above stay on wall time.
        if (self.watch_pending_chunks
                and self._deadline_age_s(self.watch_pending_chunks[0][4], now,
                                         deadline_owner="archive_coordinator") > self.limits.flush_timeout_s):
            self._fail("watch_partial_frame_flush_deadline")
        for uid, entry in list(self.pods.items()):
            if entry.get("gap") is not None:
                continue  
            # The attach budget applies to the CURRENT physical identity: a
            # silent watch after an in-place replacement must not leave the new
            # container unattached forever without a failure code (MINOR-2).
            latest_covered = any(attachment.get("physical_identity") == list(_physical(entry["latest"]))
                                 for attachment in entry["attachments"].values())
            if (entry["deleted_received_epoch_s"] is None and not latest_covered
                    and self._deadline_age_s(entry["attach_anchor_monotonic_s"], now,
                                             deadline_owner="archive_coordinator") > self._attach_budget_s):
                
                # mechanism-replaced Pod (declared band + ancestry + successor/
                # deletion evidence); otherwise the honest failure stands.
                if not self._record_replacement_gap(uid, "pod_attachment_deadline"):
                    self._fail("pod_attachment_deadline")
                continue
            if (entry["deleted_received_epoch_s"] is None and not latest_covered
                    and all(entry["latest"]["container"][key] is not None
                            for key in ("container_id", "image_id", "started_epoch_s"))):
                
                # identity completed without a further watch event (the D01
                # shape: started_epoch set one resource_version before
                # container_id) must still be attached, not only failed at
                # deadline. Idempotent: an already-covered identity returns
                # before any driver call.
                self._attach(uid)
            for source in entry["source_ids"]:
                ended = self.ends.get(source)
                if (ended and ended["termination"] == "natural" and entry["deleted_received_epoch_s"] is None
                        and entry["deletion_requested_received_epoch_s"] is None
                        and (source != entry["source_id"] or entry["latest"]["container"]["finished_epoch_s"] is None)
                        and self._replacement_boundary(uid, source) is None):
                    if self._deadline_age_s(ended["received_monotonic_s"], now,
                                            deadline_owner="archive_coordinator") > self.limits.eof_grace_s:
                        self._fail("log_natural_eof_without_lifecycle_evidence")

    def _run(self):
        try:
            value, self.initial_ref = self._read("initial")
            self.initial_rv = value["resource_version"]
            for pod in value["pods"]:
                self._observe(pod, time.time(), time.monotonic())
            on_bytes, on_end = self._callbacks("watch")
            self.handles["watch"] = self.driver.watch(self.initial_rv, on_bytes=on_bytes, on_end=on_end, timeout_s=self.limits.capture_timeout_s)
            _require(callable(getattr(self.handles["watch"], "stop", None)) and callable(getattr(self.handles["watch"], "join", None)), "invalid_watch_handle")
            self._drain()  # Watch events already observed can invalidate an initial Pod before attachment.
            for uid in tuple(self.pods):
                self._attach(uid)
            self.ready.set()
            carried = None
            while not self.closing:
                if time.monotonic() - self.started_at["monotonic_s"] > self.limits.capture_timeout_s:
                    self._fail("archive_capture_deadline")
                    break
                if carried is None:
                    try:
                        carried = self.queue.get(timeout=min(.02, self.limits.flush_timeout_s))
                    except queue.Empty:
                        carried = None
                for _ in range(self.COALESCE_MAX_PIECES):
                    # Batch-dequeue: under a backlog the flush evaluation then sees
                    # up to a full batch at once, so one write serves 16 pieces
                    # instead of the writer idling while the batch slowly fills
                    # (attempt-7 during_fault knife-edge lesson). A fetched item is
                    # carried across iterations so none is ever dropped.
                    if carried is None:
                        break
                    self._process(carried)
                    carried = None
                    try:
                        carried = self.queue.get_nowait()
                    except queue.Empty:
                        break
                self._check_pending()
            if carried is not None:
                self._process(carried)  # never lose an already-dequeued item
        except BaseException as exc:
            if isinstance(exc, ArchiveError):
                self._fail(str(exc))
            else:
                # Keep the stable code for existing consumers and add a typed
                # twin carrying the exception class (attempt-7 diagnosis lesson);
                # the full traceback lands in failure_details (S16 lesson).
                self._fail("archive_driver_or_writer_error")
                self._fail(self._record_failure_detail("archive_driver_or_writer_error", exc))
        finally:
            self.ready.set()
            self._request_stop()
            deadline = time.monotonic() + self.limits.join_timeout_s
            for source, handle in tuple(self.handles.items()):
                began = time.monotonic()
                try:
                    handle.stop()
                except BaseException:
                    self._fail("driver_stop_failed")
                finally:
                    self._record_stall("driver_stop_blocked", began, time.monotonic(), source_id=source)
            for source, handle in tuple(self.handles.items()):
                began = time.monotonic()
                try:
                    result = handle.join(timeout_s=max(.001, deadline - time.monotonic()))
                    _fields(result, "joined return_code termination")
                    _require(result["joined"] is True and type(result["return_code"]) is int
                             and result["termination"] in {"natural", "cancelled", "error", "timeout", "response_limit"}, "driver_join_unconfirmed")
                    with self.lock:
                        self.joins[source] = result
                except BaseException:
                    self._fail("driver_join_unconfirmed")
                finally:
                    
                    # be billed to the finalize force-flush of the others.
                    self._record_stall("driver_join_blocked", began, time.monotonic(), source_id=source)
            with self.lock:
                self.accepting = False
                self.cutoff = {"epoch_s": time.time(), "monotonic_s": time.monotonic(), "accepted_bytes": dict(self.accepted)}
            try:
                self._drain()
                self._finalize()
            except BaseException as exc:
                if isinstance(exc, ArchiveError):
                    self._fail(str(exc))
                else:
                    self._fail("archive_finalization_failed")
                    self._fail(self._record_failure_detail("archive_finalization_failed", exc))
                self.persistence_failed = True
                try:
                    self._discard_unqueued_stream_bytes()
                    if self.persist_worker is not None and self.persist_worker.ident is not None:
                        self._stop_persistence_writer()
                except BaseException:
                    self._fail("archive_artifact_writer_cleanup_failed")
            if self.failures:
                try:
                    self.journal.mark_failed("log_archive_failed")
                except BaseException:
                    self.persistence_failed = True

    def _discard_unqueued_stream_bytes(self):
        """Account bytes that could not be scheduled after a fail-closed stop."""
        for source, pieces in tuple(self.piece_buffers.items()):
            count = sum(len(piece["raw"]) for piece in pieces)
            if count:
                self.failed_bytes[source] = self.failed_bytes.get(source, 0) + count
                with self.lock:
                    self.pending -= count
            self.piece_buffers[source] = []
        scheduled_watch = self.scheduled.get("watch", 0)
        for _, _, raw, _, _, byte_start in self.watch_pending_chunks:
            if byte_start >= scheduled_watch:
                self.failed_bytes["watch"] = self.failed_bytes.get("watch", 0) + len(raw)
                with self.lock:
                    self.pending -= len(raw)
        self.watch_pending_chunks = []
        self.watch_pending_events = []
        self.watch_buffer.clear()
        with self.lock:
            _require(self.pending >= 0, "archive_pending_bytes_underflow")

    def _finalize(self):
        if self.persistence_failed:
            self._discard_unqueued_stream_bytes()
            self._stop_persistence_writer()
            return
        for source in tuple(self.piece_buffers):
            self._flush_source(source, force=True)
        if self.watch_buffer:
            self._fail("watch_final_partial_frame")
        if self.persistence_failed:
            self._discard_unqueued_stream_bytes()
            self._stop_persistence_writer()
            return
        # Drain already-scheduled stream chunks before reading them back to
        # derive trailing-line metadata. Keep the writer open for stop identity
        # reads below; those receipts must also close before the manifest.
        if not self._wait_persistence_queue():
            self._discard_unqueued_stream_bytes()
            self._stop_persistence_writer()
            return
        if self.persistence_failed:
            self._discard_unqueued_stream_bytes()
            self._stop_persistence_writer()
            return
        if self.persisted != self.scheduled:
            self._fail("scheduled_bytes_not_fully_persisted")
        self._discard_unqueued_stream_bytes()
        for source in self.handles:
            if source not in self.joins or source not in self.ends:
                self._fail("stream_end_or_join_missing")
            elif any(self.joins[source][key] != self.ends[source][key] for key in ("return_code", "termination")):
                self._fail("stream_end_join_disagree")
        for uid, entry in self.pods.items():
            if entry["source_id"] is None or entry["attachment"] is None:
                
                # missing is the recorded gap, not an archive failure.
                if entry.get("gap") is None:
                    self._fail("pod_stream_unattached")
                continue
            for source in entry["source_ids"]:
                attachment = entry["attachments"].get(source)
                if attachment is None:
                    if entry.get("gap") is None:
                        self._fail("pod_stream_unattached")
                    continue
                boundary = self._replacement_boundary(uid, source)
                ended = self.ends.get(source, {})
                is_latest = source == entry["source_id"]
                if (ended.get("termination") == "natural" and entry["deleted_received_epoch_s"] is None
                        and entry["deletion_requested_received_epoch_s"] is None
                        and (not is_latest or entry["latest"]["container"]["finished_epoch_s"] is None)
                        and boundary is None):
                    self._fail("log_natural_eof_without_lifecycle_evidence")
                if ended.get("termination") == "cancelled" and entry["deleted_received_epoch_s"] is None:
                    if entry["deletion_requested_received_epoch_s"] is not None:
                        # Deletion was observed live: a stop-read of a gone Pod is
                        # the expected endgame, not an identity anomaly.
                        attachment["stop_read_skipped"] = "deletion_requested_observed"
                    else:
                        after, ref = self._read("pod", entry["latest"]["name"])
                        _require(after["owners"] == entry["latest"]["owners"], "active_stream_identity_changed_at_stop")
                        allowed = [attachment["physical_identity"]] + ([boundary["to_physical"]] if boundary is not None else [])
                        _require(list(_physical(after)) in allowed, "active_stream_identity_changed_at_stop")
                        attachment["stop_read"] = ref
                finished = entry["latest"]["container"]["finished_epoch_s"] if is_latest else None
                if finished is not None:
                    upper, basis = finished, "container_finished"
                elif boundary is not None:
                    upper, basis = boundary["observed_epoch_s"], "container_replaced_boundary"
                    started = boundary["to_physical"][5]
                    if started is not None:
                        upper = min(upper, started)
                elif entry["deleted_received_epoch_s"] is not None:
                    upper, basis = entry["deleted_received_epoch_s"] + self.uncertainty, "deleted_receive_upper_bound"
                elif entry["deletion_requested_received_epoch_s"] is not None:
                    upper, basis = None, "deletion_requested_no_upper_bound"
                else:
                    upper, basis = None, "unbounded"
                attachment["lifetime_upper_epoch_s"], attachment["lifetime_upper_basis"] = upper, basis
                attachment["trailing_partial_line"] = bool(
                    (lambda raw: raw and not raw.endswith(b"\n"))(self._read_stream(source)))
            latest = entry["attachments"].get(entry["source_id"])
            if latest is not None:
                entry["lifetime_upper_epoch_s"] = latest.get("lifetime_upper_epoch_s")
                entry["lifetime_upper_basis"] = latest.get("lifetime_upper_basis")
                entry["trailing_partial_line"] = latest.get("trailing_partial_line")
        if not self._wait_persistence_queue():
            self._discard_unqueued_stream_bytes()
            self._stop_persistence_writer()
            return
        if self.persistence_failed or not self._stop_persistence_writer():
            self._discard_unqueued_stream_bytes()
            return
        for rows in self.chunks.values():
            for chunk in rows:
                flushed = self.persist_receipt_times.get(chunk["raw"]["artifact_id"])
                _require(type(flushed) in (int, float), "archive_chunk_persist_receipt_time_missing")
                chunk["flushed_monotonic_s"] = flushed
        if self.persisted != self.scheduled:
            self._fail("scheduled_bytes_not_fully_persisted")
        if self.persisted != self.accepted or self.pending != 0:
            self._fail("accepted_bytes_not_all_persisted")
        status = "BLOCKED" if self.failures else "CAPTURED_BOUNDED_NOT_EXHAUSTIVE"
        self.manifest = {"schema_version": SCHEMA, **self.identity, "archive_id": self.archive_id, "projection_format": PROJECTION,
            "evidence_kind": self.evidence_kind, "scope": vars(self.scope), "limits": vars(self.limits), "clock_uncertainty_s": self.uncertainty,
            "host_clock_capabilities": self.clock_capabilities,
            "phase": self.phase, "declared_switch_band_s": self.switch_band_s,
            "initialization_wait_budget_s": self._initialization_wait_budget_s,
            "attach_budget_s": self._attach_budget_s,
            "pod_gaps": [dict(row) for row in self.pod_gaps],
            "status": status, "started_at": self.started_at, "stop_requested_at": self.stop_request, "callback_cutoff": self.cutoff,
            "initial_read": self.initial_ref, "initial_resource_version": self.initial_rv, "reads": self.reads, "chunks": self.chunks,
            "events": self.events, "pods": self.pods, "streams": self.streams, "stream_ends": self.ends, "stream_joins": self.joins,
            "accepted_bytes": dict(self.accepted), "persisted_bytes": dict(self.persisted),
            "failed_bytes": dict(self.failed_bytes), "failures": list(self.failures),
            "failure_details": list(self.failure_details), "write_probe_retries": self.write_probe_retries,
            "stall_windows": [dict(row) for row in self.stall_windows],
            "stall_clock": {"floor_s": self.stall_floor_s,
                            "suspended_deadlines": {
                                "archive_coordinator": {
                                    "stall_kinds": ("driver_read_blocked", "driver_follow_blocked",
                                                    "driver_stop_blocked", "driver_join_blocked"),
                                    "deadlines": ("watch_partial_frame_flush_deadline", "pod_attachment_deadline",
                                                  "log_natural_eof_without_lifecycle_evidence")},
                                "persistence_writer": {
                                    "stall_kinds": ("journal_write_blocked",),
                                    "deadlines": ("archive_flush_deadline_exceeded",),
                                    "clock_segments": (("received_monotonic_s", "writer_enqueued_monotonic_s",
                                                       "archive_coordinator"),
                                                      ("writer_enqueued_monotonic_s", "flushed_monotonic_s",
                                                       "persistence_writer"))},
                                "archive_finalizer": {"stall_kinds": (), "deadlines": ()},
                                "unclassified": {"stall_kinds": (), "deadlines": ()}},
                            "journal_classifier": "writer_mutex_probe",
                            "driver_classifier": "duration_over_floor",
                            "basis": "stall rows and received/enqueued/flushed clocks keep real wall intervals; "
                                     "flush age sums pre-enqueue coordinator time and post-enqueue writer time, "
                                     "subtracting only each segment owner's stall kinds"},
            "artifact_writer": {"queue_drained": self.persist_drained,
                                "expected_receipts": len(self.persist_expected),
                                "verified_receipts": len(self.persist_results),
                                "receipt_reconciliation": "artifact_id_relative_path_sha256_exact"},
            "writer_drained_before_manifest": self.persist_drained,
            "qualification": "not_assessed", "backend_exhaustiveness": "not_claimed"}
        self._write_manifest(_json_bytes(self.manifest))

    def _read_stream(self, source):
        result, cursor = bytearray(), 0
        artifacts = {}  # relative_path -> verified artifact bytes (shared by batched pieces)
        for row in self.chunks[source]:
            _require(row["byte_start"] == cursor, "stored_stream_offset_gap")
            descriptor = row["raw"]
            raw = artifacts.get(descriptor["relative_path"])
            if raw is None:
                path = self.journal.path / descriptor["relative_path"]
                _require(path.resolve() == path and path.is_relative_to(self.journal.path), "stored_chunk_redirected")
                raw = path.read_bytes()
                _require(_sha(raw) == descriptor["sha256"], "stored_chunk_hash_or_size_changed")
                artifacts[descriptor["relative_path"]] = raw
            piece = raw[descriptor.get("byte_offset", 0):descriptor.get("byte_offset", 0)
                        + descriptor.get("bytes", row["byte_end"] - row["byte_start"])]
            _require(len(piece) == row["byte_end"] - row["byte_start"], "stored_chunk_hash_or_size_changed")
            result.extend(piece); cursor = row["byte_end"]
        return bytes(result)

    def stop(self):
        _require(self.started, "archive_not_started")
        self._request_stop()
        if self.worker is not None and self.worker.ident is not None:
            self.worker.join(timeout=self.limits.join_timeout_s + .05)
        alive = self.worker is not None and self.worker.is_alive()
        if alive:
            self._fail("archive_writer_join_unconfirmed")
        persistence_alive = self.persist_worker is not None and self.persist_worker.is_alive()
        if persistence_alive:
            self._fail("archive_artifact_writer_join_unconfirmed")
        archive_workers_joined = (not alive and not persistence_alive and self.persist_worker is not None
                                  and self.persist_worker.ident is not None)
        workers_joined = archive_workers_joined and len(self.joins) == len(self.handles)
        return {"schema_version": SCHEMA, **self.identity, "status": "BLOCKED_WORKERS_UNCONFIRMED" if not archive_workers_joined else
                "BLOCKED" if self.failures or self.persistence_failed else "CAPTURED_BOUNDED_NOT_EXHAUSTIVE",
                "workers_joined": workers_joined, "failures": list(self.failures),
                "failure_details": list(self.failure_details), "write_probe_retries": self.write_probe_retries,
                "stall_windows": [dict(row) for row in self.stall_windows],
                "manifest": self.manifest, "artifacts": list(self.descriptors), "qualification": "not_assessed"}

    def phase_sources(self, window: c.TimeWindow):
        _require(self.manifest is not None and self.worker is not None and not self.worker.is_alive()
                 and self.persist_worker is not None and not self.persist_worker.is_alive()
                 and self.persist_drained and not self.failures and not self.persistence_failed,
                 "archive_not_complete")
        _require(isinstance(window, c.TimeWindow) and window.time_basis == "unix_epoch"
                 and window.clock_id == self.journal.contract.context.to_dict()["clock_id"]
                 and self.started_at["epoch_s"] + self.uncertainty <= window.start
                 and window.end + self.uncertainty <= self.stop_request["epoch_s"], "phase_outside_archive_capture")
        rows = []
        for uid, entry in self.pods.items():
            for source in entry["source_ids"]:
                attachment = entry["attachments"].get(source) or {}
                upper = attachment.get("lifetime_upper_epoch_s")
                if entry["first"]["creation_epoch_s"] <= window.end + self.uncertainty and (upper is None or upper >= window.start - self.uncertainty):
                    rows.append({"source_id": source, "pod_uid": uid,
                                 "container": attachment.get("container_projection") or entry["latest"]["container"],
                                 "trailing_partial_line": bool(attachment.get("trailing_partial_line")),
                                 "raw": self._read_stream(source),
                                 "lifetime_upper_epoch_s": upper,
                                 "lifetime_upper_basis": attachment.get("lifetime_upper_basis") or "unbounded",
                                 "restart_boundaries": [row for row in entry["container_restarts"]
                                                        if row["from_physical"] == attachment.get("physical_identity")]})
        return tuple(rows)
