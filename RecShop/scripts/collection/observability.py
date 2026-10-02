"""Small phase-evidence bridge for the existing runner and quality consumer.

This module creates no threads, subprocesses or network connections. Readers are
explicit bounded runtime adapters. Their real timestamps are retained; a phase
name never turns a pre-run or post-run diagnostic into an in-phase observation.
"""
from __future__ import annotations

import json
import math
import re
import time
from datetime import datetime

from . import contract as c

SCHEMA = "rq4-collect/phase-observation-r10-v1"
PHASES = ("pre_fault", "during_fault", "post_recovery")


class ObservationError(ValueError):
    pass


class PhaseObservationError(ObservationError):
    """A failed observation with an independently known drain state."""
    def __init__(self, reason, *, workers_joined, context=None):
        super().__init__(reason)
        self.workers_joined = workers_joined is True
        self.context = None if context is None else canonical_copy(context)


def require(ok, reason):
    if not ok:
        raise ObservationError(reason)


def number(value):
    return type(value) in (int, float) and math.isfinite(value)


def identity(contract):
    context = contract.context.to_dict()
    return {"contract_sha256": contract.sha256, "run_id": context["run_id"],
            "attempt_id": context["attempt_id"]}


def same_identity(value, contract):
    return type(value) is dict and all(value.get(k) == v for k, v in identity(contract).items())


def canonical_copy(value):
    return json.loads(json.dumps(value, sort_keys=True, allow_nan=False))


class PhaseEvidenceCollector:
    """Runner-compatible, synchronous bounded readback lifecycle.

    ``reader.capture_phase(contract, phase, deadline_epoch_s=...)`` returns components and optionally a pre/post DB
    capture, plus ``workers_joined is True``. A reader exception or unknown drain
    never produces that claim. No background work is owned by this bridge.
    """

    def __init__(self, reader, *, max_capture_s, phase_capture_budgets_s=None,
                 clock=time.time, sampling_observer=None):
        require(callable(getattr(reader, "capture_phase", None)) and callable(clock), "explicit_deadline_reader_and_clock_required")
        require(number(max_capture_s) and max_capture_s > 0, "explicit_capture_budget_required")
        self.reader, self.max_capture_s, self.clock = reader, max_capture_s, clock
        require(phase_capture_budgets_s is None or type(phase_capture_budgets_s) is dict,
                "phase_capture_budgets_object_required")
        phase_capture_budgets_s = {} if phase_capture_budgets_s is None else dict(phase_capture_budgets_s)
        require(all(phase in PHASES and number(budget) and budget > 0
                    for phase, budget in phase_capture_budgets_s.items()),
                "phase_capture_budget_invalid")
        self.phase_capture_budgets_s = phase_capture_budgets_s
        require(sampling_observer is None or callable(getattr(sampling_observer, "start_phase", None)),
                "sampling_observer_protocol_required")
        self.sampling_observer = sampling_observer

    def start_phase(self, contract, phase, start_epoch_s, expected_end_epoch_s, clock_mapping):
        try:
            require(isinstance(contract, c.RunContract) and phase in PHASES, "invalid_phase_contract")
            capture_budget_s = self.phase_capture_budgets_s.get(phase, self.max_capture_s)
            require(number(start_epoch_s) and number(expected_end_epoch_s)
                    and expected_end_epoch_s > start_epoch_s, "invalid_phase_bounds")
            require(type(clock_mapping) is dict
                    and clock_mapping.get("clock_id") == contract.context.to_dict()["clock_id"]
                    and all(number(clock_mapping.get(k)) for k in ("monotonic_s", "unix_epoch_s", "uncertainty_s"))
                    and clock_mapping["uncertainty_s"] >= 0, "explicit_run_clock_mapping_required")
            session = _PhaseObservation(contract, phase, start_epoch_s, expected_end_epoch_s,
                                        clock_mapping, self.clock)
            begin = self.clock()
            require(number(begin) and start_epoch_s <= begin < expected_end_epoch_s, "readback_start_outside_phase")
        except Exception as exc:
            reason = str(exc) if isinstance(exc, ObservationError) else "phase_local_validation_failed"
            raise PhaseObservationError(reason, workers_joined=True) from None
        def failed_start(reason, reader_drained, *, readback_context=None):
            # Startup does not return its session on failure, so it must drain
            # sampling here before reporting the combined worker state.
            sampling_drained = session.sampling_session is None
            if session.sampling_session is not None:
                try:
                    result = session.sampling_session.abort()
                    sampling_drained = type(result) is dict and result.get("workers_joined") is True
                except BaseException:
                    sampling_drained = False
            receipt = {**identity(contract), "schema_version": "rq4-collect/phase-read-failure-v1",
                       "phase": phase, "started_at_epoch_s": begin,
                       "deadline_epoch_s": expected_end_epoch_s,
                       "capture_target_epoch_s": min(expected_end_epoch_s, begin + capture_budget_s),
                       "ended_at_epoch_s": self.clock(), "reason": str(reason)[:256],
                       "reader_workers_joined": reader_drained,
                       "sampling_workers_joined": sampling_drained,
                       "workers_joined": reader_drained and sampling_drained}
            if readback_context is not None:
                receipt["readback"] = readback_context
            return PhaseObservationError(reason, workers_joined=reader_drained and sampling_drained,
                                         context=receipt)

        try:
            if self.sampling_observer is not None:
                session.sampling_session = self.sampling_observer.start_phase(
                    contract, phase, start_epoch_s, expected_end_epoch_s, clock_mapping)
                session.boundary_budget_s = session.sampling_session.boundary_budget_s
            # The capture budget is a reporting target. Read clients still
            # bound individual calls, and the real observation phase remains
            # the shared hard deadline. A slightly slow head must not abort
            # an otherwise useful 300-second collection.
            packet = self.reader.capture_phase(contract, phase,
                deadline_epoch_s=expected_end_epoch_s)
        except BaseException as exc:
            from .live_runtime import RuntimeReadError
            known = isinstance(exc, RuntimeReadError) and exc.workers_joined is True
            
            # reader's actual failure from every receipt and stderr trace; the
            # outer message still names the class, the chain now carries why.
            details = exc.context if isinstance(exc, RuntimeReadError) else None
            raise failed_start("phase_reader_failed:" + type(exc).__name__, known,
                               readback_context=details) from exc
        if not (type(packet) is dict and packet.get("workers_joined") is True):
            raise failed_start("phase_reader_workers_not_confirmed_drained", False)
        session.drained = True
        try:
            end = self.clock()
            require(number(end) and begin <= end < expected_end_epoch_s,
                    "phase_readback_outside_observation_window")
            require(set(packet) <= {"workers_joined", "components", "database", "locks", "sampling", "issues"},
                    "unexpected_phase_reader_fields")
            require(phase != "during_fault" or packet.get("database") is None, "during_fault_checksum_forbidden")
            require(phase != "pre_fault" or packet.get("locks") is None, "locks_readback_post_recovery_only")
        except Exception as exc:
            reason = str(exc) if isinstance(exc, ObservationError) else "phase_local_validation_failed"
            raise failed_start(reason, True) from None
        try:
            session.packet = canonical_copy(packet)
            if end - begin > capture_budget_s:
                # The reader returned a drained packet inside the real phase.
                # A bookkeeping-budget overrun alone must not abort collection.
                session.packet.setdefault("issues", []).append({
                    "status": "WARN", "reason": "phase_readback_budget_overrun",
                    "duration_s": end - begin, "budget_s": capture_budget_s})
        except Exception:
            raise failed_start("phase_packet_not_serializable", True) from None
        session.started, session.ended = begin, end
        return session


class _PhaseObservation:
    def __init__(self, contract, phase, start, expected_end, mapping, clock):
        self.contract, self.phase, self.start, self.expected_end = contract, phase, start, expected_end
        self.mapping, self.clock = canonical_copy(mapping), clock
        self.packet, self.drained = None, False
        self.sampling_session, self.boundary_budget_s = None, 0

    def observe_boundary(self, actual_window, *, deadline_epoch_s):
        require(self.sampling_session is not None, "sampling_boundary_not_configured")
        self.sampling_session.observe_boundary(actual_window, deadline_epoch_s=deadline_epoch_s)

    def close(self, actual_window):
        require(isinstance(actual_window, c.TimeWindow)
                and actual_window.time_basis == "unix_epoch"
                and actual_window.clock_id == self.contract.context.to_dict()["clock_id"]
                and actual_window.start == self.start and actual_window.end <= self.expected_end,
                "observation_actual_window_mismatch")
        require(self.drained and self.packet is not None, "phase_readback_incomplete")
        require(actual_window.start <= self.started <= self.ended < actual_window.end,
                "readback_not_within_actual_phase")
        if self.sampling_session is not None:
            sampling = self.sampling_session.close(actual_window)
            if sampling.get("workers_joined") is not True:
                raise PhaseObservationError("sampling_workers_unconfirmed", workers_joined=False)
            self.packet["sampling"] = [*self.packet.get("sampling", []), *sampling["records"]]
        return {"schema_version": SCHEMA, **identity(self.contract), "phase": self.phase,
                "workers_joined": True, "started_at_epoch_s": self.started,
                "ended_at_epoch_s": self.ended, "actual_window": actual_window.to_dict(),
                "clock_mapping": self.mapping, "readback": self.packet}

    def abort(self):
        sampling_drained = self.sampling_session is None or self.sampling_session.abort().get("workers_joined") is True
        return {"workers_joined": self.drained and sampling_drained, "background_workers_created": False}


def build_phase_supplement(contract, rules):
    """Produce existing supplement fields from current-attempt phase receipts.

    Sampling readbacks retain their own IDs; coverage is a bounded-query claim,
    not an assertion that all backend history or every application byte exists.
    """
    policy = rules.to_dict()

    def provide(record):
        require(same_identity(record, contract), "foreign_run_record")
        observations = record.get("phase_observations", {})
        require(type(observations) is dict, "phase_observations_must_be_phase_map")
        result = {"artifacts": {}, "components": [], "checksums": {}, "coverage": {},
                  "instance_windows": {}, "locks": None}
        database = {}
        for phase in PHASES:
            envelope = observations.get(phase)
            if envelope is None:
                continue
            require(type(envelope) is dict and set(envelope) == {"artifact_id", "facts"}
                    and envelope["artifact_id"] == phase + ".phase-observations",
                    "phase_observation_receipt_missing")
            snapshot = envelope["facts"]
            window = record.get("actual_phases", {}).get(phase, {})
            require(same_identity(snapshot, contract) and snapshot.get("schema_version") == SCHEMA
                    and snapshot.get("phase") == phase and snapshot.get("workers_joined") is True
                    and snapshot.get("actual_window") == window, "foreign_or_incomplete_phase_observation")
            require(snapshot.get("clock_mapping") == {**record["clock_mapping"], "clock_id": contract.context.to_dict()["clock_id"]},
                    "phase_snapshot_not_bound_to_run_clock")
            require(number(window.get("start")) and number(window.get("end")), "phase_window_missing")
            aid = envelope["artifact_id"]  # already persisted by the runner
            packet = snapshot["readback"]
            for component in packet.get("components", []):
                require(same_identity(component, contract) and component.get("phase") == phase,
                        "component_identity_mismatch")
                require(number(component.get("started_at_epoch_s")) and number(component.get("timestamp_epoch_s"))
                        and window["start"] <= component["started_at_epoch_s"] <= component["timestamp_epoch_s"] < window["end"],
                        "component_readback_not_within_phase")
                if phase == "post_recovery":
                    if component.get("entity") in policy["recovery"]["component_sources"]:
                        require(component.get("source_id") == policy["recovery"]["component_sources"][component["entity"]],
                                "component_source_id_mismatch")
                        result["components"].append({**component, "phase_observation_ref": aid})
            db = packet.get("database")
            if db is not None:
                require(phase != "during_fault" and same_identity(db, contract) and db.get("phase") == phase,
                        "database_phase_identity_mismatch")
                require(db.get("source_id") == policy["recovery"]["checksum_source"],
                        "database_source_id_mismatch")
                require(number(db.get("started_at_epoch_s")) and number(db.get("timestamp_epoch_s"))
                        and window["start"] <= db["started_at_epoch_s"] <= db["timestamp_epoch_s"] < window["end"],
                        "database_capture_not_within_phase")
                require(db.get("return_code") == 0 and type(db.get("return_code")) is int
                        and db.get("evidence_kind") == "observed"
                        
                        # intention-shared business locks it observed (empty
                        # only when the workload held none at sample time);
                        # exclusive-family locks can never appear here -- the
                        # producer fails on them. Defensive shape check: every
                        # recorded entry must belong to a benign family.
                        and all((entry.get("lock_type") in ("SHARED_READ", "SHARED_WRITE"))
                                if entry.get("kind") == "metadata"
                                else entry.get("lock_mode") in ("IS", "S", "S,REC_NOT_GAP", "REC_NOT_GAP")
                                for entry in (db.get("target_metadata_locks") or []) + (db.get("target_data_locks") or [])),
                        "database_capture_failed_or_locked")
                database[phase] = db
            
            # the Q2 owned_database_locks evidence row. Receipt integrity only
            # (identity, declared source, in-window stamps, observed channel,
            # field shapes); the SEMANTIC judgments -- examined ids covering
            # every inject lock_connection_id and active_owned_locks empty --
            # stay with quality, so a still-held lock reports as a truthful
            # FAIL row instead of crashing the supplement.
            locks_row = packet.get("locks")
            if locks_row is not None:
                require(phase == "post_recovery", "locks_observation_only_in_post_recovery")
                require(same_identity(locks_row, contract) and locks_row.get("phase") == phase,
                        "locks_phase_identity_mismatch")
                require(locks_row.get("source_id") == policy["recovery"]["lock_source"],
                        "locks_source_id_mismatch")
                require(number(locks_row.get("started_at_epoch_s")) and number(locks_row.get("timestamp_epoch_s"))
                        and window["start"] <= locks_row["started_at_epoch_s"] <= locks_row["timestamp_epoch_s"] < window["end"],
                        "locks_capture_not_within_phase")
                require(type(locks_row.get("return_code")) is int and locks_row["return_code"] == 0
                        and locks_row.get("evidence_kind") == "observed"
                        and locks_row.get("owner_id") == contract.context.to_dict()["owner_id"],
                        "locks_receipt_invalid")
                require(type(locks_row.get("examined_connection_ids")) is list
                        and all(type(value) is int and value > 0 for value in locks_row["examined_connection_ids"]),
                        "locks_examined_ids_invalid")
                require(type(locks_row.get("active_owned_locks")) is list
                        and all(type(row) is dict for row in locks_row["active_owned_locks"]),
                        "locks_active_rows_invalid")
                require(result["locks"] is None, "duplicate_locks_observation")
                result["locks"] = {**locks_row, "phase_observation_ref": aid}
            for sampling in packet.get("sampling", []):
                require(same_identity(sampling, contract) and sampling.get("phase") == phase
                        and type(sampling.get("artifact_id")) is str, "sampling_phase_identity_mismatch")
                require(sampling["artifact_id"] not in result["artifacts"], "duplicate_sampling_artifact")
                result["artifacts"][sampling["artifact_id"]] = sampling
        if set(database) == {"pre_fault", "post_recovery"}:
            pre, post = database["pre_fault"], database["post_recovery"]
            require(type(pre.get("server_uuid")) is str and pre["server_uuid"]
                    and pre["server_uuid"] == post.get("server_uuid"), "database_server_identity_changed")
            require(set(pre.get("checksums", {})) == set(post.get("checksums", {})) == {"items", "inventory"},
                    "database_checksum_table_set_mismatch")
            result["checksums"] = {**identity(contract), "source_id": policy["recovery"]["checksum_source"],
                "evidence_kind": "observed", "return_code": 0, "pre_at_s": pre["timestamp_epoch_s"],
                "post_at_s": post["timestamp_epoch_s"], "server_uuid": pre["server_uuid"],
                "phase_observation_refs": ["pre_fault.phase-observations", "post_recovery.phase-observations"],
                "tables": {table: {"pre": pre["checksums"][table], "post": post["checksums"][table]}
                           for table in ("items", "inventory")}}
        for phase, modalities in record.get("bundles", {}).items():
            require(phase in PHASES, "unknown_bundle_phase")
            result["coverage"][phase] = {}
            for modality, bundle in modalities.items():
                if modality not in ("metrics", "traces", "logs"):
                    continue
                aid = "coverage-" + phase + "-" + modality
                row = {**identity(contract), "phase": phase, "artifact_id": aid,
                       "evidence_kind": "observed", "method": "bounded_query_window_v1",
                       "window": record["actual_phases"][phase], "clock_mapping": record["clock_mapping"],
                       "run_clock_anchor_ref": "run-record",
                       "projection_sha256": bundle.get("manifest", {}).get("projection_sha256")}
                if modality == "logs" and "archive_policy" in policy["data"]["coverage_policy"]:
                    row["method"] = "bounded_log_archive_v1"
                    row["archive_rule_ref"] = policy["data"]["coverage_policy"]["archive_policy"]["rule_ref"]
                result["coverage"][phase][modality] = row
                result["artifacts"][aid] = row
        for fault in contract.faults:
            fid = fault.fault_instance_id
            operations = record.get("operations", [])
            inject = [row for row in operations if row.get("instance_id") == fid and row.get("action") == "inject"]
            recover = [row for row in operations if row.get("instance_id") == fid and row.get("action") == "recover"]
            if len(inject) == len(recover) == 1:
                start, end = inject[0].get("timestamp_epoch_s"), recover[0].get("timestamp_epoch_s")
                if number(start) and number(end) and start < end:
                    result["instance_windows"][fid] = {"start": start, "end": end, "time_basis": "unix_epoch",
                                                       "clock_id": contract.context.to_dict()["clock_id"]}
        return result

    return provide


CARRIER_RULE_SCHEMA = "rq4-collect/carrier-statistic-r10-v1"

RESTART_COUNTER_RULE_SCHEMA = "rq4-collect/restart-counter-r10-v1"
RESTART_COUNTER_AGGREGATION = "max_over_snapshots_sum_same_pod_container_delta"

# pod_restart_delta family -- the S26 atom dict verbatim (via
# _combo_injection_signal for D04-F2/D08-F2/D21-F1) plus the per-instance
# query_id merge both rule builders apply.  The closed ten-field set keeps the
# consumer gate fail-closed: any other shape stays
# restart_counter_rule_not_supported.  The F-key binding rides the injection
# rule envelope's instance_id exactly like the carrier family (no query_id
# identity tie here either; condition arms remap instance_id only).
RESTART_ATOM_SIGNAL_FIELDS = frozenset({"entity", "source_id", "unit", "labels", "min_points",
                                        "mode", "operator", "threshold", "required_fraction", "query_id"})


def validate_carrier_rule(signal, kind):
    """New explicit ledger rules; old Prometheus-labelled rules are not migrated."""
    fields = {"schema_version", "entity", "source_id", "unit", "statistic", "operator", "threshold",
              "stream_id", "min_requests", "min_successful_requests", "boundary_exclusion_s",
              "settle_buffer_s", "quantile_method", "error_statuses", "artifact_ids"}
    fields.add("latency_start_field")
    require(type(signal) is dict and set(signal) == fields and signal["schema_version"] == CARRIER_RULE_SCHEMA,
            "carrier_rule_schema_invalid")
    require(signal["source_id"] == "m1-workload-ledger", "carrier_source_must_be_real_request_ledger")
    require(kind in {"carrier_p95_ratio", "carrier_error_fraction"}, "carrier_kind_unsupported")
    require((signal["statistic"], signal["unit"]) ==
            (("successful_request_p95_ratio", "ratio") if kind == "carrier_p95_ratio" else ("request_error_fraction", "fraction")),
            "carrier_statistic_unit_mismatch")
    require(signal["operator"] == "ge" and number(signal["threshold"]) and signal["threshold"] >= 0,
            "carrier_threshold_invalid")
    if kind == "carrier_error_fraction":
        require(signal["threshold"] <= 1, "carrier_fraction_threshold_invalid")
    for field in ("min_requests", "min_successful_requests"):
        require(type(signal[field]) is int and signal[field] > 0, "carrier_minimum_count_required")
    for field in ("boundary_exclusion_s", "settle_buffer_s"):
        require(number(signal[field]) and signal[field] >= 0, "carrier_explicit_window_policy_required")
    require(signal["quantile_method"] in {"nearest_rank", "linear"}, "carrier_quantile_method_required")
    require(signal["latency_start_field"] == "network_started_at_s", "carrier_network_attempt_start_required")
    require(type(signal["error_statuses"]) is list and signal["error_statuses"]
            and len(set(signal["error_statuses"])) == len(signal["error_statuses"])
            and set(signal["error_statuses"]) == {"http_error", "timed_out", "transport_error"},
            "carrier_explicit_error_denominator_required")
    require(type(signal["artifact_ids"]) is dict
            and signal["artifact_ids"] == {"pre_fault": "pre_fault.workload", "during_fault": "during_fault.workload", "run_record": "run-record"},
            "carrier_original_artifact_refs_required")
    for field in ("entity", "stream_id"):
        require(type(signal[field]) is str and bool(signal[field]), "carrier_identity_required")


def validate_restart_counter_rule(signal):
    """Recognized pod_restart_delta rule shapes; the judgment is shared.

    Two declared shapes reach the same snapshot/delta evaluator with identical
    semantics: the r10-declared rule (schema_version + aggregation pins) and
    the atom-derived rule the assembly emits for the S26 family (closed
    ten-field set, acceptance #66 / collection).  Everything else fails closed with
    the same rejection reason the r10 gate always used; no threshold, window,
    cadence or aggregation criterion is relaxed for either shape.
    """
    require(type(signal) is dict, "restart_counter_rule_not_supported")
    common = (signal.get("source_id") == "m1-kubectl-components"
              and signal.get("unit") == "restarts" and signal.get("mode") == "delta_from_pre"
              and signal.get("operator") == "ge" and type(signal.get("min_points")) is int
              and signal["min_points"] > 0 and signal.get("required_fraction") == 1)
    if signal.get("schema_version") == RESTART_COUNTER_RULE_SCHEMA:
        require(common and signal.get("aggregation") == RESTART_COUNTER_AGGREGATION,
                "restart_counter_rule_not_supported")
        return
    require(common and set(signal) == RESTART_ATOM_SIGNAL_FIELDS
            and type(signal.get("labels")) is dict, "restart_counter_rule_not_supported")


def evaluate_carrier_signal(contract, rule, evidence, inventory, signal_window, known_non_observed):
    """Assess one explicitly versioned request-ledger statistic, never AVG.

    Requests must start and complete inside the selected, trimmed window. The
    error denominator contains actual sent terminal requests only; scheduler
    drops and cancelled-before-send work are reported separately. Insufficient
    successful requests or a zero pre p95 is unknown, not a fabricated ratio.
    """
    signal, kind = rule["signal"], rule["signal_kind"]
    if signal.get("schema_version") != CARRIER_RULE_SCHEMA:
        return "NOT_ASSESSED", "legacy_carrier_source_or_statistic_not_migrated", {}
    try:
        validate_carrier_rule(signal, kind)
        require(inventory is not None and signal_window is not None, "carrier_artifacts_or_actual_window_missing")
        plan = contract.to_dict()
        fault = next(f.to_dict() for f in contract.faults if f.fault_instance_id == rule["instance_id"])
        require(signal["entity"] == fault["normalized_root_entity"], "carrier_target_entity_mismatch")
        entries = inventory["artifacts"]

        def document(aid):
            row = entries.get(aid, {})
            require(row.get("status") == "PASS" and type(row.get("data")) is dict, "carrier_raw_artifact_unverified")
            doc = row["data"]
            require(not known_non_observed(doc), "carrier_raw_marked_non_observed")
            return doc

        run = document("run-record")
        require(same_identity(run, contract), "carrier_run_record_identity_mismatch")
        anchor = run.get("clock_mapping", {})
        require(all(number(anchor.get(k)) for k in ("monotonic_s", "unix_epoch_s", "uncertainty_s"))
                and anchor["uncertainty_s"] >= 0, "carrier_run_clock_anchor_missing")
        samples, diagnostics = {}, {}
        for phase in ("pre_fault", "during_fault"):
            ledger = document(signal["artifact_ids"][phase]); lp = ledger.get("plan", {})
            require(same_identity(lp, contract) and lp.get("phase") == phase
                    and lp.get("profile_id") == plan["context"]["profile_id"]
                    and lp.get("generator_version") == plan["request_profile"]["generator_version"]
                    and lp.get("random_seed") == plan["request_profile"]["random_seed"]
                    and lp.get("phase_window") == plan["phases"][phase], "carrier_request_profile_mismatch")
            require(ledger.get("execution_mode") == "direct_http"
                    and ledger.get("summary", {}).get("workers_joined") is True
                    and ledger.get("summary", {}).get("driver_healthy") is True
                    and ledger.get("summary", {}).get("open_connections") == 0
                    and ledger.get("clock", {}).get("clock_id") == plan["context"]["clock_id"],
                    "carrier_workers_or_clock_unverified")
            streams = [row for row in lp.get("streams", []) if row.get("stream_id") == signal["stream_id"]]
            declared = [row for row in plan["request_profile"]["streams"] if row["stream_id"] == signal["stream_id"]]
            require(len(streams) == len(declared) == 1, "carrier_stream_not_in_plan")
            require(streams[0].get("endpoint") == declared[0]["endpoint"]
                    and streams[0].get("method") == declared[0]["method"], "carrier_endpoint_mismatch")
            fingerprint = streams[0].get("request_fingerprint")
            require(type(fingerprint) is str and bool(fingerprint), "carrier_request_fingerprint_missing")
            phase_window = evidence.get("actual_phases", {}).get(phase)
            require(phase_window == run.get("actual_phases", {}).get(phase), "carrier_actual_phase_mismatch")
            window = c.TimeWindow.from_dict(phase_window)
            trim = signal["boundary_exclusion_s"]
            left = window.start + trim
            right = window.end - trim
            if phase == "during_fault":
                left = max(left, signal_window.start + signal["settle_buffer_s"] + trim)
                right = min(right, signal_window.end - trim)
            require(left < right, "carrier_effective_window_empty")
            selected, dropped, seen = [], 0, set()
            requests = ledger.get("requests")
            require(type(requests) is list, "carrier_requests_missing")
            for row in requests:
                if row.get("stream_id") != signal["stream_id"]:
                    continue
                require(row.get("request_id") not in seen, "carrier_duplicate_request_id")
                seen.add(row.get("request_id"))
                require(row.get("run_id") == plan["context"]["run_id"]
                        and row.get("attempt_id") == plan["context"]["attempt_id"]
                        and row.get("phase") == phase and row.get("request_fingerprint") == fingerprint,
                        "carrier_row_identity_mismatch")
                if row.get("status") in {"dropped", "cancelled_before_offer", "cancelled_before_send"}:
                    require(row.get("network_started_at_s") is None, "started_request_cancelled_or_misclassified")
                    dropped += 1
                    continue
                start, end = row.get(signal["latency_start_field"]), row.get("terminal_at_s")
                require(number(start) and number(end) and end >= start and row.get("connection_closed") is True,
                        "carrier_request_timing_or_drain_invalid")
                at = anchor["unix_epoch_s"] + start - anchor["monotonic_s"]
                done = anchor["unix_epoch_s"] + end - anchor["monotonic_s"]
                if not (left <= at and done < right):
                    continue
                status = row.get("status")
                require(status in {"succeeded", "http_error", "timed_out", "transport_error"},
                        "carrier_diagnostic_failure_not_service_symptom")
                if status == "succeeded":
                    require(type(row.get("http_status")) is int and 200 <= row["http_status"] < 300,
                            "carrier_success_status_contradiction")
                if status == "http_error":
                    require(type(row.get("http_status")) is int and not 200 <= row["http_status"] < 300,
                            "carrier_error_status_contradiction")
                selected.append((status, (end - start) * 1000))
            samples[phase] = selected
            diagnostics[phase] = {"selected_requests": len(selected), "successful_requests": sum(s == "succeeded" for s, _ in selected),
                                  "scheduler_or_presend_excluded": dropped, "window": [left, right]}
        if any(len(rows) < signal["min_requests"] for rows in samples.values()):
            return "NOT_ASSESSED", "insufficient_real_carrier_requests", diagnostics
        if kind == "carrier_error_fraction":
            during = samples["during_fault"]
            hits = sum(status in signal["error_statuses"] for status, _ in during)
            value = hits / len(during)
            diagnostics.update(numerator=hits, denominator=len(during), statistic=value)
        else:
            successful = {phase: sorted(value for status, value in rows if status == "succeeded") for phase, rows in samples.items()}
            if any(len(values) < signal["min_successful_requests"] for values in successful.values()):
                return "NOT_ASSESSED", "insufficient_successful_carrier_requests", diagnostics
            def p95(values):
                if signal["quantile_method"] == "nearest_rank":
                    return values[math.ceil(.95 * len(values)) - 1]
                offset = .95 * (len(values) - 1); lower = math.floor(offset); upper = math.ceil(offset)
                return values[lower] + (values[upper] - values[lower]) * (offset - lower)
            pre, during = p95(successful["pre_fault"]), p95(successful["during_fault"])
            if pre <= 0:
                return "NOT_ASSESSED", "zero_pre_p95_ratio_undefined", diagnostics
            value = during / pre
            diagnostics.update(pre_p95_ms=pre, during_p95_ms=during, statistic=value)
        return ("PASS" if value >= signal["threshold"] else "FAIL"), "predeclared_carrier_statistic_evaluated", diagnostics
    except (ValueError, KeyError, TypeError, StopIteration, OverflowError) as exc:
        return "NOT_ASSESSED", str(exc) if isinstance(exc, ObservationError) else "carrier_evidence_invalid", {}


def evaluate_state_signal(contract, rule, evidence, inventory, signal_window, known_non_observed):
    """Two existing state signal kinds backed by their actual source artifacts."""
    try:
        require(inventory is not None and signal_window is not None, "state_artifacts_or_actual_window_missing")
        entries, signal, fid = inventory["artifacts"], rule["signal"], rule["instance_id"]
        fault = next(f.to_dict() for f in contract.faults if f.fault_instance_id == fid)
        require(signal.get("entity") == fault["normalized_root_entity"], "state_signal_entity_mismatch")

        def document(aid):
            row = entries.get(aid, {})
            require(row.get("status") == "PASS" and type(row.get("data")) is dict, "state_artifact_unverified")
            require(not known_non_observed(row["data"]), "state_artifact_marked_non_observed")
            return row["data"]

        if rule["signal_kind"] == "config_state_marker":
            require(fault["mechanism"] == "nginx_configmap_rollout" and fault["normalized_root_entity"] == "catalog-gw"
                    and signal.get("source_id") == "m1-gateway-transition" and signal.get("unit") == "directive_state"
                    and signal.get("mode") == "settled_window" and signal.get("operator") == "eq"
                    and signal.get("min_points") == 1 and signal.get("required_fraction") == 1,
                    "config_state_rule_not_supported")
            plan = document("gateway-plan-" + fid)
            transition = document("gateway-transition-" + fid + "-inject")
            require(plan.get("contract_sha256") == contract.sha256 and plan.get("fault_instance_id") == fid
                    and plan.get("request_spec") == fault and plan.get("mechanism_version") == fault["mechanism_version"],
                    "gateway_state_plan_identity_mismatch")
            directive = signal.get("labels", {}).get("directive")
            require(directive in {"proxy_next_upstream", "proxy_read_timeout"}
                    and plan.get("directive") == directive and plan.get("value") == signal.get("threshold")
                    and plan.get("knob_isolation", {}).get("changed_directives") == [directive],
                    "gateway_state_not_isolated_declared_directive")
            values = re.findall(r"(?m)^\s*" + re.escape(directive) + r"\s+([^;]+);", plan.get("modified_config", ""))
            require(len(values) == 1 and values[0].strip() == signal["threshold"], "gateway_config_bytes_disagree_with_marker")
            # The transition is written only after GatewayAdapter verifies the
            # real new read-only startup config, outside closure and pod identity.
            
            # live/fake distinction); both trusted kinds pass, fake/missing
            # still refuse -- the substantive legs below carry the weight.
            from .gateway import SETTLED_BASIS
            require(transition.get("fault_instance_id") == fid and transition.get("phase") == "inject"
                    and transition.get("mechanism_version") == fault["mechanism_version"]
                    and transition.get("evidence_kind") in ("live", "observed")
                    and transition.get("settled", {}).get("basis") == list(SETTLED_BASIS)
                    and transition.get("pod_generation_change", {}).get("serving_pod_uids"),
                    "gateway_runtime_settled_readback_missing")
            at = datetime.fromisoformat(transition["settled_confirmed_at_utc"])
            require(at.tzinfo is not None, "gateway_settled_timestamp_timezone_missing")
            stamp = at.timestamp()
            phase = evidence.get("actual_phases", {}).get("during_fault", {})
            require(number(phase.get("start")) and phase["start"] <= stamp < signal_window.end <= phase["end"],
                    "gateway_settled_readback_outside_fault_phase")
            return "PASS", "runtime_config_marker_verified_not_request_effect", {
                "directive": directive, "value": signal["threshold"], "observed_at_epoch_s": stamp,
                "evidence_refs": ["gateway-plan-" + fid, "gateway-transition-" + fid + "-inject"],
                "request_symptom_claimed": False}
        
        
        # baseline, delta and threshold judgment below is shape-independent
        # and unchanged.
        require(rule["signal_kind"] == "pod_restart_delta", "restart_counter_rule_not_supported")
        validate_restart_counter_rule(signal)
        require(number(signal.get("threshold")) and signal["threshold"] >= 0, "restart_counter_threshold_invalid")
        selected = {}
        for phase in ("pre_fault", "during_fault"):
            snapshot = document(phase + ".phase-observations")
            require(same_identity(snapshot, contract) and snapshot.get("phase") == phase
                    and snapshot.get("workers_joined") is True
                    and snapshot.get("actual_window") == evidence.get("actual_phases", {}).get(phase),
                    "restart_phase_snapshot_identity_mismatch")
            packets = [snapshot["readback"], *snapshot.get("readback_series", [])]
            rows = [row for packet in packets for row in packet.get("components", []) if row.get("entity") == signal["entity"]]
            valid = []
            for row in rows:
                require(same_identity(row, contract) and row.get("phase") == phase and row.get("source_id") == signal["source_id"]
                        and row.get("evidence_kind") == "observed" and type(row.get("return_code")) is int and row["return_code"] == 0,
                        "restart_source_identity_mismatch")
                stamp = row.get("timestamp_epoch_s")
                window = evidence["actual_phases"][phase]
                require(number(stamp) and window["start"] <= stamp < window["end"], "restart_snapshot_outside_phase")
                if phase == "during_fault" and not signal_window.start <= stamp < signal_window.end:
                    continue
                valid.append(row)
            selected[phase] = valid
        require(selected["pre_fault"] and len(selected["during_fault"]) >= signal["min_points"],
                "insufficient_counter_snapshots_in_actual_fault_window")
        before = max(selected["pre_fault"], key=lambda row: row["timestamp_epoch_s"])
        initial = {(row["pod_uid"], row["container"]): row["restart_count"] for row in before.get("pods", [])}
        require(initial and all(type(value) is int and value >= 0 for value in initial.values()), "restart_baseline_counter_missing")
        deltas = []
        for row in selected["during_fault"]:
            require(row.get("deployment_uid") == before.get("deployment_uid"), "restart_deployment_uid_changed")
            current = {(pod["pod_uid"], pod["container"]): pod["restart_count"] for pod in row.get("pods", [])}
            require(set(current) == set(initial) and all(type(value) is int and value >= initial[key]
                                                       for key, value in current.items()),
                    "restart_counter_reset_or_pod_replacement_not_comparable")
            deltas.append(sum(current[key] - initial[key] for key in initial))
        value = max(deltas)
        return ("PASS" if value >= signal["threshold"] else "FAIL"), "same_pod_container_restart_delta_evaluated", {
            "delta": value, "points": len(deltas), "aggregation": RESTART_COUNTER_AGGREGATION}
    except (ValueError, KeyError, TypeError, StopIteration, OverflowError) as exc:
        return "NOT_ASSESSED", str(exc) if isinstance(exc, ObservationError) else "state_signal_evidence_invalid", {}
