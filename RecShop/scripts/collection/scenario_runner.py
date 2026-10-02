"""Shared live binding for registered combinations; all execution is explicit.

The first executable face uses ordinary P1 combinations. Pricing/host/DB
auxiliaries are refused here until their typed runtime bindings are supplied;
the same registry/constructor is retained for subsequent integration.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
import hashlib
import json
import os
from pathlib import Path, PurePath
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

from . import scenario_definitions as asm, annotations as a, contract as c, journal as j
from . import auxiliary as aux, pricing_route as pr
from . import gateway as gw, primitives as p, primitives_db as dbp, runner as r, telemetry as t, workload as w, quality as q, sampling as smp
from . import live_runtime as live, observability as obs, log_archive as la
from . import log_transport as lt, log_driver_kubectl as ldk

ROOT = Path(__file__).resolve().parents[2]
from . import environment as portable_env
OLD_ROOT = ROOT
ARTIFACTS = ROOT / "runs/collection/artifacts"
OUTPUT = ROOT / "outputs/collection/attempts"
LEASE_ROOT = ROOT / ".recshop-collection/environment-leases"
CONTEXT = NAMESPACE = "unbound"
CLUSTER_UID = NAMESPACE_UID = DB_UUID = "00000000-0000-0000-0000-000000000000"
CHECKSUMS = {}
PORT = 18005
PROM = JAEGER = "http://127.0.0.1:1"
ENVIRONMENT_CONFIG = None


# T01 Jaeger slice policy v1: split the crowded catalog_service request into
# 15-second reads while preserving the per-slice limit and fail-closed hit
# detection. T01 parent and projected conditions both build their observation
# face from source_combo T01, so the projections stay identical. This module
# is part of the collector fingerprint; regenerate affected conditions after
# this change.
_DEFAULT_COMBO_TRACE_BUDGET = r.ComboTraceBudgets(30, 0, 80)
_T01_TRACE_BUDGET_V1 = r.ComboTraceBudgets(15, 0, 80)


def _combo_trace_budget(scenario_id):
    return _T01_TRACE_BUDGET_V1 if scenario_id == "T01" else _DEFAULT_COMBO_TRACE_BUDGET



# constants (collector identity/config, prom job/scrape URL, exporter origin).

# SAMPLING_BOUNDARY_BUDGET_S are ONE envelope -- the runtime budget bounds the
# snapshot wall time, the policy gap is the declared bound the quality stage
# check enforces (sampling.py stage check). a12 proved a 7.0/5.0 split
# incoherent (pre_fault head span 6.6 s passed the budget, failed the 5.0
# declared gap). Worst measured honest chain = 6.65 s (D02 during/post, action
# boundary contention, 5 reads x 1.0-1.5 s + exposition query); 9.0 gives ~35%
# margin over that and stays inside the close-side 10.0 family.
SAMPLING_POLICY = {"method": smp.METHOD, "rule_ref": "r13-common-driver-v1", "max_configured_interval_s": 2.0,
                   "nominal_interval_s": 2.0, "cadence_tolerance_s": 0.1, "max_missing_fraction": 0.15,
                   "max_source_age_s": 5.0, "max_clock_uncertainty_s": 0.1, "timestamp_resolution_s": 0.001,
                   "max_snapshot_edge_gap_s": 9.0, "max_decoded_bytes": 2_000_000}
SAMPLING_METRIC = "http_server_duration_milliseconds_count"
SAMPLING_EXPORTER_ORIGIN, SAMPLING_PROM_ORIGIN = "http://127.0.0.1:18899", "http://127.0.0.1:19090"
SAMPLING_COLLECTOR, SAMPLING_COLLECTOR_CONFIG = "recshop-m1-otel-metrics", "/etc/m1/otel-metrics-config.yaml"
SAMPLING_PROM_JOB, SAMPLING_PROM_SCRAPE_URL = "otel-collector", "http://m1-otel-metrics:8889/metrics"

# 5 serial kubectl reads per source (deploy, merged rs+pod List, exec runtime,
# fresh pod, deploy-after); under action-coupled boundary contention (during/
# post start races the driver's own inject/recover spawns) each read takes
# 1.0-1.5 s (0.23 s idle). a7/a11/a12 receipts: worst honest chain 4.7-6.65 s
# vs the old 5.0 budget -- failed by <=0.34 s at 5.0 and by the last read's
# own clamp at 7.0, never by stuck reads. The chain self-validates state
# stability (pod UID + deploy generation), so a longer completion bound does
# not weaken the boundary evidence. 9.0 ~= 35% over worst measured; must stay
# >= SAMPLING_POLICY["max_snapshot_edge_gap_s"] coherent (see policy comment).
SAMPLING_GATE_TIMEOUT_S, SAMPLING_BOUNDARY_BUDGET_S, SAMPLING_CLOSE_BUDGET_S = 10.0, 9.0, 10.0
# Declared service -> in-container source path for the sampling runtime read
# (live_runtime.service_runtime whitelist face). The 25-service roster is an
# explicit constant, never derived from GT; the two non-app.py entries are
# pinned here (sasrec_api serves from api_server.py, shop_web from its app
# package initializer).
_SERVICE_ROSTER = ("address_service", "admin_audit_service", "ai_memory_service", "announcement_service",
                   "backend_api", "cart_service", "catalog_service", "checkout_service", "interaction_service",
                   "inventory_service", "llm_rerank_service", "merchant_service", "notification_service",
                   "order_service", "payment_service", "pricing_service", "promotion_service",
                   "recommendation_agent", "review_query_service", "review_service", "sasrec_api",
                   "search_service", "shipping_service", "shop_web", "user_service")
SERVICE_SOURCE_PATHS = {**{name: "/app/services/" + name + "/app.py" for name in _SERVICE_ROSTER},
                        "sasrec_api": "/app/services/sasrec_api/api_server.py",
                        "shop_web": "/app/services/shop_web/app/__init__.py"}


# explicitly instead of a hardcoded /health in the exposition filter. Values
# are field-observed: S20 raw metrics show rec-agent serves /recommend/health
# (the old filter resolved got 0); every other roster service resolved on

SERVICE_PROBE_PATHS = {name: "/health" for name in _SERVICE_ROSTER}
SERVICE_PROBE_PATHS["recommendation_agent"] = "/recommend/health"


# shape defaults): the carrier Deployment's shell inputs for the
# construction-time shape declaration minted by build(). Strength values
# (workers/load_percent) stay in the contract parameters (NOT_FROZEN); these
# shell inputs are the runbook shape defaults (positive CPU requests, NO CPU
# limit, bare sleep shell -- stress-ng itself comes from the StressChaos CRD,
# never --vm), and live provisioning (apply + scale + rollout + readback
# before the attempt, DELETE the Deployment after reconcile CLEAN -- scale-0
# is never cleanup) remains a separately authorized driver face: offline this
# is a declaration only, nothing is applied.
STRESSOR_CARRIER_IMAGE = "recweb-user:latest"
STRESSOR_CARRIER_REQUESTS_CPU = "2"
STRESSOR_CARRIER_REQUESTS_MEMORY = "64Mi"
# Charset-valid render token for the frozen renderer path; the applied variant
# swaps the annotation value to the contract owner_id (S05 adjudication A1 /
# runbook D1: owner_id always contains "/", which the renderer's ownership
# charset structurally cannot carry).
STRESSOR_SHAPE_OWNERSHIP = "r36-carrier-shape-ref"


# GETs /api/orders/<order_no> (S09/S14 atoms; D18/D32/D33/T08 combos). The
# binding-level item id is a CATALOG item and 404s on the order route, so the
# driver resolves a real existing order_no read-only (deterministic oldest via
# the fixed DB query in live_runtime.ReadonlyOrderNoProbe), freezes it BEFORE
# assembly and rewrites exactly the /api/orders/ carrier stream endpoints --
# never the items/review-query carriers, never the endpoint shape, and never by
# creating an order. Assembly declares the precondition (S09
# spec.preconditions[0]); this driver owns the value, per that ownership note.
ORDER_CARRIER_PREFIX = "/api/orders/"


def _host_legs(spec):
    """M03: the HOST legs whose stressor carrier Deployment this driver owns.

    A spec view without ``legs`` is not a valid entry (the real ComboSpec /
    _IsolatedEntrySpec always carry one); it routes to the plain snapshot path
    and the invalid shape still refuses downstream in build/require_ordinary.
    """
    return [leg for leg in getattr(spec, "legs", ())
            if isinstance(leg, asm.FaultLegSpec) and leg.entity == "host"]


def _stressor_provision_declaration(scenario, attempt, *, profile, action_budget, lateness_budget):
    """M03: the parent's stressor_carriers declarations, pure offline assembly.

    The Deployment manifest bytes are pin-independent (name/namespace/container
    and the STRESSOR-RUNBOOK section-2 shape constants), so an offline
    double-build of the same scenario yields exactly the manifest the live
    build will declare -- one shape authority (p.render_stressor_manifest),
    no duplicated literals. Nothing here touches the cluster.
    """
    spec = entry_view(entry_spec(scenario))
    require(bool(_host_legs(spec)), "stressor provisioning declaration requires host legs")
    require(profile in {"short", "long300"}, "unknown declared time profile")
    assembled, _, metadata = build(scenario, attempt, snapshot(spec, enabled=False), enabled=False,
                                   profile=profile, action_budget=action_budget, lateness_budget=lateness_budget)
    carriers = metadata.get("stressor_carriers")
    require(type(carriers) is tuple and carriers, "host-leg scenario produced no stressor carrier declaration")
    return carriers


def _run_owned_kubectl(process, argv, *, timeout_s, stdin=None):
    """One owned bounded kubectl mutation with a receipt row (M03)."""
    started = time.monotonic()
    result = process.run(tuple(argv), timeout_s=timeout_s, max_output_bytes=200_000, stdin=stdin)
    return {"argv": [part[:80] for part in argv[:16]], "status": result.status, "return_code": result.return_code,
            "duration_s": round(result.ended_monotonic_s - result.started_monotonic_s, 6),
            "stdout_tail": result.stdout.decode("utf-8", "replace")[-200:],
            "stderr_tail": result.stderr.decode("utf-8", "replace")[-200:],
            "workers_joined": result.workers_joined}


def provision_stressor_carriers(carriers, destination, *, attempt="attempt", context=None, namespace=None):
    """M03: prepare the OWNED stressor Deployment BEFORE the fresh pin snapshot.

    Old-runner order (strict51 inject_host_stress): apply (idempotent) ->
    scale 1 -> rollout status -> the CRD apply rides the ordinary primitive
    path against the pinned pod. The readback re-verifies the runbook section-2
    shape (positive CPU requests, NO CPU limit, bare sleep shell) and the owner
    annotation, fail-closed. Cleanup is DELETE the Deployment (never scale-0)
    and is registered by the caller (delete_stressor_carriers) -- recoveries of
    the Chaos CRDs themselves stay on the runner journal discipline.
    """
    context = CONTEXT if context is None else context
    namespace = NAMESPACE if namespace is None else namespace
    process = live.BoundedProcess(execute=True)
    prepared, receipts = [], []
    for index, carrier in enumerate(carriers, 1):
        manifest = carrier["manifest"]
        name = manifest["metadata"]["name"]
        require(manifest.get("kind") == "Deployment" and manifest["metadata"]["namespace"] == namespace,
                "stressor carrier manifest shape unexpected")
        for label, argv, timeout_s, stdin in (
                ("apply", (portable_env.resolve_cli("kubectl"), "--context", context, "--namespace", namespace, "apply", "-f", "-"), 60,
                 raw(manifest)),
                ("scale", (portable_env.resolve_cli("kubectl"), "--context", context, "--namespace", namespace, "scale", "deployment", name,
                           "--replicas", "1"), 30, None),
                ("rollout", (portable_env.resolve_cli("kubectl"), "--context", context, "--namespace", namespace, "rollout", "status",
                             "deployment/" + name, "--timeout=90s"), 100, None)):
            receipt = {"step": label, **_run_owned_kubectl(process, argv, timeout_s=timeout_s, stdin=stdin)}
            receipts.append(receipt)
            require(receipt["status"] == "ok" and receipt["return_code"] == 0 and receipt["workers_joined"] is True,
                    "stressor carrier preparation failed at " + label + " rc=" + str(receipt["return_code"]))
        kube = reader(True)
        readback = kube.get("deployment", name)
        meta, spec = readback["metadata"], readback["spec"]
        container = spec["template"]["spec"]["containers"]
        require(meta.get("labels", {}).get("app") == name and not meta.get("deletionTimestamp")
                and spec["selector"].get("matchLabels") == {"app": name} and len(container) == 1,
                "stressor carrier readback identity/shape unexpected")
        container = container[0]
        require(spec["template"]["spec"].get("enableServiceLinks") is False
                and container.get("command") == ["sleep", "infinity"]
                and "cpu" not in container.get("resources", {}).get("limits", {})
                and p._cpu_cores(container["resources"]["requests"]["cpu"]) > 0
                and bool(container["resources"]["requests"].get("memory")),
                "stressor carrier readback violates the STRESSOR-RUNBOOK section 2 shape")
        owner = meta.get("annotations", {}).get(p.OWNER_ANNOTATION)
        require(owner == manifest["metadata"]["annotations"].get(p.OWNER_ANNOTATION),
                "stressor carrier readback owner annotation drifted")
        ready = readback.get("status", {}).get("readyReplicas")
        require(type(ready) is int and ready >= 1, "stressor carrier not ready after rollout")
        receipts.append({"step": "readback", "deployment": name, "uid": meta.get("uid"),
                         "generation": meta.get("generation"), "ready_replicas": ready, "owner": owner,
                         "read_receipts": kube.reads})
        prepared.append({"deployment": name, "owner_annotation": owner, "manifest_sha256": carrier["manifest_sha256"],
                         "reference_manifest_sha256": carrier["reference_manifest_sha256"], "uid": meta.get("uid")})
    face = {"carriers": prepared, "receipts": receipts,
            "order": "apply->scale->rollout->readback BEFORE snapshot/build; inject via the ordinary primitive path",
            "cleanup": "delete_deployment_never_scale_zero"}
    write_new(destination / ("HOST-PREP-" + attempt + ".json"), summary_jsonable(face))
    return face


def restamp_live_carrier_owners(metadata, destination, *, attempt="attempt", context=None, namespace=None):
    """collection condition path: align the LIVE stressor carrier owner with the arm.

    The carrier is applied inside live_runtime_snapshot under the scenario
    binding owner ("r10-<scenario>-<profile>/<attempt>"); a condition arm
    executes under its r10c owner_id, so the owner-aware baseline read would
    class the just-provisioned own carrier as an unexamined residual (D03
    anchor refusals, 2026-09-22). build_condition already re-stamps the
    DECLARED manifests; this re-annotates the applied cluster Deployment to
    the same owner and verifies the readback. Spec/name/namespace untouched.
    """
    context = CONTEXT if context is None else context
    namespace = NAMESPACE if namespace is None else namespace
    carriers = metadata.get("stressor_carriers") or ()
    if not carriers:
        return
    owner = metadata["condition_owner_id"]
    process = live.BoundedProcess(execute=True)
    receipts = []
    for carrier in carriers:
        name = carrier["manifest"]["metadata"]["name"]
        receipt = {"step": "annotate-owner",
                   **_run_owned_kubectl(process, (portable_env.resolve_cli("kubectl"), "--context", context, "--namespace", namespace,
                                                  "annotate", "deployment", name, "--overwrite",
                                                  p.OWNER_ANNOTATION + "=" + owner), timeout_s=70)}
        receipts.append(receipt)
        require(receipt["status"] == "ok" and receipt["return_code"] == 0 and receipt["workers_joined"] is True,
                "stressor carrier owner re-annotation failed rc=" + str(receipt["return_code"]))
        kube = reader(True)
        readback = kube.get("deployment", name)
        observed = readback["metadata"].get("annotations", {}).get(p.OWNER_ANNOTATION)
        require(observed == owner, "stressor carrier owner readback drifted")
        receipts.append({"step": "readback", "deployment": name, "owner": observed})
    write_new(destination / ("HOST-OWNER-" + attempt + "-" + uuid.uuid4().hex[:8] + ".json"),
              summary_jsonable({"carriers": list(carriers), "owner": owner, "receipts": receipts}))


def delete_stressor_carriers(face, destination, *, attempt="attempt", context=None, namespace=None):
    """M03 cleanup: DELETE the owned stressor Deployment (never scale-0)."""
    context = CONTEXT if context is None else context
    namespace = NAMESPACE if namespace is None else namespace
    process = live.BoundedProcess(execute=True)
    receipts = []
    for carrier in face["carriers"]:
        name = carrier["deployment"]
        receipt = {"step": "delete",
                   **_run_owned_kubectl(process, (portable_env.resolve_cli("kubectl"), "--context", context, "--namespace", namespace,
                                                  "delete", "deployment", name, "--ignore-not-found=true",
                                                  "--wait=true", "--timeout=60s"), timeout_s=70)}
        receipts.append(receipt)
        require(receipt["status"] == "ok" and receipt["return_code"] == 0 and receipt["workers_joined"] is True,
                "stressor carrier deletion failed rc=" + str(receipt["return_code"]))
    write_new(destination / ("HOST-DELETE-" + attempt + "-" + uuid.uuid4().hex[:8] + ".json"),
              summary_jsonable({"carriers": face["carriers"], "receipts": receipts,
                                "cleanup": "delete_deployment_never_scale_zero"}))
    return receipts


def live_runtime_snapshot(spec, scenario, attempt, destination, *, profile, action_budget, lateness_budget):
    """M03 order face: HOST legs provision their owned stressor carrier
    Deployment BEFORE the fresh pin snapshot (the old strict51
    inject_host_stress order); every other entry keeps the plain snapshot.
    Returns (runtime, host_face); host_face is None when nothing was owned."""
    if not _host_legs(spec):
        return snapshot(spec, enabled=True), None
    carriers = _stressor_provision_declaration(scenario, attempt, profile=profile,
                                               action_budget=action_budget, lateness_budget=lateness_budget)
    face = provision_stressor_carriers(carriers, destination, attempt=attempt)
    return snapshot(spec, enabled=True), face


def _apply_frozen_order_no(contract, spec, order_no):
    """M04: freeze the resolved read-only order_no into order stream endpoints.

    Per-stream rewrite: only carriers whose declared path_prefix is exactly
    /api/orders/ change; every other parameterized carrier keeps the binding
    item id. The endpoint keeps the frozen kubectl-proxy URL shape -- only the
    path parameter after /api/orders/ is replaced.
    """
    streams = {stream["stream_id"]: stream for stream in contract["request_profile"]["streams"]}
    for carrier in spec.carriers:
        if carrier.path_prefix != ORDER_CARRIER_PREFIX:
            continue
        stream = streams.get(carrier.stream_id)
        require(stream is not None, "order carrier stream missing from contract request profile")
        endpoint = stream["endpoint"]
        index = endpoint.find(ORDER_CARRIER_PREFIX)
        require(index != -1 and endpoint.startswith("/api/v1/namespaces/"),
                "order carrier endpoint shape unexpected: " + endpoint[:120])
        stream["endpoint"] = endpoint[:index + len(ORDER_CARRIER_PREFIX)] + order_no
    return contract


class BoundDb:
    """Existing bounded read-only probe with this experiment's physical baseline."""
    def __init__(self, enabled, environ, source_id):
        self.probe = live.ReadOnlyDbProbe(execute=enabled, environ=environ, total_timeout_s=10,
                                           socket_timeout_s=5, source_id=source_id)

    def capture_bounded(self, contract, phase, *, total_timeout_s):
        value = self.probe.capture(contract, phase, total_timeout_s=min(10, total_timeout_s))
        require(value.get("server_uuid") == DB_UUID and value.get("checksums") == CHECKSUMS
                and value.get("workers_joined") is True, "database baseline/identity/drain differs")
        return value

    def capture(self, contract, phase):
        return self.capture_bounded(contract, phase, total_timeout_s=10)


def require(ok, reason):
    if not ok:
        raise r.RunnerError(reason)


def raw(value):
    return c.canonical_json(value).encode()


def summary_jsonable(value):
    """collection (acceptance #66): shape in-memory driver trees into plain JSON values.

    The runner-side results legitimately hold tuples/sets/Path objects in
    memory (journal-serialized products are unaffected); the driver-side
    aggregation previously crashed inside contract.canonical_json on them.
    Applied at the driver's canonical write points -- collection: the SUMMARY write;
    collection (pilot cases 08-10 evidence): the write-ahead recovery-input payload
    and the recover-step RECOVERY convenience write, whose tuple leaves
    (stressor_carriers/gateway_rollouts declarations, RecoveryReport
    restored_intents/blockers) hit the same located rejection: tuples become
    lists, sets/frozensets become canonically ordered lists, paths become
    strings. Leaves without a faithful JSON shape pass through unchanged and
    stay rejected by the contract (located since collection); canonical semantics
    are untouched, and a payload that was already plain JSON writes identical
    bytes with or without the shaping.
    """
    if isinstance(value, dict):
        return {key: summary_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [summary_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((summary_jsonable(item) for item in value),
                      key=lambda item: json.dumps(item, sort_keys=True, allow_nan=True, default=repr))
    if isinstance(value, PurePath):
        return str(value)
    return value


def source_hashes():
    sources = {name: hashlib.sha256((Path(__file__).parent / (name + ".py")).read_bytes()).hexdigest()
            for name in ('scenario_runner', 'scenario_definitions', "contract", "runner", "journal", "annotations", "primitives", "quality", "telemetry",
                         "primitives_db", "gateway", "auxiliary", "pricing_route", "pricing_route_model", "release_gate",
                         "observability", "sampling", "live_runtime", "log_archive", "log_transport", "log_driver_kubectl", "workload", "environment", 'campaign_runtime', 'run_scenario')}
    sources["ops.maintenance_receipt"] = hashlib.sha256((ROOT / 'ops/metrics/maintenance_receipt.py').read_bytes()).hexdigest()
    return sources


def write_new(path, value):
    data = value if type(value) is bytes else raw(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(data); stream.flush(); os.fsync(stream.fileno())
    return {"path": str(path), "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def trace_relations():
    exceptions = {"backend_api": "backend", "recommendation_agent": "rec-agent", "sasrec_api": "sasrec",
                  "shop_web": "shop-web", "recagent": "rec-agent"}
    result = dict(exceptions)
    for spec in asm.ATOM_SPECS.values():
        name = spec.signal.service_name
        if name not in result:
            for suffix in ("_service", "_api"):
                if name.endswith(suffix):
                    result[name] = name[:-len(suffix)].replace("_", "-")
                    break
    return tuple(sorted(result.items()))


@dataclass(frozen=True)
class _IsolatedEntrySpec:
    """collection item 3: the entry reading shape for one single-root AtomSpec.

    Carries exactly the multi-leg fields the shared entry path consumes
    (build/snapshot/rule payload/sampling signal indexing); same_offset_budgets
    stays None -- an atom declares no cross-leg action pairs. Not a ComboSpec
    and never assembled as one: asm.assemble receives the AtomSpec itself."""
    scenario_id: str
    purpose: str
    phases: tuple
    legs: tuple
    carriers: tuple
    telemetry: object
    signals: tuple
    same_offset_budgets: None = None


def entry_spec(scenario):
    """collection item 3: the common entry resolves a scenario id through COMBO_SPECS
    first, then the single-root ATOM registry (an isolated condition carries
    source_combo None and names its atom in scenario_id; a plain atom scenario
    id takes the same road). No id exists in both registries (D*/T* vs S*),
    and an unknown id keeps the HEAD fail-closed refusal (spec_for raises)."""
    return asm.combo_spec_for(scenario) if scenario in asm.COMBO_SPECS else asm.spec_for(scenario)


def isolated_entry_view(atom):
    """collection item 3: one single-root AtomSpec re-faced onto the multi-leg spec
    surface build()/snapshot() consume (legs/carriers/signals as 1-tuples).

    The atom itself stays the assembler's truth (asm.assemble receives the
    AtomSpec; ComboSpec's >=2-leg law is untouched -- this is a reading shape
    for the shared entry, never a fabricated combination)."""
    return _IsolatedEntrySpec(scenario_id=atom.scenario_id, purpose=atom.purpose, phases=atom.phases,
                              legs=(atom.leg,), carriers=(atom.carrier,), telemetry=atom.telemetry,
                              signals=(atom.signal,))


def entry_view(spec):
    """The spec every shared-entry consumer reads: a ComboSpec passes through
    byte-identical (HEAD equivalence); an AtomSpec gets the 1-tuple view."""
    return spec if isinstance(spec, asm.ComboSpec) else isolated_entry_view(spec)


def main_carrier_deployment(spec):
    """The main carrier's Deployment name (combo role-label law; collection item 3:
    an isolated entry has no combo-* role labels -- its one carrier IS the
    main stream, the atom's own panel carrier)."""
    mains = [carrier.service.split(":", 1)[0] for carrier in spec.carriers
             if carrier.carrier.startswith("combo-main-")]
    require(len(mains) == 1 or (not mains and len(spec.carriers) == 1),
            "exactly one main carrier stream required")
    return mains[0] if mains else spec.carriers[0].service.split(":", 1)[0]


def parent_scope_leg(spec):
    """The one declared parent observation scope leg (runtime_rule_payload main)."""
    main = main_carrier_deployment(spec)
    for leg in spec.legs:
        if isinstance(leg, asm.FaultLegSpec) and leg.deployment == main:
            return leg
    
    # no primary log scope), so the fallback stays on the first Kubernetes leg.
    return next((leg for leg in spec.legs if isinstance(leg, asm.FaultLegSpec)), spec.legs[0])


def runtime_rule_payload(spec, payload):
    """One declared parent observation scope shared by every contrast arm."""
    value = json.loads(c.canonical_json(payload))
    main = parent_scope_leg(spec).entity
    value["data"]["required_sources"]["logs"] = [spec.telemetry.logs if leg.entity == main else spec.telemetry.logs + "-" + leg.entity
                                                  for leg in spec.legs]
    if spec.scenario_id == "S05":
        value["data"]["required_sources"]["logs"].append("pricing-pod-logs")
    value["data"]["phase_evidence_policy"] = "required_phase_observations_r10_v1"
    # Predeclaration is a rule face, not a live observation: the offline/preview
    # branch writes it too, and a query without its evidence_ref keeps an honest
    # NOT_ASSESSED row downstream.
    value["data"]["source_sampling_policy"] = dict(SAMPLING_POLICY)
    return value


def sampling_observation_target(kube, labels, source_id, *, expected_pod_uid=None):
    """Pin the exporter-observed service, which need not be the fault target."""
    require(labels.get("k8s_namespace_name") == kube.namespace, "sampling exporter namespace mismatch")
    pod = kube.get("pod", labels["k8s_pod_name"])
    require(not pod["metadata"].get("deletionTimestamp"), "sampling exporter pod terminating")
    if expected_pod_uid is not None:
        require(pod["metadata"].get("uid") == expected_pod_uid
                and pod["status"].get("phase") == "Running"
                and any(row.get("type") == "Ready" and row.get("status") == "True"
                        for row in pod["status"].get("conditions", [])),
                "sampling exporter Pod UID/readiness changed")
    owners = [row for row in pod["metadata"].get("ownerReferences", [])
              if row.get("kind") == "ReplicaSet" and row.get("controller") is True]
    require(len(owners) == 1, "sampling exporter ReplicaSet owner ambiguous")
    rs = kube.get("replicaset", owners[0]["name"])
    require(rs["metadata"]["uid"] == owners[0]["uid"], "sampling exporter ReplicaSet UID changed")
    owners = [row for row in rs["metadata"].get("ownerReferences", [])
              if row.get("kind") == "Deployment" and row.get("controller") is True]
    require(len(owners) == 1, "sampling exporter Deployment owner ambiguous")
    dep = kube.get("deployment", owners[0]["name"])
    require(dep["metadata"]["uid"] == owners[0]["uid"] and not dep["metadata"].get("deletionTimestamp"),
            "sampling exporter Deployment UID changed")
    selector = dep["spec"]["selector"].get("matchLabels", {})
    containers = dep["spec"]["template"]["spec"]["containers"]
    require(set(selector) == {"app"} and pod["metadata"].get("labels", {}).get("app") == selector["app"]
            and len(containers) == 1, "sampling exporter selector/container ambiguous")
    name = dep["metadata"]["name"]
    return live.ComponentReadTarget(name, source_id, name, dep["metadata"]["uid"], selector["app"], containers[0]["name"])


def _warm_reader(reader):
    """P02b: pay the collector prelude pre-run, off the phase clock; fake
    test doubles without warm() stay untouched."""
    warm = getattr(reader, "warm", None)
    if callable(warm):
        warm()


def resolve_sampling_observer(spec, index, fid, target, kube):
    """Live exporter-label resolution then observer construction (collection wiring).

    Live runs only; preview/offline callers never reach here. One bounded
    exposition read selects the unique in-cluster probe-path series (metric +
    service_name, excluding the 127.0.0.1 loopback http_host channel -- the
    collection-B verified pattern). SASRec also pins the canonical Service DNS face
    in http_server_name because its current /health series share one PodIP
    http_host. Any read/selection failure keeps the run going without an
    observer; the declared policy stays and the reason is returned for the
    run record.
    """
    signal = spec.signals[index]
    path = SERVICE_SOURCE_PATHS.get(signal.service_name)
    require(path is not None, "sampling source path not declared for " + signal.service_name)
    probe_path = SERVICE_PROBE_PATHS.get(signal.service_name)
    require(probe_path is not None, "sampling probe path not declared for " + signal.service_name)
    try:
        exposition = live.SourceExpositionCapture(live.ExpositionReadPolicy(
            source_id="otel-exporter-18899", origin=SAMPLING_EXPORTER_ORIGIN, allowed_paths=("/metrics",),
            timeout_s=SAMPLING_GATE_TIMEOUT_S, max_response_header_bytes=65536, max_response_body_bytes=4_000_000),
            execute=True, max_decoded_bytes=SAMPLING_POLICY["max_decoded_bytes"])
        gate = exposition.capture_bounded(total_timeout_s=SAMPLING_GATE_TIMEOUT_S)
        points = smp.exposition_points(smp.decode_body(gate, max_decoded_bytes=SAMPLING_POLICY["max_decoded_bytes"]),
                                       SAMPLING_METRIC)
        series = [row for row in points if row["labels"].get("http_target") == probe_path
                  and row["labels"].get("service_name") == signal.service_name
                  and not str(row["labels"].get("http_host", "127.0.0.1:")).startswith("127.0.0.1:")]
        if signal.service_name == "sasrec_api":
            
            # same Pod: PodIP, local loopback, and the in-cluster Service DNS.
            # http_host is PodIP on all three, so select the recorded
            # canonical server-name face explicitly. The later current-Pod,
            # UID/Ready and unique-series checks still reject stale or
            # duplicate canonical candidates.
            series = [row for row in series if row["labels"].get("http_server_name") == "sasrec:8200"]
        # Exporter retention outlives a route rollout's Pods. Discard only
        # independently stale/absent/inactive sources, never pick one of two
        # current series or collapse their full exporter label identities.
        now = time.time()
        series = [row for row in series if now - row["source_timestamp_s"] <= SAMPLING_POLICY["max_source_age_s"]]
        pods = kube.get("pod")["items"]
        require(len({pod["metadata"]["name"] for pod in pods}) == len(pods), "sampling Pod inventory ambiguous")
        by_name = {pod["metadata"]["name"]: pod for pod in pods}
        current = []
        for row in series:
            labels = row["labels"]
            require(labels.get("k8s_namespace_name") == kube.namespace, "sampling exporter namespace mismatch")
            require(now - row["source_timestamp_s"] >= -SAMPLING_POLICY["max_clock_uncertainty_s"],
                    "sampling exporter timestamp in future")
            pod = by_name.get(labels.get("k8s_pod_name"))
            if pod is None or pod["metadata"].get("deletionTimestamp"):
                continue
            if pod.get("status", {}).get("phase") != "Running" or not any(
                    item.get("type") == "Ready" and item.get("status") == "True"
                    for item in pod.get("status", {}).get("conditions", [])):
                continue
            uid = pod["metadata"].get("uid")
            require(type(uid) is str and bool(uid), "sampling exporter Pod UID missing")
            observed_target = sampling_observation_target(kube, labels, spec.telemetry.components, expected_pod_uid=uid)
            current.append((row, observed_target, uid, pod))
        if len(current) != 1:
            
            # (pod name/uid/age/health) on the resolution record -- the sasrec
            # got-3 case previously discarded all three candidate labels and
            # left only a count. The verdict itself stays the current
            # rejection: with more than one current source the resolver never
            # blind-picks the first series and never merges label variants; a
            # distinguish-and-select rule is a separate decision that needs
            # these retained labels first (xhigh U01: confirm the 3-way label
            # difference before any merge/select rule).
            return {"observer": None, "status": "resolution_failed",
                    "reason": "expected one current in-cluster probe series for "
                              + signal.service_name + " at " + probe_path + ", got " + str(len(current)),
                    "service_name": signal.service_name,
                    "candidate_count": len(current),
                    "filtered_out_series": len(series) - len(current),
                    "candidates": [{"labels": dict(row["labels"]),
                                    "pod_name": pod["metadata"].get("name"), "pod_uid": uid,
                                    "pod_phase": pod.get("status", {}).get("phase"),
                                    "ready": any(item.get("type") == "Ready" and item.get("status") == "True"
                                                 for item in pod.get("status", {}).get("conditions", [])),
                                    "deletion_timestamp": pod["metadata"].get("deletionTimestamp"),
                                    "source_age_s": round(now - row["source_timestamp_s"], 3)}
                                   for row, _target, uid, pod in current]}
        selected, target = current[0][0], current[0][1]
        series = [selected]
        age = now - selected["source_timestamp_s"]
        docker = portable_env.resolve_cli("docker")
        require(docker is not None, "docker executable not found")
    except Exception as exc:
        return {"observer": None, "status": "resolution_failed",
                "reason": type(exc).__name__ + ": " + str(exc)[:200], "service_name": signal.service_name}
    prom = t.HttpJsonClient(live.SamplingPromReadPolicy(
        source_id=spec.telemetry.metrics, origin=SAMPLING_PROM_ORIGIN,
        allowed_paths=("/api/v1/query_range", "/api/v1/targets", "/api/v1/status/config"),
        timeout_s=SAMPLING_GATE_TIMEOUT_S, max_response_header_bytes=65536, max_response_body_bytes=4_000_000), execute=True)
    reader = live.FiniteSourceSnapshotReader(kube=kube, targets={fid: target}, source_paths={fid: path},
        prom=prom, exposition=exposition, docker_executable=docker, collector_name=SAMPLING_COLLECTOR,
        collector_config_path=SAMPLING_COLLECTOR_CONFIG, prom_job=SAMPLING_PROM_JOB,
        prom_scrape_url=SAMPLING_PROM_SCRAPE_URL)
    observer = live.FiniteSamplingObserver(reader,
        {fid: {"metric": SAMPLING_METRIC, "exporter_labels": series[0]["labels"], "source_id": spec.telemetry.metrics}},
        boundary_budget_s=SAMPLING_BOUNDARY_BUDGET_S, close_budget_s=SAMPLING_CLOSE_BUDGET_S,
        max_decoded_bytes=SAMPLING_POLICY["max_decoded_bytes"])
    _warm_reader(reader)  # P02b: pay the collector prelude pre-run, off the phase clock
    return {"observer": observer, "status": "constructed", "query_id": fid, "service_name": signal.service_name,
            "source_path": path, "gate_series_age_s": round(age, 3), "exporter_label_count": len(series[0]["labels"])}


def resolve_all_sampling_observers(spec, targets, kube):
    """Keep every metric query, including the parent observation control legs."""
    rows = [resolve_sampling_observer(spec, index, "F" + str(index + 1), targets[index], kube)
            for index in range(len(spec.signals))]
    active = [row["observer"] for row in rows if row["observer"] is not None]
    details = [{key: value for key, value in row.items() if key != "observer"} for row in rows]
    if not active:
        return {"observer": None, "status": "resolution_failed", "queries": details}
    first = active[0].reader
    merged = live.FiniteSourceSnapshotReader(kube=kube,
        targets={key: value for observer in active for key, value in observer.reader.targets.items()},
        source_paths={key: value for observer in active for key, value in observer.reader.paths.items()},
        prom=first.prom, exposition=first.exposition, docker_executable=first.docker,
        collector_name=first.collector, collector_config_path=first.config_path,
        prom_job=first.job, prom_scrape_url=first.scrape_url)
    observer = live.FiniteSamplingObserver(merged,
        {key: value for observer in active for key, value in observer.sources.items()},
        boundary_budget_s=SAMPLING_BOUNDARY_BUDGET_S, close_budget_s=SAMPLING_CLOSE_BUDGET_S,
        max_decoded_bytes=SAMPLING_POLICY["max_decoded_bytes"])
    _warm_reader(merged)  # P02b: pay the collector prelude pre-run, off the phase clock
    return {"observer": observer, "status": "constructed" if len(active) == len(rows) else "partial_resolution",
            "queries": details}


def attach_sampling_reference(face, fid):
    """Point the parent-scope query at its archived sampling readback artifact.

    The observer mints artifact id "sampling-<phase>-<query_id>" per phase
    (live_runtime._FiniteSamplingSession.close) and build_phase_supplement
    archives it under exactly that id; telemetry.collect_prom substitutes the
    {phase} placeholder per phase before the bundle query is persisted.
    """
    queries = face.services.telemetry.metric_queries
    require(sum(query.query_id == fid for query in queries) == 1, "sampling scope query missing")
    replaced = tuple(replace(query, sampling=t.SourceSampling(None, None, "sampling-{phase}-" + fid))
                     if query.query_id == fid else query for query in queries)
    return replace(face, services=replace(face.services,
                   telemetry=replace(face.services.telemetry, metric_queries=replaced)))


def require_ordinary(spec):
    """Ordinary P1 gate. collection: DbFaultLegSpec legs pass because this driver
    supplies their typed runtime binding (build() mints RunServices
    db_bindings + db_client, the ASM-4 face). collection (plan N): the gateway NET
    legs -- chaos_mesh network_loss/network_delay on the catalog-gw
    Deployment -- pass too: a NetworkChaos CRD on catalog-gw is the same
    ordinary primitive face as on any service Deployment (collection derisk: with
    only this gate lifted the pure-net combos construct end-to-end; the
    readiness-only catalog-gw keeps its pod alive under netem, so the pin is
    stable). collection (HOST): host_cpu_saturation@host legs pass because this
    driver assembles the S05 stressor-carrier face (build() declares the
    Deployment + StressChaos carrier shape with the contract owner_id
    annotation and the delete-Deployment cleanup contract; the carrier pin is
    the S05 provisioned-pins shape). collection (plan G, collection ADAPTER-DESIGN): the
    nginx_configmap_rollout gateway-config legs (GWCFG) pass because this
    driver now supplies their construction face -- the offline preview pin
    casts the nginx container of the P2 binding shape, build() mints the P2
    rollout declaration, the facts carry the P2 contract budgets (120/90)
    -- and the consumption path is the existing runner dispatch
    (validate_run -> g.prepare_gateway; apply/recover through
    GatewayAdapter/GatewayExecutor + journal.reconcile). A GWCFG leg's
    raw_target is its own in-graph Deployment, so unlike DB/host this
    admission needs no new typed facts binding. Everything else keeps
    refusing with each offending leg named so an unsupplied auxiliary face
    stays precise instead of hiding behind a blanket rejection -- after collection
    every committed leg shape is supplied; the named refusals are the
    fail-closed guards for shapes outside the admitted sets."""
    offenders, kubernetes_legs = [], 0
    for index, leg in enumerate(spec.legs, 1):
        fid = "F" + str(index)
        if isinstance(leg, asm.DbFaultLegSpec):
            continue
        if not isinstance(leg, asm.FaultLegSpec):
            offenders.append(fid + " " + type(leg).__name__ + ": unsupported leg type")
            continue
        kubernetes_legs += 1
        if leg.entity == "host":
            if leg.mechanism != "chaos_mesh" or leg.fault_type != "host_cpu_saturation":
                offenders.append(fid + " " + leg.fault_type + "@" + leg.entity + ": HOST auxiliary adapter not supplied")
        elif leg.entity == "catalog-gw":
            if leg.mechanism != gw.MECHANISM and (leg.mechanism != "chaos_mesh"
                    or leg.fault_type not in {"network_loss", "network_delay"}):
                offenders.append(fid + " " + leg.fault_type + "@" + leg.entity + ": GW auxiliary adapter not supplied")
        elif leg.mechanism not in {"chaos_mesh", "app_env_hook"}:
            offenders.append(fid + " " + leg.fault_type + "@" + leg.entity + "/" + leg.mechanism
                             + ": auxiliary adapter not supplied")
    require(kubernetes_legs >= 1, "this runtime binding requires at least one ordinary Kubernetes leg "
                                  "(an all-database combination has no primary observation/log scope)")
    require(not offenders, "this runtime binding requires ordinary P1 legs; auxiliary adapter not supplied: "
                           + "; ".join(offenders))



# runtime premise that pricing reaches catalog through catalog-gw (S01/S02/S23/
# S24 atom preconditions[0], mechanism_anchor k8s/services/pricing.yaml -- the
# manifest default is the DIRECT catalog:5005 URL, so the premise never holds
# without an explicit switch).  The runner side is complete (pricing_route.py
# PricingRouteAdapter + runner prepare_attempt/PreparedAttemptSession); this
# driver supplies the construction face. Scope includes declared retry route
# premises even when Q1 uses a configuration marker. A
# catalog-gw-rooted fault is observable on the pricing measurement only when
# pricing's downstream catalog dependency actually transits the faulted gateway
# (live anchors: Pilot S02 attempt-2 and D14 attempt-1 both measured
# measured_fraction=0.0 with the manifest-default direct route).  A leg
# consumes when its Q1 face is (a) a carrier ledger rule rebound at assembly to
# a pricing:5014 stream, or (b) a metric_threshold face whose base atom signal
# measures pricing_service; (c) retry config markers with an explicit via-gw
# carrier and a declared redirect premise. The pricing:5014 stream presence is a hard runner contract
# (_registered_pricing_scope demands an explicit pricing request carrier), so a
# face that consumes without a pricing stream (D16) stays unwired and is
# registered as a design-adjudication item instead of a silent half-premise.
PRICING_AUX_ID = "pricing-route"
PRICING_AUX_PLAN_SCHEMA = "rq4-collect/common-driver-pricing-aux-plan-v1"
_GATEWAY_FAULT_KINDS = {("network_delay", "chaos_mesh"), ("network_loss", "chaos_mesh"),
                        ("timeout_misconfiguration", gw.MECHANISM),
                        ("retry_policy_misconfiguration", gw.MECHANISM)}


def _gateway_fault_leg(leg):
    return isinstance(leg, asm.FaultLegSpec) and leg.entity == "catalog-gw"\
        and (leg.fault_type, leg.mechanism) in _GATEWAY_FAULT_KINDS



# the pricing route aux and a pricing-rooted chaos_mesh service_cpu_saturation
# leg share the aux target Deployment ONLY in the proven field/window-disjoint
# shape -- the aux CAS-writes exactly the route_env field of the pricing
# Deployment at prepare time and restores it in the prepared recovery, while
# the CPU leg controls pod CPU through its StressChaos CRD selector and never

# ordering discipline this requires (aux setup settles -> fresh post-switch pin
# -> fault window -> CPU CRD recover -> aux cleanup after
# AWAITING_AUXILIARY_RECOVERY + drain -> terminal readback), so the runner's
# M05 overlap gate only lacks a channel to recognise the shape.  The
# declaration emitted here is that channel's construction-face half; anything
# else that names the pricing Deployment (a spec-writing or pod-recreating leg
# on pricing) stays UNDECLARED -- the runner gate keeps rejecting it, and
# mint_pricing_aux refuses it up front with this lane's precise reason.
_RESTRICTED_COEXISTENCE_KINDS = frozenset({("service_cpu_saturation", "chaos_mesh")})
_COEXISTENCE_DISCIPLINE = (
    "ln restricted coexistence order: aux setup settles before the fresh post-switch pin; the "
    "coexisting CRD leg injects on the pinned pod and is deleted at recover; aux cleanup "
    "(direct-route restore) runs only after AWAITING_AUXILIARY_RECOVERY + worker drain; "
    "terminal readback closes the run")


def _restricted_coexistence_leg(leg):
    """Spec-face admission predicate for the ln restricted coexistence lane."""
    return (isinstance(leg, asm.FaultLegSpec) and leg.entity != "host"
            and leg.deployment == "pricing"
            and (leg.fault_type, leg.mechanism) in _RESTRICTED_COEXISTENCE_KINDS)


def _coexisting_leg_row(fid, fault_type, entity, mechanism):
    return {"fault_instance_id": fid, "fault": fault_type + "@" + entity, "mechanism": mechanism,
            "controlled_surface": "pod CPU through the StressChaos CRD selector; never a pricing "
                                  "Deployment spec write",
            "discipline": _COEXISTENCE_DISCIPLINE}


def coexisting_restricted_faults(contract):
    """Executing-contract face of the ln restricted coexistence declaration.

    Walks the real fault instances, keeps only those whose raw_target names the
    aux target Deployment, and splits them into the admitted restricted shape
    (returned as declaration rows) and everything else (true conflicts the
    runner overlap gate must keep refusing).  Returns (rows, conflicts).
    """
    rows, conflicts = [], []
    for fault in contract.faults:
        data = fault.to_dict()
        target = data["raw_target"]
        if not (target.get("kind") == "Deployment" and target.get("name") == "pricing"
                and target.get("scope") == NAMESPACE):
            continue  # gateway legs name catalog-gw; host legs their own carrier; db legs no Deployment
        if (data["fault_type"], data["mechanism"]) in _RESTRICTED_COEXISTENCE_KINDS:
            rows.append(_coexisting_leg_row(fault.fault_instance_id, data["fault_type"],
                                            fault.normalized_root_entity, data["mechanism"]))
        else:
            conflicts.append(fault.fault_instance_id + " " + data["fault_type"] + "@"
                             + fault.normalized_root_entity + "/" + data["mechanism"])
    return rows, conflicts


def _projected_pricing_aux(parent_aux, parent_to_local):
    """Kept-leg projection of the pricing_aux declaration onto a condition.

    Consuming legs and ln coexisting legs follow the same law: a condition
    that drops every consuming leg returns None (the arm runs on the
    manifest-default direct route, exactly like an unaffected arm), kept legs
    are re-keyed to the local fids, and a dropped coexisting leg drops its
    declaration with them (the aux then controls an uncontested Deployment).
    """
    kept = [row for row in parent_aux["consuming_legs"] if row["fault_instance_id"] in parent_to_local]
    if not kept:
        return None
    projected = {**parent_aux,
        "gateway_fault_instance_ids": sorted(parent_to_local[fid] for fid in parent_aux["gateway_fault_instance_ids"]
                                             if fid in parent_to_local),
        "consuming_legs": [{**row, "fault_instance_id": parent_to_local[row["fault_instance_id"]]}
                           for row in kept],
        "projection": "condition_kept_legs_rekeyed_to_local_fids"}
    kept_coexisting = [row for row in parent_aux.get("coexisting_legs", ())
                       if row["fault_instance_id"] in parent_to_local]
    if kept_coexisting:
        projected["coexisting_legs"] = [{**row, "fault_instance_id": parent_to_local[row["fault_instance_id"]]}
                                        for row in kept_coexisting]
    else:
        projected.pop("coexisting_legs", None)
    return projected


def pricing_aux_scope(raw_spec):
    """Legs requiring the via-gw route for Q1 or an explicit retry premise.

    Pure spec arithmetic, no I/O -- the offline/preview face evaluates it too.
    Returns the construction-face declaration dict, or None when no leg
    consumes (nothing is provisioned and the HEAD construction path is kept
    byte-for-byte).  The advisory fault ids are parent-spec fids; a projected
    condition re-keys them (build_condition), and the actual AuxiliaryPlan
    always binds the fids of the executing contract (mint_pricing_aux).
    D22/T01 additionally carry coexisting_legs -- the ln restricted-coexistence
    declaration for the pricing-rooted chaos_mesh CPU leg sharing the aux
    target Deployment (see _restricted_coexistence_leg; every other shape
    stays undeclared so the runner overlap gate keeps refusing it).
    """
    if isinstance(raw_spec, asm.ComboSpec):
        fids = ["F" + str(index + 1) for index in range(len(raw_spec.legs))]
        legs, kinds = list(raw_spec.legs), list(raw_spec.injection_signal_kinds)
        signals, injections = list(raw_spec.signals), list(raw_spec.injection_signals)
        stream_service = {carrier.stream_id: carrier.service for carrier in raw_spec.carriers}
    else:
        fids, legs, kinds = ["F1"], [raw_spec.leg], [raw_spec.injection_signal_kind]
        signals, injections = [raw_spec.signal], [raw_spec.injection_signal]
        stream_service = {raw_spec.carrier.stream_id: raw_spec.carrier.service}
    consuming, gateway, coexisting = [], [], []
    for fid, leg, kind, signal, injection in zip(fids, legs, kinds, signals, injections):
        if not _gateway_fault_leg(leg):
            if _restricted_coexistence_leg(leg):
                coexisting.append(_coexisting_leg_row(fid, leg.fault_type, leg.entity, leg.mechanism))
            continue
        gateway.append(fid)
        source = None
        if kind in asm.CARRIER_SIGNAL_KINDS:
            # ComboSpec rebinds the atom rule's stream_id to the in-plan combo
            
            stream_id = injection.get("stream_id") if isinstance(injection, dict) else None
            source = stream_service.get(stream_id)
            if source == "pricing:5014":
                consuming.append({"fault_instance_id": fid, "fault": leg.fault_type + "@" + leg.entity,
                                  "q1_face": kind, "statistical_source": stream_id + "@pricing:5014"})
        elif kind == "metric_threshold" and signal.service_name == "pricing_service":
            consuming.append({"fault_instance_id": fid, "fault": leg.fault_type + "@" + leg.entity,
                              "q1_face": "metric_threshold",
                              "statistical_source": "pricing_service:" + signal.http_target})
        elif (kind == "config_state_marker" and leg.fault_type == "retry_policy_misconfiguration"
              and (raw_spec.scenario_id == "S23" or raw_spec.scenario_id in asm.PRICING_STREAM_GROUPS_REDIRECT)):
            via_gw = [stream for stream, service in stream_service.items()
                      if service == "pricing:5014" and "via-gw" in stream]
            if via_gw:
                consuming.append({"fault_instance_id": fid, "fault": leg.fault_type + "@" + leg.entity,
                                  "q1_face": kind, "statistical_source": via_gw[0] + "@pricing:5014",
                                  "binding_reason": "declared_retry_gateway_path_premise"})
    if not consuming:
        return None
    if not any(service == "pricing:5014" for service in stream_service.values()):
        # The runner pricing auxiliary refuses an arm without an explicit
        # pricing request carrier; wiring a half-premise here would only move
        
        return None
    return {"aux_id": PRICING_AUX_ID, "adapter_kind": pr.ADAPTER_KIND,
            "target": {"kind": "Deployment", "name": "pricing", "scope": NAMESPACE},
            "gateway_fault_instance_ids": gateway, "consuming_legs": consuming,
            "premise": "pricing CATALOG_SERVICE_URL -> " + pr.model.GATEWAY_URL
                       + ' (S01/S02/S23/S24 atom precondition[0], k8s/services/pricing.yaml manifest default is '
                       + pr.model.DIRECT_URL + ")",
            "expected_original_source": "live adapter read_baseline at prepare time (never offline)",
            "runner_integration": "prepare_attempt execution_kind=registered_sample; setup completes before the "
                                  "pre_fault workload; cleanup runs in the prepared recovery",
            **({"coexisting_legs": coexisting} if coexisting else {})}


def pricing_aux_plan_from_dict(value):
    """Rebuild the exact persisted AuxiliaryPlan (collection recovery face)."""
    require(type(value) is dict and value.get("adapter_kind") == pr.ADAPTER_KIND
            and value.get("aux_id") == PRICING_AUX_ID, "persisted pricing aux plan shape invalid")
    return aux.AuxiliaryPlan(value["aux_id"], value["adapter_kind"], tuple(value["fault_instance_ids"]),
                             value["target"], value["expected_original"], value["desired_fields"],
                             tuple(value.get("terminal_host_fault_ids", ())),
                             tuple(value.get("coexisting_fault_ids", ())))


def mint_pricing_aux(contract, destination, attempt):
    """collection: one live plan + observed adapter for an affected arm.

    The expected original is the field-verified DIRECT baseline read live here
    (read_baseline enforces the reviewed shape: literal direct route env,
    replicas=1, no scaler, no foreign owner, NACOS explicitly disabled); the
    desired shape swaps only the route env to the gateway URL.  The exact plan
    dict is persisted BEFORE prepare_attempt so a crash anywhere later can
    rebuild the same AuxiliaryPlan byte-for-byte (the journal/lease chain
    compares plan.to_dict() equality, so recovery cannot re-mint from a fresh
    read -- resource_version moves with every observation).
    """
    gateway = tuple(fault.fault_instance_id for fault in contract.faults
                    if fault.normalized_root_entity == "catalog-gw"
                    and (fault.to_dict()["fault_type"], fault.to_dict()["mechanism"]) in _GATEWAY_FAULT_KINDS)
    require(gateway, "pricing route premise requires this arm's gateway fault legs")
    prefix = "/api/v1/namespaces/" + NAMESPACE + "/services/pricing:5014/proxy/api/pricing/"
    require(any(stream["method"] == "GET" and stream["endpoint"].startswith(prefix)
                for stream in contract.to_dict()["request_profile"]["streams"]),
            "pricing auxiliary requires an explicit pricing request carrier")
    
    # aux target Deployment is either the proven field/window-disjoint CPU
    
    # true conflict.  Refusing the latter HERE keeps the failure loud and
    # precise; the runner overlap gate stays authoritative for everything this
    # face never declares.
    coexisting, conflicts = coexisting_restricted_faults(contract)
    require(not conflicts, "pricing route aux restricted coexistence (ln lane) refuses undeclarable "
                           "pricing-target legs; the runner overlap gate stays authoritative for them: "
                           + "; ".join(conflicts))
    adapter = pr.PricingRouteAdapter(CONTEXT, NAMESPACE, CLUSTER_UID, NAMESPACE_UID,
                                     process=live.BoundedProcess(execute=True), evidence_kind="observed", kubectl=portable_env.resolve_cli("kubectl"))
    original = adapter.read_baseline()
    desired = json.loads(json.dumps(original["fields"]))
    desired["route_env"] = pr.model._route(pr.model.GATEWAY_URL)
    plan = aux.AuxiliaryPlan(PRICING_AUX_ID, pr.ADAPTER_KIND, gateway,
                             {"kind": "Deployment", "name": "pricing", "scope": NAMESPACE}, original, desired,
                             coexisting_fault_ids=tuple(row["fault_instance_id"] for row in coexisting))
    receipt = write_new(destination / ("AUX-PLAN-" + attempt + ".json"),
                        {"schema_version": PRICING_AUX_PLAN_SCHEMA, "contract_sha256": contract.sha256,
                         "plan": plan.to_dict(), "baseline_readback": adapter.last_readback,
                         "coexisting_legs": coexisting})
    return (plan,), {PRICING_AUX_ID: adapter}, receipt


def reader(enabled):
    if enabled:
        portable_env.assert_live()
        portable_env.apply(sys.modules[__name__])
    return live.KubectlReadClient(portable_env.resolve_cli("kubectl") if enabled else "kubectl", CONTEXT, NAMESPACE,
        process=live.BoundedProcess(execute=enabled), execute=enabled, timeout_s=5, max_output_bytes=2_000_000)


def s05_observation_log_pins(spec, *, enabled):
    """Observe the real request carrier without turning it into an injected root."""
    if spec.scenario_id != "S05":
        return {}
    require(len(spec.legs) == 1 and spec.legs[0].entity == "host"
            and main_carrier_deployment(spec) == "pricing", "S05 log observation scope changed")
    if not enabled:
        return {"pricing": {"deployment_uid": "offline-pricing", "container": "pricing"}}
    deployment = reader(True).get("deployment", "pricing")
    require(deployment["spec"]["selector"]["matchLabels"] == {"app": "pricing"}
            and not deployment["metadata"].get("deletionTimestamp"), "pricing log selector/identity changed")
    containers = deployment["spec"]["template"]["spec"]["containers"]
    require(len(containers) == 1 and containers[0]["name"] == "pricing",
            "pricing log container identity changed")
    require(next((row.get("value") for row in containers[0].get("env", [])
                  if row["name"] == "NACOS_ENABLED"), None) == "false",
            "pricing log fixed-route premise missing")
    return {"pricing": {"deployment_uid": deployment["metadata"]["uid"], "container": containers[0]["name"]}}


def snapshot(spec, *, enabled):
    require_ordinary(spec)
    if not enabled:
        
        # construction-period stop point this closes): a gateway-config leg's
        # offline preview pin casts the nginx container, because the P2 binding
        # validation (g.prepare_gateway, gateway.py L204) requires
        # binding.container == "nginx" (11-catalog-gw.yaml main container). The
        # live branch below already reads the real container name.
        result = {"anchor_uid": "offline-catalog", "pins": {"F" + str(i + 1): {
            "deployment_uid": "offline-" + leg.deployment, "pod_name": "offline-" + leg.deployment,
            "pod_uid": "offline-pod-" + leg.deployment,
            "container": "nginx" if leg.mechanism == gw.MECHANISM else leg.deployment,
            "pod_images": [[leg.deployment, "offline:preview"]]} for i, leg in enumerate(spec.legs)
            if isinstance(leg, asm.FaultLegSpec)}}
        if spec.scenario_id == "S05":
            result["observation_log_pins"] = s05_observation_log_pins(spec, enabled=False)
        return result
    kube = reader(True)
    for name, expected in (("kube-system", CLUSTER_UID), (NAMESPACE, NAMESPACE_UID)):
        require(kube.get("namespace", name)["metadata"]["uid"] == expected, "cluster/namespace identity changed")
    env = portable_env.current()
    require(kube.get("node", env["node_name"])["metadata"]["uid"] == env["node_uid"], "node identity changed")
    pins = {}
    for index, leg in enumerate(spec.legs, 1):
        if isinstance(leg, asm.DbFaultLegSpec):
            continue  
        dep = kube.get("deployment", leg.deployment)
        require(dep["spec"]["selector"]["matchLabels"] == dict(leg.selector)
                and not dep["metadata"].get("deletionTimestamp"), "deployment selector/identity changed")
        containers = dep["spec"]["template"]["spec"]["containers"]
        require(len(containers) == 1, "explicit single-container runtime binding required")
        container = containers[0]
        
        # premise check is a SERVICE shape -- catalog-gw's nginx container
        # carries no env at all (11-catalog-gw.yaml L170-190; the gateway is
        # not a service-discovery participant), so demanding NACOS_ENABLED
        
        # (pilot case06 D03): the same no-env form covers the HOST stressor
        # carrier Deployment -- the frozen STRESSOR-RUNBOOK section 2 shape is
        # a bare sleep-infinity carrier (recweb-user:latest) with no env, and
        # it is not a service-discovery participant either; demanding the env
        # there misread the runbook-frozen carrier shape as drift. Scoped to
        # the gateway and host carrier entities only; every service leg keeps
        # the requirement.
        if leg.entity not in ("catalog-gw", "host"):
            require(next((v.get("value") for v in container.get("env", []) if v["name"] == "NACOS_ENABLED"), None) == "false",
                    "fixed route premise NACOS_ENABLED=false missing")
        sets = kube.get("replicaset", selector=dict(leg.selector))["items"]
        owned = {row["metadata"]["uid"] for row in sets if any(ref.get("kind") == "Deployment"
            and ref.get("uid") == dep["metadata"]["uid"] for ref in row["metadata"].get("ownerReferences", []))}
        pods = kube.get("pod", selector=dict(leg.selector))["items"]
        require(len(pods) == 1 and not pods[0]["metadata"].get("deletionTimestamp"), "runtime Pod set not quiescent")
        pod = pods[0]
        require(any(ref.get("kind") == "ReplicaSet" and ref.get("uid") in owned for ref in pod["metadata"].get("ownerReferences", [])),
                "Pod ancestry unproven")
        require(any(row.get("name") == container["name"] and row.get("ready") is True
                    for row in pod.get("status", {}).get("containerStatuses", [])), "runtime container not Ready")
        pins["F" + str(index)] = {"deployment_uid": dep["metadata"]["uid"], "pod_name": pod["metadata"]["name"],
            "pod_uid": pod["metadata"]["uid"], "container": container["name"],
            "pod_images": [[row["name"], row["image"]] for row in pod["spec"]["containers"]]}
    result = {"anchor_uid": kube.get("deployment", "catalog")["metadata"]["uid"], "pins": pins, "read_receipts": kube.reads}
    if spec.scenario_id == "S05":
        result["observation_log_pins"] = s05_observation_log_pins(spec, enabled=True)
    return result


def _worker_join_timeout_s(scenario, profile):
    """Bound declared long-window telemetry tails without changing observations."""
    if scenario == "D09" and profile == "long300":
        return 65.
    affected = {"T01", "D27", "D32", "D33", "T05", "T06", "T07", "T08"}
    return 120. if scenario in affected and profile == "long300" else 35.


def _durable_flush_timeout_s(scenario, profile):
    """Allow observed T01 long-window journal latency with a narrow 90s bound."""
    return 90. if scenario == "T01" and profile == "long300" else 30.


def build(scenario, attempt, runtime, *, enabled=False, action_budget=17., lateness_budget=2.,
          first_injection_lateness_budget=None, profile="short", stop_outer=None):
    raw_spec = entry_spec(scenario)
    database_name = portable_env.current()["database"]["name"]
    if isinstance(raw_spec, asm.ComboSpec):
        raw_spec = replace(raw_spec, legs=tuple(replace(leg, database=database_name) if isinstance(leg, asm.DbFaultLegSpec) else leg for leg in raw_spec.legs))
    elif isinstance(raw_spec.leg, asm.DbFaultLegSpec):
        raw_spec = replace(raw_spec, leg=replace(raw_spec.leg, database=database_name))
    original = entry_view(raw_spec)
    require_ordinary(original)
    require(profile in {"short", "long300"}, "unknown declared time profile")
    durable_flush_timeout_s = _durable_flush_timeout_s(scenario, profile)
    delta = 300 - original.phases[1][0] if profile == "long300" else 0
    if isinstance(raw_spec, asm.ComboSpec):
        if profile == "short" and scenario in asm.D25_D30_SHORT_PROFILE_SCENARIOS:
            spec = replace(original, purpose="smoke", phases=asm.D25_D30_SHORT_PHASES,
                           legs=tuple(replace(leg, window=window)
                                      for leg, window in zip(original.legs, asm.D25_D30_SHORT_WINDOWS)),
                           timing=replace(original.timing, decision=original.timing.decision + "; "
                                          + asm.D25_D30_SHORT_TIMING_NOTE))
        else:
            spec = replace(original, purpose="pilot" if profile == "long300" else "smoke",
                           phases=((0, 300), (300, 600), (600, 900)) if profile == "long300" else original.phases,
                           legs=tuple(replace(leg, window=(leg.window[0] + delta, leg.window[1] + delta))
                                      for leg in original.legs))
    else:
        
        # same long300 formula (build_plan_candidates.shifted precedent); the
        # AtomSpec itself stays the assembler input, the view feeds the shared
        # per-leg binding code below.
        raw_spec = replace(raw_spec, purpose="pilot" if profile == "long300" else "smoke",
                           phases=((0, 300), (300, 600), (600, 900)) if profile == "long300" else raw_spec.phases,
                           leg=replace(raw_spec.leg, window=(raw_spec.leg.window[0] + delta,
                                                             raw_spec.leg.window[1] + delta)))
        spec = isolated_entry_view(raw_spec)
    require(all(not carrier.parameterized or "user" not in carrier.path_prefix for carrier in spec.carriers),
            "user parameter condition must be explicitly resolved before this runtime binding")
    # M04: DB credentials through the established dotenv channel once, before
    # the binding (the order_no preflight below needs them; the db probe below
    # reuses the same read instead of a second dotenv pass).
    credentials = {}
    if enabled:
        from dotenv import dotenv_values
        values = portable_env.credentials()
        require(all(key in values and values[key] for key in
                    ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")),
                "database environment missing for the live binding face")
        credentials = {key: values[key] for key in ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")}
    order_streams = tuple(carrier.stream_id for carrier in spec.carriers
                          if carrier.path_prefix == ORDER_CARRIER_PREFIX)
    frozen_order = None
    if order_streams and enabled:
        # Fail-closed read-only preflight: any resolution failure refuses the
        # build with the response evidence retained; no fallback number.
        frozen_order = live.ReadonlyOrderNoProbe(execute=True, environ=credentials,
                                                 expected_server_uuid=DB_UUID).resolve()
    hashes = source_hashes()
    binding = asm.RunBinding("r10-" + scenario.lower() + "-" + profile, attempt, "r10-" + scenario.lower() + "-profile", 1, 1,
        str(ROOT), str(OUTPUT), CONTEXT, NAMESPACE, "r10-" + scenario.lower() + "-clock",
        "http://127.0.0.1:" + str(PORT), "0071341196",
        {"collector": c.canonical_sha256(hashes), "generator": hashes["workload"],
         "annotation_rules": hashlib.sha256(Path(a.__file__).read_bytes()).hexdigest(),
         "environment": c.canonical_sha256({"cluster_uid": CLUSTER_UID, "namespace_uid": NAMESPACE_UID})},
        "r10-unfrozen-engineering-profile-v1", credential_refs={"database": "env:DB_PASSWORD"})
    registry = asm.load_registry(repo_root=ROOT)
    assembled = asm.assemble_combo(spec, binding, registry) if isinstance(raw_spec, asm.ComboSpec)\
        else asm.assemble(raw_spec, binding, registry)
    pins, log_sources, scopes, targets, db_bindings = [], [], {}, [], []
    total_s = spec.phases[-1][1]
    main_deployment = main_carrier_deployment(spec)
    
    # the Kubernetes legs (require_ordinary guarantees at least one exists).
    primary_log_deployment = main_deployment if main_deployment in {leg.deployment for leg in spec.legs
                                                                    if isinstance(leg, asm.FaultLegSpec)}\
        else next(leg.deployment for leg in spec.legs if isinstance(leg, asm.FaultLegSpec))
    db_databases = {leg.database for leg in spec.legs if isinstance(leg, asm.DbFaultLegSpec)}
    require(len(db_databases) <= 1,
            "one database client binds exactly one schema; mixed-database legs need an explicit decision")
    for index, leg in enumerate(spec.legs, 1):
        fid = "F" + str(index)
        if isinstance(leg, asm.DbFaultLegSpec):
            
            # db_bindings + db_client through the runner discipline, never a
            
            # off-graph). Its required log source keeps the shared per-leg id
            # formula (runtime_rule_payload) and is backed by the catalog
            
            # mysql error log has no producer in this stack).
            db_bindings.append((fid, dbp.DbBinding(leg.database, leg.table)))
            sid = spec.telemetry.logs if leg.entity == parent_scope_leg(spec).entity\
                else spec.telemetry.logs + "-" + leg.entity
            limits = la.ArchiveLimits(max_total_bytes=20_000_000, max_read_bytes=262_144, max_chunk_bytes=65_536,
                max_buffered_bytes=262_144, max_events=20_000, max_streams=4, attach_timeout_s=15, eof_grace_s=25,
                flush_timeout_s=30, capture_timeout_s=total_s, join_timeout_s=30,
                durable_flush_timeout_s=durable_flush_timeout_s)
            scopes[sid] = lt.TransportScope(sid, "catalog", runtime["anchor_uid"], "catalog", "catalog",
                CLUSTER_UID, NAMESPACE_UID, limits, clock_uncertainty_s=.05, max_view_bytes=1_000_000,
                evidence_kind="observed" if enabled else "synthetic")
            log_sources.append(lt.DeploymentLogSource(sid, leg.entity, "catalog", runtime["anchor_uid"],
                                                      "catalog", False, ()))
            targets.append(None)
            continue
        pin = runtime["pins"][fid]
        pins.append((fid, p.PinnedTarget(CONTEXT, NAMESPACE, leg.entity, leg.deployment, pin["deployment_uid"],
            pin["container"], tuple(leg.selector.items()),
            (p.PodPin(pin["pod_name"], pin["pod_uid"], tuple(tuple(row) for row in pin["pod_images"])),))))
        sid = spec.telemetry.logs if leg.deployment == primary_log_deployment else spec.telemetry.logs + "-" + leg.entity
        limits = la.ArchiveLimits(max_total_bytes=20_000_000, max_read_bytes=262_144, max_chunk_bytes=65_536,
            max_buffered_bytes=262_144, max_events=20_000, max_streams=4, attach_timeout_s=15, eof_grace_s=25,
            flush_timeout_s=30, capture_timeout_s=total_s, join_timeout_s=30,
            durable_flush_timeout_s=durable_flush_timeout_s)
        
        # binds the stressor carrier Deployment, so the transport scope pins
        # the carrier (deployment_name = the carrier Deployment) while the log
        # source identity above keeps entity "host" -- the exact S05 split.
        # Every ordinary leg keeps the HEAD entity-named scope (entity ==
        # deployment for all of them, so no existing construction moves).
        scope = lt.TransportScope(sid, leg.deployment if leg.entity == "host" else leg.entity,
            pin["deployment_uid"], leg.selector["app"], pin["container"],
            CLUSTER_UID, NAMESPACE_UID, limits, clock_uncertainty_s=.05, max_view_bytes=1_000_000,
            evidence_kind="observed" if enabled else "synthetic")
        scopes[sid] = scope
        log_sources.append(lt.DeploymentLogSource(sid, leg.entity, leg.deployment, pin["deployment_uid"], pin["container"], False, ()))
        targets.append(live.ComponentReadTarget(leg.entity, spec.telemetry.components, leg.deployment,
                                                pin["deployment_uid"], leg.selector["app"], pin["container"]))
    if scenario == "S05":
        # The stressor is silent; pricing is the existing workload's service.
        
        observation_pin = runtime.get("observation_log_pins", {}).get("pricing")
        require(isinstance(observation_pin, dict) and observation_pin.get("deployment_uid")
                and observation_pin.get("container") == "pricing", "S05 pricing log identity missing")
        sid = "pricing-pod-logs"
        scopes[sid] = lt.TransportScope(sid, "pricing", observation_pin["deployment_uid"], "pricing", "pricing",
            CLUSTER_UID, NAMESPACE_UID, limits, clock_uncertainty_s=.05, max_view_bytes=1_000_000,
            evidence_kind="observed" if enabled else "synthetic")
        log_sources.append(lt.DeploymentLogSource(sid, "pricing", "pricing", observation_pin["deployment_uid"],
                                                 "pricing", False, ()))
    
    # host leg (the Deployment + StressChaos carrier shapes). Pure offline
    # rendering -- p.render_stressor_manifest / p.render_chaos_manifest are
    # the frozen shape authorities and never apply anything. The applied
    # carrier manifest differs from the renderer reference ONLY in the
    # recshop.dev/m1-owner annotation, which carries the contract owner_id
    # (S05 A1); the cleanup contract is the runbook D2 / S05 A2 rule (DELETE
    # the carrier Deployment, never scale-0). The StressChaos CRD itself keeps
    # flowing through the ordinary chaos_mesh primitive path -- the pinned
    # target above is the same provisioned-pins shape the S05 driver used.
    stressor_carriers = []
    if any(isinstance(leg, asm.FaultLegSpec) and leg.entity == "host" for leg in spec.legs):
        pinned = dict(pins)
        for fault in assembled.contract.faults:
            binding = pinned.get(fault.fault_instance_id)
            if binding is None or binding.entity != "host":
                continue
            prepared = p.prepare_fault(fault, binding, ownership=STRESSOR_SHAPE_OWNERSHIP)
            reference = p.render_stressor_manifest(prepared, image=STRESSOR_CARRIER_IMAGE,
                ownership=STRESSOR_SHAPE_OWNERSHIP, requests_cpu=STRESSOR_CARRIER_REQUESTS_CPU,
                requests_memory=STRESSOR_CARRIER_REQUESTS_MEMORY)
            applied = json.loads(c.canonical_json(reference))
            applied["metadata"]["annotations"] = {p.OWNER_ANNOTATION: assembled.contract.to_dict()["context"]["owner_id"]}
            require(applied["metadata"].get("labels") == reference["metadata"].get("labels")
                    and applied["spec"] == reference["spec"]
                    and applied["metadata"]["name"] == reference["metadata"]["name"]
                    and applied["metadata"]["namespace"] == reference["metadata"]["namespace"],
                    "stressor carrier manifest drifted from the frozen renderer shape")
            pod_spec = applied["spec"]["template"]["spec"]
            carrier_container = pod_spec["containers"][0]
            require(pod_spec.get("enableServiceLinks") is False and len(pod_spec["containers"]) == 1
                    and carrier_container["command"] == ["sleep", "infinity"]
                    and carrier_container.get("imagePullPolicy") == "IfNotPresent"
                    and "cpu" not in carrier_container.get("resources", {}).get("limits", {})
                    and p._cpu_cores(carrier_container["resources"]["requests"]["cpu"]) > 0
                    and bool(carrier_container["resources"]["requests"].get("memory")),
                    "stressor carrier shape contract violated (STRESSOR-RUNBOOK section 2)")
            stressor_carriers.append({
                "fault_instance_id": fault.fault_instance_id, "deployment": binding.deployment,
                "manifest": applied, "manifest_sha256": hashlib.sha256(raw(applied)).hexdigest(),
                "reference_manifest_sha256": hashlib.sha256(raw(reference)).hexdigest(),
                "stresschaos_reference_sha256": hashlib.sha256(
                    raw(p.render_chaos_manifest(prepared, ownership=STRESSOR_SHAPE_OWNERSHIP))).hexdigest(),
                "owner_annotation": applied["metadata"]["annotations"][p.OWNER_ANNOTATION],
                "carrier_shape": {"image": STRESSOR_CARRIER_IMAGE, "requests_cpu": STRESSOR_CARRIER_REQUESTS_CPU,
                                  "requests_memory": STRESSOR_CARRIER_REQUESTS_MEMORY,
                                  "command": ["sleep", "infinity"], "cpu_limit": None},
                "cleanup": "delete_deployment_never_scale_zero",
                "declaration": "offline shape declaration only; live carrier provisioning/cleanup "
                               "is a separately authorized driver face (S05/STRESSOR-RUNBOOK)"})
    
    # for every gateway-config leg. Pure declaration -- the immutable
    # ConfigMap / Deployment seed-source switch shapes, budgets and cleanup
    # contract the live path consumes; live execution itself stays on the
    # existing runner dispatch (_primitive_executor -> GatewayAdapter /
    # GatewayExecutor; recovery via journal.reconcile with crash-reopen
    # load_plan). Nothing is applied, journalled or dry-run here (the offline
    # preview applies nothing); g.prepare_gateway inside this loop is the P2
    # pure validation, so a pin/parameter shape outside the accepted set
    
    gateway_rollouts = []
    if any(isinstance(leg, asm.FaultLegSpec) and leg.mechanism == gw.MECHANISM for leg in spec.legs):
        pinned = dict(pins)
        for fault in assembled.contract.faults:
            if fault.to_dict().get("mechanism") != gw.MECHANISM:
                continue
            binding = pinned.get(fault.fault_instance_id)
            require(binding is not None, "gateway rollout leg lost its pinned target")
            request = gw.prepare_gateway(fault, binding)
            gateway_rollouts.append({
                "fault_instance_id": fault.fault_instance_id,
                "mechanism": gw.MECHANISM, "mechanism_version": gw.MECHANISM_VERSION,
                "directive": {request.directive: request.value},
                "binding": {"context": binding.context, "namespace": binding.namespace,
                            "entity": binding.entity, "deployment": binding.deployment,
                            "deployment_uid": binding.deployment_uid, "container": binding.container,
                            "selector": dict(binding.selector)},
                "plan_schema": gw.PLAN_SCHEMA, "transition_schema": gw.TRANSITION_SCHEMA,
                "active_path": gw.ACTIVE_PATH,
                # C4: the RO-baseline seed command is pinned byte-exact into
                # every plan (gateway.py _SEED_COMMAND; load_plan rejects drift).
                "seed_command": list(gw._SEED_COMMAND),
                "resource_intents": {
                    "owned_configmap_name_formula": "m1-gw-<sha256(journal ownership ':' fid)[:24]>",
                    "deployment_seed_source_switch": "catalog-gw volume switch to the owned ConfigMap "
                                                     "+ controlled rollout; never an in-place reload"},
                "settled_basis_checks": len(gw.SETTLED_BASIS),
                "facts_budgets": {"command_timeout_s": 120, "command_wait_timeout_s": 90},
                "switch_band": {"live_typical_s": "14-21", "live_tail_s": 28,
                                "declared_band_s": 25,
                                "measured_value": "transition durations_s.rollout_band_s "
                                                  "(integration contract item 8)",
                                "note": "inject/recover block through rollout settle "
                                        "(GatewayRolloutProfile: 20s rollout head + 5s C1 settle buffer)"},
                "recovery": "journal.reconcile restores the Deployment seed source and deletes the owned "
                            "ConfigMap; crash reopen = AttemptJournal.open_for_reconcile + adapter.load_plan",
                "declaration": "offline shape declaration only; live fail->pass and combo smoke are "
                               "separately authorized field faces (B2-01 section 4; P2 acceptance scope_note)"})
    rule_data = runtime_rule_payload(spec, assembled.rules.to_dict())
    rules = q.QualityRules.from_dict(rule_data)
    contract = assembled.contract.to_dict()
    contract["context"]["fingerprints"]["quality_rules"] = rules.sha256
    if order_streams and frozen_order is not None:
        _apply_frozen_order_no(contract, spec, frozen_order["order_no"])
    contract = c.RunContract.from_dict(contract, registry)
    assembled = replace(assembled, contract=contract, contract_dict=contract.to_dict(), rules=rules)
    factories = {sid: ldk.kubectl_driver_factory(scope, executable=portable_env.resolve_cli("kubectl") if enabled else "kubectl", process=live.BoundedProcess(execute=enabled), execute=enabled)
                 for sid, scope in scopes.items()}
    def log_factory(archive_scope):
        matches = [sid for sid, scope in scopes.items()
                   if scope.archive_scope(archive_scope.context, archive_scope.namespace) == archive_scope]
        require(len(matches) == 1, "log runtime scope ambiguous")
        return factories[matches[0]](archive_scope)
    capture = lt.LiveLogCapture(scopes, log_factory)
    kube = reader(enabled)
    environment = live.CatalogEnvironmentReader(kube, expected_cluster_uid=CLUSTER_UID,
        expected_namespace_uid=NAMESPACE_UID, expected_deployment_uid=runtime["anchor_uid"])
    # M04: `credentials` was read once above the binding (the order_no
    # preflight shares the established dotenv channel); no second read here.
    db_probe = BoundDb(enabled, credentials, spec.telemetry.checksum)
    
    
    # controlled_pilot mode requires; credentials are read lazily from the
    # established DB_* environment keys only when a live operation executes
    # (never during construction, never printed, never copied from .env).
    # Budgets are the S06 fixed set (precondition 3: the blocking recover path
    # fits the 35 s TAIL inside the during->post boundary).
    db_client = None
    if db_bindings:
        db_client = dbp.LiveDbCommandClient(enabled=enabled, env_prefix="DB_",
                                            expected_server_uuid=DB_UUID,
                                            expected_database=next(iter(db_databases)),
                                            connect_timeout_s=5.0, statement_timeout_s=8.0,
                                            socket_timeout_s=3.0)
    
    # SS2.5): a FRESH confirm_release readback inside the post_recovery window.
    # Examined ids close over the db client's own lock-session registry
    # (lock_session_ids -- real connect_lock_session records only, never
    # fabricated or zero-filled); source_id is the policy recovery.lock_source
    # (spec.telemetry.locks); construction reads no credentials, exactly like
    # the db client above. Lanes without db legs keep locks_probe=None and are
    # byte-for-byte on the old path.
    locks_probe = (live.OwnedLocksProbe(db_client,
                                        session_ids=db_client.lock_session_ids,
                                        tables=tuple(sorted({binding.table for _, binding in db_bindings})),
                                        source_id=spec.telemetry.locks)
                   if db_bindings else None)
    
    # the readback target list is the Kubernetes legs only; `targets` keeps the
    # per-leg alignment (None placeholder) for the parent-scope indexing below.
    readback = live.PhaseReadbackReader(kube, tuple(row for row in targets if row is not None),
                                        db_probe=db_probe, locks_probe=locks_probe)
    # enabled=False (preview/offline) never reads the exporter and never
    # constructs an observer; only the rule predeclaration above is unconditional.
    sampling_wiring = (resolve_all_sampling_observers(spec, targets, kube) if enabled
                       else {"observer": None, "status": "disabled_preview_offline"})
    # T01 engineering short smoke gets 50 s post readback inside its 60 s
    # recovery observation window. Keep every other phase,
    # scenario, and long300/formal profile on the existing 20 s cap.
    phase_capture_budgets_s = ({"post_recovery": 50}
                               if scenario == "T01" and profile == "short" else None)
    observer = obs.PhaseEvidenceCollector(readback, max_capture_s=20,
        phase_capture_budgets_s=phase_capture_budgets_s,
        sampling_observer=sampling_wiring["observer"])
    
    # 900s-protocol artifacts fail-closed with zero quality evaluation (pilot
    
    # telemetry bundle (plus workloads/phase observations/operations) in
    # run-record.json and quality/evidence.json, and _Artifacts.bundle embeds
    # records+base64 raws in each phase-modality bundle.json. Measured anchors:
    # D04 run-record content 15.5 MB (rejected at 8 MB); D09 died even earlier at
    # pre_fault/traces bundle.json ~9.9 MB; projected D09 run-record ~35 MB.
    # The T01 900 s three-stream pilot retained 75.79 MB of phase bundles;
    # run-record embeds those plus workloads and observations (>79 MB), and
    # quality-evidence embeds them again. Its measured 64/300 MB limits rejected
    # this real output after all three phases, so only T01 long300 gets bounded
    # 128/500 MB limits. Other faces keep 64/300 MB. Every consuming gate
    # (write-time,
    # journal-receipt merge, verify_artifacts read-back) keeps fail-closed
    # semantics. De-embedding instead of raising was rejected: bundle base64 is
    # the quality evaluator's integrity chain input (quality.py metric_values /
    # Q3 raw checks / _stored_files_match), so removing it would change the
    # quality evidence contract, not just the budget.
    policy = j.OutputPolicy.define(repo_root=ROOT, source_repo_root=OLD_ROOT, approved_root=OUTPUT,
                                    protected_roots=portable_env.protected_roots())
    artifact_bytes, total_artifact_bytes = ((128_000_000, 500_000_000)
                                            if spec.scenario_id == "T01" and profile == "long300"
                                            else (64_000_000, 300_000_000))
    facts = r.ComboEnvironmentFacts("controlled_pilot", r.EnvironmentIdentity(CLUSTER_UID, NAMESPACE_UID),
        LEASE_ROOT, policy,
        r.DeadlinePolicy(10, 15, lateness_budget, action_budget, _worker_join_timeout_s(scenario, profile), 20, .05, 90,
                         first_injection_start_lateness_s=first_injection_lateness_budget),
        a.TimingPolicy(contract.to_dict()["rules"]["timing"], 0, "staggered", ("simultaneous", "nested", "partial_overlap", "staggered")),
        r.ComboHttpBudgets("r10-read-only", .4, 4096, 8192, 65536, 2000), artifact_bytes, total_artifact_bytes,
        p.SubprocessClient(enabled=enabled, supports_delete_preconditions=True, supports_server_dry_run=True),
        
        # section R): command_timeout_s=120 / command_wait_timeout_s=90 for the
        # gateway rollout adapter (forced command>wait holds; live wait >= 90
        # aligns DESIGN SS7.2). The old 17/15 satisfied command>wait but sat
        # below the gateway switch band (live 14-21s typical / <=28s tail),
        # so any live rollout wait would time out. This moves the facts face
        # for EVERY combination (not GW-only) -- a declared lane drift.
        portable_env.resolve_cli("kubectl") if enabled else "kubectl", 120, 90, t.SourceSampling(None, None, None), _combo_trace_budget(spec.scenario_id), tuple(log_sources),
        t.HttpJsonClient(t.HttpReadPolicy(spec.telemetry.metrics, PROM, ("/api/v1/query_range",), 15, 4096, 8_000_000), execute=enabled),
        t.HttpJsonClient(t.HttpReadPolicy(spec.telemetry.traces, JAEGER, ("/api/traces",), 15, 4096, 8_000_000), execute=enabled),
        capture, t.CollectionLimits(100, 8000, 16_000_000), environment, tuple(pins),
        
        # (fid, DbBinding) pairs plus the validated database client here, into
        # the runner facts (empty tuple/None for every combo without a
        # database leg keeps the HEAD construction path unchanged).
        tuple(db_bindings), db_client,
        
        # corrected form) through to the facts face; None for every combo
        # without a declaration keeps the HEAD construction path unchanged.
        same_offset_budgets=spec.same_offset_budgets,
        log_capture=capture, trace_relations=trace_relations(), phase_observer=observer)
    face = r.build_combo_run_services(assembled, facts) if isinstance(assembled.spec, asm.ComboSpec)\
        else single_root_face(assembled, facts, spec)
    if sampling_wiring["observer"] is not None:
        for fid in sampling_wiring["observer"].sources:
            face = attach_sampling_reference(face, fid)
    provide = obs.build_phase_supplement(contract, rules)
    def supplement(record):
        require(callable(stop_outer), "outer worker drain callback required before persistence")
        drained = stop_outer()
        result = provide(record)
        result.setdefault("artifacts", {})["common-outer-worker-drained"] = drained
        return result
    services = replace(face.services, supplement=supplement if enabled else provide)
    
    # exactly the entries whose Q1 signal face consumes the route (pure spec
    # arithmetic; offline/preview evaluates it too and never touches kubectl).
    # Every other entry keeps the exact HEAD metadata dict -- no new key.
    aux_scope = pricing_aux_scope(raw_spec)
    return assembled, replace(face, services=services), {"source_hashes": hashes, "time_profile": profile,
        "active_windows_extended": False, "scope": "engineering_candidate_not_frozen", "runtime": runtime,
        **({"order_carrier_parameter": {
            "streams": list(order_streams),
            "order_no": frozen_order["order_no"] if frozen_order is not None else None,
            "resolution": ("read-only deterministic DB query (ORDER BY order_no ASC LIMIT 1), server identity "
                           "anchored, frozen before assembly and carrier probe"
                           if frozen_order is not None else
                           "offline declaration only; the live face resolves and freezes an existing order_no "
                           "before assembly (fail-closed, no fallback number)"),
            "receipt": frozen_order,
            "family_consistency": "deterministic oldest order_no: identical across the family's single/dual/"
                                  "triple arms while the order data is unchanged; a mid-run data change fails "
                                  "the arm-equality checks loudly instead of diverging silently",
            "precondition_owner": "assembly S09 spec.preconditions[0] (driver/runtime binding supplies and "
                                  "verifies the probe order_no; assembly never provisions)"}} if order_streams else {}),
        **({"stressor_carriers": tuple(stressor_carriers)} if stressor_carriers else {}),
        
        # combinations; every other combination keeps the exact HEAD metadata.
        **({"gateway_rollouts": tuple(gateway_rollouts)} if gateway_rollouts else {}),
        **({"source_sampling_wiring": {key: value for key, value in sampling_wiring.items() if key != "observer"}}
           if enabled else {}),
        **({"pricing_aux": aux_scope} if aux_scope is not None else {})}


def single_root_face(assembled, facts, spec):
    """collection item 3: the single-root construction face for an isolated entry.

    Mirrors build_combo_run_services's composition with the SAME runner
    helpers -- payload purity, the O-D3 timing audit, the per-leg binding
    discipline, per-leg telemetry inputs, the per-fid supplement merge and
    the operation-source law -- so an isolated arm carries the same driver
    duties as a combination leg. The two deltas are structural and honest:
    the carrier profile has ONE stream (the combo face's dual-stream
    allowlist law does not apply; workload.prepare_workload still demands the
    allowlist exactly cover the physical streams), and an atom declares no
    combo reference_dependencies, so the O-D4 soft check has nothing to
    expand. runner.py stays untouched: its >=2-leg gate is the lane law for
    combinations, and this face is the single-root road beside it."""
    contract, rules, binding = assembled.contract, assembled.rules, assembled.binding
    fids = ("F1",)
    purity = r._combo_payload_purity(spec, contract, fids)
    timing = r._combo_timing_audit(spec, facts.deadlines, facts.same_offset_budgets, fids)
    r._combo_binding_discipline(spec, binding, contract, facts, fids)
    streams = contract.to_dict()["request_profile"]["streams"]
    require(type(streams) is list and len(streams) == 1,
            "single-root face: exactly one carrier stream required")
    policy = w.HttpPolicy(facts.http.policy_id,
                          "isolated_test" if facts.mode == "isolated_test" else "read_only_business",
                          tuple(w.EndpointRule(stream["stream_id"], stream["method"], stream["entrypoint"],
                                               stream["endpoint"]) for stream in streams),
                          facts.http.max_lateness_s, facts.http.max_request_bytes,
                          facts.http.max_response_header_bytes, facts.http.max_response_body_bytes,
                          facts.http.max_planned_requests,
                          read_only_post_allowlist=w.sasrec_inference_post_allowlist(streams))
    telemetry = r._combo_telemetry_inputs(spec, contract, facts, fids)
    operation_source = spec.telemetry.operations
    require(type(operation_source) is str and bool(operation_source),
            "single-root face: spec.telemetry.operations required")
    require({row.get("operation_source_id") for row in rules.to_dict()["injection"]} == {operation_source},
            "single-root face: the quality rules' operation source must equal spec.telemetry.operations")
    supplement = r.build_combo_supplement(fids, facts.leg_supplements)
    settings = r.RunSettings(facts.environment, facts.lease_root, facts.output_policy, facts.deadlines,
                             policy, facts.annotation_policy, facts.mode,
                             facts.max_artifact_bytes, facts.max_total_artifact_bytes)
    services = r.RunServices(facts.primitive_client, facts.pins, facts.kubectl, facts.command_timeout_s,
                             facts.command_wait_timeout_s, telemetry, facts.environment_reader, rules,
                             operation_source, supplement, facts.db_bindings, facts.db_client,
                             phase_observer=facts.phase_observer)
    return r.ComboDriverFace(settings=settings, services=services,
                             timing_audit={**timing, "triple_subpairs": None, "payload_purity": purity},
                             reference_check={"rows": [], "pool_sizes": {}, "warnings": [],
                                              "note": "single-root atom: no combo reference_dependencies "
                                                      "to expand (O-D4 soft check is a combination duty)"},
                             warnings=())


def build_condition(condition, attempt, runtime, *, enabled=False, action_budget=17., lateness_budget=2.,
                    first_injection_lateness_budget=None, stop_outer=None, cleanup_only=False):
    """Execute a projected arm with its parent's complete observation face.

    Only the per-leg runtime bindings are reduced (collection item 2: Kubernetes pins
    and database bindings are projected onto this arm's fault set). Query ids
    (including parent F2), log sources and component observations retain all
    parent runtime identities.
    """
    
    # contrast arm -- its parent is the atom itself (source_combo None names
    
    # executes the atom under a condition identity with its own full
    # observation face. Every other unknown parent keeps the refusal.
    require(type(condition) is dict and (condition.get("source_combo") in asm.COMBO_SPECS
            or (condition.get("source_combo") is None and condition.get("scenario_id") in asm.ATOM_SPECS)),
            "explicit parent contrast condition required")
    registry = asm.load_registry(repo_root=ROOT)
    source_contract = c.RunContract.from_dict(condition["contract"], registry)
    source_rules = q.QualityRules.from_dict(condition["quality_rules_payload"])
    c._validated_quality_payload(source_contract, source_rules.to_dict())
    
    # admission discipline is enforced at the runner entry (r.validate_run /
    # r.run_attempt -> _validate_release_entry -> _validate_fast16_entry):
    # a formal condition enters execution only when the FAST16 ladder
    # (OFFLINE_COMPLETE + REPRESENTATIVE_VERIFIED) is reached under the
    # current fingerprints, the condition respects the first-block/expansion
    # discipline, and repetitions require CONDITION_VERIFIED. The first case
    # never demands its own completion results. This driver adds no second
    # gate and no bypass; preview validates the same entry.
    source_phases = source_contract.to_dict()["phases"]
    lengths = [source_phases[name]["end"] - source_phases[name]["start"] for name in ("pre_fault", "during_fault", "post_recovery")]
    profile = "long300" if lengths == [300, 300, 300] else "short"
    
    parent, parent_face, metadata = build(condition["source_combo"] or condition["scenario_id"], attempt,
        runtime, enabled=enabled, profile=profile, action_budget=action_budget,
        lateness_budget=lateness_budget, first_injection_lateness_budget=first_injection_lateness_budget,
        stop_outer=stop_outer)
    data, original = source_contract.to_dict(), parent.contract.to_dict()
    require(data["request_profile"] == original["request_profile"], "condition request profile differs from parent observation face")
    # The collector equality is the drift gate for fresh dispatches; the
    # BLOCKED-recovery path (execute_case cleanup_only) deliberately bypasses
    # ONLY this equality -- its drift audit already records the source change
    # and the recovery itself swaps in the persisted original contract bytes
    # (same bypass-not-satisfaction semantics as the runner's
    # PreparedAttemptSession cleanup_only, 77009d5f).
    if not cleanup_only:
        require(data["context"]["fingerprints"]["collector"] == c.canonical_sha256(metadata["source_hashes"]),
                "condition collector source changed; regenerate and review conditions")
    for phase in data["phases"]:
        require({k: v for k, v in data["phases"][phase].items() if k != "clock_id"}
                == {k: v for k, v in original["phases"][phase].items() if k != "clock_id"}, "condition phase geometry differs")
    originals = {fault["atom_id"]: fault for fault in original["faults"]}
    local_to_parent = {}
    for fault in data["faults"]:
        old = originals.get(fault["atom_id"])
        require(old is not None, "control atom is not in its parent")
        left, right = json.loads(raw(fault)), json.loads(raw(old))
        local_to_parent[left.pop("fault_instance_id")] = right.pop("fault_instance_id")
        for row in (left, right):
            row["planned_window"].pop("clock_id")
        require(left == right, "control changed parent target, mechanism, dose or active window")
    expected_rules = parent.rules.to_dict()
    parent_to_local = {value: key for key, value in local_to_parent.items()}
    expected_rules["injection"] = [{**rule, "instance_id": parent_to_local[rule["instance_id"]]}
        for rule in expected_rules["injection"] if rule["instance_id"] in parent_to_local]
    require(expected_rules == source_rules.to_dict(), "condition quality/observation scope differs from projected parent")
    
    # a condition that drops every consuming leg runs on the manifest-default
    # direct route exactly like an unaffected arm (the runner auxiliary binds
    # only this arm's actual gateway fault ids, and an arm without them cannot
    # carry the plan at all).  Kept consuming legs are re-keyed to the local
    # fids; the declaration is advisory (mint_pricing_aux re-derives the exact
    # binding from the executing contract), but its presence/absence decides
    # which execution face the run step takes.  The ln coexisting legs follow
    # the same kept-leg law (_projected_pricing_aux).
    parent_aux = metadata.get("pricing_aux")
    if parent_aux is not None:
        projected = _projected_pricing_aux(parent_aux, parent_to_local)
        if projected is None:
            metadata.pop("pricing_aux")
        else:
            metadata["pricing_aux"] = projected
    context = data["context"]
    run_id = "r10c-" + c.canonical_sha256({"condition_id": condition["condition_id"]})[:20]
    clock_id = "clock-" + run_id
    context.update(run_id=run_id, attempt_id=attempt, owner_id=run_id + "/" + attempt, clock_id=clock_id,
        contract_ref="condition:" + condition["condition_id"] + ":" + source_contract.sha256,
        evidence_root=str(Path(context["output_root"]) / data["purpose"] / run_id / attempt))
    for window in data["phases"].values():
        window["clock_id"] = clock_id
    for fault in data["faults"]:
        fault["planned_window"]["clock_id"] = clock_id
    contract = c.RunContract.from_dict(data, registry)
    policy = j.OutputPolicy.define(repo_root=ROOT, source_repo_root=OLD_ROOT, approved_root=Path(context["output_root"]),
                                   protected_roots=portable_env.protected_roots())
    settings = replace(parent_face.settings, output_policy=policy,
        deadlines=replace(parent_face.settings.deadlines,
            worker_join_timeout_s=_worker_join_timeout_s(data["scenario"]["scenario_id"], profile)))
    
    # the parent face's per-leg runtime bindings are PROJECTED onto this arm's
    # fault set, never inherited wholesale. A database leg carries no
    
    # leg whose parent fid has no pin takes the parent's db binding instead,
    # re-keyed to the local fid; a leg the condition drops drops its binding
    # with it. The coverage law is the runner's own (validate_run: pins |
    # db_bindings == exactly the condition's fault ids); the guard below
    # fails closed if a parent binding shape appears outside both registers.
    parent_pins = dict(parent_face.services.pinned_targets)
    parent_db = dict(parent_face.services.db_bindings)
    pins = tuple((local, parent_pins[parent_fid]) for local, parent_fid in local_to_parent.items()
                 if parent_fid in parent_pins)
    db_bindings = tuple((local, parent_db[parent_fid]) for local, parent_fid in local_to_parent.items()
                        if parent_fid in parent_db)
    require(len(pins) + len(db_bindings) == len(local_to_parent),
            "condition projection lost a parent runtime leg binding")
    provide = obs.build_phase_supplement(contract, source_rules)
    def supplement(record):
        require(callable(stop_outer), "outer worker drain callback required")
        drain = stop_outer()
        value = provide(record)
        value.setdefault("artifacts", {})["common-outer-worker-drained"] = drain
        return value
    services = replace(parent_face.services, pinned_targets=pins, db_bindings=db_bindings,
                       quality_rules=source_rules, supplement=supplement if enabled else provide)
    # AssembledRun is a carrier for the immutable actual contract. The parent
    # spec here describes the observation face, not this arm's truth set.
    assembled = replace(parent, contract=contract, contract_dict=contract.to_dict(), rules=source_rules)
    face = replace(parent_face, settings=settings, services=services,
        timing_audit={**parent_face.timing_audit, "scope": "parent_budget_with_exact_control_fault_windows",
                      "actual_fault_windows": {fault.fault_instance_id: fault.planned_window.to_dict() for fault in contract.faults}})
    
    # pilot case06 ledger evidence): the stressor carrier's applied
    # annotation names the EXECUTING contract owner. The parent build stamped
    # the parent binding owner into the declaration; this arm executes under
    # the r10c owner_id rewritten above, so the declared applied manifest is
    # re-stamped to it. Labels, spec, name and namespace stay byte-identical
    # (the annotation remains the only applied-vs-reference delta, exactly as
    # in build()), and manifest_sha256 is recomputed so a write-ahead ledger
    # quoting it names this arm's shape. The frozen renderer reference shas
    # are owner-independent and carry over unchanged.
    if metadata.get("stressor_carriers"):
        owner = context["owner_id"]
        restamped = []
        for carrier in metadata["stressor_carriers"]:
            applied = json.loads(raw(carrier["manifest"]))
            applied["metadata"]["annotations"] = {p.OWNER_ANNOTATION: owner}
            require(applied["metadata"].get("labels") == carrier["manifest"]["metadata"].get("labels")
                    and applied["spec"] == carrier["manifest"]["spec"]
                    and applied["metadata"]["name"] == carrier["manifest"]["metadata"]["name"]
                    and applied["metadata"]["namespace"] == carrier["manifest"]["metadata"]["namespace"],
                    "stressor carrier manifest drifted while re-stamping the run owner")
            restamped.append({**carrier, "manifest": applied,
                              "manifest_sha256": hashlib.sha256(raw(applied)).hexdigest(),
                              "owner_annotation": owner})
        metadata["stressor_carriers"] = tuple(restamped)
    r.validate_run(contract, settings, services)
    metadata.update(condition_id=condition["condition_id"], condition_owner_id=context["owner_id"],
                    source_condition_contract_sha256=source_contract.sha256,
        source_condition_sha256=c.canonical_sha256(condition), actual_contract_sha256=contract.sha256,
        local_fault_to_parent=local_to_parent, full_observation_runtime=runtime,
        observation_queries=[{"query_id": query.query_id, "entity": query.entity, "expression": query.expression,
                               "source_id": query.source_id} for query in services.telemetry.metric_queries],
        truth_entities=list(contract.root_entities), injected_pin_ids=list(local_to_parent))
    return assembled, face, metadata


def no_proxy():
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


class ProxyControl:
    """Reuse the accepted S21 suspended Windows Job ownership discipline."""
    def __init__(self, path, contract):
        self.path, self.identity = path, {"contract_sha256": contract.sha256,
            "run_id": contract.context.to_dict()["run_id"], "attempt_id": contract.context.to_dict()["attempt_id"]}
        self.process = self.job = None
        self.drained, self.may_be_running, self.receipt = False, False, None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("xb"):
            pass

    def record(self, status, **extra):
        row = {**self.identity, "status": status, "timestamp_epoch_s": time.time(), **extra}
        with self.path.open("ab") as stream:
            stream.write(raw(row) + b"\n"); stream.flush(); os.fsync(stream.fileno())
        return row

    def start(self):
        require(os.name == "nt", "accepted Windows Job process ownership required")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as check:
            check.bind(("127.0.0.1", PORT))
        self.record("START_INTENT")
        self.may_be_running = True
        try:
            self.job = live._WindowsJob()
            env = os.environ.copy(); env.update(NO_PROXY="*", no_proxy="*")
            self.process = subprocess.Popen([portable_env.resolve_cli("kubectl"), "--context", CONTEXT, "proxy", "--port", str(PORT), "--address=127.0.0.1"],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | 0x00000004 | subprocess.CREATE_NO_WINDOW, env=env)
            self.job.attach_and_resume(self.process)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                require(self.process.poll() is None, "owned proxy exited during startup")
                try:
                    for name, expected in (("kube-system", CLUSTER_UID), (NAMESPACE, NAMESPACE_UID)):
                        with no_proxy().open(f"http://127.0.0.1:{PORT}/api/v1/namespaces/{name}", timeout=1) as reply:
                            data = reply.read(2_000_001)
                        require(len(data) <= 2_000_000 and json.loads(data)["metadata"]["uid"] == expected,
                                "owned proxy wrong physical environment")
                    self.record("RUNNING", pid=self.process.pid, ownership="kill_on_close_windows_job")
                    return
                except urllib.error.URLError:
                    time.sleep(.2)
            raise r.RunnerError("owned proxy startup deadline")
        except BaseException:
            self.stop()
            raise

    def stop(self):
        if self.drained:
            return self.receipt
        try:
            if self.job is not None:
                self.job.close()
            if self.process is not None:
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill(); self.process.wait(timeout=5)
            self.may_be_running = False
            self.receipt = self.record("DRAIN_CONFIRMED", owned_handle_returned=self.process is not None,
                                       live_status_scope="owned_process_exit_only_or_no_process_started")
            self.drained = True
            return self.receipt
        except BaseException:
            self.record("UNCONFIRMED")
            raise r.RunnerError("owned proxy drain unconfirmed") from None


def require_prior_outer_drained():
    for path in ARTIFACTS.glob("WORKER-*.jsonl"):
        rows = [json.loads(line) for line in path.read_bytes().splitlines() if line]
        require(rows and rows[-1].get("status") == "DRAIN_CONFIRMED", "prior common proxy needs explicit drain audit")


def probe_carriers(assembled, directory):
    spec, contract = assembled.spec, assembled.contract
    mappings = dict(trace_relations())
    service_names = {entity: service for service, entity in mappings.items()}
    results = []
    for ordinal, stream in enumerate(contract.to_dict()["request_profile"]["streams"], 1):
        
        # AtomSpec singular field; a combination keeps the plural tuple.
        carriers = spec.carriers if isinstance(spec, asm.ComboSpec) else (spec.carrier,)
        carrier = carriers[ordinal - 1]
        entity = carrier.service.split(":", 1)[0]
        
        
        # an undeclared carrier keeps the HEAD entity reverse-lookup unchanged.
        if carrier.trace_service is None:
            require(entity in service_names, "carrier trace service mapping undeclared")
            trace_service = service_names[entity]
        else:
            require(carrier.trace_service in mappings, "carrier trace service mapping invalid (unknown service name)")
            trace_service = carrier.trace_service
        trace_id = uuid.uuid4().hex
        traceparent = "00-" + trace_id + "-" + uuid.uuid4().hex[:16] + "-01"
        post_permissions = w.sasrec_inference_post_allowlist([stream])
        if post_permissions:
            body = c.canonical_json(stream["parameters"]["json"]).encode("utf-8")
            require(hashlib.sha256(body).hexdigest() == post_permissions[0].payload_sha256,
                    "SASRec pre-probe body differs from its explicit read-only POST permission")
            request = urllib.request.Request(stream["entrypoint"] + stream["endpoint"], data=body,
                headers={"Content-Type": "application/json", "traceparent": traceparent}, method="POST")
        else:
            request = urllib.request.Request(stream["entrypoint"] + stream["endpoint"],
                headers={"traceparent": traceparent}, method="GET")
        start = time.time()
        with no_proxy().open(request, timeout=stream["timeout_s"]) as reply:
            payload = reply.read(32_769); status = reply.status
        end = time.time()
        require(status == 200 and len(payload) <= 32_768, "carrier pre-probe failed")
        response = write_new(directory / ("probe-" + str(ordinal) + ".response.raw"), payload)
        deadline, poll, seen = time.monotonic() + 20, 0, set()
        while time.monotonic() < deadline:
            poll += 1
            backend = t.HttpJsonClient(t.HttpReadPolicy(spec.telemetry.traces, JAEGER, ("/api/traces",),
                min(3, max(.1, deadline - time.monotonic())), 4096, 4_000_000), execute=True)
            result = backend.fetch_json("/api/traces", {"service": trace_service, "start": int((start - .05) * 1e6),
                "end": int((end + .05) * 1e6), "limit": 80})
            receipt = write_new(directory / f"probe-{ordinal}-{poll:03d}.trace.raw", result.raw)
            write_new(directory / f"probe-{ordinal}-{poll:03d}.json", {"raw": receipt, "status": result.status,
                "provenance": result.provenance, "trace_id": trace_id, "request_window": [start, end]})
            if result.status == "ok":
                for trace in json.loads(result.raw).get("data", []):
                    if trace.get("traceID") != trace_id:
                        continue
                    for span in trace.get("spans", []):
                        stamp = span.get("startTime")
                        if span.get("traceID") == trace_id and type(stamp) is int and start - .05 <= stamp / 1e6 <= end + .05:
                            name = trace.get("processes", {}).get(span.get("processID"), {}).get("serviceName")
                            if name:
                                seen.add(name)
                require(seen <= set(mappings), "pre-probe returned undeclared service")
                if trace_service in seen:
                    break
            time.sleep(.25)
        require(trace_service in seen and time.monotonic() < deadline, "new carrier trace deadline exceeded")
        results.append({"stream_id": stream["stream_id"], "trace_id": trace_id, "services": sorted(seen),
                        "request_window": [start, end], "response": response, "polls": poll})
    return results


def _recovery_drift_audit(saved, destination, attempt):
    """collection (minimum-use checks v0.7, P0-2 process-recoverable): audited cleanup recovery.

    A run whose source drifted mid-flight is ALREADY disqualified by the
    runner's drift gate -- it can never qualify as evidence again. Refusing
    its cleanup leaves faults and the environment lease stuck forever, which
    contradicts the recoverability P0. Admit a cleanup-and-lease-closure
    recovery ONLY for attempts whose own run summary says BLOCKED (never
    sample-eligible), and record a full drift audit (old/new hashes, changed
    modules, blocked error). Any other attempt keeps the exact-source
    refusal.
    """
    summary_path = destination / "SUMMARY.json"
    require(summary_path.is_file(), "recovery source changed and no run summary to audit")
    summary = json.loads(summary_path.read_bytes())
    require(summary.get("status") == "BLOCKED",
            "recovery source changed; exact source audit required (attempt is not BLOCKED)")
    fresh = source_hashes()
    write_new(destination / ("RECOVERY-DRIFT-AUDIT-" + uuid.uuid4().hex + ".json"), {
        "schema_version": "rq4-collect/recovery-drift-audit-r51-v1",
        "attempt_id": attempt, "run_id": summary.get("run_id"),
        "blocked_status": summary.get("status"),
        "blocked_error": str(summary.get("error"))[:500],
        "old_source_hashes": saved["source_hashes"], "new_source_hashes": fresh,
        "changed_modules": sorted(key for key in set(saved["source_hashes"]) | set(fresh)
                                  if saved["source_hashes"].get(key) != fresh.get(key)),
        "semantics": "cleanup and lease closure only; the BLOCKED attempt never "
                     "qualifies as evidence under either source state"})


def recovery_supplier(assembled, face):
    contract = assembled.contract
    observer = face.services.phase_observer
    def supply(expected):
        first = observer.reader.db.capture(contract, "post_recovery")
        packet = observer.reader.capture_phase(contract, "post_recovery", deadline_epoch_s=time.time() + 20)
        second = packet["database"]
        components = [{**row, "phase": "recovery_qualification", "diagnostic_scope": "fresh_recovery_not_sample_phase"}
                      for row in packet["components"] if row["entity"] in expected["component_sources"]]
        return {"components": components, "checksums": {
            **{key: expected[key] for key in ("contract_sha256", "run_id", "attempt_id", "evidence_kind")},
            "source_id": expected["checksum_source"], "return_code": 0,
            "pre_at_s": first["timestamp_epoch_s"], "post_at_s": second["timestamp_epoch_s"],
            "tables": {table: {"pre": first["checksums"][table], "post": second["checksums"][table]} for table in CHECKSUMS},
            "baseline": CHECKSUMS, "diagnostic_probes": [first, second]}}
    return supply


def recover_exact(assembled, face):
    contract, settings, services = assembled.contract, face.settings, face.services
    state_path = settings.lease_root / (settings.environment.key + ".state.json")
    require(state_path.is_file(), "exact attempt lease missing")
    state = json.loads(state_path.read_bytes())
    require(state.get("attempt", {}).get("contract_sha256") == contract.sha256, "lease names another attempt")
    if state["status"] == "clean":
        require(state.get("workers") == "drained", "clean lease without drain evidence")
        return {"status": "ALREADY_CLEAN", "sample_eligibility": "NOT_CHANGED", "lease": state}
    return r.qualify_recovery(contract, settings, services, recovery_supplier(assembled, face), execute=True)


def recover_pricing_aux_attempt(assembled, face, destination, attempt, *, cleanup_only=False):
    """collection: prepared recovery for one pricing-route-premise attempt.

    Rebuilds the exact persisted plan -- the journal/lease chain compares
    plan.to_dict() equality, so the plan can never be re-minted from a fresh
    baseline read (resource_version moves with every observation) -- and
    dispatches through the runner's prepared recovery entry, including the
    bootstrap-abort road when preparation died before the lease ever issued
    mutation authorization (auxiliary.initialization == INITIALIZING).
    """
    contract, settings, services = assembled.contract, face.settings, face.services
    plan_path = destination / ("AUX-PLAN-" + attempt + ".json")
    require(plan_path.is_file(), "pricing auxiliary attempt requires its persisted plan record")
    record = json.loads(plan_path.read_bytes())
    require(type(record) is dict and record.get("schema_version") == PRICING_AUX_PLAN_SCHEMA
            and record.get("contract_sha256") == contract.sha256, "persisted pricing aux plan names another contract")
    plan = pricing_aux_plan_from_dict(record["plan"])
    adapter = pr.PricingRouteAdapter(CONTEXT, NAMESPACE, CLUSTER_UID, NAMESPACE_UID,
                                     process=live.BoundedProcess(execute=True), evidence_kind="observed",
                                     cleanup_only=cleanup_only, kubectl=portable_env.resolve_cli("kubectl"))
    supplier = recovery_supplier(assembled, face)
    state_path = Path(settings.lease_root) / (settings.environment.key + ".state.json")
    require(state_path.is_file(), "exact attempt lease missing")
    state = json.loads(state_path.read_bytes())
    if state.get("auxiliary", {}).get("initialization") == "INITIALIZING":
        return r.abort_prepared_initialization(contract, settings, services.environment_reader, (plan,),
                                               {PRICING_AUX_ID: adapter}, assembled.rules, supplier, execute=True)
    return r.recover_prepared_attempt(contract, settings, services.environment_reader, (plan,),
                                      {PRICING_AUX_ID: adapter}, assembled.rules, supplier, services=services, execute=True,
                                      cleanup_only=cleanup_only)


def _verify_stressor_drift(metadata, host_face):
    """M03: the live build's stressor declaration must be the provisioned one.

    The comparison key is the owner-independent reference manifest sha (the
    condition face re-stamps the applied annotation to the executing owner,
    which is expected and does not change the carrier shape).
    """
    if host_face is None:
        return
    declared = metadata.get("stressor_carriers") or ()
    require(len(declared) == len(host_face["carriers"]),
            "live stressor declaration count drifted from the provisioned carriers")
    for carrier, provisioned in zip(declared, host_face["carriers"]):
        require(carrier["reference_manifest_sha256"] == provisioned["reference_manifest_sha256"],
                "live stressor declaration drifted from the provisioned carrier manifest")


def execute_case(args):
    portable_env.require_windows_execution()
    portable_env.resolve_cli("kubectl")
    portable_env.assert_live()
    if getattr(args, "step", None) == "run":
        portable_env.resolve_cli("docker")
    require(args.live, "run/recover requires --live")
    require_prior_outer_drained()
    supplied_condition = getattr(args, "condition", None)
    label = supplied_condition["condition_id"] if supplied_condition is not None else args.scenario.lower() + "-" + args.profile
    stem = label + "-" + args.attempt
    destination = ARTIFACTS / stem
    require(destination.resolve() == destination and ".." not in destination.parts, "explicit unredirected attempt report required")
    input_path = destination / "recovery-input.json"
    if args.step == "recover":
        saved = json.loads(input_path.read_bytes())
        cleanup_only = False
        if saved["source_hashes"] != source_hashes():
            _recovery_drift_audit(saved, destination, args.attempt)
            cleanup_only = True
        if saved.get("condition") is not None:
            assembled, face, metadata = build_condition(saved["condition"], args.attempt, saved["runtime"], enabled=True,
                action_budget=saved["action_budget"], lateness_budget=saved["lateness_budget"],
                first_injection_lateness_budget=saved.get("first_injection_lateness_budget"),
                cleanup_only=cleanup_only)
        else:
            assembled, face, metadata = build(args.scenario, args.attempt, saved["runtime"], enabled=True,
                profile=args.profile, action_budget=saved["action_budget"], lateness_budget=saved["lateness_budget"],
                first_injection_lateness_budget=saved.get("first_injection_lateness_budget"))
        if cleanup_only:
            # The rebuild can only differ from the persisted contract in the
            # collector fingerprint (it reflects the current tree); the drift
            # audit above already records that change. The recovery itself
            # must run under the ORIGINAL persisted bytes -- the lease and the
            # persisted aux plan bind the original contract sha.
            original = c.RunContract.from_dict(saved["contract"], asm.load_registry(repo_root=ROOT))
            assembled = replace(assembled, contract=original, contract_dict=saved["contract"],
                                rules=q.QualityRules.from_dict(saved["rules"]))
        else:
            require(assembled.contract.to_dict() == saved["contract"] and assembled.rules.to_dict() == saved["rules"],
                    "recovery must use the original persisted contract and rules")
        if saved.get("pricing_aux") is None:
            result = recover_exact(assembled, face)
        else:
            result = recover_pricing_aux_attempt(assembled, face, destination, args.attempt, cleanup_only=cleanup_only)
        
        # RECOVERY write carries the runner's in-memory RecoveryReport fields
        
        # journal/lease evidence chain is unaffected.
        write_new(destination / ("RECOVERY-" + uuid.uuid4().hex + ".json"), summary_jsonable(result))
        # M03: a crashed host-leg run may have left its OWNED stressor carrier
        # Deployment behind (the run branch deletes it in its finally); the
        # recover step deletes it after the recovery attempt too -- a carrier
        # that was never provisioned is a no-op (--ignore-not-found).
        carriers = tuple(metadata.get("stressor_carriers") or ())
        if carriers:
            delete_stressor_carriers({"carriers": [{"deployment": row["deployment"],
                                                    "manifest_sha256": row["manifest_sha256"]}
                                                   for row in carriers]}, destination, attempt=args.attempt)
        return result
    require(not destination.exists(), "run/attempt already exists")
    
    
    spec = entry_view(entry_spec((supplied_condition["source_combo"] or supplied_condition["scenario_id"])
                                 if supplied_condition is not None else args.scenario))
    runtime, host_face = live_runtime_snapshot(spec, spec.scenario_id, args.attempt, destination,
                                               profile=args.profile, action_budget=args.action_budget,
                                               lateness_budget=args.lateness_budget)
    control_box = {}
    if supplied_condition is not None:
        assembled, face, metadata = build_condition(supplied_condition, args.attempt, runtime, enabled=True,
            action_budget=args.action_budget, lateness_budget=args.lateness_budget,
            first_injection_lateness_budget=args.first_injection_lateness_budget,
            stop_outer=lambda: control_box["control"].stop())
        restamp_live_carrier_owners(metadata, destination, attempt=args.attempt)
    else:
        assembled, face, metadata = build(args.scenario, args.attempt, runtime, enabled=True, profile=args.profile,
            action_budget=args.action_budget, lateness_budget=args.lateness_budget,
            first_injection_lateness_budget=args.first_injection_lateness_budget,
            stop_outer=lambda: control_box["control"].stop())
    require(not face.settings.output_policy.attempt_path(assembled.contract).exists(), "sample attempt exists")
    _verify_stressor_drift(metadata, host_face)
    inner = recovery = error = None
    probes = []
    try:
        
        # is deleted after the recovery attempt in the finally below -- DELETE,
        # never scale-0; the Chaos CRD recovery itself stays on the runner
        # journal discipline.
        if metadata.get("pricing_aux") is None:
            
            # recovery-input payload carries the driver metadata verbatim, and the
            
            
            # For already-JSON payloads the shaping is an identity, so every HEAD
            # -writable recovery-input keeps its exact bytes.
            write_new(input_path, summary_jsonable({**metadata, "contract": assembled.contract.to_dict(), "rules": assembled.rules.to_dict(),
                "action_budget": args.action_budget, "lateness_budget": args.lateness_budget,
                "first_injection_lateness_budget": args.first_injection_lateness_budget,
                "condition": supplied_condition}))
            control = ProxyControl(ARTIFACTS / ("WORKER-" + stem + ".jsonl"), assembled.contract)
            control_box["control"] = control
            try:
                control.start()
                probes = probe_carriers(assembled, destination)
                require(metadata["source_hashes"] == source_hashes(), "source changed during preparation")
                inner = r.run_attempt(assembled.contract, face.settings, face.services, execute=True)
            except BaseException as exc:
                error = type(exc).__name__ + ": " + str(exc)[:300]
            finally:
                try:
                    control.stop()
                except BaseException as exc:
                    error = error or type(exc).__name__ + ": outer drain failed"
            if control.drained and face.settings.output_policy.attempt_path(assembled.contract).is_dir():
                try:
                    require(metadata["source_hashes"] == source_hashes(),
                            "source drift blocks new recovery qualification; loaded fault cleanup is retained")
                    recovery = recover_exact(assembled, face)
                except BaseException as exc:
                    recovery = {"status": "BLOCKED", "error_type": type(exc).__name__, "reason": str(exc)[:300]}
        else:
            
            # DIRECT baseline, authorize through the runner's prepared entry, switch
            # the route (setup settles before anything below observes workload),
            # then re-bind the runtime pins to the post-switch serving Pod -- the
            # route rollout replaces the pricing Pod, and a pricing-rooted leg
            # (T01-F1/D22-F1 cpu carriers) must never pin the pre-switch corpse the
            # CRD selector would otherwise name.  The contract/rules rebuild
            # byte-identical (the runner's consume gate re-verifies); the write-ahead
            # recovery-input is deferred past the switch so the persisted runtime is
            # the serving one, and still precedes every workload/proxy/attempt step.
            # Cleanup is NOT here: run_attempt ends AWAITING_AUXILIARY_RECOVERY and
            # the prepared recovery below restores the direct route after the
            # post_recovery window and worker drain (runner-owned ordering).
            session = None
            outer_begun = False
            control = None
            try:
                plans, adapters, _aux_plan_receipt = mint_pricing_aux(assembled.contract, destination, args.attempt)
                clock = r.SystemClock()
                session = r.prepare_attempt(assembled.contract, face.settings, face.services.environment_reader,
                                            plans, adapters, assembled.rules, execute=True, clock=clock,
                                            execution_kind="registered_sample")
                session.prepare_auxiliaries()
                fresh = snapshot(spec, enabled=True)
                if supplied_condition is not None:
                    rebuilt, rebuilt_face, metadata = build_condition(supplied_condition, args.attempt, fresh, enabled=True,
                        action_budget=args.action_budget, lateness_budget=args.lateness_budget,
                        first_injection_lateness_budget=args.first_injection_lateness_budget,
                        stop_outer=lambda: control_box["control"].stop())
                else:
                    rebuilt, rebuilt_face, metadata = build(args.scenario, args.attempt, fresh, enabled=True, profile=args.profile,
                        action_budget=args.action_budget, lateness_budget=args.lateness_budget,
                        first_injection_lateness_budget=args.first_injection_lateness_budget,
                        stop_outer=lambda: control_box["control"].stop())
                require(rebuilt.contract.to_dict() == assembled.contract.to_dict()
                        and rebuilt.rules.to_dict() == assembled.rules.to_dict(),
                        "pricing route switch changed the assembled contract")
                require(metadata.get("pricing_aux") is not None, "pricing route premise declaration lost after rebuild")
                assembled, face = rebuilt, rebuilt_face
                _verify_stressor_drift(metadata, host_face)
                write_new(input_path, summary_jsonable({**metadata, "contract": assembled.contract.to_dict(), "rules": assembled.rules.to_dict(),
                    "action_budget": args.action_budget, "lateness_budget": args.lateness_budget,
                    "first_injection_lateness_budget": args.first_injection_lateness_budget,
                    "condition": supplied_condition}))
                control = ProxyControl(ARTIFACTS / ("WORKER-" + stem + ".jsonl"), assembled.contract)
                control_box["control"] = control
                session.begin_outer_workers()
                outer_begun = True
                control.start()
                probes = probe_carriers(assembled, destination)
                require(metadata["source_hashes"] == source_hashes(), "source changed during preparation")
                inner = r.run_attempt(assembled.contract, face.settings, face.services, execute=True,
                                      prepared_session=session, clock=clock)
            except BaseException as exc:
                error = type(exc).__name__ + ": " + str(exc)[:300]
            finally:
                if control is not None:
                    try:
                        control.stop()
                    except BaseException as exc:
                        error = error or type(exc).__name__ + ": outer drain failed"
                if session is not None:
                    try:
                        if outer_begun:
                            session.end_outer_workers(drained=control is not None and control.drained)
                    except BaseException as exc:
                        error = error or type(exc).__name__ + ": prepared outer worker drain failed"
                    if (control is None or control.drained) and face.settings.output_policy.attempt_path(assembled.contract).is_dir():
                        try:
                            require(metadata["source_hashes"] == source_hashes(),
                                    "source drift blocks new recovery qualification; loaded fault cleanup is retained")
                            recovery = session.recover(recovery_supplier(assembled, face), services=face.services)
                        except BaseException as exc:
                            recovery = {"status": "BLOCKED", "error_type": type(exc).__name__, "reason": str(exc)[:300]}
                            error = error or type(exc).__name__ + ": " + str(exc)[:300]
                    session.close()
        complete_phases = bool(inner) and len(inner.get("actual_phase_facts", [])) == 3
        injected = bool(inner) and sum(row.get("action") == "inject" and row.get("status") == "operation_confirmed_not_physical"
                                    for row in inner.get("action_timing", [])) == len(assembled.contract.faults)
        outer_drained = control is not None and control.drained is True
        passed = not error and outer_drained and bool(inner) and not inner.get("run_error") and complete_phases and injected\
            and bool(recovery) and recovery.get("status") in {"ALREADY_CLEAN", "RECOVERY_QUALIFIED"}
        result = {"status": "ENGINEERING_EXECUTED_RECOVERED" if passed else "BLOCKED", "contract_sha256": assembled.contract.sha256,
            "inner_result": inner, "recovery": recovery, "error": error, "probes": probes, "outer_drained": outer_drained,
            "outer_worker_state": "NOT_CREATED" if control is None else ("DRAINED" if outer_drained else "DRAIN_UNCONFIRMED"),
            "fault_execution_confirmed": injected, "sample_three_phases_attempted": complete_phases,
            "formal_release_eligible": False, "complete_G16_claimed": False, "next_case_allowed": passed, **metadata}
        technical = ((inner or {}).get("quality") or {}).get("technical_assessment")
        result.update(technical_assessment=technical,
                      technical_collection_status=technical.get("status", "NOT_ASSESSED") if type(technical) is dict else "NOT_ASSESSED",
                      next_case_allowed_scope="manual_engineering_diagnostics_only_not_G19_or_formal",
                      collection_scope="per_leg_required_subset_not_full_fixed_candidate_observations")
        write_new(destination / "SUMMARY.json", summary_jsonable(result))
        return result
    finally:
        # M03: the OWNED stressor carrier Deployment is always deleted after
        # the recovery attempt (or after any earlier failure) -- never
        # scale-0. A hard process kill is the only path that skips this; the
        # residual owner scan then flags the carrier honestly on the next run.
        if host_face is not None:
            delete_stressor_carriers(host_face, destination, attempt=args.attempt)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("step", choices=("preview", "run", "recover"))
    parser.add_argument("scenario", nargs="?")
    parser.add_argument("--condition-file", type=Path)
    parser.add_argument("--condition-id")
    parser.add_argument("--attempt", default="preview-1")
    parser.add_argument("--profile", choices=("short", "long300"), default="short")
    parser.add_argument("--action-budget", type=float, default=17)
    parser.add_argument("--lateness-budget", type=float, default=2)
    parser.add_argument("--first-injection-lateness-budget", type=float, default=None)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--environment", type=Path)
    args = parser.parse_args()
    portable_env.apply(sys.modules[__name__], args.environment)
    args.condition = None
    if args.condition_file is not None:
        require(args.condition_file.is_absolute() and args.condition_file.resolve() == args.condition_file,
                "explicit unredirected condition manifest path required")
        rows = json.loads(args.condition_file.read_bytes())
        require(type(rows) is list, "condition manifest list required")
        matches = [row for row in rows if row.get("condition_id") == args.condition_id]
        require(len(matches) == 1, "exact unique condition_id required")
        args.condition = matches[0]
        require(args.scenario is None or args.scenario == args.condition["scenario_id"], "scenario/condition mismatch")
        args.scenario = args.condition["scenario_id"]
    require(args.scenario is not None, "scenario or explicit condition required")
    if args.step != "preview":
        result = execute_case(args)
        print(json.dumps({key: value for key, value in result.items() if key in {"status", "error", "contract_sha256", "next_case_allowed"}}, indent=2))
        raise SystemExit(0 if result.get("status") in {"ENGINEERING_EXECUTED_RECOVERED", "RECOVERY_QUALIFIED", "ALREADY_CLEAN"} else 1)
    spec = entry_view(entry_spec((args.condition["source_combo"] or args.condition["scenario_id"])
                                 if args.condition is not None else args.scenario))
    if args.condition is not None:
        assembled, face, metadata = build_condition(args.condition, args.attempt, snapshot(spec, enabled=False),
            action_budget=args.action_budget, lateness_budget=args.lateness_budget,
            first_injection_lateness_budget=args.first_injection_lateness_budget)
    else:
        assembled, face, metadata = build(args.scenario, args.attempt, snapshot(spec, enabled=False),
            profile=args.profile, action_budget=args.action_budget, lateness_budget=args.lateness_budget,
            first_injection_lateness_budget=args.first_injection_lateness_budget)
    preview = r.run_attempt(assembled.contract, face.settings, face.services, execute=False)
    print(json.dumps({"preview": preview, "timing": face.timing_audit, "metadata": metadata,
                      "quality_rules_sha256": assembled.rules.sha256}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
