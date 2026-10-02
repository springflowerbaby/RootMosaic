"""Collection scenario definitions. Runtime identity and source checks remain explicit."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
import hashlib
from itertools import combinations
import re
from pathlib import Path
from typing import Any, Mapping

from . import contract as c
from . import gateway as gw
from . import observability as obs
from . import primitives as p
from . import primitives_db as dbp
from . import quality as q
from . import workload as w

ASSEMBLY_VERSION = "m1-combo-asm4-v1"   # COMBO-ASM-4: the registry additionally carries the
                                        # SPECS-4 closing batch (8 duals + 4 triples) plus the
                                        # SS7 triple sub-pair closure invariant and the two ASM-3
                                        # review NIT faces (pricing scale-up Precondition,
                                        # must_inject_first_entity guard); the constant never
                                        # enters contract/rules payloads, so earlier-batch byte
                                        # stability is unaffected
DEFAULT_CATALOG_REL = Path('configs/collection/scenarios.json')
GENERATOR_VERSION = "w-fixed-rate-v1"          # S08 smoke driver value (workload fixed-rate open loop)
METRIC_INTERVAL_S = 2.0                        # S08 live-proven 2s OTel->Prometheus chain
RATE_WINDOW_S = "6s"                           # S08 driver AVG_EXPR window (3 x 2s source interval)
# Phase plan for the pilot assembly profile (seconds, run_offset): the S08 smoke
# geometry pre [0,30) / during [30,120) / post [120,150) with the fault window
# [30,85). NOT_FROZEN: the formal collection three-300s windows are frozen elsewhere

# per-carrier n>=30 sample floor is satisfied at 1 rps x 90 s (90 requests).

# MINOR-1: the driver emits int phases/windows/thresholds, not floats).
SMOKE_FAMILY_PHASES = ((0, 30), (30, 120), (120, 150))

_LEDGER = 'configs/collection/scenarios.json: assembly definition'      
_LEDGER_B3 = 'configs/collection/scenarios.json: assembly definition'   # accepted B3 ledger (service_cpu / runtime_exception)
_LEDGER_B2 = 'configs/collection/scenarios.json: assembly definition'   # accepted B2 ledger (gateway cfg family, incl. S23-A)


# service; it binds the dedicated CPU stressor carrier Deployment (old
# STRESSOR_DEPLOY L345; primitives.PinnedTarget "host" exception + render_stressor_manifest).
HOST_STRESSOR_DEPLOYMENT = "stressor"

# ASM-2 CPU-family judgement candidates, single truth source for every spec
# below (old lesson: one strength declaration must feed injection, manifest and
# GT together -- runner L13957-13973; a GT label without matching injection is
# a lying GT).  All values are NOT_FROZEN pilot candidates; anchors live in the
# spec provenance tables.
CPU_RATIO_THRESHOLD = 1.8          # old SERVICE_CPU_RATIO_THR L317 / HOST_CPU_RATIO_THR L354 (probe2/probe4:
                                   # CPU saturation shifts carriers only +50-60 ms absolute -> RELATIVE ratio,
                                   # never the NET-family absolute ms threshold)
CPU_RATIO_REQUIRED_FRACTION = 0.8  # old single_sli_gate F1_only_ok>=0.8 (L3662-3663): slow-not-failed
CPU_RECOVERY_TOLERANCE_MS = 300    # old DK12_RECOMMEND_FAST_MS recovery-window anchor (L339)
C1_MIN_POINTS = 30                 # C1 protocol SS2.5: per-window per-carrier sample floor


def load_registry(catalog_json: Path | None = None, *, repo_root: Path | None = None) -> c.DesignRegistry:
    """Parse the versioned design catalog bytes; read-only, caller-local file."""
    path = catalog_json if catalog_json is not None else ((repo_root or Path.cwd()) / DEFAULT_CATALOG_REL)
    return c.DesignRegistry.from_json(Path(path).read_bytes())


class AssemblyError(ValueError):
    """Invalid spec/binding shape. Messages never echo supplied values."""


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise AssemblyError(message)


def _sha256_hex(value: Any, field: str) -> str:
    _require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None,
             field + ": lowercase SHA256 required")
    return value


def synthetic_fingerprint(label: str) -> str:
    """Deterministic offline fingerprint for tests; never a live file identity."""
    _require(isinstance(label, str) and bool(label.strip()), "fingerprint label required")
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def file_fingerprint(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# --------------------------------------------------------------------------- #
# Declarative spec building blocks
# --------------------------------------------------------------------------- #

# SDEPLOY-SELECTOR-PIN (F-S1-3): two rollout-era deployments keep K8s-immutable
# canonical app labels that differ from their Deployment OBJECT names (verified

# for exactly these entities; every other entity keeps label == object name.
LIVE_APP_LABEL_BY_ENTITY = {"backend": "backend_api", "rec-agent": "recommendation_agent"}


def _canonical_app_label(entity: str, deployment: str) -> str:
    """The canonical live app label for a leg (object name unless deviant)."""
    return LIVE_APP_LABEL_BY_ENTITY.get(entity, deployment)


@dataclass(frozen=True)
class FaultLegSpec:
    """One k8s fault leg; JSON field spellings follow the S08 driver fault dict."""
    fault_type: str
    mechanism: str
    fault_class: str            # legacy GT fault_class anchor (runtime/network/lifecycle/resource)
    entity: str                 
    deployment: str             # raw_target Deployment name (== entity for services; the off-graph
                                
    selector: Mapping[str, str] # canonical LIVE app label selector (== deployment name except the SDEPLOY-SELECTOR-PIN deviants)
    parameters: Mapping[str, Any]
    window: tuple[int, int]     # planned fault window inside during_fault (run_offset, int form)
    # Gateway rollout mechanism binding: the
    # contract fault's mechanism version is per-leg.  Every Kubernetes
    # primitive keeps the shared P1 version; the gateway config atoms carry the
    # P2 rollout pair (gateway.py L36-37) -- on the nginx_configmap_rollout
    # mechanism the shared P1 version is structurally wrong, and vice versa.
    mechanism_version: str = p.MECHANISM_VERSION

    def __post_init__(self) -> None:
        _require(isinstance(self.fault_type, str) and bool(self.fault_type), "leg.fault_type required")
        _require(bool(self.mechanism) and bool(self.fault_class) and bool(self.entity), "leg fields required")
        if self.mechanism == gw.MECHANISM:
            _require(self.mechanism_version == gw.MECHANISM_VERSION,
                     "gateway rollout legs carry only the gateway mechanism version")
        else:
            _require(self.mechanism_version == p.MECHANISM_VERSION,
                     "non-gateway legs carry the shared primitive mechanism version")
        
        # carrier Deployment (old STRESSOR_DEPLOY, primitives.PinnedTarget
        # precedent); every ordinary service still binds its own Deployment.
        if self.entity == "host":
            _require(self.deployment == HOST_STRESSOR_DEPLOYMENT,
                     "the off-graph host root binds only the stressor carrier Deployment")
        else:
            _require(self.deployment == self.entity, "service leg must bind its own Deployment")
        # The selector pins the canonical LIVE app label (object name for every
        # entity except the SDEPLOY-SELECTOR-PIN deviants, F-S1-3).
        _require(dict(self.selector) == {"app": _canonical_app_label(self.entity, self.deployment)},
                 "leg.selector must be the canonical app label")
        _require(isinstance(self.parameters, dict) and bool(self.parameters), "leg.parameters required")
        _require(len(self.window) == 2 and 0 <= self.window[0] < self.window[1], "leg.window invalid")

    def raw_target(self, namespace: str) -> dict[str, Any]:
        return {"kind": "Deployment", "name": self.deployment, "scope": namespace, "selector": dict(self.selector)}



# Deployment -- the raw target is the MySqlTableLock identity (scope = the actual
# MySQL schema name, database schema binding=shopify2) and the runner face is
# RunServices.db_bindings + db_client (collection validate_run dispatch:
# db.prepare_db_lock(spec, DbBinding, ownership="validation-only")), never a
# Kubernetes pin.  The exact parameter set {table, mode} and the
# entity->table whitelist (DB_LOCK_TABLE_BY_ENTITY, mysql_items_lock->items)
# come from primitives_db.prepare_db_lock; the old hold/gap duty-cycle
# parameters are DELIBERATELY absent (the new adapter holds one continuous
# session lock -- ledger B1 01 SS3.2-1, an intentionally changed behaviour).
@dataclass(frozen=True)
class DbFaultLegSpec:
    """One database fault leg; JSON field spellings follow prepare_db_lock."""
    fault_type: str
    mechanism: str                # dbp.DB_MECHANISM (mysql_lock_table) only
    fault_class: str              # legacy GT fault_class anchor (resource)
    entity: str                   
    database: str                 # raw_target.scope: the MySQL schema name
    table: str                    # raw_target.name + the locked table
    mode: str                     # lock mode ("WRITE" only)
    window: tuple[int, int]       # planned lock window inside during_fault (run_offset)
    mechanism_version: str = p.MECHANISM_VERSION

    def __post_init__(self) -> None:
        _require(self.fault_type == "db_table_lock" and bool(self.fault_class),
                 "database leg: db_table_lock fault type required")
        _require(self.mechanism == dbp.DB_MECHANISM,
                 "database leg must carry the mysql_lock_table mechanism")
        _require(self.mechanism_version == p.MECHANISM_VERSION,
                 "database leg carries the shared primitive mechanism version")
        _require(self.entity in dbp.DB_LOCK_TABLE_BY_ENTITY,
                 "unsupported db_table_lock root entity")
        _require(self.table == dbp.DB_LOCK_TABLE_BY_ENTITY[self.entity],
                 "locked table does not match the root entity")
        _require(self.mode == "WRITE", "only WRITE table locks are implemented")
        _require(isinstance(self.database, str)
                 and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.database) is not None,
                 "database leg: schema identifier invalid")
        _require(len(self.window) == 2 and 0 <= self.window[0] < self.window[1],
                 "leg.window invalid")

    @property
    def parameters(self) -> dict[str, Any]:
        # Single truth source: the parameter set is fully derived from the leg
        # identity, so table/mode can never drift from the declared target.
        return {"table": self.table, "mode": self.mode}

    def raw_target(self, namespace: str) -> dict[str, Any]:
        # The namespace argument is signature-compatible with FaultLegSpec but
        # unused: a MySqlTableLock target has no Kubernetes scope.
        return {"kind": dbp.DB_TARGET_KIND, "name": self.table, "scope": self.database, "selector": {}}


@dataclass(frozen=True)
class CarrierStreamSpec:
    """Read-only carrier stream through the kubectl proxy (S08 pattern).

    direct_api=True means "straight to the named Service, bypassing shop-web";
    for gw-path carriers the Service named here is the carrier service whose
    dependency path crosses the faulted entity (ledger 03/05 carrier mapping).
    """
    stream_id: str
    service: str          # "<service>:<port>" inside the kubectl proxy path
    path_prefix: str      # e.g. "/api/items/" or "/api/orders?user_token="
    carrier: str          # carrier panel label recorded in the contract
    direct_api: bool
    rate_rps: float = 1.0
    max_concurrency: int = 2
    timeout_s: float = 10.0
    client_retry_limit: int = 0
    # ASM-2: the retarget-family panel endpoints carry query strings; the probe
    # parameter is interpolated between prefix and suffix (e.g. prefix
    # "/api/reviews?item_id=" + item + suffix "&per_page=5&enrich=1").  Empty
    # for every ASM-1 stream (no behavior change).
    path_suffix: str = ""
    
    # placeholder (old PANEL_TARGETS forms like "health" or "api/stats/model");
    # endpoint() must not interpolate item_id into them.  parameterized=True
    # keeps the ASM-1/S08 behavior for every parameterized stream.
    parameterized: bool = True
    
    
    
    # entering through a gateway cannot resolve its trace lookup from the
    # entry entity.  trace_service declares the downstream app service that
    # actually carries this stream's spans (catalog-gw /api/items/ ->
    
    # GW-leg signal service); None (every non-via-gw stream) keeps the entity
    # reverse-lookup in scenario_runner.probe_carriers unchanged, and the
    # consumer rejects a declared name outside the known service universe
    # (fail-closed, no invented gateway-span mapping).
    trace_service: str | None = None

    def __post_init__(self) -> None:
        _require(isinstance(self.path_prefix, str) and self.path_prefix.startswith("/"),
                 "carrier path_prefix must be an absolute path")
        _require(self.parameterized or not self.path_suffix,
                 "a fixed-path carrier cannot carry a path suffix (nothing is interpolated)")
        _require(self.trace_service is None
                 or (type(self.trace_service) is str and bool(self.trace_service)),
                 "carrier trace_service must be a non-empty service name or None")
        expected_carrier = w.SASREC_INFERENCE_STREAM_CARRIERS.get(self.stream_id)
        if expected_carrier is not None:
            _require(self.carrier == expected_carrier and self.service == "sasrec:8200"
                     and self.path_prefix == w.SASREC_INFERENCE_ENDPOINT and self.direct_api is True
                     and self.parameterized is False and self.path_suffix == ""
                     and self.rate_rps == 1.0 and self.max_concurrency == 1
                     and self.client_retry_limit == 0,
                     "SASRec inference carrier must match its exact versioned route and bounded request profile")
        else:
            _require(self.carrier not in set(w.SASREC_INFERENCE_STREAM_CARRIERS.values()),
                     "SASRec inference carrier label requires its registered stream identity")

    @property
    def http_method(self) -> str:
        return "POST" if self.stream_id in w.SASREC_INFERENCE_STREAM_CARRIERS else "GET"

    @property
    def http_parameters(self) -> dict[str, object]:
        if self.http_method == "POST":
            return w.sasrec_inference_parameters()
        return {"query": {}, "headers": {}, "json": None}

    def endpoint(self, namespace: str, item_id: str) -> str:
        path = self.path_prefix + item_id + self.path_suffix if self.parameterized else self.path_prefix
        return "/api/v1/namespaces/" + namespace + "/services/" + self.service + "/proxy" + path


@dataclass(frozen=True)
class MetricSignalSpec:
    """Latency GT/QC signal on the carrier service's OTel HTTP duration metric.

    The PromQL is byte-identical in shape to the S08 driver AVG_EXPR (sum-by
    rate ratio over the 2s chain), parameterized by service_name/http_target.
    The http_target keeps the literal "<item_id>" placeholder: the services'
    OTel http_target label is already parameterized server-side (S08 precedent).
    """
    entity: str                 
    service_name: str           # OTEL_SERVICE_NAME of the observed carrier service
    http_target: str            # literal label value, e.g. "/api/items/<item_id>"
    source_id: str
    unit: str = "ms"
    min_points: int = 5
    mode: str = "delta_from_pre"
    operator: str = "ge"
    threshold: int = 1000          # int form: byte-identical JSON to the S08 driver
    required_fraction: float = 0.5
    absolute_tolerance: int = 1000
    relative_tolerance: int = 0

    def expression(self) -> str:
        selector = ('service_name="' + self.service_name + '",http_target="' + self.http_target + '"')
        return ('sum by (service_name) (rate(http_server_duration_milliseconds_sum'
                '{' + selector + '}[' + RATE_WINDOW_S + ']))'
                ' / sum by (service_name) (rate(http_server_duration_milliseconds_count'
                '{' + selector + '}[' + RATE_WINDOW_S + ']))')

    def signal(self, query_id: str) -> dict[str, Any]:
        return {"entity": self.entity, "query_id": query_id, "source_id": self.source_id, "unit": self.unit,
                "labels": {"service_name": self.service_name}, "min_points": self.min_points,
                "mode": self.mode, "operator": self.operator, "threshold": self.threshold,
                "required_fraction": self.required_fraction}

    def recovery_rule(self, query_id: str) -> dict[str, Any]:
        return {"entity": self.entity, "query_id": query_id, "source_id": self.source_id, "unit": self.unit,
                "labels": {"service_name": self.service_name}, "min_points": self.min_points,
                "absolute_tolerance": self.absolute_tolerance, "relative_tolerance": self.relative_tolerance}


@dataclass(frozen=True)
class TelemetryIds:
    """Stable source identifiers shared by the contract rules and run services."""
    metrics: str
    traces: str
    logs: str
    components: str
    checksum: str
    locks: str
    operations: str


@dataclass(frozen=True)
class Precondition:
    """Declared runtime prerequisite the assembled contract silently relies on.

    collection MAJOR-2: some carrier routing premises hold only after driver/runtime
    provisioning that the cluster default does NOT satisfy.  The assembly layer
    declares such premises here; it never provisions, verifies or enforces them
    (the v1 contract schema has no open field for preconditions, and adding one
    would touch frozen contract.py -- out of ASM scope).  The driver/runtime
    binding layer (collection) owns fulfilment and evidence; a smoke without the
    declared premise will simply observe no fault signature.
    """
    statement: str          # what must hold before/at run start
    mechanism_anchor: str   # file/line anchors proving the default state differs
    owner: str              # who must fulfil it (driver/runtime binding, never assembly)
    status: str             # NOT_FROZEN candidate mechanism marker

    def __post_init__(self) -> None:
        for name in ("statement", "mechanism_anchor", "owner", "status"):
            _require(isinstance(getattr(self, name), str) and bool(getattr(self, name).strip()),
                     "precondition." + name + " required")


@dataclass(frozen=True)
class S03LivenessProfile:
    """Explicit livenessProbe-contention decision for S03 (ledger 05 SS5.1, collection MAJOR-1).

    catalog livenessProbe (k8s/services/catalog.yaml: periodSeconds 20,
    failureThreshold 3): with pod-down starting at t0, the first failed probe
    lands in [t0, t0+20) and the third consecutive failure -- the kubelet kill
    point -- lands in [t0+40, t0+60].  The old "~60 s" wording was the band's
    upper edge only (review probe measurement).
    """
    name: str
    window: tuple[int, int]
    duration_s: int
    decision: str            # the explicit decision record (ledger 05 SS5.1)

    def __post_init__(self) -> None:
        _require(bool(self.name) and len(self.window) == 2 and self.window[0] < self.window[1],
                 "liveness profile shape invalid")
        _require(type(self.duration_s) is int and self.duration_s >= self.window[1] - self.window[0],
                 "CRD duration must cover the planned window")
        _require(isinstance(self.decision, str) and bool(self.decision.strip()), "explicit decision record required")




#     pod-failure transfer function (D04-F1/F2 anchor-1, D19fix6-F1): the
#     spec patch does NOT bite a running container -- effective service
#     unavailability starts only at inject_confirm + lag (measured 28.8/
#     29.4/ 30.9 s from the three legs; declared bound 35 s) and ends at
#     CR removal + restore+Ready (measured 11-13 s, the recover action's own
#     wait_pod_ready).  The old "pod down from apply for duration 38 s"

#     D19 0.42 < threshold 0.5) -- fixed by declaring the lag head, never by
#     touching threshold/required/n>=30.  v2: window span 60, CRD duration =
#     span + lag bound (90): the eval window [inject_confirm+settle_35,
#     recover_confirm] keeps ~34 requests >= C1 SS2.5 floor 30; the EFFECTIVE
#     pod-down segment ~= [w_start+34, w_end+11] = 37 s stays below the 40 s
#     kill-band floor (the kubelet kill window opens at t0+40 ~= w_start+74,
#     strictly after the w_start+71 restore), so the third consecutive
#     liveness failure still cannot fire before recovery -- the below-band
#     premise RESTATED in effective terms.
# (b) across_kill_band (v2): window span 70, duration = span+lag (100); the
#     EFFECTIVE down segment ~= [w_start+34, w_start+81] = 47 s crosses the
#     40 s kill-band floor exactly as the profile intends, so at most one
#     kubelet restart is still expected (the pause injection persists across
#     the restart; the fault itself does not disappear -- old probe-A
#     "AllInjected lasts the whole window").  Compatibility risk unchanged:
#     the legacy legal signature restart_delta in {1,2} can be exceeded by

#     smoke under this profile MUST observe restart_count and liveness
#     events (CRI evidence).
S03_LIVENESS_PROFILES: dict[str, S03LivenessProfile] = {
    "below_kill_band": S03LivenessProfile(
        name="below_kill_band", window=(30, 90), duration_s=90,
        decision='collection MAJOR-1 (a) v2 (EXEC-POD-XFER 2026-09-22, measured transfer function): PodChaos '
                 "pod-failure bites a running container only at inject_confirm+lag (measured 28.8/29.4/30.9 s "
                 "on D04-F1/F2 + D19fix6-F1; declared bound 35 s = POD_FAILURE_TRANSFER_LAG_BOUND_S, absorbed "
                 "as the carrier settle head, never as an action budget) and ends at CR removal + restore+Ready "
                 "(measured 11-13 s inside the recover action's own wait_pod_ready); window widened 35->60 s so "
                 "the eval window [inject_confirm+35, recover_confirm] keeps ~34 requests >= C1 SS2.5 floor 30 "
                 "with threshold/required/n floors untouched; duration = span+lag bound (90) keeps the CR alive "
                 "past the runner recover (the old 38 vs 35 margin is dead arithmetic under the lag); the "
                 "EFFECTIVE pod-down segment ~= [w_start+34, w_end+11] = 37 s stays below the 40 s kill-band "
                 "floor -- the third consecutive liveness failure still cannot fire before recovery, premise "
                 "restated in effective (not apply-anchored) terms"),
    "across_kill_band": S03LivenessProfile(
        name="across_kill_band", window=(30, 100), duration_s=100,
        decision='collection MAJOR-1 (b) v2 (EXEC-POD-XFER 2026-09-22): effective down ~= [w_start+34, w_start+81] '
                 "= 47 s (lag head 35 excluded by the carrier settle; duration = span+lag 100) crosses the "
                 "40 s kill-band floor exactly as intended -- at most one kubelet restart expected, restart "
                 'observation duty KEPT; legacy restart_delta in {1,2} may be exceeded -- collection smoke must '
                 "observe restart_count/liveness events and restate the signature window in effective terms"),
}
S03_DEFAULT_LIVENESS_PROFILE = "below_kill_band"


# --------------------------------------------------------------------------- #
# ASM-2 decision structures (S03LivenessProfile precedent): CPU saturation and
# env-rollout atoms each couple the injection window to a live-site constraint
# (liveness probe timeouts / deployment rollout band).  The coupling is made
# explicit here -- no implicit defaults.
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class CpuLivenessProfile:
    """Explicit livenessProbe-contention decision for a CPU-saturation atom.

    Unlike S03 pod-failure (probe guaranteed to fail: pause image), CPU
    starvation keeps the pod and its /health handler runnable -- the probe
    timeout is the only kill path, and probe2 measured slow-not-fail with no
    readiness flap at workers=2 on a 500m cgroup (ledger 01 SS1-1).  Each
    profile records the kill-band arithmetic for its probe shape plus the
    empirical anchor; restart observation stays an collection smoke duty.
    """
    name: str
    window: tuple[int, int]
    duration_s: int
    decision: str            # the explicit decision record

    def __post_init__(self) -> None:
        _require(bool(self.name) and len(self.window) == 2 and self.window[0] < self.window[1],
                 "CPU liveness profile shape invalid")
        _require(type(self.duration_s) is int and self.duration_s >= self.window[1] - self.window[0],
                 "CRD duration must cover the planned window")
        _require(isinstance(self.decision, str) and bool(self.decision.strip()), "explicit decision record required")


# (a) std_20_3_3 (catalog/pricing/order/cart/review-query/checkout pilots): the
#     pod stays up; a kubelet kill needs 3 consecutive >3s probe timeouts, the
#     third landing in [t0+40, t0+60) s of sustained starvation.  probe2
#     measured slow-not-fail with NO readiness flap at workers=2 on the 500m
#     cgroup (workers=1 atoms are strictly weaker stress) -> the 55 s family

# (b) recagent_20_3_5 (S20): same kill band [40,60) s but a 5 s probe timeout
#     (wider headroom under throttle); same probe2 shape anchor (500m, workers=2).
# (c) tolerant_30_5_5 (S12 backend, S27 sasrec): kill needs 5 consecutive
#     failures, the fifth earliest at t0+120 s -- strictly beyond the CRD
#     duration cap, so kubelet contention is structurally unreachable inside
#     the planned exposure.
# (d) host_node_saturation (S05): node-wide starvation slows every victim probe
#     AND the kubelet itself; victims keep their 100m cpu-request guarantee so
#     /health stays within the 3 s timeouts; the old live churn gate
#     (common_cause_gate restart_delta=0) anchors the no-restart expectation.
CPU_LIVENESS_PROFILES: dict[str, CpuLivenessProfile] = {
    "std_20_3_3_slow_not_fail": CpuLivenessProfile(
        name="std_20_3_3_slow_not_fail", window=(30, 85), duration_s=60,
        decision="probe shape period20/threshold3/timeout3 (10/41/42/43/46/18 yamls): kill band [40,60) s of "
                 "sustained >3s /health starvation; pod stays up under cgroup throttle and probe2 measured "
                 'slow-not-fail with no readiness flap at workers=2 on 500m -> 55 s family window kept; collection '
                 "smoke must observe restart_count/readiness (churn evidence)"),
    "recagent_20_3_5": CpuLivenessProfile(
        name="recagent_20_3_5", window=(30, 85), duration_s=60,
        decision="rec-agent probe shape period20/threshold3/timeout5 (01-rec-agent.yaml): kill band [40,60) s; "
                 "5 s timeout widens the headroom under the 500m cgroup throttle (probe2 shape anchor); "
                 '55 s family window kept; collection smoke must observe restart_count/readiness'),
    "tolerant_30_5_5_structurally_unreachable": CpuLivenessProfile(
        name="tolerant_30_5_5_structurally_unreachable", window=(30, 85), duration_s=60,
        decision="probe shape period30/threshold5/timeout5 (30-backend.yaml / 20-sasrec.yaml): the 5th "
                 "consecutive failure lands earliest at t0+120 s > the 60 s CRD duration cap -> kubelet kill "
                 "is structurally unreachable inside the planned exposure; window kept at the family geometry"),
    "host_node_saturation": CpuLivenessProfile(
        name="host_node_saturation", window=(30, 85), duration_s=60,
        decision="node-wide saturation (stressor carrier, no cpu limit) slows every co-located victim probe "
                 "and the kubelet itself; victims keep their 100m cpu-request guarantee so /health answers "
                 "within the 3 s probe timeouts; old live anchor: common_cause_gate churn restart_delta=0; "
                 'collection smoke must observe restart_count for carrier and disjoint victims'),
}


@dataclass(frozen=True)
class EnvRolloutProfile:
    """Explicit rollout-band decision for a deployment_env atom (S07).

    Old live calibration (ledger B3 02 SS1-5): env set/unset triggers a
    deployment rollout; under the old port-forward carrier the stable-code
    moment lagged ~50 s while rollout status returned ~14 s early -- the window
    head therefore eats a rollout band.  The new chain aligns windows to actual
    inject/recover events (apply blocks through rollout settle; S08 smoke
    measured the catalog env rollout at ~13 s) and routes carriers through the
    apiserver proxy (probe10: the proxy sees pod states, unlike port-forward).
    The declared band is a candidate; collection must measure the 500-onset band
    under proxy routing and re-freeze the window if it approaches the old ~50 s.
    """
    name: str
    window: tuple[int, int]
    rollout_band_s: int
    decision: str

    def __post_init__(self) -> None:
        _require(bool(self.name) and len(self.window) == 2 and self.window[0] < self.window[1],
                 "env rollout profile shape invalid")
        _require(type(self.rollout_band_s) is int and self.rollout_band_s > 0, "rollout band must be positive")
        # C1 SS2.5: after the rollout-band head, the effective window must still
        # carry >= 30 carrier samples at 1 rps.
        _require(self.window[1] - self.window[0] - self.rollout_band_s >= C1_MIN_POINTS,
                 "window span minus rollout band must cover the C1 sample floor")
        _require(isinstance(self.decision, str) and bool(self.decision.strip()), "explicit decision record required")


S07_ROLLOUT_PROFILES: dict[str, EnvRolloutProfile] = {
    "event_aligned_family_window": EnvRolloutProfile(
        name="event_aligned_family_window", window=(30, 85), rollout_band_s=15,
        decision="keep the 55 s family window aligned to actual inject/recover events; declared rollout band "
                 "15 s (S08 smoke live advisory ~13 s catalog env rollout; NOT_FROZEN) eats the window head "
                 "leaving 40 effective samples >= C1 floor 30; the old-chain ~50 s port-forward band is NOT "
                 'inherited (new carriers use the apiserver proxy; probe10 quirk evidence) but collection must '
                 "measure the 500-onset band and re-freeze if it approaches it; DOUBLE-ROLLOUT lesson (D11 "
                 "~340 s two-env setup) recorded for any future multi-env assembly; carrier 500/200 effect "
                 "confirmation belongs to Q1 (B3 acceptance registered item)"),
}
S07_DEFAULT_ROLLOUT_PROFILE = "event_aligned_family_window"


# --------------------------------------------------------------------------- #
# ASM-3 decision structures.  (a) The S03 pod-failure selector-extension family
# (S14-S19/S22/S26, ledger B1 05 SS2.3): every atom repeats the S03 liveness-
# contention analysis against ITS OWN yaml probe shape -- no inherited defaults.
# (b) The gateway config atoms (S23/S24, ledger B2 01/03 SS0-2) have no
# composite geometry; their only timing fact is the ConfigMap-rollout switch
# band absorbed at the head of the planned window (apply blocks through rollout
# settle), recorded as an explicit rollout-band decision like S07's.
# --------------------------------------------------------------------------- #

# (a) pod-failure family: kill-band arithmetic per yaml (all line anchors
#     personally verified against the pilot manifests).
#     std_20_3_3_below_kill_band (S14 order 42-order.yaml / S15 cart 41-cart.yaml /
#     S16 review-query 18-review-query.yaml / S18 checkout 43-checkout.yaml /
#     S19 search 17-search.yaml / S26 user 04-user.yaml): every one of these
#     manifests carries periodSeconds 20 x failureThreshold 3 -- identical to
#     10-catalog.yaml -- so the third consecutive liveness failure (the kubelet
#     kill point) lands in [t0+40, t0+60) of EFFECTIVE pod-down t0 (the
#     measured lag-shifted bite point, not the apply anchor); v2 (EXEC-POD-XFER

#     ~= 37 s < 40 s floor -- the below-band premise restated in effective
#     terms; eval window [inject_confirm+35, recover_confirm] ~= 34 requests
#     >= C1 SS2.5 floor 30; restore+Ready 11-13 s measured inside the recover
#     action's own wait_pod_ready.
#     recagent_20_3_5 (S22 01-rec-agent.yaml): period 20 x threshold 3 with a
#     5 s timeout; under pod-failure the pause container refuses connections
#     instantly, so the timeout never widens the count arithmetic -- same kill
#     band [t0+40, t0+60), same v2 geometry.
#     backend_tolerant_30_5_5 (S17 30-backend.yaml): period 30 x threshold 5 --
#     the fifth consecutive failure lands earliest at t0+120 s, strictly beyond
#     any planned exposure, so kubelet contention is structurally unreachable
#     and the window rides the SAME v2 span-60 lag-head arithmetic (the old
#     span-55 form would leave only 29 eval requests under the 35 s settle).
POD_FAILURE_LIVENESS_PROFILES: dict[str, S03LivenessProfile] = {
    "std_20_3_3_below_kill_band": S03LivenessProfile(
        name="std_20_3_3_below_kill_band", window=(30, 90), duration_s=90,
        decision="probe shape period20/threshold3 (42-order/41-cart/18-review-query/43-checkout/17-search/"
                 "04-user yamls, identical to 10-catalog): third consecutive liveness failure lands in "
                 "[t0+40, t0+60) of EFFECTIVE pod-down; v2 (EXEC-POD-XFER 2026-09-22): window 60 s x 1 rps "
                 "with the measured lag head (bound 35 s = settle) keeps ~34 eval requests >= C1 SS2.5 "
                 "floor 30; duration = span+lag (90) keeps the CR alive past the runner recover; effective "
                 "down ~= 37 s < 40 s kill-band floor, so kubelet/PodChaos contention still cannot fire "
                 "(premise restated in effective terms); restore+Ready 11-13 s is confirmed inside the "
                 "recover action's own wait_pod_ready (measured D04-F1/F2, D19fix6-F1); restart_delta in "
                 "{1,2} stays the legal window signature"),
    "recagent_20_3_5_below_kill_band": S03LivenessProfile(
        name="recagent_20_3_5_below_kill_band", window=(30, 90), duration_s=90,
        decision="probe shape period20/threshold3/timeout5 (01-rec-agent.yaml): the pause container refuses "
                 "connections instantly, so the 5 s timeout never widens the probe-count arithmetic -- same kill "
                 "band [t0+40, t0+60) as the std shape; v2 lag-head geometry (span 60, duration 90, settle 35, "
                 "~34 eval requests >= C1 floor 30, effective down ~= 37 s < 40 s floor) identical to "
                 "std_20_3_3_below_kill_band"),
    "backend_tolerant_30_5_5_structurally_unreachable": S03LivenessProfile(
        name="backend_tolerant_30_5_5_structurally_unreachable", window=(30, 90), duration_s=90,
        decision="probe shape period30/threshold5 (30-backend.yaml): the fifth consecutive liveness failure "
                 "lands earliest at t0+120 s -> kubelet kill stays structurally unreachable under the v2 "
                 "effective-down arithmetic (~37 s); the window rides the same v2 lag-head geometry (span 60, "
                 "duration 90, settle 35) so the eval window keeps ~34 requests >= C1 floor 30 (the legacy "
                 "span-55 form would leave 29)"),
}


@dataclass(frozen=True)
class GatewayRolloutProfile:
    """Explicit switch-band decision for a gateway config atom (S23/S24).

    The new mechanism (candidate B) applies the knob through an immutable
    ConfigMap source switch plus a controlled Deployment rollout: the inject
    action blocks through rollout settle, so the 14-21 s live switch band
    (P2 DESIGN SS7.2 budget, tail <=28 s) sits at the HEAD of the planned
    window, exactly like S07's env-rollout band.  Single-atom gateway designs
    have no composite geometry (ledger B2 03 SS0-2): no mid_action exists, the
    whole band is absorbed inside INJECTION_TRANSITION, and the effective fault
    window starts at settled+buffer (C1 SS2.4 initial 5 s).  The declared band
    must leave the C1 SS2.5 per-window per-carrier floor after the head.
    """
    name: str
    window: tuple[int, int]
    rollout_band_s: int
    decision: str

    def __post_init__(self) -> None:
        _require(bool(self.name) and len(self.window) == 2 and self.window[0] < self.window[1],
                 "gateway rollout profile shape invalid")
        _require(type(self.rollout_band_s) is int and self.rollout_band_s > 0, "rollout band must be positive")
        # C1 SS2.5: after the switch-band head (rollout + settle buffer), the
        # effective steady window must still carry >= 30 carrier samples.
        _require(self.window[1] - self.window[0] - self.rollout_band_s >= C1_MIN_POINTS,
                 "window span minus switch band must cover the C1 sample floor")
        _require(isinstance(self.decision, str) and bool(self.decision.strip()), "explicit decision record required")


GATEWAY_ROLLOUT_PROFILES: dict[str, GatewayRolloutProfile] = {
    "injection_transition_absorbed": GatewayRolloutProfile(
        name="injection_transition_absorbed", window=(30, 85), rollout_band_s=25,
        decision="keep the 55 s family window; declared switch band 25 s = 20 s rollout head (live budget "
                 "typical 14-21 s, NOT_FROZEN; measured value = transition durations_s.rollout_band_s, "
                 "integration contract item 8) + C1 SS2.4 settle buffer 5 s, leaving 55-25=30 effective "
                 "steady samples = exactly the C1 SS2.5 floor; the <=28 s live tail would breach the floor "
                 '(28+5 leaves 22 < 30) -- collection must measure the band per transition record and re-freeze '
                 "window/band before any live run that exceeds it (window widening is a window-geometry "
                 "change requiring a board record); no composite geometry for single atoms (ledger B2 03 "
                 "SS0-2): zero mid_action, the band is absorbed inside INJECTION_TRANSITION; restore band "
                 "mirrors at recovery, post sampling starts from fault_cleared_from + buffer; rollout "
                 "completion is presence evidence only, never a fault-effect signal "
                 "(rollout_transition_is_not_timeout_signal)"),
}
GATEWAY_DEFAULT_ROLLOUT_PROFILE = "injection_transition_absorbed"


# --------------------------------------------------------------------------- #
# ASM-4 net-family probe decision records (S03LivenessProfile shape reused):
# each new network atom repeats the probe-contention analysis against ITS OWN
# yaml -- the impact chain of netem (loss/delay on the pod's WHOLE egress, per
# the verified direction:"to" + no-target semantics) onto the kubelet probes
# is reasoned explicitly, never inherited from the gw-delay atom silently.
# --------------------------------------------------------------------------- #

# (a) gw_loss_no_liveness_readiness_watched (S02; 11-catalog-gw.yaml personally
#     verified): the nginx container carries ONLY a readinessProbe (tcpSocket :80,
#     period 10 / timeout 3, no failureThreshold override -> default 3) and NO
#     livenessProbe at all -- no kubelet kill path exists on catalog-gw, so
#     restart_delta stays 0 structurally and the family window needs no shrink.
#     Loss-specific impact chain: the tcpSocket handshake's SYN-ACK leaves the
#     pod through the egress netem (ALL egress packets traverse the qdisc, the
#     old yaml comment's "only downstream = catalog" is business-graph
#     reasoning, not qdisc filtering), so at loss p each probe's handshake
#     survives only if a SYN-ACK (re)transmission lands inside the 3 s timeout
#     (TCP RTO backoff 1 s then 3 s races the timeout): at the 60% candidate
#     the per-probe failure probability is material (~p^2..p^3 = 13-22%) and
#     three consecutive failures (readiness removal) have material probability
#     inside the 55 s window (~5-6 probe periods) -- endpoint removal would
#     REPLACE the wire-loss signature with connection-refused errors on the gw
#     path.  Observation duty + flap boundary constrain the loss recalibration.
# (b) recagent_delay_within_probe_timeout (S21; 01-rec-agent.yaml personally
#     verified): livenessProbe httpGet /recommend/health period 20 / timeout 5 /
#     failureThreshold 3 (readiness 10/5/3): the always-200 health handler has
#     no LLM/DB dependency, and the 450/90 egress delay adds ~0.45-0.54 s per
#     response packet (a small health response is ~1-3 packets -> ~0.5-1.6 s),
#     an order of magnitude below the 5 s probe timeout -- the 3-consecutive-
#     failure kill band [t0+40, t0+60) is unreachable at the declared tier and
#     the family window (30, 85) with duration 60 is kept unchanged.
NET_LIVENESS_PROFILES: dict[str, S03LivenessProfile] = {
    "gw_loss_no_liveness_readiness_watched": S03LivenessProfile(
        name="gw_loss_no_liveness_readiness_watched", window=(30, 85), duration_s=60,
        decision="catalog-gw has NO livenessProbe (11-catalog-gw.yaml: readiness-only tcpSocket :80 "
                 "period10/timeout3, default failureThreshold 3) -> no kubelet kill path exists, restart_delta=0 "
                 "structurally, family window kept; the loss impact chain: the probe handshake SYN-ACK leaves the "
                 "pod through the egress netem, at 60% candidate per-probe handshake failure ~13-22% (TCP RTO 1s/3s "
                 "races the 3s timeout) -> 3 consecutive failures can remove the pod from endpoints inside the 55s "
                 'window, REPLACING the wire-loss signature with connection-refused errors; collection must observe '
                 "readiness/endpoint state and the error MIX; the flap boundary constrains the loss-tier "
                 "recalibration; readiness lag never aborts (evidence-only)"),
    "recagent_delay_within_probe_timeout": S03LivenessProfile(
        name="recagent_delay_within_probe_timeout", window=(30, 85), duration_s=60,
        decision="rec-agent probe shape liveness period20/timeout5/threshold3 + readiness 10/5/3 on "
                 "/recommend/health (01-rec-agent.yaml): the always-200 handler answers with no LLM/DB and the "
                 "450/90 egress delay adds ~0.45-0.54 s per response packet (~1-3 packets -> ~0.5-1.6 s), an "
                 "order of magnitude below the 5 s timeout -> the 3-consecutive-failure kill band [40,60) is "
                 'unreachable at the declared tier; family window 55 s kept; collection smoke still observes '
                 "restart_count/readiness as evidence"),
}





# n>=30 floor and scenario thresholds are KEPT; trim/settle parameters are
# EXPLICIT per rule; the undefined aggregate required_fraction is DELETED (never
# silently 1/1); the source is the persisted request ledger, never a Prometheus
# label query. Numeric carries live at the per-atom call sites below; the three
# window-policy fields have no legacy counterpart and take these conservative

CARRIER_BOUNDARY_EXCLUSION_S = 0.0  # any trim >0 structurally starves the 30 s
                                    # 1 rps pre_fault phase below the kept n>=30
                                    # floor (max selectable pre requests is 30);
                                    # phase-edge straddle requests are already
                                    # excluded by the evaluator's start-and-
                                    # complete-inside-window containment
CARRIER_SETTLE_BUFFER_S = 5.0       # gateway/netem/cpu-family switch-band settle
                                    # figure (GATEWAY_ROLLOUT_PROFILES: 20 rollout
                                    # + 5 settle); short-settle also DILUTES every
                                    # >=-threshold statistic with transition-
                                    # ramp requests -- conservative against ever
                                    # manufacturing signal

# settle semantic -- the spec patch does not bite a running container, so the
# effective fault starts only at inject_confirm + lag (measured 28.8/29.4/
# 30.9 s on D04-F1/F2 + D19fix6-F1, r10c pilots; evidence: anchor-1/fix-6
# workload ledgers vs operations.json confirms).  The pod family therefore
# declares its OWN settle = the lag BOUND (max measured + 4.1 s margin); the
# gateway/netem/cpu families keep 5.0 above (never a one-size global bump).
# The settle cuts the lag-ramp HEAD of the eval window only -- threshold,
# required judgement and the n>=30 floor are untouched, so every pod-leg
# window is widened (span >= 56; v2 profiles use 60) to keep ~34 eval
# requests.  The lag itself stays an OBSERVABLE per attempt: it is derivable
# from the persisted receipts (first error request time in the workload

# must record it per leg for the re-freeze.
POD_FAILURE_TRANSFER_LAG_BOUND_S = 35.0
POD_FAILURE_CARRIER_SETTLE_BUFFER_S = POD_FAILURE_TRANSFER_LAG_BOUND_S

# RECOVER action ends with the declared restore wait (wait_pod_ready;
# restore+Ready measured 11-13 s on D04-F1/F2 + D19fix6-F1). The tier budget
# (4 s at r48_fixed) covers the CR removal only; this band is the declared
# restore tail the runner's recover-duration gate carries, mirroring the env
# rollout band and the GW adapter-wait splits. Max measured 13 s + 2 s margin.
POD_FAILURE_RESTORE_BOUND_S = 15.0

# timeout_misconfiguration, nginx_configmap_rollout) declares its OWN settle =
# the measured rollout BAND BOUND + the C1 effectiveness buffer.  Measured
# inject/restore action bands on the r10c pilots: fix-3 D22-F2 inject 23.3 s /
# restore 25.5 s (operations.json action_timing), fix-1 up to ~38 s, collection
# declared GW inject anchor 23.219 s, frozen declared band 14-21 s typical /
# 25 s declared / <=28 s tail -- upper bound 40 s + the 5 s C1 settled-buffer
# (gateway.effectiveness_boundary) = 45.  The runner's instance window already
# starts at the inject CONFIRM (rollout-status + pod-set settled inside the
# action), so this settle cuts only the settled-confirmation residual: it is
# CONSERVATIVE-ONLY (never manufactures signal) and every leg that must be
# evaluated rides a paired window widening (D22-F2 span 60 -> 130) so the
# worst-case effective window span - 40 (slowest inject) - 45 + 15 (fastest
# restore) = 60 s keeps >= 60 requests, comfortably above the n>=30 floor.
GW_CONFIG_ROLLOUT_BAND_BOUND_S = 40.0
GW_CONFIG_CARRIER_SETTLE_BUFFER_S = 45.0
CARRIER_QUANTILE_METHOD = "nearest_rank"  # non-interpolating observed-order
                                           # quantile: the statistic is always an
                                           # actual request, never a synthetic
                                           # interpolation between two
_SIGNAL_MIGRATION_PROVENANCE = (
    'legacy-carry, pilot re-freeze pending (collection): threshold/operator/entity carried from the legacy '
    "injection_signal declaration; min_requests/min_successful_requests carry the C1 n>=30 floor "
    "(C1_MIN_POINTS, root r10 ruling keeps n>=30); boundary_exclusion_s=0.0 / settle_buffer_s / "
    "quantile_method=nearest_rank are conservative-explicit (see CARRIER_* constants above; no legacy "
    'counterpart); settle_buffer_s is FAMILY-SPLIT since EXEC-POD-XFER (collection 2026-09-22): pod-failure '
    "rules carry POD_FAILURE_CARRIER_SETTLE_BUFFER_S=35.0 (measured apply-to-bite lag bound 28.8-30.9 s "
    '+ margin), gateway/netem/cpu rules keep CARRIER_SETTLE_BUFFER_S=5.0; EXEC-GWXFER (collection 2026-09-22) '
    "splits the gateway-config family out with GW_CONFIG_CARRIER_SETTLE_BUFFER_S=45.0 (measured rollout "
    "band bound 40 s + 5 s C1 effectiveness buffer; the S24 rule previously and wrongly rode the "
    "pod-failure builder's 35.0); required_fraction, labels, "
    "query_id and the m1-prometheus-2s source_id have no r10 slot -- aggregate required_fraction is "
    "deleted by the r10 ruling and the stream_id ledger binding replaces the Prometheus label filter")


def _carrier_ledger_rule(kind: str, entity: str, stream_id: str, threshold: float, *,
                         settle_buffer_s: float = CARRIER_SETTLE_BUFFER_S) -> dict[str, Any]:
    """Build one r10 carrier-statistic rule (closed field set, no extras).

    The schema is closed-equivalence (obs.validate_carrier_rule: set(signal)==
    fields), so migration notes live in the atom provenance, never here.  The
    settle value is FAMILY-declared at the call site: pod-failure rules pass
    the measured lag bound (POD_FAILURE_CARRIER_SETTLE_BUFFER_S), every other
    family inherits the 5.0 switch-band figure.
    """
    statistic, unit = (("successful_request_p95_ratio", "ratio") if kind == "carrier_p95_ratio"
                       else ("request_error_fraction", "fraction"))
    return {"schema_version": obs.CARRIER_RULE_SCHEMA,
            "entity": entity, "stream_id": stream_id, "source_id": "m1-workload-ledger",
            "statistic": statistic, "unit": unit, "operator": "ge", "threshold": threshold,
            "min_requests": C1_MIN_POINTS, "min_successful_requests": C1_MIN_POINTS,
            "boundary_exclusion_s": CARRIER_BOUNDARY_EXCLUSION_S,
            "settle_buffer_s": settle_buffer_s,
            "quantile_method": CARRIER_QUANTILE_METHOD,
            "latency_start_field": "network_started_at_s",
            "error_statuses": ["http_error", "timed_out", "transport_error"],
            "artifact_ids": {"pre_fault": "pre_fault.workload", "during_fault": "during_fault.workload",
                             "run_record": "run-record"}}


def pod_failure_injection_signal(entity: str, stream_id: str, *, threshold: float = 0.5) -> dict[str, Any]:
    """Carrier error-fraction ledger rule for a pod-failure atom (S03 shape).

    collection migration: the old availability-gate error ratio (abort below 0.5; the
    inject-time bite poll used >= 0.6 on the target's own panel carrier, ledger
    B1 05 SS1 row 4) is now judged from the persisted request ledger by
    obs.evaluate_carrier_signal; the threshold is the legacy carried candidate
    (default 0.5) and the sample floor keeps the C1 n>=30 protocol floor.
    EXEC-POD-XFER (collection.1 2026-09-22): settle carries the measured PodChaos
    transfer-lag bound (35 s), so the eval window starts only after the spec
    patch has actually bitten the container -- the lag head is EXCLUDED, the
    denominator is never trimmed to manufacture signal (the paired window
    widening lives in the liveness profiles / combo windows).
    """
    return _carrier_ledger_rule("carrier_error_fraction", entity, stream_id, threshold,
                                settle_buffer_s=POD_FAILURE_CARRIER_SETTLE_BUFFER_S)


def gw_config_timeout_injection_signal(entity: str, stream_id: str, *, threshold: float = 0.5) -> dict[str, Any]:
    """Carrier error-fraction ledger rule for the gateway-config timeout atom (S24).

    collection migration: identical statistic face to the pod rule (request-ledger
    error fraction, threshold = the legacy carried 0.5 candidate, C1 n>=30
    floor) -- only the FAMILY settle differs.  EXEC-GWXFER (collection.1 2026-09-22):
    the S24 rule previously reused pod_failure_injection_signal and silently
    inherited EXEC-POD-XFER's 35 s pod lag bound; the gateway family now
    declares its own GW_CONFIG_CARRIER_SETTLE_BUFFER_S (measured rollout band
    bound 40 s + 5 s C1 effectiveness buffer).  The head cut is conservative
    only (the runner's instance window already starts at the inject confirm,
    which waits out the rollout), and the evaluable legs ride paired window
    widenings (D22-F2 60 -> 130 s) so the worst-case effective window keeps
    >= 60 requests above the n>=30 floor.
    """
    return _carrier_ledger_rule("carrier_error_fraction", entity, stream_id, threshold,
                                settle_buffer_s=GW_CONFIG_CARRIER_SETTLE_BUFFER_S)


def gateway_config_marker_signal(entity: str, directive: str, value: str) -> dict[str, Any]:
    """Presence-evidence channel for the pure retry atom (S23, C1 SS3.4).

    All old evidence shows retry-off has NO standalone failure signature (its
    observability was parasitic on timeout/slow-upstream overlap buckets, old
    L3569-3573), so S23's declared injection evidence is the config-presence
    channel -- the durable gateway transition record plus knob_isolation prove
    the knob switched as declared.  Effect signatures are an observation/pilot
    matter; the evaluator records this kind as NOT_ASSESSED honestly.
    """
    return {"entity": entity, "source_id": "m1-gateway-transition", "unit": "directive_state",
            "labels": {"directive": directive},
            "min_points": 1, "mode": "settled_window", "operator": "eq",
            "threshold": value, "required_fraction": 1.0}


def cpu_ratio_injection_signal(entity: str, stream_id: str, *, threshold: float = CPU_RATIO_THRESHOLD) -> dict[str, Any]:
    """Carrier p95-ratio ledger rule for a CPU-saturation atom (S03 precedent).

    collection migration: the old gates judged CPU legs by RELATIVE carrier p95 ratio
    (never absolute ms); the ratio is now computed by obs.evaluate_carrier_signal
    from successful 2xx request latencies in the persisted ledger
    (terminal_at_s - network_started_at_s, nearest-rank p95, during/pre).  The
    threshold carries CPU_RATIO_THRESHOLD (1.8 candidate); the legacy aggregate
    required_fraction (0.8, fraction-of-windows semantics) has no r10 slot and
    is deleted by the root r10 ruling -- the single-window aggregate judgement
    plus the n>=30 request floor replace it.
    """
    return _carrier_ledger_rule("carrier_p95_ratio", entity, stream_id, threshold)


# --------------------------------------------------------------------------- #
# First-batch atoms (task ASM-1): S08, S25, S01, S03 -- all k8s mechanisms.
# --------------------------------------------------------------------------- #

# --- S08 dependency_latency@catalog (app_env_hook) -------------------------- #
# CATALOG atom: dependency_latency@catalog (single_design_id S08; mechanism
#   "app env FAULT_DELAY_MS before_request sleep"; parameter profile status
#   NOT_FROZEN_MATCH_WITH_CONTRAST_FAMILY).
# Ledger: 04-dependency-latency.md -- env-hook primitive family, carrier
#   catalog_direct (bypass gw), single_sli_gate slow-not-failed shape.
# Live precedent: S08 smoke attempt-6 (Q1 fraction 0.923, Q2 pass) -- the byte
#   values below are that driver's declared smoke values, re-declared here at
#   pilot status; strength 2000 ms is the historical config.cat_delay_ms default.
_S08_TELEMETRY = TelemetryIds(
    metrics="m1-prometheus-2s", traces="jaeger-16686", logs="catalog-pod-logs",
    components="m1-kubectl-components", checksum="m1-mysql-checksum",
    locks="m1-mysql-locks", operations="m1-kubectl-primitives")

# --- S25 dependency_latency@inventory (same mechanism, different entity) ---- #
# CATALOG atom: dependency_latency@inventory (single_design_id S25,
#   atomic_completion from D07/D11/D17/T02 legs). Ledger 04 (D1): separate
#   entity/carrier/GT; parameter declarations unified to the single
#   {delay_ms: int} form (old direct-pass 2000 and config.inv_delay_ms both
#   collapse here; "default 2000" never enters code, it re-freezes per profile).

_S25_TELEMETRY = TelemetryIds(
    metrics="m1-prometheus-2s", traces="jaeger-16686", logs="inventory-pod-logs",
    components="m1-kubectl-components", checksum="m1-mysql-checksum",
    locks="m1-mysql-locks", operations="m1-kubectl-primitives")

# --- S01 network_delay@catalog-gw (chaos_mesh NetworkChaos) ----------------- #
# CATALOG atom: network_delay@catalog-gw (single_design_id S01; historical
#   {"delay_ms":500,"direction":"to","jitter_ms":50}). Ledger 03: single-sided
#   egress netem on the gw; carrier = pricing /api/pricing/<item> read-only GET

#   >=800 ms p95 shift on the gw path. direction:"to" is fixed by the primitive
#   schema (primitives.prepare_fault network branch); correlation 0 mirrors the
#   static pilot yaml; duration_s=60 covers the 55 s fault window as CRD upper
#   bound -- the true window is still delete-aligned (ledger 03 SS3.2-1).
# NOT_FROZEN: 800 ms keeps the old gw-path calibration only as a candidate.
_S01_TELEMETRY = TelemetryIds(
    metrics="m1-prometheus-2s", traces="jaeger-16686", logs="catalog-gw-pod-logs",
    components="m1-kubectl-components", checksum="m1-mysql-checksum",
    locks="m1-mysql-locks", operations="m1-kubectl-primitives")

# --- S03 service_unavailable@catalog (chaos_mesh PodChaos pod-failure) ------ #
# CATALOG atom: service_unavailable@catalog (single_design_id S03; mechanism
#   "Chaos Mesh PodChaos action=pod-failure; same-pod pause-image substitution"
#   -- the MECHANISM-CORRECTION wording, NOT the old "kill pod" description).
# Ledger 05: carrier = catalog through gw (errors bite only after the declared transfer-lag head (measured 28.8-30.9 s, bound 35 = pod settle));
#   availability gate judged by carrier error ratio.

#   parameterized -- see S03_LIVENESS_PROFILES below.  Default profile
#   shrinks the window below the measured kill band [40,60) s; the family

#   smoke validation of either form (restart_delta compatibility recorded).
# The injection signal is an explicit error-channel object, not a latency
#   metric_threshold: the current evaluate_quality records such signals as
#   NOT_ASSESSED(unsupported_signal_kind). Declaring the intended judgement
#   shape here is deliberate and honest -- the error-channel evaluator and the

#   30 follows the C1 protocol SS2.5 per-window per-carrier sample floor.
_S03_TELEMETRY = TelemetryIds(
    metrics="m1-prometheus-2s", traces="jaeger-16686", logs="catalog-pod-logs",
    components="m1-kubectl-components", checksum="m1-mysql-checksum",
    locks="m1-mysql-locks", operations="m1-kubectl-primitives")


@dataclass(frozen=True)
class AtomSpec:
    scenario_id: str
    atom_id: str
    purpose: str                      # smoke|pilot (formal is not assemblable here)
    scope: str                        # m1_main|extension
    rule_version_base: str
    random_seed: int
    phases: tuple[tuple[int, int], ...]
    leg: FaultLegSpec | DbFaultLegSpec
    carrier: CarrierStreamSpec
    telemetry: TelemetryIds
    signal: MetricSignalSpec                                  # latency GT + recovery rule
    injection_signal_kind: str = "metric_threshold"
    injection_signal: Mapping[str, Any] | None = None         # custom explicit object (error channel)
    preconditions: tuple[Precondition, ...] = ()              
    provenance: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require(bool(self.scenario_id) and bool(self.atom_id), "spec ids required")
        _require(self.atom_id == self.leg.fault_type + "@" + self.leg.entity, "spec: atom_id must be fault_type@entity")
        _require(self.purpose in {"smoke", "pilot"}, "spec: only smoke/pilot assembly is defined")
        _require(self.scope in {"m1_main", "extension"}, "spec: invalid scope")
        _require(len(self.phases) == 3 and all(len(w) == 2 for w in self.phases), "spec: three phase windows required")
        _require(self.phases[0][0] == 0 and self.phases[0][1] <= self.phases[1][0]
                 and self.phases[1][1] <= self.phases[2][0], "spec: phases must be ordered from zero")
        during = self.phases[1]
        _require(during[0] <= self.leg.window[0] and self.leg.window[1] <= during[1],
                 "spec: planned window must sit inside during_fault")
        _require(self.injection_signal_kind == "metric_threshold"
                 or isinstance(self.injection_signal, dict), "custom injection signal must be an explicit object")
        if self.injection_signal_kind == "metric_threshold":
            _require(self.injection_signal is None, "metric_threshold signal comes from signal spec")
        
        _require(self.signal.entity == self.leg.entity, "spec: signal entity must equal the root entity")
        if isinstance(self.injection_signal, dict):
            _require(self.injection_signal.get("entity") == self.leg.entity,
                     "spec: custom injection signal entity must equal the root entity")
            
            # stream (the workload-ledger judgement matches plan/ledger streams
            # by stream_id -- a mismatched binding can never be selected).
            if self.injection_signal.get("schema_version") == obs.CARRIER_RULE_SCHEMA:
                _require(self.injection_signal.get("stream_id") == self.carrier.stream_id,
                         "spec: carrier ledger rule must bind the atom's carrier stream_id")
        
        _require(isinstance(self.provenance, Mapping) and bool(self.provenance),
                 "spec: provenance anchors required")
        _require("catalog_atom" in self.provenance and "ledger" in self.provenance,
                 "spec: provenance must anchor the CATALOG atom and the ledger file")
        _require(type(self.preconditions) is tuple
                 and all(isinstance(item, Precondition) for item in self.preconditions),
                 "spec: preconditions must be explicit Precondition tuples")


# --------------------------------------------------------------------------- #
# Second-batch atoms (task ASM-2): host_cpu / service_cpu / runtime_exception

# --------------------------------------------------------------------------- #

def _std_telemetry(logs_source: str) -> TelemetryIds:
    """Family telemetry identity; only the log source differs per entity."""
    return TelemetryIds(metrics="m1-prometheus-2s", traces="jaeger-16686", logs=logs_source,
                        components="m1-kubectl-components", checksum="m1-mysql-checksum",
                        locks="m1-mysql-locks", operations="m1-kubectl-primitives")


_P_STD = CPU_LIVENESS_PROFILES["std_20_3_3_slow_not_fail"]
_P_RECA = CPU_LIVENESS_PROFILES["recagent_20_3_5"]
_P_TOL = CPU_LIVENESS_PROFILES["tolerant_30_5_5_structurally_unreachable"]
_P_HOST = CPU_LIVENESS_PROFILES["host_node_saturation"]
_P_S07 = S07_ROLLOUT_PROFILES[S07_DEFAULT_ROLLOUT_PROFILE]

# ASM-3 third batch: pod-failure selector-extension liveness profiles (per-yaml)
# and the gateway switch-band profile.
_PF_STD = POD_FAILURE_LIVENESS_PROFILES["std_20_3_3_below_kill_band"]
_PF_RECA = POD_FAILURE_LIVENESS_PROFILES["recagent_20_3_5_below_kill_band"]
_PF_BACKEND = POD_FAILURE_LIVENESS_PROFILES["backend_tolerant_30_5_5_structurally_unreachable"]
_P_GW = GATEWAY_ROLLOUT_PROFILES[GATEWAY_DEFAULT_ROLLOUT_PROFILE]

# ASM-4 fourth batch: net-family per-yaml probe decisions.
_P_NET_LOSS = NET_LIVENESS_PROFILES["gw_loss_no_liveness_readiness_watched"]
_P_NET_RECA = NET_LIVENESS_PROFILES["recagent_delay_within_probe_timeout"]

# Workers tiers (NOT_FROZEN; ledger 01 SS0 paths A-D + SS3.2-1 calibration
# mandate): catalog/cart take the 500m one-worker calibration; the other 500m
# retarget targets keep the old default 2; sasrec takes the canonical sweep
# result 8 (2x of its 4-core pod quota); host takes the qualification 32
# (1 worker/vCPU, topology-coupled -- declared precondition).
_WORKERS_500M_CALIBRATED = 1
_WORKERS_500M_DEFAULT = 2
_WORKERS_SASREC = 8
_WORKERS_HOST = 32
_LOAD_PERCENT = 100


ATOM_SPECS: dict[str, AtomSpec] = {
    "S08": AtomSpec(
        scenario_id="S08", atom_id="dependency_latency@catalog", purpose="pilot", scope="m1_main",
        rule_version_base="s08-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="dependency_latency", mechanism="app_env_hook", fault_class="runtime",
            entity="catalog", deployment="catalog", selector={"app": "catalog"},
            parameters={"delay_ms": 2000}, window=(30, 85)),
        carrier=CarrierStreamSpec(
            stream_id="catalog-items-read", service="catalog:5005", path_prefix="/api/items/",
            carrier="panel-catalog-direct", direct_api=True),
        telemetry=_S08_TELEMETRY,
        signal=MetricSignalSpec(
            entity="catalog", service_name="catalog_service", http_target="/api/items/<item_id>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        provenance={
            "catalog_atom": "dependency_latency@catalog",
            "ledger": _LEDGER + "/04-dependency-latency.md",
            "live_precedent": 'configs/collection/scenarios.json: assembly definition',
            "parameter_status": "NOT_FROZEN_MATCH_WITH_CONTRAST_FAMILY",
            "request_profile_status": "NOT_FROZEN",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS",
            "qc_threshold_status": "NOT_FROZEN (S08 smoke-declared values as pilot candidates)"}),
    "S25": AtomSpec(
        scenario_id="S25", atom_id="dependency_latency@inventory", purpose="pilot", scope="m1_main",
        rule_version_base="s25-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="dependency_latency", mechanism="app_env_hook", fault_class="runtime",
            entity="inventory", deployment="inventory", selector={"app": "inventory"},
            parameters={"delay_ms": 2000}, window=(30, 85)),
        carrier=CarrierStreamSpec(
            stream_id="inventory-items-read", service="inventory:5013", path_prefix="/api/inventory/",
            carrier="panel-inventory-direct", direct_api=True),
        telemetry=_S25_TELEMETRY,
        signal=MetricSignalSpec(
            entity="inventory", service_name="inventory_service", http_target="/api/inventory/<item_id>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        provenance={
            "catalog_atom": "dependency_latency@inventory",
            "ledger": _LEDGER + "/04-dependency-latency.md (SS2.1 D1)",
            "live_precedent": 'none (first use; collection smoke required)',
            "parameter_status": "NOT_FROZEN_MATCH_WITH_CONTRAST_FAMILY (two legacy declarations unified)",
            "request_profile_status": "NOT_FROZEN",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS",
            "qc_threshold_status": "NOT_FROZEN (values extrapolated from the S08 family precedent)"}),
    "S01": AtomSpec(
        scenario_id="S01", atom_id="network_delay@catalog-gw", purpose="pilot", scope="m1_main",
        rule_version_base="s01-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="network_delay", mechanism="chaos_mesh", fault_class="network",
            entity="catalog-gw", deployment="catalog-gw", selector={"app": "catalog-gw"},
            parameters={"latency_ms": 500, "jitter_ms": 50, "correlation_percent": 0, "duration_s": 60},
            window=(30, 85)),
        carrier=CarrierStreamSpec(
            stream_id="pricing-items-read", service="pricing:5014", path_prefix="/api/pricing/",
            carrier="panel-pricing-via-gw", direct_api=True),
        telemetry=_S01_TELEMETRY,
        signal=MetricSignalSpec(
            entity="catalog-gw", service_name="pricing_service", http_target="/api/pricing/<item_id>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=800, required_fraction=0.5, absolute_tolerance=800, relative_tolerance=0),
        
        # prerequisite, NOT a cluster default.  The assembly layer never
        
        # and evidence.  Without it the S01 GT simply observes no shift.
        preconditions=(
            Precondition(
                statement="pricing reaches catalog through catalog-gw: CATALOG_SERVICE_URL must point at "
                          "http://catalog-gw (default manifest value is the direct catalog:5005 URL) and the "
                          "pricing deployment must be scaled to a ready replica for the run window",
                mechanism_anchor='k8s/services/pricing.yaml default CATALOG_SERVICE_URL=http://catalog:5005; '
                                 'legacy runtime redirect in pricing-route auxiliary setup header notes '
                                 "(set env CATALOG_SERVICE_URL=http://catalog-gw + scale 1, restore scale 0 + URL "
                                 "afterwards)",
                owner='driver/runtime-binding layer (collection); assembly declares, never provisions',
                status="NOT_FROZEN candidate mechanism; default cluster state does NOT satisfy it"),),
        provenance={
            "catalog_atom": "network_delay@catalog-gw",
            "ledger": _LEDGER + "/03-network-chaos.md",
            "live_precedent": 'none in the new pipeline (collection smoke required)',
            "parameter_status": "NOT_FROZEN (historical 500/50 gw profile as candidate; direction fixed by primitive)",
            "request_profile_status": "NOT_FROZEN",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS",
            "runtime_preconditions": "pricing env redirect + scale-up required before the gw-path GT can "
                                     "trigger (see spec.preconditions; owner=driver/runtime binding)",
            "qc_threshold_status": "NOT_FROZEN (800 ms keeps the old gw-path calibration as candidate only)"}),
    "S03": AtomSpec(
        scenario_id="S03", atom_id="service_unavailable@catalog", purpose="pilot", scope="m1_main",
        rule_version_base="s03-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_unavailable", mechanism="chaos_mesh", fault_class="lifecycle",
            entity="catalog", deployment="catalog", selector={"app": "catalog"},
            parameters={"action": "pod-failure",
                        "duration_s": S03_LIVENESS_PROFILES[S03_DEFAULT_LIVENESS_PROFILE].duration_s},
            window=S03_LIVENESS_PROFILES[S03_DEFAULT_LIVENESS_PROFILE].window),
        carrier=CarrierStreamSpec(
            stream_id="catalog-items-via-gw", service="catalog-gw:80", path_prefix="/api/items/",
            
            # land on catalog_service (S03 signal service_name on the same
            # http_target; Jaeger census has catalog_service, no gateway).
            carrier="panel-catalog-via-gw", direct_api=True, trace_service="catalog_service"),
        telemetry=_S03_TELEMETRY,
        signal=MetricSignalSpec(
            entity="catalog", service_name="catalog_service", http_target="/api/items/<item_id>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        injection_signal_kind="carrier_error_fraction",
        
        
        # 0.5 = the old availability-gate ratio carried verbatim.
        injection_signal=pod_failure_injection_signal("catalog", "catalog-items-via-gw"),
        provenance={
            "catalog_atom": "service_unavailable@catalog",
            "ledger": _LEDGER + "/05-pod-failure.md",
            "live_precedent": 'none in the new pipeline (collection smoke required)',
            "parameter_status": "NOT_FROZEN ({action:pod-failure} fixed by primitive schema)",
            "request_profile_status": "NOT_FROZEN (carrier through gw: errors bite only after the declared transfer-lag head (measured 28.8-30.9 s, bound 35 = pod settle))",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             + S03_DEFAULT_LIVENESS_PROFILE + " profile, see S03_LIVENESS_PROFILES)",
            "liveness_decision": S03_LIVENESS_PROFILES[S03_DEFAULT_LIVENESS_PROFILE].decision,
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (error ratio 0.5 from the old availability gate; "
                                   'judged from the workload ledger since collection, pilot re-freeze pending)'}),
    # --- ASM-2: service_cpu_saturation singles (ledger B3 01) ----------------- #
    "S04": AtomSpec(
        scenario_id="S04", atom_id="service_cpu_saturation@catalog", purpose="pilot", scope="m1_main",
        rule_version_base="s04-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_cpu_saturation", mechanism="chaos_mesh", fault_class="resource",
            entity="catalog", deployment="catalog", selector={"app": "catalog"},
            parameters={"workers": _WORKERS_500M_CALIBRATED, "load_percent": _LOAD_PERCENT,
                        "duration_s": _P_STD.duration_s},
            window=_P_STD.window),
        carrier=CarrierStreamSpec(
            stream_id="catalog-items-cpu-read", service="catalog:5005", path_prefix="/api/items/",
            carrier="panel-catalog-direct", direct_api=True),
        telemetry=_std_telemetry("catalog-pod-logs"),
        signal=MetricSignalSpec(
            entity="catalog", service_name="catalog_service", http_target="/api/items/<item_id>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5,
            absolute_tolerance=CPU_RECOVERY_TOLERANCE_MS, relative_tolerance=0),
        injection_signal_kind="carrier_p95_ratio",
        injection_signal=cpu_ratio_injection_signal("catalog", "catalog-items-cpu-read"),
        provenance={
            "catalog_atom": "service_cpu_saturation@catalog",
            "ledger": _LEDGER_B3 + "/01-service-cpu.md",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: probe2 '
                              "slow-not-fail + static chaos-stress-catalog-cpu.yaml (workers=2)",
            "parameter_status": "NOT_FROZEN (workers=1 inherits the T05/T08 500m calibration: one worker "
                                "already saturates the 500m quota, a second only amplifies DB-pool contention "
                                "[ledger 01 SS3.2-1]; the old static-yaml default 2 must NOT be inherited "
                                "without argument)",
            "request_profile_status": "NOT_FROZEN (carrier = the target service itself through the panel/"
                                      "apiserver-proxy route, ledger 01 SS0/SS1-10; never port-forward)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "std_20_3_3_slow_not_fail profile, see CPU_LIVENESS_PROFILES)",
            "liveness_decision": _P_STD.decision,
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (ratio>=1.8x AND ok>=0.8 candidates from single_sli_gate "
                                   "L3650-3708; cfs_throttle/confinement arms stay SOFT/None-tolerant and "
                                   'never join the hard judgement; ratio evaluator placement open collection item)'}),
    "S09": AtomSpec(
        scenario_id="S09", atom_id="service_cpu_saturation@order", purpose="pilot", scope="m1_main",
        rule_version_base="s09-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_cpu_saturation", mechanism="chaos_mesh", fault_class="resource",
            entity="order", deployment="order", selector={"app": "order"},
            parameters={"workers": _WORKERS_500M_DEFAULT, "load_percent": _LOAD_PERCENT,
                        "duration_s": _P_STD.duration_s},
            window=_P_STD.window),
        carrier=CarrierStreamSpec(
            stream_id="order-detail-read", service="order:5010", path_prefix="/api/orders/",
            carrier="carrier-order-detail", direct_api=True),
        telemetry=_std_telemetry("order-pod-logs"),
        signal=MetricSignalSpec(
            entity="order", service_name="order_service", http_target="/api/orders/<order_no>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5,
            absolute_tolerance=CPU_RECOVERY_TOLERANCE_MS, relative_tolerance=0),
        injection_signal_kind="carrier_p95_ratio",
        injection_signal=cpu_ratio_injection_signal("order", "order-detail-read"),
        # The panel list endpoint /api/orders?user_token=... is inexpressible as
        # a contract stream: the frozen contract layer rejects credential-looking
        # query keys (user_token matches the token guard).  The declared carrier
        # is the read-only single-order GET with an order_no path parameter.
        preconditions=(
            Precondition(
                statement="the carrier probe parameter for /api/orders/<order_no> must be an existing, "
                          "stable order_no (read-only GET; enrich fan-out to catalog is read-only): the "
                          "driver binding supplies and verifies the probe order_no before the attempt -- "
                          "the panel list endpoint cannot serve as the contract stream because its "
                          "user_token query key is rejected by the frozen contract credential guard",
                mechanism_anchor="services/order_service/app.py L435 GET /api/orders/<order_no> (panel "
                                 "list variant L329 needs ?user_token=); contract.py _SECRET query guard "
                                 "(token keys) rejects the panel URL shape",
                owner='driver/runtime-binding layer (collection): probe order_no selection/verification; '
                      "assembly declares, never provisions",
                status='NOT_FROZEN probe premise; the panel business carrier remains a collection '
                       "observation-design alternative outside this contract shape"),),
        provenance={
            "catalog_atom": "service_cpu_saturation@order",
            "ledger": _LEDGER_B3 + "/01-service-cpu.md (SS0 path C retarget)",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: M9-R '
                              "retarget legs, 6 targets live-verified 500m (L8977-8978)",
            "parameter_status": "NOT_FROZEN (workers=2 = old retarget default on the live-verified 500m "
                                "targets; ledger 01 SS1-9/SS3.2-1)",
            "request_profile_status": "NOT_FROZEN (declared carrier = read-only single-order GET with an "
                                      "order_no path parameter and enrich fan-out; the panel list endpoint's "
                                      "user_token query key is rejected by the frozen contract credential "
                                      "guard -- see spec.preconditions)",
            "runtime_preconditions": "existing probe order_no for the carrier stream; owner=driver/runtime "
                                     "binding (see spec.preconditions)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "std_20_3_3_slow_not_fail profile, see CPU_LIVENESS_PROFILES)",
            "liveness_decision": _P_STD.decision,
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (same ratio>=1.8x AND ok>=0.8 candidates as S04; "
                                   "single-atom judgement needs no per-pod throttle hard arm)"}),
    "S10": AtomSpec(
        scenario_id="S10", atom_id="service_cpu_saturation@cart", purpose="pilot", scope="m1_main",
        rule_version_base="s10-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_cpu_saturation", mechanism="chaos_mesh", fault_class="resource",
            entity="cart", deployment="cart", selector={"app": "cart"},
            parameters={"workers": _WORKERS_500M_CALIBRATED, "load_percent": _LOAD_PERCENT,
                        "duration_s": _P_STD.duration_s},
            window=_P_STD.window),
        carrier=CarrierStreamSpec(
            stream_id="cart-health-read", service="cart:5006", path_prefix="/health",
            carrier="carrier-cart-health", direct_api=True, parameterized=False),
        telemetry=_std_telemetry("cart-pod-logs"),
        signal=MetricSignalSpec(
            entity="cart", service_name="cart_service", http_target="/health",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5,
            absolute_tolerance=CPU_RECOVERY_TOLERANCE_MS, relative_tolerance=0),
        injection_signal_kind="carrier_p95_ratio",
        injection_signal=cpu_ratio_injection_signal("cart", "cart-health-read"),
        provenance={
            "catalog_atom": "service_cpu_saturation@cart",
            "ledger": _LEDGER_B3 + "/01-service-cpu.md (SS0 path C; T05 calibration L13365-13370)",
            "live_precedent": 'none in the new pipeline (collection smoke required)',
            "parameter_status": "NOT_FROZEN (workers=1 inherits the T05 cart calibration frozen in the old "
                                "registry note: 500m one worker already saturates the quota, the second only "
                                "amplifies synchronous DB-pool contention [ledger 01 SS3.2-1 mandate])",
            "request_profile_status": "NOT_FROZEN (declared carrier = /health: cart's only business GET "
                                      "(panel /api/cart/count) needs a user_token query key that the frozen "
                                      "contract credential guard rejects, so the health endpoint is the only "
                                      "expressible read-only stream; whether /health carries the CPU "
                                      'signature is an collection measurement, with the panel business carrier as '
                                      "the observation-design alternative outside this contract shape)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "std_20_3_3_slow_not_fail profile, see CPU_LIVENESS_PROFILES)",
            "liveness_decision": _P_STD.decision,
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (same ratio candidates as S04; throttle arm SOFT only)"}),
    "S11": AtomSpec(
        scenario_id="S11", atom_id="service_cpu_saturation@review-query", purpose="pilot", scope="m1_main",
        rule_version_base="s11-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_cpu_saturation", mechanism="chaos_mesh", fault_class="resource",
            entity="review-query", deployment="review-query", selector={"app": "review-query"},
            parameters={"workers": _WORKERS_500M_DEFAULT, "load_percent": _LOAD_PERCENT,
                        "duration_s": _P_STD.duration_s},
            window=_P_STD.window),
        carrier=CarrierStreamSpec(
            stream_id="review-query-list-read", service="review-query:5018",
            path_prefix="/api/reviews?item_id=", path_suffix="&per_page=5&enrich=1",
            carrier="panel-review-query-list", direct_api=True),
        telemetry=_std_telemetry("review-query-pod-logs"),
        signal=MetricSignalSpec(
            entity="review-query", service_name="review_query_service", http_target="/api/reviews",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5,
            absolute_tolerance=CPU_RECOVERY_TOLERANCE_MS, relative_tolerance=0),
        injection_signal_kind="carrier_p95_ratio",
        injection_signal=cpu_ratio_injection_signal("review-query", "review-query-list-read"),
        provenance={
            "catalog_atom": "service_cpu_saturation@review-query",
            "ledger": _LEDGER_B3 + "/01-service-cpu.md (SS0 path C retarget)",
            "live_precedent": 'none in the new pipeline (collection smoke required)',
            "parameter_status": "NOT_FROZEN (workers=2 old retarget default, 500m live-verified)",
            "request_profile_status": "NOT_FROZEN (panel review list endpoint, item-id query parameter; "
                                      "enrich=1 fan-out to catalog is part of the panel-proven read-only path)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "std_20_3_3_slow_not_fail profile, see CPU_LIVENESS_PROFILES)",
            "liveness_decision": _P_STD.decision,
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (same ratio candidates as S04)"}),
    "S12": AtomSpec(
        scenario_id="S12", atom_id="service_cpu_saturation@backend", purpose="pilot", scope="m1_main",
        rule_version_base="s12-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_cpu_saturation", mechanism="chaos_mesh", fault_class="resource",
            entity="backend", deployment="backend",
            # live canonical label（SDEPLOY-SELECTOR-PIN deviant, F-S1-3）
            selector={"app": "backend_api"},
            parameters={"workers": _WORKERS_500M_DEFAULT, "load_percent": _LOAD_PERCENT,
                        "duration_s": _P_TOL.duration_s},
            window=_P_TOL.window),
        carrier=CarrierStreamSpec(
            stream_id="backend-stats-read", service="backend:5000", path_prefix="/api/stats/model",
            carrier="panel-backend-stats", direct_api=True, parameterized=False),
        telemetry=_std_telemetry("backend-pod-logs"),
        signal=MetricSignalSpec(
            entity="backend", service_name="backend_api", http_target="/api/stats/model",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5,
            absolute_tolerance=CPU_RECOVERY_TOLERANCE_MS, relative_tolerance=0),
        injection_signal_kind="carrier_p95_ratio",
        injection_signal=cpu_ratio_injection_signal("backend", "backend-stats-read"),
        # Runtime premise (retarget label lesson): the real pod label is
        # app=backend_api, NOT app=backend -- the contract selector itself pins
        # the LIVE label (SDEPLOY-SELECTOR-PIN precedent; no driver-side
        # name->label mapping anymore, the old RETARGET_APP_LABEL duty).
        preconditions=(
            Precondition(
                statement="backend pods carry app=backend_api, not app=backend: the canonical contract "
                          "selector pins the LIVE label {app: backend_api} directly (SDEPLOY-SELECTOR-PIN "
                          "precedent, F-S1-3) while the Deployment object stays 'backend'; the live pin "
                          "must re-verify the deployment matchLabels and the pinned pod's labels against "
                          "that selector (old RETARGET_APP_LABEL lesson -- mixing deploy name and app "
                          "label silently mismatches selectors)",
                mechanism_anchor='k8s/services/backend.yaml:21 label app=backend_api; old runner '
                                 "RETARGET_APP_LABEL L486-487; primitives.PinnedTarget pins the live "
                                 "label as the canonical selector value (F-S1-3)",
                owner='driver/runtime-binding layer (collection); assembly declares, never provisions',
                status="NOT_FROZEN pinned-live-label premise; default cluster state satisfies the canonical "
                       "selector literally (matchLabels app=backend_api)"),),
        provenance={
            "catalog_atom": "service_cpu_saturation@backend",
            "ledger": _LEDGER_B3 + "/01-service-cpu.md (SS0 path C retarget; label lesson SS1-9)",
            "live_precedent": 'none in the new pipeline (collection smoke required)',
            "parameter_status": "NOT_FROZEN (workers=2 old retarget default, 500m live-verified)",
            "request_profile_status": "NOT_FROZEN (panel backend stats/model read-only GET; the POST "
                                      "/api/recommend path to sasrec is a red line and stays out of the "
                                      "request profile)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "tolerant_30_5_5_structurally_unreachable profile, see CPU_LIVENESS_PROFILES)",
            "liveness_decision": _P_TOL.decision,
            "runtime_preconditions": "contract selector pins the live label {app: backend_api} "
                                     "(SDEPLOY-SELECTOR-PIN deviant, F-S1-3); owner=driver/runtime "
                                     "binding (see spec.preconditions)",
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (same ratio candidates as S04)"}),
    "S13": AtomSpec(
        scenario_id="S13", atom_id="service_cpu_saturation@checkout", purpose="pilot", scope="m1_main",
        rule_version_base="s13-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_cpu_saturation", mechanism="chaos_mesh", fault_class="resource",
            entity="checkout", deployment="checkout", selector={"app": "checkout"},
            parameters={"workers": _WORKERS_500M_DEFAULT, "load_percent": _LOAD_PERCENT,
                        "duration_s": _P_STD.duration_s},
            window=_P_STD.window),
        carrier=CarrierStreamSpec(
            stream_id="checkout-health-read", service="checkout:5011", path_prefix="/health",
            carrier="carrier-checkout-health", direct_api=True, parameterized=False),
        telemetry=_std_telemetry("checkout-pod-logs"),
        signal=MetricSignalSpec(
            entity="checkout", service_name="checkout_service", http_target="/health",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5,
            absolute_tolerance=CPU_RECOVERY_TOLERANCE_MS, relative_tolerance=0),
        injection_signal_kind="carrier_p95_ratio",
        injection_signal=cpu_ratio_injection_signal("checkout", "checkout-health-read"),
        provenance={
            "catalog_atom": "service_cpu_saturation@checkout",
            "ledger": _LEDGER_B3 + "/01-service-cpu.md (SS0 path C retarget)",
            "live_precedent": 'none in the new pipeline (collection smoke required)',
            "parameter_status": "NOT_FROZEN (workers=2 old retarget default, 500m live-verified)",
            "request_profile_status": "NOT_FROZEN (declared carrier = /health: checkout's only business GET "
                                      "(panel /api/checkout/preview) needs a user_token query key that the "
                                      "frozen contract credential guard rejects, so the health endpoint is "
                                      "the only expressible read-only stream; whether /health carries the CPU "
                                      'signature is an collection measurement, with the panel business carrier as '
                                      "the observation-design alternative outside this contract shape)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "std_20_3_3_slow_not_fail profile, see CPU_LIVENESS_PROFILES)",
            "liveness_decision": _P_STD.decision,
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (same ratio candidates as S04)"}),
    "S20": AtomSpec(
        scenario_id="S20", atom_id="service_cpu_saturation@rec-agent", purpose="pilot", scope="m1_main",
        rule_version_base="s20-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_cpu_saturation", mechanism="chaos_mesh", fault_class="resource",
            entity="rec-agent", deployment="rec-agent",
            # live canonical label（SDEPLOY-SELECTOR-PIN deviant, F-S1-3）
            selector={"app": "recommendation_agent"},
            parameters={"workers": _WORKERS_500M_DEFAULT, "load_percent": _LOAD_PERCENT,
                        "duration_s": _P_RECA.duration_s},
            window=_P_RECA.window),
        carrier=CarrierStreamSpec(
            stream_id="recagent-health-read", service="rec-agent:5001", path_prefix="/recommend/health",
            carrier="panel-recagent-health", direct_api=True, parameterized=False),
        telemetry=_std_telemetry("rec-agent-pod-logs"),
        signal=MetricSignalSpec(
            entity="rec-agent", service_name="recommendation_agent", http_target="/recommend/health",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5,
            absolute_tolerance=CPU_RECOVERY_TOLERANCE_MS, relative_tolerance=0),
        injection_signal_kind="carrier_p95_ratio",
        injection_signal=cpu_ratio_injection_signal("rec-agent", "recagent-health-read"),
        preconditions=(
            Precondition(
                statement="rec-agent pods carry app=recommendation_agent, not app=rec-agent: the canonical "
                          "contract selector pins the LIVE label {app: recommendation_agent} directly "
                          "(SDEPLOY-SELECTOR-PIN precedent, F-S1-3) while the Deployment object stays "
                          "'rec-agent'; the live pin must re-verify the deployment matchLabels and the "
                          "pinned pod's labels against that selector (old RETARGET_APP_LABEL lesson)",
                mechanism_anchor='k8s/services/rec-agent.yaml:28 label app=recommendation_agent; old runner '
                                 "RETARGET_APP_LABEL L486-487; primitives.PinnedTarget pins the live "
                                 "label as the canonical selector value (F-S1-3)",
                owner='driver/runtime-binding layer (collection); assembly declares, never provisions',
                status="NOT_FROZEN pinned-live-label premise; default cluster state satisfies the canonical "
                       "selector literally (matchLabels app=recommendation_agent)"),),
        provenance={
            "catalog_atom": "service_cpu_saturation@rec-agent",
            "ledger": _LEDGER_B3 + "/01-service-cpu.md (SS0 path C retarget; label lesson SS1-9)",
            "live_precedent": 'none in the new pipeline (collection smoke required)',
            "parameter_status": "NOT_FROZEN (workers=2 old retarget default, 500m)",
            "request_profile_status": "NOT_FROZEN (gate carrier = the /recommend/health panel endpoint "
                                      "(always 200, no LLM, no DB); the recommend POST business-evidence "
                                      "carrier of the old dual-carrier design stays OUT of the read-only "
                                      "request profile -- old RECAGENT_*_CARRIER L500-501 knowledge)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: recagent_20_3_5 "
                             "profile, see CPU_LIVENESS_PROFILES)",
            "liveness_decision": _P_RECA.decision,
            "runtime_preconditions": "contract selector pins the live label {app: recommendation_agent} "
                                     "(SDEPLOY-SELECTOR-PIN deviant, F-S1-3); owner=driver/runtime "
                                     "binding (see spec.preconditions)",
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (same ratio candidates as S04; whether the always-200 health "
                                   'carrier carries a CPU signature at workers=2 is an collection measurement)'}),
    "S27": AtomSpec(
        scenario_id="S27", atom_id="service_cpu_saturation@sasrec", purpose="pilot", scope="m1_main",
        rule_version_base="s27-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_cpu_saturation", mechanism="chaos_mesh", fault_class="resource",
            entity="sasrec", deployment="sasrec", selector={"app": "sasrec"},
            parameters={"workers": _WORKERS_SASREC, "load_percent": _LOAD_PERCENT,
                        "duration_s": _P_TOL.duration_s},
            window=_P_TOL.window),
        carrier=CarrierStreamSpec(
            stream_id="sasrec-inference-post-v1", service="sasrec:8200", path_prefix="/recommend",
            carrier="panel-sasrec-inference-post-v1", direct_api=True, rate_rps=1.0,
            max_concurrency=1, client_retry_limit=0, parameterized=False),
        telemetry=_std_telemetry("sasrec-pod-logs"),
        signal=MetricSignalSpec(
            entity="sasrec", service_name="sasrec_api", http_target="/recommend",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5,
            absolute_tolerance=CPU_RECOVERY_TOLERANCE_MS, relative_tolerance=0),
        injection_signal_kind="carrier_p95_ratio",
        injection_signal=cpu_ratio_injection_signal("sasrec", "sasrec-inference-post-v1"),
        # (1) no-restart pin semantics: the CRD path never restarts the pod, but
        #     a restart (9.2GB pickle reload, 90-120 s) would blow any window;
        # (2) workers=8 is quota-coupled to the 4-core pod limit (2x overrun).
        preconditions=(
            Precondition(
                statement="the sasrec pod must never restart across the attempt: the StressChaos CRD path "
                          "itself does not restart pods, but a stale pin must never silently re-select a "
                          "replaced pod -- any restart triggers a 9.2GB pickle reload costing 90-120 s "
                          "(old M8 handoff figure; the earlier ~40 s wording was unsourced and retracted), "
                          "far beyond any planned window",
                mechanism_anchor="ledger 01 SS1-7 (old inject_stress_sasrec L1758 no-restart) + SS5-6; B3 "
                                 'acceptance collection errata (90-120 s sourced from the old M8 handoff)',
                owner='driver/runtime-binding layer (collection): pin freshness/re-selection policy; assembly '
                      "declares, never provisions",
                status="NOT_FROZEN pin semantics; violated premise invalidates the attempt, not just the GT"),
            Precondition(
                statement="workers=8 assumes the pinned sasrec pod keeps cpu limit=4 (2x pod-quota overrun "
                          "-> CFS thrashing + intra-op contention); the condition-tier freeze must bind the "
                          "pod cpu-limit/topology fingerprint",
                mechanism_anchor="k8s/services/sasrec.yaml L114 cpu '4'; ledger 01 SS1-3 (canonical sweep "
                                 "[3,4,6,8]: baseline p95 43.4ms, W=8 = 22.57x = 979.4ms, noise floor W<=4 "
                                 "vs signal W>=6) + SS3.2-1/SS5-4",
                owner='driver/runtime-binding layer (collection): topology fingerprint into the condition tier; '
                      "assembly declares, never provisions",
                status="NOT_FROZEN quota coupling; workers value re-freezes per node/pod topology"),),
        provenance={
            "catalog_atom": "service_cpu_saturation@sasrec",
            "ledger": _LEDGER_B3 + "/01-service-cpu.md (SS0 path B fork; SS1-3 sweep, SS1-7 no-restart)",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: DK12 '
                              "full-curve sweep + sasrec_net_dual_gate RATIO rule",
            "parameter_status": "NOT_FROZEN (workers=8 canonical sweep result = 2x the 4-core pod quota; "
                                "old absolute measurements: baseline p95 43.4ms, W=8 = 22.57x)",
            "request_profile_status": "NOT_FROZEN (versioned direct SASRec /recommend POST inference carrier; "
                                      "fixed payload is copied from the existing SASRec API client example, "
                                      "read-only business permission pins the exact route and body hash; "
                                      "request membership and p95 response remain pilot measurements)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "tolerant_30_5_5_structurally_unreachable profile, see CPU_LIVENESS_PROFILES)",
            "liveness_decision": _P_TOL.decision,
            "runtime_preconditions": "no-restart pin semantics (pickle reload 90-120 s) + 4-core quota "
                                     "coupling of workers=8 (see spec.preconditions)",
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (ratio-only judgement: the old sasrec gate explicitly FORBADE "
                                   "absolute-ms thresholds -- W=6 reached 11.25x while absolute 444.7ms "
                                   "<800 would falsely fail, L7575-7576; recovery tolerance 300ms keeps the "
                                   "DK12 recommend-fast anchor as candidate)"}),
    "S28": AtomSpec(
        scenario_id="S28", atom_id="service_cpu_saturation@pricing", purpose="pilot", scope="m1_main",
        rule_version_base="s28-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_cpu_saturation", mechanism="chaos_mesh", fault_class="resource",
            entity="pricing", deployment="pricing", selector={"app": "pricing"},
            parameters={"workers": _WORKERS_500M_DEFAULT, "load_percent": _LOAD_PERCENT,
                        "duration_s": _P_STD.duration_s},
            window=_P_STD.window),
        carrier=CarrierStreamSpec(
            stream_id="pricing-items-cpu-read", service="pricing:5014", path_prefix="/api/pricing/",
            carrier="panel-pricing-direct", direct_api=True),
        telemetry=_std_telemetry("pricing-pod-logs"),
        signal=MetricSignalSpec(
            entity="pricing", service_name="pricing_service", http_target="/api/pricing/<item_id>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5,
            absolute_tolerance=CPU_RECOVERY_TOLERANCE_MS, relative_tolerance=0),
        injection_signal_kind="carrier_p95_ratio",
        injection_signal=cpu_ratio_injection_signal("pricing", "pricing-items-cpu-read"),
        provenance={
            "catalog_atom": "service_cpu_saturation@pricing",
            "ledger": _LEDGER_B3 + "/01-service-cpu.md (SS0 path B fork yaml 4eba9dbd)",
            "live_precedent": 'none in the new pipeline (collection smoke required)',
            "parameter_status": "NOT_FROZEN (workers=2 old fork default; pricing keeps the same 500m cgroup "
                                "as catalog/inventory -- the stressor is confined to the pricing container)",
            "request_profile_status": "NOT_FROZEN (carrier = pricing itself through the panel/proxy route, "
                                      "read-only item GET)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "std_20_3_3_slow_not_fail profile, see CPU_LIVENESS_PROFILES)",
            "liveness_decision": _P_STD.decision,
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (same ratio candidates as S04; pricing was a SOFT-carrier leg "
                                   "in old combos -- single-atom judgement uses the target-self carrier)"}),
    # --- ASM-2: host_cpu_saturation@host (ledger B1 02) ----------------------- #
    "S05": AtomSpec(
        scenario_id="S05", atom_id="host_cpu_saturation@host", purpose="pilot", scope="m1_main",
        rule_version_base="s05-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="host_cpu_saturation", mechanism="chaos_mesh", fault_class="resource",
            entity="host", deployment=HOST_STRESSOR_DEPLOYMENT, selector={"app": HOST_STRESSOR_DEPLOYMENT},
            parameters={"workers": _WORKERS_HOST, "load_percent": _LOAD_PERCENT,
                        "duration_s": _P_HOST.duration_s},
            window=_P_HOST.window),
        carrier=CarrierStreamSpec(
            stream_id="pricing-items-hostcpu-read", service="pricing:5014", path_prefix="/api/pricing/",
            carrier="panel-pricing-direct", direct_api=True),
        telemetry=_std_telemetry("stressor-pod-logs"),
        signal=MetricSignalSpec(
            entity="host", service_name="pricing_service", http_target="/api/pricing/<item_id>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5,
            absolute_tolerance=CPU_RECOVERY_TOLERANCE_MS, relative_tolerance=0),
        injection_signal_kind="carrier_p95_ratio",
        injection_signal=cpu_ratio_injection_signal("host", "pricing-items-hostcpu-read"),
        # (1) the stressor carrier Deployment is provisioned/cleaned by the run
        #     infrastructure -- the new primitive deliberately narrowed the old
        #     unconditional delete-deployment backstop out of the CAS window;
        # (2) workers=32 is node-topology-coupled (1 worker/vCPU qualification).
        preconditions=(
            Precondition(
                statement="the off-graph CPU stressor carrier Deployment (no cpu limit, positive cpu "
                          "requests, sleep shell -- render_stressor_manifest shape, verified by the "
                          "adapter's host-carrier gate) must be applied and scaled to one ready replica "
                          "before the CRD apply and deleted (or scaled to 0) after recovery: the new "
                          "primitive only deletes the StressChaos CRD inside the CAS window, so an "
                          "unfulfilled cleanup leaves the stressor residual on the node",
                mechanism_anchor="ledger B1 02 SS3.2-1/SS5-1 (old recover_host_stress L1787-1796 deleted "
                                 "deploy/stressor unconditionally; deliberate narrowing transferred to the "
                                 "run infrastructure, P1-EXT probe5 evidence: delete actuates in ~0.2 s "
                                 "even under saturation); primitives render_stressor_manifest + "
                                 "_verify_host_carrier shape gate",
                owner='driver/runtime-binding layer (collection): stressor provisioning + cleanup with '
                      "write-ahead recovery intent; assembly declares, never provisions",
                status="NOT_FROZEN operational premise; S05 live qualification does not exist before it is "
                       "closed (ledger B1 02 SS5-1)"),
            Precondition(
                statement="workers=32 assumes a >=32 vCPU node (old qualification shape: exactly one worker "
                          "per vCPU; the probe VM had 12) -- the condition-tier freeze must bind the node "
                          "topology fingerprint",
                mechanism_anchor="ledger B1 02 SS1-1 (STRESS_HOST_WORKERS=32 L350, qualification host=32 "
                                 "vCPU) + SS5-3 (workers/vCPU coupling)",
                owner='driver/runtime-binding layer (collection): topology fingerprint into the condition tier; '
                      "assembly declares, never provisions",
                status="NOT_FROZEN topology coupling; strength re-freezes per node"),),
        provenance={
            "catalog_atom": "host_cpu_saturation@host",
            "ledger": _LEDGER + "/02-host-cpu.md",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: S05 '
                              "delivered cases with common_cause_gate churn restart_delta=0",
            "parameter_status": "NOT_FROZEN (workers=32 qualification shape, 1 worker/vCPU, "
                                "topology-coupled -- see preconditions; never --vm: the parameter schema "
                                "rejects vm/mem stressors, old L344 OOMKill iron rule)",
            "request_profile_status": "NOT_FROZEN (single declared carrier stream = pricing, a co-located "
                                      "victim on the shared node; the OLD gate judged >=2 spatial victims "
                                      "plus a disjoint-user discrimination -- that second-carrier/"
                                      'disjoint observation arm is an collection observation-design item and is '
                                      "NOT inherited by default, T08-precedent discipline; routing note "
                                      "[review m-3]: the old common_cause_gate judged pricing VIA "
                                      "catalog-gw while this stream hits the pricing Service directly -- "
                                      "the ratio candidate lands on the pricing_service server metric "
                                      "either way, but the exercised ingress path differs and re-freezes "
                                      'with the collection observation design)',
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "host_node_saturation profile, see CPU_LIVENESS_PROFILES)",
            "liveness_decision": _P_HOST.decision,
            "runtime_preconditions": "stressor carrier provisioning/cleanup + node-topology coupling of "
                                     "workers=32 (see spec.preconditions)",
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (host ratio 1.8x candidate, old HOST_CPU_RATIO_THR L354; "
                                   "VM-saturation scrape starvation stays soft/post-hoc and never a hard "
                                   'gate; churn restart_delta=0 expectation carries into collection observation)'}),
    # --- ASM-2: runtime_exception@catalog (ledger B3 02) ---------------------- #
    "S07": AtomSpec(
        scenario_id="S07", atom_id="runtime_exception@catalog", purpose="pilot", scope="m1_main",
        rule_version_base="s07-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="runtime_exception", mechanism="app_env_hook", fault_class="runtime",
            entity="catalog", deployment="catalog", selector={"app": "catalog"},
            parameters={"enabled": True}, window=_P_S07.window),
        carrier=CarrierStreamSpec(
            stream_id="catalog-items-runtime-read", service="catalog:5005", path_prefix="/api/items/",
            carrier="panel-catalog-direct", direct_api=True),
        telemetry=_std_telemetry("catalog-pod-logs"),
        signal=MetricSignalSpec(
            entity="catalog", service_name="catalog_service", http_target="/api/items/<item_id>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        injection_signal_kind="carrier_error_fraction",
        
        
        # 0.8 = the old runtime-gate error-dominant ratio carried verbatim.
        injection_signal=pod_failure_injection_signal("catalog", "catalog-items-runtime-read", threshold=0.8),
        provenance={
            "catalog_atom": "runtime_exception@catalog",
            "ledger": _LEDGER_B3 + "/02-runtime-exception.md",
            "live_precedent": 'none in the new pipeline (collection smoke required)',
            "parameter_status": "NOT_FROZEN ({enabled: true} is the single locked injection state -- the "
                                "primitive schema rejects every other value, old L1644-1654 semantics; "
                                "env key/value stay FAULT_RAISE/1 by mechanism)",
            "entity_binding": "catalog-only BY ASSEMBLY CONTRACT (B3 acceptance registered item): the "
                              "primitives layer shares the env-hook entity check over {catalog,inventory} "
                              "and four services carry before_request hooks, but only catalog owns the "
                              "ATOM-CATALOG atom and the old implementation (CATALOG_DEPLOY hard-coded); "
                              "any future runtime_exception@order/announcement must verify hook semantics "
                              "first (ledger 02 SS3.2-3)",
            "request_profile_status": "NOT_FROZEN (catalog direct read-only GET bypassing catalog-gw to "
                                      "observe the raw 500s; /api/items is pure SELECT -> CHECKSUM-safe)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (rollout decision: "
                             "event_aligned_family_window profile, see S07_ROLLOUT_PROFILES)",
            "rollout_decision": _P_S07.decision,
            "signature_note": "/health is exempt from the hook -> the pod stays Ready with restart_delta=0; "
                              "the error-dominant signature (error ratio >=0.8, old single gate L3635-3647) "
                              "is thereby distinguishable from S03 pod-failure (restart in {1,2})",
            "honesty_note": "FAULT_RAISE is an UNCONDITIONAL before_request raise -- an approximation of a "
                            "conditional business exception (old L4745 honesty label); the atom must never "
                            "be described as a conditional/triggered business fault",
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (error ratio 0.8 candidate; error-channel evaluator "
                                   'placement is an open collection item, same as S03)'}),
    # --- ASM-3: service_unavailable selector extension (ledger B1 05 SS2.3) ---- #
    # S03-mechanism singles over RETARGET_SERVICES (S14-S19/S22) plus the static
    # user yaml single (S26): same inject_pod_failure/recover_pod_failure
    # mechanism; the differences are declared per atom (carrier form, app-label
    # mapping, detection basis), never inherited silently.
    "S14": AtomSpec(
        scenario_id="S14", atom_id="service_unavailable@order", purpose="pilot", scope="m1_main",
        rule_version_base="s14-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_unavailable", mechanism="chaos_mesh", fault_class="lifecycle",
            entity="order", deployment="order", selector={"app": "order"},
            parameters={"action": "pod-failure", "duration_s": _PF_STD.duration_s},
            window=_PF_STD.window),
        carrier=CarrierStreamSpec(
            stream_id="order-detail-pod-read", service="order:5010", path_prefix="/api/orders/",
            carrier="carrier-order-detail", direct_api=True),
        telemetry=_std_telemetry("order-pod-logs"),
        signal=MetricSignalSpec(
            entity="order", service_name="order_service", http_target="/api/orders/<order_no>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        injection_signal_kind="carrier_error_fraction",
        injection_signal=pod_failure_injection_signal("order", "order-detail-pod-read"),
        preconditions=(
            Precondition(
                statement="the carrier probe parameter for /api/orders/<order_no> must be an existing, "
                          "stable order_no (read-only GET; enrich fan-out to catalog is read-only): the "
                          "driver binding supplies and verifies the probe order_no before the attempt -- "
                          "the old TARGET_CARRIER panel endpoint /api/orders?user_token= cannot serve as "
                          "the contract stream because its user_token query key is rejected by the frozen "
                          "contract credential guard",
                mechanism_anchor="services/order_service/app.py L435 GET /api/orders/<order_no> (panel "
                                 "list variant L329 needs ?user_token=); contract.py _SECRET query guard; "
                                 "old runner PANEL_TARGETS order row + TARGET_CARRIER L496",
                owner='driver/runtime-binding layer (collection): probe order_no selection/verification; '
                      "assembly declares, never provisions",
                status='NOT_FROZEN probe premise; the panel business carrier remains a collection '
                       "observation-design alternative outside this contract shape"),),
        provenance={
            "catalog_atom": "service_unavailable@order",
            "ledger": _LEDGER + "/05-pod-failure.md (SS2.3 D3 selector extension table; SS1 rows 4/9)",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: '
                              "inject_pod_failure/recover_pod_failure L3339-3373/L3376-3396 over "
                              "RETARGET_SERVICES (rendered PodChaos, app label order == deploy name)",
            "parameter_status": "NOT_FROZEN ({action:pod-failure} fixed by primitive schema; duration "
                                "follows the liveness profile)",
            "request_profile_status": "NOT_FROZEN (carrier = the target service itself through the panel/"
                                      "apiserver-proxy route, old TARGET_CARRIER L496 knowledge; the panel "
                                      "list endpoint's user_token query key is rejected by the frozen "
                                      "contract credential guard -- see spec.preconditions)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "std_20_3_3_below_kill_band profile, see POD_FAILURE_LIVENESS_PROFILES)",
            "liveness_decision": _PF_STD.decision,
            "runtime_preconditions": "existing probe order_no for the carrier stream; owner=driver/runtime "
                                     "binding (see spec.preconditions)",
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (error ratio 0.5 from the old availability-gate family; "
                                   'error-channel evaluator placement is an open collection item, same as S03)'}),
    "S15": AtomSpec(
        scenario_id="S15", atom_id="service_unavailable@cart", purpose="pilot", scope="m1_main",
        rule_version_base="s15-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_unavailable", mechanism="chaos_mesh", fault_class="lifecycle",
            entity="cart", deployment="cart", selector={"app": "cart"},
            parameters={"action": "pod-failure", "duration_s": _PF_STD.duration_s},
            window=_PF_STD.window),
        carrier=CarrierStreamSpec(
            stream_id="cart-health-pod-read", service="cart:5006", path_prefix="/health",
            carrier="carrier-cart-health", direct_api=True, parameterized=False),
        telemetry=_std_telemetry("cart-pod-logs"),
        signal=MetricSignalSpec(
            entity="cart", service_name="cart_service", http_target="/health",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        injection_signal_kind="carrier_error_fraction",
        injection_signal=pod_failure_injection_signal("cart", "cart-health-pod-read"),
        provenance={
            "catalog_atom": "service_unavailable@cart",
            "ledger": _LEDGER + "/05-pod-failure.md (SS2.3 D3 selector extension table; SS1 rows 4/9)",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: '
                              "retarget pod-failure legs (app label cart == deploy name)",
            "parameter_status": "NOT_FROZEN ({action:pod-failure} fixed by primitive schema; duration "
                                "follows the liveness profile)",
            "request_profile_status": "NOT_FROZEN (declared carrier = /health: the old TARGET_CARRIER "
                                      "panel endpoint /api/cart/count?user_token= cannot serve as the "
                                      "contract stream (frozen credential guard); under pod-failure the "
                                      "pause swap refuses /health connections immediately, so the error "
                                      "channel keeps its detection semantics -- unlike the CPU family's "
                                      'slow-not-fail concern; the panel business carrier remains a collection '
                                      "observation-design alternative)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "std_20_3_3_below_kill_band profile, see POD_FAILURE_LIVENESS_PROFILES)",
            "liveness_decision": _PF_STD.decision,
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (error ratio 0.5 from the old availability-gate family; "
                                   'error-channel evaluator placement is an open collection item, same as S03)'}),
    "S16": AtomSpec(
        scenario_id="S16", atom_id="service_unavailable@review-query", purpose="pilot", scope="m1_main",
        rule_version_base="s16-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_unavailable", mechanism="chaos_mesh", fault_class="lifecycle",
            entity="review-query", deployment="review-query", selector={"app": "review-query"},
            parameters={"action": "pod-failure", "duration_s": _PF_STD.duration_s},
            window=_PF_STD.window),
        carrier=CarrierStreamSpec(
            stream_id="review-query-list-pod-read", service="review-query:5018",
            path_prefix="/api/reviews?item_id=", path_suffix="&per_page=5&enrich=1",
            carrier="panel-review-query-list", direct_api=True),
        telemetry=_std_telemetry("review-query-pod-logs"),
        signal=MetricSignalSpec(
            entity="review-query", service_name="review_query_service", http_target="/api/reviews",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        injection_signal_kind="carrier_error_fraction",
        injection_signal=pod_failure_injection_signal("review-query", "review-query-list-pod-read"),
        provenance={
            "catalog_atom": "service_unavailable@review-query",
            "ledger": _LEDGER + "/05-pod-failure.md (SS2.3 D3 selector extension table; SS1 rows 4/9)",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: '
                              "retarget pod-failure legs (app label review-query == deploy name)",
            "parameter_status": "NOT_FROZEN ({action:pod-failure} fixed by primitive schema; duration "
                                "follows the liveness profile)",
            "request_profile_status": "NOT_FROZEN (the old TARGET_CARRIER panel endpoint itself is "
                                      "expressible: item-id query parameter, enrich=1 read-only fan-out; "
                                      "endpoint form equals the ASM-2 S11 carrier)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "std_20_3_3_below_kill_band profile, see POD_FAILURE_LIVENESS_PROFILES)",
            "liveness_decision": _PF_STD.decision,
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (error ratio 0.5 from the old availability-gate family; "
                                   'error-channel evaluator placement is an open collection item, same as S03)'}),
    "S17": AtomSpec(
        scenario_id="S17", atom_id="service_unavailable@backend", purpose="pilot", scope="m1_main",
        rule_version_base="s17-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_unavailable", mechanism="chaos_mesh", fault_class="lifecycle",
            entity="backend", deployment="backend",
            # live canonical label（SDEPLOY-SELECTOR-PIN deviant, F-S1-3）
            selector={"app": "backend_api"},
            parameters={"action": "pod-failure", "duration_s": _PF_BACKEND.duration_s},
            window=_PF_BACKEND.window),
        carrier=CarrierStreamSpec(
            stream_id="backend-stats-pod-read", service="backend:5000", path_prefix="/api/stats/model",
            carrier="panel-backend-stats", direct_api=True, parameterized=False),
        telemetry=_std_telemetry("backend-pod-logs"),
        signal=MetricSignalSpec(
            entity="backend", service_name="backend_api", http_target="/api/stats/model",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        injection_signal_kind="carrier_error_fraction",
        injection_signal=pod_failure_injection_signal("backend", "backend-stats-pod-read"),
        # Runtime premise (retarget label lesson): real pod label is
        # app=backend_api, NOT app=backend -- the contract selector itself pins
        # the LIVE label, same pinning as the ASM-2 S12 leg (F-S1-3).
        preconditions=(
            Precondition(
                statement="backend pods carry app=backend_api, not app=backend: the canonical contract "
                          "selector pins the LIVE label {app: backend_api} directly (SDEPLOY-SELECTOR-PIN "
                          "precedent, F-S1-3) while the Deployment object stays 'backend'; the live pin "
                          "must re-verify the deployment matchLabels and the pinned pod's labels against "
                          "that selector (old RETARGET_APP_LABEL lesson -- mixing deploy name and app "
                          "label silently mismatches selectors)",
                mechanism_anchor='k8s/services/backend.yaml:21 label app=backend_api; old runner '
                                 "RETARGET_APP_LABEL L486-487; primitives.PinnedTarget pins the live "
                                 "label as the canonical selector value (F-S1-3)",
                owner='driver/runtime-binding layer (collection); assembly declares, never provisions',
                status="NOT_FROZEN pinned-live-label premise; default cluster state satisfies the canonical "
                       "selector literally (matchLabels app=backend_api)"),),
        provenance={
            "catalog_atom": "service_unavailable@backend",
            "ledger": _LEDGER + "/05-pod-failure.md (SS2.3 D3 selector extension table; SS1 rows 4/9, "
                               "label lesson SS1-9)",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: '
                              "retarget pod-failure legs (app label backend_api != deploy name backend)",
            "parameter_status": "NOT_FROZEN ({action:pod-failure} fixed by primitive schema; duration "
                                "follows the liveness profile)",
            "request_profile_status": "NOT_FROZEN (the old TARGET_CARRIER panel endpoint /api/stats/model "
                                      "is expressible as a fixed-path read-only GET; the POST "
                                      "/api/recommend path to sasrec is a red line and stays out of the "
                                      "request profile)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "backend_tolerant_30_5_5_structurally_unreachable profile, see "
                             "POD_FAILURE_LIVENESS_PROFILES)",
            "liveness_decision": _PF_BACKEND.decision,
            "runtime_preconditions": "contract selector pins the live label {app: backend_api} "
                                     "(SDEPLOY-SELECTOR-PIN deviant, F-S1-3); owner=driver/runtime "
                                     "binding (see spec.preconditions)",
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (error ratio 0.5 from the old availability-gate family; "
                                   'error-channel evaluator placement is an open collection item, same as S03)'}),
    "S18": AtomSpec(
        scenario_id="S18", atom_id="service_unavailable@checkout", purpose="pilot", scope="m1_main",
        rule_version_base="s18-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_unavailable", mechanism="chaos_mesh", fault_class="lifecycle",
            entity="checkout", deployment="checkout", selector={"app": "checkout"},
            parameters={"action": "pod-failure", "duration_s": _PF_STD.duration_s},
            window=_PF_STD.window),
        carrier=CarrierStreamSpec(
            stream_id="checkout-health-pod-read", service="checkout:5011", path_prefix="/health",
            carrier="carrier-checkout-health", direct_api=True, parameterized=False),
        telemetry=_std_telemetry("checkout-pod-logs"),
        signal=MetricSignalSpec(
            entity="checkout", service_name="checkout_service", http_target="/health",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        injection_signal_kind="carrier_error_fraction",
        injection_signal=pod_failure_injection_signal("checkout", "checkout-health-pod-read"),
        provenance={
            "catalog_atom": "service_unavailable@checkout",
            "ledger": _LEDGER + "/05-pod-failure.md (SS2.3 D3 selector extension table; SS1 rows 4/9)",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: '
                              "retarget pod-failure legs (app label checkout == deploy name)",
            "parameter_status": "NOT_FROZEN ({action:pod-failure} fixed by primitive schema; duration "
                                "follows the liveness profile)",
            "request_profile_status": "NOT_FROZEN (declared carrier = /health: the old TARGET_CARRIER "
                                      "panel endpoint /api/checkout/preview?user_token= cannot serve as "
                                      "the contract stream (frozen credential guard); under pod-failure "
                                      "the pause swap refuses /health connections immediately, so the "
                                      "error channel keeps its detection semantics; the panel business "
                                      'carrier remains a collection observation-design alternative)',
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "std_20_3_3_below_kill_band profile, see POD_FAILURE_LIVENESS_PROFILES)",
            "liveness_decision": _PF_STD.decision,
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (error ratio 0.5 from the old availability-gate family; "
                                   'error-channel evaluator placement is an open collection item, same as S03)'}),
    "S19": AtomSpec(
        scenario_id="S19", atom_id="service_unavailable@search", purpose="pilot", scope="m1_main",
        rule_version_base="s19-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_unavailable", mechanism="chaos_mesh", fault_class="lifecycle",
            entity="search", deployment="search", selector={"app": "search"},
            parameters={"action": "pod-failure", "duration_s": _PF_STD.duration_s},
            window=_PF_STD.window),
        carrier=CarrierStreamSpec(
            stream_id="search-list-pod-read", service="search:5017",
            path_prefix="/api/search?q=phone&per_page=5&enrich=1",
            carrier="panel-search-list", direct_api=True, parameterized=False),
        telemetry=_std_telemetry("search-pod-logs"),
        signal=MetricSignalSpec(
            entity="search", service_name="search_service", http_target="/api/search",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        injection_signal_kind="carrier_error_fraction",
        injection_signal=pod_failure_injection_signal("search", "search-list-pod-read"),
        provenance={
            "catalog_atom": "service_unavailable@search",
            "ledger": _LEDGER + "/05-pod-failure.md (SS2.3 D3 selector extension table; SS1 rows 4/9)",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: '
                              "retarget pod-failure legs (app label search == deploy name)",
            "parameter_status": "NOT_FROZEN ({action:pod-failure} fixed by primitive schema; duration "
                                "follows the liveness profile)",
            "request_profile_status": "NOT_FROZEN (the old TARGET_CARRIER panel endpoint itself is "
                                      "expressible as a fixed-path read-only GET: q/per_page/enrich query "
                                      "keys carry no credential shape, and the enrich=1 catalog fan-out is "
                                      "part of the panel-proven read-only path)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "std_20_3_3_below_kill_band profile, see POD_FAILURE_LIVENESS_PROFILES)",
            "liveness_decision": _PF_STD.decision,
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (error ratio 0.5 from the old availability-gate family; "
                                   'error-channel evaluator placement is an open collection item, same as S03)'}),
    "S22": AtomSpec(
        scenario_id="S22", atom_id="service_unavailable@rec-agent", purpose="pilot", scope="m1_main",
        rule_version_base="s22-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_unavailable", mechanism="chaos_mesh", fault_class="lifecycle",
            entity="rec-agent", deployment="rec-agent",
            # live canonical label（SDEPLOY-SELECTOR-PIN deviant, F-S1-3）
            selector={"app": "recommendation_agent"},
            parameters={"action": "pod-failure", "duration_s": _PF_RECA.duration_s},
            window=_PF_RECA.window),
        carrier=CarrierStreamSpec(
            stream_id="recagent-health-pod-read", service="rec-agent:5001",
            path_prefix="/recommend/health", carrier="panel-recagent-health",
            direct_api=True, parameterized=False),
        telemetry=_std_telemetry("rec-agent-pod-logs"),
        signal=MetricSignalSpec(
            entity="rec-agent", service_name="recommendation_agent", http_target="/recommend/health",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        injection_signal_kind="carrier_error_fraction",
        injection_signal=pod_failure_injection_signal("rec-agent", "recagent-health-pod-read"),
        preconditions=(
            Precondition(
                statement="rec-agent pods carry app=recommendation_agent, not app=rec-agent: the canonical "
                          "contract selector pins the LIVE label {app: recommendation_agent} directly "
                          "(SDEPLOY-SELECTOR-PIN precedent, F-S1-3) while the Deployment object stays "
                          "'rec-agent'; the live pin must re-verify the deployment matchLabels and the "
                          "pinned pod's labels against that selector (old RETARGET_APP_LABEL lesson)",
                mechanism_anchor='k8s/services/rec-agent.yaml:28 label app=recommendation_agent; old runner '
                                 "RETARGET_APP_LABEL L486-487; primitives.PinnedTarget pins the live "
                                 "label as the canonical selector value (F-S1-3)",
                owner='driver/runtime-binding layer (collection); assembly declares, never provisions',
                status="NOT_FROZEN pinned-live-label premise; default cluster state satisfies the canonical "
                       "selector literally (matchLabels app=recommendation_agent)"),),
        provenance={
            "catalog_atom": "service_unavailable@rec-agent",
            "ledger": _LEDGER + "/05-pod-failure.md (SS2.3 D3 selector extension table; SS1 rows 4/9, "
                               "label lesson SS1-9)",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: G1 '
                              "rec-agent retarget legs (app label recommendation_agent != deploy name "
                              "rec-agent)",
            "parameter_status": "NOT_FROZEN ({action:pod-failure} fixed by primitive schema; duration "
                                "follows the liveness profile)",
            "request_profile_status": "NOT_FROZEN (the old TARGET_CARRIER panel endpoint /recommend/health "
                                      "is expressible as a fixed-path read-only GET -- always 200, no LLM, "
                                      "no DB, old G1 RECAGENT_GATE_CARRIER knowledge; under pod-failure the "
                                      "pause swap refuses connections immediately, so the error channel "
                                      "keeps its detection semantics)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "recagent_20_3_5_below_kill_band profile, see POD_FAILURE_LIVENESS_PROFILES)",
            "liveness_decision": _PF_RECA.decision,
            "runtime_preconditions": "contract selector pins the live label {app: recommendation_agent} "
                                     "(SDEPLOY-SELECTOR-PIN deviant, F-S1-3); owner=driver/runtime "
                                     "binding (see spec.preconditions)",
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (error ratio 0.5 from the old availability-gate family; "
                                   'error-channel evaluator placement is an open collection item, same as S03)'}),
    # S26: the static-yaml user single -- leaf-victim detection basis, NOT a
    # retarget leg (user is not in RETARGET_SERVICES; old D04 F2 leg used the
    # static chaos-pod-failure-user.yaml whose comment block IS the detection-
    # basis authority: carrier-polling must not gate this atom, restart/ready
    # observation must).
    "S26": AtomSpec(
        scenario_id="S26", atom_id="service_unavailable@user", purpose="pilot", scope="m1_main",
        rule_version_base="s26-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="service_unavailable", mechanism="chaos_mesh", fault_class="lifecycle",
            entity="user", deployment="user", selector={"app": "user"},
            parameters={"action": "pod-failure", "duration_s": _PF_STD.duration_s},
            window=_PF_STD.window),
        carrier=CarrierStreamSpec(
            stream_id="user-health-pod-read", service="user:5004", path_prefix="/health",
            carrier="carrier-user-health", direct_api=True, parameterized=False),
        telemetry=_std_telemetry("user-pod-logs"),
        signal=MetricSignalSpec(
            entity="user", service_name="user_service", http_target="/health",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        injection_signal_kind="pod_restart_delta",
        injection_signal={
            "entity": "user", "source_id": "m1-kubectl-components", "unit": "restarts",
            "labels": {"app": "user"}, "min_points": 1, "mode": "delta_from_pre",
            "operator": "ge", "threshold": 1, "required_fraction": 1.0},
        preconditions=(
            Precondition(
                statement="the user leaf-victim detection basis must be restart/ready observation, never "
                          "carrier polling: direct carrier connections to a pause-swapped pod returned 200 "
                          "false negatives in probe10 (poll_carrier=False), so the run must bind a restart/"
                          "ready observation source (CRI exporter / kubelet events / kubectl) covering the "
                          "user pod across the window -- whether the proxy-routed /health carrier "
                          'additionally shows an error signature is an collection observation item, not the '
                          "declared bite gate",
                mechanism_anchor="PodChaos service-unavailability behavior (probe10 leaf-victim "
                                 "quirk; restart_delta in {1,2} legal signature, >2 or non-target pod "
                                 "restart = churn void); old runner inject_pod_failure poll_carrier=False "
                                 "(ledger B1 05 SS1 row 4)",
                owner='driver/runtime-binding layer (collection): restart/ready observation coverage and the '
                      "restart_delta window-validity judgement; assembly declares, never provisions",
                status="NOT_FROZEN detection basis; violated premise invalidates the attempt's bite "
                       "evidence, not just the GT"),),
        provenance={
            "catalog_atom": "service_unavailable@user",
            "ledger": _LEDGER + "/05-pod-failure.md (SS2.3 D3; SS1 rows 4/8, SS2 leaf-victim clause)",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: D04 F2 '
                              "user leg via the static chaos-pod-failure-user.yaml (not a retarget leg)",
            "parameter_status": "NOT_FROZEN ({action:pod-failure} fixed by primitive schema; duration "
                                "follows the liveness profile)",
            "detection_basis": "leaf victim: the panel profile endpoint /api/users/<user_token>/profile "
                               "carries a credential-shaped path parameter and cannot be the contract "
                               "stream; the declared bite channel is restart_delta in {1,2} "
                               "(poll_carrier=False quirk, probe10) -- the /health carrier is an "
                               "observation stream, never the abort gate",
            "request_profile_status": "NOT_FROZEN (declared carrier = /health fixed-path; user is "
                                      "disjoint from the catalog call graph -- /profile reads the users "
                                      "table directly, so CHECKSUM stays trivially safe)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                             "std_20_3_3_below_kill_band profile, see POD_FAILURE_LIVENESS_PROFILES)",
            "liveness_decision": _PF_STD.decision,
            "runtime_preconditions": "restart/ready observation coverage for the user pod across the "
                                     "window; owner=driver/runtime binding (see spec.preconditions)",
            "qc_threshold_status": "NOT_FROZEN (restart_delta >= 1 with the {1,2} legality band from the "
                                   "static yaml comment; restart-channel evaluator placement is an open "
                                   "collection item, same family as S03's error channel)"}),
    # --- ASM-3: gateway config atoms (ledger B2 01/02/03) -------------------- #
    # Gateway rollout mechanism binding: the gateway legs carry mechanism
    # nginx_configmap_rollout / m1-gateway-configmap-v1 (gateway usage only);
    # the legacy nginx_directive / gateway_prepare_only pair is refused by the
    # runner (validate_run) and pinned by test.
    "S23": AtomSpec(
        scenario_id="S23", atom_id="retry_policy_misconfiguration@catalog-gw", purpose="pilot",
        scope="m1_main", rule_version_base="s23-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="retry_policy_misconfiguration", mechanism=gw.MECHANISM,
            mechanism_version=gw.MECHANISM_VERSION, fault_class="configuration",
            entity="catalog-gw", deployment="catalog-gw", selector={"app": "catalog-gw"},
            parameters={"proxy_next_upstream": "off"}, window=_P_GW.window),
        carrier=CarrierStreamSpec(
            stream_id="pricing-items-via-gw", service="pricing:5014", path_prefix="/api/pricing/",
            
            
            # to the entity reverse-lookup, declared for every via-gw stream).
            carrier="panel-pricing-via-gw", direct_api=True, trace_service="pricing_service"),
        telemetry=_std_telemetry("catalog-gw-pod-logs"),
        signal=MetricSignalSpec(
            entity="catalog-gw", service_name="pricing_service", http_target="/api/pricing/<item_id>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=800, required_fraction=0.5, absolute_tolerance=800, relative_tolerance=0),
        injection_signal_kind="config_state_marker",
        injection_signal=gateway_config_marker_signal("catalog-gw", "proxy_next_upstream", "off"),
        # The gw-path carrier premise (same family as S01): pricing must reach
        # catalog through catalog-gw for the knob to sit on the carrier path.
        preconditions=(
            Precondition(
                statement="pricing reaches catalog through catalog-gw: CATALOG_SERVICE_URL must point at "
                          "http://catalog-gw (default manifest value is the direct catalog:5005 URL) and the "
                          "pricing deployment must be scaled to a ready replica for the run window",
                mechanism_anchor='k8s/services/pricing.yaml default CATALOG_SERVICE_URL=http://catalog:5005; '
                                 'legacy gw carrier wiring in pricing-route auxiliary setup '
                                 "L14752-14756 (pricing scale 1 + env redirect before, restore after)",
                owner='driver/runtime-binding layer (collection); assembly declares, never provisions',
                status="NOT_FROZEN candidate mechanism; default cluster state does NOT satisfy it"),),
        provenance={
            "catalog_atom": "retry_policy_misconfiguration@catalog-gw",
            "ledger": _LEDGER_B2 + "/01-gateway-cfg-family.md (SS1/SS3/SS4) + 02-design-level-mapping.md "
                                   "(S23 entry + S23-A)",
            "live_precedent": "none -- the old retry knob never ran standalone (always co-active with "
                              'timeout/slow primary in ov-f1on/ov-f1f2); S23 is a first use, collection smoke '
                              "required",
            "parameter_status": "NOT_FROZEN ({proxy_next_upstream: off} locked by the prepare_gateway "
                                "exact parameter set; composite or slow-route parameters are refused at "
                                "prepare time, gateway.py L211-212)",
            "request_profile_status": "NOT_FROZEN (carrier = pricing through catalog-gw, the pricing panel "
                                      "family; same runtime redirect premise as S01 -- see "
                                      "spec.preconditions)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (rollout decision: "
                             "injection_transition_absorbed profile, see GATEWAY_ROLLOUT_PROFILES; no "
                             "composite geometry for a single atom, ledger B2 03 SS0-2)",
            "rollout_decision": _P_GW.decision,
            "knob_isolation": "changed_directives must equal exactly [proxy_next_upstream]: enforced at "
                              "preflight and verify by the gateway adapter (_knob_isolation three-point "
                              "check); the legacy ov-* whole-file conf swaps never travel with this atom",
            "diagnostic_value_risk": "C1 SS3.4 registered risk: all old evidence shows retry-off has NO "
                                     "standalone failure signature (its observability was parasitic on "
                                     "timeout/slow-upstream overlap buckets, old dual_sli_gate "
                                     "L3569-3573); expected ok~1 and unmoved p95 under a healthy upstream "
                                     "-- possibly unmeasurable on every channel; the pilot must produce "
                                     "the delete-or-keep decision material; reviving the composite state "
                                     "(slow upstream, extra timeout edits) to manufacture observability "
                                     "is FORBIDDEN (prepare_gateway exact parameter set + knob isolation)",
            "runtime_preconditions": "pricing env redirect + scale-up required before the gw-path carrier "
                                     "can observe anything (see spec.preconditions; owner=driver/runtime "
                                     "binding)",
            "qc_threshold_status": "NOT_FROZEN (injection evidence = presence channel: durable transition "
                                   "record + knob_isolation prove the knob switched; effect signature is an "
                                   "observation/C1 matter; the evaluator records this signal kind as "
                                   "NOT_ASSESSED honestly; rollout completion is never a fault-effect "
                                   "signal)"}),
    "S24": AtomSpec(
        scenario_id="S24", atom_id="timeout_misconfiguration@catalog-gw", purpose="pilot",
        scope="m1_main", rule_version_base="s24-pilot-5ms", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="timeout_misconfiguration", mechanism=gw.MECHANISM,
            mechanism_version=gw.MECHANISM_VERSION, fault_class="configuration",
            entity="catalog-gw", deployment="catalog-gw", selector={"app": "catalog-gw"},
            parameters={"read_timeout_ms": 5}, window=_P_GW.window),
        carrier=CarrierStreamSpec(
            stream_id="pricing-items-via-gw", service="pricing:5014", path_prefix="/api/pricing/",
            
            
            # to the entity reverse-lookup, declared for every via-gw stream).
            carrier="panel-pricing-via-gw", direct_api=True, trace_service="pricing_service"),
        telemetry=_std_telemetry("catalog-gw-pod-logs"),
        signal=MetricSignalSpec(
            entity="catalog-gw", service_name="pricing_service", http_target="/api/pricing/<item_id>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=800, required_fraction=0.5, absolute_tolerance=800, relative_tolerance=0),
        injection_signal_kind="carrier_error_fraction",
        injection_signal=gw_config_timeout_injection_signal("catalog-gw", "pricing-items-via-gw"),
        preconditions=(
            Precondition(
                statement="pricing reaches catalog through catalog-gw: CATALOG_SERVICE_URL must point at "
                          "http://catalog-gw (default manifest value is the direct catalog:5005 URL) and the "
                          "pricing deployment must be scaled to a ready replica for the run window",
                mechanism_anchor='k8s/services/pricing.yaml default CATALOG_SERVICE_URL=http://catalog:5005; '
                                 'legacy gw carrier wiring in pricing-route auxiliary setup '
                                 "L14752-14756 (pricing scale 1 + env redirect before, restore after)",
                owner='driver/runtime-binding layer (collection); assembly declares, never provisions',
                status="NOT_FROZEN candidate mechanism; default cluster state does NOT satisfy it"),),
        provenance={
            "catalog_atom": "timeout_misconfiguration@catalog-gw",
            "ledger": _LEDGER_B2 + "/01-gateway-cfg-family.md (SS1/SS3/SS4/SS5-2) + 02-design-level-mapping.md "
                                   "(S24 entry)",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: '
                              "ov-net-f2 200 ms (old D05/M2a), ov-hostcfg-f2 20 ms (old D06/DK09), "
                              "ov-catlat-f2 1000 ms (old D12/DK14) -- each tier belonged to its combo "
                              "context, none ran standalone",
            "parameter_status": "NOT_FROZEN_MATCH_WITHIN_CONTRAST_FAMILY (user-selected 5 ms tier "
                                "for S24 and D06; prior 20 ms D06 short smoke and 12 ms diagnostic "
                                "remain separate historical attempts, not 5 ms evidence. The S24 single "
                                'control must retain the same 5 ms dose as D06 for fault-comparison pairing. Other '
                                "gateway-config combinations carry explicit tier overrides and require "
                                "their own matched controls or a documented non-pairing decision. Live "
                                "5 ms fault effect and data P0 remain untested; freeze only after pilot)",
            "request_profile_status": "NOT_FROZEN (carrier = pricing through catalog-gw, the pricing panel "
                                      "family; same runtime redirect premise as S01 -- see "
                                      "spec.preconditions)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (rollout decision: "
                             "injection_transition_absorbed profile, see GATEWAY_ROLLOUT_PROFILES; no "
                             "composite geometry for a single atom, ledger B2 03 SS0-2)",
            "rollout_decision": _P_GW.decision,
            "knob_isolation": "changed_directives must equal exactly [proxy_read_timeout]: the legacy "
                              "ov-net-f2/ov-hostcfg-f2/ov-catlat-f2 confs bundled read_timeout X + retry "
                              "off in one file -- that retry half is on the must-not-migrate list (ledger "
                              "B2 01 SS3-3) and never travels with the timeout leg; enforced by the "
                              "prepare_gateway exact parameter set and the adapter's knob isolation",
            "diagnostic_value_risk": "if the frozen tier lands at or above the healthy response floor the "
                                     "atom degenerates to presence-only (no timeout errors, S23-like "
                                     "risk); the tier freeze must verify a non-edge error signature "
                                     "(old DK09 razor-edge lesson)",
            "runtime_preconditions": "pricing env redirect + scale-up required before the gw-path carrier "
                                     "can observe anything (see spec.preconditions; owner=driver/runtime "
                                     "binding)",
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (error ratio 0.5 from the availability-gate family as the "
                                   "single-atom candidate -- the old 0.8 thresholds belonged to combo "
                                   'overlap buckets; error-channel evaluator placement is an open collection '
                                   "item, same family as S03)"}),
    # --- ASM-4: network_loss@catalog-gw (ledger B1 03, net family) ------------- #
    "S02": AtomSpec(
        scenario_id="S02", atom_id="network_loss@catalog-gw", purpose="pilot", scope="m1_main",
        rule_version_base="s02-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="network_loss", mechanism="chaos_mesh", fault_class="network",
            entity="catalog-gw", deployment="catalog-gw", selector={"app": "catalog-gw"},
            parameters={"loss_percent": 60, "correlation_percent": 0, "duration_s": 60},
            window=_P_NET_LOSS.window),
        carrier=CarrierStreamSpec(
            stream_id="pricing-items-loss-read", service="pricing:5014", path_prefix="/api/pricing/",
            carrier="panel-pricing-via-gw", direct_api=True),
        telemetry=_std_telemetry("catalog-gw-pod-logs"),
        signal=MetricSignalSpec(
            entity="catalog-gw", service_name="pricing_service", http_target="/api/pricing/<item_id>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=800, required_fraction=0.5, absolute_tolerance=800, relative_tolerance=0),
        # (1) the gw-path carrier premise (same family as S01/S23/S24);
        # (2) the loss strength is NOT pilot-qualified: the old T03 live loss
        #     value was never verified (SEVEN-REDESIGNS iron rule) and must
        #     never be cited as "T03 measured 60%".
        preconditions=(
            Precondition(
                statement="pricing reaches catalog through catalog-gw: CATALOG_SERVICE_URL must point at "
                          "http://catalog-gw (default manifest value is the direct catalog:5005 URL) and the "
                          "pricing deployment must be scaled to a ready replica for the run window",
                mechanism_anchor='k8s/services/pricing.yaml default CATALOG_SERVICE_URL=http://catalog:5005; '
                                 'legacy runtime redirect in pricing-route auxiliary setup header notes '
                                 "(set env CATALOG_SERVICE_URL=http://catalog-gw + scale 1, restore scale 0 + URL "
                                 "afterwards)",
                owner='driver/runtime-binding layer (collection); assembly declares, never provisions',
                status="NOT_FROZEN candidate mechanism; default cluster state does NOT satisfy it"),
            Precondition(
                statement='the loss_percent strength is NOT_FROZEN and must be pilot-recalibrated in collection '
                          "before any formal claim: loss_percent=60 keeps the old shared NET_LOSS_PCT default "
                          "as a CANDIDATE tier only (probe-B 2026-06-27 calibration: carrier ok~7/10, "
                          "avg ~6450 ms via TCP retransmission); the old T03 live loss value was never "
                          "verified and must never be cited as 'T03 measured 60%' (SEVEN-REDESIGNS iron "
                          "rule); the DK15-dedicated 85% tier belonged to a composite context and is not "
                          "inherited; the readiness-flap boundary (see the probe decision) constrains the "
                          "recalibrated tier",
                mechanism_anchor="old runner NET_LOSS_PCT=60 L266 / NET_LOSS_DK15_PCT=85 L262-267 / "
                                 "NET_LOSS_PROBE_TIMEOUT_SEC=3 L273; NetworkChaos loss configuration "
                                 "header (probe-B calibration + dropped-counter honesty); ledger B1 03 "
                                 "SS3.2-2 (loss tier re-freeze mandate)",
                owner='pilot qualification (collection); assembly declares, never calibrates',
                status="NOT_FROZEN tier candidate; hard precondition before any formal claim"),),
        provenance={
            "catalog_atom": "network_loss@catalog-gw",
            "ledger": _LEDGER + "/03-network-chaos.md",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: probe-B '
                              "2026-06-27 calibration via chaos-net-catalog-loss.yaml header",
            "parameter_status": "NOT_FROZEN (loss_percent=60 = old shared NET_LOSS_PCT L266 candidate ONLY; "
                                "the old T03 live loss value was never verified (SEVEN-REDESIGNS iron rule) "
                                "and must never be cited as measured; DK15's 85% tier is composite-context "
                                "and NOT inherited; correlation 0 mirrors the static pilot yaml; direction "
                                "'to' is fixed by the primitive schema)",
            "request_profile_status": "NOT_FROZEN (carrier = pricing through catalog-gw, the pricing panel "
                                      "family, same runtime redirect premise as S01 -- see "
                                      "spec.preconditions)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (probe decision: "
                             "gw_loss_no_liveness_readiness_watched profile, see NET_LIVENESS_PROFILES)",
            "probe_decision": _P_NET_LOSS.decision,
            "honesty_note": "netem qdisc loss NEVER enters cAdvisor container_network_*_packets_dropped "
                            "counters (probe-B measured dropped_rate=0.0 under 60% loss) -- never claim "
                            "dropped-metric loss/delay discrimination; the app-layer signature (p95 shift + "
                            "error_ratio discriminant) is the only honest channel (loss yaml header iron "
                            "rule; ledger 03 #8)",
            "runtime_preconditions": "pricing env redirect + scale-up (gw-path premise) + loss-tier pilot "
                                     "recalibration (see spec.preconditions)",
            "qc_threshold_status": "NOT_FROZEN (800 ms p95-shift keeps the old net_loss_single gw-path "
                                   "calibration as candidate; the loss branch deliberately carries NO "
                                   "ok>=0.8 floor -- loss legitimately errors (old gate loss branch); "
                                   "error_ratio as the loss-vs-delay discriminant and the old probe-timeout "
                                   'tightening (NET_LOSS_PROBE_TIMEOUT_SEC=3) stay collection observation-design '
                                   "items; evaluator arms as in S03's error channel remain open collection items)"}),
    # --- ASM-4: db_table_lock@mysql_items_lock (ledger B1 01, first db leg) ---- #
    "S06": AtomSpec(
        scenario_id="S06", atom_id="db_table_lock@mysql_items_lock", purpose="pilot", scope="m1_main",
        rule_version_base="s06-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=DbFaultLegSpec(
            fault_type="db_table_lock", mechanism=dbp.DB_MECHANISM, fault_class="resource",
            entity="mysql_items_lock", database="shopify2", table="items", mode="WRITE",
            window=(30, 85)),
        carrier=CarrierStreamSpec(
            stream_id="catalog-items-dblock-read", service="catalog:5005", path_prefix="/api/items/",
            carrier="panel-catalog-direct", direct_api=True),
        telemetry=TelemetryIds(
            metrics="m1-prometheus-2s", traces="jaeger-16686", logs="mysql-error-log",
            components="m1-mysql-sessions", checksum="m1-mysql-checksum",
            locks="m1-mysql-locks", operations="m1-db-primitives"),
        signal=MetricSignalSpec(
            entity="mysql_items_lock", service_name="catalog_service", http_target="/api/items/<item_id>",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=1000, required_fraction=0.5, absolute_tolerance=1000, relative_tolerance=0),
        injection_signal_kind="carrier_error_fraction",
        
        
        # lag, so it explicitly keeps the 5 s switch-band settle.
        injection_signal=_carrier_ledger_rule(
            "carrier_error_fraction", "mysql_items_lock", "catalog-items-dblock-read", 0.5,
            settle_buffer_s=CARRIER_SETTLE_BUFFER_S),
        # (1) CHECKSUM iron-rule sequencing (owner = driver layer): pre before
        #     lock, NEVER during the held lock (the adapter's fail-closed gate
        #     covers only its own connection -- a driver-side sidecar sampler
        #     must pause during during_fault), post only after confirmed release;
        # (2) lock identity durable-first + foreign-lock/crash semantics;
        # (3) the blocking recover path must fit the run timing (TAIL budget).
        preconditions=(
            Precondition(
                statement="CHECKSUM iron-rule sequencing: checksum_pre runs in pre_fault BEFORE the lock "
                          "acquisition, NO checksum may run against items during the held-lock phase "
                          "(during_fault) -- including any driver-side sidecar sampler on its OWN "
                          "connection, which the adapter's fail-closed gate cannot see (CHECKSUM on a "
                          "WRITE-locked table blocks until release; the old chain used sentinel "
                          "{items:-1,inventory:-1} to fail the case rather than hang) -- and checksum_post "
                          "runs in post_recovery only after reconcile confirms release; unconfirmed "
                          "release means no post, no lease release",
                mechanism_anchor="old runner L381-383 (iron-rule text), L15068-15070 (pre gate), "
                                 "L16198-16210 (post sentinel); primitives_db checksum_tables L593-603 "
                                 '(owned-lock refusal, adapter path only); runbook 02 SS2.4 + collection m-4 '
                                 "(sidecar gap)",
                owner='driver/runtime-binding layer (collection): checksum sampler window discipline '
                      "(pre_fault/post_recovery only, during_fault paused); assembly declares, never runs",
                status="NOT_FROZEN operational premise; violating it re-opens the CHECKSUM-hang vector"),
            Precondition(
                statement="lock identity is durable-first and crash-recoverable: the adapter writes the "
                          "db-lock-session evidence BEFORE LOCK TABLES, a FOREIGN pre-existing lock "
                          "(ownership None) blocks apply, and crash recovery claims sessions via the "
                          "session-minus-release evidence difference -- the driver must resolve the live "
                          "database credentials through the contract context credential_refs into the "
                          "process environment for the db client (never printed, never written to "
                          "evidence), keep the attempt directory intact for reconcile and never assume "
                          "the in-memory handle survives",
                mechanism_anchor="primitives_db L526-529 (durable session evidence before lock), "
                                 "L507-514 (foreign-lock ownership None blocks apply), L464-471 "
                                 "(session-release difference claim); ledger B1 01 SS3.2-2",
                owner='driver/runtime-binding layer (collection): attempt-directory lifetime + live DB '
                      "credential wiring via contract context credential_refs (never printed); assembly "
                      "declares, never provisions",
                status="NOT_FROZEN operational premise; live lock face itself is fake-verified only "
                       '(P1-EXT scope, collection live smoke required)'),
            Precondition(
                statement="the BLOCKING recover path must fit the run timing: recover = unlock four folds "
                          "+ up to TWO confirm_release probes (compare_and_restore plus the closing "
                          "observe) each blocking up to confirm_timeout_s=15 s, plus statement budgets "
                          "and connection setup -- the worst case exceeds the 35 s during->post boundary "
                          'at the family geometry unless the collection driver tightens the db client budgets '
                          "(statement_timeout_s / confirm_timeout_s) or widens the boundary; a "
                          "TimingViolation mid-recover leaves the attempt dirty by design",
                mechanism_anchor='runbook 02 SS2.2 + collection m-3 (worst case = two full confirms + four '
                                 "statement budgets + setup; the simple lower bound is NOT sufficient); "
                                 "old T3_TAIL_BUDGET L14343; runner action_duration_s deadline "
                                 "L341/L1273-1274",
                owner='driver/runtime-binding layer (collection): deadline/budget sizing before any live run; '
                      "assembly declares, never sizes",
                status="NOT_FROZEN timing premise; violated premise aborts the attempt as dirty, not "
                       "just the GT"),),
        provenance={
            "catalog_atom": "db_table_lock@mysql_items_lock",
            "ledger": _LEDGER + "/01-db-table-lock.md",
            "runbook": 'configs/collection/scenarios.json: assembly definition'
                       "(SS2.1 parameter contract, SS2.4 checksum sequencing, SS2.5 Q2 locks row)",
            "live_precedent": 'none in the new pipeline (collection smoke required); old-chain anchors: S06 '
                              "delivered cases judged by common_cause_gate_db (error-burst + disjoint "
                              "flat + restart_delta=0 + checksum_zero_drift, L4336-4482)",
            "entity_binding": "off-graph shared root: mysql_items_lock is the shared items-table lock, "
                              "NOT a service Deployment -- the FIRST database leg: raw_target kind "
                              "MySqlTableLock scope shopify2 (database schema binding), no pod/selector, and "
                              'the runner face is db_bindings/db_client (collection validate dispatch), never '
                              "a Kubernetes pin",
            "parameter_status": "NOT_FROZEN_MATCH_WITHIN_CONTRAST_FAMILY ({table: items, mode: WRITE} "
                                "locked by the prepare_db_lock exact parameter set; entity->table "
                                "whitelist DB_LOCK_TABLE_BY_ENTITY; the old hold=12/gap=2 duty-cycle "
                                "parameters are DELIBERATELY absent -- see hold_profile)",
            "hold_profile": "continuous single-session WRITE lock (ledger 01 SS3.2-1, intentionally "
                            "changed): readers are blocked ~100% of the window instead of the old 12s/2s "
                            "duty cycle, so error_ratio is expected HIGHER than the old-gate calibration "
                            "context; QC thresholds and GT intensity declarations must be recalibrated "
                            "for the sustained profile (old DB_LOCK_ERROR_RATIO_THR=0.5 was duty-cycle "
                            "context)",
            "request_profile_status": "NOT_FROZEN (declared single carrier = catalog-direct items read, "
                                      "an items-reader victim arm of the old multi-carrier s2_dblock "
                                      "profile; pure SELECT -> CHECKSUM-safe; the old disjoint user arm "
                                      'and search victim arm are collection observation-design items NOT '
                                      "inherited by default; probe9 lesson: real concurrency fast-fails "
                                      "through pool exhaustion, only single-threaded probes hang)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (55 s family window; no CRD "
                             "duration knob -- the session holds the lock across the planned window and "
                             "reconcile releases it; recover blocking budget see spec.preconditions)",
            "lock_evidence_channel": "GT-side lock evidence = the Q2 owned_database_locks locks row "
                                     "(sidecar pattern, runbook 02 SS2.5): a FRESH confirm_release "
                                     "readback inside the post_recovery window via the locks_supplier "
                                     "face, examined_connection_ids covering every inject "
                                     "lock_connection_id, active_owned_locks==[]; operations inject "
                                     "rows must merge lock_connection_ids (G-3) and resource_uid must "
                                     "equal str(connection_id) -- fabricated or missing ids fail, "
                                     "never zero-filled",
            "runtime_preconditions": "CHECKSUM iron-rule sequencing + durable-first lock identity + "
                                     "recover TAIL budget (see spec.preconditions; owner=driver/runtime "
                                     "binding)",
            "signal_migration": _SIGNAL_MIGRATION_PROVENANCE,
            "qc_threshold_status": "NOT_FROZEN (error ratio 0.5 keeps the old common_cause_gate_db "
                                   "threshold as candidate UNDER THE CHANGED SUSTAINED PROFILE -- "
                                   "recalibration mandatory; error-channel evaluator placement is an "
                                   'open collection item, same family as S03)'}),
    # --- ASM-4: network_delay@rec-agent (ledger B1 03, net family) ------------- #
    "S21": AtomSpec(
        scenario_id="S21", atom_id="network_delay@rec-agent", purpose="pilot", scope="m1_main",
        rule_version_base="s21-pilot", random_seed=11,
        phases=SMOKE_FAMILY_PHASES,
        leg=FaultLegSpec(
            fault_type="network_delay", mechanism="chaos_mesh", fault_class="network",
            entity="rec-agent", deployment="rec-agent",
            # live canonical label（SDEPLOY-SELECTOR-PIN deviant, F-S1-3）
            selector={"app": "recommendation_agent"},
            parameters={"latency_ms": 450, "jitter_ms": 90, "correlation_percent": 0, "duration_s": 60},
            window=_P_NET_RECA.window),
        carrier=CarrierStreamSpec(
            stream_id="recagent-health-delay-read", service="rec-agent:5001",
            path_prefix="/recommend/health", carrier="panel-recagent-health",
            direct_api=True, parameterized=False),
        telemetry=_std_telemetry("rec-agent-pod-logs"),
        signal=MetricSignalSpec(
            entity="rec-agent", service_name="recommendation_agent", http_target="/recommend/health",
            source_id="m1-prometheus-2s", min_points=5, mode="delta_from_pre", operator="ge",
            threshold=800, required_fraction=0.5, absolute_tolerance=800, relative_tolerance=0),
        # (1) the rec-agent netem injection face was NEVER live-verified in the
        #     old chain (old runner G1 note) -- first live use must smoke-verify
        #     the tc attachment; (2) the app-label lesson (S20/S22): the
        #     contract selector itself pins the LIVE label (F-S1-3).
        preconditions=(
            Precondition(
                statement="the rec-agent netem injection face is UNVERIFIED: the old runner's G1 note "
                          "explicitly marks netem injection on the rec-agent pod as unverified, so the "
                          "first live use MUST smoke-verify that the NetworkChaos tc qdisc actually "
                          "attaches to the rec-agent pod's egress (tc/CRD AllInjected evidence) before "
                          "any carrier observation is trusted",
                mechanism_anchor='old runner G1 note L481 (rec-agent netem face unverified; collection errata: '
                                 "the note is at L481, NOT L13469 -- that line is the deepseek-env "
                                 "comment); ledger B1 03 SS3.2-3",
                owner='driver/runtime-binding layer (collection): first-use tc attachment smoke; assembly '
                      "declares, never verifies",
                status="NOT_FROZEN injection-face premise; violated premise invalidates the attempt's "
                       "bite evidence"),
            Precondition(
                statement="rec-agent pods carry app=recommendation_agent, not app=rec-agent: the canonical "
                          "contract selector pins the LIVE label {app: recommendation_agent} directly "
                          "(SDEPLOY-SELECTOR-PIN precedent, F-S1-3) while the Deployment object stays "
                          "'rec-agent'; the live pin must re-verify the deployment matchLabels and the "
                          "pinned pod's labels against that selector (old RETARGET_APP_LABEL lesson)",
                mechanism_anchor='k8s/services/rec-agent.yaml:28 label app=recommendation_agent; old runner '
                                 "RETARGET_APP_LABEL L486-487; primitives.PinnedTarget pins the live "
                                 "label as the canonical selector value (F-S1-3)",
                owner='driver/runtime-binding layer (collection); assembly declares, never provisions',
                status="NOT_FROZEN pinned-live-label premise; default cluster state satisfies the canonical "
                       "selector literally (matchLabels app=recommendation_agent)"),),
        provenance={
            "catalog_atom": "network_delay@rec-agent",
            "ledger": _LEDGER + "/03-network-chaos.md (S21 entry; README SS2.2 boundary D2)",
            "live_precedent": 'none in the new pipeline (collection smoke required); the old chain NEVER '
                              "live-verified the rec-agent netem face either (G1 note, see "
                              "spec.preconditions)",
            "parameter_status": "NOT_FROZEN (450/90 keeps the ATOM-CATALOG historical declaration + the "
                                "G2ext leg definition L13477-13484 as candidate; FULL-EGRESS semantics: "
                                "netem on an ordinary service pod delays EVERY egress packet "
                                "(MySQL/catalog/... each accumulate) -- never re-grade the gw single-edge "
                                "tier onto this face, ledger 03 #9 SS5-4)",
            "request_profile_status": "NOT_FROZEN (carrier = the /recommend/health panel endpoint, "
                                      "always 200, no LLM, no DB -- old RECAGENT_GATE_CARRIER L500-501 "
                                      "knowledge; the recommend POST business-evidence carrier of the old "
                                      "dual-carrier design stays OUT of the read-only request profile; "
                                      "the delay signature rides the pod egress once per response packet)",
            "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (probe decision: "
                             "recagent_delay_within_probe_timeout profile, see NET_LIVENESS_PROFILES)",
            "liveness_decision": _P_NET_RECA.decision,
            "mechanism_semantics": "direction 'to' + no target on an ordinary service pod = the pod's "
                                   "WHOLE egress is delayed (ledger 03 #9), unlike catalog-gw where the "
                                   "business graph has a single downstream edge -- the e2e semantics are "
                                   "carried by raw_target/parameters explicitly to prevent old M10 "
                                   "semantic drift from being copied (ledger 03 SS5-4)",
            "runtime_preconditions": "netem-face smoke verification + live-label pin "
                                     "{app: recommendation_agent} in the contract selector "
                                     "(SDEPLOY-SELECTOR-PIN deviant, F-S1-3; see spec.preconditions)",
            "qc_threshold_status": "NOT_FROZEN (the 800 ms absolute shift is the old gw-path single-gate "
                                   "calibration kept ONLY as a family candidate -- ledger 03 SS4 forbids "
                                   "auto-inheriting it for non-gw paths; rec-agent egress accumulation "
                                   "~450 ms x ~1-3 response packets (~0.5-1.4 s) straddles the candidate, "
                                   "so collection must calibrate a NON-EDGE tier; the old delay gate's "
                                   "ok>=0.8 slow-not-failed floor stays an observation item)"}),
}

FIRST_BATCH_ATOMS = ("S08", "S25", "S01", "S03")
# ASM-2 second batch: service_cpu singles (S04, S09-S13, S20, S27, S28),

SECOND_BATCH_ATOMS = ("S04", "S09", "S10", "S11", "S12", "S13", "S20", "S27", "S28", "S05", "S07")
# ASM-3 third batch: the S03-mechanism pod-failure selector extension
# (S14-S19/S22 plus the static-yaml user single S26) and the two gateway
# config atoms (S23 pure retry, S24 pure timeout; explicit rollout mechanism pair).
THIRD_BATCH_ATOMS = ("S14", "S15", "S16", "S17", "S18", "S19", "S22", "S26", "S23", "S24")

# coverage -- the net-family loss single (S02), the FIRST database leg (S06,
# db_bindings/db_client runner face, never a Kubernetes pin) and the rec-agent
# netem delay single (S21, unverified injection face declared).
FOURTH_BATCH_ATOMS = ("S02", "S06", "S21")


def spec_for(scenario_id: str) -> AtomSpec:
    spec = ATOM_SPECS.get(scenario_id)
    _require(spec is not None, "unknown scenario spec: " + str(scenario_id))
    return spec


def db_binding_for(spec: AtomSpec) -> dbp.DbBinding:
    """Derive the runtime DbBinding from a database-leg spec (ASM-4, S06 face).

    The database/table identity is contract-level (raw_target scope/name), so
    the binding is derived here and NOT a RunBinding field (RunBinding carries
    only attempt identity that is not derivable from the spec).  The driver/
    test layer pairs this with a DbCommandClient (fake offline, live via
    contract context credential_refs) into RunServices.db_bindings/db_client.
    """
    _require(isinstance(spec.leg, DbFaultLegSpec),
             "db binding requires a database leg spec (db_table_lock)")
    return dbp.DbBinding(spec.leg.database, spec.leg.table)


def s03_spec(profile_name: str = S03_DEFAULT_LIVENESS_PROFILE) -> AtomSpec:
    """S03 spec under an explicit liveness-contention profile (collection MAJOR-1).

    Both profiles stay assemblable so collection smoke can validate the chosen
    form; the default is the below-kill-band window.
    """
    profile = S03_LIVENESS_PROFILES.get(profile_name)
    _require(profile is not None,
             "unknown S03 liveness profile: " + str(profile_name))
    base = ATOM_SPECS["S03"]
    leg = replace(base.leg, window=profile.window,
                  parameters={**dict(base.leg.parameters), "duration_s": profile.duration_s})
    provenance = {**dict(base.provenance),
                  "timing_status": "NOT_FROZEN_MATCH_COMMON_TARGET_WINDOWS (liveness decision: "
                                   + profile_name + " profile, see S03_LIVENESS_PROFILES)",
                  "liveness_decision": profile.decision}
    return replace(base, leg=leg, provenance=provenance)


# --------------------------------------------------------------------------- #
# COMBO-ASM-1 (fifth batch): SPECS-1 gateway cfg family combinations
# (D01/D05/D06/D12/D22/T01).  This is a PARALLEL builder surface: the 28


# entity/mechanism/mechanism_version/selector/parameters; only the planned
# window -- and where the combo window demands it the CRD duration_s -- is
# replaced), so a combo leg can never drift from its reference atom's identity.
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ComboTimingProfile:
    """Explicit nested-window timing decision for one combination (SPECS-1 (d)).

    The planned windows themselves live on the legs (single truth); this record
    pins the declared geometry label, the EXPLICIT inject/recover orders
    (blueprint CROSS-CUTTING SS8-4: the new chain declares window nesting, it
    is never a silent side effect of apply/recover ordering) and the decision
    text carrying the budget arithmetic.  Injection must equal the legs sorted
    by window start and recovery must equal the legs sorted by ascending
    window end (INNER recovers first); ComboSpec cross-checks both against the
    legs.  COMBO-ASM-2: the additional reversed(inject) relation is a nested/
    common-geometry law only -- a staggered design (disjoint sub-windows, the
    D04 legacy shape) recovers in window-end order, which equals the inject
    order itself, so the reversed requirement is keyed on the declared
    geometry class (nested*/common*) instead of being a blanket structural
    law (CROSS-CUTTING SS8-1 lists all four classes: nested / staggered /
    partial_overlap / simultaneous).  COMBO-ASM-3: the declared geometry
    label is additionally cross-checked against the DERIVED window-pair shape
    inside ComboSpec (see _combo_window_relation) -- the label stays a design
    declaration, but a label the window values cannot support now fails loud
    (ASM-2 review MINOR-1, probes N1/N3).
    """
    name: str
    geometry: str              # declared geometry label (nested / nested_pod_inner / nested_triple /
                               # partial_overlap / staggered_disjoint / common_target_window)
    inject_order: tuple[str, ...]
    recover_order: tuple[str, ...]
    decision: str              # the explicit decision record (blueprint option + budget)

    def __post_init__(self) -> None:
        _require(bool(self.name) and bool(self.geometry), "combo timing profile shape invalid")
        _require(bool(self.inject_order) and len(set(self.inject_order)) == len(self.inject_order),
                 "combo timing inject order invalid")
        if self.geometry.split("_", 1)[0] in ("nested", "common"):
            
            # ruling 2 -- five-tension same-end recover reslice): nested/common
            # designs may declare SAME-END recover pairs (the D11 precedent
            # shape, now the five tension reslices).  The blanket
            # reversed(inject) equality assumed STRICT nesting -- distinct
            # window ends, where the ascending-end order is unique and equals
            # the reversed injects.  With a same-end tie the tie order is the
            # LEG order (the runner's same-offset event sort breaks ties by
            # contract.faults order), which this windowless record cannot
            # verify.  The strict equality stays for every strictly-nested
            # profile; a tie deviation is admitted here only as a permutation
            # of the inject set, and ComboSpec.__post_init__ verifies it
            # against the windows AND the same_offset_budgets declarations
            # (the runner face stays the authoritative fail-closed gate).
            _require(tuple(self.recover_order) == tuple(reversed(self.inject_order))
                     or (len(set(self.recover_order)) == len(self.inject_order)
                         and set(self.recover_order) == set(self.inject_order)),
                     "combo recovery must be reverse-nested (INNER legs first) or the declared "
                     'same-end tie order (collection reslice ruling; D11 precedent)')
        _require(isinstance(self.decision, str) and bool(self.decision.strip()),
                 "explicit combo timing decision record required")


@dataclass(frozen=True)
class ComboSpec:
    """One combination scenario: >=2 legs, dual-stream carriers, per-leg rules.

    Six-element blueprint mapping: (a) legs derive from single-root AtomSpecs;
    (b) carriers carry role labels (exactly one combo-main-, >=1 combo-bypass-,
    extras declared combo-third-); (c) the OBS-collection audit is structural (single
    pod: key type only); (d) timing is an explicit ComboTimingProfile; (e) the
    interaction expectation is DESIGN INTENT ONLY and must carry the
    期望非观测 marker (C1 SS0 B observation gate adjudicates, never the
    assembly); (f) per-leg reference dependencies must name assembled
    single-root specs and are declared as a runtime ordering premise.
    """
    scenario_id: str
    purpose: str
    scope: str
    rule_version_base: str
    random_seed: int
    phases: tuple[tuple[int, int], ...]
    legs: tuple[FaultLegSpec | DbFaultLegSpec, ...]
    carriers: tuple[CarrierStreamSpec, ...]
    telemetry: TelemetryIds
    signals: tuple[MetricSignalSpec, ...]                      # one per leg (Q1 latency + Q2 recovery)
    injection_signal_kinds: tuple[str, ...]                    # one per leg
    injection_signals: tuple[Mapping[str, Any] | None, ...]    # one per leg (None <=> metric_threshold)
    timing: ComboTimingProfile
    interaction_expectation: str                               # 期望非观测 marker required (element (e))
    reference_dependencies: Mapping[str, tuple[str, ...]]      
    obs_r1_audit: str                                          # element (c) decision text
    obs_r1_target_kind: str = "pod"                            
    preconditions: tuple[Precondition, ...] = ()
    provenance: Mapping[str, str] = field(default_factory=dict)
    # COMBO-ASM-4 (ASM-3 review NIT-C): optional construction-level direction
    # guard for MUST-preserve injection-order knowledge (the D03 catalog-first
    # rule) -- a future edit that consistently swaps the leg windows, the
    # declared orders AND the test fixture would still pass every consistency
    # check; this field pins the knowledge itself, not just its consistency.
    must_inject_first_entity: str | None = None
    
    # budget declarations for same-offset CROSS-LEG action pairs (the D11
    # recover-recover same-end shape). Keys are frozenset fid PAIRS, values the
    # declaration notes stating the serial/parallel recovery budget; the driver
    # face consumes them as ComboEnvironmentFacts.same_offset_budgets (runner
    # L2935-2939 lookup is per same-offset pair). A combo without same-offset
    # pairs stays None and its construction path is unchanged.
    same_offset_budgets: Mapping[frozenset[str], str] | None = None

    def __post_init__(self) -> None:
        _require(bool(self.scenario_id), "combo: scenario id required")
        _require(self.purpose in {"smoke", "pilot"}, "combo: only smoke/pilot assembly is defined")
        _require(self.scope in {"m1_main", "extension"}, "combo: invalid scope")
        _require(len(self.phases) == 3 and all(len(w) == 2 for w in self.phases), "combo: three phase windows required")
        _require(self.phases[0][0] == 0 and self.phases[0][1] <= self.phases[1][0]
                 and self.phases[1][1] <= self.phases[2][0], "combo: phases must be ordered from zero")
        
        # (family fact: no attempt carries two gateway cfg faults).
        _require(len(self.legs) >= 2 and all(isinstance(leg, (FaultLegSpec, DbFaultLegSpec)) for leg in self.legs),
                 "combo: at least two validated legs required")
        entities = [leg.entity for leg in self.legs]
        _require(len(set(entities)) == len(entities),
                 'combo: legs must bind distinct root entities (collection main set: one instance per entity)')
        _require(sum(1 for leg in self.legs if leg.mechanism == gw.MECHANISM) <= 1,
                 "combo: no attempt may carry two gateway config legs")
        during = self.phases[1]
        for leg in self.legs:
            _require(during[0] <= leg.window[0] < leg.window[1] <= during[1],
                     "combo: every leg window must sit inside during_fault")
        # COMBO-ASM-2 MINOR-1 (ASM-1 review P8 gap): a CRD duration must COVER
        # the planned leg window -- a chaos_mesh CRD expiring mid-window would
        # silently truncate the fault (the batch-1 duration values were all
        # correct but only fixture-pinned, never machine-checked).  Env-hook
        # and database legs carry no duration_s parameter and are exempt by
        # parameter presence, not by mechanism string matching.
        for leg in self.legs:
            if isinstance(leg, FaultLegSpec) and "duration_s" in leg.parameters:
                _require(type(leg.parameters["duration_s"]) is int
                         and leg.parameters["duration_s"] >= leg.window[1] - leg.window[0],
                         "combo: CRD duration_s must cover the planned leg window")
        # COMBO-ASM-3 MINOR-2 (ASM-2 review MINOR-2, probe N5): a combination
        # chaos_mesh leg must CARRY duration_s at all -- the cover check above
        # is keyed on parameter presence, so a hand-written parameters mapping
        # that drops the key entirely would silently bypass it.  Every chaos_mesh
        
        # legs, gateway rollout legs and database legs stay exempt by mechanism.
        for leg in self.legs:
            if isinstance(leg, FaultLegSpec) and leg.mechanism == "chaos_mesh":
                _require("duration_s" in leg.parameters,
                         "combo: chaos_mesh legs must carry an explicit duration_s "
                         "(bounded CRD contract; a missing duration is driver-delete discipline "
                         "that the assembly face does not declare)")
        # COMBO-ASM-3 NIT-2 (ASM-2 review NIT-2, probe N2): the db-lock TAIL
        # arithmetic as a machine invariant -- every chaos_mesh CRD co-leg's
        # delete point (planned window end) sits at least one lock-release TAIL
        # budget past the lock window end, so the confirmed release completes
        # strictly before the CRD delete (relation pinned by the ASM-2 D02/D10
        # values: 85 + 35 == 120).
        db_legs = [leg for leg in self.legs if isinstance(leg, DbFaultLegSpec)]
        if db_legs:
            lock_end = max(leg.window[1] for leg in db_legs)
            for leg in self.legs:
                if isinstance(leg, FaultLegSpec) and leg.mechanism == "chaos_mesh":
                    _require(lock_end + DB_LOCK_RELEASE_TAIL_BUDGET_S <= leg.window[1],
                             "combo: the CRD delete point must sit >= one db lock-release "
                             "TAIL budget past the lock window end (confirm_release before CRD delete)")
        # (d) timing: the declared orders must be consistent with the leg
        # windows (inject == ascending starts, recover == reversed == descending
        # ends) -- the nesting is declared AND checked, never assumed.
        fids = ["F" + str(index + 1) for index in range(len(self.legs))]
        _require(len(set(self.timing.inject_order)) == len(self.legs)
                 and set(self.timing.inject_order) == set(fids),
                 "combo timing: inject order must name every leg exactly once")
        starts = {fid: leg.window[0] for fid, leg in zip(fids, self.legs)}
        _require(tuple(self.timing.inject_order) == tuple(sorted(fids, key=lambda fid: starts[fid])),
                 "combo timing: inject order must equal legs sorted by window start")
        ends = {fid: leg.window[1] for fid, leg in zip(fids, self.legs)}
        _require(tuple(self.timing.recover_order) == tuple(sorted(fids, key=lambda fid: ends[fid])),
                 "combo timing: recover order must equal legs sorted by ascending window end (INNER recovers first)")
        
        # 2 -- five-tension same-end recover reslice): a nested/common design
        # whose recover order deviates from reversed(inject) is legal ONLY as
        # the same-end tie order -- every adjacent recover_order pair that is
        # out of reversed-inject order must have EQUAL window ends (the tie),
        # run in leg order (the runner's same-offset event sort breaks ties by
        # contract.faults order), and carry a same_offset_budgets declaration
        # for that pair.  Strictly-nested designs keep the blanket law (their
        # sorted-by-end order IS reversed(inject)); the runner's same-offset
        # declaration gate remains the authoritative fail-closed face for any
        # tie the deviation scan does not reach (the D11 shape declares
        # voluntarily).
        if self.timing.geometry.split("_", 1)[0] in ("nested", "common")\
                and tuple(self.timing.recover_order) != tuple(reversed(self.timing.inject_order)):
            reverse_rank = {fid: rank for rank, fid in enumerate(reversed(self.timing.inject_order))}
            for first, second in zip(self.timing.recover_order, self.timing.recover_order[1:]):
                if reverse_rank[first] > reverse_rank[second]:
                    _require(ends[first] == ends[second] and fids.index(first) < fids.index(second),
                             "combo timing: a non-reverse-nested recover order is legal only as the "
                             'same-end tie order (equal ends, leg order; collection reslice ruling)')
                    _require(self.same_offset_budgets is not None
                             and frozenset((first, second)) in self.same_offset_budgets,
                             "combo timing: same-end recover ties must carry a same_offset_budgets "
                             'declaration (collection reslice ruling; D11 precedent)')
        # COMBO-ASM-3 MINOR-1 (ASM-2 review MINOR-1, probes N1/N3): the declared
        # geometry label is cross-checked against the DERIVED window-pair shape.
        # Vocabulary = the CROSS-CUTTING SS8-1 four classes (simultaneous rides
        
        # LEGAL partial_overlap form (the three-segment reading replaces the
        # F2_only tail with the OUTER leg's tail, SS8-4; the ASM-2 D07/D13/D29
        # partial_overlap labels are containment-shaped and stay valid), so:
        #   staggered*  -> every window pair disjoint;
        #   nested*/common* -> every pair nested (containment chain, no crossing);
        #   partial*    -> every pair overlapping (nested OR crossing forms).
        geometry_prefix = self.timing.geometry.split("_", 1)[0]
        _require(geometry_prefix in ("nested", "common", "staggered", "partial"),
                 "combo timing: geometry label must use the CROSS-CUTTING SS8-1 vocabulary "
                 "(nested*/common*/staggered*/partial*)")
        relations = {_combo_window_relation(leg_a.window, leg_b.window)
                     for index, leg_a in enumerate(self.legs) for leg_b in self.legs[index + 1:]}
        if geometry_prefix == "staggered":
            _require(relations <= {"disjoint"},
                     "combo timing: staggered geometry requires disjoint leg windows "
                     "(overlapping windows declared staggered are a mislabel)")
        elif geometry_prefix in ("nested", "common"):
            _require("disjoint" not in relations and "crossing" not in relations,
                     "combo timing: nested/common geometry requires a containment chain "
                     "(crossing or disjoint window pairs are a mislabel)")
        else:
            _require("disjoint" not in relations,
                     "combo timing: partial_overlap geometry requires overlapping leg windows "
                     "(disjoint window pairs are a mislabel)")
        # COMBO-ASM-4 NIT-C (ASM-3 review NIT-C, probe P9 face): MUST-preserve
        # injection-order direction knowledge gets a construction-level guard.
        # The timing checks above validate CONSISTENCY (orders vs windows);
        # this optional field validates the DIRECTION itself -- the declared
        # first-injected entity must be the leg carrying the earliest window
        # start.  A consistent swap (windows + orders + fixture all flipped
        # together) now fails loud here instead of riding through.
        if self.must_inject_first_entity is not None:
            _require(self.must_inject_first_entity in set(entities),
                     "combo timing: must_inject_first_entity must name a leg root entity")
            first_leg = self.legs[int(self.timing.inject_order[0][1:]) - 1]
            _require(first_leg.entity == self.must_inject_first_entity,
                     "combo timing: must_inject_first_entity disagrees with the declared "
                     "injection order (MUST-preserve direction knowledge violated)")
        
        # runner re-validates at the facts face). A key must be a frozenset fid
        # PAIR of THIS spec's legs (the driver looks up frozenset((fid, fid))
        # per same-offset action pair) and must name an ACTUAL same-offset pair
        # -- a dead declaration would ride through the driver lookup silently.
        # Whether every actual same-offset pair IS declared stays the driver's
        # refusal (runner _combo_timing_audit), so an undeclared spec keeps its
        # HEAD constructibility and only the build face refuses.
        if self.same_offset_budgets is not None:
            _require(isinstance(self.same_offset_budgets, Mapping),
                     "combo timing: same_offset_budgets must be a mapping")
            actions = []
            for fid, leg in zip(fids, self.legs):
                actions.append((leg.window[0], fid))
                actions.append((leg.window[1], fid))
            same_offset_pairs = {frozenset((actions[index][1], actions[other][1]))
                                 for index in range(len(actions)) for other in range(index + 1, len(actions))
                                 if actions[index][0] == actions[other][0] and actions[index][1] != actions[other][1]}
            for key, note in self.same_offset_budgets.items():
                _require(type(key) is frozenset and len(key) == 2 and set(key) <= set(fids),
                         "combo timing: same_offset_budgets keys must be frozenset fid pairs of this spec's legs")
                _require(key in same_offset_pairs,
                         "combo timing: same_offset_budgets must declare an actual same-offset "
                         "action pair (a declaration for a non-same-offset pair would never be "
                         "consumed by the driver lookup)")
                _require(type(note) is str and bool(note.strip()),
                         "combo timing: same_offset_budgets notes must be non-empty declarations")
        _require(self.scenario_id != "T08" or ("simultaneous" in self.timing.decision and "C1 观测核定" in self.timing.decision), "combo: T08 must keep the design-vs-implementation deviation precedent ('simultaneous') and the C1 观测核定 duty inside timing.decision (COMBO-ASM-4 review MINOR-1 machine pin)")
        # (b) carriers: dual-stream contract with role labels.
        _require(len(self.carriers) >= 2, "combo: main+bypass dual carrier streams required")
        labels = [carrier.carrier for carrier in self.carriers]
        _require(sum(1 for label in labels if label.startswith("combo-main-")) == 1,
                 "combo: exactly one combo-main- carrier required")
        _require(sum(1 for label in labels if label.startswith("combo-bypass-")) >= 1,
                 "combo: at least one combo-bypass- carrier required")
        _require(all(label.startswith(("combo-main-", "combo-bypass-", "combo-third-")) for label in labels),
                 "combo: every carrier label must declare its combo role prefix")
        stream_ids = [carrier.stream_id for carrier in self.carriers]
        _require(len(set(stream_ids)) == len(stream_ids), "combo: duplicate carrier stream_id")
        
        # stream to its in-plan combo stream (COMBO_LEG_STREAM_BINDINGS),
        # mirroring the AtomSpec carrier-stream guard on the combination face.
        # Idempotent by table lookup, so dataclasses.replace() re-runs it
        # safely; the atom's own rule object is copied, never mutated.
        bound_signals = []
        for fid, kind, signal in zip(fids, self.injection_signal_kinds, self.injection_signals):
            entry = COMBO_LEG_STREAM_BINDINGS.get((self.scenario_id, fid))
            if kind in CARRIER_SIGNAL_KINDS:
                _require(entry is not None,
                         "combo: carrier-rule leg " + fid + " of " + self.scenario_id
                         + ' declares no in-plan stream binding (collection: the atom-stream '
                           "inheritance is a registered evaluation gap, not a default)")
                bound_signals.append({**signal, "stream_id": entry[0]})
            else:
                _require(entry is None,
                         "combo: stream binding declared for non-carrier-rule leg " + fid
                         + " of " + self.scenario_id)
                bound_signals.append(signal)
        object.__setattr__(self, "injection_signals", tuple(bound_signals))
        # signals/injection rules: one per leg, entity-aligned.
        _require(len(self.signals) == len(self.legs), "combo: one signal per leg required")
        for signal, leg in zip(self.signals, self.legs):
            _require(signal.entity == leg.entity, "combo: signal entity must equal the leg root entity")
        _require(len(self.injection_signal_kinds) == len(self.legs)
                 and len(self.injection_signals) == len(self.legs),
                 "combo: per-leg injection signal declarations required")
        for kind, signal, leg in zip(self.injection_signal_kinds, self.injection_signals, self.legs):
            if kind == "metric_threshold":
                _require(signal is None, "combo: metric_threshold signal comes from the leg signal spec")
            else:
                _require(isinstance(signal, dict) and signal.get("entity") == leg.entity,
                         "combo: custom injection signal must be an explicit entity-aligned object")
        
        
        # declare a valid evidence mode; a carrier_signal leg must bind a
        # stream whose request configuration is field-identical to the base
        # atom's own carrier (contrast-family consistency: the combo arm's
        
        
        # explicit mechanism_or_bypass declaration exempts a leg from the
        # identity requirement, and never silently.
        plan_stream_ids = set(stream_ids)
        for (bind_sid, fid), (bound_stream_id, mode, rationale) in COMBO_LEG_STREAM_BINDINGS.items():
            if bind_sid != self.scenario_id:
                continue
            _require(fid in set(fids),
                     "combo: stream binding names a leg this combo does not carry (" + fid + ")")
            _require(bound_stream_id in plan_stream_ids,
                     "combo: carrier-rule leg " + fid + " of " + self.scenario_id
                     + " binds a stream outside this combo's run plan")
            _require(mode in CARRIER_EVIDENCE_MODES,
                     "combo: unknown carrier evidence mode for leg " + fid)
            _require(type(rationale) is str and bool(rationale.strip()),
                     "combo: stream binding for leg " + fid + " must carry a rationale")
            if mode == "carrier_signal":
                # A spec with missing/malformed/unknown reference dependencies
                # is refused by the (f) checks below; the identity comparison
                # runs whenever the base atom reference resolves.
                references = self.reference_dependencies.get(fid) or ()
                base_atom = ATOM_SPECS.get(references[0]) if references else None
                if base_atom is not None:
                    bound_carrier = next(carrier for carrier in self.carriers
                                         if carrier.stream_id == bound_stream_id)
                    _require(_carrier_request_config(bound_carrier) == _carrier_request_config(base_atom.carrier),
                             "combo: carrier_signal leg " + fid + " of " + self.scenario_id
                             + " must bind an endpoint-config-identical stream (family consistency: "
                               "same request configuration and observation face as the single-root arm; "
                               "a different-shape stream needs the explicit mechanism_or_bypass declaration)")
        # (e) interaction expectation: design intent only, 期望非观测 marker mandatory.
        _require(isinstance(self.interaction_expectation, str)
                 and "期望非观测" in self.interaction_expectation,
                 "combo: interaction expectation is design intent only and must carry the 期望非观测 marker")
        
        _require(set(self.reference_dependencies) == set(fids),
                 "combo: per-leg reference dependencies required for every fault instance")
        for fid, refs in self.reference_dependencies.items():
            _require(isinstance(refs, tuple) and bool(refs),
                     "combo: reference dependency must name at least one single-root scenario")
            for ref in refs:
                _require(ref in ATOM_SPECS, "combo: reference must name an assembled single-root spec")
        
        _require(self.obs_r1_target_kind == "pod",
                 'combo: CRI targets must stay the single pod: key type (OBS-collection)')
        _require(isinstance(self.obs_r1_audit, str) and bool(self.obs_r1_audit.strip()),
                 'combo: OBS-collection audit decision required')
        _require(isinstance(self.provenance, Mapping) and bool(self.provenance),
                 "combo: provenance anchors required")
        for key in ("blueprint", "catalog_scenario", "ledger", "parameter_status",
                    "request_profile_status", "timing_status"):
            _require(key in self.provenance, "combo: provenance must anchor " + key)
        _require(type(self.preconditions) is tuple
                 and all(isinstance(item, Precondition) for item in self.preconditions),
                 "combo: preconditions must be explicit Precondition tuples")


def _combo_window_relation(a: tuple[int, int], b: tuple[int, int]) -> str:
    """Derive the pairwise window shape: disjoint / nested / crossing.

    COMBO-ASM-3 (ASM-2 review MINOR-1 machine closure): the declared geometry
    label is cross-checked against these derived shapes.  "nested" covers
    containment including equal windows; "crossing" is an overlap where neither
    window contains the other (the literal partial-overlap shape).
    """
    if a[1] <= b[0] or b[1] <= a[0]:
        return "disjoint"
    if (a[0] <= b[0] and b[1] <= a[1]) or (b[0] <= a[0] and a[1] <= b[1]):
        return "nested"
    return "crossing"


# COMBO-ASM-3 (ASM-2 review NIT-2): the db-lock release TAIL budget as a single
# machine-checkable constant.  Source arithmetic = the S06 recover precondition
# (two confirm_release readbacks <=15 s each + statements + margin); the ASM-2
# D02/D10 combos pinned db_window_end + 35 == net CRD delete point (120), and
# that relation is now a construction-time invariant for every combination that
# carries a database lock leg next to chaos_mesh CRD legs: the CRD delete point
# (the CRD leg's planned window end) must sit at least one TAIL budget past the
# lock window end so the confirmed release always completes BEFORE the CRD
# delete (a CRD delete racing the release confirmation voids the reverse-nested
# recovery evidence, _db_net_order_precondition).
DB_LOCK_RELEASE_TAIL_BUDGET_S = 35


def _combo_leg(base_id: str, window: tuple[int, int], *,
               parameters: Mapping[str, Any] | None = None, duration_s: int | None = None) -> FaultLegSpec:
    """Derive a combination k8s leg from a single-root AtomSpec leg.

    Identity fields (entity/deployment/selector/mechanism/mechanism_version/
    fault_class) come from the single-root leg verbatim; only the planned
    window -- and, when the combo window demands it, the CRD duration_s --
    may be replaced.  Parameter sets for env/gateway legs stay the atom's
    exact locked set unless an explicit combo parameters mapping is supplied
    (timeout tier divergence is an argued C11 decision, never silent).
    """
    base = ATOM_SPECS[base_id].leg
    _require(isinstance(base, FaultLegSpec),
             "combo k8s legs derive from FaultLegSpec single roots only (database legs use _combo_db_leg)")
    params = dict(base.parameters) if parameters is None else dict(parameters)
    if duration_s is not None:
        params["duration_s"] = duration_s
    return replace(base, window=window, parameters=params)


def _combo_db_leg(base_id: str, window: tuple[int, int]) -> DbFaultLegSpec:
    """Derive a combination DATABASE leg from the S06 single-root leg (ASM-2).

    The MySqlTableLock identity (entity/database/table/mode/mechanism/
    mechanism_version) comes from the single-root leg verbatim and the
    parameter set is fully derived from that identity (DbFaultLegSpec
    discipline), so only the planned lock window is replaced -- a combo lock
    leg can never drift from the S06 reference atom's table/mode contract.
    """
    base = ATOM_SPECS[base_id].leg
    _require(isinstance(base, DbFaultLegSpec),
             "combo database legs derive from DbFaultLegSpec single roots only")
    return replace(base, window=window)


def _combo_signal(base_id: str) -> MetricSignalSpec:
    return ATOM_SPECS[base_id].signal


def _combo_signal_on_stream(base_id: str, *, service_name: str, http_target: str) -> MetricSignalSpec:
    """X01 (collection.1, EXEC-⑦ 2026-09-22): rebind an atom-derived combo signal onto
    a stream face the combo actually carries.

    ``_combo_signal`` is only endpoint-identical for combos that provision a
    stream of the ATOM's own carrier family.  A combo whose request profile
    carries NO stream of that family must not keep requiring the atom's metric
    face: the runner builds the per-leg Q2/Q3 metric and trace queries from
    ``spec.signals`` (``_combo_telemetry_inputs``), so a verbatim atom face
    would demand a service the plan never requests -- the registered X01
    observation-face mismatch (D16 field evidence: F1 0 points, Q2 no pre/post,
    Q3 no series; D05 strong inference, xhigh matrix).  The rebind moves ONLY
    the observation face (service_name + http_target) onto a declared in-plan
    stream's span face; entity (leg-root alignment), unit, min_points, mode and
    tolerances stay atom-derived -- no new statistic, threshold or collector is
    minted here (tolerance re-freeze stays an collection family-wide decision).
    """
    base = ATOM_SPECS[base_id].signal
    _require(bool(service_name) and http_target.startswith("/"),
             "combo signal rebind: a concrete in-plan service/path face is required "
             "(X01: the rebound face must name a stream the combo actually carries)")
    return replace(base, service_name=service_name, http_target=http_target)


def _combo_injection_signal(base_id: str) -> Mapping[str, Any] | None:
    """Per-leg injection signal, derived verbatim from the single-root atom.

    collection closes the collection-registered gap: the atom-derived rule's ``stream_id``
    (the ATOM's single-root carrier stream) is rebound at ComboSpec
    construction to one of the combo's own in-plan carrier streams, per the
    module-level COMBO_LEG_STREAM_BINDINGS table (user directive 2026-09-21
    item 4: reuse the existing streams, never mint combo-private ones; root
    adjudication "reuse-existing-streams" direction, collection option 1).  The
    rebind is construction-guarded fail-closed: an r10 carrier-rule leg with
    no table entry refuses assembly instead of silently inheriting the atom
    stream (the old inheritance honestly NOT_ASSESSED as
    ``carrier_stream_not_in_plan`` -- the face this lane exists to close).
    """
    return ATOM_SPECS[base_id].injection_signal



# in-plan streams): the per-leg carrier-rule stream binding table.  Keys name
# (scenario_id, fid) for every combination leg whose injection signal is an

# are (bound in-plan stream_id, evidence mode, rationale).
#

#   * reuse -- the bound stream is always a stream THIS combo already declares
#     in request_profile.streams (carriers); no new stream is ever minted and
#     several roots MAY share one stream (D05/D06/D22 carry two legs on their

#   * family consistency -- a "carrier_signal" leg binds the combo stream
#     whose REQUEST CONFIGURATION is field-identical to the base atom's own
#     carrier (service/path_prefix/path_suffix/parameterized/rate_rps/
#     timeout_s/max_concurrency/client_retry_limit/direct_api), so the combo


#     legs have exactly one such stream; T01's two pricing-shaped streams are
#     disambiguated by their declared roles (third = the pricing-cpu
#     self-carrier per T01 provenance, main = the via-gw stacked channel).
#   * masked roots -- a "mechanism_or_bypass" leg is the explicit degraded
#     mode the user directive allows: the combo provisions no

#     a masking hypothesis, so the carrier statistic may legitimately FAIL to

#     then rides the mechanism checks (Q1.<fid>.operation/.window) plus
#     bypass/mechanism-specific evidence.  D05 F2 is the declared case: its
#     spec's interaction_expectation pins the pod-down-502-masks-read_timeout
#     -504 direction on the shared main stream.  The mode is declared, never
#     silently applied; the evaluation consumer stamps it into the Q1 row.
CARRIER_EVIDENCE_MODES = ("carrier_signal", "mechanism_or_bypass")
CARRIER_SIGNAL_KINDS = ("carrier_error_fraction", "carrier_p95_ratio")

COMBO_LEG_STREAM_BINDINGS: Mapping[tuple[str, str], tuple[str, str, str]] = {
    # (scenario_id, fid): (bound in-plan stream_id, evidence mode, rationale)
    ("D02", "F2"): ("d02-main-catalog-items-dblock", "carrier_signal",
                    "endpoint-identical to S06 carrier catalog-items-dblock-read; family face preserved"),
    ("D03", "F1"): ("d03-bypass-pricing-direct", "carrier_signal",
                    "endpoint-identical to S05 carrier pricing-items-hostcpu-read; family face preserved"),
    ("D03", "F2"): ("d03-main-catalog-items-cpu", "carrier_signal",
                    "endpoint-identical to S04 carrier catalog-items-cpu-read; family face preserved"),
    ("D04", "F1"): ("d04-main-catalog-items-via-gw", "carrier_signal",
                    "endpoint-identical to S03 carrier catalog-items-via-gw; family face preserved"),
    ("D05", "F1"): ("d05-main-catalog-items-via-gw", "carrier_signal",
                    "endpoint-identical to S03 carrier catalog-items-via-gw; multi-root shared stream with F2"),
    ("D05", "F2"): ("d05-main-catalog-items-via-gw", "mechanism_or_bypass",
                    "no endpoint-identical stream in plan (D05 provisions no pricing stream); bound to the "
                    "main stream whose serving gw IS this leg's target (read_timeout knob governs it directly); "
                    "spec-declared masking: pod-down instant 502 masks the read_timeout 504 signature during "
                    "the pod subwindow (D05 interaction_expectation), so validity rides mechanism+bypass evidence; "
                    "X01 (EXEC-⑦ 2026-09-22): the Q2/Q3 recovery metric + trace face rides the SAME stream "
                    "(signals F2 rebound to catalog_service /api/items/ -- see _combo_signal_on_stream), so the "
                    "Q1 carrier binding and the recovery observation face name one request face, not two"),
    ("D06", "F1"): ("d06-main-pricing-via-gw", "carrier_signal",
                    "endpoint-identical to S05 carrier pricing-items-hostcpu-read; multi-root shared stream with F2"),
    ("D06", "F2"): ("d06-main-pricing-via-gw", "carrier_signal",
                    "endpoint-identical to S24 carrier pricing-items-via-gw; F1_only subwindows keep the legs "
                    "separable on the shared stream"),
    ("D09", "F1"): ("d09-main-sasrec-inference-post-v1", "carrier_signal",
                    "endpoint-identical to S27 carrier sasrec-inference-post-v1; fixed read-only body and family face preserved"),
    ("D10", "F1"): ("d10-bypass-catalog-items-dblock", "carrier_signal",
                    "endpoint-identical to S06 carrier catalog-items-dblock-read; family face preserved"),
    ("D11", "F2"): ("d11-main-catalog-items-runtime", "carrier_signal",
                    "endpoint-identical to S07 carrier catalog-items-runtime-read; family face preserved"),
    ("D12", "F2"): ("d12-main-pricing-via-gw", "carrier_signal",
                    "endpoint-identical to S24 carrier pricing-items-via-gw; family face preserved"),
    ("D14", "F2"): ("d14-bypass-catalog-items-cpu", "carrier_signal",
                    "endpoint-identical to S04 carrier catalog-items-cpu-read; family face preserved"),
    ("D15", "F2"): ("d15-main-pricing-items-cpu", "carrier_signal",
                    "endpoint-identical to S28 carrier pricing-items-cpu-read; family face preserved"),
    ("D16", "F2"): ("d16-main-catalog-items-via-gw", "carrier_signal",
                    "endpoint-identical to S03 carrier catalog-items-via-gw; family face preserved"),
    ("D17", "F1"): ("d17-main-checkout-health", "carrier_signal",
                    "endpoint-identical to S18 carrier checkout-health-pod-read; family face preserved"),
    ("D18", "F1"): ("d18-main-cart-health", "carrier_signal",
                    "endpoint-identical to S10 carrier cart-health-read; family face preserved"),
    ("D18", "F2"): ("d18-bypass-order-detail", "carrier_signal",
                    "endpoint-identical to S09 carrier order-detail-read; family face preserved"),
    ("D19", "F1"): ("d19-main-search-list", "carrier_signal",
                    "endpoint-identical to S19 carrier search-list-pod-read; family face preserved"),
    ("D19", "F2"): ("d19-bypass-review-query-list", "carrier_signal",
                    "endpoint-identical to S11 carrier review-query-list-read; family face preserved"),
    ("D20", "F1"): ("d20-main-recagent-health", "carrier_signal",
                    "endpoint-identical to S20 carrier recagent-health-read; family face preserved"),
    ("D20", "F2"): ("d20-bypass-backend-stats", "carrier_signal",
                    "endpoint-identical to S12 carrier backend-stats-read; family face preserved"),
    ("D21", "F2"): ("d21-main-backend-stats", "carrier_signal",
                    "endpoint-identical to S12 carrier backend-stats-read; family face preserved"),
    ("D22", "F1"): ("d22-main-pricing-via-gw", "carrier_signal",
                    "endpoint-identical to S28 carrier pricing-items-cpu-read; multi-root shared stream with F2"),
    ("D22", "F2"): ("d22-main-pricing-via-gw", "carrier_signal",
                    "endpoint-identical to S24 carrier pricing-items-via-gw; F1_only subwindows keep the legs "
                    "separable on the shared stream"),
    ("D23", "F1"): ("d23-main-cart-health", "carrier_signal",
                    "endpoint-identical to S10 carrier cart-health-read; family face preserved"),
    ("D23", "F2"): ("d23-bypass-pricing-items-cpu", "carrier_signal",
                    "endpoint-identical to S28 carrier pricing-items-cpu-read; family face preserved"),
    ("D24", "F1"): ("d24-bypass-cart-health", "carrier_signal",
                    "endpoint-identical to S10 carrier cart-health-read; family face preserved"),
    ("D24", "F2"): ("d24-main-checkout-health", "carrier_signal",
                    "endpoint-identical to S18 carrier checkout-health-pod-read; family face preserved"),
    ("D25", "F1"): ("d25-bypass-pricing-items-cpu", "carrier_signal",
                    "endpoint-identical to S28 carrier pricing-items-cpu-read; family face preserved"),
    ("D25", "F2"): ("d25-main-checkout-health", "carrier_signal",
                    "endpoint-identical to S18 carrier checkout-health-pod-read; family face preserved"),
    ("D26", "F2"): ("d26-bypass-backend-stats", "carrier_signal",
                    "endpoint-identical to S12 carrier backend-stats-read; family face preserved"),
    ("D27", "F1"): ("d27-main-backend-stats", "carrier_signal",
                    "endpoint-identical to S12 carrier backend-stats-read; family face preserved"),
    ("D27", "F2"): ("d27-bypass-sasrec-inference-post-v1", "carrier_signal",
                    "endpoint-identical to S27 carrier sasrec-inference-post-v1; fixed read-only body and family face preserved"),
    ("D28", "F2"): ("d28-main-sasrec-inference-post-v1", "carrier_signal",
                    "endpoint-identical to S27 carrier sasrec-inference-post-v1; fixed read-only body and family face preserved"),
    ("D29", "F2"): ("d29-main-catalog-items-via-gw", "carrier_signal",
                    "endpoint-identical to S03 carrier catalog-items-via-gw; family face preserved"),
    ("D30", "F1"): ("d30-bypass-sasrec-inference-post-v1", "carrier_signal",
                    "endpoint-identical to S27 carrier sasrec-inference-post-v1; fixed read-only body and family face preserved"),
    ("D30", "F2"): ("d30-main-catalog-items-via-gw", "carrier_signal",
                    "endpoint-identical to S03 carrier catalog-items-via-gw; family face preserved"),
    ("D31", "F1"): ("d31-main-catalog-items-cpu", "carrier_signal",
                    "endpoint-identical to S04 carrier catalog-items-cpu-read; family face preserved"),
    ("D31", "F2"): ("d31-bypass-review-query-list", "carrier_signal",
                    "endpoint-identical to S11 carrier review-query-list-read; family face preserved"),
    ("D32", "F1"): ("d32-bypass-catalog-items-cpu", "carrier_signal",
                    "endpoint-identical to S04 carrier catalog-items-cpu-read; family face preserved"),
    ("D32", "F2"): ("d32-main-order-detail-pod", "carrier_signal",
                    "endpoint-identical to S14 carrier order-detail-pod-read; family face preserved"),
    ("D33", "F1"): ("d33-bypass-review-query-list-pod", "carrier_signal",
                    "endpoint-identical to S11 carrier review-query-list-read; family face preserved"),
    ("D33", "F2"): ("d33-main-order-detail-pod", "carrier_signal",
                    "endpoint-identical to S14 carrier order-detail-pod-read; family face preserved"),
    ("T01", "F1"): ("t01-third-pricing-items-cpu", "carrier_signal",
                    "endpoint-identical to S28 carrier pricing-items-cpu-read; T01 disambiguation: the third "
                    "stream is the declared pricing-cpu self-carrier (provenance 'third=pricing-direct pricing "
                    "cpu 自载体')"),
    ("T01", "F3"): ("t01-main-pricing-via-gw", "carrier_signal",
                    "endpoint-identical to S24 carrier pricing-items-via-gw; T01 disambiguation: the main "
                    "stream is the declared three-leg stacked via-gw channel (provenance 'main=pricing-via-gw "
                    "三腿叠加主通道'), the path that crosses this leg's catalog-gw target"),
    ("T05", "F1"): ("t05-main-checkout-health", "carrier_signal",
                    "endpoint-identical to S18 carrier checkout-health-pod-read; family face preserved"),
    ("T05", "F2"): ("t05-bypass-cart-health", "carrier_signal",
                    "endpoint-identical to S10 carrier cart-health-read; family face preserved"),
    ("T05", "F3"): ("t05-third-pricing-items-cpu", "carrier_signal",
                    "endpoint-identical to S28 carrier pricing-items-cpu-read; family face preserved"),
    ("T06", "F1"): ("t06-bypass-backend-stats", "carrier_signal",
                    "endpoint-identical to S12 carrier backend-stats-read; family face preserved"),
    ("T06", "F2"): ("t06-third-sasrec-inference-post-v1", "carrier_signal",
                    "endpoint-identical to S27 carrier sasrec-inference-post-v1; fixed read-only body and family face preserved"),
    ("T07", "F2"): ("t07-third-sasrec-inference-post-v1", "carrier_signal",
                    "endpoint-identical to S27 carrier sasrec-inference-post-v1; fixed read-only body and family face preserved"),
    ("T07", "F3"): ("t07-main-catalog-items-via-gw", "carrier_signal",
                    "endpoint-identical to S03 carrier catalog-items-via-gw; family face preserved"),
    ("T08", "F1"): ("t08-main-order-detail-pod", "carrier_signal",
                    "endpoint-identical to S14 carrier order-detail-pod-read; family face preserved"),
    ("T08", "F2"): ("t08-bypass-review-query-list-pod", "carrier_signal",
                    "endpoint-identical to S11 carrier review-query-list-read; family face preserved"),
    ("T08", "F3"): ("t08-third-catalog-items-cpu", "carrier_signal",
                    "endpoint-identical to S04 carrier catalog-items-cpu-read; family face preserved"),
}


def _carrier_request_config(carrier: CarrierStreamSpec) -> tuple[Any, ...]:
    """Full request-configuration face of one carrier stream (collection guard basis).

    Every field a request row's judgment basis depends on; the contrast-family
    consistency check compares this tuple between the combo arm's bound stream
    and the single-root arm's own carrier.
    """
    return (carrier.service, carrier.path_prefix, carrier.path_suffix, carrier.parameterized,
            carrier.rate_rps, carrier.timeout_s, carrier.max_concurrency,
            carrier.client_retry_limit, carrier.direct_api)


# D01/D12 share nested fault geometry: OUTER (30,122) injects first; the GW

# measured D01 GW dispatch margin. In the short engineering profile, during
# now ends at 210 (D12's observed serial restore completed near 173, leaving
# about 37 s before post), followed by 60 s of post observation. Neither fault
# window moves. common_driver still projects long300 to 300/300/300.
COMBO_NESTED_PHASES = ((0, 30), (30, 210), (210, 270))
COMBO_OUTER_WINDOW = (30, 122)
COMBO_GW_INNER_WINDOW = (62, 122)

# D06 anchor-1 observed a 39.875 s F2 injection under host saturation. The
# actual F2 instance begins at inject confirmation; Q1 then applies the declared
# 45 s GW settle. Keep F1 active through the maximum adapter-bounded F2 restore
# so its Q1 scoring window remains host-amplified. The F2 restore precedes F1
# cleanup (strict nested order); 17 s action + 90 s adapter wait + 2 s start
# lateness are carried by the 115 s planned recovery gap. Short-profile cleanup
# also completes inside DURING; long300 keeps the formal 300/300/300 phases.
D06_PHASES = ((0, 30), (30, 330), (330, 360))
D06_HOST_WINDOW = (30, 305)
D06_GATEWAY_WINDOW = (62, 190)


# fix-1/2/3 pilots proved the 20 ms tier near-unfiring on THIS chain (fix-3 Q1
# F2 statistic 0.0 over 44 eval requests; whole during phase 297 main-stream
# requests carried only 2-3 errors, 2 on switch edges): the pricing-CPU
# amplifier leg does NOT raise the gw->catalog upstream (Jaeger server spans
# during p50 7.4 ms vs pre 7.5 ms), so the old ov-hostcfg-f2 20 ms razor-edge
# tier -- calibrated for a HOST-amplified chain (D06: host lifts TTFB to
# 30-50 ms, tier below that fires) -- lost its amplification coupling here.
# Calibration-first re-argument (design text: razor-edge tiers are candidates,
# never copied nor silently dropped): fix-1/2/3 pooled pre_fault catalog
# upstream spans n=900, p25/p50/p75/p90 = 7.24/7.62/8.06/8.54 ms; nginx read
# gap = span + in-cluster transit (~0.5-1.5 ms).  Declared tier 7 ms (integer,
# prepare_gateway renders "7ms") sits below the pooled p50 with zero-transit
# lower bound frac(span>7)=0.838 >= 0.5; 8 ms would bound at 0.283 (reject),
# 10 ms at 0.019 (reject).  Window: the GW-config carrier settle moves to the
# family value 45 s (GW_CONFIG_CARRIER_SETTLE_BUFFER_S), so the F2 subwindow
# widens 60 -> 130 s (62,192): worst-case effective eval window = span 130 -
# 40 (slowest measured inject rollout) - 45 (settle) + 15 (fastest restore) =
# 60 requests, comfortably above the n>=30 floor; typical ~85.  The F1 pricing
# CPU OUTER window slides its end 122 -> 192 WITH the GW leg (duration 92 ->
# 162): the nested option-B shape (amplifier full window + CFG subwindow,

# only the common end moves.  Action offsets 30/62/192/192 keep min positive
# gap 32 s > the default serial budget 19 s.  Phases widen to a D22-private
# tuple: during (30,222) keeps the <=28 s GW restore tail (192+28=220) inside
# during exactly like the D05 pattern (135 -> 165); at long300 the run phases
# are forced to (300,600) and the F2 recover completion 462 + dispatch lag
# (~17 s measured fix-3) + rollout (<=40 s) = ~519 stays <= 600. D01/D12 keep
# their shared fault windows and GW anchors; D06 uses its separate phases.
D22_PHASES = ((0, 30), (30, 222), (222, 252))
D22_PRICING_CPU_OUTER_WINDOW = (30, 192)
D22_GW_INNER_WINDOW = (62, 192)

# D05 (option B, pod INNER): gw timeout OUTER (30,135) injects first (switch
# band 25 s -> settled ~55); pod-failure INNER rides the below_kill_band v2
# profile (55,115) -- span 60, lag head absorbed by the pod carrier settle;
# the EFFECTIVE pod-down segment [~89,~126] sits fully inside the gw steady
# segment [55,135] (co-active ~37 samples >= floor 30; the old head-62
# dispatch margin is superseded -- the +34 s lag IS the margin); pod recover
# @115 restore+Ready ~128 stays strictly before the gw restore start (T4
# cfg/pod strict-ordering knowledge; collection head discipline kept on the grid);
# gw restore tail <=28 s ends <=163 inside during [30,165).

# offsets {30,55,115,135} keep min positive gap 20 > the default budget sum.
D05_PHASES = ((0, 30), (30, 165), (165, 195))
D05_GW_OUTER_WINDOW = (30, 135)
D05_POD_INNER_WINDOW = (55, 115)

# Historical T01 timing rationale; the current a5 reslice is pinned below.
# T01 (legacy nested, three legs): catlat F2 OUTER (30,130) first (catlat-first
# gives the svccpu leg a stable pod, StressChaos-survives-rollout avoidance;
# the env rollout settles ~45 and F1 now injects @50 with a 5 s margin); pricing
# cpu F1 (50,110) on the settled pod; gw F3 (70,110) late subwindow (switch


# sit SAME-END with the pricing recover @110 (recover side staggered 105/110 ->

# unchanged AT THAT RULING -- its honest residual was the inject-side 5 s gap,

# takes the global minimum positive gap over all six offsets and the pinned


# 45->50, F3 inject 50->70, F2 recover 120->130; recover same-end tie @110 and
# its declaration unchanged): offsets {30,50,70,110,110,130} -> minimum
# positive gap 20 > the 19 s default budget sum, so the as-declared chain

# the real chain); the honest typical/tail sums (26/33) still exceed 20 -- the

# live backfill), never small-budgeted past.  Floors after the move (raw

# [70,110] = 40 and pairwise F1/F2 = 60, F1/F3 = F2/F3 = 40, all >= 30; the

# 15 -- the move's scientific footprint, registered in the three note sites,
# never silently re-counted.


# (settle alone is 45 s), and the legacy 1000 ms dose killed ~0 % of the
# pricing face (EXEC-3 span calibration n=900: p50 7.62 ms, frac>7 ms=0.838

# (injection side unchanged at 30/50/70; recover side re-argued):
# F1 (50,110)->(50,150) [duration 65->100], F2 (30,130)->(30,170),

# preserved: recover order (F1,F3,F2), deviation only at the declared tie);
# nested chain F2 (30,170) >= F1 (50,150) >= F3 (70,150) intact.  Offsets
# {30,50,70,150,150,170}: min positive gap 20 > the 19 s default budget sum.
# Floors: raw triple co-active [70,150] = 80, pairwise F1/F2 = 100,
# F1/F3 = 80, F2/F3 = 100; post-settle steady triple (gw settled ~115)
# [115,150] = 35 >= 30; F3's own eval window [115,150] = 35 s >= 30
# min_requests at 1 rps.  Dose read_timeout_ms 1000 -> 7 (span-calibrated,
# D22 precedent).  Recover tail: F3 gw restore <=~175, F2 env unset <=~187,
# during extends to (30,190).

# serial footprint = its carry slot (+17) + the restore action itself
# (measured 15.2 s: patch 0.1 + rollout band 12.9 + settled readback 2.2)
# + the post-action journal reconcile (measured ~11 s) = slot 137 -> done
# ~163. F2's bare offset deadline must clear ALL of it plus its own env
# footprint (<=17): 205 (deadline 207, dispatch ~163 leaves 44 s margin,
# F2 ends <=190), during (30,225); min positive gap stays 20 (50-30).

# during_fault, leaving the 20 s head snapshot before the first catalog env
# rollout. Preserve each fault duration, all pair/triple intersections, the
# F1/F3 same-end recovery tie, and the F3 post-settle 35 s signal window.
# F2 recovery remains 55 s ahead of during end, giving the catalog rollback
# Pod lifecycle time to settle before the tail snapshot. This short smoke is
# not a formal sample; common_driver still projects long300 to 300/300/300.
T01_PHASES = ((0, 30), (30, 280), (280, 340))
T01_CATLAT_WINDOW = (50, 225)
T01_PRICING_CPU_WINDOW = (70, 170)
T01_GW_INNER_WINDOW = (90, 170)


def _pricing_gw_path_precondition() -> Precondition:
    """Single truth source: reuse the S24 gw-path premise verbatim (S01 family)."""
    return ATOM_SPECS["S24"].preconditions[0]


def _reference_ordering_precondition(scenario_id: str, refs: str) -> Precondition:
    return Precondition(
        statement="the single-root reference blocks (" + refs + ") must enter the SAME sampling plan at the "
                  "same frozen CRD/env parameters, ordered before-or-same-batch as combo " + scenario_id + " "
                  "(C1 pooled-reference design, 口径B): a missing or parameter-mismatched single keeps the "
                  "combo row in the pairing denominator (not_estimable) -- it is never silently dropped",
        mechanism_anchor="combo-blueprint CROSS-CUTTING.md §6 constraint 1; DESIGN §7.3-3; C1 §2-1 "
                         "(params_id = mechanism_version:sha256(canonical(mechanism+parameters))[:12])",
        owner='sampling-plan/driver layer (collection); assembly declares, never schedules',
        status="NOT_FROZEN ordering premise")


def _combo_execution_prerequisite() -> Precondition:
    return Precondition(
        statement="the gateway rollout live-execution wiring is not landed (B2-01 §4 R 接线缺口; the "
                  "integration-contract items are pending): this combination has NO runnable new-chain "
                  "form yet -- blueprint-first assembly is not executability; every live use additionally "
                  'owes the collection combination smoke (组合实现不等于组合采集资格)',
        mechanism_anchor='configs/collection/scenarios.json: assembly definition'
                         "01-gateway-cfg-family.md §4 (integration contract)",
        owner='integration/driver layer (collection); assembly declares, never provisions',
        status="NOT_FROZEN prerequisite")


def _s23a_pilot_coupling_precondition() -> Precondition:
    return Precondition(
        statement="S23-A pilot coupling: the D01 assembly is produced in lockstep with the S23-A "
                  "diagnostic-value pilot material; if the pilot deletes S23, D01's reference side "
                  "follows the 'missing single stays in the denominator' rule and is registered -- "
                  "the combo spec is never silently rewritten",
        mechanism_anchor="combo-blueprint CROSS-CUTTING.md §6 constraint 2 (B2-02 S23-A-4); SPECS-1 D01(f)",
        owner='pilot/sampling-plan layer (collection); assembly declares, never decides',
        status="NOT_FROZEN pilot coupling")


def _t01_stable_pod_precondition() -> Precondition:
    return Precondition(
        statement="catlat-first injection order is a runtime MUST: the catalog deployment env rollout "
                  "must settle BEFORE the pricing StressChaos pins its pod (a StressChaos surviving a "
                  "rollout keeps stressing the replaced pod) and the gateway rollout subwindow applies "
                  "last; the current short window order F2(50,225) -> F1(70,170) -> F3(90,170) "
                  "encodes this and the driver must inject in exactly that order. The 2026-09-24 "
                  "a5 reslice shifts all three faults by 20 s without changing pairwise geometry; "
                  "during head and tail reserve the catalog Pod rollout/rollback margins",
        mechanism_anchor="B1-06 §2 D03 same rule (catalog-first-on-idle-VM); SPECS-1 T01(d) 注入顺序知识; "
                         "CROSS-CUTTING §8-2",
        owner='driver/runtime-binding layer (collection): injection sequencing; assembly declares, never sequences',
        status="NOT_FROZEN ordering premise; violated order invalidates the attempt, not just the GT")


# --------------------------------------------------------------------------- #
# COMBO-ASM-2 (sixth batch): SPECS-2 B1 pure-family combinations
# (D02/D04/D07/D08/D10/D11/D13/D16/D17/D29 -- net/pod/db/env leg mixes, NO
# gateway cfg leg anywhere in the family, so the 25 s gateway switch band of
# batch 1 never enters this batch's budget arithmetic).  Shared budget atoms:
# C1 SS2.5 floor 30 samples at 1 rps; env-rollout head band 15 s (S07/S08

# measured 28.8-30.9 s, absorbed as the pod-family carrier settle head);
# pod restore+Ready 11-13 s (confirmed inside the recover action);
# db lock recover TAIL 35 s
# (S06 precondition 3 arithmetic); CRD legs are seconds-level.
# --------------------------------------------------------------------------- #

# D02/D10 (db + net, simultaneous = COMMON TARGET WINDOW with a NET-first
# head and the lock-release TAIL tail): the net CRD injects first (B1-06 SS2
# D10 NET-first L15161-15183), the db lock enters right behind it; the
# dual-active analysis window runs 35->85 (50 samples); recovery is
# lock-release-first -- the net window end sits one TAIL budget (35 s) past
# the lock window end so the CRD delete is sequenced strictly AFTER the
# confirmed release; the seconds-level CRD deletion ends <=121 inside during
# [30,125).

# chaos-mesh CRD inject latency is 3.109-3.125 s (three points: D09a2 F1/F2
# injects 3.109, D02-S02 network_loss 3.110, D09a1 StressChaos 3.125), so the
# old 3 s head-gap scheduled the db inject while the netem apply was still in
# flight -- physically violating the serial action law the O-D3 audit encodes.
# 5 s restores the law with the field-proven budget 4.0/0.8 (D09a2: four
# actions all confirmed under 4.0; 4.0+0.8=4.8<5). The lock window end stays
# 85 so the release TAIL invariant lock_end+35 == net_end (120) and the
# reverse-nested recovery ordering are untouched; the dual-active analysis
# window yields 50 samples (35->85), still >= the C1 2.5 floor of 30.
COMBO2_DBNET_PHASES = ((0, 30), (30, 125), (125, 155))
COMBO2_DBNET_NET_WINDOW = (30, 120)     # duration_s=90 covers the 90 s span (MINOR-1)
COMBO2_DBNET_DB_WINDOW = (35, 85)

# D04 (dual pod-failure, staggered = two DISJOINT sub-windows): catalog F1
# below_kill_band v2 (30,90) -- span 60 with the measured lag head absorbed by
# the pod-family carrier settle (35 s), eval window ~34 requests >= floor 30;
# the mid-action strict sequence (recover -> restart +2 registration ->
# snapshot -> user inject) keeps the 40 s gap = restore/registration ~35 s +
# margin 5 (the recover action's own wait_pod_ready confirms Ready ~13 s,
# measured 13.17/13.39 on the r10c pilots); user F2 below_kill_band v2
# (130,190); user restore+Ready ~203 completes inside during end 205.  This
# staggered class is exactly why ComboTimingProfile's reversed(inject) law is
# geometry-conditional (a disjoint design recovers in window-end order).

# (= span + lag bound; the old 38-vs-35 margin is dead arithmetic under the
# measured transfer lag), offsets {30,90,130,190} keep min positive gap 40 >
# the default-tier budget sum 19.
D04_PHASES = ((0, 30), (30, 205), (205, 235))
D04_CATALOG_WINDOW = (30, 90)
D04_USER_WINDOW = (130, 190)

# D07/D13 (env OUTER whole window + net CRD INNER late sub-window,
# partial_overlap as recommended, recovery reverse -- the legacy F2_only tail
# segment is replaced by the OUTER leg's post-inner tail, explicitly
# declared): OUTER settles ~45 (env rollout band 15), F1_only steady 45->80 =
# 35 samples; INNER CRD is seconds-level, overlap steady ~82->115 = 33
# samples; recovery: CRD delete @120 SAME-END (seconds) then env unset serial

# slides 80/115 -> 85/120 (span 35 preserved, overlap 88->120 = 32 samples)
# so every positive corner gap is >= 35 > the app_env-family budget sum 17.
COMBO2_ENVNET_PHASES = ((0, 30), (30, 140), (140, 170))
COMBO2_ENVNET_OUTER_WINDOW = (30, 120)
COMBO2_ENVNET_INNER_WINDOW = (85, 120)

# D08/D29 (net CRD OUTER whole window + pod-failure INNER sub-window,
# nested_pod_inner): netem settles ~32 (CRD seconds); pod INNER rides the
# below_kill_band v2 profile (50,110) -- span 60 with the measured lag head
# absorbed by the pod carrier settle; EFFECTIVE pod-down [~84,~121] keeps the
# dual-active/overlap segment [~84,~121] = ~37 samples >= the C1 floor 30
# (net exposure [~32,131]); pod recover @110 restore+Ready ~123 inside during
# [30,135); the net CRD delete @130 ends ~131 inside during.  EXEC-POD-XFER

# 35 -> 60 span (duration 38 -> 90) so the EFFECTIVE overlap stays >= 30 --
# the raw-window overlap arithmetic died with the lag; offsets {30,50,110,130}
# keep min positive gap 20 > the default budget sum, no same-end tie needed.
COMBO2_NETPOD_PHASES = ((0, 30), (30, 135), (135, 165))
COMBO2_NETPOD_NET_WINDOW = (30, 130)    # duration_s=100 covers the 100 s span (MINOR-1)
COMBO2_NETPOD_POD_WINDOW = (50, 110)

# D11 (two env legs, simultaneous = common target window, pre-inject-both):
# runtime F2 injects FIRST (30,110) and inventory F1 right behind (35,110) --
# the legacy order L15271-15273; both rollout heads (15 s each; the
# deployments differ so they may roll in parallel, but the budget is declared
# serial-conservative, settled ~50) are absorbed before the steady
# dual-active segment ~50->110 = 60 samples; recovery F1 then F2 (declared
# order at the shared window end, = the legacy F1-first recovery), env unset
# rollouts ~15 s each ending <=~140 inside during [30,140); the f1win subset
# f2win nesting is EXPLICIT (CROSS-CUTTING SS8-4), never a silent side effect.


# spacing applied to the triple set collection explicitly deferred; same reslice
# as T05/T07/T08 so all four C1-held families share one grid.

# inject-side head gap cannot absorb the FIRST env leg's declared footprint
# (rollout band 15 + write 2 = 17 s; measured catalog-env apply ~12 s inside
# the action) -- the scheduler is serial, so F1 @35 fired only after F2's
# rollout completed and missed its 0.8 s start deadline. The inventory head
# moves 35 -> 52 (F2's worst completion 30+17=47 < 52.8 with 5.8 s slack);
# the common-target dual-active segment becomes (52,110) = 58 samples,

# declaration, now under the runner's env-footprint carry (band+write = 17):
# F2's slot 110+17=127, its own footprint ends <=144, so during extends
# 140 -> 155 (11 s slack past the declared worst completion).
D11_PHASES = ((0, 30), (30, 155), (155, 185))
D11_RUNTIME_WINDOW = (30, 110)
D11_INV_WINDOW = (52, 110)

# D16 (net CRD OUTER + catalog pod INNER, nested_pod_inner -- the batch-1 D05
# shape with the roles inverted): netem (30,135) settles ~32; pod INNER rides
# the below_kill_band v2 profile (55,115) -- span 60, lag head absorbed by the
# pod carrier settle; EFFECTIVE pod-down [~89,~126] sits inside the net steady
# segment [~32,135] (co-active ~37 samples >= floor 30); pod recover @115
# restore+Ready ~128 completes strictly BEFORE the net CRD delete @135 (the T4
# cfg/pod strict-ordering knowledge mirrored from D05); CRD deletion ends ~136

# span, duration 38 -> 90; offsets {30,55,115,135} keep min positive gap 20 >
# the default budget sum (the collection 57-head grid discipline rides the 5 s
# grid).
D16_PHASES = ((0, 30), (30, 140), (140, 170))
D16_NET_WINDOW = (30, 135)              # duration_s=105 covers the 105 s span (MINOR-1)
D16_POD_WINDOW = (55, 115)

# D17 (inv env OUTER whole window + checkout pod INNER sub-window): inv
# (30,135) settles ~45, checkout pod INNER rides the below_kill_band v2
# profile (55,115) -- span 60, lag head absorbed by the pod carrier settle;
# EFFECTIVE pod-down [~89,~126] keeps the overlap segment with the inv steady
# exposure [45,135] at ~37 samples >= the C1 floor 30; recovery reverse: pod
# CRD delete @115 (restore+Ready ~128, inside during) then inv unset @135
# (rollout <=~150) inside during [30,155).  The legacy dual17 order (checkout
# first, inv after the pod recover) is reshaped to env-first per CROSS-
# CUTTING SS8-2 -- the blueprint leaves both orders open (装配记录定并写明).

# inv window 120 -> 135 (env legs carry no CRD duration; the extension keeps
# the EFFECTIVE overlap >= 30 under the lag); during 140 -> 155; offsets
# {30,60,115,135} keep min positive gap 20 > the default budget sum.
D17_PHASES = ((0, 30), (30, 155), (155, 185))
D17_INV_WINDOW = (30, 135)
D17_CHECKOUT_WINDOW = (55, 115)


def _b1_execution_prerequisite() -> Precondition:
    """Batch-2 analog of the batch-1 not-runnable-yet premise (no gw leg)."""
    return Precondition(
        statement="the B1-family combination live-execution wiring is not landed (no new-chain combination "
                  "driver exists: the net/pod/db/env live-action wiring, the db-lock driver face and the "
                  "combination smoke are all pending): this combination has NO runnable new-chain form yet "
                  "-- blueprint-first assembly is not executability; every live use additionally owes the "
                  'collection combination smoke (组合实现不等于组合采集资格)',
        mechanism_anchor="SPECS-2-B1-NET-POD-DB-ENV.md header (execution_status=DESIGN_ONLY_NOT_COLLECTED); "
                         "combo-blueprint CROSS-CUTTING.md §8",
        owner='integration/driver layer (collection); assembly declares, never provisions',
        status="NOT_FROZEN prerequisite")


def _db_net_order_precondition(scenario_id: str) -> Precondition:
    return Precondition(
        statement="NET-first injection with confirmed-release recovery (B1-06 SS2 D10/D02): the net CRD leg "
                  "injects FIRST (AllInjected is seconds-level and clean, old is_db_lock_combo "
                  "L15161-15183 NET-first) and the db lock enters right behind it; recovery is "
                  "lock-release-first -- the "
                  "driver must confirm the lock release (confirm_release readback) BEFORE deleting the net "
                  "CRD, which is why the net window end sits one TAIL budget past the lock window end; a CRD "
                  "delete racing the release confirmation voids the reverse-nested recovery evidence",
        mechanism_anchor="old is_db_lock_combo NET-first L15161-15183; S06 recover TAIL precondition "
                         "(two confirms <=15 s each + statements + margin); SPECS-2 " + scenario_id + "(d)",
        owner='driver/runtime-binding layer (collection): injection/recovery sequencing + release confirmation; '
              "assembly declares, never sequences",
        status="NOT_FROZEN ordering premise")


def _d04_stagger_strict_order_precondition() -> Precondition:
    return Precondition(
        statement="restart-attribution strict sequence (restated for the new chain, B1-06 SS2 D04): catalog "
                  "F1 recovers, the pod's restart+2 registration must COMPLETE, then the reference snapshot, "
                  "and only then the user F2 injection (poll_carrier=False) -- a user inject before the "
                  "catalog restart is attributed would spill restart evidence across windows and void the "
                  "per-window restart_delta signatures; the declared 90->130 gap encodes restore/registration ~35 s + "
                  "registration/snapshot margin",
        mechanism_anchor="old _stagger_mid_action L15522-15555 (docstring L15523-15526 spillover lesson); "
                         "SPECS-2 D04(d)",
        owner='driver/runtime-binding layer (collection): mid-action sequencing; assembly declares, never sequences',
        status="NOT_FROZEN ordering premise; violated order invalidates the attempt, not just the GT")


def _d11_pre_inject_both_precondition() -> Precondition:
    return Precondition(
        statement="double env-rollout sequencing (legacy SIMULTANEOUS pre-inject-both restated): the runtime "
                  "F2 env rollout starts FIRST and the inventory F1 rollout immediately behind it (both heads "
                  "absorbed before the steady dual-active segment; legacy DOUBLE-ROLLOUT ~340 s serial setup "
                  "lesson L15267-15268 -- the two deployments differ so they may roll in parallel, but the "
                  "budget is declared serial-conservative); recovery runs F1 (inv unset) then F2 (runtime "
                  "unset) exactly as declared; the f1win subset f2win nesting is an EXPLICIT declaration "
                  "(CROSS-CUTTING SS8-4), never a silent side effect of apply/recover ordering",
        mechanism_anchor="old injection order L15271-15273 (runtime first, inv second); B1-06 SS2 D11; "
                         "CROSS-CUTTING §8-2/§8-4",
        owner='driver/runtime-binding layer (collection): injection/recovery sequencing; assembly declares, '
              "never sequences",
        status="NOT_FROZEN ordering premise")


# --------------------------------------------------------------------------- #
# COMBO-ASM-3 (seventh batch): SPECS-3 CPU-mixed combinations
# (D03/D09/D14/D15/D19/D21/D26/D28/D30/D31 -- service-CPU legs mixed with
# host-CPU / net / pod / env legs; NO gateway cfg leg anywhere in the family).
# Shared budget atoms: C1 SS2.5 floor 30 samples at 1 rps; CPU StressChaos
# injection is seconds-level with a conservatively declared 15 s saturation
# ramp head (batch-1 D06/D22 arithmetic); net CRD legs seconds-level
# (settled ~2 s); pod-failure rides the S03 below_kill_band profile (35 s
# window v2: span 60 x CRD 90, lag head 35 absorbed by the pod carrier settle)
# with the restore+Ready ~11-13 s confirmed inside the recover action; the env
# rollout head band stays 15 s (S08 family); sasrec legs carry the no-restart
# pin semantics (9.2 GB pickle reload) and workers=8 quota coupling from S27.
# --------------------------------------------------------------------------- #

# D03 (host + catalog cpu, nested -- the geometry restated per the new timing
# contract): the catalog cpu leg is OUTER (30,115) and injects FIRST on an
# idle VM (legacy catalog-first knowledge: reverse order forces the catalog
# AllInjected poll through a chaos-controller starved by the host saturation,
# ~30 s flaky); the host leg rides INNER (60,105) after catalog settles ~45;
# catalog settles ~45 (30+15 ramp), host settles ~75 (60+15) -> F2_only
# steady 45->75 = 30 and overlap steady 75->105 = 30 both hit the C1 floor;
# recovery is reverse-nested exactly like the old sequence (host CRD delete
# @105 first, catalog @115 after); CRD deletions end <=120 inside during
# [30,120).  The blueprint's legacy parenthetical "host 窗⊃catalog 窗" reads
# inverted against its own inject/recover sequence (catalog injects first AND
# recovers last => catalog is the OUTER window); the assembly binds to the
# injection-order knowledge, which the blueprint marks as MUST-preserve.
D03_PHASES = ((0, 30), (30, 120), (120, 150))
D03_CATALOG_OUTER_WINDOW = (30, 115)
D03_HOST_INNER_WINDOW = (60, 105)

# D09 (sasrec cpu + gw delay, partial_overlap as recommended): the sasrec leg
# rides the whole window (30,120) pre-injected (CRD seconds-level, settles
# ~45 with the conservative ramp head) and the gw netem rides the late INNER
# sub-window (80,115; settles ~82); F1_only steady 45->80 = 35 and overlap
# steady ~82->115 = 33 both hit the C1 floor; recovery reverse: gw CRD delete
# @115 (seconds-level) then sasrec CRD delete @120, ending <=125 inside
# during [30,125).
D09_PHASES = ((0, 30), (30, 125), (125, 155))
D09_SASREC_WINDOW = (30, 120)
D09_GW_INNER_WINDOW = (80, 115)

# D14/D26/D28 (net + cpu, common target window carrying the legacy
# simultaneous semantics): the CPU leg enters first on the stable pod
# (30,110; settles ~45 with the ramp head) and the net CRD right behind it
# (35,105; settles ~37); the dual-active analysis window 45->105 = 60 samples
# clears the C1 floor; recovery reverse: net CRD delete @105 then cpu CRD
# delete @110, ending <=115 inside during [30,115).
# collection: spacing 3->5 s, same ruling as the DBNET family -- BOTH legs here are
# chaos-mesh CRDs (measured inject anchors 3.109-3.125 s), so the old 3 s
# head scheduled the net inject while the cpu apply was still in flight.
# Budget 4.0/0.8 (D09a2 field-proven, 4.0+0.8=4.8<5); window end 105 and the
# reverse recover gap 5 are untouched.
COMBO3_NETCPU_PHASES = ((0, 30), (30, 115), (115, 145))
COMBO3_NETCPU_CPU_WINDOW = (30, 110)
COMBO3_NETCPU_NET_WINDOW = (35, 105)

# D15 (catlat env + pricing cpu, common target window per the blueprint's
# 公共目标窗 recommendation for the new pair): catlat F1 rides the whole
# window (30,120) injected first to absorb the env rollout (~13-15 s ->
# settled ~45), pricing cpu F2 enters right behind on the stable pod (50,120;
# settles ~65); the steady dual-active segment 65->120 = 55 samples clears
# the C1 floor; recovery SAME-END @120: pricing CRD delete (seconds-level)
# then catlat env unset serial (rollout <= ~138) inside during [30,140).

# for the app_env-family budget (15+2 < 20) and moves the recover pair

# claim is released (T01 reslices separately).
D15_PHASES = ((0, 30), (30, 140), (140, 170))
D15_CATLAT_WINDOW = (30, 120)
D15_PRICING_CPU_WINDOW = (50, 120)

# D19/D21/D30 (pod-failure sub-window + cpu whole window, partial_overlap
# carrying the legacy shapes): the cpu leg rides the whole window (30,135;
# settles ~45 with the conservative ramp head) and the pod-failure leg rides
# the mid INNER sub-window (55,115) on the below_kill_band v2 profile -- span
# 60 with the measured lag head absorbed by the pod carrier settle (EFFECTIVE
# pod-down [~89,~126]); the triple co-active segment with the cpu steady
# exposure [45,135] is [~89,~126] = ~37 samples >= the C1 floor 30; pod
# recover @115 restore+Ready ~128 inside during [30,140); the cpu CRD delete

# 35 -> 60 span (duration 38 -> 90); cpu window 110 -> 135 (duration 80 ->
# 105) so the EFFECTIVE co-active stays >= 30 under the lag; during
# 125 -> 140; offsets {30,55,115,135} keep min positive gap 20 > the default
# budget sum.
COMBO3_CPUPOD_PHASES = ((0, 30), (30, 140), (140, 170))
COMBO3_CPUPOD_CPU_WINDOW = (30, 135)
COMBO3_CPUPOD_POD_WINDOW = (55, 115)

# D31 (dual service-CPU legs, common target window per the blueprint's
# simultaneous-class recommendation): catalog F1 (30,110; workers=1, settles
# ~45) injects first and review-query F2 (35,105; settles ~50) right behind
# -- injection F1->F2 and recovery reverse (F2 first) exactly as the
# blueprint's 注入序 F1→F2/恢复 reverse; the steady dual-active segment
# 50->105 = 55 samples clears the C1 floor; CRD deletions @105/@110 end
# <=111 inside during [30,115); the near-equal windows' containment side
# effect (F2 ⊂ F1) is declared, never incidental (CROSS-CUTTING SS8-4).

# the collection chaos-mesh spacing applied to the remaining tight dual-CPU set.
D31_PHASES = ((0, 30), (30, 115), (115, 145))
D31_CATALOG_WINDOW = (30, 110)
D31_RQ_WINDOW = (35, 105)


def _cpu_mixed_execution_prerequisite() -> Precondition:
    """Batch-3 analog of the batch-2 not-runnable-yet premise (CPU faces)."""
    return Precondition(
        statement="the CPU-mixed combination live-execution wiring is not landed (no new-chain combination "
                  "driver exists: the StressChaos resource-leg wiring, the host stressor-carrier "
                  "provisioning/cleanup face for D03 and the combination smoke are all pending): this "
                  "combination has NO runnable new-chain form yet -- blueprint-first assembly is not "
                  'executability; every live use additionally owes the collection combination smoke '
                  "(组合实现不等于组合采集资格)",
        mechanism_anchor="SPECS-3-CPU-MIXED.md header (execution_status=DESIGN_ONLY_NOT_COLLECTED); "
                         "combo-blueprint CROSS-CUTTING.md §8",
        owner='integration/driver layer (collection); assembly declares, never provisions',
        status="NOT_FROZEN prerequisite")


def _d03_catalog_first_precondition() -> Precondition:
    return Precondition(
        statement="catalog-first injection order is a runtime MUST on an idle VM (legacy host_cpu_x_svccpu "
                  "knowledge): the catalog StressChaos must reach AllInjected BEFORE the host StressChaos "
                  "CRD applies -- in the reverse order the catalog AllInjected poll has to traverse a "
                  "chaos-controller starved by the host saturation (observed ~30 s flaky); the declared "
                  "window order F2 catalog (30,115) -> F1 host (60,105) encodes this and the driver must "
                  "inject in exactly that order; recovery is reverse (host CRD delete first, catalog "
                  "after), mirroring the old recover sequence L16142-16143",
        mechanism_anchor="old inject_stress_catalog L15145 (catalog-first on idle VM) + recover "
                         "L16142-L16143; B1-06 §2 D03; SPECS-3 D03(d) 注入顺序知识必须保留; "
                         "CROSS-CUTTING §8-2",
        owner='driver/runtime-binding layer (collection): injection sequencing; assembly declares, never '
              "sequences",
        status="NOT_FROZEN ordering premise; violated order invalidates the attempt, not just the GT")


def _dual_cpu_throttle_attribution_precondition() -> Precondition:
    return Precondition(
        statement="dual-CPU-leg attribution premise (OPEN-Q3): the old multi_leg_retarget_gate judged "
                  "svccpu legs by per-pod cfs_throttle > THROTTLE_EPS(0.003) as the HARD separable axis "
                  "(carrier ratio is never the unique criterion for a combination CPU leg, old constraint "
                  "E L8430-8431) -- the new quality layer has NO per-pod cfs_throttle arm yet, so this "
                  "combination's per-leg attribution is not separable from the two self-carrier ratio "
                  "signals alone until the throttle evidence arm lands or an argued alternative "
                  "judgement is adjudicated; the catalog throttle readout must exclude catalog-gw pods "
                  "(old regex L13498)",
        mechanism_anchor="ledger B3 01 §3.2-3 (per-pod throttle hard-arm gap, OPEN-Q3) + constraint E "
                         "L8430-8431 (multi_leg_retarget_gate L8422-8496); SPECS-3 D31(e)",
        owner='quality/observation-design layer (collection/collection): throttle evidence arm or adjudicated '
              "alternative; assembly declares, never implements",
        status="NOT_FROZEN attribution premise; violated premise leaves the dual-CPU attribution not "
               "separable, never silently passed")


# --------------------------------------------------------------------------- #
# COMBO-ASM-4 (eighth/closing batch): SPECS-4 combinations -- eight duals
# (D18/D20/D23/D24/D25/D27/D32/D33: CPU x CPU or CPU x pod) plus four triples
# (T05/T06/T07/T08; T01 is batch-1); NO gateway cfg leg anywhere in the family,
# so the 25 s gateway switch band never enters this batch's arithmetic either.
# Shared budget atoms (batch-3 same family): C1 SS2.5 floor 30 samples at 1 rps;
# CPU StressChaos seconds-level injection with the conservative 15 s saturation
# ramp head; net CRD legs seconds-level (settled ~2 s); pod-failure rides the
# std below_kill_band v2 profile (span 60 x CRD 90, lag head 35 absorbed by
# the pod carrier settle) with the restore+Ready ~11-13 s
# lag evidence-only; cart/catalog legs stay workers=1 (T05/T08 calibration),
# sasrec stays workers=8 (quota/topology coupled, no-restart pin semantics).
# --------------------------------------------------------------------------- #

# D18/D20/D23/D27 (dual-CPU common target window carrying the legacy
# simultaneous semantics, the D31 shape): the first leg (30,110) injects first
# on its stable pod (CRD seconds-level + ramp head -> settled ~45,
# duration_s=80 covering the span) and the second leg right behind it (35,105;
# settled ~50, duration_s=70); the steady dual-active segment 50->105 = 55
# samples clears the C1 floor; recovery reverse (second CRD delete @105 first,
# first @110, seconds-level, <=111 inside during [30,115)); the near-equal
# windows' containment side effect (F2 within F1) is declared, never

# is widened to 5 s (the collection chaos-mesh spacing) so the 4.0+0.8 dispatch
# budget clears the O-D3 serial gate (4.8 < 5) against real CRD anchors.
COMBO4_DUALCPU_PHASES = ((0, 30), (30, 115), (115, 145))
COMBO4_DUALCPU_FIRST_WINDOW = (30, 110)    # duration_s=80 covers the 80 s span
COMBO4_DUALCPU_SECOND_WINDOW = (35, 105)   # duration_s=70 covers the 70 s span

# D24/D25/D32/D33 (CPU whole window + pod-failure mid sub-window,
# partial_overlap carrying the old T05/T08 implementation shapes): the cpu leg
# rides the whole window (30,135; settled ~45, duration_s=105) and the pod leg
# rides the mid sub-window (55,115) on the below_kill_band v2 profile -- span
# 60 with the measured lag head absorbed by the pod carrier settle (EFFECTIVE
# pod-down [~89,~126], fully inside the cpu steady segment; co-active ~37
# samples >= the C1 floor 30); pod recover @115 restore+Ready ~128 inside
# during [30,140) (restart_delta window attribution does not spill into the
# cpu window); the cpu CRD delete @135 ends <=136 inside during.

# cpu window 110 -> 135 (duration 80 -> 105); during 125 -> 140; offsets
# {30,55,115,135} keep min positive gap 20 > the default budget sum.
COMBO4_CPUPOD_PHASES = ((0, 30), (30, 140), (140, 170))
COMBO4_CPUPOD_CPU_WINDOW = (30, 135)       # duration_s=105 covers the 105 s span
COMBO4_CPUPOD_POD_WINDOW = (55, 115)       # below_kill_band v2 (span 60, CRD 90)

# D25/D30-only short overlay: preserve the 105 s CPU / 60 s Pod windows and
# order, while moving CPU cleanup six seconds later. Pass action_budget=11 and
# lateness_budget=2 explicitly. Shared COMBO constants and long300 stay intact.
D25_D30_SHORT_PROFILE_SCENARIOS = frozenset(("D25", "D30"))
D25_D30_SHORT_PHASES = ((0, 30), (30, 155), (155, 185))
D25_D30_SHORT_WINDOWS = ((36, 141), (55, 115))
D25_D30_SHORT_TIMING_NOTE = (
    "Short-only engineering overlay: F1=[36,141], F2=[55,115]; "
    "partial_overlap and reverse recovery retained; canonical long300 unchanged."
)

# T05 (checkout pod + cart cpu + pricing cpu, legacy partial_overlap default --
# the old triple shape: both CPU legs ride the whole window pre-injected as
# OUTER, the pod-failure leg rides the mid sub-window): cart F2 (30,135, W=1,
# settled ~45) injects first, pricing F3 (35,130, W=2, settled ~50) right
# behind, and the checkout pod F1 (55,115) enters mid on the below_kill_band
# v2 profile -- span 60 with the measured lag head absorbed by the pod
# carrier settle; the EFFECTIVE pod-down segment [~89,~126] is the triple
# co-active segment (both cpu legs steady from ~45/~50 through 135/130):
# [~89,~126] = ~37 samples >= the C1 floor 30; recovery reverse (pod CRD
# @115 restore+Ready ~128 -> pricing @130 -> cart @135, all inside during
# end 140).


# spacing applied to the triple set collection explicitly deferred.  EXEC-POD-XFER

# (80 -> 105), pricing 105 -> 130 (72 -> 95), during 125 -> 140; the "exact
# D23 pair geometry" equality claim is RELEASED (D23 keeps 30/110+35/105;
# precedent: collection released the D15/T01 sub-geometry claim); offsets
# {30,35,55,115,130,135} keep min positive gap 5 > the r48_fixed budget sum
# 4.8.
T05_PHASES = ((0, 30), (30, 140), (140, 170))
T05_CHECKOUT_POD_WINDOW = (55, 115)
T05_CART_CPU_WINDOW = (30, 135)
T05_PRICING_CPU_WINDOW = (35, 130)

# T06 (backend cpu + sasrec cpu + gw net delay, legacy simultaneous default --
# old injection order cpu -> cpu -> netem, recovery reverse L15364-15365/
# L16118-16126): backend F1 (30,110, W=2) then sasrec F2 (35,105, W=8) -- the
# D27 pair geometry -- then the netem F3 (40,100) as the last-injected inner
# leg (CRD seconds-level, settled ~43; duration_s=60 covers the span); triple
# co-active 50->100 = 50 samples >= the C1 floor; recovery exactly reverse
# (net CRD @100 -> sasrec @105 -> backend @110, <=111 inside during [30,115)).

# O-D3) and the netem tail pulled to 100 so every positive corner gap is >= 5.
T06_PHASES = ((0, 30), (30, 115), (115, 145))
T06_BACKEND_CPU_WINDOW = (30, 110)
T06_SASREC_CPU_WINDOW = (35, 105)
T06_GW_NET_WINDOW = (40, 100)

# T07 (rec-agent net delay + sasrec cpu + catalog pod, legacy partial_overlap
# default -- old shape: F1/F2 whole-window pre-injected + F3 pod mid INNER).

# took 4.031 s and 4.015 s against the 4 s action cap. Keep inject/recover order,
# mechanisms, intensity, entities, roles and routes; declare a +9 s F2 outer
# exposure and shift F1 while preserving its 95 s span. F2 sasrec CPU [30,144],
# duration 114 (was [30,135]/105); F1 rec-agent netem [42,137], duration 95;
# F3 catalog pod [55,115], 60 s window and CRD cap 90 unchanged. Offsets
# {30,42,55,115,137,144} give min gap 7; T07 tier 5.0+0.8=5.8. The declared
# F3 recovery ceiling is 5+POD_FAILURE_RESTORE_BOUND_S(15)+0.8 lateness = 20.8,
# so recover@115 completes by 135.8 before F1 recover@137 (1.2 s slack); F1's
# 5.8 s recovery bound fits before F2 recover@144 (1.2 s slack); F2 completes
# by149.8 inside short during end160. The below_kill_band v2 transfer remains
# effective [~89,~126], so triple co-active is min(126,137,144)-89=37 samples
# >= C1's 30. Long300 still forces 300/300/300.
# S21 first-use tc smoke remains a separate prerequisite; the old net-delay
# judgement shape is design knowledge, never an observation.
T07_PHASES = ((0, 30), (30, 160), (160, 190))
T07_RECAGENT_NET_WINDOW = (42, 137)
T07_SASREC_CPU_WINDOW = (30, 144)
T07_CATALOG_POD_WINDOW = (55, 115)

# T08 (order pod + rq cpu + catalog cpu, legacy partial_overlap default AND
# the label must be EXPLICITLY declared: the old design-table simultaneous vs
# implemented partial_overlap divergence precedent L13389-13392 -- a podfail
# leg caps during at <=60 s, which cannot carry a CPU throttle statistics
# window, so neither the old label nor the old implementation is inherited
# silently; C1 observation re-verification required, B1-06 SS3 T08): catalog
# F3 (30,135, W=1) first then rq F2 (35,130, W=2) and the order pod F1
# (55,115) mid on the below_kill_band v2 profile -- span 60 with the measured
# lag head absorbed by the pod carrier settle; EFFECTIVE pod-down [~89,~126]
# is the triple co-active segment: ~37 samples >= the C1 floor 30; recovery
# reverse (pod @115 restore+Ready ~128 -> rq @130 -> catalog @135, all inside
# during end 140); fan-in knowledge: order/review-query both enrich through
# catalog, the shared downstream sags together (old combo key L13378).



# rq 105 -> 130 (72 -> 95), during 125 -> 140; the D31 pair-geometry equality
# claim is RELEASED (D31 keeps 30/110+35/105; collection precedent); offsets
# {30,35,55,115,130,135} keep min positive gap 5 > 4.8.
T08_PHASES = ((0, 30), (30, 140), (140, 170))
T08_ORDER_POD_WINDOW = (55, 115)
T08_RQ_CPU_WINDOW = (35, 130)
T08_CATALOG_CPU_WINDOW = (30, 135)


def _cpu_cpu_execution_prerequisite() -> Precondition:
    """Batch-4 analog of the batch-3 not-runnable-yet premise (SPECS-4 family)."""
    return Precondition(
        statement="the SPECS-4 combination live-execution wiring is not landed (no new-chain combination "
                  "driver exists: the multi-leg StressChaos/PodChaos/NetworkChaos sequencing face, the "
                  "rec-agent netem first-use wiring for T07 and the combination smoke are all pending): this "
                  "combination has NO runnable new-chain form yet -- blueprint-first assembly is not "
                  'executability; every live use additionally owes the collection combination smoke '
                  "(组合实现不等于组合采集资格)",
        mechanism_anchor="SPECS-4-CPU-CPU-AND-TRIPLES.md header (execution_status=DESIGN_ONLY_NOT_COLLECTED); "
                         "combo-blueprint CROSS-CUTTING.md §8",
        owner='integration/driver layer (collection); assembly declares, never provisions',
        status="NOT_FROZEN prerequisite")


def _pricing_scale_up_precondition() -> Precondition:
    """ASM-3 review NIT-B: the weaker pricing premise made explicit.

    CROSS-CUTTING SS3 constraint 2 distinguishes the pricing redirect premise
    (14 pricing-via-gw groups, carried per-spec) from the WIDER pricing
    scale-up premise (every group with a pricing:5014 stream, incl. the
    pricing-direct shapes).  Until COMBO-ASM-4 the latter lived only in
    blueprint text and provenance strings; this Precondition gives it a
    declared runtime face on the new pricing-direct groups (D23/D25/T05 third
    stream).  The batch-3 groups D03/D15 keep their accepted spec bytes; the
    module-level PRICING_STREAM_* constants below are their checklist face.
    """
    return Precondition(
        statement="pricing scale-up premise (CROSS-CUTTING §3 constraint 2, ASM-3 review NIT-B): a combination "
                  "carrying a pricing:5014 stream needs the pricing Deployment present and reachable through "
                  "the panel/proxy route for the whole attempt; the pricing-DIRECT stream shapes of this batch "
                  "do NOT need the catalog-gw redirect -- only this weaker presence premise; fulfilment and "
                  'evidence belong to the collection driver checklist',
        mechanism_anchor="combo-blueprint CROSS-CUTTING.md §3 constraint 2 (pricing scale-up-only groups: "
                         'D03/D15/D23/D25/T05); k8s/services/pricing.yaml default state',
        owner='driver/runtime-binding layer (collection): deployment presence/reachability check; assembly '
              "declares, never provisions",
        status="NOT_FROZEN premise; a missing pricing carrier yields no stream samples, never a silent pass")


def _d20_deepseek_secret_precondition() -> Precondition:
    return Precondition(
        statement="deepseek-env secret premise (SPECS-4 D20(e) checklist item): the rec-agent deployment reads "
                  "its DeepSeek credentials from the deepseek-env secret; any rec-agent env-var/rollout action "
                  "in this attempt's pipeline must keep the secret reference intact -- the old restore script "
                  "once dropped the secret and every recommend call returned 401 (the recommend POST is "
                  "business evidence only, but a 401 wave still contaminates the ratio carriers); the driver "
                  "must verify the secret reference before and after the attempt",
        mechanism_anchor="old restore script dropped the deepseek-env secret -> recommend 全 401 (design_note "
                         "L13468-13469); SPECS-4 D20(e) 装配检查单",
        owner='driver/runtime-binding layer (collection): secret-reference verification around the attempt; '
              "assembly declares, never provisions",
        status="NOT_FROZEN premise; a DeepSeek outage never voids the case itself (recommend POST is "
               "business evidence only, L13412)")


def _triple_oe5_precondition(scenario_id: str, pair_ids: str) -> Precondition:
    """O-E5 eval-side requirement for every three-leg combo (design text only)."""
    return Precondition(
        statement="three-leg adjudication premise (O-E5): the amplified/amplifier split of this triple is NOT "
                  "decided at assembly -- the eval-side requirement is the per-amplifier sub-pair reduction "
                  "(逐 amplifier 子对降维) whose sub-cases are the already-designed double-root combos "
                  "(" + pair_ids + ", CROSS-CUTTING §7); this declaration is design text, never an "
                  "observation; the C1 §0 B observation gate plus the O-E5 ruling own the final three-leg "
                  "judgement",
        mechanism_anchor="combo-blueprint CROSS-CUTTING.md §7 (三腿组子对闭合) + §3 三腿组第三腿说明 (O-E5); "
                         "SPECS-4 " + scenario_id + "(e)",
        owner='eval-design/O-E5 ruling layer (collection/collection); assembly declares, never adjudicates',
        status="NOT_FROZEN eval-side requirement (design text, not observation)")



# every combo carrying a pricing:5014 stream, split by which premise applies.
# The 14 redirect groups observe the gw->catalog edge and additionally carry
# the redirect premise per-spec (S01 family / _pricing_gw_path_precondition);
# the 5 scale-up-only groups carry pricing-direct streams and need only the
# weaker presence premise (D03/D15 as accepted batch-3 bytes keep the premise
# in provenance text -- these constants are the machine-readable checklist).
PRICING_STREAM_GROUPS_REDIRECT = ("D01", "D02", "D06", "D07", "D08", "D09", "D10", "D12",
                                  "D13", "D14", "D22", "D26", "T01", "T06")
PRICING_STREAM_GROUPS_SCALE_UP_ONLY = ("D03", "D15", "D23", "D25", "T05")


COMBO_SPECS: dict[str, ComboSpec] = {
    "D01": ComboSpec(
        scenario_id="D01", purpose="pilot", scope="m1_main",
        rule_version_base="d01-combo-pilot", random_seed=11,
        phases=COMBO_NESTED_PHASES,
        legs=(_combo_leg("S25", COMBO_OUTER_WINDOW),
              _combo_leg("S23", COMBO_GW_INNER_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d01-main-inventory-items", service="inventory:5013",
                              path_prefix="/api/inventory/", carrier="combo-main-panel-inventory-direct",
                              direct_api=True),
            CarrierStreamSpec(stream_id="d01-bypass-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-bypass-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service")),
        telemetry=_std_telemetry("inventory-pod-logs"),
        signals=(_combo_signal("S25"), _combo_signal("S23")),
        injection_signal_kinds=("metric_threshold", "config_state_marker"),
        injection_signals=(None, _combo_injection_signal("S23")),
        timing=ComboTimingProfile(
            name="d01_nested_retry_subwindow", geometry="nested",
            inject_order=("F1", "F2"),
            
            # event sort breaks same-offset ties by contract.faults order), so
            # the declared serial recovery is F1 (inv env unset) -> F2 (gw
            # restore); the stable-sorted-by-end law forces exactly this order.
            recover_order=("F1", "F2"),
            decision="SPECS-1 D01(d) 选项B 采纳（蓝图建议：retry 腿可观测性风险使 overlap 段签名对 pilot 材料"
                     "更有信息量）：inv OUTER (30,122) 慢 rollout 腿先注（旧 T1 inv 先→CFG 后 L15315-15321；env "
                     "rollout ~13-15s→settled ~45），retry gw INNER (62,122) 后注（切换带 25s=20 rollout+5 buffer，"
                     "GATEWAY_ROLLOUT_PROFILES 同源→settled ~87；注入侧＝重切前原值，时序定义 2026-09-21T04:04"
                     "+08:00 时序配置 裁定二不变项）；F1_only 稳态 45→87=42 与 overlap 稳态 87→122=35 均达 C1 §2.5 "
                     '地板（§8-6 子窗预算；旧 F2_OFFSET=14/F2_DURATION=31 不照抄）；collection 同端恢复重切（同裁定二）：'
                     'gw recover 105→110 与 inv env unset 同端@122（collection；恢复侧 stagger 105/110→110/110→122/122；重切前 5s 恢复'
                     '正间隙使一切诚实预算对（action≥typical 带 21s）过不了 O-D3 最小正间隙断言，collection BAND-RULING-'
                     "INPUT §四算术）；串行恢复按声明序 F1 inv unset（rollout ≤~15s→settled ≤~137）→F2 gw restore"
                     "（冻结带裁定一 typical 14-21s/声明带 25s/尾注记 ≤28s：完成 typical ≤~146、声明带 ≤~150、尾注记"
                     ' ≤~153——typical 完成落 recovery_transition 标记区＝重切声明性后果，runner 无门、collection 须实测 '
                     'rollout_band_s 回填）；collection 重切后动作偏移 30/62/122/122、最小正间隙 32s＞GW 预算和 30s（28+2）（as-'
                     'declared 构造面通过；collection 后预算和 30＜32，live 张力解除；注入侧 50→62 平移吸收 D01 实测 GW 锚 23.219s（collection 张力登记原文见 RESLICE-RULING.md），见 RESLICE-RULING.md）；'
                     "科学注记：恢复侧 stagger 改同端、注入侧不变——入 C1 pending（staggered 与 independent_"
                     "parallel 症状定义对应）显式登记不静默；timing NOT_FROZEN"),
        
        # section 6, ruling 2): the GW leg recover window moved 105 -> 110 to
        
        # declaration (O-D3/O-D7 corrected form; D11 precedent) registers the
        # serial recovery budget. The budget arithmetic is the timing.decision
        # text above (values same-source).
        same_offset_budgets={frozenset(("F1", "F2")):
            'D01 recover-recover 同端对 offset 122 显式预算声明（collection 110→122）（权威＝时序定义 2026-09-21T04:04+08:00 时序配置 '
            'configs/collection/scenarios.json: assembly definition'
            'combo-driver-design/timing policy O-D3/O-D7 修正后批准 2026-09-18＋D11/collection 同端恢复对；'
            "预算算术与本 spec timing.decision 同源）：恢复按声明序（＝腿序）F1（inventory env unset，rollout "
            "≤~15s→settled ≤~125）→F2（gateway restore 经滚切 settle，冻结带 typical 14-21s/声明带 25s/尾注记 "
            "≤28s：完成 typical ≤~158、声明带 ≤~162、尾注记 ≤~165——typical 完成落 recovery_transition 标记区，"
            '为重切声明性后果非门违例，collection 须实测 rollout_band_s 回填）；跨 deployment 恢复理论上可并行但按'
            "串行保守预算申报；其余动作对的最小正间隙断言照常生效（重切后最小正间隙 32s＞GW 预算和 30s（28+2））；"
            "科学注记：恢复侧 stagger（105/110）改同端（110/110）、注入侧不变——C1 pending 项（staggered 与 "
            "independent_parallel 症状定义对应）显式登记，不静默"},
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门"
                                "重新核定）——依据=旧 T1 GT PROVISIONAL 备注（inv-DEP independent_parallel，"
                                "L9994-9999）在删 timeout 后的剩余假设；旧 T1 CFG 链 trigger_amplifier 知识不迁移",
        reference_dependencies={"F1": ("S25",), "F2": ("S23",)},
        obs_r1_audit="PASS：live 容器={inventory pod, catalog-gw pod}，两腿两容器一一对应、无交叉；载体 Service"
                     "（inventory/pricing）非 CRI 目标；CRI 目标从 pinned pods 派生、单一 pod: 键型（无 id:/pod: "
                     '混合，OBS-collection）；长期规避=目标清单程序化派生＋解析后身份碰撞检查（CROSS-CUTTING §4）',
        preconditions=(
            _pricing_gw_path_precondition(),
            _reference_ordering_precondition(
                "D01", "F1=S25 dependency_latency@inventory, F2=S23 retry_policy_misconfiguration@catalog-gw"),
            _s23a_pilot_coupling_precondition(),
            _combo_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "§2 C9/C12、§3 D01 行、§4、§8",
            "catalog_scenario": "D01 (origin=reduced_from_same_entity 旧T02; id_reused=True; composition_signature "
                                "dependency_latency@inventory || retry_policy_misconfiguration@catalog-gw; G=2)",
            "ledger": "m14-ledger-b1/04-dependency-latency.md（inv 腿）+ m14-ledger-b2/01-gateway-cfg-family.md、"
                      "02-design-level-mapping.md（retry 腿，S23-A）",
            "origin": "reduced_from_same_entity（旧 T02 删 timeout 腿）；must-not-migrate：旧 T02 timeout 腿全套、"
                      "catalog-bad 慢 primary（新 D01 必须进排除语义）、旧 F3 回 ov-f1on 保 timeout 恢复语义、"
                      "旧三根 nested 几何与 --deep 强制三载体",
            "parameter_status": "NOT_FROZEN（inv delay_ms=2000 沿 S25 候选，C9；retry {proxy_next_upstream: off} 由 "
                                "prepare_gateway 精确参数集锁定，C12；旋钮隔离 changed_directives 恰 "
                                "[proxy_next_upstream]）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D01 行：main=inventory-direct 受害臂，"
                                      "bypass=pricing-via-gw 承载 retry 旋钮的 gw 路径通道；前提=pricing 重定向）",
            "timing_status": "NOT_FROZEN（选项B 嵌套 retry 子窗⊂inv 整窗；profile d01_nested_retry_subwindow；"
                             "legacy=null 新构成，选择理由在 timing.decision）",
            "gt_correspondence": "inv 腿=Q1 延迟通道（p95 位移候选）＋Q2 恢复（env patch 回原值＋rollout 一致性）；"
                                 "retry 腿=在场证据 transition 记录＋knob_isolation（Q2 消费 gateway transition 行），"
                                 "效果签名不做 GT 断言（S23-A）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（R 接线前置＋collection 冒烟义务）'}),
    "D05": ComboSpec(
        scenario_id="D05", purpose="pilot", scope="m1_main",
        rule_version_base="d05-combo-pilot", random_seed=11,
        phases=D05_PHASES,
        legs=(_combo_leg("S03", D05_POD_INNER_WINDOW),
              _combo_leg("S24", D05_GW_OUTER_WINDOW, parameters={"read_timeout_ms": 7})),
        carriers=(
            CarrierStreamSpec(stream_id="d05-main-catalog-items-via-gw", service="catalog-gw:80",
                              path_prefix="/api/items/", carrier="combo-main-panel-catalog-via-gw",
                              
                              direct_api=True, trace_service="catalog_service"),
            CarrierStreamSpec(stream_id="d05-bypass-catalog-items-direct", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-bypass-panel-catalog-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("catalog-pod-logs"),
        signals=(_combo_signal("S03"),
                 
                 # (pricing_service /api/pricing/) names a stream family D05
                 # does not carry -- its request profile is catalog-items
                 # via-gw + catalog-direct and the pricing redirect premise is
                 # explicitly NOT carried (preconditions).  The read_timeout
                 # knob governs the shared main stream, so the F2 recovery
                 # metric + trace face rebinds onto the catalog face those
                 
                 
                 # coverage).  D16-class observation-face mismatch closed for
                 # D05; statistic/tolerance fields stay atom-derived.
                 _combo_signal_on_stream("S24", service_name="catalog_service",
                                         http_target="/api/items/<item_id>")),
        injection_signal_kinds=("carrier_error_fraction", "carrier_error_fraction"),
        injection_signals=(_combo_injection_signal("S03"), _combo_injection_signal("S24")),
        timing=ComboTimingProfile(
            name="d05_nested_pod_inner", geometry="nested_pod_inner",
            inject_order=("F2", "F1"), recover_order=("F1", "F2"),
            decision="SPECS-1 D05(d) 选项B 采纳（蓝图建议；选项A 公共目标窗警示=T08 先例同型：pod-failure 整窗压"
                     "during 上限，长窗 pod-down 压垮载体采样与 churn 门）：timeout gw OUTER (30,135) 先注（切换带 "
                     "25s→settled ~55），pod INNER (55,115) below_kill_band v2 剖面（60s 窗/CRD 90s，S03/"
                     "POD_FAILURE_LIVENESS_PROFILES 同源；EXEC-POD-XFER 2026-09-22：实测 lag 头 35s 由 pod 族 carrier "
                     "settle 吸收，评估窗 [inject确认+35, recover确认] ~34 样本≥C1 地板 30，有效下线段 [~89,~126] "
                     "全落 gw 稳态段→co-active ~37 样本；偏移 {30,55,115,135} 最小正间隙 20＞默认预算和 19）；窗归属严序：pod "
                     "recover @115 的 restore+Ready ~13s（recover 动作内实测确认）严格先于 gw restore @135（不互污；旧 T4 cfg/pod 严序"
                     "知识＋D04 restart 严序教训），gw restore 带尾≤28s→≤163≤during 末 165；预算=2×切换带＋pod "
                     "bite＋dwell＋pod recover ~35s＋margin（旧 T4 地板公式 L14373 按带长替换重算，B2-03 §1 D05）；"
                     "gw-only 稳态段为零系设计（pod 注入始于 gw settled 点；timeout 单根基线来自 S24 家族池化参照"
                     "C1 口径B，不依赖组合内部段）；注入序 gw(OUTER)→pod；恢复 reverse：pod 先（readiness 滞后仅"
                     "证据、绝不做 abort）→gw 后；timing NOT_FROZEN"),
        interaction_expectation="无旧标签（新构成）——设计意图期望=independent_parallel 类叠加（期望非观测：设计"
                                "意图期望，非观测结论；走 C1 §0 B 观测门）；masking 方向假设（pod-down（lag 头 35s 后）502 "
                                "掩盖 read_timeout 504 签名）列为 pilot 观察项而非 GT 断言；旧 T04 标签不复用",
        reference_dependencies={"F1": ("S03",), "F2": ("S24",)},
        obs_r1_audit="PASS：live 容器={catalog pod, catalog-gw pod}，两腿两容器一一对应、无交叉；注意 catalog-gw "
                     "既是腿2目标 pod 又是 main 载体 Service 名——Service 对象非 CRI 目标，无键冲突（§4 D05 行）；"
                     'CRI 目标从 pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _reference_ordering_precondition(
                "D05", "F1=S03 service_unavailable@catalog, F2=S24 timeout_misconfiguration@catalog-gw"),
            _combo_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "§2 C4/C11、§3 D05 行、§4、§8",
            "catalog_scenario": "D05 (origin=reduced_from_same_entity 旧T04; id_reused=True; composition_signature "
                                "service_unavailable@catalog || timeout_misconfiguration@catalog-gw; G=2)",
            "ledger": "m14-ledger-b1/05-pod-failure.md（pod 腿）+ m14-ledger-b2/01-gateway-cfg-family.md、"
                      "02-design-level-mapping.md（timeout 腿）",
            "origin": "reduced_from_same_entity（旧 T04 删 catlat 腿）；must-not-migrate：catlat 腿、旧 T4 六步单序"
                      "闭包几何、旧 catalog livenessProbe 临时修补（是否需要按新 pod 腿重评估，不默认继承）、"
                      "旧 1000ms 档直传",
            "parameter_status": "NOT_FROZEN_MATCH_WITHIN_CONTRAST_FAMILY（pod 腿 {action: pod-failure, duration_s: "
                                "38}=S03 below_kill_band 剖面；timeout 腿 read_timeout_ms=200=旧 ov-net-f2 本组自身"
                                "历史档作候选（C11 显式分档论证：旧 1000ms 档属 catlat 带宽窗设计，新 D05 无 catlat "
                                '须重论证；collection 家族冻结时与 S24 统一定档））',
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D05 行：main=catalog-items-via-gw 双腿共路径受害＋"
                                      "承载 timeout 旋钮，bypass=catalog-direct 绕 gw 直连验证 pod 腿在场于 gw 外"
                                      "通道；pricing 重定向不涉）",
            "timing_status": "NOT_FROZEN（选项B：timeout 整窗＋pod 子窗，pod INNER；profile "
                             "d05_nested_pod_inner；legacy=null 新构成，选择理由在 timing.decision）",
            "gt_correspondence": "pod 腿=carrier_error_fraction（≥0.5 候选）＋restart_delta∈{1,2} 窗签名＋Q2 恢复"
                                 "（pinned 原镜像＋Ready 复验）；timeout 腿=transition 在场；效果=观察 504/超时类"
                                 "错误与时序",
            "observation_face_status": 'X01 重绑（collection EXEC-⑦ 2026-09-22）：F2 信号不再逐字继承 S24 原子面'
                                       "（pricing_service /api/pricing/——D05 无该流族，历史残留错配，xhigh 矩阵"
                                       " X01/D16 类）；恢复 metric/trace 面重绑到 main 流实际落点 catalog_service "
                                       '/api/items/<item_id>（collection span 归属），entity 保持 catalog-gw（Q2 根覆盖）；'
                                       "Q1 载流证据仍走 mechanism_or_bypass（共享主流），Q2/Q3 面与 Q1 绑定同名一流；"
                                       '统计/阈值/容差字段原子继承，未另造采集器，容差重冻结归 collection 家族冻结；'
                                       "同步对照＝族投影臂（contrast-D05-S03/S24）由同一父合同投影，观察面随父件"
                                       "一致",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（R 接线前置＋collection 冒烟义务）'}),
    "D06": ComboSpec(
        scenario_id="D06", purpose="pilot", scope="m1_main",
        rule_version_base="d06-combo-pilot-5ms", random_seed=11,
        phases=D06_PHASES,
        legs=(_combo_leg("S05", D06_HOST_WINDOW, duration_s=275),
              _combo_leg("S24", D06_GATEWAY_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d06-main-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-main-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service"),
            CarrierStreamSpec(stream_id="d06-bypass-catalog-items-direct", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-bypass-panel-catalog-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("stressor-pod-logs"),
        signals=(_combo_signal("S05"), _combo_signal("S24")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_error_fraction"),
        injection_signals=(_combo_injection_signal("S05"), _combo_injection_signal("S24")),
        timing=ComboTimingProfile(
            name="d06_nested_timeout_mid_during", geometry="nested",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="collection D06 P0 re-cut from anchor-1. Preserve host F1 then gateway F2; set the user-selected 5 ms timeout (formerly 20 ms), both entities and the pricing-via-gateway carrier. F1 OUTER (30,305), F2 INNER (62,190): injection gap 32 s remains above the collection 30 s O-D3 minimum; the 128 s F2 window leaves about 43 s after the measured 40 s host-contented rollout plus the 45 s observed-window trim, about 11 s beyond the 30 s floor after lateness/edge allowance. F2 restores first; the declared action ceiling is 17+90=107 s, so its restore completes by offset 297 and remains inside F1 through 305. F1 cleanup's 17 s ceiling completes by 322 inside the short during phase ending 330. Under long300 the windows are F1 [300,575], F2 [332,460]; F2 Q1 evaluation interval is bounded near 417..567 and remains within host F1 through 575; this is plan arithmetic, not a live signal claim. D06 anchor-1 (39.875 s F2 inject under host saturation) predates the current 107 s gateway action ceiling; that old TimingViolation is addressed by the shared runner fix, while live signal and recovery still need an anchor. This preserves the timing/exposure re-cut while changing only the timeout dose to 5 ms: G=2, root identities, host workers/load, request route, Q1 threshold and n>=30 stay fixed. Historical 20/12 ms attempts are not 5 ms evidence."),
        interaction_expectation="trigger_amplifier（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）——"
                                "D06 正是 C1 等价性检验 5 分歧例之一（池化参照 3.08% 背景 error vs 旧'严格零'门）；"
                                "正式判定=被放大腿 overlap 期效应超单根池化参照过门槛且放大腿单独不产生该签名"
                                "（C1 §1）",
        reference_dependencies={"F1": ("S05",), "F2": ("S24",)},
        obs_r1_audit="PASS：live 容器={stressor pod, catalog-gw pod}（host 腿容器=stressor 载体，非 victim pods）；"
                     '两腿两容器一一对应、无交叉；CRI 目标从 pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S05"].preconditions[0],
            ATOM_SPECS["S05"].preconditions[1],
            _pricing_gw_path_precondition(),
            _reference_ordering_precondition(
                "D06", "F1=S05 host_cpu_saturation@host, F2=S24 timeout_misconfiguration@catalog-gw"),
            _combo_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "§2 C6/C11、§3 D06 行、§4、§8",
            "catalog_scenario": "D06 (origin=retained_composition 旧DK09; id_reused=False; legacy=nested/"
                                "trigger_amplifier; composition_signature host_cpu_saturation@host || "
                                "timeout_misconfiguration@catalog-gw; G=2)",
            "ledger": "m14-ledger-b1/02-host-cpu.md（host 腿）+ m14-ledger-b2/01-gateway-cfg-family.md、"
                      "02-design-level-mapping.md（timeout 腿）",
            "origin": "retained_composition（旧 DK09）；legacy=nested（无异议默认）＋trigger_amplifier（仅知识输入，"
                      "见 interaction_expectation）",
            "parameter_status": "NOT_FROZEN（host {workers:32, load_percent:100} 沿 S05 候选，duration_s=275 覆盖"
                                "重切后的组合 OUTER 窗；绝不 --vm；用户选择 timeout read_timeout_ms=5，"
                                "与 S24 单根对照同档；旧 20/12 ms 尝试只作历史证据，5 ms 尚无现场验证）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D06 行：main=pricing-via-gw——host 抬 TTFB→低于 "
                                      "timeout 档即 504 的主通道；bypass=catalog-direct——gw 外对照分离 gw 错误定位；"
                                      "前提=pricing 重定向）",
            "timing_status": "NOT_FROZEN (D06 host-contention re-cut from anchor-1; fresh conditions and live anchor pending)",
            "gt_correspondence": "host 腿=多受害者 p95 相对比 ≥1.8× 候选＋churn restart_delta=0（观测设计）；cfg "
                                 "腿=transition/knob_isolation 在场＋cfg-validity（旧 hostcpu_cfg_gate 三臂语义由 "
                                 "transition/classify_probe 承接，B2-01 §4）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（R 接线前置＋collection 冒烟义务）'}),
    "D12": ComboSpec(
        scenario_id="D12", purpose="pilot", scope="m1_main",
        rule_version_base="d12-combo-pilot", random_seed=11,
        phases=COMBO_NESTED_PHASES,
        legs=(_combo_leg("S08", COMBO_OUTER_WINDOW),
              _combo_leg("S24", COMBO_GW_INNER_WINDOW, parameters={"read_timeout_ms": 1000})),
        carriers=(
            CarrierStreamSpec(stream_id="d12-main-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-main-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service"),
            CarrierStreamSpec(stream_id="d12-bypass-catalog-items", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-bypass-panel-catalog-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("catalog-pod-logs"),
        signals=(_combo_signal("S08"), _combo_signal("S24")),
        injection_signal_kinds=("metric_threshold", "carrier_error_fraction"),
        injection_signals=(None, _combo_injection_signal("S24")),
        timing=ComboTimingProfile(
            name="d12_nested_timeout_mid_during", geometry="nested",
            inject_order=("F1", "F2"),
            
            # env unset first, then the gw restore); the stable-sorted-by-end
            # law forces exactly this order.
            recover_order=("F1", "F2"),
            decision="SPECS-1 D12(d)：legacy=nested 默认（旧 DK14 形态 L15880-15884：catlat env rollout 预注吸收"
                     "〔S08 实测 ~13s〕＋timeout 子窗 mid-during）：catlat OUTER (30,122) 先注吸收 rollout→settled "
                     "~45，timeout INNER (62,122) 后注（切换带 25s→settled ~87；注入侧＝重切前原值，时序定义 "
                     "2026-09-21T04:04+08:00 时序配置 裁定二不变项）；F1_only 稳态 45→87=42 与 overlap 稳态 87→122="
                     "35 均达 C1 §2.5 地板（§8-6 子窗预算；旧 F2_OFFSET/DURATION 不照抄）；带宽窗排序约束：噪声 "
                     "~30ms ≪ timeout 档(1000) ≪ catlat 档(2000) ≪ baseline 8000ms 必须保持（B2-03 §1 D12，C11 "
                     '显式分档论证）；注入序 catlat（env rollout）→gw；collection 同端恢复重切（时序定义 2026-09-21'
                     'T04:04+08:00 时序配置 裁定二）：gw recover 105→110 与 catlat env unset 同端@122（collection；恢复侧 '
                     'stagger 105/110→110/110→122/122；重切前 5s 恢复正间隙使一切诚实预算对过不了 O-D3 断言，collection BAND-'
                     "RULING-INPUT §四算术）；串行恢复按声明序 F1 catlat env unset（rollout ≤~15s→settled ≤~137）"
                     "→F2 gw restore（冻结带裁定一 typical 14-21s/声明带 25s/尾注记 ≤28s：完成 typical ≤~146、"
                     "声明带 ≤~150、尾注记 ≤~153——typical 完成落 recovery_transition 标记区＝重切声明性后果，"
                     'runner 无门、collection 须实测 rollout_band_s 回填）；collection 重切后动作偏移 30/62/122/122、最小正间隙 '
                     '32s＞GW 预算和 30s（28+2）（as-declared 构造面通过；collection 后预算和 30＜32，live 张力解除；注入侧 50→62 平移吸收 D01 实测 GW 锚 23.219s（collection 张力登记原文见 RESLICE-RULING.md），'
                     "见 RESLICE-RULING.md）；gw 带在 during 内（gate 须消费 classify_probe 分桶剔除带+settle 段，"
                     '属 collection 能力）；科学注记：恢复侧 stagger 改同端、注入侧不变——入 C1 pending（staggered 与 '
                     "independent_parallel 症状定义对应）显式登记不静默；timing NOT_FROZEN"),
        
        
        # same-offset serial recovery budget declaration (O-D3/O-D7 corrected
        # form; D11 precedent). Budget arithmetic = the timing.decision above.
        same_offset_budgets={frozenset(("F1", "F2")):
            'D12 recover-recover 同端对 offset 122 显式预算声明（collection 110→122）（权威＝时序定义 2026-09-21T04:04+08:00 时序配置 '
            'configs/collection/scenarios.json: assembly definition'
            'combo-driver-design/timing policy O-D3/O-D7 修正后批准 2026-09-18＋D11/collection 同端恢复对；'
            "预算算术与本 spec timing.decision 同源）：恢复按声明序（＝腿序）F1（catalog catlat env unset，"
            "rollout ≤~15s→settled ≤~125）→F2（gateway restore 经滚切 settle，冻结带 typical 14-21s/声明带 25s/"
            "尾注记 ≤28s：完成 typical ≤~158、声明带 ≤~162、尾注记 ≤~165——typical 完成落 recovery_transition "
            '标记区，为重切声明性后果非门违例，collection 须实测 rollout_band_s 回填）；跨 deployment 恢复理论上可'
            "并行但按串行保守预算申报；其余动作对的最小正间隙断言照常生效（重切后最小正间隙 20s＞默认预算和 "
            "19s）；科学注记：恢复侧 stagger（105/110）改同端（110/110）、注入侧不变——C1 pending 项（staggered "
            "与 independent_parallel 症状定义对应）显式登记，不静默"},
        interaction_expectation="trigger_amplifier（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）；"
                                "C1 前提(a)：S08/S24 参照在场（C1 §0 点名 D12→S08 闭合）",
        reference_dependencies={"F1": ("S08",), "F2": ("S24",)},
        obs_r1_audit="PASS：live 容器={catalog pod, catalog-gw pod}，两腿两容器一一对应、无交叉；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _pricing_gw_path_precondition(),
            _reference_ordering_precondition(
                "D12", "F1=S08 dependency_latency@catalog, F2=S24 timeout_misconfiguration@catalog-gw"),
            _combo_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "§2 C8/C11、§3 D12 行（§1.4 原文示例组）、§4、§8",
            "catalog_scenario": "D12 (origin=retained_composition 旧DK14; id_reused=False; legacy=nested/"
                                "trigger_amplifier; composition_signature dependency_latency@catalog || "
                                "timeout_misconfiguration@catalog-gw; G=2)",
            "ledger": "m14-ledger-b1/04-dependency-latency.md（catlat 腿）+ m14-ledger-b2/01-gateway-cfg-family.md、"
                      "02-design-level-mapping.md（timeout 腿）",
            "origin": "retained_composition（旧 DK14）；legacy=nested（默认）＋trigger_amplifier（仅知识输入）",
            "parameter_status": "NOT_FROZEN_MATCH_WITHIN_CONTRAST_FAMILY（catlat delay_ms=2000 沿 S08 候选（C8，"
                                "S08 同机理唯一有 live 冒烟证据的原子）；timeout read_timeout_ms=1000=旧 ov-catlat-f2 "
                                "本组自身历史档作候选，满足带宽窗排序 噪声~30ms≪1000≪catlat 2000≪baseline 8000"
                                '（B2-03 §1 D12；collection 家族冻结时统一定档））',
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D12 行＝§1.4 原文示例组：main=pricing-via-gw，"
                                      "bypass=catalog-direct——catlat 原始延迟在场、绕 gw；前提=pricing 重定向）",
            "timing_status": "NOT_FROZEN（legacy=nested 默认；profile d12_nested_timeout_mid_during；改公共目标窗"
                             "须记录）",
            "gt_correspondence": "catlat 腿=Q1 延迟（bypass 通道直读原始位移，≥1000ms 候选）＋ok≥0.8＋Q2；cfg 腿="
                                 "transition 在场＋超时错误（error ratio 候选）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（R 接线前置＋collection 冒烟义务）'}),
    "D22": ComboSpec(
        scenario_id="D22", purpose="pilot", scope="m1_main",
        rule_version_base="d22-combo-pilot", random_seed=11,
        phases=D22_PHASES,
        legs=(_combo_leg("S28", D22_PRICING_CPU_OUTER_WINDOW, duration_s=162),
              _combo_leg("S24", D22_GW_INNER_WINDOW, parameters={"read_timeout_ms": 7})),
        carriers=(
            CarrierStreamSpec(stream_id="d22-main-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-main-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service"),
            CarrierStreamSpec(stream_id="d22-bypass-catalog-items-direct", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-bypass-panel-catalog-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("pricing-pod-logs"),
        signals=(_combo_signal("S28"), _combo_signal("S24")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_error_fraction"),
        injection_signals=(_combo_injection_signal("S28"), _combo_injection_signal("S24")),
        timing=ComboTimingProfile(
            name="d22_nested_timeout_subwindow", geometry="nested",
            inject_order=("F1", "F2"),
            
            # CRD delete first, then the gw restore); the stable-sorted-by-end
            
            # tie 122 -> 192 together with the window widening (dose+settle
            
            # form, only its offset moved.
            recover_order=("F1", "F2"),
            decision="SPECS-1 D22(d)：legacy=null（新对）；装配采纳蓝图建议=选项B（承旧 T01 形态：pricing CPU 整窗＋CFG "
                     "子窗）：pricing cpu OUTER (30,192) 轻腿先注先稳（CPU CRD 秒级先、gw rollout 带后注，B2-03 §1 D22/"
                     "B1-06 §6-1 同则；饱和爬坡保守 15s 头预算→settled ~45），timeout INNER (62,192) 后注；"
                     "F1_only 稳态 45→87=42 与 overlap 稳态 87→192=105 均达 C1 §2.5 地板（§8-6）；注入序 pricing→gw；"
                     '恢复对=collection/collection 同端裁定的同形延承：gw 与 pricing CRD 删除同端@192（tie 122→192 随窗宽整体平移，'
                     "形状不变）；串行恢复按声明序 F1 pricing CRD 删除（秒级 ≤~2.6s，r10c 实测 recover 锚 2.61s）→F2 gw "
                     "restore（冻结带 typical 14-21s/声明带 25s/尾注记 ≤28s：完成 typical ≤~215≤during 末 222、声明带 "
                     '≤~219、尾注记 ≤220 均落 during 内；collection 须实测 rollout_band_s 回填）；EXEC-GWXFER 重算后动作偏移 '
                     "30/62/192/192、最小正间隙 32s＞默认预算和 19s（as-declared 构造面通过；恢复对为同端 tie，走 "
                     "same_offset_budgets 串行申报路径，无新增正间隙对）；long300 投影（delta=270）：F2 运行窗 (332,462)、"
                     "recover 完成 ≤462+17(实测 dispatch 滞后 fix-3)+40(rollout 上界)=519≤during 末 600；科学注记：本次为"
                     "剂量+观测窗重切（EXEC-GWXFER）：timeout 档 20→7ms（标定依据=fix-1/2/3 pre_fault 池化 Jaeger catalog "
                     "上游 span n=900 p50=7.62ms，frac(span>7)=0.838 零传递下界，声明预期错误分数 ~0.84≥0.5；20ms 档实测"
                     "不触发，fix-3 Q1 F2 statistic 0.0/44）、GW 族 carrier settle 拆分为 45s（rollout 带上界 40+5）、F2 窗 "
                     "60→130s 保最坏有效窗 ≥60 请求＞n≥30 地板；阈值 0.5/required/n≥30 判据全部未动；timing NOT_FROZEN"),
        
        
        # recovery budget declaration (O-D3/O-D7 corrected form; D11 precedent).
        
        # widening. Budget arithmetic = the timing.decision above.
        same_offset_budgets={frozenset(("F1", "F2")):
            'D22 recover-recover 同端对 offset 192 显式预算声明（collection 裁定形；EXEC-GWXFER 2026-09-22 随窗宽 110→122→192 '
            "平移）（权威＝时序定义 2026-09-21T04:04+08:00 时序配置 裁定二：GW recover 同端＋O-D3 串行申报；机制先例＝"
            'configs/collection/scenarios.json: assembly definition'
            "同端恢复对；预算算术与本 spec timing.decision 同源）：恢复按声明序（＝腿序）F1（pricing StressChaos CRD 删除，"
            "r10c 实测锚 ≤~2.6s）→F2（gateway restore 经滚切 settle，冻结带 typical 14-21s/声明带 25s/尾注记 ≤28s：完成 "
            'typical ≤~215≤during 末 222、声明带 ≤~219、尾注记 ≤220 落 during 内，collection 须实测 rollout_band_s 回填）；'
            "跨 deployment 恢复理论上可并行但按串行保守预算申报；其余动作对的最小正间隙断言照常生效（重切后动作偏移 "
            "30/62/192/192、最小正间隙 32s＞默认预算和 19s；GW 串行预算和 30（28+2）＜同端 tie 免除断言）；科学注记："
            "同端 tie 形状不变、仅 offset 随 EXEC-GWXFER 窗宽平移——C1 pending 项维持显式登记，不静默"},
        interaction_expectation="无旧标签（新对）——设计意图期望未定，候选 trigger_amplifier（EXEC-GWXFER 重校准后"
                                "前提更新：timeout 档 7ms 已低于健康 gw→catalog 上游 span p50 7.62ms（fix-1/2/3 池化"
                                "实测），错误源自足；pricing 慢化的边际贡献＝上游尾段抬升入/出超时档的比例，是本对的"
                                "交互问题）（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）；不预设",
        reference_dependencies={"F1": ("S28",), "F2": ("S24",)},
        obs_r1_audit="PASS：live 容器={pricing pod, catalog-gw pod}，两腿两容器一一对应、无交叉；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _pricing_gw_path_precondition(),
            _reference_ordering_precondition(
                "D22", "F1=S28 service_cpu_saturation@pricing, F2=S24 timeout_misconfiguration@catalog-gw"),
            _combo_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "§2 C5/C11、§3 D22 行、§4、§7、§8",
            "catalog_scenario": "D22 (origin=triple_pair_completion T01 拆对; id_reused=False; legacy=null; "
                                "composition_signature service_cpu_saturation@pricing || "
                                "timeout_misconfiguration@catalog-gw; G=2)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（pricing cpu 腿，SOFT 载体警示）+ m14-ledger-b2/"
                      "01-gateway-cfg-family.md、02-design-level-mapping.md（timeout 腿）",
            "origin": "triple_pair_completion（T01 拆对）；T01 三腿子对闭合={D15, D12, D22}（CROSS-CUTTING §7）",
            "parameter_status": "NOT_FROZEN（pricing cpu {workers:2（500m，T05 腿源默认）, load_percent:100, "
                                "duration_s:162 覆盖组合 OUTER 窗（EXEC-GWXFER 92→162 随窗宽）}；timeout "
                                "read_timeout_ms=7＝EXEC-GWXFER calibration-first 重标定（C11 显式分档论证：标定依据＝"
                                "fix-1/2/3 pre_fault Jaeger catalog 上游 span 池化 n=900 p50=7.62ms/p90=8.54ms，"
                                "frac(span>7)=0.838 零传递下界→声明预期错误分数 ~0.84≥0.5；20ms 档在该链实测不触发"
                                "——fix-3 Q1 F2 statistic 0.0/44、during 全相位 297 请求仅 2-3 错；8ms/10ms 候选"
                                "零传递下界 0.283/0.019 不达 0.5 弃用；S24 原子默认 20 保留给 D06 host 放大链另行"
                                '锚重跑；collection 家族冻结时统一定档））',
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D22 行：main=pricing-via-gw——pricing 慢×gw 超时叠加"
                                      "主通道；bypass=catalog-direct——gw 外对照；前提=pricing 重定向）；载体臂 SOFT"
                                      "（旧 T01/T05 pricing carrier-latency 未验证，L13371）：throttle/自载体证据优先，"
                                      "载体 ratio 不作硬判据（C5 SOFT 警示）",
            "timing_status": "NOT_FROZEN（选项B 承旧 T01 形态；profile d22_nested_timeout_subwindow；legacy=null "
                             "新构成，选择理由在 timing.decision）",
            "gt_correspondence": "pricing cpu 腿=ratio 臂（SOFT 载体警示）＋per-pod throttle 证据（新 Q 缺口，"
                                 "OPEN-Q3）；cfg 腿=transition 在场＋超时错误",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（R 接线前置＋collection 冒烟义务）'}),
    "T01": ComboSpec(
        scenario_id="T01", purpose="pilot", scope="m1_main",
        rule_version_base="t01-combo-pilot", random_seed=11,
        phases=T01_PHASES,
        legs=(_combo_leg("S28", T01_PRICING_CPU_WINDOW, duration_s=100),
              _combo_leg("S08", T01_CATLAT_WINDOW),
              _combo_leg("S24", T01_GW_INNER_WINDOW, parameters={"read_timeout_ms": 7})),
        carriers=(
            CarrierStreamSpec(stream_id="t01-main-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-main-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service"),
            CarrierStreamSpec(stream_id="t01-bypass-catalog-items", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-bypass-panel-catalog-direct",
                              direct_api=True),
            CarrierStreamSpec(stream_id="t01-third-pricing-items-cpu", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-third-panel-pricing-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("pricing-pod-logs"),
        signals=(_combo_signal("S28"), _combo_signal("S08"), _combo_signal("S24")),
        injection_signal_kinds=("carrier_p95_ratio", "metric_threshold", "carrier_error_fraction"),
        injection_signals=(_combo_injection_signal("S28"), None, _combo_injection_signal("S24")),
        timing=ComboTimingProfile(
            name="t01_nested_triple", geometry="nested_triple",
            inject_order=("F2", "F1", "F3"),
            
            # CRD delete, then F3 gw restore; F2 catlat @130 last); the
            # stable-sorted-by-end law forces exactly this order.
            recover_order=("F1", "F3", "F2"),
            decision="CURRENT 2026-09-24 a5 sampling reslice: short phases (0,30)/(30,280)/(280,340); "
                     "F2(50,225) -> F1(70,170) -> F3(90,170), recover F1/F3@170 then F2@225. "
                     "Every fault window shifts +20 s; pair/triple overlap lengths and F3 post-settle 35 s "
                     "stay fixed. The first fault starts 20 s after during begins, and F2 recovery starts "
                     "55 s before during ends; these are observation margins, pending a fresh field check. "
                     "Earlier design rationale (historical offsets) follows: "
                     "SPECS-1 T01(d)：legacy=nested（三腿旧形态）默认（改三腿公共目标窗=窗几何变化须记录；"
                     '本行 collection 三窗移动的记录＝时序定义 2026-09-21T04:58+08:00 时序配置）：catlat F2 '
                     "OUTER (30,130) 先注吸收 rollout（catlat-first 给 svccpu stable pod、避免 StressChaos-"
                     "survives-rollout，B1-06 §2 D03 同则；settled ~45）→pricing cpu F1 (50,110) 后注落 stable pod"
                     "（CRD 秒级；@50 较 settle ~45 留 5s 裕量）→gw F3 (70,110) 子窗偏后（切换带 25s→settled ~95）；"
                     'f1win(RES)⊆f2win(DEP) 嵌套为显式声明（§8-4）而非默认发生；collection 恢复侧同端重切（时序定义 '
                     "2026-09-21T04:04+08:00 裁定二：GW recover 105→110 与 pricing CRD 删除同端@110＋O-D3 串行申报；彼时注入侧"
                     '不变为该裁定不变项）遗留注入侧 F1@45→F3@50 正间隙 5s 使一切诚实预算停 O-D3——collection 停报证明'
                     "预授权最小重切不可行（O-D3 门取六动作偏移全局最小正间隙，平台上限 gap 10<19，X∈[51,109] 全"
                     '扫描真链三档停）；collection 扩大授权三窗移动：F1 inject 45→50、F3 inject 50→70、F2 recover 120→130'
                     "（恢复同端@110 与既有 {F1,F3} 申报不动）⇒ 六偏移 {30,50,70,110,110,130} 最小正间隙 20＞默认"
                     '预算和 19，as-declared 默认档全链通过（collection 解域探针已真链实测该形状）；地板算术（collection 授权口径＝'
                     "原始窗交集）：三腿 co-active [70,110]=40、逐对 F1∩F2=60、F1∩F3=F2∩F3=40，全≥C1 §2.5 地板 30；"
                     'settle 后三腿稳态段由 75→110=35（collection）缩至 95→110=15——三窗移动的科学足迹，如实登记不重计；'
                     "诚实残余：typical 26/tail 33 仍＞20 停 O-D3 静态门＝与四组（D01/D06/D12/D22）同类 live 张力"
                     '（静态门保守性，物理串行算术无实际冲突；collection 实测回填后再裁定，不填小预算）；串行恢复按声明序 '
                     "F1 pricing CRD 删除（秒级 ≤~2s）→F3 gw restore（冻结带裁定一 typical 14-21s/声明带 25s/尾注记 "
                     "≤28s：完成 typical ≤~133、声明带 ≤~137、尾注记 ≤~140）→F2 catlat env unset @130（排至 F3 完成"
                     "后启动：start-lateness ≤~3s，rollout ≤~15s→完成 typical ≤~148≤during 末 150、声明带 ≤~152/尾"
                     '注记 ≤~155 落 recovery_transition 标记区，collection 须实测 rollout_band_s 回填）；带宽窗排序同 D12'
                     '（噪声~30ms≪1000≪catlat 2000≪baseline 8000）；科学注记：collection 注入侧 stagger F1-F3 由 5s 变 20s'
                     '＋F2 recover 120→130——变更足迹大于 collection 恢复侧（改窗口头即改三腿 co-active 段与 provenance 时序'
                     '知识），逐字引 时序定义链：04:04 collection 裁定二『五张力组 GW recover 105→110 同端＋O-D3 串行申报'
                     "……科学注记显式登记：恢复侧 stagger 变更入 C1 pending 项口径，不静默』＋04:58 扩大授权『采纳 "
                     'collection 解域三窗移动（F1 45→50/F3 50→70/F2 recover 120→130 ⇒ min-gap 20>19 默认档过）……硬约束：'
                     "……科学注记三处升级〔注入 stagger 5→20s＋F2 recover 变更足迹大于恢复侧〕/typical-tail 张力同类"
                     "登记』——入 C1 pending 项（staggered 与 independent_parallel 症状定义对应）显式登记不静默；"
                     "timing NOT_FROZEN"),
        
        # section 6, ruling 2): GW recover 105 -> 110 same-end with the pricing
        
        
        
        # itself and this declaration key are UNCHANGED; the inject-side
        # stagger F1-F3 moved 5 s -> 20 s and F2 recover 120 -> 130 lift the
        # O-D3 stop (min positive gap 20 > the 19 s default sum), with the
        # honest typical/tail sums (26/33) still above 20 -- the same
        
        # live backfill), never small-budgeted past.
        same_offset_budgets={frozenset(("F1", "F3")):
            "CURRENT short recover tie F1/F3@170, with F2 recover@225 and a 55 s declared gap; "
            "serial order F1 then F3 remains explicit. a5 measured F1 2.312 s, F3 action 22.937 s "
            "plus dispatch/reconcile before F2; short during ends@280. Earlier budget rationale follows: "
            'T01 recover-recover 同端对 offset 150 显式预算声明（collection GW-config 传递函数重切 2026-09-22：tie '
            "@110→@150，F1/F3 窗联动扩至 (50,150)/(70,150)，F2 OUTER (30,170)，during (30,190)——settle 45s 下旧 "
            "40s 窗的 F3 评估窗为空；注入侧 30/50/70 不动沿用 时序定义 2026-09-21T04:58+08:00；权威链＝"
            "时序定义 2026-09-21T04:04+08:00 时序配置 裁定二（同端＋O-D3 串行申报）＋timing policy "
            'O-D3/O-D7 修正后批准 2026-09-18＋D11/collection 同端恢复对；min positive gap {30,50,70,150,150,170}=20>19 '
            "默认档过）：恢复按声明序（＝腿序）F1（pricing StressChaos CRD 删除，秒级 ≤~2s）→F3（gateway "
            "restore 经滚切 settle，典型 14-21s/声明带 25s/尾注记 ≤28s：完成典型 ≤~173、声明带 ≤~177、尾注记 "
            "≤~180）→F2 catlat env unset @205（anchor-1/2 现场校正 2026-09-22/23：F3 恢复全串行脚印＝carry 槽 17＋动作实测 "
            "15.2（patch 0.1＋rollout 带 12.9＋settled 回读 2.2）＋动作后 journal reconcile 实测 ~11＝槽 137→完成 ~163；旧 "
            "170/185 槽裸 deadline 均被吞→中止级联切归档挂接；新槽 deadline 207＞163 留 44s 余量，F2 脚印 ≤~17→完成 "
            "≤~190 ≤ during 末 225）；跨 deployment 恢复"
            "理论上可并行但按串行保守预算申报；其余动作对的最小正间隙断言照常生效——三窗移动后最小正间隙=20s（六偏移 "
            '{30,50,70,110,110,130}），as-declared 默认预算和 19＜20 构造面通过 O-D3 串行门（collection 解域真链预实测＋'
            'collection 落地实测）；诚实残余：typical 26/tail 33 仍＞20 停静态门＝与 D01/D06/D12/D22 同类 live 张力'
            '（collection 实测回填，不填小预算）；科学注记：collection 注入侧 stagger F1-F3 由 5s 变 20s＋F2 recover 120→130'
            '——变更足迹大于 collection 恢复侧（collection 为恢复侧 stagger 105/110 改同端 110/110、彼时注入侧不变），逐字引 root '
            '裁定链：04:04 collection 裁定二『五张力组 GW recover 105→110 同端＋O-D3 串行申报……科学注记显式登记：恢复侧 '
            'stagger 变更入 C1 pending 项口径，不静默』＋04:58 扩大授权『采纳 collection 解域三窗移动（F1 45→50/F3 '
            "50→70/F2 recover 120→130 ⇒ min-gap 20>19 默认档过）……硬约束：……科学注记三处升级〔注入 stagger "
            "5→20s＋F2 recover 变更足迹大于恢复侧〕/typical-tail 张力同类登记』——入 C1 pending 项（staggered 与 "
            "independent_parallel 症状定义对应）显式登记，不静默"},
        interaction_expectation="trigger_amplifier（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）；"
                                "旧 GT 标'放大(co-primary)'（FAULT_DESIGN.md L76）——co_primary 角色字段按 C1 §0 "
                                "范围声明在实例层标注，不迁移为设计结论；三腿交互判定结构依赖 O-E5（逐 amplifier "
                                "子对降维：D15/D12/D22 三个子对已有设计，CROSS-CUTTING §7）",
        reference_dependencies={"F1": ("S28",), "F2": ("S08",), "F3": ("S24",)},
        obs_r1_audit="PASS：live 容器={pricing pod, catalog pod, catalog-gw pod}，三腿三容器一一对应、无交叉；"
                     'CRI 目标从 pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _pricing_gw_path_precondition(),
            _t01_stable_pod_precondition(),
            _reference_ordering_precondition(
                "T01", "F1=S28 service_cpu_saturation@pricing, F2=S08 dependency_latency@catalog, "
                       "F3=S24 timeout_misconfiguration@catalog-gw"),
            _combo_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "§2 C5/C8/C11、§3 T01 行与三腿说明、§4、§7、§8",
            "catalog_scenario": "T01 (origin=retained_composition 旧三-01; id_reused=False; legacy=nested/"
                                "trigger_amplifier; composition_signature dependency_latency@catalog || "
                                "service_cpu_saturation@pricing || timeout_misconfiguration@catalog-gw; G=3)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（pricing cpu 腿，SOFT）+ m14-ledger-b1/04-dependency-latency.md"
                      "（catlat 腿）+ m14-ledger-b2/01-gateway-cfg-family.md、02-design-level-mapping.md（timeout 腿）",
            "origin": "retained_composition（旧三-01）；三流处置=pricing cpu 第三腿以自载体 t01-third-pricing-items-cpu"
                      "（pricing-direct，SOFT）进 streams[] 并在 eval 配置标角色（O-E5 裁定前建议形态，§3 三腿说明）；"
                      "T01 三腿子对闭合={D15, D12, D22}（CROSS-CUTTING §7，机器不变量 TRIPLE_PAIR_CLOSURE"
                      "〔COMBO-ASM-4 补登〕）",
            "parameter_status": "NOT_FROZEN_MATCH_WITHIN_CONTRAST_FAMILY（pricing cpu {workers:2, load_percent:100, "
                                "duration_s:65 覆盖组合窗}；catlat delay_ms=2000；timeout read_timeout_ms=1000=旧 "
                                "ov-catlat-f2 家族档候选，带宽窗排序 timeout<catlat 保持（B2-03 §1 D12 同则））",
            "request_profile_status": "NOT_FROZEN（三流载体 §3 T01 行：main=pricing-via-gw 三腿叠加主通道，"
                                      "bypass=catalog-direct catlat 原始延迟在场，third=pricing-direct pricing cpu "
                                      "自载体（SOFT）；前提=pricing 重定向）",
            "timing_status": "NOT_FROZEN（2026-09-24 a5 sampling reslice：short head/tail margins 20/55s; "
                             "F2(50,225), F1(70,170), F3(90,170); nested overlaps unchanged; field recheck pending）",
            "gt_correspondence": "pricing 腿=ratio（SOFT）＋throttle；catlat 腿=延迟通道（bypass 直读原始位移）；cfg "
                                 "腿=transition 在场＋cfg-validity；旧 gate deep_triple_pricing_cat_cfg_gate L6310 "
                                 "的 per_root_RES（pricing throttle>eps＋RES_WITNESS_DOMINANCE_FRAC=0.3）为阈值 "
                                 "provenance 候选锚（B3-03 §3.2），不迁移为结论",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（R 接线前置＋collection 冒烟义务）'}),
    # --- COMBO-ASM-2: SPECS-2 B1 pure family (D02/D04/D07/D08/D10/D11/D13/ ---- #
    # --- D16/D17/D29) -- net/pod/db/env leg mixes, no gateway cfg leg -------- #
    "D02": ComboSpec(
        scenario_id="D02", purpose="pilot", scope="m1_main",
        rule_version_base="d02-combo-pilot", random_seed=11,
        phases=COMBO2_DBNET_PHASES,
        legs=(_combo_leg("S02", COMBO2_DBNET_NET_WINDOW, duration_s=90),
              _combo_db_leg("S06", COMBO2_DBNET_DB_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d02-main-catalog-items-dblock", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-main-panel-catalog-direct",
                              direct_api=True),
            CarrierStreamSpec(stream_id="d02-bypass-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-bypass-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service")),
        telemetry=_std_telemetry("catalog-gw-pod-logs"),
        signals=(_combo_signal("S02"), _combo_signal("S06")),
        injection_signal_kinds=("metric_threshold", "carrier_error_fraction"),
        injection_signals=(None, _combo_injection_signal("S06")),
        timing=ComboTimingProfile(
            name="d02_common_target_window_net_first", geometry="common_target_window",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-2 D02(d)：legacy=null（旧 T03 删 delay 腿后新构成，三腿嵌套错峰几何不可照抄，B1-06 "
                     "§2 D02）；采纳蓝图建议=公共目标窗（simultaneous 类）：loss CRD F1 (30,120) NET-first 先注"
                     '（AllInjected 秒级，§8-2 轻腿先注；duration_s=90 覆盖窗），db 锁 F2 (35,85) 随后进锁（collection 间距'
                     "3→5：netem inject 实测 3.11-3.125s，旧 3s 头距时 db 注入排在 netem apply 在飞期间；锁窗尾保持 "
                     "85 不动，TAIL 不变量 85+35=120 与恢复排序原样）；双活"
                     "分析窗 35→85=50 样本≥C1 §2.5 地板 30；恢复=锁释放确认先行（confirm_release×2 各≤15s＋语句"
                     "与余量＝TAIL 35s，S06 precondition 3 同源算术）：锁窗尾 85+35=120 覆盖后 loss CRD 删除"
                     "（@120，秒级，≤121≤during 末 125）；旧 T03 recover_db_lock 阻塞 confirm 需 TAIL 预算教训"
                     "（L14339-14359）以相位预算≥TAIL 承接；timing NOT_FROZEN"),
        interaction_expectation="无旧标签（新构成）——设计意图期望=independent_parallel（两腿作用面分离：传输层 "
                                "loss vs 表锁；期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）",
        reference_dependencies={"F1": ("S02",), "F2": ("S06",)},
        obs_r1_audit="PASS：live 容器={catalog-gw pod}（db 腿=MySqlTableLock 无 live 容器目标）；MySQL pod 仅"
                     "日志/锁观测源（单键单源）；载体 Service（catalog/pricing）非 CRI 目标；CRI 目标从 pinned "
                     'pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S02"].preconditions[0],      # pricing redirect (bypass stream premise)
            ATOM_SPECS["S02"].preconditions[1],      # loss-tier pilot recalibration
            ATOM_SPECS["S06"].preconditions[0],      # CHECKSUM iron-rule sequencing
            ATOM_SPECS["S06"].preconditions[1],      # durable-first lock identity
            ATOM_SPECS["S06"].preconditions[2],      # blocking recover TAIL budget
            _db_net_order_precondition("D02"),
            _reference_ordering_precondition(
                "D02", "F1=S02 network_loss@catalog-gw, F2=S06 db_table_lock@mysql_items_lock"),
            _b1_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C2/C7、§3 D02 行、§4、§8",
            "catalog_scenario": "D02 (origin=reduced_from_same_entity 旧T03; id_reused=True; composition_signature "
                                "network_loss@catalog-gw || db_table_lock@mysql_items_lock; G=2)",
            "ledger": "m14-ledger-b1/03-network-chaos.md（loss 腿）+ m14-ledger-b1/01-db-table-lock.md（锁腿，"
                      "S06 先例）",
            "origin": "reduced_from_same_entity（旧 T03 删 delay 腿）；must-not-migrate：旧 T03 三腿嵌套错峰几何、"
                      "旧 12s/2s 占空比锁剖面（S06 持续单会话锁有意不迁移）、旧 T03 loss 值（未核定，禁引实测）",
            "parameter_status": "NOT_FROZEN（loss_percent=60 沿 S02 候选档（C2），DK15 85% 旧复合工况不继承；"
                                "duration_s=90 覆盖组合窗；db 锁 {table: items, mode: WRITE} 由 prepare_db_lock "
                                "精确参数集锁定（C7），参数由腿身份全导出零漂移）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D02 行：main=catalog-direct 锁 error-burst 受害者臂"
                                      "（纯 SELECT→CHECKSUM 安全），bypass=pricing-via-gw loss 通道绕开 DB 锁；"
                                      "前提=pricing 重定向）",
            "timing_status": "NOT_FROZEN（公共目标窗＋NET-first＋锁释放确认先行；profile "
                             "d02_common_target_window_net_first；legacy=null 新构成，选择理由在 timing.decision）",
            "gt_correspondence": "loss 腿=延迟/错误双通道（dropped-metric 禁用铁律）；锁腿=error-burst（≥0.5 候选"
                                 "按持续锁剖面重标定）＋Q2 owned_database_locks 锁证据行＋CHECKSUM 双铁律（锁前 "
                                 "pre/锁期禁/释放确认后 post，driver 侧 sidecar 采样暂停——ASM S06 precondition 1 "
                                 "同款）；QC 阈值按持续锁新剖面标定（旧 12/2 间歇 error 语境预期抬高，B1-06 §2 "
                                 "D02）",
            "telemetry_note": "v1 单 operations/log 源形状沿用 ASM-1 §9.4 限制：组合 rules 携 kubectl 面"
                              "（m1-kubectl-primitives），mysql-error-log/m1-mysql-sessions/m1-db-primitives 的 "
                              "db 观测源不进 v1 rules；Q2 锁/checksum 证据仍走 m1-mysql-locks/m1-mysql-checksum；"
                              '混合运行多源扩展属 collection 观测设计',
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D04": ComboSpec(
        scenario_id="D04", purpose="pilot", scope="m1_main",
        rule_version_base="d04-combo-pilot", random_seed=11,
        phases=D04_PHASES,
        legs=(_combo_leg("S03", D04_CATALOG_WINDOW),
              _combo_leg("S26", D04_USER_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d04-main-catalog-items-via-gw", service="catalog-gw:80",
                              path_prefix="/api/items/", carrier="combo-main-panel-catalog-via-gw",
                              
                              direct_api=True, trace_service="catalog_service"),
            CarrierStreamSpec(stream_id="d04-bypass-user-health", service="user:5004",
                              path_prefix="/health", carrier="combo-bypass-panel-user-health",
                              direct_api=True, parameterized=False)),
        telemetry=_std_telemetry("catalog-pod-logs"),
        signals=(_combo_signal("S03"), _combo_signal("S26")),
        injection_signal_kinds=("carrier_error_fraction", "pod_restart_delta"),
        injection_signals=(_combo_injection_signal("S03"), _combo_injection_signal("S26")),
        timing=ComboTimingProfile(
            name="d04_staggered_disjoint_subwindows", geometry="staggered_disjoint",
            inject_order=("F1", "F2"), recover_order=("F1", "F2"),
            decision="SPECS-2 D04(d)：legacy=staggered＝默认（旧 dual_podfail_staggered L15522-15555；C1 §2.6 "
                     "RETAIN 无异议默认，改公共目标窗须记录）——装配采纳=保持 staggered 两错峰子窗（EXEC-POD-XFER "
                     "2026-09-22 重切：实测传递函数=spec 补丁对运行中容器不立即生效，咬合点=inject 确认后 "
                     "lag 28.8-30.9s（声明界 35＝pod 族 carrier settle），结束点=CR 移除＋restore+Ready ~11-13s；"
                     "窗口跨度 35→60、CRD 38→90（=跨度+lag 界），阈值/required/n≥30 全保持）：catalog "
                     "F1 (30,90) below_kill_band v2（评估窗 [inject确认+35, recover确认] ~34 样本≥地板 30；"
                     "有效下线段 ~[34,71]=37s<40s kill 带）；mid 严序＝catalog recover@90"
                     "→restore+Ready ~13s（recover 动作内 wait_pod_ready 实测确认）→restart+2 注册完成→snapshot"
                     "→user F2 注入 (130,190)（"
                     "poll_carrier=False；40s 间隙=restore/注册 35＋snapshot 余量 5，防 restart 证据溢入对方窗，"
                     "docstring L15523-15526 教训重述并硬化为 Precondition）；user 子窗 v2 同形（评估窗 ~34 样本"
                     "≥地板）；user recover@190 restore+Ready ~203 于 during 末 205 前完成（evidence-only 绝不 "
                     "abort）；staggered 类恢复序＝按窗尾升序（F1 先收），ComboTimingProfile 的 reversed(inject) "
                     "律按几何类豁免（嵌套/公共窗类不豁免）；timing NOT_FROZEN"),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）"
                                "——legacy CATALOG 标签 independent_parallel；旧 independent_gate L7827（D04 "
                                "staggered availability）为阈值 provenance 候选锚，不迁移为结论",
        reference_dependencies={"F1": ("S03",), "F2": ("S26",)},
        obs_r1_audit="PASS：live 容器={catalog pod, user pod}，两腿两容器一一对应、无交叉；CRI 目标从 pinned "
                     'pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S26"].preconditions[0],     # leaf-victim restart/ready detection basis
            _d04_stagger_strict_order_precondition(),
            _reference_ordering_precondition(
                "D04", "F1=S03 service_unavailable@catalog, F2=S26 service_unavailable@user"),
            _b1_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C4、§3 D04 行、§4、§8",
            "catalog_scenario": "D04 (origin=retained_composition 旧同号; id_reused=False; legacy=staggered/"
                                "independent_parallel; composition_signature service_unavailable@catalog || "
                                "service_unavailable@user; G=2)",
            "ledger": "m14-ledger-b1/05-pod-failure.md（双腿；S26 叶子口径 SS2）",
            "origin": "retained_composition（旧同号）；legacy=staggered 默认保持；旧 _ensure_catalog_healthy_for_"
                      "dual_podfail 健康预门语义由 mid 严序 Precondition 重述（实现细节不迁移）",
            "parameter_status": "NOT_FROZEN（双腿 {action: pod-failure, duration_s: 90}=below_kill_band v2 剖面，"
                                "POD_FAILURE_LIVENESS_PROFILES 同源（catalog=S03 默认剖面；user=std_20_3_3 "
                                "同形 04-user.yaml）；MINOR-1 不变量 90≥60 双腿成立；EXEC-POD-XFER 2026-09-22 重切）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D04 行：main=catalog-items-via-gw——catalog pod 错误"
                                      "在 lag 头（界 35s）后咬中；bypass=user /health 固定路径叶子在场/恢复观察口径；pricing 重定向"
                                      "不涉（catalog-items-via-gw 流））",
            "timing_status": "NOT_FROZEN（legacy=staggered 默认保持；profile d04_staggered_disjoint_subwindows；"
                             "restart 按窗归属严序必须重述——见 timing.decision 与 preconditions）",
            "gt_correspondence": "catalog 腿=carrier error fraction（≥0.5 候选）＋restart_delta∈{1,2} 窗签名＋Q2 "
                                 "恢复（pinned 原镜像＋Ready 复验）；user 腿=pod_restart_delta（叶子：restart/ready "
                                 "观察口径，无 carrier 错误通道，probe10 假阴 quirk，injection signal kind="
                                 'pod_restart_delta）；S26=atomic_completion 首用，叶子观测口径需 collection 冒烟',
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D07": ComboSpec(
        scenario_id="D07", purpose="pilot", scope="m1_main",
        rule_version_base="d07-combo-pilot", random_seed=11,
        phases=COMBO2_ENVNET_PHASES,
        legs=(_combo_leg("S25", COMBO2_ENVNET_OUTER_WINDOW),
              _combo_leg("S01", COMBO2_ENVNET_INNER_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d07-main-inventory-items", service="inventory:5013",
                              path_prefix="/api/inventory/", carrier="combo-main-panel-inventory-direct",
                              direct_api=True),
            CarrierStreamSpec(stream_id="d07-bypass-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-bypass-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service")),
        telemetry=_std_telemetry("inventory-pod-logs"),
        signals=(_combo_signal("S25"), _combo_signal("S01")),
        injection_signal_kinds=("metric_threshold", "metric_threshold"),
        injection_signals=(None, None),
        timing=ComboTimingProfile(
            name="d07_partial_overlap_env_outer_net_inner", geometry="partial_overlap",
            inject_order=("F1", "F2"), recover_order=("F1", "F2"),  # collection: same-end tie runs in leg order
            decision="SPECS-2 D07(d)：legacy=partial_overlap＝默认（旧三窗几何 F1_only→overlap→F2_only，inv 预注 "
                     "L15254-15256/gw mid 注 L15567/恢复 L15571/L15577，gate deep_dual_edge_gate L7280，B1-06 §2 "
                     "D07）；采纳建议=保留三段式但恢复按 §8-2 reverse（旧 F2_only 段由 inv 尾段替代，显式声明"
                     "非默认发生，§8-4）：inv F1 (30,120) env rollout 注入相位吸收（头带 15s→settled ~45），gw F2 "
                     "(85,120) CRD 秒级 mid 注；F1_only 稳态 45→85=40 与 overlap 稳态 ~88→120=32 均达 C1 §2.5 "
                     "地板 30（§8-6 子窗预算；旧 F2_OFFSET=14/F2_DURATION=31 不照抄）；恢复 reverse 同端@120：gw CRD 删除"
                     '@120 同端（秒级）→inv env unset 串行（rollout ≤~138≤during 末 140；collection 同端声明在案；collection 平移）；C1 缺口①'
                     "（independent_parallel 延迟侧容差带未量化）按 B-GATE 保守占位；timing NOT_FROZEN"),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）"
                                "——两腿独立签名通道分离（inv 直读 p95 vs gw 路径 p95）",
        same_offset_budgets={frozenset(("F1", "F2")):
            'D07 recover-recover 同端对 offset 120 显式预算声明（collection 内窗 80/115→85/120 平移后落同端；机制先例＝'
            'collection 同端恢复对＋collection GW 族同端重切；预算算术与本 spec timing.decision 同源）：恢复按声明序（＝腿序）'
            "F1（inventory env unset，rollout ≤~15s→≤~135）→F2（gw netem CRD 删除，秒级）串行完成 ≤~138 ≤ during "
            "末 140；最小正间隙 35＞预算和 19，无正间隙对逃逸 O-D3。"},
        reference_dependencies={"F1": ("S25",), "F2": ("S01",)},
        obs_r1_audit="PASS：live 容器={inventory pod, catalog-gw pod}，两腿两容器一一对应、无交叉；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S01"].preconditions[0],     # pricing redirect (bypass stream premise)
            _reference_ordering_precondition(
                "D07", "F1=S25 dependency_latency@inventory, F2=S01 network_delay@catalog-gw"),
            _b1_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C9/C1、§3 D07 行、§4、§8",
            "catalog_scenario": "D07 (origin=retained_composition 旧DK11; id_reused=False; legacy=partial_overlap/"
                                "independent_parallel; composition_signature dependency_latency@inventory || "
                                "network_delay@catalog-gw; G=2)",
            "ledger": "m14-ledger-b1/04-dependency-latency.md（inv 腿，S25 首用无现场证据）+ m14-ledger-b1/"
                      "03-network-chaos.md（gw delay 腿）",
            "origin": "retained_composition（旧 DK11）；legacy=partial_overlap 默认；恢复 reverse 为蓝图建议（旧 "
                      'F2_only 段不保留）；S25 首用冒烟义务（collection）',
            "parameter_status": "NOT_FROZEN（inv delay_ms=2000 沿 S25 候选（C9）；gw {latency_ms:500, jitter_ms:50, "
                                "correlation_percent:0, duration_s:60} 沿 S01 候选（C1），duration_s=60 覆盖 35s "
                                "子窗（MINOR-1））",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D07 行：main=inventory-direct——inv 慢受害；"
                                      "bypass=pricing-via-gw——gw delay 通道；前提=pricing 重定向）",
            "timing_status": "NOT_FROZEN（legacy=partial_overlap 默认＋恢复 reverse；profile "
                             "d07_partial_overlap_env_outer_net_inner）",
            "gt_correspondence": "inv 腿=延迟通道（inventory_service p95 位移＋ok≥0.8）；gw 腿=延迟通道（pricing "
                                 "面板 p95 ≥800ms 候选＋ok≥0.8）；旧 isolation_gate 隔离三证知识（target_hit/第三方 "
                                 'flat/abort 语义）在新链的落位=开放 collection 项（B1-03 §3.2-4），不迁移为结论',
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D08": ComboSpec(
        scenario_id="D08", purpose="pilot", scope="m1_main",
        rule_version_base="d08-combo-pilot", random_seed=11,
        phases=COMBO2_NETPOD_PHASES,
        legs=(_combo_leg("S01", COMBO2_NETPOD_NET_WINDOW, duration_s=100),
              _combo_leg("S26", COMBO2_NETPOD_POD_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d08-main-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-main-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service"),
            CarrierStreamSpec(stream_id="d08-bypass-user-health", service="user:5004",
                              path_prefix="/health", carrier="combo-bypass-panel-user-health",
                              direct_api=True, parameterized=False)),
        telemetry=_std_telemetry("catalog-gw-pod-logs"),
        signals=(_combo_signal("S01"), _combo_signal("S26")),
        injection_signal_kinds=("metric_threshold", "pod_restart_delta"),
        injection_signals=(None, _combo_injection_signal("S26")),
        timing=ComboTimingProfile(
            name="d08_nested_pod_inner", geometry="nested_pod_inner",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-2 D08(d)：legacy=staggered（旧 net_delay_x_podfail，pod 腿注入 L15628 附近、gate "
                     "net_podfail_cross_gate L7948）；蓝图两式均开放（gw 腿整窗＋user pod 子窗，或反之——装配"
                     "记录定），装配选择=gw netem 整窗＋user pod 子窗（蓝图第一式，窗几何上 pod INNER⊂net "
                     "OUTER）：netem F1 (30,130) 先注（CRD 秒级→settled ~32；duration_s=100 覆盖窗），user F2 "
                     "(50,110) below_kill_band v2（60s 窗/CRD 90s；EXEC-POD-XFER 2026-09-22：实测 lag 头 35s 由 "
                     "pod 族 carrier settle 吸收，有效下线段 [~84,~121] 与 net 稳态 exposure [~32,131] 的 "
                     "co-active ~37 样本≥地板 30；偏移 {30,50,110,130} 最小正间隙 20＞默认预算和 19）；user 叶子 "
                     "recover 观察口径与 gw 窗不互污：pod recover@110 restore+Ready ~123 于 during 末 135 内收口"
                     "（evidence-only），net CRD 删除@130（秒级）在 during 内；恢复 reverse-nested（pod 先收→net "
                     "后收）；timing NOT_FROZEN"),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）；"
                                "旧 net_podfail_cross_gate L7948 为阈值 provenance 候选锚",
        reference_dependencies={"F1": ("S01",), "F2": ("S26",)},
        obs_r1_audit="PASS：live 容器={catalog-gw pod, user pod}，两腿两容器一一对应、无交叉；CRI 目标从 pinned "
                     'pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S01"].preconditions[0],     # pricing redirect (main stream premise)
            ATOM_SPECS["S26"].preconditions[0],     # leaf-victim restart/ready detection basis
            _reference_ordering_precondition(
                "D08", "F1=S01 network_delay@catalog-gw, F2=S26 service_unavailable@user"),
            _b1_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C1/C4、§3 D08 行、§4、§8",
            "catalog_scenario": "D08 (origin=retained_composition 旧DK05B; id_reused=False; legacy=staggered/"
                                "independent_parallel; composition_signature network_delay@catalog-gw || "
                                "service_unavailable@user; G=2)",
            "ledger": "m14-ledger-b1/03-network-chaos.md（gw delay 腿）+ m14-ledger-b1/05-pod-failure.md（user "
                      "腿，叶子口径 SS2）",
            "origin": "retained_composition（旧 DK05B）；legacy=staggered；蓝图两式均开放，装配取 gw 整窗＋pod "
                      "子窗式（选择理由在 timing.decision）；user 叶子检测口径=pod_restart_delta",
            "parameter_status": "NOT_FROZEN（gw {latency_ms:500, jitter_ms:50, correlation_percent:0, duration_s:"
                                "100 覆盖组合窗} 沿 S01 候选（C1）；user {action: pod-failure, duration_s: 90}="
                                "below_kill_band v2 剖面（C4 user 变体）；MINOR-1 不变量 100≥100/90≥60 成立；"
                                "EXEC-POD-XFER 2026-09-22 重切）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D08 行：main=pricing-via-gw——gw delay 主通道；"
                                      "bypass=user /health——user 叶子在场/恢复；前提=pricing 重定向）",
            "timing_status": "NOT_FROZEN（蓝图两式开放，装配选择 gw 整窗＋pod 子窗（nested_pod_inner）；profile "
                             "d08_nested_pod_inner，选择理由在 timing.decision）",
            "gt_correspondence": "net 腿=延迟通道（pricing_service p95 位移）；user 腿=restart 通道"
                                 "（pod_restart_delta，restart/ready 观察口径 evidence-only）；旧 gate "
                                 "net_podfail_cross_gate 为阈值 provenance 候选锚",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D10": ComboSpec(
        scenario_id="D10", purpose="pilot", scope="m1_main",
        rule_version_base="d10-combo-pilot", random_seed=11,
        phases=COMBO2_DBNET_PHASES,
        legs=(_combo_db_leg("S06", COMBO2_DBNET_DB_WINDOW),
              _combo_leg("S01", COMBO2_DBNET_NET_WINDOW, duration_s=90)),
        carriers=(
            CarrierStreamSpec(stream_id="d10-main-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-main-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service"),
            CarrierStreamSpec(stream_id="d10-bypass-catalog-items-dblock", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-bypass-panel-catalog-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("catalog-gw-pod-logs"),
        signals=(_combo_signal("S06"), _combo_signal("S01")),
        injection_signal_kinds=("carrier_error_fraction", "metric_threshold"),
        injection_signals=(_combo_injection_signal("S06"), None),
        timing=ComboTimingProfile(
            name="d10_common_target_window_net_first", geometry="common_target_window",
            inject_order=("F2", "F1"), recover_order=("F1", "F2"),
            decision="SPECS-2 D10(d)：legacy=simultaneous（两根 during 前就位=整 during 活跃）；simultaneous 语义"
                     "在新链由公共目标窗承载（§8-1）：NET-first 注入序知识必须保留（轻的 AllInjected 秒级先注、"
                     "db 锁后进，旧 is_db_lock_combo L15161-15183 NET-first 干净，B1-06 §2 D10）——net F2 (30,120) "
                     '先注（duration_s=90 覆盖窗），db F1 (35,85) 后进锁（collection 间距 3→5，netem inject 实测 3.11-3.125s；'
                     "锁窗尾 85 不动保 TAIL 不变量）；双活分析窗 35→85=50 样本≥地板 30；恢复＝"
                     "锁释放确认（confirm_release×2 ≤15s×2＋语句＋余量＝TAIL 35s）先行→确认完成后删 net CRD"
                     "（@120，秒级，≤121≤during 末 125）；checksum 双铁律（B1-01 #1）与 S06 precondition 族整体"
                     "随腿迁移；timing NOT_FROZEN"),
        interaction_expectation="fault_masking（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）——被掩盖"
                                "腿主通道签名被压至参照下容差之下、旁路通道仍验证其在场（C1 §1）；本组 bypass"
                                "（catalog-direct 错误 burst）即掩盖判定的旁路验证通道；masking_mode=error 配置"
                                "需 err 双阈值（pilot 标定，OPEN-Q15）",
        reference_dependencies={"F1": ("S06",), "F2": ("S01",)},
        obs_r1_audit="PASS：live 容器={catalog-gw pod}（db 腿=MySqlTableLock 无 live 容器目标）；载体 Service"
                     '（pricing/catalog）非 CRI 目标；CRI 目标从 pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S01"].preconditions[0],     # pricing redirect (main stream premise)
            ATOM_SPECS["S06"].preconditions[0],     # CHECKSUM iron-rule sequencing
            ATOM_SPECS["S06"].preconditions[1],     # durable-first lock identity
            ATOM_SPECS["S06"].preconditions[2],     # blocking recover TAIL budget
            _db_net_order_precondition("D10"),
            _reference_ordering_precondition(
                "D10", "F1=S06 db_table_lock@mysql_items_lock, F2=S01 network_delay@catalog-gw"),
            _b1_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C7/C1、§3 D10 行、§4、§8",
            "catalog_scenario": "D10 (origin=retained_composition 旧同号; id_reused=False; legacy=simultaneous/"
                                "fault_masking; composition_signature db_table_lock@mysql_items_lock || "
                                "network_delay@catalog-gw; G=2)",
            "ledger": "m14-ledger-b1/01-db-table-lock.md（锁腿，S06 先例；checksum 双铁律 B1-01 #1）+ "
                      "m14-ledger-b1/03-network-chaos.md（gw delay 腿）",
            "origin": "retained_composition（旧同号）；legacy=simultaneous；NET-first 注入序知识必须保留"
                      "（is_db_lock_combo）；旧 12/2 占空比锁剖面不迁移（持续锁）",
            "parameter_status": "NOT_FROZEN（db 锁 {table: items, mode: WRITE} 由腿身份全导出（C7）；gw "
                                "{latency_ms:500, jitter_ms:50, correlation_percent:0, duration_s:90 覆盖组合窗} "
                                "沿 S01 候选（C1）；MINOR-1 不变量 90≥90 成立）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D10 行：main=pricing-via-gw——gw delay 签名主通道；"
                                      "bypass=catalog-direct——锁 error-burst 在场、绕 gw；前提=pricing 重定向）",
            "timing_status": "NOT_FROZEN（公共目标窗＋NET-first＋锁释放确认先行；profile "
                             "d10_common_target_window_net_first；simultaneous 语义由公共目标窗承载）",
            "gt_correspondence": "lock 腿=error-burst＋checksum_zero_drift（pre==post==基线）＋Q2 锁证据行；net 腿="
                                 "延迟通道＋旧 NET_SEP control 判据（≥5×/800ms，probe11 标定）为 provenance 候选"
                                 "锚（组合 gate db_lock_netdelay_combo_gate L4648）；checksum 双铁律（B1-01 #1）",
            "telemetry_note": "v1 单 operations/log 源形状沿用 ASM-1 §9.4 限制（同 D02）：db 观测源不进 v1 "
                              "rules，Q2 锁/checksum 证据走 m1-mysql-locks/m1-mysql-checksum",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D11": ComboSpec(
        scenario_id="D11", purpose="pilot", scope="m1_main",
        rule_version_base="d11-combo-pilot", random_seed=11,
        phases=D11_PHASES,
        legs=(_combo_leg("S25", D11_INV_WINDOW),
              _combo_leg("S07", D11_RUNTIME_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d11-main-catalog-items-runtime", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-main-panel-catalog-direct",
                              direct_api=True),
            CarrierStreamSpec(stream_id="d11-bypass-inventory-items", service="inventory:5013",
                              path_prefix="/api/inventory/", carrier="combo-bypass-panel-inventory-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("inventory-pod-logs"),
        signals=(_combo_signal("S25"), _combo_signal("S07")),
        injection_signal_kinds=("metric_threshold", "carrier_error_fraction"),
        injection_signals=(None, _combo_injection_signal("S07")),
        timing=ComboTimingProfile(
            name="d11_common_target_window_pre_inject_both", geometry="common_target_window",
            inject_order=("F2", "F1"), recover_order=("F1", "F2"),
            decision="SPECS-2 D11(d)：legacy=simultaneous（旧注入序 runtime F2 先注 L15271、inv F1 后注 "
                     "L15272-15273（SIMULTANEOUS pre-inject-both）；恢复 F1 先→F2 后→f1win⊆f2win 机械嵌套，"
                     "B1-06 §2 D11/B3-02 #7）；采纳建议=保持 pre-inject-both 公共目标窗：runtime F2 (30,110) 先注、"
                     'inv F1 (35,110) 紧随（注入序=旧链；collection：3s 头距按 collection/collection 5s 网格重切，33→35，与 T05/T07/T08 '
                     "同网格）；双 env rollout 头带（各 15s，跨 deployment 可并行但"
                     "预算按串行保守→settled ~50）全部在稳态段前吸收；during 全窗双活稳态 ~50→110=60 样本≥地板 "
                     "30；恢复按声明序 F1（inv unset）→F2（runtime unset）（＝旧链 F1 先收），各 rollout ≤~13s、"
                     "串行保守 ≤~140≤during 末 140；f1win⊆f2win 嵌套为显式声明（§8-4）而非默认发生；DOUBLE-"
                     "ROLLOUT ~340s 旧教训（L15267-15268）记录为 setup 预算上限知识，数值不照抄；timing "
                     "NOT_FROZEN"),
        
        # recover-recover same-end pair F1/F2 @110 (authority: combo-driver-
        
        # same-end pairs are legal and owe an explicit serial/parallel recovery
        # budget; the budget arithmetic is the timing.decision text above).
        same_offset_budgets={frozenset(("F1", "F2")):
            'configs/collection/scenarios.json: assembly definition'
            "combo-driver-design/timing policy O-D3/O-D7 修正后批准 2026-09-18：同端恢复对合法、"
            "豁免 min_gap 断言、须声明串行/并行恢复预算；预算算术与本 spec timing.decision 同源）：恢复按"
            "声明序 F1（inventory env unset）→F2（catalog env unset），两腿均 app_env_hook rollout，"
            '声明串行脚印各＝带15＋写2＝17s（collection D11 anchor-1 现场校正 2026-09-22：r48_fixed 档通用 4s '
            "预留对 12-17s rollout 脚印结构性迟滞，runner carry 已按声明脚印下限取 max(4,17)）；跨 "
            "deployment rollout 可并行但按串行保守预算（F1 槽 110 完成 ≤~127，F2 槽 110+17=127 完成 "
            "≤~144 ≤ during 末 155）；其余动作对的 min positive gap 断言照常生效（本组合 min gap "
            "22s，52-30）"},
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）"
                                "——两腿作用面分离（catalog 500 error 主导 vs inv 延迟）；'unconditional "
                                "approximation' 标注义务记于 gt_correspondence（B3-02 §5-4）",
        reference_dependencies={"F1": ("S25",), "F2": ("S07",)},
        obs_r1_audit="PASS：live 容器={inventory pod, catalog pod}（两 env 腿各一次独立 rollout）；无交叉；CRI "
                     '目标从 pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _d11_pre_inject_both_precondition(),
            _reference_ordering_precondition(
                "D11", "F1=S25 dependency_latency@inventory, F2=S07 runtime_exception@catalog"),
            _b1_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C9/C10、§3 D11 行、§4、§8",
            "catalog_scenario": "D11 (origin=retained_composition 旧DK13; id_reused=False; legacy=simultaneous/"
                                "independent_parallel; composition_signature dependency_latency@inventory || "
                                "runtime_exception@catalog; G=2)",
            "ledger": "m14-ledger-b1/04-dependency-latency.md（inv 腿，S25 首用）+ m14-ledger-b3/02-runtime-"
                      "exception.md（runtime 腿）",
            "origin": "retained_composition（旧 DK13）；legacy=simultaneous（pre-inject-both 注入序与恢复序照旧，"
                      "窗几何新构成声明）；DOUBLE-ROLLOUT setup 吸收知识保留",
            "parameter_status": "NOT_FROZEN（inv delay_ms=2000 沿 S25 候选（C9）；runtime {enabled: true} 单态锁定"
                                "（C10，B3-02 §3.2-2），env 键值 FAULT_RAISE/1 由机制携带）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D11 行：main=catalog-direct——catalog:5005 500 "
                                      "error 主导通道；bypass=inventory-direct——inv 慢在场；pricing 重定向不涉）",
            "timing_status": "NOT_FROZEN（pre-inject-both 公共目标窗；profile d11_common_target_window_pre_inject_"
                             "both；setup 头带预算≥2×rollout 带串行保守）",
            "gt_correspondence": "runtime 腿=error 通道（error_ratio≥0.8 候选）＋'unconditional approximation' 标注"
                                 "义务（非条件业务异常，B3-02 §5-4）＋restart_delta=0（/health 豁免保 Ready，与 "
                                 "pod-failure 可分）；inv 腿=延迟通道（p95 位移＋ok≥0.8）；旧 gate inv_latency_"
                                 "runtime_dual_gate L4739（per_root_B error≥0.5 L4935＋env-rollout churn 豁免 "
                                 "L4758）为阈值 provenance 候选锚，churn 口径按 C1 重述",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D13": ComboSpec(
        scenario_id="D13", purpose="pilot", scope="m1_main",
        rule_version_base="d13-combo-pilot", random_seed=11,
        phases=COMBO2_ENVNET_PHASES,
        legs=(_combo_leg("S08", COMBO2_ENVNET_OUTER_WINDOW),
              _combo_leg("S02", COMBO2_ENVNET_INNER_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d13-main-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-main-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service"),
            CarrierStreamSpec(stream_id="d13-bypass-catalog-items", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-bypass-panel-catalog-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("catalog-pod-logs"),
        signals=(_combo_signal("S08"), _combo_signal("S02")),
        injection_signal_kinds=("metric_threshold", "metric_threshold"),
        injection_signals=(None, None),
        timing=ComboTimingProfile(
            name="d13_partial_overlap_catlat_outer_loss_inner", geometry="partial_overlap",
            inject_order=("F1", "F2"), recover_order=("F1", "F2"),  # collection: same-end tie runs in leg order
            decision="SPECS-2 D13(d)：legacy=partial_overlap＝默认（旧 catlat F1 预注、loss F2 mid 注 L15598；"
                     "gate deep_catlat_loss_gate L5416）；采纳建议=保留 catlat 整窗＋loss 子窗：catlat F1 (30,120) "
                     "env rollout 注入相位吸收（头带 15s→settled ~45），loss F2 (85,120) CRD 秒级 mid 注；F1_only "
                     '稳态 45→85=40 与 overlap 稳态 ~88→120=32 均达地板 30（collection 内窗平移，角距 55/35＞预算和 19）；恢复 reverse 同端@120：loss CRD 删除（秒级）'
                     '→catlat unset 串行（rollout ≤~138≤during 末 140；collection 同端声明在案）；loss 窗探针超时收紧知识'
                     "（NET_LOSS_PROBE_TIMEOUT_SEC=3 联动，B1-03 #15）为观测设计输入；timing NOT_FROZEN"),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）"
                                "（CATALOG legacy_interaction=independent_parallel；B1-06 记旧数据另见 co_primary "
                                "标签——以 CSV 为准，co_primary 角色字段属实例层，C1 §0 范围）",
        same_offset_budgets={frozenset(("F1", "F2")):
            'D13 recover-recover 同端对 offset 120 显式预算声明（collection 内窗 80/115→85/120 平移后落同端；机制先例＝'
            'collection 同端恢复对＋collection GW 族同端重切；预算算术与本 spec timing.decision 同源）：恢复按声明序（＝腿序）'
            "F1（catalog env unset，rollout ≤~15s→≤~135）→F2（gw loss CRD 删除，秒级）串行完成 ≤~138 ≤ during "
            "末 140；最小正间隙 35＞预算和 19，无正间隙对逃逸 O-D3。"},
        reference_dependencies={"F1": ("S08",), "F2": ("S02",)},
        obs_r1_audit="PASS：live 容器={catalog pod, catalog-gw pod}，两腿两容器一一对应、无交叉；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S02"].preconditions[0],     # pricing redirect (main stream premise)
            ATOM_SPECS["S02"].preconditions[1],     # loss-tier recalibration couples to the S02 pilot
            _reference_ordering_precondition(
                "D13", "F1=S08 dependency_latency@catalog, F2=S02 network_loss@catalog-gw"),
            _b1_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C8/C2、§3 D13 行、§4、§8",
            "catalog_scenario": "D13 (origin=retained_composition 旧DK15; id_reused=False; legacy=partial_overlap/"
                                "independent_parallel; composition_signature dependency_latency@catalog || "
                                "network_loss@catalog-gw; G=2)",
            "ledger": "m14-ledger-b1/04-dependency-latency.md（catlat 腿，S08 唯一有 live 冒烟证据的原子）+ "
                      "m14-ledger-b1/03-network-chaos.md（loss 腿）",
            "origin": "retained_composition（旧 DK15）；legacy=partial_overlap 默认；loss 强度 NOT_FROZEN——旧 "
                      "DK15 专用 85% 档（NET_LOSS_DK15_* L262-267）是'catlat 非 net_delay 不放大 loss 重传'的"
                      "工况标定（60% 只 21% error<0.3 阈），新 D13 按同理由重新论证档位（catlat 腿是否放大 loss "
                      "效应决定档位），不照抄也不默认回到 60%（B1-06 §2 D13）",
            "parameter_status": "NOT_FROZEN（catlat delay_ms=2000 沿 S08 候选（C8）；loss_percent=60 沿 S02 候选档"
                                "（C2），S02 loss 档 pilot 重标定联动本组档位论证（蓝图 (f)）；duration_s=60 覆盖 "
                                "35s 子窗（MINOR-1））",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D13 行：main=pricing-via-gw——loss×catlat 叠加通道；"
                                      "bypass=catalog-direct——catlat 原始延迟在场；前提=pricing 重定向）",
            "timing_status": "NOT_FROZEN（legacy=partial_overlap 默认＋恢复 reverse；profile "
                             "d13_partial_overlap_catlat_outer_loss_inner）",
            "gt_correspondence": "catlat 腿=延迟通道（bypass 直读原始位移，≥1000ms 候选）＋ok≥0.8；loss 腿=延迟/"
                                 "错误双通道（无 ok 下限，loss 合法 error）；旧 gate deep_catlat_loss_gate 为阈值 "
                                 "provenance 候选锚",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D16": ComboSpec(
        scenario_id="D16", purpose="pilot", scope="m1_main",
        rule_version_base="d16-combo-pilot", random_seed=11,
        phases=D16_PHASES,
        legs=(_combo_leg("S01", D16_NET_WINDOW, duration_s=105),
              _combo_leg("S03", D16_POD_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d16-main-catalog-items-via-gw", service="catalog-gw:80",
                              path_prefix="/api/items/", carrier="combo-main-panel-catalog-via-gw",
                              
                              direct_api=True, trace_service="catalog_service"),
            CarrierStreamSpec(stream_id="d16-bypass-catalog-items-direct", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-bypass-panel-catalog-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("catalog-gw-pod-logs"),
        
        # pricing_service metric family, but this combo carries NO pricing
        # stream (main=catalog-items-via-gw with catalog_service spans,
        # bypass=catalog-direct) -- the same registered observation-face
        # mismatch as D05-F2 (field evidence: F1 0 points, Q2 no pre/post, Q3
        # no series). Rebound onto the main stream's span face via
        
        signals=(_combo_signal_on_stream("S01", service_name="catalog_service",
                                         http_target="/api/items/<item_id>"),
                 _combo_signal("S03")),
        injection_signal_kinds=("metric_threshold", "carrier_error_fraction"),
        injection_signals=(None, _combo_injection_signal("S03")),
        timing=ComboTimingProfile(
            name="d16_nested_pod_inner", geometry="nested_pod_inner",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-2 D16(d)：legacy=nested（pod INNER⊂net OUTER）＝默认（旧 _podfail_netdelay_mid_action "
                     "L15647-15671；gate deep_podfail_netdelay_masking_gate L8201）：net F1 (30,135) 整窗注入相位"
                     "先行（CRD 秒级→settled ~32；duration_s=105 覆盖窗），catalog pod F2 (55,115) below_kill_band v2 "
                     "子窗 mid-during（60s 窗/CRD 90s；EXEC-POD-XFER 2026-09-22：实测 lag 头 35s 由 pod 族 carrier "
                     "settle 吸收，评估窗 ~34 样本≥地板 30，有效下线段 [~89,~126] 落 net 稳态段→co-active ~37 "
                     "样本；偏移 {30,55,115,135} 最小正间隙 20＞默认预算和 19）；pod recover BLOCKS："
                     "recover@115 restore+Ready ~128 严格先于 net CRD 删除@135（不互污；ASM-1 D05 严序镜像）；"
                     "恢复 pod 先收→net 后收；hit_timeout=60 非默认 30 知识（netem 下 probe 慢，30s 只 ~3 轮达"
                     "不到 err≥0.6→窗头掺健康流量，B1-06 §2 D16）——新链咬中/有效性判定落位时继承该量级教训；"
                     "timing NOT_FROZEN"),
        interaction_expectation="fault_masking（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）——方向"
                                "假设：pod-down（lag 头 35s 后）错误在共路径上掩盖 delay 位移签名；bypass（direct）通道在 gw "
                                "外验证 pod 在场，net 腿在场由 CRD/transition 证据承担；masking_mode=error 配置"
                                "需 err 双阈值（pilot 标定）",
        reference_dependencies={"F1": ("S01",), "F2": ("S03",)},
        obs_r1_audit="PASS：live 容器={catalog-gw pod, catalog pod}，无交叉；main 载体 Service=catalog-gw 与 net "
                     "腿目标 pod 同名不同对象（Service≠CRI 目标，§4 D16 行备注）；CRI 目标从 pinned pods 派生、"
                     '单一 pod: 键型（OBS-collection）',
        preconditions=(
            _reference_ordering_precondition(
                "D16", "F1=S01 network_delay@catalog-gw, F2=S03 service_unavailable@catalog"),
            _b1_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C1/C4、§3 D16 行、§4、§8",
            "catalog_scenario": "D16 (origin=retained_composition 旧DK19; id_reused=False; legacy=nested/"
                                "fault_masking; composition_signature network_delay@catalog-gw || "
                                "service_unavailable@catalog; G=2)",
            "ledger": "m14-ledger-b1/03-network-chaos.md（gw delay 腿）+ m14-ledger-b1/05-pod-failure.md（catalog "
                      "pod 腿）",
            "origin": "retained_composition（旧 DK19）；legacy=nested 默认；hit_timeout=60 量级教训继承（观测设计"
                      "输入，非参数）",
            "parameter_status": "NOT_FROZEN（gw {latency_ms:500, jitter_ms:50, correlation_percent:0, duration_s:"
                                "105 覆盖组合窗} 沿 S01 候选（C1）；pod {action: pod-failure, duration_s: 90}="
                                "below_kill_band v2 剖面（C4）；MINOR-1 不变量 105≥105/90≥60 成立；"
                                "EXEC-POD-XFER 2026-09-22 重切）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D16 行：main=catalog-items-via-gw——net 外窗×pod "
                                      "内窗共路径主通道；bypass=catalog-direct——pod 错误在场、绕 gw 延迟＝掩盖"
                                      "判定旁路验证通道；pricing 重定向不涉（catalog-items-via-gw 流））",
            "timing_status": "NOT_FROZEN（legacy=nested 默认（pod INNER⊂net OUTER）；profile d16_nested_pod_inner；"
                             "pod recover BLOCKS ~35s 进预算）",
            "gt_correspondence": "net 腿=延迟通道（pricing 面板 p95 位移，main 通道）；pod 腿=error 通道（main+"
                                 "bypass 双面）＋restart_delta∈{1,2}；旧 gate deep_podfail_netdelay_masking_gate "
                                 "L8201 为阈值 provenance 候选锚",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D17": ComboSpec(
        scenario_id="D17", purpose="pilot", scope="m1_main",
        rule_version_base="d17-combo-pilot", random_seed=11,
        phases=D17_PHASES,
        legs=(_combo_leg("S18", D17_CHECKOUT_WINDOW),
              _combo_leg("S25", D17_INV_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d17-main-checkout-health", service="checkout:5011",
                              path_prefix="/health", carrier="combo-main-panel-checkout-health",
                              direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="d17-bypass-inventory-items", service="inventory:5013",
                              path_prefix="/api/inventory/", carrier="combo-bypass-panel-inventory-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("checkout-pod-logs"),
        signals=(_combo_signal("S18"), _combo_signal("S25")),
        injection_signal_kinds=("carrier_error_fraction", "metric_threshold"),
        injection_signals=(_combo_injection_signal("S18"), None),
        timing=ComboTimingProfile(
            name="d17_nested_pod_inner_env_outer", geometry="nested_pod_inner",
            inject_order=("F2", "F1"), recover_order=("F1", "F2"),
            decision="SPECS-2 D17(d)：legacy=staggered（旧组合定义 L13443-13461：checkout podfail 早子窗＋inv "
                     "set-env 于 podfail recover 之后注→错峰；DK11 地板 stage≥f2_offset+230 L13447-13448——参数"
                     "不照抄、手法作设计参考，B1-06 §2 D17）；蓝图两式均开放（装配记录定并写明），装配选择=inv "
                     "整窗＋checkout pod 子窗（蓝图第二式'inv 先注 checkout 子窗后'）：inv F2 (30,135) env "
                     "rollout 注入相位吸收（§8-2 env 腿与 CRD 腿同案时 env 先注；头带 15s→settled ~45），"
                     "checkout F1 (55,115) below_kill_band v2 子窗（std_20_3_3 剖面，60s 窗/CRD 90s；"
                     "EXEC-POD-XFER 2026-09-22：实测 lag 头 35s 由 pod 族 carrier settle 吸收，评估窗 ~34 样本"
                     "≥地板 30，有效下线段 [~89,~126] 落 inv 稳态 exposure [45,135]→co-active ~37 样本≥地板 30；"
                     "inv 窗 120→135 专为该有效 co-active 地板延长，env 腿无 CRD duration 参数；偏移 "
                     "{30,55,115,135} 最小正间隙 20＞默认预算和 19）；选择理由＝staggered 错峰的 restart 证据"
                     "分离目标由'pod 子窗完整落在 inv 稳态"
                     "段内＋恢复 reverse-nested（pod 先收@115，restore+Ready ~128 evidence-only；inv unset@135 "
                     "rollout ≤~150≤during 末 155）'同等承载，且与 §8-2 env-first 注入序知识一致；timing "
                     "NOT_FROZEN"),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）；"
                                "旧 gate multi_leg_retarget_gate L8422 系为 provenance 候选锚",
        reference_dependencies={"F1": ("S18",), "F2": ("S25",)},
        obs_r1_audit="PASS：live 容器={checkout pod, inventory pod}，两腿两容器一一对应、无交叉；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _reference_ordering_precondition(
                "D17", "F1=S18 service_unavailable@checkout, F2=S25 dependency_latency@inventory"),
            _b1_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C4/C9、§3 D17 行、§4、§8",
            "catalog_scenario": "D17 (origin=retained_composition 旧dual17; id_reused=False; legacy=staggered/"
                                "independent_parallel; composition_signature service_unavailable@checkout || "
                                "dependency_latency@inventory; G=2)",
            "ledger": "m14-ledger-b1/05-pod-failure.md（checkout pod 腿，C4 retarget 家族）+ m14-ledger-b1/"
                      "04-dependency-latency.md（inv 腿，S25 首用）",
            "origin": "retained_composition（旧 dual17）；legacy=staggered；蓝图两式均开放，装配取 inv 整窗＋"
                      "checkout 子窗式（选择理由在 timing.decision）；旧 DK11 地板公式参数不照抄",
            "parameter_status": "NOT_FROZEN（checkout {action: pod-failure, duration_s: 90}=below_kill_band v2 剖面"
                                "（C4 retarget 家族 std_20_3_3）；inv delay_ms=2000 沿 S25 候选（C9）；MINOR-1 "
                                "不变量 90≥60 成立；EXEC-POD-XFER 2026-09-22 重切（inv 窗 120→135 保有效 "
                                "co-active 地板））",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D17 行：main=checkout /health 固定——pod 错误在 lag 头（界 35s）后咬合；"
                                      "bypass=inventory-direct——inv 慢在场；pricing 重定向不涉）",
            "timing_status": "NOT_FROZEN（蓝图两式开放，装配选择 inv 整窗＋checkout pod 子窗（nested_pod_inner）；"
                             "profile d17_nested_pod_inner_env_outer，选择理由在 timing.decision）",
            "gt_correspondence": "pod 腿=error/restart（checkout_service error fraction＋restart_delta）；inv 腿="
                                 "延迟（inventory_service p95 位移）；旧 gate multi_leg_retarget_gate L8422 系为 "
                                 "provenance 候选锚",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D29": ComboSpec(
        scenario_id="D29", purpose="pilot", scope="m1_main",
        rule_version_base="d29-combo-pilot", random_seed=11,
        phases=COMBO2_NETPOD_PHASES,
        legs=(_combo_leg("S21", COMBO2_NETPOD_NET_WINDOW, duration_s=100),
              _combo_leg("S03", COMBO2_NETPOD_POD_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d29-main-catalog-items-via-gw", service="catalog-gw:80",
                              path_prefix="/api/items/", carrier="combo-main-panel-catalog-via-gw",
                              
                              direct_api=True, trace_service="catalog_service"),
            CarrierStreamSpec(stream_id="d29-bypass-recagent-health", service="rec-agent:5001",
                              path_prefix="/recommend/health", carrier="combo-bypass-panel-recagent-health",
                              direct_api=True, parameterized=False)),
        telemetry=_std_telemetry("rec-agent-pod-logs"),
        signals=(_combo_signal("S21"), _combo_signal("S03")),
        injection_signal_kinds=("metric_threshold", "carrier_error_fraction"),
        injection_signals=(None, _combo_injection_signal("S03")),
        timing=ComboTimingProfile(
            name="d29_partial_overlap_netem_outer_pod_inner", geometry="partial_overlap",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-2 D29(d)：legacy=null（新对，T07 拆对）；旧 T07 知识输入（F1 rec-agent netem＋F2 sasrec "
                     "整 during 预注＋F3 catalog pod mid INNER 子窗，recagent_netdelay_x_sasrec_cpu_x_catalog_"
                     "podfail L13464 起，B1-06 §2 D29）；采纳建议=partial_overlap：rec-agent netem F1 (30,130) 整窗"
                     "（全 egress 语义；duration_s=100 覆盖窗；CRD 秒级→settled ~32），catalog pod F2 (50,110) "
                     "below_kill_band v2 子窗（60s 窗/CRD 90s；EXEC-POD-XFER 2026-09-22：实测 lag 头 35s 由 pod "
                     "族 carrier settle 吸收，评估窗 ~34 样本≥地板 30，有效下线段 [~84,~121] 与 netem 稳态 "
                     "exposure [~32,131] 的 co-active ~37 样本≥地板 30；偏移 {30,50,110,130} 最小正间隙 20＞"
                     "默认预算和 19）；恢复 reverse：pod CRD "
                     "删除@110（restore+Ready ~123 于 during 末 135 内收口，evidence-only）→netem CRD 删除@130（秒级，"
                     "在 during 内）；timing NOT_FROZEN"),
        interaction_expectation="无旧标签（新对）——设计意图期望=independent_parallel（作用面分离：rec-agent "
                                "egress vs catalog pod）（期望非观测：设计意图期望，非观测结论；走 C1 §0 B "
                                "观测门）",
        reference_dependencies={"F1": ("S21",), "F2": ("S03",)},
        obs_r1_audit="PASS：live 容器={rec-agent pod, catalog pod}，无交叉；rec-agent 目标键按实际 pod 名"
                     "（app=recommendation_agent 标签映射纪律，§4 规避 3）；CRI 目标从 pinned pods 派生、单一 "
                     'pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S21"].preconditions[0],     # netem face first-use smoke (binds this combo too)
            ATOM_SPECS["S21"].preconditions[1],     # live-label pin (recommendation_agent, F-S1-3)
            _reference_ordering_precondition(
                "D29", "F1=S21 network_delay@rec-agent, F2=S03 service_unavailable@catalog"),
            _b1_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C3/C4、§3 D29 行、§4、§8",
            "catalog_scenario": "D29 (origin=triple_pair_completion T07 拆对; id_reused=False; legacy=null; "
                                "composition_signature network_delay@rec-agent || service_unavailable@catalog; "
                                "G=2)",
            "ledger": "m14-ledger-b1/03-network-chaos.md（rec-agent netem 腿，S21 首用）+ m14-ledger-b1/"
                      "05-pod-failure.md（catalog pod 腿）",
            "origin": "triple_pair_completion（T07 拆对）；承旧 T07 形态（netem 整窗＋pod mid INNER 子窗）；S21 "
                      "首用冒烟前置同样约束本组（蓝图 (f)）",
            "parameter_status": "NOT_FROZEN（rec-agent {latency_ms:450, jitter_ms:90, correlation_percent:0, "
                                "duration_s:100 覆盖组合窗} 沿 S21 候选（C3；全 egress 语义，禁把 gw 单边档照搬）；"
                                "pod {action: pod-failure, duration_s: 90}=below_kill_band v2 剖面（C4）；MINOR-1 "
                                "不变量 100≥100/90≥60 成立；EXEC-POD-XFER 2026-09-22 重切）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D29 行：main=catalog-items-via-gw——catalog pod 受害"
                                      "主通道；bypass=rec-agent /recommend/health 固定——rec-agent delay 在场；"
                                      "pricing 重定向不涉（catalog-items-via-gw 流））",
            "timing_status": "NOT_FROZEN（legacy=null 新对；partial_overlap 承旧 T07 形态；profile "
                             "d29_partial_overlap_netem_outer_pod_inner，选择理由在 timing.decision）",
            "gt_correspondence": "net 腿=延迟通道（recommendation_agent /recommend/health p95 位移）；pod 腿="
                                 "error/restart（catalog_service error fraction＋restart_delta）；T07 三腿子对"
                                 "闭合={D28, D30, D29}（CROSS-CUTTING §7）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    # --- COMBO-ASM-3: SPECS-3 CPU-mixed family (D03/D09/D14/D15/D19/D21/ ---- #
    # --- D26/D28/D30/D31) --------------------------------------------------- #
    "D03": ComboSpec(
        scenario_id="D03", purpose="pilot", scope="m1_main",
        rule_version_base="d03-combo-pilot", random_seed=11,
        phases=D03_PHASES,
        legs=(_combo_leg("S05", D03_HOST_INNER_WINDOW, duration_s=45),
              _combo_leg("S04", D03_CATALOG_OUTER_WINDOW, duration_s=85)),
        carriers=(
            CarrierStreamSpec(stream_id="d03-main-catalog-items-cpu", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-main-panel-catalog-direct",
                              direct_api=True),
            CarrierStreamSpec(stream_id="d03-bypass-pricing-direct", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-bypass-panel-pricing-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("stressor-pod-logs"),
        signals=(_combo_signal("S05"), _combo_signal("S04")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_p95_ratio"),
        injection_signals=(_combo_injection_signal("S05"), _combo_injection_signal("S04")),
        timing=ComboTimingProfile(
            name="d03_nested_host_inner_catalog_first", geometry="nested",
            inject_order=("F2", "F1"), recover_order=("F1", "F2"),
            decision="SPECS-3 D03(d)：legacy=nested＝默认，窗几何按新时序合同重述（C1 §2.6/§8-1；改公共"
                     "目标窗=窗几何变化须记录）：catalog cpu OUTER (30,115) 先注（旧链注入序知识"
                     "catalog-first-on-idle-VM L15145 必须保留——反序则 catalog AllInjected poll 须穿饿死 "
                     "chaos-controller，30s flaky；CRD 秒级＋饱和爬坡保守 15s 头预算→settled ~45），host "
                     "INNER (60,105) 后注（stressor 载体 CRD；settled ~75）；F2_only 稳态 45→75=30 与 "
                     "overlap 稳态 75→105=30 均达 C1 §2.5 地板（§8-6 子窗预算；旧 F2_OFFSET/DURATION 不"
                     "照抄）；恢复 reverse-nested＝旧链恢复序（host CRD 删除@105 先、catalog@115 后，"
                     "L16142-16143 镜像），秒级删除≤120≤during 末 120；注：蓝图 legacy 括注『host 窗⊃"
                     "catalog 窗』与其自述注入/恢复序（catalog 先注后收=OUTER）互倒，装配绑定必须保留"
                     "的注入顺序知识，catalog OUTER⊃host INNER；timing NOT_FROZEN"),
        interaction_expectation="fault_masking（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）——"
                                "host 饱和压低 catalog cpu 腿主通道签名；被掩盖腿主通道签名被压至参照下容差"
                                "之下、旁路通道仍验证其在场（C1 §1）；本组 bypass（pricing-direct，host 多受"
                                "害者臂）即 host 在场验证通道；throttle 面板不可见先例=如实跳过不凑数（B3-01 "
                                "§5-3）",
        reference_dependencies={"F1": ("S05",), "F2": ("S04",)},
        obs_r1_audit="PASS：live 容器={stressor pod, catalog pod}（host 腿容器=stressor 载体，非 victim "
                     "pods）；两腿两容器一一对应、无交叉；CRI 目标从 pinned pods 派生、单一 pod: 键型"
                     "（OBS-R1）",
        preconditions=(
            ATOM_SPECS["S05"].preconditions[0],      # stressor carrier provisioning/cleanup
            ATOM_SPECS["S05"].preconditions[1],      # workers=32 node-topology coupling
            _d03_catalog_first_precondition(),
            _reference_ordering_precondition(
                "D03", "F1=S05 host_cpu_saturation@host, F2=S04 service_cpu_saturation@catalog"),
            _cpu_mixed_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C6/C5、§3 D03 行、§4、§5、§8",
            "catalog_scenario": "D03 (origin=retained_composition 旧同号; id_reused=False; legacy=nested/"
                                "fault_masking; composition_signature host_cpu_saturation@host || "
                                "service_cpu_saturation@catalog; G=2)",
            "ledger": "m14-ledger-b1/02-host-cpu.md（host 腿）+ m14-ledger-b3/01-service-cpu.md（catalog "
                      "cpu 腿，workers=1 标定）+ m14-ledger-b1/06-design-level-mapping.md §2 D03",
            "origin": "retained_composition（旧同号 host_cpu_x_svccpu）；legacy=nested（默认）；注入序知识"
                      "catalog-first 必须保留（硬化为 Precondition）",
            "parameter_status": "NOT_FROZEN（host {workers:32, load_percent:100, duration_s:45 覆盖组合 "
                                "INNER 窗} 沿 S05 候选——绝不 --vm；catalog {workers:1（500m 单 worker 饱和"
                                "标定，T05/T08 腿源）, load_percent:100, duration_s:85 覆盖组合 OUTER 窗} 沿 "
                                "S04 候选；combo 下 catalog throttle 被压至 ~0.0102（旧 L361-364 首注）→eps "
                                "0.003 razor-thin 知识进 Q 标定，B3-03 §3.1 D03 行）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D03 行：main=catalog-direct——被掩盖腿主通道"
                                      "（catalog cpu 签名）；bypass=pricing-direct——host 多受害者臂=host 在场"
                                      "证据；pricing-direct 流只涉 pricing scale-up 前提（§3 约束 2），不涉"
                                      "重定向）",
            "timing_status": "NOT_FROZEN（legacy=nested 默认，窗几何按新时序合同重述=catalog OUTER⊃host "
                             "INNER（注入序知识绑定）；profile d03_nested_host_inner_catalog_first；改公共"
                             "目标窗须记录）",
            "gt_correspondence": "host 腿=多受害者 p95 相对比 ≥1.8× 候选＋churn restart_delta=0（观测设计；"
                                 "旧 disjoint user 对照属观测设计不默认继承）；catalog cpu 腿=ratio＋per-pod "
                                 "cfs_throttle 可分判据（OPEN-Q3 新 Q 缺位）；旧 gate fault_masking_gate L3901"
                                 "（cfs_throttle per-root 可分，THROTTLE_EPS=0.003 L365）为 provenance 候选"
                                 "锚；throttle 面板不可见先例=如实跳过（B3-01 §5-3）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'},
        # COMBO-ASM-4 NIT-C: the must-preserve catalog-first direction knowledge
        # now carries a construction-level guard (see ComboSpec field comment);
        # a consistent window/order/fixture swap fails loud instead of riding.
        must_inject_first_entity="catalog"),
    "D09": ComboSpec(
        scenario_id="D09", purpose="pilot", scope="m1_main",
        rule_version_base="d09-combo-pilot", random_seed=11,
        phases=D09_PHASES,
        legs=(_combo_leg("S27", D09_SASREC_WINDOW, duration_s=90),
              _combo_leg("S01", D09_GW_INNER_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d09-main-sasrec-inference-post-v1", service="sasrec:8200",
                              path_prefix="/recommend", carrier="combo-main-panel-sasrec-inference-post-v1",
                              direct_api=True, rate_rps=1.0, max_concurrency=1,
                              client_retry_limit=0, parameterized=False),
            CarrierStreamSpec(stream_id="d09-bypass-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-bypass-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service")),
        telemetry=_std_telemetry("sasrec-pod-logs"),
        signals=(_combo_signal("S27"), _combo_signal("S01")),
        injection_signal_kinds=("carrier_p95_ratio", "metric_threshold"),
        injection_signals=(_combo_injection_signal("S27"), None),
        timing=ComboTimingProfile(
            name="d09_partial_overlap_sasrec_outer_net_inner", geometry="partial_overlap",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-3 D09(d)：legacy=partial_overlap＝默认（旧 sasrec_cpu_x_catalog_netdelay：sasrec "
                     "整 during 头 L15262、MID-during recover L15831-15836；B1-06 §2 D09）；采纳蓝图建议＝"
                     "保留 sasrec 整窗＋gw 子窗 partial_overlap（三段式语义：F2_only 尾段由 OUTER 腿尾段替"
                     "代，§8-4 显式声明）：sasrec F1 (30,120) 整窗预注（CRD 秒级＋爬坡 15s 头预算→settled "
                     "~45；duration_s=90 覆盖窗），gw F2 (80,115) CRD 秒级 mid 注（settled ~82）；F1_only "
                     "稳态 45→80=35 与 overlap 稳态 ~82→115=33 均达 C1 §2.5 地板（§8-6；旧 F2_OFFSET=14/"
                     "F2_DURATION=31 不照抄）；恢复 reverse：gw CRD 删除@115（秒级）→sasrec CRD 删除@120"
                     "（≤121≤during 末 125）；旧 _wait_recommend_fast(<300ms) 稳定快恢复窗对齐在新链由相位"
                     "事件＋Q 复位判据承接（B3-01 §3.2-5，DK12_RECOMMEND_FAST_MS L339 为候选锚）；timing "
                     "NOT_FROZEN",
        ),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测"
                                "门）——两腿独立签名通道分离（sasrec /recommend inference p95 ratio vs gw 路径 p95 位移）",
        reference_dependencies={"F1": ("S27",), "F2": ("S01",)},
        obs_r1_audit="PASS：live 容器={sasrec pod, catalog-gw pod}，两腿两容器一一对应、无交叉；CRI 目标"
                     '从 pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S27"].preconditions[0],      # sasrec no-restart pin semantics
            ATOM_SPECS["S27"].preconditions[1],      # workers=8 quota/topology coupling
            ATOM_SPECS["S01"].preconditions[0],      # pricing redirect (bypass stream premise)
            _reference_ordering_precondition(
                "D09", "F1=S27 service_cpu_saturation@sasrec, F2=S01 network_delay@catalog-gw"),
            _cpu_mixed_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C5/C1、§3 D09 行、§4、§5、§7、§8",
            "catalog_scenario": "D09 (origin=retained_composition 旧DK12; id_reused=False; legacy="
                                "partial_overlap/independent_parallel; composition_signature "
                                "service_cpu_saturation@sasrec || network_delay@catalog-gw; G=2)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（sasrec 腿，W=8 拓扑绑定＋不重启）+ m14-ledger-b1/"
                      "03-network-chaos.md（gw delay 腿）+ B3-03 §3.1 D09 行",
            "origin": "retained_composition（旧 DK12）；legacy=partial_overlap 默认保持（三段式，恢复 "
                      "reverse）；T06 三腿子对闭合={D27, D26, D09}（CROSS-CUTTING §7）；S27=atomic_"
                      "completion 首用（W=8 拓扑指纹绑定）",
            "parameter_status": "NOT_FROZEN（sasrec {workers:8（canonical 全曲线 sweep 定案，2× 4 核 pod 配额"
                                "，绑定拓扑指纹）, load_percent:100, duration_s:90 覆盖组合窗} 沿 S27 候选；"
                                "gw {latency_ms:500, jitter_ms:50, correlation_percent:0, duration_s:60} 沿 "
                                "S01 候选，60≥35 覆盖子窗（MINOR-1））",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D09 行：main=sasrec versioned /recommend inference POST——sasrec 受害"
                                      "（RATIO 禁绝对：W=8 绝对 979ms 但 W=6 444.7ms<800 会误 FAIL，旧 "
                                      "L7575-7576 教训）；bypass=pricing-via-gw——gw delay 通道；前提=pricing "
                                      "重定向）",
            "timing_status": "NOT_FROZEN（legacy=partial_overlap 默认保持＋恢复 reverse；profile "
                             "d09_partial_overlap_sasrec_outer_net_inner）",
            "gt_correspondence": "sasrec 腿=ratio 臂（载体 SOFT 警示：与 backend 共 recommend 路径不可二分"
                                 "——本组与 gw 腿归因靠各自独立通道）＋per-pod throttle 证据（OPEN-Q3）；net 腿"
                                 "=延迟臂（pricing_service p95 位移）；恢复窗推荐 <300ms 稳定快为 provenance "
                                 "候选锚（DK12_RECOMMEND_FAST_MS L339）；旧 gate sasrec_net_dual_gate L7560 "
                                 "为阈值 provenance 候选锚",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D14": ComboSpec(
        scenario_id="D14", purpose="pilot", scope="m1_main",
        rule_version_base="d14-combo-pilot", random_seed=11,
        phases=COMBO3_NETCPU_PHASES,
        legs=(_combo_leg("S01", COMBO3_NETCPU_NET_WINDOW, duration_s=72),
              _combo_leg("S04", COMBO3_NETCPU_CPU_WINDOW, duration_s=80)),
        carriers=(
            CarrierStreamSpec(stream_id="d14-main-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-main-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service"),
            CarrierStreamSpec(stream_id="d14-bypass-catalog-items-cpu", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-bypass-panel-catalog-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("catalog-gw-pod-logs"),
        signals=(_combo_signal("S01"), _combo_signal("S04")),
        injection_signal_kinds=("metric_threshold", "carrier_p95_ratio"),
        injection_signals=(None, _combo_injection_signal("S04")),
        timing=ComboTimingProfile(
            name="d14_common_target_window_cpu_first", geometry="common_target_window",
            inject_order=("F2", "F1"), recover_order=("F1", "F2"),
            decision="SPECS-3 D14(d)：legacy=simultaneous＝默认（旧 net_delay_x_svccpu：svccpu=F2 OUTER 先注"
                     " L15282、恢复后收 L16078；gate deep_netdelay_svccpu_gate L5633）；simultaneous 语义在"
                     "新链由公共目标窗承载（§8-1）：cpu F2 (30,110) 先注落 stable pod（CRD 秒级＋爬坡 15s "
                     "头预算→settled ~45；duration_s=80 覆盖窗），net F1 (33,105) 紧随（CRD 秒级→settled "
                     "~35；duration_s=72 覆盖窗）；双活稳态 45→105=60 样本≥C1 §2.5 地板 30；恢复 reverse："
                     "net CRD 删除@105→cpu CRD 删除@110（秒级，≤111≤during 末 115）；注入序取 cpu-first＝旧"
                     "形态 svccpu 先注（L15282）同构＋§8-2 轻腿先注原则；嵌套副产物（F1⊂F2）显式声明"
                     "（§8-4）；timing NOT_FROZEN"),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测"
                                "门）——gw→catalog 边 delay＋cpu 叠加于 main 通道，两腿在场证据通道分离"
                                "（pricing 位移 vs catalog-direct ratio）",
        reference_dependencies={"F1": ("S01",), "F2": ("S04",)},
        obs_r1_audit="PASS：live 容器={catalog-gw pod, catalog pod}，两腿两容器一一对应、无交叉；注意 "
                     "catalog-gw 既是腿1目标 pod 又是 main 载体路径上的 Service 名（pricing 经 gw）——"
                     "Service 对象非 CRI 目标，无键冲突；CRI 目标从 pinned pods 派生、单一 pod: 键型"
                     "（OBS-R1）",
        preconditions=(
            ATOM_SPECS["S01"].preconditions[0],      # pricing redirect (main stream premise)
            _reference_ordering_precondition(
                "D14", "F1=S01 network_delay@catalog-gw, F2=S04 service_cpu_saturation@catalog"),
            _cpu_mixed_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C1/C5、§3 D14 行、§4、§5、§8",
            "catalog_scenario": "D14 (origin=retained_composition 旧DK17; id_reused=False; legacy="
                                "simultaneous/independent_parallel; composition_signature "
                                "network_delay@catalog-gw || service_cpu_saturation@catalog; G=2)",
            "ledger": "m14-ledger-b1/03-network-chaos.md（gw delay 腿）+ m14-ledger-b3/01-service-cpu.md"
                      "（catalog cpu 腿，workers 档与 S04 联动论证）",
            "origin": "retained_composition（旧 DK17）；legacy=simultaneous（公共目标窗承载）；注入序="
                      "cpu-first（旧形态 svccpu 先注同构＋§8-2 轻腿先注）",
            "parameter_status": "NOT_FROZEN（gw {latency_ms:500, jitter_ms:50, correlation_percent:0, "
                                "duration_s:72 覆盖组合窗} 沿 S01 候选（C1）；catalog {workers:1（与 S04 "
                                "联动论证）, load_percent:100, duration_s:80 覆盖组合窗} 沿 S04 候选（C5）；"
                                "MINOR-1 不变量 72≥72/80≥80 成立）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D14 行：main=pricing-via-gw——gw→catalog 边："
                                      "delay＋cpu 叠加；bypass=catalog-direct——catalog cpu 在场（正则排 "
                                      "catalog-gw）；前提=pricing 重定向）",
            "timing_status": "NOT_FROZEN（legacy=simultaneous 由公共目标窗承载＋cpu-first＋恢复 reverse；"
                             "profile d14_common_target_window_cpu_first）",
            "gt_correspondence": "net 腿=延迟臂（pricing_service p95 位移）；cpu 腿=ratio 臂＋ok≥0.8＋throttle"
                                 "（正则排 catalog-gw，L13498）；旧 gate deep_netdelay_svccpu_gate L5633 为 "
                                 "provenance 候选锚",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D15": ComboSpec(
        scenario_id="D15", purpose="pilot", scope="m1_main",
        rule_version_base="d15-combo-pilot", random_seed=11,
        phases=D15_PHASES,
        legs=(_combo_leg("S08", D15_CATLAT_WINDOW),
              _combo_leg("S28", D15_PRICING_CPU_WINDOW, duration_s=70)),
        carriers=(
            CarrierStreamSpec(stream_id="d15-main-pricing-items-cpu", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-main-panel-pricing-direct",
                              direct_api=True),
            CarrierStreamSpec(stream_id="d15-bypass-catalog-items", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-bypass-panel-catalog-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("catalog-pod-logs"),
        signals=(_combo_signal("S08"), _combo_signal("S28")),
        injection_signal_kinds=("metric_threshold", "carrier_p95_ratio"),
        injection_signals=(None, _combo_injection_signal("S28")),
        timing=ComboTimingProfile(
            name="d15_common_target_window_catlat_first", geometry="common_target_window",
            inject_order=("F1", "F2"), recover_order=("F1", "F2"),  # collection: same-end tie runs in leg order
            decision="SPECS-3 D15(d)：legacy=null（T01 拆对新对，id_reused=True——旧 D15=catlat×catalog-cpu "
                     "已并入 S08，不得按旧 D15 实现）；采纳蓝图建议＝公共目标窗（两腿 during 前就位）："
                     "catlat F1 (30,120) env rollout 先注吸收（头带 15s→settled ~45，旧 T01 catlat-first "
                     "L15304-15312 知识——env 腿先注吸收 rollout），pricing cpu F2 (50,120) CRD 秒级后注落 "
                     'stable pod（settled ~65；duration_s=70 覆盖窗；collection 45/110→50/120，T01 子窗同值断言解除）；双活稳态 '
                     "65→120=55 样本≥C1 §2.5 地板 30；恢复 reverse 同端@120：pricing CRD 删除（秒级）→catlat "
                     'env unset 串行（rollout ≤~138≤during 末 140；collection 同端声明在案）；嵌套副产物（F2⊂F1）显式声明（§8-4）；'
                     "timing NOT_FROZEN"),
        interaction_expectation="无旧标签（新对）——设计意图期望=independent_parallel（空间可分：不同 "
                                "cgroup）（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测门）；"
                                "C1 前提(a)：T01→新 D15 参照闭合已点名（C1 §0）",
        same_offset_budgets={frozenset(("F1", "F2")):
            'D15 recover-recover 同端对 offset 120 显式预算声明（collection pricing 45/110→50/120 后落同端；机制先例＝'
            'collection 同端恢复对＋collection GW 族同端重切；预算算术与本 spec timing.decision 同源）：恢复按声明序（＝腿序）'
            "F1（catalog env unset，rollout ≤~15s→≤~135）→F2（pricing StressChaos CRD 删除，秒级）串行完成 "
            "≤~138 ≤ during 末 140；最小正间隙 20＞预算和 19，无正间隙对逃逸 O-D3。"},
        reference_dependencies={"F1": ("S08",), "F2": ("S28",)},
        obs_r1_audit="PASS：live 容器={catalog pod, pricing pod}，两腿两容器一一对应、无交叉；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _reference_ordering_precondition(
                "D15", "F1=S08 dependency_latency@catalog, F2=S28 service_cpu_saturation@pricing"),
            _cpu_mixed_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C8/C5、§3 D15 行、§4、§5、§7、§8",
            "catalog_scenario": "D15 (origin=triple_pair_completion T01 拆对; id_reused=True〔新编号新含义〕; "
                                "legacy=null; composition_signature dependency_latency@catalog || "
                                "service_cpu_saturation@pricing; G=2)",
            "ledger": "m14-ledger-b1/04-dependency-latency.md（catlat 腿）+ m14-ledger-b3/01-service-cpu.md"
                      "（pricing cpu 腿，SOFT 载体警示）+ B1-06 §2 D15",
            "origin": "triple_pair_completion（T01 拆对）；场景定义约束：CPU 目标已变为 pricing——"
                      "selector/强度/carrier/GT/QC 全部按新目标（S28），不得按旧 D15（catalog CPU）实现；"
                      "T01 三腿子对闭合={D15, D12, D22}（CROSS-CUTTING §7）；S28=atomic_completion 首用",
            "parameter_status": "NOT_FROZEN（catlat delay_ms=2000 沿 S08 候选（C8）；pricing {workers:2（500m）"
                                ", load_percent:100, duration_s:65 覆盖组合窗=T01 pricing 子窗同值} 沿 S28 "
                                "候选（C5，载体臂 SOFT）；MINOR-1 不变量 65≥65 成立）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D15 行：main=pricing-direct——pricing cpu 受害"
                                      "（SOFT 警示）；bypass=catalog-direct——catlat 在场；pricing-direct 流"
                                      "只涉 pricing scale-up 前提（§3 约束 2），不涉重定向）",
            "timing_status": "NOT_FROZEN（legacy=null 新对，公共目标窗＋catlat env 先注吸收＋恢复 reverse；"
                             "profile d15_common_target_window_catlat_first，选择理由在 timing.decision）",
            "gt_correspondence": "catlat 腿=延迟臂（catalog_service p95 位移＋ok≥0.8）；pricing 腿=ratio（SOFT）"
                                 "＋throttle/自载体证据优先；旧 gate deep_triple_pricing_cat_cfg_gate（per_"
                                 "root_RES＋空间见证）为 provenance 候选锚",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D19": ComboSpec(
        scenario_id="D19", purpose="pilot", scope="m1_main",
        rule_version_base="d19-combo-pilot", random_seed=11,
        phases=COMBO3_CPUPOD_PHASES,
        legs=(_combo_leg("S19", COMBO3_CPUPOD_POD_WINDOW),
              _combo_leg("S11", COMBO3_CPUPOD_CPU_WINDOW, duration_s=105)),
        carriers=(
            CarrierStreamSpec(stream_id="d19-main-search-list", service="search:5017",
                              path_prefix="/api/search?q=phone&per_page=5&enrich=1",
                              carrier="combo-main-panel-search-list", direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="d19-bypass-review-query-list", service="review-query:5018",
                              path_prefix="/api/reviews?item_id=", path_suffix="&per_page=5&enrich=1",
                              carrier="combo-bypass-panel-review-query-list", direct_api=True)),
        telemetry=_std_telemetry("search-pod-logs"),
        signals=(_combo_signal("S19"), _combo_signal("S11")),
        injection_signal_kinds=("carrier_error_fraction", "carrier_p95_ratio"),
        injection_signals=(_combo_injection_signal("S19"), _combo_injection_signal("S11")),
        timing=ComboTimingProfile(
            name="d19_partial_overlap_cpu_outer_pod_inner", geometry="partial_overlap",
            inject_order=("F2", "F1"), recover_order=("F1", "F2"),
            decision="SPECS-3 D19(d)：legacy=partial_overlap＝默认（旧 dual19：search podfail F1 子窗＋"
                     "review-query cpu 整窗；两腿共享下游 catalog enrich（fan-in）、disjoint=user，B1-06 §2 "
                     "D19）；采纳蓝图建议＝cpu 整窗（CRD 预注）＋pod 子窗 mid（EXEC-POD-XFER 2026-09-22 重切：实测传递函数＝"
                     "spec 补丁对运行中容器不立即生效，咬合点=inject 确认后 lag 28.8-30.9s（声明界 35＝pod 族 "
                     "carrier settle，r10c fix-6 实测 statistic 16/38=0.42<0.5 即旧窗稀释所致；阈值/required/"
                     "n≥30 全保持）：rq cpu F2 (30,135) 整窗预注"
                     "（CRD 秒级＋爬坡 15s 头预算→settled ~45；duration_s=105 覆盖窗），search pod F1 (55,115) "
                     "below_kill_band v2 子窗（60s 窗/CRD 90s，评估窗 [inject确认+35, recover确认] ~34 样本≥"
                     "C1 地板 30；有效下线段 [~89,~126] 落 cpu 稳态段→co-active ~37 样本≥地板；偏移 "
                     "{30,55,115,135} 最小正间隙 20＞默认预算和 19）；恢复 reverse＋restart 严序：pod recover@115 "
                     "的 restore+Ready ~13s（recover 动作内实测确认，~128）于 "
                     "during 末 140 内收口（evidence-only 绝不 abort；restart_delta 窗归属按窗尾切分，不溢入"
                     "cpu 窗），rq CRD 删除@135（秒级，≤136≤during 末 140）；timing NOT_FROZEN"),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测"
                                "门）；旧 gate 走 multi_leg_retarget_gate L8422 系（约束 E 臂）为 provenance "
                                "候选锚",
        reference_dependencies={"F1": ("S19",), "F2": ("S11",)},
        obs_r1_audit="PASS：live 容器={search pod, review-query pod}，两腿两容器一一对应、无交叉；CRI 目标"
                     '从 pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _reference_ordering_precondition(
                "D19", "F1=S19 service_unavailable@search, F2=S11 service_cpu_saturation@review-query"),
            _cpu_mixed_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C4/C5、§3 D19 行、§4、§5、§8",
            "catalog_scenario": "D19 (origin=retained_composition 旧dual19; id_reused=False; legacy="
                                "partial_overlap/independent_parallel; composition_signature "
                                "service_unavailable@search || service_cpu_saturation@review-query; G=2)",
            "ledger": "m14-ledger-b1/05-pod-failure.md（search pod 腿，SS2.3 D3 selector 扩展）+ "
                      "m14-ledger-b3/01-service-cpu.md（rq cpu 腿）",
            "origin": "retained_composition（旧 dual19）；legacy=partial_overlap 默认保持（cpu 整窗＋pod "
                      "子窗）",
            "parameter_status": "NOT_FROZEN（search {action: pod-failure, duration_s: 90}=below_kill_band v2 剖面"
                                "（C4 std 剖面）；rq {workers:2 候选, load_percent:100, duration_s:105 覆盖组"
                                "合窗} 沿 S11 候选（C5）；MINOR-1 不变量 90≥60/105≥105 成立；EXEC-POD-XFER "
                                "2026-09-22 重切）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D19 行：main=search /api/search 固定——pod 错误"
                                      "在 lag 头（界 35s）后咬中（q/per_page/enrich 无凭据形状，enrich=1 catalog fan-in 属"
                                      "panel-proven 只读路径）；bypass=rq /api/reviews——cpu 在场；pricing "
                                      "重定向不涉）",
            "timing_status": "NOT_FROZEN（legacy=partial_overlap 默认保持＋恢复 reverse＋restart 严序；"
                             "profile d19_partial_overlap_cpu_outer_pod_inner）",
            "gt_correspondence": "pod 腿=error/restart（search_service error fraction＋restart_delta∈{1,2} 窗"
                                 "签名）；cpu 腿=ratio（review_query_service）＋ok≥0.8＋throttle（OPEN-Q3）；"
                                 "两腿共享下游 catalog enrich（fan-in）知识进观测设计（disjoint=user）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D21": ComboSpec(
        scenario_id="D21", purpose="pilot", scope="m1_main",
        rule_version_base="d21-combo-pilot", random_seed=11,
        phases=COMBO3_CPUPOD_PHASES,
        legs=(_combo_leg("S26", COMBO3_CPUPOD_POD_WINDOW),
              _combo_leg("S12", COMBO3_CPUPOD_CPU_WINDOW, duration_s=105)),
        carriers=(
            CarrierStreamSpec(stream_id="d21-main-backend-stats", service="backend:5000",
                              path_prefix="/api/stats/model", carrier="combo-main-panel-backend-stats",
                              direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="d21-bypass-user-health", service="user:5004",
                              path_prefix="/health", carrier="combo-bypass-panel-user-health",
                              direct_api=True, parameterized=False)),
        telemetry=_std_telemetry("user-pod-logs"),
        signals=(_combo_signal("S26"), _combo_signal("S12")),
        injection_signal_kinds=("pod_restart_delta", "carrier_p95_ratio"),
        injection_signals=(_combo_injection_signal("S26"), _combo_injection_signal("S12")),
        timing=ComboTimingProfile(
            name="d21_partial_overlap_cpu_outer_pod_inner", geometry="partial_overlap",
            inject_order=("F2", "F1"), recover_order=("F1", "F2"),
            decision="SPECS-3 D21(d)：legacy=partial_overlap＝默认（旧 dual21：user podfail 子窗＋backend cpu "
                     "整窗；G2ext 唯一 disjoint 对照组〔user⊥backend，disjoint=announcement〕，B1-06 §2 "
                     "D21）；采纳蓝图建议＝cpu 整窗＋pod 子窗（EXEC-POD-XFER 2026-09-22 重切，同 D19 形）：backend cpu "
                     "F2 (30,135) 整窗预注（CRD 秒级＋"
                     "爬坡 15s 头预算→settled ~45；duration_s=105 覆盖窗），user pod F1 (55,115) below_kill_"
                     "band v2 子窗（60s 窗/CRD 90s；lag 头 35s 由 pod 族 carrier settle 吸收；有效下线段 "
                     "[~89,~126] 落 cpu 稳态段→co-active ~37 样本；偏移 {30,55,115,135} 最小正间隙 20＞默认"
                     "预算和 19）；恢复 reverse：pod recover@115（restore+Ready ~128 during 内收口，"
                     "evidence-only，restart_delta 窗归属不溢入 cpu 窗）→backend CRD "
                     "删除@135（秒级，≤136≤during 末 140）；timing NOT_FROZEN"),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测"
                                "门）；disjoint 对照知识（user⊥backend，disjoint=announcement）进观测设计",
        reference_dependencies={"F1": ("S26",), "F2": ("S12",)},
        obs_r1_audit="PASS：live 容器={user pod, backend pod}，两腿两容器一一对应、无交叉；backend 键按实际"
                     " pod 名（app=backend_api 标签映射纪律，§4 规避 3）；CRI 目标从 pinned pods 派生、"
                     '单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S26"].preconditions[0],      # leaf-victim restart/ready detection basis
            ATOM_SPECS["S12"].preconditions[0],      # backend live-label pin (app=backend_api, F-S1-3)
            _reference_ordering_precondition(
                "D21", "F1=S26 service_unavailable@user, F2=S12 service_cpu_saturation@backend"),
            _cpu_mixed_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C4/C5、§3 D21 行、§4、§5、§8",
            "catalog_scenario": "D21 (origin=retained_composition 旧dual21; id_reused=False; legacy="
                                "partial_overlap/independent_parallel; composition_signature "
                                "service_unavailable@user || service_cpu_saturation@backend; G=2)",
            "ledger": "m14-ledger-b1/05-pod-failure.md（user pod 腿，叶子口径 SS2）+ m14-ledger-b3/"
                      "01-service-cpu.md（backend cpu 腿，app=backend_api 映射纪律）",
            "origin": "retained_composition（旧 dual21）；legacy=partial_overlap 默认保持（cpu 整窗＋pod "
                      "子窗）",
            "parameter_status": "NOT_FROZEN（user {action: pod-failure, duration_s: 90}=below_kill_band v2 剖面"
                                "（C4 叶子变体）；backend {workers:2 候选, load_percent:100, duration_s:105 "
                                "覆盖组合窗} 沿 S12 候选（C5）；MINOR-1 不变量 90≥60/105≥105 成立；"
                                "EXEC-POD-XFER 2026-09-22 重切）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D21 行：main=backend /api/stats/model 固定——"
                                      "cpu 受害；bypass=user /health——叶子在场/恢复（检测口径=restart/ready，"
                                      "无 carrier 错误通道）；pricing 重定向不涉）",
            "timing_status": "NOT_FROZEN（legacy=partial_overlap 默认保持＋恢复 reverse；profile "
                             "d21_partial_overlap_cpu_outer_pod_inner）",
            "gt_correspondence": "cpu 腿=backend_api ratio＋ok≥0.8＋throttle（OPEN-Q3）；user 腿=pod_restart_"
                                 "delta（叶子：restart/ready 观察口径 evidence-only，restart_delta∈{1,2} "
                                 "窗签名）；旧 G2ext disjoint 对照组知识进观测设计",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D26": ComboSpec(
        scenario_id="D26", purpose="pilot", scope="m1_main",
        rule_version_base="d26-combo-pilot", random_seed=11,
        phases=COMBO3_NETCPU_PHASES,
        legs=(_combo_leg("S01", COMBO3_NETCPU_NET_WINDOW, duration_s=72),
              _combo_leg("S12", COMBO3_NETCPU_CPU_WINDOW, duration_s=80)),
        carriers=(
            CarrierStreamSpec(stream_id="d26-main-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-main-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service"),
            CarrierStreamSpec(stream_id="d26-bypass-backend-stats", service="backend:5000",
                              path_prefix="/api/stats/model", carrier="combo-bypass-panel-backend-stats",
                              direct_api=True, parameterized=False)),
        telemetry=_std_telemetry("catalog-gw-pod-logs"),
        signals=(_combo_signal("S01"), _combo_signal("S12")),
        injection_signal_kinds=("metric_threshold", "carrier_p95_ratio"),
        injection_signals=(None, _combo_injection_signal("S12")),
        timing=ComboTimingProfile(
            name="d26_common_target_window_cpu_first", geometry="common_target_window",
            inject_order=("F2", "F1"), recover_order=("F1", "F2"),
            decision="SPECS-3 D26(d)：legacy=null（T06 拆对新对）；旧源知识输入（T06 注入序 cpu→cpu→netem、"
                     "恢复逆序 L15364-15365/L16118-16126——cpu 先注知识与此一致）；采纳蓝图建议＝公共目标窗："
                     "backend cpu F2 (30,110) 先注（CRD 秒级＋爬坡 15s 头预算→settled ~45；duration_s=80 覆盖"
                     "窗），gw net F1 (35,105) 紧随（CRD 秒级→settled ~35；duration_s=72 覆盖窗）；双活稳态 "
                     "45→105=60 样本≥C1 §2.5 地板 30；恢复 reverse：net CRD 删除@105→cpu CRD 删除@110（秒级"
                     "，≤111≤during 末 115）；嵌套副产物（F1⊂F2）显式声明（§8-4）；backend 腿在旧 T06 中 "
                     "carrier_hard=True——观测设计注意（ratio 可作硬判但 throttle 仍缺位）；timing "
                     "NOT_FROZEN",
        ),
        interaction_expectation="无旧标签（新对）——设计意图期望=independent_parallel（期望非观测：设计意图"
                                "期望，非观测结论；走 C1 §0 B 观测门）",
        reference_dependencies={"F1": ("S01",), "F2": ("S12",)},
        obs_r1_audit="PASS：live 容器={catalog-gw pod, backend pod}，两腿两容器一一对应、无交叉；backend 键"
                     "按实际 pod 名（app=backend_api 映射纪律）；CRI 目标从 pinned pods 派生、单一 pod: "
                     "键型（OBS-R1）",
        preconditions=(
            ATOM_SPECS["S01"].preconditions[0],      # pricing redirect (main stream premise)
            ATOM_SPECS["S12"].preconditions[0],      # backend live-label pin (app=backend_api, F-S1-3)
            _reference_ordering_precondition(
                "D26", "F1=S01 network_delay@catalog-gw, F2=S12 service_cpu_saturation@backend"),
            _cpu_mixed_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C1/C5、§3 D26 行、§4、§5、§7、§8",
            "catalog_scenario": "D26 (origin=triple_pair_completion T06 拆对; id_reused=False; legacy=null; "
                                "composition_signature network_delay@catalog-gw || service_cpu_saturation@"
                                "backend; G=2)",
            "ledger": "m14-ledger-b1/03-network-chaos.md（gw delay 腿）+ m14-ledger-b3/01-service-cpu.md"
                      "（backend cpu 腿，carrier_hard=True 观测注意）",
            "origin": "triple_pair_completion（T06 拆对）；T06 三腿子对闭合={D27, D26, D09}（CROSS-CUTTING "
                      "§7）",
            "parameter_status": "NOT_FROZEN（gw {latency_ms:500, jitter_ms:50, correlation_percent:0, "
                                "duration_s:72 覆盖组合窗} 沿 S01 候选（C1）；backend {workers:2, load_"
                                "percent:100, duration_s:80 覆盖组合窗} 沿 S12 候选（C5）；MINOR-1 不变量 "
                                "72≥72/80≥80 成立）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D26 行：main=pricing-via-gw——gw delay 主通道；"
                                      "bypass=backend /api/stats/model——backend cpu 在场；前提=pricing 重定向）",
            "timing_status": "NOT_FROZEN（legacy=null 新对，公共目标窗＋cpu 先注＋恢复 reverse；profile "
                             "d26_common_target_window_cpu_first，选择理由在 timing.decision）",
            "gt_correspondence": "net 腿=延迟臂（pricing_service p95 位移）；cpu 腿=backend_api ratio＋ok≥0.8＋"
                                 "throttle（OPEN-Q3；旧 T06 carrier_hard=True 知识进观测设计）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D28": ComboSpec(
        scenario_id="D28", purpose="pilot", scope="m1_main",
        rule_version_base="d28-combo-pilot", random_seed=11,
        phases=COMBO3_NETCPU_PHASES,
        legs=(_combo_leg("S21", COMBO3_NETCPU_NET_WINDOW, duration_s=72),
              _combo_leg("S27", COMBO3_NETCPU_CPU_WINDOW, duration_s=80)),
        carriers=(
            CarrierStreamSpec(stream_id="d28-main-sasrec-inference-post-v1", service="sasrec:8200",
                              path_prefix="/recommend", carrier="combo-main-panel-sasrec-inference-post-v1",
                              direct_api=True, rate_rps=1.0, max_concurrency=1,
                              client_retry_limit=0, parameterized=False),
            CarrierStreamSpec(stream_id="d28-bypass-recagent-health", service="rec-agent:5001",
                              path_prefix="/recommend/health", carrier="combo-bypass-panel-recagent-health",
                              direct_api=True, parameterized=False)),
        telemetry=_std_telemetry("rec-agent-pod-logs"),
        signals=(_combo_signal("S21"), _combo_signal("S27")),
        injection_signal_kinds=("metric_threshold", "carrier_p95_ratio"),
        injection_signals=(None, _combo_injection_signal("S27")),
        timing=ComboTimingProfile(
            name="d28_common_target_window_cpu_first", geometry="common_target_window",
            inject_order=("F2", "F1"), recover_order=("F1", "F2"),
            decision="SPECS-3 D28(d)：legacy=null（T07 拆对新对）；旧源知识输入（T07 F1 rec-agent netem 450/90＋"
                     "F2 sasrec workers=8 整 during 预注 L13464-13493——sasrec 整窗预注形态与此一致）；采纳"
                     "蓝图建议＝公共目标窗（cpu 先注秒级→netem 后注；恢复 reverse）：sasrec cpu F2 (30,110) "
                     "先注（CRD 秒级＋爬坡 15s 头预算→settled ~45；duration_s=80 覆盖窗），rec-agent netem "
                     "F1 (33,105) 紧随（CRD 秒级→settled ~35；duration_s=72 覆盖窗）；双活稳态 45→105=60 样本"
                     "≥C1 §2.5 地板 30；恢复 reverse：netem CRD 删除@105→sasrec CRD 删除@110（秒级，≤111≤"
                     "during 末 115）；嵌套副产物（F1⊂F2）显式声明（§8-4）；归因靠各自独立通道（/recommend/"
                      "health 位移 vs sasrec /recommend inference ratio＋throttle）；timing NOT_FROZEN",
        ),
        interaction_expectation="无旧标签（新对）——设计意图期望=independent_parallel（期望非观测：设计意图"
                                "期望，非观测结论；走 C1 §0 B 观测门）",
        reference_dependencies={"F1": ("S21",), "F2": ("S27",)},
        obs_r1_audit="PASS：live 容器={rec-agent pod, sasrec pod}，两腿两容器一一对应、无交叉；rec-agent 键"
                     "名纪律（app=recommendation_agent 标签映射，§4 规避 3）；CRI 目标从 pinned pods 派生、"
                     '单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S21"].preconditions[0],      # netem face first-use smoke (binds this combo too)
            ATOM_SPECS["S21"].preconditions[1],      # live-label pin (recommendation_agent, F-S1-3)
            ATOM_SPECS["S27"].preconditions[0],      # sasrec no-restart pin semantics
            ATOM_SPECS["S27"].preconditions[1],      # workers=8 quota/topology coupling
            _reference_ordering_precondition(
                "D28", "F1=S21 network_delay@rec-agent, F2=S27 service_cpu_saturation@sasrec"),
            _cpu_mixed_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C3/C5、§3 D28 行、§4、§5、§7、§8",
            "catalog_scenario": "D28 (origin=triple_pair_completion T07 拆对; id_reused=False; legacy=null; "
                                "composition_signature network_delay@rec-agent || service_cpu_saturation@"
                                "sasrec; G=2)",
            "ledger": "m14-ledger-b1/03-network-chaos.md（rec-agent netem 腿，S21 首用＋全 egress 语义）+ "
                      "m14-ledger-b3/01-service-cpu.md（sasrec cpu 腿，SOFT 载体：共 recommend 路径不可二"
                      "分，B5 审计 L13418-13422）",
            "origin": "triple_pair_completion（T07 拆对）；T07 三腿子对闭合={D28, D30, D29}（CROSS-CUTTING "
                      "§7）；S21 首用冒烟前置约束本组（蓝图 (f)）",
            "parameter_status": "NOT_FROZEN（rec-agent {latency_ms:450, jitter_ms:90, correlation_percent:0, "
                                "duration_s:72 覆盖组合窗} 沿 S21 候选（C3；全 egress 语义，禁把 gw 单边档"
                                "照搬）；sasrec {workers:8, load_percent:100, duration_s:80 覆盖组合窗} 沿 "
                                "S27 候选（C5，不重启 pod＋拓扑绑定）；MINOR-1 不变量 72≥72/80≥80 成立）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D28 行：main=sasrec versioned /recommend inference POST——cpu 受害（SOFT→"
                                      "throttle 优先）；bypass=rec-agent /recommend/health——net delay 在场；"
                                      "pricing 重定向不涉）",
            "timing_status": "NOT_FROZEN（legacy=null 新对，公共目标窗＋cpu 先注＋恢复 reverse；profile "
                             "d28_common_target_window_cpu_first，选择理由在 timing.decision）",
            "gt_correspondence": "cpu 腿=sasrec_api ratio（RATIO 禁绝对）＋throttle（OPEN-Q3；载体 SOFT：与 "
                                 "rec-agent 腿共 recommend 路径同型，归因靠各自独立通道）；net 腿=延迟臂"
                                 "（recommendation_agent /recommend/health p95 位移）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D30": ComboSpec(
        scenario_id="D30", purpose="pilot", scope="m1_main",
        rule_version_base="d30-combo-pilot", random_seed=11,
        phases=COMBO3_CPUPOD_PHASES,
        legs=(_combo_leg("S27", COMBO3_CPUPOD_CPU_WINDOW, duration_s=105),
              _combo_leg("S03", COMBO3_CPUPOD_POD_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d30-main-catalog-items-via-gw", service="catalog-gw:80",
                              path_prefix="/api/items/", carrier="combo-main-panel-catalog-via-gw",
                              
                              direct_api=True, trace_service="catalog_service"),
            CarrierStreamSpec(stream_id="d30-bypass-sasrec-inference-post-v1", service="sasrec:8200",
                              path_prefix="/recommend", carrier="combo-bypass-panel-sasrec-inference-post-v1",
                              direct_api=True, rate_rps=1.0, max_concurrency=1,
                              client_retry_limit=0, parameterized=False)),
        telemetry=_std_telemetry("sasrec-pod-logs"),
        signals=(_combo_signal("S27"), _combo_signal("S03")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_error_fraction"),
        injection_signals=(_combo_injection_signal("S27"), _combo_injection_signal("S03")),
        timing=ComboTimingProfile(
            name="d30_partial_overlap_cpu_outer_pod_inner", geometry="partial_overlap",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-3 D30(d)：legacy=null（T07 拆对新对）；旧源知识输入（T07 F2 sasrec 整 during 预注＋"
                     "F3 catalog pod mid INNER 子窗）；采纳蓝图建议＝partial_overlap（承旧 T07 形态）：sasrec "
                     "cpu F1 (30,135) 整窗预注（CRD 秒级＋爬坡 15s 头预算→settled ~45；duration_s=105 覆盖"
                     "窗），catalog pod F2 (55,115) below_kill_band v2 子窗（60s 窗/CRD 90s；EXEC-POD-XFER "
                     "2026-09-22：lag 头 35s 由 pod 族 carrier settle 吸收，评估窗 ~34 样本，有效下线段 "
                     "[~89,~126] 落 cpu 稳态段→co-active ~37 样本；偏移 {30,55,115,135} 最小正间隙 20＞默认"
                     "预算和 19）；"
                     "恢复 reverse：pod CRD 删除@115（restore+Ready ~128 during 内收口，evidence-only，restart_"
                     "delta 窗归属不溢入 cpu 窗）→sasrec CRD 删除@135（秒级，≤136≤during 末 140）；timing "
                     "NOT_FROZEN",
        ),
        interaction_expectation="无旧标签（新对）——设计意图期望=independent_parallel（期望非观测：设计意图"
                                "期望，非观测结论；走 C1 §0 B 观测门）",
        reference_dependencies={"F1": ("S27",), "F2": ("S03",)},
        obs_r1_audit="PASS：live 容器={sasrec pod, catalog pod}，两腿两容器一一对应、无交叉；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S27"].preconditions[0],      # sasrec no-restart pin semantics
            ATOM_SPECS["S27"].preconditions[1],      # workers=8 quota/topology coupling
            _reference_ordering_precondition(
                "D30", "F1=S27 service_cpu_saturation@sasrec, F2=S03 service_unavailable@catalog"),
            _cpu_mixed_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C5/C4、§3 D30 行、§4、§5、§7、§8",
            "catalog_scenario": "D30 (origin=triple_pair_completion T07 拆对; id_reused=False; legacy=null; "
                                "composition_signature service_cpu_saturation@sasrec || service_unavailable@"
                                "catalog; G=2)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（sasrec cpu 腿，SOFT）+ m14-ledger-b1/05-pod-"
                      "failure.md（catalog pod 腿）",
            "origin": "triple_pair_completion（T07 拆对）；承旧 T07 形态（cpu 整窗＋pod mid INNER 子窗）；"
                      "T07 三腿子对闭合={D28, D30, D29}（CROSS-CUTTING §7）",
            "parameter_status": "NOT_FROZEN（sasrec {workers:8, load_percent:100, duration_s:105 覆盖组合窗} "
                                "沿 S27 候选（C5，不重启 pod＋拓扑绑定）；pod {action: pod-failure, "
                                "duration_s: 90}=below_kill_band v2 剖面（C4）；MINOR-1 不变量 105≥105/90≥60 "
                                "成立；EXEC-POD-XFER 2026-09-22 重切）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D30 行：main=catalog-items-via-gw——catalog pod "
                                      "受害主通道；bypass=sasrec versioned /recommend inference POST——sasrec cpu 在场（SOFT）；pricing "
                                      "重定向不涉（catalog-items-via-gw 流））",
            "timing_status": "NOT_FROZEN（legacy=null 新对，partial_overlap 承旧 T07 形态；profile "
                             "d30_partial_overlap_cpu_outer_pod_inner，选择理由在 timing.decision）",
            "gt_correspondence": "pod 腿=error/restart（catalog_service error fraction＋restart_delta∈{1,2} "
                                 "窗签名）；cpu 腿=sasrec_api ratio＋throttle（OPEN-Q3；SOFT 载体）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D31": ComboSpec(
        scenario_id="D31", purpose="pilot", scope="m1_main",
        rule_version_base="d31-combo-pilot", random_seed=11,
        phases=D31_PHASES,
        legs=(_combo_leg("S04", D31_CATALOG_WINDOW, duration_s=80),
              _combo_leg("S11", D31_RQ_WINDOW, duration_s=72)),
        carriers=(
            CarrierStreamSpec(stream_id="d31-main-catalog-items-cpu", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-main-panel-catalog-direct",
                              direct_api=True),
            CarrierStreamSpec(stream_id="d31-bypass-review-query-list", service="review-query:5018",
                              path_prefix="/api/reviews?item_id=", path_suffix="&per_page=5&enrich=1",
                              carrier="combo-bypass-panel-review-query-list", direct_api=True)),
        telemetry=_std_telemetry("catalog-pod-logs"),
        signals=(_combo_signal("S04"), _combo_signal("S11")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_p95_ratio"),
        injection_signals=(_combo_injection_signal("S04"), _combo_injection_signal("S11")),
        timing=ComboTimingProfile(
            name="d31_common_target_window_dual_cpu", geometry="common_target_window",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-3 D31(d)：legacy=null（T08 拆对新对）；旧 T08 实现 partial_overlap 成因=podfail 腿 "
                     "≤60s 上限（L13389-13392）——D31 无 podfail 腿，偏离理由不成立，旧标签不可沿用，timing "
                     "按 C1 显式核定（B3-03 §2 D31）；采纳蓝图建议＝公共目标窗（simultaneous 类）：两 CRD "
                     "秒级，catalog cpu F1 (30,110) 先注（workers=1 承旧 T08 catalog 腿标定 L13383-13384；爬坡 "
                     "15s 头预算→settled ~45；duration_s=80 覆盖窗），rq cpu F2 (33,105) 紧随（settled ~48；"
                     'duration_s=72 覆盖窗）；双活稳态 50→105=55 样本≥C1 §2.5 地板 30（collection: 3→5 格网）；注入序 F1→F2、恢复 '
                     "reverse（F2 先收）；嵌套副产物（F2⊂F1）显式声明（§8-4）；CRD 删除@105/@110（秒级，"
                     "≤111≤during 末 115）；timing NOT_FROZEN",
        ),
        interaction_expectation="无旧标签（新对）——设计意图期望=independent_parallel（纯空间可分：catalog≠"
                                "review-query pod）（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测"
                                "门）",
        reference_dependencies={"F1": ("S04",), "F2": ("S11",)},
        obs_r1_audit="PASS：live 容器={catalog pod, review-query pod}，两腿两容器一一对应、无交叉；CRI 目标"
                     '从 pinned pods 派生、单一 pod: 键型（OBS-collection）；catalog throttle 正则排 catalog-gw'
                     "（L13498）",
        preconditions=(
            _dual_cpu_throttle_attribution_precondition(),
            _reference_ordering_precondition(
                "D31", "F1=S04 service_cpu_saturation@catalog, F2=S11 service_cpu_saturation@review-query"),
            _cpu_mixed_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C5×2、§3 D31 行、§4、§5、§7、§8",
            "catalog_scenario": "D31 (origin=triple_pair_completion T08 拆对; id_reused=False; legacy=null; "
                                "composition_signature service_cpu_saturation@catalog || service_cpu_"
                                "saturation@review-query; G=2)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（双腿；T08 拆对；rq 腿旧 carrier_hard=True；§3.2-3 "
                      "OPEN-Q3 本组归因前置）+ B3-03 §2 D31",
            "origin": "triple_pair_completion（T08 拆对）；旧 partial_overlap 成因（podfail ≤60s 上限）不在"
                      "场，timing 按 C1 显式核定=公共目标窗；T08 三腿子对闭合={D33, D31, D32}（CROSS-"
                      "CUTTING §7）",
            "parameter_status": "NOT_FROZEN（catalog {workers:1 承旧 T08 catalog 腿标定 L13383-13384，与 S04 "
                                "联动论证, load_percent:100, duration_s:80 覆盖组合窗}；rq {workers:2 候选"
                                "（旧 T08 carrier_hard=True）, load_percent:100, duration_s:72 覆盖组合窗}；"
                                "per-leg workers 档禁全局默认吞并（B3-01 §4-3）；MINOR-1 不变量 80≥80/72≥72 "
                                "成立）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D31 行：main=catalog-direct——catalog cpu 受害"
                                      "（workers=1）；bypass=rq /api/reviews——rq cpu 在场；pricing 重定向"
                                      "不涉）",
            "timing_status": "NOT_FROZEN（legacy=null 新对，旧标签不可沿用（偏离理由不成立），公共目标窗＋"
                             "注入序 F1→F2＋恢复 reverse；profile d31_common_target_window_dual_cpu，选择"
                             "理由在 timing.decision）",
            "gt_correspondence": "两腿→ratio＋throttle 双臂；**双 CPU 腿归因唯一可分轴=per-pod cfs_throttle "
                                 "独判**——载体 ratio 不可作唯一判据（旧约束 E L8430-8431）；per-pod throttle "
                                 "QC 臂缺口=本组归因前置（OPEN-Q3，硬化为 Precondition）；catalog throttle "
                                 "正则排 catalog-gw（L13498）；旧 gate multi_leg_retarget_gate L8422 系为 "
                                 "provenance 候选锚",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    # --- COMBO-ASM-4: SPECS-4 closing family (D18/D20/D23/D24/D25/D27/D32/ --- #
    # --- D33 duals + T05/T06/T07/T08 triples) ------------------------------- #
    "D18": ComboSpec(
        scenario_id="D18", purpose="pilot", scope="m1_main",
        rule_version_base="d18-combo-pilot", random_seed=11,
        phases=COMBO4_DUALCPU_PHASES,
        legs=(_combo_leg("S10", COMBO4_DUALCPU_FIRST_WINDOW, duration_s=80),
              _combo_leg("S09", COMBO4_DUALCPU_SECOND_WINDOW, duration_s=72)),
        carriers=(
            CarrierStreamSpec(stream_id="d18-main-cart-health", service="cart:5006",
                              path_prefix="/health", carrier="combo-main-panel-cart-health",
                              direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="d18-bypass-order-detail", service="order:5010",
                              path_prefix="/api/orders/", carrier="combo-bypass-panel-order-detail",
                              direct_api=True)),
        telemetry=_std_telemetry("cart-pod-logs"),
        signals=(_combo_signal("S10"), _combo_signal("S09")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_p95_ratio"),
        injection_signals=(_combo_injection_signal("S10"), _combo_injection_signal("S09")),
        timing=ComboTimingProfile(
            name="d18_common_target_window_dual_cpu", geometry="common_target_window",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-4 D18(d)：legacy=simultaneous＝默认（旧 dual18 两腿均 svccpu→pre-inject-both 整 "
                     "during，L13311 纪律；注入序 F1→F2、恢复逆腿序 L16121-16126，B3-03 §2 D18）；simultaneous"
                     "由公共目标窗承载（C1 §2.6 语义，f2win⊆f1win 机械嵌套=注入/恢复逆序副产物、显式声明"
                     "§8-4）：cart cpu F1 (30,110) 先注（workers=1 承 cart 500m 单 worker 饱和标定 L13365-"
                     "13370；CRD 秒级＋爬坡 15s 头预算→settled ~45；duration_s=80 覆盖窗），order cpu F2 "
                     '(35,105) 紧随（settled ~50；duration_s=72 覆盖窗）；双活稳态 50→105=55 样本≥C1 §2.5（collection）'
                     "地板 30；恢复 reverse：order CRD 删除@105→cart CRD 删除@110（秒级，≤111≤during 末 "
                     "115）；mid_actions=[]（L15002）；timing NOT_FROZEN"),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测"
                                "门）——legacy CATALOG 标签 independent_parallel；纯空间可分（cart≠order pod，"
                                "design_note L13337）＋per-pod cfs_throttle 各自 pod 独判（OPEN-Q3 归因前置）",
        reference_dependencies={"F1": ("S10",), "F2": ("S09",)},
        obs_r1_audit="PASS：live 容器={cart pod, order pod}，两腿两容器一一对应、无交叉；载体 Service（cart/"
                     'order）非 CRI 目标；CRI 目标从 pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _dual_cpu_throttle_attribution_precondition(),
            ATOM_SPECS["S09"].preconditions[0],      # order probe order_no premise
            _reference_ordering_precondition(
                "D18", "F1=S10 service_cpu_saturation@cart, F2=S09 service_cpu_saturation@order"),
            _cpu_cpu_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C5×2、§3 D18 行、§4、§8",
            "catalog_scenario": "D18 (origin=retained_composition 旧dual18; id_reused=False; legacy="
                                "simultaneous/independent_parallel; composition_signature "
                                "service_cpu_saturation@cart || service_cpu_saturation@order; G=2)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（双腿；cart W=1 标定 L13365-13370；per-pod throttle "
                      "独判空间可分 L13337）+ B3-03 §2 D18",
            "origin": "retained_composition（旧 dual18）；legacy=simultaneous 由公共目标窗承载；旧 GT G2ext "
                      "参数化块 L10023-10096（role=co_primary×2、affected=各腿自身）为字段映射参考"
                      "（labels_provisional）",
            "parameter_status": "NOT_FROZEN（cart {workers:1（500m 单 worker 即饱和标定）, load_percent:100, "
                                "duration_s:80 覆盖组合窗}；order {workers:2 候选（500m）, load_percent:100, "
                                "duration_s:72 覆盖组合窗}；per-leg workers 档禁全局默认吞并（B3-01 §4-3）；"
                                "MINOR-1 不变量 80≥80/72≥72 成立）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D18 行：main=cart /health——cart cpu 受害（W=1，"
                                      "/health 为 cart 唯一可表达只读流，S10 同源限制）；bypass=order "
                                      "/api/orders/<order_no>——order cpu 在场（order_no 探针前提随 S09 前置）；"
                                      "业务证据=两腿各=自载体 p95 相对比 ≥1.8× 候选＋ok≥0.8＋per-pod "
                                      "cfs_throttle 各自 pod 独判）",
            "timing_status": "NOT_FROZEN（legacy=simultaneous 默认由公共目标窗承载＋注入 F1→F2＋恢复逆腿序；"
                             "profile d18_common_target_window_dual_cpu；改窗几何须记录）",
            "gt_correspondence": "两腿→ratio＋throttle 双臂；per-pod cfs_throttle 各自 pod 独判是双 CPU 腿"
                                 "归因唯一可分轴（纯空间可分 cart≠order pod，L13337；OPEN-Q3 新 Q 缺位硬化"
                                 "为 Precondition）；旧 GT G2ext 参数化块 L10023-10096 为字段映射参考",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D20": ComboSpec(
        scenario_id="D20", purpose="pilot", scope="m1_main",
        rule_version_base="d20-combo-pilot", random_seed=11,
        phases=COMBO4_DUALCPU_PHASES,
        legs=(_combo_leg("S20", COMBO4_DUALCPU_FIRST_WINDOW, duration_s=80),
              _combo_leg("S12", COMBO4_DUALCPU_SECOND_WINDOW, duration_s=72)),
        carriers=(
            CarrierStreamSpec(stream_id="d20-main-recagent-health", service="rec-agent:5001",
                              path_prefix="/recommend/health", carrier="combo-main-panel-recagent-health",
                              direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="d20-bypass-backend-stats", service="backend:5000",
                              path_prefix="/api/stats/model", carrier="combo-bypass-panel-backend-stats",
                              direct_api=True, parameterized=False)),
        telemetry=_std_telemetry("rec-agent-pod-logs"),
        signals=(_combo_signal("S20"), _combo_signal("S12")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_p95_ratio"),
        injection_signals=(_combo_injection_signal("S20"), _combo_injection_signal("S12")),
        timing=ComboTimingProfile(
            name="d20_common_target_window_dual_cpu", geometry="common_target_window",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-4 D20(d)：legacy=simultaneous＝默认（旧 dual20 pre-inject-both 整 during；注入 F1→"
                     "F2、恢复逆序，B3-03 §2 D20）；simultaneous 由公共目标窗承载（嵌套副产物显式声明 §8-4）："
                     "rec-agent cpu F1 (30,110) 先注（workers=2 承 20/3/5 liveness 剖面；CRD 秒级＋爬坡 15s "
                     "头预算→settled ~45；duration_s=80 覆盖窗），backend cpu F2 (33,105) 紧随（settled ~48；"
                     'duration_s=72 覆盖窗）；双活稳态 50→105=55 样本≥C1 §2.5 地板 30（collection）；恢复 reverse：backend '
                     "CRD 删除@105→rec-agent CRD 删除@110（秒级，≤111≤during 末 115）；timing NOT_FROZEN",
        ),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测"
                                "门）——legacy CATALOG 标签 independent_parallel；双 caller 打 sasrec fan-in"
                                "（共享下游）→载体 latency 无法二分两 CPU 腿，归因靠各自 per-pod cfs_throttle"
                                "（L13410-13412）",
        reference_dependencies={"F1": ("S20",), "F2": ("S12",)},
        obs_r1_audit="PASS：live 容器={rec-agent pod, backend pod}，两腿两容器一一对应、无交叉；两 pod 键均按"
                     "实际 pod 名（app=recommendation_agent / app=backend_api 标签映射纪律，§4 规避 3）；CRI "
                     '目标从 pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S20"].preconditions[0],      # rec-agent live-label pin (F-S1-3)
            ATOM_SPECS["S12"].preconditions[0],      # backend live-label pin (F-S1-3)
            _d20_deepseek_secret_precondition(),
            _dual_cpu_throttle_attribution_precondition(),
            _reference_ordering_precondition(
                "D20", "F1=S20 service_cpu_saturation@rec-agent, F2=S12 service_cpu_saturation@backend"),
            _cpu_cpu_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C5×2、§3 D20 行、§4、§8",
            "catalog_scenario": "D20 (origin=retained_composition 旧dual20; id_reused=False; legacy="
                                "simultaneous/independent_parallel; composition_signature "
                                "service_cpu_saturation@rec-agent || service_cpu_saturation@backend; G=2)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（双腿；rec-agent 20/3/5 剖面＋backend tolerant 剖面；"
                      "双 per-pod throttle L13410-13412；DeepSeek 挂不废 case L13412）+ B3-03 §2 D20",
            "origin": "retained_composition（旧 dual20）；legacy=simultaneous 由公共目标窗承载；G1 五件套 "
                      "g2ext 等价知识（拓扑追加 L14891-14903——旧 --deep 替换语义会丢 rec-agent→verify 必 "
                      "FAIL；新链拓扑/遥测覆盖必须显式含 rec-agent）进观测设计",
            "parameter_status": "NOT_FROZEN（rec-agent {workers:2 候选（20/3/5 liveness 剖面）, load_percent:"
                                "100, duration_s:80 覆盖组合窗}；backend {workers:2 候选, load_percent:100, "
                                "duration_s:72 覆盖组合窗}；MINOR-1 不变量 80≥80/72≥72 成立）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D20 行：main=rec-agent /recommend/health——固定"
                                      "路径 always-200 无 LLM/DB；bypass=backend /api/stats/model——固定路径"
                                      "cpu 在场；recommend POST 仅业务证据（DeepSeek 挂不废 case，L13412）；"
                                      "业务证据=rec-agent 腿 recommendation_agent ratio＋backend 腿 backend_api "
                                      "ratio＋双 per-pod throttle）",
            "timing_status": "NOT_FROZEN（legacy=simultaneous 默认由公共目标窗承载＋注入 F1→F2＋恢复逆序；"
                             "profile d20_common_target_window_dual_cpu；改窗几何须记录）",
            "gt_correspondence": "两腿→ratio＋双 per-pod throttle 臂（归因前提，L13410-13412；OPEN-Q3 新 Q 缺位"
                                 "硬化为 Precondition）；deepseek-env secret 前置踩坑（restore 脚本会摘 secret→"
                                 "recommend 全 401，L13468-13469）硬化为 Precondition（装配检查单）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D23": ComboSpec(
        scenario_id="D23", purpose="pilot", scope="m1_main",
        rule_version_base="d23-combo-pilot", random_seed=11,
        phases=COMBO4_DUALCPU_PHASES,
        legs=(_combo_leg("S10", COMBO4_DUALCPU_FIRST_WINDOW, duration_s=80),
              _combo_leg("S28", COMBO4_DUALCPU_SECOND_WINDOW, duration_s=72)),
        carriers=(
            CarrierStreamSpec(stream_id="d23-main-cart-health", service="cart:5006",
                              path_prefix="/health", carrier="combo-main-panel-cart-health",
                              direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="d23-bypass-pricing-items-cpu", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-bypass-panel-pricing-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("cart-pod-logs"),
        signals=(_combo_signal("S10"), _combo_signal("S28")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_p95_ratio"),
        injection_signals=(_combo_injection_signal("S10"), _combo_injection_signal("S28")),
        timing=ComboTimingProfile(
            name="d23_common_target_window_dual_cpu", geometry="common_target_window",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-4 D23(d)：legacy=null（T05 拆对新对，无先验默认，装配记录写明 §8-1）；旧 T05 "
                     "partial_overlap 成因=podfail 子窗——本组无 podfail 腿，timing NOT_FROZEN 显式声明不沿用"
                     "（B3-03 §2 D23）；采纳本图建议=公共目标窗（simultaneous 类）：cart cpu F1 (30,110) 先注"
                     "（workers=1 承旧 T05 cart 腿标定 L13365-13370：500m 一 worker 即饱和，第 2 worker 只放大"
                     "同步 DB-pool 争用不增强 per-pod throttle 信号；CRD 秒级＋爬坡 15s 头预算→settled ~45；"
                     "duration_s=80 覆盖窗），pricing cpu F2 (35,105) 紧随（settled ~50；duration_s=72 覆盖"
                     "窗）；双活稳态 48→105=57 样本≥C1 §2.5 地板 30；恢复 reverse：pricing CRD 删除@105→cart "
                     "CRD 删除@110（秒级，≤111≤during 末 115）；timing NOT_FROZEN",
        ),
        interaction_expectation="无旧标签（新对）——设计意图期望=independent_parallel（期望非观测：设计意图"
                                "期望，非观测结论；走 C1 §0 B 观测门）",
        reference_dependencies={"F1": ("S10",), "F2": ("S28",)},
        obs_r1_audit="PASS：live 容器={cart pod, pricing pod}，两腿两容器一一对应、无交叉；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _dual_cpu_throttle_attribution_precondition(),
            _pricing_scale_up_precondition(),
            _reference_ordering_precondition(
                "D23", "F1=S10 service_cpu_saturation@cart, F2=S28 service_cpu_saturation@pricing"),
            _cpu_cpu_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C5×2、§3 D23 行、§4、§7、§8",
            "catalog_scenario": "D23 (origin=triple_pair_completion T05 拆对; id_reused=False; legacy=null; "
                                "composition_signature service_cpu_saturation@cart || "
                                "service_cpu_saturation@pricing; G=2)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（双腿；cart W=1/pricing SOFT 知识随 T05 腿定义继承，"
                      "B3-01 §3.2-1 条件档冻结论证）",
            "origin": "triple_pair_completion（T05 拆对）；T05 三腿子对闭合={D24, D23, D25}（CROSS-CUTTING "
                      "§7）",
            "parameter_status": "NOT_FROZEN（cart {workers:1 承旧 T05 标定 L13365-13370, load_percent:100, "
                                "duration_s:80 覆盖组合窗}；pricing {workers:2 候选, load_percent:100, "
                                "duration_s:72 覆盖组合窗}，载体臂 SOFT；MINOR-1 不变量 80≥80/72≥72 成立）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D23 行：main=cart /health——cart cpu 受害（W=1）；"
                                      "bypass=pricing pricing-direct——pricing cpu 在场（SOFT→throttle 优先）；"
                                      "pricing-direct 流只涉 pricing scale-up 前提〔§3 约束 2，硬化为 "
                                      "Precondition（ASM-3 审核 NIT-B）〕不涉重定向）",
            "timing_status": "NOT_FROZEN（legacy=null 新对，公共目标窗＋注入 F1→F2＋恢复 reverse；profile "
                             "d23_common_target_window_dual_cpu，选择理由在 timing.decision）",
            "gt_correspondence": "两腿→ratio＋per-pod throttle 双臂（OPEN-Q3 新 Q 缺位硬化为 Precondition）；"
                                 "pricing 腿载体臂 SOFT（carrier-latency 未验证 L13371）——throttle/自载体证据"
                                 "优先；workers 档（cart 1 vs 2）与 pricing SOFT 知识随 T05 腿定义继承",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D24": ComboSpec(
        scenario_id="D24", purpose="pilot", scope="m1_main",
        rule_version_base="d24-combo-pilot", random_seed=11,
        phases=COMBO4_CPUPOD_PHASES,
        legs=(_combo_leg("S10", COMBO4_CPUPOD_CPU_WINDOW, duration_s=105),
              _combo_leg("S18", COMBO4_CPUPOD_POD_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d24-main-checkout-health", service="checkout:5011",
                              path_prefix="/health", carrier="combo-main-panel-checkout-health",
                              direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="d24-bypass-cart-health", service="cart:5006",
                              path_prefix="/health", carrier="combo-bypass-panel-cart-health",
                              direct_api=True, parameterized=False)),
        telemetry=_std_telemetry("checkout-pod-logs"),
        signals=(_combo_signal("S10"), _combo_signal("S18")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_error_fraction"),
        injection_signals=(_combo_injection_signal("S10"), _combo_injection_signal("S18")),
        timing=ComboTimingProfile(
            name="d24_partial_overlap_cpu_outer_pod_inner", geometry="partial_overlap",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-4 D24(d)：legacy=null（T05 拆对新对）；旧 T05 partial_overlap 成因=checkout podfail "
                     "子窗——本组含 pod 腿，同型约束复现（pod 子窗 ≤kill-band 语义）；采纳本图建议="
                     "partial_overlap 承旧 T05 形态" + "（EXEC-POD-XFER 2026-09-22：pod 子窗 35→60s 窗/CRD 90s，lag 头 35s 由 pod 族 carrier settle 吸收，评估窗 ~34 样本，有效下线段 [~89,~126] 落 cpu 稳态段→co-active ~37 样本；cpu 窗 110→135 （duration 80→105）保有效 co-active 地板；偏移 {30,55,115,135} 最小正间隙 20＞默认预算和 19）" + "：cart cpu F1 (30,135) 整窗预注（workers=1 承 T05 标定；CRD "
                     "秒级＋爬坡 15s 头预算→settled ~45；duration_s=105 覆盖窗），checkout pod F2 (55,115) "
                     "below_kill_band v2 子窗；恢复 "
                     "reverse：pod CRD 删除@115（restore+Ready ~128 during 内收口，evidence-only，restart_delta 窗"
                     "归属不溢入 cpu 窗）→cart CRD 删除@135（秒级，≤136≤during 末 140）；timing NOT_FROZEN",
        ),
        interaction_expectation="无旧标签（新对）——设计意图期望=independent_parallel（期望非观测：设计意图"
                                "期望，非观测结论；走 C1 §0 B 观测门）",
        reference_dependencies={"F1": ("S10",), "F2": ("S18",)},
        obs_r1_audit="PASS：live 容器={cart pod, checkout pod}，两腿两容器一一对应、无交叉；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _reference_ordering_precondition(
                "D24", "F1=S10 service_cpu_saturation@cart, F2=S18 service_unavailable@checkout"),
            _cpu_cpu_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C5/C4、§3 D24 行、§4、§7、§8",
            "catalog_scenario": "D24 (origin=triple_pair_completion T05 拆对; id_reused=False; legacy=null; "
                                "composition_signature service_cpu_saturation@cart || "
                                "service_unavailable@checkout; G=2)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（cart cpu 腿，W=1 标定注记 B3-03 §3.1 D24 行）+ "
                      "m14-ledger-b1/05-pod-failure.md（checkout pod 腿，std below-kill-band 剖面）",
            "origin": "triple_pair_completion（T05 拆对）；承旧 T05 形态（cpu 整窗＋pod 子窗）；T05 三腿子对"
                      "闭合={D24, D23, D25}（CROSS-CUTTING §7）",
            "parameter_status": "NOT_FROZEN（cart {workers:1 承 T05 标定, load_percent:100, duration_s:105 覆盖"
                                "组合窗}；checkout {action: pod-failure, duration_s: 90}=below_kill_band v2 剖面"
                                "（C4 std）；MINOR-1 不变量 105≥105/90≥60 成立；EXEC-POD-XFER 2026-09-22 重切）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D24 行：main=checkout /health——pod 错误在 lag 头（界 35s）后咬合；"
                                      "bypass=cart /health——cpu 在场（W=1）；pricing 重定向不涉；pod 腿窗长"
                                      "上限耦合（≤kill-band 语义）随 C4 剖面）",
            "timing_status": "NOT_FROZEN（legacy=null 新对，partial_overlap 承旧 T05 形态＋恢复 reverse＋restart"
                             " 严序；profile d24_partial_overlap_cpu_outer_pod_inner，选择理由在 "
                             "timing.decision）",
            "gt_correspondence": "pod 腿=carrier_error_fraction（≥0.5 候选）＋restart_delta∈{1,2} 窗签名；cpu "
                                 "腿=cart_service ratio＋ok≥0.8＋throttle（OPEN-Q3 新 Q 缺位；单 cpu 腿归因"
                                 "不涉双臂不可分问题）；T05 cart workers=1 标定注记继承（B3-03 §3.1）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D25": ComboSpec(
        scenario_id="D25", purpose="pilot", scope="m1_main",
        rule_version_base="d25-combo-pilot", random_seed=11,
        phases=COMBO4_CPUPOD_PHASES,
        legs=(_combo_leg("S28", COMBO4_CPUPOD_CPU_WINDOW, duration_s=105),
              _combo_leg("S18", COMBO4_CPUPOD_POD_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d25-main-checkout-health", service="checkout:5011",
                              path_prefix="/health", carrier="combo-main-panel-checkout-health",
                              direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="d25-bypass-pricing-items-cpu", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-bypass-panel-pricing-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("checkout-pod-logs"),
        signals=(_combo_signal("S28"), _combo_signal("S18")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_error_fraction"),
        injection_signals=(_combo_injection_signal("S28"), _combo_injection_signal("S18")),
        timing=ComboTimingProfile(
            name="d25_partial_overlap_cpu_outer_pod_inner", geometry="partial_overlap",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-4 D25(d)：legacy=null（T05 拆对新对）；同 D24：pod 腿窗长上限耦合（≤kill-band）；"
                     "采纳本图建议=partial_overlap 承旧 T05 形态：pricing cpu F1 (30,110) 整窗预注（SOFT 载体"
                     "——旧 T05 pricing 腿 carrier_hard=False L13371，throttle 优先；CRD 秒级＋爬坡 15s 头预算→"
                     "settled ~45；duration_s=105 覆盖窗），checkout pod F2 (55,115) below_kill_band v2 子窗"
                     "（EXEC-POD-XFER 2026-09-22：pod 子窗 35→60s 窗/CRD 90s，lag 头 35s 由 pod 族 carrier settle "
                     "吸收，评估窗 ~34 样本，有效下线段 [~89,~126] 落 cpu 稳态段→co-active ~37 样本；cpu 窗 "
                     "110→135（duration 80→105）保有效 co-active 地板；偏移 {30,55,115,135} 最小正间隙 20＞默认"
                     "预算和 19）；恢复 reverse：pod CRD 删除@115（restore+Ready ~128 during 内，"
                     "evidence-only）→pricing CRD 删除@135（秒级，≤136≤during 末 140）；timing NOT_FROZEN",
        ),
        interaction_expectation="无旧标签（新对）——设计意图期望=independent_parallel（期望非观测：设计意图"
                                "期望，非观测结论；走 C1 §0 B 观测门）",
        reference_dependencies={"F1": ("S28",), "F2": ("S18",)},
        obs_r1_audit="PASS：live 容器={pricing pod, checkout pod}，两腿两容器一一对应、无交叉；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _pricing_scale_up_precondition(),
            _reference_ordering_precondition(
                "D25", "F1=S28 service_cpu_saturation@pricing, F2=S18 service_unavailable@checkout"),
            _cpu_cpu_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C5/C4、§3 D25 行、§4、§7、§8",
            "catalog_scenario": "D25 (origin=triple_pair_completion T05 拆对; id_reused=False; legacy=null; "
                                "composition_signature service_cpu_saturation@pricing || "
                                "service_unavailable@checkout; G=2)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（pricing cpu 腿，SOFT 载体 L13371）+ m14-ledger-b1/"
                      "05-pod-failure.md（checkout pod 腿）+ B1-06 §2 D25（carrier-latency 未验证警示）",
            "origin": "triple_pair_completion（T05 拆对）；承旧 T05 形态；T05 三腿子对闭合={D24, D23, D25}"
                      "（CROSS-CUTTING §7）",
            "parameter_status": "NOT_FROZEN（pricing {workers:2 候选, load_percent:100, duration_s:105 覆盖"
                                "组合窗}（SOFT carrier_hard=False L13371）；checkout {action: pod-failure, "
                                "duration_s: 90}=below_kill_band v2 剖面（C4）；MINOR-1 不变量 105≥105/90≥60 成立；"
                                "EXEC-POD-XFER 2026-09-22 重切）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D25 行：main=checkout /health——pod 错误在 lag 头（界 35s）后咬合；"
                                      "bypass=pricing pricing-direct——cpu 在场（SOFT→throttle 优先）；pricing-"
                                      "direct 流只涉 pricing scale-up 前提〔§3 约束 2，硬化为 Precondition"
                                      "（ASM-3 审核 NIT-B）〕不涉重定向）",
            "timing_status": "NOT_FROZEN（legacy=null 新对，partial_overlap 承旧 T05 形态＋恢复 reverse；"
                             "profile d25_partial_overlap_cpu_outer_pod_inner，选择理由在 timing.decision）",
            "gt_correspondence": "pod 腿=error/restart；cpu 腿=pricing ratio（SOFT）＋throttle（OPEN-Q3 新 Q "
                                 "缺位）；pricing SOFT 观测警示适用（carrier-latency 未验证，B1-06 §2 D25）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D27": ComboSpec(
        scenario_id="D27", purpose="pilot", scope="m1_main",
        rule_version_base="d27-combo-pilot", random_seed=11,
        phases=COMBO4_DUALCPU_PHASES,
        legs=(_combo_leg("S12", COMBO4_DUALCPU_FIRST_WINDOW, duration_s=80),
              _combo_leg("S27", COMBO4_DUALCPU_SECOND_WINDOW, duration_s=72)),
        carriers=(
            CarrierStreamSpec(stream_id="d27-main-backend-stats", service="backend:5000",
                              path_prefix="/api/stats/model", carrier="combo-main-panel-backend-stats",
                              direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="d27-bypass-sasrec-inference-post-v1", service="sasrec:8200",
                              path_prefix="/recommend", carrier="combo-bypass-panel-sasrec-inference-post-v1",
                              direct_api=True, rate_rps=1.0, max_concurrency=1,
                              client_retry_limit=0, parameterized=False)),
        telemetry=_std_telemetry("backend-pod-logs"),
        signals=(_combo_signal("S12"), _combo_signal("S27")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_p95_ratio"),
        injection_signals=(_combo_injection_signal("S12"), _combo_injection_signal("S27")),
        timing=ComboTimingProfile(
            name="d27_common_target_window_dual_cpu", geometry="common_target_window",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-4 D27(d)：legacy=null（T06 拆对新对）；旧 T06 simultaneous（参考）不沿用；采纳本图"
                     "建议=公共目标窗（simultaneous 类）：backend cpu F1 (30,110) 先注（旧 T06 backend 腿 "
                     "carrier_hard=True；CRD 秒级＋爬坡 15s 头预算→settled ~45；duration_s=80 覆盖窗），sasrec "
                     "cpu F2 (33,105) 紧随（workers=8 且 SOFT——backend/sasrec 共 recommend 路径→载体 latency "
                     "无法二分两 CPU 腿，归因只靠各自 per-pod cfs_throttle，B5 审计 L13418-13422；settled "
                     "~48；duration_s=72 覆盖窗）；双活稳态 48→105=57 样本≥C1 §2.5 地板 30；恢复 reverse："
                     "sasrec CRD 删除@105→backend CRD 删除@110（秒级，≤111≤during 末 115）；嵌套副产物"
                     "（F2⊂F1）显式声明（§8-4）；timing NOT_FROZEN",
        ),
        interaction_expectation="无旧标签（新对）——设计意图期望=independent_parallel（期望非观测：设计意图"
                                "期望，非观测结论；走 C1 §0 B 观测门）——归因前提=per-pod cfs_throttle 独判"
                                "（B5 审计最尖锐处，OPEN-Q3）",
        reference_dependencies={"F1": ("S12",), "F2": ("S27",)},
        obs_r1_audit="PASS：live 容器={backend pod, sasrec pod}，两腿两容器一一对应、无交叉；backend 键按实际"
                     " pod 名（app=backend_api 映射纪律，§4 规避 3）；CRI 目标从 pinned pods 派生、单一 "
                     'pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S12"].preconditions[0],      # backend live-label pin (F-S1-3)
            ATOM_SPECS["S27"].preconditions[0],      # sasrec no-restart pin semantics
            ATOM_SPECS["S27"].preconditions[1],      # workers=8 quota/topology coupling
            _dual_cpu_throttle_attribution_precondition(),
            _reference_ordering_precondition(
                "D27", "F1=S12 service_cpu_saturation@backend, F2=S27 service_cpu_saturation@sasrec"),
            _cpu_cpu_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C5×2、§3 D27 行、§4、§7、§8",
            "catalog_scenario": "D27 (origin=triple_pair_completion T06 拆对; id_reused=False; legacy=null; "
                                "composition_signature service_cpu_saturation@backend || "
                                "service_cpu_saturation@sasrec; G=2)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（双腿；backend carrier_hard=True；sasrec W=8 SOFT B5 "
                      "审计 L13418-13422）",
            "origin": "triple_pair_completion（T06 拆对）；T06 三腿子对闭合={D27, D26, D09}（CROSS-CUTTING "
                      "§7）",
            "parameter_status": "NOT_FROZEN（backend {workers:2 候选, load_percent:100, duration_s:80 覆盖组"
                                "合窗}；sasrec {workers:8（不重启 pod＋拓扑绑定）, load_percent:100, "
                                "duration_s:72 覆盖组合窗}；MINOR-1 不变量 80≥80/72≥72 成立）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D27 行：main=backend /api/stats/model——backend "
                                      "cpu 受害；bypass=sasrec versioned /recommend inference POST——sasrec cpu 在场（SOFT→throttle 优先）；"
                                      "业务证据=backend 腿 backend_api ratio＋sasrec 腿 sasrec_api ratio（RATIO "
                                      "禁绝对）＋throttle；pricing 重定向不涉）",
            "timing_status": "NOT_FROZEN（legacy=null 新对，公共目标窗＋注入 F1→F2＋恢复 reverse；profile "
                             "d27_common_target_window_dual_cpu，选择理由在 timing.decision）",
            "gt_correspondence": "两腿→ratio＋per-pod throttle 独判（本组归因前提，B5 审计最尖锐处 L13418-"
                                 "13422；OPEN-Q3 新 Q 缺位硬化为 Precondition）；sasrec RATIO 禁绝对教训随腿",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D32": ComboSpec(
        scenario_id="D32", purpose="pilot", scope="m1_main",
        rule_version_base="d32-combo-pilot", random_seed=11,
        phases=COMBO4_CPUPOD_PHASES,
        legs=(_combo_leg("S04", COMBO4_CPUPOD_CPU_WINDOW, duration_s=105),
              _combo_leg("S14", COMBO4_CPUPOD_POD_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d32-main-order-detail-pod", service="order:5010",
                              path_prefix="/api/orders/", carrier="combo-main-panel-order-detail",
                              direct_api=True),
            CarrierStreamSpec(stream_id="d32-bypass-catalog-items-cpu", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-bypass-panel-catalog-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("order-pod-logs"),
        signals=(_combo_signal("S04"), _combo_signal("S14")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_error_fraction"),
        injection_signals=(_combo_injection_signal("S04"), _combo_injection_signal("S14")),
        timing=ComboTimingProfile(
            name="d32_partial_overlap_cpu_outer_pod_inner", geometry="partial_overlap",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-4 D32(d)：legacy=null（T08 拆对新对）；旧 T08 实现 partial_overlap（pod 子窗）；本组"
                     "含 pod 腿同型；采纳本图建议=partial_overlap 承旧 T08 实现形态——但显式声明并经 C1 观测"
                     "核定，不默认沿用（B3-03 §2 D31/D32 警示同源）"
                     "（EXEC-POD-XFER 2026-09-22：pod 子窗 35→60s 窗/CRD 90s，lag 头 35s 由 pod 族 carrier settle "
                     "吸收，评估窗 ~34 样本，有效下线段 [~89,~126] 落 cpu 稳态段→co-active ~37 样本；cpu 窗 "
                     "110→135（duration 80→105）保有效 co-active 地板；偏移 {30,55,115,135} 最小正间隙 20＞默认"
                     "预算和 19）：catalog cpu F1 (30,135) 整窗预注（workers=1 "
                     "承旧 T08 catalog 腿 L13383-13384；CRD 秒级＋爬坡 15s 头预算→settled ~45；duration_s=105 "
                     "覆盖窗），order pod F2 (55,115) below_kill_band v2 子窗；"
                     "恢复 reverse：pod CRD 删除@115（restore+Ready ~128 during 内，evidence-only，restart_delta 窗"
                     "归属不溢入 cpu 窗）→catalog CRD 删除@135（秒级，≤136≤during 末 140）；timing NOT_FROZEN",
        ),
        interaction_expectation="无旧标签（新对）——设计意图期望=independent_parallel（期望非观测：设计意图"
                                "期望，非观测结论；走 C1 §0 B 观测门）",
        reference_dependencies={"F1": ("S04",), "F2": ("S14",)},
        obs_r1_audit="PASS：live 容器={catalog pod, order pod}，两腿两容器一一对应、无交叉；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S14"].preconditions[0],      # order probe order_no premise
            _reference_ordering_precondition(
                "D32", "F1=S04 service_cpu_saturation@catalog, F2=S14 service_unavailable@order"),
            _cpu_cpu_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C5/C4、§3 D32 行、§4、§7、§8",
            "catalog_scenario": "D32 (origin=triple_pair_completion T08 拆对; id_reused=False; legacy=null; "
                                "composition_signature service_cpu_saturation@catalog || "
                                "service_unavailable@order; G=2)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（catalog cpu 腿，W=1 承 L13383-13384）+ m14-ledger-"
                      "b1/05-pod-failure.md（order pod 腿）+ B3-03 §2 D31/D32 警示同源",
            "origin": "triple_pair_completion（T08 拆对）；承旧 T08 实现形态（cpu 整窗＋pod 子窗，显式声明）；"
                      "T08 三腿子对闭合={D33, D31, D32}（CROSS-CUTTING §7）",
            "parameter_status": "NOT_FROZEN（catalog {workers:1 承旧 T08 catalog 腿 L13383-13384（与 S04 联动"
                                "论证）, load_percent:100, duration_s:105 覆盖组合窗}；order {action: "
                                "pod-failure, duration_s: 90}=below_kill_band v2 剖面（C4 std）；MINOR-1 不变量 "
                                "105≥105/90≥60 成立；EXEC-POD-XFER 2026-09-22 重切）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D32 行：main=order /api/orders/<order_no>——pod "
                                      "错误即时（order_no 探针前提随 S14 前置）；bypass=catalog-direct——cpu "
                                      "在场；pricing 重定向不涉）",
            "timing_status": "NOT_FROZEN（legacy=null 新对，partial_overlap 承旧 T08 实现形态（显式声明经 C1 "
                             "核定）＋恢复 reverse；profile d32_partial_overlap_cpu_outer_pod_inner，选择"
                             "理由在 timing.decision）",
            "gt_correspondence": "pod 腿=order_service error fraction＋restart_delta∈{1,2} 窗签名；cpu 腿="
                                 "catalog_service ratio＋throttle（正则排 catalog-gw，L13498；OPEN-Q3 新 Q "
                                 "缺位）；catalog workers=1 vs 2 与 S04 联动论证",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "D33": ComboSpec(
        scenario_id="D33", purpose="pilot", scope="m1_main",
        rule_version_base="d33-combo-pilot", random_seed=11,
        phases=COMBO4_CPUPOD_PHASES,
        legs=(_combo_leg("S11", COMBO4_CPUPOD_CPU_WINDOW, duration_s=105),
              _combo_leg("S14", COMBO4_CPUPOD_POD_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="d33-main-order-detail-pod", service="order:5010",
                              path_prefix="/api/orders/", carrier="combo-main-panel-order-detail",
                              direct_api=True),
            CarrierStreamSpec(stream_id="d33-bypass-review-query-list-pod", service="review-query:5018",
                              path_prefix="/api/reviews?item_id=", path_suffix="&per_page=5&enrich=1",
                              carrier="combo-bypass-panel-review-query-list", direct_api=True)),
        telemetry=_std_telemetry("order-pod-logs"),
        signals=(_combo_signal("S11"), _combo_signal("S14")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_error_fraction"),
        injection_signals=(_combo_injection_signal("S11"), _combo_injection_signal("S14")),
        timing=ComboTimingProfile(
            name="d33_partial_overlap_cpu_outer_pod_inner", geometry="partial_overlap",
            inject_order=("F1", "F2"), recover_order=("F2", "F1"),
            decision="SPECS-4 D33(d)：legacy=null（T08 拆对新对）；同 D32：本图建议=partial_overlap 承旧 T08 "
                     "实现形态（显式声明）：rq cpu F1 (30,110) 整窗预注（旧 T08 rq 腿 carrier_hard=True、"
                     "workers=2 候选；CRD 秒级＋爬坡 15s 头预算→settled ~45；duration_s=105 覆盖窗），order pod "
                     "F2 (55,115) below_kill_band v2 子窗（EXEC-POD-XFER 2026-09-22：pod 子窗 35→60s 窗/CRD "
                     "90s，lag 头 35s 由 pod 族 carrier settle 吸收，评估窗 ~34 样本，有效下线段 [~89,~126] 落 "
                     "cpu 稳态段→co-active ~37 样本；rq 窗 110→135（duration 80→105）保有效 co-active 地板；"
                     "偏移 {30,55,115,135} 最小正间隙 20＞默认预算和 19）；恢复 reverse：pod CRD "
                     "删除@115（restore+Ready ~128 during 内，evidence-only）→rq CRD 删除@135（秒级，≤136≤during "
                     "末 140）；timing NOT_FROZEN",
        ),
        interaction_expectation="无旧标签（新对）——设计意图期望=independent_parallel（期望非观测：设计意图"
                                "期望，非观测结论；走 C1 §0 B 观测门）",
        reference_dependencies={"F1": ("S11",), "F2": ("S14",)},
        obs_r1_audit="PASS：live 容器={review-query pod, order pod}，两腿两容器一一对应、无交叉；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S14"].preconditions[0],      # order probe order_no premise
            _reference_ordering_precondition(
                "D33", "F1=S11 service_cpu_saturation@review-query, F2=S14 service_unavailable@order"),
            _cpu_cpu_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C5/C4、§3 D33 行、§4、§7、§8",
            "catalog_scenario": "D33 (origin=triple_pair_completion T08 拆对; id_reused=False; legacy=null; "
                                "composition_signature service_cpu_saturation@review-query || "
                                "service_unavailable@order; G=2)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（rq cpu 腿，旧 T08 carrier_hard=True）+ m14-ledger-"
                      "b1/05-pod-failure.md（order pod 腿）",
            "origin": "triple_pair_completion（T08 拆对）；承旧 T08 实现形态（显式声明）；T08 三腿子对闭合="
                      "{D33, D31, D32}（CROSS-CUTTING §7）",
            "parameter_status": "NOT_FROZEN（rq {workers:2 候选（旧 T08 carrier_hard=True）, load_percent:100, "
                                "duration_s:105 覆盖组合窗}；order {action: pod-failure, duration_s: 90}="
                                "below_kill_band v2 剖面（C4 std）；MINOR-1 不变量 105≥105/90≥60 成立；"
                                "EXEC-POD-XFER 2026-09-22 重切）",
            "request_profile_status": "NOT_FROZEN（双流载体 §3 D33 行：main=order /api/orders/<order_no>——pod "
                                      "错误即时（order_no 探针前提随 S14 前置）；bypass=rq /api/reviews——cpu "
                                      "在场；pricing 重定向不涉）",
            "timing_status": "NOT_FROZEN（legacy=null 新对，partial_overlap 承旧 T08 实现形态（显式声明）；"
                             "profile d33_partial_overlap_cpu_outer_pod_inner，选择理由在 timing.decision）",
            "gt_correspondence": "pod 腿=order error/restart；cpu 腿=review_query_service ratio＋throttle"
                                 "（OPEN-Q3 新 Q 缺位）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "T05": ComboSpec(
        scenario_id="T05", purpose="pilot", scope="m1_main",
        rule_version_base="t05-combo-pilot", random_seed=11,
        phases=T05_PHASES,
        legs=(_combo_leg("S18", T05_CHECKOUT_POD_WINDOW),
              _combo_leg("S10", T05_CART_CPU_WINDOW, duration_s=105),
              _combo_leg("S28", T05_PRICING_CPU_WINDOW, duration_s=95)),
        carriers=(
            CarrierStreamSpec(stream_id="t05-main-checkout-health", service="checkout:5011",
                              path_prefix="/health", carrier="combo-main-panel-checkout-health",
                              direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="t05-bypass-cart-health", service="cart:5006",
                              path_prefix="/health", carrier="combo-bypass-panel-cart-health",
                              direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="t05-third-pricing-items-cpu", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-third-panel-pricing-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("checkout-pod-logs"),
        signals=(_combo_signal("S18"), _combo_signal("S10"), _combo_signal("S28")),
        injection_signal_kinds=("carrier_error_fraction", "carrier_p95_ratio", "carrier_p95_ratio"),
        injection_signals=(_combo_injection_signal("S18"), _combo_injection_signal("S10"),
                           _combo_injection_signal("S28")),
        timing=ComboTimingProfile(
            name="t05_partial_overlap_cpu_outer_pod_inner", geometry="partial_overlap",
            inject_order=("F2", "F3", "F1"), recover_order=("F1", "F3", "F2"),
            decision="SPECS-4 T05(d)：legacy=partial_overlap＝默认（旧 triple05 形态：checkout podfail F1 子窗＋"
                     "cart/pricing CPU 整窗；渲染 L14950-14955、预注入 L15357-15363、恢复 L16121-16126，B1-06 "
                     "§3 T05）：CPU 腿恒整 during 预注（OUTER）——cart F2 (30,110) 先注（W=1 标定 L13365-13370；"
                     "CRD 秒级＋爬坡 15s 头预算→settled ~45；duration_s=80 覆盖窗），pricing F3 (35,105) 紧随"
                     '（SOFT L13371；settled ~50；duration_s=95 覆盖窗；collection：3s 头距按 '
                     'collection/collection 5s 网格重切，33→35）'
                     '（EXEC-POD-XFER 2026-09-22 重切：实测 pod-failure 传递函数＝inject 确认后 lag 28.8-30.9s 才咬合（声明界 35＝pod 族 carrier settle 吸收），restore+Ready ~11-13s 在 recover 动作内确认；pod 子窗 35→60s 窗/CRD 90s，评估窗 ~34 样本≥地板；有效下线段 [~89,~126] 即三腿 co-active 段（两 CPU 腿稳态 ~45/~50 起）＝~37 样本≥C1 §2.5 地板 30，逐腿 n≥30 保持；外窗 cpu/net 腿协同延长（110/105→135/130）保有效 co-active 地板；D23/D28/D31 子几何同值声明解除（参照 collection D15/T01 先例）；偏移 {30,35,55,115,130,135} 最小正间隙 5＞r48_fixed 预算和 4.8；during 125→140）：cart F2 (30,135；duration_s=105)、pricing F3 '
                     "(35,130)，checkout pod "
                     "F1 (55,115) mid INNER 子窗（below_kill_band v2）；恢复逆序＝reverse（F1 pod CRD 删除@115"
                     "（restore+Ready ~128 during 内，evidence-only）→F3 pricing@130→F2 cart@135，全部≤during "
                     "末 140）；timing NOT_FROZEN",
        ),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测"
                                "门）——legacy CATALOG 标签 independent_parallel；三腿判定结构依赖 O-E5（逐 "
                                "amplifier 子对降维：D24/D23/D25 三个子对已有设计，CROSS-CUTTING §7）",
        reference_dependencies={"F1": ("S18",), "F2": ("S10",), "F3": ("S28",)},
        obs_r1_audit="PASS：live 容器={checkout pod, cart pod, pricing pod}，三腿三容器一一对应、无交叉；CRI "
                     '目标从 pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            _triple_oe5_precondition("T05", "checkout×cart=D24, cart×pricing=D23, checkout×pricing=D25"),
            _dual_cpu_throttle_attribution_precondition(),
            _pricing_scale_up_precondition(),
            _reference_ordering_precondition(
                "T05", "F1=S18 service_unavailable@checkout, F2=S10 service_cpu_saturation@cart, "
                       "F3=S28 service_cpu_saturation@pricing"),
            _cpu_cpu_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C4/C5×2、§3 T05 行与三腿说明、§4、§7、§8",
            "catalog_scenario": "T05 (origin=retained_composition 旧triple05; id_reused=False; legacy="
                                "partial_overlap/independent_parallel; composition_signature "
                                "service_unavailable@checkout || service_cpu_saturation@cart || "
                                "service_cpu_saturation@pricing; G=3)",
            "ledger": "m14-ledger-b1/05-pod-failure.md（checkout pod 腿）+ m14-ledger-b3/01-service-cpu.md"
                      "（cart W=1/pricing SOFT 知识随腿定义冻结继承，B3-01 §3.2-1）",
            "origin": "retained_composition（旧 triple05）；三流处置=pricing cpu 第三腿以自载体 t05-third-"
                      "pricing-items-cpu（pricing-direct，SOFT）进 streams[] 并在 eval 配置标角色（O-E5 裁定"
                      "前建议形态，§3 三腿说明）；T05 三腿子对闭合={D24, D23, D25}（CROSS-CUTTING §7，机器"
                      "不变量 TRIPLE_PAIR_CLOSURE）",
            "parameter_status": "NOT_FROZEN（checkout {action: pod-failure, duration_s: 90}=below_kill_band v2 剖面"
                                "（C4）；cart {workers:1 标定注记 L13365-13370, load_percent:100, duration_s:105 "
                                "覆盖组合窗}；pricing {workers:2 候选, load_percent:100, duration_s:95 覆盖组"
                                "合窗}（SOFT L13371）；per-leg workers 档禁全局默认吞并；MINOR-1 不变量 "
                                "90≥60/105≥105/95≥95 成立（EXEC-POD-XFER 重切后窗跨 105/95）",
            "request_profile_status": "NOT_FROZEN（三流载体 §3 T05 行：main=checkout /health——pod 错误在 lag 头（界 35s）后咬合；"
                                      "bypass=cart /health——cart cpu 在场（W=1）；third=pricing pricing-"
                                      "direct——pricing cpu 自载体（SOFT→throttle 优先；O-E5 前建议形态）；"
                                      "fan-out same_request_path：三根共一条 preview 路径（旧组合键 L13361/"
                                      "L13375）；pricing-direct 流只涉 pricing scale-up 前提〔§3 约束 2，硬化"
                                      "为 Precondition（ASM-3 审核 NIT-B）〕不涉重定向）",
            "timing_status": "NOT_FROZEN（legacy=partial_overlap 默认＝承旧 T05 形态＋CPU 腿整窗预注＋pod mid "
                             "子窗＋恢复逆序；profile t05_partial_overlap_cpu_outer_pod_inner）",
            "gt_correspondence": "pod 腿=error/restart；两 CPU 腿→ratio＋per-pod throttle（归因前提 OPEN-Q3，"
                                 "硬化为 Precondition）；三腿判定依赖 O-E5 逐 amplifier 子对降维（D24/D23/D25 "
                                 "子案已有设计；设计文字非观测，硬化为 Precondition）；cart workers=1/pricing "
                                 "SOFT 知识随腿定义冻结继承（B3-01 §3.2-1）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "T06": ComboSpec(
        scenario_id="T06", purpose="pilot", scope="m1_main",
        rule_version_base="t06-combo-pilot", random_seed=11,
        phases=T06_PHASES,
        legs=(_combo_leg("S12", T06_BACKEND_CPU_WINDOW, duration_s=80),
              _combo_leg("S27", T06_SASREC_CPU_WINDOW, duration_s=72),
              _combo_leg("S01", T06_GW_NET_WINDOW, duration_s=67)),
        carriers=(
            CarrierStreamSpec(stream_id="t06-main-pricing-via-gw", service="pricing:5014",
                              path_prefix="/api/pricing/", carrier="combo-main-panel-pricing-via-gw",
                              
                              direct_api=True, trace_service="pricing_service"),
            CarrierStreamSpec(stream_id="t06-bypass-backend-stats", service="backend:5000",
                              path_prefix="/api/stats/model", carrier="combo-bypass-panel-backend-stats",
                              direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="t06-third-sasrec-inference-post-v1", service="sasrec:8200",
                              path_prefix="/recommend", carrier="combo-third-panel-sasrec-inference-post-v1",
                              direct_api=True, rate_rps=1.0, max_concurrency=1,
                              client_retry_limit=0, parameterized=False)),
        telemetry=_std_telemetry("backend-pod-logs"),
        signals=(_combo_signal("S12"), _combo_signal("S27"), _combo_signal("S01")),
        injection_signal_kinds=("carrier_p95_ratio", "carrier_p95_ratio", "metric_threshold"),
        injection_signals=(_combo_injection_signal("S12"), _combo_injection_signal("S27"), None),
        timing=ComboTimingProfile(
            name="t06_common_target_window_triple", geometry="common_target_window",
            inject_order=("F1", "F2", "F3"), recover_order=("F3", "F2", "F1"),
            decision="SPECS-4 T06(d)：legacy=simultaneous＝默认（旧注入序 cpu→cpu→netem＋恢复逆序 "
                     "L15364-15365/L16118-16126，B1-06 §3 T06）；simultaneous 由公共目标窗承载（嵌套副产物"
                     "显式声明 §8-4）：backend cpu F1 (30,110) 先注（W=2；CRD 秒级＋爬坡 15s 头预算→settled "
                     "~45；duration_s=80 覆盖窗），sasrec cpu F2 (33,105) 紧随（W=8 不重启约束＋拓扑绑定；"
                     'settled ~50；duration_s=72 覆盖窗——F1/F2=D27 子几何同值 collection 3→5），gw netem F3 (40,100) 最后'
                     "（注入序知识 cpu→cpu→netem；CRD 秒级→settled ~38；duration_s=67 覆盖窗）；三腿 co-"
                     'active 稳态 50→100=50 样本≥C1 §2.5 地板 30（collection：3s 级联头扩 5s 格网、netem 尾 103→100）；恢复恰好逆序：net CRD 删除@100→sasrec@105→'
                     "backend@110（秒级，≤111≤during 末 115）；前提=pricing 重定向（main 流经 gw）；timing "
                     "NOT_FROZEN",
        ),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测"
                                "门）——legacy CATALOG 标签 independent_parallel；三腿判定依赖 O-E5（逐 "
                                "amplifier 子对降维：D27/D26/D09 三个子对已有设计，CROSS-CUTTING §7）",
        reference_dependencies={"F1": ("S12",), "F2": ("S27",), "F3": ("S01",)},
        obs_r1_audit="PASS：live 容器={backend pod, sasrec pod, catalog-gw pod}，三腿三容器一一对应、无交叉；"
                     "backend 键按实际 pod 名（app=backend_api 映射纪律，§4 规避 3）；CRI 目标从 pinned "
                     'pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S12"].preconditions[0],      # backend live-label pin (F-S1-3)
            ATOM_SPECS["S27"].preconditions[0],      # sasrec no-restart pin semantics
            ATOM_SPECS["S27"].preconditions[1],      # workers=8 quota/topology coupling
            _pricing_gw_path_precondition(),
            _triple_oe5_precondition("T06", "backend×sasrec=D27, backend×netdelay=D26, sasrec×netdelay=D09"),
            _dual_cpu_throttle_attribution_precondition(),
            _reference_ordering_precondition(
                "T06", "F1=S12 service_cpu_saturation@backend, F2=S27 service_cpu_saturation@sasrec, "
                       "F3=S01 network_delay@catalog-gw"),
            _cpu_cpu_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C1/C5×2、§3 T06 行与三腿说明、§4、§7、§8",
            "catalog_scenario": "T06 (origin=retained_composition 旧triple06; id_reused=False; legacy="
                                "simultaneous/independent_parallel; composition_signature "
                                "service_cpu_saturation@backend || service_cpu_saturation@sasrec || "
                                "network_delay@catalog-gw; G=3)",
            "ledger": "m14-ledger-b3/01-service-cpu.md（backend/sasrec 双 cpu 腿，sasrec W=8 SOFT B5 审计）+ "
                      "m14-ledger-b1/03-network-chaos.md（gw netem 腿）",
            "origin": "retained_composition（旧 triple06）；三流处置=sasrec cpu 第三腿以自载体 t06-third-"
                      "sasrec-inference-post-v1（SOFT→throttle 优先）进 streams[] 并在 eval 配置标角色（O-E5 裁定前"
                      "建议形态）；T06 三腿子对闭合={D27, D26, D09}（CROSS-CUTTING §7，机器不变量 "
                      "TRIPLE_PAIR_CLOSURE）",
            "parameter_status": "NOT_FROZEN（backend {workers:2 候选, load_percent:100, duration_s:80 覆盖组"
                                "合窗}；sasrec {workers:8（拓扑绑定＋不重启约束）, load_percent:100, "
                                "duration_s:72 覆盖组合窗}；gw {latency_ms:500, jitter_ms:50, correlation_"
                                "percent:0, duration_s:67 覆盖组合窗} 沿 S01 候选（C1）；MINOR-1 不变量 "
                                "80≥80/72≥72/67≥67 成立）",
            "request_profile_status": "NOT_FROZEN（三流载体 §3 T06 行：main=pricing-via-gw——gw delay 主通道"
                                      "（前提=pricing 重定向，前置硬化）；bypass=backend /api/stats/model——"
                                      "backend cpu 在场；third=sasrec versioned /recommend inference POST——sasrec cpu 自载体（SOFT→"
                                      "throttle 优先））",
            "timing_status": "NOT_FROZEN（legacy=simultaneous 默认由公共目标窗承载＋注入序 cpu→cpu→netem＋"
                             "恢复恰好逆序；profile t06_common_target_window_triple）",
            "gt_correspondence": "net 腿=延迟臂（pricing_service p95 位移）；两 CPU 腿→ratio＋per-pod throttle"
                                 "（B5 归因前提 OPEN-Q3，硬化为 Precondition）；sasrec W=8 拓扑绑定＋不重启"
                                 "约束随前置；三腿判定依赖 O-E5 逐 amplifier 子对降维（D27/D26/D09 子案已有"
                                 "设计；设计文字非观测，硬化为 Precondition）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "T07": ComboSpec(
        scenario_id="T07", purpose="pilot", scope="m1_main",
        rule_version_base="t07-combo-pilot", random_seed=11,
        phases=T07_PHASES,
        legs=(_combo_leg("S21", T07_RECAGENT_NET_WINDOW, duration_s=95),
              _combo_leg("S27", T07_SASREC_CPU_WINDOW, duration_s=114),
              _combo_leg("S03", T07_CATALOG_POD_WINDOW)),
        carriers=(
            CarrierStreamSpec(stream_id="t07-main-catalog-items-via-gw", service="catalog-gw:80",
                              path_prefix="/api/items/", carrier="combo-main-panel-catalog-via-gw",
                              
                              direct_api=True, trace_service="catalog_service"),
            CarrierStreamSpec(stream_id="t07-bypass-recagent-health", service="rec-agent:5001",
                              path_prefix="/recommend/health", carrier="combo-bypass-panel-recagent-health",
                              direct_api=True, parameterized=False),
            CarrierStreamSpec(stream_id="t07-third-sasrec-inference-post-v1", service="sasrec:8200",
                              path_prefix="/recommend", carrier="combo-third-panel-sasrec-inference-post-v1",
                              direct_api=True, rate_rps=1.0, max_concurrency=1,
                              client_retry_limit=0, parameterized=False)),
        telemetry=_std_telemetry("rec-agent-pod-logs"),
        signals=(_combo_signal("S21"), _combo_signal("S27"), _combo_signal("S03")),
        injection_signal_kinds=("metric_threshold", "carrier_p95_ratio", "carrier_error_fraction"),
        injection_signals=(None, _combo_injection_signal("S27"), _combo_injection_signal("S03")),
        timing=ComboTimingProfile(
            name="t07_partial_overlap_cpu_outer_pod_inner", geometry="partial_overlap",
            inject_order=("F2", "F1", "F3"), recover_order=("F3", "F1", "F2"),
            decision="SPECS-4 T07(d): preserve the legacy partial_overlap geometry and F2->F1->F3 injection / "
                     'F3->F1->F2 recovery order. collection-37 re-cut from the two native short F2 apply receipts '
                     "(4.031 s and 4.015 s against the 4 s cap): sasrec CPU F2 [30,144], workers=8/load=100, "
                     "duration_s=114 (+9 s declared exposure); rec-agent netem F1 [42,137], duration_s=95 "
                     "unchanged; catalog pod F3 [55,115], 60 s subwindow and CRD cap 90 unchanged. Positive "
                     "action offsets {30,42,55,115,137,144} give min gap 7 s for the explicit T07 5.0/0.8 s "
                     "action tier. EXEC-POD-XFER v2 retains the measured pod-failure lag 28.8-30.9 s and "
                     "restore+Ready ~11-13 s; effective pod-down [~89,~126] keeps the triple co-active span "
                     "at 37 samples >= C1 floor 30. F3 recover bound is 5+15+0.8=20.8 s, completing by "
                     "135.8 before F1 recovery @137; F1's 5.8 s bound fits before F2 recovery @144. Short "
                     "phases are 30/130/30; F2's 144+5.8=149.8 s recovery ceiling fits before during end 160. "
                     "Long300 still forces "
                     "300/300/300. T07 D28/D29/D30 projections inherit the re-cut parent windows and doses. "
                     "This is a declared design re-cut, not a live effectiveness claim; S21 first-use tc smoke "
                     "remains a separate prerequisite.",
        ),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测"
                                "门）——legacy CATALOG 标签 independent_parallel；三腿判定依赖 O-E5（逐 "
                                "amplifier 子对降维：D28/D30/D29 三个子对已有设计，CROSS-CUTTING §7）",
        reference_dependencies={"F1": ("S21",), "F2": ("S27",), "F3": ("S03",)},
        obs_r1_audit="PASS：live 容器={rec-agent pod, sasrec pod, catalog pod}，三腿三容器一一对应、无交叉；"
                     "rec-agent 键按实际 pod 名（app=recommendation_agent 映射纪律，§4 规避 3）；CRI 目标从 "
                     'pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S21"].preconditions[0],      # netem face first-use smoke (binds this combo)
            ATOM_SPECS["S21"].preconditions[1],      # live-label pin (recommendation_agent, F-S1-3)
            ATOM_SPECS["S27"].preconditions[0],      # sasrec no-restart pin semantics
            ATOM_SPECS["S27"].preconditions[1],      # workers=8 quota/topology coupling
            _triple_oe5_precondition("T07", "recagent-net×sasrec=D28, sasrec×catalog=D30, recagent-net×catalog=D29"),
            _reference_ordering_precondition(
                "T07", "F1=S21 network_delay@rec-agent, F2=S27 service_cpu_saturation@sasrec, "
                       "F3=S03 service_unavailable@catalog"),
            _cpu_cpu_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C3/C5/C4、§3 T07 行与三腿说明、§4、§7、§8",
            "catalog_scenario": "T07 (origin=retained_composition 旧同号; id_reused=False; legacy="
                                "partial_overlap/independent_parallel; composition_signature "
                                "network_delay@rec-agent || service_cpu_saturation@sasrec || "
                                "service_unavailable@catalog; G=3)",
            "ledger": "m14-ledger-b1/03-network-chaos.md（rec-agent netem 腿，S21 首用；全 egress 语义）+ "
                      "m14-ledger-b3/01-service-cpu.md（sasrec cpu 腿，W=8 SOFT）+ m14-ledger-b1/05-pod-"
                      "failure.md（catalog pod 腿）；rec-agent 五件套复用与串扰论证 L13466-13477 为观测设计"
                      "输入",
            "origin": "retained_composition（旧同号）；三流处置=sasrec cpu 第三腿以自载体 t07-third-sasrec-"
                      "health（SOFT→throttle 优先）进 streams[] 并在 eval 配置标角色（O-E5 裁定前建议形态）；"
                      "T07 三腿子对闭合={D28, D30, D29}（CROSS-CUTTING §7，机器不变量 TRIPLE_PAIR_CLOSURE）；"
                      "S21 首用 tc 冒烟前置约束本组（蓝图 (f)）",
            "parameter_status": "NOT_FROZEN（rec-agent {latency_ms:450, jitter_ms:90, correlation_percent:0, "
                                "duration_s:95} 沿 S21 候选（C3；全 egress，注入面未验证→首用 tc 冒烟前置）；"
                                "sasrec {workers:8, load_percent:100, duration_s:114} 沿 S27 候选（C5），r51-37 "
                                "明确 +9 s 外窗；catalog pod {action: pod-failure, duration_s:90}=below_kill_v2 "
                                "剖面（C4）；MINOR-1 不变量 95≥95/114≥114/90≥60 成立）",
            "request_profile_status": "NOT_FROZEN（三流载体 §3 T07 行：main=catalog-items-via-gw——catalog pod "
                                      "受害主通道（catalog-items-via-gw 流不涉 pricing 前提）；bypass=rec-"
                                      "agent /recommend/health——rec-agent delay 在场；third=sasrec versioned /recommend inference POST——"
                                      "sasrec cpu 自载体（SOFT→throttle））",
            "timing_status": "NOT_FROZEN（legacy=partial_overlap 默认＝承旧 T07 形态＋sasrec/netem 整窗预注＋"
                             "pod mid INNER 子窗＋恢复逆序；profile t07_partial_overlap_cpu_outer_pod_inner）",
            "gt_correspondence": "net 腿=延迟臂（/recommend/health）——旧 T07 判据=载体绝对位移 ≥800 无 control "
                                 "臂（旧 T07 定义，知识不迁移为结论）；cpu 腿=sasrec_api ratio（SOFT）＋"
                                 "throttle（OPEN-Q3 新 Q 缺位）；pod 腿=error/restart；三腿判定依赖 O-E5 逐 "
                                 "amplifier 子对降维（D28/D30/D29 子案已有设计；设计文字非观测，硬化为 "
                                 "Precondition）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
    "T08": ComboSpec(
        scenario_id="T08", purpose="pilot", scope="m1_main",
        rule_version_base="t08-combo-pilot", random_seed=11,
        phases=T08_PHASES,
        legs=(_combo_leg("S14", T08_ORDER_POD_WINDOW),
              _combo_leg("S11", T08_RQ_CPU_WINDOW, duration_s=95),
              _combo_leg("S04", T08_CATALOG_CPU_WINDOW, duration_s=105)),
        carriers=(
            CarrierStreamSpec(stream_id="t08-main-order-detail-pod", service="order:5010",
                              path_prefix="/api/orders/", carrier="combo-main-panel-order-detail",
                              direct_api=True),
            CarrierStreamSpec(stream_id="t08-bypass-review-query-list-pod", service="review-query:5018",
                              path_prefix="/api/reviews?item_id=", path_suffix="&per_page=5&enrich=1",
                              carrier="combo-bypass-panel-review-query-list", direct_api=True),
            CarrierStreamSpec(stream_id="t08-third-catalog-items-cpu", service="catalog:5005",
                              path_prefix="/api/items/", carrier="combo-third-panel-catalog-direct",
                              direct_api=True)),
        telemetry=_std_telemetry("order-pod-logs"),
        signals=(_combo_signal("S14"), _combo_signal("S11"), _combo_signal("S04")),
        injection_signal_kinds=("carrier_error_fraction", "carrier_p95_ratio", "carrier_p95_ratio"),
        injection_signals=(_combo_injection_signal("S14"), _combo_injection_signal("S11"),
                           _combo_injection_signal("S04")),
        timing=ComboTimingProfile(
            name="t08_partial_overlap_cpu_outer_pod_inner", geometry="partial_overlap",
            inject_order=("F3", "F2", "F1"), recover_order=("F1", "F2", "F3"),
            decision="SPECS-4 T08(d)：legacy=partial_overlap＝默认且必须显式声明——旧『设计表 simultaneous vs "
                     "实现 partial_overlap』实现偏离先例（design_note L13389-13392：含 podfail 腿时整窗 "
                     "simultaneous 会把 during 压到 ≤60s〔约束 A podfail 上限〕撑不起 CPU throttle 统计窗——"
                     "约束 C L13320、分派注释 L250-251）：时序标签不得默认沿用旧标签或旧实现任一方（B1-06 "
                     "§3 T08 装配警示、B3-03 §4-8），须经 C1 观测核定；装配采纳本图建议=承实现形态 "
                     "partial_overlap（两 CPU 整窗＋order pod 子窗）"
                     '（EXEC-POD-XFER 2026-09-22 重切：实测 pod-failure 传递函数＝inject 确认后 lag 28.8-30.9s 才咬合（声明界 35＝pod 族 carrier settle 吸收），restore+Ready ~11-13s 在 recover 动作内确认；pod 子窗 35→60s 窗/CRD 90s，评估窗 ~34 样本≥地板；有效下线段 [~89,~126] 即三腿 co-active 段＝~37 样本≥C1 §2.5 地板 30，逐腿 n≥30 保持；外窗 cpu 腿协同延长（110/105→135/130）保有效 co-active 地板；D31 子几何同值声明解除（collection 先例）；偏移 {30,35,55,115,130,135} 最小正间隙 5＞r48_fixed 预算和 4.8；during 125→140）：catalog cpu F3 (30,135) 先注'
                     "（W=1 承 L13383-13384；CRD 秒级＋爬坡 15s 头预算→settled ~45；duration_s=105 覆盖窗），"
                     "rq cpu F2 (35,130) 紧随（settled ~48；duration_s=95 覆盖窗），order pod F1 "
                     "(55,115) mid 子窗（below_kill_band v2）；"
                     "恢复逆序＝reverse（F1 pod CRD 删除@115（restore+Ready ~128 during 内，evidence-only）"
                     "→F2 rq@130→F3 catalog@135，全部≤during 末 140）；timing NOT_FROZEN",
        ),
        interaction_expectation="independent_parallel（期望非观测：设计意图期望，非观测结论；走 C1 §0 B 观测"
                                "门）——legacy CATALOG 标签 independent_parallel；三腿判定依赖 O-E5（逐 "
                                "amplifier 子对降维：D33/D31/D32 三个子对已有设计，CROSS-CUTTING §7）",
        reference_dependencies={"F1": ("S14",), "F2": ("S11",), "F3": ("S04",)},
        obs_r1_audit="PASS：live 容器={order pod, review-query pod, catalog pod}，三腿三容器一一对应、无交叉；"
                     'CRI 目标从 pinned pods 派生、单一 pod: 键型（OBS-collection）',
        preconditions=(
            ATOM_SPECS["S14"].preconditions[0],      # order probe order_no premise
            _triple_oe5_precondition("T08", "order×rq=D33, rq×catalog=D31, catalog×order=D32"),
            _dual_cpu_throttle_attribution_precondition(),
            _reference_ordering_precondition(
                "T08", "F1=S14 service_unavailable@order, F2=S11 service_cpu_saturation@review-query, "
                       "F3=S04 service_cpu_saturation@catalog"),
            _cpu_cpu_execution_prerequisite()),
        provenance={
            "blueprint": 'configs/collection/scenarios.json: assembly definition'
                         "CROSS-CUTTING.md §2 C4/C5×2、§3 T08 行与三腿说明、§4、§7、§8",
            "catalog_scenario": "T08 (origin=retained_composition 旧triple08; id_reused=False; legacy="
                                "partial_overlap/independent_parallel; composition_signature "
                                "service_unavailable@order || service_cpu_saturation@review-query || "
                                "service_cpu_saturation@catalog; G=3)",
            "ledger": "m14-ledger-b1/05-pod-failure.md（order pod 腿）+ m14-ledger-b3/01-service-cpu.md（rq/"
                      "catalog 双 cpu 腿；旧设计-实现偏离先例 L13389-13392）",
            "origin": "retained_composition（旧 triple08）；三流处置=catalog cpu 第三腿以自载体 t08-third-"
                      "catalog-items-cpu（catalog-direct）进 streams[] 并在 eval 配置标角色（O-E5 裁定前建议"
                      "形态）；fan-in：order/review-query 两 caller enrich＋catalog 共享下游同衰（旧组合键 "
                      "L13378）；T08 三腿子对闭合={D33, D31, D32}（CROSS-CUTTING §7，机器不变量 "
                      "TRIPLE_PAIR_CLOSURE）",
            "parameter_status": "NOT_FROZEN（order {action: pod-failure, duration_s: 90}=below_kill_band v2 剖面"
                                "（C4 std）；rq {workers:2 候选（carrier_hard=True）, load_percent:100, "
                                "duration_s:72 覆盖组合窗}；catalog {workers:1 承 L13384（与 S04/S32 联动论证）, "
                                "load_percent:100, duration_s:80 覆盖组合窗}；throttle 正则排 catalog-gw"
                                "（L13498）；MINOR-1 不变量 90≥60/95≥95/105≥105 成立；EXEC-POD-XFER 2026-09-22 重切）",
            "request_profile_status": "NOT_FROZEN（三流载体 §3 T08 行：main=order /api/orders/<order_no>——pod "
                                      "错误即时（order_no 探针前提随 S14 前置）；bypass=rq /api/reviews——rq "
                                      "cpu 在场；third=catalog-direct——catalog cpu 自载体；pricing 重定向不涉）",
            "timing_status": "NOT_FROZEN（legacy=partial_overlap 默认但显式声明并经 C1 观测核定——旧设计-实现"
                             "偏离先例不默认沿用任一方；承实现形态两 CPU 整窗＋pod 子窗＋恢复逆序；profile "
                             "t08_partial_overlap_cpu_outer_pod_inner）",
            "gt_correspondence": "pod 腿=order error/restart；两 CPU 腿→ratio＋per-pod throttle（归因前提 "
                                 "OPEN-Q3，硬化为 Precondition；catalog throttle 正则排 catalog-gw）；catalog "
                                 "workers=1 与 S04/S32 联动论证；三腿判定依赖 O-E5 逐 amplifier 子对降维"
                                 "（D33/D31/D32 子案已有设计；设计文字非观测，硬化为 Precondition）",
            "execution_status": 'DESIGN_ONLY_NOT_COLLECTED（CATALOG）；装配≠采集资格（collection 组合冒烟义务）'}),
}

# COMBO-ASM-1 batch: the SPECS-1 gateway cfg family (six combinations, every
# one carrying exactly one nginx_configmap_rollout leg).
COMBO_BATCH_1 = ("D01", "D05", "D06", "D12", "D22", "T01")
# COMBO-ASM-2 batch: the SPECS-2 B1 pure family (ten combinations of
# net/pod/db/env legs -- NO gateway cfg leg anywhere in the family; D02/D10
# carry the first database combination legs via _combo_db_leg).
COMBO_BATCH_2 = ("D02", "D04", "D07", "D08", "D10", "D11", "D13", "D16", "D17", "D29")
# COMBO-ASM-3 batch: the SPECS-3 CPU-mixed family (ten combinations of
# service-CPU legs with host-CPU / net / pod / env legs -- NO gateway cfg leg
# anywhere in the family; per-leg workers tiers stay the single roots'
# calibrated values; D03 carries the first host-CPU combination leg).
COMBO_BATCH_3 = ("D03", "D09", "D14", "D15", "D19", "D21", "D26", "D28", "D30", "D31")
# COMBO-ASM-4 batch: the SPECS-4 closing family -- eight dual combinations
# (CPU x CPU / CPU x pod) plus four three-leg triples; NO gateway cfg leg
# anywhere in the family; cart/catalog legs stay workers=1, sasrec stays
# workers=8; the batch completes the 38-group COMBO registry (6+10+10+12).
COMBO_BATCH_4 = ("D18", "D20", "D23", "D24", "D25", "D27", "D32", "D33",
                 "T05", "T06", "T07", "T08")

# COMBO-ASM-4: the CROSS-CUTTING §7 triple sub-pair closure as declared data --
# each three-leg combo's three leg pairs must equal the atom sets of exactly

# batch-1, the invariant now covers all five triples).  O-E5 uses these as the
# per-amplifier sub-pair reduction sub-cases; the closure itself is a design
# fact (CATALOG construction), never an observation.
TRIPLE_PAIR_CLOSURE: dict[str, tuple[str, str, str]] = {
    "T01": ("D15", "D12", "D22"),
    "T05": ("D24", "D23", "D25"),
    "T06": ("D27", "D26", "D09"),
    "T07": ("D28", "D30", "D29"),
    "T08": ("D33", "D31", "D32"),
}


def _validate_triple_pair_closure(specs: Mapping[str, ComboSpec] | None = None) -> None:
    """Machine-check the SS7 closure: every triple's leg-pair atom-set set
    equals the atom sets of its three named double-root combos.

    Runs once at import over the live registry (and is re-runnable over a
    caller-supplied mapping so tests can probe a tampered registry copy).
    Fail-loud: any drift between a triple's legs and its named pairs is an
    AssemblyError at import, never a silent mismatch.
    """
    registry_specs = COMBO_SPECS if specs is None else specs
    for triple_id, pair_ids in TRIPLE_PAIR_CLOSURE.items():
        triple = registry_specs[triple_id]
        _require(len(triple.legs) == 3,
                 "triple pair closure: " + triple_id + " must carry exactly three legs")
        derived = {frozenset(leg.fault_type + "@" + leg.entity for leg in leg_pair)
                   for leg_pair in combinations(triple.legs, 2)}
        declared = []
        for pair_id in pair_ids:
            pair_spec = registry_specs[pair_id]
            _require(len(pair_spec.legs) == 2,
                     "triple pair closure: " + pair_id + " must carry exactly two legs")
            declared.append(frozenset(leg.fault_type + "@" + leg.entity for leg in pair_spec.legs))
        _require(len(set(declared)) == 3 and derived == set(declared),
                 "triple pair closure violated: the three leg pairs of " + triple_id
                 + " must equal the atom sets of " + "/".join(pair_ids) + " (CROSS-CUTTING §7)")


_validate_triple_pair_closure()


def _validate_combo_stream_bindings() -> None:
    """collection table-global closure: the binding table covers exactly the r10 legs.

    The per-spec ComboSpec.__post_init__ guards already validate every entry
    naming a constructed spec; this validator closes the global face -- an
    entry for an unknown scenario/fid would never be consumed by any
    __post_init__ scan and must refuse at import, not ride through as a dead
    declaration (the same discipline as _validate_triple_pair_closure).
    """
    expected = set()
    for sid, spec in COMBO_SPECS.items():
        for index, kind in enumerate(spec.injection_signal_kinds):
            if kind in CARRIER_SIGNAL_KINDS:
                expected.add((sid, "F" + str(index + 1)))
    declared = set(COMBO_LEG_STREAM_BINDINGS)
    _require(declared == expected,
             "combo stream binding table must declare exactly every r10 carrier-rule leg "
             "(missing: " + str(sorted(expected - declared)) + "; dead: "
             + str(sorted(declared - expected)) + ")")


_validate_combo_stream_bindings()


def combo_spec_for(scenario_id: str) -> ComboSpec:
    spec = COMBO_SPECS.get(scenario_id)
    _require(spec is not None, "unknown combo scenario spec: " + str(scenario_id))
    return spec


# --------------------------------------------------------------------------- #
# Run binding and builder
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class RunBinding:
    """Concrete per-attempt identity; nothing here is derivable from the spec."""
    run_id: str
    attempt_id: str
    profile_id: str
    block: int
    replicate: int
    repo_root: str
    output_root: str
    kube_context: str
    namespace: str
    clock_id: str
    entrypoint: str                  # loopback/forwarding origin, e.g. http://127.0.0.1:18002
    item_id: str                     # carrier probe item (read-only GET path parameter)
    fingerprints: Mapping[str, str]  # collector/generator/annotation_rules/environment (sha256)
    contract_ref: str
    credential_refs: Mapping[str, str] = field(
        default_factory=lambda: {"database": "env:DB_PASSWORD"})

    def __post_init__(self) -> None:
        for name in ("run_id", "attempt_id", "profile_id", "namespace"):
            _require(isinstance(getattr(self, name), str) and bool(getattr(self, name)), "binding." + name + " required")
        _require(self.owner_id == self.run_id + "/" + self.attempt_id, "binding: owner identity mismatch")
        for name in ("clock_id", "kube_context", "contract_ref"):
            _require(isinstance(getattr(self, name), str) and bool(getattr(self, name)), "binding." + name + " required")
        for name in ("repo_root", "output_root"):
            path = Path(getattr(self, name))
            _require(isinstance(getattr(self, name), str) and path.is_absolute() and ".." not in path.parts,
                     "binding." + name + ": explicit absolute path required")
        _require(type(self.block) is int and self.block >= 1 and type(self.replicate) is int and self.replicate >= 1,
                 "binding: positive block/replicate required")
        _require(isinstance(self.entrypoint, str) and self.entrypoint.startswith("http://"), "binding.entrypoint required")
        _require(isinstance(self.item_id, str) and bool(self.item_id), "binding.item_id required")
        required = {"collector", "generator", "annotation_rules", "environment"}
        _require(set(self.fingerprints) == required, "binding.fingerprints: exact four sources required")
        for key, value in self.fingerprints.items():
            _sha256_hex(value, "binding.fingerprints." + key)

    @property
    def owner_id(self) -> str:
        return self.run_id + "/" + self.attempt_id


@dataclass(frozen=True)
class AssembledRun:
    spec: AtomSpec
    binding: RunBinding
    contract: c.RunContract
    rules: q.QualityRules
    contract_dict: dict[str, Any]

    @property
    def contract_sha256(self) -> str:
        return self.contract.sha256


def _contract_dict(spec: AtomSpec, binding: RunBinding, registry: c.DesignRegistry) -> dict[str, Any]:
    """S08-driver-shaped contract dict; the quality fingerprint is added later."""
    (pre_start, pre_end), (during_start, during_end), (post_start, post_end) = spec.phases
    evidence_root = Path(binding.output_root) / spec.purpose / binding.run_id / binding.attempt_id
    return {
        "schema_version": c.SCHEMA_VERSION,
        "scenario": {"design_version": registry.design_version, "scenario_id": spec.scenario_id},
        "catalog_sha256": registry.catalog_sha256,
        "purpose": spec.purpose,
        "protocol_status": "pilot_fixed",
        "qualification": "not_assessed",
        "scope": spec.scope,
        "context": {
            "run_id": binding.run_id, "attempt_id": binding.attempt_id, "profile_id": binding.profile_id,
            "block": binding.block, "replicate": binding.replicate,
            "repo_root": binding.repo_root, "output_root": binding.output_root, "evidence_root": str(evidence_root),
            "contract_ref": binding.contract_ref, "owner_id": binding.owner_id, "clock_id": binding.clock_id,
            "kube_context": binding.kube_context, "namespace": binding.namespace,
            "fingerprints": {**dict(binding.fingerprints)},
            "credential_refs": {**dict(binding.credential_refs)},
        },
        "request_profile": {
            "profile_id": binding.profile_id, "generator_version": GENERATOR_VERSION, "random_seed": spec.random_seed,
            "arrival_model": "fixed_rate_open_loop", "max_concurrency": spec.carrier.max_concurrency,
            "overload_policy": "drop", "stop_policy": "drain",
            "streams": [{
                "stream_id": spec.carrier.stream_id, "entrypoint": binding.entrypoint,
                "endpoint": spec.carrier.endpoint(binding.namespace, binding.item_id),
                "method": spec.carrier.http_method, "parameters": spec.carrier.http_parameters,
                "carrier": spec.carrier.carrier, "direct_api": spec.carrier.direct_api,
                "rate_rps": spec.carrier.rate_rps, "max_concurrency": spec.carrier.max_concurrency,
                "timeout_s": spec.carrier.timeout_s, "client_retry_limit": spec.carrier.client_retry_limit}],
        },
        "phases": {
            "pre_fault": {"start": pre_start, "end": pre_end, "time_basis": "run_offset", "clock_id": binding.clock_id},
            "during_fault": {"start": during_start, "end": during_end, "time_basis": "run_offset", "clock_id": binding.clock_id},
            "post_recovery": {"start": post_start, "end": post_end, "time_basis": "run_offset", "clock_id": binding.clock_id},
        },
        "faults": [{
            "fault_instance_id": "F1", "atom_id": spec.atom_id, "normalized_root_entity": spec.leg.entity,
            "fault_class": spec.leg.fault_class, "fault_type": spec.leg.fault_type,
            "mechanism": spec.leg.mechanism, "mechanism_version": spec.leg.mechanism_version,
            "normalization_version": registry.design_version,
            "raw_target": spec.leg.raw_target(binding.namespace),
            "parameters": {**dict(spec.leg.parameters)},
            "planned_window": {"start": spec.leg.window[0], "end": spec.leg.window[1],
                               "time_basis": "run_offset", "clock_id": binding.clock_id}}],
        "metric_interval_s": METRIC_INTERVAL_S,
        "rules": {
            "timing": spec.rule_version_base + "-timing-v1", "injection": spec.rule_version_base + "-q1-v1",
            "recovery": spec.rule_version_base + "-q2-v1", "completeness": spec.rule_version_base + "-q3-v1",
            "annotation": spec.rule_version_base + "-timing-v1"},
        "release_gate_ref": None,
        "catalog_source": {"kind": registry.source_kind, "sha256": registry.source_sha256},
    }


def _quality_rule_data(spec: AtomSpec, contract: dict[str, Any]) -> dict[str, Any]:
    """S08-driver-shaped QualityRules payload derived only from spec + contract."""
    recovery_rule = spec.signal.recovery_rule("F1")
    if spec.injection_signal_kind == "metric_threshold":
        injection_signal = spec.signal.signal("F1")
    elif spec.injection_signal.get("schema_version") == obs.CARRIER_RULE_SCHEMA:
        
        # merge is forbidden here (validate_carrier_rule rejects extra fields).
        injection_signal = dict(spec.injection_signal)
    else:
        injection_signal = {**dict(spec.injection_signal or {}), "query_id": "F1"}
    injection = [{
        "instance_id": "F1", "mechanism": spec.leg.mechanism, "mechanism_version": spec.leg.mechanism_version,
        "operation_source_id": spec.telemetry.operations, "signal_kind": spec.injection_signal_kind,
        "signal": injection_signal}]
    return {
        "schema_version": q.RULE_SCHEMA, "version": spec.rule_version_base + "-rules-formal-frozen-r51", "status": "formal_frozen",
        "rule_refs": {key: contract["rules"][key] for key in ("injection", "recovery", "completeness", "annotation")},
        "injection": injection,
        "recovery": {"metric_rules": [recovery_rule],
                     "component_sources": {spec.leg.entity: spec.telemetry.components},
                     "checksum_source": spec.telemetry.checksum, "lock_source": spec.telemetry.locks},
        "data": {"duration_tolerance_s": 0.5, "max_missing_fraction": 0.15, "max_nonfinite_fraction": 0.2,
                 "max_source_interval_s": 2,
                 "required_metric_queries": [{k: recovery_rule[k] for k in ("query_id", "source_id", "entity", "unit")}],
                 "required_sources": {"metrics": [spec.telemetry.metrics], "traces": [spec.telemetry.traces],
                                      "logs": [spec.telemetry.logs]},
                 "require_human_review": True,
                 
                 # log transport actually used is the bounded archive, so the
                 # evidence build flips logs to bounded_log_archive_v1 and the
                 # verifier consumes these declared limits (declared-before-
                 # capture; never inferred from this run's observed gaps).
                 "coverage_policy": {"method": "bounded_query_window_v1",
                                     "min_ingestion_wait_s": {"metrics": 5, "traces": 10, "logs": 2},
                                     "jaeger_min_lookback_s": 0, "max_clock_uncertainty_s": 0.1,
                                     "archive_policy": {"method": "bounded_log_archive_v1",
                                                        "rule_ref": spec.rule_version_base + "-log-archive-v1",
                                                        "max_capture_start_gap_s": 2.0, "max_attachment_gap_s": 30.0}}},
    }


def assemble(spec: AtomSpec, binding: RunBinding, registry: c.DesignRegistry) -> AssembledRun:
    """Deterministic spec+binding -> validated RunContract + QualityRules.

    The composition is re-checked against the versioned catalog via
    RunContract.from_dict (scenario resolve + atom set equality), and the rule
    fingerprint is sealed into the contract exactly like the S08 driver.
    """
    _require(isinstance(spec, AtomSpec) and isinstance(binding, RunBinding), "validated spec/binding required")
    contract = _contract_dict(spec, binding, registry)
    rules = q.QualityRules.from_dict(_quality_rule_data(spec, contract))
    contract["context"]["fingerprints"]["quality_rules"] = rules.sha256
    run_contract = c.RunContract.from_dict(contract, registry)
    return AssembledRun(spec=spec, binding=binding, contract=run_contract, rules=rules, contract_dict=contract)


# --------------------------------------------------------------------------- #
# COMBO-ASM-1 builder: the combination surface parallels assemble() above; the

# (RunContract.from_dict validates multi-fault compositions against the combo
# scenario: sorted atom set equality + one instance per entity for m1_main);
# request_profile carries the main+bypass(+third) streams; QualityRules carry
# one injection rule per fault instance and recovery metric rules covering


# exemption in evaluate_quality).
# --------------------------------------------------------------------------- #

def _combo_contract_dict(spec: ComboSpec, binding: RunBinding, registry: c.DesignRegistry) -> dict[str, Any]:
    (pre_start, pre_end), (during_start, during_end), (post_start, post_end) = spec.phases
    evidence_root = Path(binding.output_root) / spec.purpose / binding.run_id / binding.attempt_id
    streams = [{
        "stream_id": carrier.stream_id, "entrypoint": binding.entrypoint,
        "endpoint": carrier.endpoint(binding.namespace, binding.item_id),
        "method": carrier.http_method, "parameters": carrier.http_parameters,
        "carrier": carrier.carrier, "direct_api": carrier.direct_api,
        "rate_rps": carrier.rate_rps, "max_concurrency": carrier.max_concurrency,
        "timeout_s": carrier.timeout_s, "client_retry_limit": carrier.client_retry_limit,
    } for carrier in spec.carriers]
    faults = [{
        "fault_instance_id": "F" + str(index + 1),
        "atom_id": leg.fault_type + "@" + leg.entity,
        "normalized_root_entity": leg.entity, "fault_class": leg.fault_class,
        "fault_type": leg.fault_type, "mechanism": leg.mechanism,
        "mechanism_version": leg.mechanism_version,
        "normalization_version": registry.design_version,
        "raw_target": leg.raw_target(binding.namespace),
        "parameters": {**dict(leg.parameters)},
        "planned_window": {"start": leg.window[0], "end": leg.window[1],
                           "time_basis": "run_offset", "clock_id": binding.clock_id},
    } for index, leg in enumerate(spec.legs)]
    return {
        "schema_version": c.SCHEMA_VERSION,
        "scenario": {"design_version": registry.design_version, "scenario_id": spec.scenario_id},
        "catalog_sha256": registry.catalog_sha256,
        "purpose": spec.purpose,
        "protocol_status": "pilot_fixed",
        "qualification": "not_assessed",
        "scope": spec.scope,
        "context": {
            "run_id": binding.run_id, "attempt_id": binding.attempt_id, "profile_id": binding.profile_id,
            "block": binding.block, "replicate": binding.replicate,
            "repo_root": binding.repo_root, "output_root": binding.output_root, "evidence_root": str(evidence_root),
            "contract_ref": binding.contract_ref, "owner_id": binding.owner_id, "clock_id": binding.clock_id,
            "kube_context": binding.kube_context, "namespace": binding.namespace,
            "fingerprints": {**dict(binding.fingerprints)},
            "credential_refs": {**dict(binding.credential_refs)},
        },
        "request_profile": {
            "profile_id": binding.profile_id, "generator_version": GENERATOR_VERSION, "random_seed": spec.random_seed,
            "arrival_model": "fixed_rate_open_loop",
            "max_concurrency": sum(carrier.max_concurrency for carrier in spec.carriers),
            "overload_policy": "drop", "stop_policy": "drain",
            "streams": streams,
        },
        "phases": {
            "pre_fault": {"start": pre_start, "end": pre_end, "time_basis": "run_offset", "clock_id": binding.clock_id},
            "during_fault": {"start": during_start, "end": during_end, "time_basis": "run_offset", "clock_id": binding.clock_id},
            "post_recovery": {"start": post_start, "end": post_end, "time_basis": "run_offset", "clock_id": binding.clock_id},
        },
        "faults": faults,
        "metric_interval_s": METRIC_INTERVAL_S,
        "rules": {
            "timing": spec.rule_version_base + "-timing-v1", "injection": spec.rule_version_base + "-q1-v1",
            "recovery": spec.rule_version_base + "-q2-v1", "completeness": spec.rule_version_base + "-q3-v1",
            "annotation": spec.rule_version_base + "-timing-v1"},
        "release_gate_ref": None,
        "catalog_source": {"kind": registry.source_kind, "sha256": registry.source_sha256},
    }


def _combo_quality_rule_data(spec: ComboSpec, contract: dict[str, Any]) -> dict[str, Any]:
    injection = []
    for index, leg in enumerate(spec.legs):
        fid = "F" + str(index + 1)
        if spec.injection_signal_kinds[index] == "metric_threshold":
            signal = spec.signals[index].signal(fid)
        elif spec.injection_signals[index].get("schema_version") == obs.CARRIER_RULE_SCHEMA:
            
            # binding rides the injection rule envelope's instance_id, not the
            # carrier statistic itself).
            signal = dict(spec.injection_signals[index])
        else:
            signal = {**dict(spec.injection_signals[index] or {}), "query_id": fid}
        envelope = {"instance_id": fid, "mechanism": leg.mechanism,
                    "mechanism_version": leg.mechanism_version,
                    "operation_source_id": spec.telemetry.operations,
                    "signal_kind": spec.injection_signal_kinds[index],
                    "signal": signal}
        binding = COMBO_LEG_STREAM_BINDINGS.get((spec.scenario_id, fid))
        if spec.injection_signal_kinds[index] in CARRIER_SIGNAL_KINDS:
            
            # validity-evidence mode explicitly (carrier_signal vs the
            
            # stay six-field -- their own carrier stream is unmasked by
            # construction.  The per-spec guard already refused a missing
            # entry; the .get default only keeps this builder total.
            envelope["evidence_mode"] = binding[1] if binding is not None else "carrier_signal"
        injection.append(envelope)
    recovery_rules = [signal.recovery_rule("F" + str(index + 1)) for index, signal in enumerate(spec.signals)]
    
    
    # host/mysql_items_lock -- stressor lifecycle is a declared runtime premise).
    component_sources = {leg.entity: spec.telemetry.components for leg in spec.legs
                         if leg.entity not in ("host", "mysql_items_lock")}
    return {
        "schema_version": q.RULE_SCHEMA, "version": spec.rule_version_base + "-rules-formal-frozen-r51", "status": "formal_frozen",
        "rule_refs": {key: contract["rules"][key] for key in ("injection", "recovery", "completeness", "annotation")},
        "injection": injection,
        "recovery": {"metric_rules": recovery_rules,
                     "component_sources": component_sources,
                     "checksum_source": spec.telemetry.checksum, "lock_source": spec.telemetry.locks},
        "data": {"duration_tolerance_s": 0.5, "max_missing_fraction": 0.15, "max_nonfinite_fraction": 0.2,
                 "max_source_interval_s": 2,
                 "required_metric_queries": [{k: rule[k] for k in ("query_id", "source_id", "entity", "unit")}
                                             for rule in recovery_rules],
                 "required_sources": {"metrics": [spec.telemetry.metrics], "traces": [spec.telemetry.traces],
                                      "logs": [spec.telemetry.logs]},
                 "require_human_review": True,
                 
                 
                 # the only path a replacement gap can soften reconciliation.
                 "coverage_policy": {"method": "bounded_query_window_v1",
                                     "min_ingestion_wait_s": {"metrics": 5, "traces": 10, "logs": 2},
                                     "jaeger_min_lookback_s": 0, "max_clock_uncertainty_s": 0.1,
                                     "archive_policy": {"method": "bounded_log_archive_v1",
                                                        "rule_ref": spec.rule_version_base + "-log-archive-v1",
                                                        "max_capture_start_gap_s": 2.0, "max_attachment_gap_s": 30.0}}},
    }


def assemble_combo(spec: ComboSpec, binding: RunBinding, registry: c.DesignRegistry) -> AssembledRun:
    """Deterministic ComboSpec+binding -> validated multi-fault run, offline.

    Same sealing discipline as assemble(): the contract is re-validated against
    the versioned catalog (the combo scenario's sorted atom set must equal the
    legs' atom ids) and the quality-rule fingerprint is sealed into the
    contract.  Validation-only by construction; nothing here provisions,
    executes or observes -- combination assembly is NOT combination collection
    qualification (collection smoke duty, see the spec preconditions).
    """
    _require(isinstance(spec, ComboSpec) and isinstance(binding, RunBinding),
             "validated combo spec/binding required")
    contract = _combo_contract_dict(spec, binding, registry)
    rules = q.QualityRules.from_dict(_combo_quality_rule_data(spec, contract))
    contract["context"]["fingerprints"]["quality_rules"] = rules.sha256
    run_contract = c.RunContract.from_dict(contract, registry)
    return AssembledRun(spec=spec, binding=binding, contract=run_contract, rules=rules, contract_dict=contract)
