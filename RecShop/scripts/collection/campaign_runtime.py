"""Shared completion, recovery and read-only baseline helpers for repeat collection."""
from __future__ import annotations
import json
from pathlib import Path
from . import scenario_runner as cd, live_runtime as live, pricing_route as pricing
from . import pricing_route_model as route_model, runner, telemetry, environment
ROOT = Path(__file__).resolve().parents[2]

class StopBatch(RuntimeError):
    pass

def need(ok: bool, reason: str) -> None:
    if not ok:
        raise StopBatch(reason)

def read_json(path: Path):
    return json.loads(path.read_bytes().decode("utf-8"))

def read_lease():
    identity = runner.EnvironmentIdentity(cd.CLUSTER_UID, cd.NAMESPACE_UID)
    lease_root = cd.LEASE_ROOT
    registry = runner.environment_registry_root()
    binding_path = registry / (identity.key + ".binding.json")
    state_path = lease_root / (identity.key + ".state.json")
    need(binding_path.is_file() and state_path.is_file(), "registered shared lease is missing")
    binding = read_json(binding_path)
    state = read_json(state_path)
    expected_binding = {"schema_version": runner.RUNNER_SCHEMA,
                        "cluster_uid": cd.CLUSTER_UID, "namespace_uid": cd.NAMESPACE_UID,
                        "lease_root": str(lease_root)}
    need(binding == expected_binding, "shared lease binding changed")
    need(state.get("schema_version") == runner.RUNNER_SCHEMA
         and state.get("environment_key") == identity.key,
         "shared lease identity/schema changed")
    return {"status": state.get("status"), "workers": state.get("workers"),
            "mode": state.get("mode"), "attempt": state.get("attempt")}

def require_clean_lease():
    state = read_lease()
    need(state["status"] == "clean" and state["workers"] == "drained"
         and state["mode"] == "controlled_pilot",
         "shared lease is not clean/drained controlled_pilot")
    cd.require_prior_outer_drained()
    return state

def route_baseline():
    environment.assert_live()
    adapter = pricing.PricingRouteAdapter(
        cd.CONTEXT, cd.NAMESPACE, cd.CLUSTER_UID, cd.NAMESPACE_UID,
        process=live.BoundedProcess(execute=True), evidence_kind="observed", kubectl=environment.resolve_cli("kubectl"),
    )
    projection = adapter.read_baseline()
    readback = adapter.last_readback or {}
    runtime = readback.get("runtime_env") or {}
    need(projection.get("owner") is None and not projection.get("conflicts")
         and projection["fields"]["route_env"].get("entry", {}).get("value") == route_model.DIRECT_URL
         and projection["fields"].get("replicas") == 1,
         "pricing route is not clean/direct or still has an owner")
    need(readback.get("decision", {}).get("settled") is True
         and runtime.get(route_model.ROUTE_ENV) == route_model.DIRECT_URL
         and runtime.get(route_model.NACOS_ENV) == "false"
         and runtime.get("pod_uid_before") == runtime.get("pod_uid_after"),
         "pricing route runtime readback is not stable/direct")
    return {"state": "DIRECT", "owner": None, "replicas": 1}

def prom_baseline():
    environment.assert_live()
    policy = live.PromTargetReadPolicy(
        source_id="collection-prom-targets", origin=cd.PROM,
        allowed_paths=("/api/v1/targets",), timeout_s=10,
        max_response_header_bytes=4096, max_response_body_bytes=2_000_000,
    )
    reply = telemetry.HttpJsonClient(policy, execute=True).fetch_json(
        "/api/v1/targets", {"state": "active"})
    need(reply.status == "ok", "Prometheus target read failed")
    payload = json.loads(reply.raw)
    rows = [row for row in payload.get("data", {}).get("activeTargets", [])
            if row.get("scrapeUrl") == cd.SAMPLING_PROM_SCRAPE_URL]
    need(payload.get("status") == "success" and len(rows) == 1
         and rows[0].get("health") == "up" and not rows[0].get("lastError"),
         'required collection OTel Prometheus target is not uniquely UP')
    return {"target": cd.SAMPLING_PROM_SCRAPE_URL, "health": "up"}

def read_completion(item, runner_rc):
    root = item["report_root"]
    summary_path = root / "SUMMARY.json"
    input_path = root / "recovery-input.json"
    need(summary_path.is_file() and input_path.is_file(),
         f'{item["condition_id"]}: SUMMARY/recovery-input missing; preserve attempt and stop')
    summary = read_json(summary_path)
    saved = read_json(input_path)
    contract_data = saved.get("contract") or {}
    context = contract_data.get("context") or {}
    evidence_root = Path(context.get("evidence_root", ""))
    need(evidence_root.resolve() == item["evidence_root"].resolve(),
         f'{item["condition_id"]}: attempt evidence root mismatch')
    quality_path = evidence_root / "artifacts/quality/result.json"
    evidence_path = evidence_root / "artifacts/quality/evidence.json"
    need(quality_path.is_file() and evidence_path.is_file(),
         f'{item["condition_id"]}: raw quality report/evidence missing')
    quality, evidence = read_json(quality_path), read_json(evidence_path)
    need(quality.get("sample_purpose") == "formal"
         and quality.get("run_id") == context.get("run_id")
         and quality.get("attempt_id") == item["attempt"]
         and quality.get("contract_sha256") == saved.get("actual_contract_sha256"),
         f'{item["condition_id"]}: raw quality identity/purpose mismatch')
    identity = {"contract_sha256": saved.get("actual_contract_sha256"),
                "run_id": context.get("run_id"), "attempt_id": item["attempt"]}
    for source in (evidence, evidence.get("checksums") or {}):
        need(type(source) is dict and source.get("evidence_kind") == "observed"
             and all(source.get(key) == value for key, value in identity.items()),
             f'{item["condition_id"]}: raw DB evidence identity/provenance mismatch')
    p0 = quality.get("p0_assessment") or {}
    p0s = {name: (p0.get("p0s") or {}).get(name, {}).get("status")
           for name in ("experiment_semantics", "process_recoverable", "data_real")}
    q_states = {name: (quality.get("groups") or {}).get(name, {}).get("status")
                for name in ("PROTOCOL", "Q1", "Q2", "Q3")}
    nonpass = [
        {"group": group, "check": row.get("check"), "status": row.get("status"),
         "reason": row.get("reason")}
        for group, body in (quality.get("groups") or {}).items()
        for row in body.get("checks", []) if row.get("status") != "PASS"
    ]
    try:
        lease = read_lease()
    except StopBatch as exc:
        lease = {"status": "UNKNOWN", "workers": "UNKNOWN", "mode": "UNKNOWN",
                 "read_error": str(exc)}
    recovery_status = (summary.get("recovery") or {}).get("status")
    result = {
        "runner_return_code": runner_rc,
        "summary_status": summary.get("status"),
        "formal": quality.get("sample_purpose"),
        "condition_id": item["condition_id"], "attempt": item["attempt"],
        "p0_status": p0.get("status"), "p0s": p0s,
        "old_q_states": q_states, "nonpass_checks": nonpass,
        "next_case_allowed": summary.get("next_case_allowed"),
        "recovery_status": recovery_status,
        "lease": lease,
    }
    return summary, quality, evidence, result

def verify_database_evidence(quality, evidence):
    checks = {
        row.get("check"): row for body in quality["groups"].values()
        for row in body.get("checks", [])
    }
    for name in ("items.checksum", "inventory.checksum", "owned_database_locks"):
        need(name in checks and checks[name].get("status") == "PASS",
             "DB lock/checksum evidence is not PASS: " + name)
    checksum = evidence.get("checksums") or {}
    need(checksum.get("return_code") == 0 and checksum.get("server_uuid") == cd.DB_UUID
         and all(checksum.get("tables", {}).get(table, {}).get("pre") == value
                 and checksum.get("tables", {}).get(table, {}).get("post") == value
                 for table, value in cd.CHECKSUMS.items()),
         "post-recovery DB lock/checksum readback is missing or changed")

def database_baseline(item):
    try:
        values = environment.credentials()
        keys = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")
        need(all(values.get(key) for key in keys), "DB environment is incomplete for pre-run baseline")
        source_id = item["condition"]["quality_rules_payload"]["recovery"]["checksum_source"]
        probe = live.ReadOnlyDbProbe(execute=True, environ={key: values[key] for key in keys},
                                            total_timeout_s=10, socket_timeout_s=5,
                                            source_id=source_id)
        result = probe.capture(item["contract"], "pre_fault", total_timeout_s=10)
    except StopBatch:
        raise
    except Exception as exc:
        raise StopBatch('pre-run DB baseline failed: ' + type(exc).__name__) from None
    need(result.get("evidence_kind") == "observed" and result.get("return_code") == 0
                and result.get("workers_joined") is True and result.get("server_uuid") == cd.DB_UUID
                and result.get("checksums") == cd.CHECKSUMS,
                f'{item["condition_id"]}: pre-run DB UUID/checksum/lock baseline differs')
    return {key: result[key] for key in ("phase", "source_id", "evidence_kind", "return_code",
            "workers_joined", "contract_sha256", "run_id", "attempt_id", "started_at_epoch_s",
            "timestamp_epoch_s", "server_uuid", "checksums")} | {
                "metadata_lock_count": len(result.get("target_metadata_locks", [])),
                "data_lock_count": len(result.get("target_data_locks", [])),
                "operations": result.get("operations", [])}
