import json
import math
import statistics
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT_JSON = ROOT / "reports" / "trainticket_root_signal_audit.json"
OUT_MD = ROOT / "reports" / "trainticket_root_signal_audit.md"


def as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def median(values):
    return statistics.median(values) if values else None


def safe_ratio(pre, during):
    if pre in (None, 0):
        return math.inf if during else 1
    return during / pre


def service_traffic(metadata, service):
    out = {}
    for stage in ["pre_fault", "during_fault", "post_recovery"]:
        rows = ((metadata.get("traffic_stats") or {}).get(stage) or {}).get("by_endpoint") or []
        for row in rows:
            if row.get("service") == service:
                out[stage] = row
    return out


def endpoint_evidence(metadata, service, evidence_type="endpoint_sli"):
    traffic = service_traffic(metadata, service)
    pre = traffic.get("pre_fault")
    during = traffic.get("during_fault")
    post = traffic.get("post_recovery")
    if not pre or not during:
        return None

    pre_p95 = pre.get("p95_ms")
    during_p95 = during.get("p95_ms")
    post_p95 = (post or {}).get("p95_ms")
    ok_drop = pre.get("ok_ratio", 1) - during.get("ok_ratio", 1)
    p95_shift = (during_p95 - pre_p95) if pre_p95 is not None and during_p95 is not None else 0
    p95_ratio = safe_ratio(pre_p95, during_p95)
    passed = (
        p95_shift >= 200
        or p95_ratio >= 1.5
        or ok_drop >= 0.1
        or during.get("ok_ratio", 1) < 0.9
    )

    return {
        "type": evidence_type,
        "service": service,
        "passed": passed,
        "pre_p95_ms": pre_p95,
        "during_p95_ms": during_p95,
        "post_p95_ms": post_p95,
        "p95_shift_ms": round(p95_shift, 3),
        "p95_ratio": round(p95_ratio, 3) if math.isfinite(p95_ratio) else "inf",
        "ok_drop": round(ok_drop, 4),
    }


def inferred_bound_service(component):
    if component and component.endswith("-mongo"):
        return component[:-6] + "-service"
    return None


def metric_evidence_for_targets(case_dir, targets, fault_types_by_target=None):
    target_set = set(targets)
    values = {target: defaultdict(lambda: defaultdict(list)) for target in target_set}
    entities = {target: defaultdict(set) for target in target_set}
    metrics_path = case_dir / "raw" / "metrics" / "metrics_v2.jsonl"
    if not metrics_path.exists():
        return {target: [] for target in target_set}

    with metrics_path.open(encoding="utf-8-sig") as handle:
        for line in handle:
            record = json.loads(line)
            stage = record.get("stage")
            if stage not in {"pre_fault", "during_fault", "post_recovery"}:
                continue

            metric = record.get("metric")
            if metric in {"http_status_code", "request_success", "request_duration_ms"}:
                continue

            labels = json.dumps(record.get("labels", {}), ensure_ascii=False)
            text = " ".join(str(record.get(key, "")) for key in ["service", "entity", "container"])
            text = f"{text} {labels}"
            for target in target_set:
                if target in text:
                    values[target][metric][stage].append(float(record.get("value") or 0))
                    entity = record.get("entity")
                    if entity:
                        entities[target][stage].add(str(entity))

    output = {}
    for target, metric_map in values.items():
        evidence = []
        target_fault_tokens = set(fault_types_by_target.get(target, [])) if fault_types_by_target else set()
        restart_like = any(
            token and ("restart" in token or "pod" in token or "kill" in token)
            for token in target_fault_tokens
        )
        if restart_like:
            pre_entities = sorted(entities[target].get("pre_fault", set()))
            during_entities = sorted(entities[target].get("during_fault", set()))
            post_entities = sorted(entities[target].get("post_recovery", set()))
            replacement_entities = during_entities or post_entities
            if pre_entities and replacement_entities and set(pre_entities) != set(replacement_entities):
                evidence.append(
                    {
                        "type": "target_lifecycle_metric",
                        "metric": "pod_entity_changed",
                        "passed": True,
                        "pre_entities": pre_entities,
                        "during_entities": during_entities,
                        "post_entities": post_entities,
                    }
                )
        for metric, stages in metric_map.items():
            if "pre_fault" not in stages or "during_fault" not in stages:
                continue
            pre = median(stages["pre_fault"])
            during = median(stages["during_fault"])
            post = median(stages.get("post_recovery", []))
            ratio = safe_ratio(pre, during)
            shift = (during - pre) if pre is not None and during is not None else 0

            passed = False
            if metric == "container_cpu_usage_cores":
                passed = (ratio >= 1.5 and shift >= 0.002) or shift >= 0.02
            elif metric in {
                "container_memory_usage_bytes",
                "container_memory_working_set_bytes",
                "container_memory_rss_bytes",
            }:
                passed = (ratio >= 1.15 and shift >= 20 * 1024 * 1024) or shift >= 80 * 1024 * 1024
            elif metric in {
                "container_ready",
                "container_running",
                "pod_ready",
                "deployment_replicas_ready",
            }:
                passed = during is not None and pre is not None and during < pre
            elif metric == "container_restart_count":
                passed = during is not None and pre is not None and during > pre
            elif metric == "container_start_time_seconds":
                passed = restart_like and during is not None and pre is not None and during > pre
            elif "network" in metric and ("errors" in metric or "dropped" in metric):
                passed = during is not None and pre is not None and during > pre and during > 0

            if passed:
                evidence.append(
                    {
                        "type": "target_metric_shift",
                        "metric": metric,
                        "passed": True,
                        "pre_median": pre,
                        "during_median": during,
                        "post_median": post,
                        "ratio": round(ratio, 3) if math.isfinite(ratio) else "inf",
                        "shift": round(shift, 6) if abs(shift) < 1_000_000 else shift,
                    }
                )
        output[target] = evidence[:5]
    return output


def audit_case(case_dir):
    metadata = json.loads((case_dir / "metadata.json").read_text(encoding="utf-8-sig"))
    faults = as_list(metadata.get("faults"))

    preliminary = []
    metric_targets = []
    fault_types_by_target = defaultdict(set)
    for fault in faults:
        target = fault.get("target_component")
        evidence = []
        if target:
            fault_types_by_target[target].update(
                str(value)
                for value in [
                    fault.get("fault_type"),
                    fault.get("injection_fault"),
                ]
                if value
            )

        target_endpoint = endpoint_evidence(metadata, target)
        if target_endpoint:
            evidence.append(target_endpoint)

        bound_service = inferred_bound_service(target)
        if bound_service:
            bound_evidence = endpoint_evidence(metadata, bound_service, "bound_service_sli")
            if bound_evidence:
                bound_evidence["root_component"] = target
                evidence.append(bound_evidence)

        if not any(item.get("passed") for item in evidence):
            metric_targets.append(target)

        injection_ok = bool(
            fault.get("injected_at")
            and fault.get("recovered_at")
            and fault.get("status") == "recovered"
        )
        preliminary.append((fault, injection_ok, evidence))

    metric_cache = (
        metric_evidence_for_targets(case_dir, metric_targets, fault_types_by_target)
        if metric_targets
        else {}
    )
    fault_results = []
    for fault, injection_ok, evidence in preliminary:
        target = fault.get("target_component")
        full_evidence = list(evidence) + metric_cache.get(target, [])
        telemetry_ok = any(item.get("passed") for item in full_evidence)
        status = "passed" if injection_ok and telemetry_ok else "needs_review"
        fault_results.append(
            {
                "fault_instance_id": fault.get("fault_instance_id"),
                "target_component": target,
                "fault_type": fault.get("fault_type"),
                "injection_fault": fault.get("injection_fault"),
                "role": fault.get("role"),
                "injection_ok": injection_ok,
                "telemetry_ok": telemetry_ok,
                "status": status,
                "evidence": full_evidence,
            }
        )

    case_status = "passed" if all(item["status"] == "passed" for item in fault_results) else "needs_review"
    try:
        relative_path = str(case_dir.relative_to(ROOT)).replace("\\", "/")
    except ValueError:
        relative_path = str(case_dir.resolve()).replace("\\", "/")

    return {
        "relative_path": relative_path,
        "slot_id": metadata.get("formal_slot_id"),
        "sample_id": metadata.get("sample_id"),
        "root_count": metadata.get("root_count"),
        "root_causes": as_list(metadata.get("root_causes")),
        "case_status": case_status,
        "fault_results": fault_results,
    }


def write_markdown(report, generated_at, out_md):
    fault_status = Counter()
    case_status = Counter()
    needs_review = report["needs_review"]
    for case in report["cases"]:
        case_status[case["case_status"]] += 1
        for fault in case["fault_results"]:
            fault_status[fault["status"]] += 1

    md_lines = [
        "# TrainTicket Root Signal Audit",
        "",
        f"Generated at: `{generated_at}`",
        "",
        "This is a post-hoc `each_root_signal_present` audit for TrainTicket cases.",
        "",
        "## Gate Definition",
        "",
        "A fault instance passes when both conditions hold:",
        "",
        "```text",
        "1. injection evidence: injected_at + recovered_at exist and status == recovered",
        "2. telemetry evidence: at least one endpoint, bound-service, or target-metric signal passes threshold",
        "```",
        "",
        "## Summary",
        "",
        "| Item | Count |",
        "|---|---:|",
        f"| TrainTicket fault cases audited | {report['summary']['case_count']} |",
        f"| Fault instances audited | {report['summary']['fault_instance_count']} |",
        f"| Passed fault instances | {fault_status.get('passed', 0)} |",
        f"| Needs-review fault instances | {fault_status.get('needs_review', 0)} |",
        f"| Fully passed cases | {case_status.get('passed', 0)} |",
        f"| Needs-review cases | {case_status.get('needs_review', 0)} |",
        "",
        "## Needs Review",
        "",
    ]

    if needs_review:
        md_lines.extend(
            [
                "| Slot | Fault | Target | Fault type | Injection OK | Telemetry OK | Evidence summary |",
                "|---|---|---|---|---:|---:|---|",
            ]
        )
        for case in needs_review:
            for fault in case["fault_results"]:
                summaries = []
                for evidence in fault["evidence"]:
                    if evidence.get("type") in {"endpoint_sli", "bound_service_sli"}:
                        summaries.append(
                            f"{evidence.get('type')}:{evidence.get('service')} shift={evidence.get('p95_shift_ms')}ms ratio={evidence.get('p95_ratio')} passed={evidence.get('passed')}"
                        )
                    elif evidence.get("type") == "target_metric_shift":
                        summaries.append(
                            f"{evidence.get('metric')} ratio={evidence.get('ratio')} passed={evidence.get('passed')}"
                        )
                summary = "<br>".join(summaries) if summaries else "no telemetry evidence found"
                md_lines.append(
                    f"| {case['slot_id']} | {fault['fault_instance_id']} | {fault['target_component']} | {fault['fault_type']} | {fault['injection_ok']} | {fault['telemetry_ok']} | {summary} |"
                )
    else:
        md_lines.append("No needs-review cases.")

    md_lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "`needs_review` does not mean the injection command failed. It means the post-hoc audit did not find a strong per-root telemetry signal under the current thresholds. These cases should be manually checked before claiming TrainTicket has Recshop-level per-root signal validation.",
            "",
            f"Machine-readable report: `{out_md.with_suffix('.json').name}`",
            "",
        ]
    )
    out_md.write_text("\n".join(md_lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--case-dir",
        action="append",
        required=True,
        default=[],
        help="Audit one case directory. Can be passed multiple times. Required; no implicit dataset scan.",
    )
    parser.add_argument("--out-json", default=str(OUT_JSON))
    parser.add_argument("--out-md", default=str(OUT_MD))
    args = parser.parse_args()

    generated_at = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
    if args.case_dir:
        case_dirs = [Path(item).resolve() for item in args.case_dir]
    else:
        case_dirs = sorted(path.parent for path in (ROOT / "MR1").rglob("metadata.json"))
    case_results = [audit_case(case_dir) for case_dir in case_dirs]

    fault_status = Counter()
    case_status = Counter()
    needs_review = []
    for case in case_results:
        case_status[case["case_status"]] += 1
        if case["case_status"] != "passed":
            needs_review.append(case)
        for fault in case["fault_results"]:
            fault_status[fault["status"]] += 1

    report = {
        "schema_version": "trainticket-root-signal-audit.v1",
        "dataset_id": "k8s-v3.1-main",
        "system": "trainticket",
        "generated_at": generated_at,
        "gate_definition": {
            "name": "each_root_signal_present",
            "pass_condition": "Each fault instance must have recovered injection timestamps and at least one telemetry evidence item that passes the audit threshold.",
            "telemetry_evidence": [
                "target endpoint p95 shift >= 200 ms or p95 ratio >= 1.5 or ok-ratio drop >= 0.1",
                "bound service endpoint SLI shift for Mongo/dependency roots",
                "target CPU/memory/readiness/restart/network error metric shift",
                "restart/pod-kill faults may pass on target pod identity or container start-time change",
            ],
        },
        "summary": {
            "case_count": len(case_results),
            "fault_instance_count": sum(len(case["fault_results"]) for case in case_results),
            "case_status": dict(case_status),
            "fault_status": dict(fault_status),
        },
        "needs_review": [
            {
                "relative_path": case["relative_path"],
                "slot_id": case["slot_id"],
                "sample_id": case["sample_id"],
                "root_causes": case["root_causes"],
                "fault_results": [
                    fault for fault in case["fault_results"] if fault["status"] != "passed"
                ],
            }
            for case in needs_review
        ],
        "cases": case_results,
    }

    out_json = Path(args.out_json)
    out_md = Path(args.out_md)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_markdown(report, generated_at, out_md)


if __name__ == "__main__":
    main()
