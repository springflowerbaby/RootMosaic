"""Read-only historical Prometheus inventory for accepted experiment runs.

No collector imports, Kubernetes calls, range-value export, or changes to runs.
One serial GET per case (three historical phases), a cooldown between requests,
and stop-on-slow/error behavior keep this audit bounded. Querying a shared server
still costs resources: this script does not claim mathematically zero impact.
"""
from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import json
import math
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from source_paths import SourcePaths, guard_output, load_mappings, protected_sources
from source_integrity import freeze_consumed_inputs, require_consumed_manifest

ROOT = Path(__file__).resolve().parents[2]
PHASES = ("pre_fault", "during_fault", "post_recovery")
# Historical raw inputs of the old resource/HTTP panel, not invented values.
# Pod metadata permits later reconstruction of replacement generations.
METRICS = {
    "container_cpu_usage_seconds_total": "cpu",
    "container_cpu_cfs_throttled_periods_total": "cpu",
    "container_cpu_cfs_throttled_seconds_total": "cpu",
    "container_memory_usage_bytes": "memory",
    "container_memory_working_set_bytes": "memory",
    "container_memory_rss": "memory",
    "container_memory_failcnt": "memory",
    "container_start_time_seconds": "lifecycle",
    "container_network_receive_bytes_total": "network",
    "container_network_transmit_bytes_total": "network",
    "container_network_receive_packets_total": "network",
    "container_network_transmit_packets_total": "network",
    "container_network_receive_packets_dropped_total": "network",
    "container_network_transmit_packets_dropped_total": "network",
    "container_network_receive_errors_total": "network",
    "container_network_transmit_errors_total": "network",
    "kube_pod_status_ready": "lifecycle",
    "kube_pod_container_status_ready": "lifecycle",
    "kube_pod_container_status_running": "lifecycle",
    "kube_pod_container_status_restarts_total": "lifecycle",
    "kube_deployment_status_replicas_ready": "lifecycle",
    "http_server_duration_milliseconds_sum": "http",
    "http_server_duration_milliseconds_count": "http",
    "http_server_duration_milliseconds_bucket": "http",
    "kube_pod_info": "identity",
    "kube_pod_owner": "identity",
    "kube_replicaset_owner": "identity",
}
COUNTED = {"P0_COUNTED", "P0_COUNTED_REVIEWED", "P0_COUNTED_LEGACY"}
LIMITS_NOTE = (
    "PRESENT means some samples exist within the exact historical phase, not "
    "complete temporal/service coverage or baseline eligibility. NO_SAMPLES "
    "does not distinguish never collected from expired. Counts can include "
    "multiple series/buckets and are not independent 2-second observations. "
    "No performance values were exported. Host/DB custom evidence and old "
    "probe-panel records are not certified by this Prometheus-only inventory."
)


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def resolved(root, value):
    p = Path(value)
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def load_cases(root, firstpass, ledgers, mappings=()):
    resolver = SourcePaths(root, mappings)
    selections, inputs = [], []
    for path in ([firstpass] if firstpass else []) + list(ledgers):
        inputs.append({"path": str(path), "sha256": digest(path.read_bytes())})
    if firstpass:
        with firstpass.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                if row["state"] not in COUNTED:
                    continue
                if not row.get('summary_path') or not row.get('summary_sha256'):
                    raise ValueError('CSV acceptance requires explicit summary_path and summary_sha256')
                sp = resolver.resolve(row['summary_path'])
                selections.append((1, row["scenario_id"], row["attempt_id"], sp, row['summary_sha256'],
                                   row.get("design_version"), []))
    for path in ledgers:
        doc = read_json(path)
        for row in doc["rows"]:
            if row.get("decision") not in (
                    "ACCEPTED_MINIMUM_COLLECTION", "ACCEPT_MINIMUM_COLLECTION"):
                raise ValueError("unknown acceptance decision in " + str(path))
            rounds = {int(value) for value in (doc.get("round"), doc.get("logical_round"),
                      row.get("round"), row.get("repeat_number")) if value is not None}
            if len(rounds) != 1 or next(iter(rounds)) < 1:
                raise ValueError("missing/conflicting accepted round in " + str(path))
            refs = [x for x in row["evidence_refs"] if
                    x["path"].replace("\\", "/").split('/')[-1] == "SUMMARY.json"]
            if len(refs) != 1:
                raise ValueError("expected one SUMMARY reference")
            if not refs[0].get("sha256"):
                raise ValueError("missing SUMMARY hash in " + str(path))
            selections.append((rounds.pop(), row["scenario_id"], row["attempt_id"],
                               resolver.resolve(refs[0]["path"]), refs[0]["sha256"],
                               row.get("design_version"), row["evidence_refs"]))
    cases, seen = [], set()
    for rnd, sc, attempt, summary_path, expected, design, refs in selections:
        if not isinstance(sc, str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]*', sc):
            raise ValueError('scenario id is not a safe path component')
        raw = summary_path.read_bytes()
        if expected and digest(raw) != expected:
            raise ValueError("SUMMARY hash mismatch: " + sc)
        recovery_path = summary_path.parent / "recovery-input.json"
        recovery_raw = recovery_path.read_bytes()
        inp = json.loads(recovery_raw)
        contract = inp["contract"]
        ctx = contract["context"]
        if ctx["attempt_id"] != attempt or contract.get("purpose") != "formal":
            raise ValueError("wrong attempt/purpose: " + sc)
        if (contract["scenario"]["scenario_id"] != sc or
                (design and contract["scenario"]["design_version"] != design) or
                ctx.get("replicate", rnd) != rnd):
            raise ValueError("wrong accepted scenario/design/round: " + sc)
        key = (contract["scenario"]["design_version"], sc, rnd)
        if key in seen:
            raise ValueError("duplicate accepted slot: " + str(key))
        seen.add(key)
        native = resolver.resolve(ctx["evidence_root"])
        # Bind optional original identity references to this selected SUMMARY,
        # not merely to another file with a legitimate hash and accepted label.
        for name, source in (("recovery-input.json", summary_path.parent / "recovery-input.json"),
                             ("contract.json", native / "contract.json")):
            identity_refs = [r for r in refs if Path(r["path"]).name == name]
            if len(identity_refs) > 1:
                raise ValueError("ambiguous accepted source: " + name)
            if identity_refs:
                ref = identity_refs[0]
                if (resolver.resolve(ref["path"]) != source.resolve() or
                        not ref.get("sha256") or digest(source.read_bytes()) != ref["sha256"]):
                    raise ValueError("accepted source/hash mismatch: " + name)
                if name == "contract.json" and read_json(source) != contract:
                    raise ValueError("accepted contract/recovery mismatch: " + sc)
        windows, manifests = {}, []
        for ph in PHASES:
            path = native / "artifacts" / ph / "metrics/bundle.json"
            raw_bundle = path.read_bytes()
            scope = json.loads(raw_bundle)["scope"]
            if (scope["attempt_id"], scope["run_id"], scope["phase"]) != (
                    attempt, ctx["run_id"], ph):
                raise ValueError("foreign phase bundle: " + sc)
            win = scope["actual_window"]
            if (win["time_basis"] != "unix_epoch" or
                    not all(math.isfinite(win[k]) for k in ("start", "end")) or
                    not 0 < win["end"] - win["start"] <= 3600):
                raise ValueError("invalid actual phase window: " + sc)
            windows[ph] = {k: win[k] for k in ("start", "end")}
            manifests.append({"path": str(path), "sha256": digest(raw_bundle)})
        cases.append({"design_version": key[0], "scenario_id": sc, "round": rnd,
                      "attempt_id": attempt, "run_id": ctx["run_id"],
                      "namespace": ctx["namespace"], "windows": windows,
                      "summary_path": str(summary_path), "summary_sha256": digest(raw),
                      "recovery_input_path": str(recovery_path),
                      "recovery_input_sha256": digest(recovery_raw),
                      "native_root": str(native),
                      "native_contract_sha256": digest((native / 'contract.json').read_bytes()),
                      "source_resolution": resolver.config(),
                      "phase_sources": manifests})
        cases[-1]['consumed_inputs'] = freeze_consumed_inputs(summary_path, native)
        require_consumed_manifest(cases[-1])
    return sorted(cases, key=lambda x: x["windows"]["pre_fault"]["start"]), inputs


def make_query(case):
    expressions = []
    group = "pod,k8s_pod_name,container,service_name,deployment,namespace,k8s_namespace_name"
    for ph, w in case["windows"].items():
        seconds = w["end"] - w["start"]
        # Prometheus 2.x duration syntax requires integral units, not 300.0s.
        # Its stored timestamp resolution is milliseconds; retain original
        # floating epoch windows in PLAN rather than rewriting source facts.
        duration = str(round(seconds * 1000)) + "ms"
        for name, family in METRICS.items():
            ns_label = "k8s_namespace_name" if family == "http" else "namespace"
            filters = ns_label + "=" + json.dumps(case["namespace"])
            if name.startswith("container_") and not name.startswith("container_network_"):
                filters += ',container!="",container!="POD"'
            # Range selectors use (start, end]; no current-time lookback fallback.
            raw = (f"count_over_time({name}{{{filters}}}"
                   f"[{duration}] @ {w['end']:.9f})")
            agg = f"sum by ({group}) ({raw})"
            labelled = f'label_replace({agg},"audit_metric","{name}","",".*")'
            expressions.append(f'label_replace({labelled},"audit_phase","{ph}","",".*")')
    return " or ".join(expressions)


def summarise(result):
    if not isinstance(result, list):
        raise ValueError("expected a vector result")
    cells = {(p, m): [] for p in PHASES for m in METRICS}
    for row in result:
        labels = dict(row["metric"])
        key = (labels.pop("audit_phase", None), labels.pop("audit_metric", None))
        if key not in cells:
            raise ValueError("unexpected query label")
        n = float(row["value"][1])
        if not math.isfinite(n) or n <= 0:
            raise ValueError("nonpositive or nonfinite sample count")
        cells[key].append({"labels": labels, "sample_observations": n})
    return [{"phase": p, "metric": m, "family": METRICS[m],
             "status": "PRESENT" if rows else "NO_SAMPLES",
             "entity_groups": len(rows),
             "sample_observations": sum(x["sample_observations"] for x in rows),
             "groups": rows} for (p, m), rows in cells.items()]


class Reader:
    def __init__(self, origin, gap, slow):
        u = urllib.parse.urlsplit(origin)
        if u.scheme != "http" or u.hostname != "127.0.0.1" or u.username or u.password or u.path not in ("", "/") or u.query or u.fragment:
            raise ValueError("only explicit local 127.0.0.1 HTTP origin allowed")
        self.origin, self.gap, self.slow = origin.rstrip("/"), gap, slow
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.last_end = None
        self.stop_path = None

    def get(self, path, params=None):
        if path not in ("/api/v1/query", "/api/v1/status/flags"):
            raise ValueError("read-only endpoint not allowed")
        if self.last_end is not None:
            time.sleep(max(0, self.gap - (time.monotonic() - self.last_end)))
        if self.stop_path and self.stop_path.exists():
            raise RuntimeError("operator requested audit stop")
        url = self.origin + path + ("?" + urllib.parse.urlencode(params) if params else "")
        start, at = time.monotonic(), now()
        try:
            with self.opener.open(url, timeout=4) as response:
                body = response.read(2_000_001)
        except urllib.error.HTTPError as exc:
            detail = exc.read(1000).decode("utf-8", "replace")
            raise RuntimeError(f"Prometheus HTTP {exc.code}: {detail}") from None
        self.last_end = time.monotonic()
        elapsed = self.last_end - start
        if len(body) > 2_000_000:
            raise ValueError("audit response too large; no automatic retry")
        doc = json.loads(body)
        if doc.get("status") != "success" or doc.get("warnings") or doc.get("infos"):
            raise ValueError("query error/annotation; do not infer missingness")
        return doc, {"queried_at": at, "elapsed_s": elapsed,
                     "response_bytes": len(body), "response_sha256": digest(body),
                     "slow": elapsed > self.slow}


def write_json(path, doc):
    path.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def finish(out, cases, records, state, reason=None):
    counts = collections.Counter()
    per_metric = collections.defaultdict(collections.Counter)
    for r in records:
        for cell in r["cells"]:
            counts[cell["status"]] += 1
            per_metric[cell["metric"]][cell["status"]] += 1
    doc = {"schema": "m1/metric-retention-check-v1", "finished_at": now(),
           "state": state, "reason": reason, "planned_cases": len(cases),
           "checked_cases": len(records), "pending_cases": len(cases) - len(records),
           "checked_case_phases": len(records) * len(PHASES), "cell_counts": dict(counts),
           "per_metric": {k: dict(v) for k, v in per_metric.items()},
           "max_query_elapsed_s": max((r["receipt"]["elapsed_s"] for r in records), default=0),
           "limitations": LIMITS_NOTE, "range_values_exported": False,
           "collection_modified": False}
    write_json(out / "SUMMARY.json", doc)
    with (out / "availability.csv").open("w", newline="", encoding="utf-8-sig") as f:
        fields = ["scenario_id", "round", "attempt_id", "phase", "metric", "family",
                  "status", "entity_groups", "sample_observations"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in records:
            for c in r["cells"]:
                row = {k: r[k] for k in ("scenario_id", "round", "attempt_id")}
                row.update({k: c[k] for k in fields if k in c})
                w.writerow(row)
    print(json.dumps({k: doc[k] for k in ("state", "planned_cases", "checked_cases",
                                        "pending_cases", "cell_counts", "max_query_elapsed_s")}))
    return 0 if state in ("CHECKED", "PLANNED_NO_NETWORK") else 2


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source-root", type=Path, required=True, help="base for relative ledger and contract evidence paths")
    ap.add_argument("--path-map", type=Path, help="explicit absolute-prefix relocation map JSON")
    ap.add_argument("--firstpass-ledger", type=Path)
    ap.add_argument("--accepted-ledger", action="append", default=[])
    ap.add_argument("--prom-url", default="http://127.0.0.1:19090")
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--min-gap-s", type=float, default=2)
    ap.add_argument("--slow-query-s", type=float, default=1)
    ap.add_argument("--execute-readonly", action="store_true", help="without this, only write the local input plan")
    args = ap.parse_args()
    if args.min_gap_s < 1 or args.slow_query_s <= 0 or (args.limit is not None and args.limit <= 0):
        ap.error("positive limit/slow budget and at least one second cooldown required")
    root = args.source_root.resolve()
    cases, inputs = load_cases(root, args.firstpass_ledger.resolve() if args.firstpass_ledger else None,
                              [Path(p).resolve() for p in args.accepted_ledger], load_mappings(args.path_map))
    if not cases:
        ap.error("no accepted completed samples")
    if args.limit:
        cases = cases[:args.limit]
    out = guard_output(args.out, protected_sources({'inputs': inputs, 'cases': cases}))
    out.mkdir(parents=True)
    plan = {"created_at": now(), "script_sha256": digest(Path(__file__).read_bytes()),
            "inputs": inputs, "cases": cases, "metrics": METRICS,
            "source_resolution": cases[0]['source_resolution'],
            "prometheus_origin": args.prom_url, "cooldown_seconds": args.min_gap_s,
            "slow_query_stop_seconds": args.slow_query_s, "limitations": LIMITS_NOTE}
    write_json(out / "PLAN.json", plan)
    if not args.execute_readonly:
        return finish(out, cases, [], "PLANNED_NO_NETWORK")
    records = []
    try:
        client = Reader(args.prom_url, args.min_gap_s, args.slow_query_s)
        client.stop_path = out / "STOP"
        flags, receipt = client.get("/api/v1/status/flags")
        write_json(out / "SERVER.json", {"checked_at": now(), "receipt": receipt,
            "retention": {k: v for k, v in flags["data"].items() if k in
                          ("storage.tsdb.retention.time", "storage.tsdb.retention.size")}})
        if receipt["slow"]:
            return finish(out, cases, records, "STOPPED_SLOW_SERVER")
        with (out / "checks.jsonl").open("x", encoding="utf-8") as log:
            for case in cases:
                query = make_query(case)
                doc, receipt = client.get("/api/v1/query", {
                    "query": query, "time": case["windows"]["post_recovery"]["end"], "timeout": "2s"})
                if doc["data"].get("resultType") != "vector":
                    raise ValueError("expected vector response")
                record = {"scenario_id": case["scenario_id"], "round": case["round"],
                          "attempt_id": case["attempt_id"], "query": query,
                          "receipt": receipt, "cells": summarise(doc["data"]["result"])}
                records.append(record)
                log.write(json.dumps(record, ensure_ascii=False) + "\n")
                log.flush()
                if len(records) % 10 == 0:
                    print(f"checked {len(records)}/{len(cases)}", flush=True)
                if receipt["slow"]:
                    return finish(out, cases, records, "STOPPED_SLOW_QUERY")
        return finish(out, cases, records, "CHECKED")
    except (Exception, KeyboardInterrupt) as exc:
        # Do not turn a server/identity failure into 'NO_SAMPLES'.
        return finish(out, cases, records, "INCOMPLETE", type(exc).__name__ + ": " + str(exc)[:180])


if __name__ == "__main__":
    raise SystemExit(main())
