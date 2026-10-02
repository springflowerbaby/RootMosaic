"""Prometheus text-format (0.0.4) exposition for the CRI resource exporter.

Rules encoded here:
- container metrics are GAUGES (cumulative values as reported by CRI) and
  always carry an EXPLICIT timestamp = the CRI source timestamp of that field,
  converted from epoch nanoseconds to milliseconds with round-half-up;
- exporter self/health metrics carry NO explicit timestamp (scrape time);
- a family sample is omitted entirely when the source field is missing
  (no zero fill) or its source timestamp is absent;
- a sample is withheld once its newest fresh source timestamp is older than
  max_sample_age_ns; the withheld target is counted in cri_exporter_stale_targets.
  Withholding (series goes absent) is chosen over re-serving an old timestamp
  forever: Prometheus would otherwise record repeated duplicate/out-of-order
  sample errors for the same series (documented in DEPLOY-PLAN.md);
- ``*_cached`` families exist only to record not-certified-fresh fields; they
  never feed rate calculations and their HELP text says so;
- ``cri_exporter_target_identity_changes_total`` is emitted for EVERY
  configured target from process start (0 before any change), so the identity
  signal is observable without waiting for a change to happen (LW1). The
  counting rule (frozen, review F3 disposition) lives in the collector:
  +1 on a newly resolved id different from the last id the target ever
  resolved to - covering direct replacement and replacement after a
  ``no_match`` gap - while the same id returning after a gap is not a change;
  the counter resets on process restart.
"""
from __future__ import annotations

import math

FRESH_FAMILIES = (
    ("cri_container_cpu_usage_core_nanoseconds", "usage_core_nano_seconds", "cpu_timestamp_ns",
     "Cumulative CPU nanoseconds used (CRI ContainerStats usageCoreNanoSeconds); "
     "fresh per-call path; explicit timestamp is the CRI CPU source timestamp."),
    ("cri_container_memory_working_set_bytes", "working_set_bytes", "memory_timestamp_ns",
     "Container memory working set bytes; explicit timestamp is the CRI memory source timestamp."),
    ("cri_container_memory_usage_bytes", "usage_bytes", "memory_timestamp_ns",
     "Container memory usage bytes; explicit timestamp is the CRI memory source timestamp."),
    ("cri_container_memory_rss_bytes", "rss_bytes", "memory_timestamp_ns",
     "Container RSS bytes; explicit timestamp is the CRI memory source timestamp."),
    ("cri_container_memory_available_bytes", "available_bytes", "memory_timestamp_ns",
     "Container available bytes; explicit timestamp is the CRI memory source timestamp."),
    ("cri_container_memory_page_faults", "page_faults", "memory_timestamp_ns",
     "Cumulative container page faults; explicit timestamp is the CRI memory source timestamp."),
    ("cri_container_memory_major_page_faults", "major_page_faults", "memory_timestamp_ns",
     "Cumulative container major page faults; explicit timestamp is the CRI memory source timestamp."),
)

CACHED_FAMILIES = (
    ("cri_container_cpu_usage_nano_cores_cached", "usage_nano_cores", "cpu_timestamp_ns",
     "CRI-reported usageNanoCores. NOT certified fresh (statsCollector precomputed "
     "rate path); recorded for diagnostics only, never a 2 s rate source."),
    ("cri_container_writable_layer_used_bytes_cached", "writable_used_bytes", "writable_layer_timestamp_ns",
     "Writable layer used bytes. NOT certified fresh (snapshot cache path); carries "
     "its own (older) CRI source timestamp."),
    ("cri_container_writable_layer_inodes_used_cached", "writable_inodes_used", "writable_layer_timestamp_ns",
     "Writable layer inodes used. NOT certified fresh (snapshot cache path); carries "
     "its own (older) CRI source timestamp."),
)

ALL_FAMILIES = FRESH_FAMILIES + CACHED_FAMILIES

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def ns_to_ms(timestamp_ns: int) -> int:
    """Epoch nanoseconds -> milliseconds, round-half-up (never truncates bias)."""
    return int((int(timestamp_ns) + 500_000) // 1_000_000)


def escape_label_value(value: str) -> str:
    return (value.replace("\\", "\\\\").replace("\"", "\\\"")
                 .replace("\n", "\\n"))


def format_value(value):
    """Format a sample value; returns None for non-finite floats (omit line)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return repr(value)
    return None


def _render_labels(label_pairs) -> str:
    if not label_pairs:
        return ""
    inner = ",".join(
        '{}="{}"'.format(name, escape_label_value(str(value)))
        for name, value in label_pairs)
    return "{" + inner + "}"


def _base_label_pairs(snapshot: dict, state: dict, sample: dict):
    pairs = []
    if snapshot.get("node"):
        pairs.append(("node", snapshot["node"]))
    if snapshot.get("node_uid"):
        pairs.append(("node_uid", snapshot["node_uid"]))
    if sample.get("pod_namespace"):
        pairs.append(("namespace", sample["pod_namespace"]))
    if sample.get("pod_name"):
        pairs.append(("pod", sample["pod_name"]))
    if sample.get("pod_uid"):
        pairs.append(("pod_uid", sample["pod_uid"]))
    if sample.get("container_name"):
        pairs.append(("container", sample["container_name"]))
    if sample.get("container_id"):
        pairs.append(("container_id", sample["container_id"]))
    if sample.get("attempt") is not None:
        pairs.append(("attempt", str(int(sample["attempt"]))))
    pairs.sort()
    return pairs


def _container_families(snapshot: dict, now_ns: int):
    by_family = {}
    stale_targets = 0
    ages = []
    for state in snapshot["states"]:
        sample = state.get("last_sample")
        if not sample:
            if state.get("resolved_id"):
                stale_targets += 1
            continue
        fresh_ts = sample.get("cpu_timestamp_ns") or sample.get("memory_timestamp_ns")
        if not fresh_ts or fresh_ts <= 0:
            if state.get("resolved_id"):
                stale_targets += 1
            continue
        if (now_ns - fresh_ts) > snapshot["max_sample_age_ns"]:
            stale_targets += 1
            continue
        ages.append((sample["container_id"], (now_ns - fresh_ts) / 1e9))
        labels = _base_label_pairs(snapshot, state, sample)
        for name, sample_key, ts_key, _help in ALL_FAMILIES:
            value = sample.get(sample_key)
            ts_ns = sample.get(ts_key)
            if value is None or not ts_ns or ts_ns <= 0:
                continue  # missing in source: omit, never zero-fill
            by_family.setdefault(name, []).append((labels, value, ns_to_ms(ts_ns)))
    return by_family, stale_targets, ages


SELF_META = {
    "cri_exporter_up": ("Exporter process liveness (self).", "gauge"),
    "cri_exporter_build_info": ("Exporter build identity.", "gauge"),
    "cri_exporter_config_interval_seconds": ("Configured tick interval in seconds.", "gauge"),
    "cri_exporter_config_rpc_timeout_seconds": ("Configured per-RPC timeout in seconds.", "gauge"),
    "cri_exporter_config_max_sample_age_seconds": ("Samples older than this are withheld.", "gauge"),
    "cri_exporter_ticks_total": ("Executed collection ticks.", "counter"),
    "cri_exporter_missed_ticks_total": ("Tick slots skipped after overruns (never re-run).", "counter"),
    "cri_exporter_tick_overruns_total": ("Ticks whose work exceeded the interval.", "counter"),
    "cri_exporter_last_tick_duration_seconds": ("Wall duration of the most recent tick.", "gauge"),
    "cri_exporter_cpu_cumulative_decreases_total": ("Cumulative CPU decreased within one container id (unexpected; raw value still exposed).", "counter"),
    "cri_exporter_stale_targets": ("Targets currently withheld: no sample fresher than max age.", "gauge"),
    "cri_exporter_target_identity_changes_total": (
        "Resolved container id changes per target (replacement signal): +1 when a "
        "newly resolved id differs from the last id this target ever resolved to, "
        "whether the replacement is observed directly or after a no_match gap; the "
        "same id returning after a gap is not a change; the first resolution is not "
        "a change; resets on process restart. Emitted for every configured target "
        "from process start (0 before any change).", "counter"),
    "cri_exporter_rpc_total": ("RPC attempts by kind and status (single attempt per target per tick).", "counter"),
    "cri_exporter_rpc_last_duration_seconds": ("Duration of the most recent RPC per kind.", "gauge"),
    "cri_exporter_resolve_total": ("Target resolution outcomes for name targets (ok/no_match/ambiguous/rpc_*).", "counter"),
    "cri_exporter_target_sample_age_seconds": ("Exporter-derived age of the newest fresh source timestamp per exposed target.", "gauge"),
}


def _self_families(snapshot: dict, stale_targets: int, ages) -> dict:
    counters = snapshot["counters"]
    version = snapshot.get("version", "")
    by_family = {
        "cri_exporter_up": [([], 1)],
        "cri_exporter_build_info": [([("version", version)], 1)],
        "cri_exporter_config_interval_seconds": [([], snapshot["interval_s"])],
        "cri_exporter_config_rpc_timeout_seconds": [([], snapshot["rpc_timeout_s"])],
        "cri_exporter_config_max_sample_age_seconds": [([], snapshot["max_sample_age_ns"] / 1e9)],
        "cri_exporter_ticks_total": [([], counters["ticks_total"])],
        "cri_exporter_missed_ticks_total": [([], counters["missed_ticks_total"])],
        "cri_exporter_tick_overruns_total": [([], counters["tick_overruns_total"])],
        "cri_exporter_last_tick_duration_seconds": [([], counters["last_tick_duration_s"])],
        "cri_exporter_cpu_cumulative_decreases_total": [([], counters["cpu_cumulative_decreases_total"])],
        "cri_exporter_stale_targets": [([], stale_targets)],
    }
    for state in snapshot["states"]:
        # LW1: always emit (0 until the first change) so the per-target identity
        # signal exists in the exposition from process start; a conditionally
        # absent series made "no change yet" indistinguishable from "not watched".
        by_family.setdefault("cri_exporter_target_identity_changes_total", []).append(
            ([("target", state["target_key"])], state["identity_changes"]))
    for key, count in sorted(counters["rpc_total"].items()):
        kind, status = key.split("|", 1)
        by_family.setdefault("cri_exporter_rpc_total", []).append(
            ([("kind", kind), ("status", status)], count))
    for kind, duration in sorted(counters["rpc_last_duration_s"].items()):
        by_family.setdefault("cri_exporter_rpc_last_duration_seconds", []).append(
            ([("kind", kind)], duration))
    for status, count in sorted(counters.get("resolve_total", {}).items()):
        by_family.setdefault("cri_exporter_resolve_total", []).append(
            ([("status", status)], count))
    for container_id, age_s in sorted(ages):
        by_family.setdefault("cri_exporter_target_sample_age_seconds", []).append(
            ([("container_id", container_id)], round(age_s, 6)))
    return by_family


def render_exposition(snapshot: dict, now_ns: int) -> str:
    """Render the full /metrics body. Deterministic: families sorted, labels sorted."""
    by_family, stale_targets, ages = _container_families(snapshot, now_ns)
    self_families = _self_families(snapshot, stale_targets, ages)
    blocks = []
    for name, _sample_key, _ts_key, help_text in sorted(ALL_FAMILIES, key=lambda f: f[0]):
        samples = by_family.get(name)
        if not samples:
            continue
        lines = ["# HELP {} {}".format(name, help_text),
                 "# TYPE {} gauge".format(name)]
        for labels, value, ts_ms in sorted(samples, key=lambda s: str(s[0])):
            formatted = format_value(value)
            if formatted is None:
                continue
            lines.append("{}{} {} {}".format(name, _render_labels(labels), formatted, ts_ms))
        blocks.append("\n".join(lines))

    for name in sorted(self_families):
        samples = self_families[name]
        if not samples:
            continue
        help_text, metric_type = SELF_META[name]
        lines = ["# HELP {} {}".format(name, help_text),
                 "# TYPE {} {}".format(name, metric_type)]
        for labels, value in sorted(samples, key=lambda s: str(s[0])):
            formatted = format_value(value)
            if formatted is None:
                continue
            lines.append("{}{} {}".format(name, _render_labels(labels), formatted))
        blocks.append("\n".join(lines))
    return "\n".join(blocks) + "\n"
