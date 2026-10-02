"""CRI client and response normalization for the container resource exporter.

Transport: gRPC over the node containerd CRI socket, RPCs used are exactly
``runtime.v1.RuntimeService/ContainerStats`` (per target container, per tick)
and ``runtime.v1.RuntimeService/ListContainers`` (metadata-only resolution of
name targets; never used as a stats source). ListContainerStats is NOT used:
its ``usageNanoCores``/``WritableLayer`` paths are not certified fresh.

Normalization accepts both protobuf messages and the protobuf-JSON dict shape
(camelCase keys, int64 values as strings, UInt64Value wrappers as
``{"value": "123"}``, the stats payload reachable as ``{"stats": [...]}`` for
list-shaped captures or ``{"stats": {...}}``/a bare object for the per-container
RPC response). Missing fields normalize to ``None``; nothing is defaulted.
"""
from __future__ import annotations

from . import cri_proto

POD_NAME_LABEL = "io.kubernetes.pod.name"
POD_NAMESPACE_LABEL = "io.kubernetes.pod.namespace"
POD_UID_LABEL = "io.kubernetes.pod.uid"
CONTAINER_NAME_LABEL = "io.kubernetes.container.name"

# upstream cri-api v1 enum ContainerState (wire form = varint)
CONTAINER_STATE_NAMES = {
    0: "CONTAINER_CREATED",
    1: "CONTAINER_RUNNING",
    2: "CONTAINER_EXITED",
    3: "CONTAINER_UNKNOWN",
}


def _normalize_state(value):
    """Enum int -> upstream value name; name strings pass through; else None.

    The protobuf message path reads the enum as an int (upb); dict/JSON paths
    usually carry the value name. Unknown ints stay visible as their number.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return CONTAINER_STATE_NAMES.get(value, str(value))
    if isinstance(value, str):
        return value or None
    return None

STATS_KEYS = (
    "container_id", "container_name", "pod_name", "pod_namespace", "pod_uid",
    "attempt",
    "cpu_timestamp_ns", "usage_core_nano_seconds", "usage_nano_cores",
    "memory_timestamp_ns", "working_set_bytes", "available_bytes",
    "usage_bytes", "rss_bytes", "page_faults", "major_page_faults",
    "writable_layer_timestamp_ns", "writable_used_bytes", "writable_inodes_used",
)


def _is_message(obj) -> bool:
    return hasattr(obj, "DESCRIPTOR")


def _as_int(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            return None
    return None


def _as_ts(value):
    """Timestamps are epoch nanoseconds; absent/zero stays None, never 0-filled."""
    ts = _as_int(value)
    if ts is None or ts <= 0:
        return None
    return ts


def _dict_u64(container):
    if not isinstance(container, dict):
        return None
    return _as_int(container.get("value"))


def _msg_u64(parent, field_name):
    try:
        if parent.HasField(field_name):
            return int(getattr(parent, field_name).value)
    except ValueError:
        # Field is not message-typed or otherwise not HasField-able; treat as absent.
        return None
    return None


def _stats_payload(raw):
    """Return the single ContainerStats object from any recorded raw shape."""
    if raw is None:
        return None
    if _is_message(raw):
        field_names = {f.name for f in raw.DESCRIPTOR.fields}
        if "stats" not in field_names:
            return raw  # already a ContainerStats-like message
        stats = getattr(raw, "stats")
        return stats if stats is not None else None
    if isinstance(raw, dict):
        stats = raw.get("stats", raw)
        if isinstance(stats, list):
            if len(stats) != 1:
                return None
            stats = stats[0]
        return stats if isinstance(stats, dict) else None
    return None


def normalize_stats(raw):
    """Normalize one ContainerStats payload to a flat dict, or None.

    None means: unparseable / empty / ambiguous (0 or >1 stats entries).
    Every field may individually be None (absent in the source); the exporter
    must omit those metrics rather than substitute values.
    """
    stats = _stats_payload(raw)
    if stats is None or (_is_message(stats) is False and not isinstance(stats, dict)):
        return None
    result = {key: None for key in STATS_KEYS}

    if _is_message(stats):
        attrs = stats.attributes
        labels = dict(attrs.labels)
        metadata = attrs.metadata
        result["container_id"] = attrs.id.strip() or None
        result["container_name"] = labels.get(CONTAINER_NAME_LABEL) or (metadata.name or None)
        result["pod_name"] = labels.get(POD_NAME_LABEL)
        result["pod_namespace"] = labels.get(POD_NAMESPACE_LABEL)
        result["pod_uid"] = labels.get(POD_UID_LABEL)
        result["attempt"] = _as_int(metadata.attempt)
        cpu = stats.cpu
        result["cpu_timestamp_ns"] = _as_ts(cpu.timestamp)
        result["usage_core_nano_seconds"] = _msg_u64(cpu, "usage_core_nano_seconds")
        result["usage_nano_cores"] = _msg_u64(cpu, "usage_nano_cores")
        memory = stats.memory
        result["memory_timestamp_ns"] = _as_ts(memory.timestamp)
        result["working_set_bytes"] = _msg_u64(memory, "working_set_bytes")
        result["available_bytes"] = _msg_u64(memory, "available_bytes")
        result["usage_bytes"] = _msg_u64(memory, "usage_bytes")
        result["rss_bytes"] = _msg_u64(memory, "rss_bytes")
        result["page_faults"] = _msg_u64(memory, "page_faults")
        result["major_page_faults"] = _msg_u64(memory, "major_page_faults")
        writable = stats.writable_layer
        result["writable_layer_timestamp_ns"] = _as_ts(writable.timestamp)
        result["writable_used_bytes"] = _msg_u64(writable, "used_bytes")
        result["writable_inodes_used"] = _msg_u64(writable, "inodes_used")
    elif isinstance(stats, dict):
        attrs = stats.get("attributes") or {}
        labels = attrs.get("labels") or {}
        metadata = attrs.get("metadata") or {}
        result["container_id"] = (attrs.get("id") or "").strip() or None
        result["container_name"] = labels.get(CONTAINER_NAME_LABEL) or metadata.get("name")
        result["pod_name"] = labels.get(POD_NAME_LABEL)
        result["pod_namespace"] = labels.get(POD_NAMESPACE_LABEL)
        result["pod_uid"] = labels.get(POD_UID_LABEL)
        result["attempt"] = _as_int(metadata.get("attempt"))
        cpu = stats.get("cpu") or {}
        result["cpu_timestamp_ns"] = _as_ts(cpu.get("timestamp"))
        result["usage_core_nano_seconds"] = _dict_u64(cpu.get("usageCoreNanoSeconds"))
        result["usage_nano_cores"] = _dict_u64(cpu.get("usageNanoCores"))
        memory = stats.get("memory") or {}
        result["memory_timestamp_ns"] = _as_ts(memory.get("timestamp"))
        result["working_set_bytes"] = _dict_u64(memory.get("workingSetBytes"))
        result["available_bytes"] = _dict_u64(memory.get("availableBytes"))
        result["usage_bytes"] = _dict_u64(memory.get("usageBytes"))
        result["rss_bytes"] = _dict_u64(memory.get("rssBytes"))
        result["page_faults"] = _dict_u64(memory.get("pageFaults"))
        result["major_page_faults"] = _dict_u64(memory.get("majorPageFaults"))
        writable = stats.get("writableLayer") or {}
        result["writable_layer_timestamp_ns"] = _as_ts(writable.get("timestamp"))
        result["writable_used_bytes"] = _dict_u64(writable.get("usedBytes"))
        result["writable_inodes_used"] = _dict_u64(writable.get("inodesUsed"))

    if result["container_id"] is None:
        return None
    return result


def _container_payload(raw):
    """ListContainers entries: a protobuf Container message or its dict form."""
    if _is_message(raw) or isinstance(raw, dict):
        return raw
    return None


def normalize_container(raw):
    """Normalize one CRI ``Container`` listing entry (metadata only).

    Never raises for message/dict inputs: a container without kubernetes
    labels (e.g. a non-k8s container) simply yields None-valued fields and
    matches no pod target; a malformed entry returns None and is skipped.
    """
    obj = _container_payload(raw)
    if obj is None:
        return None
    try:
        result = {
            "id": None, "state": None, "pod_name": None, "pod_namespace": None,
            "pod_uid": None, "container_name": None, "attempt": None,
            "pod_sandbox_id": None,
        }
        if _is_message(obj):
            labels = dict(obj.labels)
            result["id"] = obj.id.strip() or None
            result["state"] = _normalize_state(obj.state)
            result["pod_sandbox_id"] = obj.pod_sandbox_id or None
            metadata_name = obj.metadata.name or None
            metadata_attempt = obj.metadata.attempt
        else:
            labels = obj.get("labels") or {}
            metadata = obj.get("metadata") if isinstance(obj.get("metadata"), dict) else {}
            result["id"] = (obj.get("id") or "").strip() or None
            result["state"] = _normalize_state(obj.get("state"))
            result["pod_sandbox_id"] = obj.get("podSandboxId") or None
            metadata_name = metadata.get("name")
            metadata_attempt = metadata.get("attempt")
        result["pod_name"] = labels.get(POD_NAME_LABEL)
        result["pod_namespace"] = labels.get(POD_NAMESPACE_LABEL)
        result["pod_uid"] = labels.get(POD_UID_LABEL)
        result["container_name"] = labels.get(CONTAINER_NAME_LABEL) or metadata_name
        result["attempt"] = _as_int(metadata_attempt)
        return result
    except Exception:
        return None


def iter_containers(raw):
    """Yield normalized container entries from a ListContainers response.

    Per-entry failures are skipped, never raised: one malformed or label-less
    entry must not kill the collection loop (review F2).
    """
    if raw is None:
        return
    if _is_message(raw):
        entries = list(getattr(raw, "containers", []))
    elif isinstance(raw, dict):
        entries = list(raw.get("containers") or [])
    else:
        return
    for entry in entries:
        normalized = normalize_container(entry)
        if normalized and normalized.get("id"):
            yield normalized


def classify_rpc_error(exc) -> str:
    """Map an RPC failure to a stable reason label; never rewrites values."""
    code = None
    getter = getattr(exc, "code", None)
    if callable(getter):
        try:
            code = str(getter())
        except Exception:
            code = None
    if isinstance(exc, TimeoutError):
        return "timeout"
    if code:
        if "DEADLINE_EXCEEDED" in code:
            return "timeout"
        if "NOT_FOUND" in code:
            return "not_found"
        if "UNAVAILABLE" in code:
            return "unavailable"
        if "PERMISSION_DENIED" in code:
            return "permission_denied"
    return "error"


class GrpcCriClient:
    """Thin gRPC client for the two RPCs this exporter is allowed to use.

    The gRPC module is imported lazily so unit tests and offline tooling can
    use the normalization helpers without a grpc installation.
    """

    def __init__(self, endpoint: str, default_timeout_s: float = 1.0):
        import grpc  # lazy: only needed on the node

        self._grpc = grpc
        self.default_timeout_s = float(default_timeout_s)
        self._channel = grpc.insecure_channel(endpoint)
        stats_resp_cls = cri_proto.message_class("runtime.v1.ContainerStatsResponse")
        list_resp_cls = cri_proto.message_class("runtime.v1.ListContainersResponse")
        self._container_stats_stub = self._channel.unary_unary(
            "/runtime.v1.RuntimeService/ContainerStats",
            request_serializer=lambda m: m.SerializeToString(),
            response_deserializer=stats_resp_cls.FromString,
        )
        self._list_containers_stub = self._channel.unary_unary(
            "/runtime.v1.RuntimeService/ListContainers",
            request_serializer=lambda m: m.SerializeToString(),
            response_deserializer=list_resp_cls.FromString,
        )

    def container_stats(self, container_id: str, timeout_s: float = None):
        timeout = self.default_timeout_s if timeout_s is None else float(timeout_s)
        return self._container_stats_stub(
            cri_proto.new_container_stats_request(container_id), timeout=timeout)

    def list_containers(self, label_selector=None, timeout_s: float = None):
        timeout = self.default_timeout_s if timeout_s is None else float(timeout_s)
        return self._list_containers_stub(
            cri_proto.new_list_containers_request(label_selector), timeout=timeout)

    def close(self):
        self._channel.close()
