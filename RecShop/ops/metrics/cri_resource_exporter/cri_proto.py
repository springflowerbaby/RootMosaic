"""Minimal runtime.v1 (CRI) descriptor subset for the exporter.

The descriptor is built programmatically as a ``FileDescriptorProto`` so the
exporter needs only ``grpcio`` + ``protobuf`` at runtime (no grpcio-tools, no
vendored generated code, no network at build).

Field numbers AND wire types mirror kubernetes/cri-api ``runtime.proto`` v1
for the subset this exporter touches: ``UInt64Value``, ``ContainerMetadata``,
``ContainerAttributes``, ``CpuUsage``, ``MemoryUsage``, ``FilesystemIdentifier``,
``WritableLayer``, ``ContainerStats``, ``ContainerStatsRequest/Response``,
``ImageSpec``, ``Container``, ``ContainerFilter``,
``ListContainersRequest/Response``, plus enum ``ContainerState`` and its
``ContainerStateValue`` wrapper (``Container.state`` is a varint enum and
``ContainerFilter.state`` a wrapping message upstream). The
subset uses the upstream-shaped raw enum-varint ``ListContainers`` response. Unknown server-side fields (e.g.
PSI, io) are skipped by protobuf parsing and are not consumed. Live field
mapping must still be cross-checked against the configured runtime before use.
"""
from __future__ import annotations

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

PACKAGE = "runtime.v1"
FILE_NAME = "cri_resource_exporter_runtime_v1_subset.proto"

_FD = descriptor_pb2.FieldDescriptorProto

_POOL = None


def _field(name, number, type_, type_name=None, repeated=False):
    f = _FD()
    f.name = name
    f.number = number
    f.label = _FD.LABEL_REPEATED if repeated else _FD.LABEL_OPTIONAL
    f.type = type_
    if type_name is not None:
        f.type_name = type_name
    return f


def _message(name, fields=(), nested=()):
    d = descriptor_pb2.DescriptorProto()
    d.name = name
    d.field.extend(fields)
    d.nested_type.extend(nested)
    return d


def _map_entry(entry_name):
    d = descriptor_pb2.DescriptorProto()
    d.name = entry_name
    d.field.extend([
        _field("key", 1, _FD.TYPE_STRING),
        _field("value", 2, _FD.TYPE_STRING),
    ])
    d.options.map_entry = True
    return d


def _enum(name, values):
    e = descriptor_pb2.EnumDescriptorProto()
    e.name = name
    for value_name, number in values:
        v = e.value.add()
        v.name = value_name
        v.number = number
    return e


def _build_file() -> descriptor_pb2.FileDescriptorProto:
    fd = descriptor_pb2.FileDescriptorProto()
    fd.name = FILE_NAME
    fd.package = PACKAGE
    fd.syntax = "proto3"

    # Upstream cri-api v1: enum ContainerState (wire form = varint).
    container_state = _enum("ContainerState", (
        ("CONTAINER_CREATED", 0),
        ("CONTAINER_RUNNING", 1),
        ("CONTAINER_EXITED", 2),
        ("CONTAINER_UNKNOWN", 3),
    ))

    uint64_value = _message("UInt64Value", [
        _field("value", 1, _FD.TYPE_UINT64),
    ])
    container_metadata = _message("ContainerMetadata", [
        _field("name", 1, _FD.TYPE_STRING),
        _field("attempt", 2, _FD.TYPE_UINT32),
    ])
    container_attributes = _message("ContainerAttributes", [
        _field("id", 1, _FD.TYPE_STRING),
        _field("metadata", 2, _FD.TYPE_MESSAGE, "." + PACKAGE + ".ContainerMetadata"),
        _field("labels", 3, _FD.TYPE_MESSAGE,
               "." + PACKAGE + ".ContainerAttributes.LabelsEntry", repeated=True),
        _field("annotations", 4, _FD.TYPE_MESSAGE,
               "." + PACKAGE + ".ContainerAttributes.AnnotationsEntry", repeated=True),
    ], nested=[
        _map_entry("LabelsEntry"),
        _map_entry("AnnotationsEntry"),
    ])
    cpu_usage = _message("CpuUsage", [
        _field("timestamp", 1, _FD.TYPE_INT64),
        _field("usage_core_nano_seconds", 2, _FD.TYPE_MESSAGE, "." + PACKAGE + ".UInt64Value"),
        _field("usage_nano_cores", 3, _FD.TYPE_MESSAGE, "." + PACKAGE + ".UInt64Value"),
    ])
    memory_usage = _message("MemoryUsage", [
        _field("timestamp", 1, _FD.TYPE_INT64),
        _field("working_set_bytes", 2, _FD.TYPE_MESSAGE, "." + PACKAGE + ".UInt64Value"),
        _field("available_bytes", 3, _FD.TYPE_MESSAGE, "." + PACKAGE + ".UInt64Value"),
        _field("usage_bytes", 4, _FD.TYPE_MESSAGE, "." + PACKAGE + ".UInt64Value"),
        _field("rss_bytes", 5, _FD.TYPE_MESSAGE, "." + PACKAGE + ".UInt64Value"),
        _field("page_faults", 6, _FD.TYPE_MESSAGE, "." + PACKAGE + ".UInt64Value"),
        _field("major_page_faults", 7, _FD.TYPE_MESSAGE, "." + PACKAGE + ".UInt64Value"),
    ])
    filesystem_identifier = _message("FilesystemIdentifier", [
        _field("mountpoint", 1, _FD.TYPE_STRING),
    ])
    writable_layer = _message("WritableLayer", [
        _field("timestamp", 1, _FD.TYPE_INT64),
        _field("fs_id", 2, _FD.TYPE_MESSAGE, "." + PACKAGE + ".FilesystemIdentifier"),
        _field("used_bytes", 3, _FD.TYPE_MESSAGE, "." + PACKAGE + ".UInt64Value"),
        _field("inodes_used", 4, _FD.TYPE_MESSAGE, "." + PACKAGE + ".UInt64Value"),
    ])
    container_stats = _message("ContainerStats", [
        _field("attributes", 1, _FD.TYPE_MESSAGE, "." + PACKAGE + ".ContainerAttributes"),
        _field("cpu", 2, _FD.TYPE_MESSAGE, "." + PACKAGE + ".CpuUsage"),
        _field("memory", 3, _FD.TYPE_MESSAGE, "." + PACKAGE + ".MemoryUsage"),
        _field("writable_layer", 4, _FD.TYPE_MESSAGE, "." + PACKAGE + ".WritableLayer"),
    ])
    container_stats_request = _message("ContainerStatsRequest", [
        _field("container_id", 1, _FD.TYPE_STRING),
    ])
    container_stats_response = _message("ContainerStatsResponse", [
        _field("stats", 1, _FD.TYPE_MESSAGE, "." + PACKAGE + ".ContainerStats"),
    ])
    image_spec = _message("ImageSpec", [
        _field("image", 1, _FD.TYPE_STRING),
    ])
    # Upstream cri-api v1: ContainerStateValue wraps the enum in a message.
    container_state_value = _message("ContainerStateValue", [
        _field("state", 1, _FD.TYPE_ENUM, "." + PACKAGE + ".ContainerState"),
    ])
    container = _message("Container", [
        _field("id", 1, _FD.TYPE_STRING),
        _field("pod_sandbox_id", 2, _FD.TYPE_STRING),
        _field("metadata", 3, _FD.TYPE_MESSAGE, "." + PACKAGE + ".ContainerMetadata"),
        _field("image", 4, _FD.TYPE_MESSAGE, "." + PACKAGE + ".ImageSpec"),
        _field("image_ref", 5, _FD.TYPE_STRING),
        _field("state", 6, _FD.TYPE_ENUM, "." + PACKAGE + ".ContainerState"),
        _field("created_at", 7, _FD.TYPE_INT64),
        _field("labels", 8, _FD.TYPE_MESSAGE,
               "." + PACKAGE + ".Container.LabelsEntry", repeated=True),
        _field("annotations", 9, _FD.TYPE_MESSAGE,
               "." + PACKAGE + ".Container.AnnotationsEntry", repeated=True),
    ], nested=[
        _map_entry("LabelsEntry"),
        _map_entry("AnnotationsEntry"),
    ])
    container_filter = _message("ContainerFilter", [
        _field("id", 1, _FD.TYPE_STRING),
        _field("state", 2, _FD.TYPE_MESSAGE, "." + PACKAGE + ".ContainerStateValue"),
        _field("pod_sandbox_id", 3, _FD.TYPE_STRING),
        _field("label_selector", 4, _FD.TYPE_MESSAGE,
               "." + PACKAGE + ".ContainerFilter.LabelSelectorEntry", repeated=True),
    ], nested=[
        _map_entry("LabelSelectorEntry"),
    ])
    list_containers_request = _message("ListContainersRequest", [
        _field("filter", 1, _FD.TYPE_MESSAGE, "." + PACKAGE + ".ContainerFilter"),
    ])
    list_containers_response = _message("ListContainersResponse", [
        _field("containers", 1, _FD.TYPE_MESSAGE, "." + PACKAGE + ".Container", repeated=True),
    ])

    fd.message_type.extend([
        uint64_value,
        container_metadata,
        container_attributes,
        cpu_usage,
        memory_usage,
        filesystem_identifier,
        writable_layer,
        container_stats,
        container_stats_request,
        container_stats_response,
        image_spec,
        container_state_value,
        container,
        container_filter,
        list_containers_request,
        list_containers_response,
    ])
    fd.enum_type.extend([container_state])

    service = fd.service.add()
    service.name = "RuntimeService"
    for method_name, in_type, out_type in (
        ("ListContainers", "ListContainersRequest", "ListContainersResponse"),
        ("ContainerStats", "ContainerStatsRequest", "ContainerStatsResponse"),
    ):
        m = service.method.add()
        m.name = method_name
        m.input_type = "." + PACKAGE + "." + in_type
        m.output_type = "." + PACKAGE + "." + out_type
        m.client_streaming = False
        m.server_streaming = False
    return fd


def _ensure_pool() -> descriptor_pool.DescriptorPool:
    global _POOL
    if _POOL is None:
        pool = descriptor_pool.DescriptorPool()
        pool.Add(_build_file())
        _POOL = pool
    return _POOL


def message_class(full_name: str):
    """Return a runtime message class for e.g. 'runtime.v1.ContainerStats'."""
    return message_factory.GetMessageClass(_ensure_pool().FindMessageTypeByName(full_name))


def new_container_stats_request(container_id: str):
    req = message_class(PACKAGE + ".ContainerStatsRequest")()
    req.container_id = container_id
    return req


def new_list_containers_request(label_selector=None):
    req = message_class(PACKAGE + ".ListContainersRequest")()
    if label_selector:
        for key, value in sorted(label_selector.items()):
            req.filter.label_selector[key] = value
    return req
