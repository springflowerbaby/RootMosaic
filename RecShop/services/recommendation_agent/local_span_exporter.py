"""Optional local JSONL span exporter, enabled by SPAN_FILE.

Uses SimpleSpanProcessor to flush finished spans immediately. Consumers can
select complete traces by trace_id instead of assuming a wall-clock request
window contains every descendant span. Each row retains trace/span/parent IDs,
source times, status and attributes. A module lock serializes writes.
"""

import json
import os
import threading

from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

_WRITE_LOCK = threading.Lock()


def _fmt_trace_id(tid):
    try:
        return format(tid, "032x")
    except Exception:
        return ""


def _fmt_span_id(sid):
    try:
        if sid in (None, 0):
            return ""
        return format(sid, "016x")
    except Exception:
        return ""


def _serialize_attributes(attrs):
    """把 span attributes 转成 JSON 可序列化的 dict（只保留我们关心的标量/字符串）。"""
    out = {}
    if not attrs:
        return out
    for k, v in attrs.items():
        try:
            if isinstance(v, (str, int, float, bool)):
                out[k] = v
            elif isinstance(v, (list, tuple)):
                out[k] = [x for x in v if isinstance(x, (str, int, float, bool))]
            else:
                out[k] = str(v)
        except Exception:
            out[k] = "<unserializable>"
    return out


class LocalJSONLSpanExporter(SpanExporter):
    """把每条 ReadableSpan 序列化为一行 JSON append 到 file_path。"""

    def __init__(self, file_path):
        self._file_path = file_path
        d = os.path.dirname(file_path)
        if d:
            os.makedirs(d, exist_ok=True)

    def export(self, spans):
        lines = []
        for sp in spans:
            try:
                ctx = sp.get_span_context()
                parent = sp.parent
                status = sp.status
                rec = {
                    "trace_id": _fmt_trace_id(ctx.trace_id),
                    "span_id": _fmt_span_id(ctx.span_id),
                    "parent_span_id": _fmt_span_id(parent.span_id) if parent else "",
                    "name": sp.name,
                    "start_unix_nano": sp.start_time,
                    "end_unix_nano": sp.end_time,
                    "duration_ms": ((sp.end_time - sp.start_time) / 1e6)
                    if (sp.end_time and sp.start_time) else None,
                    "status_code": status.status_code.name if status else "UNSET",
                    "kind": sp.kind.name if sp.kind is not None else None,
                    "attributes": _serialize_attributes(sp.attributes),
                }
                lines.append(json.dumps(rec, ensure_ascii=False))
            except Exception:
                # 单条 span 序列化失败不连累整批
                continue
        if not lines:
            return SpanExportResult.SUCCESS
        try:
            with _WRITE_LOCK:
                with open(self._file_path, "a", encoding="utf-8") as f:
                    f.write("\n".join(lines) + "\n")
            return SpanExportResult.SUCCESS
        except Exception:
            return SpanExportResult.FAILURE

    def shutdown(self):
        return None

    def force_flush(self, timeout_millis=30000):
        return True
