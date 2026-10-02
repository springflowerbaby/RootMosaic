"""Pure, byte-preserving AST-located service patch; never imports a business app."""
from __future__ import annotations

import ast
import hashlib
from pathlib import PurePosixPath

MARKER = "# M1 metric interval: validate before the optional OTel init handler."


def transform(source: bytes, relative_source: str) -> tuple[bytes, dict]:
    path = PurePosixPath(relative_source)
    if path.parts[:1] != ("services",) or ".." in path.parts:
        raise ValueError("service source path outside approved layout")
    if path.name == "__init__.py" and path.parts == ("services", "shop_web", "app", "__init__.py"):
        root_depth = 3
    elif len(path.parts) == 3 and path.name in {"app.py", "api_server.py"}:
        root_depth = 2
    else:
        raise ValueError("unsupported service source layout")
    if MARKER.encode() in source:
        raise ValueError("source already patched; do not patch an overlay again")
    # AST offsets are UTF-8 byte columns. Preserve all original bytes/EOL except
    # one literal and the explicit bootstrap block, including non-ASCII comments.
    tree = ast.parse(source.decode("utf-8-sig"))
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    aliases = {name.asname or name.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
               and node.module == "opentelemetry.sdk.metrics.export" for name in node.names
               if name.name == "PeriodicExportingMetricReader"}
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name) and node.func.id in aliases]
    if len(calls) != 1:
        raise ValueError("exactly one explicit metric reader call is required")
    call = calls[0]
    interval = [keyword.value for keyword in call.keywords if keyword.arg == "export_interval_millis"]
    if len(interval) != 1 or not isinstance(interval[0], ast.Constant) or type(interval[0].value) is not int or interval[0].value != 15000:
        raise ValueError("expected original literal 15000; unknown baseline rejected")
    ancestors = []
    node = call
    while node in parents:
        node = parents[node]
        ancestors.append(node)
    tries = [node for node in ancestors if isinstance(node, ast.Try)]
    if not tries:
        raise ValueError("expected optional OTel init try handler")
    outer_try = tries[-1]
    parent = parents[outer_try]
    if not isinstance(parent, ast.If) or "OTEL_ENABLED" not in ast.unparse(parent.test):
        raise ValueError("outer OTel guard shape changed")
    newline = b"\r\n" if b"\r\n" in source else b"\n"
    bom = b"\xef\xbb\xbf" if source.startswith(b"\xef\xbb\xbf") else b""
    body = source[len(bom):]
    lines = body.splitlines(keepends=True)
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line))
    position = offsets[outer_try.lineno - 1]
    indent = b" " * outer_try.col_offset
    if not lines[outer_try.lineno - 1].startswith(indent + b"try:"):
        raise ValueError("unsupported indentation")
    additions = [MARKER, "import sys as _otel_config_sys", "from pathlib import Path as _OtelConfigPath",
                 f"_otel_config_root = str(_OtelConfigPath(__file__).resolve().parents[{root_depth}])",
                 "if _otel_config_root not in _otel_config_sys.path:", "    _otel_config_sys.path.insert(0, _otel_config_root)",
                 "from shared.otel_metric_config import metric_export_interval_millis as _otel_metric_interval",
                 "_otel_metric_export_interval_ms = _otel_metric_interval()"]
    insertion = b"".join(indent + line.encode("ascii") + newline for line in additions)
    literal = interval[0]
    left = offsets[literal.lineno - 1] + literal.col_offset
    right = offsets[literal.end_lineno - 1] + literal.end_col_offset
    if body[left:right] != b"15000":
        raise ValueError("literal byte offset mismatch")
    patched = body[:left] + b"_otel_metric_export_interval_ms" + body[right:]
    patched = bom + patched[:position] + insertion + patched[position:]
    ast.parse(patched.decode("utf-8-sig"))
    return patched, {"relative_source": relative_source, "before_sha256": hashlib.sha256(source).hexdigest(),
                     "after_sha256": hashlib.sha256(patched).hexdigest(), "original_reader_line": call.lineno,
                     "bootstrap_before_try_line": outer_try.lineno, "project_root_parent_index": root_depth,
                     "newline": "CRLF" if newline == b"\r\n" else "LF", "status": "source_only_not_deployed"}
