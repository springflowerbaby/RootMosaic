"""CLI + HTTP entrypoint for the container resource exporter.

Two run modes:
- server (default): bounded fixed-interval collection thread plus a small
  HTTP server exposing /metrics (Prometheus text 0.0.4) and /healthz;
- ``--once N``: run exactly N ticks at the configured interval and print one
  exposition to stdout; intended for root's on-node read-only smoke before any
  port/listener is opened.

Offline deliverable: nothing here has run against a real containerd socket.
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import __version__, cri_client
from .collector import (
    IdTarget, NameTarget, ResourceCollector, run_collection_loop)
from .prometheus_text import CONTENT_TYPE, render_exposition

DEFAULT_LISTEN = "0.0.0.0:9738"
DEFAULT_ENDPOINT = "unix:///run/containerd/containerd.sock"


def parse_target(spec: str):
    spec = spec.strip()
    if spec.startswith("id:"):
        spec = spec[3:].strip()
        if len(spec) != 64 or any(c not in "0123456789abcdef" for c in spec.lower()):
            raise ValueError("id: target must be a 64-hex container id: %r" % spec)
        return IdTarget(spec.lower())
    if spec.startswith("pod:"):
        parts = spec[4:].strip().split("/")
        if len(parts) != 3 or not all(parts):
            raise ValueError("pod: target must be pod:<namespace>/<pod-name>/<container-name>: %r" % spec)
        return NameTarget(namespace=parts[0], pod_name=parts[1], container_name=parts[2])
    raise ValueError("target must be id:<64-hex> or pod:<ns>/<pod>/<container>: %r" % spec)


class DuplicateTargetError(ValueError):
    """Two or more --target specs resolve to the same target key (OBS-1)."""


def build_targets(specs):
    """Parse target specs, refusing duplicates (OBS-1 guard).

    Targets are identified by their exposition key; two states sharing one
    key would emit two identical series per key (Prometheus rejects the whole
    scrape) while the collection path dedups by resolved container id,
    leaving the losing state to inflate ``cri_exporter_stale_targets`` even
    though collection succeeds. Duplicate specs carry no extra information
    (same key -> same series), so refuse them at startup instead of guessing
    which state keeps the data. Note the key is the id 12-hex prefix /
    pod triple: two DIFFERENT container ids can collide on that prefix - also
    refused, because the exposition cannot honestly represent both.
    """
    targets = [parse_target(spec) for spec in specs]
    seen = {}
    for spec, target in zip(specs, targets):
        if target.key in seen:
            raise DuplicateTargetError(
                "duplicate --target: %r and %r both identify %r; give each "
                "container exactly one --target (duplicates would emit "
                "duplicate series and inflate cri_exporter_stale_targets)"
                % (seen[target.key], spec, target.key))
        seen[target.key] = spec
    return targets


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cri_resource_exporter",
        description="Export source-timestamped per-container CRI resource observations.")
    parser.add_argument("--cri-endpoint", default=DEFAULT_ENDPOINT,
                        help="CRI socket endpoint (default unix:///run/containerd/containerd.sock)")
    parser.add_argument("--listen", default=DEFAULT_LISTEN,
                        help="host:port for /metrics and /healthz (server mode)")
    parser.add_argument("--interval", type=float, default=2.0,
                        help="fixed tick interval seconds (default 2.0)")
    parser.add_argument("--rpc-timeout", type=float, default=1.0,
                        help="per-RPC timeout seconds (default 1.0)")
    parser.add_argument("--max-sample-age", type=float, default=6.0,
                        help="withhold samples older than this many seconds (default 6.0)")
    parser.add_argument("--node", default=os.environ.get("NODE_NAME", ""),
                        help="node name label (required for full identity labels)")
    parser.add_argument("--node-uid", default=os.environ.get("NODE_UID", ""),
                        help="node uid label (required for full identity labels)")
    parser.add_argument("--resolve-every-ticks", type=int, default=1,
                        help="ListContainers resolution cadence for name targets (default every tick)")
    parser.add_argument("--concurrency", type=int, default=4,
                        help="bounded parallel ContainerStats calls per tick (default 4)")
    parser.add_argument("--target", action="append", required=True,
                        help="target spec, repeatable: id:<64-hex> | pod:<ns>/<pod>/<container>")
    parser.add_argument("--once", type=int, default=0, metavar="N",
                        help="run exactly N ticks and print one exposition to stdout (no HTTP server)")
    parser.add_argument("--version", action="version", version=__version__)
    return parser


def build_collector(args) -> ResourceCollector:
    targets = build_targets(args.target)
    return ResourceCollector(
        targets,
        node=args.node,
        node_uid=args.node_uid,
        interval_s=args.interval,
        rpc_timeout_s=args.rpc_timeout,
        max_sample_age_s=args.max_sample_age,
        resolve_every_ticks=args.resolve_every_ticks,
        concurrency=args.concurrency,
    )


def run_once(args, collector: ResourceCollector, client) -> int:
    for index in range(args.once):
        if index:
            time.sleep(args.interval)
        report = collector.tick(client)
        sys.stderr.write(
            "tick %d duration_s=%.3f resolve=%s outcomes=%s\n"
            % (report.tick_index, report.duration_s, report.resolve_notes,
               [(o["container_id"][:12], o["status"]) for o in report.stats_outcomes]))
    text = render_exposition(collector.snapshot(), time.time_ns())
    sys.stdout.write(text)
    sys.stdout.flush()
    return 0


def make_handler(collector: ResourceCollector):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            if self.path == "/metrics":
                body = render_exposition(collector.snapshot(), time.time_ns()).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif self.path == "/healthz":
                body = b"ok\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()

        def log_message(self, format, *log_args):
            # Bounded logging: one line per request at most; bodies never logged.
            sys.stderr.write("%s - %s\n" % (self.address_string(), format % log_args))

    return Handler


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        collector = build_collector(args)
    except DuplicateTargetError as exc:
        # OBS-1: refuse to start rather than emitting duplicate series or a
        # false cri_exporter_stale_targets. Exit code matches argparse's
        # usage-error convention (2): a configuration error, not a runtime one.
        sys.stderr.write("cri_resource_exporter: refusing to start: %s\n" % exc)
        return 2
    client = cri_client.GrpcCriClient(args.cri_endpoint,
                                      default_timeout_s=args.rpc_timeout)
    try:
        if args.once > 0:
            return run_once(args, collector, client)

        stop_event = threading.Event()
        loop = threading.Thread(
            target=run_collection_loop,
            args=(collector, client),
            kwargs={"interval_s": args.interval, "stop_fn": stop_event.set},
            name="cri-collection-loop", daemon=True)
        loop.start()

        host, _, port = args.listen.rpartition(":")
        server = ThreadingHTTPServer((host or "0.0.0.0", int(port)), make_handler(collector))

        def request_stop(signum, frame):
            stop_event.set()
            threading.Thread(target=server.shutdown, daemon=True).start()

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        sys.stderr.write("cri_resource_exporter %s listening on %s endpoint=%s interval=%ss\n"
                         % (__version__, args.listen, args.cri_endpoint, args.interval))
        try:
            server.serve_forever()
        finally:
            stop_event.set()
            loop.join(timeout=5)
            server.server_close()
            collector.close()
            client.close()
        return 0
    finally:
        if args.once > 0:
            collector.close()
            client.close()


if __name__ == "__main__":
    sys.exit(main())
