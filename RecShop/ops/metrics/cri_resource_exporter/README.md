# Collection CRI Resource Exporter

The exporter provides container resource observations for the collection metrics stack.
The current environment configuration expects the `m1-cri-resource-exporter`
Deployment and a `cri-resource` Prometheus job; see the [collection metrics overview](../README.md).
Existing deployment does not mean this component is installed on a new machine.
Verify the container runtime, socket, selected targets, namespace, and source
field behavior for each new environment.

The implementation reads per-container CRI `ContainerStats`, while the existing
cAdvisor job remains a separate observation source. A configured 2 s interval
is not a guarantee that every returned field contains a new source observation.

## What it does

- Resolves an explicit target list: `id:<64-hex container id>` or
  `pod:<namespace>/<pod-name>/<container-name>` (resolution via
  `ListContainers` metadata only, re-checked every tick by default).
  Two `--target` specs resolving to the same target key (duplicate spec,
  different id-hex spelling, or two distinct ids colliding on the 12-hex
  prefix) are REFUSED at startup: exit code 2 with a readable error.
- Each tick (fixed interval, default 2 s) issues exactly ONE
  `runtime.v1.RuntimeService/ContainerStats` call per resolved target,
  bounded in parallel (`--concurrency`, default 4). Slow calls may overrun a
  tick; the exporter records overruns and skips missed slots rather than
  promising a fresh observation for every configured interval.
- Exposes Prometheus text format on `/metrics`:

| metric | freshness | timestamp |
|---|---|---|
| `cri_container_cpu_usage_core_nanoseconds` | intended per-call CPU observation; check source time | CRI CPU source ts |
| `cri_container_memory_working_set_bytes` / `_usage_bytes` / `_rss_bytes` / `_available_bytes` / `_page_faults` / `_major_page_faults` | intended per-call memory observation; check source time | CRI memory source ts |
| `cri_container_cpu_usage_nano_cores_cached` | **cached; not guaranteed fresh** (statsCollector precomputed rate path); diagnostics only | CRI CPU source ts |
| `cri_container_writable_layer_used_bytes_cached` / `_inodes_used_cached` | **cached; not guaranteed fresh** (snapshot cache; check the separate writable-layer timestamp) | writable layer's own ts |

Container identity labels: `node`, `node_uid`, `namespace`, `pod`, `pod_uid`,
`container`, `container_id`, `attempt` (omitted when the source lacks them).
Exporter self metrics (`cri_exporter_*`): up/build/config echo, tick and RPC
counters by status, last durations, missed/overrun ticks, per-target identity
changes (always present, 0 until a change), stale targets, per-target source
age. Self metrics carry no explicit timestamp; container metrics always carry
the CRI source timestamp (nanoseconds -> milliseconds, round-half-up).

## Data handling

- No interpolation, no zero fill, no rate computation anywhere. A missing CRI
  field yields an absent series, not 0. A failed RPC keeps the last sample
  only until it exceeds `--max-sample-age` (default 6 s); then the series is
  withheld (goes stale in Prometheus) and `cri_exporter_stale_targets` rises.
- One RPC attempt per target per tick; timeouts/errors are classified
  (`timeout`, `not_found`, `unavailable`, `permission_denied`, `error`,
  `unparseable_stats`, `identity_mismatch`) and counted. No retries are made
  within a tick.
- Skipped tick slots after an overrun stay skipped (no catch-up burst).
- A payload whose `attributes.id` differs from the requested id is rejected
  (`identity_mismatch`), so a wrong-entity read can never be attributed to the
  target.
- Cumulative CPU decreases within one container id are counted
  (`cri_exporter_cpu_cumulative_decreases_total`) but the raw value is still
  exposed unchanged.
- Container replacement (new container id) increments
  `cri_exporter_target_identity_changes_total{target=...}` and starts a fresh
  sample history; the old container's last sample is never carried over. A
  change counts whether the replacement is observed directly or after a
  `no_match` gap; the same container id returning after a gap does not count;
  the first resolution is not a change. The series is emitted for EVERY
  configured target from process start (value 0 before any change), so the
  identity signal is observable without waiting for a change; the counter
  resets on exporter restart (target re-pointing restarts the process).

## Boundaries

- The containerd socket is mounted read-only, but a read-only **mount is not
  read-only API access**: holding the socket allows the full CRI surface
  (including container lifecycle mutations). This exporter only issues
  `ContainerStats` and `ListContainers`, and the deployment keeps the mount
  read-only to prevent socket replacement, not to restrict the API.
- `usageNanoCores` must never be consumed as a 2 s rate; compute rates (if
  needed) from adjacent real cumulative samples and their source-time delta.
- Prometheus with `honor_timestamps: true` will log duplicate/out-of-order
  sample drops when the same source timestamp is scraped twice while a new
  sample is late. Inspect source timestamps and actual gaps rather than treating
  every scrape as a fresh observation.

## Usage

Server mode (in the deployment image):

```
python -m cri_resource_exporter.exporter \
  --cri-endpoint unix:///run/containerd/containerd.sock \
  --listen 0.0.0.0:9738 --interval 2 --rpc-timeout 1 \
  --node <node-name> --node-uid <node-uid> \
  --target pod:<namespace>/<pod-name>/<container-name> \
  --target id:<64-hex-container-id>
```

On-node read-only smoke from the repository root, without opening a listener
(prints one exposition after N ticks to stdout):

```
python -m ops.metrics.cri_resource_exporter.exporter --once 3 --interval 2 \
  --node <node-name> --target id:<64-hex-container-id>
```

The release includes the ContainerStats adapter, not historical live-validation records.
A new runtime must independently check field mapping, target identity and source timestamps.

## gRPC plumbing note

`cri_proto.py` builds a minimal `runtime.v1` descriptor subset at runtime
(field numbers AND wire types mirrored from kubernetes/cri-api
`runtime.proto` v1, including the varint enum `Container.state` and the
`ContainerStateValue` filter wrapper and the enum-varint
`ListContainers` response), so the image needs only `grpcio` + `protobuf`.
For a new runtime, cross-check the CRI field mapping and timestamps against
`crictl stats` before relying on the output.

## Image build and offline path

All intra-package imports are RELATIVE (`from . import ...`); the package
works both as `ops.metrics.cri_resource_exporter` (repo/tests) and as
`cri_resource_exporter` (image, `PYTHONPATH=/app`, flat `COPY *.py` layout).
Always invoke it as a module (`python -m ...exporter`), never as a loose
script — relative imports require package context.

The delivered Dockerfile uses online `pip install`. For an offline build, in a temporary build context (do
not commit wheels into this directory) add a `wheels/` directory obtained on
the host with

```
pip download grpcio==1.78.0 protobuf==6.33.6 typing_extensions \
    --platform manylinux_2_17_x86_64 --only-binary=:all: -d wheels
```

copy it into the context, and change the RUN line to
`pip install --no-index --find-links=/tmp/wheels -r ...` (COPY wheels/ to
/tmp/wheels first). Keep such a modified Dockerfile in the temporary build
directory only; the delivered file stays online-pip.
