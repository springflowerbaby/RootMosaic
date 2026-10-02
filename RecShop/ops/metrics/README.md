# Collection Metrics and Resource Observation

This directory contains the active metrics pipeline and maintenance modules; its presence does not establish successful deployment on a new machine. Application metrics pass through a separate metrics-only Collector. Prometheus scrapes four source types: application OTel metrics, cAdvisor, kube-state-metrics, and the CRI resource exporter.

| File | Purpose |
|---|---|
| docker-compose.metrics.yml | Separate Collector/Prometheus, using the existing `ops_default` network and an explicitly selected new TSDB volume |
| otel-metrics-config.yaml | Metrics-only export pipeline that preserves source timestamps |
| prometheus-container-resources.yml | Four-target template; replace the node, namespace, and proxy addresses |
| docker-compose.container-resources.yml | Mounts the four-target configuration into Prometheus; the base Compose file alone does not enable the CRI target |
| prometheus.yml / container-resources-job.yml | Base three-target configuration and a separate CRI fragment; neither is the complete four-target configuration |
| cri_resource_exporter/ | Source code, Dockerfile, and deployment example for the containerd CRI resource exporter |
| maintenance_receipt.py / rollout_sampling.py | Sampling-configuration maintenance with ownership and recovery receipts |
| prepare_sampling_overlays.py / Dockerfile.metric-overlay / verify_base_source.py | Optional overlay builds for verified base images; the current source already supports sampling environment variables |
| patch_metric_sources.py | Compatibility conversion only for the explicitly recognized original 15000ms source pattern; do not patch already integrated source again |

Compose requires explicit values for `M1_OTLP_METRICS_GRPC_PORT`, `M1_OTEL_PROMETHEUS_PORT`, `M1_PROMETHEUS_PORT`, and `M1_PROMETHEUS_VOLUME`; common port choices are 14317/18899/19090. The volume name must belong to the new instance; do not reuse an existing volume of unknown provenance. The deployer must explicitly select `ops_default` as the baseline observability network.

Example command (run only after verifying the images, ports, network, and volume):

```powershell
docker compose -f ops/metrics/docker-compose.metrics.yml -f ops/metrics/docker-compose.container-resources.yml up -d
```

The template values `recshop-example` and `replace-with-node-name` must match the actual configuration derived from `configs/collection/environment.example.json`. The address `host.docker.internal:8001` applies only to deployments with that host-proxy route. Set the application metric export interval to 2000ms and ensure that the metric-specific endpoint is reachable. A 2-second query/scrape configuration does not guarantee a new observation from every source every 2 seconds. Do not interpolate or fill missing values with zero.

See [COLLECTION-ENVIRONMENT](../../docs/COLLECTION-ENVIRONMENT.md) for new-environment preparation. After installing the gateway, Chaos Mesh, CRI exporter, and observability pipeline, bind the actual UIDs, database identity, and new data baseline. Business-environment readiness does not replace these steps.

The `M1_*` environment variables, `m1-*` container/resource names, and image tags retain existing deployment compatibility identifiers; public scripts and directories are named by function. Do not change physical resource identities merely to rename them, or reuse data volumes of unknown provenance.
