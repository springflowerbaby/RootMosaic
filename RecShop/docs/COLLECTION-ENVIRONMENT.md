# Preparing a collection environment

These are explicit preparation steps for a dedicated research environment. They are not an automatic fault deployment command, and the portable business installer intentionally reports `collection_ready=false`. Actual collection and fault recovery use a Windows control host; Linux application containers and Kubernetes nodes are separate from that host requirement. Tool discovery uses PATH first and the supported Docker Desktop bundled CLI location second; see [environment support](../ENVIRONMENT.md).

## 1. Business and carrier assets

Deploy the 25 application services using your own image references and model/data assets. Preserve the application's service names, API routes, probe behavior and fault hooks. Real scientific carrier item/order IDs and SASRec vocabulary must match the collection conditions; the business installer's small synthetic demo data does not establish this match.

If your business deployment has its own `recshop-mysql` Deployment in the same namespace, list it explicitly in `additional_deployments`. The asset-loader Pod must already be gone; arbitrary extra Pods are not accepted. Expose that database to the collection host through an explicitly managed endpoint/port-forward and bind its actual UUID/checksums; do not reuse another host's baselines.

Keep `NACOS_ENABLED=false` for fixed experimental routing. Pricing's baseline is `CATALOG_SERVICE_URL=http://catalog:5005`, one replica and no experiment owner. Relevant campaigns configure pricing→catalog-gw before measurement and restore direct routing only after all three measurement phases and worker drain.

### Host-pressure carrier image

Host-pressure scenarios use `recweb-user:latest` as a separate carrier dependency. Changing the business deployment's user-service image does not change this tag. From the repository root, prepare its existing base and service Dockerfiles:

```powershell
docker build -f docker/services.Dockerfile -t recweb-base:latest .
docker build -f services/user_service/Dockerfile -t recweb-user:latest .
```

These are explicit image-preparation commands; the collection entry does not build or pull images on your behalf. The user-service Dockerfile inherits `recweb-base:latest`; the carrier overrides the normal app command with `sleep infinity` and retains the existing fault/cleanup behavior. Do not replace it with an arbitrary image or add a different stress program.

Make that exact image tag available to the selected Kubernetes node's container runtime. If the cluster uses a separate image store, export the built image and use that cluster's documented node-image import procedure, preserving `recweb-user:latest`; merely loading it into the control host's Docker store is insufficient. For example, after choosing a new archive path, `docker save recweb-user:latest -o <archive.tar>` produces an archive for such an import, not proof that the node can run it. A local image check alone cannot certify node availability or establish that a remote node cannot pull it. When authoritative node evidence is unavailable, availability remains `UNKNOWN`, not `PASS` or an extra manual approval gate. The existing owned-carrier rollout/readback still determines actual readiness.

## 2. Fault and observation components

Install a compatible Chaos Mesh controller/daemon stack separately in the configured `chaos_namespace`. The collector consumes NetworkChaos, StressChaos and PodChaos; controller readiness is checked, and existing fault resources or stressor residues block a new attempt.

Prepare the baseline catalog-gw Deployment/Service/ConfigMap and kube-state-metrics from the provided Kubernetes templates, replacing the namespace explicitly. The baseline Nginx route is `catalog:5005` with 8-second timeout settings. Do not deploy historical fault-config examples as the baseline.

Prepare the CRI resource exporter using `ops/metrics/cri_resource_exporter`. Bind it to the actual node and containerd socket and configure explicit container targets. The exporter reads source timestamps; its cached fields are not fresh 2-second measurements. Holding a containerd socket is privileged access even if the mount is read-only. Run it only in the dedicated environment.

For a new installation, prepare the trace/log stack from `ops/docker-compose.otel.yml` and `ops/otel-config.yaml` first, including its `ops_default` network. Then use the separate metrics Compose file **with** `ops/metrics/docker-compose.container-resources.yml`; that overlay selects `ops/metrics/prometheus-container-resources.yml` with all four required jobs. The metrics receiver configuration is `ops/metrics/otel-metrics-config.yaml`.

After configuring the images, ports, network and a new explicit TSDB volume, the new-installation order is:

```powershell
docker compose -f ops/docker-compose.otel.yml up -d
docker compose -f ops/metrics/docker-compose.metrics.yml -f ops/metrics/docker-compose.container-resources.yml up -d
```

An existing environment should retain its containers and data volumes rather than rerun these deployment commands to match a moved source tree. Its actual bind sources can be declared as described below; never remount or recreate stored telemetry just to satisfy a path check.

The origins read directly by the collector—`telemetry.prometheus`, `telemetry.jaeger` and `telemetry.metrics_exporter`—must be HTTP literal-IP origins, such as `http://127.0.0.1:19090`, without credentials, a path other than `/`, query or fragment. DNS hostnames and HTTPS for those three fields are rejected during configuration validation, before live changes. This restriction does not apply to `metrics_endpoint_for_apps`, `metrics_scrape_url` or the Prometheus target URLs: their application/container/Service DNS names remain valid. Loki uses a different reader and is not subject to this literal-IP rule.

The metrics Collector must expose its configuration at the fixed **container-internal** path `/etc/m1/otel-metrics-config.yaml`; `telemetry.metrics_collector_config_path` is validated against that layout. This is independent of the host file's location, which may be declared through `windows_startup.bind_sources` below.

For every application, set `OTEL_METRIC_EXPORT_INTERVAL=2000` and `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` to the environment's metrics receiver. The service image must actually use the interval helper; an environment variable alone does not prove image contents changed. The metric overlay tools in `ops/metrics` retain bounded rollout/source-check support. Their rendered image references and identities need your own build records.

Prometheus must have exactly one healthy target for each required job: `otel-collector`, `cadvisor`, `kube-state-metrics`, `cri-resource`, each configured for a 2-second scrape. Update node, namespace and host proxy URLs in the configuration, and copy the exact URLs into the environment JSON. API proxy paths and Docker host addresses depend on deployment; they are not universal defaults.

The formal collector archives important Pod logs directly through UID/container-bound Kubernetes reads. Loki is a platform log backend and possible supplemental source, not the mandatory origin of every collected raw log. The Nginx gateway has no independently guaranteed trace span.

## 3. Bind a real environment

Copy `configs/collection/environment.example.json` to a private `*.local.json` file, such as `configs/collection/environment.local.json`. Obtain the Kubernetes context, namespace, node name, kube-system UID, namespace UID and node UID from the actual environment; example placeholders and another machine's identities are not usable bindings. Fill the MySQL server UUID and observed `items`/`inventory` checksums. Read lock state before calculating checksums: a checksum query while a table lock is held can block. Credentials stay in the separately configured `.env.collection` file with `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`.

Configure independent attempt, batch, artifact and lease roots plus protected data/model roots. Code/configuration paths resolve relative to this project; external assets and output roots can be explicit absolute paths. The process-wide machine/user lease registry binds a physical cluster/namespace identity to a single lease root and prevents switching output paths to escape dirty state.

For a **new physical environment**, choose its own roots and perform first registration only after preparation. For an **already registered environment**, retain the registered `paths.lease_root` and the existing `paths.artifact_root` that contains its worker/drain records. Moving the code directory is not a reason to create a second lease or point the worker checks at an empty directory. Do not run first registration against an existing binding or delete its state to make it appear new. Keep the real configuration private; secret values remain in the separately referenced credential file.

```powershell
# Pure local JSON/schema validation; no credentials or network are read:
python -B -X utf8 -m scripts.collection.environment --environment configs/collection/environment.local.json

# First registration: read-only site checks, then a new local binding/clean state.
# This refuses any existing registration and never resets a dirty environment.
.\start_collection_environment.ps1 -Environment configs/collection/environment.local.json -CheckOnly -RegisterReadOnly

# Subsequent read-only readiness snapshot:
.\start_collection_environment.ps1 -Environment configs/collection/environment.local.json -CheckOnly
```

Registration holds the physical environment lock while checking the site. It requires no active collector, no fault/stressor residue, ready components, stable direct pricing routing and unchanged database baselines. It writes only new local registration files. It is not a way to erase an earlier recovery failure. A crash leaving an incomplete registration remains blocked for explicit examination.

## 4. Optional Windows startup recovery

`start_collection_environment.ps1 -Environment ...` can restore an already deployed Windows Docker Desktop setup only when `windows_startup.enabled=true`. Its exact existing-container identities, network, TSDB volume and proxy settings must match your setup. The known Docker socket repair remains a bounded, exact-match recovery for identified failed Docker instances; unknown objects/processes or a failed repair stop the operation. It does not reset Docker, remove database volumes, initialize a cluster or run collection.

### Existing Docker bind sources

The optional `windows_startup.bind_sources` object maps a checked container name to its exact existing Docker `Mounts[].Source`. Omit it, or leave it as `{}`, to use the current public directory layout for every mount; omitted keys retain their default layout checks. Explicit entries are useful when containers still use configuration files outside the current code directory.

For example, merge the following **fictional paths** into your private environment configuration, replacing both values with the actual corresponding `Source` strings reported by `docker inspect`:

```json
{
  "windows_startup": {
    "bind_sources": {
      "recshop-m1-otel-metrics": "C:/example/observability/otel-metrics-config.yaml",
      "recshop-m1-prometheus": "/run/desktop/mnt/host/c/example/observability/prometheus-container-resources.yml"
    }
  }
}
```

Docker may report a Windows drive path, a UNC path, or a Unix-style path such as `/run/desktop/...`. Copy the actual absolute `Source` value; do not translate it to a guessed repository path. The other supported bind-source keys are `recweb2-otel-collector` and `recweb2-loki`. This option describes existing mounts and does not change them. The fixed destination, bind type, read-only attribute, container identity, network and TSDB-volume checks remain in force.

For an already configured Windows environment, use the public entry from the project root:

```powershell
.\start_collection_environment.ps1 -Environment configs/collection/environment.local.json -CheckOnly
.\start_collection_environment.ps1 -Environment configs/collection/environment.local.json
```

The second command requires `windows_startup.enabled=true`; it restores existing components and does not perform first installation or collection.

The default example disables this optional startup recovery. Infrastructure may be managed separately and inspected with `--check-only`; that does not extend actual collection or fault recovery to Linux/macOS control hosts. Source qualification still requires the expected Docker-hosted metrics receiver because it inspects that receiver's configuration.

`READY` records the environment state at the time of the check. Each attempt repeats its safety and source checks. Initial deployment validation covers image/model identity, carrier-data compatibility and an actual collection run; see [validation scope](DEPLOYMENT.md#验证范围).
