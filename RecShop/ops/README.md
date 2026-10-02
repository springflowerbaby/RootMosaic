# Observability Configuration

`docker-compose.otel.yml` provides the application OTel Collector, Jaeger, baseline Prometheus, Loki, and Grafana. Grafana data sources are provisioned through `grafana/provisioning/`. Anonymous access is disabled by default; administration ports are intended for trusted networks only.

Collection metrics use the separate Collector and Prometheus in [metrics/](metrics/README.md). The baseline instance on port 9090 and the collection instance on port 19090 serve different purposes: do not interchange their historical data sources or mount the same existing TSDB. The deployer selects images, networks, ports, and persistent volumes. This directory does not create a business database, download models, or install Chaos Mesh.

The local business launcher may reuse or start this baseline observability stack. Configure a new collection deployment separately as described in [Collection Environment Preparation](../docs/COLLECTION-ENVIRONMENT.md). Stopping containers and deleting volumes are different operations.
