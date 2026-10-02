# Kubernetes Source Templates

Use the root `deploy_recshop.ps1` to create an isolated business instance, and `scripts/entrypoints/start_recshop.py` and `scripts/entrypoints/check_recshop.py` to restore and check it. The deployment tool reads the `services/` templates through an allowlist of 25 applications and rewrites the namespace, images, database settings, Secret references, and asset PVCs. Do not recursively apply this entire directory.

`services/` contains source templates named by service. The values `recshop-example`, `mysql.example.internal`, and `replace-with-node-name` are explicit example placeholders, not default identities for an existing machine. Select resources, images, and scheduling constraints for the target environment.

- The 25 application Deployment/Service templates retain their service and probe structure; the deployment tool renders them from configuration.
- `catalog-gw.yaml` contains only the normal 8-second proxy configuration, read-only runtime mounts, and the Deployment/Service. The current collection primitives generate fault configurations from the contract.
- `kube-state-metrics.yaml` provides the state-metrics component with a ClusterIP Service and a namespace-bound ServiceAccount. Use distinct ClusterRole/Binding names when running multiple instances.
- The CRI resource exporter template is at `ops/metrics/cri_resource_exporter/deployment.example.yaml`.

Business deployment does not install all experimental infrastructure. See [COLLECTION-ENVIRONMENT](../docs/COLLECTION-ENVIRONMENT.md) for collection preparation and readiness checks, and [DEPLOYMENT](../docs/DEPLOYMENT.md) for business deployment.
