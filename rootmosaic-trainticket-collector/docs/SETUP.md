# Experiment environment

Run commands from Windows PowerShell at the repository root. This package
provides configuration files; it does not provision a Kubernetes cluster or
install Chaos Mesh automatically.

## 1. Check the selected cluster

```powershell
kubectl config current-context
kubectl get nodes
kubectl get crd networkchaos.chaos-mesh.org podchaos.chaos-mesh.org
```

Install Chaos Mesh for your Kubernetes version and container runtime if the
CRDs or daemon are absent. The source workspace did not provide a pinned
cluster/Chaos Mesh installation specification, so those versions are not
claimed as reproduced by this package.

## 2. Deploy the observed TrainTicket subset

```powershell
kubectl apply -n trainticket -f deployment/trainticket.yaml
kubectl get deployments -n trainticket
```

The manifest begins with the namespace resource and contains the selected
deployments and services. It retains the source image tags and resource limits;
MongoDB is pinned to `mongo:4.4`. It uses `IfNotPresent` image pull policies.
Registry access, image availability, architecture, and sufficient cluster
resources must be checked in your environment. Images are not included in the
GitHub archive and have not been rebuilt or fetched during packaging.

The UI container must allow the collector to copy and reload
`/usr/local/openresty/nginx/conf/nginx.conf`. The original UI image is retained;
the collector installs its baseline/fault proxy configuration for UI fault runs.

## 3. Deploy monitoring

```powershell
kubectl apply -f deployment/monitoring.yaml
kubectl rollout status deployment/rca-prometheus -n monitoring --timeout=300s
kubectl rollout status deployment/kube-state-metrics -n monitoring --timeout=300s
kubectl rollout status deployment/jaeger -n trainticket --timeout=300s
```

The collector starts hidden `kubectl.exe port-forward` processes for:

| Local endpoint | Kubernetes service |
| --- | --- |
| `http://127.0.0.1:19090` | `monitoring/rca-prometheus:9090` |
| `http://127.0.0.1:16686` | `trainticket/jaeger-query:16686` |

An existing listener is reused; ensure these ports belong to the intended
services. Port-forward processes remain active after a run. Stop your own
forwarding processes when finished. The namespace names above are the profile
supported by the batch runner.

## 4. Check workload and injection prerequisites

```powershell
kubectl exec -n trainticket deploy/ts-travel2-service -- sh -c 'command -v wget'
kubectl exec -n trainticket deploy/ts-train-service -- sh -c 'command -v perl'
```

Other targets of `memory` plans also require `perl`; CPU injection requires a
shell. Check the targets in `workspace/trainticket/manifests/collection-plan.json`.
The contacts workload signs JWTs for the configured TrainTicket test user
(`fdse_microservice`, role `ROLE_USER`); set `TRAINTICKET_JWT_SECRET` to the key
accepted by your deployed service. The topology records the test user's ID.

Allow service startup/data initialization to finish. Run B01 once and inspect
its quality report before launching fault scenarios. Readiness alone does not
guarantee healthy workload responses, complete tracing, or Prometheus coverage.

## Validation boundary

The distribution is a source extraction of the historical collection workflow.
Offline package checks are separate from live environment validation. No
cluster deployment or fault experiment was run to prepare this archive.
