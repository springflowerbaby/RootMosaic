# Collection protocol and interpretation

## What is preserved

The collector runs three stages, exports per-component logs and Jaeger spans,
and combines workload records with Prometheus infrastructure metrics. The
Prometheus sources include kube-state-metrics and kubelet/cAdvisor; application
`/metrics` coverage is not claimed. Stage labels and recorded timestamps define
the actual observation windows.

The plan contains eight injection modes: `cpu`, `memory`, `network_delay`,
`packet_loss`, `pod_kill`, `pod_failure`, `nginx_timeout`, and
`nginx_retry_disabled`. The underlying collector also implements `pause`, which
is not selected in the supplied 55-scenario plan. Composed faults are applied
sequentially with brief delays and then observed together; they are recovered
in reverse order. This is not simultaneous atomic injection.

Default collection parameters retained from the source:

| Parameter | Default |
| --- | --- |
| Stage duration | 300 seconds |
| Poll delay | 2 seconds |
| Network delay / jitter | 700 / 0 milliseconds |
| Packet loss | 40 percent |
| Memory allocation | 160 MiB |
| Chaos duration | max(30, StageSeconds - 20) seconds |

The supplied batch runner uses these collector defaults. Injection and recovery
timestamps, exit codes, and concrete fault parameters are saved under
`raw/operations/`. Some faults expire before the observation stage ends; use
the recorded windows when aligning data.

## Gates

Collection checks deployment readiness, traffic windows, Prometheus presence,
expected trace services, log-file completeness, and injection/recovery outcomes.
The per-root auditor additionally examines observed endpoint or infrastructure
changes, with special evidence for restart/pod replacement. The runner copies
accepted runs into the local dataset and writes `validation.json` and the index.

The checks retain their source definitions. For example, the Prometheus gate
requires nonempty exported records and records missing services separately;
it is not proof of complete per-service coverage. The
`metadata_schema_valid` field in the historical validation summary is a fixed
flag, not an independent JSON Schema validation. Review the actual reports
before interpreting a passed gate as a stronger guarantee.

Signal thresholds are implementation-level collection checks, not evidence of
a causal fault-interaction mechanism. Canonical label adjudication and final
dataset packaging are separate stages and are not performed by these scripts.

## Comparing single and composed faults

The collector adds `ui-proxy-travel` to the workload for a plan targeting the UI
dashboard. Accordingly, different scenarios can use different workload sets.
Do not assume a matching entity/fault name automatically makes a valid paired
comparison. Check workload, injection mechanism/parameters, stage windows, and
replicate metadata in the dataset. Match and filter pairs explicitly for RQ4.

## Recovery after a failed collection

The historical collector does not guarantee fault cleanup if interrupted or
if an exception occurs partway through a stage. Do not immediately restart the
batch after an error. Inspect the failed run's `raw/operations/` and cluster
state first:

```powershell
kubectl get networkchaos,podchaos -n trainticket
kubectl get pods -n trainticket
```

Remove only the specific Chaos objects created by that run, recover its target
processes/configuration, and verify healthy baseline responses before resuming.
CPU and memory injections save their process IDs under `/tmp/multirca-*-stress.pid`
inside the affected container. The exact recovery commands are in
`Stop-SingleFault` in the collector. Do not run two collectors against the same
namespace concurrently.
