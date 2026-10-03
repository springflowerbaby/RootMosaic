# RootMosaic: TrainTicket Data Collection

[中文说明](README.zh-CN.md)

Code for collecting the TrainTicket (RM-TT) portion of RootMosaic. The collector
injects single and composed faults into a Kubernetes deployment and records
metrics, logs, and traces before, during, and after each injection.

**Dataset:** distributed separately on Hugging Face. The dataset URL will be
added when the dataset repository is available. This repository contains no
collected telemetry, model checkpoints, or evaluation results.

## Scope

This package contains the **v3.1 expanded-topology collection workflow**:

| Scenario group | Scenarios | Default repeats | Runs |
| --- | ---: | ---: | ---: |
| No-fault baseline | 5 | 5 | 25 |
| Single fault | 18 | 5 | 90 |
| Two faults | 24 | 5 | 120 |
| Three faults | 8 | 5 | 40 |
| Total | 55 | | 275 |

The collection inventory includes 25 baseline runs in addition to 250 fault
runs. These counts describe the collection plan, not a claim that rerunning it
will reproduce the published measurements. Historical injection descriptions
and roles are retained; final adjudicated labels belong to the dataset release.

The topology has 21 observed components: 12 UI/business services and 9 databases.
Jaeger and the monitoring stack are infrastructure outside that count.

## Requirements

- A **Windows** collection host with Windows PowerShell 5.1 and `powershell.exe`
  on PATH. The collector uses `Get-NetTCPConnection` and Windows process options;
  Linux/macOS execution is not currently supported.
- Python 3.8+ on PATH for root-signal auditing; only the standard library is used.
- `kubectl.exe`, configured for a dedicated experiment cluster.
- TrainTicket in namespace `trainticket`, with the bundled 21-component profile.
- Chaos Mesh installed with `NetworkChaos` and `PodChaos` CRDs and a working
  chaos daemon compatible with the cluster's container runtime.
- Prometheus and kube-state-metrics in namespace `monitoring`; Jaeger in
  namespace `trainticket`. See [environment setup](docs/SETUP.md).

The workload executes `wget` inside `ts-travel2-service`. Fault targets must
provide a POSIX shell; memory injection additionally needs `perl`. Collection
changes deployment state, sends process signals, and applies network/pod faults.
Use an experiment cluster and run one collection process at a time.

## Quick start

Open **Windows PowerShell at this repository's root**. First inspect the plan;
this needs no cluster, credentials, or dataset download and writes no run data:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass
$runner = '.\workspace\trainticket\scripts\rebuild-v3-main-mr1-replicates.ps1'
& $runner -PlanOnly
& $runner -Slots S01,D01,T01 -Replicates r1 -PlanOnly
```

The first command opens a PowerShell session with a process-local script policy;
it does not change the machine's saved execution policy. The automated checks
use the same process-local option for their offline child processes.

After completing [setup](docs/SETUP.md), provide the JWT signing key configured
in your TrainTicket deployment. It is read from the environment, not a tracked
configuration file:

```powershell
$env:TRAINTICKET_JWT_SECRET = '<your benchmark deployment signing key>'
```

Start with a no-fault baseline and examine its quality report:

```powershell
& $runner -Slots B01 -Replicates r1
```

Then collect one single-fault scenario or a chosen set:

```powershell
& $runner -Slots S02 -Replicates r1
& $runner -Slots S01,D01,T01 -Replicates r1,r2,r3,r4,r5
```

Collect the complete inventory:

```powershell
& $runner
```

Defaults are 300 seconds per stage and a 2-second polling delay. Each iteration
executes multiple probes sequentially, so actual sampling intervals and stage
durations include command/probe overhead. Three stages per run imply at least
68.75 hours for 275 runs, excluding overhead, recovery, and retries. Shorter
windows are useful for debugging but change the collection protocol:

```powershell
& $runner -Slots B01 -Replicates r1 -StageSeconds 60 -PollSeconds 2
```

The runner stops when collection or a release gate fails. It skips a scenario/
replicate already recorded in the local output index. `-Initialize` is optional
and refuses to reset a nonempty index. Use `-DatasetDir` to choose a new local
dataset destination. Raw staging runs still use `workspace/trainticket/runs/`.

In this code, **publish means copy a passed run into the local dataset layout**.
No command uploads to GitHub or Hugging Face.

## Outputs and validation

```text
workspace/trainticket/runs/<run-id>/
  metadata.json
  groundtruth.json                 # fault runs
  baseline_quality.json
  summary.md
  raw/metrics/metrics_v2.jsonl
  raw/metrics/manifest.json
  raw/logs/<stage>__<component>.log
  raw/logs/manifest.json
  raw/traces/<stage>_traces.jsonl
  raw/traces/manifest.json
  raw/operations/
  scripts/collect-v3-expanded-baseline.ps1

workspace/trainticket/datasets/k8s-v3.1-main/
  index.jsonl
  _manifests/trainticket_mr1_rebuild_status.json
  MR1/<scenario-group>/<slot>/<replicate>/<run-id>/
    validation.json
    ...copied run artifacts...
```

Stages are `pre_fault`, `during_fault`, and `post_recovery`. Fault plans,
ground-truth labels, and injection operation records must not be treated as
model input telemetry. See [protocol and limits](docs/COLLECTION.md) for metric
sources, readiness gates, pairing caveats, and failure recovery.

Audit an existing fault run separately:

```powershell
python tools/audit_root_signals.py --case-dir '<path to a fault run>'
```

This writes JSON and Markdown reports under `reports/`. A successful auditor
process means a report was generated; inspect `summary.fault_status` and
`needs_review`. The batch runner treats any `needs_review` root as a failed gate.

## Repository checks

```powershell
python -m unittest discover -s tests -v
```

Checks cover PowerShell parsing, plan consistency, safe plan-only operation, and
unknown-slot rejection. They do not inject faults or contact a cluster.

## Provenance and licensing

The original collection files and their SHA-256 hashes are listed in
[SOURCE_PROVENANCE.json](SOURCE_PROVENANCE.json). The release changes portable
configuration and packaging behavior; it retains the collector's injection
implementations and measurement/quality thresholds.

TrainTicket-derived deployment manifests retain the upstream Apache-2.0 license
in [third_party/TrainTicket-LICENSE](third_party/TrainTicket-LICENSE). See
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). A license for the original
RootMosaic collection scripts has not yet been selected by their authors;
the third-party license does not automatically license those scripts.
