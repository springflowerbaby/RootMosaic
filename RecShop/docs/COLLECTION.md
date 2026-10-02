# Versioned fault collection

This entry collects new runs. It does not contain a previous dataset, historical approval files, or a ready-to-run campaign for somebody else's machine.

## Before collection

Use Python 3.10 and install `requirements.txt`. Actual collection and fault recovery currently require a Windows control host; the application containers and Kubernetes worker nodes run Linux. This restriction does not prohibit help, offline preparation, status reading or data adaptation. Complete [collection environment preparation](COLLECTION-ENVIRONMENT.md). A business deployment being healthy is not sufficient: the fault engine, exact API carriers, observability stack, source interval and safety registration must also be ready.

Copy `configs/collection/environment.example.json` to a private configuration file and replace every placeholder using the actual environment. Paths inside that file are relative to the project unless absolute. Credentials belong in the separately named environment file, never in the JSON. Keep the private configuration and credentials out of version control.

The example intentionally fails validation until its identity and checksum fields are filled. `binding_mode: offline_fixture` is reserved for offline tests and is refused by live readers and credential loading. Setting `observed` does not prove readiness: subsequent readbacks must match the actual cluster, namespace, node, database and clean lease.

## Prepare a new campaign

The original 66 definitions and additional 3 definitions retain separate design versions. Prepare them separately and execute serially, using the same environment and raw output root. Together they cover 69 definitions; campaign count is not dataset count.

```powershell
python -B -X utf8 -m scripts.collection.prepare_campaign --environment configs/collection/environment.local.json --output-dir runs/collection/prepared/base --campaign-id base-run --rounds 1 --conditions-file configs/collection/campaign_conditions.json
python -B -X utf8 -m scripts.collection.gateway_cpu_network prepare --environment configs/collection/environment.local.json --output-dir runs/collection/prepared/gateway-cpu-network --campaign-id gateway-cpu-network-run --rounds 1
```

Preparation constructs contracts, source fingerprints and offline production previews. `OFFLINE_VALIDATED_NOT_EXECUTED` does not certify a new cluster or a sample. The prepared directory must be new and inside the project so its relative source references remain bounded. Runtime evidence/output roots can be configured separately; they must not overlap protected code, data or models.

The raw scenario IDs, fault instances, normalized entities, dose, three 300-second phases and 2-second configured metric interval are retained. Environment namespace, API proxy port, local paths and identity fingerprints are rebound explicitly. Collection uses the configured namespace with its bound UID; a new instance does not need a historical namespace name, and must not take over another instance's resources or lease. Selected saved carrier item IDs are scientific inputs: the corresponding real data/model vocabulary must be present. Do not substitute synthetic demo products and call that equivalent reproduction.

## Inspect or execute

```powershell
.\scripts\entrypoints\collect_dataset.ps1 -Python python -Plan runs/collection/prepared/base/campaign.json -Round 1 -Status
# Only after preparation and a fresh environment readiness check:
.\scripts\entrypoints\collect_dataset.ps1 -Python python -Plan runs/collection/prepared/base/campaign.json -Round 1 -Execute
```

The wrapper requires an explicit plan. It chooses the current regular or incremental entry from the design version. The plan contains the non-secret environment configuration path and hash; changed configuration or source requires a new preparation. It is not safe to edit hashes of an old plan until it passes.

The collector preserves same-attempt identity, operation receipts, actual stage windows, original QC, recovery records and bounded worker drain. It stops on unresolved safety, source or recovery state. `Resume` does not authorize taking over an in-flight or dirty attempt, and deleting lease files is never a recovery procedure.

## End-of-batch decisions and data adaptation

After the campaign emits its review template, copy it to a new decision file, retain manifest/attempt/repeat/raw hashes, and provide actual evidence paths plus hashes. Import it with `-DecisionFile`; do not hand-edit batch journals or mark old samples as new passes. Nonpassing QC and missing modalities remain visible even when a separately justified limited-use decision accepts the sample.

Completed-run inputs for the data adapter remain `SUMMARY.json`, adjacent `recovery-input.json`, the contract's `context.evidence_root`, phase bundles and a same-attempt acceptance ledger. A failed/replaced attempt is preserved but does not create an extra selected slot. See the data migration entry under `scripts/dataset` for the public input contract and delivery reader.

For the validation scope of this source distribution and target-environment checks, see [deployment validation](DEPLOYMENT.md#验证范围) and [environment preparation](COLLECTION-ENVIRONMENT.md).
