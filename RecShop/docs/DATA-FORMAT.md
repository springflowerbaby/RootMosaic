# Dataset native inputs and V1 delivery format

## Identity and input data

A slot is `design_version + scenario_id + repeat_number`. Its one selected run is
`attempt_id`. Duplicate selected slots and another attempt's evidence are rejected;
failed history is not silently counted as an additional sample.

An accepted JSON document has `rows`. Each row supplies `scenario_id`, `attempt_id`,
`decision` and `evidence_refs`; include `design_version` and an unambiguous
`round`/`repeat_number`. A document may provide a common `round` or `logical_round`;
all supplied declarations must agree. Supported decisions are
`ACCEPTED_MINIMUM_COLLECTION` and `ACCEPT_MINIMUM_COLLECTION`. Exactly one evidence
reference gives the SUMMARY path and `sha256`. Optional recovery/contract
references must bind the same executed input. The selection record must come from
the completed run's acceptance decision.

```text
source-root/
  accepted-ledger.json
  runs/<run>/SUMMARY.json
  runs/<run>/recovery-input.json   # executed contract
  native/<attempt>/contract.json  # original contract
  native/<attempt>/artifacts/
    operations.json
    annotation-draft.json
    quality/result.json
    <pre_fault|during_fault|post_recovery>/
      metrics/bundle.json, projection.json
      traces/bundle.json, projection.json
      logs/bundle.json, projection.json
      workload.json, phase-observations.json
```

The native location comes from `context.evidence_root`, not a guessed directory.
Original and executed contracts stay distinct. Bundles bind attempt, run, phase
and actual epoch windows. Historical cache inputs contain a frozen plan,
per-case resource/HTTP matrices, source/query receipts and a manifest. These are
external data inputs, not source templates. The
[synthetic example](../examples/migration/README.md) demonstrates the shape only.

Each prepared case carries a versioned `consumed_inputs` manifest. Its 30
explicit entries cover the converter's actual
source read/copy set and record relative basis, resolved path, presence, size and
SHA256. Missing files are recorded as absent; scientific-QC results are preserved
separately. Cache manifests bind both response and receipt hashes.
Conversion uses the verified case snapshot, while package `audit/` retains the
same original bytes. Re-prepare older unbound plans rather than modifying them.

## Delivery layout

```text
delivery/
  MANIFEST.json, RELEASE.json, DELIVERY-VALIDATION.json, SHA256SUMS
  README.md, STATUS-GUIDE.md
  adapter/                       # converter source snapshot
  traditional/<single|dual|triple>/<sample-id>/
    metadata.json, groundtruth.json, migration.json
    raw/metrics/, raw/logs/, raw/traces/, raw/traces_calltree/
    raw/operations/
    audit/                       # original summary, QC, annotation and ledger
    scripts/                     # recorded contracts/profile, not a launcher
    eval/                        # observation views, not algorithm results
```

`single/dual/triple` and `mr<G>` use distinct normalized root entities, not scenario
letters. Fault instances remain separate: multiple instances can in general map
to one entity. GT comes from the executed contract and must match SUMMARY.

## Conversion rules

| Output | Meaning |
|---|---|
| Stages | Same-attempt bundle windows in UTC, half-open `[start,end)`; configured metric interval preserved. |
| Fault times | Successful inject/recover operation return times, not exact physical effect boundaries; planned windows remain separate. |
| Metric long table | 16 explicit fields; original labels, timestamps and response matrices are retained with the derived view. |
| Resource rates | Adjacent counter delta / actual elapsed time; resets, nonfinite values and gaps over 60 seconds become missing, never zero. |
| HTTP values | Pair original sum/count labels and aggregate increments into rates and weighted mean latency. Exact health/metrics/ready/live routes are excluded from the default derived view. |
| Service identity | Original Pod→ReplicaSet→Deployment data, preserving different generations. Unknown ownership is not guessed from GT. |
| Units/source | Metric-specific units; compatibility `source=prometheus` retains actual source in `labels.source_raw`. Observed, derived and missing remain distinct. |
| Traces | 18-field full call-tree view preserves spans/parents. Separate flat compatibility view keeps one root/earliest span per trace and is not a complete graph. |
| Logs | Capture source ID/raw reference resolve the producer; an associated root entity is not the producer. Preserve timestamp prefix and message. |
| Audit | Preserve original QC, SUMMARY, annotation and accepted ledger byte-for-byte; unprovided role/path/interaction labels retain their missing state. |

Default `eval/` uses the 26 application/gateway candidates in `metric_views.py`,
pre-fault/during-fault observations only, and excludes GT, scenario-selected
native probes, injection parameters and recovery observations. `data_missing.csv`
retains missing cells on a 2-second alignment grid. Compatibility `data.csv` fills
within each phase up to 60 seconds forward and only the leading edge backward;
remaining missing columns are omitted and listed. No zero-fill or cross-stage
fill occurs. Alignment at 2 seconds does not prove independent source values at
that cadence. `inject_time.txt` is the during observation boundary.

`host` and `mysql_items_lock` remain GT when present but are outside the default
26-service view. A method must explicitly state its applicable scope. The
converter never changes a root to make a method applicable.

## Delivery status and data availability

After identity, readback, count and hash checks, the package sets
`sample_status=accepted`, `ready_for_release=true`, `validation_complete=true`,
`validation_scope=delivery_v1`. These fields describe delivery validation.
Original per-run QC, annotations and selection evidence remain in `audit/`.
Downstream analyses use those records and observed coverage to determine sample
applicability, including eligibility for matched experimental comparisons.

`available` means nonempty observations; `partial` indicates limited or
unconfirmed coverage; `missing` indicates no available observations. Unprovided
labels use `not_provided` or `null`.

Reader references are package-relative. Original absolute paths may remain as
provenance inside immutable copied evidence but are not dereferenced by the
package reader. Publishing experimental data requires a separate privacy/license
review and versioned export, not changes to original evidence or hashes.

## Compatibility identifiers

Some serialized identifiers retain historical names, including
`m1-consumed-inputs-v1`, `m1-delivery-v1`, `m1-history-export-v2-bound-inputs`,
the `m1-strict255-adapter-*` implementation versions, `RecShop-M1-v1.0`,
and existing sample/source labels. These are machine-format compatibility
values, not public commands or prerequisites. Keep them unchanged when reading
existing data; use the descriptive scripts documented in
[DATA-MIGRATION.md](DATA-MIGRATION.md). A source-file rename produces a new source
fingerprint and does not rewrite old packages, plans or their hashes.
