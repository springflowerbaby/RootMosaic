# Convert accepted attempts into a dataset package

Python 3.10+ and its standard library are sufficient. Migration runs without a
Kubernetes client, model, database connection or RCA baseline. Source attempts
and historical monitoring data are external inputs, not bundled in the code.

## Workflow

CLI file arguments are relative to the current working directory. Paths inside
original ledgers/contracts are relative to the explicit `--source-root`.

```powershell
$source = 'path/to/source-data'
$work = 'path/to/new-migration'
python -B scripts/dataset/prepare_export.py --source-root $source --accepted-ledger "$source/accepted-ledger.json" --out "$work/PLAN.json"
python -B scripts/dataset/export_metrics.py --plan "$work/PLAN.json" --out "$work/history" --origin http://127.0.0.1:19090 --execute-readonly
python -B scripts/dataset/build_dataset.py --history "$work/history" --out "$work/delivery"
python -B scripts/dataset/validate_dataset.py "$work/delivery" --hashes
```

1. `prepare_export` freezes the chosen attempt, original hashes and actual phase
   windows. The acceptance ledger is a machine-readable selection record from
   the completed experiment; its fields are described in [input format](DATA-FORMAT.md).
2. `export_metrics` exports original Prometheus resource/HTTP matrices for those
   historical windows. Without `--execute-readonly`, it saves only a local export
   plan. The reader allows an explicit HTTP origin on `127.0.0.1`, bypasses proxy
   environment variables, uses serial requests and pauses on slow/error results.
   An operator may separately establish a verified local forwarded port; the
   exporter does not create forwarding or change monitoring configuration.
3. `build_dataset` consumes native evidence and cached matrices/receipts without
   network access. It rechecks selected identity, creates a new package, reads
   back the records and writes V1 delivery status.
4. `validate_dataset` verifies a package from its own files. A completed package can
   move to another directory/computer without its original source tree. A separate
   reader distribution needs `validate_dataset.py`, `log_views.py`, `metric_views.py`.

Expired historical values cannot be reconstructed unless a verified cache exists.
A current-time query is not a replacement. `check_metrics_retention.py` is also a
read-only inventory CLI; its helpers are required by the other entry points.
It accepts the same source-root, ledger, mapping and independent-output inputs;
monitoring requests require `--execute-readonly`.

## Explicit source relocation

There is no arbitrary basename search. Each accepted row identifies one SUMMARY
path/hash. `recovery-input.json` beside it contains the executed contract. The
contract's `context.evidence_root` identifies native `contract.json` and
`artifacts/`. Relative SUMMARY references and evidence roots both use
`--source-root`; existing absolute paths are supported.

When originals contain absolute paths from another location, use a separate map
rather than changing original bytes:

```json
{"mappings": [{"from": "/former/source-root", "to": "./relocated-inputs"}]}
```

Pass `--path-map path/to/map.json` during preparation. `from` must be an absolute
path prefix (Windows drive prefixes are supported). Relative `to` values use the
map file's directory. Longest exact path-component prefix wins; duplicate source
mappings and `..` traversal are rejected. The resolver never rewrites ledger,
contract, QC or observation bytes or guesses identity from a scenario number.

Plans bind resolved paths and mapping. After moving input data again, create a
new plan/output for the new location; do not edit old hashes to make it pass.
First-pass CSV inputs require explicit `summary_path` and `summary_sha256`; the
old historical directory convention is not reconstructed. Prefer JSON for new
selection records. See [input format](DATA-FORMAT.md).

Preparation also records the presence, size and SHA256 of the exact artifacts
the converter consumes: SUMMARY/recovery input, original contract, operations,
annotation, original QC, and each phase's metric/log/trace bundles/projections,
workload and phase observations. This is a bounded file list, not a scan of the
entire raw-data tree. Empty observations stay empty; absent files are recorded as
absent rather than silently filled. Disappearance, replacement or a formerly
absent file appearing invalidates the prepared input binding.

Old plans without the consumed-input binding must be prepared again; the builder never
fills in missing hashes at build time. Historical export manifests also bind
their query receipts. After verification, one case's consumed source and cache
bytes are held in a local snapshot for conversion/copying. A later change to the
original source cannot silently replace values between validation and copying.
No scientific threshold or original QC label is changed by these checks.

## Independent output and repeated runs

`--out` chooses the destination; there is no report-tree requirement. It must be
disjoint from the input attempts, ledgers, cache and migration code. A plan file
must be new. An existing history/package directory requires its expected marker
and identical input identity; unrelated or incomplete directories are rejected.

Rebuilding unchanged inputs/code checks file hashes and is idempotent. Competing
attempts for an existing slot, changed source/adapter hashes and partial outputs
require a new directory. `--case DEMO01:r1` filters already frozen inputs; it does
not create or accept samples. A `STOP` file requests a pause at the next boundary.

V1 status records delivery identity, format, readability and integrity. The
package's `audit/` keeps original QC and acceptance provenance. Field definitions
and analysis-specific applicability are described in
[delivery status and data availability](DATA-FORMAT.md#delivery-status-and-data-availability).

For a network-free exercise, use the clearly invented
[synthetic example](../examples/migration/README.md); it is not research data.
