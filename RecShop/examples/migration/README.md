# Synthetic format exercise — no real measurements

Every identity, value and acceptance row generated here is invented test data.
`DEMO01` is not a research scenario. The `purpose=formal` field is only the input
shape needed to exercise the converter: **no formal collection, fault injection,
monitoring query, cluster or model call occurs**.

Use a new work directory, from the project root:

```powershell
$work = 'path/to/new-format-exercise'
python -B examples/migration/synthetic_demo.py --out "$work/source"
python -B scripts/dataset/prepare_export.py --source-root "$work/source" --accepted-ledger "$work/source/accepted-ledger.json" --out "$work/PLAN.json"
python -B examples/migration/synthetic_demo.py --cache-from-plan "$work/PLAN.json" --out "$work/history"
python -B scripts/dataset/build_dataset.py --history "$work/history" --out "$work/delivery" --gap-s 0
python -B scripts/dataset/validate_dataset.py "$work/delivery" --expected 1 --hashes
```

The cache helper refuses non-demo identities. Inputs/cache carry
`SYNTHETIC-NOT-COLLECTED.json`; the package preserves synthetic contract, summary,
log and QC markers. Format PASS means consistent reading of this invented fixture.
Never merge its output into scientific data or report its values as measurements.

Identical build inputs/code are checked and reused. Existing fixture/cache/plan
destinations are rejected. A completed package can move elsewhere and still be
read without its original source directory.
