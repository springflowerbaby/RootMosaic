# Dataset conversion and reading

These tools convert accepted completed native attempts into V1 packages. They do not inject faults, automatically accept experiments, or run RCA algorithms.

- [Migration workflow and portable paths](../../docs/DATA-MIGRATION.md)
- [Input and output format](../../docs/DATA-FORMAT.md)
- [Synthetic offline example](../../examples/migration/README.md)

Entry points: `prepare_export.py`, `export_metrics.py`, `build_dataset.py`, `validate_dataset.py`.
Keep `check_metrics_retention.py`, `source_paths.py`, `source_integrity.py`, `metric_views.py`, `log_views.py`, `trace_formats.py` and `design_labels.py` alongside them: these are runtime dependencies. In the repository, the versioned label mapping is `configs/dataset/design_labels.v1.json`; exported adapter snapshots carry it beside the Python files.

`build_dataset.py` automatically assigns design path relationships, interaction intent and instance roles for the supported fixed scenarios, including newly collected attempts. See [design-label semantics](../../docs/DATA-FORMAT.md#design-labels). Existing deliveries require a separate label update; changing converter code does not rewrite them.

Python 3.10+ and its standard library suffice. Input/output paths are explicit; source evidence is supplied separately. Machine format identifiers are retained for compatibility; see the format guide. Existing packages are never relabeled by this source rename.
