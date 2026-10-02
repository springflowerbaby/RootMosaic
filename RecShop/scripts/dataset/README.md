# Dataset conversion and reading

These tools convert accepted completed native attempts into V1 packages. They do not inject faults, automatically accept experiments, or run RCA algorithms.

- [Migration workflow and portable paths](../../docs/DATA-MIGRATION.md)
- [Input and output format](../../docs/DATA-FORMAT.md)
- [Synthetic offline example](../../examples/migration/README.md)

Entry points: `prepare_export.py`, `export_metrics.py`, `build_dataset.py`, `validate_dataset.py`.
Keep `check_metrics_retention.py`, `source_paths.py`, `source_integrity.py`, `metric_views.py`, `log_views.py` and `trace_formats.py` alongside them: these are runtime dependencies.

Python 3.10+ and its standard library suffice. Input/output paths are explicit; source evidence is supplied separately. Machine format identifiers are retained for compatibility; see the format guide. Existing packages are never relabeled by this source rename.
