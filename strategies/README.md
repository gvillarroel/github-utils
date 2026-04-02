# Strategy Exporters

This directory contains separate implementations for GitHub repository collection strategies.

Each strategy must:

- Keep its implementation isolated in its own folder
- Reuse shared logic from the repository root where practical
- Support the same output formats: `parquet`, `csv`, and `jsonl`
- Write repository-shaped rows compatible with the shared schema

Current target folders:

- `trees_only`
- `trees_selective_blobs`
- `archives_snapshot`
- `incremental_refresh`
- `partial_clone`
- `shallow_clone`

Each strategy folder uses `exporter.py` as the main entrypoint.
