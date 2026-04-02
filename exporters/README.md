# Main Exporters

This directory contains the primary exporters and their shared implementation.

Files:

- `core.py`: REST-first exporter plus shared helpers and schema
- `graphql_exporter.py`: GraphQL-first exporter
- `hybrid_exporter.py`: hybrid exporter with recency cutoff and incremental writes

Repository-root scripts are retained as compatibility wrappers:

- `github_repo_stats_to_parquet.py`
- `github_repo_stats_to_parquet_graphql.py`
- `github_repo_stats_to_parquet_hybrid.py`

Prefer the `exporters/` paths for new documentation, benchmarks, and automation.
