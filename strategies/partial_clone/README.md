# Partial Clone Strategy

This strategy uses Git partial clone with `--filter=blob:none` to inspect repository structure efficiently.

What it is good for:

- Collecting repository metadata
- Listing repository file inventories without downloading all file contents
- Materializing only selected paths or prefixes when content is needed

Behavior:

- Repositories are cloned with `git clone --filter=blob:none --no-checkout`
- File inventories are read from Git tree objects
- Optional sparse prefixes and file patterns can be materialized on demand
- Materialized files can be used to compute line counts for a subset of the tree

Example:

```bash
python strategies/partial_clone/exporter.py ^
  --owner gvillarroel ^
  --owner-type user ^
  --sparse-path src ^
  --sparse-path tests ^
  --materialize-pattern "*.py" ^
  --output-format parquet
```

Recommended use:

- Use this strategy when you need repository shape plus targeted content inspection
- Prefer `trees_only` if you only need inventory
- Prefer `archives_snapshot` if you need full snapshots faster than Git checkout semantics
