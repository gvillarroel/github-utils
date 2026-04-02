# Incremental Refresh Strategy

This strategy keeps a persisted state file and only refreshes repositories whose default-branch commit has changed.

## What It Does

- Lists repositories for an owner
- Reads the current default-branch commit SHA for each repository
- Reuses the stored row when the repository is unchanged
- Refreshes only changed repositories
- Uses the GitHub compare endpoint to record changed file paths in the state file
- Writes the final dataset in `parquet`, `csv`, or `jsonl`

## Files

- `exporter.py`: runnable exporter
- `incremental_refresh_state.json`: default state file created next to the output

## Workflow

### Baseline Run

The first run builds the state file and writes the dataset.

Example:

```bash
python strategies/incremental_refresh/exporter.py ^
  --owner gvillarroel ^
  --owner-type user ^
  --output-dir output/incremental-refresh ^
  --output-format parquet
```

If no state file exists, the script behaves like a baseline run.

### Refresh Run

On later runs, the script compares the stored head SHA with the current default-branch SHA.

- If the SHA is unchanged and the settings match, the stored row is reused.
- If the SHA changed, the repository is refreshed and the compare endpoint is queried.

Example:

```bash
python strategies/incremental_refresh/exporter.py ^
  --owner gvillarroel ^
  --owner-type user ^
  --output-dir output/incremental-refresh ^
  --output-format jsonl
```

## State Model

The state file stores:

- Repository ID
- Full name
- Default branch
- Latest processed head SHA
- The last generated row
- Refresh settings used for that row
- Compare metadata for changed repositories

## Refresh Boundary

This strategy uses the default-branch HEAD as the refresh boundary.

That is a pragmatic tradeoff:

- It is efficient
- It avoids reprocessing unchanged repositories
- It may leave some non-code metadata slightly stale until the repository moves again

## Inventory Modes

The script supports the same inventory modes used by the other exporters:

- `tree-only`
- `tree-then-clone`
- `clone`

Recommended default for incremental refresh:

- `tree-only` when you want the fastest run
- `tree-then-clone` when you also want line counts

## Output Formats

The exporter supports:

- `parquet`
- `csv`
- `jsonl`

## Notes

- The script requires GitHub authentication.
- The state file is validated against the owner and owner type.
- If you change refresh settings, the affected repositories are refreshed again so the stored rows stay consistent.
