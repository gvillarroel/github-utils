# trees_only

This strategy collects repository metadata and file inventories without cloning repositories and without fetching blob contents.

## How it works

1. Discover repositories with GraphQL.
2. Normalize repository metadata and languages.
3. Resolve the default branch tree with REST.
4. Fetch the recursive tree and persist file paths and sizes.
5. Skip clone-based line counting entirely.

## Output

The exporter writes the shared repository schema in one of these formats:

- `parquet`
- `csv`
- `jsonl`

## Usage

```bash
python strategies/trees_only/exporter.py --owner gvillarroel --owner-type user --output-format parquet
python strategies/trees_only/exporter.py --owner openai --owner-type org --output-format csv
python strategies/trees_only/exporter.py --owner gvillarroel --owner-type user --max-repos 50 --output-format jsonl
```

## Notes

- This strategy is optimized for inventory-first analytics.
- It avoids `git clone` entirely.
- It does not compute line counts from file contents.
