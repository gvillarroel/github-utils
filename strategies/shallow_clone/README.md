# Shallow Clone Strategy

This strategy exports repository metadata plus a full local snapshot built from a shallow Git clone.

## Characteristics

- Uses `git clone --depth 1`
- Produces exact file inventories and line counts for the checked-out snapshot
- Avoids GitHub blob-by-blob API traffic
- Suitable when a full current snapshot is needed and local Git transport is acceptable

## Script

- `exporter.py`

## Usage

```bash
python strategies/shallow_clone/exporter.py --owner gvillarroel --owner-type user --output-format parquet
python strategies/shallow_clone/exporter.py --owner gvillarroel --owner-type user --output-format csv
python strategies/shallow_clone/exporter.py --owner gvillarroel --owner-type user --output-format jsonl
```

## Notes

- The script reuses the shared repository schema and writer helpers from the repository root.
- Temporary clone directories are removed by default unless `--keep-clones` is set.
- The exported rows include `executed_at` and the same nested fields as the shared schema.
