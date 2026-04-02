# Trees + Selective Blobs

This strategy collects repository metadata with GraphQL, builds the file inventory from Git trees, and then fetches blob contents only for files that match a practical text-file filter.

## What it is good for

- Fast repository inventory generation
- Selective extraction of text files without cloning entire repositories
- Lower transfer cost than full clone strategies
- Better scaling than file-by-file contents traversal

## Selection rules

By default the exporter fetches contents only for files that:

- Are smaller than `--max-content-bytes`
- Match an allowed extension
- Or match an allowed filename such as `README` or `Dockerfile`

You can further constrain or expand the selection with:

- `--include-extension`
- `--include-filename`
- `--include-path-prefix`
- `--exclude-path-prefix`
- `--max-selected-files`

## Output

The exporter writes the shared repository fields plus:

- `selected_content_count`
- `selected_content_bytes`
- `selected_content_error`
- `selected_files`

`selected_files` stores the fetched text for each selected file together with its blob SHA and selection reason. For `csv` and `jsonl`, nested values are serialized as JSON.

## Example

```bash
python strategies/trees_selective_blobs/exporter.py ^
  --owner gvillarroel ^
  --owner-type user ^
  --output-format parquet ^
  --output-dir output/trees-selective-blobs
```
