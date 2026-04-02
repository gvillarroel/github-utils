# Archives Snapshot Strategy

This strategy downloads one GitHub source archive per repository and computes the file inventory from the extracted snapshot.

## What It Optimizes

- Very low API call count
- Simple snapshot semantics
- Full file inventory and line counts from a local extraction
- Clean temporary workspace handling

## Behavior

- Lists repositories with the shared REST client
- Downloads `zipball` or `tarball` for the repository default branch
- Extracts the archive into a temporary directory
- Walks the extracted tree and produces the shared repository row shape
- Supports `parquet`, `csv`, and `jsonl`

## Usage

```bash
python strategies/archives_snapshot/exporter.py --owner gvillarroel --owner-type user --output-format parquet
python strategies/archives_snapshot/exporter.py --owner openai --owner-type org --archive-format tarball --output-format jsonl
```
