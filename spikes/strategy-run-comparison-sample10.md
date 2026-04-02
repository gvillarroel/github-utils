# Strategy Run Comparison: `gvillarroel` Sample Of 10 Repositories

## Scope

This comparison ran the strategy implementations sequentially against `gvillarroel` with `--max-repos 10`.

Output format:

- `jsonl`

Output root:

- `output/compare-strategies-sample10`

## Commands

- `python strategies/trees_only/exporter.py --owner gvillarroel --owner-type user --max-repos 10 --output-dir output/compare-strategies-sample10/trees_only --output-format jsonl`
- `python strategies/trees_selective_blobs/exporter.py --owner gvillarroel --owner-type user --max-repos 10 --output-dir output/compare-strategies-sample10/trees_selective_blobs --output-format jsonl`
- `python strategies/archives_snapshot/exporter.py --owner gvillarroel --owner-type user --max-repos 10 --output-dir output/compare-strategies-sample10/archives_snapshot --output-format jsonl`
- `python strategies/incremental_refresh/exporter.py --owner gvillarroel --owner-type user --max-repos 10 --output-dir output/compare-strategies-sample10/incremental_refresh --output-format jsonl --inventory-mode tree-only`
- `python strategies/incremental_refresh/exporter.py --owner gvillarroel --owner-type user --max-repos 10 --output-dir output/compare-strategies-sample10/incremental_refresh --output-format jsonl --inventory-mode tree-only`
- `python strategies/partial_clone/exporter.py --owner gvillarroel --owner-type user --max-repos 10 --output-dir output/compare-strategies-sample10/partial_clone --output-format jsonl --inventory-mode tree-then-clone --materialize-pattern '*.py' --materialize-pattern '*.md' --materialize-pattern '*.json'`
- `python strategies/shallow_clone/exporter.py --owner gvillarroel --owner-type user --max-repos 10 --output-dir output/compare-strategies-sample10/shallow_clone --output-format jsonl`

## Runtime Summary

| Strategy | Status | Time (s) | Output bytes |
| --- | --- | ---: | ---: |
| `trees_only` | ok | 6.20 | 30,285 |
| `trees_selective_blobs` | ok | 1,533.55 | 1,272,449 |
| `archives_snapshot` | ok | 73.21 | 166,238 |
| `incremental_refresh` baseline | ok | 35.01 | 207,814 |
| `incremental_refresh` second run | ok | 2.70 | 207,814 |
| `partial_clone` | ok | 756.52 | 168,228 |
| `shallow_clone` | ok | 49.67 | 166,238 |

## Important Caveat

The strategies do not currently choose the same repository sample under `--max-repos 10`.

- `trees_only` and `trees_selective_blobs` produced:
  - `adk-conn`
  - `agent-news`
  - `astro-gcs`
  - `backstage`
  - `blog`
  - `codex_efx`
  - `convnet2`
  - `d3_test`
  - `datacatalog-ml`
  - `dc-angular`
- `archives_snapshot`, `incremental_refresh`, `partial_clone`, and `shallow_clone` produced:
  - `adk-conn`
  - `agent-news`
  - `astro-gcs`
  - `backstage`
  - `codex_efx`
  - `convnet2`
  - `d3_test`
  - `datacatalog-ml`
  - `ellmtree`
  - `emeres`

That means direct comparison is fully fair only on the `8` repositories shared by all strategies.

## Shared-Repo Comparison

Common repositories across all six outputs:

- `gvillarroel/adk-conn`
- `gvillarroel/agent-news`
- `gvillarroel/astro-gcs`
- `gvillarroel/backstage`
- `gvillarroel/codex_efx`
- `gvillarroel/convnet2`
- `gvillarroel/d3_test`
- `gvillarroel/datacatalog-ml`

Metrics on those shared repositories:

| Strategy | File count | Total size bytes | Total line count | Binary files |
| --- | ---: | ---: | ---: | ---: |
| `trees_only` | 151 | 5,388,389 | 0 | 0 |
| `trees_selective_blobs` | 151 | 5,388,389 | 0 | 0 |
| `archives_snapshot` | 151 | 5,388,389 | 57,677 | 12 |
| `incremental_refresh` | 151 | 5,388,389 | 0 | 0 |
| `partial_clone` | 142 | 5,244,522 | 14,287 | 0 |
| `shallow_clone` | 151 | 5,388,389 | 57,677 | 12 |

## Findings

### 1. `trees_only` is the fastest baseline by a large margin

- Lowest runtime
- Smallest output
- Same file-count and file-size totals as the full-snapshot strategies on shared repositories
- No line counts, no binary detection

### 2. `trees_selective_blobs` is far more expensive than expected

- Same inventory totals as `trees_only`
- Runtime was roughly `25` minutes for the sample
- Output grew because it stores selected file contents
- It adds value only when the fetched content is actually needed downstream

### 3. `archives_snapshot` and `shallow_clone` matched exactly on shared repositories

For shared repositories they produced identical values for:

- `file_count`
- `total_file_size_bytes`
- `total_line_count`
- `binary_file_count`

In this sample, `shallow_clone` was faster than `archives_snapshot`.

### 4. `incremental_refresh` behaves as intended

- Baseline run: `35.01s`
- Second run with unchanged heads: `2.70s`
- Reused `10` rows and refreshed `0`

This is the strongest option when repeated runs matter more than first-run completeness.

### 5. `partial_clone` is useful but currently less robust

- Runtime was much higher than `shallow_clone`
- One repository (`gvillarroel/adk-conn`) failed inventory because the destination path already existed
- That left `file_inventory_ready` true for only `9/10` rows
- Because the run materialized only selected patterns, totals were lower than full-snapshot strategies

## Practical Recommendation

- Use `trees_only` for the fastest inventory-first scan.
- Use `incremental_refresh` for recurring runs once a baseline exists.
- Use `shallow_clone` when exact line counts and full snapshot fidelity are required.
- Use `archives_snapshot` when archive transport is preferred over Git clone transport.
- Use `trees_selective_blobs` only when selective file contents are truly needed.
- Fix `partial_clone` path cleanup before relying on it for unattended runs.

## Follow-Up Work

- Normalize repository ordering across all strategies so `--max-repos` selects the same sample everywhere.
- Fix workspace cleanup in `partial_clone`.
- Consider adding a common comparison harness so future strategy regressions are easy to measure.
