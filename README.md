# GitHub Repo Stats To Parquet

This repository contains several Python scripts that export one Parquet row per GitHub repository.

## Output Format

Each exporter writes a single file:

- `repositories.parquet`

Each row represents one repository and contains:

- repository metadata and counters from GitHub
- language totals as JSON
- contributor summaries as JSON
- GitHub stats payloads as JSON
- file inventory summary columns such as `file_count`, `total_file_size_bytes`, and `total_line_count`
- a nested `files` column with a list of:
  - `path`
  - `size_bytes`
  - `line_count` when available

## Requirements

- Python 3.10+
- `git`
- GitHub authentication for higher API limits and private repositories

Authentication is resolved in this order:

- `--token`
- `GITHUB_TOKEN`
- `gh auth token` if the GitHub CLI is already logged in

Install dependencies:

```bash
pip install -r requirements.txt
```

## Exporters

- `github_repo_stats_to_parquet.py`
  - baseline REST-first exporter
  - supports nested `files`, `languages`, and `contributors`
- `github_repo_stats_to_parquet_graphql.py`
  - GraphQL-first variant for repository metadata and languages
  - REST is used only for optional contributors and stats
- `github_repo_stats_to_parquet_hybrid.py`
  - hybrid variant for large organizations
  - discovers repositories ordered by recent activity
  - supports `--updated-since` for early cutoff
  - supports `--inventory-mode tree-only` for fast path/size inventory
  - supports `--inventory-mode tree-then-clone` when exact `line_count` is needed
  - writes incrementally to Parquet during the run

## Usage

Export all repositories for an organization:

```bash
python github_repo_stats_to_parquet.py --owner openai --owner-type org --output-dir output/openai
```

Export repositories for a user and limit the run to five repositories:

```bash
python github_repo_stats_to_parquet.py --owner octocat --owner-type user --max-repos 5 --output-dir output/octocat
```

Auto-detect the owner type:

```bash
python github_repo_stats_to_parquet.py --owner some-account --owner-type auto
```

Run the hybrid exporter for recently updated repositories:

```bash
python github_repo_stats_to_parquet_hybrid.py --owner google --owner-type org --updated-since 2025-09-29 --inventory-mode tree-only --output-dir output/google-6mo
```

## Notes

- File inventories are computed from shallow clones of each repository.
- Repositories without a default branch keep an empty file list and an explanatory error field.
- The fast inventory path does not require `line_count`. In tree-based runs, `line_count` may be `null`.
- The hybrid exporter is the recommended option for large organizations because it reduces REST core usage and can stop discovery early by date.
- Large files above 5 MiB keep size information, but line counting is skipped when line counting is enabled.
- Some GitHub stats endpoints may remain unavailable and are stored as empty JSON payloads when GitHub does not materialize them in time.
- The script checks the available GitHub API quota before the run and fails fast if the remaining quota is not enough for the selected repository count.
