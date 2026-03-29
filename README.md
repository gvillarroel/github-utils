# GitHub Repo Stats To Parquet

This repository contains a Python script that exports repository metadata, GitHub stats, and a file-level inventory for every repository owned by a GitHub user or organization.

## What It Exports

The script writes one Parquet file per dataset:

- `repos.parquet`: repository metadata and counters.
- `languages.parquet`: language byte counts per repository.
- `contributors.parquet`: contributors and contribution counts.
- `commit_activity_weekly.parquet`: weekly commit totals for the last year.
- `code_frequency_weekly.parquet`: weekly additions and deletions.
- `participation_weekly.parquet`: weekly owner and total commit participation.
- `punch_card.parquet`: commits grouped by weekday and hour.
- `files.parquet`: file path, size in bytes, extension, binary flag, and line count.
- `run_manifest.parquet`: run-level metadata.

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

## Notes

- File inventories are computed from shallow clones of each repository.
- Repositories without a default branch are skipped for file inventory collection.
- Large files above 5 MiB keep size information, but line counting is skipped to avoid excessive memory use.
- The script checks the available GitHub API quota before the run and fails fast if the remaining quota is not enough for the selected repository count.
