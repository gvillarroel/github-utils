# GitHub Repo Stats To Parquet

This repository contains Python exporters that scan GitHub repositories and write one row per repository in Parquet, CSV, or JSONL format.

The project is designed for repository analytics workflows where you want a single structured dataset that combines:

- repository metadata
- language usage
- contributor summaries
- optional GitHub statistics payloads
- file inventory details such as path, size, and line counts

The result is a structured dataset that can be consumed by Python, DuckDB, Spark, Polars, Pandas, or any other tool that can read the selected output format.

## What The Project Does

Each exporter takes a GitHub owner as input:

- a user
- an organization
- or `auto` detection

It then discovers repositories for that owner, gathers GitHub API data, optionally clones repositories to inspect their files, and writes a normalized dataset.

At a high level, the scripts answer questions like:

- Which repositories belong to this owner?
- How active are they?
- Which languages dominate each repository?
- How many contributors are there?
- How large is each repository at the file level?
- How many files and lines of text does each repository contain?

## Repository Layout

The repository is organized into three main areas:

- `exporters/`: the primary exporters and shared implementation
- `strategies/`: isolated alternative collection strategies
- `spikes/`: research notes, experiments, and run comparisons

Compatibility wrappers remain in the repository root for the three legacy entrypoints, but the canonical scripts now live under `exporters/`.

## Exporters

There are three exporters in this repository. They share the same output schema, but they optimize for different tradeoffs.

### `exporters/core.py`

REST-first exporter.

Use this when you want the most complete per-repository payload by default.

Characteristics:

- fetches repository metadata with the GitHub REST API
- always fetches languages
- always fetches contributors
- always fetches GitHub stats endpoints:
  - `commit_activity`
  - `code_frequency`
  - `participation`
  - `punch_card`
- clones each repository to compute file inventory and line counts
- performs a pre-run REST quota check
- supports `--no-include-contributors` and `--no-include-stats` to reduce REST cost
- supports `--inventory-mode clone|tree-only|tree-then-clone`

This is the most complete option, but also the most expensive in API calls and local work.

### `exporters/graphql_exporter.py`

GraphQL-first exporter.

Use this when metadata and language discovery should be cheaper, but you still want optional contributors, stats, and cloned file inventories.

Characteristics:

- uses GraphQL for repository metadata and languages
- uses REST only for optional contributors and optional stats
- supports `--inventory-mode clone|tree-only|tree-then-clone`
- requires GitHub authentication
- performs a REST quota check for the optional REST portion
- includes retry handling for GraphQL timeouts and secondary rate limits

This is a good middle ground when GraphQL can reduce metadata discovery cost.

### `exporters/hybrid_exporter.py`

Hybrid exporter for larger owners and recency-based scans.

Use this when you want to focus on recently updated repositories and reduce work on stale repositories.

Characteristics:

- discovers repositories from REST ordered by most recently updated
- stops early with `--updated-since`
- enriches discovered repositories in GraphQL batches
- writes output incrementally during the run
- supports two inventory modes:
  - `tree-only`: uses the Git tree API for file paths and sizes only
  - `tree-then-clone`: tries the tree API first, then clones when line counts are needed
- requires GitHub authentication

This is the recommended option for large organizations or recurring exports where only recent repositories matter.

## Strategy Implementations

The repository also includes isolated strategy implementations under `strategies/`.

These are intended as focused alternatives that separate transport and extraction approaches into independent folders:

- `strategies/trees_only`: metadata plus tree-based file inventory, no content fetches
- `strategies/trees_selective_blobs`: tree-based inventory plus selective blob content fetches
- `strategies/archives_snapshot`: metadata plus source archive download and local snapshot inspection
- `strategies/incremental_refresh`: persisted baseline and compare-based refresh flow
- `strategies/partial_clone`: partial clone with optional sparse materialization
- `strategies/shallow_clone`: shallow full snapshot clone

Use these when you want to compare collection strategies directly or evolve them independently without changing the three main exporters.

Current primary direction:

- default strategy for future requirements: `strategies/incremental_refresh/exporter.py`
- default operational profile: `tree-only`
- preferred full-fidelity fallback: `strategies/shallow_clone/exporter.py`

## Output

Each run writes a single file selected by `--output-format`:

- `repositories.parquet`
- `repositories.csv`
- `repositories.jsonl`

Each row represents one repository.

Format notes:

- `parquet` preserves nested columns such as `languages`, `contributors`, and `files`
- `jsonl` writes one JSON object per line and preserves nested structures
- `csv` serializes nested columns as compact JSON strings per cell

## Performance Tuning

If speed matters more than full enrichment, use these controls:

- `--inventory-mode tree-only`: fastest inventory mode, avoids cloning, stores path and size, and leaves `line_count` empty
- `--inventory-mode tree-then-clone`: tries the fast tree path first and clones only when line counts are needed
- `--no-include-contributors`: REST exporter only, avoids contributor calls
- `--no-include-stats`: REST exporter only, avoids the four GitHub stats endpoints per repository

Recommended profiles:

- fastest large-owner export: use `exporters/hybrid_exporter.py` with `--inventory-mode tree-only`
- fastest GraphQL export with file inventory: use `exporters/graphql_exporter.py --inventory-mode tree-only`
- fastest REST export with minimal quota usage: use `exporters/core.py --no-include-contributors --no-include-stats --inventory-mode tree-only`

### Main Columns

Repository metadata:

- `owner`
- `owner_type`
- `executed_at`
- `repo_name`
- `full_name`
- `private`
- `fork`
- `archived`
- `disabled`
- `is_template`
- `visibility`
- `default_branch`
- `description`
- `homepage`
- `language`
- `license_key`
- `license_name`
- `topics_json`
- `created_at`
- `updated_at`
- `pushed_at`
- `size_kib`
- `stargazers_count`
- `watchers_count`
- `subscribers_count`
- `forks_count`
- `open_issues_count`

Repository URLs and flags:

- `mirror_url`
- `allow_forking`
- `web_commit_signoff_required`
- `clone_url`
- `ssh_url`
- `html_url`

Nested structured data:

- `languages`
- `contributors`
- `files`

Derived contributor metrics:

- `contributor_count`
- `total_contributions`

GitHub stats payloads serialized as JSON strings:

- `commit_activity_json`
- `code_frequency_json`
- `participation_json`
- `punch_card_json`

File inventory summary:

- `file_inventory_ready`
- `file_inventory_error`
- `file_count`
- `total_file_size_bytes`
- `total_line_count`
- `binary_file_count`

### Nested Column Shapes

`languages` contains a list of structs:

- `language`
- `bytes`

`contributors` contains a list of structs:

- `login`
- `user_id`
- `contributions`
- `type`
- `site_admin`

`files` contains a list of structs:

- `path`
- `size_bytes`
- `line_count`

## Requirements

- Python 3.10+
- `git` available in `PATH`
- dependencies from `requirements.txt`

Install dependencies:

```bash
pip install -r requirements.txt
```

## Authentication

Authentication is resolved in this order:

1. `--token`
2. `GITHUB_TOKEN`
3. `gh auth token` if GitHub CLI is installed and already logged in

Notes:

- the REST-first exporter can run without authentication, but quota is usually much lower
- the GraphQL and hybrid exporters require authentication
- private repositories are only available when the token has access to them

## Usage

### REST-First Export

Export all repositories for an organization:

```bash
python exporters/core.py --owner openai --owner-type org --output-dir output/openai
```

Export repositories for a user and limit the run:

```bash
python exporters/core.py --owner octocat --owner-type user --max-repos 5 --output-dir output/octocat
```

Auto-detect the owner type:

```bash
python exporters/core.py --owner some-account --owner-type auto
```

Keep local clones for inspection:

```bash
python exporters/core.py --owner openai --owner-type org --keep-clones --workspace-dir tmp/repos
```

Write CSV instead of Parquet:

```bash
python exporters/core.py --owner openai --owner-type org --output-format csv --output-dir output/openai-csv
```

Run the fastest REST profile with reduced quota usage:

```bash
python exporters/core.py --owner openai --owner-type org --no-include-contributors --no-include-stats --inventory-mode tree-only --output-dir output/openai-fast
```

### GraphQL-First Export

Fetch metadata and languages with GraphQL, plus contributors and stats with REST:

```bash
python exporters/graphql_exporter.py --owner openai --owner-type org --include-contributors --include-stats --output-dir output/openai-graphql
```

Run a lighter GraphQL export without contributors or stats:

```bash
python exporters/graphql_exporter.py --owner openai --owner-type org --max-repos 20 --output-dir output/openai-graphql-light
```

Write JSONL with the GraphQL exporter:

```bash
python exporters/graphql_exporter.py --owner openai --owner-type org --output-format jsonl --output-dir output/openai-graphql-jsonl
```

Run the fastest GraphQL profile:

```bash
python exporters/graphql_exporter.py --owner openai --owner-type org --inventory-mode tree-only --output-dir output/openai-graphql-fast
```

### Hybrid Export

Export only repositories updated since a cutoff date:

```bash
python exporters/hybrid_exporter.py --owner google --owner-type org --updated-since 2025-09-29 --output-dir output/google-recent
```

Use the fast tree-only inventory mode:

```bash
python exporters/hybrid_exporter.py --owner google --owner-type org --updated-since 2025-09-29 --inventory-mode tree-only --output-dir output/google-tree
```

Use clone fallback when exact line counts are required:

```bash
python exporters/hybrid_exporter.py --owner google --owner-type org --updated-since 2025-09-29 --inventory-mode tree-then-clone --output-dir output/google-lines
```

Write CSV from the hybrid exporter:

```bash
python exporters/hybrid_exporter.py --owner google --owner-type org --updated-since 2025-09-29 --output-format csv --output-dir output/google-csv
```

Run the fastest hybrid profile:

```bash
python exporters/hybrid_exporter.py --owner google --owner-type org --updated-since 2025-09-29 --inventory-mode tree-only --output-dir output/google-fast
```

## Inventory Behavior

The file inventory logic is intentionally conservative.

- repositories without a default branch produce an empty file list and an explanatory error message
- large text files above 5 MiB keep size information, but `line_count` is skipped
- binary files are detected by null bytes and counted in `binary_file_count`
- `.git` internals are excluded from the inventory
- in hybrid `tree-only` mode, `line_count` is not computed
- in hybrid tree-based inventory, the Git tree API may be unavailable or truncated for some repositories; `tree-then-clone` is the fallback when exact counts matter

## Rate Limits And Reliability

The exporters include rate-limit and retry handling, but they do not hide GitHub API cost.

- the REST-first exporter estimates the minimum required REST calls before starting
- the GraphQL-first exporter estimates only the REST portion that remains after GraphQL metadata discovery
- the hybrid exporter reduces work by limiting discovery to recently updated repositories
- GraphQL requests include retry logic for transient failures, timeouts, and secondary rate limits
- GitHub stats endpoints may temporarily return no materialized result; in those cases the scripts store empty JSON payloads

## Which Exporter To Choose

Choose `exporters/core.py` when:

- you want the richest payload by default
- you are working with a modest number of repositories
- you want contributors and stats every time

Choose `exporters/graphql_exporter.py` when:

- you want cheaper metadata discovery
- you still want cloned file inventories
- contributors and stats should be optional

Choose `exporters/hybrid_exporter.py` when:

- you are scanning a large organization
- you only care about recently updated repositories
- you want faster runs with incremental output
- you can accept `tree-only` inventories or selectively fall back to cloning

## Limitations

- the scripts are single-process and repository processing is sequential
- GitHub API availability and rate limits still determine how large a run can be
- line counts depend on cloning or local file access; they are not available in pure tree mode
- some metadata fields are richer in REST than in GraphQL, so the GraphQL and hybrid variants normalize a subset into the shared schema
- the output is a single Parquet file rather than a partitioned dataset

## Repository Files

- `exporters/core.py`: REST-first exporter and shared helpers
- `exporters/graphql_exporter.py`: GraphQL-first exporter
- `exporters/hybrid_exporter.py`: hybrid exporter with recency cutoff and incremental writes
- `exporters/README.md`: overview of the main exporter package
- `strategies/`: isolated alternative implementations for tree, archive, incremental, and clone-based collection strategies
- `requirements.txt`: Python dependencies

## Typical Workflow

1. Choose the exporter based on owner size and data completeness needs.
2. Authenticate with `GITHUB_TOKEN` or GitHub CLI when possible.
3. Run the exporter into an output directory.
4. Load the generated dataset into your analysis tool of choice.
5. Switch to `--output-format csv` or `--output-format jsonl` when Parquet is not the right downstream format.
