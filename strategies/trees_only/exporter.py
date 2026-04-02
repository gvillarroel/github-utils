#!/usr/bin/env python
"""Export GitHub repository metadata and tree-based file inventories without cloning."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from exporters.core import (
    GitHubClient,
    RepoContext,
    build_repo_row,
    OUTPUT_FORMAT_CHOICES,
    fetch_default_branch_tree_oid,
    fetch_tree_files,
    minimum_required_requests,
    resolve_github_token,
    write_repositories,
)
from exporters.graphql_exporter import GitHubGraphQLClient, normalize_graphql_repo


DEFAULT_OUTPUT_DIR = "output-trees-only"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner", required=True, help="GitHub user or organization login.")
    parser.add_argument(
        "--owner-type",
        choices=("auto", "user", "org"),
        default="auto",
        help="Owner type. Default: auto-detect.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where the exported dataset will be written.",
    )
    parser.add_argument(
        "--output-format",
        choices=OUTPUT_FORMAT_CHOICES,
        default="parquet",
        help="Output format. Default: parquet.",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="GitHub token. Defaults to GITHUB_TOKEN or gh auth token.",
    )
    parser.add_argument(
        "--include-archived",
        action="store_true",
        help="Include archived repositories.",
    )
    parser.add_argument(
        "--max-repos",
        type=int,
        default=None,
        help="Optional maximum number of repositories to process.",
    )
    return parser.parse_args()


def estimate_tree_only_requests(repo_count: int) -> int:
    # GraphQL discovery is not included here because its point budget is separate from REST core quota.
    return minimum_required_requests(
        repo_count,
        include_contributors=False,
        include_stats=False,
        inventory_mode="tree-only",
    )


def collect_tree_only_inventory(
    client: GitHubClient,
    repo: RepoContext,
) -> tuple[list[dict[str, Any]], int, int, int, bool, str | None]:
    files: list[dict[str, Any]] = []
    total_file_size_bytes = 0
    total_line_count = 0
    binary_file_count = 0

    if not repo.default_branch:
        return files, total_file_size_bytes, total_line_count, binary_file_count, False, "Repository has no default branch."

    default_oid = fetch_default_branch_tree_oid(client, repo.full_name, repo.default_branch)
    if not default_oid:
        return (
            files,
            total_file_size_bytes,
            total_line_count,
            binary_file_count,
            False,
            "Default branch exists, but its commit OID is not available from REST.",
        )

    tree_files = fetch_tree_files(client, repo.full_name, default_oid)
    if tree_files is None:
        return (
            files,
            total_file_size_bytes,
            total_line_count,
            binary_file_count,
            False,
            "Tree API inventory is unavailable or truncated for this repository.",
        )

    total_file_size_bytes = sum(int(item.get("size_bytes") or 0) for item in tree_files)
    return (
        tree_files,
        total_file_size_bytes,
        total_line_count,
        binary_file_count,
        True,
        "Tree API mode: line_count is not computed.",
    )


def main() -> int:
    args = parse_args()
    token = resolve_github_token(args.token)
    if not token:
        raise RuntimeError("The trees_only exporter requires GitHub authentication.")

    graphql_client = GitHubGraphQLClient(token=token)
    rest_client = GitHubClient(token=token)

    owner_type = args.owner_type
    if owner_type == "auto":
        owner_type = rest_client.resolve_owner_type(args.owner)

    graphql_repos, graphql_rate = graphql_client.list_repos(
        args.owner,
        owner_type,
        max_repos=args.max_repos,
    )

    normalized_repos: list[tuple[dict[str, Any], dict[str, int]]] = []
    for repo in graphql_repos:
        normalized, languages = normalize_graphql_repo(args.owner, owner_type, repo)
        if not args.include_archived and normalized["archived"]:
            continue
        normalized_repos.append((normalized, languages))

    rest_rate = rest_client.fetch_rate_limit()["resources"]["core"]
    required_core_requests = estimate_tree_only_requests(len(normalized_repos))
    remaining_core_requests = int(rest_rate["remaining"])
    if remaining_core_requests < required_core_requests:
        raise RuntimeError(
            "Not enough GitHub REST core quota to run safely. "
            f"Remaining core requests: {remaining_core_requests}. "
            f"Estimated minimum required: {required_core_requests}. "
            f"GraphQL remaining: {graphql_rate['remaining']}."
        )

    output_dir = Path(args.output_dir).resolve()
    executed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    repository_rows: list[dict[str, Any]] = []

    for repo_summary, languages in normalized_repos:
        repo_context = RepoContext(
            repo_id=repo_summary["id"],
            owner=args.owner,
            owner_type=owner_type,
            repo_name=repo_summary["name"],
            full_name=repo_summary["full_name"],
            default_branch=repo_summary["default_branch"],
            clone_url=repo_summary["clone_url"],
        )
        print(f"Processing {repo_context.full_name}...", file=sys.stderr)

        (
            files,
            total_file_size_bytes,
            total_line_count,
            binary_file_count,
            file_inventory_ready,
            file_inventory_error,
        ) = collect_tree_only_inventory(rest_client, repo_context)

        row = build_repo_row(
            owner=args.owner,
            owner_type=owner_type,
            executed_at=executed_at,
            repo=repo_summary,
            languages=languages,
            contributors=[],
            commit_activity=None,
            code_frequency=None,
            participation=None,
            punch_card=None,
            files=files,
            file_inventory_ready=file_inventory_ready,
            file_inventory_error=file_inventory_error,
            total_file_size_bytes=total_file_size_bytes,
            total_line_count=total_line_count,
            binary_file_count=binary_file_count,
        )
        repository_rows.append(row)

    output_path = write_repositories(repository_rows, output_dir, args.output_format)
    print(f"Processed {len(repository_rows)} repositories into {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
