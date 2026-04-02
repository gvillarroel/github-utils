#!/usr/bin/env python
"""Shallow-clone exporter for full repository snapshot extraction."""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from exporters.core import (
    GitHubClient,
    RepoContext,
    build_repo_row,
    clone_repo,
    collect_files,
    ensure_git_available,
    resolve_github_token,
    remove_tree,
    write_repositories,
)


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
        default="output-shallow-clone",
        help="Directory where the exported dataset will be written.",
    )
    parser.add_argument(
        "--output-format",
        choices=("parquet", "csv", "jsonl"),
        default="parquet",
        help="Output format. Default: parquet.",
    )
    parser.add_argument(
        "--workspace-dir",
        default=str(Path(tempfile.gettempdir()) / "gh-repo-stats"),
        help="Directory used for temporary clones.",
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
    parser.add_argument(
        "--keep-clones",
        action="store_true",
        help="Keep cloned repositories after the run.",
    )
    return parser.parse_args()


def estimate_required_requests(repo_count: int) -> int:
    return repo_count


def main() -> int:
    args = parse_args()
    ensure_git_available()
    token = resolve_github_token(args.token)

    client = GitHubClient(token=token)
    owner_type = args.owner_type
    if owner_type == "auto":
        owner_type = client.resolve_owner_type(args.owner)

    repos = client.list_repos(args.owner, owner_type)
    if not args.include_archived:
        repos = [repo for repo in repos if not repo.get("archived")]
    if args.max_repos is not None:
        repos = repos[: args.max_repos]

    rate_limit = client.fetch_rate_limit()["rate"]
    required_requests = estimate_required_requests(len(repos))
    if int(rate_limit["remaining"]) < required_requests:
        reset_at = int(rate_limit["reset"])
        reset_in_seconds = max(0, reset_at - int(time.time()))
        raise RuntimeError(
            "Not enough GitHub API quota to run safely. "
            f"Remaining core requests: {rate_limit['remaining']}. "
            f"Estimated minimum required: {required_requests}. "
            f"Retry after about {reset_in_seconds} seconds or provide GitHub authentication."
        )

    output_dir = Path(args.output_dir).resolve()
    workspace_dir = Path(args.workspace_dir).resolve()
    executed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    repository_rows: list[dict[str, Any]] = []

    for repo_summary in repos:
        repo_context = RepoContext(
            repo_id=repo_summary["id"],
            owner=args.owner,
            owner_type=owner_type,
            repo_name=repo_summary["name"],
            full_name=repo_summary["full_name"],
            default_branch=repo_summary.get("default_branch"),
            clone_url=repo_summary["clone_url"],
        )

        print(f"Processing {repo_context.full_name}...", file=sys.stderr)
        languages = client.fetch_languages(repo_context.full_name)

        file_inventory_ready = False
        file_inventory_error: str | None = None
        files: list[dict[str, Any]] = []
        total_file_size_bytes = 0
        total_line_count = 0
        binary_file_count = 0

        try:
            repo_path = clone_repo(repo_context, workspace_dir, token)
        except Exception as exc:
            file_inventory_error = str(exc)[:500]
        else:
            try:
                files, total_file_size_bytes, total_line_count, binary_file_count = collect_files(repo_path)
                file_inventory_ready = True
            finally:
                if not args.keep_clones and repo_path.exists():
                    remove_tree(repo_path)

        repository_rows.append(
            build_repo_row(
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
        )

    output_path = write_repositories(repository_rows, output_dir, args.output_format)
    print(f"Processed {len(repository_rows)} repositories into {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
