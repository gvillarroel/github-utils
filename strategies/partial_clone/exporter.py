#!/usr/bin/env python
"""Export GitHub repository data using git partial clone and selective materialization."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from exporters.core import (
    GitHubApiError,
    GitHubClient,
    OUTPUT_FORMAT_CHOICES,
    RepoContext,
    build_repo_row,
    ensure_git_available,
    resolve_github_token,
    write_repositories,
)


TEXT_FILE_SIZE_LIMIT = 5 * 1024 * 1024


@dataclass
class FileRecord:
    path: str
    size_bytes: int | None
    line_count: int | None


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
        default="output-partial-clone",
        help="Directory where the exported dataset will be written.",
    )
    parser.add_argument(
        "--output-format",
        choices=OUTPUT_FORMAT_CHOICES,
        default="parquet",
        help="Output format. Default: parquet.",
    )
    parser.add_argument(
        "--workspace-dir",
        default=str(Path(tempfile.gettempdir()) / "gh-repo-partial-clone"),
        help="Directory used for temporary clones.",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("GITHUB_TOKEN"),
        help="GitHub token. Defaults to GITHUB_TOKEN.",
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
    parser.add_argument(
        "--sparse-path",
        action="append",
        default=[],
        help="Sparse checkout prefix to materialize. Repeatable.",
    )
    parser.add_argument(
        "--materialize-pattern",
        action="append",
        default=[],
        help="Glob pattern for files to materialize after clone. Repeatable.",
    )
    parser.add_argument(
        "--materialize-file",
        action="append",
        default=[],
        help="Exact file path to materialize after clone. Repeatable.",
    )
    parser.add_argument(
        "--inventory-mode",
        choices=("tree-only", "tree-then-clone"),
        default="tree-then-clone",
        help="Tree-only keeps the checkout minimal; tree-then-clone materializes selected paths and computes line counts for them.",
    )
    return parser.parse_args()


def remove_tree(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def authenticated_clone_url(clone_url: str, token: str | None) -> str:
    if not token or not clone_url.startswith("https://"):
        return clone_url
    if clone_url.startswith("https://"):
        return clone_url.replace("https://", f"https://x-access-token:{token}@", 1)
    return clone_url


def clone_partial_repo(repo: RepoContext, workspace_dir: Path, token: str | None) -> Path:
    repo_key = hashlib.sha1(f"{repo.repo_id}:{repo.full_name}".encode("utf-8"), usedforsecurity=False)
    repo_path = workspace_dir / f"{repo.repo_id}-{repo_key.hexdigest()[:10]}"
    if repo_path.exists():
        remove_tree(repo_path)
    repo_path.parent.mkdir(parents=True, exist_ok=True)

    base_command = [
        "git",
        "-c",
        "core.longpaths=true",
        "clone",
        "--filter=blob:none",
        "--no-checkout",
        "--single-branch",
        authenticated_clone_url(repo.clone_url, token),
        str(repo_path),
    ]

    command = base_command.copy()
    if repo.default_branch:
        command[6:6] = ["--branch", repo.default_branch]
        try:
            subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return repo_path
        except subprocess.CalledProcessError as exc:
            if repo_path.exists():
                remove_tree(repo_path)
            if "Remote branch" not in (exc.stderr or ""):
                raise

    subprocess.run(
        base_command,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return repo_path


def git(repo_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo_path), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def read_default_branch_commit(repo_path: Path) -> str | None:
    result = git(repo_path, "rev-parse", "HEAD")
    commit = result.stdout.strip()
    return commit or None


def read_tree_entries(repo_path: Path) -> list[dict[str, Any]]:
    result = git(repo_path, "ls-tree", "-r", "--long", "HEAD")
    entries: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        mode, kind, _sha, size_and_path = line.split(None, 3)
        size_text, path = size_and_path.split("\t", 1)
        if kind != "blob":
            continue
        size_bytes = None if size_text == "-" else int(size_text)
        entries.append({"path": path, "size_bytes": size_bytes, "line_count": None})
    return entries


def select_materialized_paths(
    files: list[dict[str, Any]],
    *,
    sparse_paths: list[str],
    materialize_patterns: list[str],
    materialize_files: list[str],
) -> list[str]:
    selected: list[str] = []
    exact_files = set(materialize_files)
    for item in files:
        path = item["path"]
        if path in exact_files:
            selected.append(path)
            continue
        if materialize_patterns and any(fnmatch.fnmatch(path, pattern) for pattern in materialize_patterns):
            selected.append(path)
            continue
        if sparse_paths and any(path == prefix or path.startswith(prefix.rstrip("/") + "/") for prefix in sparse_paths):
            selected.append(path)
    return sorted(set(selected))


def configure_sparse_checkout(repo_path: Path, sparse_paths: list[str]) -> None:
    if not sparse_paths:
        return
    git(repo_path, "sparse-checkout", "init", "--cone")
    git(repo_path, "sparse-checkout", "set", *sparse_paths)


def materialize_selected_paths(repo_path: Path, paths: list[str]) -> None:
    if not paths:
        return
    git(repo_path, "checkout", "HEAD", "--", *paths)


def count_lines(path: Path) -> tuple[int | None, bool]:
    size = path.stat().st_size
    if size == 0:
        return 0, False
    if size > TEXT_FILE_SIZE_LIMIT:
        return None, False
    data = path.read_bytes()
    if b"\x00" in data:
        return None, True
    line_count = data.count(b"\n")
    if data and not data.endswith(b"\n"):
        line_count += 1
    return line_count, False


def collect_inventory(
    repo_path: Path,
    *,
    selected_paths: list[str],
) -> tuple[list[dict[str, Any]], int, int, int, str | None]:
    selected = set(selected_paths)
    files: list[dict[str, Any]] = []
    total_file_size_bytes = 0
    total_line_count = 0
    binary_file_count = 0
    inventory_error: str | None = None

    for entry in read_tree_entries(repo_path):
        path = entry["path"]
        size_bytes = entry.get("size_bytes")
        line_count = None

        if path in selected:
            file_path = repo_path / path
            if file_path.exists():
                line_count, is_binary = count_lines(file_path)
                if is_binary:
                    binary_file_count += 1
                if line_count is not None:
                    total_line_count += line_count
            else:
                inventory_error = "Selected path was not materialized in the working tree."

        if size_bytes is not None:
            total_file_size_bytes += int(size_bytes)

        files.append(
            {
                "path": path,
                "size_bytes": size_bytes,
                "line_count": line_count,
            }
        )

    return files, total_file_size_bytes, total_line_count, binary_file_count, inventory_error


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

    output_dir = Path(args.output_dir).resolve()
    workspace_dir = Path(args.workspace_dir).resolve()
    executed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    rows: list[dict[str, Any]] = []
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
        try:
            repo_path = clone_partial_repo(repo_context, workspace_dir, token)
        except subprocess.CalledProcessError as exc:
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
                files=[],
                file_inventory_ready=False,
                file_inventory_error=(exc.stderr or "")[:500],
                total_file_size_bytes=0,
                total_line_count=0,
                binary_file_count=0,
            )
            rows.append(row)
            continue
        try:
            if args.inventory_mode == "tree-then-clone":
                configure_sparse_checkout(repo_path, args.sparse_path)
                selected_paths = select_materialized_paths(
                    read_tree_entries(repo_path),
                    sparse_paths=args.sparse_path,
                    materialize_patterns=args.materialize_pattern,
                    materialize_files=args.materialize_file,
                )
                materialize_selected_paths(repo_path, selected_paths)
            else:
                selected_paths = []

            files, total_file_size_bytes, total_line_count, binary_file_count, inventory_error = collect_inventory(
                repo_path,
                selected_paths=selected_paths,
            )
        finally:
            if not args.keep_clones and repo_path.exists():
                remove_tree(repo_path)

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
            file_inventory_ready=True,
            file_inventory_error=inventory_error,
            total_file_size_bytes=total_file_size_bytes,
            total_line_count=total_line_count,
            binary_file_count=binary_file_count,
        )
        rows.append(row)

    output_path = write_repositories(rows, output_dir, args.output_format)
    print(f"Processed {len(rows)} repositories into {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
