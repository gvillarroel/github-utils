#!/usr/bin/env python
"""Export GitHub repository metadata, tree inventories, and selective blob contents."""

from __future__ import annotations

import argparse
import base64
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from exporters.core import (  # noqa: E402
    GitHubClient,
    OUTPUT_FORMAT_CHOICES,
    REPOSITORIES_SCHEMA,
    RepoContext,
    build_repo_row,
    resolve_github_token,
    serialize_row_for_text_output,
)
from exporters.graphql_exporter import (  # noqa: E402
    GitHubGraphQLClient,
    normalize_graphql_repo,
)


DEFAULT_TEXT_EXTENSIONS = [
    ".bat",
    ".cfg",
    ".c",
    ".cc",
    ".clj",
    ".cpp",
    ".cs",
    ".css",
    ".dart",
    ".dockerfile",
    ".go",
    ".gradle",
    ".h",
    ".hpp",
    ".html",
    ".ini",
    ".java",
    ".js",
    ".json",
    ".jsx",
    ".kt",
    ".kts",
    ".lua",
    ".md",
    ".mk",
    ".php",
    ".pl",
    ".ps1",
    ".py",
    ".rb",
    ".rs",
    ".scala",
    ".sh",
    ".sql",
    ".swift",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
]

DEFAULT_TEXT_FILENAMES = [
    ".editorconfig",
    ".gitattributes",
    ".gitignore",
    "CHANGELOG",
    "CODEOWNERS",
    "Dockerfile",
    "LICENSE",
    "Makefile",
    "README",
]

SELECTED_FILES_TYPE = pa.list_(
    pa.struct(
        [
            ("path", pa.string()),
            ("size_bytes", pa.int64()),
            ("blob_sha", pa.string()),
            ("content_fetched", pa.bool_()),
            ("content_truncated", pa.bool_()),
            ("content_encoding", pa.string()),
            ("selection_reason", pa.string()),
            ("content_text", pa.string()),
        ]
    )
)

TREE_SELECTIVE_BLOBS_SCHEMA = pa.schema(
    list(REPOSITORIES_SCHEMA)
    + [
        ("selected_content_count", pa.int64()),
        ("selected_content_bytes", pa.int64()),
        ("selected_content_error", pa.string()),
        ("selected_files", SELECTED_FILES_TYPE),
    ]
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
        default="output-trees-selective-blobs",
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
    parser.add_argument(
        "--include-extension",
        action="append",
        dest="include_extensions",
        default=list(DEFAULT_TEXT_EXTENSIONS),
        help="File extension that qualifies for blob content fetches. Repeat to add more values.",
    )
    parser.add_argument(
        "--include-filename",
        action="append",
        dest="include_filenames",
        default=list(DEFAULT_TEXT_FILENAMES),
        help="Base filename that qualifies for blob content fetches. Repeat to add more values.",
    )
    parser.add_argument(
        "--exclude-path-prefix",
        action="append",
        dest="exclude_path_prefixes",
        default=[],
        help="Skip content fetches for paths with this prefix. Repeatable.",
    )
    parser.add_argument(
        "--include-path-prefix",
        action="append",
        dest="include_path_prefixes",
        default=[],
        help="Restrict content fetches to paths with this prefix. Repeatable.",
    )
    parser.add_argument(
        "--max-content-bytes",
        type=int,
        default=1_048_576,
        help="Maximum blob size in bytes to fetch as content. Default: 1 MiB.",
    )
    parser.add_argument(
        "--max-selected-files",
        type=int,
        default=None,
        help="Optional cap on how many files per repository will have content fetched.",
    )
    parser.add_argument(
        "--max-tree-depth",
        type=int,
        default=None,
        help="Optional subtree recursion limit when the recursive tree response is truncated.",
    )
    return parser.parse_args()


def parse_owner_type(client: GitHubClient, owner: str, owner_type: str) -> str:
    if owner_type != "auto":
        return owner_type
    return client.resolve_owner_type(owner)


def fetch_default_branch_tree_sha(client: GitHubClient, full_name: str, default_branch: str) -> str | None:
    response = client.request(
        "GET",
        f"/repos/{full_name}/branches/{default_branch}",
        expected_statuses=(200, 404, 409),
    )
    if response.status_code != 200:
        return None
    payload = response.json()
    commit = payload.get("commit") or {}
    tree = commit.get("commit", {}).get("tree", {})
    return tree.get("sha")


def fetch_tree_page(client: GitHubClient, full_name: str, tree_sha: str, *, recursive: bool) -> dict[str, Any] | None:
    params = {"recursive": "1"} if recursive else None
    response = client.request(
        "GET",
        f"/repos/{full_name}/git/trees/{tree_sha}",
        params=params,
        expected_statuses=(200, 404, 409, 422),
    )
    if response.status_code != 200:
        return None
    payload = response.json()
    if not isinstance(payload, dict):
        return None
    return payload


def collect_tree_inventory(
    client: GitHubClient,
    full_name: str,
    tree_sha: str,
    *,
    max_tree_depth: int | None,
) -> tuple[list[dict[str, Any]], bool]:
    recursive_payload = fetch_tree_page(client, full_name, tree_sha, recursive=True)
    if recursive_payload and not recursive_payload.get("truncated"):
        files = [
            {
                "path": item["path"],
                "size_bytes": item.get("size"),
                "line_count": None,
                "blob_sha": item.get("sha"),
            }
            for item in recursive_payload.get("tree", [])
            if item.get("type") == "blob"
        ]
        return files, True

    root_payload = fetch_tree_page(client, full_name, tree_sha, recursive=False)
    if not root_payload:
        return [], False

    files: list[dict[str, Any]] = []
    stack: list[tuple[str, str, int]] = [("", tree_sha, 0)]

    while stack:
        prefix, current_sha, depth = stack.pop()
        if max_tree_depth is not None and depth > max_tree_depth:
            continue
        payload = root_payload if current_sha == tree_sha and not prefix else fetch_tree_page(client, full_name, current_sha, recursive=False)
        if not payload:
            continue
        for item in payload.get("tree", []):
            item_type = item.get("type")
            item_path = item["path"] if not prefix else f"{prefix}/{item['path']}"
            if item_type == "blob":
                files.append(
                    {
                        "path": item_path,
                        "size_bytes": item.get("size"),
                        "line_count": None,
                        "blob_sha": item.get("sha"),
                    }
                )
            elif item_type == "tree" and item.get("sha"):
                stack.append((item_path, item["sha"], depth + 1))

    return files, False


def is_selected_for_content(path: str, size_bytes: int | None, args: argparse.Namespace) -> tuple[bool, str]:
    if size_bytes is None:
        return False, "missing size"
    if size_bytes > args.max_content_bytes:
        return False, f"size>{args.max_content_bytes}"
    if args.include_path_prefixes and not any(path.startswith(prefix) for prefix in args.include_path_prefixes):
        return False, "path prefix not included"
    if any(path.startswith(prefix) for prefix in args.exclude_path_prefixes):
        return False, "excluded path prefix"

    basename = Path(path).name
    suffix = Path(path).suffix.lower()
    if basename in args.include_filenames:
        return True, "matched filename"
    if suffix in {extension.lower() for extension in args.include_extensions}:
        return True, "matched extension"
    return False, "extension not selected"


def fetch_blob_content(client: GitHubClient, full_name: str, blob_sha: str) -> tuple[str | None, bool, str | None, str | None]:
    response = client.request("GET", f"/repos/{full_name}/git/blobs/{blob_sha}")
    payload = response.json()
    content = payload.get("content") or ""
    encoding = payload.get("encoding")
    truncated = bool(payload.get("truncated"))
    if encoding != "base64":
        return None, truncated, encoding, "unsupported blob encoding"
    try:
        raw_bytes = base64.b64decode(content)
    except (ValueError, TypeError) as exc:
        return None, truncated, encoding, f"base64 decode failed: {exc}"
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return None, truncated, encoding, "blob is not valid utf-8 text"
    return text, truncated, encoding, None


def build_selected_file_rows(
    client: GitHubClient,
    full_name: str,
    files: list[dict[str, Any]],
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], int, int, list[str]]:
    selected_files: list[dict[str, Any]] = []
    selected_count = 0
    selected_bytes = 0
    errors: list[str] = []

    for item in files:
        if args.max_selected_files is not None and selected_count >= args.max_selected_files:
            break

        path = item["path"]
        size_bytes = int(item.get("size_bytes") or 0)
        blob_sha = item.get("blob_sha")
        selected, reason = is_selected_for_content(path, size_bytes, args)
        if not selected or not blob_sha:
            continue

        text, truncated, encoding, fetch_error = fetch_blob_content(client, full_name, blob_sha)
        if fetch_error:
            errors.append(f"{path}: {fetch_error}")
            selected_files.append(
                {
                    "path": path,
                    "size_bytes": size_bytes,
                    "blob_sha": blob_sha,
                    "content_fetched": False,
                    "content_truncated": truncated,
                    "content_encoding": encoding,
                    "selection_reason": f"{reason}; {fetch_error}",
                    "content_text": None,
                }
            )
            continue

        selected_count += 1
        selected_bytes += size_bytes
        selected_files.append(
            {
                "path": path,
                "size_bytes": size_bytes,
                "blob_sha": blob_sha,
                "content_fetched": True,
                "content_truncated": truncated,
                "content_encoding": encoding,
                "selection_reason": reason,
                "content_text": text,
            }
        )

    return selected_files, selected_count, selected_bytes, errors


def write_repositories_parquet(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "repositories.parquet"
    table = pa.Table.from_pylist(rows, schema=TREE_SELECTIVE_BLOBS_SCHEMA)
    pq.write_table(table, output_path)
    return output_path


def write_repositories_csv(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "repositories.csv"
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[field.name for field in TREE_SELECTIVE_BLOBS_SCHEMA])
        writer.writeheader()
        for row in rows:
            writer.writerow(serialize_row_for_text_output(row))
    return output_path


def write_repositories_jsonl(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "repositories.jsonl"
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")
    return output_path


def write_repositories(rows: list[dict[str, Any]], output_dir: Path, output_format: str) -> Path:
    if output_format == "parquet":
        return write_repositories_parquet(rows, output_dir)
    if output_format == "csv":
        return write_repositories_csv(rows, output_dir)
    if output_format == "jsonl":
        return write_repositories_jsonl(rows, output_dir)
    raise ValueError(f"Unsupported output format: {output_format}")


def main() -> int:
    args = parse_args()
    token = resolve_github_token(args.token)
    if not token:
        raise RuntimeError("The selective blob exporter requires GitHub authentication.")

    rest_client = GitHubClient(token=token)
    owner_type = parse_owner_type(rest_client, args.owner, args.owner_type)
    graphql_client = GitHubGraphQLClient(token=token)

    repos, graphql_rate = graphql_client.list_repos(args.owner, owner_type, max_repos=args.max_repos)
    if not args.include_archived:
        repos = [repo for repo in repos if not repo.get("isArchived")]

    output_dir = Path(args.output_dir).resolve()
    executed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    repository_rows: list[dict[str, Any]] = []

    for repo in repos:
        normalized, languages = normalize_graphql_repo(args.owner, owner_type, repo)
        repo_context = RepoContext(
            repo_id=normalized["id"],
            owner=args.owner,
            owner_type=owner_type,
            repo_name=normalized["name"],
            full_name=normalized["full_name"],
            default_branch=normalized["default_branch"],
            clone_url=normalized["clone_url"],
        )

        print(f"Processing {repo_context.full_name}...", file=sys.stderr)

        file_inventory_ready = False
        file_inventory_error: str | None = None
        files: list[dict[str, Any]] = []
        tree_sha: str | None = None

        if repo_context.default_branch:
            tree_sha = fetch_default_branch_tree_sha(rest_client, repo_context.full_name, repo_context.default_branch)
            if tree_sha:
                files, file_inventory_ready = collect_tree_inventory(
                    rest_client,
                    repo_context.full_name,
                    tree_sha,
                    max_tree_depth=args.max_tree_depth,
                )
                if not file_inventory_ready:
                    file_inventory_error = "Tree inventory could not be collected."
            else:
                file_inventory_error = "Default branch exists, but its tree SHA is not available."
        else:
            file_inventory_error = "Repository has no default branch."

        selected_files: list[dict[str, Any]] = []
        selected_count = 0
        selected_bytes = 0
        selected_errors: list[str] = []
        if file_inventory_ready and files:
            selected_files, selected_count, selected_bytes, selected_errors = build_selected_file_rows(
                rest_client,
                repo_context.full_name,
                files,
                args,
            )

        base_row = build_repo_row(
            owner=args.owner,
            owner_type=owner_type,
            executed_at=executed_at,
            repo=normalized,
            languages=languages,
            contributors=[],
            commit_activity=None,
            code_frequency=None,
            participation=None,
            punch_card=None,
            files=files,
            file_inventory_ready=file_inventory_ready,
            file_inventory_error=file_inventory_error,
            total_file_size_bytes=sum(int(item.get("size_bytes") or 0) for item in files),
            total_line_count=0,
            binary_file_count=0,
        )
        base_row.update(
            {
                "selected_content_count": selected_count,
                "selected_content_bytes": selected_bytes,
                "selected_content_error": "; ".join(selected_errors) if selected_errors else None,
                "selected_files": selected_files,
            }
        )
        repository_rows.append(base_row)

    output_path = write_repositories(repository_rows, output_dir, args.output_format)
    print(f"Processed {len(repository_rows)} repositories into {output_path}")
    print(f"GraphQL remaining: {graphql_rate['remaining']}.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
