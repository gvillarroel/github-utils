#!/usr/bin/env python
"""Archive snapshot exporter for GitHub repositories."""

from __future__ import annotations

import argparse
import sys
import tarfile
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from exporters.core import (  # noqa: E402
    API_BASE_URL,
    DEFAULT_HTTP_RETRIES,
    DEFAULT_RATE_LIMIT_BUFFER_SECONDS,
    GitHubApiError,
    GitHubClient,
    build_repo_row,
    collect_files,
    resolve_github_token,
    write_repositories,
)


ARCHIVE_FORMAT_CHOICES = ("zipball", "tarball")


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
        default="output-archives",
        help="Directory where the exported dataset will be written.",
    )
    parser.add_argument(
        "--output-format",
        choices=("parquet", "csv", "jsonl"),
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
        "--archive-format",
        choices=ARCHIVE_FORMAT_CHOICES,
        default="zipball",
        help="Source archive format to download. Default: zipball.",
    )
    return parser.parse_args()


def build_archive_url(full_name: str, archive_format: str, ref: str) -> str:
    return f"{API_BASE_URL}/repos/{full_name}/{archive_format}/{ref}"


def safe_extract_zip(archive_path: Path, destination: Path) -> None:
    base_dir = destination.resolve()
    with zipfile.ZipFile(archive_path) as archive:
        for member in archive.infolist():
            member_path = (destination / member.filename).resolve()
            if base_dir not in member_path.parents and member_path != base_dir:
                raise GitHubApiError(f"Unsafe path in archive: {member.filename!r}")
        archive.extractall(destination)


def safe_extract_tar(archive_path: Path, destination: Path) -> None:
    base_dir = destination.resolve()
    with tarfile.open(archive_path) as archive:
        for member in archive.getmembers():
            member_path = (destination / member.name).resolve()
            if base_dir not in member_path.parents and member_path != base_dir:
                raise GitHubApiError(f"Unsafe path in archive: {member.name!r}")
        archive.extractall(destination)


def download_archive(client: GitHubClient, url: str, archive_path: Path) -> None:
    for attempt in range(DEFAULT_HTTP_RETRIES):
        try:
            with client.session.get(url, stream=True, timeout=client.timeout, allow_redirects=True) as response:
                if response.status_code in {403, 429} and response.headers.get("X-RateLimit-Remaining") == "0":
                    reset_at = int(response.headers.get("X-RateLimit-Reset", "0") or "0")
                    wait_seconds = max(0, reset_at - int(time.time()) + DEFAULT_RATE_LIMIT_BUFFER_SECONDS)
                    if wait_seconds > 0:
                        print(
                            f"GitHub rate limit reached while downloading archives. Waiting {wait_seconds} seconds...",
                            file=sys.stderr,
                        )
                        time.sleep(wait_seconds)
                    else:
                        time.sleep(DEFAULT_RATE_LIMIT_BUFFER_SECONDS)
                    continue

                if response.status_code != 200:
                    raise GitHubApiError(
                        f"GitHub archive download failed: GET {url} returned {response.status_code}: {response.text[:500]}"
                    )

                with archive_path.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            handle.write(chunk)
                return
        except requests.RequestException as exc:
            if attempt == DEFAULT_HTTP_RETRIES - 1:
                raise GitHubApiError(f"GitHub archive download failed after retries: {url}: {exc}") from exc
            time.sleep(2**attempt)
            continue

    raise GitHubApiError(f"GitHub archive download failed after retries: {url}")


def extract_archive(archive_path: Path, destination: Path, archive_format: str) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    if archive_format == "zipball":
        safe_extract_zip(archive_path, destination)
        return
    if archive_format == "tarball":
        safe_extract_tar(archive_path, destination)
        return
    raise ValueError(f"Unsupported archive format: {archive_format}")


def choose_snapshot_root(extract_dir: Path) -> Path:
    entries = [path for path in extract_dir.iterdir()]
    dirs = [path for path in entries if path.is_dir()]
    files = [path for path in entries if path.is_file()]
    if len(dirs) == 1 and not files:
        return dirs[0]
    return extract_dir


def collect_archive_snapshot(
    client: GitHubClient,
    repo: dict[str, Any],
    *,
    archive_format: str,
) -> tuple[list[dict[str, Any]], int, int, int, bool, str | None]:
    default_branch = repo.get("default_branch")
    if not default_branch:
        return [], 0, 0, 0, False, "Repository has no default branch."

    full_name = repo["full_name"]
    archive_url = build_archive_url(full_name, archive_format, default_branch)

    with tempfile.TemporaryDirectory(prefix="github-archive-") as temp_dir:
        temp_path = Path(temp_dir)
        archive_path = temp_path / f"{repo['name']}.{archive_format}"
        extract_dir = temp_path / "extract"

        download_archive(client, archive_url, archive_path)
        extract_archive(archive_path, extract_dir, archive_format)
        snapshot_root = choose_snapshot_root(extract_dir)
        files, total_file_size_bytes, total_line_count, binary_file_count = collect_files(snapshot_root)

    return files, total_file_size_bytes, total_line_count, binary_file_count, True, None


def main() -> int:
    args = parse_args()
    token = resolve_github_token(args.token)
    if not token:
        raise RuntimeError("The archive snapshot exporter requires GitHub authentication.")

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
    executed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    rows: list[dict[str, Any]] = []

    for repo in repos:
        print(f"Processing {repo['full_name']}...", file=sys.stderr)
        languages = client.fetch_languages(repo["full_name"])
        files, total_file_size_bytes, total_line_count, binary_file_count, file_inventory_ready, file_inventory_error = collect_archive_snapshot(
            client,
            repo,
            archive_format=args.archive_format,
        )

        rows.append(
            build_repo_row(
                owner=args.owner,
                owner_type=owner_type,
                executed_at=executed_at,
                repo=repo,
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

    output_path = write_repositories(rows, output_dir, args.output_format)
    print(f"Processed {len(rows)} repositories into {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
