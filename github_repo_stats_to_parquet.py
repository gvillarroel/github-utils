#!/usr/bin/env python
"""Export GitHub repository metadata, stats, and file inventories to Parquet."""

from __future__ import annotations

import argparse
import json
import os
import stat
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit, urlunsplit

import pyarrow as pa
import pyarrow.parquet as pq
import requests


API_BASE_URL = "https://api.github.com"
DEFAULT_STATS_RETRIES = 6
DEFAULT_STATS_DELAY = 2.0
TEXT_FILE_SIZE_LIMIT = 5 * 1024 * 1024


TABLE_SCHEMAS: dict[str, pa.Schema] = {
    "repos": pa.schema(
        [
            ("owner", pa.string()),
            ("owner_type", pa.string()),
            ("repo_name", pa.string()),
            ("full_name", pa.string()),
            ("private", pa.bool_()),
            ("fork", pa.bool_()),
            ("archived", pa.bool_()),
            ("disabled", pa.bool_()),
            ("is_template", pa.bool_()),
            ("visibility", pa.string()),
            ("default_branch", pa.string()),
            ("description", pa.string()),
            ("homepage", pa.string()),
            ("language", pa.string()),
            ("license_key", pa.string()),
            ("license_name", pa.string()),
            ("topics_json", pa.string()),
            ("created_at", pa.string()),
            ("updated_at", pa.string()),
            ("pushed_at", pa.string()),
            ("size_kib", pa.int64()),
            ("stargazers_count", pa.int64()),
            ("watchers_count", pa.int64()),
            ("subscribers_count", pa.int64()),
            ("forks_count", pa.int64()),
            ("open_issues_count", pa.int64()),
            ("network_count", pa.int64()),
            ("has_issues", pa.bool_()),
            ("has_projects", pa.bool_()),
            ("has_downloads", pa.bool_()),
            ("has_wiki", pa.bool_()),
            ("has_pages", pa.bool_()),
            ("has_discussions", pa.bool_()),
            ("mirror_url", pa.string()),
            ("allow_forking", pa.bool_()),
            ("web_commit_signoff_required", pa.bool_()),
            ("clone_url", pa.string()),
            ("ssh_url", pa.string()),
            ("html_url", pa.string()),
        ]
    ),
    "languages": pa.schema(
        [
            ("owner", pa.string()),
            ("repo_name", pa.string()),
            ("language", pa.string()),
            ("bytes", pa.int64()),
        ]
    ),
    "contributors": pa.schema(
        [
            ("owner", pa.string()),
            ("repo_name", pa.string()),
            ("login", pa.string()),
            ("user_id", pa.int64()),
            ("contributions", pa.int64()),
            ("type", pa.string()),
            ("site_admin", pa.bool_()),
        ]
    ),
    "commit_activity_weekly": pa.schema(
        [
            ("owner", pa.string()),
            ("repo_name", pa.string()),
            ("week_unix", pa.int64()),
            ("total_commits", pa.int64()),
            ("days_json", pa.string()),
        ]
    ),
    "code_frequency_weekly": pa.schema(
        [
            ("owner", pa.string()),
            ("repo_name", pa.string()),
            ("week_unix", pa.int64()),
            ("additions", pa.int64()),
            ("deletions", pa.int64()),
        ]
    ),
    "participation_weekly": pa.schema(
        [
            ("owner", pa.string()),
            ("repo_name", pa.string()),
            ("week_index", pa.int64()),
            ("all_commits", pa.int64()),
            ("owner_commits", pa.int64()),
        ]
    ),
    "punch_card": pa.schema(
        [
            ("owner", pa.string()),
            ("repo_name", pa.string()),
            ("day_of_week", pa.int64()),
            ("hour_of_day", pa.int64()),
            ("commits", pa.int64()),
        ]
    ),
    "files": pa.schema(
        [
            ("owner", pa.string()),
            ("repo_name", pa.string()),
            ("repo_full_name", pa.string()),
            ("default_branch", pa.string()),
            ("path", pa.string()),
            ("extension", pa.string()),
            ("size_bytes", pa.int64()),
            ("line_count", pa.int64()),
            ("is_binary", pa.bool_()),
        ]
    ),
    "run_manifest": pa.schema(
        [
            ("owner", pa.string()),
            ("owner_type", pa.string()),
            ("repo_count", pa.int64()),
            ("generated_at_unix", pa.int64()),
            ("output_dir", pa.string()),
        ]
    ),
}


class GitHubApiError(RuntimeError):
    """Raised when a GitHub API request fails."""


@dataclass
class RepoContext:
    owner: str
    owner_type: str
    repo_name: str
    full_name: str
    default_branch: str
    clone_url: str


class GitHubClient:
    """Thin GitHub REST client with pagination and stats retries."""

    def __init__(self, token: str | None, timeout: int = 60) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "github-repo-stats-to-parquet",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self.session.headers.update(headers)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        expected_statuses: Iterable[int] = (200,),
    ) -> requests.Response:
        url = f"{API_BASE_URL}{path}"
        response = self.session.request(method, url, params=params, timeout=self.timeout)
        if response.status_code not in set(expected_statuses):
            raise GitHubApiError(
                f"GitHub API request failed: {method} {url} "
                f"returned {response.status_code}: {response.text[:500]}"
            )
        return response

    def paginate(self, path: str, *, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        merged_params = dict(params or {})
        merged_params.setdefault("per_page", 100)
        url = f"{API_BASE_URL}{path}"
        items: list[dict[str, Any]] = []

        while url:
            response = self.session.get(url, params=merged_params, timeout=self.timeout)
            if response.status_code != 200:
                raise GitHubApiError(
                    f"GitHub API request failed: GET {url} "
                    f"returned {response.status_code}: {response.text[:500]}"
                )
            payload = response.json()
            if not isinstance(payload, list):
                raise GitHubApiError(f"Expected a list response from {url}, got {type(payload)!r}")
            items.extend(payload)
            url = response.links.get("next", {}).get("url")
            merged_params = None

        return items

    def resolve_owner_type(self, owner: str) -> str:
        response = self.request("GET", f"/users/{owner}")
        owner_type = response.json()["type"].lower()
        if owner_type not in {"user", "organization"}:
            raise GitHubApiError(f"Unsupported owner type for {owner!r}: {owner_type!r}")
        return "org" if owner_type == "organization" else "user"

    def list_repos(self, owner: str, owner_type: str) -> list[dict[str, Any]]:
        if owner_type == "org":
            return self.paginate(f"/orgs/{owner}/repos", params={"type": "all", "sort": "full_name"})
        return self.paginate(f"/users/{owner}/repos", params={"type": "owner", "sort": "full_name"})

    def fetch_repo_details(self, full_name: str) -> dict[str, Any]:
        return self.request("GET", f"/repos/{full_name}").json()

    def fetch_languages(self, full_name: str) -> dict[str, int]:
        return self.request("GET", f"/repos/{full_name}/languages").json()

    def fetch_contributors(self, full_name: str) -> list[dict[str, Any]]:
        response = self.request(
            "GET",
            f"/repos/{full_name}/contributors",
            params={"anon": "true", "per_page": 100},
            expected_statuses=(200, 204, 409),
        )
        if response.status_code in {204, 409}:
            return []

        items = response.json()
        if not isinstance(items, list):
            raise GitHubApiError(
                f"Expected a list response from contributors endpoint for {full_name!r}."
            )

        next_url = response.links.get("next", {}).get("url")
        while next_url:
            page = self.session.get(next_url, timeout=self.timeout)
            if page.status_code != 200:
                raise GitHubApiError(
                    f"GitHub API request failed: GET {next_url} "
                    f"returned {page.status_code}: {page.text[:500]}"
                )
            payload = page.json()
            if not isinstance(payload, list):
                raise GitHubApiError(f"Expected a list response from {next_url}.")
            items.extend(payload)
            next_url = page.links.get("next", {}).get("url")

        return items

    def fetch_stats(self, full_name: str, endpoint: str) -> Any:
        path = f"/repos/{full_name}/stats/{endpoint}"
        for attempt in range(DEFAULT_STATS_RETRIES):
            response = self.request("GET", path, expected_statuses=(200, 202, 204, 422))
            if response.status_code == 200:
                return response.json()
            if response.status_code in {204, 422}:
                return None
            if attempt < DEFAULT_STATS_RETRIES - 1:
                time.sleep(DEFAULT_STATS_DELAY * (attempt + 1))
        print(
            f"Warning: stats endpoint did not become ready after retries and will be skipped: {path}",
            file=sys.stderr,
        )
        return None


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
        default="output",
        help="Directory where Parquet files will be written.",
    )
    parser.add_argument(
        "--workspace-dir",
        default=".cache/github-repo-clones",
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
    return parser.parse_args()


def ensure_git_available() -> None:
    if shutil.which("git") is None:
        raise RuntimeError("git is required but was not found in PATH.")


def remove_tree(path: Path) -> None:
    def handle_remove_readonly(function: Any, target: str, excinfo: Any) -> None:
        os.chmod(target, stat.S_IWRITE)
        function(target)

    shutil.rmtree(path, onexc=handle_remove_readonly)


def authenticated_clone_url(clone_url: str, token: str | None) -> str:
    if not token or not clone_url.startswith("https://"):
        return clone_url
    parts = urlsplit(clone_url)
    return urlunsplit((parts.scheme, f"x-access-token:{token}@{parts.netloc}", parts.path, "", ""))


def clone_repo(repo: RepoContext, workspace_dir: Path, token: str | None) -> Path:
    repo_path = workspace_dir / repo.owner / repo.repo_name
    if repo_path.exists():
        remove_tree(repo_path)
    repo_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--branch",
            repo.default_branch,
            "--single-branch",
            authenticated_clone_url(repo.clone_url, token),
            str(repo_path),
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return repo_path


def count_lines(path: Path) -> tuple[int | None, bool]:
    size = path.stat().st_size
    if size == 0:
        return 0, False
    if size > TEXT_FILE_SIZE_LIMIT:
        return None, False

    try:
        data = path.read_bytes()
    except OSError:
        return None, False

    if b"\x00" in data:
        return None, True

    line_count = data.count(b"\n")
    if data and not data.endswith(b"\n"):
        line_count += 1
    return line_count, False


def collect_file_rows(repo: RepoContext, repo_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for file_path in repo_path.rglob("*"):
        if not file_path.is_file():
            continue
        relative_path = file_path.relative_to(repo_path).as_posix()
        if relative_path.startswith(".git/"):
            continue
        line_count, is_binary = count_lines(file_path)
        rows.append(
            {
                "owner": repo.owner,
                "repo_name": repo.repo_name,
                "repo_full_name": repo.full_name,
                "default_branch": repo.default_branch,
                "path": relative_path,
                "extension": file_path.suffix.lower() or None,
                "size_bytes": file_path.stat().st_size,
                "line_count": line_count,
                "is_binary": is_binary,
            }
        )
    return rows


def write_parquet(table_name: str, rows: list[dict[str, Any]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    schema = TABLE_SCHEMAS[table_name]
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, output_dir / f"{table_name}.parquet")


def repo_row(owner: str, owner_type: str, repo: dict[str, Any]) -> dict[str, Any]:
    license_info = repo.get("license") or {}
    return {
        "owner": owner,
        "owner_type": owner_type,
        "repo_name": repo["name"],
        "full_name": repo["full_name"],
        "private": repo.get("private"),
        "fork": repo.get("fork"),
        "archived": repo.get("archived"),
        "disabled": repo.get("disabled"),
        "is_template": repo.get("is_template"),
        "visibility": repo.get("visibility"),
        "default_branch": repo.get("default_branch"),
        "description": repo.get("description"),
        "homepage": repo.get("homepage"),
        "language": repo.get("language"),
        "license_key": license_info.get("key"),
        "license_name": license_info.get("name"),
        "topics_json": json.dumps(repo.get("topics", [])),
        "created_at": repo.get("created_at"),
        "updated_at": repo.get("updated_at"),
        "pushed_at": repo.get("pushed_at"),
        "size_kib": repo.get("size"),
        "stargazers_count": repo.get("stargazers_count"),
        "watchers_count": repo.get("watchers_count"),
        "subscribers_count": repo.get("subscribers_count"),
        "forks_count": repo.get("forks_count"),
        "open_issues_count": repo.get("open_issues_count"),
        "network_count": repo.get("network_count"),
        "has_issues": repo.get("has_issues"),
        "has_projects": repo.get("has_projects"),
        "has_downloads": repo.get("has_downloads"),
        "has_wiki": repo.get("has_wiki"),
        "has_pages": repo.get("has_pages"),
        "has_discussions": repo.get("has_discussions"),
        "mirror_url": repo.get("mirror_url"),
        "allow_forking": repo.get("allow_forking"),
        "web_commit_signoff_required": repo.get("web_commit_signoff_required"),
        "clone_url": repo.get("clone_url"),
        "ssh_url": repo.get("ssh_url"),
        "html_url": repo.get("html_url"),
    }


def main() -> int:
    args = parse_args()
    ensure_git_available()

    client = GitHubClient(token=args.token)
    owner_type = args.owner_type
    if owner_type == "auto":
        owner_type = client.resolve_owner_type(args.owner)

    output_dir = Path(args.output_dir).resolve()
    workspace_dir = Path(args.workspace_dir).resolve()

    repos = client.list_repos(args.owner, owner_type)
    if not args.include_archived:
        repos = [repo for repo in repos if not repo.get("archived")]
    if args.max_repos is not None:
        repos = repos[: args.max_repos]

    repo_rows: list[dict[str, Any]] = []
    language_rows: list[dict[str, Any]] = []
    contributor_rows: list[dict[str, Any]] = []
    commit_activity_rows: list[dict[str, Any]] = []
    code_frequency_rows: list[dict[str, Any]] = []
    participation_rows: list[dict[str, Any]] = []
    punch_card_rows: list[dict[str, Any]] = []
    file_rows: list[dict[str, Any]] = []

    for repo_summary in repos:
        repo_details = client.fetch_repo_details(repo_summary["full_name"])
        repo_rows.append(repo_row(args.owner, owner_type, repo_details))

        repo_context = RepoContext(
            owner=args.owner,
            owner_type=owner_type,
            repo_name=repo_details["name"],
            full_name=repo_details["full_name"],
            default_branch=repo_details["default_branch"],
            clone_url=repo_details["clone_url"],
        )

        languages = client.fetch_languages(repo_context.full_name)
        for language, byte_count in languages.items():
            language_rows.append(
                {
                    "owner": args.owner,
                    "repo_name": repo_context.repo_name,
                    "language": language,
                    "bytes": byte_count,
                }
            )

        contributors = client.fetch_contributors(repo_context.full_name)
        for contributor in contributors:
            contributor_rows.append(
                {
                    "owner": args.owner,
                    "repo_name": repo_context.repo_name,
                    "login": contributor.get("login"),
                    "user_id": contributor.get("id"),
                    "contributions": contributor.get("contributions"),
                    "type": contributor.get("type"),
                    "site_admin": contributor.get("site_admin"),
                }
            )

        commit_activity = client.fetch_stats(repo_context.full_name, "commit_activity") or []
        for week in commit_activity:
            commit_activity_rows.append(
                {
                    "owner": args.owner,
                    "repo_name": repo_context.repo_name,
                    "week_unix": week["week"],
                    "total_commits": week["total"],
                    "days_json": json.dumps(week["days"]),
                }
            )

        code_frequency = client.fetch_stats(repo_context.full_name, "code_frequency") or []
        for week_unix, additions, deletions in code_frequency:
            code_frequency_rows.append(
                {
                    "owner": args.owner,
                    "repo_name": repo_context.repo_name,
                    "week_unix": week_unix,
                    "additions": additions,
                    "deletions": deletions,
                }
            )

        participation = client.fetch_stats(repo_context.full_name, "participation") or {}
        for index, (all_commits, owner_commits) in enumerate(
            zip(participation.get("all", []), participation.get("owner", []), strict=False)
        ):
            participation_rows.append(
                {
                    "owner": args.owner,
                    "repo_name": repo_context.repo_name,
                    "week_index": index,
                    "all_commits": all_commits,
                    "owner_commits": owner_commits,
                }
            )

        punch_card = client.fetch_stats(repo_context.full_name, "punch_card") or []
        for day_of_week, hour_of_day, commits in punch_card:
            punch_card_rows.append(
                {
                    "owner": args.owner,
                    "repo_name": repo_context.repo_name,
                    "day_of_week": day_of_week,
                    "hour_of_day": hour_of_day,
                    "commits": commits,
                }
            )

        if repo_context.default_branch:
            repo_path = clone_repo(repo_context, workspace_dir, args.token)
            try:
                file_rows.extend(collect_file_rows(repo_context, repo_path))
            finally:
                if not args.keep_clones and repo_path.exists():
                    remove_tree(repo_path)

    write_parquet("repos", repo_rows, output_dir)
    write_parquet("languages", language_rows, output_dir)
    write_parquet("contributors", contributor_rows, output_dir)
    write_parquet("commit_activity_weekly", commit_activity_rows, output_dir)
    write_parquet("code_frequency_weekly", code_frequency_rows, output_dir)
    write_parquet("participation_weekly", participation_rows, output_dir)
    write_parquet("punch_card", punch_card_rows, output_dir)
    write_parquet("files", file_rows, output_dir)
    write_parquet(
        "run_manifest",
        [
            {
                "owner": args.owner,
                "owner_type": owner_type,
                "repo_count": len(repos),
                "generated_at_unix": int(time.time()),
                "output_dir": str(output_dir),
            }
        ],
        output_dir,
    )

    print(f"Processed {len(repos)} repositories into {output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
