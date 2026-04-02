#!/usr/bin/env python
"""Export one Parquet row per GitHub repository with nested file inventory."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import stat
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit, urlunsplit

import pyarrow as pa
import pyarrow.parquet as pq
import requests


API_BASE_URL = "https://api.github.com"
DEFAULT_STATS_RETRIES = 2
DEFAULT_STATS_DELAY = 1.0
TEXT_FILE_SIZE_LIMIT = 5 * 1024 * 1024
DEFAULT_RATE_LIMIT_BUFFER_SECONDS = 2
DEFAULT_HTTP_RETRIES = 5
OUTPUT_FORMAT_CHOICES = ("parquet", "csv", "jsonl")
INVENTORY_MODE_CHOICES = ("clone", "tree-only", "tree-then-clone")


FILES_TYPE = pa.list_(
    pa.struct(
        [
            ("path", pa.string()),
            ("size_bytes", pa.int64()),
            ("line_count", pa.int64()),
        ]
    )
)

LANGUAGES_TYPE = pa.list_(
    pa.struct(
        [
            ("language", pa.string()),
            ("bytes", pa.int64()),
        ]
    )
)

CONTRIBUTORS_TYPE = pa.list_(
    pa.struct(
        [
            ("login", pa.string()),
            ("user_id", pa.int64()),
            ("contributions", pa.int64()),
            ("type", pa.string()),
            ("site_admin", pa.bool_()),
        ]
    )
)

REPOSITORIES_SCHEMA = pa.schema(
    [
        ("owner", pa.string()),
        ("owner_type", pa.string()),
        ("executed_at", pa.string()),
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
        ("languages", LANGUAGES_TYPE),
        ("contributor_count", pa.int64()),
        ("total_contributions", pa.int64()),
        ("contributors", CONTRIBUTORS_TYPE),
        ("commit_activity_json", pa.string()),
        ("code_frequency_json", pa.string()),
        ("participation_json", pa.string()),
        ("punch_card_json", pa.string()),
        ("file_inventory_ready", pa.bool_()),
        ("file_inventory_error", pa.string()),
        ("file_count", pa.int64()),
        ("total_file_size_bytes", pa.int64()),
        ("total_line_count", pa.int64()),
        ("binary_file_count", pa.int64()),
        ("files", FILES_TYPE),
    ]
)


class GitHubApiError(RuntimeError):
    """Raised when a GitHub API request fails."""


@dataclass
class RepoContext:
    repo_id: int
    owner: str
    owner_type: str
    repo_name: str
    full_name: str
    default_branch: str | None
    clone_url: str


class GitHubClient:
    """Thin GitHub REST client with pagination, retries, and rate-limit handling."""

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

    def _send_request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        for attempt in range(DEFAULT_HTTP_RETRIES):
            try:
                response = self.session.request(method, url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt == DEFAULT_HTTP_RETRIES - 1:
                    raise GitHubApiError(f"GitHub request failed after retries: {method} {url}: {exc}") from exc
                time.sleep(2**attempt)
                continue

            if response.status_code in {403, 429} and response.headers.get("X-RateLimit-Remaining") == "0":
                reset_at = int(response.headers.get("X-RateLimit-Reset", "0") or "0")
                wait_seconds = max(0, reset_at - int(time.time()) + DEFAULT_RATE_LIMIT_BUFFER_SECONDS)
                if wait_seconds > 0:
                    print(
                        f"GitHub rate limit reached. Waiting {wait_seconds} seconds for reset...",
                        file=sys.stderr,
                    )
                    time.sleep(wait_seconds)
                else:
                    time.sleep(DEFAULT_RATE_LIMIT_BUFFER_SECONDS)
                continue
            return response
        raise GitHubApiError(f"GitHub request failed after retries: {method} {url}")

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        expected_statuses: Iterable[int] = (200,),
    ) -> requests.Response:
        url = f"{API_BASE_URL}{path}"
        response = self._send_request(method, url, params=params)
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
            response = self._send_request("GET", url, params=merged_params)
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

    def fetch_rate_limit(self) -> dict[str, Any]:
        return self.request("GET", "/rate_limit").json()

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
            raise GitHubApiError(f"Expected a list response from contributors endpoint for {full_name!r}.")

        next_url = response.links.get("next", {}).get("url")
        while next_url:
            page = self._send_request("GET", next_url)
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
        default=str(Path(tempfile.gettempdir()) / "gh-repo-stats"),
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
        "--include-contributors",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fetch contributors for each repository. Default: true.",
    )
    parser.add_argument(
        "--include-stats",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fetch GitHub stats endpoints for each repository. Default: true.",
    )
    parser.add_argument(
        "--inventory-mode",
        choices=INVENTORY_MODE_CHOICES,
        default="clone",
        help="File inventory strategy. 'clone' is exact, 'tree-only' is fastest, 'tree-then-clone' uses the tree API first and clones only for line counts.",
    )
    return parser.parse_args()


def ensure_git_available() -> None:
    if shutil.which("git") is None:
        raise RuntimeError("git is required but was not found in PATH.")


def resolve_github_token(cli_token: str | None) -> str | None:
    if cli_token:
        return cli_token

    env_token = os.environ.get("GITHUB_TOKEN")
    if env_token:
        return env_token

    if shutil.which("gh") is None:
        return None

    result = subprocess.run(
        ["gh", "auth", "token"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    token = result.stdout.strip()
    return token or None


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
        "--depth",
        "1",
        "--single-branch",
        authenticated_clone_url(repo.clone_url, token),
        str(repo_path),
    ]

    if repo.default_branch:
        branch_command = base_command.copy()
        branch_command[6:6] = ["--branch", repo.default_branch]
        try:
            subprocess.run(
                branch_command,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            return repo_path
        except subprocess.CalledProcessError as exc:
            if "Remote branch" not in (exc.stderr or ""):
                raise
            if repo_path.exists():
                remove_tree(repo_path)

    subprocess.run(
        base_command,
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


def collect_files(repo_path: Path) -> tuple[list[dict[str, Any]], int, int, int]:
    files: list[dict[str, Any]] = []
    total_file_size_bytes = 0
    total_line_count = 0
    binary_file_count = 0

    for file_path in repo_path.rglob("*"):
        if not file_path.is_file():
            continue
        relative_path = file_path.relative_to(repo_path).as_posix()
        if relative_path.startswith(".git/"):
            continue

        size_bytes = file_path.stat().st_size
        line_count, is_binary = count_lines(file_path)
        total_file_size_bytes += size_bytes
        if line_count is not None:
            total_line_count += line_count
        if is_binary:
            binary_file_count += 1

        files.append(
            {
                "path": relative_path,
                "size_bytes": size_bytes,
                "line_count": line_count,
            }
        )

    return files, total_file_size_bytes, total_line_count, binary_file_count


def fetch_tree_files(client: GitHubClient, full_name: str, tree_sha: str) -> list[dict[str, Any]] | None:
    response = client.request(
        "GET",
        f"/repos/{full_name}/git/trees/{tree_sha}",
        params={"recursive": "1"},
        expected_statuses=(200, 404, 409, 422),
    )
    if response.status_code != 200:
        return None
    payload = response.json()
    if payload.get("truncated"):
        return None
    files: list[dict[str, Any]] = []
    for item in payload.get("tree", []):
        if item.get("type") != "blob":
            continue
        files.append(
            {
                "path": item["path"],
                "size_bytes": item.get("size"),
                "line_count": None,
            }
        )
    return files


def fetch_default_branch_tree_oid(client: GitHubClient, full_name: str, default_branch: str) -> str | None:
    response = client.request(
        "GET",
        f"/repos/{full_name}/branches/{default_branch}",
        expected_statuses=(200, 404, 409),
    )
    if response.status_code != 200:
        return None
    payload = response.json()
    commit = payload.get("commit") or {}
    return commit.get("sha")


def collect_repo_inventory(
    client: GitHubClient,
    repo: RepoContext,
    *,
    inventory_mode: str,
    workspace_dir: Path,
    token: str | None,
    keep_clones: bool,
) -> tuple[list[dict[str, Any]], int, int, int, bool, str | None]:
    files: list[dict[str, Any]] = []
    total_file_size_bytes = 0
    total_line_count = 0
    binary_file_count = 0
    file_inventory_ready = False
    file_inventory_error: str | None = None

    if not repo.default_branch:
        return files, total_file_size_bytes, total_line_count, binary_file_count, False, "Repository has no default branch."

    if inventory_mode != "clone":
        default_oid = fetch_default_branch_tree_oid(client, repo.full_name, repo.default_branch)
        if default_oid:
            tree_files = fetch_tree_files(client, repo.full_name, default_oid)
            if tree_files is not None:
                files = tree_files
                total_file_size_bytes = sum(int(item.get("size_bytes") or 0) for item in files)
                file_inventory_ready = True
                if inventory_mode == "tree-only":
                    file_inventory_error = "Tree API mode: line_count is not computed."
                    return (
                        files,
                        total_file_size_bytes,
                        total_line_count,
                        binary_file_count,
                        file_inventory_ready,
                        file_inventory_error,
                    )
                file_inventory_error = "Tree API provided path and size; line_count requires clone fallback."
            else:
                file_inventory_error = "Tree API inventory is unavailable or truncated for this repository."
        else:
            file_inventory_error = "Default branch exists, but its commit OID is not available from REST."

    try:
        repo_path = clone_repo(repo, workspace_dir, token)
    except subprocess.CalledProcessError as exc:
        file_inventory_error = (exc.stderr or "").strip()[:500]
        return files, total_file_size_bytes, total_line_count, binary_file_count, file_inventory_ready, file_inventory_error

    try:
        files, total_file_size_bytes, total_line_count, binary_file_count = collect_files(repo_path)
        file_inventory_ready = True
        file_inventory_error = None
    finally:
        if not keep_clones and repo_path.exists():
            remove_tree(repo_path)

    return files, total_file_size_bytes, total_line_count, binary_file_count, file_inventory_ready, file_inventory_error


def write_repositories_parquet(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "repositories.parquet"
    table = pa.Table.from_pylist(rows, schema=REPOSITORIES_SCHEMA)
    pq.write_table(table, output_path)
    return output_path


def output_filename(output_format: str) -> str:
    if output_format == "parquet":
        return "repositories.parquet"
    if output_format == "csv":
        return "repositories.csv"
    if output_format == "jsonl":
        return "repositories.jsonl"
    raise ValueError(f"Unsupported output format: {output_format}")


def serialize_row_for_text_output(row: dict[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, (list, dict)):
            serialized[key] = json.dumps(value, separators=(",", ":"))
        else:
            serialized[key] = value
    return serialized


def write_repositories_csv(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "repositories.csv"
    fieldnames = [field.name for field in REPOSITORIES_SCHEMA]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
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


def minimum_required_requests(
    repo_count: int,
    *,
    include_contributors: bool,
    include_stats: bool,
    inventory_mode: str,
) -> int:
    per_repo = 1
    if include_contributors:
        per_repo += 1
    if include_stats:
        per_repo += 4
    if inventory_mode != "clone":
        per_repo += 2
    return repo_count * per_repo


def normalize_contributors(contributors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "login": item.get("login"),
            "user_id": item.get("id"),
            "contributions": item.get("contributions"),
            "type": item.get("type"),
            "site_admin": item.get("site_admin"),
        }
        for item in contributors
    ]


def contributor_summary(contributors: list[dict[str, Any]]) -> tuple[int, int, list[dict[str, Any]]]:
    contributor_count = len(contributors)
    total_contributions = sum(int(item.get("contributions") or 0) for item in contributors)
    return contributor_count, total_contributions, normalize_contributors(contributors)


def normalize_languages(languages: dict[str, int]) -> list[dict[str, Any]]:
    return [{"language": language, "bytes": byte_count} for language, byte_count in languages.items()]


def build_repo_row(
    owner: str,
    owner_type: str,
    executed_at: str,
    repo: dict[str, Any],
    languages: dict[str, int],
    contributors: list[dict[str, Any]],
    commit_activity: Any,
    code_frequency: Any,
    participation: Any,
    punch_card: Any,
    files: list[dict[str, Any]],
    file_inventory_ready: bool,
    file_inventory_error: str | None,
    total_file_size_bytes: int,
    total_line_count: int,
    binary_file_count: int,
) -> dict[str, Any]:
    license_info = repo.get("license") or {}
    contributor_count, total_contributions, normalized_contributors = contributor_summary(contributors)
    return {
        "owner": owner,
        "owner_type": owner_type,
        "executed_at": executed_at,
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
        "topics_json": json.dumps(repo.get("topics", []), separators=(",", ":")),
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
        "languages": normalize_languages(languages),
        "contributor_count": contributor_count,
        "total_contributions": total_contributions,
        "contributors": normalized_contributors,
        "commit_activity_json": json.dumps(commit_activity or [], separators=(",", ":")),
        "code_frequency_json": json.dumps(code_frequency or [], separators=(",", ":")),
        "participation_json": json.dumps(participation or {}, separators=(",", ":")),
        "punch_card_json": json.dumps(punch_card or [], separators=(",", ":")),
        "file_inventory_ready": file_inventory_ready,
        "file_inventory_error": file_inventory_error,
        "file_count": len(files),
        "total_file_size_bytes": total_file_size_bytes,
        "total_line_count": total_line_count,
        "binary_file_count": binary_file_count,
        "files": files,
    }


def main() -> int:
    args = parse_args()
    ensure_git_available()
    token = resolve_github_token(args.token)

    client = GitHubClient(token=token)
    owner_type = args.owner_type
    if owner_type == "auto":
        owner_type = client.resolve_owner_type(args.owner)

    output_dir = Path(args.output_dir).resolve()
    workspace_dir = Path(args.workspace_dir).resolve()
    executed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    repos = client.list_repos(args.owner, owner_type)
    if not args.include_archived:
        repos = [repo for repo in repos if not repo.get("archived")]
    if args.max_repos is not None:
        repos = repos[: args.max_repos]

    rate_limit = client.fetch_rate_limit()["rate"]
    required_requests = minimum_required_requests(
        len(repos),
        include_contributors=args.include_contributors,
        include_stats=args.include_stats,
        inventory_mode=args.inventory_mode,
    )
    remaining_requests = int(rate_limit["remaining"])
    if remaining_requests < required_requests:
        reset_at = int(rate_limit["reset"])
        reset_in_seconds = max(0, reset_at - int(time.time()))
        raise RuntimeError(
            "Not enough GitHub API quota to run safely. "
            f"Remaining core requests: {remaining_requests}. "
            f"Estimated minimum required: {required_requests}. "
            f"Retry after about {reset_in_seconds} seconds or provide GitHub authentication."
        )

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
        contributors: list[dict[str, Any]] = []
        if args.include_contributors:
            contributors = client.fetch_contributors(repo_context.full_name)

        commit_activity = None
        code_frequency = None
        participation = None
        punch_card = None
        if args.include_stats:
            commit_activity = client.fetch_stats(repo_context.full_name, "commit_activity")
            code_frequency = client.fetch_stats(repo_context.full_name, "code_frequency")
            participation = client.fetch_stats(repo_context.full_name, "participation")
            punch_card = client.fetch_stats(repo_context.full_name, "punch_card")

        (
            files,
            total_file_size_bytes,
            total_line_count,
            binary_file_count,
            file_inventory_ready,
            file_inventory_error,
        ) = collect_repo_inventory(
            client,
            repo_context,
            inventory_mode=args.inventory_mode,
            workspace_dir=workspace_dir,
            token=token,
            keep_clones=args.keep_clones,
        )

        repository_rows.append(
            build_repo_row(
                owner=args.owner,
                owner_type=owner_type,
                executed_at=executed_at,
                repo=repo_summary,
                languages=languages,
                contributors=contributors,
                commit_activity=commit_activity,
                code_frequency=code_frequency,
                participation=participation,
                punch_card=punch_card,
                files=files,
                file_inventory_ready=file_inventory_ready,
                file_inventory_error=file_inventory_error,
                total_file_size_bytes=total_file_size_bytes,
                total_line_count=total_line_count,
                binary_file_count=binary_file_count,
            )
        )

    output_path = write_repositories(repository_rows, output_dir, args.output_format)
    print(f"Processed {len(repos)} repositories into {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
