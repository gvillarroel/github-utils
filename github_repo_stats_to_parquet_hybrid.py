#!/usr/bin/env python
"""Hybrid exporter with early cutoff, incremental Parquet writes, and tree-first inventory."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from github_repo_stats_to_parquet import (
    REPOSITORIES_SCHEMA,
    GitHubApiError,
    GitHubClient,
    RepoContext,
    build_repo_row,
    clone_repo,
    collect_files,
    ensure_git_available,
    remove_tree,
    resolve_github_token,
)


GRAPHQL_URL = "https://api.github.com/graphql"
DISCOVERY_PAGE_SIZE = 100
GRAPHQL_BATCH_SIZE = 20

GRAPHQL_REPOS_BY_ID = """
query($ids: [ID!]!) {
  rateLimit {
    cost
    remaining
    resetAt
  }
  nodes(ids: $ids) {
    ... on Repository {
      databaseId
      name
      nameWithOwner
      isPrivate
      isFork
      isArchived
      isDisabled
      isTemplate
      visibility
      description
      homepageUrl
      createdAt
      updatedAt
      pushedAt
      diskUsage
      stargazerCount
      forkCount
      openIssues: issues(states: OPEN) {
        totalCount
      }
      watchers {
        totalCount
      }
      licenseInfo {
        key
        name
      }
      repositoryTopics(first: 100) {
        nodes {
          topic {
            name
          }
        }
      }
      defaultBranchRef {
        name
        target {
          ... on Commit {
            oid
          }
        }
      }
      primaryLanguage {
        name
      }
      languages(first: 100, orderBy: {field: SIZE, direction: DESC}) {
        edges {
          size
          node {
            name
          }
        }
      }
      hasIssuesEnabled
      hasProjectsEnabled
      hasWikiEnabled
      mirrorUrl
      url
      sshUrl
    }
  }
}
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner", required=True, help="GitHub user or organization login.")
    parser.add_argument("--owner-type", choices=("auto", "user", "org"), default="auto")
    parser.add_argument("--output-dir", default="output-hybrid")
    parser.add_argument("--token", default=None)
    parser.add_argument("--workspace-dir", default=str(Path(tempfile.gettempdir()) / "gh-repo-stats"))
    parser.add_argument("--updated-since", required=True, help="UTC date in YYYY-MM-DD format.")
    parser.add_argument("--max-repos", type=int, default=None)
    parser.add_argument("--include-archived", action="store_true")
    parser.add_argument("--keep-clones", action="store_true")
    parser.add_argument(
        "--inventory-mode",
        choices=("tree-only", "tree-then-clone"),
        default="tree-only",
        help="Use tree-only for fast path/size inventory, or tree-then-clone to also compute line counts.",
    )
    return parser.parse_args()


class GraphQLBatchClient:
    def __init__(self, token: str, timeout: int = 60) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "User-Agent": "github-repo-stats-hybrid",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )

    def query(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        response = self.session.post(GRAPHQL_URL, json={"query": query, "variables": variables}, timeout=self.timeout)
        if response.status_code != 200:
            raise GitHubApiError(f"GitHub GraphQL request failed: {response.status_code}: {response.text[:500]}")
        body = response.json()
        if body.get("errors"):
            raise GitHubApiError(f"GitHub GraphQL query returned errors: {body['errors']}")
        return body["data"]

    def fetch_repos(self, node_ids: list[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        data = self.query(GRAPHQL_REPOS_BY_ID, {"ids": node_ids})
        repos = [item for item in data["nodes"] if item is not None]
        return repos, data["rateLimit"]


def parse_cutoff(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def normalize_graphql_repo(repo: dict[str, Any]) -> tuple[dict[str, Any], dict[str, int], str | None]:
    topics = [node["topic"]["name"] for node in repo["repositoryTopics"]["nodes"]]
    languages = {edge["node"]["name"]: edge["size"] for edge in repo["languages"]["edges"]}
    default_branch = repo["defaultBranchRef"]["name"] if repo["defaultBranchRef"] else None
    default_oid = None
    if repo["defaultBranchRef"] and repo["defaultBranchRef"]["target"]:
        default_oid = repo["defaultBranchRef"]["target"]["oid"]
    normalized = {
        "id": repo["databaseId"],
        "name": repo["name"],
        "full_name": repo["nameWithOwner"],
        "private": repo["isPrivate"],
        "fork": repo["isFork"],
        "archived": repo["isArchived"],
        "disabled": repo["isDisabled"],
        "is_template": repo["isTemplate"],
        "visibility": str(repo["visibility"]).lower(),
        "default_branch": default_branch,
        "description": repo["description"],
        "homepage": repo["homepageUrl"] or "",
        "language": repo["primaryLanguage"]["name"] if repo["primaryLanguage"] else None,
        "license": repo["licenseInfo"],
        "topics": topics,
        "created_at": repo["createdAt"],
        "updated_at": repo["updatedAt"],
        "pushed_at": repo["pushedAt"],
        "size": repo["diskUsage"],
        "stargazers_count": repo["stargazerCount"],
        "watchers_count": repo["stargazerCount"],
        "subscribers_count": repo["watchers"]["totalCount"],
        "forks_count": repo["forkCount"],
        "open_issues_count": repo["openIssues"]["totalCount"],
        "network_count": None,
        "has_issues": repo["hasIssuesEnabled"],
        "has_projects": repo["hasProjectsEnabled"],
        "has_downloads": None,
        "has_wiki": repo["hasWikiEnabled"],
        "has_pages": None,
        "has_discussions": None,
        "mirror_url": repo["mirrorUrl"],
        "allow_forking": True,
        "web_commit_signoff_required": False,
        "clone_url": f"https://github.com/{repo['nameWithOwner']}.git",
        "ssh_url": repo["sshUrl"],
        "html_url": repo["url"],
        "node_id": None,
    }
    return normalized, languages, default_oid


def discover_recent_repos(
    client: GitHubClient,
    owner: str,
    owner_type: str,
    cutoff: datetime,
    *,
    include_archived: bool,
    max_repos: int | None,
) -> list[dict[str, Any]]:
    path = f"/orgs/{owner}/repos" if owner_type == "org" else f"/users/{owner}/repos"
    params: dict[str, Any] = {"per_page": DISCOVERY_PAGE_SIZE, "sort": "updated", "direction": "desc"}
    if owner_type == "org":
        params["type"] = "all"
    else:
        params["type"] = "owner"

    url = f"{client.request('GET', path, params=params).url}"
    repos: list[dict[str, Any]] = []

    while url:
        response = client._send_request("GET", url)
        if response.status_code != 200:
            raise GitHubApiError(f"GitHub discovery request failed: {response.status_code}: {response.text[:500]}")
        batch = response.json()
        if not isinstance(batch, list):
            raise GitHubApiError("Expected list while discovering repositories.")

        stop = False
        for repo in batch:
            updated_at = datetime.fromisoformat(repo["updated_at"].replace("Z", "+00:00"))
            if updated_at < cutoff:
                stop = True
                break
            if repo.get("archived") and not include_archived:
                continue
            repos.append(repo)
            if max_repos is not None and len(repos) >= max_repos:
                return repos

        if stop:
            break
        url = response.links.get("next", {}).get("url")

    return repos


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


def append_row(writer: pq.ParquetWriter | None, row: dict[str, Any], output_path: Path) -> pq.ParquetWriter:
    table = pa.Table.from_pylist([row], schema=REPOSITORIES_SCHEMA)
    if writer is None:
        writer = pq.ParquetWriter(output_path, REPOSITORIES_SCHEMA)
    writer.write_table(table)
    return writer


def main() -> int:
    args = parse_args()
    ensure_git_available()
    token = resolve_github_token(args.token)
    if not token:
        raise RuntimeError("Hybrid exporter requires GitHub authentication.")

    cutoff = parse_cutoff(args.updated_since)
    rest_client = GitHubClient(token=token)
    owner_type = args.owner_type
    if owner_type == "auto":
        owner_type = rest_client.resolve_owner_type(args.owner)

    discovered = discover_recent_repos(
        rest_client,
        args.owner,
        owner_type,
        cutoff,
        include_archived=args.include_archived,
        max_repos=args.max_repos,
    )
    print(f"Discovered {len(discovered)} repositories updated since {args.updated_since}.", file=sys.stderr)

    gql = GraphQLBatchClient(token)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "repositories.parquet"
    if output_path.exists():
        output_path.unlink()
    writer: pq.ParquetWriter | None = None

    workspace_dir = Path(args.workspace_dir).resolve()

    for index in range(0, len(discovered), GRAPHQL_BATCH_SIZE):
        batch = discovered[index : index + GRAPHQL_BATCH_SIZE]
        batch_by_id = {repo["node_id"]: repo for repo in batch}
        gql_repos, _rate = gql.fetch_repos([repo["node_id"] for repo in batch])

        for gql_repo in gql_repos:
            normalized, languages, default_oid = normalize_graphql_repo(gql_repo)
            rest_repo = batch_by_id[gql_repo.get("id")] if gql_repo.get("id") in batch_by_id else None
            if rest_repo is None:
                for candidate in batch:
                    if candidate["full_name"] == normalized["full_name"]:
                        rest_repo = candidate
                        break
            if rest_repo is None:
                continue

            repo_context = RepoContext(
                repo_id=normalized["id"],
                owner=args.owner,
                owner_type=owner_type,
                repo_name=normalized["name"],
                full_name=normalized["full_name"],
                default_branch=normalized["default_branch"],
                clone_url=normalized["clone_url"],
            )

            files: list[dict[str, Any]] = []
            total_file_size_bytes = 0
            total_line_count = 0
            binary_file_count = 0
            file_inventory_ready = False
            file_inventory_error: str | None = None

            if default_oid:
                tree_files = fetch_tree_files(rest_client, normalized["full_name"], default_oid)
                if tree_files is not None:
                    files = tree_files
                    total_file_size_bytes = sum(int(item.get("size_bytes") or 0) for item in files)
                    file_inventory_ready = True
                    if args.inventory_mode == "tree-only":
                        file_inventory_error = "Tree API mode: line_count is not computed."
                    else:
                        file_inventory_error = "Tree API provided path and size; line_count requires clone fallback."

            if repo_context.default_branch and args.inventory_mode == "tree-then-clone":
                try:
                    repo_path = clone_repo(repo_context, workspace_dir, token)
                except Exception as exc:
                    if not files:
                        file_inventory_error = str(exc)[:500]
                else:
                    try:
                        files, total_file_size_bytes, total_line_count, binary_file_count = collect_files(repo_path)
                        file_inventory_ready = True
                        file_inventory_error = None
                    finally:
                        if not args.keep_clones and repo_path.exists():
                            remove_tree(repo_path)

            row = build_repo_row(
                owner=args.owner,
                owner_type=owner_type,
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
                total_file_size_bytes=total_file_size_bytes,
                total_line_count=total_line_count,
                binary_file_count=binary_file_count,
            )
            writer = append_row(writer, row, output_path)

    if writer is not None:
        writer.close()
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
