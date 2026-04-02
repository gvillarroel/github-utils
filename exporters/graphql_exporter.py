#!/usr/bin/env python
"""Alternative exporter using GraphQL for repository metadata and languages."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from exporters.core import (
    API_BASE_URL,
    DEFAULT_HTTP_RETRIES,
    DEFAULT_RATE_LIMIT_BUFFER_SECONDS,
    GitHubApiError,
    GitHubClient,
    OUTPUT_FORMAT_CHOICES,
    RepoContext,
    build_repo_row,
    collect_repo_inventory,
    ensure_git_available,
    INVENTORY_MODE_CHOICES,
    resolve_github_token,
    write_repositories,
)


GRAPHQL_URL = "https://api.github.com/graphql"
GRAPHQL_PAGE_SIZE = 100
GRAPHQL_SECONDARY_LIMIT_SLEEP_SECONDS = 60


ORG_QUERY = """
query($owner: String!, $first: Int!, $after: String) {
  rateLimit {
    cost
    remaining
    resetAt
  }
  organization(login: $owner) {
    repositories(first: $first, after: $after, orderBy: {field: NAME, direction: ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
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
}
"""


USER_QUERY = """
query($owner: String!, $first: Int!, $after: String) {
  rateLimit {
    cost
    remaining
    resetAt
  }
  user(login: $owner) {
    repositories(first: $first, after: $after, orderBy: {field: NAME, direction: ASC}, ownerAffiliations: OWNER) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
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
}
"""


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
        default="output-graphql",
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
        default=None,
        help="Directory used for temporary clones. Defaults to the main script default.",
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
    parser.add_argument(
        "--include-contributors",
        action="store_true",
        help="Fetch contributors for each repository.",
    )
    parser.add_argument(
        "--include-stats",
        action="store_true",
        help="Fetch GitHub stats endpoints for each repository.",
    )
    parser.add_argument(
        "--inventory-mode",
        choices=INVENTORY_MODE_CHOICES,
        default="clone",
        help="File inventory strategy. 'clone' is exact, 'tree-only' is fastest, 'tree-then-clone' uses the tree API first and clones only for line counts.",
    )
    return parser.parse_args()


class GitHubGraphQLClient:
    def __init__(self, token: str, timeout: int = 60) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "User-Agent": "github-repo-stats-to-parquet-graphql",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )

    def query(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        payload = {"query": query, "variables": variables}
        for attempt in range(DEFAULT_HTTP_RETRIES):
            try:
                response = self.session.post(GRAPHQL_URL, json=payload, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt == DEFAULT_HTTP_RETRIES - 1:
                    raise GitHubApiError(f"GitHub GraphQL request failed after retries: {exc}") from exc
                import time

                time.sleep(2**attempt)
                continue

            if response.status_code in {403, 429} and response.headers.get("X-RateLimit-Remaining") == "0":
                import time
                from datetime import datetime, timezone

                reset_value = response.headers.get("X-RateLimit-Reset")
                if reset_value:
                    reset_at = int(reset_value)
                else:
                    reset_at = int(datetime.now(timezone.utc).timestamp()) + DEFAULT_RATE_LIMIT_BUFFER_SECONDS
                wait_seconds = max(
                    0,
                    reset_at - int(datetime.now(timezone.utc).timestamp()) + DEFAULT_RATE_LIMIT_BUFFER_SECONDS,
                )
                time.sleep(wait_seconds or DEFAULT_RATE_LIMIT_BUFFER_SECONDS)
                continue

            if response.status_code == 403 and "secondary rate limit" in response.text.lower():
                import time

                sleep_seconds = GRAPHQL_SECONDARY_LIMIT_SLEEP_SECONDS * (attempt + 1)
                print(
                    f"GitHub GraphQL secondary rate limit reached. Waiting {sleep_seconds} seconds...",
                    file=sys.stderr,
                )
                time.sleep(sleep_seconds)
                continue

            if response.status_code == 504 and attempt < DEFAULT_HTTP_RETRIES - 1:
                import time

                time.sleep(5 * (attempt + 1))
                continue

            if response.status_code != 200:
                raise GitHubApiError(
                    f"GitHub GraphQL request failed: {response.status_code}: {response.text[:500]}"
                )

            body = response.json()
            if body.get("errors"):
                raise GitHubApiError(f"GitHub GraphQL query returned errors: {body['errors']}")
            return body["data"]

        raise GitHubApiError("GitHub GraphQL request failed after retries.")

    def list_repos(
        self,
        owner: str,
        owner_type: str,
        *,
        max_repos: int | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        query = ORG_QUERY if owner_type == "org" else USER_QUERY
        all_repos: list[dict[str, Any]] = []
        after: str | None = None
        last_rate_limit: dict[str, Any] = {}

        while True:
            page_size = GRAPHQL_PAGE_SIZE
            if max_repos is not None:
                page_size = max(1, min(GRAPHQL_PAGE_SIZE, max_repos - len(all_repos)))
            data = self.query(query, {"owner": owner, "first": page_size, "after": after})
            last_rate_limit = data["rateLimit"]
            owner_node = data["organization"] if owner_type == "org" else data["user"]
            if owner_node is None:
                raise GitHubApiError(f"Owner {owner!r} not found in GraphQL.")

            repos = owner_node["repositories"]["nodes"]
            all_repos.extend(repos)
            if max_repos is not None and len(all_repos) >= max_repos:
                all_repos = all_repos[:max_repos]
                break
            page_info = owner_node["repositories"]["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            after = page_info["endCursor"]

        return all_repos, last_rate_limit


def normalize_graphql_repo(owner: str, owner_type: str, repo: dict[str, Any]) -> tuple[dict[str, Any], dict[str, int]]:
    topics = [node["topic"]["name"] for node in repo["repositoryTopics"]["nodes"]]
    languages = {edge["node"]["name"]: edge["size"] for edge in repo["languages"]["edges"]}
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
        "default_branch": repo["defaultBranchRef"]["name"] if repo["defaultBranchRef"] else None,
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
    }
    return normalized, languages


def estimate_rest_core_requests(
    repo_count: int,
    *,
    include_contributors: bool,
    include_stats: bool,
    inventory_mode: str,
) -> int:
    per_repo = 0
    if include_contributors:
        per_repo += 1
    if include_stats:
        per_repo += 4
    if inventory_mode != "clone":
        per_repo += 2
    return repo_count * per_repo


def main() -> int:
    args = parse_args()
    ensure_git_available()
    token = resolve_github_token(args.token)
    if not token:
        raise RuntimeError("The GraphQL exporter requires GitHub authentication.")

    rest_client = GitHubClient(token=token)
    owner_type = args.owner_type
    if owner_type == "auto":
        owner_type = rest_client.resolve_owner_type(args.owner)

    graphql_client = GitHubGraphQLClient(token=token)
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
    required_core_requests = estimate_rest_core_requests(
        len(normalized_repos),
        include_contributors=args.include_contributors,
        include_stats=args.include_stats,
        inventory_mode=args.inventory_mode,
    )
    remaining_core_requests = int(rest_rate["remaining"])
    if remaining_core_requests < required_core_requests:
        raise RuntimeError(
            "Not enough GitHub REST core quota to run safely. "
            f"Remaining core requests: {remaining_core_requests}. "
            f"Estimated minimum required: {required_core_requests}. "
            f"GraphQL remaining: {graphql_rate['remaining']}."
        )

    output_dir = Path(args.output_dir).resolve()
    workspace_dir = Path(args.workspace_dir).resolve() if args.workspace_dir else None
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

        contributors: list[dict[str, Any]] = []
        if args.include_contributors:
            contributors = rest_client.fetch_contributors(repo_context.full_name)

        commit_activity = None
        code_frequency = None
        participation = None
        punch_card = None
        if args.include_stats:
            commit_activity = rest_client.fetch_stats(repo_context.full_name, "commit_activity")
            code_frequency = rest_client.fetch_stats(repo_context.full_name, "code_frequency")
            participation = rest_client.fetch_stats(repo_context.full_name, "participation")
            punch_card = rest_client.fetch_stats(repo_context.full_name, "punch_card")

        (
            files,
            total_file_size_bytes,
            total_line_count,
            binary_file_count,
            file_inventory_ready,
            file_inventory_error,
        ) = collect_repo_inventory(
            rest_client,
            repo_context,
            inventory_mode=args.inventory_mode,
            workspace_dir=workspace_dir or Path(),
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
    print(f"Processed {len(repository_rows)} repositories into {output_path}")
    print(
        "Estimated REST core requests per repo path: "
        f"{estimate_rest_core_requests(len(repository_rows), include_contributors=args.include_contributors, include_stats=args.include_stats, inventory_mode=args.inventory_mode)}; "
        f"GraphQL remaining: {graphql_rate['remaining']}.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
