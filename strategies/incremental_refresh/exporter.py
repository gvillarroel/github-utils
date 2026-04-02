#!/usr/bin/env python
"""Incremental exporter that reuses stored rows and refreshes only changed repositories."""

from __future__ import annotations

import argparse
import copy
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exporters.core import (  # noqa: E402
    GitHubApiError,
    GitHubClient,
    INVENTORY_MODE_CHOICES,
    OUTPUT_FORMAT_CHOICES,
    RepoContext,
    build_repo_row,
    collect_repo_inventory,
    ensure_git_available,
    output_filename,
    resolve_github_token,
    write_repositories,
)


DEFAULT_STATE_FILENAME = "incremental_refresh_state.json"


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
        default="output-incremental-refresh",
        help="Directory where the exported dataset will be written.",
    )
    parser.add_argument(
        "--output-format",
        choices=OUTPUT_FORMAT_CHOICES,
        default="parquet",
        help="Output format. Default: parquet.",
    )
    parser.add_argument(
        "--state-path",
        default=None,
        help="Path to the persisted refresh state. Defaults to <output-dir>/incremental_refresh_state.json.",
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
        default="tree-only",
        help="Inventory mode. tree-only is fastest; tree-then-clone computes line counts; clone is exact.",
    )
    parser.add_argument(
        "--rebuild-state",
        action="store_true",
        help="Ignore any existing state and rebuild the baseline from scratch.",
    )
    return parser.parse_args()


def parse_repo_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def default_state_path(output_dir: Path) -> Path:
    return output_dir / DEFAULT_STATE_FILENAME


def load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {
            "version": 1,
            "owner": None,
            "owner_type": None,
            "settings": {},
            "created_at": None,
            "updated_at": None,
            "repos": {},
        }
    with state_path.open("r", encoding="utf-8") as handle:
        state = json.load(handle)
    if not isinstance(state, dict):
        raise RuntimeError(f"Invalid state file: {state_path}")
    state.setdefault("version", 1)
    state.setdefault("repos", {})
    state.setdefault("settings", {})
    return state


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    temp_path.replace(path)


def normalize_repo_settings(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "include_contributors": bool(args.include_contributors),
        "include_stats": bool(args.include_stats),
        "inventory_mode": args.inventory_mode,
    }


def repo_fresh_enough(
    stored: dict[str, Any] | None,
    current_head_sha: str | None,
    current_settings: dict[str, Any],
) -> bool:
    if not stored:
        return False
    if stored.get("head_sha") != current_head_sha:
        return False
    if stored.get("settings") != current_settings:
        return False
    return bool(stored.get("row"))


def fetch_default_branch_head_sha(client: GitHubClient, full_name: str, default_branch: str | None) -> str | None:
    if not default_branch:
        return None
    response = client.request("GET", f"/repos/{full_name}/branches/{default_branch}", expected_statuses=(200, 404, 409))
    if response.status_code != 200:
        return None
    payload = response.json()
    commit = payload.get("commit") or {}
    return commit.get("sha")


def fetch_compare_changed_paths(
    client: GitHubClient,
    full_name: str,
    base_sha: str,
    head_sha: str,
) -> dict[str, Any] | None:
    response = client.request(
        "GET",
        f"/repos/{full_name}/compare/{base_sha}...{head_sha}",
        expected_statuses=(200, 404, 409, 422),
    )
    if response.status_code != 200:
        return None
    payload = response.json()
    files = payload.get("files") or []
    changed_paths = [item.get("filename") for item in files if item.get("filename")]
    return {
        "status": payload.get("status"),
        "ahead_by": payload.get("ahead_by"),
        "behind_by": payload.get("behind_by"),
        "total_commits": payload.get("total_commits"),
        "changed_file_count": len(changed_paths),
        "changed_paths": changed_paths,
    }


def refreshed_row_from_state(
    stored_row: dict[str, Any],
    repo_summary: dict[str, Any],
    *,
    owner: str,
    owner_type: str,
    executed_at: str,
) -> dict[str, Any]:
    row = copy.deepcopy(stored_row)
    license_info = repo_summary.get("license") or {}
    row.update(
        {
            "owner": owner,
            "owner_type": owner_type,
            "executed_at": executed_at,
            "repo_name": repo_summary["name"],
            "full_name": repo_summary["full_name"],
            "private": repo_summary.get("private"),
            "fork": repo_summary.get("fork"),
            "archived": repo_summary.get("archived"),
            "disabled": repo_summary.get("disabled"),
            "is_template": repo_summary.get("is_template"),
            "visibility": repo_summary.get("visibility"),
            "default_branch": repo_summary.get("default_branch"),
            "description": repo_summary.get("description"),
            "homepage": repo_summary.get("homepage"),
            "language": repo_summary.get("language"),
            "license_key": license_info.get("key"),
            "license_name": license_info.get("name"),
            "topics_json": json.dumps(repo_summary.get("topics", []), separators=(",", ":")),
            "created_at": repo_summary.get("created_at"),
            "updated_at": repo_summary.get("updated_at"),
            "pushed_at": repo_summary.get("pushed_at"),
            "size_kib": repo_summary.get("size"),
            "stargazers_count": repo_summary.get("stargazers_count"),
            "watchers_count": repo_summary.get("watchers_count"),
            "subscribers_count": repo_summary.get("subscribers_count"),
            "forks_count": repo_summary.get("forks_count"),
            "open_issues_count": repo_summary.get("open_issues_count"),
            "network_count": repo_summary.get("network_count"),
            "has_issues": repo_summary.get("has_issues"),
            "has_projects": repo_summary.get("has_projects"),
            "has_downloads": repo_summary.get("has_downloads"),
            "has_wiki": repo_summary.get("has_wiki"),
            "has_pages": repo_summary.get("has_pages"),
            "has_discussions": repo_summary.get("has_discussions"),
            "mirror_url": repo_summary.get("mirror_url"),
            "allow_forking": repo_summary.get("allow_forking"),
            "web_commit_signoff_required": repo_summary.get("web_commit_signoff_required"),
            "clone_url": repo_summary.get("clone_url"),
            "ssh_url": repo_summary.get("ssh_url"),
            "html_url": repo_summary.get("html_url"),
        }
    )
    return row


def build_fresh_row(
    client: GitHubClient,
    repo_context: RepoContext,
    repo_summary: dict[str, Any],
    *,
    executed_at: str,
    include_contributors: bool,
    include_stats: bool,
    inventory_mode: str,
    workspace_dir: Path,
    token: str | None,
    keep_clones: bool,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    languages = client.fetch_languages(repo_context.full_name)
    contributors: list[dict[str, Any]] = []
    if include_contributors:
        contributors = client.fetch_contributors(repo_context.full_name)

    commit_activity = None
    code_frequency = None
    participation = None
    punch_card = None
    if include_stats:
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
        inventory_mode=inventory_mode,
        workspace_dir=workspace_dir,
        token=token,
        keep_clones=keep_clones,
    )

    row = build_repo_row(
        owner=repo_context.owner,
        owner_type=repo_context.owner_type,
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
    return row, {
        "inventory_mode": inventory_mode,
        "include_contributors": include_contributors,
        "include_stats": include_stats,
    }


def main() -> int:
    args = parse_args()
    ensure_git_available()
    token = resolve_github_token(args.token)
    if not token:
        raise RuntimeError("Incremental refresh requires GitHub authentication.")

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = Path(args.state_path).resolve() if args.state_path else default_state_path(output_dir)
    workspace_dir = Path(args.workspace_dir).resolve()
    executed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    client = GitHubClient(token=token)
    owner_type = args.owner_type
    if owner_type == "auto":
        owner_type = client.resolve_owner_type(args.owner)

    current_settings = normalize_repo_settings(args)
    state = load_state(state_path) if state_path.exists() and not args.rebuild_state else {
        "version": 1,
        "owner": args.owner,
        "owner_type": owner_type,
        "settings": current_settings,
        "created_at": executed_at,
        "updated_at": executed_at,
        "repos": {},
    }

    if state.get("owner") not in {None, args.owner} or state.get("owner_type") not in {None, owner_type}:
        raise RuntimeError(
            f"State file {state_path} belongs to owner={state.get('owner')!r} owner_type={state.get('owner_type')!r} "
            f"and cannot be reused for owner={args.owner!r} owner_type={owner_type!r}."
        )

    state["owner"] = args.owner
    state["owner_type"] = owner_type
    state["settings"] = current_settings
    state.setdefault("repos", {})

    repos = client.list_repos(args.owner, owner_type)
    if not args.include_archived:
        repos = [repo for repo in repos if not repo.get("archived")]
    if args.max_repos is not None:
        repos = repos[: args.max_repos]

    repository_rows: list[dict[str, Any]] = []
    refreshed_count = 0
    reused_count = 0

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
        repo_key = str(repo_context.repo_id)
        stored = state["repos"].get(repo_key)
        current_head_sha = fetch_default_branch_head_sha(client, repo_context.full_name, repo_context.default_branch)

        if repo_fresh_enough(stored, current_head_sha, current_settings):
            row = refreshed_row_from_state(
                stored["row"],
                repo_summary,
                owner=args.owner,
                owner_type=owner_type,
                executed_at=executed_at,
            )
            repository_rows.append(row)
            stored["row"] = row
            stored["head_sha"] = current_head_sha
            stored["repo_summary"] = repo_summary
            stored["executed_at"] = executed_at
            stored["settings"] = current_settings
            reused_count += 1
            continue

        compare_info = None
        if stored and stored.get("head_sha") and current_head_sha and stored.get("head_sha") != current_head_sha:
            compare_info = fetch_compare_changed_paths(client, repo_context.full_name, stored["head_sha"], current_head_sha)

        print(f"Refreshing {repo_context.full_name}...", file=sys.stderr)
        row, row_settings = build_fresh_row(
            client,
            repo_context,
            repo_summary,
            executed_at=executed_at,
            include_contributors=args.include_contributors,
            include_stats=args.include_stats,
            inventory_mode=args.inventory_mode,
            workspace_dir=workspace_dir,
            token=token,
            keep_clones=args.keep_clones,
        )
        repository_rows.append(row)
        state["repos"][repo_key] = {
            "repo_id": repo_context.repo_id,
            "full_name": repo_context.full_name,
            "default_branch": repo_context.default_branch,
            "head_sha": current_head_sha,
            "executed_at": executed_at,
            "settings": row_settings,
            "repo_summary": repo_summary,
            "row": row,
            "compare": compare_info,
        }
        refreshed_count += 1

    state["updated_at"] = executed_at
    state["last_run"] = {
        "executed_at": executed_at,
        "repository_count": len(repository_rows),
        "reused_count": reused_count,
        "refreshed_count": refreshed_count,
    }
    atomic_write_json(state_path, state)

    output_path = write_repositories(repository_rows, output_dir, args.output_format)
    print(
        f"Processed {len(repository_rows)} repositories into {output_path} "
        f"({reused_count} reused, {refreshed_count} refreshed)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
