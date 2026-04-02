# GitHub Repository Data Collection Strategies

## Goal

Identify efficient alternatives to collect:

- Repository metadata
- Repository file inventories
- Repository file contents

The main constraints are:

- Minimize GitHub API rate-limit pressure
- Keep throughput high for many repositories
- Support large repositories without failing on edge cases
- Make implementation paths explicit so each strategy can be built in a separate folder

## Scope

This spike focuses on public or authenticated GitHub access patterns using:

- GitHub REST API
- GitHub GraphQL API
- Git transport (`git clone`, partial clone, sparse checkout)
- GitHub source archives (`zipball` / `tarball`)

## Primary Sources

- GitHub REST API rate limits:
  [https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api)
- GitHub GraphQL API rate limits:
  [https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api](https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api)
- Git trees endpoint:
  [https://docs.github.com/en/rest/git/trees?apiVersion=2022-11-28](https://docs.github.com/en/rest/git/trees?apiVersion=2022-11-28)
- Repository contents endpoint:
  [https://docs.github.com/en/rest/repos/contents](https://docs.github.com/en/rest/repos/contents)
- Git blobs endpoint:
  [https://docs.github.com/en/rest/git/blobs?apiVersion=2022-11-28](https://docs.github.com/en/rest/git/blobs?apiVersion=2022-11-28)
- Source archives:
  [https://docs.github.com/en/repositories/working-with-files/using-files/downloading-source-code-archives](https://docs.github.com/en/repositories/working-with-files/using-files/downloading-source-code-archives)
- GraphQL pagination:
  [https://docs.github.com/en/graphql/guides/using-pagination-in-the-graphql-api](https://docs.github.com/en/graphql/guides/using-pagination-in-the-graphql-api)
- Commits and compare:
  [https://docs.github.com/en/rest/commits/commits](https://docs.github.com/en/rest/commits/commits)
- Git partial clone:
  [https://git-scm.com/docs/partial-clone/2.38.0.html](https://git-scm.com/docs/partial-clone/2.38.0.html)
- Git clone filters:
  [https://git-scm.com/docs/git-clone](https://git-scm.com/docs/git-clone)

## Relevant Platform Facts

### Rate limits

- REST primary rate limit for authenticated requests is commonly `5,000 requests/hour` per user token.
- REST secondary limits include a points-per-minute control, which matters when fanning out aggressively.
- GraphQL uses a point budget model instead of a simple request count.
- GraphQL secondary limits also matter; concurrency must stay controlled.

### Trees endpoint constraints

- Recursive tree fetches are efficient for file inventories.
- GitHub documents a recursive response limit of `100,000` entries and `7 MB`.
- If a recursive tree is truncated, clients must fall back to non-recursive subtree traversal.

### Contents and blobs

- `contents` is convenient for individual files or small directories, but inefficient for full-repository traversal.
- `git/blobs` is better than `contents` when the client already knows blob SHAs from a tree.
- Blobs can be fetched selectively, avoiding file-by-file directory walking.

### Archives and clone transport

- `zipball` / `tarball` fetches repository snapshots efficiently with low API call overhead.
- Partial clone with `--filter=blob:none` avoids downloading file contents until they are needed.
- Sparse checkout can reduce working-tree materialization if only a subset of paths is required.

## Evaluation Criteria

Each strategy is evaluated on:

- Metadata efficiency
- File inventory efficiency
- File content efficiency
- Rate-limit pressure
- Network transfer cost
- Large-repository behavior
- Implementation complexity
- Incremental refresh friendliness

## Strategy A: GraphQL metadata + REST recursive tree + selective blob fetch

### Summary

Use GraphQL for repository listing and high-density metadata, then use REST Git trees for file inventory, and fetch blobs only for files that pass a selection rule.

### Flow

1. List repositories in GraphQL pages of up to `100`.
2. Collect metadata, default branch, and object IDs in the same query.
3. For each repository, resolve the root tree for the default branch.
4. Fetch the recursive tree.
5. If the tree is truncated, traverse subtrees non-recursively.
6. Fetch blobs only for selected files:
   - code extensions
   - config files
   - files below a size threshold
   - paths matching allowlists

### Strengths

- Best balance for metadata density and API efficiency.
- Avoids cloning entire repositories.
- Good for inventory-first analytics.
- Good control over rate-limit spend because blob fetches are deliberate.

### Weaknesses

- Full content extraction for every file still becomes expensive.
- Requires fallback logic when recursive trees are truncated.
- Binary detection and content decoding require care.

### Best use case

- Large-scale repository analytics where file inventory is mandatory and contents are needed only for a subset of files.

## Strategy B: GraphQL metadata + source archive download

### Summary

Use GraphQL for repository discovery and metadata, then download one source archive per repository at the default branch or a target commit.

### Flow

1. Discover repositories in GraphQL.
2. Capture default branch and target ref.
3. Download `zipball` or `tarball`.
4. Extract locally.
5. Build inventory and content records from the extracted snapshot.

### Strengths

- Very low API request count.
- Often faster than API-driven per-file retrieval for full content snapshots.
- Good fit when full repository contents are needed.
- Simple mental model.

### Weaknesses

- Transfers more bytes than tree-plus-selective-blob strategies.
- Requires local extraction and disk I/O.
- Snapshot only; no commit history.
- Archive handling must tolerate large repositories and generated assets.

### Best use case

- Batch extraction when the whole repository snapshot is needed and local disk/network throughput is acceptable.

## Strategy C: GraphQL metadata + partial clone (`--filter=blob:none`) + targeted checkout

### Summary

Use GraphQL for discovery and metadata, then partial-clone repositories to get commits and trees without all blob contents. Materialize or fetch blobs only when needed.

### Flow

1. Discover repositories in GraphQL.
2. Run `git clone --filter=blob:none --no-checkout`.
3. Inspect trees and repository structure from Git objects.
4. Optionally enable sparse checkout for target paths.
5. Materialize selected files or fetch required blobs lazily.

### Strengths

- Strong option for very large repositories.
- Lower transfer cost than full clone when content access is selective.
- Gives native Git structure and future incremental fetch options.

### Weaknesses

- More implementation complexity than archive-based extraction.
- Requires careful Git process management and cleanup.
- Behavior depends on server and client support for partial clone features.

### Best use case

- Repositories where future incremental sync and selective content access matter more than one-shot simplicity.

## Strategy D: GraphQL metadata + shallow full clone

### Summary

Use GraphQL for discovery, then `git clone --depth 1` to get the current snapshot and full working tree in one transport.

### Flow

1. Discover repositories in GraphQL.
2. Shallow-clone only the default branch tip.
3. Walk the local working tree to compute inventory and read contents.

### Strengths

- Operationally simple.
- Robust for full snapshot extraction.
- Avoids many REST calls.
- Preserves Git-native behavior better than archives if follow-up fetches are needed.

### Weaknesses

- Pulls all blobs for the snapshot.
- Slower and heavier than partial clone or archives when only partial contents are needed.
- Highest disk usage among the snapshot-oriented options.

### Best use case

- Smaller repositories, or workflows where a local Git repo is directly useful for downstream processing.

## Strategy E: REST/GraphQL metadata + trees only

### Summary

Collect metadata plus file inventories, but never fetch file contents.

### Flow

1. List repositories in GraphQL or REST.
2. Get default branch tree.
3. Fetch recursive trees, with non-recursive fallback on truncation.
4. Persist file paths, modes, sizes, and extensions.

### Strengths

- Fastest and cheapest strategy for inventory-only analysis.
- Minimal network transfer compared with any full-content strategy.
- Good for language mix, path taxonomy, file counts, and size distributions.

### Weaknesses

- No file contents.
- No line counts unless computed later from fetched content or clones.
- Limited usefulness for semantic code analysis.

### Best use case

- Repository census, codebase shape metrics, language/path analysis, repository risk scoring.

## Strategy F: Incremental sync using compare/diff + selective refresh

### Summary

After a baseline snapshot exists, refresh repositories by comparing a stored commit SHA with the current default branch SHA, then update only changed files.

### Flow

1. Persist baseline metadata:
   - repository ID
   - default branch
   - last processed commit SHA
2. On refresh, resolve latest default branch SHA.
3. If unchanged, skip the repository.
4. If changed, use compare endpoints or Git diff logic to find modified paths.
5. Refresh only changed blobs or rebuild only affected inventory segments.

### Strengths

- Best long-term efficiency after baseline ingestion.
- Ideal for recurring runs.
- Prevents re-downloading unchanged repositories.

### Weaknesses

- More stateful and more complex.
- Requires careful handling of force pushes, rebases, renamed paths, and fallback rebuilds.
- Not a first strategy by itself; it complements a baseline strategy.

### Best use case

- Scheduled refreshes after an initial full import.

## Comparison Matrix

| Strategy | Metadata | Inventory | Full contents | API pressure | Network bytes | Large repo fit | Complexity |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A. GraphQL + Trees + Selective Blobs | Excellent | Excellent | Partial by design | Low to medium | Low to medium | Good | Medium |
| B. GraphQL + Archives | Excellent | Excellent | Excellent | Very low | Medium to high | Good | Low |
| C. GraphQL + Partial Clone | Excellent | Excellent | Selective to broad | Very low API, medium Git transport | Low to medium | Excellent | High |
| D. GraphQL + Shallow Clone | Excellent | Excellent | Excellent | Very low API, high Git transport | High | Fair to good | Medium |
| E. Metadata + Trees Only | Excellent | Excellent | None | Lowest | Lowest | Good, with truncation fallback | Low |
| F. Incremental Compare Refresh | Good | Good | Good | Lowest after baseline | Lowest after baseline | Excellent | High |

## Recommended Implementation Order

### 1. Strategy E first

Implement `metadata + trees only` first.

Reason:

- Fastest to stabilize
- Lowest cost
- Already aligned with the current hybrid/tree-only direction in this repository
- Gives immediate value for inventory analytics

### 2. Strategy A second

Implement `GraphQL + trees + selective blobs`.

Reason:

- Natural extension of the current architecture
- Adds content access without paying for full snapshots
- Best general-purpose production baseline

### 3. Strategy B third

Implement `GraphQL + archives`.

Reason:

- Useful reference point
- Likely strong throughput for full snapshot extraction
- Simpler than clone-based strategies

### 4. Strategy F fourth

Implement `incremental compare refresh`.

Reason:

- Multiplies efficiency once baseline collectors exist
- Most useful after a stable baseline format and repository identity model are established

### 5. Strategy C fifth

Implement `partial clone`.

Reason:

- Valuable for very large repositories
- More engineering effort and operational nuance

### 6. Strategy D last

Implement `shallow full clone`.

Reason:

- Straightforward, but usually inferior to archives for one-shot snapshots
- Still useful as a control implementation and fallback path

## Proposed Folder Layout

If each strategy will be implemented separately, this layout keeps the repository explicit:

```text
strategies/
  trees_only/
  trees_selective_blobs/
  archives_snapshot/
  incremental_refresh/
  partial_clone/
  shallow_clone/
```

Alternative if you want to preserve the current naming style:

```text
exporters/
  trees_only/
  trees_selective_blobs/
  archives_snapshot/
  incremental_refresh/
  partial_clone/
  shallow_clone/
```

## Shared Components To Extract Early

Regardless of strategy, these components should be shared:

- Authentication and token loading
- GraphQL repository discovery
- Rate-limit tracking and adaptive throttling
- Repository normalization schema
- Output writers for `parquet`, `csv`, and `jsonl`
- File inventory normalization
- Binary/text detection
- Retry and backoff policy
- Execution timestamp and run metadata

## Recommended Throttling Rules

These are implementation recommendations, not GitHub guarantees:

- Keep GraphQL page size at `100` unless query cost becomes unstable.
- Limit concurrency conservatively and tune using observed secondary-limit behavior.
- Prefer queue-based work dispatch with per-strategy backpressure.
- Treat blob fetching as a separately throttled stage.
- Cache default branch SHA and tree SHA when possible.
- Persist partial progress incrementally so retries do not restart whole runs.

## Practical Recommendation

If the target is "best efficiency under GitHub rate limits" for repository analytics:

- Use `Strategy E` when file contents are not required.
- Use `Strategy A` when some content is required.
- Use `Strategy B` when full snapshot contents are required with minimal API usage.
- Add `Strategy F` once recurring refreshes matter.

## Decision

Recommended first set to implement in separate folders:

1. `trees_only`
2. `trees_selective_blobs`
3. `archives_snapshot`
4. `incremental_refresh`
5. `partial_clone`
6. `shallow_clone`

This order is the best balance of implementation cost, runtime efficiency, and long-term extensibility.
