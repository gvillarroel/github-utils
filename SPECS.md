# Specifications

## Project Direction

This repository is evolving from a set of standalone exporters into a structured GitHub repository collection toolkit.

The codebase now has three major areas:

- `exporters/`: primary supported exporters
- `strategies/`: isolated strategy implementations for comparison and experimentation
- `spikes/`: research notes, experiments, and benchmark results

## Primary Strategy Decision

### Decision

The primary strategy for future product requirements is:

- `strategies/incremental_refresh/exporter.py`
- with `--inventory-mode tree-only` as the default operational profile

### Why This Is The Primary Strategy

Based on the comparison documented in `spikes/strategy-run-comparison-sample10.md`:

- `trees_only` is the fastest one-shot inventory strategy
- `incremental_refresh` has the best overall operational profile for repeated runs
- `incremental_refresh` baseline performance was acceptable
- `incremental_refresh` second-run performance was dramatically better because unchanged repositories were reused
- future requirements are likely to benefit from state, reuse, and refresh boundaries more than from pure one-shot speed

### Practical Interpretation

For new requirements, start from `incremental_refresh` unless a requirement explicitly demands something else.

## Default Execution Profile

Unless a requirement says otherwise, use these defaults:

- strategy: `incremental_refresh`
- inventory mode: `tree-only`
- output format: `parquet`
- authenticated execution: required
- recurring refresh model: preferred over repeated full rescans

## Approved Fallback Strategies

Use these only when the requirement makes the tradeoff necessary.

### Fastest one-shot inventory

- `strategies/trees_only/exporter.py`

Use when:

- only file inventory is needed
- file contents are not needed
- line counts are not needed
- a fast first run matters more than refresh behavior

### Full-fidelity snapshot with line counts

- `strategies/shallow_clone/exporter.py`

Use when:

- exact line counts are required
- binary detection is required
- the full repository snapshot is required

### Full snapshot without Git clone transport preference

- `strategies/archives_snapshot/exporter.py`

Use when:

- full contents are required
- archive download is preferable to Git clone transport

### Selective content extraction

- `strategies/trees_selective_blobs/exporter.py`

Use when:

- only a subset of file contents is needed
- selective blob fetches are acceptable despite higher runtime

### Experimental only

- `strategies/partial_clone/exporter.py`

Current status:

- not the default
- not production-preferred
- requires cleanup hardening before being trusted for unattended runs

## Explicit Non-Default Choices

These are not the default product direction:

- `trees_only` as the primary long-term strategy
  - too limited for evolving requirements
- `trees_selective_blobs` as the primary strategy
  - too slow for the observed value in the benchmark
- `partial_clone` as the primary strategy
  - not robust enough yet

## Product Requirements Boundary

When defining new requirements, assume:

- repository-level metadata must remain part of the output
- file inventory must remain supported
- repeated execution efficiency matters
- stateful refresh is a first-class concern
- output compatibility across `parquet`, `csv`, and `jsonl` should be preserved

## Known Gaps

These should be addressed in future work:

- normalize repository selection ordering across strategies so `--max-repos` is comparable
- fix workspace cleanup in `strategies/partial_clone/exporter.py`
- reduce duplication between `exporters/core.py` and strategy-specific logic
- consider extracting shared helpers from `exporters/core.py` into smaller reusable modules

## Documentation Rule

When a future change affects architecture, strategy choice, or execution defaults:

- update this file first
- then update `README.md` if user-facing usage changes
- then update `AGENTS.md` only for collaboration or workflow rules
