# AI Code Evolution Metrics Spike

## Goal

Identify the most useful metrics to evaluate how a codebase evolves as AI-assisted coding usage increases.

This spike focuses on metrics that are:

- meaningful at repository or team level
- resistant to gaming
- measurable from repository, CI, and PR data
- useful for comparing AI-assisted and non-AI-assisted changes over time

## Summary

The most defensible approach is to measure outcomes, not just AI adoption or raw output.

The central question is not "how much code did AI generate?" but:

- did delivery get faster?
- did review and rework get cheaper or more expensive?
- did defect rates change?
- did maintainability improve or degrade?

The strongest metric set combines:

1. delivery flow metrics
2. quality and reliability metrics
3. maintainability and rework metrics
4. AI adoption segmentation

## Recommended Metrics

### 1. AI-Assisted Change Ratio

Measure:

- percentage of PRs marked AI-assisted
- percentage of commits marked AI-assisted
- percentage of files touched by AI-assisted changes

Why it matters:

- it provides the segmentation key for all comparisons
- without this dimension, the rest of the metrics cannot be interpreted in the context of AI adoption

### 2. Lead Time for Changes

Measure:

- PR open to merge
- or first commit to production, depending on available data

Why it matters:

- this is one of the cleanest ways to detect whether AI improves delivery speed in practice

### 3. Review Cycle Time

Measure:

- PR open to first review
- first review to approval
- approval to merge

Why it matters:

- AI can reduce authoring time while increasing review burden
- this metric catches that tradeoff

### 4. Review Rework Rate

Measure:

- number of review rounds per PR
- number of review comments per PR
- number of fixup commits after review starts

Why it matters:

- useful to detect "fast draft, expensive cleanup"
- often a better signal than throughput alone

### 5. First-Pass CI Success Rate

Measure:

- percentage of PRs or commits that pass CI on the first attempt

Why it matters:

- strong early signal of quality and correctness
- low cost to measure if CI data is available

### 6. Change Failure Rate

Measure:

- percentage of changes that cause incidents, rollbacks, emergency fixes, or hotfixes

Why it matters:

- a core reliability metric
- if AI increases velocity but also increases failures, the net effect may be negative

### 7. Mean Time to Recovery

Measure:

- time from incident detection to stable recovery

Why it matters:

- indicates operational cost of low-quality or poorly understood changes

### 8. Post-Merge Defect Rate

Measure:

- bugs or incidents attributed to a PR within 7, 14, or 30 days after merge

Why it matters:

- stronger signal than static analysis alone
- captures escape into downstream use

### 9. Code Churn After Merge

Measure:

- lines or files modified again within 7, 14, or 30 days after merge
- percentage of code rewritten shortly after merge

Why it matters:

- strong indicator of unstable or low-confidence code
- especially relevant when AI makes drafting cheap but correctness uneven

### 10. Duplication / Clone Rate

Measure:

- duplicate blocks
- clone classes
- repeated near-identical logic over time

Why it matters:

- research suggests AI-assisted coding can increase copy-paste or clone-like growth
- important long-term maintainability signal

### 11. Complexity Delta

Measure:

- cyclomatic complexity delta
- cognitive complexity delta
- complexity added per PR or per file

Why it matters:

- useful to detect silent structural degradation even when velocity improves

### 12. Test Protection Delta

Measure:

- test coverage delta
- touched lines covered
- tests added per changed module
- ratio of test files changed to production files changed

Why it matters:

- helps distinguish safe speedups from risky speedups

## Best Starting Set

If only a small set can be instrumented first, use:

1. AI-assisted change ratio
2. lead time for changes
3. post-merge defect rate
4. code churn after merge
5. duplication rate
6. first-pass CI success rate

This set balances speed, quality, and maintainability.

## Metrics To Avoid As Primary KPIs

Do not use these as primary success metrics:

- lines of code
- commits per developer
- PR count per developer
- accepted AI suggestions count
- files changed per author

Why:

- they are easy to game
- they reward output volume more than system outcomes
- they can hide quality regression

## Practical Measurement Model

Track metrics weekly or per sprint, and segment them into:

- AI-assisted
- non-AI-assisted

Then compare trends over time.

Recommended breakdown:

### Adoption

- AI-assisted PR ratio
- AI-assisted commit ratio

### Flow

- lead time
- review cycle time
- first-pass CI success

### Quality

- post-merge defect rate
- change failure rate
- mean time to recovery

### Maintainability

- churn after merge
- duplication growth
- complexity delta
- hotspot score

### Validation Discipline

- test protection delta

## Hotspot Metric

A particularly useful derived metric is:

- hotspot score = churn x complexity

Why it matters:

- it identifies parts of the codebase that are both changing frequently and structurally risky
- useful to see whether AI usage is concentrating debt into specific modules

## Interpretation Rule

AI usage is likely helping if:

- lead time decreases
- while defect rate, churn, duplication, and change failure rate stay flat or improve

AI usage is likely hurting if:

- lead time decreases
- but rework, defect escape, duplication, or review burden increases

## Recommended Dashboard Fields

Suggested dataset fields:

- executed_at
- repository
- team or service
- ai_assisted_flag
- pr_number
- merged_at
- lead_time_minutes
- review_cycle_minutes
- first_pass_ci_success
- post_merge_defect_flag
- post_merge_defect_count_30d
- churn_ratio_30d
- duplication_percent
- complexity_delta
- test_coverage_delta
- change_failure_flag
- recovery_time_minutes

## Source Notes

### DORA

Relevant:

- DORA 2025 report
- DORA AI capabilities model
- DORA metrics guide

Takeaway:

- use delivery and reliability outcomes, not vanity metrics
- AI should be evaluated in context of team practices and system performance

Sources:

- https://dora.dev/research/2025/dora-report/
- https://dora.dev/ai/
- https://dora.dev/guides/dora-metrics/

### Google Research

Relevant:

- Google describes fine-grained internal software engineering logs including edits, build outcomes, review fixes, and repository submissions

Takeaway:

- best measurement systems combine coding, build, review, and submission events

Source:

- https://research.google/blog/ai-in-software-engineering-at-google-progress-and-the-path-ahead/

### GitClear

Relevant:

- large-scale longitudinal analysis of changed lines and code quality patterns under AI assistant adoption

Takeaway:

- duplication and code churn should be treated as first-class metrics

Source:

- https://www.gitclear.com/ai_assistant_code_quality_2025_research

### METR

Relevant:

- empirical study on experienced open-source developers using early-2025 AI tools

Takeaway:

- subjective speed and actual delivery improvement can diverge
- objective operational metrics matter

Source:

- https://metr.org/blog/2025-07-10-early-2025-ai-experienced-os-dev-study/

## Conclusions

The best metrics for tracking code evolution during increasing AI usage are:

- lead time
- review rework
- first-pass CI success
- post-merge defect rate
- churn after merge
- duplication rate
- complexity delta
- test protection delta
- change failure rate
- MTTR

But these only make sense if every change is segmented by AI-assisted versus non-AI-assisted origin.

Without that segmentation, the analysis becomes observational noise rather than a meaningful measurement system.
