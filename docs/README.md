# RustFS Large PUT Analysis Pack

## Purpose

This index page groups the large-object PUT analysis materials produced for the
multi-node, multi-disk RustFS vs MinIO performance gap.

Recommended starting point depends on what you want to do next:

1. Understand the current root-cause analysis
2. Run the stage-breakdown benchmark
3. Build dashboards and queries
4. Write issue / PR / report updates

## Quick Start

### 1. Root-Cause Analysis

Read this first if you want the full current-code explanation:

- [rustfs-put-large-object-vs-minio-multi-node-analysis-and-stage-breakdown-zh.md](/Users/zhi/Documents/code/rust/rustfs/rustfs/docs/rustfs-put-large-object-vs-minio-multi-node-analysis-and-stage-breakdown-zh.md)

Use this when:

- you need to understand why large PUT falls behind MinIO
- you want the current-code hot path breakdown
- you need the suggested tuning and optimization priorities

### 2. Execution Checklist

Use this when you are about to run the benchmark:

- [scripts/run_put_large_stage_breakdown_with_capture.sh](/Users/zhi/Documents/code/rust/rustfs/rustfs/scripts/run_put_large_stage_breakdown_with_capture.sh)
- [rustfs-put-large-object-stage-breakdown-checklist-zh.md](/Users/zhi/Documents/code/rust/rustfs/rustfs/docs/rustfs-put-large-object-stage-breakdown-checklist-zh.md)
- [issue-706-put-large-stage-breakdown-ops-guide-zh.md](/Users/zhi/Documents/code/rust/rustfs/rustfs/docs/issue-706-put-large-stage-breakdown-ops-guide-zh.md)
- [scripts/run_put_large_stage_breakdown.sh](/Users/zhi/Documents/code/rust/rustfs/rustfs/scripts/run_put_large_stage_breakdown.sh)
- [scripts/collect_put_large_stage_breakdown_artifacts.sh](/Users/zhi/Documents/code/rust/rustfs/rustfs/scripts/collect_put_large_stage_breakdown_artifacts.sh)

Use this when:

- you need a step-by-step benchmark runbook
- you want the exact preflight and capture checklist
- you need the A/B execution order

### 3. Prometheus / Grafana Queries

Use this when you are preparing observability:

- [rustfs-put-large-object-prometheus-grafana-query-template-zh.md](/Users/zhi/Documents/code/rust/rustfs/rustfs/docs/rustfs-put-large-object-prometheus-grafana-query-template-zh.md)

Use this when:

- you need PromQL queries for PUT totals, stage histograms, EC inflight, CPU, and disk
- you want a suggested Grafana panel layout
- you need a quick way to identify whether encode or commit tail is hotter

### 4. Report Templates

Use this after a benchmark round finishes:

- [rustfs-put-large-object-stage-breakdown-report-template-zh.md](/Users/zhi/Documents/code/rust/rustfs/rustfs/docs/rustfs-put-large-object-stage-breakdown-report-template-zh.md)

Use this when:

- you want a ready-to-fill issue update
- you need a PR validation summary
- you want a weekly report or final conclusion template

### 5. Tuning Matrix Ops

Use this when you are moving from the `#706` baseline into `#708` parameter A/B work:

- [issue-708-put-large-tuning-ops-guide-zh.md](/Users/zhi/Documents/code/rust/rustfs/rustfs/docs/issue-708-put-large-tuning-ops-guide-zh.md)

Use this when:

- you want a profile-based tuning matrix runner
- you need baseline/profile compare conventions
- you want `--apply-cmd` guidance for restart/reload flows

## Suggested Workflow

1. Read the analysis document.
2. Run the execution checklist baseline for `16MiB` and `32MiB`.
3. Capture metrics and logs with the Prometheus / Grafana template.
4. Fill the report template with measured results.
5. Use the report to decide whether the next step is tuning or code changes.

## Related Context

Related repository materials:

- [minio-vs-rustfs-erasure-coding-function-level-comparison.md](/Users/zhi/Documents/code/rust/rustfs/rustfs/docs/minio-vs-rustfs-erasure-coding-function-level-comparison.md)
- [rustfs-multi-node-multi-disk-performance-throughput-playbook-zh.md](/Users/zhi/Documents/code/rust/rustfs/rustfs/docs/rustfs-multi-node-multi-disk-performance-throughput-playbook-zh.md)
- [issue-2573-grafana-and-acceptance-guide-zh.md](/Users/zhi/Documents/code/rust/rustfs/rustfs/docs/issue-2573-grafana-and-acceptance-guide-zh.md)

## Ownership Hint

If the next step is:

- benchmark execution: start from the checklist
- observability/dashboard work: start from the Prometheus / Grafana template
- GitHub communication: start from the report template
- implementation planning: start from the root-cause analysis
