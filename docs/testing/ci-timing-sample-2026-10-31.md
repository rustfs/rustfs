# CI timing sample 2026-10-31 (backlog #2963 CI01 measurement)

Operational measurement of ordinary-code PR feedback and runner occupancy against the CI01 experimental targets (feedback median 15–20 min, P95 ≤ 30 min, paid occupancy median 50–60 runner minutes). Methodology: [ci-timing.md](ci-timing.md) and [ci-resource-accounting.md](ci-resource-accounting.md). Raw snapshots: `/tmp/ci-timing.json` (limit 30 reproduction), `/tmp/ci-timing-200.json` (analysis sample), `/tmp/ci-acc-w{1,3}.json`. Nothing in this report is an acceptance result; the targets are **not met** by the measured sample.

## Sample window and n

| Item | Value |
|---|---|
| Collection time | 2026-10-31 ~02:50–03:15 UTC |
| Window | completed `pull_request` runs of `ci.yml` created in the last 7 days |
| Runs sampled | 30 (reproduction) and 200 (analysis, script max) |
| Successful-code runs (classifier) | 1 of 30; **39 of 200** |
| Other outcomes in 200-run sample | 68 cancelled (34 %), 36 failed, remainder non-code greens/closure handlers |
| Lane shapes in sample | 24 combined-lane runs (`Workspace Test and Lint`), 15 split-lane runs (`Workspace Lint and Build` + `Workspace tests` + `Core contract`) |

The limit-30 reproduction reports median wall 76.85 min and median job-sum 133.33 runner-min on n=1 (per methodology, an absent/small median is not zero). All medians/P95 below are from the n=39 analysis sample. Two of the 39 are rerun continuations (run_attempt > 1, wall 7.0 and 22.6 min re-executing only failed jobs under the rerun-start convention); they do not move the median.

## Measured feedback and occupancy (ordinary successful code runs, n=39)

| Metric | Median | P90 | P95 | Max |
|---|---|---|---|---|
| Wall feedback (created/rerun-start → last job end) | **76.3 min** | 94.5 | **97.0** | 103.2 |
| Runner-minute job sum per run | **179.5** | 206.5 | **213.6** | 220.4 |

Independent accounting cross-check (`scripts/ci/actions_cost_report.py`, 2026-10-30T00–12Z window, 547 attempts, collection **incomplete** — see gaps): successful-code PR feedback median 88.34 min (n=12), runner_sum median 199.68 min (n=12). A complete half-day window (2026-10-31T00–12Z, 78 attempts) shows total known runner occupancy 1379.72 min with 42 jobs of unknown occupancy; queue wait summed 28.43 min, so queueing is not the dominant cost.

## Per-job critical path breakdown

Execution minutes across the 39 runs (known only):

| Job | n | Median | P95 |
|---|---|---|---|
| Workspace Test and Lint (combined lane) | 24 | 63.6 | 73.9 |
| Connect Feature Boundaries | 38 | 53.5 | 70.4 |
| Workspace Lint and Build (split lane) | 15 | 43.3 | 56.5 |
| Workspace tests (split lane) | 15 | 29.2 | 43.0 |
| Build RustFS Debug Binary | 39 | 18.5 | 24.9 |
| End-to-End Tests | 39 | 11.4 | 16.7 |
| S3 Implemented Tests | 39 | 9.8 | 11.1 |
| io_uring Integration (real) | 39 | 7.7 | 8.8 |
| Quick Checks | 39 | 7.2 | 9.2 |

Per-run chain timeline (start = attempt start; see `/tmp/per-run-jobs.json`):

- Split lane (15 runs): `Quick Checks` (~7–10) → `Workspace Lint and Build` (ends ~46–67) → `Workspace tests` (ends ~71–103) → shard evidence → aggregator `Test and Lint`/`required-checks`. The lint→tests chain sets the wall in most split runs.
- Combined lane (24 runs): `Quick Checks` → `Workspace Test and Lint` alone runs 63–86 min and sets the wall.
- `Connect Feature Boundaries` (median 53.5, P95 70.4) runs parallel to the lint chain and occasionally becomes the tail (e.g. runs 41021787700, 40750122159: connect end within 0.2 min of wall).
- `Build RustFS Debug Binary` → E2E/Core-contract/S3 chain finishes with 30–60 min of slack in every sampled run.
- The required-check aggregator `Test and Lint` (`required-checks`, `ci_gate.py verify`) finishes last by construction (~0.2 min).

## Against the CI01 targets

| Target | Measured | Verdict |
|---|---|---|
| Feedback median 15–20 min | 76.3 min (n=39) | **not met** (~3.8–5× over) |
| Feedback P95 ≤ 30 min | 97.0 min | **not met** (~3.2× over) |
| Occupancy median 50–60 runner-min | 179.5 runner-min | **not met** (~3–3.6× over) |

These are the experimental-target measurements. The full merge-lifecycle cost is separate and was not established here (the accounting PR-allocation buckets were empty in the partial window; see gaps).

## Bottlenecks (honest list)

1. **Workspace lint+test compile/test chain (critical path):** combined `Workspace Test and Lint` median 63.6 min, or split `Workspace Lint and Build` 43.3 + `Workspace tests` 29.2 ≈ 72.5 min after Quick Checks. This is the wall clock.
2. **Connect Feature Boundaries (parallel near-critical):** median 53.5 min on its own lane with `CARGO_BUILD_JOBS: "2"` limiting build concurrency on the credential-test step; becomes the tail when the lint chain is fast.
3. **Duplicate compilation across lanes:** `Workspace Lint and Build` builds workspace test binaries; `Build RustFS Debug Binary` separately builds the debug binary + E2E binaries on another runner; Connect builds again. `cache-save-if: 'false'` on the Connect lane means that lane never warms the shared `ci-dev` cache.
4. Runner queueing is small (28.43 queue-min vs 1379.72 runner-min in the complete half-day window) — not the bottleneck.

## Gaps and caveats

- Accounting windows: 2026-10-30T00–12Z collected 547 attempts but is flagged incomplete; 2026-10-30T12–24Z collection did not finish (timeout) and has no report; 2026-10-31T12–24Z was empty at collection time. Occupancy figures are per-window partial sums and must not be summed as a bill.
- No CI workflow or selection change was made in this pass; strict gate semantics unchanged. No savings are claimed. Candidate fixes (build-artifact reuse between lanes, enabling cache save on the Connect lane, finer affected-selection granularity in shadow mode per ci-timing.md) are **unmeasured proposals**, not results.
- Timing data alone does not establish functional coverage or escaped-regression rate; the 15–20 min target remains an experiment that currently misses by a wide margin.
