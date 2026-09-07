---
name: test-coverage-improver
description: Analyze a supplied coverage report or perform an explicitly requested RustFS coverage assessment, rank uncovered risks, and propose focused tests. Do not trigger for ordinary implementation verification, a single regression test, documentation wording, or release preparation without a coverage request.
---

# Test Coverage Improver

Use this skill when you need a prioritized, risk-aware plan to improve tests from coverage results.

## Usage assumptions
- Focus scope is either changed lines/files, a module, or the whole repository.
- Reuse a supplied coverage artifact when its revision, scope, and format match.
- If required context is missing, call out assumptions explicitly before proposing work.

## Workflow

1. Define scope and baseline
   - Derive the revision and scope from the request, diff, or supplied report.
   - Default to the affected files/module; whole-workspace coverage requires that
     scope in the request. Ask only if a wrong scope would change the result.

2. Obtain coverage evidence
   - First inspect a matching existing artifact; do not regenerate it merely
     because this skill was selected.
   - If measurement is needed, read the Coverage section of
     [the testing guide](../../../docs/testing/README.md#coverage), check disk
     space/tool availability, and select package/test-scoped `cargo llvm-cov`
     using the repository's nextest configuration. `make coverage` measures the
     whole workspace (excluding E2E) and is only for that requested scope.
   - Collect only metrics the report supports. Missing branch/changed-line
     coverage is unknown, not zero.
   - If measurement cannot run, continue with code-based test proposals and
     mark measured coverage unverified; do not invent a coverage percentage.

3. Rank highest-risk gaps
   - Prioritize changed code, branch coverage gaps, and low-confidence boundaries.
   - Apply the risk rubric in [coverage-prioritization.md](references/coverage-prioritization.md).
   - Report up to 5–8 evidenced gaps; do not pad a small scope.
   - For each gap, capture: file, lines, uncovered branches, and estimated risk score.

4. Propose high-impact tests
   - For each gap, name the behavior and regression, distinguishing assertions,
     relevant normal/edge/failure cases, necessary setup, and estimated effort.
   - Include only scenarios and setup that apply; reuse shared fixture details.

5. Close with validation plan
   - State which gaps remain after proposals.
   - Give a scoped verification command and behavior-based acceptance criterion;
     use a coverage threshold only when the task or repository requires one.
   - List assumptions or blockers (environment, fixtures, flaky dependencies).

## Report

Summarize the supported metrics, then combine each ranked gap with its proposed
test and validation. Include source lines only when supplied or inspected;
mark missing metrics or locations as unknown. Do not duplicate gaps and tests
in separate templates or fill empty categories for an otherwise small report.
