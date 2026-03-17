---
name: test-coverage-improver
description: Run project coverage checks, rank high-risk gaps, and propose high-impact tests to improve regression confidence for changed and critical code paths before release.
---

# Test Coverage Improver

Use this skill when you need a prioritized, risk-aware plan to improve tests from coverage results.

## Usage assumptions
- Focus scope is either changed lines/files, a module, or the whole repository.
- Coverage artifact must be generated or provided in a supported format.
- If required context is missing, call out assumptions explicitly before proposing work.

## Workflow

1. Define scope and baseline
   - Confirm target language, framework, and branch.
   - Confirm whether the scope is changed files only or full-repo.

2. Produce coverage snapshot
   - Rust: `cargo llvm-cov` (or `cargo tarpaulin`) with existing repo config.
   - JavaScript/TypeScript: `npm test -- --coverage` and read `coverage/coverage-final.json`.
   - Python: `pytest --cov=<pkg> --cov-report=json` and read `coverage.json`.
   - Collect total, per-file, and changed-line coverage.

3. Rank highest-risk gaps
   - Prioritize changed code, branch coverage gaps, and low-confidence boundaries.
   - Apply the risk rubric in [coverage-prioritization.md](references/coverage-prioritization.md).
   - Keep shortlist to 5–8 gaps.
   - For each gap, capture: file, lines, uncovered branches, and estimated risk score.

4. Propose high-impact tests
   - For each shortlisted gap, output:
     - Intent and expected behavior.
     - Normal, edge, and failure scenarios.
     - Assertions and side effects to verify.
     - Setup needs (fixtures, mocks, integration dependencies).
     - Estimated effort (`S/M/L`).

5. Close with validation plan
   - State which gaps remain after proposals.
   - Provide concrete verification command and acceptance threshold.
   - List assumptions or blockers (environment, fixtures, flaky dependencies).

## Output template

### Coverage Snapshot
- total / branch coverage
- changed-file coverage
- top missing regions by size

### Top Gaps (ranked)
- `path:line-range` | risk score | why critical

### Test Proposals
- `path:line-range`
  - Test name
  - scenarios
  - assertions
  - effort

### Validation Plan
- command
- pass criteria
- remaining risk
