---
name: code-change-verification
description: Verify code changes by identifying correctness, regression, security, and performance risks from diffs or patches, then produce prioritized findings with file/line evidence and concrete fixes. Use when reviewing commits, PRs, and merged patches before/after release.
---

# Code Change Verification

Use this skill to review code changes consistently before merge, before release, and during incident follow-up.

## Quick Start

1. Read the scope: commit, PR, patch, or file list.
2. Map each changed area by risk and user impact.
3. Inspect each risky change in context.
4. Report findings first, ordered by severity.
5. Close with residual risks and verification recommendations.

## Core Workflow

### 1) Scope and assumptions
- Confirm change source (diff, commit, PR, files), target branch, language/runtime, and version.
- If context is missing, state assumptions before deeper analysis.
- Focus only on requested scope; avoid reviewing unrelated files.

### 2) Risk map
- Prioritize in this order:
  - Data correctness and user-visible behavior
  - API/contract compatibility
  - Security and authz/authn boundaries
  - Concurrency and lifecycle correctness
  - Performance and resource usage
- Give higher priority to stateful paths, migration logic, defaults, and error handling.

### 3) Evidence-based inspection
- Read each modified hunk with neighboring context.
- Trace call paths and call-site expectations.
- Check for:
  - invariant breaks and missing guards
  - unchecked assumptions and null/empty/error-path handling
  - stale tests, fixtures, and configs
  - hidden coupling to shared helpers/constants/features
- If a point is uncertain, mark it as an open question instead of guessing.

### 4) Findings-first output
- Order findings by severity:
  - P0: critical failure, security breach, or data loss risk
  - P1: high-impact regression
  - P2: medium risk correctness gap
  - P3: low risk/quality debt
- For each finding include:
  - Severity
  - `path:line` reference
  - concise issue statement
  - impact and likely failure mode
  - specific fix or mitigation
  - validation step to confirm
- If no issues exist, explicitly state `No findings` and why.

### 5) Close
- Report assumptions and unknowns.
- Suggest targeted checks (tests, canary checks, logs/metrics, migration validation).

## Output Template

1. Findings
2. No findings (if applicable)
3. Assumptions / Unknowns
4. Recommended verification steps

## Finding Template

- `[P1] Missing timeout for downstream call`
  - Location: `path/to/file.rs:123`
  - Issue: ...
  - Impact: ...
  - Fix suggestion: ...
  - Validation: ...

