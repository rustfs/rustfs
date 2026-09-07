---
name: code-change-verification
description: Review a commit, PR, or merged patch when the user requests ordinary code-change verification. Do not combine with adversarial-validation; use that skill instead for explicitly adversarial, substantial, or high-risk RustFS reviews.
---

# Code Change Verification

Use this skill for an ordinary requested review. If the root policy or user calls
for adversarial validation, use `adversarial-validation` instead of running both.

## Core Workflow

### 1) Scope and assumptions
- Derive the change source, target branch, and relevant runtime/version from the
  supplied diff and metadata. Ask only when missing context could change the verdict.
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
- Apply root `AGENTS.md`'s finding standard: try to disprove a candidate before
  reporting it. Mention an unresolved question only when it could materially
  change the verdict; do not fill the report with speculative possibilities.

#### Rust-specific checks

For changed Rust behavior, use the matching sections of [rust-code-quality](../rust-code-quality/SKILL.md). Reuse checks already performed by the selected review workflow. Comment-only or formatting-only Rust diffs do not require the full Rust checklist. Carry its P0–P3 ratings over unchanged and use this skill's output format.

### 4) Findings-first output
- Order supported findings by P0–P3 severity; preserve the Rust ratings above.
  Include `path:line`, the failure and impact, a focused fix, and its validation.
- If no supported issues remain, state `No findings` with the reviewed scope and
  any material verification limitation. Do not append optional improvements to
  make a clean review look productive.

Close after the required review. Recommend additional verification only for an
identified unresolved risk or required gate; reuse evidence for unchanged code.
