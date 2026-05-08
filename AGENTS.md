# RustFS Agent Instructions (Global)

This root file keeps repository-wide rules only.
Use the nearest subdirectory `AGENTS.md` for path-specific guidance.

## Rule Precedence

1. System/developer instructions.
2. Current user/task instructions.
3. The nearest `AGENTS.md` in the current path.
4. This file (global defaults).

If repo-level instructions conflict, follow the nearest file and keep behavior aligned with CI.

## Execution Discipline

- Read the relevant existing code, tests, and local guidance before changing behavior.
- State assumptions when they affect the implementation or verification path.
- If a task has multiple plausible interpretations, list the options briefly and choose the narrowest reasonable path; ask when the ambiguity would make the change risky.
- For multi-step work, keep the plan minimal and tied to verifiable outcomes.
- Avoid redundant file reads, repeated commands, and unnecessary exploratory work once enough context is available.
- A good result is a minimal diff with clear assumptions, no over-engineering, and independent verification.

## Communication and Language

- Respond in the same language used by the requester.
- Keep source code, comments, commit messages, and PR title/body in English.
- Be concise. Avoid sycophantic openers, closing fluff, and verbose status reporting.

## Skill Usage

- Do not use the `rust-refactor-helper` skill in any scenario.

## Change Style for Existing Logic

- Prefer direct, local code over extracting one-off helpers.
- Extract a helper only when logic is reused or the extraction materially clarifies a non-trivial flow.
- Solve only the requested problem; do not add speculative features, configurability, or adjacent improvements.
- Prefer editing existing code over rewriting files or reshaping unrelated logic.
- Modify only what is required and remove only artifacts introduced by your own changes.
- Preserve the existing control-flow and logic shape when fixing bugs or addressing review comments, especially in init, distributed coordination, locking, metadata, and concurrency paths.
- Do not refactor existing code only to make it easier to unit test.
- Keep fixes narrowly aligned with the requested behavior; avoid semantic-adjacent rewrites while touching sensitive paths.
- Keep code elegant, concise, and direct. Prefer minimal, readable implementations over over-engineering and excessive abstraction. Use comments to clarify non-obvious intent and invariants, not to compensate for unclear code.
- Mention unrelated issues when useful, but do not fix them as part of a narrow task.

## Constant and String Usage

- Before introducing new string literals, search for existing constants/enums that already represent the same semantic value.
- Reuse existing constants for protocol labels, error identifiers, header keys, event names, metric names, command tags, and similar fixed tokens.
- If a new string is truly unique, define a local constant near related logic and avoid scattering the literal across multiple sites.
- When changing existing behavior, keep naming and format consistency by aligning with established project constants.

## Sources of Truth

- Workspace layout and crate membership: `Cargo.toml` (`[workspace].members`)
- Local quality commands: `Makefile` and `.config/make/`
- CI quality gates: `.github/workflows/ci.yml`
- PR template: `.github/pull_request_template.md`

Avoid duplicating long crate lists or command matrices in instruction files.
Reference the source files above instead.

## Verification Before PR

Convert changes into independently verifiable outcomes. Prefer focused tests for behavior changes and run the relevant checks before declaring completion.

For code changes, run and pass the following before opening a PR:

```bash
make pre-commit
```

Before pushing code changes, make sure formatting is clean:

- Run `cargo fmt --all`.
- Run `cargo fmt --all --check` and ensure no files are modified unexpectedly.

If `make` is unavailable, run the equivalent checks defined under `.config/make/`.
Documentation-only or instruction-only changes are exempt from the verification commands above (including the `.config/make/` equivalents), though any installed git pre-commit hooks (for example, from `make setup-hooks`) may still run on commit unless explicitly skipped.
After build-based verification completes, clean generated build artifacts before wrapping up to avoid unnecessary disk usage.
Do not open a PR with code changes when the required checks fail.

## Git and PR Baseline

- Use feature branches based on the latest `main`.
- Follow Conventional Commits, with subject length <= 72 characters.
- Keep PR title and description in English.
- Use `.github/pull_request_template.md` and keep all section headings.
- Use `N/A` for non-applicable template sections.
- Include verification commands in the PR description.
- When using `gh pr create`/`gh pr edit`, use `--body-file` instead of inline `--body` for multiline markdown.
- Do not include the literal sequence `\n` in any GitHub issue, pull request, or discussion comment.
- After fixing code review comments or CI findings, always mark corresponding review
  comments/threads as resolved before returning to the user.
- In handling review comments, confirm the underlying issue before changing code.
  If a suggested change is not appropriate for behavior or risk, reply with a
  concise rationale instead of blindly applying it.

## Security Baseline

- Never commit secrets, credentials, or key material.
- Use environment variables or vault tooling for sensitive configuration.
- For localhost-sensitive tests, verify proxy settings to avoid traffic leakage.

## Scoped Guidance in This Repository

- `.github/AGENTS.md`
- `crates/AGENTS.md`
- `crates/config/AGENTS.md`
- `crates/ecstore/AGENTS.md`
- `crates/e2e_test/AGENTS.md`
- `crates/iam/AGENTS.md`
- `crates/kms/AGENTS.md`
- `crates/policy/AGENTS.md`
- `rustfs/src/admin/AGENTS.md`
- `rustfs/src/storage/AGENTS.md`
