# RustFS Agent Instructions (Global)

This root file keeps repository-wide rules only.
Use the nearest subdirectory `AGENTS.md` for path-specific guidance.

## Rule Precedence

1. System/developer instructions.
2. This file (global defaults).
3. The nearest `AGENTS.md` in the current path (more specific scope wins).

If repo-level instructions conflict, follow the nearest file and keep behavior aligned with CI.

## Communication and Language

- Respond in the same language used by the requester.
- Keep source code, comments, commit messages, and PR title/body in English.

## Change Style for Existing Logic

- Prefer direct, local code over extracting one-off helpers.
- Extract a helper only when logic is reused or the extraction materially clarifies a non-trivial flow.
- Preserve the existing control-flow and logic shape when fixing bugs or addressing review comments, especially in init, distributed coordination, locking, metadata, and concurrency paths.
- Do not refactor existing code only to make it easier to unit test.
- Keep fixes narrowly aligned with the requested behavior; avoid semantic-adjacent rewrites while touching sensitive paths.
- Keep code elegant, concise, and direct. Prefer minimal, readable implementations over over-engineering and excessive abstraction. Use comments to clarify non-obvious intent and invariants, not to compensate for unclear code.

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

For code changes, run and pass the following before opening a PR:

```bash
make pre-commit
```

If `make` is unavailable, run the equivalent checks defined under `.config/make/`.
Documentation-only or instruction-only changes are exempt from the verification commands above (including the `.config/make/` equivalents), though any installed git pre-commit hooks (for example, from `make setup-hooks`) may still run on commit unless explicitly skipped.
Do not open a PR with code changes when the required checks fail.

## Git and PR Baseline

- Use feature branches based on the latest `main`.
- Follow Conventional Commits, with subject length <= 72 characters.
- Keep PR title and description in English.
- Use `.github/pull_request_template.md` and keep all section headings.
- Use `N/A` for non-applicable template sections.
- Include verification commands in the PR description.
- When using `gh pr create`/`gh pr edit`, use `--body-file` instead of inline `--body` for multiline markdown.

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
