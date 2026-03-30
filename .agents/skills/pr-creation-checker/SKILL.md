---
name: pr-creation-checker
description: Prepare PR-ready diffs by validating scope, checking required verification steps, drafting a compliant English PR title/body, and surfacing blockers before opening or updating a pull request in RustFS.
---

# PR Creation Checker

Use this skill before `gh pr create`, before `gh pr edit`, or when reviewing whether a branch is ready for PR.

## Read sources of truth first

- Read `AGENTS.md`.
- Read `.github/pull_request_template.md`.
- Use `Makefile` and `.config/make/` for local quality commands.
- Use `.github/workflows/ci.yml` for CI expectations.
- Do not restate long command matrices or template sections from memory when the files exist.

## Workflow

1. Collect PR context
- Confirm base branch, current branch, change goal, and scope.
- Confirm whether the task is: draft a new PR, update an existing PR, or preflight-check readiness.
- Confirm whether the branch includes only intended changes.

2. Inspect change scope
- Review the diff and summarize what changed.
- Call out unrelated edits, generated artifacts, logs, or secrets as blockers.
- Mark risky areas explicitly: auth, storage, config, network, migrations, breaking changes.
- Scan the diff for newly added string literals and confirm whether they duplicate values already defined as constants/enums/typed wrappers in the same module or shared modules.
- Treat introducing a new hardcoded literal where a project constant already exists as a likely regression risk; require either a refactor to reuse the constant or an explicit exception explanation in the PR body.

3. Verify readiness requirements
- Require `make pre-commit` before marking the PR ready.
- If `make` is unavailable, use the equivalent commands from `.config/make/`.
- Add scope-specific verification commands when the changed area needs more than the baseline.
- If required checks fail, stop and return `BLOCKED`.

4. Draft PR metadata
- Write the PR title in English using Conventional Commits and keep it within 72 characters.
- If a generic PR workflow suggests a different title format, ignore it and follow the repository rule instead.
- In RustFS, do not use tool-specific prefixes such as `[codex]` when the repository requires Conventional Commits.
- Keep the PR body in English.
- Use the exact section headings from `.github/pull_request_template.md`.
- Fill non-applicable sections with `N/A`.
- Include verification commands in the PR description.
- Do not include local filesystem paths in the PR body unless the user explicitly asks for them.
- Prefer repo-relative paths, command names, and concise summaries over machine-specific paths such as `/Users/...`.

5. Prepare reviewer context
- Summarize why the change exists.
- Summarize what was verified.
- Call out risks, rollout notes, config impact, and rollback notes when applicable.
- Mention assumptions or missing context instead of guessing.

6. Prepare CLI-safe output
- When proposing `gh pr create` or `gh pr edit`, use `--body-file`, never inline `--body` for multiline markdown.
- Return a ready-to-save PR body plus a short title.
- If not ready, return blockers first and list the minimum steps needed to unblock.

## Output format

### Status
- `READY` or `BLOCKED`

### Title
- `<type>(<scope>): <summary>`

### PR Body
- Reproduce the repository template headings exactly.
- Fill every section.
- Omit local absolute paths unless explicitly required.

### Verification
- List each command run.
- State pass/fail.

### Risks
- List breaking changes, config changes, migration impact, or `N/A`.

## Blocker rules

- Return `BLOCKED` if `make pre-commit` has not passed.
- Return `BLOCKED` if the diff contains unrelated changes that are not acknowledged.
- Return `BLOCKED` if required template sections are missing.
- Return `BLOCKED` if the title/body is not in English.
- Return `BLOCKED` if the title does not follow the repository's Conventional Commit rule.
- Return `BLOCKED` if the diff introduces string literals that should use existing constants but did not.

## Reference

- Use [pr-readiness-checklist.md](references/pr-readiness-checklist.md) for a short final pass before opening or editing the PR.
