---
name: pr-creation-checker
description: Perform the final RustFS PR preflight and draft compliant English title/body metadata immediately before creating or updating a PR. Do not use during implementation or as a second general code review.
---

# PR Creation Checker

Use this skill only at the PR boundary. Reuse completed diff review and
verification evidence; do not reread the repository or rerun equivalent checks.

## Preflight

1. Confirm the branch is based on current `origin/main` and contains only the
   intended task diff.
2. Inspect `git diff --stat`, `git diff --check`, and changed file names for
   secrets, logs, generated artifacts, or unrelated edits.
3. Confirm the checks selected by root `AGENTS.md` passed on the final diff.
   Do not replace focused behavioral tests with a generic gate or rerun checks
   already covered by an unchanged umbrella run.
4. Read `.github/pull_request_template.md`. Consult `Makefile`, `.config/make/`,
   or CI only when the required command/current gate is uncertain.
5. Return `BLOCKED` for an unclean scope, missing required evidence, failed
   required checks, or non-compliant metadata.

## Metadata

- Title: English Conventional Commit, at most 72 characters, with no tool
  prefix.
- Body: English, exact template headings, `N/A` where needed, concise rationale,
  actual verification commands, and material risks/rollback notes.
- Use repository-relative paths; never include local absolute paths.
- Keep prose paragraphs on one logical line and never include the literal
  sequence `\n`.
- Use a temporary body file with `gh pr create --body-file` or
  `gh pr edit --body-file`; never pass multiline Markdown inline.

## Output

- Status: `READY` or `BLOCKED`.
- Title.
- Complete PR body.
- Verification commands and results.
- Risks or `N/A`.

Immediately before the GitHub write, repeat only the five preflight checks above
against the final head.
