# Git and Pull Request Rules

Applies to commits, pushes, PRs, and issue/discussion actions. Paths below are
repository-relative. User authorization and root `AGENTS.md` still govern scope.

## Final PR Preflight

Before creating or updating a PR, reuse completed review and verification:

- Verify the actual base (normally `origin/main`) and the complete task diff,
  including file names and whitespace. Exclude secrets, logs, generated
  artifacts, and unrelated edits. Retain an existing PR's base unless requested.
- Confirm the final diff passed the root verification tier; fix task-owned
  failures and run missing scoped checks. Report unresolved required checks or
  authority without expanding the task. Do not start another general review.
- Keep the English Conventional Commit title at most 72 characters. Use the
  template headings, actual checks, material risks, and rollback notes.
- Immediately before writing to GitHub, confirm the head and task diff are
  unchanged. Rerun only checks invalidated by edits or relevant state changes.

## Pull Request Lifecycle

- Creating or updating a PR includes one immediate snapshot of checks,
  mergeability, reviews, and unresolved threads.
- Unless the user explicitly requests monitoring, a release workflow requires
  it, or an automation already owns it, hand off after the PR is open with the
  current state and next event to watch. Do not delay ordinary handoff with
  fixed quiet-period sleeps.
- For requested monitoring, use event-driven or bounded waits. Report only state
  changes, actionable failures, or a meaningful prolonged delay.
- Investigate failures/comments before changing code. Fix task-attributable
  issues, rerun affected verification, push, reply or resolve the thread, then
  resume the requested monitor.
- Never merge without required reviewer approval or explicit authority.
- After an observed merge, verify the commit reached the base, then clean the
  task worktree/branch when safe. Preserve unmerged work for closed PRs unless
  deletion was explicitly authorized.

## Git and PR Baseline

- Follow Conventional Commits; keep the subject at most 72 characters.
- Source comments, commits, PR titles, and PR bodies are in English.
- Keep every heading from `.github/pull_request_template.md`; use `N/A` where
  needed and include commands actually run.
- Use `--body-file` for multiline `gh pr create`/`gh pr edit` content.
- PR/issue/discussion content must not contain the literal sequence `\n` or
  hard-wrapped prose paragraphs.
- Do not include local absolute paths or tool-specific labels/prefixes in GitHub
  content.
- Resolve review threads after the underlying issue is fixed. If declining a
  suggestion, reply with a short evidence-based reason.
