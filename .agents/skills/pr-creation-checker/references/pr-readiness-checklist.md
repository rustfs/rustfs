# PR Readiness Checklist

- Confirm the branch is based on current `main`.
- Confirm the diff matches the stated scope.
- Confirm no secrets, logs, temp files, or unrelated refactors are included.
- Confirm `make pre-commit` passed, or document why it could not run.
- Confirm extra verification commands are listed for risky changes.
- Confirm the PR title uses Conventional Commits and stays within 72 characters.
- Confirm the PR body is in English.
- Confirm the PR body keeps the exact headings from `.github/pull_request_template.md`.
- Confirm non-applicable sections are filled with `N/A`.
- Confirm multiline GitHub CLI commands use `--body-file`.
