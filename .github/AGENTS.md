# GitHub Workflow Instructions

Applies to `.github/`.

## Pull Requests

PR conventions (English title/body, template usage, `--body-file` with the
heredoc pattern, review-thread etiquette) live in the root `AGENTS.md` under
"Git and PR Baseline" — that section is the single normative home; do not
duplicate its rules here.

## Workflow Changes

- Any change touching `.github/workflows/` must pass `actionlint` (run from
  the repo root) with zero findings before commit/PR. Install via
  `brew install actionlint`, or the download script in the actionlint repo
  (rhysd/actionlint); `make pre-pr` does not cover workflow files, so this is
  the required check for them.
- Custom self-hosted runner labels (`sm-standard-*`, `dind-*`) are declared
  in `.github/actionlint.yaml`. When introducing a new `runs-on:` label,
  declare it there in the same change. Never use the config or `-ignore` to
  silence real findings — fix the workflow (root `AGENTS.md`: make checks
  pass by fixing the cause, not by weakening the gate).
- If `shellcheck` is installed, actionlint also lints `run:` scripts; treat
  those findings as part of the gate.

## CI Alignment

When changing CI-sensitive behavior, keep local validation aligned with
`.github/workflows/ci.yml`. Read the workflow file directly for the current
gate steps — do not rely on (or add) a copied command list here; copies go
stale. `make pre-pr` is the local equivalent of the main gate.
