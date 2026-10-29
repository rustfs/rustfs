# GitHub Workflow Instructions

Applies to `.github/`.

## Pull Requests

PR conventions (English title/body, template usage, `--body-file` with the heredoc pattern, review-thread etiquette) live in [Git and PR rules](../.agents/references/pull-requests.md); do not duplicate them here.

## Workflow Changes

- Changes to workflow YAML or `.github/actionlint.yaml` must pass `actionlint` (run from the repo root) with zero findings before commit/PR. Install via `brew install actionlint`, or the download script in the actionlint repo (rhysd/actionlint); `make pre-pr` does not cover workflow files, so this is the required check for them.
- Custom self-hosted runner labels (`sm-standard-*`, `dind-*`) are declared in `.github/actionlint.yaml`. When introducing a new `runs-on:` label, declare it there in the same change. Never use the config or `-ignore` to silence real findings — fix the workflow (root `AGENTS.md`: make checks pass by fixing the cause, not by weakening the gate).
- If `shellcheck` is installed, actionlint also lints `run:` scripts; treat those findings as part of the gate.
- Runner placement and billing rules live in `.github/workflows/README.md`. A new job defaults to `ubuntu-latest` (free for this public repository); a self-hosted `runs-on` needs one of the reasons listed there, stated in a comment next to it, and a `rust-cache` key is never shared between the hosted and self-hosted images.

## CI Alignment

When changing CI-sensitive behavior, keep local validation aligned with `.github/workflows/ci.yml`. Read the workflow file directly for the current gate steps — do not rely on (or add) a copied command list here; copies go stale. `make pre-pr` covers the shared local gates, not the full CI feature, platform, or end-to-end matrix. Select checks using the root verification tiers.
