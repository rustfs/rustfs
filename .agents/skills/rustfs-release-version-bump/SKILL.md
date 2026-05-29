---
name: rustfs-release-version-bump
description: "Publish a RustFS alpha/beta/stable release with an auditable flow: confirm target version and scope, update workspace and release assets (including strict rustfs.spec changelog identity/date/version format), run required verification, and finish with commit, push, and GitHub PR creation."
---
# RustFS Release Version Bump

Use this skill to publish a RustFS release (alpha, beta, or stable) with a minimal, auditable diff and a complete ship flow (`edit -> verify -> commit -> push -> PR`).

Validated baseline: release pattern used in PR `#2957`.

## Required inputs

- Exact target version, for example `1.0.0-beta.4`.
- Delivery scope:
- Local only (`edit/verify`).
- Local + git (`commit/push`).
- Full GitHub flow (`commit/push/PR`).

If target version is missing or ambiguous, stop and ask before editing.

## Read before editing

- `AGENTS.md` (root and nearest path-specific files).
- `.github/pull_request_template.md`.
- Current branch status and diff against `origin/main`.

## Default release file scope

Treat the following file list as the default checklist for each release bump:

- `Cargo.toml`
- `Cargo.lock`
- `README.md`
- `README_ZH.md`
- `flake.nix`
- `helm/rustfs/Chart.yaml`
- `rustfs.spec`

Only drop a file when the current repository release process clearly no longer requires it.

## Hard release policy

- Docker doc tags use `<version>` (for example `rustfs/rustfs:1.0.0-beta.4`), not `v<version>`.
- Helm chart version mapping follows `beta.N -> 0.N.0`.
- `rustfs.spec` `Release` uses prerelease suffix only (for example `beta.4`).
- Do not change these rules without explicit confirmation.

## Step-by-step workflow

1. Confirm intent and isolate scope
- Confirm target version string exactly.
- Confirm whether user requested local-only or full GitHub flow.
- Inspect current branch and ensure only release-related files are touched for this task.

2. Update workspace versions
- Bump `[workspace.package].version` in `Cargo.toml`.
- Bump internal workspace crate dependency versions in `Cargo.toml`.
- Update `Cargo.lock` so workspace package versions match target version.
- Re-scan for partial leftovers.

3. Update release assets
- `README.md` and `README_ZH.md`: update versioned Docker examples to target version.
- `flake.nix`: update package version to target version.
- `helm/rustfs/Chart.yaml`:
- `appVersion` = target version.
- `version` follows chart mapping rule, for example:
- `1.0.0-beta.3` -> `0.3.0`
- `1.0.0-beta.4` -> `0.4.0`
- `rustfs.spec`:
- Set `Release` to prerelease suffix (example `beta.4`).
- Add/update top changelog entry with exact format:
- `* Thu May 20 2026 houseme <housemecn@gmail.com>`
- `- Update RPM package to RustFS 1.0.0-beta.4`
- Changelog identity and time must come from current environment:
- `git config --get user.name`
- `git config --get user.email`
- `date '+%a %b %d %Y'`
- Changelog version text must match target release version exactly.

4. Verify before shipping
- Run:
- `cargo fmt --all`
- `cargo fmt --all --check`
- `make pre-commit`
- If verification passes, run `cargo clean`.
- If `make pre-commit` fails, return `BLOCKED` with root cause and do not silently widen scope to fix unrelated issues unless user asks.

5. Commit strategy
- Preferred split when both parts changed:
- `chore(release): prepare <version>` for `Cargo.toml` and `Cargo.lock`.
- `chore(release): align release assets for <version>` for docs and packaging files.
- If user asks for one commit, use one commit.
- Stage only intended release files; do not include unrelated working tree changes.

6. Push and PR
- Push branch:
- `git push -u origin <branch>` (first push), or `git push` (tracking already exists).
- Create PR with template headings unchanged:
- `gh pr create --base main --head <branch> --title ... --body-file ...`
- PR title/body must be English.
- Use `N/A` for non-applicable template sections.
- Include verification commands and any `BLOCKED` reason clearly.

## Recommended check commands

- `git status --short --branch`
- `git diff --name-only origin/main...HEAD`
- `git diff --stat origin/main...HEAD`
- `rg -n "<old_version>|<new_version>" Cargo.toml Cargo.lock README.md README_ZH.md flake.nix helm/rustfs/Chart.yaml rustfs.spec`
- `cargo fmt --all`
- `cargo fmt --all --check`
- `make pre-commit`
- `cargo clean`

## Output contract

When using this skill, always report:

- Target version.
- Files changed.
- Any assumptions or uncertainties requiring confirmation.
- Verification result (`PASSED` or `BLOCKED`) with key evidence.
- Commit message(s) used.
- Push status and PR URL when GitHub flow is requested.
