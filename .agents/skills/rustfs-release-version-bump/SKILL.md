---
name: rustfs-release-version-bump
description: "Prepare Cargo versions for an exact RustFS target, or align installation references after its artifacts are published, with verification and optional commit/push/PR delivery. Use for an explicit version bump or when invoked by the release-publish workflow."
---
# RustFS Release Version Bump

Use this skill to prepare and verify release version files. Commit, push, and PR steps apply only when included in the user's delivery scope; publishing release tags belongs to `rustfs-release-publish`.

## Required inputs

- Exact target version, for example `1.0.0-beta.4`.
- Stage: `prepare` (default) or `post-release`. Infer `post-release` only when the user or parent release workflow explicitly requests installation updates after publication; a normal version bump means `prepare`.
- Delivery scope: local (`edit/verify`), git (`commit/push`), or GitHub
  (`commit/push/PR`). Derive it from the conversation; when unspecified, prepare
  and verify locally without blocking on a delivery question.

If target version is missing or ambiguous, stop and ask before editing.

Reject any target version containing `-preview`: preview identifiers are tag-only (see `rustfs-release-publish`) and must never be written into version files. If asked for one, stop and point to the release pipeline instead of editing.

## Read before editing

- `AGENTS.md` (root and nearest path-specific files).
- `.github/pull_request_template.md` only when preparing a PR.
- Current branch status and diff against `origin/main`.

## Stage boundaries

`prepare` updates only:

- `Cargo.toml`
- `Cargo.lock`

`post-release` updates installation and packaging references only:

- `README.md`
- `README_ZH.md`
- `flake.nix`
- `helm/rustfs/Chart.yaml`
- `rustfs.spec`

During preview, installation references intentionally retain the previous published deliverable. Do not sweep them into a source bump to make every version string match. `flake.nix` builds local source, but its package label is aligned with the other packaging references after publication.

Before `post-release` edits, verify that the exact non-preview target has a published GitHub Release, downloadable assets and source archive, and a pullable `rustfs/rustfs:<target>` image manifest. If any prerequisite is missing, report `BLOCKED` and leave installation references unchanged. If a newer deliverable has already superseded this target, do not downgrade installation defaults during a retry.

Helm CI derives chart versions from the triggering tag, so the final tag can retain the previous Chart.yaml values. Update the repository copy only after the target image and published chart are available. Neither post-release changes nor their merge commit may replace the preview-validated final tag.

## Hard release policy

- Docker doc tags use `<version>` (for example `rustfs/rustfs:1.0.0-beta.4`), not `v<version>`.
- Derive Helm chart and app versions with `scripts/helm_chart_version.sh <target>`; the existing mapping is `beta.N -> 0.N.0`, with other target versions retained.
- `rustfs.spec` `Release` uses the prerelease suffix (for example `beta.4`), or `1` for a stable release.
- Do not change these rules without explicit confirmation.

## Step-by-step workflow

1. Confirm intent and isolate scope
- Use the exact target and delivery scope already supplied; ask only for a missing or ambiguous target or a material release-policy choice.
- Inspect current branch and ensure only release-related files are touched for this task.

2. Update workspace versions (`prepare` only)
- Bump `[workspace.package].version` in `Cargo.toml`.
- Bump internal workspace crate dependency versions in `Cargo.toml`.
- Update `Cargo.lock` so workspace package versions match target version.
- Re-scan Cargo.toml and workspace members in Cargo.lock for partial leftovers; leave external dependency versions unchanged.

3. Update installation references (`post-release` only)
- `README.md` and `README_ZH.md`: update versioned Docker examples to target version.
- `flake.nix`: update package version to target version.
- `helm/rustfs/Chart.yaml`: use the app and chart versions returned by `scripts/helm_chart_version.sh <target>`.
- `rustfs.spec`:
- Set `Version` to the numeric version and `Release` to the prerelease suffix (example `beta.4`), or `1` for a stable release. Verify that `Source0` and the unpacked source directory resolve to the exact published target, including its prerelease suffix when present.
- Add/update top changelog entry with exact format:
- `* Thu May 20 2026 houseme <housemecn@gmail.com>`
- `- Update RPM package to RustFS 1.0.0-beta.4`
- Changelog identity and time must come from current environment:
- `git config --get user.name`
- `git config --get user.email`
- `date '+%a %b %d %Y'`
- Changelog version text must match target release version exactly.

4. Verify before shipping
- Follow the root verification tiers for the final diff instead of running a full-workspace gate for version strings.
- For `prepare`, validate Cargo metadata with the updated lockfile and confirm workspace package/internal dependency versions agree. Do not accept a lockfile containing unrelated dependency updates.
- For `post-release`, render the Helm chart and confirm its default image is the verified target; run `scripts/test_helm_chart_version.sh` when chart versions change. Check the README image tags, Nix package version, and expanded RPM source URL against that same target.
- Run `git diff --check` for either stage. Report unresolved required checks as `BLOCKED`; do not silently widen scope to fix unrelated issues.

5. Commit strategy (only when committing is authorized)
- Use `chore(release): prepare <version>` for `prepare` and `chore(release): align installation references for <version>` for `post-release`.
- These stages happen on opposite sides of publication; do not combine them in a preview-preparation commit or PR.
- Stage only intended release files; do not include unrelated working tree changes.

6. Push and PR (only for the authorized delivery scope)
- Push branch:
- Use the user-requested or configured push remote: `git push -u <push-remote> <branch>` (first push), or `git push` when tracking is already configured.
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

## Output contract

When using this skill, always report:

- Target version and stage.
- Files changed.
- Any assumptions or uncertainties requiring confirmation.
- Verification result (`PASSED` or `BLOCKED`) with key evidence.
- Commit message(s) used.
- Push status and PR URL when GitHub flow is requested.
