---
name: rustfs-release-version-bump
description: Prepare a RustFS release branch like PR #2957 by bumping versioned files, aligning release assets, running required verification, and finishing with commit, push, and gh PR creation. Use when publishing a new RustFS alpha, beta, or stable release.
---

# RustFS Release Version Bump

Use this skill when the task is to prepare a new RustFS version release branch following the pattern validated in PR `#2957`.

## Read first

- Read `AGENTS.md`.
- Read `.github/pull_request_template.md`.
- Inspect the current branch diff against `origin/main`.
- Do not assume every release file should change in the same way every time.

## Scope validated by PR #2957

The `1.0.0-beta.3` release branch updated these files:

- `Cargo.toml`
- `Cargo.lock`
- `README.md`
- `README_ZH.md`
- `flake.nix`
- `helm/rustfs/Chart.yaml`
- `rustfs.spec`

Treat this file list as the default checklist for future release bumps. Only drop a file if the repository's current release pattern clearly says it is no longer part of the publish flow.

## Workflow

1. Confirm release intent
- Identify the target version string exactly, for example `1.0.0-beta.4`.
- Check whether the user wants only local commit preparation or the full `commit + push + PR` flow.
- Compare the branch against `origin/main` and isolate only release-related edits.
- If the target version is not explicit, stop and ask for it before editing.

2. Update core Rust workspace versions
- Bump the workspace version in `Cargo.toml`.
- Bump all internal workspace crate versions in `Cargo.toml`.
- Ensure `Cargo.lock` reflects the same release version for workspace packages.
- Re-read the diff and verify there are no partial version leftovers.

3. Align release assets
- Update versioned Docker examples in `README.md` and `README_ZH.md` using the `<version>` tag form, for example `rustfs/rustfs:1.0.0-beta.4`.
- Update `flake.nix` package version.
- Update `helm/rustfs/Chart.yaml` `appVersion`.
- Update `helm/rustfs/Chart.yaml` `version` using the release-chart rule:
- `1.0.0-beta.3` -> `0.3.0`
- `1.0.0-beta.4` -> `0.4.0`
- Follow the same pattern for later beta releases unless the repository rule changes.
- Update `rustfs.spec` release metadata and changelog entry.
- Keep `rustfs.spec` `Release` aligned with the app version string.
- Keep the release asset changes in a separate commit from the core Rust workspace version bump when the branch contains both.

4. Stop and discuss before changing release policy
- Ask before changing the established Docker tag style away from `<version>`.
- Ask before changing the established Helm chart version mapping away from `beta.N -> 0.N.0`.
- Ask before changing the established `rustfs.spec` `Release` rule away from the app version string.
- Ask before widening the release scope beyond the files already validated in PR `#2957`.

5. Verify before shipping
- Run `make pre-commit`.
- If verification succeeds, run `cargo clean` to remove generated build artifacts before wrapping up.
- If `make pre-commit` fails, stop and return `BLOCKED`.

6. Commit structure
- Prefer two commits when the change naturally splits:
- `chore(release): prepare <version>` for `Cargo.toml` and `Cargo.lock`
- `chore(release): align release assets for <version>` for docs and packaging metadata
- If the user asks for a single commit, follow that request.

7. Push and PR
- Push the release branch with `git push -u origin <branch>` or `git push` if upstream already exists.
- Create the PR with `gh pr create --base main --head <branch> --title ... --body-file ...`.
- Keep the PR title and body in English.
- Keep the `.github/pull_request_template.md` headings exactly.

## Ready-to-check commands

- `git diff --name-only origin/main...HEAD`
- `git diff --stat origin/main...HEAD`
- `make pre-commit`
- `cargo clean`
- `git status --short --branch`

## Output expectations

When using this skill, return:

- The files changed for the release bump
- Any uncertainty that needs user confirmation before editing
- Verification status
- Commit messages used
- Push status and PR URL when the GitHub flow was requested

## Established release policy

- Docs use Docker tags in `<version>` form, not `v<version>`.
- `helm/rustfs/Chart.yaml` `version` follows `beta.N -> 0.N.0` based on the current release policy.
- `rustfs.spec` `Release` follows the app version string.
- If any of these rules need to change in the future, pause and confirm before editing.
