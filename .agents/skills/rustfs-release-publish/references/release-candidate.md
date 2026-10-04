# Release Candidate Preparation

Read for Phases 1–2. The [parent release invariants](../SKILL.md) and Console gate apply.

## Candidate Resume Preflight

- `git status --short` clean; `git fetch origin main --tags`.
- `gh auth status` works; confirm you can view `gh release list -L 3`.
- Confirm the exact final target version with the user if not explicit.
- Check whether `release/<target>` already exists before creating it; a resumed release reuses the existing branch and its history. For an in-flight release that already has a preview tag but no branch, anchor the new branch to that tag's commit only after verifying its target versions and prior acceptance evidence.

## Phase 1 — Source version bump to the final target (once)

- If `release/<target>` exists, verify its Cargo.toml and workspace members in Cargo.lock read `<target>` and skip the source bump, even if main has advanced. If the branch versions differ, stop and resolve the mismatch without downgrading main.
- If no branch exists but an unfinished preview tag does, verify its commit has `<target>` in both version files and skip the bump; Phase 2 anchors the branch to that commit. Stop if those files disagree. Otherwise, if main already has `<target>` in both files, skip the bump. Installation references intentionally retain the previous published deliverable during preview; if they were advanced to an unavailable target, restore that baseline before choosing the next candidate.
- Only when neither the release branch, an unfinished preview tag, nor current main already has `<target>`: if main is at a later version, select and verify a historical commit containing `<target>` rather than downgrading main; block if none exists. Otherwise invoke the `rustfs-release-version-bump` skill with stage `prepare`, the final `<target>` (NOT a preview version), and full GitHub flow (commit/push/PR).
- If a bump PR was needed, get it merged into main and record its merge commit. For a new release branch when main already has `<target>`, identify the main commit that introduced those versions. Phase 2 chooses a release candidate containing that commit; an existing release branch keeps its own history and a newer main HEAD is not required.

Do not wait for the moving latest main HEAD to become green. The release-branch CI in Phase 2 validates the chosen commit before a preview tag is created.

## Phase 2 — Pin and validate the release candidate, then publish the preview tag

- Use `release/<target>`. On the first attempt, create it from a selected main commit containing the version bump and intended changes; it need not be the latest main HEAD. On a restart, retain the existing branch and backport only reviewed fixes from main. Verify the branch's Cargo versions still equal `<target>`. If adopting an existing preview, run this branch CI before continuing its acceptance; reuse the tag only when it points to the validated SHA.
- The current `ci.yml` runs automatically on main pushes, not release-branch pushes. Dispatch it explicitly with `gh workflow run ci.yml --ref "release/<target>"`. Locate that dispatch run, verify its `head_sha` equals the release branch's remote HEAD, and require the full expected CI job set to pass, including `Quick Checks`, `Test and Lint`, and `End-to-End Tests (full merge gate)`. Treat a missing or skipped required lane as incomplete. A successful PR check or CI run for a different SHA does not qualify. If the branch advances or a run is cancelled, validate the new SHA again. Do not tag a candidate with failing or incomplete CI.
- Prefer protecting `release/*` against force pushes and deletion and requiring reviewed backport PRs. Check live rulesets rather than assuming the main ruleset covers release branches; report a missing release-branch rule as a gap.
- Set `PREVIEW_HASH` to the CI-validated release-branch commit and recheck the remote branch HEAD immediately before tagging. Report the branch, commit, and CI run URL; do not derive `PREVIEW_HASH` from a later `origin/main` fetch. When adopting an existing preview tag at this SHA, skip tag creation and continue Phase 3.

```bash
git tag -a "<preview-tag>" -m "Release <preview-tag>" "$PREVIEW_HASH"
git push origin "<preview-tag>"
```

Pushing the tag triggers `.github/workflows/build.yml` ("Build and Release"); `docker.yml` chains off it via `workflow_run`.

The preview run builds versioned artifacts and publishes them in a GitHub prerelease. Docker may publish exact preview image tags. Latest-channel, R2, and Helm publication must be skipped; installation references and the website announcement stay unchanged.

On a restart after a backport, re-run release-branch CI before setting the new `PREVIEW_HASH`. Use the next unused preview iteration when the validated commit differs from an existing preview tag; never move an existing tag.
