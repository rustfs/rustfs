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

Do not wait for the moving latest main HEAD to become green. The release-branch CI in Phase 2 validates the chosen commit before a candidate build is dispatched.

## Phase 2 — Pin and validate the release source

- Use `release/<target>`. On the first attempt, create it from a selected main commit containing the version bump and intended changes; it need not be the latest main HEAD. On a restart, retain the existing branch and backport only reviewed fixes from main. Verify the branch's Cargo versions still equal `<target>`. A legacy preview can anchor source selection, but its tag and old artifacts do not constitute a candidate in the new protocol; freeze and verify a new candidate after the release recipe is aligned.
- The current `ci.yml` runs automatically on main pushes, not release-branch pushes. Dispatch it explicitly with `gh workflow run ci.yml --ref "release/<target>"`. Locate that dispatch run, verify its `head_sha` equals the release branch's remote HEAD, and require the full expected CI job set to pass, including `Quick Checks`, `Test and Lint`, and `End-to-End Tests (full merge gate)`. Treat a missing or skipped required lane as incomplete. A successful PR check or CI run for a different SHA does not qualify. If the branch advances or a run is cancelled, validate the new SHA again. Do not dispatch a candidate with failing or incomplete CI.
- Prefer protecting `release/*` against force pushes and deletion and requiring reviewed backport PRs. Check live rulesets rather than assuming the main ruleset covers release branches; report a missing release-branch rule as a gap.
- Set `CANDIDATE_SHA` to the CI-validated release-branch commit. Record the branch, exact commit and CI run URL. The new candidate protocol starts from a commit whose release recipe matches the reviewed main dispatcher; backport the recipe before freezing an older release branch if needed.
- Dispatch the Linux candidate as described in [candidate verification](preview-release.md). Do not create a preview or final tag here. The final executable already contains `<target>`; publication later promotes those same bytes.
- After a source or build-input change, run the affected source checks and create a new candidate. Reuse the frozen candidate after an infrastructure failure. Do not restart acceptance for unrelated main advances.
