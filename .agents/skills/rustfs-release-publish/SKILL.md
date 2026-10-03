---
name: rustfs-release-publish
description: "Run the RustFS console gate, source version bump, release-branch CI, preview validation, human confirmation, final-tag publication, and post-release milestone, installation, and website updates. Use only when the user explicitly asks to release or publish a RustFS version (发版/发布)."
---
# RustFS Release Publish (preview-validated pipeline)

This skill orchestrates a full release. It wraps `rustfs-release-version-bump` (invoked here with the authorized commit/push/PR scope) with a mandatory preview-tag validation loop before the final tag is published.

The binary reports its build tag (`build::TAG` via shadow_rs; `SHORT_VERSION` in
`rustfs/src/config/cli.rs`), and `build.yml` derives asset names and preview
classification from that tag. Cargo.toml supplies only the no-tag fallback.
Preview and final tags must therefore share the validated source commit;
their tag-dependent version and asset names differ. The channel and cleanup
constraints are defined once under Preview tag naming and Hard rules below.

Pipeline shape:

```
check console main against its latest Release
  -> if ahead: publish console -> wait for Release asset + latest API
  -> bump Cargo.toml and Cargo.lock to <target> -> merge
  -> cut release/<target> from a selected main commit containing the bump
  -> CI green for that exact release-branch commit
  -> tag <preview-tag> at that commit
  -> verify preview Release assets -> run binary locally + console checks
  -> validate with latest rc client
  -> report preview acceptance results -> STOP for explicit human confirmation
  -> tag <target> at the SAME commit (zero delta) -> re-verify CI/release
  -> CI deletes the <target>-preview.N Releases (tags kept)
  -> verify published images -> publish Helm chart from the validated source
  -> close the released version's milestone -> ensure the next version's milestone exists
  -> update installation references on main -> update rustfs.com announcement
```

On candidate or preview validation failure: fix lands on main via normal PR, then backport only the needed fix to `release/<target>` through a reviewed PR. Re-run release-branch CI at the new commit and, if a preview was already tagged, tag `<preview-tag N+1>` there and restart acceptance. Installation references remain on the previous published deliverable throughout preview validation.

## Required inputs

- Final target version, for example `1.0.0-beta.10`.
- Preview iteration `N` (default: next unused preview tag for that target; check with `git tag -l '<target>-preview.*'` after `git fetch --tags`).

If the target version is missing or ambiguous, collect the current release/tag baseline and ask before version edits or publication. Continue independent read-only preflight while the answer is pending (see the semver gate below).

## Semver gate — resolve the target before version edits or publication

Versions follow [SemVer 2.0.0](https://semver.org/). Precedence reminder:

```
1.0.0-alpha < 1.0.0-alpha.1 < 1.0.0-beta.2 < 1.0.0-beta.11 < 1.0.0-rc.1 < 1.0.0 < 1.0.1 < 1.1.0 < 2.0.0
```

Numeric prerelease identifiers compare numerically (`beta.9 < beta.10`), not lexically — see [semver.org spec item 11](https://semver.org/#spec-item-11). Preview tags are internal validation tags layered on top of the target's prerelease channel — they are never themselves a deliverable version and never appear in version files.

Rules:

- A request like "发个版" / "release the next version" without an exact version string is ALWAYS ambiguous. Derive the current latest tag (`git tag --sort=-v:refname | head`), then ask the user to choose with concrete candidates, e.g. from `1.0.0-beta.10`: next prerelease `1.0.0-beta.11`, promote to `1.0.0-rc.1`, promote to stable `1.0.0`. Never guess between these — they have very different meanings (channel promotion vs. iteration) and different CI classification consequences.
- After a stable `X.Y.Z` exists, the next version must state which component bumps: patch `X.Y.(Z+1)` for fixes only, minor `X.(Y+1).0` for backward-compatible features, major `(X+1).0.0` for breaking changes. If the user names a bump type but not a number, compute it from the latest stable tag and echo the exact resulting version back for confirmation.
- Echo the final confirmed version string verbatim in your first status report; every later phase must use exactly that string. If at any point the user's wording and the confirmed version diverge, stop and re-confirm.

## Preview tag naming

- Use `<target>-preview.N` for every target, e.g. `1.0.0-beta.10-preview.3` or `1.1.0-preview.1`.
- The canonical suffix is exactly `-preview.<digits>`. `build.yml` recognizes it before alpha/beta/rc classification and routes it to the preview-only path; any other tag containing `-preview` fails closed instead of being treated as a release.
- A preview Release MUST be published with `isPrerelease=true` and `isLatest=false`. Any `*-latest` preview asset or preview-triggered `latest.json`, R2, Helm, or moving Docker channel tag is a pipeline failure. Docker images may use the exact preview version tag, including its existing variant suffixes.
- Preview Releases are cleaned up by the `cleanup-preview-releases` job after `publish-release` succeeds for the deliverable tag. It deletes every Release whose tag is exactly `<target>-preview.<digits>` and never passes `--cleanup-tag`, so the tags survive.

## Hard rules

- Before preview, bump only Cargo.toml (workspace package and internal dependency versions) and the corresponding workspace members in Cargo.lock, directly to `<target>`. Keep README installation examples, flake.nix, Chart.yaml, and rustfs.spec on the previous published deliverable until Phase 7. Never write a `-preview.N` suffix into these files; preview identifiers belong to validation tags and artifacts.
- Publishing a tag alone does not make an installation target available. Verify the exact image manifest, Release assets, and source archive before advancing references that consume them. Helm packaging derives its versions from the final tag and waits for that image; it does not require an early Chart.yaml bump on main.
- Preview Release assets are versioned and intentionally visible on the Releases page for the duration of validation. Do not label them Latest or use them to update any latest distribution channel.
- Never delete a preview Release by hand before Phase 6 finishes — Phase 4 downloads its assets and the final Release notes are generated while it still exists. Cleanup is CI's job; only step in manually (`gh release delete "<preview-tag>" --yes`, never `--cleanup-tag`) if `cleanup-preview-releases` failed.
- Tags have no `v` prefix. Always annotated: `git tag -a <tag> -m "Release <tag>"`.
- The release branch is the candidate source during multi-day acceptance. Do not merge main wholesale into it, force-push it, or advance it with unrelated changes. The final tag MUST point at exactly `PREVIEW_HASH` — the commit the validated preview tag points at. Never tag current `main` HEAD or an advanced release-branch HEAD, and never create an extra version-bump commit between preview and final. Phase 7 updates installation references on main after publication; neither tag moves to that follow-up commit.
- When a previous deliverable exists, GitHub Release notes for the preview and final tags MUST use it as their shared comparison baseline: the most recently published non-preview Release before the target. Internal `-preview.N` Releases are explicitly excluded from that selection, even when they point at the same commit as the final tag — cleanup runs after the notes are generated, so the preview Release is still present and would otherwise be picked as the baseline. If no previous deliverable exists, omit `previous_tag_name` and record that GitHub's default baseline fallback was used.
- Generated Release notes carry a workflow-management marker so retries can repair them. Before manually curating a generated body, remove that marker; unmarked non-placeholder notes are preserved by later workflow runs.
- Phases run in order; a failure blocks dependent steps. For a candidate or preview/source acceptance failure before the final tag exists, land the fix on main, backport it to the release branch, verify Cargo there still matches the confirmed target, and restart from release-branch CI in Phase 2. Use the next preview iteration if any preview was already tagged. Main may advance to another target without changing this release candidate.
- Once the final tag exists, preserve its commit. Retry failed publication jobs for that exact tag; do not recreate the tag or restart preview on newer main. If a source change is required after final-tag publication, report the failure and obtain a new target version. Phase 7 failures resume only the failed follow-up after rechecking artifact availability and whether a newer release has superseded it.
- Completing preview acceptance does not authorize the final tag. After Phases 3–5 pass, report the acceptance evidence and stop until the user explicitly confirms continuation. The original release request, an earlier confirmation, silence, or an automated follow-up does not satisfy this gate.
- Confirmation is scoped to the reported `<target>`, `<preview-tag>`, and `PREVIEW_HASH`. A failed or repeated acceptance cycle, including any new preview iteration, invalidates prior confirmation and requires a new one.
- If the release is abandoned after Phase 1 merged, main's version files claim a version that was never tagged. Either revert the bump PR or leave it to be overwritten by the next release — but tell the user explicitly and record the decision. Do not delete the release branch or preview tags as part of abandonment without an explicit decision.
- User-facing status updates in Chinese; commits, PR titles/bodies, and tag messages in English. No hard-wrapping in commit messages, PR bodies, or documentation prose — one logical line per sentence/paragraph, let soft wrap handle display.

## Phase 0 — Preflight

- `git status --short` clean; `git fetch origin main --tags`.
- `gh auth status` works; confirm you can view `gh release list -L 3`.
- Confirm the exact final target version with the user if not explicit.
- Check whether `release/<target>` already exists before creating it; a resumed release reuses the existing branch and its history. For an in-flight release that already has a preview tag but no branch, anchor the new branch to that tag's commit only after verifying its target versions and prior acceptance evidence.

### Console release gate

Read and complete [the Console gate](references/console-gate.md) before Phase 1. Verify the latest published Console asset and exact commit; if Console main is ahead, complete its release and asset verification first. A successful build alone does not satisfy this gate.

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

## Phase 3 — CI and preview Release verification

- Find and watch the tag build: `gh run list --workflow build.yml --branch "<preview-tag>" --limit 1` then `gh run watch <run-id>`. Every build matrix target must succeed (linux x86_64/aarch64 × musl/gnu, macos-aarch64, windows-x86_64).
- Confirm the preview tag resolves to the Phase 2 CI-validated `PREVIEW_HASH`. If the release branch advances during acceptance, repeat Phases 2–5 with a new preview tag.
- Confirm the Release publication jobs (`create-release`, `upload-release-assets`, and `publish-release`) succeed while `update-latest-version` is skipped.
- Verify `gh release view "<preview-tag>" --json isPrerelease,assets,url`: `isPrerelease` must be `true`, and the Release must contain all 6 versioned platform zips, checksums, SBOM, and provenance with no `-latest` assets. Confirm `gh api repos/{owner}/{repo}/releases/latest --jq .tag_name` does not return `<preview-tag>`.
- Record `PREVIOUS_DELIVERABLE`, selected from published Releases by `publishedAt` after excluding the current tag and every `-preview.N` tag. Verify `gh release view "<preview-tag>" --json body --jq .body` contains `## What's Changed` and, when `PREVIOUS_DELIVERABLE` exists, `**Full Changelog**: https://github.com/rustfs/rustfs/compare/<PREVIOUS_DELIVERABLE>...<preview-tag>`. For a repository with no previous deliverable, verify a Full Changelog link exists and record the GitHub baseline fallback.
- Confirm Helm is skipped. If preview Docker images are published, confirm they use only exact preview tags (and variant suffixes), with no `latest`, `alpha`, `beta`, or `rc` channel updates. Preview validation covers the built RustFS binaries, embedded console, and rc compatibility; it does not authorize advancing any installation default.

## Phases 4–5 — Local artifact, Console, and rc acceptance

Read and complete [preview acceptance](references/preview-acceptance.md): verify the downloaded binary's tag/SHA and readiness, exercise Console CRUD with byte-identical download, and pass the full latest-rc command matrix. Any failure blocks final publication. Retain the results for the confirmation gate below.

### Manual confirmation gate

After every Phase 3–5 check passes, verify the release branch still points to `PREVIEW_HASH`. Report the target, release branch, preview tag, `PREVIEW_HASH`, release-branch CI run, preview Release URL, console result, and rc matrix, then explicitly ask the user whether to publish the final tag. End the turn without creating or pushing `<target>`.

Continue to Phase 6 only after a new user reply explicitly confirms the reported target, preview tag, and commit. A clear affirmative reply to that exact report, such as `确认继续`, is sufficient; if the reply is ambiguous or any reported value changed, ask again.

## Phase 6 — Publish the final tag on the validated commit

No second source version bump. Before creating the final tag, recheck the remote `release/<target>` HEAD equals `PREVIEW_HASH`. If it moved after confirmation, the approval no longer applies; validate and obtain confirmation for a new preview. The final tag goes on the exact commit the preview validated:

```bash
git fetch origin --tags
git rev-parse "<preview-tag>^{commit}"          # must equal PREVIEW_HASH — abort if not
git tag -a "<target>" -m "Release <target>" "$PREVIEW_HASH"
git push origin "<target>"
```

- CI rebuilds from the same source; the only changed input is the tag name, so the binary now self-reports `<target>`.
- Verify the final tag's complete publication path: all matrix and release jobs green; `gh release view "<target>"` shows the full versioned and `-latest` asset set plus checksums, SBOM, and provenance; Docker and Helm workflows succeed; `latest.json` points to `<target>`. A stable target must have `isPrerelease=false` and `isLatest=true`. An alpha/beta/rc target must have `isPrerelease=true`; GitHub does not permit prereleases to be Latest, but the project `latest.json` still advances to the final non-preview target.
- Verify the final Release body contains `## What's Changed` and a Full Changelog link. When `PREVIOUS_DELIVERABLE` exists, the link MUST be `https://github.com/rustfs/rustfs/compare/<PREVIOUS_DELIVERABLE>...<target>` and the baseline MUST equal the preview Release baseline; for example, both `1.0.0-beta.12-preview.1` and `1.0.0-beta.12` compare from `1.0.0-beta.11`.
- Verify the preview cleanup: `cleanup-preview-releases` must succeed, `gh release view "<preview-tag>"` must then report `release not found` for every preview iteration of this target, and `git rev-parse "<preview-tag>^{commit}"` must still resolve to `PREVIEW_HASH` (the tag is kept). If the job failed, delete the leftover Releases manually with `gh release delete "<preview-tag>" --yes` and report it.
- Optionally spot-check `./rustfs --version` from a final-tag artifact — it must report `<target>`.

## Phase 7 — Milestones, installation references, and website announcement

Only after Phase 6 succeeds, complete [post-release updates](references/post-release-updates.md): close the released version's milestone and ensure the next version's milestone exists, align installation references on main, then update the existing top banner in `rustfs/rustfs.com`. Milestone maintenance is required for every non-preview release and is independent of installation PR merges or website deployment. A failed or incomplete publication leaves milestones unchanged and installation references and the banner on the previous available version. These follow-up commits never change `PREVIEW_HASH` or either release tag. Track milestone, PR, merge, and deployment states separately from artifact publication; an open PR is not a live website update.

## Output contract

Always report:

- Console gate result: previous/latest Console tags, whether merged changes required a release, `CONSOLE_HASH`, and Console run/Release URLs when a release was published.
- Target version, release branch, release-branch CI run and validated SHA, preview tag(s) used, `PREVIEW_HASH` (which both tags point at).
- Manual confirmation gate status (`WAITING_FOR_CONFIRMATION` or `CONFIRMED`) and its exact target, preview tag, and `PREVIEW_HASH`.
- Per-phase result (PASS/FAIL/BLOCKED) with key evidence: preview and final Release URLs, preview `isPrerelease`/`isLatest` state, final latest-channel state, console check results, the rc command matrix, and the preview-Release cleanup result (deleted Releases plus surviving tags).
- Released-version milestone title, URL, and verified closed state; next-version milestone title, URL, state, and whether it already existed or was created. Report missing milestones or failed checks/updates as incomplete follow-up work.
- Post-release installation PR and verification results; website banner target, text, link, PR, and observed deployment state. Report any remaining merge authorization or failed deployment explicitly rather than claiming the banner is live.
- Any deviation from this pipeline and why the user approved it.
