---
name: rustfs-release-publish
description: Publish a RustFS version through frozen Linux candidate acceptance, desktop completion, manifest approval, byte-preserving publication and post-release follow-up. Use only for an explicit release request.
---
# RustFS Release

Require the exact final SemVer target before version edits or publication. If unspecified, inspect releases/tags and ask with concrete candidates; do not guess the channel or bump. Reuse the user's confirmed target and authorized delivery scope. Independent read-only preflight may continue.

## Invariants

- Cargo and every candidate executable contain the final product version. `<target>-preview.<run_number>` is an immutable candidate identifier, not a public tag or displayed binary version.
- Freeze the exact source, reviewed workflow recipe, toolchain, Console, CLI, dependency lock, previous stable version and validation plan. Newer unrelated main or Console releases do not change those inputs.
- Retain `release/<target>` history when a release branch is used; backport only reviewed fixes. Pin `CANDIDATE_SHA` to its exact CI-validated commit. Never force-push a release branch or move a public tag.
- Build Linux first. Complete field acceptance before macOS/Windows and packaging. Full certification and final acceptance are required before publication; a partial Linux PASS grants no release authority.
- Promote the accepted bytes. Final publication never recompiles binaries or rebuilds packages/images. Infrastructure retries reuse successful artifacts; changed source or inputs require a new candidate and affected acceptance.
- Phase failures block dependents. No latest/default-channel update before versioned delivery. Use the [pipeline runbook](../../../docs/operations/release-pipeline.md) for executable entry points, locks and recovery.

## Read by phase

| Phase | Procedure |
|---|---|
| 0 | Clean tree, fetch main/tags, verify access, then complete the mandatory [Console gate](references/console-gate.md). |
| 1–2 | [Prepare the exact source and run branch CI](references/release-candidate.md). Version edits use `rustfs-release-version-bump`. |
| 3 | [Build and verify the Linux candidate](references/preview-release.md). |
| 4–5 | [Run artifact, field, Console and rc acceptance](references/preview-acceptance.md), then complete desktop/package/image/chart validation. |
| Confirmation | Report the complete manifest and obtain the confirmation below. |
| 6 | [Publish the accepted manifest](references/final-release.md). |
| 7 | [Reconcile advisories, milestones, installation references, website and release branch](references/post-release-updates.md). |

## Human confirmation

After all acceptance passes, report the target, source SHA, source CI run, Linux and completion run URLs, final manifest SHA-256, accepted Console/CLI identities, compatibility/performance results and backlog report. Explicitly ask whether to publish this exact manifest and end the turn without dispatching publication.

Only a new affirmative reply to that exact report authorizes Phase 6. An earlier general release request, silence or automatic continuation does not count. Reuse confirmation for retries of that same manifest; do not ask again merely because a destination failed. A changed source, input, manifest or acceptance result invalidates it and requires renewed acceptance/confirmation.

## Recovery and reporting

Resume failed infrastructure jobs without rebuilding successful units. Fix product defects on main, backport when needed, and start a new candidate. After a final tag exists, retry the same bytes or use a new patch version; never move the tag. Do not conflate implemented, CI-passed, field-accepted, approved, published and post-release-complete states. Report incomplete work and deviations with evidence; a tag or green build alone is not a completed release.
