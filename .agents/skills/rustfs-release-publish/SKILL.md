---
name: rustfs-release-publish
description: Publish a RustFS version through Console, release-branch CI, preview acceptance, human confirmation, and final publication. Use only for an explicit release request.
---
# RustFS Release

Require the exact final SemVer target before version edits or publication. If unspecified, inspect current releases/tags and ask with concrete candidates; do not guess the channel or bump. A named bump type resolves to an exact version that still needs confirmation. Report the confirmed target verbatim; resolve any later mismatch before proceeding. Independent read-only preflight may continue.

## Invariants

- Cargo stores `<target>`, never `-preview.N`. Before publication, installation defaults retain the previous available deliverable. Final and preview binaries embed their own tag names, so version/asset names differ despite identical source.
- Use annotated tags without `v`. Preview names are exactly `<target>-preview.<digits>`; choose the next unused iteration after fetching tags. Preview Releases are prereleases, never Latest; publish only versioned assets/exact Docker tags, never latest assets, R2, Helm, moving Docker channels, or installation/banner updates.
- `release/<target>` owns the candidate. Retain its history, backport only reviewed fixes, and never merge main wholesale or force-push it. Pin `PREVIEW_HASH` to its exact commit with complete passing CI. Preview and final tags both point there; never tag a later main/branch HEAD or insert another bump.
- Phase failures block dependents. Keep preview Releases until final publication and notes generation finish; CI removes Releases while retaining tags. Never move an existing preview or final tag.

## Read by Phase

Read only the current phase's procedure; retain completed evidence across turns. Follow root verification, Git/PR, and authorization rules.

| Phase | Procedure |
|---|---|
| 0 | Clean tree, fetch main/tags, verify GitHub access, then complete the mandatory [Console release gate](references/console-gate.md). A green Console build alone is insufficient. |
| 1–2 | [Prepare/reuse the release candidate and run exact-SHA branch CI](references/release-candidate.md). Source version changes use `rustfs-release-version-bump` with authorized delivery scope. |
| 3 | [Verify the preview build, assets, channels, and notes](references/preview-release.md). |
| 4–5 | [Run the downloaded binary, Console CRUD, and latest-rc matrix](references/preview-acceptance.md). Every check must pass. |
| Confirmation | Complete the gate below and end the turn. |
| 6 | After fresh confirmation, [publish and verify the final tag](references/final-release.md) at `PREVIEW_HASH`. |
| 7 | [Maintain milestones, installation references, and the website](references/post-release-updates.md) only after complete publication. |

## Human Confirmation

After Phases 3–5 pass, recheck that the release branch still equals `PREVIEW_HASH`. Report the target, branch, preview tag/hash, branch CI run, preview Release URL, Console result, and rc matrix. Explicitly ask whether to publish the final tag and end the turn without creating/pushing it.

Only a new affirmative user reply to that exact report authorizes Phase 6. The original release request, earlier confirmation, silence, or automated follow-up does not count. Confirmation binds the target, preview tag, and hash; a changed value or repeated/failed acceptance cycle invalidates it. Revalidate and obtain fresh confirmation before continuing.

## Recovery

- Before the final tag exists, land fixes on main, backport to the candidate, verify target versions, and restart branch CI/acceptance. Use a new preview iteration once a preview was tagged; main may advance independently.
- Once the final tag exists, retry publication jobs for that exact tag. A source fix requires a newly confirmed target, not retagging or a newer preview. Phase 7 retries recheck artifact availability and superseding releases.
- If abandoned after the bump merged, report the unpublished version on main and record whether to revert or let the next release overwrite it. Deleting candidate branches or preview tags requires an explicit decision.

Report completed phase results and incomplete work with evidence: Console previous/latest tags, release need, hash and run/Release URLs; target, branch CI/SHA, previews and hash, confirmation state; preview/final assets, latest-channel state, Console/rc results and cleanup; both milestone states, installation PR checks/merge state, and website text/link/PR/deployment state. Keep artifact publication separate from follow-up delivery; do not call an open PR a live update. Report deviations and their authorization.
