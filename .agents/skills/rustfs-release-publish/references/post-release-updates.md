# Post-release Security Advisory, Milestone, Installation, and Website Updates

Read only after the final non-preview tag has passed Phase 6. Tag creation or a green binary build alone is insufficient: the release assets, source archive, container images, Helm package, and applicable latest channels must be available. Preview tags never enter this phase.

## Security Advisories

Reconcile advisories after every completed final release, including Console-only releases. Read-only preparation may start earlier; do not claim an unpublished artifact is an available fix.

1. Fetch every page of the repository's live Security Advisories, including triage, draft, published, and closed states. A permission error or partial response blocks reconciliation; it does not mean there are no advisories. Keep unpublished advisory details in private task artifacts, never in a public PR, release note, or issue.
2. For each potentially relevant advisory, map its fix to the exact published tag and verify the regression evidence. Main-branch reachability, a merged private fix, or a green build alone does not prove the shipped version is fixed. Check cherry-picked equivalents and reverted fixes explicitly. For embedded Console or dependency fixes, verify the shipped component version.
3. Determine the first available fixed release and affected version ranges from evidence. Preserve an earlier correct patched version; do not relabel all fixes as the current release. An unchanged server baseline such as a Console-only release requires checking the earlier server release as well. Leave unresolved reports and fixes absent from the tag pending; do not close them merely because a release shipped.
4. Prepare any necessary corrections to package identity, affected ranges, patched versions, fix/release links, and advisory text. Preserve unrelated fields, credit, CVE identifiers, and disclosure coordination. Apply updates only within existing user authorization; read back the result to verify it. Publishing a draft, requesting a CVE, or closing a report is a separate action: require explicit authorization for the concrete advisories and respect any embargo. Release-tag approval alone does not authorize those actions. Prepare the exact proposed changes before asking for missing authorization.
5. Record each relevant advisory's URL, current state, fix evidence, first fixed version, action taken or proposed, and remaining blocker in private task artifacts. Report advisory follow-up separately from artifact publication. A successful release with unresolved advisory work is not complete follow-up; retain that work for the release monitor or a concrete user decision.

## Version milestones

1. Query all pages of both open and closed milestones in the RustFS release repository, matching titles exactly to version strings without a `v` prefix. A failed or incomplete query is not evidence that a milestone is absent; resolve the lookup before making changes.
2. Close the milestone matching the published `<target>` if it is open; an already closed milestone needs no change. If it is missing, report the gap and continue checking the next version. Do not create a substitute for the released milestone or close/reassign its issues or PRs.
3. Use an explicitly planned next version when provided; otherwise, for a stable `X.Y.Z`, use `X.Y.(Z+1)`. For example, after `1.0.1` is published, close milestone `1.0.1` and ensure milestone `1.0.2` exists. For an alpha/beta/rc target with a trailing numeric counter, increment that counter within the same channel unless a different next version was specified. If the next version cannot be determined, ask for that version and report this follow-up as blocked. Creating a planning milestone does not confirm the target of a future release or bypass the semver gate.
4. Reuse an existing next-version milestone, whether open or closed; do not reopen it or create a duplicate. Only when the complete query confirms it is absent, create an open milestone with the exact next-version title. Do not invent a due date or description. After an uncertain creation result, query again before retrying.
5. Read back both milestones and report their titles, URLs, and actual states, including whether the next milestone was reused or created. A failed close, create, or verification leaves milestone maintenance incomplete; retry only the unfinished operation after refreshing live state. Continue independent installation and website work without moving or republishing the release tag.

## Installation references

1. Re-read the published deliverable state before edits or a retry. If a newer release has superseded this target, do not downgrade installation references or the website announcement. Report the superseding release and leave those defaults intact.
2. Invoke `rustfs-release-version-bump` with stage `post-release`, the exact target, and the authorized delivery scope. Prepare the follow-up on current main; do not edit or move either validated release tag. Cargo versions may already be preparing the next target and must remain untouched.
3. Verify the exact Docker image manifest, Release download links, RPM source archive, and Helm repository entry before publishing the installation PR. The chart workflow overrides chart/app versions from the final tag, pins the source checkout to that release, and waits for its default Docker image before packaging.
4. Report the installation PR and its merge state separately from the completed release. Reuse existing merge authorization and repository checks; do not infer new merge authority from this reference.

## rustfs.com top banner

Use the existing announcement configuration from [website PR #112](https://github.com/rustfs/rustfs.com/pull/112). Do not add a second banner or redesign the header.

1. In an isolated checkout of `rustfs/rustfs.com`, read its current `AGENTS.md`, PR template if present, `data/announcement.ts`, and the consuming header component. Search for an existing announcement PR for this target before creating another.
2. Update `homeAnnouncement` in `data/announcement.ts` after the release and installation availability checks pass. Keep `enabled: true`, `badge: 'New'`, and the existing component. For a stable release, use:

   ```ts
   export const homeAnnouncement = {
     enabled: true,
     badge: 'New',
     message: 'RustFS <target> is now available! Please',
     linkText: 'upgrade and try',
     href: '/download',
   } as const
   ```

   Replace `<target>` with the exact published version. Reserve GA wording for the actual first stable release. For a final alpha/beta/rc tag, explicitly call it a prerelease; use its GitHub Release URL if `/download` does not expose that target. Never announce a `-preview.N` build here.
3. Use the site's current package manager and required checks. For this config-only change, type-check and lint the changed file, build the site, and inspect the generated homepage for the version, visible link text, and destination. Confirm `/download` actually offers the announced version before using that link.
4. Commit, push, and create or update the website PR when included in the authorized release scope. Include the published RustFS Release URL and checks run. Keep merge and production-deployment authority separate; reuse permission already given, but this reference grants none by itself.
5. After an authorized merge/deployment, verify the live `https://rustfs.com/` banner and its destination. If only the PR is ready, report that state and URL; do not report the banner as deployed. A website failure does not invalidate or move the published release tag: retry this phase after resolving the failure.

## Reconcile main and remove the completed release branch

1. Compare the exact published tag with current main. Audit every release-side commit since their merge base, including inherited backports. An ahead count is a history difference, not proof of missing changes. Use ancestry and patch equivalence, then inspect current code and regression evidence for squashed, adapted, superseded, or reverted changes. Record each commit as present, superseded, release-only, or missing; a positive `git cherry` result alone does not prove a missing fix.
2. Bring missing shared changes to main through a reviewed PR and the required checks before closing the follow-up. Preserve main's later changes; decide version bumps and pinned Console configuration separately. If a history merge is needed, use a merge commit and verify the release commit is an ancestor of main; squash does not establish that ancestry. Do not merge main into the published release branch or move its tags. Report unresolved changes instead of claiming reconciliation is complete.
3. After publication and authorized follow-ups are complete, delete the remote `release/<target>` branch. Immediately before deletion, verify its HEAD still equals the published tag's commit and no open PR targets or originates from it. Preserve a branch with newer work or open PRs and report the blocker. Use a deletion with an expected-old-SHA check to avoid removing a concurrently advanced branch.
4. Verify the remote branch is absent and the final and preview tags still resolve to their recorded commits. Remove a local release branch only if it is task-owned, unchecked out, and has no unpreserved work. Record cleanup separately from artifact publication; an explicitly authorized deletion before reconciliation does not make unresolved main changes complete.
