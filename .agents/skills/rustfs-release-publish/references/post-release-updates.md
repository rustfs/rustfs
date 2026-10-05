# Post-release Milestone, Installation, and Website Updates

Read only after the final non-preview tag has passed Phase 6. Tag creation or a green binary build alone is insufficient: the release assets, source archive, container images, Helm package, and applicable latest channels must be available. Preview tags never enter this phase.

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
