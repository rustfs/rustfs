# Preview Release Verification

Read for Phase 3 after the candidate passed exact-SHA CI. The [parent release invariants](../SKILL.md) apply.

## Preview channels

- Use `<target>-preview.N` for every target, e.g. `1.0.0-beta.10-preview.3` or `1.1.0-preview.1`.
- The canonical suffix is exactly `-preview.<digits>`. `build.yml` recognizes it before alpha/beta/rc classification and routes it to the preview-only path; any other tag containing `-preview` fails closed instead of being treated as a release.
- A preview Release MUST be published with `isPrerelease=true` and `isLatest=false`. Any `*-latest` preview asset or preview-triggered `latest.json`, R2, Helm, or moving Docker channel tag is a pipeline failure. Docker images may use the exact preview version tag, including its existing variant suffixes.
- Preview Releases are cleaned up by the `cleanup-preview-releases` job after `publish-release` succeeds for the deliverable tag. It deletes every Release whose tag is exactly `<target>-preview.<digits>` and never passes `--cleanup-tag`, so the tags survive.

## Phase 3 — CI and preview Release verification

- Find and watch the tag build: `gh run list --workflow build.yml --branch "<preview-tag>" --limit 1` then `gh run watch <run-id>`. Every build matrix target must succeed (linux x86_64/aarch64 × musl/gnu, macos-aarch64, windows-x86_64).
- Confirm the preview tag resolves to the Phase 2 CI-validated `PREVIEW_HASH`. If the release branch advances during acceptance, repeat Phases 2–5 with a new preview tag.
- Confirm the Release publication jobs (`create-release`, `upload-release-assets`, and `publish-release`) succeed while `update-latest-version` is skipped.
- Verify `gh release view "<preview-tag>" --json isPrerelease,assets,url`: `isPrerelease` must be `true`, and the Release must contain all 6 versioned platform zips, checksums, SBOM, and provenance with no `-latest` assets. Confirm `gh api repos/{owner}/{repo}/releases/latest --jq .tag_name` does not return `<preview-tag>`.
- Record `PREVIOUS_DELIVERABLE`, selected from published Releases by `publishedAt` after excluding the current tag and every `-preview.N` tag. Verify `gh release view "<preview-tag>" --json body --jq .body` contains `## What's Changed` and, when `PREVIOUS_DELIVERABLE` exists, `**Full Changelog**: https://github.com/rustfs/rustfs/compare/<PREVIOUS_DELIVERABLE>...<preview-tag>`. For a repository with no previous deliverable, verify a Full Changelog link exists and record the GitHub baseline fallback.
- Confirm Helm is skipped. If preview Docker images are published, confirm they use only exact preview tags (and variant suffixes), with no `latest`, `alpha`, `beta`, or `rc` channel updates. Preview validation covers the built RustFS binaries, embedded console, and rc compatibility; it does not authorize advancing any installation default.

## Release notes

- When a previous deliverable exists, GitHub Release notes for the preview and final tags MUST use it as their shared comparison baseline: the most recently published non-preview Release before the target. Internal `-preview.N` Releases are explicitly excluded from that selection, even when they point at the same commit as the final tag — cleanup runs after the notes are generated, so the preview Release is still present and would otherwise be picked as the baseline. If no previous deliverable exists, omit `previous_tag_name` and record that GitHub's default baseline fallback was used.
- Generated Release notes carry a workflow-management marker so retries can repair them. Before manually curating a generated body, remove that marker; unmarked non-placeholder notes are preserved by later workflow runs.
