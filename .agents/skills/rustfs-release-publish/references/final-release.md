# Final release publication

Read only after fresh confirmation of the complete candidate report. The [parent confirmation gate](../SKILL.md#human-confirmation) applies.

Dispatch `release-publish.yml` on main with the successful completion run ID and approved `release-manifest.json` SHA-256. Do not create the tag manually. The workflow revalidates source, bytes and trusted certification, then creates the annotated final tag at `CANDIDATE_SHA`. A recipe change fails closed and requires a new candidate; unrelated main changes do not require a rebuild.

Require both versioned publication and channel publication jobs to succeed. Verify the final tag's commit and manifest identity; all six platform ZIPs and compatibility aliases; DEB/RPM, checksums, SBOM, provenance and evidence bundle; both multi-architecture image variants in Docker Hub, GHCR and Quay; and the accepted Helm chart in the chart repository. Download a host-compatible final artifact and confirm its digest is exactly the accepted candidate digest and its version/source output agrees.

For stable releases verify GitHub Latest, R2 download aliases and `latest.json` unless a newer stable release already owns those channels. Prereleases must remain GitHub prereleases and leave stable pointers unchanged. Verify generated release notes compare against the frozen previous stable release, excluding internal previews.

On infrastructure failure retry the same workflow/seal; matching destinations are reused. Never clobber an asset, force a tag, rebuild the image or silently accept a changed digest. A source fix after the final tag requires a new patch version. Cross-registry writes are not atomic: report the unfinished destination rather than claiming publication complete.

New candidates create no preview Releases to clean up. For a legacy preview release, preserve its tag and remove its Release only after final delivery and notes are verified. Do not pass `--cleanup-tag`. Complete the [post-release follow-up](post-release-updates.md) separately.
