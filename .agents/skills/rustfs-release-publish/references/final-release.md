# Final Release Publication

Read only after fresh user confirmation of the preview acceptance report. The [parent confirmation gate](../SKILL.md#human-confirmation) applies.

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

Never pass `--cleanup-tag` when removing preview Releases. Remove them manually only if `cleanup-preview-releases` failed, after final publication and notes generation. Record deleted Releases and surviving tags.
